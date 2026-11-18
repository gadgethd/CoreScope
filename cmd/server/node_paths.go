package main

import (
	"sort"
	"strings"
)

// collectResidentNodePaths uses the path-hop index when the cache covers the
// query, and confirms exact pubkeys against persisted paths after dropping the
// store lock.
func (s *PacketStore) collectResidentNodePaths(lowerPK string, aggregate *nodePathAggregate) {
	prefix1 := lowerPK[:min(2, len(lowerPK))]
	prefix2 := lowerPK[:min(4, len(lowerPK))]

	type candidate struct {
		tx                   *StoreTx
		confirmed            bool
		requiresConfirmation bool
	}
	var candidates []candidate
	snapshots := make(map[int][]rpObs)
	seen := make(map[int]bool)
	s.mu.RLock()
	indexed := make(map[int]bool)
	if s.useResolvedPathIndex {
		for _, id := range s.resolvedPubkeyIndex[resolvedPubkeyHash(lowerPK)] {
			indexed[id] = true
		}
	}
	addCandidates := func(key string, confirmed bool) {
		for _, tx := range s.byPathHop[key] {
			if seen[tx.ID] {
				continue
			}
			seen[tx.ID] = true
			_, hasReverse := s.resolvedPubkeyReverse[tx.ID]
			needsConfirmation := s.useResolvedPathIndex && hasReverse
			if needsConfirmation && !indexed[tx.ID] {
				continue
			}
			candidates = append(candidates, candidate{tx: tx, confirmed: confirmed, requiresConfirmation: needsConfirmation})
			snapshots[tx.ID] = snapshotRPObs(tx)
		}
	}
	addCandidates(lowerPK, true)
	addCandidates(prefix1, false)
	addCandidates(prefix2, false)
	for key := range s.byPathHop {
		if matchesLongPathPrefix(key, lowerPK, prefix2) {
			addCandidates(key, false)
		}
	}
	s.mu.RUnlock()

	// Use the same stored path as the packet page (#1278). The hash index
	// needs a separate collision check only when the canonical path is absent.
	canonical := s.loadCanonicalResolvedPaths(snapshots)
	ready := candidates[:0]
	for _, record := range candidates {
		if record.requiresConfirmation && canonical[record.tx.ID] == nil {
			if !s.confirmResolvedPathContains(record.tx.ID, lowerPK) {
				continue
			}
			record.confirmed = true
		}
		ready = append(ready, record)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, record := range ready {
		aggregate.add(record.tx, canonical[record.tx.ID], record.confirmed)
	}
}

func matchesLongPathPrefix(hop, target, prefix2 string) bool {
	return len(hop) > 4 && len(hop) < len(target) && strings.HasPrefix(hop, prefix2)
}

// One compact representative per distinct resolved path is sufficient for
// exact counts; only the returned page needs name and coordinate fields.
type nodePathHop struct {
	prefix, pubkey string
}

type nodePathGroup struct {
	signature  string
	hops       []nodePathHop
	count      int
	lastSeen   string
	sampleHash string
}

type nodePathAggregate struct {
	target             string
	hopContext         []string
	pm                 *prefixMap
	graph              *NeighborGraph
	fallback           map[string]*nodeInfo
	groups             map[string]*nodePathGroup
	totalTransmissions int
}

func newNodePathAggregate(target string, pm *prefixMap, graph *NeighborGraph) *nodePathAggregate {
	return &nodePathAggregate{
		target: target, hopContext: []string{target}, pm: pm, graph: graph,
		fallback: make(map[string]*nodeInfo), groups: make(map[string]*nodePathGroup),
	}
}

func (a *nodePathAggregate) fallbackNode(hop string) *nodeInfo {
	if n, ok := a.fallback[hop]; ok {
		return n
	}
	pm := a.pm
	n, _, _ := pm.resolveWithContext(hop, a.hopContext, a.graph)
	if len(a.fallback) < 512 {
		a.fallback[hop] = n
	}
	return n
}

func (a *nodePathAggregate) addRetained(packet *retainedPacket) {
	tx := packet.tx
	canonical, _ := longestStoredResolvedPath(snapshotRPObs(tx), packet.paths)
	if canonical != nil {
		a.add(tx, canonical, false)
		return
	}
	// The SQLite prefilter may match a non-selected observation. Preserve
	// the resident index's best-path membership before resolving fallback hops.
	prefix1 := a.target[:min(2, len(a.target))]
	prefix2 := a.target[:min(4, len(a.target))]
	for _, hop := range txGetParsedPath(tx) {
		hop = strings.ToLower(hop)
		if hop == a.target || hop == prefix1 || hop == prefix2 || matchesLongPathPrefix(hop, a.target, prefix2) {
			a.add(tx, nil, hop == a.target)
			return
		}
	}
}

func (a *nodePathAggregate) add(tx *StoreTx, canonical []*string, preconfirmed bool) {
	hops := tx.parsedPath
	if !tx.pathParsed {
		hops = parsePathJSON(tx.PathJSON)
	}
	keys := make([]string, len(hops))
	containsTarget := false
	if canonical != nil {
		// Match the canonical observation's resolved pubkeys while displaying
		// the best raw path, as on the packet page (#1278, #810).
		for i := range min(len(hops), len(canonical)) {
			if canonical[i] != nil {
				keys[i] = strings.ToLower(*canonical[i])
				containsTarget = containsTarget || keys[i] == a.target
			}
		}
	} else {
		containsTarget = preconfirmed
		for i, hop := range hops {
			lowerHop := strings.ToLower(hop)
			matches := strings.HasPrefix(a.target, lowerHop)
			if n := a.fallbackNode(hop); n != nil {
				keys[i] = strings.ToLower(n.PublicKey)
				matches = keys[i] == a.target
			}
			// Anchoring the fallback resolver to the requested node is useful
			// for rendering. It cannot prove a hop with a sibling prefix
			// collision traversed that node (#1352).
			if lowerHop == a.target || (lowerHop != "" && matches && len(a.pm.m[lowerHop]) <= 1) {
				containsTarget = true
			}
		}
	}
	if !containsTarget {
		return
	}
	a.totalTransmissions++
	var signature strings.Builder
	for i, hop := range hops {
		if i > 0 {
			signature.WriteString("→")
		}
		if keys[i] != "" {
			signature.WriteString(keys[i])
		} else {
			signature.WriteString(hop)
		}
	}
	sig := signature.String()
	group := a.groups[sig]
	if group == nil {
		representative := make([]nodePathHop, len(hops))
		for i, hop := range hops {
			representative[i] = nodePathHop{prefix: hop, pubkey: keys[i]}
		}
		group = &nodePathGroup{
			signature: sig, hops: representative, lastSeen: tx.FirstSeen, sampleHash: tx.Hash,
		}
		a.groups[sig] = group
	} else if tx.FirstSeen > group.lastSeen || (tx.FirstSeen == group.lastSeen && tx.Hash < group.sampleHash) {
		group.lastSeen, group.sampleHash = tx.FirstSeen, tx.Hash
	}
	group.count++
}

func (g *nodePathGroup) ranksBefore(other *nodePathGroup) bool {
	if g.lastSeen != other.lastSeen {
		return g.lastSeen > other.lastSeen
	}
	if g.count != other.count {
		return g.count > other.count
	}
	return g.signature < other.signature
}

func (a *nodePathAggregate) paths() []PathEntryResp {
	const limit = 50
	ranked := make([]*nodePathGroup, 0, min(limit, len(a.groups)))
	for _, group := range a.groups {
		position := sort.Search(len(ranked), func(i int) bool { return group.ranksBefore(ranked[i]) })
		if position == limit {
			continue
		}
		if len(ranked) < limit {
			ranked = append(ranked, nil)
		}
		copy(ranked[position+1:], ranked[position:])
		ranked[position] = group
	}

	paths := make([]PathEntryResp, len(ranked))
	for i, group := range ranked {
		hops := make([]PathHopResp, len(group.hops))
		for j, hop := range group.hops {
			entry := PathHopResp{Prefix: hop.prefix, Name: hop.prefix}
			if hop.pubkey != "" {
				entry.Pubkey = hop.pubkey
				if n := a.pm.resolve(hop.pubkey); n != nil && strings.EqualFold(n.PublicKey, hop.pubkey) {
					entry.Name, entry.Pubkey = n.Name, n.PublicKey
					if n.HasGPS {
						entry.Lat, entry.Lon = n.Lat, n.Lon
					}
				}
			}
			hops[j] = entry
		}
		paths[i] = PathEntryResp{
			Hops: hops, Count: group.count, LastSeen: strOrNil(group.lastSeen), SampleHash: group.sampleHash,
		}
	}
	return paths
}
