package main

import (
	"context"
	"log"
	"sort"
	"strings"
	"time"
)

// subpathOccurrences counts overlapping runs, matching wire prefixes
// case-insensitively for both the resident and retained detail views.
func subpathOccurrences(hops, rawHops []string) int {
	count := 0
	for start := 0; start+len(rawHops) <= len(hops); start++ {
		match := true
		for i, hop := range rawHops {
			if !strings.EqualFold(hops[start+i], hop) {
				match = false
				break
			}
		}
		if match {
			count++
		}
	}
	return count
}

// txsWithSubpath retains the resident lookup by occurrence for store callers.
func (s *PacketStore) txsWithSubpath(rawHops []string) []*StoreTx {
	if len(rawHops) < 2 || len(rawHops) > subpathMaxLen {
		return nil
	}
	var matched []*StoreTx
	for _, tx := range s.packets {
		for range subpathOccurrences(txGetParsedPath(tx), rawHops) {
			matched = append(matched, tx)
		}
	}
	return matched
}

type subpathDetailAccum struct {
	pm                  *prefixMap
	graph               *NeighborGraph
	context             subpathContext
	firstSeen, lastSeen string
	hours               [24]int
	snrSum, rssiSum     float64
	snrCount, rssiCount int
	matchCount          int
	observers, parents  map[string]int
}

func (a *subpathDetailAccum) add(tx *StoreTx, count int) {
	a.matchCount += count
	ts := tx.FirstSeen
	if ts != "" {
		if a.firstSeen == "" || ts < a.firstSeen {
			a.firstSeen = ts
		}
		if a.lastSeen == "" || ts > a.lastSeen {
			a.lastSeen = ts
		}
		stamp, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			stamp, err = time.Parse("2006-01-02 15:04:05", ts)
		}
		if err == nil {
			a.hours[stamp.Hour()] += count
		}
	}
	if tx.displayObservation().SNR != nil {
		a.snrCount += count
		for range count {
			a.snrSum += *tx.displayObservation().SNR
		}
	}
	if tx.displayObservation().RSSI != nil {
		a.rssiCount += count
		for range count {
			a.rssiSum += *tx.displayObservation().RSSI
		}
	}
	if tx.displayObservation().ObserverName != "" {
		a.observers[tx.displayObservation().ObserverName] += count
	}

	pm := a.pm
	txContext := buildHopContextPubkeys(tx, pm)
	a.context.add(txContext)
	hops := txGetParsedPath(tx)
	resolved := make([]string, len(hops))
	for i, hop := range hops {
		resolved[i] = hop
		if node, _, _ := pm.resolveWithContext(hop, txContext, a.graph); node != nil {
			resolved[i] = node.Name
		}
	}
	a.parents[strings.Join(resolved, " → ")] += count
}

func (a *subpathDetailAccum) response(rawHops []string) packetFields {
	pm := a.pm
	nodes := make([]packetFields, len(rawHops))
	for i, hop := range rawHops {
		entry := packetFields{"hop": hop, "name": hop, "lat": nil, "lon": nil, "pubkey": nil}
		if node, _, _ := pm.resolveWithContext(hop, a.context.pubkeys, a.graph); node != nil {
			entry["name"], entry["pubkey"] = node.Name, node.PublicKey
			if node.HasGPS {
				entry["lat"], entry["lon"] = node.Lat, node.Lon
			}
		}
		nodes[i] = entry
	}

	var avgSnr, avgRssi any
	if a.snrCount > 0 {
		avgSnr = a.snrSum / float64(a.snrCount)
	}
	if a.rssiCount > 0 {
		avgRssi = a.rssiSum / float64(a.rssiCount)
	}
	return packetFields{
		"hops": rawHops, "nodes": nodes, "totalMatches": a.matchCount,
		"firstSeen": a.firstSeen, "lastSeen": a.lastSeen,
		"signal":           packetFields{"avgSnr": avgSnr, "avgRssi": avgRssi, "samples": a.snrCount},
		"hourDistribution": a.hours[:],
		"parentPaths":      topSubpathDetailCounts(a.parents, "path", 15),
		"observers":        topSubpathDetailCounts(a.observers, "name", 10),
	}
}

func topSubpathDetailCounts(counts map[string]int, field string, limit int) []packetFields {
	rows := make([]packetFields, 0, len(counts))
	for name, count := range counts {
		rows = append(rows, packetFields{field: name, "count": count})
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i]["count"].(int), rows[j]["count"].(int)
		if a != b {
			return a > b
		}
		return rows[i][field].(string) < rows[j][field].(string)
	})
	return rows[:min(len(rows), limit)]
}

func (s *PacketStore) GetSubpathDetail(rawHops []string) packetFields {
	result, err := s.subpathDetail(context.Background(), rawHops)
	if err != nil {
		log.Printf("[subpaths] detail: %v", err)
	}
	return result
}

func (s *PacketStore) subpathDetail(ctx context.Context, rawHops []string) (packetFields, error) {
	_, pm := s.getCachedNodesAndPM()
	accum := &subpathDetailAccum{
		pm: pm, graph: s.graph.Load(), observers: make(map[string]int), parents: make(map[string]int),
	}
	if len(rawHops) >= 2 && len(rawHops) <= subpathMaxLen {
		if s.packetHistoryRequired() {
			if err := s.visitRetainedSubpathMatches(ctx, rawHops, accum.add); err != nil {
				return nil, err
			}
		} else {
			s.mu.RLock()
			for _, tx := range s.packets {
				if count := subpathOccurrences(txGetParsedPath(tx), rawHops); count > 0 {
					accum.add(tx, count)
				}
			}
			s.mu.RUnlock()
		}
	}
	return accum.response(rawHops), ctx.Err()
}
