package main

import (
	"container/heap"
	"context"
	"fmt"
	"hash/maphash"
	"log"
	"math"
	"sort"
	"strings"
	"time"
)

const subpathMaxLen = 8

type subpathTally struct {
	tx    int
	count int32
	start uint16
	n     uint16
}

type subpathAccum struct {
	count int
	raw   string
}

// The hot path uses the same compact, pointer-free tally for either source.
// Scoped analytics historically grouped equal rendered names even if the
// underlying runs had different lengths, for example a name containing " → ".
// Store per-length tallies only for those names so bulk ranges can still agree
// with the corresponding single request.
type subpathCounter struct {
	seed     maphash.Seed
	min, max int
	foldCase bool
	resolve  func(string) string
	total    int
	tallies  map[uint64]subpathTally
	byLength map[uint64][]subpathTally
	named    []string
	key      []byte
}

func newSubpathCounter(groups []subpathGroup, foldCase bool, resolve func(string) string) *subpathCounter {
	minLen, maxLen := math.MaxInt, 0
	for _, g := range groups {
		minLen, maxLen = min(minLen, max(2, g.MinLen)), max(maxLen, g.MaxLen)
	}
	if foldCase {
		maxLen = min(maxLen, subpathMaxLen)
	}
	return &subpathCounter{
		seed: maphash.MakeSeed(), min: minLen, max: maxLen, foldCase: foldCase, resolve: resolve,
		tallies: make(map[uint64]subpathTally),
	}
}

func (c *subpathCounter) rawHop(hop string) string {
	if c.foldCase {
		return strings.ToLower(hop)
	}
	return hop
}

func (c *subpathCounter) add(txID int, hops []string) {
	if len(hops) < 2 {
		return
	}
	c.total++
	c.named = c.named[:0]
	for _, hop := range hops {
		c.named = append(c.named, c.resolve(c.rawHop(hop)))
	}
	for n := c.min; n <= min(c.max, len(hops)); n++ {
		for start := 0; start+n <= len(hops); start++ {
			c.key = appendNamedSubpath(c.key[:0], c.named[start:start+n])
			key := maphash.Bytes(c.seed, c.key)
			old, found := c.tallies[key]
			if byLength := c.byLength[key]; byLength != nil {
				i := 0
				for i < len(byLength) && byLength[i].n != uint16(n) {
					i++
				}
				if i == len(byLength) {
					c.byLength[key] = append(byLength, subpathTally{tx: txID, count: 1, start: uint16(start), n: uint16(n)})
				} else {
					byLength[i].count++
				}
			} else if !found {
				c.tallies[key] = subpathTally{tx: txID, count: 1, start: uint16(start), n: uint16(n)}
			} else if !c.foldCase && old.n != uint16(n) {
				if c.byLength == nil {
					c.byLength = make(map[uint64][]subpathTally)
				}
				c.byLength[key] = []subpathTally{old, {tx: txID, count: 1, start: uint16(start), n: uint16(n)}}
				delete(c.tallies, key)
			} else {
				old.count++
				c.tallies[key] = old
			}
		}
	}
}

func appendNamedSubpath(buf []byte, named []string) []byte {
	for i, n := range named {
		if i > 0 {
			buf = append(buf, " → "...)
		}
		buf = append(buf, n...)
	}
	return buf
}

func (c *subpathCounter) top(g subpathGroup) []subpathTally {
	if g.Limit <= 0 {
		return nil
	}
	h := make(tallyHeap, 0, g.Limit)
	fits := func(n uint16) bool { return int(n) >= g.MinLen && int(n) <= g.MaxLen }
	offer := func(t subpathTally) {
		if len(h) < g.Limit {
			heap.Push(&h, t)
		} else if t.count > h[0].count {
			h[0] = t
			heap.Fix(&h, 0)
		}
	}
	for _, tally := range c.tallies {
		if fits(tally.n) {
			offer(tally)
		}
	}
	for _, lengths := range c.byLength {
		var chosen subpathTally
		// Lengths are in first-visit order. Retained tx IDs can differ from
		// visit order when transmissions are ingested out of timestamp order.
		for _, tally := range lengths {
			if !fits(tally.n) {
				continue
			}
			if chosen.count == 0 {
				chosen = tally
			} else {
				chosen.count += tally.count
			}
		}
		if chosen.count > 0 {
			offer(chosen)
		}
	}
	return h
}

func (c *subpathCounter) names(top []subpathTally, pathFor func(int) []string) map[string]*subpathAccum {
	result := make(map[string]*subpathAccum, len(top))
	var named []string
	for _, t := range top {
		hops := pathFor(t.tx)[int(t.start) : int(t.start)+int(t.n)]
		raw := make([]string, len(hops))
		named = named[:0]
		for j, hop := range hops {
			raw[j] = c.rawHop(hop)
			named = append(named, c.resolve(raw[j]))
		}
		result[string(appendNamedSubpath(nil, named))] = &subpathAccum{count: int(t.count), raw: strings.Join(raw, ",")}
	}
	return result
}

type tallyHeap []subpathTally

func (h tallyHeap) Len() int           { return len(h) }
func (h tallyHeap) Less(i, j int) bool { return h[i].count < h[j].count }
func (h tallyHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *tallyHeap) Push(x any)        { *h = append(*h, x.(subpathTally)) }
func (h *tallyHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func makeSubpathResolver(pm *prefixMap, graph *NeighborGraph, hopContext []string) func(string) string {
	hops := make(map[string]string)
	return func(hop string) string {
		if name, ok := hops[hop]; ok {
			return name
		}
		name := hop
		if resolved, _, _ := pm.resolveWithContext(hop, hopContext, graph); resolved != nil {
			name = resolved.Name
		}
		if len(hops) < 8192 {
			hops[hop] = name
		}
		return name
	}
}

func (s *PacketStore) topNamedSubpaths(groups []subpathGroup, resolve func(string) string) ([]map[string]*subpathAccum, int) {
	counter := newSubpathCounter(groups, true, resolve)
	for i, tx := range s.packets {
		counter.add(i, txGetParsedPath(tx))
	}
	out := make([]map[string]*subpathAccum, len(groups))
	for i, g := range groups {
		out[i] = counter.names(counter.top(g), func(id int) []string { return txGetParsedPath(s.packets[id]) })
	}
	return out, counter.total
}

func subpathIncludes(tx *StoreTx, window TimeWindow, regionObservers map[string]bool) bool {
	if !window.IsZero() && !window.Includes(tx.FirstSeen) {
		return false
	}
	if regionObservers == nil {
		return true
	}
	for _, obs := range tx.Observations {
		if regionObservers[obs.ObserverID] {
			return true
		}
	}
	return false
}

func (s *PacketStore) computeResidentSubpaths(region string, groups []subpathGroup, window TimeWindow) []packetFields {
	var observers map[string]bool
	if region != "" {
		observers = s.resolveRegionObservers(region)
	}
	_, pm := s.getCachedNodesAndPM()
	s.mu.RLock()
	defer s.mu.RUnlock()
	resolve := makeSubpathResolver(pm, s.graph.Load(), buildAggregateHopContextPubkeys(s.packets, pm))
	counter := newSubpathCounter(groups, region == "" && window.IsZero(), resolve)
	for i, tx := range s.packets {
		if subpathIncludes(tx, window, observers) {
			counter.add(i, txGetParsedPath(tx))
		}
	}
	return s.rankSubpathGroups(counter, groups, func(id int) []string { return txGetParsedPath(s.packets[id]) })
}

func (s *PacketStore) computeAnalyticsSubpaths(region string, minLen, maxLen, limit int) packetFields {
	return s.computeAnalyticsSubpathsWindowed(region, minLen, maxLen, limit, TimeWindow{})
}

func (s *PacketStore) computeAnalyticsSubpathsWindowed(region string, minLen, maxLen, limit int, window TimeWindow) packetFields {
	return s.computeResidentSubpaths(region, []subpathGroup{{minLen, maxLen, limit}}, window)[0]
}

func (s *PacketStore) rankSubpathGroups(counter *subpathCounter, groups []subpathGroup, pathFor func(int) []string) []packetFields {
	result := make([]packetFields, len(groups))
	for i, g := range groups {
		result[i] = s.rankSubpaths(counter.names(counter.top(g), pathFor), counter.total, g.Limit)
	}
	return result
}

func (s *PacketStore) GetAnalyticsSubpaths(region string, minLen, maxLen, limit int) packetFields {
	return s.GetAnalyticsSubpathsWithWindow(region, minLen, maxLen, limit, TimeWindow{})
}

func (s *PacketStore) GetAnalyticsSubpathsWithWindow(region string, minLen, maxLen, limit int, window TimeWindow) packetFields {
	result := s.GetAnalyticsSubpathsBulkWithWindow(region, []subpathGroup{{minLen, maxLen, limit}}, window)
	if len(result) == 0 {
		return nil
	}
	return result[0]
}

func (s *PacketStore) GetAnalyticsSubpathsBulk(region string, groups []subpathGroup) []packetFields {
	return s.GetAnalyticsSubpathsBulkWithWindow(region, groups, TimeWindow{})
}

func (s *PacketStore) GetAnalyticsSubpathsBulkWithWindow(region string, groups []subpathGroup, window TimeWindow) []packetFields {
	result, err := s.analyticsSubpaths(context.Background(), region, groups, window)
	if err != nil {
		log.Printf("[subpaths] analytics: %v", err)
	}
	return result
}

func (s *PacketStore) analyticsSubpaths(ctx context.Context, region string, groups []subpathGroup, window TimeWindow) ([]packetFields, error) {
	retained := s.packetHistoryRequired()
	keys := make([]string, len(groups))
	results := make([]packetFields, len(groups))
	now := time.Now()
	s.cacheMu.Lock()
	hits := 0
	for i, g := range groups {
		keys[i] = fmt.Sprintf("%t|%s|%d|%d|%d|%s", retained, region, g.MinLen, g.MaxLen, g.Limit, window.CacheKey())
		if cached := s.subpathCache[keys[i]]; cached != nil && now.Before(cached.expiresAt) {
			results[i] = cached.data
			hits++
		}
	}
	if hits == len(groups) {
		s.cacheHits += int64(hits)
		s.cacheMu.Unlock()
		return results, ctx.Err()
	}
	s.cacheMisses += int64(len(groups) - hits)
	s.cacheMu.Unlock()

	if retained {
		var err error
		results, err = s.computeRetainedSubpaths(ctx, region, groups, window)
		if err != nil {
			return nil, err
		}
	} else {
		results = s.computeResidentSubpaths(region, groups, window)
	}

	expires := time.Now().Add(s.rfCacheTTL)
	s.cacheMu.Lock()
	for i, key := range keys {
		s.subpathCache[key] = &cachedResult{data: results[i], expiresAt: expires}
	}
	s.cacheMu.Unlock()
	return results, ctx.Err()
}

// rankSubpaths sorts accumulated subpath counts by frequency, truncates to
// limit, and builds the API response map.
func (s *PacketStore) rankSubpaths(counts map[string]*subpathAccum, totalPaths, limit int) packetFields {
	type subpathEntry struct {
		path  string
		count int
		raw   string
	}
	ranked := make([]subpathEntry, 0, len(counts))
	for path, data := range counts {
		ranked = append(ranked, subpathEntry{path, data.count, data.raw})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].count != ranked[j].count {
			return ranked[i].count > ranked[j].count
		}
		return ranked[i].path < ranked[j].path
	})
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}

	subpaths := make([]packetFields, 0, len(ranked))
	for _, e := range ranked {
		pct := 0.0
		if totalPaths > 0 {
			pct = math.Round(float64(e.count)/float64(totalPaths)*1000) / 10
		}
		subpaths = append(subpaths, packetFields{
			"path":    e.path,
			"rawHops": strings.Split(e.raw, ","),
			"count":   e.count,
			"hops":    len(strings.Split(e.path, " → ")),
			"pct":     pct,
		})
	}

	return packetFields{
		"subpaths":   subpaths,
		"totalPaths": totalPaths,
	}
}
