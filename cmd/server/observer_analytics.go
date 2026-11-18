// Package main — observer analytics helpers.
//
// #1828 Phase A: extracted from cmd/server/routes.go handleObserverAnalytics
// (routes.go:2819-2953). Split the 5 aggregate builders into standalone
// functions for readability + isolated tests. No behavior change relative to
// the pre-#1828 handler; JSON output is byte-identical.
//
// The `filtered` argument is the time-window-filtered, timestamp-desc sorted
// slice of *StoreObs produced by the handler after snapshotting under RLock
// (see #1481 P0-2). Helpers do NOT touch store.mu — the handler owns lock
// scoping.
//
// Concurrency note (#1830): buildPacketTypes / buildNodesTimeline /
// buildRecentPackets take a txByID map instead of *PacketStore for
// transmission lookups. store.byTxID is guarded by store.mu (writes from
// ingest/eviction); reading it here — after the handler's RLock snapshot has
// already been released, to keep JSON decode/enrichment off the hot lock per
// #1481 P0-2 — would race with those writers (Go maps can panic with
// "concurrent map read and map write" during a rehash, not just fail under
// -race). The handler resolves txByID once, under the same RLock that
// snapshots the observation slice, and passes it down instead.
//
// Perf note (#1839 MINOR): dropping enrichObs on the histogram / nodes-
// timeline paths also eliminates N fetchResolvedPathForObs SQL calls per
// /analytics request (store.go:fetchResolvedPathForObs), not just the alloc/
// boxing win — /analytics SQL-load drops materially.
package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// observerAnalyticsBucketDur returns the timeline bucket size for a given day
// range. Mirrors the constant table in the legacy handler.
func observerAnalyticsBucketDur(days int) time.Duration {
	bucketDur := 24 * time.Hour
	if days <= 1 {
		bucketDur = time.Hour
	} else if days <= 7 {
		bucketDur = 4 * time.Hour
	}
	return bucketDur
}

// observerAnalyticsFormatLabel returns a closure that formats a bucket time
// as a human-readable label appropriate for the day range.
func observerAnalyticsFormatLabel(days int) func(time.Time) string {
	return func(t time.Time) string {
		if days <= 1 {
			return t.UTC().Format("15:04")
		}
		if days <= 7 {
			return t.UTC().Format("Mon 15:04")
		}
		return t.UTC().Format("Jan 02")
	}
}

// buildTimelineBuckets is the shared kernel used by both buildTimeline and
// buildNodesTimeline. Sorts the count map by bucket-key and returns
// TimeBucket entries with labels formatted per days.
func buildTimelineBuckets(counts map[int64]int, days int) []TimeBucket {
	formatLabel := observerAnalyticsFormatLabel(days)
	keys := make([]int64, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]TimeBucket, 0, len(keys))
	for _, k := range keys {
		lbl := formatLabel(time.Unix(k, 0))
		out = append(out, TimeBucket{Label: &lbl, Count: counts[k]})
	}
	return out
}

// buildTimeline builds the packet-timeline aggregate for /analytics.
func buildTimeline(filtered []*StoreObs, days int) []TimeBucket {
	bucketDur := observerAnalyticsBucketDur(days)
	counts := map[int64]int{}
	for _, obs := range filtered {
		ts, ok := obs.ParsedTime()
		if !ok {
			continue
		}
		bucketStart := ts.UTC().Truncate(bucketDur).Unix()
		counts[bucketStart]++
	}
	return buildTimelineBuckets(counts, days)
}

func addObserverPacketType(counts map[string]int, tx *StoreTx) {
	if tx != nil && tx.PayloadType != nil {
		counts[strconv.Itoa(*tx.PayloadType)]++
	}
}

// buildPacketTypes builds the payload-type histogram. Uses a direct
// store.byTxID read (see #1828 triage) rather than enrichObs — the loop only
// needs payload_type, which is a single indirection off the transmission.
// This avoids one map allocation + several interface-boxing conversions per
// observation (routes.go:2886 hot path pre-#1828).
func buildPacketTypes(filtered []*StoreObs, txByID map[int]*StoreTx) map[string]int {
	out := map[string]int{}
	for _, obs := range filtered {
		addObserverPacketType(out, txByID[obs.TransmissionID])
	}
	return out
}

// buildNodesTimeline builds the distinct-node-per-bucket timeline aggregate.
// Nodes = path-json hops ∪ decoded_json pubKey/srcHash/destHash.
func buildNodesTimeline(filtered []*StoreObs, days int, txByID map[int]*StoreTx) []TimeBucket {
	bucketDur := observerAnalyticsBucketDur(days)
	nodeBucketSets := map[int64]map[string]struct{}{}
	for _, obs := range filtered {
		ts, ok := obs.ParsedTime()
		if !ok {
			continue
		}
		bucketStart := ts.UTC().Truncate(bucketDur).Unix()
		if nodeBucketSets[bucketStart] == nil {
			nodeBucketSets[bucketStart] = map[string]struct{}{}
		}
		addObserverNodes(nodeBucketSets[bucketStart], txByID[obs.TransmissionID], obs)
	}
	nodeCounts := make(map[int64]int, len(nodeBucketSets))
	for k, nodes := range nodeBucketSets {
		nodeCounts[k] = len(nodes)
	}
	return buildTimelineBuckets(nodeCounts, days)
}

func addObserverNodes(nodes map[string]struct{}, tx *StoreTx, obs *StoreObs) {
	if tx != nil && tx.DecodedJSON != "" {
		var decoded map[string]json.RawMessage
		if json.Unmarshal([]byte(tx.DecodedJSON), &decoded) == nil {
			for _, key := range []string{"pubKey", "srcHash", "destHash"} {
				var value string
				if json.Unmarshal(decoded[key], &value) == nil && value != "" {
					nodes[value] = struct{}{}
				}
			}
		}
	}
	for _, hop := range parsePathJSON(obs.PathJSON) {
		if hop != "" {
			nodes[hop] = struct{}{}
		}
	}
}

func addObserverSnr(buckets map[int]*SnrDistributionEntry, obs *StoreObs) {
	if obs.SNR == nil {
		return
	}
	// Keep the established treatment of negative fractional values exactly.
	bucket := int(*obs.SNR) / 2 * 2
	if *obs.SNR < 0 && int(*obs.SNR) != bucket {
		bucket -= 2
	}
	if buckets[bucket] == nil {
		buckets[bucket] = &SnrDistributionEntry{Range: fmt.Sprintf("%d to %d", bucket, bucket+2)}
	}
	buckets[bucket].Count++
}

// buildSnrDistribution builds the SNR histogram (2-unit buckets, floor).
func buildSnrDistribution(filtered []*StoreObs) []SnrDistributionEntry {
	snrBuckets := map[int]*SnrDistributionEntry{}
	for _, obs := range filtered {
		addObserverSnr(snrBuckets, obs)
	}
	return observerSnrDistribution(snrBuckets)
}

func observerSnrDistribution(snrBuckets map[int]*SnrDistributionEntry) []SnrDistributionEntry {
	keys := make([]int, 0, len(snrBuckets))
	for k := range snrBuckets {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	out := make([]SnrDistributionEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, *snrBuckets[k])
	}
	return out
}

// buildRecentPackets builds the "first N enriched observations" list. This is
// the only aggregate that needs the full enrichObs map — recentPackets is a
// UI-facing payload and the extra fields matter here.
//
// Legacy parity (#1839): the pre-#1828 routes.go loop was
//
//	for i, obs := range filtered {
//	    if _, ok := obs.ParsedTime(); !ok { continue }
//	    ...
//	    if i < limit { recentPackets = append(..., enriched) }
//	}
//
// The `i` in the gate is the RAW slice index — a bad-ts obs at position k<limit
// consumed its slot (via `continue`) and could not be replaced by a later
// good-ts obs. Result can be <limit when bad-ts obs sit in the head of
// `filtered`. We reproduce that exact semantic here to keep output byte-
// identical.
func buildRecentPackets(store *PacketStore, filtered []*StoreObs, limit int, txByID map[int]*StoreTx) []map[string]interface{} {
	if limit <= 0 {
		return []map[string]interface{}{}
	}
	out := make([]map[string]interface{}, 0, limit)
	for i, obs := range filtered {
		if i >= limit {
			break
		}
		if _, ok := obs.ParsedTime(); !ok {
			continue
		}
		out = append(out, store.enrichObsWithTx(obs, txByID[obs.TransmissionID]))
	}
	return out
}

// txAnalyticsFields returns a detached copy of the tx fields the helpers in
// this file read (hash, raw hex, payload and route type, decoded JSON), or
// nil for a nil tx. Caller holds store.mu: the content-hash migration and the
// rewrite feed change tx.Hash and tx.DecodedJSON under it, and the helpers
// run after the handler released it (#2237).
func txAnalyticsFields(tx *StoreTx) *StoreTx {
	if tx == nil {
		return nil
	}
	return &StoreTx{
		ID:          tx.ID,
		Hash:        tx.Hash,
		RawHex:      tx.RawHex,
		PayloadType: tx.PayloadType,
		RouteType:   tx.RouteType,
		DecodedJSON: tx.DecodedJSON,
	}
}

type observerRecentPacket struct {
	timestamp string
	fields    packetFields
}

// Retained history is reduced without an observation snapshot. Only grouped
// counters, distinct-node sets and the twenty rendered response rows survive
// a visit; a historical packet or its observation list is never retained.
type observerAnalyticsAccum struct {
	days      int
	bucketDur time.Duration
	timeline  map[int64]int
	types     map[string]int
	nodes     map[int64]map[string]struct{}
	snr       map[int]*SnrDistributionEntry
	recent    []observerRecentPacket
}

func newObserverAnalyticsAccum(days int) *observerAnalyticsAccum {
	return &observerAnalyticsAccum{days: days, bucketDur: observerAnalyticsBucketDur(days),
		timeline: make(map[int64]int), types: make(map[string]int),
		nodes: make(map[int64]map[string]struct{}), snr: make(map[int]*SnrDistributionEntry),
		recent: make([]observerRecentPacket, 0, 20)}
}

func (a *observerAnalyticsAccum) add(tx *StoreTx, obs *StoreObs, render func() packetFields) {
	stamp, ok := obs.ParsedTime()
	if !ok {
		return
	}
	bucket := stamp.UTC().Truncate(a.bucketDur).Unix()
	a.timeline[bucket]++
	addObserverPacketType(a.types, tx)
	if a.nodes[bucket] == nil {
		a.nodes[bucket] = make(map[string]struct{})
	}
	addObserverNodes(a.nodes[bucket], tx, obs)
	addObserverSnr(a.snr, obs)
	// Transmission traversal order is unrelated to reception time. Keep the
	// actual newest twenty observations, using the handler's timestamp order.
	position := sort.Search(len(a.recent), func(i int) bool { return a.recent[i].timestamp < obs.Timestamp })
	if position >= 20 {
		return
	}
	packet := observerRecentPacket{timestamp: obs.Timestamp, fields: render()}
	if len(a.recent) < 20 {
		a.recent = append(a.recent, observerRecentPacket{})
	}
	copy(a.recent[position+1:], a.recent[position:len(a.recent)-1])
	a.recent[position] = packet
}

func (a *observerAnalyticsAccum) response() ObserverAnalyticsResponse {
	nodeCounts := make(map[int64]int, len(a.nodes))
	for bucket, nodes := range a.nodes {
		nodeCounts[bucket] = len(nodes)
	}
	recent := make([]packetFields, 0, len(a.recent))
	for _, packet := range a.recent {
		recent = append(recent, packet.fields)
	}
	return ObserverAnalyticsResponse{Timeline: buildTimelineBuckets(a.timeline, a.days), PacketTypes: a.types,
		NodesTimeline: buildTimelineBuckets(nodeCounts, a.days), SnrDistribution: observerSnrDistribution(a.snr),
		RecentPackets: recent}
}
