package main

import (
	"strings"
	"time"
)

// GetRepeaterRelayInfoMap returns a cached pubkey → RepeaterRelayInfo
// map covering EVERY pubkey that currently appears as a path hop in any
// non-advert StoreTx. This is the bulk equivalent of calling
// GetRepeaterRelayInfo(pk, windowHours) once per node.
//
// Why this exists (issue #1257): handleNodes used to call the per-node
// helper inside a per-page loop. Each call grabbed its own RLock and
// re-parsed FirstSeen on every StoreTx indexed under that pubkey's
// byPathHop entry (plus, when the pubkey was >= 2 hex chars, the 1-byte
// prefix bucket — which on busy networks fans out to almost the whole
// non-advert tx set). For the default top-50 page of hot repeaters this
// burned 50 lock acquisitions and hundreds of thousands of timestamp
// parses per request, dominating /api/nodes latency.
//
// The cached map is keyed by lowercase pubkey/hop key (same shape as
// byPathHop). Lookups should use strings.ToLower(pk).
//
// The cache is refreshed by the background recomputer (every 5 min by
// default). This function never rebuilds inline on a populated cache —
// serving a slightly stale snapshot is always preferable to a 700ms
// on-request rebuild. The only time an inline compute happens is when
// the cache is nil (i.e. before the recomputer's synchronous prewarm
// completes, which can occur in tests without a running recomputer).
func (s *PacketStore) GetRepeaterRelayInfoMap(windowHours float64) map[string]RepeaterRelayInfo {
	s.repeaterEnrichMu.Lock()
	cached := s.repeaterRelayCache
	s.repeaterEnrichMu.Unlock()
	if cached != nil {
		return cached
	}

	// Cache is nil — recomputer hasn't prewarmed yet (edge case: tests
	// without a running recomputer, or a request racing the initial
	// synchronous prewarm). Build once inline; the recomputer takes over.
	result := s.computeRepeaterRelayInfoMap(windowHours)

	s.repeaterEnrichMu.Lock()
	if s.repeaterRelayCache == nil {
		s.repeaterRelayCache = result
		s.repeaterRelayCacheWin = windowHours
		s.repeaterRelayAt = time.Now()
	}
	cached = s.repeaterRelayCache
	s.repeaterEnrichMu.Unlock()
	return cached
}

// computeRepeaterRelayInfoMap reduces the live hop buckets under one read
// lock. Each published result covers one store generation, including relay
// scope rewrites and evictions. Only the result and prefix deduplication
// need allocations; copying the whole index and caching each timestamp
// cost tens of MiB during every refresh.
func (s *PacketStore) computeRepeaterRelayInfoMap(windowHours float64) map[string]RepeaterRelayInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now().UTC()
	cutoff1h := now.Add(-1 * time.Hour)
	cutoff24h := now.Add(-24 * time.Hour)
	var windowCutoff time.Time
	if windowHours > 0 {
		windowCutoff = now.Add(-time.Duration(windowHours * float64(time.Hour)))
	}

	out := make(map[string]RepeaterRelayInfo, len(s.byPathHop))
	var seen map[int]struct{}
	for key, list := range s.byPathHop {
		info := RepeaterRelayInfo{WindowHours: windowHours}
		// #1751: accumulate the set of region scope names carried by this
		// hop key across every non-advert path-hop tx (NOT time-windowed).
		// Captured by the visit closure below — lazily allocated on the first
		// scope hit so hosts without scope_name pay nothing per key; converted
		// to a sorted, capped slice before this key's info is stored.
		var scopeSet map[string]struct{}
		// When key is longer than one wire byte, also fold
		// in the matching 1-byte raw-prefix bucket to mirror
		// GetRepeaterRelayInfo's behavior. We dedup by tx ID.
		var prefixList []*StoreTx
		if len(key) > 2 {
			prefixList = s.byPathHop[key[:2]]
		}
		deduplicate := len(prefixList) > 0
		if deduplicate {
			if seen == nil {
				seen = make(map[int]struct{}, len(list)+len(prefixList))
			}
			clear(seen)
		}
		// viaPrefix marks the 1-byte wire-prefix bucket, shared by every
		// node with the same first pubkey byte. See collectRelayEntriesLocked
		// for the counters-vs-scopes split (#662 / #1902).
		visit := func(txs []*StoreTx, viaPrefix bool) {
			for _, tx := range txs {
				if tx == nil {
					continue
				}
				if deduplicate {
					if _, exists := seen[tx.ID]; exists {
						continue
					}
					seen[tx.ID] = struct{}{}
				}
				if tx.PayloadType != nil && *tx.PayloadType == payloadTypeAdvert {
					continue
				}
				// #1751: scope accumulation is intentionally NOT gated on
				// timestamp parseability — a packet with an
				// unparseable first_seen still proves the repeater
				// transported that scope. RelayCount/LastRelayed below
				// remain timestamp-gated.
				//
				// #1902: it IS gated on full-pubkey attribution — a 1-byte
				// hop cannot prove which of the nodes sharing that byte
				// carried the packet.
				if !viaPrefix && tx.ScopeName != nil && *tx.ScopeName != "" {
					if scopeSet == nil {
						scopeSet = map[string]struct{}{}
					}
					scopeSet[*tx.ScopeName] = struct{}{}
				}
				observed, ok := parseRelayTS(tx.FirstSeen)
				if !ok {
					continue
				}
				if observed.After(cutoff24h) {
					info.RelayCount24h++
					if tx.RouteType != nil && *tx.RouteType == routeTypeFlood {
						info.UnscopedRelayCount24h++
					}
					if observed.After(cutoff1h) {
						info.RelayCount1h++
					}
				}
				if info.LastRelayed == "" || tx.FirstSeen > info.LastRelayed {
					info.LastRelayed = tx.FirstSeen
					if windowHours > 0 && observed.After(windowCutoff) {
						info.RelayActive = true
					} else if windowHours > 0 {
						info.RelayActive = false
					}
				}
			}
		}
		visit(list, false)
		visit(prefixList, true)
		info.TransportedScopes = sortedCappedScopes(scopeSet)
		out[key] = info
	}
	return out
}

// GetRepeaterUsefulnessScoreMap returns a cached pubkey → 0..1 score
// for every pubkey appearing in byPathHop. Bulk equivalent of
// GetRepeaterUsefulnessScore. See GetRepeaterRelayInfoMap for the
// motivation (#1257) and the no-inline-rebuild rationale (#1272).
func (s *PacketStore) GetRepeaterUsefulnessScoreMap() map[string]float64 {
	s.repeaterEnrichMu.Lock()
	cached := s.repeaterUsefulCache
	s.repeaterEnrichMu.Unlock()
	if cached != nil {
		return cached
	}

	result := s.computeRepeaterUsefulnessScoreMap()

	s.repeaterEnrichMu.Lock()
	if s.repeaterUsefulCache == nil {
		s.repeaterUsefulCache = result
		s.repeaterUsefulAt = time.Now()
	}
	cached = s.repeaterUsefulCache
	s.repeaterEnrichMu.Unlock()
	return cached
}

func (s *PacketStore) computeRepeaterUsefulnessScoreMap() map[string]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalNonAdvert := 0
	for pt, list := range s.byPayloadType {
		if pt == payloadTypeAdvert {
			continue
		}
		totalNonAdvert += len(list)
	}
	out := make(map[string]float64, len(s.byPathHop))
	if totalNonAdvert == 0 {
		return out
	}
	denom := float64(totalNonAdvert)
	var ids []int
	for key, list := range s.byPathHop {
		// Distinct transmissions, matching the denominator (#2108). Only
		// full-pubkey keys (the resolved hops, and the only keys a
		// repeater's score is read under) can hold a transmission more
		// than once; raw hop buckets get one entry per transmission from
		// addTxToPathHopIndex and are counted directly, which keeps this
		// pass near its previous cost (BenchmarkTrafficShareScoreMap_2108).
		var relayed int
		if len(key) == 64 {
			relayed, ids = countDistinctNonAdvert(list, ids)
		} else {
			relayed = countNonAdvert(list)
		}
		if relayed == 0 {
			continue
		}
		score := float64(relayed) / denom
		if score < 0 {
			score = 0
		} else if score > 1 {
			score = 1
		}
		out[key] = score
	}
	return out
}

// lookupRelayInfo is a small helper to make handleNodes' map lookup
// case-insensitive (byPathHop keys are lowercase; pubkeys arriving from
// the DB row may be either case).
func lookupRelayInfo(m map[string]RepeaterRelayInfo, pubkey string) (RepeaterRelayInfo, bool) {
	if v, ok := m[pubkey]; ok {
		return v, true
	}
	if lc := strings.ToLower(pubkey); lc != pubkey {
		if v, ok := m[lc]; ok {
			return v, true
		}
	}
	return RepeaterRelayInfo{}, false
}

// lookupUsefulnessScore mirrors lookupRelayInfo for the score map.
func lookupUsefulnessScore(m map[string]float64, pubkey string) float64 {
	if v, ok := m[pubkey]; ok {
		return v
	}
	if lc := strings.ToLower(pubkey); lc != pubkey {
		if v, ok := m[lc]; ok {
			return v
		}
	}
	return 0
}
