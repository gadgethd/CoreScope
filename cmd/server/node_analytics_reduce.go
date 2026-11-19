package main

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type nodeObserverAccum struct {
	name                       string
	snrSum, rssiSum            float64
	snrCount, rssiCount, count int
	first, last                string
}

type nodePeerAccum struct {
	key, name   string
	count       int
	lastContact string
}

// One reduction for resident and retained history. Only response points and
// grouped counters survive a visit; no StoreTx or observation is retained.
type nodeAnalyticsAccum struct {
	pubkey                   string
	pm                       *prefixMap
	timeline, certain        map[string]int
	snrTrend                 []SnrTrendEntry
	types                    map[int]int
	observers                map[string]*nodeObserverAccum
	hops                     map[string]int
	peers                    map[string]*nodePeerAccum
	heat                     map[string]*HeatmapCell
	total, withPath, relayed int
}

func newNodeAnalyticsAccum(pubkey string, pm *prefixMap) *nodeAnalyticsAccum {
	return &nodeAnalyticsAccum{pubkey: pubkey, pm: pm,
		timeline: make(map[string]int), certain: make(map[string]int), snrTrend: make([]SnrTrendEntry, 0),
		types: make(map[int]int), observers: make(map[string]*nodeObserverAccum),
		hops: make(map[string]int), peers: make(map[string]*nodePeerAccum), heat: make(map[string]*HeatmapCell)}
}

func (a *nodeAnalyticsAccum) add(p *StoreTx) {
	a.total++
	display := p.displayObservation()
	if len(p.FirstSeen) >= 13 {
		bucket := p.FirstSeen[:13] + ":00:00Z"
		a.timeline[bucket]++
		if certainFor(p, a.pubkey, a.pm) {
			a.certain[bucket]++
		}
	}
	if display.SNR != nil {
		a.snrTrend = append(a.snrTrend, SnrTrendEntry{Timestamp: p.FirstSeen,
			SNR: floatPtrOrNil(display.SNR), RSSI: floatPtrOrNil(display.RSSI),
			ObserverID: strOrNil(display.ObserverID), ObserverName: strOrNil(display.ObserverName)})
	}
	if p.PayloadType != nil {
		a.types[*p.PayloadType]++
	}
	if display.ObserverID != "" {
		o := a.observers[display.ObserverID]
		if o == nil {
			o = &nodeObserverAccum{name: display.ObserverName, first: p.FirstSeen, last: p.FirstSeen}
			a.observers[display.ObserverID] = o
		}
		o.count++
		if display.SNR != nil {
			o.snrSum += *display.SNR
			o.snrCount++
		}
		if display.RSSI != nil {
			o.rssiSum += *display.RSSI
			o.rssiCount++
		}
		if p.FirstSeen < o.first {
			o.first = p.FirstSeen
		}
		if p.FirstSeen > o.last {
			o.last = p.FirstSeen
		}
	}
	hops := txGetParsedPath(p)
	if len(hops) > 0 {
		key := fmt.Sprintf("%d", len(hops))
		if len(hops) >= 4 {
			key = "4+"
		}
		a.hops[key]++
		a.withPath++
		if len(hops) > 1 {
			a.relayed++
		}
	} else {
		a.hops["0"]++
	}
	a.addPeers(p)
	if t, ok := parseStoreTime(p.FirstSeen); ok {
		dow, hour := int(t.UTC().Weekday()), t.UTC().Hour()
		key := fmt.Sprintf("%d:%d", dow, hour)
		if a.heat[key] == nil {
			a.heat[key] = &HeatmapCell{DayOfWeek: dow, Hour: hour}
		}
		a.heat[key].Count++
	}
}

func (a *nodeAnalyticsAccum) addPeers(p *StoreTx) {
	if p.DecodedJSON == "" {
		return
	}
	decoded := p.ParsedDecoded()
	if decoded == nil {
		return
	}
	add := func(key, name string) {
		if key == "" || key == a.pubkey {
			return
		}
		peer := a.peers[key]
		if peer == nil {
			if name == "" && len(key) >= 12 {
				name = key[:12]
			}
			peer = &nodePeerAccum{key: key, name: name, lastContact: p.FirstSeen}
			a.peers[key] = peer
		}
		peer.count++
		if p.FirstSeen > peer.lastContact {
			peer.lastContact = p.FirstSeen
		}
	}
	sender, _ := decoded["sender_key"].(string)
	senderName, _ := decoded["sender_name"].(string)
	if senderName == "" {
		senderName, _ = decoded["sender_short_name"].(string)
	}
	add(sender, senderName)
	recipient, _ := decoded["recipient_key"].(string)
	recipientName, _ := decoded["recipient_name"].(string)
	if recipientName == "" {
		recipientName, _ = decoded["recipient_short_name"].(string)
	}
	add(recipient, recipientName)
	key, _ := decoded["pubkey"].(string)
	name, _ := decoded["name"].(string)
	add(key, name)
}

func (a *nodeAnalyticsAccum) finish(result *NodeAnalyticsResponse, coveredFrom, now time.Time) {
	bucketKeys := make([]string, 0, len(a.timeline))
	for key := range a.timeline {
		bucketKeys = append(bucketKeys, key)
	}
	sort.Strings(bucketKeys)
	result.ActivityTimeline = make([]TimeBucket, 0, len(bucketKeys))
	certainKeys := make([]string, 0, len(a.certain))
	for _, key := range bucketKeys {
		bucket, certain := key, a.certain[key]
		result.ActivityTimeline = append(result.ActivityTimeline, TimeBucket{Bucket: &bucket, Count: a.timeline[key], Certain: &certain})
		if certain > 0 {
			certainKeys = append(certainKeys, key)
		}
	}
	result.SnrTrend = a.snrTrend
	result.PacketTypeBreakdown = make([]PayloadTypeCount, 0, len(a.types))
	for payload, count := range a.types {
		result.PacketTypeBreakdown = append(result.PacketTypeBreakdown, PayloadTypeCount{PayloadType: payload, Count: count})
	}
	result.ObserverCoverage = make([]NodeObserverStatsResp, 0, len(a.observers))
	for id, o := range a.observers {
		var snr, rssi any
		if o.snrCount > 0 {
			snr = o.snrSum / float64(o.snrCount)
		}
		if o.rssiCount > 0 {
			rssi = o.rssiSum / float64(o.rssiCount)
		}
		result.ObserverCoverage = append(result.ObserverCoverage, NodeObserverStatsResp{
			ObserverID: id, ObserverName: o.name, PacketCount: o.count, AvgSnr: snr, AvgRssi: rssi, FirstSeen: o.first, LastSeen: o.last})
	}
	sort.Slice(result.ObserverCoverage, func(i, j int) bool {
		return result.ObserverCoverage[i].PacketCount > result.ObserverCoverage[j].PacketCount
	})
	result.HopDistribution = make([]HopDistEntry, 0)
	for _, hops := range []string{"0", "1", "2", "3", "4+"} {
		if count, ok := a.hops[hops]; ok {
			result.HopDistribution = append(result.HopDistribution, HopDistEntry{Hops: hops, Count: count})
		}
	}
	result.PeerInteractions = make([]PeerInteraction, 0, len(a.peers))
	for _, p := range a.peers {
		result.PeerInteractions = append(result.PeerInteractions, PeerInteraction{
			PeerKey: p.key, PeerName: p.name, MessageCount: p.count, LastContact: p.lastContact})
	}
	sort.Slice(result.PeerInteractions, func(i, j int) bool {
		return result.PeerInteractions[i].MessageCount > result.PeerInteractions[j].MessageCount
	})
	if len(result.PeerInteractions) > 20 {
		result.PeerInteractions = result.PeerInteractions[:20]
	}
	result.UptimeHeatmap = make([]HeatmapCell, 0, len(a.heat))
	for _, cell := range a.heat {
		result.UptimeHeatmap = append(result.UptimeHeatmap, *cell)
	}
	coveredHours := now.Sub(coveredFrom).Hours()
	var availability, perDay *float64
	if coveredHours >= minCoveredHours {
		available := math.Min(round(float64(len(certainKeys))*100/coveredHours, 1), 100)
		daily := round(float64(a.total)*24/coveredHours, 1)
		availability, perDay = &available, &daily
	}
	// Preserve the original calculation over the API's trend points, without
	// retaining historical transmissions or changing floating-point order.
	var mean, deviation float64
	if len(a.snrTrend) > 0 {
		var sum float64
		for _, point := range a.snrTrend {
			if value, ok := point.SNR.(float64); ok {
				sum += value
			}
		}
		mean = sum / float64(len(a.snrTrend))
		if len(a.snrTrend) > 1 {
			var squares float64
			for _, point := range a.snrTrend {
				if value, ok := point.SNR.(float64); ok {
					squares += (value - mean) * (value - mean)
				}
			}
			deviation = math.Sqrt(squares / float64(len(a.snrTrend)))
		}
	}
	grade := "D"
	switch {
	case mean > 15 && deviation < 2:
		grade = "A"
	case mean > 15:
		grade = "A-"
	case mean > 12 && deviation < 3:
		grade = "B+"
	case mean > 8:
		grade = "B"
	case mean > 3:
		grade = "C"
	}
	var relayPct float64
	if a.withPath > 0 {
		relayPct = round(float64(a.relayed)*100/float64(a.withPath), 1)
	}
	silence, silenceStart := longestSilence(certainKeys, coveredFrom, now)
	result.ComputedStats = ComputedNodeStats{AvailabilityPct: availability, AvgPacketsPerDay: perDay,
		LongestSilenceMs: silence, LongestSilenceStart: silenceStart,
		SignalGrade: grade, SnrMean: round(mean, 1), SnrStdDev: round(deviation, 1), RelayPct: relayPct,
		TotalPackets: a.total, UniqueObservers: len(a.observers), UniquePeers: len(result.PeerInteractions)}
}
