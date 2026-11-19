package main

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

func TestRetainedRFMatchesResidentAggregatesAfterEviction(t *testing.T) {
	server, router := setupTestServer(t)
	defer server.db.Close()
	routes := []string{"/api/analytics/rf", "/api/analytics/rf?region=SJC"}
	resident := make([]map[string]any, len(routes))
	for i, route := range routes {
		getJSON(t, router, route, &resident[i])
	}
	if resident[0]["totalTransmissions"] != float64(3) || resident[0]["totalAllPackets"] != float64(4) {
		t.Fatalf("expected exact resident fixture counts: %v %v", resident[0]["totalTransmissions"], resident[0]["totalAllPackets"])
	}

	server.store.mu.Lock()
	server.store.maxMemoryMB = 1
	server.store.packets = nil
	server.store.byObserver = nil
	server.store.byTxID = nil
	server.store.mu.Unlock()

	normalize := func(result map[string]any) {
		for _, field := range []string{"payloadTypes", "snrByType", "scatterData"} {
			rows, _ := result[field].([]any)
			sort.Slice(rows, func(i, j int) bool {
				a, _ := json.Marshal(rows[i])
				b, _ := json.Marshal(rows[j])
				return string(a) < string(b)
			})
		}
	}
	for i, route := range routes {
		var retained map[string]any
		getJSON(t, router, route, &retained)
		normalize(resident[i])
		normalize(retained)
		if !reflect.DeepEqual(resident[i], retained) {
			for name, want := range resident[i] {
				if !reflect.DeepEqual(want, retained[name]) {
					t.Errorf("%s: retained and resident %s differ", route, name)
				}
			}
		}
	}
}

func TestRFAggregationDeduplicatesHoursAcrossReceptionOrders(t *testing.T) {
	first := "2026-11-18T12:00:01Z"
	second := "2026-11-18T13:00:01Z"
	typ := 3
	snr := []float64{-10, -6, 2, 6}
	tx := &StoreTx{
		Hash: "packet-a", FirstSeen: first, RawHex: "010203", PayloadType: &typ,
		Observations: []*StoreObs{
			{Timestamp: second, SNR: &snr[0]},
			{Timestamp: first, SNR: &snr[1]},
			{Timestamp: second, SNR: &snr[2]},
		},
	}
	other := &StoreTx{Hash: "packet-b", FirstSeen: first, RawHex: "0102", PayloadType: &typ,
		Observations: []*StoreObs{{Timestamp: first, SNR: &snr[3]}}}
	for _, regional := range []bool{false, true} {
		got, err := aggregateRF(4, 2, regional, nil, TimeWindow{}, func(add func(*StoreTx, *rfPacketCounts), observe func(*StoreObs, *StoreTx)) error {
			if regional {
				observe(tx.Observations[0], tx)
				observe(other.Observations[0], other)
				observe(tx.Observations[1], tx)
				observe(tx.Observations[2], tx)
			} else {
				older := &StoreTx{Hash: tx.Hash, FirstSeen: tx.FirstSeen, PayloadType: tx.PayloadType,
					Observations: tx.Observations[:1]}
				newer := &StoreTx{Hash: tx.Hash, FirstSeen: tx.FirstSeen, RawHex: tx.RawHex, PayloadType: tx.PayloadType,
					Observations: tx.Observations[1:]}
				// A second resident copy can supply a size missing
				// from the first without counting the hour or hash twice.
				duplicate := &rfPacketCounts{}
				add(older, duplicate)
				add(other, nil)
				add(newer, duplicate)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			TotalTransmissions int
			TotalAllPackets    int
			MinPacketSize      int
			MaxPacketSize      int
			PacketsPerHour     []struct {
				Hour  string
				Count int
			}
			SnrByType []struct {
				Name          string
				Count         int
				Avg, Min, Max float64
			}
		}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if result.TotalTransmissions != 2 || result.TotalAllPackets != 4 ||
			result.MinPacketSize != 2 || result.MaxPacketSize != 3 ||
			len(result.PacketsPerHour) != 2 || result.PacketsPerHour[0].Hour != first[:13] ||
			result.PacketsPerHour[0].Count != 2 || result.PacketsPerHour[1].Hour != second[:13] ||
			result.PacketsPerHour[1].Count != 1 {
			t.Fatalf("regional=%v: unexpected hourly or transmission counts: %+v", regional, result)
		}
		if len(result.SnrByType) != 1 {
			t.Fatalf("regional=%v: unexpected signal groups: %+v", regional, result.SnrByType)
		}
		signal := result.SnrByType[0]
		if signal.Name != "ACK" || signal.Count != 4 || signal.Avg != -2 || signal.Min != -10 || signal.Max != 6 {
			t.Fatalf("regional=%v: unexpected signal summary: %+v", regional, signal)
		}
	}
}
