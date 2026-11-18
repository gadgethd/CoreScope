package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestNodeAnalyticsRetainedParityAfterCacheEviction(t *testing.T) {
	srv, router := setupTestServer(t)
	t.Cleanup(func() { srv.db.Close() })
	const pk = "aabbccdd11223344"
	const count = retainedPacketBatch + 17
	start := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	write, err := srv.db.conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback()
	for i := 0; i < count; i++ {
		stamp := start.Add(time.Duration(i) * time.Minute)
		decoded := fmt.Sprintf(`{"pubKey":%q,"sender_key":"peer-%d","sender_name":"Peer %d"}`, pk, i%3, i%3)
		if _, err := write.Exec(`INSERT INTO transmissions (id, hash, raw_hex, first_seen, payload_type, decoded_json)
			VALUES (?, ?, 'AA', ?, 4, ?)`, 1000+i, fmt.Sprintf("history-%d", i), stamp.Format(time.RFC3339), decoded); err != nil {
			t.Fatal(err)
		}
		if _, err := write.Exec(`INSERT INTO observations (transmission_id, observer_idx, snr, rssi, path_json, resolved_path, timestamp)
			VALUES (?, ?, ?, ?, '["aa"]', ?, ?)`, 1000+i, 1+i%2, float64(i%17)-5, -90-i%7,
			`["`+pk+`"]`, stamp.Unix()); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	srv.store = NewPacketStore(srv.db, nil)
	if err := srv.store.Load(); err != nil {
		t.Fatal(err)
	}
	if !srv.store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes not ready")
	}
	resident, err := srv.store.GetNodeAnalytics(pk, 7)
	if err != nil || resident == nil || resident.ComputedStats.TotalPackets != count+2 {
		t.Fatalf("resident analytics: %+v, %v", resident, err)
	}
	if resident.ComputedStats.UniqueObservers != 2 || resident.ComputedStats.UniquePeers != 3 ||
		len(resident.SnrTrend) != count+2 || resident.ComputedStats.RelayPct != 0.2 {
		t.Fatalf("unexpected fixture summaries: %+v, trend=%d", resident.ComputedStats, len(resident.SnrTrend))
	}
	srv.store.mu.Lock()
	srv.store.maxMemoryMB = 1
	srv.store.packets = nil
	srv.store.byNode = make(map[string][]*StoreTx)
	srv.store.byHash = make(map[string]*StoreTx)
	srv.store.byTxID = make(map[int]*StoreTx)
	srv.store.mu.Unlock()
	var retained NodeAnalyticsResponse
	getJSON(t, router, "/api/nodes/"+pk+"/analytics?days=7", &retained)
	// Request-clock fields differ between the two actual calls. Unordered
	// map reductions and equal-count ties have no stable order in the API.
	normalize := func(v *NodeAnalyticsResponse) {
		v.TimeRange.From, v.TimeRange.To = "", ""
		v.ComputedStats.LongestSilenceMs = 0
		sort.Slice(v.PacketTypeBreakdown, func(i, j int) bool {
			return v.PacketTypeBreakdown[i].PayloadType < v.PacketTypeBreakdown[j].PayloadType
		})
		sort.Slice(v.ObserverCoverage, func(i, j int) bool {
			return strVal(v.ObserverCoverage[i].ObserverID) < strVal(v.ObserverCoverage[j].ObserverID)
		})
		sort.Slice(v.PeerInteractions, func(i, j int) bool { return v.PeerInteractions[i].PeerKey < v.PeerInteractions[j].PeerKey })
		sort.Slice(v.UptimeHeatmap, func(i, j int) bool {
			a, b := v.UptimeHeatmap[i], v.UptimeHeatmap[j]
			return a.DayOfWeek < b.DayOfWeek || (a.DayOfWeek == b.DayOfWeek && a.Hour < b.Hour)
		})
	}
	normalize(resident)
	normalize(&retained)
	// Round-trip both: nullable interface fields decode to float64 over HTTP.
	a, _ := json.Marshal(resident)
	b, _ := json.Marshal(retained)
	if string(a) != string(b) {
		var oldFields, newFields map[string]json.RawMessage
		json.Unmarshal(a, &oldFields)
		json.Unmarshal(b, &newFields)
		for field, old := range oldFields {
			if string(old) != string(newFields[field]) {
				t.Errorf("retained analytics differ after eviction in %s", field)
			}
		}
	}
}

func TestNodeAnalyticsReadFailureAndRequestCancellation(t *testing.T) {
	srv, router := setupTestServer(t)
	defer srv.db.Close()
	srv.store.maxMemoryMB = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/api/nodes/aabbccdd11223344/analytics", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("cancelled read returned %d: %s", w.Code, w.Body.String())
	}
	if err := srv.db.Close(); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/nodes/aabbccdd11223344/analytics", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("database error returned %d instead of failure: %s", w.Code, w.Body.String())
	}
}

func TestRetainedClockReportsShareCalibrationAfterEviction(t *testing.T) {
	srv, router := setupTestServer(t)
	defer srv.db.Close()
	var residentFleet, retainedFleet []*NodeClockSkew
	var residentObservers, retainedObservers []ObserverCalibration
	getJSON(t, router, "/api/nodes/clock-skew", &residentFleet)
	getJSON(t, router, "/api/observers/clock-skew", &residentObservers)
	if len(residentFleet) == 0 || len(residentObservers) != 2 {
		t.Fatalf("fixture must include valid multi-observer calibration: fleet=%d observers=%d", len(residentFleet), len(residentObservers))
	}

	srv.store.mu.Lock()
	srv.store.maxMemoryMB = 1
	srv.store.packets = nil
	srv.store.byNode = nil
	srv.store.byPayloadType = nil
	srv.store.mu.Unlock()
	getJSON(t, router, "/api/nodes/clock-skew", &retainedFleet)
	getJSON(t, router, "/api/observers/clock-skew", &retainedObservers)

	sort.Slice(residentFleet, func(i, j int) bool { return residentFleet[i].Pubkey < residentFleet[j].Pubkey })
	sort.Slice(retainedFleet, func(i, j int) bool { return retainedFleet[i].Pubkey < retainedFleet[j].Pubkey })
	sort.Slice(residentObservers, func(i, j int) bool { return residentObservers[i].ObserverID < residentObservers[j].ObserverID })
	sort.Slice(retainedObservers, func(i, j int) bool { return retainedObservers[i].ObserverID < retainedObservers[j].ObserverID })
	if !reflect.DeepEqual(residentFleet, retainedFleet) || !reflect.DeepEqual(residentObservers, retainedObservers) {
		t.Fatalf("retained clock reports lost history or changed calibration: fleet=%d/%d observers=%v/%v",
			len(residentFleet), len(retainedFleet), residentObservers, retainedObservers)
	}
}
