package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestObserverAnalyticsRetainedParityAfterEviction(t *testing.T) {
	srv, router := setupTestServer(t)
	t.Cleanup(func() { srv.db.Close() })
	const count = retainedPacketBatch + 17
	now := time.Now().UTC().Truncate(time.Second)
	write, err := srv.db.conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback()
	for i := 0; i < count; i++ {
		// Reception order deliberately opposes FirstSeen. An old transmission
		// heard again recently belongs in this observer's requested range.
		first := now.Add(-40*24*time.Hour - time.Duration(i)*time.Second)
		received := now.Add(-3*time.Hour + time.Duration(i)*time.Second)
		if _, err := write.Exec(`INSERT INTO transmissions (id, hash, raw_hex, first_seen, payload_type, decoded_json)
			VALUES (?, ?, 'AA', ?, 4, '{"pubKey":"aabbccdd11223344"}')`,
			1000+i, fmt.Sprintf("observer-history-%d", i), first.Format(time.RFC3339)); err != nil {
			t.Fatal(err)
		}
		if _, err := write.Exec(`INSERT INTO observations (transmission_id, observer_idx, direction, snr, rssi, path_json, resolved_path, timestamp)
			VALUES (?, 1, 'rx', ?, -90, '["aa","ee"]', '["aabbccdd11223344","eeff00112233aabb"]', ?)`,
			1000+i, float64(i%17)-5.5, received.Unix()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := write.Exec(`INSERT INTO observations (transmission_id, observer_idx, snr, timestamp, path_json)
		VALUES (1000, 2, 99, ?, '[]')`, now.Unix()); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := srv.store.Load(); err != nil {
		t.Fatal(err)
	}
	if !srv.store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes not ready")
	}
	var resident, retained ObserverAnalyticsResponse
	getJSON(t, router, "/api/observers/obs1/analytics?days=7", &resident)
	total := 0
	for _, bucket := range resident.Timeline {
		total += bucket.Count
	}
	if total != count+3 || resident.PacketTypes["4"] != count+2 || resident.PacketTypes["5"] != 1 {
		t.Fatalf("unexpected fixture counts: total=%d types=%v", total, resident.PacketTypes)
	}
	srv.store.mu.Lock()
	srv.store.maxMemoryMB = 1
	srv.store.byObserver = make(map[string][]*StoreObs)
	srv.store.packets = nil
	srv.store.byTxID = make(map[int]*StoreTx)
	srv.store.mu.Unlock()
	getJSON(t, router, "/api/observers/obs1/analytics?days=7", &retained)
	a, _ := json.Marshal(resident)
	b, _ := json.Marshal(retained)
	if string(a) != string(b) {
		var before, after map[string]json.RawMessage
		json.Unmarshal(a, &before)
		json.Unmarshal(b, &after)
		for field, value := range before {
			if string(value) != string(after[field]) {
				t.Errorf("retained observer analytics differ in %s", field)
			}
		}
	}
	if len(retained.RecentPackets) != 20 {
		t.Fatalf("recent packets=%d, want 20", len(retained.RecentPackets))
	}
	for i, packet := range retained.RecentPackets {
		if packet["observer_id"] != "obs1" || packet["resolved_path"] == nil {
			t.Fatalf("recent packet lost observer scope or path: %v", packet)
		}
		if i > 0 && retained.RecentPackets[i-1]["timestamp"].(string) < packet["timestamp"].(string) {
			t.Fatal("recent packets are not ordered by reception time")
		}
	}
}

func TestObserverAnalyticsRetainedReadErrors(t *testing.T) {
	srv, router := setupTestServer(t)
	defer srv.db.Close()
	srv.store.maxMemoryMB = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/observers/obs1/analytics", nil).WithContext(ctx))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("cancelled observer read returned %d", w.Code)
	}
	if err := srv.db.Close(); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/observers/obs1/analytics", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("closed database returned %d", w.Code)
	}
}
