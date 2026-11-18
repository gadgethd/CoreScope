package main

import (
	"reflect"
	"testing"
	"time"
)

func TestPollObservationDeliveryAcrossEvictionAndReadFailure(t *testing.T) {
	srv, _ := setupTestServer(t)
	db, store := srv.db, srv.store
	defer db.Close()
	seedRadioHistory(t, srv)
	if _, err := db.conn.Exec(`INSERT INTO observer_radios (observer_id, radio_id, radio, first_seen, last_seen)
		VALUES ('obs1', 'narrow', '868.1,125,9,5', '2000-01-01T00:00:00Z', '2099-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	refreshRadioHistory(db.conn, false)

	lastTx, lastObs := db.GetMaxTransmissionID(), db.GetMaxObservationID()
	store.retentionHours = 0.5
	if removed := store.EvictStale(); removed == 0 {
		t.Fatal("fixture must evict its retained parent")
	}
	past := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	if _, err := db.conn.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen, route_type, payload_type, decoded_json)
		VALUES (4, 'AABB', 'abcd000000000004', ?, 1, 4, '{"name":"recent parent"}');
		INSERT INTO observations (id, transmission_id, observer_idx, path_json, timestamp)
		VALUES (5, 4, 1, '["aa"]', 1780272000)`, past); err != nil {
		t.Fatal(err)
	}
	first, delivered := store.ingestNewTransmissions(lastTx, 100)
	if len(first) != 1 || first[0]["hash"] != "abcd000000000004" {
		t.Fatalf("initial broadcasts = %v", first)
	}
	if removed := store.EvictStale(); removed != 1 {
		t.Fatalf("newly delivered parent eviction removed %d", removed)
	}

	// These observations commit after the transmission read took its snapshot.
	if _, err := db.conn.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen, route_type, payload_type)
		VALUES (5, 'AABB', 'abcd000000000005', ?, 1, 4);
		INSERT INTO observations (id, transmission_id, observer_idx, snr, path_json, timestamp, resolved_path) VALUES
			(6, 1, 1, 10, '["cc"]', 1780272001, '["1122334455667788"]'),
			(7, 4, 2, 'invalid', '["bb"]', 1780272002, NULL),
			(8, 5, 1, 10, '[]', 1780272003, NULL);
		INSERT INTO observation_radios (observation_id, radio_id) VALUES (6, 'narrow')`, past); err != nil {
		t.Fatal(err)
	}
	if messages, cursor, err := store.ingestLateObservations(lastObs, 500, delivered); err == nil || cursor != lastObs || len(messages) != 0 {
		t.Fatalf("failed read messages = %v, cursor = %d, error = %v", messages, cursor, err)
	}
	if _, err := db.conn.Exec(`UPDATE observations SET snr = 12 WHERE id = 7`); err != nil {
		t.Fatal(err)
	}

	messages, cursor, err := store.ingestLateObservations(lastObs, 500, delivered)
	if err != nil || cursor != 8 || len(messages) != 2 {
		t.Fatalf("continued read messages = %v, cursor = %d, error = %v", messages, cursor, err)
	}
	wantPath := "1122334455667788"
	if got := messages[0]; got["hash"] != "abc123def4567890" || got["path_json"] != `["cc"]` ||
		got["radio"] != "868.1,125,9,5" || got["observation_count"] != 3 ||
		!reflect.DeepEqual(got["resolved_path"], []*string{&wantPath}) {
		t.Fatalf("evicted parent broadcast = %v", got)
	}
	if got := messages[1]; got["hash"] != "abcd000000000004" || got["path_json"] != `["bb"]` {
		t.Fatalf("observation committed between queries broadcast = %v", got)
	}
	if store.GetPacketByHash("abc123def4567890") != nil || store.GetPacketByHash("abcd000000000004") != nil {
		t.Fatal("late deliveries must not restore evicted transmissions")
	}

	next, _ := store.ingestNewTransmissions(delivered.throughTx, 100)
	if len(next) != 1 || next[0]["hash"] != "abcd000000000005" {
		t.Fatalf("newer parent must be delivered by the transmission poll = %v", next)
	}
}
