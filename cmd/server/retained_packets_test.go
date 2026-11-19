package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRetainedPacketFiltersAndPageParity(t *testing.T) {
	srv, router := setupTestServer(t)
	seedRadioHistory(t, srv)
	_, err := srv.db.conn.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen)
		VALUES (100, 'AA', '', '2026-01-01T00:00:00Z'), (101, 'AA', '1000000000000001', '')`)
	if err != nil {
		t.Fatal(err)
	}

	const firstHash = "abc123def4567890"
	cases := []struct {
		query string
		total int
		hash  string
	}{
		{"?limit=1&offset=1&order=asc", 3, "1234567890abcdef"},
		{"?limit=1&offset=1&order=desc", 3, "1234567890abcdef"},
		{"?groupByHash=true&limit=1", 3, firstHash},
		{"?groupByHash=true&region=sfo&observer=obs1", 1, firstHash},
		{"?node=TestCompanion", 1, firstHash}, // Relay participation, not only origin.
		{"?nodes=TestCompanion,TestRoom&limit=1&order=asc&offset=1", 2, "def456abc1230099"},
		{"?channel=%23test", 1, "1234567890abcdef"},
		{"?radio=869.618,62.5,8,8&observer=obs1", 0, ""},
		{"?radio=869.618,62.5,8,8&observer=obs2&expand=observations", 1, firstHash},
	}
	for _, test := range cases {
		t.Run(test.query, func(t *testing.T) {
			srv.store.maxMemoryMB = 0
			var resident, retained packetsBody
			getJSON(t, router, "/api/packets"+test.query, &resident)
			srv.store.maxMemoryMB = 1
			getJSON(t, router, "/api/packets"+test.query, &retained)
			if retained.Total != test.total || retained.Total != resident.Total {
				t.Fatalf("retained total %d; resident %d; want %d", retained.Total, resident.Total, test.total)
			}
			if test.hash != "" && (len(retained.Packets) == 0 || retained.Packets[0]["hash"] != test.hash) {
				t.Fatalf("wrong page: %+v, want %s first", retained.Packets, test.hash)
			}
			a, _ := json.Marshal(resident.Packets)
			b, _ := json.Marshal(retained.Packets)
			if string(a) != string(b) {
				t.Fatalf("page shape or data diverged\nresident: %s\nretained: %s", a, b)
			}
		})
	}
}

func TestRetainedUnfilteredCountSkipsEmptyAndNull(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE transmissions (id INTEGER PRIMARY KEY, hash TEXT, first_seen TEXT);
		INSERT INTO transmissions (hash, first_seen) VALUES ('valid', 'stamp'), ('also-valid', 'stamp'),
			('', ''), (NULL, ''), ('', NULL), (NULL, NULL), ('hash', NULL), ('hash', ''), ('', 'stamp'), (NULL, 'stamp')`)
	if err != nil {
		t.Fatal(err)
	}
	var total int
	if err = db.QueryRow(retainedUnfilteredPacketCountSQL).Scan(&total); err != nil || total != 2 {
		t.Fatalf("exact total = %d, error = %v; want 2 valid transmissions", total, err)
	}
}

func TestRetainedPacketReadIncludesRowsOutsideActiveCache(t *testing.T) {
	srv, router := setupTestServer(t)
	srv.store.maxMemoryMB = 1
	srv.db.hasObsRawHex = true
	const hash = "deadbeef00112233"
	_, err := srv.db.conn.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen)
		VALUES (100, 'AABB', 'deadbeef00112233', '2000-01-01T00:00:00Z');
		INSERT INTO observations (id, transmission_id, observer_idx, path_json, timestamp, resolved_path, raw_hex)
		VALUES (200, 100, 1, '["ab"]', 1800000000, '["abc"]', 'CCDD'),
		       (201, 100, 1, '["ab"]', 1799999999, '["abc"]', 'EEFF')`)
	if err != nil {
		t.Fatal(err)
	}
	if got := srv.store.GetPacketByHash(hash); got != nil {
		t.Fatal("retained test requires the packet to be outside the active cache")
	}
	var listing packetsBody
	getJSON(t, router, "/api/packets?expand=observations&limit=1", &listing)
	if listing.Total != 4 || len(listing.Packets) != 1 || listing.Packets[0]["hash"] != hash {
		t.Fatalf("list missed late ingest: %+v", listing)
	}
	for _, id := range []string{hash, "100", "200", "201"} {
		var detail struct {
			Packet       struct{ Hash string }
			Observations []struct {
				RawHex string `json:"raw_hex"`
			}
		}
		getJSON(t, router, "/api/packets/"+id, &detail)
		if detail.Packet.Hash != hash || len(detail.Observations) != 2 ||
			detail.Observations[0].RawHex != "CCDD" || detail.Observations[1].RawHex != "EEFF" {
			t.Fatalf("detail %s did not retain its observation's wire bytes: %+v", id, detail)
		}
	}
	var timestamps []string
	getJSON(t, router, "/api/packets/timestamps?since=1999-01-01T00:00:00Z", &timestamps)
	if len(timestamps) != 4 || timestamps[0] != "2000-01-01T00:00:00Z" {
		t.Fatalf("VCR timestamps should include historical first_seen: %v", timestamps)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/packets/observations",
		strings.NewReader(`{"hashes":["deadbeef00112233","0000000000000000"]}`))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	var payload struct{ Results map[string][]ObservationResp }
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil || recorder.Code != 200 ||
		len(payload.Results[hash]) != 2 || payload.Results[hash][0].ID != 200 || payload.Results[hash][1].ID != 201 ||
		payload.Results["0000000000000000"] == nil || len(payload.Results["0000000000000000"]) != 0 {
		t.Fatalf("batch history failed: %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestRetainedPacketsCountsAcrossBatches(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	_, err := db.conn.Exec(fmt.Sprintf(`WITH RECURSIVE seq(x) AS
		(VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x < %d)
		INSERT INTO transmissions (id, raw_hex, hash, first_seen, payload_type, decoded_json)
		SELECT x, 'AA', printf('%%016x',x), '2026-01-01T00:00:00Z', 5,
			CASE WHEN x %% 2 = 0 THEN '{"channel":"public"}' ELSE '{"channel":"other"}' END FROM seq`,
		retainedPacketBatch*3))
	if err != nil {
		t.Fatal(err)
	}
	s := NewPacketStore(db, &PacketStoreConfig{MaxMemoryMB: 1})
	q := PacketQuery{Channel: "public", Order: "ASC", Limit: 4, Offset: retainedPacketBatch/2 - 1}
	rows, err := s.queryRetainedPackets(context.Background(), q, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Total != retainedPacketBatch*3/2 || len(rows.Packets) != 4 ||
		rows.Packets[0]["id"] != retainedPacketBatch || rows.Packets[3]["id"] != retainedPacketBatch+6 {
		t.Fatalf("filter or paging truncated at a chunk boundary: %+v", rows)
	}
}

func TestRetainedPacketReadFailsOnStoreError(t *testing.T) {
	srv, router := setupTestServer(t)
	srv.store.maxMemoryMB = 1
	if err := srv.db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"/api/packets", "/api/packets?groupByHash=true", "/api/packets/abc123def4567890",
		"/api/packets/timestamps?since=2000-01-01T00:00:00Z"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, route, nil))
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("%s: got %d %s; SQLite failure must not return partial data", route, recorder.Code, recorder.Body.String())
		}
	}
}
