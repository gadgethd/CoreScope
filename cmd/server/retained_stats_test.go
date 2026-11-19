package main

import (
	"testing"
	"time"
)

func TestRetainedStoreStatsCountLogicalObservations(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) *DB
	}{
		{"v2", setupTestDBV2},
		{"v3", func(t *testing.T) *DB { return setupTestDB(t) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.open(t)
			defer db.Close()
			_, err := db.conn.Exec(`
				INSERT INTO transmissions (id, raw_hex, hash, first_seen) VALUES
					(1, '01', '01', '2026-11-18T12:00:00Z'),
					(2, '02', '02', '2026-11-18T12:01:00Z'),
					(3, '03', '03', '2026-11-18T12:02:00Z');
				INSERT INTO observers (rowid, id) VALUES (1, 'OBS'), (2, 'obs'), (3, '');
			`)
			if err != nil {
				t.Fatal(err)
			}
			observer, upper, lower, empty := "observer_id", any("OBS"), any("obs"), any("")
			if db.isV3 {
				observer, upper, lower, empty = "observer_idx", 1, 2, 3
			}
			query := "INSERT INTO observations (transmission_id, " + observer + ", path_json, timestamp) VALUES (?,?,?,?)"
			now := time.Now().Unix()
			for _, row := range [][]any{
				{1, upper, "[]"}, {1, upper, "[]"}, {1, upper, `["AA"]`}, {1, lower, "[]"},
				{1, nil, nil}, {1, empty, ""}, {2, upper, "[]"},
			} {
				if _, err := db.conn.Exec(query, append(row, now)...); err != nil {
					t.Fatal(err)
				}
			}
			store := &PacketStore{db: db, maxMemoryMB: 1}
			stats, err := store.GetStoreStats()
			if err != nil {
				t.Fatal(err)
			}
			if stats.TotalPackets != 3 || stats.TotalTransmissions != 3 || stats.TotalObservations != 5 {
				t.Fatalf("unexpected retained counts: %+v", stats)
			}
			if stats.PacketsLastHour != 7 || stats.PacketsLast24h != 7 {
				t.Fatalf("recent statistics must count physical observations: %+v", stats)
			}
		})
	}
}
