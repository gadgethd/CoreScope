package main

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestNodePathsRetainedMatchesResidentAfterEviction(t *testing.T) {
	sc := setupCollisionScenario(t, true)
	defer sc.db.Close()

	addPath := func(id int, hash, path string, resolved any, stamp int64) {
		t.Helper()
		mustExec(t, sc.db, `INSERT INTO transmissions (id, raw_hex, hash, first_seen)
			VALUES (?, 'AABB', ?, ?)`, id, hash, time.Unix(stamp, 0).UTC().Format(time.RFC3339))
		mustExec(t, sc.db, `INSERT INTO observations (transmission_id, path_json, timestamp, resolved_path)
			VALUES (?, ?, ?, ?)`, id, path, stamp, resolved)
	}
	const distinctCanonical = 55
	for id := 1; id <= distinctCanonical; id++ {
		resolved := fmt.Sprintf(`["%016x",%q]`, id, sc.nodeAPK)
		addPath(id, fmt.Sprintf("path-%d", id), `["ab","c0"]`, resolved, sc.recentEpoch+int64(id))
	}
	mustExec(t, sc.db, `INSERT INTO observations (transmission_id, path_json, timestamp, resolved_path)
		VALUES (55, '["c0"]', ?, ?)`, sc.recentEpoch-1, fmt.Sprintf(`[%q]`, sc.nodeBPK))
	addPath(56, "repeated-latest", `["ab","c0"]`,
		fmt.Sprintf(`["%016x",%q]`, distinctCanonical, sc.nodeAPK), sc.recentEpoch+56)
	addPath(57, "ambiguous", `["c0"]`, nil, sc.recentEpoch+57)
	addPath(58, "unique-raw", fmt.Sprintf(`[%q]`, sc.nodeAPK[:4]), nil, sc.recentEpoch+58)
	sc.reloadStore(t)

	resident := sc.query(t, sc.nodeAPK)
	if resident.TotalPaths != distinctCanonical+1 || resident.TotalTransmissions != distinctCanonical+2 || len(resident.Paths) != 50 {
		t.Fatalf("resident path totals: %d paths, %d transmissions, %d returned",
			resident.TotalPaths, resident.TotalTransmissions, len(resident.Paths))
	}
	if resident.Paths[0].SampleHash != "unique-raw" || resident.Paths[1].SampleHash != "repeated-latest" ||
		resident.Paths[1].Count != 2 || resident.Paths[1].Hops[1].Pubkey != sc.nodeAPK {
		t.Fatalf("resident ranked paths = %+v", resident.Paths[:2])
	}
	if sibling := sc.query(t, sc.nodeBPK); sibling.TotalTransmissions != 0 {
		t.Fatalf("ambiguous or non-canonical sibling path appeared: %+v", sibling)
	}

	sc.srv.store.retentionHours = 0.5
	if evicted := sc.srv.store.EvictStale(); evicted != 58 {
		t.Fatalf("evicted %d transmissions, want 58", evicted)
	}
	retained := sc.query(t, sc.nodeAPK)
	if !reflect.DeepEqual(retained, resident) {
		t.Fatalf("retained response differs after eviction\nretained: %+v\nresident: %+v", retained, resident)
	}
	if sibling := sc.query(t, sc.nodeBPK); sibling.TotalTransmissions != 0 {
		t.Fatalf("retained path matches a collision sibling: %+v", sibling)
	}
}
