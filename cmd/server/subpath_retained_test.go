package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func TestSubpathAnalyticsRetainedAfterEviction(t *testing.T) {
	db := setupSubpathWindowDB(t)
	defer db.Close()
	current := time.Now().UTC().Truncate(time.Second)
	boundary := current.Add(-3 * time.Hour).Format(time.RFC3339)
	mustExec(t, db, `INSERT INTO observers (id, name, iata) VALUES ('outside','Other observer','SFO');
		INSERT INTO nodes (public_key, name, role, lat, lon, last_seen, first_seen) VALUES
		('aabbaa0000000000','Anchored','repeater',50,-1,?,?),
		('aacccc0000000000','Other prefix sibling','repeater',30,30,?,?),
		('eebbaa0000000000','Anchor','repeater',50,-1,?,?)`, boundary, boundary, boundary, boundary, boundary, boundary)
	mustExec(t, db, `UPDATE transmissions SET decoded_json = '{"pubKey":"eebbaa0000000000"}'
		WHERE hash = 'old_hash_window_002'`)
	mustExec(t, db, `UPDATE transmissions SET first_seen = ? WHERE hash = 'recent_hash_window_001'`,
		current.Add(-3*time.Hour).Format("2006-01-02T15:04:05.000Z"))
	mustExec(t, db, `UPDATE observations SET path_json = '["AA","bb","aa","bb"]'
		WHERE transmission_id = 1`)
	mustExec(t, db, `UPDATE observations SET path_json = '["aa","bb"]'
		WHERE transmission_id = 2`)
	mustExec(t, db, `INSERT INTO transmissions (id,raw_hex,hash,first_seen)
		VALUES (3,'AA','outside',?);
		INSERT INTO observations (transmission_id,observer_idx,snr,rssi,path_json,timestamp)
		VALUES (3,2,15,-85,'["99","98"]',?)`, current.Add(-2*time.Hour).Format(time.RFC3339), current.Add(-2*time.Hour).Unix())

	cfg := &Config{Port: 3000}
	srv := NewServer(db, cfg, NewHub())
	store := NewPacketStore(db, nil)
	if err := store.Load(); err != nil {
		t.Fatal(err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes never became ready")
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	get := func(path string) map[string]any {
		t.Helper()
		var body map[string]any
		getJSON(t, router, path, &body)
		return body
	}
	list := "/api/analytics/subpaths?minLen=2&maxLen=2"
	window := list + "&region=SJC&from=" + url.QueryEscape(boundary)
	bulk := "/api/analytics/subpaths-bulk?groups=2-2:20,3-4:20&region=SJC"
	detail := "/api/analytics/subpath-detail?hops=AA,bb"
	responses := map[string]map[string]any{}
	for _, path := range []string{list, window, bulk, detail} {
		responses[path] = get(path)
	}
	if got := responses[list]["totalPaths"]; got != float64(3) {
		t.Fatalf("full retained fixture totalPaths = %v", got)
	}
	if got := responses[window]["totalPaths"]; got != float64(1) {
		t.Fatalf("regional inclusive millisecond boundary totalPaths = %v", got)
	}
	for _, path := range []string{list, window} {
		matchList := responses[path]["subpaths"].([]any)
		if got := matchList[0].(map[string]any)["path"]; got != "Anchored → bb" {
			t.Fatalf("old off-window anchor failed to resolve sibling in %s: %v", path, got)
		}
	}
	if got := responses[detail]["totalMatches"]; got != float64(3) {
		t.Fatalf("detail occurrences = %v", got)
	}

	store.retentionHours = 0.5
	if removed := store.EvictStale(); removed != 3 {
		t.Fatalf("evicted %d transmissions, want 3", removed)
	}
	// Existing ordering among equal counts was unspecified. Compare entries
	// by path while preserving every row, representative, count, and field.
	normalise := func(body map[string]any) {
		if rows, ok := body["subpaths"].([]any); ok {
			byPath := map[string]any{}
			for _, raw := range rows {
				row := raw.(map[string]any)
				byPath[row["path"].(string)] = row
			}
			body["subpaths"] = byPath
		}
	}
	for path, resident := range responses {
		retained := get(path)
		normalise(resident)
		normalise(retained)
		if buckets, ok := resident["results"].([]any); ok {
			for i, bucket := range buckets {
				normalise(bucket.(map[string]any))
				normalise(retained["results"].([]any)[i].(map[string]any))
			}
		}
		if !reflect.DeepEqual(retained, resident) {
			t.Fatalf("%s\nretained: %#v\nresident: %#v", path, retained, resident)
		}
	}

	// An unavailable history cannot be reported as a successful partial map.
	db.Close()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, list+"&limit=17", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("failed retained read status = %d: %s", w.Code, w.Body.String())
	}
}

func TestScopedSubpathUsesFirstVisitAcrossCollidingLengths(t *testing.T) {
	groups := []subpathGroup{{2, 3, 10}, {2, 2, 10}, {3, 3, 10}}
	paths := map[int][]string{
		90: {"aa", "bb", "cc"},
		12: {"ab", "cc"},
		3:  {"ac", "cc"},
	}
	names := map[string]string{"aa": "A", "bb": "B", "cc": "C", "ab": "A → B", "ac": "A → B"}
	counter := newSubpathCounter(groups, false, func(hop string) string { return names[hop] })
	for _, id := range []int{90, 12, 3} {
		counter.add(id, paths[id])
	}
	want := []subpathAccum{
		{count: 3, raw: "aa,bb,cc"},
		{count: 2, raw: "ab,cc"},
		{count: 1, raw: "aa,bb,cc"},
	}
	for i, group := range groups {
		result := counter.names(counter.top(group), func(id int) []string { return paths[id] })
		if got := result["A → B → C"]; got == nil || *got != want[i] {
			t.Errorf("group %v selected %v, want first visit and count %+v", group, got, want[i])
		}
	}
}
