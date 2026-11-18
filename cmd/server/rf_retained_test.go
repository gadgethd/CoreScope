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
