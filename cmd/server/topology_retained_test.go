package main

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

func TestRetainedTopologyMatchesResidentAfterEviction(t *testing.T) {
	server, router := setupTestServer(t)
	defer server.db.Close()
	routes := []string{"/api/analytics/topology", "/api/analytics/topology?region=SJC"}
	resident := make([]map[string]any, len(routes))
	for i, route := range routes {
		getJSON(t, router, route, &resident[i])
	}
	if resident[0]["medianHops"] == float64(0) {
		t.Fatal("the fixture must exercise hop statistics")
	}

	server.store.mu.Lock()
	server.store.maxMemoryMB = 1
	server.store.packets = nil
	server.store.mu.Unlock()

	// Maps used by the reducer give equal-rank rows no guaranteed order.
	var normalize func(any)
	normalize = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			for _, field := range value {
				normalize(field)
			}
		case []any:
			for _, entry := range value {
				normalize(entry)
			}
			sort.Slice(value, func(i, j int) bool {
				a, _ := json.Marshal(value[i])
				b, _ := json.Marshal(value[j])
				return string(a) < string(b)
			})
		}
	}
	for i, route := range routes {
		var retained map[string]any
		getJSON(t, router, route, &retained)
		normalize(resident[i])
		normalize(retained)
		for name, want := range resident[i] {
			if !reflect.DeepEqual(want, retained[name]) {
				t.Errorf("%s: %s = %v; want %v", route, name, retained[name], want)
			}
		}
	}
}
