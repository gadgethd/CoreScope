package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestUKMeshShellSeparatesAssetsAndPreservesStandalone(t *testing.T) {
	root := filepath.Join("..", "..", "public")
	handler := spaHandler(root, http.FileServer(http.Dir(root)))
	shell := func(path string) string {
		t.Helper()
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
		if res.Code != http.StatusOK || !strings.Contains(res.Header().Get("Cache-Control"), "no-store") {
			t.Fatalf("unexpected shell response: %d %v", res.Code, res.Header())
		}
		return res.Body.String()
	}
	app := shell(ukmeshAppShellPath)
	for _, required := range []string{
		`src="/corescope/config.js" data-ukmesh-app-config`,
		`src="/corescope/static/deployment.js?v=`,
		`href="/corescope/static/style.css?v=`,
		`src="/corescope/static/vendor/leaflet-1.9.4/leaflet.js"`,
		`href="#/live"`,
	} {
		if !strings.Contains(app, required) {
			t.Fatalf("app shell missing %s", required)
		}
	}
	standalone := shell("/")
	if strings.Contains(standalone, "/corescope/config.js") ||
		!strings.Contains(standalone, `src="deployment.js?v=`) ||
		!strings.Contains(standalone, `href="style.css?v=`) {
		t.Fatal("standalone shell paths changed")
	}
}
