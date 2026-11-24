package main

// Regression coverage for the bounded cold-load / graph-refresh allocation
// work (lean-runtime Stage 1). The shared parsed-path pool, the lowercase
// key cache, the map-free short-path dedup and the ordered last-id dedup in
// scanAndMergeChunk must all be behavior-preserving: these tests pin parity
// with the nil-cache / nil-pool paths, the timestamp-format coverage behind
// the ParsedTime switch, and the pre-size guard.

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// --- addTxToPathHopIndex: nil vs shared lowercase cache ---------------------

func TestAddTxToPathHopIndexLowerCacheParity(t *testing.T) {
	paths := []string{
		`["A4","F1","A4"]`, // repeated hop, uppercase
		`["aa","bb","cc"]`,
		`["01","02","03","04","05","06","07","08","09","01","02"]`, // >8 hops: map arm, with repeats
		`[]`,
		`garbage`,
	}
	nilIdx := make(map[string][]*StoreTx)
	cacheIdx := make(map[string][]*StoreTx)
	cache := make(map[string]string)
	var txs []*StoreTx
	for i, p := range paths {
		tx := &StoreTx{ID: i + 1, selectedObservation: &StoreObs{PathJSON: p}}
		txs = append(txs, tx)
		addTxToPathHopIndex(nilIdx, tx, nil)
		addTxToPathHopIndex(cacheIdx, tx, cache)
	}
	if !reflect.DeepEqual(nilIdx, cacheIdx) {
		t.Fatalf("cached and uncached indexes diverge:\n nil=%v\n cache=%v", nilIdx, cacheIdx)
	}
	if got := cacheIdx["a4"]; len(got) != 1 || got[0] != txs[0] {
		t.Fatalf("repeated hop must index the tx once under its lowercase key, got %v", got)
	}
	if got := cacheIdx["01"]; len(got) != 1 || got[0] != txs[2] {
		t.Fatalf("long-path dedup failed for key 01: %v", got)
	}
	if _, ok := cacheIdx["A4"]; ok {
		t.Fatal("index keys must be lowercased")
	}
}

// The warm bulk path (cached parsed path, warm lowercase cache, index slices
// with headroom) must not allocate per transmission: that is the property
// that keeps a 500k-row rebuild from churning the GC.
func TestAddTxToPathHopIndexWarmCacheAllocs(t *testing.T) {
	tx := &StoreTx{selectedObservation: &StoreObs{PathJSON: `["A4","F1"]`}}
	txGetParsedPath(tx) // warm the per-tx parsed-path cache
	cache := map[string]string{"A4": "a4", "F1": "f1"}
	idx := map[string][]*StoreTx{
		"a4": make([]*StoreTx, 0, 1024),
		"f1": make([]*StoreTx, 0, 1024),
	}
	allocs := testing.AllocsPerRun(100, func() {
		addTxToPathHopIndex(idx, tx, cache)
	})
	if allocs != 0 {
		t.Fatalf("warm path-hop indexing must not allocate, got %v allocs/run", allocs)
	}
}

// --- resolvePathForObsColdLoad: nil vs pooled/cache parity ------------------

func TestResolvePathForObsColdLoadPoolParity(t *testing.T) {
	pm := buildPrefixMap(directHeardNodes)
	cases := []string{
		`["BB11"]`,           // unique prefix, uppercase input
		`["A4"]`,             // ambiguous: under-attribute + skipped++
		`["ZZ"]`,             // no match: nil, no skip
		`["bb11","A4","ZZ"]`, // mixed
		`[]`,
		`garbage`,
	}
	deref := func(rp []*string) []string {
		out := make([]string, len(rp))
		for i, p := range rp {
			if p != nil {
				out[i] = *p
			}
		}
		return out
	}
	pool := make(loadParsedPaths)
	cache := make(map[string]string)
	for _, raw := range cases {
		var skipNil, skipPool int
		want := resolvePathForObsColdLoad(raw, "obs1", nil, pm, &skipNil, nil, nil)
		got := resolvePathForObsColdLoad(raw, "obs1", nil, pm, &skipPool, pool, cache)
		if !reflect.DeepEqual(deref(want), deref(got)) || skipNil != skipPool {
			t.Fatalf("pooled resolve diverges for %q: want %v (skip %d), got %v (skip %d)",
				raw, deref(want), skipNil, deref(got), skipPool)
		}
		// A second pass must reuse the pool and stay identical.
		got2 := resolvePathForObsColdLoad(raw, "obs1", nil, pm, &skipPool, pool, cache)
		if !reflect.DeepEqual(deref(got), deref(got2)) {
			t.Fatalf("pool reuse changed the result for %q", raw)
		}
	}
	if got := resolvePathForObsColdLoad(`["BB11"]`, "obs1", nil, pm, nil, nil, nil); got == nil || *got[0] != dhUnique {
		t.Fatalf("unique-prefix resolution broken: %v", got)
	}
}

// --- ParsedTime covers parseTimestamp's format chain -------------------------

func TestParsedTimeCoversParseTimestampFormats(t *testing.T) {
	// Every string parseTimestamp accepts must parse to the same instant
	// through ParsedTime, and edge times serialize through UTC RFC3339, so
	// the rendered payload must be identical too. Note time.Parse's
	// fractional-second leniency: RFC3339 accepts a fractional field even
	// though the layout omits it, so nanosecond and offset-fractional
	// shapes are parity cases, not superset cases.
	parity := []string{
		"2026-11-18T21:55:22Z",
		"2026-11-18T21:55:22.123Z", // strftime('%f') shape from the chunk SQL
		"2026-11-18T21:55:22+02:00",
		"2026-11-18T21:55:22.123456789Z",
		"2026-11-18T21:55:22.123+02:00",
		"2026-11-18 21:55:22",
		"1970-01-01T00:00:00Z", // UnixNano==0 reparse branch
		"1600-01-01T00:00:00Z", // pre-1678 reparse branch
		"",
		"not-a-time",
	}
	for _, s := range parity {
		obs := &StoreObs{Timestamp: s}
		got, ok := obs.ParsedTime()
		want := parseTimestamp(s)
		if want.IsZero() {
			if ok || !got.IsZero() {
				t.Fatalf("%q: parseTimestamp fails but ParsedTime returned (%v, %v)", s, got, ok)
			}
		} else {
			if !ok || !got.Equal(want) {
				t.Fatalf("%q: ParsedTime (%v, %v) != parseTimestamp %v", s, got, ok, want)
			}
			if got.UTC().Format(time.RFC3339) != want.UTC().Format(time.RFC3339) {
				t.Fatalf("%q: UTC RFC3339 rendering diverges", s)
			}
		}
		// Second call exercises the cached (or reparse) branch.
		got2, ok2 := obs.ParsedTime()
		if ok2 != ok || !got2.Equal(got) {
			t.Fatalf("%q: cached ParsedTime diverges: (%v, %v) vs (%v, %v)", s, got2, ok2, got, ok)
		}
	}
}

// --- scanAndMergeChunk: ordered last-id dedup --------------------------------

func TestLoadChunkedOrderedDedupMultiObs(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "multiobs.db")
	createTestDBWithObs(t, dbPath, 12) // deterministic 1-5 observations per tx
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.conn.Close()
	store := NewPacketStore(db, &PacketStoreConfig{})

	var chunks []int
	store.OnChunkLoaded(func(rowsThisChunk, totalRows int) {
		chunks = append(chunks, rowsThisChunk)
	})
	if err := store.LoadChunked(5); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	wantObs := 0
	for i := 1; i <= 12; i++ {
		wantObs += (i % 5) + 1
	}
	if len(store.packets) != 12 {
		t.Fatalf("distinct tx count wrong: got %d, want 12", len(store.packets))
	}
	if store.totalObs != wantObs || len(store.byObsID) != wantObs {
		t.Fatalf("observation count wrong: totalObs=%d byObsID=%d, want %d", store.totalObs, len(store.byObsID), wantObs)
	}
	if want := []int{5, 5, 2}; !reflect.DeepEqual(chunks, want) {
		t.Fatalf("chunk callbacks must count distinct transmissions: got %v, want %v", chunks, want)
	}
}

func TestLoadChunkedIncludesIDZero(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "zero.db")
	createTestDBAt(t, dbPath, 3)
	conn, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := conn.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json)
		VALUES (0, 'aabb', 'h0000', '2026-01-01T00:00:00Z', 0, 4, 1, '{"pubKey":"pk0000"}')`); err != nil {
		t.Fatalf("insert id=0 tx: %v", err)
	}
	if _, err := conn.Exec(`INSERT INTO observations (id, transmission_id, observer_id, observer_name, direction, snr, rssi, score, path_json, timestamp)
		VALUES (100, 0, 'obs1', 'Obs1', 'RX', -10, -80, 5, '["aa","bb"]', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("insert id=0 obs: %v", err)
	}
	conn.Close()

	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.conn.Close()
	store := NewPacketStore(db, &PacketStoreConfig{})
	var chunks []int
	store.OnChunkLoaded(func(rowsThisChunk, totalRows int) {
		chunks = append(chunks, rowsThisChunk)
	})
	if err := store.LoadChunked(2); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	if len(store.packets) != 4 || store.byTxID[0] == nil {
		t.Fatalf("id=0 must load: packets=%d byTxID[0]=%v", len(store.packets), store.byTxID[0])
	}
	if want := []int{2, 2}; !reflect.DeepEqual(chunks, want) {
		t.Fatalf("chunk sizes with id=0 row: got %v, want %v", chunks, want)
	}
}

// --- pre-size guard -----------------------------------------------------------

func TestLoadChunkedPresizesEmptyStoreOnly(t *testing.T) {
	store := openChunkedTestStore(t, 12)
	defer store.db.conn.Close()
	if err := store.LoadChunked(5); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	// Empty-store load pre-sizes to the filtered DB count instead of the
	// constructor's 65536 hint (observable via slice capacity).
	if got := cap(store.packets); got != 12 {
		t.Fatalf("empty-store load must pre-size packets to the row count: cap=%d, want 12", got)
	}
	// A second pass into the now-live store keeps the existing capacity
	// and must not duplicate rows.
	if err := store.LoadChunked(5); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := cap(store.packets); got != 12 {
		t.Fatalf("reload must keep existing capacity: cap=%d, want 12", got)
	}
	if len(store.packets) != 12 || len(store.byObsID) != 12 {
		t.Fatalf("reload duplicated rows: packets=%d byObsID=%d", len(store.packets), len(store.byObsID))
	}
}

// --- graph builder: pooled paths + ParsedTime integration ---------------------

func TestBuildFromStoreEdgeTimesMatchObservationInstants(t *testing.T) {
	nodes := []nodeInfo{
		{Role: "repeater", PublicKey: "aaaa1111"},
		{Role: "repeater", PublicKey: "bbaa2222"},
	}
	ts := "2026-11-18T21:55:22.123Z" // strftime %f shape used by the chunk loader
	tx := ngMakeTx(1, PayloadADVERT, `{"pubKey":"aaaa1111"}`, []*StoreObs{
		ngMakeObs("bbaa2222", `["AAAA"]`, ts, ngFloatPtr(-9)),
	})
	g := BuildFromStore(ngTestStore(nodes, []*StoreTx{tx}))
	want, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatal(err)
	}
	edges := g.AllEdges()
	// Originator↔path[0] is a self-edge (guarded); only the
	// observer↔last-hop edge survives.
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d: %+v", len(edges), edges)
	}
	e := edges[0]
	if !e.FirstSeen.Equal(want) || !e.LastSeen.Equal(want) {
		t.Fatalf("edge times must equal the observation instant: first=%v last=%v want=%v",
			e.FirstSeen, e.LastSeen, want)
	}
}

// A non-UTC offset must survive the ParsedTime reparse branch end-to-end
// through the graph builder: the edge instant equals the observation's
// absolute instant, not a mis-read local wall clock.
func TestBuildFromStoreEdgeTimesNonUTCOffset(t *testing.T) {
	nodes := []nodeInfo{
		{Role: "repeater", PublicKey: "aaaa1111"},
		{Role: "repeater", PublicKey: "bbaa2222"},
	}
	ts := "2026-11-18T21:55:22.123+02:00" // fractional + non-UTC → tsReparse branch
	tx := ngMakeTx(1, PayloadADVERT, `{"pubKey":"aaaa1111"}`, []*StoreObs{
		ngMakeObs("bbaa2222", `["AAAA"]`, ts, ngFloatPtr(-9)),
	})
	g := BuildFromStore(ngTestStore(nodes, []*StoreTx{tx}))
	want, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatal(err)
	}
	edges := g.AllEdges()
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d: %+v", len(edges), edges)
	}
	if !edges[0].FirstSeen.Equal(want) || !edges[0].LastSeen.Equal(want) {
		t.Fatalf("non-UTC edge times must equal the observation instant: first=%v last=%v want=%v",
			edges[0].FirstSeen, edges[0].LastSeen, want)
	}
}

// Two distinct transmission ids sharing one hash must collapse to a single
// stored tx (byHash dedup) while the chunk callback still counts both ids.
// This pins the ordered last-id dedup's shared-hash equivalence: the second
// id reuses the first tx and is not separately indexed, and the count is
// unchanged from the old seenTxIDs-map behavior.
func TestLoadChunkedSharedHashDedup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sharedhash.db")
	createTestDBAt(t, dbPath, 3) // tx 1..3, hashes h0001..h0003
	conn, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// tx id=4 carries the SAME hash as tx id=1 (h0001), ADVERT payload.
	if _, err := conn.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json)
		VALUES (4, 'aabb', 'h0001', '2026-01-01T00:04:00Z', 0, 4, 1, '{"pubKey":"pk0001"}')`); err != nil {
		t.Fatalf("insert shared-hash tx: %v", err)
	}
	if _, err := conn.Exec(`INSERT INTO observations (id, transmission_id, observer_id, observer_name, direction, snr, rssi, score, path_json, timestamp)
		VALUES (4, 4, 'obs1', 'Obs1', 'RX', -10, -80, 5, '["aa","bb"]', '2026-01-01T00:04:00Z')`); err != nil {
		t.Fatalf("insert shared-hash obs: %v", err)
	}
	if _, err := conn.Exec(`CREATE TABLE advert_route_evidence(id INTEGER PRIMARY KEY, tx_id INTEGER, bit INTEGER);
		INSERT INTO advert_route_evidence(tx_id, bit) VALUES (1, 1), (4, 2)`); err != nil {
		t.Fatalf("insert distinct route evidence: %v", err)
	}
	conn.Close()

	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.conn.Close()
	store := NewPacketStore(db, &PacketStoreConfig{})
	var txCount, maskReads int
	db.advertEvidenceReadHook = func() { maskReads++ }
	store.OnChunkLoaded(func(rowsThisChunk, totalRows int) { txCount += rowsThisChunk })
	if err := store.LoadChunked(3); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	if len(store.packets) != 3 {
		t.Fatalf("shared hash must collapse to one stored tx per hash: packets=%d, want 3", len(store.packets))
	}
	if got := store.byHash["h0001"]; got == nil || got.ID != 1 {
		t.Fatalf("first-seen tx must win the shared hash: byHash[h0001]=%v", got)
	}
	if maskReads != 1 || store.byHash["h0001"].AdvertRouteEvidence != 1 {
		t.Fatalf("expected one canonical mask read and no duplicate evidence, reads=%d mask=%d",
			maskReads, store.byHash["h0001"].AdvertRouteEvidence)
	}
	if txCount != 4 {
		t.Fatalf("chunk callback must count all distinct ids incl. the shared-hash id: txCount=%d, want 4", txCount)
	}
}

// The pre-size clamp arm: with a memory budget that caps maxPackets below
// the row count, the empty-store indexes pre-size to maxPackets (not the
// full row count), matching the walk's memory cap.
func TestLoadChunkedPresizeClampedByMemoryBudget(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "clamped.db")
	const numTx = 2000
	createTestDBAt(t, dbPath, numTx)
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.conn.Close()
	store := NewPacketStore(db, &PacketStoreConfig{MaxMemoryMB: 1})
	if err := store.LoadChunked(5); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	// Mirror the production maxPackets derivation to get the expected cap.
	avgBytes := int64(1000)
	if sample := estimateStoreTxBytesTypical(10); sample > avgBytes {
		avgBytes = sample
	}
	wantMax := (int64(1) * 1048576) / avgBytes
	if wantMax < 1000 {
		wantMax = 1000
	}
	if wantMax >= numTx {
		t.Fatalf("test premise: memory cap must clamp below the row count (maxPackets=%d, numTx=%d)", wantMax, numTx)
	}
	if got := cap(store.packets); int64(got) != wantMax {
		t.Fatalf("clamped pre-size must cap packets at maxPackets: cap=%d, want %d", got, wantMax)
	}
	if len(store.packets) > numTx || int64(len(store.packets)) < wantMax {
		t.Fatalf("clamped walk must load ~maxPackets rows: len=%d, maxPackets=%d", len(store.packets), wantMax)
	}
}

// The shared lowercase cache is bounded (AGENTS.md map size limit): inserts
// stop at maxLowerCacheEntries, but reads of cached keys still hit and keys
// beyond the cap still return the correct lowercase value (recomputed).
func TestCachedToLowerBounded(t *testing.T) {
	cache := make(map[string]string)
	for i := 0; i < maxLowerCacheEntries+50; i++ {
		key := "KEY" + strconv.Itoa(i)
		if got := cachedToLower(cache, key); got != "key"+strconv.Itoa(i) {
			t.Fatalf("cachedToLower(%q)=%q, want lowercase", key, got)
		}
	}
	if len(cache) != maxLowerCacheEntries {
		t.Fatalf("cache must be bounded at %d entries, got %d", maxLowerCacheEntries, len(cache))
	}
	// An early cached key still hits; a beyond-cap key recomputes correctly.
	if got := cachedToLower(cache, "KEY0"); got != "key0" {
		t.Fatalf("cached early key must still hit: %q", got)
	}
	if got := cachedToLower(cache, "KEY"+strconv.Itoa(maxLowerCacheEntries+49)); got != "key"+strconv.Itoa(maxLowerCacheEntries+49) {
		t.Fatalf("beyond-cap key must still lowercase correctly: %q", got)
	}
}

// --- aggregateRF: signal accumulators pre-sized to avoid growth churn -------

// The signal scratch owns two float arrays and one observation flag byte.
func TestAggregateRFSignalSlicesPreSized(t *testing.T) {
	const n = 100000
	// Pre-build the transmissions OUTSIDE the measured region so the scan feeds
	// them without allocating; only aggregateRF's own scratch is counted.
	// Timestamps must stay Z-suffixed UTC: time.Parse then allocates nothing,
	// while a numeric UTC offset would allocate a FixedZone per transmission
	// and blow the upper bound.
	snr, rssi := 10.0, -90.0
	txs := make([]*StoreTx, n)
	for i := range txs {
		obs := &StoreObs{SNR: &snr, RSSI: &rssi, Timestamp: "2026-11-18T12:00:00.000Z"}
		txs[i] = &StoreTx{Hash: strconv.Itoa(i % 1000), FirstSeen: obs.Timestamp, Observations: []*StoreObs{obs}}
	}
	scan := func(addTx func(*StoreTx, *rfPacketCounts), _ func(*StoreObs, *StoreTx)) error {
		for _, tx := range txs {
			addTx(tx, nil)
		}
		return nil
	}
	if _, err := aggregateRF(n, n, false, nil, TimeWindow{}, scan); err != nil {
		t.Fatalf("warm aggregateRF: %v", err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	result, err := aggregateRF(n, n, false, nil, TimeWindow{}, scan)
	if err != nil {
		t.Fatalf("aggregateRF: %v", err)
	}
	runtime.ReadMemStats(&after)
	// Output assertions prove the scan actually FILLED the accumulators: the
	// allocation bounds alone cannot (pre-sized backing arrays are allocated
	// even if observe appends nothing). Every transmission carries one
	// signal-bearing observation, so both counters must equal n.
	if got := result["totalPackets"]; got != n {
		t.Errorf("totalPackets = %v, want %d: signal accumulators were not filled", got, n)
	}
	if got := result["totalAllPackets"]; got != n {
		t.Errorf("totalAllPackets = %v, want %d: observation counter mismatch", got, n)
	}
	allocated := after.TotalAlloc - before.TotalAlloc
	if limit := uint64(n) * 35; allocated > limit {
		t.Errorf("aggregateRF allocated %d bytes, want at most %d without a second signal copy", allocated, limit)
	}

}

// --- analyticsWorkGate: release reclaims before freeing the slot ------------

// TestAnalyticsWorkGateReleaseReclaimsScratch pins the lean-runtime Stage 1
// ordering change: release() must run a forced GC (reclaiming the finished
// compute's scratch while the slot is still held) and then free the slot for
// the next compute. The stop-abort path uses releaseSlot(), which frees the
// slot without a GC because no compute ran.
func TestAnalyticsWorkGateReleaseReclaimsScratch(t *testing.T) {
	var gate analyticsWorkGate
	stop := make(chan struct{})
	if !gate.acquire(stop) {
		t.Fatal("acquire on a fresh gate must succeed")
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	gate.release()
	runtime.ReadMemStats(&after)
	if after.NumGC <= before.NumGC {
		t.Fatalf("release must run runtime.GC() before freeing the slot (NumGC %d -> %d)", before.NumGC, after.NumGC)
	}
	// The slot must be free again: a second acquire succeeds immediately.
	if !gate.acquire(stop) {
		t.Fatal("release left the work slot held")
	}
	gate.releaseSlot()
	if len(gate.slot) != 0 {
		t.Fatal("releaseSlot must free the slot")
	}
	// A stopped acquire must return false and never leave the slot held,
	// whichever way the first select resolves.
	stopped := make(chan struct{})
	close(stopped)
	if gate.acquire(stopped) {
		t.Fatal("acquire with a closed stop channel must return false")
	}
	if len(gate.slot) != 0 {
		t.Fatal("stop-abort must not leave the work slot held")
	}
}
