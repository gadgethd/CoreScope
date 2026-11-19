package main

import (
	"slices"
	"strings"
	"testing"
)

func hopContextFixture() (*StoreTx, *prefixMap) {
	pm := buildPrefixMap([]nodeInfo{
		{PublicKey: "aa11000000000000000000000000000000000000000000000000000000000001", Role: "repeater"},
		{PublicKey: "bb22000000000000000000000000000000000000000000000000000000000002", Role: "repeater"},
		{PublicKey: "bb33000000000000000000000000000000000000000000000000000000000003", Role: "repeater"},
	})
	tx := &StoreTx{
		selectedObservation: &StoreObs{ObserverID: "DD55000000000000000000000000000000000000000000000000000000000005", PathJSON: `["AA","BB","aa","AA11"]`},
		DecodedJSON:         `{"pubKey":"cc44000000000000000000000000000000000000000000000000000000000004"}`,
	}
	tx.ParsedDecoded()
	txGetParsedPath(tx)
	return tx, pm
}

// buildHopContextPubkeys runs per transmission in every topology and
// distance pass and per matched packet in subpath detail. Its de-dup map,
// sized for 16 keys, was allocated on every call (150MB of churn per
// analytics recompute on a busy week) to de-duplicate a handful of keys,
// and live hops and observer IDs are upper-case, so each was lowered into
// a new string just to look it up. The fixture is cased like live data.
func TestBuildHopContextPubkeysAllocatesOnlyItsResult(t *testing.T) {
	tx, pm := hopContextFixture()
	// The result slice, and the lowered observer ID it holds.
	if n := testing.AllocsPerRun(200, func() { _ = buildHopContextPubkeys(tx, pm) }); n > 2 {
		t.Fatalf("buildHopContextPubkeys allocates %.0f times per call, want at most 2", n)
	}
}

// Same keys, same order: sender, observer, then unambiguous hop anchors,
// lower-cased and de-duplicated.
func TestBuildHopContextPubkeysOrderAndDedup(t *testing.T) {
	tx, pm := hopContextFixture()
	got := buildHopContextPubkeys(tx, pm)
	want := []string{
		"cc44000000000000000000000000000000000000000000000000000000000004",
		"dd55000000000000000000000000000000000000000000000000000000000005",
		"aa11000000000000000000000000000000000000000000000000000000000001",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestHopContextAvoidsRetainingUnrelatedDecodedFields(t *testing.T) {
	_, pm := hopContextFixture()
	tx := &StoreTx{DecodedJSON: `{"ackHash":"aabb","sequence":24}`}
	if got := buildHopContextPubkeys(tx, pm); len(got) != 0 || tx.parsedDecoded != nil {
		t.Fatalf("ACK created sender context or retained a decoded map: %v", got)
	}
	if got := tx.ParsedDecoded()["ackHash"]; got != "aabb" {
		t.Fatalf("decoded packet lost its ACK field: %v", got)
	}
	escaped := &StoreTx{DecodedJSON: `{"pub\u004bey":"AaBb"}`}
	if got := buildHopContextPubkeys(escaped, pm); len(got) != 1 || got[0] != "aabb" {
		t.Fatalf("escaped sender key changed hop context: %v", got)
	}
}

func TestBulkDisplayPathsShareReadOnlyPaths(t *testing.T) {
	const raw = `["AA","bb"]`
	cache := make(loadParsedPaths)
	a := &StoreTx{Observations: []*StoreObs{{PathJSON: raw}}}
	b := &StoreTx{Observations: []*StoreObs{{PathJSON: raw}}}
	pickBestObservationWithPaths(a, cache)
	pickBestObservationWithPaths(b, cache)
	if &a.parsedPath[0] != &b.parsedPath[0] {
		t.Fatal("a repeated bulk path should share its decoded storage")
	}
	txToMap(a)["_parsedPath"].([]string)[0] = "CC"
	if !slices.Equal(txGetParsedPath(a), []string{"AA", "bb"}) || !slices.Equal(txGetParsedPath(b), []string{"AA", "bb"}) {
		t.Fatal("an exported path mutated the shared packet cache")
	}
	b.Observations = []*StoreObs{{PathJSON: `["DD"]`}}
	pickBestObservation(b)
	if !slices.Equal(txGetParsedPath(b), []string{"DD"}) || !slices.Equal(txGetParsedPath(a), []string{"AA", "bb"}) {
		t.Fatal("changing a packet's path altered the shared value")
	}
}

func TestAggregateHopContextReusesScratch(t *testing.T) {
	tx, pm := hopContextFixture()
	tx.displayObservation().ObserverID = strings.ToLower(tx.displayObservation().ObserverID)
	want := buildHopContextPubkeys(tx, pm)
	scratch := make([]string, 0, 8)
	if n := testing.AllocsPerRun(200, func() {
		scratch = appendHopContextPubkeys(scratch[:0], tx, pm)
		if !slices.Equal(scratch, want) {
			t.Fatalf("got %v, want %v", scratch, want)
		}
	}); n != 0 {
		t.Fatalf("aggregate context allocated %.0f times per packet", n)
	}
}

func TestDistanceIndexKeepsAckHopWithoutDecodingUnrelatedFields(t *testing.T) {
	tx := &StoreTx{selectedObservation: &StoreObs{PathJSON: `["a1","b2"]`}, DecodedJSON: `{"ackHash":"aabb","sequence":24}`}
	a := &nodeInfo{PublicKey: "a1", HasGPS: true, Lat: 51.4, Lon: -0.1}
	b := &nodeInfo{PublicKey: "b2", HasGPS: true, Lat: 51.5, Lon: -0.1}
	nodes := map[string]*nodeInfo{"a1": a, "b2": b}
	hops, path := computeDistancesForTx(tx, nodes, nil, func(key string) *nodeInfo { return nodes[key] })
	if len(hops) != 1 || path == nil || path.HopCount != 1 || hops[0].FromPk != "a1" || hops[0].ToPk != "b2" {
		t.Fatalf("expected the packet's resolved hop and distance, got %v, %v", hops, path)
	}
	if tx.parsedDecoded != nil {
		t.Fatal("distance index decoded unrelated ACK payload fields")
	}
}
