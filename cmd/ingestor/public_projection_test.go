package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestF01CopiedDerivedProjectionRevokesAndReplaysAfterFailure(t *testing.T) {
	resetScopeMatchCounters(t)
	store, err := OpenStore(filepath.Join(t.TempDir(), "projection.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := waitForUpstreamIdentityReady(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	policy := &corePublicPolicy{generation: 1}
	unavailable := false
	projection, err := newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) {
		if unavailable {
			return nil, errors.New("authority unavailable")
		}
		return policy, nil
	}, applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	// Match main: the root Store owns its projection before steady Batch.
	store.publicProjection = projection
	const frame = "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	payload, _ := json.Marshal(pgEnvelope{Raw: frame, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	privateNode := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	records := []publicProjectionRecord{
		{ObservationID: "11111111-1111-4111-8111-111111111111", Topic: "meshcore/ZZZ/observer/packets", Payload: payload, Network: "northeast", RXNodeID: privateNode, Tag: "pg", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}},
		{ObservationID: "22222222-2222-4222-8222-222222222222", Topic: "meshcore/ZZZ/observer/packets", Payload: payload, Network: "ukmesh", Tag: "redis", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}},
	}
	ctx := context.Background()
	if err := projection.Batch(ctx, records); err != nil {
		t.Fatal(err)
	}
	assertCount := func(table string, want int) {
		t.Helper()
		var n int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("%s=%d want=%d", table, n, want)
		}
	}
	assertCount("observations", 2)
	policy = &corePublicPolicy{generation: 2, prefixes: []corePrivatePrefix{{NodeID: privateNode, Network: "teesside", Prefix: "AA", Size: 1}}}
	if _, err := projection.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	assertCount("observations", 1)
	assertCount("public_projection_journal", 1)
	var count int
	store.db.QueryRow("SELECT packet_count FROM observers WHERE id='observer'").Scan(&count)
	if count != 1 {
		t.Fatalf("revoked derived observer count=%d", count)
	}
	unavailable = true
	if _, err := projection.Refresh(ctx); err == nil {
		t.Fatal("missing authority accepted")
	}
	unavailable = false
	// A failed derived write leaves the projection unreadable. Retry rebuilds
	// from the durable journal, so partial side effects cannot accumulate.
	if _, err := store.db.Exec(`CREATE TRIGGER projection_fail BEFORE INSERT ON observations BEGIN SELECT RAISE(FAIL,'fixture failure'); END`); err != nil {
		t.Fatal(err)
	}
	third := records[1]
	third.ObservationID = "33333333-3333-4333-8333-333333333333"
	if err := projection.Batch(ctx, []publicProjectionRecord{third}); err == nil {
		t.Fatal("write failure accepted")
	}
	var ready int
	store.db.QueryRow("SELECT ready FROM public_projection_state").Scan(&ready)
	if ready != 0 {
		t.Fatal("failed write published a public epoch")
	}
	store.db.Exec("DROP TRIGGER projection_fail")
	if _, err := projection.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	assertCount("observations", 2)
	store.db.QueryRow("SELECT packet_count FROM observers WHERE id='observer'").Scan(&count)
	if count != 2 {
		t.Fatalf("retry double-counted derived state=%d", count)
	}
}

func TestF01PublicProjectionRefusesUntrackedExistingMeasurements(t *testing.T) {
	resetScopeMatchCounters(t)
	store, err := OpenStore(filepath.Join(t.TempDir(), "mixed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.db.Exec(`INSERT INTO transmissions(raw_hex,hash,first_seen)VALUES('00','existing','2026-10-11T00:00:00Z')`)
	if _, err := newCorePublicProjection(store, nil, nil, 30); err == nil {
		t.Fatal("existing untracked data could be silently rebuilt")
	}
	var n int
	store.db.QueryRow("SELECT COUNT(*) FROM transmissions").Scan(&n)
	if n != 1 {
		t.Fatal("untracked data was mutated")
	}
}

func TestF01PublicProjectionRefusesUntrackedNodeOnlyData(t *testing.T) {
	resetScopeMatchCounters(t)
	store, err := OpenStore(filepath.Join(t.TempDir(), "nodes.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.db.Exec(`INSERT INTO nodes(public_key,name) VALUES('untracked','existing')`); err != nil {
		t.Fatal(err)
	}
	if _, err := newCorePublicProjection(store, nil, nil, 30); err == nil {
		t.Fatal("existing untracked node data could be silently rebuilt")
	}
}

func TestF01DuplicateSourcePreservesParticipantProvenance(t *testing.T) {
	payload, _ := json.Marshal(pgEnvelope{Raw: "00", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	previous := publicProjectionRecord{Payload: payload, Topic: "meshcore/ZZZ/observer/packets", Network: "ukmesh", RXNodeID: "observer", SrcNodeID: "source", PathHashes: []string{"ABCD"}, PathHashSizeBytes: 2}
	incoming := previous
	incoming.SrcNodeID, incoming.PathHashes, incoming.PathHashSizeBytes = "", nil, 0
	enriched, err := enrichPublicProjectionRecord(incoming, previous)
	if err != nil {
		t.Fatal(err)
	}
	if enriched.SrcNodeID != "source" || len(enriched.PathHashes) != 1 || enriched.PathHashes[0] != "ABCD" || enriched.PathHashSizeBytes != 2 {
		t.Fatal("duplicate source discarded revocation provenance")
	}
}

// Steady PG/RAM delivery applies through the root Store, unlike replay pages.
// Match production's projection assignment and the tracked completion/cursor
// boundary so ownership metadata cannot be mistaken for an active transaction.
func TestProjectionRootOwnerCommitsAndHoldsFailedSourceCursor(t *testing.T) {
	resetScopeMatchCounters(t)
	store := newTestStore(t)
	policy := &corePublicPolicy{generation: 1, fingerprint: "initial"}
	projection, err := newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) {
		return policy, nil
	}, applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	store.publicProjection = projection // main assigns this before source startup
	buffer := NewIngestBuffer(4)
	buffer.Start()
	buffer.Ready()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := buffer.Drain(ctx); err != nil {
			t.Error(err)
		}
	})
	cursorPath := filepath.Join(t.TempDir(), "source.json")
	if err := savePgSourceState(cursorPath, pgSourceState{LastSeq: 7}); err != nil {
		t.Fatal(err)
	}
	apply := func(records []publicProjectionRecord, nextSeq int64) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		completed, accepted := buffer.SubmitTracked(func() error { return projection.Batch(ctx, records) })
		if !accepted {
			return errors.New("root projection fixture admission failed")
		}
		return finishSourceBatch([]<-chan error{completed}, false, func() error {
			return savePgSourceState(cursorPath, pgSourceState{LastSeq: nextSeq})
		})
	}
	assertState := func(observations, transmissions, journal, ready int) {
		t.Helper()
		for _, check := range []struct {
			query string
			want  int
		}{
			{`SELECT COUNT(*) FROM observations`, observations},
			{`SELECT COUNT(*) FROM transmissions`, transmissions},
			{`SELECT COUNT(*) FROM public_projection_journal`, journal},
			{`SELECT ready FROM public_projection_state WHERE singleton=1`, ready},
			{`PRAGMA synchronous`, 2},
		} {
			var got int
			if err := store.db.QueryRow(check.query).Scan(&got); err != nil || got != check.want {
				t.Fatalf("%s = %d, want %d, err=%v", check.query, got, check.want, err)
			}
		}
		var journalMode string
		if err := store.db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil || journalMode != "wal" {
			t.Fatalf("journal mode=%s, err=%v", journalMode, err)
		}
	}
	assertCursor := func(want int64) {
		t.Helper()
		cursor, ok := loadPgSourceState(cursorPath)
		if !ok || cursor.LastSeq != want {
			t.Fatalf("committed source cursor=%+v, want sequence %d", cursor, want)
		}
	}
	const raw = "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	payload, err := json.Marshal(pgEnvelope{Raw: raw, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	privateNode := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	records := []publicProjectionRecord{
		{ObservationID: "11111111-1111-4111-8111-111111111111", Topic: "meshcore/ZZZ/observer/packets", Payload: payload, Network: "northeast", RXNodeID: privateNode, Tag: "pg", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}},
		{ObservationID: "22222222-2222-4222-8222-222222222222", Topic: "meshcore/ZZZ/observer/packets", Payload: payload, Network: "ukmesh", Tag: "redis", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}},
	}
	if err := apply(records, 8); err != nil {
		t.Fatal(err)
	}
	assertState(2, 1, 2, 1)
	assertCursor(8)
	if err := apply(records, 8); err != nil {
		t.Fatal(err)
	}
	assertState(2, 1, 2, 1)
	var packets int
	if err := store.db.QueryRow(`SELECT packet_count FROM observers WHERE id='observer'`).Scan(&packets); err != nil || packets != 2 {
		t.Fatalf("duplicate steady batch changed observer count=%d, err=%v", packets, err)
	}
	policy = &corePublicPolicy{generation: 2, fingerprint: "private", prefixes: []corePrivatePrefix{{NodeID: privateNode, Network: "teesside", Prefix: "AA", Size: 1}}}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertState(1, 1, 1, 1)
	third := records[1]
	third.ObservationID = "33333333-3333-4333-8333-333333333333"
	third.Payload, err = json.Marshal(pgEnvelope{Raw: "0D00AABBCCDD", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER root_projection_failure BEFORE INSERT ON observations BEGIN SELECT RAISE(ABORT,'root observation failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := apply([]publicProjectionRecord{third}, 9); err == nil {
		t.Fatal("critical rooted projection write succeeded")
	}
	// The FULL journal is durable, the failing observation is atomic, and
	// public visibility/cursor remain fenced until the journal rebuild succeeds.
	assertState(1, 1, 2, 0)
	assertCursor(8)
	if _, err := store.db.Exec(`DROP TRIGGER root_projection_failure`); err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertState(2, 2, 2, 1)
	if err := apply([]publicProjectionRecord{third}, 9); err != nil {
		t.Fatal(err)
	}
	assertState(2, 2, 2, 1)
	assertCursor(9)
	if err := store.db.QueryRow(`SELECT packet_count FROM observers WHERE id='observer'`).Scan(&packets); err != nil || packets != 2 {
		t.Fatalf("failure/replay/retry double-counted observer=%d, err=%v", packets, err)
	}
}
