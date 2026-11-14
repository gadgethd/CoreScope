package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestPgCursorRestoresHistoryAfterMissingPrivatePolicyWindow(t *testing.T) {
	resetScopeMatchCounters(t)
	database := filepath.Join(t.TempDir(), "projection.db")
	store, err := OpenStore(database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()
	policy := &corePublicPolicy{generation: 1, fingerprint: corePolicyFingerprint(nil)}
	load := func(context.Context) (*corePublicPolicy, error) { return policy, nil }
	projection, err := newCorePublicProjection(store, load, applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	const receiver = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	payload, err := json.Marshal(pgEnvelope{Raw: "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	record := publicProjectionRecord{ObservationID: "11111111-1111-4111-8111-111111111111",
		Topic: "meshcore/ZZZ/cursor-fixture/packets", Payload: payload, Network: "ukmesh",
		RXNodeID: receiver, Tag: "pg", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}}
	if err := projection.Batch(ctx, []publicProjectionRecord{record}); err != nil {
		t.Fatal(err)
	}
	initial, err := projection.RefreshCheckpoint(ctx)
	if err != nil || initial.Revision < 1 {
		t.Fatalf("initial checkpoint=%+v err=%v", initial, err)
	}
	state := pgSourceState{PolicyGeneration: 1, PolicyFingerprint: policy.fingerprint,
		ProjectionRevision: initial.Revision,
		LastSeq:            99, LastTime: time.Now().UTC().Format(time.RFC3339Nano), LastObsID: record.ObservationID}
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	if err := savePgSourceState(cursorFile, state); err != nil {
		t.Fatal(err)
	}
	prefixes := []corePrivatePrefix{{NodeID: receiver, Network: "teesside", Size: 1, Prefix: "AA"}}
	policy = &corePublicPolicy{generation: 2, fingerprint: corePolicyFingerprint(prefixes), prefixes: prefixes}
	// The independent maintenance refresher observes and applies the restrictive
	// epoch. The PostgreSQL follower does not poll during this ready window.
	if _, err := projection.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"observations", "public_projection_journal"} {
		var count int
		if err := store.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("revocation %s=%d err=%v", table, count, err)
		}
	}
	policy = &corePublicPolicy{generation: 3, fingerprint: corePolicyFingerprint(nil)}
	checkpoint, err := projection.RefreshCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seeks := 0
	next, err := pgPolicyCursor(state, checkpoint, time.Now().AddDate(0, 0, -30),
		func(time.Time) (int64, error) { seeks++; return 7, nil })
	if err != nil || seeks != 1 || next.LastSeq != 7 {
		t.Fatalf("missed restrictive epoch skipped retained history: next=%+v seeks=%d err=%v", next, seeks, err)
	}
	if err := projection.Batch(ctx, []publicProjectionRecord{record, record}); err != nil {
		t.Fatal(err)
	}
	var observations, journal int
	store.db.QueryRow("SELECT count(*) FROM observations").Scan(&observations)
	store.db.QueryRow("SELECT count(*) FROM public_projection_journal").Scan(&journal)
	if observations != 1 || journal != 1 {
		t.Fatalf("restored duplicate identities: observations=%d journal=%d", observations, journal)
	}
	next.LastSeq = 99
	if err := savePgSourceState(cursorFile, next); err != nil {
		t.Fatal(err)
	}
	acknowledged, ok := loadPgSourceState(cursorFile)
	if !ok || acknowledged.ProjectionRevision != checkpoint.Revision {
		t.Fatalf("cursor lost acknowledged pruning revision: %+v ok=%v", acknowledged, ok)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(database)
	if err != nil {
		t.Fatal(err)
	}
	projection, err = newCorePublicProjection(store, load, applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart, err := projection.RefreshCheckpoint(ctx)
	if err != nil || afterRestart.Revision != checkpoint.Revision {
		t.Fatalf("restart changed completed pruning revision: %+v err=%v", afterRestart, err)
	}
	seeks = 0
	if _, err := pgPolicyCursor(acknowledged, afterRestart, time.Now(), func(time.Time) (int64, error) {
		seeks++
		return 7, nil
	}); err != nil || seeks != 0 {
		t.Fatalf("restart replayed already acknowledged history: seeks=%d err=%v", seeks, err)
	}
}

func TestProjectionRevisionRollsBackWithFailedJournalReset(t *testing.T) {
	resetScopeMatchCounters(t)
	store := newTestStore(t)
	policy := &corePublicPolicy{generation: 1, fingerprint: corePolicyFingerprint(nil)}
	projection, err := newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) {
		return policy, nil
	}, applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := projection.RefreshCheckpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(pgEnvelope{Raw: "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Batch(context.Background(), []publicProjectionRecord{{
		ObservationID: "22222222-2222-4222-8222-222222222222", Topic: "meshcore/ZZZ/reset-fixture/packets",
		Payload: payload, Network: "ukmesh", Tag: "pg", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER reject_projection_reset BEFORE DELETE ON observations
    BEGIN SELECT RAISE(FAIL,'fixture reset interruption'); END`); err != nil {
		t.Fatal(err)
	}
	policy = &corePublicPolicy{generation: 2, fingerprint: "changed"}
	if _, err := projection.RefreshCheckpoint(context.Background()); err == nil {
		t.Fatal("failed reset published a checkpoint")
	}
	var revision int64
	var ready int
	if err := store.db.QueryRow(`SELECT policy_revision,ready FROM public_projection_state`).Scan(&revision, &ready); err != nil || revision != initial.Revision || ready != 0 {
		t.Fatalf("failed reset revision=%d ready=%d err=%v", revision, ready, err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER reject_projection_reset`); err != nil {
		t.Fatal(err)
	}
	completed, err := projection.RefreshCheckpoint(context.Background())
	if err != nil || completed.Revision != initial.Revision+1 {
		t.Fatalf("retry checkpoint=%+v err=%v", completed, err)
	}
}

func TestLegacyProjectionCursorReplaysOnceWithoutRebuildingCompletedSQLite(t *testing.T) {
	store := newTestStore(t)
	if _, err := store.db.Exec(`CREATE TABLE public_projection_state (
    singleton INTEGER PRIMARY KEY, generation INTEGER, ready INTEGER, policy_fingerprint TEXT);
    INSERT INTO public_projection_state VALUES(1,9,1,'current')`); err != nil {
		t.Fatal(err)
	}
	policy := &corePublicPolicy{generation: 9, fingerprint: "current"}
	projection, err := newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) {
		return policy, nil
	}, func(*Store, publicProjectionRecord) error {
		t.Fatal("legacy completed SQLite was unnecessarily replayed")
		return nil
	}, 30)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := projection.RefreshCheckpoint(context.Background())
	if err != nil || checkpoint.Revision != 1 {
		t.Fatalf("legacy baseline=%+v err=%v", checkpoint, err)
	}
	state := pgSourceState{PolicyGeneration: 9, PolicyFingerprint: "current", LastSeq: 900}
	seeks := 0
	seek := func(time.Time) (int64, error) { seeks++; return 17, nil }
	next, err := pgPolicyCursor(state, checkpoint, time.Now(), seek)
	if err != nil || seeks != 1 || next.LastSeq != 17 || next.ProjectionRevision != 1 {
		t.Fatalf("legacy cursor=%+v seeks=%d err=%v", next, seeks, err)
	}
	policy.generation = 10
	checkpoint, err = projection.RefreshCheckpoint(context.Background())
	if err != nil || checkpoint.Revision != 1 {
		t.Fatalf("counter-only bump changed pruning revision: %+v err=%v", checkpoint, err)
	}
	seeks = 0
	if _, err := pgPolicyCursor(next, checkpoint, time.Now(), seek); err != nil || seeks != 0 {
		t.Fatalf("counter-only bump replayed history: seeks=%d err=%v", seeks, err)
	}
	if _, err := pgPolicyCursor(pgSourceState{LastSeq: 900}, checkpoint, time.Now(), seek); err != nil || seeks != 0 {
		t.Fatalf("fresh follow-only cursor replayed history: seeks=%d err=%v", seeks, err)
	}
}
