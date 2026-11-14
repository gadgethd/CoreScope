package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestProjectionReplayKeepsRadioAndUpstreamIdentityAtomic(t *testing.T) {
	store := newTestStore(t)
	if err := waitForUpstreamIdentityReady(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertObserver("OBS1", "multi", "", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	insert := func(target *Store, identity, radio string) error {
		data := &PacketData{Hash: "paired-source", RawHex: "1100aa", PathJSON: "[]",
			PayloadType: 4, RouteType: 1, ObserverID: "OBS1", RxRadioID: radio,
			UpstreamObservationID: identity, PreserveTime: true}
		_, err := target.InsertTransmission(data)
		return err
	}
	const identity = "11111111-1111-4111-8111-111111111111"
	if err := store.withProjectionReplayTx(ctx, func(target *Store) error {
		if err := insert(target, identity, "narrow"); err != nil {
			return err
		}
		return insert(target, identity, "narrow")
	}); err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("roll back radio alongside observation")
	if err := store.withProjectionReplayTx(ctx, func(target *Store) error {
		if err := insert(target, "22222222-2222-4222-8222-222222222222", "wide"); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("replay error = %v, want rollback", err)
	}
	var observations, radios int
	var storedIdentity, radio string
	if err := store.db.QueryRow(`SELECT COUNT(*), MIN(upstream_observation_id) FROM observations`).Scan(&observations, &storedIdentity); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*), MIN(radio_id) FROM observation_radios`).Scan(&radios, &radio); err != nil {
		t.Fatal(err)
	}
	if observations != 1 || radios != 1 || storedIdentity != identity || radio != "narrow" || store.Stats.ObservationsInserted.Load() != 1 {
		t.Fatalf("observations=%d identity=%s radios=%d radio=%s inserts=%d", observations, storedIdentity, radios, radio, store.Stats.ObservationsInserted.Load())
	}
}

func TestPublicProjectionRebuildClearsUpstreamAuxiliaryMeasurements(t *testing.T) {
	resetScopeMatchCounters(t)
	store := newTestStore(t)
	policy := &corePublicPolicy{generation: 1, fingerprint: "initial"}
	projection, err := newCorePublicProjection(store, func(context.Context) (*corePublicPolicy, error) {
		return policy, nil
	}, applyReplayFixture, 30)
	if err != nil {
		t.Fatal(err)
	}
	const privateNode = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	payload, err := json.Marshal(struct {
		Raw       string `json:"raw"`
		Timestamp string `json:"timestamp"`
		Radio     string `json:"rx_radio_id"`
	}{"0A00D69FD7A5A7475DB07337749AE61FA53A4788E976", time.Now().UTC().Format(time.RFC3339Nano), "narrow"})
	if err != nil {
		t.Fatal(err)
	}
	record := publicProjectionRecord{ObservationID: "11111111-1111-4111-8111-111111111111",
		Topic: "meshcore/ZZZ/observer/packets", Payload: payload, Network: "ukmesh", RXNodeID: privateNode,
		Tag: "pg", Source: MQTTSource{PreserveEnvelopeTime: true, MaxEnvelopePastDays: -1}}
	if err := projection.Batch(context.Background(), []publicProjectionRecord{record}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`INSERT INTO observer_radios(observer_id,radio_id,radio,first_seen,last_seen) VALUES('observer','narrow','869.618,62.5,8,8','2026-11-13T00:00:00Z','2026-11-13T00:00:00Z')`,
		`INSERT INTO observer_neighbor_reports(observer_id,received_at,neighbor_count,neighbors_json) VALUES('observer','2026-11-13T00:00:00Z',1,'[]')`,
		`INSERT INTO tx_rewrite_feed(tx_id) SELECT id FROM transmissions`,
		`INSERT INTO client_node_names(target,rx_pubkey,observed_at,ingested_at,name) VALUES('private-node','observer','2026-11-13T00:00:00Z','2026-11-13T00:00:00Z','private name')`,
	} {
		if _, err := store.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	policy = &corePublicPolicy{generation: 2, fingerprint: "restricted", prefixes: []corePrivatePrefix{{NodeID: privateNode, Network: "ukmesh", Prefix: "AA", Size: 1}}}
	if _, err := projection.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"observations", "observation_radios", "observer_radios", "observer_neighbor_reports", "client_node_names", "tx_rewrite_feed"} {
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("revoked %s retained %d rows", table, count)
		}
	}
}

func TestPublicProjectionDoesNotRunIndependentKeyBackfill(t *testing.T) {
	store := newTestStore(t)
	store.publicProjection = &corePublicProjection{}
	backfill := newKeyBackfiller(store, nil, nil)
	// A projection's decoder and rebuild are its only measurement authority.
	// No key-backfill state/feed may be written by a separate background owner.
	select {
	case <-backfill.Start(context.Background()):
	case <-time.After(time.Second):
		t.Fatal("public projection started independent key backfill")
	}
	for _, table := range []string{"key_backfill_state", "key_backfill_done", "tx_rewrite_feed"} {
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Errorf("%s count=%d err=%v", table, count, err)
		}
	}
}
