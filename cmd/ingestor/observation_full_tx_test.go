package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func ownedObservationStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "observation.db"))
	if err != nil {
		t.Fatal(err)
	}
	s.WaitForAsyncMigrations()
	t.Cleanup(func() { s.Close() })
	return s
}

// The same rollback contract first runs against the existing autocommit path.
// Its expected failure is retained as evidence, never counted as green.
func ownedObservationApply(s *Store, ctx context.Context, apply func(*Store) error) error {
	if os.Getenv("OWNED_OBSERVATION_BASELINE") == "1" {
		return apply(s)
	}
	return s.withOwnedObservationFullTx(ctx, apply)
}

func ownedObservationPacket(hash string) *PacketData {
	return &PacketData{
		Hash: hash, ObserverID: "OBS", RawHex: "0d00aabbccdd",
		PayloadType: 3, RouteType: 1, PathJSON: "[]",
		Timestamp: "2026-11-14T00:00:00Z", PreserveTime: true,
	}
}

func ownedObservationCount(t *testing.T, s *Store, table string) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestOwnedObservationRollbackContract(t *testing.T) {
	s := ownedObservationStore(t)
	if err := s.UpsertObserverAt("OBS", "before", "LHR", nil, "2026-11-13T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("failure after packet plus observer writes")
	err := ownedObservationApply(s, context.Background(), func(scoped *Store) error {
		if _, err := scoped.InsertTransmission(ownedObservationPacket("rollback")); err != nil {
			return err
		}
		if err := scoped.UpsertObserverAt("OBS", "after", "LHR", nil, "2026-11-14T00:00:00Z"); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error identity lost: %v", err)
	}
	if n := ownedObservationCount(t, s, "transmissions"); n != 0 {
		t.Fatalf("rolled-back transmission persisted: %d", n)
	}
	if n := ownedObservationCount(t, s, "observations"); n != 0 {
		t.Fatalf("rolled-back observation persisted: %d", n)
	}
	var name string
	var packets int
	if err := s.db.QueryRow("SELECT name, packet_count FROM observers WHERE id='OBS'").Scan(&name, &packets); err != nil {
		t.Fatal(err)
	}
	if name != "before" || packets != 1 || s.Stats.TransmissionsInserted.Load() != 0 || s.Stats.WALCommits.Load() != 0 {
		t.Fatalf("rollback changed observer or committed counters: name=%s packets=%d", name, packets)
	}
}

func TestOwnedObservationCommitAndReopen(t *testing.T) {
	s := ownedObservationStore(t)
	if err := s.UpsertObserverAt("OBS", "seed", "LHR", nil, "2026-11-13T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	beforeUpserts := s.Stats.ObserverUpserts.Load()
	s.hopIndexDays = 3
	err := s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
		if scoped.hopIndexDays != 3 || scoped.db != s.db || scoped.ingestDB == s.ingestDB || scoped.replayTx == nil {
			t.Fatal("scoped execution/configuration capability missing")
		}
		data := ownedObservationPacket("committed")
		data.RxRadioID = "radio-a"
		if _, err := scoped.InsertTransmission(data); err != nil {
			return err
		}
		if !data.ObservationInserted {
			t.Fatal("new observation identity not reported")
		}
		if err := scoped.UpsertObserverAt("OBS", "committed", "LHR", nil, data.Timestamp); err != nil {
			return err
		}
		if s.Stats.TransmissionsInserted.Load() != 0 || s.Stats.ObserverUpserts.Load() != beforeUpserts {
			t.Fatal("success counters escaped before commit")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.Stats.TransmissionsInserted.Load() != 1 || s.Stats.ObservationsInserted.Load() != 1 || s.Stats.WALCommits.Load() != 1 || s.Stats.ObserverUpserts.Load() != beforeUpserts+1 {
		t.Fatal("committed counters did not transfer once")
	}
	if n := ownedObservationCount(t, s, "observation_radios"); n != 1 {
		t.Fatalf("radio association missing: %d", n)
	}
	path := s.path
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.WaitForAsyncMigrations()
	var hash, radio, name string
	var packets, synchronous int
	if err := reopened.db.QueryRow(`SELECT t.hash,r.radio_id,b.name,b.packet_count FROM transmissions t JOIN observations o ON o.transmission_id=t.id JOIN observation_radios r ON r.observation_id=o.id JOIN observers b ON b.rowid=o.observer_idx`).Scan(&hash, &radio, &name, &packets); err != nil {
		t.Fatal(err)
	}
	if err := reopened.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if hash != "committed" || radio != "radio-a" || name != "committed" || packets != 2 || synchronous != 2 {
		t.Fatalf("reopened durable row mismatch: %s %s %s %d FULL=%d", hash, radio, name, packets, synchronous)
	}
}

func TestOwnedObservationCriticalWriteFailure(t *testing.T) {
	s := ownedObservationStore(t)
	if err := s.UpsertObserver("OBS", "seed", "LHR", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER owned_observation_failure BEFORE INSERT ON observations BEGIN SELECT RAISE(ABORT,'owned failure'); END`); err != nil {
		t.Fatal(err)
	}
	err := s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
		_, err := scoped.InsertTransmission(ownedObservationPacket("failed"))
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "owned failure") {
		t.Fatalf("expected actual SQLite failure, got %v", err)
	}
	if ownedObservationCount(t, s, "transmissions") != 0 || ownedObservationCount(t, s, "observations") != 0 || s.Stats.TransmissionsInserted.Load() != 0 || s.Stats.WriteErrors.Load() != 1 {
		t.Fatal("SQL error did not rollback transmission/counters while retaining failure")
	}
}

func TestOwnedObservationDuplicateUpstreamIdentity(t *testing.T) {
	s := ownedObservationStore(t)
	for _, id := range []string{"OBS", "OTHER"} {
		if err := s.UpsertObserverAt(id, "seed", "LHR", nil, "2026-11-13T00:00:00Z"); err != nil {
			t.Fatal(err)
		}
	}
	for i, id := range []string{"OBS", "OBS", "OTHER"} {
		err := s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
			p := ownedObservationPacket("same-transmission")
			p.ObserverID = id
			p.UpstreamObservationID = "00000000-0000-4000-8000-00000000000" + map[string]string{"OBS": "1", "OTHER": "2"}[id]
			if _, err := scoped.InsertTransmission(p); err != nil {
				return err
			}
			if p.ObservationInserted != (i != 1) {
				t.Fatalf("inserted identity on delivery %d: %v", i, p.ObservationInserted)
			}
			if p.ObservationInserted {
				return scoped.IncrementObserverPacketCount(id)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if ownedObservationCount(t, s, "transmissions") != 1 || ownedObservationCount(t, s, "observations") != 2 {
		t.Fatal("transmission/observation dedup changed")
	}
	var packets int
	if err := s.db.QueryRow("SELECT sum(packet_count) FROM observers").Scan(&packets); err != nil || packets != 4 {
		t.Fatalf("historical source packet count changed: %d %v", packets, err)
	}
}

func TestOwnedObservationRelayRollbackRetry(t *testing.T) {
	s := ownedObservationStore(t)
	const relay = "aa11223344556677889900aabbccddeeff00112233445566778899aabbccddee"
	seedRelayNode(t, s, relay, "relay", "2026-07-01T00:00:00Z")
	const stamp = "2026-07-10T12:00:00Z"
	wantErr := errors.New("rollback relay")
	err := s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
		scoped.touchRelayNodesLocked([]string{relay}, stamp)
		if s.Stats.RelayTouches.Load() != 0 {
			t.Fatal("relay success counter escaped before commit")
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) || nodeLastSeen(t, s, relay) != "2026-07-01T00:00:00Z" || len(s.relayTouched) != 0 {
		t.Fatal("rolled-back relay/cache persisted")
	}
	if err := s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
		scoped.touchRelayNodesLocked([]string{relay}, stamp)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if nodeLastSeen(t, s, relay) != stamp || s.Stats.RelayTouches.Load() != 1 || s.relayTouched[relay].IsZero() {
		t.Fatal("rollback suppressed subsequent legitimate durable relay touch")
	}
}

func TestOwnedObservationRejectsNestedAndReducedDurability(t *testing.T) {
	s := ownedObservationStore(t)
	called := false
	if err := s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
		if err := scoped.withOwnedObservationFullTx(context.Background(), func(*Store) error { called = true; return nil }); err == nil {
			t.Fatal("nested transaction accepted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		t.Fatal(err)
	}
	err := s.withOwnedObservationFullTx(context.Background(), func(*Store) error { called = true; return nil })
	if err == nil || called {
		t.Fatal("reduced durability accepted or callback invoked")
	}
	if _, err := s.db.Exec("PRAGMA synchronous=FULL"); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedObservationPanicAndCancellationRelease(t *testing.T) {
	for _, mode := range []string{"panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s := ownedObservationStore(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hadPanic := false
			func() {
				defer func() {
					if v := recover(); v != nil {
						hadPanic = true
						if mode != "panic" || v != "owned panic" {
							t.Fatalf("panic identity changed: %v", v)
						}
					}
				}()
				err := s.withOwnedObservationFullTx(ctx, func(scoped *Store) error {
					if _, err := scoped.InsertTransmission(ownedObservationPacket("interrupted")); err != nil {
						return err
					}
					if mode == "panic" {
						panic("owned panic")
					}
					cancel()
					return ctx.Err()
				})
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel identity changed: %v", err)
				}
			}()
			if hadPanic != (mode == "panic") {
				t.Fatal("interruption changed panic propagation")
			}
			if ownedObservationCount(t, s, "transmissions") != 0 {
				t.Fatal("interrupted packet persisted")
			}
			finished := make(chan error, 1)
			go func() {
				finished <- s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
					_, err := scoped.InsertTransmission(ownedObservationPacket("recovered"))
					return err
				})
			}()
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("interruption leaked writer mutex/connection")
			}
		})
	}
}

func ownedObservationSemanticRows(t *testing.T, s *Store) string {
	t.Helper()
	queries := []string{
		`SELECT hash,route_type,payload_type,payload_version,decoded_json,channel_hash,scope_name,from_pubkey,code1,code2 FROM transmissions ORDER BY hash`,
		`SELECT t.hash,b.id,o.direction,o.snr,o.rssi,o.score,o.path_json,o.raw_hex,o.resolved_path,o.upstream_observation_id FROM observations o JOIN transmissions t ON t.id=o.transmission_id JOIN observers b ON b.rowid=o.observer_idx ORDER BY t.hash,b.id,o.id`,
		`SELECT id,name,iata,packet_count FROM observers ORDER BY id`,
		`SELECT public_key,name,role,lat,lon,advert_count,foreign_advert FROM nodes ORDER BY public_key`,
		`SELECT hash,raw_hex,reason,observer_id,observer_name,node_pubkey,node_name FROM dropped_packets ORDER BY id`,
		`SELECT o.raw_hex,r.radio_id FROM observation_radios r JOIN observations o ON o.id=r.observation_id ORDER BY o.id`,
	}
	all := make([][][]interface{}, 0, len(queries))
	for _, query := range queries {
		rows, err := s.db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		values := make([][]interface{}, 0)
		for rows.Next() {
			row := make([]interface{}, len(columns))
			ptrs := make([]interface{}, len(columns))
			for i := range row {
				ptrs[i] = &row[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			values = append(values, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		all = append(all, values)
	}
	data, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestOwnedObservationActualPacketFormats(t *testing.T) {
	private := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	advert := buildAdvertHex(public, private, 1700000000, makeAppdata("Owned node"))
	tampered := []byte(advert)
	if tampered[76] == '0' {
		tampered[76] = 'f'
	} else {
		tampered[76] = '0'
	}
	for _, tc := range []struct {
		name, topic, payload        string
		transmissions, drops, nodes int
	}{
		{"advert", "meshcore/LHR/OBS/packets", `{"raw":"` + advert + `","origin":"owner"}`, 1, 0, 1},
		{"quarantine", "meshcore/LHR/OBS/packets", `{"raw":"` + string(tampered) + `","origin":"owner"}`, 0, 1, 0},
		{"ack-radio", "meshcore/LHR/OBS/packets", `{"raw":"0d00aabbccdd","origin":"owner","rx_radio_id":"narrow","SNR":8,"RSSI":-80}`, 1, 0, 0},
		{"channel", "meshcore/message/channel/0", `{"text":"owner: hello","SNR":8,"RSSI":-80}`, 1, 0, 0},
		{"direct", "meshcore/message/direct/0", `{"text":"owner: direct","SNR":8,"RSSI":-80}`, 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, candidate := ownedObservationStore(t), ownedObservationStore(t)
			message := &mockMessage{topic: tc.topic, payload: []byte(tc.payload)}
			// The bridge format hashes ingest time to seconds. Both sides run
			// inside one reserved second; a crossed boundary is an invalid
			// equivalence fixture, not permission to normalize changed hashes.
			if tc.name == "channel" || tc.name == "direct" {
				for time.Now().Nanosecond() > int(500*time.Millisecond) {
					time.Sleep(time.Millisecond)
				}
			}
			start := time.Now().Unix()
			var parsed map[string]interface{}
			if err := json.Unmarshal(message.payload, &parsed); err != nil {
				t.Fatal(err)
			}
			// The extracted packet body is unchanged for successful inputs.
			// Compare its previous autocommit execution against the actual
			// integrated handler, rather than wrapping a test-only callback.
			if err := handlePacketObservation(base, "owned-baseline", MQTTSource{}, message, nil, nil, &Config{}, message.topic, strings.Split(message.topic, "/"), parsed); err != nil {
				t.Fatal(err)
			}
			if err := handleMessage(candidate, "owned-candidate", MQTTSource{}, message, nil, nil, &Config{}); err != nil {
				t.Fatal(err)
			}
			if (tc.name == "channel" || tc.name == "direct") && start != time.Now().Unix() {
				t.Fatal("bridge test crossed its whole-second input authority")
			}
			if ownedObservationCount(t, candidate, "transmissions") != tc.transmissions || ownedObservationCount(t, candidate, "dropped_packets") != tc.drops || ownedObservationCount(t, candidate, "nodes") != tc.nodes {
				t.Fatal("actual packet helper writes missing")
			}
			if got, want := ownedObservationSemanticRows(t, candidate), ownedObservationSemanticRows(t, base); got != want {
				t.Fatalf("actual packet semantics differ\nwant %s\ngot %s", want, got)
			}
		})
	}
}

func TestOwnedObservationQuarantineRollsBackAsUnit(t *testing.T) {
	s := ownedObservationStore(t)
	wantErr := errors.New("failed quarantine unit")
	err := s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
		if err := scoped.InsertDroppedPacket(&DroppedPacket{Hash: "bad", RawHex: "bad", Reason: "invalid signature", ObserverID: "OBS"}); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) || ownedObservationCount(t, s, "dropped_packets") != 0 || s.Stats.SignatureDrops.Load() != 0 {
		t.Fatal("rolled-back quarantine or success counter escaped")
	}
}

func TestOwnedObservationActualBridgeFailureRollsBack(t *testing.T) {
	for _, topic := range []string{"meshcore/message/channel/0", "meshcore/message/direct/0"} {
		t.Run(topic, func(t *testing.T) {
			s := ownedObservationStore(t)
			if _, err := s.db.Exec(`CREATE TRIGGER owned_bridge_failure BEFORE INSERT ON observations BEGIN SELECT RAISE(ABORT,'owned bridge observation failure'); END`); err != nil {
				t.Fatal(err)
			}
			message := &mockMessage{topic: topic, payload: []byte(`{"text":"owner: rollback bridge"}`)}
			err := handleMessage(s, "owned-bridge-failure", MQTTSource{}, message, nil, nil, &Config{})
			if err == nil || !strings.Contains(err.Error(), "owned bridge observation failure") {
				t.Fatalf("critical actual bridge insertion error was swallowed: %v", err)
			}
			if ownedObservationCount(t, s, "transmissions") != 0 || ownedObservationCount(t, s, "observations") != 0 || ownedObservationCount(t, s, "observers") != 0 {
				t.Fatal("actual bridge failure committed a partial packet/observer")
			}
			if s.Stats.TransmissionsInserted.Load() != 0 || s.Stats.WALCommits.Load() != 0 || s.Stats.WriteErrors.Load() != 1 {
				t.Fatal("actual bridge failure published success counters or lost failure")
			}
			if _, err := s.db.Exec("DROP TRIGGER owned_bridge_failure"); err != nil {
				t.Fatal(err)
			}
			if err := handleMessage(s, "owned-bridge-recovery", MQTTSource{}, message, nil, nil, &Config{}); err != nil {
				t.Fatal(err)
			}
			if ownedObservationCount(t, s, "transmissions") != 1 || ownedObservationCount(t, s, "observations") != 1 || s.Stats.WALCommits.Load() != 1 {
				t.Fatal("actual bridge did not recover after failed observation")
			}
		})
	}
}
