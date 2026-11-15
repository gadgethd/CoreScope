package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/meshcore-analyzer/dbschema"
)

// A separate SQLite writer is outside writerMu and the Store's pool. Reserving
// before the packet's first read lets busy_timeout arbitrate this contention;
// a deferred read snapshot cannot safely be upgraded by waiting afterward.
func observationExternalWriter(t *testing.T, s *Store) (*sql.Conn, func()) {
	t.Helper()
	db, err := sql.Open("sqlite3", dbschema.WriterDSN(s.path))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	held := true
	release := func() {
		if held {
			held = false
			if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(release)
	return conn, release
}

func observationWALReader(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func observationNoPartialPacket(t *testing.T, reader *sql.DB, s *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var transmissions, observations int
	if err := reader.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM transmissions), (SELECT count(*) FROM observations)`).Scan(&transmissions, &observations); err != nil {
		t.Fatalf("WAL reader blocked by writer reservation: %v", err)
	}
	if transmissions != 0 || observations != 0 || s.Stats.TransmissionsInserted.Load() != 0 || s.Stats.ObservationsInserted.Load() != 0 || s.Stats.WALCommits.Load() != 0 {
		t.Fatal("uncommitted packet rows or success counters escaped")
	}
}

func observationWaitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(7 * time.Second):
		t.Fatal("operation failed to release writer mutex/connection within existing busy budget")
		return nil
	}
}

func TestOwnedObservationWriterReservationActualHandlers(t *testing.T) {
	for _, tc := range []struct{ name, topic, payload string }{
		{"raw", "meshcore/LHR/OBS/packets", `{"raw":"0d00aabbccdd","origin":"held writer","rx_radio_id":"radio"}`},
		{"channel", "meshcore/message/channel/0", `{"text":"held writer: channel"}`},
		{"direct", "meshcore/message/direct/0", `{"text":"held writer: direct"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := ownedObservationStore(t)
			reader := observationWALReader(t, s.path)
			_, release := observationExternalWriter(t, s)
			result := make(chan error, 1)
			started := time.Now()
			go func() {
				result <- handleMessage(s, "external-contention", MQTTSource{}, &mockMessage{topic: tc.topic, payload: []byte(tc.payload)}, nil, nil, &Config{})
			}()
			select {
			case err := <-result:
				t.Fatalf("packet returned before external writer released: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			observationNoPartialPacket(t, reader, s)
			// Match the one-second recovery fault without changing busy_timeout.
			time.Sleep(time.Until(started.Add(time.Second)))
			release()
			if err := observationWaitResult(t, result); err != nil {
				t.Fatalf("packet did not resume after external reservation: %v", err)
			}
			if s.Stats.TransmissionsInserted.Load() != 1 || s.Stats.ObservationsInserted.Load() != 1 || s.Stats.WALCommits.Load() != 1 || s.Stats.WriteErrors.Load() != 0 {
				t.Fatal("successful observation did not publish its exact committed counters")
			}
			if ownedObservationCount(t, s, "transmissions") != 1 || ownedObservationCount(t, s, "observations") != 1 {
				t.Fatal("actual handler lost/duplicated the observation")
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
			var synchronous int
			var journal string
			if err := reopened.db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
				t.Fatal(err)
			}
			if err := reopened.db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
				t.Fatal(err)
			}
			if synchronous != 2 || journal != "wal" || ownedObservationCount(t, reopened, "transmissions") != 1 || ownedObservationCount(t, reopened, "observations") != 1 {
				t.Fatal("reopened packet or FULL/WAL durability differs")
			}
			t.Logf("one observation resumed and reopened after %s", time.Since(started))
		})
	}
}

func TestOwnedObservationWriterReservationTimeoutAndReuse(t *testing.T) {
	s := ownedObservationStore(t)
	reader := observationWALReader(t, s.path)
	_, release := observationExternalWriter(t, s)
	called := false
	started := time.Now()
	err := s.withOwnedObservationFullTx(context.Background(), func(*Store) error { called = true; return nil })
	elapsed := time.Since(started)
	var busy sqlite3.Error
	if !errors.As(err, &busy) || busy.Code != sqlite3.ErrBusy || called || elapsed < 4500*time.Millisecond || elapsed > 6500*time.Millisecond {
		t.Fatalf("existing five-second admission budget/type changed: elapsed=%s called=%v err=%T %v", elapsed, called, err, err)
	}
	observationNoPartialPacket(t, reader, s)
	release()
	result := make(chan error, 1)
	go func() {
		result <- s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
			_, err := scoped.InsertTransmission(ownedObservationPacket("after-busy-timeout"))
			return err
		})
	}()
	if err := observationWaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if s.Stats.WALCommits.Load() != 1 || s.Stats.WriteErrors.Load() != 1 || ownedObservationCount(t, s, "observations") != 1 {
		t.Fatal("failed begin leaked state or prevented later durable work")
	}
	t.Logf("typed busy after %s; same Store successfully reused; admission WriteErrors=%d", elapsed, s.Stats.WriteErrors.Load())
}

func TestOwnedObservationWriterReservationCancellationAndReuse(t *testing.T) {
	s := ownedObservationStore(t)
	reader := observationWALReader(t, s.path)
	_, release := observationExternalWriter(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := make(chan struct{}, 1)
	result := make(chan error, 1)
	started := time.Now()
	go func() {
		result <- s.withOwnedObservationFullTx(ctx, func(*Store) error { called <- struct{}{}; return nil })
	}()
	select {
	case err := <-result:
		t.Fatalf("begin completed before contention/cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	// SQLite may finish its busy-handler wait before surfacing cancellation.
	// Do not turn this test into a new promise of immediate interruption.
	time.Sleep(100 * time.Millisecond)
	release()
	err := observationWaitResult(t, result)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("cancellation identity not retained: %T %v", err, err)
	}
	select {
	case <-called:
		t.Fatal("canceled reservation entered packet callback")
	default:
	}
	observationNoPartialPacket(t, reader, s)
	result = make(chan error, 1)
	go func() {
		result <- s.withOwnedObservationFullTx(context.Background(), func(scoped *Store) error {
			_, err := scoped.InsertTransmission(ownedObservationPacket("after-canceled-begin"))
			return err
		})
	}()
	if err := observationWaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if s.Stats.WALCommits.Load() != 1 || ownedObservationCount(t, s, "observations") != 1 {
		t.Fatal("canceled begin leaked reservation or transaction")
	}
	t.Logf("canceled begin/reuse completed in %s (%s)", time.Since(started), fmt.Sprint(err))
}

func TestOwnedObservationWriterFailureReportedByActualBufferedCallback(t *testing.T) {
	s := ownedObservationStore(t)
	if _, err := s.db.Exec(`CREATE TRIGGER owned_callback_failure BEFORE INSERT ON observations BEGIN SELECT RAISE(ABORT,'owned callback failure'); END`); err != nil {
		t.Fatal(err)
	}
	var reported bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&reported)
	defer log.SetOutput(previousOutput)
	buffer := NewIngestBuffer(2)
	buffer.Start()
	t.Cleanup(func() {
		buffer.Stop()
		select {
		case <-buffer.Done():
		case <-time.After(2 * time.Second):
			t.Error("buffer cleanup did not finish")
		}
	})
	buffer.Ready()
	finished := make(chan struct{})
	buffer.Submit(func() {
		defer close(finished)
		handleMQTTMessage(s, "owned-callback", MQTTSource{}, &mockMessage{topic: "meshcore/LHR/OBS/packets", payload: []byte(`{"raw":"0d00aabbccdd"}`)}, nil, nil, &Config{})
	})
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("buffered callback did not return")
	}
	buffer.Stop()
	<-buffer.Done()
	if !strings.Contains(reported.String(), "MQTT [owned-callback] handler error: insert observation: owned callback failure") {
		t.Fatalf("actual buffered handler failed silently: %s", reported.String())
	}
	if s.Stats.WriteErrors.Load() != 1 || s.Stats.WALCommits.Load() != 0 || ownedObservationCount(t, s, "transmissions") != 0 || ownedObservationCount(t, s, "observations") != 0 {
		t.Fatal("reporting double-counted failure or committed a partial packet")
	}
}
