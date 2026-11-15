package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// One packet observation, including observer metadata.
// This capability must never wrap status/neighbors/client routes: some of
// those deliberately use the base pool rather than the packet executor.
// Existing replay owns a separate NORMAL policy; live observations stay FULL.
func (s *Store) withOwnedObservationFullTx(ctx context.Context, apply func(*Store) error) error {
	if s.replayTx != nil {
		return fmt.Errorf("packet observation requires an unscoped live Store")
	}
	if apply == nil {
		return fmt.Errorf("packet observation callback is nil")
	}
	waitStart := time.Now()
	writerMu.Lock()
	wait := time.Since(waitStart)
	holdStart := time.Now()
	defer func() {
		writerMu.Unlock()
		recordWriterTiming("packet_observation", wait, time.Since(holdStart), "one FULL observation")
	}()

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var synchronous int
	var journal string
	if err := conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return err
	}
	if synchronous != 2 || journal != "wal" {
		return fmt.Errorf("packet observation requires existing FULL/WAL, got synchronous=%d journal=%s", synchronous, journal)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Explicit construction avoids copying mutexes, atomics or backfill owners.
	// replayTx is the transaction/writer-lock capability. Its owner selects
	// FULL live-observation durability or reconstructible replay policy.
	observation := &Store{
		db: s.db, ingestDB: tx, replayTx: tx, path: s.path,
		sampleIntervalSec: s.sampleIntervalSec,
		linkedCompanions:  s.linkedCompanions,
		hopIndexDays:      s.hopIndexDays,
		relayTouched:      s.relayTouched,
	}
	observation.prefixIdx.store(s.prefixIdx.load())
	observation.neighborGraph.store(s.neighborGraph.load())
	observation.advertEvidenceComplete.Store(s.advertEvidenceComplete.Load())
	committed := false
	defer func() {
		// Failed attempts remain observable, but successful-row counters only
		// become visible below, after the durable transaction commits.
		s.Stats.WriteErrors.Add(observation.Stats.WriteErrors.Load())
		if !committed {
			// All relay cache access holds writerMu. Discard speculative cache
			// entries before releasing it so retry cannot skip a rolled-back touch.
			s.relayTouched = nil
		}
	}()
	if err := observation.prepareStatements(); err != nil {
		return err
	}
	if err := apply(observation); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	s.relayTouched = observation.relayTouched
	for _, counters := range []struct{ dst, src *atomic.Int64 }{
		{&s.Stats.TransmissionsInserted, &observation.Stats.TransmissionsInserted},
		{&s.Stats.ObservationsInserted, &observation.Stats.ObservationsInserted},
		{&s.Stats.DuplicateTransmissions, &observation.Stats.DuplicateTransmissions},
		{&s.Stats.NodeUpserts, &observation.Stats.NodeUpserts},
		{&s.Stats.ObserverUpserts, &observation.Stats.ObserverUpserts},
		{&s.Stats.SignatureDrops, &observation.Stats.SignatureDrops},
		{&s.Stats.ClientUnlinkedDropped, &observation.Stats.ClientUnlinkedDropped},
		{&s.Stats.RelayTouches, &observation.Stats.RelayTouches},
	} {
		counters.dst.Add(counters.src.Load())
	}
	s.Stats.WALCommits.Add(1)
	return nil
}
