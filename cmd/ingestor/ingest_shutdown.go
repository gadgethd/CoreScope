package main

import (
	"context"
	"errors"
)

// finishIngestShutdown is shared by main's real signal path and its behavior
// tests. It does not close SQLite while accepted work could still be active.
// The source producer fence/disconnect is owned by main before entering here.
func finishIngestShutdown(ctx context.Context, store *Store, buffer *IngestBuffer, stopStats func()) error {
	drainErr := buffer.Drain(ctx)
	if stopStats != nil {
		stopStats() // no older periodic snapshot may overwrite the final outcome
	}
	if err := ingestDrainContextErr(ctx); err != nil {
		drainErr = errors.Join(drainErr, buffer.drainTimedOut(err))
	}
	writeErr := writeIngestStatsNow(store, buffer)
	if err := ingestDrainContextErr(ctx); err != nil {
		// A slow final publication cannot extend the shutdown budget into
		// success. Retain the failed outcome even when work itself drained.
		drainErr = errors.Join(drainErr, buffer.drainTimedOut(err))
		writeErr = errors.Join(writeErr, writeIngestStatsNow(store, buffer))
	}
	return errors.Join(drainErr, writeErr)
}
