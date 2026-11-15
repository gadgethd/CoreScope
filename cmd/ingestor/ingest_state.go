package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// IngestStateSnapshot is one coherent buffer lifecycle sample. Other existing
// stats counters remain eventually consistent; none may substitute for this
// accepted-work invariant. Missing identity/state means unknown, never zero.
type IngestStateSnapshot struct {
	Version      int    `json:"version"`
	Generation   uint64 `json:"generation"`
	PID          int    `json:"pid"`
	StartTicks   uint64 `json:"startTicks"`
	Ready        bool   `json:"ready"`
	Accepting    bool   `json:"accepting"`
	Pending      int    `json:"pending"`
	Active       int    `json:"active"`
	Outstanding  int    `json:"outstanding"`
	DrainState   string `json:"drainState"`
	DroppedTotal int64  `json:"droppedTotal"`
}

// Every field is protected by IngestBuffer.stateMu. Outstanding is charged at
// successful admission, before the consumer can run, and released only after
// callback return. A channel receive can therefore never manufacture zero.
type ingestBufferState struct {
	generation  uint64
	pid         int
	startTicks  uint64
	ready       bool
	accepting   bool
	active      int
	outstanding int
	drainState  string
}

func newIngestBufferState() ingestBufferState {
	return ingestBufferState{
		generation: 1, pid: os.Getpid(), startTicks: ingestProcessStartTicks(),
		accepting: true, drainState: "running",
	}
}

func ingestProcessStartTicks() uint64 {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0 // unavailable identity is explicit; controllers must refuse it
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) <= 19 { // fields start at process state, original field 3
		return 0
	}
	value, err := strconv.ParseUint(fields[19], 10, 64) // original field 22
	if err != nil {
		return 0
	}
	return value
}

func (b *IngestBuffer) Snapshot() IngestStateSnapshot {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return IngestStateSnapshot{
		Version: 1, Generation: b.state.generation,
		PID: b.state.pid, StartTicks: b.state.startTicks,
		Ready: b.state.ready, Accepting: b.state.accepting,
		Pending: b.state.outstanding - b.state.active, Active: b.state.active,
		Outstanding: b.state.outstanding, DrainState: b.state.drainState,
		DroppedTotal: b.dropped.Load(),
	}
}

// CloseAdmission is the shutdown fence, not proof that an external source is
// quiescent. Callers must separately stop/disconnect their source producers.
// Already-accepted jobs retain their FIFO position and completion channels.
func (b *IngestBuffer) CloseAdmission() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.state.accepting {
		b.state.accepting = false
		b.state.drainState = "draining"
		b.state.generation++
	}
}

func (b *IngestBuffer) runAccepted(job func()) {
	b.stateMu.Lock()
	b.state.active++
	b.state.generation++
	b.stateMu.Unlock()
	returned := false
	defer func() {
		b.stateMu.Lock()
		if !returned {
			b.state.accepting = false
			b.state.drainState = "incomplete"
		}
		b.state.active--
		b.state.outstanding--
		b.state.generation++
		b.stateMu.Unlock()
	}()
	job()
	returned = true
}

func (b *IngestBuffer) workerExited() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.state.drainState != "incomplete" {
		if b.state.outstanding == 0 {
			b.state.drainState = "drained"
		} else {
			b.state.drainState = "incomplete"
		}
	}
	b.state.generation++
	// Publish Done while holding the same state mutex: no snapshot can
	// observe drained before the worker's completion signal is closed.
	close(b.done)
}

// Drain never authorizes an unready startup queue to write. Done plus zero
// accepted work is required for success; timeouts stay explicitly incomplete
// even if a still-running callback returns later. The caller owns its existing
// shutdown deadline and must not close SQLite beneath an active callback.
func (b *IngestBuffer) Drain(ctx context.Context) error {
	b.Stop()
	if err := ingestDrainContextErr(ctx); err != nil {
		return b.drainTimedOut(err)
	}
	select {
	case <-b.Done():
		if err := ingestDrainContextErr(ctx); err != nil {
			return b.drainTimedOut(err)
		}
		state := b.Snapshot()
		if state.Outstanding == 0 && state.DrainState == "drained" {
			return nil
		}
		return fmt.Errorf("ingest drain incomplete: ready=%v outstanding=%d", state.Ready, state.Outstanding)
	case <-ctx.Done():
		return b.drainTimedOut(ctx.Err())
	}
}

func ingestDrainContextErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (b *IngestBuffer) drainTimedOut(err error) error {
	b.stateMu.Lock()
	b.state.drainState = "incomplete"
	b.state.generation++
	outstanding := b.state.outstanding
	b.stateMu.Unlock()
	return fmt.Errorf("ingest drain incomplete with %d accepted jobs: %w", outstanding, err)
}
