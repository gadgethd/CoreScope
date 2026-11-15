package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func ownedStateInvariant(t *testing.T, state IngestStateSnapshot) {
	t.Helper()
	if state.Outstanding != state.Pending+state.Active || state.Active < 0 || state.Active > 1 || state.Pending < 0 || state.Generation == 0 || state.Version != 1 || state.PID != os.Getpid() {
		t.Fatalf("incoherent accepted-work state: %+v", state)
	}
	if runtime.GOOS == "linux" && state.StartTicks == 0 {
		t.Fatal("Linux process start identity missing")
	}
}

func TestOwnedIngestStateIncludesActiveAndDispatchHandoff(t *testing.T) {
	b := NewIngestBuffer(2)
	defer b.Stop()
	b.Submit(func() {})
	queued := b.Snapshot()
	ownedStateInvariant(t, queued)
	if queued.Outstanding != 1 || queued.Pending != 1 || queued.Active != 0 {
		t.Fatal("admitted job missing before dispatch")
	}
	// Deterministically inspect the actual receive-to-active handoff. The
	// existing Pending depth is zero, but accepted work must still be one.
	job := <-b.jobs
	handoff := b.Snapshot()
	ownedStateInvariant(t, handoff)
	if b.Pending() != 0 || handoff.Outstanding != 1 || handoff.Pending != 1 {
		t.Fatal("channel receive manufactured false zero outstanding")
	}
	b.runAccepted(job)
	done := b.Snapshot()
	ownedStateInvariant(t, done)
	if done.Outstanding != 0 || done.Generation <= queued.Generation {
		t.Fatal("callback return did not release accepted-work state")
	}
}

func TestOwnedIngestStateDrainWaitsAcceptedFIFO(t *testing.T) {
	b := NewIngestBuffer(8)
	b.Start()
	b.Ready()
	started, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	order := []int{}
	first, accepted := b.SubmitTracked(func() error {
		close(started)
		<-release
		mu.Lock()
		order = append(order, 0)
		mu.Unlock()
		return nil
	})
	if !accepted {
		t.Fatal("first job rejected")
	}
	<-started
	for i := 1; i <= 3; i++ {
		i := i
		b.Submit(func() { mu.Lock(); order = append(order, i); mu.Unlock() })
	}
	state := b.Snapshot()
	ownedStateInvariant(t, state)
	if state.Active != 1 || state.Pending != 3 || state.Outstanding != 4 || !state.Accepting || !state.Ready {
		t.Fatalf("active plus queued state differs: %+v", state)
	}
	b.CloseAdmission()
	if result, accepted := b.SubmitTracked(func() error { return errors.New("must not run") }); accepted || result != nil {
		t.Fatal("closed admission accepted new work")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- b.Drain(ctx) }()
	select {
	case err := <-drained:
		t.Fatalf("drain finished while accepted job active: %v", err)
	default:
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	final := b.Snapshot()
	ownedStateInvariant(t, final)
	if final.Accepting || final.Outstanding != 0 || final.DrainState != "drained" || final.DroppedTotal != 0 {
		t.Fatalf("drain did not prove accepted work complete: %+v", final)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, got := range order {
		if got != i {
			t.Fatalf("FIFO changed: %v", order)
		}
	}
	if len(order) != 4 {
		t.Fatalf("accepted job lost: %v", order)
	}
}

func TestOwnedIngestStateUnreadyBacklogIsIncomplete(t *testing.T) {
	b := NewIngestBuffer(2)
	b.Start()
	var ran atomic.Bool
	b.Submit(func() { ran.Store(true) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Drain(ctx); err == nil {
		t.Fatal("unready accepted backlog falsely certified drained")
	}
	b.Ready() // shutdown must not retroactively authorize startup writes
	state := b.Snapshot()
	ownedStateInvariant(t, state)
	if ran.Load() || state.Ready || state.Accepting || state.Outstanding != 1 || state.DrainState != "incomplete" {
		t.Fatalf("unready accepted work hidden or processed: %+v", state)
	}
}

func TestOwnedIngestStateDeadlineRemainsIncomplete(t *testing.T) {
	b := NewIngestBuffer(2)
	b.Start()
	b.Ready()
	started, release := make(chan struct{}), make(chan struct{})
	b.Submit(func() { close(started); <-release })
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("deadline error identity lost: %v", err)
	}
	if state := b.Snapshot(); state.Outstanding != 1 || state.Active != 1 || state.DrainState != "incomplete" {
		t.Fatalf("active timeout incorrectly complete: %+v", state)
	}
	close(release)
	<-b.Done()
	if state := b.Snapshot(); state.Outstanding != 0 || state.DrainState != "incomplete" {
		t.Fatalf("late callback rewrote failed deadline into success: %+v", state)
	}
	if err := b.Drain(context.Background()); err == nil {
		t.Fatal("later call waived prior timeout")
	}
}

func TestOwnedIngestStateConcurrentSnapshotsAndFailures(t *testing.T) {
	b := NewIngestBuffer(1024)
	b.Start()
	b.Ready()
	var accepted, executed atomic.Int64
	var submit sync.WaitGroup
	for g := 0; g < 4; g++ {
		submit.Add(1)
		go func() {
			defer submit.Done()
			for i := 0; i < 100; i++ {
				_, ok := b.SubmitTracked(func() error { executed.Add(1); return errors.New("owned attempt failed") })
				if ok {
					accepted.Add(1)
				}
			}
		}()
	}
	submit.Wait()
	for i := 0; i < 200; i++ {
		ownedStateInvariant(t, b.Snapshot())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if accepted.Load() != 400 || executed.Load() != accepted.Load() {
		t.Fatalf("accepted callbacks lost: accepted=%d executed=%d", accepted.Load(), executed.Load())
	}
	// Drained certifies execution completion, not successful persistence;
	// tracked errors and existing WriteErrors/integrity checks remain separate.
	ownedStateInvariant(t, b.Snapshot())
}

func ownedStateReadStats(t *testing.T, path string) IngestorStatsSnapshot {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snap IngestorStatsSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestOwnedIngestStatePeriodicAndFinalStats(t *testing.T) {
	s := ownedObservationStore(t)
	path := filepath.Join(t.TempDir(), "stats.json")
	t.Setenv("CORESCOPE_INGESTOR_STATS", path)
	b := NewIngestBuffer(2)
	b.Start()
	b.Ready()
	started, release := make(chan struct{}), make(chan struct{})
	b.Submit(func() { close(started); <-release })
	<-started
	stop := StartStatsFileWriter(s, 5*time.Millisecond, b)
	defer stop()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real periodic stats did not publish")
		}
		time.Sleep(time.Millisecond)
	}
	before := ownedStateReadStats(t, path)
	if before.SampledAt == "" || before.IngestState == nil || before.IngestState.Active != 1 || before.IngestState.Outstanding != 1 {
		t.Fatal("periodic stats omitted actual active write")
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := finishIngestShutdown(ctx, s, b, stop); err != nil {
		t.Fatal(err)
	}
	final := ownedStateReadStats(t, path)
	if final.IngestState == nil || final.IngestState.DrainState != "drained" || final.IngestState.Outstanding != 0 || final.IngestState.Accepting || final.IngestState.Generation <= before.IngestState.Generation {
		t.Fatal("final stats did not certify real drain")
	}
	bytes, _ := os.ReadFile(path)
	time.Sleep(15 * time.Millisecond)
	after, _ := os.ReadFile(path)
	if string(bytes) != string(after) {
		t.Fatal("periodic writer overwrote final drain state")
	}
}

func TestOwnedIngestStateMissingBufferRemainsUnknown(t *testing.T) {
	s := ownedObservationStore(t)
	snap := ingestStatsSnapshot(s, time.Now(), nil)
	if snap.IngestState != nil {
		t.Fatal("legacy caller fabricated zero buffer state")
	}
}

func TestOwnedIngestStateFinalStatsFailureIsNotCleanShutdown(t *testing.T) {
	s := ownedObservationStore(t)
	t.Setenv("CORESCOPE_INGESTOR_STATS", filepath.Join(t.TempDir(), "missing", "stats.json"))
	b := NewIngestBuffer(2)
	b.Start()
	b.Ready()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := finishIngestShutdown(ctx, s, b, nil); err == nil {
		t.Fatal("failed final state publication falsely certified clean shutdown")
	}
}

func TestOwnedIngestStateFinalStopCannotExtendDeadline(t *testing.T) {
	s := ownedObservationStore(t)
	path := filepath.Join(t.TempDir(), "stats.json")
	t.Setenv("CORESCOPE_INGESTOR_STATS", path)
	b := NewIngestBuffer(2)
	b.Start()
	b.Ready()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := finishIngestShutdown(ctx, s, b, func() { <-ctx.Done() })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late stop/publication passed deadline: %v", err)
	}
	final := ownedStateReadStats(t, path)
	if final.IngestState == nil || final.IngestState.DrainState != "incomplete" {
		t.Fatal("late stop was published as drained")
	}
}

func TestOwnedIngestStateSignalWorker(t *testing.T) {
	dir := os.Getenv("OWNED_CORE_SIGNAL_FIXTURE")
	if dir == "" {
		t.Skip("private signal worker")
	}
	s, err := OpenStore(filepath.Join(dir, "signal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.WaitForAsyncMigrations()
	b := NewIngestBuffer(2)
	b.Start()
	b.Ready()
	b.Submit(func() {
		if err := os.WriteFile(filepath.Join(dir, "active"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			panic(err)
		}
		for {
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if _, err := s.InsertTransmission(ownedObservationPacket("signal-accepted")); err != nil {
			panic(err)
		}
	})
	stop := StartStatsFileWriter(s, 5*time.Millisecond, b)
	defer stop()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	defer signal.Stop(sig)
	if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	<-sig
	b.CloseAdmission()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := finishIngestShutdown(ctx, s, b, stop); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedIngestStateRealSIGTERMDrainsAcceptedWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.json")
	cmd := exec.Command(os.Args[0], "-test.run=^TestOwnedIngestStateSignalWorker$", "-test.timeout=8s")
	cmd.Env = append(os.Environ(), "OWNED_CORE_SIGNAL_FIXTURE="+dir, "CORESCOPE_INGESTOR_STATS="+path)
	var output strings.Builder
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	reaped := false
	defer func() {
		if !reaped {
			_ = cmd.Process.Kill()
			<-wait
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, ready := os.Stat(filepath.Join(dir, "ready"))
		_, active := os.Stat(filepath.Join(dir, "active"))
		_, stats := os.Stat(path)
		if ready == nil && active == nil && stats == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("signal fixture failed to become actually active")
		}
		time.Sleep(time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-wait:
		reaped = true
		t.Fatalf("SIGTERM exited with accepted active work: %v\n%s", err, output.String())
	case <-time.After(20 * time.Millisecond):
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := <-wait
	reaped = true
	if err != nil {
		t.Fatalf("signal worker failed: %v\n%s", err, output.String())
	}
	snap := ownedStateReadStats(t, path)
	if snap.IngestState == nil || snap.IngestState.PID != cmd.Process.Pid || snap.IngestState.Outstanding != 0 || snap.IngestState.DrainState != "drained" || snap.TxInserted != 1 {
		t.Fatalf("real signal lacked final accepted-write proof: %+v", snap)
	}
}
