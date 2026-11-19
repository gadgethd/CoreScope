package main

import (
	"fmt"
	"testing"
	"time"
)

// Synthetic traffic: eight resolved relays and their raw prefixes per tx.
func relaySnapshotStore1908(count, nodes, expired int) *PacketStore {
	pubkeys := make([]string, nodes)
	for i := range pubkeys {
		pubkeys[i] = fmt.Sprintf("%02x%062x", i%256, i)
	}
	now := time.Now().UTC()
	old := now.Add(-2 * time.Hour).Format(time.RFC3339)
	recent := now.Add(-30 * time.Minute).Format(time.RFC3339)
	store := &PacketStore{
		packets:        make([]*StoreTx, count),
		byPathHop:      make(map[string][]*StoreTx, nodes+256),
		retentionHours: 1,
	}
	pt := 5
	hopsSeen := make(map[string]bool, 16)
	for i := range store.packets {
		tx := &StoreTx{ID: i + 1, FirstSeen: recent, PayloadType: &pt, parsedPath: make([]string, 8), pathParsed: true}
		if i < expired {
			tx.FirstSeen = old
		}
		resolved := make([]string, 8)
		for hop := range resolved {
			resolved[hop] = pubkeys[(i*17+hop*13)%nodes]
			tx.parsedPath[hop] = resolved[hop][:2]
		}
		store.packets[i] = tx
		addTxToPathHopIndex(store.byPathHop, tx)
		store.addResolvedPubkeysToPathHopIndex(tx, resolved, hopsSeen)
	}
	return store
}

func TestRelaySnapshotSurvivesEviction1908(t *testing.T) {
	store := relaySnapshotStore1908(4, 8, 2)
	before := store.computeRepeaterRelayInfoMap(24)
	if len(before) != 16 {
		t.Fatalf("snapshot has %d buckets, want eight raw and eight resolved", len(before))
	}
	if got := store.RunEviction(); got != 2 {
		t.Fatalf("evicted %d transmissions, want 2", got)
	}
	after := store.computeRepeaterRelayInfoMap(24)
	for key, info := range before {
		if info.RelayCount24h != 4 || after[key].RelayCount24h != 2 {
			t.Errorf("bucket %q: before %d after %d, want 4 and 2", key, info.RelayCount24h, after[key].RelayCount24h)
		}
	}
}

func TestRelayComputeConcurrentEviction1908(t *testing.T) {
	store := relaySnapshotStore1908(30000, 256, 1)
	key := fmt.Sprintf("%064x", 0)
	want := len(store.byPathHop[key])
	const readers = 2
	start := make(chan struct{})
	result := make(chan map[string]RepeaterRelayInfo, readers)
	for i := 0; i < readers; i++ {
		go func() {
			<-start
			result <- store.computeRepeaterRelayInfoMap(24)
		}()
	}
	evicted := make(chan int, 1)
	go func() {
		<-start
		evicted <- store.RunEviction()
	}()
	close(start)

	// Either lock order is valid. Each concurrent rebuild must observe one
	// complete iteration of the index, on either side of the eviction.
	deadline := time.After(30 * time.Second)
	select {
	case got := <-evicted:
		if got != 1 {
			t.Fatalf("evicted %d transmissions, want 1", got)
		}
	case <-deadline:
		t.Fatal("eviction did not finish")
	}
	for i := 0; i < readers; i++ {
		select {
		case got := <-result:
			if count := got[key].RelayCount24h; count != want && count != want-1 {
				t.Errorf("relay snapshot count = %d, want before-eviction %d or after-eviction %d", count, want, want-1)
			}
		case <-deadline:
			t.Fatal("relay computation did not finish")
		}
	}
}

func BenchmarkRelaySnapshot1908(b *testing.B) {
	for _, size := range []struct{ tx, nodes int }{{30000, 50}, {30000, 2000}, {100000, 2000}} {
		store := relaySnapshotStore1908(size.tx, size.nodes, 0)
		b.Run(fmt.Sprintf("tx=%d/nodes=%d", size.tx, size.nodes), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snapshot := store.computeRepeaterRelayInfoMap(24)
				if len(snapshot) != len(store.byPathHop) {
					b.Fatal("refresh lost a hop bucket")
				}
			}
			b.ReportMetric(float64(size.tx*16), "entries/refresh")
		})
	}
}
