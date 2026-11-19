package main

import "testing"

func TestSelectedObservationKeepsStableTiesAndUpdatesForLatePath(t *testing.T) {
	tx := &StoreTx{}
	if row := txToMap(tx); row["observer_id"] != nil || row["snr"] != nil ||
		row["path_json"] != nil || row["_parsedPath"] != nil {
		t.Fatalf("display metadata for a packet without observations: %v", row)
	}

	snr, rssi := 5.5, -92.0
	first := &StoreObs{ObserverID: "first", ObserverName: "First repeater", ObserverIATA: "SJC",
		Direction: "rx", PathJSON: `["aa"]`, SNR: &snr, RSSI: &rssi}
	tie := &StoreObs{ObserverID: "tie", PathJSON: `["bb"]`}
	tx.addObservation(first)
	tx.addObservation(tie)
	pickBestObservation(tx)
	if got := tx.displayObservation(); got != first {
		t.Fatalf("equal path lengths selected %p, want first observation %p", got, first)
	}
	grouped := snapshotGroupedRow(tx)
	if grouped.observerID != first.ObserverID || grouped.observerName != first.ObserverName ||
		grouped.observerIATA != first.ObserverIATA || grouped.snr != &snr || grouped.rssi != &rssi ||
		grouped.pathJSON != first.PathJSON {
		t.Fatalf("grouped row lost selected observation fields: %+v", grouped)
	}
	row := txToMap(tx)
	if row["direction"] != "rx" {
		t.Fatalf("packet row direction = %v, want rx", row["direction"])
	}
	exposedHops := row["_parsedPath"].([]string)
	exposedHops[0] = "modified"
	if got := txGetParsedPath(tx); len(got) != 1 || got[0] != "aa" {
		t.Fatalf("packet row shared its parsed path with the store: %v", got)
	}

	late := &StoreObs{ObserverID: "late", PathJSON: `["cc","dd"]`}
	tx.addObservation(late)
	pickBestObservation(tx)
	if got := tx.displayObservation(); got != late {
		t.Fatalf("longer late path selected %p, want late observation %p", got, late)
	}
	// An earlier serialization snapshot keeps its own originally selected values.
	if grouped.observerID != "first" || grouped.pathJSON != first.PathJSON {
		t.Fatalf("late observation changed a completed row snapshot: %+v", grouped)
	}
	if got := txGetParsedPath(tx); len(got) != 2 || got[0] != "cc" || got[1] != "dd" {
		t.Fatalf("cached path after the late observation: %v", got)
	}
}
