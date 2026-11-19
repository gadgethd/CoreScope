package main

import "fmt"

// retainedPacketCounts reports the same logical observations as the packet
// store. A transmission can retain multiple observations from the same
// observer and path because upstream observation IDs are unique on their own.
func (db *DB) retainedPacketCounts() (transmissions, observations int, err error) {
	observer, observerJoin := "o.observer_id", ""
	if db.isV3 {
		observer, observerJoin = "b.id", "LEFT JOIN observers b ON b.rowid = o.observer_idx"
	}
	// One statement reads both totals from the same snapshot. The
	// transmission index handles the common zero/one observation case.
	// The temporary grouping for repeats covers only the current transmission.
	query := fmt.Sprintf(`
		SELECT COUNT(*), COALESCE(SUM(CASE
			WHEN EXISTS (SELECT 1 FROM observations WHERE transmission_id = t.id LIMIT 1 OFFSET 1) THEN (
				SELECT COUNT(*) FROM (
					SELECT 1 FROM observations o %s
					WHERE o.transmission_id = t.id
					GROUP BY CAST(COALESCE(%s, '') AS TEXT) COLLATE BINARY,
					         CAST(COALESCE(o.path_json, '') AS TEXT) COLLATE BINARY
				)
			)
			ELSE EXISTS (SELECT 1 FROM observations WHERE transmission_id = t.id)
		END), 0) FROM transmissions t`, observerJoin, observer)
	err = db.conn.QueryRow(query).Scan(&transmissions, &observations)
	return
}
