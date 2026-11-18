package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

const retainedPacketBatch = 384

// packetHistoryRequired selects SQLite whenever the store may omit committed
// transmissions. A timestamp alone can't prove coverage: an old transmission
// can receive a new observation after startup or eviction.
func (s *PacketStore) packetHistoryRequired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxMemoryMB > 0 || s.retentionHours > 0 || s.hotStartupHours > 0 ||
		s.loadCapEdge != "" || s.evictedThrough != ""
}

// The default feed needs only the page's transmissions. An ingest-ID
// index walk avoids copying and sorting the entire live cache on each poll.
func (q PacketQuery) unfilteredFlatPage() bool {
	q.Limit, q.Offset, q.Order = 0, 0, ""
	return q == (PacketQuery{})
}

type retainedPacket struct {
	tx        *StoreTx
	paths     map[int][]*string
	rawFrames map[int]string
	allObs    []*StoreObs
}

type packetHydration struct {
	page, completeObservations, wireFrames bool
	decodedKeys, relayPaths, relayEvidence bool
}

func (db *DB) retainedPacketRows(ctx context.Context, read *sql.Tx, ids []int, radioCols, radioJoin string, mode packetHydration) (map[int]*retainedPacket, error) {
	packets := make(map[int]*retainedPacket, len(ids))
	if len(ids) == 0 {
		return packets, nil
	}
	scope, resolved, rawFrame := "NULL", "NULL", "NULL"
	txData := "NULL, NULL, NULL, NULL"
	obsData := "NULL, NULL, NULL, NULL, NULL"
	if mode.page {
		txData = "t.raw_hex, t.route_type, t.payload_type, t.decoded_json"
		obsData = "o.snr, o.rssi, o.score,"
		if db.isV3 {
			obsData += " b.name, b.iata"
		} else {
			obsData += " o.observer_name, b.iata"
		}
	} else if mode.decodedKeys {
		txData = "NULL, NULL, NULL, t.decoded_json"
	}
	if mode.page && db.hasScopeName {
		scope = "t.scope_name"
	}
	if (mode.page || mode.relayPaths) && db.hasResolvedPath {
		resolved = "o.resolved_path"
	}
	if (mode.wireFrames || mode.relayEvidence) && db.hasObsRawHex {
		rawFrame = "o.raw_hex"
	}
	observer, stampExpr, observerJoin := "o.observer_id", "o.timestamp", "LEFT JOIN observers b ON b.id = o.observer_id"
	if db.isV3 {
		observer = "b.id"
		stampExpr = "strftime('%Y-%m-%dT%H:%M:%fZ', o.timestamp, 'unixepoch')"
		observerJoin = "LEFT JOIN observers b ON b.rowid = o.observer_idx"
	}
	query := fmt.Sprintf(`SELECT t.id, t.hash, t.first_seen, %s, %s, o.id, o.direction,
		%s, o.path_json, %s, %s, %s, %s%s
		FROM transmissions t LEFT JOIN observations o ON o.transmission_id = t.id
		%s %s WHERE t.id IN (%s) ORDER BY t.id, o.timestamp DESC, o.id ASC`,
		txData, scope, obsData, observer, stampExpr, resolved, rawFrame, radioCols, observerJoin, radioJoin,
		strings.TrimSuffix(strings.Repeat("?,", len(ids)), ","))
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := read.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var id int
	var raw, hash, first, decoded, scopeName, direction, path, observerID, name, iata, stamp, resolvedPath, frame, radio sql.NullString
	var route, payload, observationID, score sql.NullInt64
	var snr, rssi sql.NullFloat64
	for rows.Next() {
		if err := rows.Scan(&id, &hash, &first, &raw, &route, &payload, &decoded, &scopeName, &observationID,
			&direction, &snr, &rssi, &score, &name, &iata, &path, &observerID, &stamp, &resolvedPath, &frame, &radio); err != nil {
			return nil, err
		}
		packet := packets[id]
		if packet == nil {
			packet = &retainedPacket{tx: &StoreTx{
				ID: id, RawHex: raw.String, Hash: hash.String, FirstSeen: first.String, LatestSeen: first.String,
				RouteType: nullIntPtr(route), PayloadType: nullIntPtr(payload), DecodedJSON: decoded.String,
				ScopeName: nullStrPtr(scopeName),
			}}
			packets[id] = packet
		}
		tx := packet.tx
		if !observationID.Valid {
			continue
		}
		relayWidth := observedRelayHopHexLen(frame.String, path.String)
		duplicate := tx.mergeObservationEvidence(observerID.String, path.String, relayWidth) != nil
		if duplicate && !mode.completeObservations {
			continue
		}
		obs := &StoreObs{ID: int(observationID.Int64), TransmissionID: id, ObserverID: observerID.String,
			ObserverName: name.String, ObserverIATA: iata.String, Direction: direction.String,
			SNR: nullFloatPtr(snr), RSSI: nullFloatPtr(rssi), Score: nullIntPtr(score),
			PathJSON: path.String, Timestamp: normalizeTimestamp(stamp.String), relayHopHexLen: relayWidth,
		}
		obs.setRadioID(radio)
		if !duplicate {
			tx.addObservation(obs)
			if obs.Timestamp > tx.LatestSeen {
				tx.LatestSeen = obs.Timestamp
			}
		}
		if mode.completeObservations {
			packet.allObs = append(packet.allObs, obs)
		}
		if rp := unmarshalResolvedPath(resolvedPath.String); rp != nil {
			if packet.paths == nil {
				packet.paths = make(map[int][]*string)
			}
			packet.paths[obs.ID] = rp
		}
		if mode.wireFrames && frame.String != "" {
			if packet.rawFrames == nil {
				packet.rawFrames = make(map[int]string)
			}
			packet.rawFrames[obs.ID] = frame.String
		}
	}
	return packets, rows.Err()
}

func (p *retainedPacket) spansNode(keys map[string]bool, pm *prefixMap) bool {
	decoded := p.tx.ParsedDecoded()
	for _, field := range []string{"pubKey", "destPubKey", "srcPubKey"} {
		if pk, ok := decoded[field].(string); ok && keys[pk] {
			return true
		}
	}
	skipped := 0
	for _, obs := range p.tx.Observations {
		path := p.paths[obs.ID]
		if len(path) == 0 && pm != nil && obs.PathJSON != "" && obs.PathJSON != "[]" {
			path = resolvePathForObsColdLoad(obs.PathJSON, obs.ObserverID, p.tx, pm, &skipped)
		}
		for _, relay := range path {
			if relay != nil && keys[*relay] {
				return true
			}
		}
	}
	return false
}

func (p *retainedPacket) renderExpanded() packetFields {
	fields := txToMap(p.tx, true)
	for i, observation := range fields["observations"].([]packetFields) {
		if rp := p.paths[p.tx.Observations[i].ID]; rp != nil {
			observation["resolved_path"] = rp
		}
	}
	p.addBestPath(fields)
	return fields
}

func (p *retainedPacket) addBestPath(fields packetFields) {
	if best, _ := longestStoredResolvedPath(snapshotRPObs(p.tx), p.paths); best != nil {
		fields["resolved_path"] = best
	}
}

func (p *retainedPacket) renderObservation(obs *StoreObs) packetFields {
	fields := enrichObsFields(obs, p.tx)
	if path := p.paths[obs.ID]; path != nil {
		fields["resolved_path"] = path
	}
	if rawHex := p.rawFrames[obs.ID]; rawHex != "" {
		fields["raw_hex"] = rawHex
	}
	return fields
}

func (p *retainedPacket) renderObservations() []packetFields {
	result := make([]packetFields, 0, len(p.allObs))
	for _, obs := range p.allObs {
		result = append(result, p.renderObservation(obs))
	}
	return result
}

// retainedPacketFilter pushes simple predicates into indexed SQLite reads.
// More complex predicates use the same matcher as the resident packet store.
func (db *DB) retainedPacketFilter(q PacketQuery, match packetMatcher, inclusive bool) (string, []any) {
	where := []string{"t.hash <> ''", "t.first_seen <> ''"}
	var args []any
	add := func(predicate string, value any) {
		where = append(where, predicate)
		args = append(args, value)
	}
	if q.Hash != "" {
		add("t.hash = ?", strings.ToLower(q.Hash))
	}
	if q.Type != nil {
		add("t.payload_type = ?", *q.Type)
	}
	if q.Route != nil {
		add("t.route_type = ?", *q.Route)
	}
	lower, upper := "t.first_seen > ?", "t.first_seen < ?"
	if inclusive {
		lower, upper = "t.first_seen >= ?", "t.first_seen <= ?"
	}
	if q.Since != "" {
		add(lower, q.Since)
	}
	if q.Until != "" {
		add(upper, q.Until)
	}
	observer := "o.observer_id"
	if db.isV3 {
		observer = "o.observer_idx"
	}
	addObservers := func(set map[string]bool) {
		ids := make([]string, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		keys, _ := json.Marshal(ids)
		values := "SELECT value FROM json_each(?)"
		if db.isV3 {
			values = "SELECT b.rowid FROM observers b WHERE b.id IN (" + values + ")"
		}
		add("EXISTS (SELECT 1 FROM observations o WHERE o.transmission_id = t.id AND "+observer+" IN ("+values+"))", string(keys))
	}
	if q.Observer != "" {
		addObservers(match.observerIDs)
	}
	if q.Region != "" {
		addObservers(match.sets.regionObs)
	}
	if q.Channel != "" {
		where = append(where, "t.payload_type = 5")
	}
	return " FROM transmissions t WHERE " + strings.Join(where, " AND "), args
}

func (db *DB) retainedPacketOrder(q PacketQuery, grouped bool) string {
	if !grouped {
		if q.Order == "ASC" {
			return " ORDER BY t.id ASC"
		}
		return " ORDER BY t.id DESC"
	}
	// SQLite v3 stores reception time as Unix seconds. Legacy stores use text.
	stamp := "o.timestamp"
	if db.isV3 {
		stamp = "strftime('%Y-%m-%dT%H:%M:%fZ', o.timestamp, 'unixepoch')"
	}
	return " ORDER BY MAX(t.first_seen, COALESCE((SELECT " + stamp + " FROM observations o " +
		"WHERE o.transmission_id = t.id ORDER BY o.timestamp DESC LIMIT 1), t.first_seen)) DESC, t.id DESC"
}

func (s *PacketStore) queryRetainedPackets(ctx context.Context, q PacketQuery, grouped bool, nodes []string) (*PacketResult, error) {
	match := newPacketMatcher(q, s.resolvePacketFilterSets(q))
	keys := make(map[string]bool, len(nodes)+1)
	if q.Node != "" {
		keys[match.sets.nodePK] = true
	}
	for _, node := range nodes {
		keys[s.resolveQueryNode(node)] = true
	}
	var pm *prefixMap
	if len(keys) > 0 {
		_, pm = s.getCachedNodesAndPM()
	}
	complex := len(keys) > 0 || q.ExcludeTx || q.Area != "" || q.Channel != "" || q.Radio != ""
	if complex {
		release, err := s.db.claimRetainedScan(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	// The indexed predicates already select the date, type and route. The
	// multi-node endpoint historically includes both boundary timestamps.
	inclusive := len(nodes) != 0
	match.query.Since, match.query.Until, match.query.Type, match.query.Route = "", "", nil, nil
	if q.Channel != "" {
		// The channel's payload type is already enforced by SQLite.
		match.query.Channel = ""
	}
	where, args := s.db.retainedPacketFilter(q, match, inclusive)
	sqlIDs := "SELECT t.id" + where + s.db.retainedPacketOrder(q, grouped)
	radioCols, radioJoin, err := s.db.observationRadioSQL(ctx)
	if err != nil {
		return nil, err
	}
	// The cursor, total and hydrated page all refer to one committed snapshot.
	read, err := s.db.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer read.Rollback()
	result := &PacketResult{Packets: make([]packetFields, 0)}
	if !complex {
		if err := read.QueryRowContext(ctx, "SELECT COUNT(*)"+where, args...).Scan(&result.Total); err != nil {
			return nil, err
		}
		sqlIDs += " LIMIT ? OFFSET ?"
		args = append(args, q.Limit, q.Offset)
	}
	rows, err := read.QueryContext(ctx, sqlIDs, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var page []int
	if complex {
		mode := packetHydration{
			decodedKeys: len(keys) > 0 || q.Area != "" || q.Channel != "",
			relayPaths:  len(keys) > 0,
		}
		ids := make([]int, 0, retainedPacketBatch)
		for {
			ids, err = scanPacketIDs(rows, ids[:0])
			if err != nil {
				return nil, err
			}
			if len(ids) == 0 {
				break
			}
			candidates, err := s.db.retainedPacketRows(ctx, read, ids, radioCols, radioJoin, mode)
			if err != nil {
				return nil, err
			}
			for _, id := range ids {
				packet := candidates[id]
				if !match.transmission(packet.tx) || !match.observations(packet.tx) ||
					(q.Channel != "" && !matchesDecodedChannel(packet.tx.ParsedDecoded(), q.Channel)) ||
					(len(keys) > 0 && !packet.spansNode(keys, pm)) {
					continue
				}
				result.Total++
				if result.Total > q.Offset && len(page) < q.Limit {
					page = append(page, id)
				}
			}
		}
	} else {
		page, err = scanPacketIDs(rows, make([]int, 0, q.Limit))
		if err != nil {
			return nil, err
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var groupedRows []groupedRow
	for ids := range slices.Chunk(page, retainedPacketBatch) {
		packets, err := s.db.retainedPacketRows(ctx, read, ids, radioCols, radioJoin, packetHydration{page: true})
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			packet := packets[id]
			pickBestObservation(packet.tx)
			if grouped {
				groupedRows = append(groupedRows, snapshotGroupedRow(packet.tx))
			} else if q.ExpandObservations {
				result.Packets = append(result.Packets, packet.renderExpanded())
			} else {
				fields := txToMap(packet.tx)
				packet.addBestPath(fields)
				result.Packets = append(result.Packets, fields)
			}
		}
	}
	if grouped {
		return groupedRowsToPage(groupedRows, result.Total), nil
	}
	return result, nil
}

func (db *DB) claimRetainedScan(ctx context.Context) (func(), error) {
	db.retainedScanInit.Do(func() { db.retainedScanSlot = make(chan struct{}, 1) })
	select {
	case db.retainedScanSlot <- struct{}{}:
		return func() { <-db.retainedScanSlot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (db *DB) withRetainedSnapshot(ctx context.Context, run func(*sql.Tx) error) error {
	release, err := db.claimRetainedScan(ctx)
	if err != nil {
		return err
	}
	defer release()
	read, err := db.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer read.Rollback()
	return run(read)
}

func (s *PacketStore) visitRetainedNodeHistory(ctx context.Context, pubkey, since string, visit func(*StoreTx)) error {
	_, pm := s.getCachedNodesAndPM()
	keys := map[string]bool{pubkey: true}
	return s.visitRetainedHistory(ctx, retainedHistoryScope{query: PacketQuery{Since: since}}, func(packet *retainedPacket) {
		if packet.spansNode(keys, pm) {
			pickBestObservation(packet.tx)
			visit(packet.tx)
		}
	})
}

func (s *PacketStore) visitRetainedHopHistory(ctx context.Context, pubkey, since string, visit func(*StoreTx)) error {
	if len(pubkey) < 2 {
		return nil
	}
	return s.visitRetainedHistory(ctx, retainedHistoryScope{query: PacketQuery{Since: since}, floodPrefix: strings.ToUpper(pubkey[:2])}, func(packet *retainedPacket) {
		visit(packet.tx)
	})
}

func (s *PacketStore) visitRetainedObserverHistory(ctx context.Context, observer string, since time.Time, visit func(*retainedPacket, *StoreObs)) error {
	scope := retainedHistoryScope{observer: observer, receivedSince: since}
	return s.visitRetainedHistory(ctx, scope, func(packet *retainedPacket) {
		for _, obs := range packet.tx.Observations {
			if obs.ObserverID != observer {
				continue
			}
			if stamp, ok := obs.ParsedTime(); ok && !stamp.Before(since) {
				visit(packet, obs)
			}
		}
	})
}

func (s *PacketStore) visitRetainedPathHistory(ctx context.Context, pubkey string, visit func(*retainedPacket)) error {
	if len(pubkey) < 2 {
		return nil
	}
	return s.visitRetainedHistory(ctx, retainedHistoryScope{pathNode: strings.ToLower(pubkey)}, func(packet *retainedPacket) {
		pickBestObservation(packet.tx)
		visit(packet)
	})
}

type retainedHistoryScope struct {
	query         PacketQuery
	floodPrefix   string
	pathNode      string
	observer      string
	receivedSince time.Time
	advertOrigin  string
	groupByOrigin bool
}

// Read matching retained transmissions in chronological order and bounded batches.
func (s *PacketStore) visitRetainedHistory(ctx context.Context, scope retainedHistoryScope, visit func(*retainedPacket)) error {
	radioCols, radioJoin, err := s.db.observationRadioSQL(ctx)
	if err != nil {
		return err
	}
	q := scope.query
	where, args := s.db.retainedPacketFilter(q, newPacketMatcher(q, packetFilterSets{}), true)
	if scope.floodPrefix != "" {
		where += ` AND t.route_type IN (?, ?) AND EXISTS (
			SELECT 1 FROM observations o WHERE o.transmission_id = t.id AND instr(o.path_json, ?) > 0)`
		args = append(args, RouteTransportFlood, RouteFlood, `"`+scope.floodPrefix)
	}
	if scope.pathNode != "" {
		pathMatch := "instr(lower(o.path_json), ?) > 0 OR instr(o.path_json, char(92)) > 0"
		args = append(args, `"`+scope.pathNode[:2])
		if s.db.hasResolvedPath {
			pathMatch += " OR instr(lower(o.resolved_path), ?) > 0 OR instr(o.resolved_path, char(92)) > 0"
			args = append(args, `"`+scope.pathNode+`"`)
		}
		where += " AND EXISTS (SELECT 1 FROM observations o WHERE o.transmission_id = t.id AND (" + pathMatch + "))"
	}
	if scope.observer != "" {
		predicate := "o.observer_id = ? AND julianday(o.timestamp) >= julianday(?)"
		since := any(scope.receivedSince.UTC().Format(time.RFC3339Nano))
		if s.db.isV3 {
			predicate = "o.observer_idx = (SELECT rowid FROM observers WHERE id = ?) AND o.timestamp >= ?"
			since = scope.receivedSince.Unix()
		}
		where += " AND EXISTS (SELECT 1 FROM observations o WHERE o.transmission_id = t.id AND " + predicate + ")"
		args = append(args, scope.observer, since)
	}
	const origin = `lower(CASE WHEN json_valid(t.decoded_json)
		THEN json_extract(t.decoded_json, '$.pubKey') END)`
	if scope.advertOrigin != "" {
		where += " AND " + origin + " = ?"
		args = append(args, scope.advertOrigin)
	}
	order := " ORDER BY t.first_seen, t.id"
	if scope.groupByOrigin {
		order = " ORDER BY " + origin + ", t.first_seen, t.id"
	}
	return s.db.withRetainedSnapshot(ctx, func(read *sql.Tx) error {
		return s.db.visitRetainedPacketRows(ctx, read, "SELECT t.id"+where+order, args, radioCols, radioJoin,
			packetHydration{page: true, relayEvidence: true}, visit)
	})
}

// visitRetainedPacketRows streams ordered packet IDs and materializes one batch
// at a time using the caller's SQLite snapshot.
func (db *DB) visitRetainedPacketRows(ctx context.Context, read *sql.Tx, idQuery string, args []any,
	radioCols, radioJoin string, mode packetHydration, visit func(*retainedPacket)) error {
	rows, err := read.QueryContext(ctx, idQuery, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	ids := make([]int, 0, retainedPacketBatch)
	for {
		ids, err = scanPacketIDs(rows, ids[:0])
		if err != nil || len(ids) == 0 {
			return err
		}
		packets, err := db.retainedPacketRows(ctx, read, ids, radioCols, radioJoin, mode)
		if err != nil {
			return err
		}
		for _, id := range ids {
			visit(packets[id])
		}
	}
}

func scanPacketIDs(rows *sql.Rows, ids []int) ([]int, error) {
	for len(ids) < cap(ids) && rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// retainedTimestamps returns the same transmission clock as the VCR list.
func (db *DB) retainedTimestamps(ctx context.Context, since string) ([]string, error) {
	rows, err := db.conn.QueryContext(ctx, `SELECT first_seen FROM transmissions
		WHERE first_seen > ? AND hash <> '' ORDER BY first_seen, id`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stamps []string
	for rows.Next() {
		var stamp string
		if err := rows.Scan(&stamp); err != nil {
			return nil, err
		}
		stamps = append(stamps, stamp)
	}
	return stamps, rows.Err()
}

func (db *DB) lookupRetainedPackets(ctx context.Context, hashes []string, numericID int, includeFrames bool) (map[int]*retainedPacket, error) {
	radioCols, radioJoin, err := db.observationRadioSQL(ctx)
	if err != nil {
		return nil, err
	}
	read, err := db.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer read.Rollback()
	var ids []int
	if len(hashes) != 0 {
		keys := make([]string, len(hashes))
		for i, hash := range hashes {
			keys[i] = strings.ToLower(hash)
		}
		encoded, err := json.Marshal(keys)
		if err != nil {
			return nil, err
		}
		rows, err := read.QueryContext(ctx,
			"SELECT id FROM transmissions WHERE hash IN (SELECT value FROM json_each(?))", string(encoded))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	} else {
		// Numeric URLs historically prefer a transmission ID when both exist.
		var id sql.NullInt64
		err := read.QueryRowContext(ctx, `SELECT COALESCE(
			(SELECT id FROM transmissions WHERE id = ?),
			(SELECT transmission_id FROM observations WHERE id = ?))`, numericID, numericID).Scan(&id)
		if err != nil {
			return nil, err
		}
		if id.Valid {
			ids = append(ids, int(id.Int64))
		}
	}
	return db.retainedPacketRows(ctx, read, ids, radioCols, radioJoin, packetHydration{page: true, completeObservations: true, wireFrames: includeFrames})
}

func (db *DB) retainedPacketDetail(ctx context.Context, param string) (packetFields, []packetFields, error) {
	var hashes []string
	var numericID int
	if hashPattern.MatchString(strings.ToLower(param)) {
		hashes = []string{param}
	} else {
		id, err := strconv.Atoi(param)
		if err != nil {
			return nil, nil, nil
		}
		numericID = id
	}
	packets, err := db.lookupRetainedPackets(ctx, hashes, numericID, true)
	if err != nil {
		return nil, nil, err
	}
	for id, p := range packets {
		pickBestObservation(p.tx)
		if len(hashes) != 0 || id == numericID {
			return p.renderExpanded(), p.renderObservations(), nil
		}
		for _, obs := range p.allObs {
			if obs.ID == numericID {
				return p.renderObservation(obs), p.renderObservations(), nil
			}
		}
	}
	return nil, nil, nil
}
