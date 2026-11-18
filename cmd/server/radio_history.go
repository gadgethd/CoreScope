package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/meshcore-analyzer/dbschema"
)

// Which radio setting heard a packet (#1869).
//
// An observer reports its "MHz,kHz,SF,CR" in every status message, but
// observers.radio keeps only the latest one. The ingestor records each
// stretch of time an observer stayed on one setting in observer_radios, and
// the radio id a multi-radio node puts on a packet (`rx_radio_id`) in the
// sparse observation_radios, keyed by observation id. SQLite supplies the ID
// when the observation is loaded so it can leave memory with the packet.
//
// The poller refreshes stints before loading observations for broadcast. A
// periodic reload also catches assignments rewritten in place by legacy
// upserts. Historical SQLite reads always use the persisted assignment.

const radioHistoryFullReload = 10 * time.Minute

// canonicalRadio is the one normalisation of "MHz,kHz,SF,CR", shared with
// the ingestor.
var canonicalRadio = dbschema.CanonicalRadio

// RadioStint is one stretch of time an observer's radio stayed on one setting.
type RadioStint struct {
	RadioID   string `json:"radio_id"`
	Radio     string `json:"radio"`
	FirstSeen string `json:"first_seen"`
	LastSeen  string `json:"last_seen"`
}

// observerRadios is one observer's history: the main radio and, on a
// multi-radio node, each radio id. Stints are ordered by FirstSeen.
type observerRadios struct {
	main []RadioStint
	byID map[string][]RadioStint
}

type radioHistorySnapshot struct {
	observers  map[string]*observerRadios
	maxStintID int64
	stintCount int64
}

var (
	radioHistory   atomic.Pointer[radioHistorySnapshot]
	radioRefreshMu sync.Mutex
)

func radioTableExists(ctx context.Context, conn *sql.DB, name string) (bool, error) {
	var present int
	err := conn.QueryRowContext(ctx, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// loadRadioStints reads observer_radios whole. A table the ingestor has not
// created yet (server started first, or an old ingestor) reads as empty and
// is probed again on the next refresh.
func loadRadioStints(conn *sql.DB) (*radioHistorySnapshot, error) {
	snap := &radioHistorySnapshot{observers: map[string]*observerRadios{}}
	present, err := radioTableExists(context.Background(), conn, "observer_radios")
	if err != nil || !present {
		return snap, err
	}
	rows, err := conn.Query(`SELECT id, observer_id, radio_id, radio, first_seen, last_seen
		FROM observer_radios ORDER BY observer_id, radio_id, first_seen, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var obs string
		var st RadioStint
		if err := rows.Scan(&id, &obs, &st.RadioID, &st.Radio, &st.FirstSeen, &st.LastSeen); err != nil {
			return nil, err
		}
		// The ingestor stores canonical settings; rows from before it did
		// are normalised here, so lookups compare plain strings.
		st.Radio = canonicalRadio(st.Radio)
		o := snap.observers[obs]
		if o == nil {
			o = &observerRadios{}
			snap.observers[obs] = o
		}
		if st.RadioID == "" {
			o.main = append(o.main, st)
		} else {
			if o.byID == nil {
				o.byID = map[string][]RadioStint{}
			}
			o.byID[st.RadioID] = append(o.byID[st.RadioID], st)
		}
		if id > snap.maxStintID {
			snap.maxStintID = id
		}
		snap.stintCount++
	}
	return snap, rows.Err()
}

// observationRadioSQL selects the optional sparse radio ID for observation alias o.
func (db *DB) observationRadioSQL(ctx context.Context) (column, join string, err error) {
	if !db.observationRadioTable.Load() {
		present, err := radioTableExists(ctx, db.conn, "observation_radios")
		if err != nil {
			return "", "", err
		}
		if !present {
			return ", NULL", "", nil
		}
		db.observationRadioTable.Store(true)
	}
	return ", radio_assignment.radio_id", " LEFT JOIN observation_radios radio_assignment ON radio_assignment.observation_id = o.id ", nil
}

func (obs *StoreObs) setRadioID(id sql.NullString) {
	if !id.Valid || id.String == "" {
		obs.radioID.Store(nil)
	} else if current := obs.radioID.Load(); current == nil || *current != id.String {
		radioID := id.String
		obs.radioID.Store(&radioID)
	}
}

func (obs *StoreObs) radioIDBytes() int64 {
	if id := obs.radioID.Load(); id != nil {
		return int64(strHdr + len(*id))
	}
	return 0
}

func refreshRadioHistory(conn *sql.DB, full bool) {
	radioRefreshMu.Lock()
	defer radioRefreshMu.Unlock()
	if current := radioHistory.Load(); current != nil && !full {
		present, err := radioTableExists(context.Background(), conn, "observer_radios")
		if err != nil {
			log.Printf("[radio-history] schema check failed: %v", err)
			return
		}
		if !present {
			return
		}
		var maxID, count int64
		if err := conn.QueryRow(`SELECT COALESCE(MAX(id), 0), COUNT(*) FROM observer_radios`).Scan(&maxID, &count); err != nil {
			log.Printf("[radio-history] check failed: %v", err)
			return
		}
		if maxID == current.maxStintID && count == current.stintCount {
			return
		}
	}
	snapshot, err := loadRadioStints(conn)
	if err != nil {
		log.Printf("[radio-history] reload failed: %v", err)
		return
	}
	radioHistory.Store(snapshot)
}

// refreshObservationRadioIDs rechecks sparse assignments for cached rows only.
// Close each SQLite cursor before updating the store so SQL never holds s.mu.
func (s *PacketStore) refreshObservationRadioIDs(ctx context.Context) error {
	_, join, err := s.db.observationRadioSQL(ctx)
	if err != nil || join == "" {
		return err
	}
	snapshot := radioHistory.Load()
	var ids []int
	s.mu.RLock()
	for id, obs := range s.byObsID {
		if obs.radioID.Load() != nil || snapshot.hasRadioIDs(obs.ObserverID) {
			ids = append(ids, id)
		}
	}
	s.mu.RUnlock()

	sort.Ints(ids)
	for batch := range slices.Chunk(ids, 512) {
		assignments, err := s.db.observationRadioIDs(ctx, batch)
		if err != nil {
			return err
		}
		s.mu.Lock()
		for _, id := range batch {
			if obs := s.byObsID[id]; obs != nil {
				prior := obs.radioIDBytes()
				obs.setRadioID(assignments[id])
				s.trackedBytes += obs.radioIDBytes() - prior
			}
		}
		s.mu.Unlock()
	}
	return nil
}

func (h *radioHistorySnapshot) hasRadioIDs(observer string) bool {
	if h == nil {
		return false
	}
	o := h.observers[observer]
	return o != nil && len(o.byID) > 0
}

func (db *DB) observationRadioIDs(ctx context.Context, ids []int) (map[int]sql.NullString, error) {
	tokens := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := db.conn.QueryContext(ctx, `SELECT observation_id, radio_id FROM observation_radios WHERE observation_id IN (`+tokens+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	assignments := make(map[int]sql.NullString, len(args))
	for rows.Next() {
		var id int
		var radio sql.NullString
		if err := rows.Scan(&id, &radio); err != nil {
			return nil, err
		}
		assignments[id] = radio
	}
	return assignments, rows.Err()
}

func runRadioHistory(ctx context.Context, db *DB, currentStore func() *PacketStore) {
	refreshRadioHistory(db.conn, true)
	go func() {
		t := time.NewTicker(radioHistoryFullReload)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refreshRadioHistory(db.conn, true)
				if store := currentStore(); store != nil {
					if err := store.refreshObservationRadioIDs(ctx); err != nil {
						log.Printf("[radio-history] observation assignments: %v", err)
					}
				}
			}
		}
	}()
}

// radioAt returns the setting the observer was on at ts, "" when it never
// reported one. Before its first recorded stretch the first one is assumed:
// the history starts when this feature was deployed, seeded with each
// observer's setting at that time. An unrecognised ID uses the main radio.
func (h *radioHistorySnapshot) radioAt(observer, radioID, ts string) string {
	if h == nil || observer == "" {
		return ""
	}
	o := h.observers[observer]
	if o == nil {
		return ""
	}
	stints := o.main
	if assigned := o.byID[radioID]; len(assigned) > 0 {
		stints = assigned
	}
	switch len(stints) {
	case 0:
		return ""
	case 1:
		return stints[0].Radio
	}
	// RFC 3339 strings compare in time order (the observation side carries
	// fractional seconds, which only matters within the same second).
	i := sort.Search(len(stints), func(i int) bool { return stints[i].FirstSeen > ts })
	if i == 0 {
		return stints[0].Radio
	}
	return stints[i-1].Radio
}

// observationRadio is the setting one observation was heard on.
func observationRadio(obs *StoreObs) string {
	return radioHistory.Load().radioForObservation(obs)
}

func (h *radioHistorySnapshot) radioForObservation(obs *StoreObs) string {
	id := ""
	if assignment := obs.radioID.Load(); assignment != nil {
		id = *assignment
	}
	return h.radioAt(obs.ObserverID, id, obs.Timestamp)
}

// txRadio is the setting of the observation a transmission is displayed by
// (the header observer and path), with that observation's own timestamp.
func txRadio(tx *StoreTx) string {
	h := radioHistory.Load()
	var fallback *StoreObs
	for _, o := range tx.Observations {
		if o.ObserverID != tx.ObserverID {
			continue
		}
		if o.PathJSON == tx.PathJSON {
			return h.radioForObservation(o)
		}
		if fallback == nil {
			fallback = o
		}
	}
	if fallback != nil {
		return h.radioForObservation(fallback)
	}
	return h.radioAt(tx.ObserverID, "", tx.FirstSeen)
}

// txMatchesRadio reports whether some observation of tx was heard on radio
// (canonical). With observers set, only their observations count: "A heard
// it on R", not "A heard it, and someone heard it on R".
func txMatchesRadio(h *radioHistorySnapshot, tx *StoreTx, radio string, observers map[string]bool) bool {
	for _, obs := range tx.Observations {
		if observers != nil && !observers[obs.ObserverID] {
			continue
		}
		if h.radioForObservation(obs) == radio {
			return true
		}
	}
	return false
}

// observerRadioStints returns an observer's recorded radio history, oldest
// first.
func observerRadioStints(observer string) []RadioStint {
	h := radioHistory.Load()
	if h == nil || h.observers[observer] == nil {
		return nil
	}
	o := h.observers[observer]
	out := append([]RadioStint(nil), o.main...)
	for _, s := range o.byID {
		out = append(out, s...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FirstSeen != out[j].FirstSeen {
			return out[i].FirstSeen < out[j].FirstSeen
		}
		return out[i].RadioID < out[j].RadioID
	})
	return out
}

// RadioSummary is one setting in GET /api/radios.
type RadioSummary struct {
	Radio     string   `json:"radio"`
	Observers []string `json:"observers"`
	FirstSeen string   `json:"first_seen"`
	LastSeen  string   `json:"last_seen"`
}

// RadiosResponse is the body of GET /api/radios.
type RadiosResponse struct {
	Radios []RadioSummary `json:"radios"`
}

// handleRadios lists every setting some observer has listened on, most
// observers first.
func (s *Server) handleRadios(w http.ResponseWriter, r *http.Request) {
	byRadio := map[string]*RadioSummary{}
	add := func(observer string, st RadioStint) {
		sum := byRadio[st.Radio]
		if sum == nil {
			sum = &RadioSummary{Radio: st.Radio, FirstSeen: st.FirstSeen, LastSeen: st.LastSeen}
			byRadio[st.Radio] = sum
		}
		if st.FirstSeen < sum.FirstSeen {
			sum.FirstSeen = st.FirstSeen
		}
		if st.LastSeen > sum.LastSeen {
			sum.LastSeen = st.LastSeen
		}
		for _, o := range sum.Observers {
			if o == observer {
				return
			}
		}
		sum.Observers = append(sum.Observers, observer)
	}
	if h := radioHistory.Load(); h != nil {
		for id, o := range h.observers {
			for _, st := range o.main {
				add(id, st)
			}
			for _, stints := range o.byID {
				for _, st := range stints {
					add(id, st)
				}
			}
		}
	}
	resp := RadiosResponse{Radios: make([]RadioSummary, 0, len(byRadio))}
	for _, v := range byRadio {
		sort.Strings(v.Observers)
		resp.Radios = append(resp.Radios, *v)
	}
	sort.Slice(resp.Radios, func(i, j int) bool {
		a, b := resp.Radios[i], resp.Radios[j]
		if len(a.Observers) != len(b.Observers) {
			return len(a.Observers) > len(b.Observers)
		}
		return a.Radio < b.Radio
	})
	writeJSON(w, resp)
}
