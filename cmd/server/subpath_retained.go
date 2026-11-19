package main

import (
	"context"
	"database/sql"
	"slices"
	"strings"
)

const retainedSubpathIDs = "SELECT t.id FROM transmissions t"
const retainedSubpathOrder = " ORDER BY t.first_seen, t.id"

type subpathContext struct {
	pubkeys []string
	seen    map[string]struct{}
}

func (c *subpathContext) add(keys []string) {
	if len(keys) == 0 {
		return
	}
	if c.seen == nil {
		c.seen = make(map[string]struct{})
	}
	for _, key := range keys {
		if _, seen := c.seen[key]; !seen {
			c.seen[key] = struct{}{}
			c.pubkeys = append(c.pubkeys, key)
		}
	}
}

func (s *PacketStore) computeRetainedSubpaths(ctx context.Context, region string, groups []subpathGroup, window TimeWindow) ([]packetFields, error) {
	var regionObservers map[string]bool
	if region != "" {
		ids, err := s.db.GetObserverIdsForRegion(region)
		if err != nil {
			return nil, err
		}
		// An unknown IATA region uses the unfiltered legacy aggregate.
		if len(ids) > 0 {
			regionObservers = make(map[string]bool, len(ids))
			for _, id := range ids {
				regionObservers[id] = true
			}
		}
	}
	_, pm := s.getCachedNodesAndPM()
	db := s.db
	var results []packetFields
	err := db.withRetainedSnapshot(ctx, func(read *sql.Tx) error {

		// Even a narrow regional or time-window query resolves its hop names
		// using the whole packet history. Keep both passes and the sample-page
		// reads in one snapshot so the same context applies throughout.
		var hopContext subpathContext
		if err := db.visitRetainedPacketRows(ctx, read, retainedSubpathIDs+retainedSubpathOrder, nil,
			", NULL", "", packetHydration{decodedKeys: true}, func(packet *retainedPacket) {
				pickBestObservation(packet.tx)
				hopContext.add(buildHopContextPubkeys(packet.tx, pm))
			}); err != nil {
			return err
		}
		counter := newSubpathCounter(groups, region == "" && window.IsZero(),
			makeSubpathResolver(pm, s.graph.Load(), hopContext.pubkeys))
		if err := db.visitRetainedPacketRows(ctx, read, retainedSubpathIDs+retainedSubpathOrder, nil,
			", NULL", "", packetHydration{}, func(packet *retainedPacket) {
				tx := packet.tx
				if subpathIncludes(tx, window, regionObservers) {
					pickBestObservation(tx)
					counter.add(tx.ID, txGetParsedPath(tx))
				}
			}); err != nil {
			return err
		}

		selected := make([][]subpathTally, len(groups))
		var sampleIDs []int
		seen := make(map[int]struct{})
		for i, group := range groups {
			selected[i] = counter.top(group)
			for _, tally := range selected[i] {
				if _, ok := seen[tally.tx]; !ok {
					seen[tally.tx] = struct{}{}
					sampleIDs = append(sampleIDs, tally.tx)
				}
			}
		}
		// Counts are no longer needed after ranking. Hydrate only samples.
		counter.tallies, counter.byLength = nil, nil
		samples := make(map[int][]string, len(sampleIDs))
		for ids := range slices.Chunk(sampleIDs, retainedPacketBatch) {
			packets, err := db.retainedPacketRows(ctx, read, ids, ", NULL", "", packetHydration{})
			if err != nil {
				return err
			}
			for _, id := range ids {
				pickBestObservation(packets[id].tx)
				samples[id] = txGetParsedPath(packets[id].tx)
			}
		}

		results = make([]packetFields, len(groups))
		for i, group := range groups {
			names := counter.names(selected[i], func(id int) []string { return samples[id] })
			results[i] = s.rankSubpaths(names, counter.total, group.Limit)
		}
		return ctx.Err()
	})
	return results, err
}

func (s *PacketStore) visitRetainedSubpathMatches(ctx context.Context, rawHops []string, visit func(*StoreTx, int)) error {
	db := s.db
	query := retainedSubpathIDs
	var args []any
	if isHexAnyCase(rawHops[0]) {
		query += ` WHERE EXISTS (SELECT 1 FROM observations o WHERE o.transmission_id = t.id
		  AND (instr(lower(o.path_json), ?) > 0 OR instr(o.path_json, char(92)) > 0))`
		args = append(args, `"`+strings.ToLower(rawHops[0])+`"`)
	}
	return db.withRetainedSnapshot(ctx, func(read *sql.Tx) error {
		return db.visitRetainedPacketRows(ctx, read, query+retainedSubpathOrder, args, ", NULL", "",
			packetHydration{page: true}, func(packet *retainedPacket) {
				tx := packet.tx
				pickBestObservation(tx)
				if count := subpathOccurrences(txGetParsedPath(tx), rawHops); count > 0 {
					visit(tx, count)
				}
			})
	})
}
