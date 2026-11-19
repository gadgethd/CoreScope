package main

import (
	"strings"
	"sync"
)

const (
	loadPathEntries = 256
	loadPathBytes   = 2048
)

// internMax bounds the intern table. Observer IDs, names, IATA codes and
// directions number in the hundreds on a busy instance, so the cap is only a
// guard against an unexpected flood of distinct values.
const internMax = 1 << 16

var (
	internMu    sync.RWMutex
	internTable = make(map[string]string)
)

// internStr returns the canonical copy of s, so a store of hundreds of
// thousands of observations keeps one copy of each string that repeats
// across them (observer IDs, names, IATA codes, directions) rather than the
// one each row scan allocates.
//
// The table holds its copies for the life of the process: they are few, and
// a strongly held copy is what makes the sharing survive garbage collections
// (unique.Make holds its copies weakly, so a value interned again after a GC
// came back as a new copy). Past internMax entries a string is returned as
// it came: nothing is shared, nothing breaks. A hit takes only the read lock
// and does not allocate.
func internStr(s string) string {
	if s == "" {
		return ""
	}
	internMu.RLock()
	c, ok := internTable[s]
	internMu.RUnlock()
	if ok {
		return c
	}
	internMu.Lock()
	defer internMu.Unlock()
	if c, ok := internTable[s]; ok {
		return c
	}
	if len(internTable) >= internMax {
		return s
	}
	c = strings.Clone(s) // do not pin a larger buffer s may be a slice of
	internTable[c] = c
	return c
}

// loadPathStrings shares repeated path JSON within one load batch without
// retaining one-off paths in the process-wide observer string table.
type loadPathStrings map[string]string

func (paths loadPathStrings) share(raw string) string {
	if shared, ok := paths[raw]; ok {
		return shared
	}
	if len(paths) < loadPathEntries && len(raw) <= loadPathBytes {
		paths[raw] = raw
	}
	return raw
}

// loadParsedPaths shares immutable parsed paths during the bulk display pass.
// The pool is discarded when the pass completes. Unique paths beyond its
// budget still use the same decoder.
type loadParsedPaths map[string][]string

func (paths loadParsedPaths) parse(raw string) []string {
	if parsed, ok := paths[raw]; ok {
		return parsed
	}
	parsed := parsePathJSON(raw)
	if paths != nil && len(paths) < loadPathEntries && len(raw) <= loadPathBytes {
		paths[raw] = parsed
	}
	return parsed
}

// loadTimestamps shares adjacent equal timestamps in the loader's row order.
// The first-seen and observation formats have separate caches. Only two
// timestamp values from the current batch can be retained by the cache.
type loadTimestamps struct {
	firstSeen      string
	observationRaw string
	observationAt  string
}

func (times *loadTimestamps) transmission(raw string) string {
	if times.firstSeen != raw {
		times.firstSeen = raw
	}
	return times.firstSeen
}

func (times *loadTimestamps) observation(raw string) string {
	if times.observationRaw != raw {
		times.observationRaw = raw
		times.observationAt = normalizeTimestamp(raw)
	}
	return times.observationAt
}
