package main

import (
	"database/sql"
	"math"
	"strings"
	"testing"
)

// The nullable store fields keep their pointer types but share immutable
// values through bounded pools. These tests pin the behavior the memory
// accounting relies on: identical values share one pointer, distinct values
// do not, the pools are bounded, and NULL/"" stay distinct.
func TestSharedNullablePools(t *testing.T) {
	a := nullIntPtr(sql.NullInt64{Int64: 7, Valid: true})
	b := nullIntPtr(sql.NullInt64{Int64: 7, Valid: true})
	if a != b {
		t.Fatal("equal int values must share one pooled pointer")
	}
	c := nullIntPtr(sql.NullInt64{Int64: 8, Valid: true})
	if a == c {
		t.Fatal("distinct int values must not share a pointer")
	}
	if nullIntPtr(sql.NullInt64{}) != nil {
		t.Fatal("invalid int must stay nil")
	}
	// Values at or past the shared table fall back to a private allocation.
	big := sharedIntMax + 1
	p1 := nullIntPtr(sql.NullInt64{Int64: int64(big), Valid: true})
	p2 := nullIntPtr(sql.NullInt64{Int64: int64(big), Valid: true})
	if p1 == p2 {
		t.Fatal("values past the pool cap must allocate privately")
	}

	fa := nullFloatPtr(sql.NullFloat64{Float64: 1.5, Valid: true})
	fb := nullFloatPtr(sql.NullFloat64{Float64: 1.5, Valid: true})
	if fa != fb {
		t.Fatal("equal float values must share one pooled pointer")
	}
	negZero := nullFloatPtr(sql.NullFloat64{Float64: math.Copysign(0, -1), Valid: true})
	posZero := nullFloatPtr(sql.NullFloat64{Float64: 0, Valid: true})
	if negZero == posZero || math.Signbit(*negZero) != true || math.Signbit(*posZero) != false {
		t.Fatal("-0.0 and +0.0 must stay distinct and preserve their sign")
	}
	nan := nullFloatPtr(sql.NullFloat64{Float64: math.NaN(), Valid: true})
	nan2 := nullFloatPtr(sql.NullFloat64{Float64: math.NaN(), Valid: true})
	if nan == nan2 {
		t.Fatal("NaN must never be pooled")
	}

	empty := nullStrPtr(sql.NullString{String: "", Valid: true})
	empty2 := nullStrPtr(sql.NullString{String: "", Valid: true})
	if empty == nil || empty != empty2 || *empty != "" {
		t.Fatal(`scope "" must stay non-nil, shared and empty`)
	}
	if nullStrPtr(sql.NullString{}) != nil {
		t.Fatal("invalid scope must stay nil (NULL)")
	}
	scoped := nullStrPtr(sql.NullString{String: "region-a", Valid: true})
	if scoped == nil || scoped == empty || *scoped != "region-a" {
		t.Fatal("scope value must be shared and distinct from empty")
	}
	if nullStrPtr(sql.NullString{String: "region-a", Valid: true}) != scoped {
		t.Fatal("equal scope values must share one pooled pointer")
	}
}

func TestScopeNamePoolBoundsOwnedBytes(t *testing.T) {
	pool := scopeNamePool{values: make(map[string]*string)}
	first := strings.Repeat("x", sharedScopeBytes)
	if pool.share(first) != pool.share(first) {
		t.Fatal("pooled value lost identity at the byte limit")
	}
	if a, b := pool.share("other"), pool.share("other"); a == b || *a != "other" || *b != "other" {
		t.Fatal("value beyond byte limit should stay exact without retention")
	}
	if pool.bytes != sharedScopeBytes || len(pool.values) != 1 {
		t.Fatalf("unexpected retained scope budget: %d bytes, %d values", pool.bytes, len(pool.values))
	}
}
