// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package geos wraps github.com/twpayne/go-geos (libgeos bindings) to provide
// OGC-correct geometry relational predicates for the spatial-index POC. GEOS is
// the canonical engine MySQL's GIS layer also derives its semantics from, so
// using it keeps boundary/DE-9IM behaviour (ST_Within/ST_Contains/ST_Intersects
// /...) matching MySQL instead of the earlier hand-rolled ray-casting.
//
// CGO note: this package links against the system libgeos (libgeos_c). It is
// kept as a small leaf package so the CGO/system-library dependency is isolated
// and easy to gate; only callers that need the predicates pull it in.
//
// Geometry inputs are EWKB (the POC's storage form: a 4-byte little-endian SRID
// prefix followed by standard WKB); the SRID prefix is stripped before handing
// the WKB to GEOS.
package geos

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/twpayne/go-geos"
)

// contextPool hands out per-goroutine GEOS contexts. A geos.Context serializes
// its own operations with an internal mutex, but pooling avoids that contention
// and matches go-geos's "one context per goroutine" guidance.
var contextPool = sync.Pool{New: func() any { return geos.NewContext() }}

// Predicate identifies a binary spatial relational predicate.
type Predicate int

// Supported relational predicates (OGC DE-9IM family).
const (
	Within Predicate = iota
	Contains
	Intersects
	Equals
	Disjoint
	Touches
	Crosses
	Overlaps
)

// stripSRID removes the 4-byte little-endian SRID prefix from an EWKB value,
// returning the standard WKB GEOS understands.
func stripSRID(ewkb string) ([]byte, error) {
	if len(ewkb) < 4 {
		return nil, errors.New("geos: invalid geometry value: too short")
	}
	return []byte(ewkb[4:]), nil
}

// Relate evaluates a binary relational predicate between two EWKB geometries.
// go-geos panics when GEOS raises a topology exception (e.g. an invalid,
// self-intersecting polygon), so the panic is recovered and returned as an error
// rather than aborting the goroutine on user-controlled input.
func Relate(pred Predicate, ewkb1, ewkb2 string) (res bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = false, errors.Errorf("geos: %v", r)
		}
	}()
	return relate(pred, ewkb1, ewkb2)
}

func relate(pred Predicate, ewkb1, ewkb2 string) (bool, error) {
	wkb1, err := stripSRID(ewkb1)
	if err != nil {
		return false, err
	}
	wkb2, err := stripSRID(ewkb2)
	if err != nil {
		return false, err
	}
	ctx := contextPool.Get().(*geos.Context)
	defer contextPool.Put(ctx)

	g1, err := ctx.NewGeomFromWKB(wkb1)
	if err != nil {
		return false, errors.Trace(err)
	}
	g2, err := ctx.NewGeomFromWKB(wkb2)
	if err != nil {
		return false, errors.Trace(err)
	}

	switch pred {
	case Within:
		return g1.Within(g2), nil
	case Contains:
		return g1.Contains(g2), nil
	case Intersects:
		return g1.Intersects(g2), nil
	case Equals:
		return g1.Equals(g2), nil
	case Disjoint:
		return g1.Disjoint(g2), nil
	case Touches:
		return g1.Touches(g2), nil
	case Crosses:
		return g1.Crosses(g2), nil
	case Overlaps:
		return g1.Overlaps(g2), nil
	default:
		return false, errors.Errorf("geos: unknown predicate %d", pred)
	}
}
