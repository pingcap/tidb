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

// Package geomrel provides OGC-correct geometry relational predicates for the
// spatial-index POC. It uses github.com/peterstace/simplefeatures (a pure-Go
// implementation of the OpenGIS Simple Feature Access specification — the same
// spec GEOS/JTS/PostGIS implement), so boundary/DE-9IM semantics match MySQL
// without a cgo/libgeos dependency.
//
// Geometry inputs are EWKB (the POC's storage form: a 4-byte little-endian SRID
// prefix followed by standard WKB); the SRID prefix is stripped before decoding.
package geomrel

import (
	"encoding/binary"

	"github.com/peterstace/simplefeatures/geom"
	"github.com/pingcap/errors"
)

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
	Covers
	CoveredBy
)

// decodeEWKB returns the SRID (the 4-byte little-endian prefix) and the parsed WKB.
func decodeEWKB(ewkb string) (uint32, geom.Geometry, error) {
	if len(ewkb) < 4 {
		return 0, geom.Geometry{}, errors.New("geomrel: invalid geometry value: too short")
	}
	srid := binary.LittleEndian.Uint32([]byte(ewkb[:4]))
	g, err := geom.UnmarshalWKB([]byte(ewkb[4:]))
	if err != nil {
		return 0, geom.Geometry{}, errors.Trace(err)
	}
	return srid, g, nil
}

// Relate evaluates a binary relational predicate between two EWKB geometries.
func Relate(pred Predicate, ewkb1, ewkb2 string) (bool, error) {
	srid, g1, err := decodeEWKB(ewkb1)
	if err != nil {
		return false, err
	}
	_, g2, err := decodeEWKB(ewkb2)
	if err != nil {
		return false, err
	}
	// SRID-4326 region predicates use a geodesic (S2) evaluator for the
	// point-in-polygon case, so polygon edges are great-circle arcs (matching MySQL);
	// other operands/predicates use the planar simplefeatures evaluator below.
	if srid == srid4326 {
		if res, handled := geodesic4326PointInPolygon(pred, g1, g2); handled {
			return res, nil
		}
	}
	switch pred {
	case Within:
		return geom.Within(g1, g2)
	case Contains:
		return geom.Contains(g1, g2)
	case Intersects:
		return geom.Intersects(g1, g2), nil
	case Equals:
		return geom.Equals(g1, g2)
	case Disjoint:
		return geom.Disjoint(g1, g2)
	case Touches:
		return geom.Touches(g1, g2)
	case Crosses:
		return geom.Crosses(g1, g2)
	case Overlaps:
		return geom.Overlaps(g1, g2)
	case Covers:
		return geom.Covers(g1, g2)
	case CoveredBy:
		return geom.CoveredBy(g1, g2)
	default:
		return false, errors.Errorf("geomrel: unknown predicate %d", pred)
	}
}
