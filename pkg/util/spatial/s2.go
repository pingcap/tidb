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

package spatial

import (
	"encoding/binary"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/pingcap/errors"
)

// SRID4326 is WGS 84 lat/long, covered with Google's S2 cell hierarchy. A point
// (x=longitude, y=latitude in degrees) maps to its S2 leaf cell; an S2 CellID is
// a Hilbert-ordered uint64, so its big-endian bytes are order-preserving like the
// planar Morton key, and both schemes share the 8-byte CellKey width.
const SRID4326 = 4326

// EarthRadiusMeters is MySQL's default sphere radius for ST_Distance_Sphere, used
// here so the index covering and the refine predicate agree.
const EarthRadiusMeters = 6370986.0

// s2CellKey encodes an S2 CellID as an order-preserving 8-byte big-endian key.
func s2CellKey(id s2.CellID) CellKey {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

// EncodePointS2 returns the CellKey for a WGS 84 point (the S2 leaf cell).
func EncodePointS2(lng, lat float64) CellKey {
	ll := s2.LatLngFromDegrees(lat, lng)
	return s2CellKey(s2.CellIDFromLatLng(ll))
}

// s2MaxCells caps the S2 covering size (and thus the planner's OR width), the
// spherical analogue of the planar targetCellsAcross cap.
const s2MaxCells = 16

func s2Ranges(covering s2.CellUnion) []CellKeyRange {
	ranges := make([]CellKeyRange, 0, len(covering))
	for _, id := range covering {
		// RangeMin/RangeMax give the leaf-cell CellID span under this cover cell,
		// which contains the leaf cell of any point inside it.
		ranges = append(ranges, CellKeyRange{
			Lo: s2CellKey(id.RangeMin()),
			Hi: s2CellKey(id.RangeMax()),
		})
	}
	return mergeRanges(ranges)
}

// CoverCapDegrees returns CellKey ranges covering the spherical cap of the given
// radius (metres) around a WGS 84 centre — the covering for an
// ST_Distance_Sphere(point, centre) <= radius query.
func CoverCapDegrees(lng, lat, radiusMeters float64) ([]CellKeyRange, error) {
	if radiusMeters < 0 {
		return nil, errors.New("spatial: negative radius")
	}
	center := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
	angle := s1.Angle(radiusMeters / EarthRadiusMeters)
	cap := s2.CapFromCenterAngle(center, angle)
	rc := &s2.RegionCoverer{MaxLevel: 30, MaxCells: s2MaxCells}
	return s2Ranges(rc.Covering(cap)), nil
}

// CoverLatLngRectDegrees returns CellKey ranges covering a WGS 84 lat/long
// rectangle — used for containment queries on SRID 4326.
func CoverLatLngRectDegrees(minLng, minLat, maxLng, maxLat float64) ([]CellKeyRange, error) {
	rect := s2.EmptyRect().
		AddPoint(s2.LatLngFromDegrees(minLat, minLng)).
		AddPoint(s2.LatLngFromDegrees(maxLat, maxLng))
	rc := &s2.RegionCoverer{MaxLevel: 30, MaxCells: s2MaxCells}
	return s2Ranges(rc.Covering(rect)), nil
}
