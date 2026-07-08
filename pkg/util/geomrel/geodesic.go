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

package geomrel

import (
	"github.com/golang/geo/s2"
	"github.com/peterstace/simplefeatures/geom"
)

// srid4326 is the WGS 84 geographic SRID.
const srid4326 = 4326

// geodesic4326PointInPolygon evaluates a SRID-4326 region predicate where one operand
// is a POINT and the other a POLYGON, using S2 so the polygon edges are geodesics
// (great-circle arcs) — matching MySQL's geographic semantics — rather than the planar
// lat/long segments the simplefeatures evaluator would use. This is where 4326 results
// diverge from MySQL (large regions, near the poles / the antimeridian).
//
// Returns (result, handled); handled=false means the operands are not a point/polygon
// pair, so the caller falls back to the planar evaluator. 4326 axis order is
// (latitude, longitude): the first coordinate is the latitude.
//
// Scope (POC): point-in-polygon for the region predicates. The interior-vs-boundary
// distinction is collapsed to S2's ContainsPoint (an exact-on-edge point is a
// measure-zero case that may differ from MySQL); polygon/polygon and the
// Equals/Touches/Crosses/Overlaps family stay on the planar evaluator.
func geodesic4326PointInPolygon(pred Predicate, g1, g2 geom.Geometry) (result, handled bool) {
	pt, poly, pointIsG1, ok := pointPolygonPair(g1, g2)
	if !ok {
		return false, false
	}
	s2poly, ok := s2PolygonFromGeom(poly)
	if !ok {
		return false, false
	}
	p, ok := pt.AsPoint()
	if !ok {
		return false, false
	}
	xy, ok := p.XY()
	if !ok {
		return false, false
	}
	contains := s2poly.ContainsPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(xy.X, xy.Y)))
	switch pred {
	case Within, CoveredBy:
		// g1 within / covered-by g2: only when the point is g1 (a polygon is never
		// within a point).
		return pointIsG1 && contains, true
	case Contains, Covers:
		// g1 contains / covers g2: only when the polygon is g1.
		return !pointIsG1 && contains, true
	case Intersects:
		return contains, true
	case Disjoint:
		return !contains, true
	default:
		return false, false // Equals/Touches/Crosses/Overlaps → planar fallback
	}
}

// pointPolygonPair returns the point and polygon of a (point, polygon) operand pair,
// and whether the point is the first operand.
func pointPolygonPair(g1, g2 geom.Geometry) (pt, poly geom.Geometry, pointIsG1, ok bool) {
	switch {
	case g1.IsPoint() && g2.IsPolygon():
		return g1, g2, true, true
	case g1.IsPolygon() && g2.IsPoint():
		return g2, g1, false, true
	default:
		return geom.Geometry{}, geom.Geometry{}, false, false
	}
}

// s2PolygonFromGeom builds an S2 polygon from a simplefeatures polygon, treating each
// vertex as (latitude=x, longitude=y). Rings are normalized and PolygonFromLoops
// resolves shell/hole nesting.
func s2PolygonFromGeom(g geom.Geometry) (*s2.Polygon, bool) {
	poly, ok := g.AsPolygon()
	if !ok {
		return nil, false
	}
	rings := []geom.LineString{poly.ExteriorRing()}
	for i := range poly.NumInteriorRings() {
		rings = append(rings, poly.InteriorRingN(i))
	}
	loops := make([]*s2.Loop, 0, len(rings))
	for _, ring := range rings {
		seq := ring.Coordinates()
		n := seq.Length()
		if n < 4 { // a ring needs ≥3 distinct vertices plus the repeated closing one
			return nil, false
		}
		// Drop the repeated closing vertex; S2 loops are implicitly closed.
		pts := make([]s2.Point, 0, n-1)
		for i := range n - 1 {
			xy := seq.GetXY(i)
			pts = append(pts, s2.PointFromLatLng(s2.LatLngFromDegrees(xy.X, xy.Y)))
		}
		loop := s2.LoopFromPoints(pts)
		loop.Normalize()
		loops = append(loops, loop)
	}
	return s2.PolygonFromLoops(loops), true
}
