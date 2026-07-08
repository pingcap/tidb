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
	"sync"
	"testing"

	"github.com/peterstace/simplefeatures/geom"
	"github.com/stretchr/testify/require"
)

// ewkb builds the POC EWKB form (<srid_le><wkb>) from a WKT literal.
func ewkb(t *testing.T, wktStr string) string {
	g, err := geom.UnmarshalWKT(wktStr)
	require.NoError(t, err)
	return string(append([]byte{0, 0, 0, 0}, g.AsBinary()...))
}

func TestRelateOGCSemantics(t *testing.T) {
	box := ewkb(t, "POLYGON((0 0,10 0,10 10,0 10,0 0))")

	cases := []struct {
		pred Predicate
		a, b string
		want bool
	}{
		// Interior point: within / contained.
		{Within, ewkb(t, "POINT(5 5)"), box, true},
		{Contains, box, ewkb(t, "POINT(5 5)"), true},
		// Boundary corners: NOT within / contained (OGC), and consistent.
		{Within, ewkb(t, "POINT(0 0)"), box, false},
		{Within, ewkb(t, "POINT(10 10)"), box, false},
		{Contains, box, ewkb(t, "POINT(0 0)"), false},
		// Exterior point.
		{Within, ewkb(t, "POINT(15 5)"), box, false},
		// Intersects: boundary touch is true, exterior is false.
		{Intersects, box, ewkb(t, "POINT(0 0)"), true},
		{Intersects, box, ewkb(t, "POINT(15 5)"), false},
		{Disjoint, box, ewkb(t, "POINT(15 5)"), true},
		{Equals, ewkb(t, "POINT(1 1)"), ewkb(t, "POINT(1 1)"), true},
		{Equals, ewkb(t, "POINT(1 1)"), ewkb(t, "POINT(2 2)"), false},
	}
	for _, c := range cases {
		got, err := Relate(c.pred, c.a, c.b)
		require.NoError(t, err)
		require.Equalf(t, c.want, got, "pred=%d", c.pred)
	}
}

// TestRelateConcurrent exercises the context pool from many goroutines.
func TestRelateConcurrent(t *testing.T) {
	box := ewkb(t, "POLYGON((0 0,10 0,10 10,0 10,0 0))")
	pt := ewkb(t, "POINT(5 5)")
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				got, err := Relate(Within, pt, box)
				require.NoError(t, err)
				require.True(t, got)
			}
		}()
	}
	wg.Wait()
}

// TestRelateInvalidGeometryNoPanic confirms an invalid (self-intersecting)
// polygon is handled by returning an error, not by panicking. simplefeatures
// validates by default, so the invalid input is rejected at WKB decode.
func TestRelateInvalidGeometryNoPanic(t *testing.T) {
	// Build the self-intersecting "bowtie" without validation, then encode it.
	g, err := geom.UnmarshalWKT("POLYGON((0 0,2 2,2 0,0 2,0 0))", geom.NoValidate{})
	require.NoError(t, err)
	bowtie := string(append([]byte{0, 0, 0, 0}, g.AsBinary()...))
	pt := ewkb(t, "POINT(1 1)")
	require.NotPanics(t, func() {
		_, err1 := Relate(Within, pt, bowtie)
		_, err2 := Relate(Overlaps, bowtie, bowtie)
		// The invalid geometry is rejected at decode with an error (not a panic).
		require.Error(t, err1)
		require.Error(t, err2)
	})
}

// ewkb4326 builds the POC EWKB form with the WGS 84 SRID prefix (4326 = 0x10E6).
func ewkb4326(t *testing.T, wktStr string) string {
	g, err := geom.UnmarshalWKT(wktStr)
	require.NoError(t, err)
	return string(append([]byte{0xE6, 0x10, 0, 0}, g.AsBinary()...))
}

// TestGeodesic4326PointInPolygon verifies the SRID-4326 geodesic (S2) refine for
// point-in-polygon. The big spherical triangle (lat lng) (0,0),(80,0),(0,80) has a
// great-circle hypotenuse that bows away from the origin, so (41,41) — which is
// planar-OUTSIDE (lat+lng=82) — is geodesically INSIDE (MySQL 9.7 ST_Within=1). A
// passing assertion here proves the geodesic path ran (the planar evaluator returns
// false). Values verified against MySQL 9.7.
func TestGeodesic4326PointInPolygon(t *testing.T) {
	poly := ewkb4326(t, "POLYGON((0 0,80 0,0 80,0 0))")
	within := func(wkt string) bool {
		res, err := Relate(Within, ewkb4326(t, wkt), poly)
		require.NoError(t, err)
		return res
	}
	require.True(t, within("POINT(41 41)"), "geodesically inside (MySQL=1) though planar-outside")
	require.True(t, within("POINT(20 20)"), "clearly inside")
	require.False(t, within("POINT(70 70)"), "clearly outside")
	require.False(t, within("POINT(-5 20)"), "negative latitude, outside")

	// ST_Contains(polygon, point): the polygon is the first operand.
	cres, err := Relate(Contains, poly, ewkb4326(t, "POINT(41 41)"))
	require.NoError(t, err)
	require.True(t, cres)
	// Disjoint is the negation of intersects.
	dres, err := Relate(Disjoint, ewkb4326(t, "POINT(70 70)"), poly)
	require.NoError(t, err)
	require.True(t, dres)
}
