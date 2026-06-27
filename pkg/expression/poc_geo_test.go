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

package expression_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestPOCGeoFunctions exercises the geometry storage + ST_ builtins end to end.
func TestPOCGeoFunctions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// WKT round-trip.
	tk.MustQuery("SELECT ST_AsText(ST_GeomFromText('POINT(1 2)'))").Check(testkit.Rows("POINT(1 2)"))
	tk.MustQuery("SELECT ST_SRID(ST_GeomFromText('POINT(1 2)', 4326))").Check(testkit.Rows("4326"))
	tk.MustQuery("SELECT ST_X(ST_GeomFromText('POINT(3 4)')), ST_Y(ST_GeomFromText('POINT(3 4)'))").Check(testkit.Rows("3 4"))

	// Planar distance: (0,0) to (3,4) = 5.
	tk.MustQuery("SELECT ST_Distance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'))").Check(testkit.Rows("5"))

	// Point in polygon (GEOS / OGC semantics, matching MySQL).
	box := "ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))')"
	tk.MustQuery("SELECT ST_Contains(" + box + ", ST_GeomFromText('POINT(5 5)'))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_Contains(" + box + ", ST_GeomFromText('POINT(15 5)'))").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT ST_Within(ST_GeomFromText('POINT(5 5)'), " + box + ")").Check(testkit.Rows("1"))
	// Boundary points are NOT within / contained (OGC boundary-not-interior).
	// Both corners agree, fixing the earlier ray-cast inconsistency.
	tk.MustQuery("SELECT ST_Within(ST_GeomFromText('POINT(0 0)'), " + box + ")").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT ST_Within(ST_GeomFromText('POINT(10 10)'), " + box + ")").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT ST_Contains(" + box + ", ST_GeomFromText('POINT(0 0)'))").Check(testkit.Rows("0"))

	// Other relational predicates via GEOS.
	tk.MustQuery("SELECT ST_Intersects(" + box + ", ST_GeomFromText('POINT(0 0)'))").Check(testkit.Rows("1")) // boundary touch
	tk.MustQuery("SELECT ST_Intersects(" + box + ", ST_GeomFromText('POINT(15 5)'))").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT ST_Disjoint(" + box + ", ST_GeomFromText('POINT(15 5)'))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_Equals(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(1 1)'))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_Equals(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(2 2)'))").Check(testkit.Rows("0"))

	// Table storage round-trip: store geometry, read back, query by distance.
	tk.MustExec("CREATE TABLE locs (id int primary key, p POINT NOT NULL SRID 0)")
	tk.MustExec("INSERT INTO locs VALUES (1, ST_GeomFromText('POINT(0 0)', 0)), (2, ST_GeomFromText('POINT(3 4)', 0)), (3, ST_GeomFromText('POINT(10 10)', 0))")
	tk.MustQuery("SELECT id, ST_AsText(p) FROM locs ORDER BY id").Check(testkit.Rows(
		"1 POINT(0 0)", "2 POINT(3 4)", "3 POINT(10 10)"))
	// Within radius 6 of origin: ids 1 and 2.
	tk.MustQuery("SELECT id FROM locs WHERE ST_Distance(p, ST_GeomFromText('POINT(0 0)', 0)) <= 6 ORDER BY id").Check(testkit.Rows("1", "2"))

	// Regression: INSERT ... SELECT of a geometry column must preserve the value.
	// chunk.Row.GetDatum had no TypeGeometry case, so set-based inserts read the
	// geometry as a NULL datum and failed the NOT NULL column.
	tk.MustExec("CREATE TABLE locs2 (id int primary key, p POINT NOT NULL SRID 0)")
	tk.MustExec("INSERT INTO locs2 SELECT id, p FROM locs")
	tk.MustQuery("SELECT id, ST_AsText(p) FROM locs2 ORDER BY id").Check(testkit.Rows(
		"1 POINT(0 0)", "2 POINT(3 4)", "3 POINT(10 10)"))

	// Regression: set/aggregate operations over a geometry column must not hit a
	// missing TypeGeometry type switch (UNION previously asserted in the cast-flen
	// setup; GROUP BY / DISTINCT exercise the hash path).
	tk.MustQuery("SELECT count(*) FROM (SELECT p FROM locs UNION SELECT p FROM locs2) t").Check(testkit.Rows("3"))
	tk.MustQuery("SELECT count(*) FROM (SELECT p FROM locs GROUP BY p) t").Check(testkit.Rows("3"))
	tk.MustQuery("SELECT count(DISTINCT p) FROM locs").Check(testkit.Rows("3"))

	// Accessors: ST_GeometryType, ST_Envelope, and the WKB I/O round-trip.
	tk.MustQuery("SELECT ST_GeometryType(ST_GeomFromText('POINT(1 2)'))").Check(testkit.Rows("POINT"))
	tk.MustQuery("SELECT ST_GeometryType(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))").Check(testkit.Rows("POLYGON"))
	// ST_Envelope matches MySQL: CCW ring from the min corner, and degenerate MBRs
	// collapse to a POINT (zero area) or LINESTRING (zero width/height).
	tk.MustQuery("SELECT ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 0,2 3)')))").Check(testkit.Rows("POLYGON((0 0,2 0,2 3,0 3,0 0))"))
	tk.MustQuery("SELECT ST_AsText(ST_Envelope(ST_GeomFromText('POINT(1 2)')))").Check(testkit.Rows("POINT(1 2)"))
	tk.MustQuery("SELECT ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 0,2 0)')))").Check(testkit.Rows("LINESTRING(0 0,2 0)"))
	// ST_Longitude/ST_Latitude on a geographic (4326) point (long=coord1, lat=coord0).
	tk.MustQuery("SELECT ST_Longitude(ST_GeomFromText('POINT(1 2)',4326)), ST_Latitude(ST_GeomFromText('POINT(1 2)',4326))").Check(testkit.Rows("2 1"))
	// ST_Crosses is dimension-gated like MySQL: NULL unless dim(g1) < dim(g2) or
	// both are lines (a symmetric library would wrongly return a boolean here).
	tk.MustQuery("SELECT ST_Crosses(ST_GeomFromText('LINESTRING(8 3,8 12)'), ST_GeomFromText('POLYGON((2 7,12 7,12 9,2 9,2 7))'))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_Crosses(ST_GeomFromText('POLYGON((2 7,12 7,12 9,2 9,2 7))'), ST_GeomFromText('LINESTRING(8 3,8 12)')) IS NULL").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_Crosses(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), ST_GeomFromText('POLYGON((2 2,3 2,3 3,2 3,2 2))')) IS NULL").Check(testkit.Rows("1"))
	// ST_GeomFromWKB(ST_AsBinary(g)) round-trips; ST_AsWKB is an alias of ST_AsBinary.
	tk.MustQuery("SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT(5 6)'))))").Check(testkit.Rows("POINT(5 6)"))
	tk.MustQuery("SELECT ST_AsWKB(ST_GeomFromText('POINT(0 0)')) = ST_AsBinary(ST_GeomFromText('POINT(0 0)'))").Check(testkit.Rows("1"))

	// ST_IsValid / ST_IsEmpty, and the ST_SRID(g, srid) setter form.
	tk.MustQuery("SELECT ST_IsValid(ST_GeomFromText('POINT(1 1)'))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_IsEmpty(ST_GeomFromText('POINT(1 1)')), ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))").Check(testkit.Rows("0 1"))
	tk.MustQuery("SELECT ST_SRID(ST_SRID(ST_GeomFromText('POINT(1 1)', 0), 4326))").Check(testkit.Rows("4326"))

	// ST_Covers/ST_CoveredBy predicates and GeoJSON I/O.
	tk.MustQuery("SELECT ST_Covers(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_GeomFromText('POINT(2 2)'))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_CoveredBy(ST_GeomFromText('POINT(2 2)'), ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'))").Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(1 2)'))`).Check(testkit.Rows(`{"type":"Point","coordinates":[1,2]}`))
	// GeoJSON parses to a 4326 geometry (MySQL default) and round-trips.
	tk.MustQuery(`SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[3,4]}'))`).Check(testkit.Rows("POINT(3 4)"))

	// Measurement/derived: ST_Area, ST_Length, ST_Dimension, ST_Centroid.
	tk.MustQuery("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,3 0,3 4,0 4,0 0))'))").Check(testkit.Rows("12"))
	tk.MustQuery("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0,3 4)'))").Check(testkit.Rows("5"))
	tk.MustQuery("SELECT ST_Dimension(ST_GeomFromText('POINT(1 1)')), ST_Dimension(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))").Check(testkit.Rows("0 2"))
	tk.MustQuery("SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))')))").Check(testkit.Rows("POINT(2 2)"))

	// Component accessors (NULL when the argument is the wrong geometry type).
	tk.MustQuery("SELECT ST_AsText(ST_StartPoint(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'))), ST_AsText(ST_EndPoint(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)')))").Check(testkit.Rows("POINT(0 0) POINT(2 2)"))
	tk.MustQuery("SELECT ST_StartPoint(ST_GeomFromText('POINT(1 1)')) IS NULL").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_AsText(ST_ExteriorRing(ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0),(1 1,2 1,2 2,1 2,1 1))')))").Check(testkit.Rows("LINESTRING(0 0,3 0,3 3,0 3,0 0)"))
	tk.MustQuery("SELECT ST_NumInteriorRings(ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0),(1 1,2 1,2 2,1 2,1 1))'))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_NumPoints(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)')), ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), 2))").Check(testkit.Rows("3 POINT(1 1)"))
	tk.MustQuery("SELECT ST_PointN(ST_GeomFromText('LINESTRING(0 0,1 1)'), 5) IS NULL").Check(testkit.Rows("1"))

	// Geometry constructors (MySQL): POINT/LineString/Polygon build geometries
	// (SRID 0) and compose, so e.g. ST_Within with a constructed polygon works.
	tk.MustQuery("SELECT ST_AsText(POINT(1,2))").Check(testkit.Rows("POINT(1 2)"))
	tk.MustQuery("SELECT ST_AsText(LineString(POINT(0,0),POINT(1,1),POINT(2,0)))").Check(testkit.Rows("LINESTRING(0 0,1 1,2 0)"))
	tk.MustQuery("SELECT ST_AsText(Polygon(LineString(POINT(0,0),POINT(4,0),POINT(4,4),POINT(0,4),POINT(0,0))))").Check(testkit.Rows("POLYGON((0 0,4 0,4 4,0 4,0 0))"))
	tk.MustQuery("SELECT ST_Within(POINT(2,2), Polygon(LineString(POINT(0,0),POINT(4,0),POINT(4,4),POINT(0,4),POINT(0,0))))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT ST_Within(POINT(9,9), Polygon(LineString(POINT(0,0),POINT(4,0),POINT(4,4),POINT(0,4),POINT(0,0))))").Check(testkit.Rows("0"))
}

// TestPOCSpatialKey checks tidb_spatial_key encodes points to ordered keys.
func TestPOCSpatialKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Same leaf cell -> equal key; key length is 8 bytes.
	tk.MustQuery("SELECT length(tidb_spatial_key(ST_GeomFromText('POINT(0 0)', 0)))").Check(testkit.Rows("8"))
	// Distinct far-apart points -> distinct keys; ordering is well-defined.
	tk.MustQuery("SELECT hex(tidb_spatial_key(ST_GeomFromText('POINT(0 0)',0))) = hex(tidb_spatial_key(ST_GeomFromText('POINT(0 0)',0)))").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT hex(tidb_spatial_key(ST_GeomFromText('POINT(0 0)',0))) <> hex(tidb_spatial_key(ST_GeomFromText('POINT(1000000 1000000)',0)))").Check(testkit.Rows("1"))
}

// TestPOCSphereSRIDCheck verifies ST_Distance_Sphere rejects non-4326 SRIDs
// (evaluated per row so constant folding does not defer the error).
func TestPOCSphereSRIDCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE pts0 (id int primary key, p POINT NOT NULL SRID 0)")
	tk.MustExec("INSERT INTO pts0 VALUES (1, ST_GeomFromText('POINT(0 0)',0))")
	err := tk.QueryToErr("SELECT ST_Distance_Sphere(p, ST_GeomFromText('POINT(1 1)',0)) FROM pts0")
	require.ErrorContains(t, err, "only SRID 4326 is supported")
}
