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
