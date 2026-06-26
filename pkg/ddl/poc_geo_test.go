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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// TestPOCGeoCreateTable is a throwaway POC check that a table with geometry
// columns (and a per-column SRID) can be created and inspected.
func TestPOCGeoCreateTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	_, err := tk.Exec("CREATE TABLE locs (id int primary key, g GEOMETRY, p POINT NOT NULL SRID 4326, poly POLYGON)")
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	res := tk.MustQuery("SHOW CREATE TABLE locs").Rows()
	t.Logf("SHOW CREATE TABLE:\n%s", res[0][1])

	// Raw EWKB-ish bytes round-trip as a binary string for now (no value codec yet).
	tk.MustExec("CREATE TABLE pts (id int primary key, p POINT SRID 0)")
	tk.MustExec("INSERT INTO pts VALUES (1, x'00000000010100000000000000000000000000000000000000')")
	tk.MustQuery("SELECT id, hex(p) FROM pts").Check(
		testkit.Rows("1 00000000010100000000000000000000000000000000000000"))
}

// TestPOCCreateSpatialIndex verifies CREATE SPATIAL INDEX builds a hidden
// generated column over tidb_spatial_key and writes one index entry per row.
func TestPOCCreateSpatialIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("CREATE TABLE locs (id int primary key, p POINT NOT NULL SRID 0)")
	tk.MustExec("INSERT INTO locs VALUES " +
		"(1, ST_GeomFromText('POINT(0 0)',0)), (2, ST_GeomFromText('POINT(3 4)',0)), (3, ST_GeomFromText('POINT(10 10)',0))")

	tk.MustExec("CREATE SPATIAL INDEX sidx ON locs (p)")

	// The index exists and admin check passes (entries consistent with rows).
	tk.MustExec("ADMIN CHECK TABLE locs")
	tk.MustExec("ADMIN CHECK INDEX locs sidx")

	// A later insert keeps the index consistent.
	tk.MustExec("INSERT INTO locs VALUES (4, ST_GeomFromText('POINT(1 1)',0))")
	tk.MustExec("ADMIN CHECK TABLE locs")

	// Rejections: nullable / non-geometry / non-zero SRID for general geometry.
	tk.MustExec("CREATE TABLE bad1 (id int primary key, p POINT SRID 0)") // nullable
	tk.MustGetErrMsg("CREATE SPATIAL INDEX b ON bad1 (p)", "[ddl:8200]SPATIAL index requires a NOT NULL column")
	// A general (non-point) geometry is now allowed (multi-valued index); a
	// general geometry with a non-zero SRID is rejected (POC scope).
	tk.MustExec("CREATE TABLE good2 (id int primary key, g GEOMETRY NOT NULL)")
	tk.MustExec("CREATE SPATIAL INDEX b ON good2 (g)")
	tk.MustExec("CREATE TABLE bad2 (id int primary key, g GEOMETRY NOT NULL SRID 4326)")
	tk.MustGetErrMsg("CREATE SPATIAL INDEX b ON bad2 (g)", "[ddl:8200]SPATIAL index on a general geometry supports SRID 0 in the POC, got SRID 4326")
	// SRID 4326 is supported for POINT; an unsupported SRID (e.g. Web Mercator 3857) is rejected.
	tk.MustExec("CREATE TABLE bad3 (id int primary key, p POINT NOT NULL SRID 3857)")
	tk.MustGetErrMsg("CREATE SPATIAL INDEX b ON bad3 (p)", "[ddl:8200]SPATIAL index on a POINT supports SRID 0 or 4326 in the POC, got SRID 3857")
	// A 4326 spatial index is accepted.
	tk.MustExec("CREATE TABLE geo4326 (id int primary key, p POINT NOT NULL SRID 4326)")
	tk.MustExec("CREATE SPATIAL INDEX g4326 ON geo4326 (p)")
}
