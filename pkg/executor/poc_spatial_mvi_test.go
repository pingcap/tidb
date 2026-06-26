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

package executor_test

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestPOCSpatialMVIGeneralGeometry exercises the general-geometry (non-point)
// spatial index: CREATE SPATIAL INDEX on a GEOMETRY column builds a multi-valued
// index over the covering-cell set (CAST(tidb_spatial_keys(g,...) AS CHAR ARRAY)),
// one index entry per covering cell. An ST_Intersects-style query expressed with
// JSON_OVERLAPS of the query geometry's cells is served by an IndexMerge over the
// matching cells, then refined to exact results with ST_Intersects.
func TestPOCSpatialMVIGeneralGeometry(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE shapes (id int primary key, g GEOMETRY NOT NULL)")
	tk.MustExec("CREATE SPATIAL INDEX gidx ON shapes (g) COMMENT 'spatial:6,0,0,64,64'")

	// Three well-separated shapes.
	tk.MustExec("INSERT INTO shapes VALUES " +
		"(1, ST_GeomFromText('POLYGON((1 1,9 1,9 9,1 9,1 1))',0))," + // lower-left block
		"(2, ST_GeomFromText('POLYGON((40 40,60 40,60 60,40 60,40 40))',0))," + // upper-right block
		"(3, ST_GeomFromText('LINESTRING(2 40,8 46)',0))") // upper-left segment
	tk.MustExec("ADMIN CHECK TABLE shapes")
	tk.MustExec("ADMIN CHECK INDEX shapes gidx")

	cells := func(wkt string) string {
		return "tidb_spatial_keys(ST_GeomFromText('" + wkt + "',0),6,0,0,64,64)"
	}
	// Query window overlapping only shape 1.
	const qfmt = "SELECT id FROM shapes %s WHERE " +
		"json_overlaps(tidb_spatial_keys(g,6,0,0,64,64), %s) AND " +
		"ST_Intersects(g, ST_GeomFromText('%s',0)) ORDER BY id"
	win1 := "POLYGON((3 3,5 3,5 5,3 5,3 3))"
	want1 := tk.MustQuery(fmt.Sprintf(qfmt, "", cells(win1), win1)).Rows()
	require.Equal(t, [][]any{{"1"}}, want1)

	// Same query forcing the MVI returns the same rows, and the plan is an
	// IndexMerge over the spatial index.
	forced := fmt.Sprintf(qfmt, "FORCE INDEX (gidx)", cells(win1), win1)
	tk.MustQuery(forced).Check(testkit.Rows("1"))
	plan := tk.MustQuery("EXPLAIN " + forced).Rows()
	var sb strings.Builder
	for _, r := range plan {
		sb.WriteString(fmt.Sprintf("%v ", r[0]))
	}
	require.Contains(t, sb.String(), "IndexMerge", "expected the MVI to be used via IndexMerge")

	// A window over shape 2's area selects only shape 2.
	win2 := "POLYGON((45 45,55 45,55 55,45 55,45 45))"
	tk.MustQuery(fmt.Sprintf(qfmt, "", cells(win2), win2)).Check(testkit.Rows("2"))
}

// TestPOCSpatialMVIAutoInjectAndBBox covers the automatic general-geometry path:
// a plain ST_Within(geom, const_poly) (no manual json_overlaps) is auto-rewritten
// by SpatialIndexResolver to a json_overlaps over the query's covering cells, so
// the MVI is used via IndexMerge; and the MBR bbox columns prune covering false
// positives during the partial index scans, before the table lookup.
func TestPOCSpatialMVIAutoInjectAndBBox(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE g (id int primary key, geom GEOMETRY NOT NULL SRID 0)")
	for i := 0; i < 60; i++ {
		for j := 0; j < 60; j++ {
			w := fmt.Sprintf("POLYGON((%d %d,%d %d,%d %d,%d %d,%d %d))", i, j, i+1, j, i+1, j+1, i, j+1, i, j)
			tk.MustExec(fmt.Sprintf("INSERT INTO g VALUES (%d, ST_GeomFromText('%s',0))", i*60+j, w))
		}
	}
	// Coarse cells (8-unit) so the covering overflows the query's bounding box and
	// the bbox filter has false positives to prune.
	tk.MustExec("CREATE SPATIAL INDEX sidx ON g (geom) COMMENT 'spatial:3,0,0,64,64'")

	const pred = "ST_Within(geom, ST_GeomFromText('POLYGON((10 10,13 10,13 13,10 13,10 10))',0))"
	want := tk.MustQuery("SELECT id FROM g IGNORE INDEX (sidx) WHERE " + pred + " ORDER BY id").Rows()
	require.NotEmpty(t, want)

	// Plain predicate (no FORCE, no manual json_overlaps) is auto-rewritten to use
	// the MVI, and returns the same rows as the full scan.
	forced := "SELECT id FROM g FORCE INDEX (sidx) WHERE " + pred + " ORDER BY id"
	tk.MustQuery(forced).Check(want)

	re := regexp.MustCompile(`^\s*(\d+)`)
	sum := func(query, opSubstr string) int {
		total := 0
		for _, r := range tk.MustQuery("EXPLAIN ANALYZE " + query).Rows() {
			if !strings.Contains(fmt.Sprintf("%v", r[0]), opSubstr) {
				continue
			}
			if m := re.FindStringSubmatch(fmt.Sprintf("%v", r[2])); m != nil {
				n, _ := strconv.Atoi(m[1])
				total += n
			}
		}
		return total
	}
	plan := tk.MustQuery("EXPLAIN " + forced).Rows()
	var sb strings.Builder
	for _, r := range plan {
		sb.WriteString(fmt.Sprintf("%v ", r[0]))
	}
	require.Contains(t, sb.String(), "IndexMerge", "ST_Within should auto-select the MVI via IndexMerge")

	rawCells := sum(forced, "IndexRangeScan") // candidates from the cell covering
	lookups := sum(forced, "TableRowIDScan")  // table lookups after the bbox filter
	require.Positive(t, rawCells)
	require.LessOrEqual(t, lookups, rawCells, "bbox filter must not increase lookups")
	require.Less(t, lookups, rawCells, "bbox filter should prune covering false positives before the lookup")
	t.Logf("MVI bbox pruning: %d covering candidates -> %d table lookups -> %d results", rawCells, lookups, len(want))
}
