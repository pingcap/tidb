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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// seedSpatialTable creates a grid of points and returns the testkit.
func seedSpatialTable(t *testing.T) (*testkit.TestKit, func()) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE locs (id int primary key, p POINT NOT NULL SRID 0)")
	// 31x31 grid of points from (0,0) to (300,300), step 10.
	var vals []string
	id := 0
	for x := 0; x <= 300; x += 10 {
		for y := 0; y <= 300; y += 10 {
			id++
			vals = append(vals, fmt.Sprintf("(%d, ST_GeomFromText('POINT(%d %d)',0))", id, x, y))
		}
	}
	tk.MustExec("INSERT INTO locs VALUES " + strings.Join(vals, ","))
	return tk, func() {}
}

// TestPOCSpatialIndexEquivalence is the Milestone 3 acceptance test: a distance
// query and a containment query return identical rows with and without the
// spatial index, and EXPLAIN shows the index scan plus a refine filter.
func TestPOCSpatialIndexEquivalence(t *testing.T) {
	tk, cleanup := seedSpatialTable(t)
	defer cleanup()

	const distPred = "ST_Distance(p, ST_GeomFromText('POINT(150 150)',0)) <= 25"
	const containPred = "ST_Within(p, ST_GeomFromText('POLYGON((100 100,100 130,130 130,130 100,100 100))',0))"
	const distQuery = "SELECT id FROM locs WHERE " + distPred + " ORDER BY id"
	const containQuery = "SELECT id FROM locs WHERE " + containPred + " ORDER BY id"

	// Baseline: no index (full scan).
	wantDist := tk.MustQuery(distQuery).Rows()
	wantContain := tk.MustQuery(containQuery).Rows()
	require.NotEmpty(t, wantDist)
	require.NotEmpty(t, wantContain)

	// Create the spatial index and ANALYZE so the optimizer has real statistics.
	tk.MustExec("CREATE SPATIAL INDEX sidx ON locs (p)")
	tk.MustExec("ANALYZE TABLE locs")

	// Same results with the index present — and these selective queries auto-select
	// the index with no FORCE INDEX hint (the spatial-stats ANALYZE fix).
	tk.MustQuery(distQuery).Check(wantDist)
	tk.MustQuery(containQuery).Check(wantContain)

	// EXPLAIN shows an index range scan on the spatial index plus a refine
	// Selection carrying the original ST_Distance predicate.
	explain := tk.MustQuery("EXPLAIN " + distQuery).Rows()
	var planText strings.Builder
	for _, row := range explain {
		for _, c := range row {
			planText.WriteString(fmt.Sprintf("%v ", c))
		}
		planText.WriteString("\n")
	}
	plan := planText.String()
	t.Logf("EXPLAIN:\n%s", plan)
	require.Contains(t, plan, "sidx", "expected the spatial index sidx in the plan")
	require.Contains(t, plan, "IndexRangeScan", "expected an index range scan, not a full scan")
	require.Contains(t, strings.ToLower(plan), "st_distance", "expected ST_Distance retained as a refine filter")
}

// TestPOCSpatialDMLMaintenance verifies UPDATE/DELETE re-cover the spatial index:
// the hidden generated columns (cell-key + x/y for a point; the covering-cell
// array + bbox for a general-geometry MVI) are recomputed on modification, so the
// index stays consistent with the table (ADMIN CHECK) and queries find the moved
// geometry at its new location, not the old one.
func TestPOCSpatialDMLMaintenance(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Point index.
	tk.MustExec("CREATE TABLE pt (id int primary key, p POINT NOT NULL SRID 0)")
	tk.MustExec("INSERT INTO pt VALUES (1,ST_GeomFromText('POINT(1 1)',0)),(2,ST_GeomFromText('POINT(2 2)',0)),(3,ST_GeomFromText('POINT(3 3)',0))")
	tk.MustExec("CREATE SPATIAL INDEX si ON pt (p) COMMENT 'spatial:12,0,0,64,64'")
	tk.MustExec("UPDATE pt SET p=ST_GeomFromText('POINT(50 50)',0) WHERE id=1")
	tk.MustExec("DELETE FROM pt WHERE id=2")
	tk.MustExec("ADMIN CHECK TABLE pt")
	tk.MustExec("ADMIN CHECK INDEX pt si")
	tk.MustQuery("SELECT id FROM pt FORCE INDEX (si) WHERE ST_Distance(p, ST_GeomFromText('POINT(50 50)',0)) <= 1").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT id FROM pt FORCE INDEX (si) WHERE ST_Distance(p, ST_GeomFromText('POINT(1 1)',0)) <= 1").Check(testkit.Rows())

	// General-geometry multi-valued index.
	tk.MustExec("CREATE TABLE pg (id int primary key, g GEOMETRY NOT NULL SRID 0)")
	tk.MustExec("INSERT INTO pg VALUES (1,ST_GeomFromText('POLYGON((1 1,2 1,2 2,1 2,1 1))',0)),(2,ST_GeomFromText('POLYGON((5 5,6 5,6 6,5 6,5 5))',0))")
	tk.MustExec("CREATE SPATIAL INDEX gi ON pg (g) COMMENT 'spatial:3,0,0,64,64'")
	tk.MustExec("UPDATE pg SET g=ST_GeomFromText('POLYGON((40 40,42 40,42 42,40 42,40 40))',0) WHERE id=1")
	tk.MustExec("DELETE FROM pg WHERE id=2")
	tk.MustExec("ADMIN CHECK TABLE pg")
	tk.MustExec("ADMIN CHECK INDEX pg gi")
	tk.MustQuery("SELECT id FROM pg FORCE INDEX (gi) WHERE ST_Intersects(g, ST_GeomFromText('POLYGON((39 39,43 39,43 43,39 43,39 39))',0))").Check(testkit.Rows("1"))
}

// TestPOCSpatialAutoSelect proves the spatial index is selected WITHOUT a FORCE
// INDEX hint once ANALYZE has built real statistics (see the ANALYZE-stats fix):
// the cost model picks the index for a selective query and falls back to a full
// scan when most of the table matches.
func TestPOCSpatialAutoSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE locs (id int primary key, p POINT NOT NULL SRID 0)")
	id := 0
	for i := 0; i < 100; i++ {
		var sb strings.Builder
		sb.WriteString("INSERT INTO locs VALUES ")
		for j := 0; j < 100; j++ {
			if j > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "(%d, ST_GeomFromText('POINT(%d %d)',0))", id, i, j)
			id++
		}
		tk.MustExec(sb.String())
	}
	tk.MustExec("CREATE SPATIAL INDEX sidx ON locs (p) COMMENT 'spatial:12,0,0,100,100'")
	tk.MustExec("ANALYZE TABLE locs")

	usesIndex := func(radius int) bool {
		q := fmt.Sprintf("SELECT id FROM locs WHERE ST_Distance(p, ST_GeomFromText('POINT(50 50)',0)) <= %d", radius)
		var sb strings.Builder
		for _, r := range tk.MustQuery("EXPLAIN " + q).Rows() {
			sb.WriteString(fmt.Sprintf("%v ", r[0]))
		}
		return strings.Contains(sb.String(), "IndexRangeScan")
	}
	require.True(t, usesIndex(5), "selective spatial query (no hint) should auto-select the index")
	require.False(t, usesIndex(70), "unselective spatial query should fall back to a full scan")
}
