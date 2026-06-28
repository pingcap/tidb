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
	"sync"
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

// TestPOCSpatialRefinePushdown verifies Layer B: the exact spatial refine
// predicate (ST_Within) is pushed to the coprocessor and evaluated there over the
// stored EWKB, returning the same rows as a root/full-scan evaluation. unistore
// reuses TiDB's Go evaluator via the pb round-trip (geomRelPbCode/getSignatureByPB).
func TestPOCSpatialRefinePushdown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE locs (id int primary key, p POINT NOT NULL SRID 0)")
	for i := 0; i < 20; i++ {
		for j := 0; j < 20; j++ {
			tk.MustExec(fmt.Sprintf("INSERT INTO locs VALUES (%d, ST_GeomFromText('POINT(%d %d)',0))", i*20+j, i, j))
		}
	}
	const pred = "ST_Within(p, ST_GeomFromText('POLYGON((5 5,5 10,10 10,10 5,5 5))',0))"
	want := tk.MustQuery("SELECT id FROM locs IGNORE INDEX (primary) WHERE " + pred + " ORDER BY id").Rows()
	require.NotEmpty(t, want)

	// copSelectionHas reports whether the plan pushes a Selection carrying needle
	// to a cop[tikv] task.
	copSelectionHas := func(query, needle string) bool {
		for _, r := range tk.MustQuery("EXPLAIN " + query).Rows() {
			op, task := fmt.Sprintf("%v", r[0]), fmt.Sprintf("%v", r[2])
			info := strings.ToLower(fmt.Sprintf("%v", r[4]))
			if strings.Contains(op, "Selection") && strings.Contains(task, "cop") && strings.Contains(info, needle) {
				return true
			}
		}
		return false
	}

	// Table scan: ST_Within is pushed to the coprocessor, results unchanged.
	tableQuery := "SELECT id FROM locs IGNORE INDEX (primary) WHERE " + pred + " ORDER BY id"
	require.True(t, copSelectionHas(tableQuery, "st_within"), "ST_Within should push to the coprocessor on a table scan")
	tk.MustQuery(tableQuery).Check(want)

	// Spatial index: the refine is pushed to the cop on the table (probe) side.
	tk.MustExec("CREATE SPATIAL INDEX sidx ON locs (p)")
	idxQuery := "SELECT id FROM locs FORCE INDEX (sidx) WHERE " + pred + " ORDER BY id"
	require.True(t, copSelectionHas(idxQuery, "st_within"), "ST_Within refine should push to the coprocessor under the index lookup")
	tk.MustQuery(idxQuery).Check(want)
}

// TestPOCSpatialCoversIndex verifies ST_Covers/ST_CoveredBy are index-eligible
// region predicates: Covers ⊇ Contains and CoveredBy ⊇ Within both imply a
// non-empty intersection, so the covering-cell prefilter is valid (no false
// negatives) and they use the spatial index like ST_Within/ST_Contains.
func TestPOCSpatialCoversIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE pt (id int primary key, p POINT NOT NULL SRID 0)")
	var b strings.Builder
	b.WriteString("INSERT INTO pt VALUES ")
	id := 0
	for x := 0; x < 10; x++ {
		for y := 0; y < 10; y++ {
			if id > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(%d, ST_GeomFromText('POINT(%d %d)',0))", id, x, y)
			id++
		}
	}
	tk.MustExec(b.String())
	tk.MustExec("CREATE SPATIAL INDEX si ON pt (p)")

	const pred = "ST_CoveredBy(p, ST_GeomFromText('POLYGON((2 2,2 5,5 5,5 2,2 2))',0))"
	var plan strings.Builder
	for _, r := range tk.MustQuery("EXPLAIN SELECT id FROM pt FORCE INDEX(si) WHERE " + pred).Rows() {
		plan.WriteString(fmt.Sprintf("%v ", r[0]))
	}
	require.Contains(t, plan.String(), "IndexRangeScan", "ST_CoveredBy should use the spatial index")

	// CoveredBy includes the boundary: the closed [2,5]x[2,5] box = 16 grid points.
	want := tk.MustQuery("SELECT id FROM pt IGNORE INDEX(si) WHERE " + pred + " ORDER BY id").Rows()
	require.Len(t, want, 16)
	tk.MustQuery("SELECT id FROM pt FORCE INDEX(si) WHERE " + pred + " ORDER BY id").Check(want)
}

// TestPOCSpatialCostBasedSelection verifies the optimizer chooses the spatial index
// by cost, without a FORCE INDEX hint: a selective region picks the index range scan,
// while a region covering essentially all rows falls back to a full table scan
// (where the index + table lookups would be more expensive). Both must stay correct.
func TestPOCSpatialCostBasedSelection(t *testing.T) {
	tk, cleanup := seedSpatialTable(t)
	defer cleanup()
	tk.MustExec("CREATE SPATIAL INDEX sidx ON locs (p)")
	tk.MustExec("ANALYZE TABLE locs")

	planOf := func(q string) string {
		var b strings.Builder
		for _, r := range tk.MustQuery("EXPLAIN " + q).Rows() {
			b.WriteString(fmt.Sprintf("%v ", r[0]))
		}
		return b.String()
	}
	baseline := func(q string) [][]any {
		return tk.MustQuery(strings.Replace(q, "FROM locs", "FROM locs IGNORE INDEX(sidx)", 1)).Rows()
	}

	// Low selectivity (a small 20x20 window, ~9 of 961 rows): auto-pick the index.
	const sel = "SELECT id FROM locs WHERE ST_Within(p, ST_GeomFromText('POLYGON((100 100,100 120,120 120,120 100,100 100))',0)) ORDER BY id"
	selPlan := planOf(sel)
	require.Contains(t, selPlan, "IndexRangeScan", "low-selectivity query should auto-pick the spatial index:\n"+selPlan)
	tk.MustQuery(sel).Check(baseline(sel))

	// High selectivity (a window covering the whole grid, all 961 rows): a full table
	// scan is cheaper, so the optimizer should not use the index.
	const all = "SELECT id FROM locs WHERE ST_Within(p, ST_GeomFromText('POLYGON((-10 -10,-10 310,310 310,310 -10,-10 -10))',0)) ORDER BY id"
	allPlan := planOf(all)
	require.NotContains(t, allPlan, "IndexRangeScan", "high-selectivity query should fall back to a table scan:\n"+allPlan)
	tk.MustQuery(all).Check(baseline(all))
}

// TestPOCSpatialConcurrentDML stresses the spatial index under concurrent writes:
// several workers (each owning a disjoint id range) interleave INSERT/UPDATE/DELETE
// of points while autocommitting. Afterwards the hidden generated columns + index
// must remain consistent with the table (ADMIN CHECK), and an index-served query
// must match a full scan. Workers use t.Errorf (not require/FailNow, which must run
// on the test goroutine) and retry transient write conflicts.
func TestPOCSpatialConcurrentDML(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setup := testkit.NewTestKit(t, store)
	setup.MustExec("use test")
	setup.MustExec("CREATE TABLE pts (id int primary key, p POINT NOT NULL SRID 0)")
	setup.MustExec("CREATE SPATIAL INDEX si ON pts (p) COMMENT 'spatial:14,0,0,1000,1000'")

	const workers, perWorker = 4, 60
	tks := make([]*testkit.TestKit, workers)
	for w := range tks {
		tks[w] = testkit.NewTestKit(t, store)
		tks[w].MustExec("use test")
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			tk := tks[w]
			exec := func(sql string) {
				for attempt := 0; ; attempt++ {
					_, err := tk.Exec(sql)
					if err == nil {
						return
					}
					msg := err.Error()
					if attempt < 40 && (strings.Contains(msg, "Write conflict") ||
						strings.Contains(msg, "try again") || strings.Contains(msg, "Deadlock")) {
						continue
					}
					t.Errorf("worker %d: %s: %v", w, sql, err)
					return
				}
			}
			base := w * 100000
			for i := 0; i < perWorker; i++ {
				id := base + i
				x, y := (w*200+i*3)%1000, (i*7)%1000
				exec(fmt.Sprintf("INSERT INTO pts VALUES (%d, ST_GeomFromText('POINT(%d %d)',0))", id, x, y))
				if i%3 == 0 {
					exec(fmt.Sprintf("UPDATE pts SET p=ST_GeomFromText('POINT(%d %d)',0) WHERE id=%d", (x+1)%1000, (y+2)%1000, id))
				}
				if i%5 == 0 {
					exec(fmt.Sprintf("DELETE FROM pts WHERE id=%d", id))
				}
			}
		}(w)
	}
	wg.Wait()

	setup.MustExec("ADMIN CHECK TABLE pts")
	setup.MustExec("ADMIN CHECK INDEX pts si")

	const q = "SELECT id FROM pts %s WHERE ST_Within(p, ST_GeomFromText('POLYGON((0 0,0 300,300 300,300 0,0 0))',0)) ORDER BY id"
	withIdx := setup.MustQuery(fmt.Sprintf(q, "FORCE INDEX(si)")).Rows()
	noIdx := setup.MustQuery(fmt.Sprintf(q, "IGNORE INDEX(si)")).Rows()
	require.Equal(t, noIdx, withIdx, "index result must match full scan after concurrent DML")
}
