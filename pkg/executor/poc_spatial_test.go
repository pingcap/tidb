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

	// Create the spatial index.
	tk.MustExec("CREATE SPATIAL INDEX sidx ON locs (p)")

	// Same results when the index is used. The index is FORCE'd: automatically
	// preferring the spatial index over a full scan needs spatial cost/statistics
	// (tracked as a follow-up; see OVERNIGHT-PLAN.md).
	const distForced = "SELECT id FROM locs FORCE INDEX (sidx) WHERE " + distPred + " ORDER BY id"
	const containForced = "SELECT id FROM locs FORCE INDEX (sidx) WHERE " + containPred + " ORDER BY id"
	tk.MustQuery(distForced).Check(wantDist)
	tk.MustQuery(containForced).Check(wantContain)

	// EXPLAIN shows an index range scan on the spatial index plus a refine
	// Selection carrying the original ST_Distance predicate.
	explain := tk.MustQuery("EXPLAIN " + distForced).Rows()
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
