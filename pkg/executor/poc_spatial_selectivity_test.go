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

// indexScanActRows pulls the actRows of the IndexRangeScan operator out of an
// EXPLAIN ANALYZE result, so we can show the index touched far fewer rows than
// the table holds.
func indexScanActRows(t *testing.T, tk *testkit.TestKit, query string) (int, bool) {
	rows := tk.MustQuery("EXPLAIN ANALYZE " + query).Rows()
	re := regexp.MustCompile(`^\s*(\d+)`)
	for _, r := range rows {
		op := fmt.Sprintf("%v", r[0])
		if !strings.Contains(op, "IndexRangeScan") {
			continue
		}
		// actRows is the 3rd column in EXPLAIN ANALYZE output.
		m := re.FindStringSubmatch(fmt.Sprintf("%v", r[2]))
		if m == nil {
			return 0, false
		}
		n, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		return n, true
	}
	return 0, false
}

// TestPOCSpatialSelectivity demonstrates that a tuned spatial index actually
// PRUNES: on a 10000-point grid spanning a 0..1 lon/lat box, a small-radius
// query touches only a tiny fraction of the rows via the index, yet returns
// exactly the same rows as a full-scan plan. It also reports the candidate
// (false-positive) ratio of the covering.
func TestPOCSpatialSelectivity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// 100x100 grid of points over [0,1]x[0,1] (10000 rows).
	tk.MustExec("CREATE TABLE locs (id int primary key, p POINT NOT NULL SRID 0)")
	const side = 100
	var b strings.Builder
	b.WriteString("INSERT INTO locs VALUES ")
	id := 0
	for i := 0; i < side; i++ {
		for j := 0; j < side; j++ {
			if id > 0 {
				b.WriteString(",")
			}
			x := float64(i) / float64(side)
			y := float64(j) / float64(side)
			fmt.Fprintf(&b, "(%d, ST_GeomFromText('POINT(%g %g)',0))", id, x, y)
			id++
		}
	}
	tk.MustExec(b.String())
	tk.MustQuery("SELECT count(*) FROM locs").Check(testkit.Rows("10000"))

	// A query region: within radius 0.05 of (0.5, 0.5).
	const query = "SELECT id FROM locs WHERE ST_Distance(p, ST_GeomFromText('POINT(0.5 0.5)',0)) <= 0.05 ORDER BY id"

	// Baseline (full scan) result.
	want := tk.MustQuery(query).Rows()
	require.NotEmpty(t, want)

	// Tuned spatial index: cells sized to the data (level 12 over the [0,1] box
	// => ~1/4096-wide leaf cells, much smaller than the query region).
	tk.MustExec("CREATE SPATIAL INDEX sidx ON locs (p) COMMENT 'spatial:12,0,0,1,1'")

	// Same rows with the index present (the optimizer may still pick a full scan
	// here; the result must be identical either way).
	tk.MustQuery(query).Check(want)

	// Forcing the index proves the selectivity: the same correct rows, served by
	// an index range scan that touches only a small fraction of the table.
	// (Automatic selection of the spatial index over a full scan needs spatial
	// statistics/cost estimation, tracked as a follow-up.)
	const forced = "SELECT id FROM locs FORCE INDEX (sidx) WHERE " +
		"ST_Distance(p, ST_GeomFromText('POINT(0.5 0.5)',0)) <= 0.05 ORDER BY id"
	tk.MustQuery(forced).Check(want)

	act, ok := indexScanActRows(t, tk, forced)
	require.True(t, ok, "expected an IndexRangeScan in EXPLAIN ANALYZE")
	t.Logf("index scanned %d candidate rows for %d true matches (%d total rows); "+
		"false-positive ratio = %.2fx; index touched %.1f%% of the table",
		act, len(want), id, float64(act)/float64(len(want)), 100*float64(act)/float64(id))
	require.Less(t, act, 2000, "index should prune to a small fraction of the 10000 rows")
	require.GreaterOrEqual(t, act, len(want), "candidate set must be a superset of the matches")
}
