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

package rule

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

const altRoundFailpoint = "github.com/pingcap/tidb/pkg/planner/failIfAlternativeLogicalPlanRoundTriggered"

// TestFTSLikeFallbackRebuildsRound1 checks that when round 1's MATCH...AGAINST
// cannot run natively (here: boolean mode and no FULLTEXT index), round 1 is
// rebuilt with the ILIKE rewrite instead of being optimized and discarded, so
// the then-redundant fts-like-fallback round no longer runs, while the other
// alternative rounds still do.
func TestFTSLikeFallbackRebuildsRound1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table fts_doc (id int primary key, title varchar(100), body text, key k_title (title))")
	tk.MustExec("create table fts_rel (id int primary key, doc_id int, ref_id int, key k_doc (doc_id), key k_ref (ref_id))")
	tk.MustExec("insert into fts_doc values (1, 'a', 'tidb database'), (2, 'b', 'storage engine'), (3, 'c', 'Database systems'), (4, 'd', 'other')")
	tk.MustExec("insert into fts_rel values (1, 1, 2), (2, 3, 4), (3, 2, 1)")
	tk.MustExec("analyze table fts_doc, fts_rel")

	single := "select id from fts_doc where match(body) against('+database' in boolean mode) order by id"
	join := "select d.id from fts_doc d join fts_rel r on r.ref_id = d.id join fts_doc d2 on d2.id = r.doc_id " +
		"where match(d2.body) against('+database' in boolean mode) order by d.title limit 10"

	// Without alternative logical plans the native path is final, and boolean
	// mode cannot be pushed down, so the statement still fails as before.
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans = off")
	require.ErrorContains(t, tk.ExecToErr(single), "MATCH...AGAINST with this modifier on the native FTS path")

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans = on")
	expectRound := func(round, sql string, triggered bool) {
		testfailpoint.Enable(t, altRoundFailpoint, fmt.Sprintf("return(%q)", round+":"+sql))
		err := tk.ExecToErr(sql)
		if triggered {
			require.ErrorContains(t, err, "unexpected alternative logical plan round", "%s: %s", round, sql)
		} else {
			require.NoError(t, err, "%s: %s", round, sql)
		}
		testfailpoint.Disable(t, altRoundFailpoint)
	}

	// The single-table query is planned once, with the ILIKE rewrite: the
	// fts-like-fallback round does not run, and the plan filters with ilike.
	expectRound("fts-like-fallback", single, false)
	plan := testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + single).Rows())
	require.True(t, planContains(plan, "ilike"), strings.Join(plan, "\n"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.AlternativeLogicalPlanFTSLikeFallback)
	tk.MustQuery(single).Check(testkit.Rows("1", "3"))

	// The join with ORDER BY ... LIMIT still gets the order-aware-reorder round,
	// built with the ILIKE rewrite too, but no fts-like-fallback round.
	expectRound("fts-like-fallback", join, false)
	expectRound("order-aware-reorder", join, true)
	tk.MustQuery(join).Sort().Check(testkit.Rows("2", "4"))
}

func planContains(plan []string, s string) bool {
	for _, row := range plan {
		if strings.Contains(row, s) {
			return true
		}
	}
	return false
}
