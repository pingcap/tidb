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

package core_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func prepareFTSLikeTable(t *testing.T) *testkit.TestKit {
	t.Helper()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_local_match_against = ON")
	tk.MustExec("create table articles (id int primary key, body varchar(255))")
	tk.MustExec(`insert into articles values
		(1, 'Distributed SQL database'),
		(2, 'relational storage engine'),
		(3, 'distributed storage layer'),
		(4, 'SQL is distributed here'),
		(5, 'nothing relevant at all'),
		(6, NULL),
		(7, 'concatenate categories')`)
	return tk
}

// TestFTSLocalMatchPreFilterPushesDown checks the entailed substring predicate
// reaches the coprocessor, which is the entire point: the MATCH itself can only
// run in TiDB, so without this every row is shipped here.
func TestFTSLocalMatchPreFilterPushesDown(t *testing.T) {
	tk := prepareFTSLikeTable(t)

	var plan strings.Builder
	for _, row := range tk.MustQuery(
		"explain select id from articles where match(body) against('+distributed +sql' in boolean mode)").Rows() {
		for _, cell := range row {
			plan.WriteString(cell.(string) + " ")
		}
		plan.WriteString("\n")
	}
	out := plan.String()

	require.Contains(t, out, "cop[tikv]", "the pre-filter should reach the coprocessor:\n"+out)
	require.Contains(t, strings.ToLower(out), "like", "a LIKE pre-filter should be pushed:\n"+out)
	// Both terms should be pushed, not just one.
	require.Contains(t, strings.ToLower(out), "%distributed%", out)
	require.Contains(t, strings.ToLower(out), "%sql%", out)
	// The exact check must remain in TiDB.
	require.Contains(t, strings.ToLower(out), "match_against",
		"MATCH must remain as the exact residual:\n"+out)
}

// TestFTSLocalMatchPreFilterKeepsResults is the correctness property: the
// pre-filter must never discard a row the MATCH would have accepted. The
// expectations are stated explicitly rather than compared against another run
// of the same query, since the pre-filter cannot be switched off from SQL.
func TestFTSLocalMatchPreFilterKeepsResults(t *testing.T) {
	tk := prepareFTSLikeTable(t)

	for _, tc := range []struct {
		search string
		want   []string
	}{
		// Both required terms present, case-insensitively.
		{"+distributed +sql", []string{"1", "4"}},
		// Required minus prohibited.
		{"+distributed -sql", []string{"3"}},
		// Either optional term.
		{"distributed storage", []string{"1", "2", "3", "4"}},
		// Phrase: adjacency is checked by the residual, not the pre-filter, so
		// row 4 ('SQL is distributed here') must be dropped in TiDB.
		{`+"distributed sql"`, []string{"1"}},
		// A prefix contributes '%sq%' to the pre-filter and is still resolved
		// exactly by the matcher.
		{"+distributed +sq*", []string{"1", "4"}},
		// Word boundaries: 'cat' must not match 'concatenate'/'categories',
		// even though the pushed-down LIKE '%cat%' does.
		{"+cat", nil},
		{"+concatenate", []string{"7"}},
		{"+nosuchtoken", nil},
	} {
		t.Run(tc.search, func(t *testing.T) {
			rows := tk.MustQuery("select id from articles where match(body) against('" +
				tc.search + "' in boolean mode) order by id").Rows()
			got := make([]string, 0, len(rows))
			for _, row := range rows {
				got = append(got, row[0].(string))
			}
			require.Equal(t, tc.want, nilIfEmpty(got))
		})
	}
}

func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

// TestFTSLocalMatchPreFilterCaseInsensitive checks the LOWER wrapper does its
// job: MySQL full-text search is case-insensitive, so a mixed-case document
// must not be filtered out before the matcher sees it.
func TestFTSLocalMatchPreFilterCaseInsensitive(t *testing.T) {
	tk := prepareFTSLikeTable(t)

	// Row 1 stores 'Distributed SQL database' with capitals; the analyzer
	// lowercases, so the pre-filter must too.
	tk.MustQuery("select id from articles where match(body) against('+distributed +sql' in boolean mode) order by id").
		Check(testkit.Rows("1", "4"))
}

// TestFTSLocalMatchPreFilterNullColumn checks a NULL column does not make the
// pre-filter NULL, which would drop rows a negative-only query should return.
func TestFTSLocalMatchPreFilterNullColumn(t *testing.T) {
	tk := prepareFTSLikeTable(t)
	tk.MustQuery("select id from articles where match(body) against('+storage -relational' in boolean mode) order by id").
		Check(testkit.Rows("3"))
}
