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

	cop, root := planPredicates(t, tk,
		"select id from articles where match(body) against('+distributed +sql' in boolean mode)")

	// Both entailed predicates must be evaluated by the coprocessor.
	require.Contains(t, cop, "%distributed%", "cop task predicates:\n"+cop)
	require.Contains(t, cop, "%sql%", "cop task predicates:\n"+cop)
	// And the exact check must stay in TiDB, not be pushed with them.
	require.Contains(t, root, "match_against", "root predicates:\n"+root)
	require.NotContains(t, cop, "match_against", "MATCH must not reach the coprocessor:\n"+cop)
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

// TestFTSLocalMatchPreFilterSkipsUnsafeCharsets covers the cases where SQL
// LOWER does not lowercase the way the analyzer does, so a pushed predicate
// would discard rows the MATCH accepts. Binary is the stark case - LOWER is a
// no-op there - and GB18030 the subtle one: its case table leaves U+1C90
// unchanged where strings.ToLower maps it to U+10D0.
func TestFTSLocalMatchPreFilterSkipsUnsafeCharsets(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_local_match_against = ON")
	// The Georgian characters below are single tokens, so the default minimum
	// token size would filter them out before any of this matters.
	tk.MustExec("set global innodb_ft_min_token_size = 1")
	tk.MustExec("create table g (id int primary key, body varchar(255) charset gb18030)")
	tk.MustExec("insert into g values (1, 'Ა'), (2, 'ა')")

	// Both rows match: the analyzer lowercases the document as well as the
	// query. A pushed LOWER(...) LIKE would keep row 1 as U+1C90 and drop it.
	tk.MustQuery("select id from g where match(body) against('+ა' in boolean mode) order by id").
		Check(testkit.Rows("1", "2"))

	cop, root := planPredicates(t, tk,
		"select id from g where match(body) against('+ა' in boolean mode)")
	require.NotContains(t, cop, "like",
		"no pre-filter may be pushed for a charset whose LOWER differs:\n"+cop)
	require.Contains(t, root, "match_against", "the MATCH still decides:\n"+root)

	// A utf8mb4 column on the same server still gets one.
	tk.MustExec("create table u (id int primary key, body varchar(255))")
	utf8Cop, _ := planPredicates(t, tk,
		"select id from u where match(body) against('+storage' in boolean mode)")
	require.Contains(t, utf8Cop, "%storage%",
		"utf8mb4 should still be narrowed:\n"+utf8Cop)
}

func TestFTSLocalMatchPreFilterSkipsBinaryColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_local_match_against = ON")
	tk.MustExec("create table b (id int primary key, body varbinary(255))")
	tk.MustExec("insert into b values (1, 'Distributed SQL'), (2, 'distributed sql')")

	// Both rows match: the analyzer lowercases the document as well as the query.
	tk.MustQuery("select id from b where match(body) against('+distributed' in boolean mode) order by id").
		Check(testkit.Rows("1", "2"))

	cop, root := planPredicates(t, tk,
		"select id from b where match(body) against('+distributed' in boolean mode)")
	require.NotContains(t, cop, "like",
		"no pre-filter may be pushed for a binary column:\n"+cop)
	require.Contains(t, root, "match_against",
		"the MATCH still decides, in TiDB:\n"+root)
}

// TestFTSLocalMatchPreFilterSkipsMultiColumn covers another case where
// narrowing would be wrong. A multi-column MATCH is satisfied by a token in any
// of its columns, so a pre-filter on one column would discard rows the MATCH
// accepts.
func TestFTSLocalMatchPreFilterSkipsMultiColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_local_match_against = ON")
	tk.MustExec("create table m (id int primary key, title varchar(255), body varchar(255))")
	tk.MustExec(`insert into m values
		(1, 'nothing here', 'storage engine'),
		(2, 'storage title', 'other text'),
		(3, 'unrelated', 'unrelated')`)

	// Row 1 has the token only in the second column, which a pre-filter on the
	// first would have discarded.
	tk.MustQuery("select id from m where match(title, body) against('+storage' in boolean mode) order by id").
		Check(testkit.Rows("1", "2"))

	cop, root := planPredicates(t, tk,
		"select id from m where match(title, body) against('+storage' in boolean mode)")
	require.NotContains(t, cop, "like",
		"no pre-filter may be pushed for a multi-column MATCH:\n"+cop)
	require.Contains(t, root, "match_against",
		"the MATCH still decides, in TiDB:\n"+root)

	// A single-column MATCH on the same table still gets one, so the guard is
	// scoped to the multi-column case rather than disabling the optimisation.
	singleCop, _ := planPredicates(t, tk,
		"select id from m where match(body) against('+storage' in boolean mode)")
	require.Contains(t, singleCop, "%storage%",
		"a single-column MATCH should still be narrowed:\n"+singleCop)
}

// planPredicates splits an EXPLAIN into the operator info of coprocessor tasks
// and of root tasks. Asserting against these separately matters: the whole plan
// text always contains "cop[tikv]" because the table scan runs there, so a
// substring check on it proves nothing about where a predicate ended up.
func planPredicates(t *testing.T, tk *testkit.TestKit, sql string) (cop, root string) {
	t.Helper()
	var copB, rootB strings.Builder
	for _, row := range tk.MustQuery("explain format='brief' " + sql).Rows() {
		task, info := row[2].(string), row[4].(string)
		if strings.Contains(task, "cop[") {
			copB.WriteString(info + "\n")
			continue
		}
		rootB.WriteString(info + "\n")
	}
	return strings.ToLower(copB.String()), strings.ToLower(rootB.String())
}
