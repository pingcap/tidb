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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestLocalMatchMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table fts_mode (id int primary key, a text, fulltext index (a))")
	tk.MustExec("insert into fts_mode values (1,'cat'),(2,'category'),(3,'dog'),(4,NULL)")
	q := "select id from fts_mode where match(a) against('cat' in boolean mode) order by id"
	for _, alternatives := range []string{"off", "on"} {
		t.Run(alternatives, func(t *testing.T) {
			tk.MustExec("set tidb_opt_enable_alternative_logical_plans=" + alternatives)
			tk.MustExec("set tidb_enable_local_match_against=off")
			require.ErrorContains(t, tk.ExecToErr(q), "without tidb_enable_local_match_against")
			require.ErrorContains(t, tk.ExecToErr(fmt.Sprintf("prepare disabled from %q", q)), "without tidb_enable_local_match_against")
			tk.MustExec("set tidb_enable_local_match_against=on")
			tk.MustQuery(q).Check(testkit.Rows("1"))
			plan := fmt.Sprint(tk.MustQuery("explain format='brief' " + q).Rows())
			require.Contains(t, plan, "match_against")
			require.NotContains(t, plan, "ilike")
			tk.MustQuery("select id,a from fts_mode having match(a) against('cat' in boolean mode) order by id").Check(testkit.Rows("1 cat"))
			tk.MustQuery("select x.id from fts_mode x join fts_mode y on x.id=y.id and match(x.a) against('cat' in boolean mode) order by x.id").Check(testkit.Rows("1"))
			tk.MustQuery("select id from fts_mode where match(a) against('cat*' in boolean mode) order by id").Check(testkit.Rows("1", "2"))
			require.Error(t, tk.ExecToErr("select id from fts_mode where match(a) against('cat')"))
			// Explicit substring matching remains available independently of MATCH.
			tk.MustQuery("select id from fts_mode where a ilike '%CAT%' order by id").Check(testkit.Rows("1", "2"))
		})
	}
	require.ErrorContains(t, tk.ExecToErr("set tidb_enable_fts_like_fallback=on"), "Unknown system variable")
}

func TestLocalMatchPreparedMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_local_match_against=on, tidb_opt_enable_alternative_logical_plans=on")
	tk.MustExec("create table fts_cache (id int primary key, a text, fulltext index (a))")
	tk.MustExec("insert into fts_cache values (1,'cat'),(2,'category'),(3,'dog'),(4,NULL)")
	tk.MustExec("prepare ps from 'select id from fts_cache where match(a) against(? in boolean mode) order by id'")
	for _, tc := range []struct {
		value string
		rows  []string
	}{
		{"NULL", nil}, {"'cat'", []string{"1"}}, {"'dog'", []string{"3"}}, {"NULL", nil},
	} {
		tk.MustExec("set @q=" + tc.value)
		tk.MustQuery("execute ps using @q").Check(testkit.Rows(tc.rows...))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	tk.MustExec("set @q='cat'")
	tk.MustExec("set tidb_enable_local_match_against=off")
	require.ErrorContains(t, tk.ExecToErr("execute ps using @q"), "without tidb_enable_local_match_against")
	tk.MustExec("set tidb_enable_local_match_against=on")
	tk.MustQuery("execute ps using @q").Check(testkit.Rows("1"))
}
