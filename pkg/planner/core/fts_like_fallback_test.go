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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestFTSLikeFallbackMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select @@tidb_enable_fts_like_fallback").Check(testkit.Rows("0"))
	tk.MustExec("create table fts_mode (id int primary key, a text, b text, fulltext index (a))")
	tk.MustExec("insert into fts_mode values (1, 'cat', NULL), (2, 'category', 'dog'), (3, 'dog', NULL), (4, NULL, 'cat')")
	q := "select id from fts_mode where match(a) against('cat' in boolean mode) order by id"
	require.Error(t, tk.ExecToErr(q))
	tk.MustExec("set tidb_enable_fts_like_fallback=on")
	for _, alt := range []string{"off", "on"} {
		tk.MustExec("set tidb_opt_enable_alternative_logical_plans=" + alt)
		tk.MustQuery(q).Check(testkit.Rows("1", "2"))
		tk.MustQuery("select id,a from fts_mode having match(a) against('cat' in boolean mode) order by id").Check(testkit.Rows("1 cat", "2 category"))
		tk.MustQuery("select x.id from fts_mode x join fts_mode y on x.id=y.id and match(x.a) against('cat' in boolean mode) order by x.id").Check(testkit.Rows("1", "2"))
		tk.MustQuery("select id from fts_mode where match(a) against('cat' in boolean mode) and id in (select id from fts_mode where b is null) order by id").Check(testkit.Rows("1"))
		tk.MustQuery("select id from fts_mode where match(a) against('ca' in boolean mode) order by id").Check(testkit.Rows("1", "2"))
		tk.MustQuery("select id from fts_mode where match(a,b) against('+cat -dog' in boolean mode) order by id").Check(testkit.Rows("1", "4"))
		tk.MustQuery("select id from fts_mode where not(match(a) against(NULL in boolean mode))").Check(testkit.Rows())
		tk.MustQuery("select id from fts_mode where match(a) against('-dog' in boolean mode)").Check(testkit.Rows())
		tk.MustQuery("select id from fts_mode where match(a) against('cat') order by id").Check(testkit.Rows("1", "2"))
		for _, sql := range []string{
			"select match(a) against('cat' in boolean mode) from fts_mode",
			"select id from fts_mode where match(a) against('cat' in boolean mode)>0",
			"select id from fts_mode where match(a) against('cat*' in boolean mode)",
			`select id from fts_mode where match(a) against('"cat dog"' in boolean mode)`,
			"select id from fts_mode where match(a) against('cat' with query expansion)",
			"select id from fts_mode where match(id) against(NULL in boolean mode)",
			"select id from fts_mode where match(a) against(a in boolean mode)",
		} {
			require.Error(t, tk.ExecToErr(sql), sql)
		}
	}
	// Local MATCH has priority even if fallback remains enabled.
	tk.MustExec("set tidb_enable_local_match_against=on")
	tk.MustQuery(q).Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("select id from fts_mode where match(b) against('cat' in boolean mode)"))
	require.Error(t, tk.ExecToErr("select id from fts_mode where match(a) against('cat')"))
	tk.MustExec("set tidb_enable_fts_like_fallback=off")
	tk.MustQuery(q).Check(testkit.Rows("1"))
	// A separate session retains its own default mode.
	other := testkit.NewTestKit(t, store)
	other.MustExec("use test")
	require.Error(t, other.ExecToErr(q))
}

func TestFTSLikeFallbackPrepared(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table fts_cache (id int primary key, a text, fulltext index (a))")
	tk.MustExec("insert into fts_cache values (1,'cat'),(2,'category'),(3,'dog')")
	tk.MustExec("set tidb_enable_fts_like_fallback=on")
	tk.MustExec("prepare ps from 'select id from fts_cache where match(a) against(? in boolean mode) order by id'")
	for _, tc := range []struct {
		value string
		rows  []string
	}{
		{"NULL", nil}, {"'cat'", []string{"1", "2"}}, {"'dog'", []string{"3"}}, {"NULL", nil},
	} {
		tk.MustExec("set @q=" + tc.value)
		tk.MustQuery("execute ps using @q").Check(testkit.Rows(tc.rows...))
	}
	// Literal searches may be cached, but switching modes must select a new plan.
	tk.MustExec("prepare lit from \"select id from fts_cache where match(a) against('cat' in boolean mode) order by id\"")
	for i := 0; i < 2; i++ {
		tk.MustQuery("execute lit").Check(testkit.Rows("1", "2"))
	}
	tk.MustExec("set tidb_enable_local_match_against=on")
	for i := 0; i < 2; i++ {
		tk.MustQuery("execute lit").Check(testkit.Rows("1"))
	}
	tk.MustExec("set tidb_enable_local_match_against=off")
	tk.MustQuery("execute lit").Check(testkit.Rows("1", "2"))
	tk.MustExec("set tidb_enable_fts_like_fallback=off")
	require.Error(t, tk.ExecToErr("execute lit"))
}
