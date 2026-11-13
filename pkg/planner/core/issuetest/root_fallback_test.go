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

package issuetest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestRootFallbackForNonCopChildren(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select @@tidb_opt_enable_mpp_shared_cte_execution").Check(testkit.Rows("0"))
	tk.MustExec("create table t(id int primary key, a int, b int, name varchar(20), key ia(a), key ib(b))")
	tk.MustExec("insert into t values (1, 1, 1, 'hello'), (2, 2, 2, 'world')")
	tk.MustExec("create table years(c1 year, c2 year, key idx(c1,c2))")
	cases := []struct {
		name string
		sql  string
		rows []string
	}{
		{"point_get_limit", "select id, name from t where id=1 order by id limit 1 offset 0", []string{"1 hello"}},
		{"point_get_offset", "select id, name from t where id=1 order by id limit 1,1", nil},
		{"empty_max", "select max(b) from t where a is null and a=2", []string{"<nil>"}},
		{"empty_min", "select min(b) from t where a is null and a=2", []string{"<nil>"}},
		{"empty_count", "select count(*) from t where a is null and a=2", []string{"0"}},
		{"empty_group", "select /*+ AGG_TO_COP() STREAM_AGG() */ a, count(*) from t where a is null and a=2 group by a", nil},
		{"empty_distinct", "select count(distinct b) from t where a is null and a=2", []string{"0"}},
		{"empty_index_merge", "select /*+ USE_INDEX_MERGE(t, ia, ib) */ id from t where a=null and b=13 order by id limit 1", nil},
		{"forced_limit", "select /*+ LIMIT_TO_COP() */ id from t where id=1 order by id limit 1", []string{"1"}},
	}
	for _, flag := range []string{"off", "on"} {
		t.Run(flag, func(t *testing.T) {
			newTestKit := func(t *testing.T) *testkit.TestKit {
				t.Helper()
				kit := testkit.NewTestKit(t, store)
				kit.MustExec("use test")
				kit.MustExec("set tidb_opt_enable_mpp_shared_cte_execution=" + flag)
				kit.MustExec("set tidb_isolation_read_engines='tikv'")
				return kit
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					queryKit := newTestKit(t)
					queryKit.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
				})
			}
			t.Run("constant_union", func(t *testing.T) {
				queryKit := newTestKit(t)
				queryKit.MustQuery("select null as a union all select 'a' as a").Sort().Check(testkit.Rows("<nil>", "a"))
			})
			t.Run("empty_update", func(t *testing.T) {
				queryKit := newTestKit(t)
				queryKit.MustExec("update years set c2=16777215 where c1>=-8388608 and c1<-9 order by c1 limit 2")
				queryKit.MustQuery("select count(*) from years").Check(testkit.Rows("0"))
			})
			t.Run("prepared_ranges", func(t *testing.T) {
				queryKit := newTestKit(t)
				queryKit.MustExec("prepare s from 'select max(b) from t where a>=? and a<?'")
				for _, run := range []struct{ params, result string }{{"@lo=1, @hi=3", "2"}, {"@lo=3, @hi=1", "<nil>"}, {"@lo=1, @hi=3", "2"}} {
					queryKit.MustExec("set " + run.params)
					queryKit.MustQuery("execute s using @lo,@hi").Check(testkit.Rows(run.result))
				}
			})
		})
	}
}
