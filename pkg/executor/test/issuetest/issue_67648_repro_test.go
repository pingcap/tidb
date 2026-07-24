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

package issuetest_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIssue67648CurrentMasterRepro(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists database1")
	tk.MustExec("create database database1")
	tk.MustExec("use database1")
	tk.MustExec("create table t2 (c0 blob not null)")
	tk.MustExec("create table t3 (c0 blob not null)")
	tk.MustExec("create definer = root@`%` view v1 as select `t2`.`c0` as `c0` from (`database1`.`t2`) join `database1`.`t3` where true")

	for range 6 {
		tk.MustExec("insert into database1.t2 (c0) values (0x313130323336343636)")
	}
	for _, sql := range []string{
		"insert into database1.t3 (c0) values (0x31343234343732313935)",
		"insert into database1.t3 (c0) values (0x2D3165353030)",
		"insert into database1.t3 (c0) values (0x7E6267525E252866280929)",
		"insert into database1.t3 (c0) values (0x2D31363934323832373739)",
		"insert into database1.t3 (c0) values (0x302E39343539373532343435363137353935)",
		"insert into database1.t3 (c0) values (0x2D3535373436363831)",
		"insert into database1.t3 (c0) values (0x302E37333138313335313735323335393736)",
		"insert into database1.t3 (c0) values (0x302E35373032313331323131393433393235)",
	} {
		tk.MustExec(sql)
	}

	issueQuery := "select variance(t2.c0) from v1,t2,t3"
	checkVarianceAlwaysZero := func(name string, rounds int) {
		for range rounds {
			tk.MustQuery(issueQuery).Check(testkit.Rows("0"))
			tk.MustQuery("show warnings").Check(testkit.Rows())
		}
		t.Logf("%s rounds=%d", name, rounds)
	}

	t.Logf("cast sample=%v warnings=%v",
		tk.MustQuery("select hex(c0), cast(c0 as double), cast(c0 as decimal(65,0)) from t2 limit 1").Rows(),
		tk.MustQuery("show warnings").Rows())
	t.Logf("controls=%v warnings=%v",
		tk.MustQuery("select count(t2.c0), sum(t2.c0), var_pop(t2.c0), variance(t2.c0), var_pop(cast(t2.c0 as double)), variance(cast(t2.c0 as double)), var_pop(cast(t2.c0 as decimal(65,0))), variance(cast(t2.c0 as decimal(65,0))) from v1,t2,t3").Rows(),
		tk.MustQuery("show warnings").Rows())

	for _, sql := range []string{
		"explain " + issueQuery,
		"explain select count(t2.c0), sum(t2.c0), var_pop(t2.c0), variance(t2.c0) from v1,t2,t3",
	} {
		t.Logf("%s rows=%v", sql, tk.MustQuery(sql).Rows())
	}

	tk.MustExec("set @@tidb_opt_agg_push_down=0")
	tk.MustExec("set @@tidb_hashagg_partial_concurrency=default")
	tk.MustExec("set @@tidb_hashagg_final_concurrency=default")
	checkVarianceAlwaysZero("root_hashagg_default", 100)

	tk.MustExec("set @@tidb_opt_agg_push_down=0")
	tk.MustExec("set @@tidb_hashagg_partial_concurrency=1")
	tk.MustExec("set @@tidb_hashagg_final_concurrency=1")
	checkVarianceAlwaysZero("root_hashagg_serial", 20)

	tk.MustExec("set @@tidb_opt_agg_push_down=1")
	tk.MustExec("set @@tidb_hashagg_partial_concurrency=default")
	tk.MustExec("set @@tidb_hashagg_final_concurrency=default")
	checkVarianceAlwaysZero("agg_pushdown_default", 100)
	t.Logf("agg_pushdown_plan=%v", tk.MustQuery("explain "+issueQuery).Rows())
}
