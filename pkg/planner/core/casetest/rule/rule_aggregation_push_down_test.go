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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAggPushDownJoinCollationMismatch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t0(c0 varchar(10) collate utf8mb4_general_ci)")
	tk.MustExec("create table t1(c0 varbinary(10))")
	tk.MustExec("insert into t0 values ('a'), ('A')")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("create table t2(c0 text collate utf8mb4_bin)")
	tk.MustExec("create table t3(c0 blob)")
	tk.MustExec("insert into t2 values (''), (' ')")
	tk.MustExec("insert into t3 values ('')")
	tk.MustExec("set @@tidb_opt_agg_push_down = 1")

	checkNoPushedAgg := func(sql string) {
		aggCnt := 0
		for _, row := range tk.MustQuery("explain format = 'plan_tree' " + sql).Rows() {
			if strings.Contains(row[0].(string), "Agg") {
				aggCnt++
			}
		}
		require.Equal(t, 1, aggCnt, sql)
	}
	for _, c := range []struct {
		sql  string
		rows []string
	}{
		{"select count(*) from t1 join t0 on t1.c0 = t0.c0 group by t0.c0", []string{"1"}},
		{"select t0.c0, count(*) from t1 join t0 on t1.c0 <> t0.c0 group by t0.c0", []string{"A 1"}},
		{"select count(*) from t3 join t2 on t3.c0 = t2.c0 group by t2.c0", []string{"1"}},
		{"select count(*) from t1 join t0 on concat(t0.c0, '') = t1.c0 group by t0.c0", []string{"1"}},
		{"select count(*) from t1 join t0 on concat(t0.c0, '') <> t1.c0 group by t0.c0", []string{"1"}},
		{"select count(*) from t1 join t0 on strcmp(t0.c0, t1.c0) = 0 group by t0.c0", []string{"1"}},
		{"select count(*) from t1 join t0 on t0.c0 in (t1.c0, 'b') group by t0.c0", []string{"1"}},
		{"select count(*) from t2 a join t2 b on a.c0 like b.c0 group by a.c0", []string{"2"}},
		{"select count(a.c0) from t0 a left join t0 b on a.c0 = b.c0 and a.c0 like binary 'a' group by a.c0", []string{"3"}},
	} {
		checkNoPushedAgg(c.sql)
		tk.MustQuery(c.sql).Sort().Check(testkit.Rows(c.rows...))
	}

	// The cast join key is projected below the join as a binary column, so the pushed agg groups by it and is correct.
	tk.MustQuery("select count(*) from t1 join t0 on cast(t0.c0 as binary) = t1.c0 group by t0.c0").Check(testkit.Rows("1"))
	// The agg can still be pushed down when the join compares with the column's own collation.
	tk.MustQuery("explain format = 'plan_tree' select count(*) from t0 a join t0 b on a.c0 = b.c0 group by b.c0").
		CheckContain("funcs:count(1)")
	tk.MustQuery("select count(*) from t0 a join t0 b on a.c0 = b.c0 group by b.c0").Check(testkit.Rows("4"))
}
