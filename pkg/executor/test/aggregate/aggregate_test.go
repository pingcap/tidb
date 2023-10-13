// Copyright 2015 PingCAP, Inc.
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

package aggregate

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestGroupConcatAggr(t *testing.T) {
	var err error
	// issue #5411
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test;")
	tk.MustExec("create table test(id int, name int)")
	tk.MustExec("insert into test values(1, 10);")
	tk.MustExec("insert into test values(1, 20);")
	tk.MustExec("insert into test values(1, 30);")
	tk.MustExec("insert into test values(2, 20);")
	tk.MustExec("insert into test values(3, 200);")
	tk.MustExec("insert into test values(3, 500);")
	result := tk.MustQuery("select id, group_concat(name) from test group by id order by id")
	result.Check(testkit.Rows("1 10,20,30", "2 20", "3 200,500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR ';') from test group by id order by id")
	result.Check(testkit.Rows("1 10;20;30", "2 20", "3 200;500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR ',') from test group by id order by id")
	result.Check(testkit.Rows("1 10,20,30", "2 20", "3 200,500"))

	result = tk.MustQuery(`select id, group_concat(name SEPARATOR '%') from test group by id order by id`)
	result.Check(testkit.Rows("1 10%20%30", "2 20", `3 200%500`))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR '') from test group by id order by id")
	result.Check(testkit.Rows("1 102030", "2 20", "3 200500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR '123') from test group by id order by id")
	result.Check(testkit.Rows("1 101232012330", "2 20", "3 200123500"))

	tk.MustQuery("select group_concat(id ORDER BY name) from (select * from test order by id, name limit 2,2) t").Check(testkit.Rows("2,1"))
	tk.MustQuery("select group_concat(id ORDER BY name desc) from (select * from test order by id, name limit 2,2) t").Check(testkit.Rows("1,2"))
	tk.MustQuery("select group_concat(name ORDER BY id) from (select * from test order by id, name limit 2,2) t").Check(testkit.Rows("30,20"))
	tk.MustQuery("select group_concat(name ORDER BY id desc) from (select * from test order by id, name limit 2,2) t").Check(testkit.Rows("20,30"))

	result = tk.MustQuery("select group_concat(name ORDER BY name desc SEPARATOR '++') from test;")
	result.Check(testkit.Rows("500++200++30++20++20++10"))

	result = tk.MustQuery("select group_concat(id ORDER BY name desc, id asc SEPARATOR '--') from test;")
	result.Check(testkit.Rows("3--3--1--1--2--1"))

	result = tk.MustQuery("select group_concat(name ORDER BY name desc SEPARATOR '++'), group_concat(id ORDER BY name desc, id asc SEPARATOR '--') from test;")
	result.Check(testkit.Rows("500++200++30++20++20++10 3--3--1--1--2--1"))

	result = tk.MustQuery("select group_concat(distinct name order by name desc) from test;")
	result.Check(testkit.Rows("500,200,30,20,10"))

	expected := "3--3--1--1--2--1"
	for maxLen := 4; maxLen < len(expected); maxLen++ {
		tk.MustExec(fmt.Sprintf("set session group_concat_max_len=%v", maxLen))
		result = tk.MustQuery("select group_concat(id ORDER BY name desc, id asc SEPARATOR '--') from test;")
		result.Check(testkit.Rows(expected[:maxLen]))
		require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	}
	expected = "1--2--1--1--3--3"
	for maxLen := 4; maxLen < len(expected); maxLen++ {
		tk.MustExec(fmt.Sprintf("set session group_concat_max_len=%v", maxLen))
		result = tk.MustQuery("select group_concat(id ORDER BY name asc, id desc SEPARATOR '--') from test;")
		result.Check(testkit.Rows(expected[:maxLen]))
		require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	}
	expected = "500,200,30,20,10"
	for maxLen := 4; maxLen < len(expected); maxLen++ {
		tk.MustExec(fmt.Sprintf("set session group_concat_max_len=%v", maxLen))
		result = tk.MustQuery("select group_concat(distinct name order by name desc) from test;")
		result.Check(testkit.Rows(expected[:maxLen]))
		require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	}

	tk.MustExec(fmt.Sprintf("set session group_concat_max_len=%v", 1024))

	// test varchar table
	tk.MustExec("drop table if exists test2;")
	tk.MustExec("create table test2(id varchar(20), name varchar(20));")
	tk.MustExec("insert into test2 select * from test;")

	tk.MustQuery("select group_concat(id ORDER BY name) from (select * from test2 order by id, name limit 2,2) t").Check(testkit.Rows("2,1"))
	tk.MustQuery("select group_concat(id ORDER BY name desc) from (select * from test2 order by id, name limit 2,2) t").Check(testkit.Rows("1,2"))
	tk.MustQuery("select group_concat(name ORDER BY id) from (select * from test2 order by id, name limit 2,2) t").Check(testkit.Rows("30,20"))
	tk.MustQuery("select group_concat(name ORDER BY id desc) from (select * from test2 order by id, name limit 2,2) t").Check(testkit.Rows("20,30"))

	result = tk.MustQuery("select group_concat(name ORDER BY name desc SEPARATOR '++'), group_concat(id ORDER BY name desc, id asc SEPARATOR '--') from test2;")
	result.Check(testkit.Rows("500++30++200++20++20++10 3--1--3--1--2--1"))

	// test Position Expr
	tk.MustQuery("select 1, 2, 3, 4, 5 , group_concat(name, id ORDER BY 1 desc, id SEPARATOR '++') from test;").Check(testkit.Rows("1 2 3 4 5 5003++2003++301++201++202++101"))
	tk.MustQuery("select 1, 2, 3, 4, 5 , group_concat(name, id ORDER BY 2 desc, name SEPARATOR '++') from test;").Check(testkit.Rows("1 2 3 4 5 2003++5003++202++101++201++301"))
	err = tk.ExecToErr("select 1, 2, 3, 4, 5 , group_concat(name, id ORDER BY 3 desc, name SEPARATOR '++') from test;")
	require.EqualError(t, err, "[planner:1054]Unknown column '3' in 'order clause'")
	// test Param Marker
	tk.MustExec(`prepare s1 from "select 1, 2, 3, 4, 5 , group_concat(name, id ORDER BY floor(id/?) desc, name SEPARATOR '++') from test";`)
	tk.MustExec("set @a=2;")
	tk.MustQuery("execute s1 using @a;").Check(testkit.Rows("1 2 3 4 5 202++2003++5003++101++201++301"))

	tk.MustExec(`prepare s1 from "select 1, 2, 3, 4, 5 , group_concat(name, id ORDER BY ? desc, name SEPARATOR '++') from test";`)
	tk.MustExec("set @a=2;")
	tk.MustQuery("execute s1 using @a;").Check(testkit.Rows("1 2 3 4 5 2003++5003++202++101++201++301"))
	tk.MustExec("set @a=3;")
	err = tk.ExecToErr("execute s1 using @a;")
	require.EqualError(t, err, "[planner:1054]Unknown column '?' in 'order clause'")
	tk.MustExec("set @a=3.0;")
	tk.MustQuery("execute s1 using @a;").Check(testkit.Rows("1 2 3 4 5 101++202++201++301++2003++5003"))

	// test partition table
	tk.MustExec("drop table if exists ptest;")
	tk.MustExec("CREATE TABLE ptest (id int,name int) PARTITION BY RANGE ( id ) " +
		"(PARTITION `p0` VALUES LESS THAN (2), PARTITION `p1` VALUES LESS THAN (11))")
	tk.MustExec("insert into ptest select * from test;")

	for i := 0; i <= 1; i++ {
		for j := 0; j <= 1; j++ {
			tk.MustExec(fmt.Sprintf("set session tidb_opt_distinct_agg_push_down = %v", i))
			tk.MustExec(fmt.Sprintf("set session tidb_opt_agg_push_down = %v", j))

			result = tk.MustQuery("select /*+ agg_to_cop */ group_concat(name ORDER BY name desc SEPARATOR '++'), group_concat(id ORDER BY name desc, id asc SEPARATOR '--') from ptest;")
			result.Check(testkit.Rows("500++200++30++20++20++10 3--3--1--1--2--1"))

			result = tk.MustQuery("select /*+ agg_to_cop */ group_concat(distinct name order by name desc) from ptest;")
			result.Check(testkit.Rows("500,200,30,20,10"))
		}
	}

	// issue #9920
	tk.MustQuery("select group_concat(123, null)").Check(testkit.Rows("<nil>"))

	// issue #23129
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(cid int, sname varchar(100));")
	tk.MustExec("insert into t1 values(1, 'Bob'), (1, 'Alice');")
	tk.MustExec("insert into t1 values(3, 'Ace');")
	tk.MustExec("set @@group_concat_max_len=5;")
	rows := tk.MustQuery("select group_concat(sname order by sname) from t1 group by cid;")
	rows.Check(testkit.Rows("Alice", "Ace"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 varchar(10));")
	tk.MustExec("insert into t1 values('0123456789');")
	tk.MustExec("insert into t1 values('12345');")
	tk.MustExec("set @@group_concat_max_len=8;")
	rows = tk.MustQuery("select group_concat(c1 order by c1) from t1 group by c1;")
	rows.Check(testkit.Rows("01234567", "12345"))
}

func TestSelectDistinct(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	internal.FillData(tk, "select_distinct_test")

	tk.MustExec("begin")
	r := tk.MustQuery("select distinct name from select_distinct_test;")
	r.Check(testkit.Rows("hello"))
	tk.MustExec("commit")
}

func TestInjectProjBelowTopN(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (i int);")
	tk.MustExec("insert into t values (1), (1), (1),(2),(3),(2),(3),(2),(3);")
	var (
		input  []string
		output [][]string
	)
	aggMergeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func TestIssue12759HashAggCalledByApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.Session().GetSessionVars().SetHashAggFinalConcurrency(4)
	tk.MustExec(`insert into mysql.opt_rule_blacklist value("decorrelate");`)
	defer func() {
		tk.MustExec(`delete from mysql.opt_rule_blacklist where name = "decorrelate";`)
		tk.MustExec(`admin reload opt_rule_blacklist;`)
	}()
	tk.MustExec(`drop table if exists test;`)
	tk.MustExec("create table test (a int);")
	tk.MustExec("insert into test value(1);")
	tk.MustQuery("select /*+ hash_agg() */ sum(a), (select NULL from test where tt.a = test.a limit 1),(select NULL from test where tt.a = test.a limit 1),(select NULL from test where tt.a = test.a limit 1) from test tt;").Check(testkit.Rows("1 <nil> <nil> <nil>"))

	var (
		input  []string
		output [][]string
	)
	aggMergeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func TestHashAggRuntimeStat(t *testing.T) {
	partialInfo := &aggregate.AggWorkerInfo{
		Concurrency: 5,
		WallTime:    int64(time.Second * 20),
	}
	finalInfo := &aggregate.AggWorkerInfo{
		Concurrency: 8,
		WallTime:    int64(time.Second * 10),
	}
	stats := &aggregate.HashAggRuntimeStats{
		PartialConcurrency: 5,
		PartialWallTime:    int64(time.Second * 20),
		FinalConcurrency:   8,
		FinalWallTime:      int64(time.Second * 10),
	}
	for i := 0; i < partialInfo.Concurrency; i++ {
		stats.PartialStats = append(stats.PartialStats, &aggregate.AggWorkerStat{
			TaskNum:    5,
			WaitTime:   int64(2 * time.Second),
			ExecTime:   int64(1 * time.Second),
			WorkerTime: int64(i) * int64(time.Second),
		})
	}
	for i := 0; i < finalInfo.Concurrency; i++ {
		stats.FinalStats = append(stats.FinalStats, &aggregate.AggWorkerStat{
			TaskNum:    5,
			WaitTime:   int64(2 * time.Millisecond),
			ExecTime:   int64(1 * time.Millisecond),
			WorkerTime: int64(i) * int64(time.Millisecond),
		})
	}
	expect := "partial_worker:{wall_time:20s, concurrency:5, task_num:25, tot_wait:10s, tot_exec:5s, tot_time:10s, max:4s, p95:4s}, final_worker:{wall_time:10s, concurrency:8, task_num:40, tot_wait:16ms, tot_exec:8ms, tot_time:28ms, max:7ms, p95:7ms}"
	require.Equal(t, expect, stats.String())
	require.Equal(t, expect, stats.Clone().String())
	stats.Merge(stats.Clone())
	expect = "partial_worker:{wall_time:40s, concurrency:5, task_num:50, tot_wait:20s, tot_exec:10s, tot_time:20s, max:4s, p95:4s}, final_worker:{wall_time:20s, concurrency:8, task_num:80, tot_wait:32ms, tot_exec:16ms, tot_time:56ms, max:7ms, p95:7ms}"
	require.Equal(t, expect, stats.String())
}

func reconstructParallelGroupConcatResult(rows [][]interface{}) []string {
	data := make([]string, 0, len(rows))
	for _, row := range rows {
		if str, ok := row[0].(string); ok {
			tokens := strings.Split(str, ",")
			sort.Slice(tokens, func(i, j int) bool {
				return tokens[i] < tokens[j]
			})
			data = append(data, strings.Join(tokens, ","))
		}
	}

	sort.Slice(data, func(i, j int) bool {
		return data[i] < data[j]
	})

	return data
}

func TestParallelStreamAggGroupConcat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t(a bigint, b bigint);")
	tk.MustExec("set tidb_init_chunk_size=1;")
	tk.MustExec("set tidb_max_chunk_size=32;")

	var insertSQL string
	for i := 0; i < 1000; i++ {
		if i == 0 {
			insertSQL += fmt.Sprintf("(%d, %d)", rand.Intn(100), rand.Intn(100))
		} else {
			insertSQL += fmt.Sprintf(",(%d, %d)", rand.Intn(100), rand.Intn(100))
		}
	}
	tk.MustExec(fmt.Sprintf("insert into t values %s;", insertSQL))

	sql := "select /*+ stream_agg() */ group_concat(a, b) from t group by b;"
	concurrencies := []int{1, 2, 4, 8}
	var expected []string
	for _, con := range concurrencies {
		tk.MustExec(fmt.Sprintf("set @@tidb_streamagg_concurrency=%d", con))
		if con == 1 {
			expected = reconstructParallelGroupConcatResult(tk.MustQuery(sql).Rows())
		} else {
			er := tk.MustQuery("explain format = 'brief' " + sql).Rows()
			ok := false
			for _, l := range er {
				str := fmt.Sprintf("%v", l)
				if strings.Contains(str, "Shuffle") {
					ok = true
					break
				}
			}
			require.True(t, ok)
			obtained := reconstructParallelGroupConcatResult(tk.MustQuery(sql).Rows())
			require.Equal(t, len(expected), len(obtained))
			for i := 0; i < len(obtained); i++ {
				require.Equal(t, expected[i], obtained[i])
			}
		}
	}
}

func TestIssue20658(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	aggFuncs := []string{"count(a)", "sum(a)", "avg(a)", "max(a)", "min(a)", "bit_or(a)", "bit_xor(a)", "bit_and(a)", "var_pop(a)", "var_samp(a)", "stddev_pop(a)", "stddev_samp(a)", "approx_count_distinct(a)", "approx_percentile(a, 7)"}
	sqlFormat := "select /*+ stream_agg() */ %s from t group by b;"

	sqls := make([]string, 0, len(aggFuncs))
	for _, af := range aggFuncs {
		sql := fmt.Sprintf(sqlFormat, af)
		sqls = append(sqls, sql)
	}

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t(a bigint, b bigint);")
	tk.MustExec("set tidb_init_chunk_size=1;")
	tk.MustExec("set tidb_max_chunk_size=32;")
	randSeed := time.Now().UnixNano()
	r := rand.New(rand.NewSource(randSeed))
	var insertSQL strings.Builder
	for i := 0; i < 1000; i++ {
		insertSQL.WriteString("(")
		insertSQL.WriteString(strconv.Itoa(r.Intn(10)))
		insertSQL.WriteString(",")
		insertSQL.WriteString(strconv.Itoa(r.Intn(10)))
		insertSQL.WriteString(")")
		if i < 1000-1 {
			insertSQL.WriteString(",")
		}
	}
	tk.MustExec(fmt.Sprintf("insert into t values %s;", insertSQL.String()))

	mustParseAndSort := func(rows [][]interface{}, cmt string) []float64 {
		ret := make([]float64, len(rows))
		for i := 0; i < len(rows); i++ {
			rowStr := rows[i][0].(string)
			if rowStr == "<nil>" {
				ret[i] = 0
				continue
			}
			v, err := strconv.ParseFloat(rowStr, 64)
			require.NoError(t, err, cmt)
			ret[i] = v
		}
		sort.Float64s(ret)
		return ret
	}
	for _, sql := range sqls {
		tk.MustExec("set @@tidb_streamagg_concurrency = 1;")
		exp := tk.MustQuery(sql).Rows()
		expected := mustParseAndSort(exp, fmt.Sprintf("sql: %s; seed: %d", sql, randSeed))
		for _, con := range []int{2, 4, 8} {
			comment := fmt.Sprintf("sql: %s; concurrency: %d, seed: %d", sql, con, randSeed)
			tk.MustExec(fmt.Sprintf("set @@tidb_streamagg_concurrency=%d;", con))
			er := tk.MustQuery("explain format = 'brief' " + sql).Rows()
			ok := false
			for _, l := range er {
				str := fmt.Sprintf("%v", l)
				if strings.Contains(str, "Shuffle") {
					ok = true
					break
				}
			}
			require.True(t, ok, comment)
			rows := mustParseAndSort(tk.MustQuery(sql).Rows(), comment)

			require.Equal(t, len(expected), len(rows), comment)
			for i := range rows {
				require.Less(t, math.Abs(rows[i]-expected[i]), 1e-3, comment)
			}
		}
	}
}

func TestAggInDisk(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_hashagg_final_concurrency = 1;")
	tk.MustExec("set tidb_hashagg_partial_concurrency = 1;")
	tk.MustExec("set tidb_mem_quota_query = 4194304")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(a int)")
	sql := "insert into t values (0)"
	for i := 1; i <= 200; i++ {
		sql += fmt.Sprintf(",(%v)", i)
	}
	sql += ";"
	tk.MustExec(sql)
	rows := tk.MustQuery("desc analyze select /*+ HASH_AGG() */ avg(t1.a) from t t1 join t t2 group by t1.a, t2.a;").Rows()
	for _, row := range rows {
		length := len(row)
		line := fmt.Sprintf("%v", row)
		disk := fmt.Sprintf("%v", row[length-1])
		if strings.Contains(line, "HashAgg") {
			require.False(t, strings.Contains(disk, "0 Bytes"))
			require.True(t, strings.Contains(disk, "MB") ||
				strings.Contains(disk, "KB") ||
				strings.Contains(disk, "Bytes"))
		}
	}

	// Add code cover
	// Test spill chunk. Add a line to avoid tmp spill chunk is always full.
	tk.MustExec("insert into t values(0)")
	tk.MustQuery("select sum(tt.b) from ( select /*+ HASH_AGG() */ avg(t1.a) as b from t t1 join t t2 group by t1.a, t2.a) as tt").Check(
		testkit.Rows("4040100.0000"))
	// Test no groupby and no data.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(c int, c1 int);")
	tk.MustQuery("select /*+ HASH_AGG() */ count(c) from t;").Check(testkit.Rows("0"))
	tk.MustQuery("select /*+ HASH_AGG() */ count(c) from t group by c1;").Check(testkit.Rows())
}

func TestRandomPanicConsume(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_max_chunk_size=32")
	tk.MustExec("set @@tidb_init_chunk_size=1")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(pk bigint primary key auto_random,a int, index idx(a));")
	tk.MustExec("SPLIT TABLE t BETWEEN (-9223372036854775808) AND (9223372036854775807) REGIONS 50;") // Split 50 regions to simulate many requests
	for i := 0; i <= 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t(a) values(%v),(%v),(%v)", i, i, i))
	}
	tk.MustExec("drop table if exists s;")
	tk.MustExec("create table s(pk bigint primary key auto_random,a int, b int, index idx(a));")
	tk.MustExec("SPLIT TABLE s BETWEEN (-9223372036854775808) AND (9223372036854775807) REGIONS 50;") // Split 50 regions to simulate many requests
	for i := 0; i <= 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into s(a,b) values(%v,%v),(%v,%v),(%v,%v)", i, i, i, i, i, i))
	}

	fpName := "github.com/pingcap/tidb/pkg/executor/aggregate/ConsumeRandomPanic"
	require.NoError(t, failpoint.Enable(fpName, "5%panic(\"ERROR 1105 (HY000): Out Of Memory Quota![conn=1]\")"))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
	}()
	fpName2 := "github.com/pingcap/tidb/pkg/store/copr/ConsumeRandomPanic"
	require.NoError(t, failpoint.Enable(fpName2, "3%panic(\"ERROR 1105 (HY000): Out Of Memory Quota![conn=1]\")"))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName2))
	}()

	sqls := []string{
		// Without index
		"select /*+ HASH_AGG() */ /*+ USE_INDEX(t) */ count(a) from t group by a",                                  // HashAgg Paralleled
		"select /*+ HASH_AGG() */ /*+ USE_INDEX(t) */ count(distinct a) from t",                                    // HashAgg Unparalleled
		"select /*+ STREAM_AGG() */ /*+ USE_INDEX(t) */ count(a) from t group by a",                                // Shuffle+StreamAgg
		"select /*+ USE_INDEX(t) */ a * a, a / a, a + a , a - a from t",                                            // Projection
		"select /*+ HASH_JOIN(t1) */ /*+ USE_INDEX(t1) */ /*+ USE_INDEX(t2) */* from t t1 join t t2 on t1.a=t2.a",  // HashJoin
		"select /*+ MERGE_JOIN(t1) */ /*+ USE_INDEX(t1) */ /*+ USE_INDEX(t2) */* from t t1 join t t2 on t1.a=t2.a", // Shuffle+MergeJoin
		"select /*+ USE_INDEX(t) */ * from t",                                                                      // TableScan

		// With index
		"select /*+ HASH_AGG() */ /*+ USE_INDEX(t,idx) */ count(a) from t group by a",                                       // HashAgg Paralleled
		"select /*+ HASH_AGG() */ /*+ USE_INDEX(t,idx) */ count(distinct a) from t",                                         // HashAgg Unparalleled
		"select /*+ STREAM_AGG() */ /*+ USE_INDEX(t,idx) */ count(a) from t group by a",                                     // Shuffle+StreamAgg
		"select /*+ USE_INDEX(t,idx) */ a * a, a / a, a + a , a - a from t",                                                 // Projection
		"select /*+ HASH_JOIN(t1) */ /*+ USE_INDEX(t1,idx) */ /*+ USE_INDEX(t2,idx) */ * from t t1 join t t2 on t1.a=t2.a",  // HashJoin
		"select /*+ MERGE_JOIN(t1) */ /*+ USE_INDEX(t1,idx) */ /*+ USE_INDEX(t2,idx) */ * from t t1 join t t2 on t1.a=t2.a", // Shuffle+MergeJoin
		"select /*+ INL_JOIN(t2) */ * from t t1 join t t2 on t1.a=t2.a;",                                                    // Index Join
		"select /*+ INL_HASH_JOIN(t2) */ * from t t1 join t t2 on t1.a=t2.a;",                                               // Index Hash Join
		"select /*+ USE_INDEX(t, idx) */ * from t",                                                                          // IndexScan

		// With IndexLookUp
		"select /*+ MERGE_JOIN(t1) */ /*+ USE_INDEX(t1,idx) */ /*+ USE_INDEX(t2,idx) */ * from s t1 join s t2 on t1.a=t2.a", // Shuffle+MergeJoin
		"select /*+ INL_JOIN(t2) */ * from s t1 join s t2 on t1.a=t2.a;",                                                    // Index Join
		"select /*+ INL_HASH_JOIN(t2) */ * from s t1 join s t2 on t1.a=t2.a;",                                               // Index Hash Join
		"select /*+ USE_INDEX(s, idx) */ * from s",                                                                          // IndexLookUp
	}

	// Test 10 times panic for each Executor.
	var res sqlexec.RecordSet
	for _, sql := range sqls {
		for i := 1; i <= 10; i++ {
			concurrency := rand.Int31n(4) + 1 // test 1~5 concurrency randomly
			tk.MustExec(fmt.Sprintf("set @@tidb_executor_concurrency=%v", concurrency))
			tk.MustExec(fmt.Sprintf("set @@tidb_merge_join_concurrency=%v", concurrency))
			tk.MustExec(fmt.Sprintf("set @@tidb_streamagg_concurrency=%v", concurrency))
			distConcurrency := rand.Int31n(15) + 1
			tk.MustExec(fmt.Sprintf("set @@tidb_distsql_scan_concurrency=%v", distConcurrency))
			var err error
			for err == nil {
				res, err = tk.Exec(sql)
				if err == nil {
					_, err = session.GetRows4Test(context.Background(), tk.Session(), res)
					require.NoError(t, res.Close())
				}
			}
			require.EqualError(t, err, "failpoint panic: ERROR 1105 (HY000): Out Of Memory Quota![conn=1]")
		}
	}
}
