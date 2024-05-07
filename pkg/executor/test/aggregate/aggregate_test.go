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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestHashAggRuntimeStat(t *testing.T) {
	partialInfo := &aggregate.AggWorkerInfo{
		Concurrency: 5,
	}
	finalInfo := &aggregate.AggWorkerInfo{
		Concurrency: 8,
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

func reconstructParallelGroupConcatResult(rows [][]any) []string {
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

	mustParseAndSort := func(rows [][]any, cmt string) []float64 {
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
	fpName3 := "github.com/pingcap/tidb/pkg/executor/join/ConsumeRandomPanic"
	require.NoError(t, failpoint.Enable(fpName3, "3%panic(\"ERROR 1105 (HY000): Out Of Memory Quota![conn=1]\")"))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName3))
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

func checkResults(actualRes [][]any, expectedRes map[string]string) bool {
	if len(actualRes) != len(expectedRes) {
		return false
	}

	var key string
	var expectVal string
	var actualVal string
	var ok bool
	for _, row := range actualRes {
		if len(row) != 2 {
			return false
		}

		key, ok = row[0].(string)
		if !ok {
			return false
		}

		expectVal, ok = expectedRes[key]
		if !ok {
			return false
		}

		actualVal, ok = row[1].(string)
		if !ok {
			return false
		}

		if expectVal != actualVal {
			return false
		}
	}
	return true
}

func genListPartition(begin, end int) string {
	buf := &bytes.Buffer{}
	buf.WriteString("(")
	for i := begin; i < end-1; i++ {
		buf.WriteString(fmt.Sprintf("%v, ", i))
	}
	buf.WriteString(fmt.Sprintf("%v)", end-1))
	return buf.String()
}

func TestParallelHashAgg(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.parallel_hash_agg;")
	tk.MustExec("create table test.parallel_hash_agg(k varchar(30), v int);")
	for i := 0; i < 20; i++ {
		tk.MustExec("insert into test.parallel_hash_agg (k, v) values ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), ('dD', 1), ('Dd', 1), ('ee', 1), ('EE', 1), ('eE', 1), ('Ee', 1);")
	}

	tk.MustExec("set @@tidb_max_chunk_size=32;")

	expectedResult := make(map[string]string)
	expectedResult["dd"] = "20"
	expectedResult["AA"] = "20"
	expectedResult["cc"] = "20"
	expectedResult["eE"] = "20"
	expectedResult["bb"] = "20"
	expectedResult["Cc"] = "20"
	expectedResult["EE"] = "20"
	expectedResult["Aa"] = "20"
	expectedResult["ee"] = "20"
	expectedResult["Bb"] = "20"
	expectedResult["dD"] = "20"
	expectedResult["aa"] = "20"
	expectedResult["cC"] = "20"
	expectedResult["DD"] = "20"
	expectedResult["BB"] = "20"
	expectedResult["Dd"] = "20"
	expectedResult["CC"] = "20"
	expectedResult["bB"] = "20"
	expectedResult["aA"] = "20"
	expectedResult["Ee"] = "20"
	res := tk.MustQuery("select k, sum(v) from parallel_hash_agg group by k;")
	tk.RequireEqual(true, checkResults(res.Rows(), expectedResult))

	tk.MustExec("create database list_partition_agg")
	tk.MustExec("use list_partition_agg")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tnormal (a int, b int)`)

	vals := ""
	for i := 0; i < 100; i++ {
		if vals != "" {
			vals += ", "
		}
		vals += fmt.Sprintf("(%v, %v)", rand.Intn(100), rand.Intn(100))
	}
	tk.MustExec(`insert into tnormal values ` + vals)
	tk.MustExec(`insert into tlist values ` + vals)

	for _, aggFunc := range []string{"min", "max", "sum", "count"} {
		c1, c2 := "a", "b"
		for i := 0; i < 2; i++ {
			rs := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tnormal group by %v`, c1, aggFunc, c2, c1)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			rsDynamic := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tlist group by %v`, c1, aggFunc, c2, c1)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			rsStatic := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tlist group by %v`, c1, aggFunc, c2, c1)).Sort()

			rs.Check(rsDynamic.Rows())
			rs.Check(rsStatic.Rows())
		}
	}
}

func TestIssue50849(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")
	tk.MustExec("insert into t select * from t;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/injectHashAggClosePanic", "return(true)"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/aggregate/injectHashAggClosePanic")
	rs, err := tk.ExecWithContext(context.Background(), "select  /*+hash_agg()*/ sum(t1.a) from t t1 join t t2;")
	require.NoError(t, err)
	err = rs.Close()
	// Check the error contains stack information
	require.True(t, errors.HasStack(err))
}
