// Copyright 2017 PingCAP, Inc.
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

package join_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func checkMergeAndRun(tk *testkit.TestKit, t *testing.T, sql string) *testkit.Result {
	explainedSQL := "explain format = 'brief' " + sql
	result := tk.MustQuery(explainedSQL)
	resultStr := fmt.Sprintf("%v", result.Rows())
	require.Contains(t, resultStr, "MergeJoin")
	return tk.MustQuery(sql)
}

func TestShuffleMergeJoinInDisk(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/testMergeJoinRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/testMergeJoinRowContainerSpill"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("set @@tidb_merge_join_concurrency=4;")
	tk.MustExec("set @@tidb_max_chunk_size=32;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3),(4,4)")
	for i := 1; i <= 1024; i += 4 {
		tk.MustExec(fmt.Sprintf("insert into t1 values(%v,%v),(%v,%v),(%v,%v),(%v,%v)", i, i, i+1, i+1, i+2, i+2, i+3, i+3))
	}
	result := checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")

	var expect []string
	expect = append(expect, "1 1 1 1")
	for i := 21; i <= 1024; i++ {
		expect = append(expect, fmt.Sprintf("%v %v <nil> <nil>", i, i))
	}
	sort.Strings(expect)
	result.Sort().Check(testkit.Rows(expect...))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().MemTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().MemTracker.MaxConsumed(), int64(0))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().DiskTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().DiskTracker.MaxConsumed(), int64(0))
}

func TestMergeJoinInDisk(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/testMergeJoinRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/testMergeJoinRowContainerSpill"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	tk.MustExec("use test")

	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("insert into t1 values(1,3),(4,4)")

	result := checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 3 1 1"))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(0))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.DiskTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.DiskTracker.MaxConsumed(), int64(0))
}

// TestVectorizedMergeJoin is used to test vectorized merge join with some corner cases.
//
//nolint:gosimple // generates false positive fmt.Sprintf warnings which keep aligned
func TestVectorizedMergeJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	existTableMap := make(map[string]struct{})
	runTest := func(ts1, ts2 []int) {
		getTable := func(prefix string, ts []int) string {
			tableName := prefix
			for _, i := range ts {
				tableName = tableName + "_" + strconv.Itoa(i)
			}
			if _, ok := existTableMap[tableName]; ok {
				return tableName
			}
			tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
			tk.MustExec(fmt.Sprintf("create table %s (a int, b int)", tableName))
			existTableMap[tableName] = struct{}{}
			for i, n := range ts {
				if n == 0 {
					continue
				}
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("insert into %v values ", tableName))
				for j := 0; j < n; j++ {
					if j > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(fmt.Sprintf("(%v, %v)", i, rand.Intn(10)))
				}
				tk.MustExec(buf.String())
			}
			return tableName
		}
		t1 := getTable("t", ts1)
		t2 := getTable("t", ts2)
		if t1 == t2 {
			t2 = getTable("t2", ts2)
		}

		tk.MustQuery(fmt.Sprintf("explain format = 'brief' select /*+ TIDB_SMJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Check(testkit.Rows(
			fmt.Sprintf(`MergeJoin 4150.01 root  inner join, left key:test.%s.a, right key:test.%s.a`, t1, t2),
			fmt.Sprintf(`├─Sort(Build) 3320.01 root  test.%s.a`, t2),
			`│ └─TableReader 3320.01 root  data:Selection`,
			fmt.Sprintf(`│   └─Selection 3320.01 cop[tikv]  lt(test.%s.b, 5), not(isnull(test.%s.a))`, t2, t2),
			fmt.Sprintf(`│     └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t2),
			fmt.Sprintf(`└─Sort(Probe) 3330.00 root  test.%s.a`, t1),
			`  └─TableReader 3330.00 root  data:Selection`,
			fmt.Sprintf(`    └─Selection 3330.00 cop[tikv]  gt(test.%s.b, 5), not(isnull(test.%s.a))`, t1, t1),
			fmt.Sprintf(`      └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t1),
		))
		tk.MustQuery(fmt.Sprintf("explain format = 'brief' select /*+ TIDB_HJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Check(testkit.Rows(
			fmt.Sprintf(`HashJoin 4150.01 root  inner join, equal:[eq(test.%s.a, test.%s.a)]`, t1, t2),
			`├─TableReader(Build) 3320.01 root  data:Selection`,
			fmt.Sprintf(`│ └─Selection 3320.01 cop[tikv]  lt(test.%s.b, 5), not(isnull(test.%s.a))`, t2, t2),
			fmt.Sprintf(`│   └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t2),
			`└─TableReader(Probe) 3330.00 root  data:Selection`,
			fmt.Sprintf(`  └─Selection 3330.00 cop[tikv]  gt(test.%s.b, 5), not(isnull(test.%s.a))`, t1, t1),
			fmt.Sprintf(`    └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t1),
		))

		r1 := tk.MustQuery(fmt.Sprintf("select /*+ TIDB_SMJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Sort()
		r2 := tk.MustQuery(fmt.Sprintf("select /*+ TIDB_HJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Sort()
		require.Equal(t, len(r2.Rows()), len(r1.Rows()))

		i := 0
		n := len(r1.Rows())
		for i < n {
			require.Equal(t, len(r2.Rows()[i]), len(r1.Rows()[i]))
			for j := range r1.Rows()[i] {
				require.Equal(t, r2.Rows()[i][j], r1.Rows()[i][j])
			}
			i += rand.Intn((n-i)/5+1) + 1 // just compare parts of results to speed up
		}
	}

	tk.Session().GetSessionVars().MaxChunkSize = variable.DefInitChunkSize
	chunkSize := tk.Session().GetSessionVars().MaxChunkSize
	cases := []struct {
		t1 []int
		t2 []int
	}{
		{[]int{0}, []int{chunkSize}},
		{[]int{0}, []int{chunkSize - 1}},
		{[]int{0}, []int{chunkSize + 1}},
		{[]int{1}, []int{chunkSize}},
		{[]int{1}, []int{chunkSize - 1}},
		{[]int{1}, []int{chunkSize + 1}},
		{[]int{chunkSize - 1}, []int{chunkSize}},
		{[]int{chunkSize - 1}, []int{chunkSize - 1}},
		{[]int{chunkSize - 1}, []int{chunkSize + 1}},
		{[]int{chunkSize}, []int{chunkSize}},
		{[]int{chunkSize}, []int{chunkSize + 1}},
		{[]int{chunkSize + 1}, []int{chunkSize + 1}},
		{[]int{1, 1, 1}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{0, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{chunkSize + 1, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
	}
	for _, ca := range cases {
		runTest(ca.t1, ca.t2)
		runTest(ca.t2, ca.t1)
	}
	fmt.Println(existTableMap)
	for tableName := range existTableMap {
		tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	}
}

// TestVectorizedShuffleMergeJoin is used to test vectorized shuffle merge join with some corner cases.
//
//nolint:gosimple // generates false positive fmt.Sprintf warnings which keep aligned
func TestVectorizedShuffleMergeJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_merge_join_concurrency = 4;")
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	existTableMap := make(map[string]struct{})
	runTest := func(ts1, ts2 []int) {
		getTable := func(prefix string, ts []int) string {
			tableName := prefix
			for _, i := range ts {
				tableName = tableName + "_" + strconv.Itoa(i)
			}
			if _, ok := existTableMap[tableName]; ok {
				return tableName
			}
			tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
			tk.MustExec(fmt.Sprintf("create table %s (a int, b int)", tableName))
			existTableMap[tableName] = struct{}{}
			for i, n := range ts {
				if n == 0 {
					continue
				}
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("insert into %v values ", tableName))
				for j := 0; j < n; j++ {
					if j > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(fmt.Sprintf("(%v, %v)", i, rand.Intn(10)))
				}
				tk.MustExec(buf.String())
			}
			return tableName
		}
		t1 := getTable("t", ts1)
		t2 := getTable("t", ts2)
		if t1 == t2 {
			t2 = getTable("t2", ts2)
		}

		tk.MustQuery(fmt.Sprintf("explain format = 'brief' select /*+ TIDB_SMJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Check(testkit.Rows(
			`Shuffle 4150.01 root  execution info: concurrency:4, data sources:[TableReader TableReader]`,
			fmt.Sprintf(`└─MergeJoin 4150.01 root  inner join, left key:test.%s.a, right key:test.%s.a`, t1, t2),
			fmt.Sprintf(`  ├─Sort(Build) 3320.01 root  test.%s.a`, t2),
			`  │ └─ShuffleReceiver 3320.01 root  `,
			`  │   └─TableReader 3320.01 root  data:Selection`,
			fmt.Sprintf(`  │     └─Selection 3320.01 cop[tikv]  lt(test.%s.b, 5), not(isnull(test.%s.a))`, t2, t2),
			fmt.Sprintf(`  │       └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t2),
			fmt.Sprintf(`  └─Sort(Probe) 3330.00 root  test.%s.a`, t1),
			`    └─ShuffleReceiver 3330.00 root  `,
			`      └─TableReader 3330.00 root  data:Selection`,
			fmt.Sprintf(`        └─Selection 3330.00 cop[tikv]  gt(test.%s.b, 5), not(isnull(test.%s.a))`, t1, t1),
			fmt.Sprintf(`          └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t1),
		))
		tk.MustQuery(fmt.Sprintf("explain format = 'brief' select /*+ TIDB_HJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Check(testkit.Rows(
			fmt.Sprintf(`HashJoin 4150.01 root  inner join, equal:[eq(test.%s.a, test.%s.a)]`, t1, t2),
			`├─TableReader(Build) 3320.01 root  data:Selection`,
			fmt.Sprintf(`│ └─Selection 3320.01 cop[tikv]  lt(test.%s.b, 5), not(isnull(test.%s.a))`, t2, t2),
			fmt.Sprintf(`│   └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t2),
			`└─TableReader(Probe) 3330.00 root  data:Selection`,
			fmt.Sprintf(`  └─Selection 3330.00 cop[tikv]  gt(test.%s.b, 5), not(isnull(test.%s.a))`, t1, t1),
			fmt.Sprintf(`    └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t1),
		))

		r1 := tk.MustQuery(fmt.Sprintf("select /*+ TIDB_SMJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Sort()
		r2 := tk.MustQuery(fmt.Sprintf("select /*+ TIDB_HJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Sort()
		require.Equal(t, len(r2.Rows()), len(r1.Rows()))

		i := 0
		n := len(r1.Rows())
		for i < n {
			require.Equal(t, len(r2.Rows()[i]), len(r1.Rows()[i]))
			for j := range r1.Rows()[i] {
				require.Equal(t, r2.Rows()[i][j], r1.Rows()[i][j])
			}
			i += rand.Intn((n-i)/5+1) + 1 // just compare parts of results to speed up
		}
	}

	tk.Session().GetSessionVars().MaxChunkSize = variable.DefInitChunkSize
	chunkSize := tk.Session().GetSessionVars().MaxChunkSize
	cases := []struct {
		t1 []int
		t2 []int
	}{
		{[]int{0}, []int{chunkSize}},
		{[]int{0}, []int{chunkSize - 1}},
		{[]int{0}, []int{chunkSize + 1}},
		{[]int{1}, []int{chunkSize}},
		{[]int{1}, []int{chunkSize - 1}},
		{[]int{1}, []int{chunkSize + 1}},
		{[]int{chunkSize - 1}, []int{chunkSize}},
		{[]int{chunkSize - 1}, []int{chunkSize - 1}},
		{[]int{chunkSize - 1}, []int{chunkSize + 1}},
		{[]int{chunkSize}, []int{chunkSize}},
		{[]int{chunkSize}, []int{chunkSize + 1}},
		{[]int{chunkSize + 1}, []int{chunkSize + 1}},
		{[]int{1, 1, 1}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{0, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{chunkSize + 1, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
	}
	for _, ca := range cases {
		runTest(ca.t1, ca.t2)
		runTest(ca.t2, ca.t1)
	}
}
