// Copyright 2021 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestSpillToDisk(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testCTEStorageSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testCTEStorageSpill"))
		tk.MustExec("set tidb_mem_quota_query = 1073741824;")
	}()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"))
	}()

	// Use duplicated rows to test UNION DISTINCT.
	tk.MustExec("set tidb_mem_quota_query = 1073741824;")
	insertStr := "insert into t1 values(0)"
	rowNum := 1000
	vals := make([]int, rowNum)
	vals[0] = 0
	for i := 1; i < rowNum; i++ {
		v := rand.Intn(100)
		vals[i] = v
		insertStr += fmt.Sprintf(", (%d)", v)
	}
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec(insertStr)
	tk.MustExec("set tidb_mem_quota_query = 40000;")
	tk.MustExec("set cte_max_recursion_depth = 500000;")
	sql := fmt.Sprintf("with recursive cte1 as ( "+
		"select c1 from t1 "+
		"union "+
		"select c1 + 1 c1 from cte1 where c1 < %d) "+
		"select c1 from cte1 order by c1;", rowNum)
	rows := tk.MustQuery(sql)

	memTracker := tk.Session().GetSessionVars().StmtCtx.MemTracker
	diskTracker := tk.Session().GetSessionVars().StmtCtx.DiskTracker
	require.Greater(t, memTracker.MaxConsumed(), int64(0))
	require.Greater(t, diskTracker.MaxConsumed(), int64(0))

	sort.Ints(vals)
	resRows := make([]string, 0, rowNum)
	for i := vals[0]; i <= rowNum; i++ {
		resRows = append(resRows, fmt.Sprintf("%d", i))
	}
	rows.Check(testkit.Rows(resRows...))
}
