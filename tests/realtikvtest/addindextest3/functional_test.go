// Copyright 2024 PingCAP, Inc.
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

package addindextest

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestDDLTestEstimateTableRowSize(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1);")

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "estimate_row_size")
	tkSess := tk.Session()
	exec := tkSess.GetRestrictedSQLExecutor()
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	size := ddl.EstimateTableRowSizeForTest(ctx, store, exec, tbl)
	require.Equal(t, 0, size) // No data in information_schema.columns.
	tk.MustExec("analyze table t all columns;")
	size = ddl.EstimateTableRowSizeForTest(ctx, store, exec, tbl)
	require.Equal(t, 16, size)

	tk.MustExec("alter table t add column c varchar(255);")
	tk.MustExec("update t set c = repeat('a', 50) where a = 1;")
	tk.MustExec("analyze table t all columns;")
	size = ddl.EstimateTableRowSizeForTest(ctx, store, exec, tbl)
	require.Equal(t, 67, size)

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (id bigint primary key, b text) partition by hash(id) partitions 4;")
	for i := 1; i < 10; i++ {
		insertSQL := fmt.Sprintf("insert into t values (%d, repeat('a', 10))", i*10000)
		tk.MustExec(insertSQL)
	}
	tk.MustQuery("split table t between (0) and (1000000) regions 2;").Check(testkit.Rows("4 1"))
	tk.MustExec("set global tidb_analyze_skip_column_types=`json,blob,mediumblob,longblob`")
	tk.MustExec("analyze table t all columns;")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	size = ddl.EstimateTableRowSizeForTest(ctx, store, exec, tbl)
	require.Equal(t, 19, size)
	ptbl := tbl.GetPartitionedTable()
	pids := ptbl.GetAllPartitionIDs()
	for _, pid := range pids {
		partition := ptbl.GetPartition(pid)
		size = ddl.EstimateTableRowSizeForTest(ctx, store, exec, partition)
		require.Equal(t, 19, size)
	}
}

func TestBackendCtxConcurrentUnregister(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int);")
	var realJob *model.Job
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.State == model.JobStateDone && job.Type == model.ActionAddIndex {
			realJob = job.Clone()
		}
	})
	tk.MustExec("alter table t add index idx(a);")
	require.NotNil(t, realJob)

	cfg, bd, err := ingest.CreateLocalBackend(context.Background(), store, realJob, false, 0)
	require.NoError(t, err)
	bCtx, err := ingest.NewBackendCtxBuilder(context.Background(), store, realJob).Build(cfg, bd)
	require.NoError(t, err)
	idxIDs := []int64{1, 2, 3, 4, 5, 6, 7}
	uniques := make([]bool, 0, len(idxIDs))
	for range idxIDs {
		uniques = append(uniques, false)
	}
	_, err = bCtx.Register([]int64{1, 2, 3, 4, 5, 6, 7}, uniques, tables.MockTableFromMeta(&model.TableInfo{}))
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(3)
	for range 3 {
		go func() {
			err := bCtx.FinishAndUnregisterEngines(ingest.OptCloseEngines)
			require.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
	bCtx.Close()
	bd.Close()
}

func TestMockMemoryUsedUp(t *testing.T) {
	t.Skip("TODO(tangenta): support memory tracking later")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/ingest/setMemTotalInMB", "return(100)")
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (c int, c2 int, c3 int, c4 int);")
	tk.MustExec("insert into t values (1,1,1,1), (2,2,2,2), (3,3,3,3);")
	tk.MustGetErrMsg("alter table t add index i(c), add index i2(c2);", "[ddl:8247]Ingest failed: memory used up")
}

func TestTiDBEncodeKeyTempIndexKey(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int);")
	tk.MustExec("insert into t values (1, 1);")
	runDML := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if !runDML && job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteOnly {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			tk2.MustExec("insert into t values (2, 2);")
			runDML = true
		}
	})
	tk.MustExec("create index idx on t(b);")
	require.True(t, runDML)

	rows := tk.MustQuery("select tidb_mvcc_info(tidb_encode_index_key('test', 't', 'idx', 1, 1));").Rows()
	rs := rows[0][0].(string)
	require.Equal(t, 1, strings.Count(rs, "writes"), rs)
	rows = tk.MustQuery("select tidb_mvcc_info(tidb_encode_index_key('test', 't', 'idx', 2, 2));").Rows()
	rs = rows[0][0].(string)
	require.Equal(t, 2, strings.Count(rs, "writes"), rs)
}

func TestAddIndexPresplitIndexRegions(t *testing.T) {
	testutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var splitKeyHex [][]byte
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex", func(splitKeys [][]byte) {
		for _, k := range splitKeys {
			splitKeyHex = append(splitKeyHex, bytes.Clone(k))
		}
	})
	checkSplitKeys := func(idxID int64, count int, reset bool) {
		cnt := 0
		for _, k := range splitKeyHex {
			indexID, err := tablecodec.DecodeIndexID(k)
			if err == nil && indexID == idxID {
				cnt++
			}
		}
		require.Equal(t, count, cnt, splitKeyHex)
		if reset {
			splitKeyHex = nil
		}
	}
	var idxID int64
	nextIdxID := func() int64 {
		idxID++
		return idxID
	}
	resetIdxID := func() {
		idxID = 0
	}

	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 10 {
		insertSQL := fmt.Sprintf("insert into t values (%[1]d, %[1]d);", 10000*i)
		tk.MustExec(insertSQL)
	}
	retRows := tk.MustQuery("show table t regions;").Rows()
	require.Len(t, retRows, 1)
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = off;")
	tk.MustExec("set @@global.tidb_enable_dist_task = off;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = 4;")
	checkSplitKeys(nextIdxID(), 3, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));")
	checkSplitKeys(nextIdxID(), 3, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("alter table t add index idx(b) /*T![pre_split] pre_split_regions = (by (10000), (20000), (30000)) */;")
	checkSplitKeys(nextIdxID(), 3, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);")
	checkSplitKeys(nextIdxID(), 2, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on;")

	tk.MustExec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));")
	nextID := nextIdxID()
	checkSplitKeys(nextID, 0, false)
	checkSplitKeys(tablecodec.TempIndexPrefix|nextID, 3, true)

	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = off;")

	// Test partition tables.
	resetIdxID()
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int primary key, b int) partition by hash(a) partitions 4;")
	for i := range 10 {
		insertSQL := fmt.Sprintf("insert into t values (%[1]d, %[1]d);", 10000*i)
		tk.MustExec(insertSQL)
	}
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));")
	checkSplitKeys(nextIdxID(), 3*4, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);")
	checkSplitKeys(nextIdxID(), 2*4, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));")
	checkSplitKeys(nextIdxID(), 0, false)
	checkSplitKeys(tablecodec.TempIndexPrefix|3, 12, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = off;")

	resetIdxID()
	tk.MustExec("drop table t;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on;")
	tk.MustExec("set @@global.tidb_enable_dist_task = off;")
	tk.MustExec("create table t (a int, b int) partition by range (b)" +
		" (partition p0 values less than (10), " +
		"  partition p1 values less than (maxvalue));")
	tk.MustExec("alter table t add unique index p_a (a) global pre_split_regions = (by (5), (15));")
	checkSplitKeys(tablecodec.TempIndexPrefix|nextIdxID(), 2, true)
}

func TestAddIndexPresplitFunctional(t *testing.T) {
	testutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int);")

	tk.MustGetErrMsg("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 0);",
		"Split index region num should be greater than 0")
	tk.MustGetErrMsg("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 10000);",
		"Split index region num exceeded the limit 1000")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockSplitIndexRegionAndWaitErr", "2*return")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockSplitIndexRegionAndWaitErr")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a bigint primary key, b int);")
	tk.MustExec("insert into t values (1, 1), (10, 1);")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (between (1) and (2) regions 3);")
	tk.MustExec("drop table t;")
}
