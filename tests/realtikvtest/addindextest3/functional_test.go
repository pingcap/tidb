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
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
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
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
	tbl, err = dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
	discovery := store.(tikv.Storage).GetRegionCache().PDClient().GetServiceDiscovery()
	bCtx, err := ingest.LitBackCtxMgr.Register(context.Background(), 1, false, nil, discovery, "test", 1, 0, 0)
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
	for i := 0; i < 3; i++ {
		go func() {
			err := bCtx.FinishAndUnregisterEngines(ingest.OptCloseEngines)
			require.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
	ingest.LitBackCtxMgr.Unregister(1)
}

func TestMockMemoryUsedUp(t *testing.T) {
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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
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
