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

package importintotest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestImportFromQueryGlobalSort(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires a real TiKV cluster")
	}
	if kerneltype.IsClassic() {
		t.Skip("classic IMPORT FROM SELECT uses the local executor")
	}
	sortURI := realtikvtest.GetNextGenObjStoreURI("query-import-" + uuid.NewString())
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_import_src(g bigint, v decimal(20,2), i bigint,key idx_i(i))")
	tk.MustExec("insert into query_import_src values (1,10,1),(1,20,2),(1,NULL,3),(2,7,4),(NULL,3,5),(NULL,NULL,6),(3,NULL,7)")
	tk.MustExec("analyze table query_import_src all columns")
	previousURI := vardef.CloudStorageURI.Load()
	tk.MustExec("set global tidb_cloud_storage_uri = ?", sortURI)
	t.Cleanup(func() {
		tk.MustExec("set global tidb_cloud_storage_uri = ?", previousURI)
	})
	// A background sysvar refresh must retain the configured storage URI.
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache(true)
	require.Equal(t, sortURI, vardef.CloudStorageURI.Load())
	query := "select /*+ STREAM_AGG() */ i,count(*),count(v),sum(v),min(v),max(v) from query_import_src use index(idx_i) where i>0 group by i"
	target := "query_import_dst"
	tk.MustExec(fmt.Sprintf("create table %s(g bigint, c bigint, cv bigint, s decimal(42,2), lo decimal(20,2), hi decimal(20,2), key(g))", target))
	tk.MustQuery("select count(*) from mysql.tidb_import_jobs where table_name=?", target).Check(testkit.Rows("0"))
	var readTS, processTS atomic.Uint64
	var encodeTasks atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/importinto/syncBeforeSortChunk", func() {
		encodeTasks.Add(1)
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
		readTS.Store(p.SCtx().GetSessionVars().SnapshotTS)
		processTS.Store(tk.Session().ShowProcess().CurTxnStartTS)
	})
	rs, err := tk.Exec(fmt.Sprintf("import into %s from (%s) with thread=2", target, query))
	require.NoError(t, err)
	if rs != nil {
		t.Cleanup(func() { require.NoError(t, rs.Close()) })
	}
	require.Nil(t, rs)
	require.EqualValues(t, 2, encodeTasks.Load())
	require.NotZero(t, readTS.Load())
	require.Equal(t, readTS.Load(), processTS.Load())
	require.Equal(t, readTS.Load(), tk.Session().GetSessionVars().LastQueryInfo.StartTS)
	require.EqualValues(t, 7, tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	message := tk.Session().GetSessionVars().StmtCtx.GetMessage()
	tk.MustQuery(fmt.Sprintf("select * from %s order by g is not null,g", target)).Check(testkit.Rows(
		"1 1 1 10.00 10.00 10.00", "2 1 1 20.00 20.00 20.00", "3 1 0 <nil> <nil> <nil>",
		"4 1 1 7.00 7.00 7.00", "5 1 1 3.00 3.00 3.00", "6 1 0 <nil> <nil> <nil>", "7 1 0 <nil> <nil> <nil>",
	))
	jobID := tk.MustQuery("select max(id) from mysql.tidb_import_jobs where table_name=?", target).Rows()[0][0]
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	mgr, err := storage.GetDXFSvcTaskMgr()
	require.NoError(t, err)
	var id int64
	_, err = fmt.Sscan(fmt.Sprint(jobID), &id)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("Records: 7, ID: %d", id), message)
	task, err := mgr.GetTaskByKeyWithHistory(ctx, importinto.TaskKey(id))
	require.NoError(t, err)
	require.Equal(t, 1, task.MaxNodeCount)
	var meta importinto.TaskMeta
	require.NoError(t, json.Unmarshal(task.Meta, &meta))
	require.NotNil(t, meta.Plan.Query)
	require.Equal(t, readTS.Load(), meta.Plan.Query.ReadTS)
	require.Nil(t, meta.ChunkMap)
	require.EqualValues(t, 7, meta.Summary.ImportedRows)
	require.Empty(t, meta.EligibleInstances)
}
