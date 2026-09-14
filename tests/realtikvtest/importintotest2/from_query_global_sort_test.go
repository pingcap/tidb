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
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestImportFromQueryGlobalSortNextGen(t *testing.T) {
	if !kerneltype.IsNextGen() || !*realtikvtest.WithRealTiKV {
		t.Skip("requires a real NextGen TiKV/TiKV-worker/S3 cluster")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_import_src(g bigint, v decimal(20,2), i bigint)")
	tk.MustExec("insert into query_import_src values (1,10,1),(1,20,2),(1,NULL,3),(2,7,4),(NULL,3,5),(NULL,NULL,6),(3,NULL,7)")
	tk.MustExec("analyze table query_import_src all columns")
	tk.MustExec("create table query_import_dst(g bigint, c bigint, cv bigint, s decimal(42,2), lo decimal(20,2), hi decimal(20,2), key(g))")
	sortURI := realtikvtest.GetNextGenObjStoreURI("query-import-" + uuid.NewString())
	previousURI := vardef.CloudStorageURI.Load()
	vardef.CloudStorageURI.Store(sortURI)
	t.Cleanup(func() { vardef.CloudStorageURI.Store(previousURI) })
	query := "select g,count(*),count(v),sum(v),min(v),max(v) from query_import_src where i>0 group by g"
	tk.MustExec(fmt.Sprintf("import into query_import_dst from (%s) with thread=2", query))
	tk.MustQuery("select * from query_import_dst order by g is not null,g").Check(testkit.Rows(
		"<nil> 2 1 3.00 3.00 3.00", "1 3 2 30.00 10.00 20.00", "2 1 1 7.00 7.00 7.00", "3 1 0 <nil> <nil> <nil>",
	))
	jobID := tk.MustQuery("select max(id) from mysql.tidb_import_jobs where table_name='query_import_dst'").Rows()[0][0]
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	mgr, err := storage.GetDXFSvcTaskMgr()
	require.NoError(t, err)
	var id int64
	_, err = fmt.Sscan(fmt.Sprint(jobID), &id)
	require.NoError(t, err)
	task, err := mgr.GetTaskByKeyWithHistory(ctx, importinto.TaskKey(id))
	require.NoError(t, err)
	require.Equal(t, 1, task.MaxNodeCount)
	var meta importinto.TaskMeta
	require.NoError(t, json.Unmarshal(task.Meta, &meta))
	require.NotNil(t, meta.Plan.Query)
	require.Contains(t, meta.Plan.Query.SQL, query)
	require.EqualValues(t, 4, meta.Summary.ImportedRows)
}
