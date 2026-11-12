// Copyright 2019 PingCAP, Inc.
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

package materializedview

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

// TestPrepareMViewBuildTaskInDDLSession creates the import job of the build in
// a real DDL job and prepares its DXF task without the planner. The rest of the
// build runs through the store's ordinary path, because global sort needs real
// TiKV.
func TestPrepareMViewBuildTaskInDDLSession(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int)")
	tk.MustExec("insert into t values (1, 1), (1, 2), (2, 3)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-amz-bucket-region", "us-east-1")
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") != "":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
		case r.Method == http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
		default:
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(server.Close)
	params := url.Values{"endpoint": {server.URL}, "region": {"us-east-1"},
		"access-key": {"test"}, "secret-access-key": {"test"}}
	originalURI := vardef.CloudStorageURI.Load()
	vardef.CloudStorageURI.Store("s3://mv-build-sort?" + params.Encode())
	t.Cleanup(func() { vardef.CloudStorageURI.Store(originalURI) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/util/IsRaftKv2", "return(false)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMViewBuildImportJob", "return")

	var (
		task       *proto.Task
		prepareErr error
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/prepareCreateMaterializedViewBuildTask",
		func(prepare func() (*proto.Task, error)) {
			task, prepareErr = prepare()
		})
	tk.MustExec("create materialized view mv (a, c) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t group by a")
	require.NoError(t, prepareErr)
	require.NotNil(t, task)
	require.Equal(t, proto.ImportInto, task.Type)
	require.Equal(t, 1, task.MaxNodeCount)

	var meta importer.TaskMeta
	require.NoError(t, json.Unmarshal(task.Meta, &meta))
	require.Contains(t, meta.Stmt, "disable_precheck")
	require.True(t, meta.Plan.IsGlobalSort())
	ddlJobID := findLatestDDLHistoryJobID(t, tk, model.ActionCreateMaterializedView)
	// The DDL job created the import job, and the precheck ignored it.
	tk.MustQuery("select id, status from mysql.tidb_import_jobs where group_key = ?", fmt.Sprintf("mview-build-%d", ddlJobID)).
		Check(testkit.Rows(fmt.Sprintf("%d pending", meta.JobID)))
	is := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	base, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	mv, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	require.Equal(t, mv.Meta().ID, meta.Plan.TableInfo.ID)
	query := meta.Plan.QuerySource
	require.True(t, query.InMViewMaintenance)
	require.Equal(t, []importer.SourceTable{{DBID: mv.Meta().DBID, ID: base.Meta().ID, Revision: base.Meta().Revision}}, query.Tables)
	tk.MustQuery("select count(*) from mysql.tidb_global_task").Check(testkit.Rows("0"))

	// A worker can run the prepared query, and rows after the snapshot stay out.
	tk.MustExec("insert into t values (3, 4)")
	workerTK := testkit.NewTestKit(t, store)
	output := make(chan importer.QueryChunk, 4)
	done := make(chan error, 1)
	go func() {
		done <- executor.RunImportQuery(ctx, workerTK.Session(), query, meta.Stmt, 1<<30, output)
		close(output)
	}()
	rows := 0
	for chk := range output {
		rows += chk.Chk.NumRows()
	}
	require.NoError(t, <-done)
	require.Equal(t, 2, rows)
}

// TestRollbackMViewBuildCancelsImportJob checks that rolling back the DDL job
// cancels an import job that has no task yet.
func TestRollbackMViewBuildCancelsImportJob(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int)")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMViewBuildImportJob", "return")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return")
	err := tk.ExecToErr("create materialized view mv (a, c) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t group by a")
	require.ErrorContains(t, err, "mock create materialized view build error")
	tk.MustQuery("select status from mysql.tidb_import_jobs where group_key like 'mview-build-%'").Check(testkit.Rows("cancelled"))
}
