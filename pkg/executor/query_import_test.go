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

package executor_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func newQueryImportSession(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().InMViewMaintenance = true
	return tk
}

// normalizedQueryImportPlan removes the values that differ between two captures.
func normalizedQueryImportPlan(t *testing.T, plan *importer.Plan) map[string]any {
	data, err := json.Marshal(plan)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(data, &fields))
	query := fields["QuerySource"].(map[string]any)
	delete(query, "ReadTS")
	delete(query, "EncodeTimestamp")
	states := query["SessionStates"].(map[string]any)
	delete(states, "affected-rows")
	delete(states, "found-rows")
	delete(states, "txn-info")
	// A worker replaces the status with its own, see QuerySource.RestoreStates.
	delete(states, "status")
	sysVars := states["sys-vars"].(map[string]any)
	for _, name := range []string{"rand_seed1", "rand_seed2", "timestamp"} {
		delete(sysVars, name)
	}
	return fields
}

func TestPrepareQueryImport(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table src(a int, b int)")
	tk.MustExec("create table dst(a int, c bigint)")
	tk.MustExec("create table full_dst(a int, c bigint)")
	tk.MustExec("insert into src values (1, 1)")
	tk.MustExec("insert into full_dst values (1, 1)")
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
	vardef.CloudStorageURI.Store("s3://query-import-sort?" + params.Encode())
	t.Cleanup(func() { vardef.CloudStorageURI.Store(originalURI) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/util/IsRaftKv2", "return(false)")
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)

	const sql = "import into test.dst from (select a, count(1) c from test.src group by a) with disable_precheck, thread=2, disk_quota='1GiB'"
	is := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
	dstTbl, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("dst"))
	require.NoError(t, err)
	srcTbl, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("src"))
	require.NoError(t, err)
	request := &importer.QueryImportRequest{
		SQL: sql, DBName: "test", DBID: dstTbl.Meta().DBID, Table: dstTbl,
		SourceTableIDs: []int64{srcTbl.Meta().ID},
	}

	directTK := newQueryImportSession(t, store)
	direct, err := importer.PrepareQueryImport(ctx, directTK.Session(), request)
	require.NoError(t, err)
	txn, err := directTK.Session().Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	require.Equal(t, 1, direct.MaxNodeCnt)

	t.Run("matches a planned statement", func(t *testing.T) {
		plannedTK := newQueryImportSession(t, store)
		// A transaction gives the session a context provider, as a running statement has.
		plannedTK.MustExec("begin")
		se := plannedTK.Session()
		nodes, err := se.Parse(ctx, sql)
		require.NoError(t, err)
		require.NoError(t, executor.ResetContextOfStmt(se, nodes[0]))
		stmt, err := (&executor.Compiler{Ctx: se}).Compile(ctx, nodes[0])
		require.NoError(t, err)
		physical := stmt.Plan.(*plannercore.ImportInto)
		planned, err := executor.NewImportPlan(ctx, se, physical, dstTbl)
		require.NoError(t, err)
		controller, err := importer.NewLoadDataController(planned, dstTbl, &importer.ASTArgs{
			FileLocRef:         ast.FileLocServerOrRemote,
			ColumnsAndUserVars: physical.ColumnsAndUserVars,
			ColumnAssignments:  physical.ColumnAssignments,
			OnDuplicate:        ast.OnDuplicateKeyHandlingReplace,
		})
		require.NoError(t, err)
		require.NoError(t, executor.PlanImportQueryForTest(se, sql))
		query, err := importer.CaptureQuerySource(se, sql)
		require.NoError(t, err)
		require.NoError(t, controller.SetQuerySource(ctx, query))
		se.RollbackTxn(ctx)
		require.Equal(t, normalizedQueryImportPlan(t, planned), normalizedQueryImportPlan(t, direct))
	})

	t.Run("a worker reads the captured snapshot", func(t *testing.T) {
		tk.MustExec("insert into src values (2, 2), (3, 3)")
		workerTK := testkit.NewTestKit(t, store)
		output := make(chan importer.QueryChunk, 4)
		done := make(chan error, 1)
		go func() {
			done <- executor.RunImportQuery(ctx, workerTK.Session(), direct.QuerySource, sql, 1<<30, output)
			close(output)
		}()
		rows := 0
		for chk := range output {
			rows += chk.Chk.NumRows()
		}
		require.NoError(t, <-done)
		require.Equal(t, 1, rows)
	})

	t.Run("keeps the resource group of the session", func(t *testing.T) {
		// Without a planned statement, the statement context holds no group.
		se := newQueryImportSession(t, store).Session()
		se.GetSessionVars().ResourceGroupName = "rg1"
		version, err := store.CurrentVersion(kv.GlobalTxnScope)
		require.NoError(t, err)
		query, err := importer.NewQuerySourceAtSnapshot(se, version.Ver, dstTbl.Meta().DBID, []int64{srcTbl.Meta().ID})
		require.NoError(t, err)
		require.Equal(t, "rg1", query.SessionStates.ResourceGroupName)
	})

	t.Run("runs the import prechecks", func(t *testing.T) {
		fullTbl, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("full_dst"))
		require.NoError(t, err)
		_, err = importer.PrepareQueryImport(ctx, newQueryImportSession(t, store).Session(), &importer.QueryImportRequest{
			SQL:    "import into test.full_dst from (select a, count(1) c from test.src group by a) with disable_precheck",
			DBName: "test", DBID: fullTbl.Meta().DBID, Table: fullTbl, SourceTableIDs: []int64{srcTbl.Meta().ID},
		})
		require.ErrorContains(t, err, "target table is not empty")
	})

	t.Run("rejects unreadable sources", func(t *testing.T) {
		tk.MustExec("create view src_view as select a from src")
		tk.MustExec("create sequence src_seq")
		is := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
		dbID := dstTbl.Meta().DBID
		version, err := store.CurrentVersion(kv.GlobalTxnScope)
		require.NoError(t, err)
		for name, message := range map[string]string{"src_view": "views", "src_seq": "sequences"} {
			tbl, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr(name))
			require.NoError(t, err)
			_, err = importer.NewQuerySourceAtSnapshot(tk.Session(), version.Ver, dbID, []int64{tbl.Meta().ID})
			require.ErrorContains(t, err, message)
		}
		_, err = importer.NewQuerySourceAtSnapshot(tk.Session(), version.Ver, dbID, []int64{1 << 40})
		require.ErrorContains(t, err, "not found")
		_, err = importer.PrepareQueryImport(ctx, newQueryImportSession(t, store).Session(), &importer.QueryImportRequest{
			SQL:    "import into test.dst from (select nextval(src_seq), 1)",
			DBName: "test", DBID: dbID, Table: dstTbl,
		})
		require.ErrorContains(t, err, "sequences")
	})
}
