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

	before, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	directTK := newQueryImportSession(t, store)
	direct, err := importer.PrepareQueryImport(ctx, directTK.Session(), request)
	require.NoError(t, err)
	txn, err := directTK.Session().Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	require.Equal(t, 1, direct.MaxNodeCnt)
	require.Equal(t, "test", direct.DBName)
	require.Equal(t, dstTbl.Meta().ID, direct.TableInfo.ID)
	require.Equal(t, importer.DataSourceTypeQuery, direct.DataSourceType)
	require.True(t, direct.DisablePrecheck)
	require.EqualValues(t, 1<<30, direct.DiskQuota)
	require.Positive(t, direct.ThreadCnt)
	require.GreaterOrEqual(t, direct.QuerySource.ReadTS, before.Ver)
	require.NotZero(t, direct.QuerySource.SchemaVersion)
	require.NotZero(t, direct.QuerySource.EncodeTimestamp)
	require.True(t, direct.QuerySource.InMViewMaintenance)
	require.NotEmpty(t, direct.QuerySource.SessionStates.SystemVars[vardef.Timestamp])
	require.Equal(t, []importer.SourceTable{{DBID: request.DBID, ID: srcTbl.Meta().ID,
		Revision: srcTbl.Meta().Revision}}, direct.QuerySource.Tables)

	t.Run("keeps the resource group of the session", func(t *testing.T) {
		// Without a planned statement, the statement context holds no group.
		se := newQueryImportSession(t, store).Session()
		se.GetSessionVars().ResourceGroupName = "rg1"
		plan, err := importer.PrepareQueryImport(ctx, se, request)
		require.NoError(t, err)
		require.Equal(t, "rg1", plan.QuerySource.SessionStates.ResourceGroupName)
	})

	t.Run("preserves source table order and missing entries", func(t *testing.T) {
		tk.MustExec("create table src_other(a int, b int)")
		latest := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
		other, err := latest.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("src_other"))
		require.NoError(t, err)
		req := *request
		req.SQL = "import into test.dst from (select a, b from test.src_other union all select a, b from test.src) with disable_precheck"
		req.SourceTableIDs = []int64{other.Meta().ID, srcTbl.Meta().ID, other.Meta().ID}
		plan, err := importer.PrepareQueryImport(ctx, newQueryImportSession(t, store).Session(), &req)
		require.NoError(t, err)
		require.Len(t, plan.QuerySource.Tables, len(req.SourceTableIDs))
		for i, id := range req.SourceTableIDs {
			require.Equal(t, id, plan.QuerySource.Tables[i].ID)
		}
		req.SourceTableIDs[1] = 1 << 40
		_, err = importer.PrepareQueryImport(ctx, newQueryImportSession(t, store).Session(), &req)
		require.ErrorContains(t, err, "source table 1099511627776 not found")
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

	t.Run("caller supplies view dependencies", func(t *testing.T) {
		tk.MustExec("create view src_view as select a, b from src")
		latest := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
		view, err := latest.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("src_view"))
		require.NoError(t, err)
		for _, complete := range []bool{true, false} {
			req := *request
			req.SQL = "import into test.dst from (select * from test.src_view) with disable_precheck"
			req.SourceTableIDs = []int64{view.Meta().ID}
			if complete {
				req.SourceTableIDs = append(req.SourceTableIDs, srcTbl.Meta().ID)
			}
			plan, err := importer.PrepareQueryImport(ctx, newQueryImportSession(t, store).Session(), &req)
			require.NoError(t, err)
			worker := newWorkerSession(t, store)
			if complete {
				require.Equal(t, []string{"[1 1]"}, runImportQuery(t, worker, plan.QuerySource, req.SQL))
			} else {
				err := executor.RunImportQuery(ctx, worker, plan.QuerySource, req.SQL, importQueryMemQuota, make(chan importer.QueryChunk, 1))
				require.Error(t, err, "missing dependencies must fail during execution")
			}
		}
	})

	t.Run("rejects unreadable sources", func(t *testing.T) {
		tk.MustExec("create sequence src_seq")
		is := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
		for _, tc := range []struct {
			source, sql, message string
		}{
			{source: "src_seq", message: "sequences"},
			{source: "src", sql: "import into test.dst from (select nextval(src_seq), 1)", message: "sequences"},
		} {
			tbl, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr(tc.source))
			require.NoError(t, err)
			req := *request
			req.SourceTableIDs = []int64{tbl.Meta().ID}
			if tc.sql != "" {
				req.SQL = tc.sql
			}
			_, err = importer.PrepareQueryImport(ctx, newQueryImportSession(t, store).Session(), &req)
			require.ErrorContains(t, err, tc.message)
		}
	})
}
