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
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
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

func TestBuildQueryImportPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table src(a int, b int)")
	tk.MustExec("create table dst(a int, c bigint)")
	backend := s3mem.New()
	require.NoError(t, backend.CreateBucket("query-import-sort"))
	server := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(server.Close)
	originalURI := vardef.CloudStorageURI.Load()
	vardef.CloudStorageURI.Store(fmt.Sprintf("s3://query-import-sort?region=us-east-1&endpoint=%s&access-key=test&secret-access-key=test", server.URL))
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
	directTK.Session().GetSessionVars().ResourceGroupName = "rg1"
	direct, err := importer.BuildQueryImportPlan(ctx, directTK.Session(), request)
	require.NoError(t, err)
	txn, err := directTK.Session().Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	require.Equal(t, dstTbl.Meta().ID, direct.TableInfo.ID)
	require.GreaterOrEqual(t, direct.QuerySource.ReadTS, before.Ver)
	require.Equal(t, "rg1", direct.QuerySource.SessionStates.ResourceGroupName)
	require.Equal(t, []importer.SourceTable{{DBID: request.DBID, ID: srcTbl.Meta().ID,
		Revision: srcTbl.Meta().Revision}}, direct.QuerySource.Tables)

	t.Run("preserves source table order and missing entries", func(t *testing.T) {
		tk.MustExec("create table src_other(a int, b int)")
		latest := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
		other, err := latest.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("src_other"))
		require.NoError(t, err)
		req := *request
		req.SQL = "import into test.dst from (select a, b from test.src_other union all select a, b from test.src) with disable_precheck"
		req.SourceTableIDs = []int64{other.Meta().ID, srcTbl.Meta().ID, other.Meta().ID}
		plan, err := importer.BuildQueryImportPlan(ctx, newQueryImportSession(t, store).Session(), &req)
		require.NoError(t, err)
		require.Len(t, plan.QuerySource.Tables, len(req.SourceTableIDs))
		for i, id := range req.SourceTableIDs {
			require.Equal(t, id, plan.QuerySource.Tables[i].ID)
		}
		req.SourceTableIDs[1] = 1 << 40
		_, err = importer.BuildQueryImportPlan(ctx, newQueryImportSession(t, store).Session(), &req)
		require.ErrorContains(t, err, "source table 1099511627776 not found")
	})

	t.Run("rejects memory system tables before metadata reads", func(t *testing.T) {
		for _, name := range []string{"tables", "cluster_processlist"} {
			tbl, err := is.TableByName(ctx, ast.NewCIStr("information_schema"), ast.NewCIStr(name))
			require.NoError(t, err)
			req := *request
			req.SQL = "import into test.dst from (select * from information_schema." + name + ") with disable_precheck"
			req.SourceTableIDs = []int64{tbl.Meta().ID}
			_, err = importer.BuildQueryImportPlan(ctx, newQueryImportSession(t, store).Session(), &req)
			require.ErrorContains(t, err, "virtual source tables in IMPORT INTO FROM SELECT")
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
			_, err = importer.BuildQueryImportPlan(ctx, newQueryImportSession(t, store).Session(), &req)
			require.ErrorContains(t, err, tc.message)
		}
	})
}
