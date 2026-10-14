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

package executor_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGenerateImportQueryPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_src(i bigint)")
	t.Run("source tables", func(t *testing.T) {
		tk.MustExec("create table query_other(i int)")
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		q, err := executor.GenerateImportQueryPlan(tk.Session(), "import into unused_target from (select i from query_src union all select i from query_other)")
		require.NoError(t, err)
		require.Len(t, q.Databases, 1)
		db, ok := tk.Session().GetLatestInfoSchema().SchemaByName(ast.NewCIStr("test"))
		require.True(t, ok)
		require.Contains(t, q.Databases, db.ID)
		var tables []string
		for _, tbl := range q.Tables[db.ID] {
			tables = append(tables, tbl.Name.L)
		}
		require.ElementsMatch(t, []string{"query_src", "query_other"}, tables)
		tk.MustExec("create temporary table query_tmp(i int)")
		tk.MustExec("create global temporary table query_gtmp(i int) on commit delete rows")
		for _, tbl := range []string{"query_tmp", "query_gtmp"} {
			_, err = executor.GenerateImportQueryPlan(tk.Session(), "import into unused_target from select * from "+tbl)
			require.ErrorContains(t, err, "temporary source tables in IMPORT INTO FROM SELECT")
		}
		_, err = executor.GenerateImportQueryPlan(tk.Session(), "import into unused_target from select ? from query_src")
		require.ErrorContains(t, err, "unexpected '?'")
	})
	t.Run("views and cached tables", func(t *testing.T) {
		tk.MustExec("create database query_view_db")
		tk.MustExec("create table query_view_db.base(i int)")
		tk.MustExec("create table query_cached(i int)")
		tk.MustExec("alter table query_cached cache")
		tk.MustExec("use query_view_db")
		tk.MustExec("create view inner_v as select i from base")
		tk.MustExec("use test")
		tk.MustExec("create view outer_v as select i from query_view_db.inner_v union all select i from query_cached")
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		q, err := executor.GenerateImportQueryPlan(tk.Session(), "import into unused_target from select * from outer_v")
		require.NoError(t, err)
		tables := make(map[string]*model.TableInfo)
		for dbID, tbls := range q.Tables {
			for _, tbl := range tbls {
				tables[q.Databases[dbID].Name.L+"."+tbl.Name.L] = tbl
			}
		}
		require.Len(t, tables, 4)
		require.True(t, tables["test.outer_v"].IsView())
		require.True(t, tables["query_view_db.inner_v"].IsView())
		require.Contains(t, tables, "query_view_db.base")
		require.Contains(t, tables, "test.query_cached")
	})
	t.Run("capture transaction schema", func(t *testing.T) {
		ddlTK := testkit.NewTestKit(t, store)
		ddlTK.MustExec("use test")
		ddlTK.MustExec("create table query_capture_schema(i int)")
		queryTK := testkit.NewTestKit(t, store)
		queryTK.MustExec("use test")
		queryTK.MustQuery("select * from query_capture_schema").Check(testkit.Rows())
		require.NoError(t, queryTK.Session().PrepareTxnCtx(context.Background(), nil))
		// Capture runs inside the IMPORT INTO statement, which is not read-only.
		queryTK.Session().GetSessionVars().StmtCtx.IsReadOnly = false
		ddlTK.MustExec("alter table query_capture_schema add column j int default 42")
		txnIS := queryTK.Session().GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
		tbl, err := txnIS.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("query_capture_schema"))
		require.NoError(t, err)
		require.Len(t, tbl.Meta().Columns, 1)
		latest, err := queryTK.Session().GetLatestInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("query_capture_schema"))
		require.NoError(t, err)
		require.Len(t, latest.Columns, 2)
		q, err := executor.GenerateImportQueryPlan(queryTK.Session(), "import into unused_target from select * from query_capture_schema")
		require.NoError(t, err)
		db, ok := txnIS.SchemaByName(ast.NewCIStr("test"))
		require.True(t, ok)
		require.Len(t, q.Tables[db.ID], 1)
		require.Len(t, q.Tables[db.ID][0].Columns, 1)
		_, locked := queryTK.Session().GetSessionVars().GetRelatedTableForMDL().Load(tbl.Meta().ID)
		require.False(t, locked)
	})
}
