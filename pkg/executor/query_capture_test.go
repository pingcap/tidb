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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCaptureImportQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_src(i bigint)")
	t.Run("source tables", func(t *testing.T) {
		tk.MustExec("create table query_other(i int)")
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		q, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from (select i from query_src union all select i from query_other)")
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
		_, err = executor.CaptureImportQuery(tk.Session(), "import into unused_target from select ? from query_src")
		require.ErrorContains(t, err, "unexpected '?'")
		for _, sql := range []string{
			"select @v from query_src", "select @v:=i from query_src",
			"select @@sql_mode", "with c as (select @v) select * from c",
		} {
			_, err = executor.CaptureImportQuery(tk.Session(), "import into unused_target from ("+sql+")")
			require.ErrorContains(t, err, "variables in IMPORT INTO FROM SELECT")
		}
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
		q, err := executor.CaptureImportQuery(queryTK.Session(), "import into unused_target from select * from query_capture_schema")
		require.NoError(t, err)
		db, ok := txnIS.SchemaByName(ast.NewCIStr("test"))
		require.True(t, ok)
		require.Len(t, q.Tables[db.ID], 1)
		require.Len(t, q.Tables[db.ID][0].Columns, 1)
		_, locked := queryTK.Session().GetSessionVars().GetRelatedTableForMDL().Load(tbl.Meta().ID)
		require.False(t, locked)
	})
}
