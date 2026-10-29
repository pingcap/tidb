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

package importer_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCaptureQuerySource(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_src(i bigint)")
	tk.MustExec("create table unused_target(i bigint)")
	capture := func(se sessionctx.Context, sel string) (*importer.QuerySource, error) {
		sql := "import into unused_target from " + sel
		if err := importer.PlanImportQueryForTest(se, sql); err != nil {
			return nil, err
		}
		return importer.CaptureQuerySource(se, sql)
	}
	sourceTables := func(q *importer.QuerySource) map[string]importer.SourceTable {
		is := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
		tables := make(map[string]importer.SourceTable, len(q.Tables))
		for _, src := range q.Tables {
			db, ok := is.SchemaByID(src.DBID)
			require.True(t, ok)
			tbl, ok := is.TableByID(context.Background(), src.ID)
			require.True(t, ok)
			tables[db.Name.L+"."+tbl.Meta().Name.L] = src
		}
		return tables
	}
	t.Run("session variables", func(t *testing.T) {
		tk.MustExec("set cte_max_recursion_depth = 5")
		tk.MustExec("set tidb_mem_quota_query = 1048576")
		tk.MustExec("set @v = 1")
		defer tk.MustExec("set cte_max_recursion_depth = default, tidb_mem_quota_query = default")
		tk.MustExec("begin")
		defer tk.MustExec("rollback")
		q, err := capture(tk.Session(), "select i from query_src")
		require.NoError(t, err)
		require.Equal(t, "5", q.SessionStates.SystemVars[vardef.CTEMaxRecursionDepth])
		require.NotEmpty(t, q.SessionStates.SystemVars[vardef.Timestamp])
		require.Contains(t, q.SessionStates.UserVars, "v")
	})
	t.Run("source tables", func(t *testing.T) {
		tk.MustExec("create table query_other(i int)")
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		tk.Session().GetSessionVars().StmtCtx.ResourceGroupName = "rg1"
		q, err := capture(tk.Session(), "(select i from query_src union all select i from query_other)")
		require.NoError(t, err)
		require.Equal(t, "rg1", q.SessionStates.ResourceGroupName)
		tables := sourceTables(q)
		require.Len(t, tables, 2)
		require.Contains(t, tables, "test.query_src")
		require.Contains(t, tables, "test.query_other")
		require.NotContains(t, tables, "test.unused_target")
		tk.MustExec("create temporary table query_tmp(i int)")
		tk.MustExec("create global temporary table query_gtmp(i int) on commit delete rows")
		for _, tbl := range []string{"query_tmp", "query_gtmp"} {
			_, err = capture(tk.Session(), "select * from "+tbl)
			require.ErrorContains(t, err, "temporary source tables in IMPORT INTO FROM SELECT")
		}
		tk.MustExec("create sequence query_seq")
		for sql, msg := range map[string]string{
			"select nextval(query_seq)":                                                         "sequences in IMPORT INTO FROM SELECT",
			"select 1 from query_seq":                                                           "sequences in IMPORT INTO FROM SELECT",
			"select table_name from information_schema.tables":                                  "virtual source tables in IMPORT INTO FROM SELECT",
			"select variable_name from performance_schema.session_variables":                    "virtual source tables in IMPORT INTO FROM SELECT",
			"select i from query_src where i in (select 1 from information_schema.processlist)": "virtual source tables in IMPORT INTO FROM SELECT",
		} {
			_, err = capture(tk.Session(), sql)
			require.ErrorContains(t, err, msg, sql)
		}
		_, err = capture(tk.Session(), "select ? from query_src")
		require.ErrorContains(t, err, "unexpected '?'")
		// A stale read replaces the txn context of the session, so use a new one.
		staleTK := testkit.NewTestKit(t, store)
		staleTK.MustExec("use test")
		_, err = capture(staleTK.Session(), "select i from query_src as of timestamp now(6)")
		require.ErrorContains(t, err, "stale reads in IMPORT INTO FROM SELECT")
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
		q, err := capture(tk.Session(), "select * from outer_v")
		require.NoError(t, err)
		tables := sourceTables(q)
		require.Len(t, tables, 4)
		require.Contains(t, tables, "test.outer_v")
		require.Contains(t, tables, "query_view_db.inner_v")
		require.Contains(t, tables, "query_view_db.base")
		require.Contains(t, tables, "test.query_cached")

		// The worker rebuilds the schema the submitter resolved the query with.
		parent := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
		is, err := q.BuildMinimalInfoSchema(store, parent)
		require.NoError(t, err)
		for name, src := range tables {
			tbl, ok := is.TableByID(context.Background(), src.ID)
			require.True(t, ok, name)
			require.Equal(t, src.Revision, tbl.Meta().Revision, name)
		}
		require.Equal(t, q.SchemaVersion, is.SchemaMetaVersion())
		// A table that changed after the query was resolved fails the rebuild.
		changed := *q
		changed.Tables = append([]importer.SourceTable(nil), q.Tables...)
		changed.Tables[0].Revision++
		_, err = changed.BuildMinimalInfoSchema(store, parent)
		require.ErrorContains(t, err, "changed before the import query snapshot")
	})
	t.Run("views ignore local temporary tables", func(t *testing.T) {
		tk.MustExec("create table query_shadowed(i int)")
		tk.MustExec("create view query_shadowed_v as select i from query_shadowed")
		tbl, err := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema).TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("query_shadowed"))
		require.NoError(t, err)
		tk.MustExec("create temporary table query_shadowed(i int)")
		defer tk.MustExec("drop temporary table query_shadowed")
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		q, err := capture(tk.Session(), "select * from query_shadowed_v")
		require.NoError(t, err)
		tables := sourceTables(q)
		require.Len(t, tables, 2)
		require.Equal(t, tbl.Meta().ID, tables["test.query_shadowed"].ID)
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
		q, err := capture(queryTK.Session(), "select * from query_capture_schema")
		require.NoError(t, err)
		// The planner of the statement locks the table with MDL and takes its
		// latest definition, which capture takes too.
		require.Equal(t, []importer.SourceTable{{DBID: tbl.Meta().DBID, ID: tbl.Meta().ID, Revision: latest.Revision}}, q.Tables)
		require.Less(t, tbl.Meta().Revision, latest.Revision)
		require.Equal(t, txnIS.SchemaMetaVersion(), q.SchemaVersion)
		_, locked := queryTK.Session().GetSessionVars().GetRelatedTableForMDL().Load(tbl.Meta().ID)
		require.True(t, locked)
		_, err = q.BuildMinimalInfoSchema(store, txnIS)
		require.NoError(t, err)
	})
	t.Run("restore session states", func(t *testing.T) {
		submitTK := testkit.NewTestKit(t, store)
		submitTK.MustExec("use test")
		submitTK.MustExec("set cte_max_recursion_depth = 5, tidb_mem_quota_query = 1048576, @v = 1")
		submitTK.MustExec("begin")
		defer submitTK.MustExec("rollback")
		submitTK.Session().GetSessionVars().StmtCtx.AppendWarning(errors.New("warning of the statement before"))
		q, err := capture(submitTK.Session(), "select i from query_src")
		require.NoError(t, err)

		workerTK := testkit.NewTestKit(t, store)
		workerVars := workerTK.Session().GetSessionVars()
		memQuota := workerVars.MemQuotaQuery
		require.NoError(t, q.RestoreStates(context.Background(), workerTK.Session()))
		require.Equal(t, "test", workerVars.CurrentDB)
		require.Equal(t, 5, workerVars.CTEMaxRecursionDepth)
		v, ok := workerVars.GetUserVarVal("v")
		require.True(t, ok)
		require.Equal(t, int64(1), v.GetInt64())
		timestamp, err := workerVars.GetSessionOrGlobalSystemVar(context.Background(), vardef.Timestamp)
		require.NoError(t, err)
		require.Equal(t, q.SessionStates.SystemVars[vardef.Timestamp], timestamp)
		// The worker keeps its own memory budget and transaction, and drops the
		// warnings of the submitter.
		require.Equal(t, memQuota, workerVars.MemQuotaQuery)
		require.False(t, workerVars.InTxn())
		require.Empty(t, workerVars.StmtCtx.GetWarnings())
	})
}
