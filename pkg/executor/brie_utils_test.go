// Copyright 2023 PingCAP, Inc.
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
	"math/rand"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

// batch create table with table id reused
func TestSplitBatchCreateTableWithTableId(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_id_reused1")
	tk.MustExec("drop table if exists table_id_reused2")
	tk.MustExec("drop table if exists table_id_new")

	d := dom.DDL()
	require.NotNil(t, d)

	infos1 := []*model.TableInfo{}
	infos1 = append(infos1, &model.TableInfo{
		ID:   124,
		Name: ast.NewCIStr("table_id_reused1"),
	})
	infos1 = append(infos1, &model.TableInfo{
		ID:   125,
		Name: ast.NewCIStr("table_id_reused2"),
	})
	querys1 := []string{
		"create table test.table_id_reused1 (id int)",
		"create table test.table_id_reused2 (id int)",
	}

	sctx := tk.Session()

	// keep/reused table id verification
	sctx.SetValue(sessionctx.QueryString, "TODO")
	err := executor.SplitBatchCreateTableForTest(sctx, ast.NewCIStr("test"), infos1, querys1, ddl.WithIDAllocated(true))
	require.NoError(t, err)
	require.Equal(t, "create table test.table_id_reused1 (id int);create table test.table_id_reused2 (id int);", sctx.Value(sessionctx.QueryString))

	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'table_id_reused1'").
		Check(testkit.Rows("124"))
	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'table_id_reused2'").
		Check(testkit.Rows("125"))
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)

	// allocate new table id verification
	// query the global id
	var id int64
	err = kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		var err error
		id, err = m.GenGlobalID()
		return err
	})

	require.NoError(t, err)

	infos2 := []*model.TableInfo{}
	infos2 = append(infos2, &model.TableInfo{
		ID:   124,
		Name: ast.NewCIStr("table_id_new"),
	})
	querys2 := []string{"create table test.table_id_new (id int)"}

	tk.Session().SetValue(sessionctx.QueryString, "TODO")
	err = executor.SplitBatchCreateTableForTest(sctx, ast.NewCIStr("test"), infos2, querys2)
	require.NoError(t, err)
	require.Equal(t, "create table test.table_id_new (id int);", sctx.Value(sessionctx.QueryString))

	idGen, ok := tk.MustQuery(
		"select tidb_table_id from information_schema.tables where table_name = 'table_id_new'").
		Rows()[0][0].(string)
	require.True(t, ok)
	idGenNum, err := strconv.ParseInt(idGen, 10, 64)
	require.NoError(t, err)
	require.Greater(t, idGenNum, id)

	// a empty table info with len(info3) = 0
	infos3 := []*model.TableInfo{}
	querys3 := []string{}

	err = executor.SplitBatchCreateTableForTest(sctx, ast.NewCIStr("test"), infos3, querys3, ddl.WithIDAllocated(true))
	require.NoError(t, err)
	require.Equal(t, "", sctx.Value(sessionctx.QueryString))
}

// batch create table with table id reused
func TestSplitBatchCreateTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("drop table if exists table_2")
	tk.MustExec("drop table if exists table_3")

	d := dom.DDL()
	require.NotNil(t, d)

	infos := []*model.TableInfo{}
	infos = append(infos, &model.TableInfo{
		ID:   1234,
		Name: ast.NewCIStr("tables_1"),
	})
	infos = append(infos, &model.TableInfo{
		ID:   1235,
		Name: ast.NewCIStr("tables_2"),
	})
	infos = append(infos, &model.TableInfo{
		ID:   1236,
		Name: ast.NewCIStr("tables_3"),
	})
	querys := []string{
		"create table test.tables_1 (id int)",
		"create table test.tables_2 (id int)",
		"create table test.tables_3 (id int)"}

	sctx := tk.Session()

	// keep/reused table id verification
	tk.Session().SetValue(sessionctx.QueryString, "TODO")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/RestoreBatchCreateTableEntryTooLarge", "return(1)"))
	err := executor.SplitBatchCreateTableForTest(sctx, ast.NewCIStr("test"), infos, querys, ddl.WithIDAllocated(true))
	require.NoError(t, err)
	require.Equal(t, "create table test.tables_3 (id int);", sctx.Value(sessionctx.QueryString))

	tk.MustQuery("show tables like '%tables_%'").Check(testkit.Rows("tables_1", "tables_2", "tables_3"))
	jobs := tk.MustQuery("admin show ddl jobs").Rows()
	require.Greater(t, len(jobs), 3)
	// check table_1
	job1 := jobs[0]
	require.Equal(t, "test", job1[1])
	require.Equal(t, "tables_3", job1[2])
	require.Equal(t, "create tables", job1[3])
	require.Equal(t, "public", job1[4])

	// check table_2
	job2 := jobs[1]
	require.Equal(t, "test", job2[1])
	require.Equal(t, "tables_2", job2[2])
	require.Equal(t, "create tables", job2[3])
	require.Equal(t, "public", job2[4])

	// check table_3
	job3 := jobs[2]
	require.Equal(t, "test", job3[1])
	require.Equal(t, "tables_1", job3[2])
	require.Equal(t, "create tables", job3[3])
	require.Equal(t, "public", job3[4])

	// check reused table id
	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'tables_1'").
		Check(testkit.Rows("1234"))
	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'tables_2'").
		Check(testkit.Rows("1235"))
	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'tables_3'").
		Check(testkit.Rows("1236"))

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/RestoreBatchCreateTableEntryTooLarge"))
}

// batch create table with table id reused
func TestSplitBatchCreateTableFailWithEntryTooLarge(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("drop table if exists table_2")
	tk.MustExec("drop table if exists table_3")

	d := dom.DDL()
	require.NotNil(t, d)

	infos := []*model.TableInfo{}
	infos = append(infos, &model.TableInfo{
		Name: ast.NewCIStr("tables_1"),
	})
	infos = append(infos, &model.TableInfo{
		Name: ast.NewCIStr("tables_2"),
	})
	infos = append(infos, &model.TableInfo{
		Name: ast.NewCIStr("tables_3"),
	})
	querys := []string{
		"create table test.tables_1 (id int)",
		"create table test.tables_2 (id int)",
		"create table test.tables_3 (id int)"}

	sctx := tk.Session()

	tk.Session().SetValue(sessionctx.QueryString, "TODO")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/RestoreBatchCreateTableEntryTooLarge", "return(0)"))
	err := executor.SplitBatchCreateTableForTest(sctx, ast.NewCIStr("test"), infos, querys)
	require.Equal(t, "create table test.tables_1 (id int);", sctx.Value(sessionctx.QueryString))
	require.True(t, kv.ErrEntryTooLarge.Equal(err))

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/RestoreBatchCreateTableEntryTooLarge"))
}

func TestBRIECreateDatabase(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists db_1")
	tk.MustExec("drop database if exists db_1")

	d := dom.DDL()
	require.NotNil(t, d)

	sctx := tk.Session()
	originQueryString := sctx.Value(sessionctx.QueryString)
	schema1 := &model.DBInfo{
		ID:      1230,
		Name:    ast.NewCIStr("db_1"),
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
		State:   model.StatePublic,
	}
	err := executor.BRIECreateDatabase(sctx, schema1, "/* from test */")
	require.NoError(t, err)

	schema2 := &model.DBInfo{
		ID:      1240,
		Name:    ast.NewCIStr("db_2"),
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
		State:   model.StatePublic,
	}
	err = executor.BRIECreateDatabase(sctx, schema2, "")
	require.NoError(t, err)
	require.Equal(t, originQueryString, sctx.Value(sessionctx.QueryString))
	tk.MustExec("use db_1")
	tk.MustExec("use db_2")
}

func mockTableInfo(t *testing.T, sctx sessionctx.Context, createSQL string) *model.TableInfo {
	node, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	info, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	info.State = model.StatePublic
	return info
}

func TestBRIECreateTable(t *testing.T) {
	t.Run("TrustedRestoreReadOnlyBoundary", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaReadOnlyDDL", "return")
		tk.MustExec("create database restore_ro")
		tk.MustExec("create database other_ro")
		tk.MustExec("create table other_ro.t(a int)")
		tk.MustExec("alter database restore_ro read only = 1")
		tk.MustExec("alter database other_ro read only = 1")
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		require.False(t, privilege.HasExplicitReplicaWriterAdmin(tk.Session()), "SUPER is not an ordinary write bypass")
		tk.MustGetErrCode("create table restore_ro.denied(a int)", 3989)
		one := mockTableInfo(t, tk.Session(), "create table restore_ro.restored_one(a int)")
		require.NoError(t, executor.BRIECreateTable(tk.Session(), ast.NewCIStr("restore_ro"), one, ""))
		two := mockTableInfo(t, tk.Session(), "create table restore_ro.restored_two(a int)")
		require.NoError(t, executor.BRIECreateTables(tk.Session(), map[string][]*model.TableInfo{"restore_ro": {two}}, ""))
		tk.MustExec("create user restore_operator")
		tk.MustExec("grant RESTORE_ADMIN on *.* to restore_operator")
		tk.MustExec("grant create, insert on restore_ro.* to restore_operator")
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "restore_operator", Hostname: "%"}, nil, nil, nil))
		require.False(t, privilege.HasExplicitReplicaWriterAdmin(tk.Session()))
		tk.MustGetErrCode("create table restore_ro.denied_operator(a int)", 3989)
		three := mockTableInfo(t, tk.Session(), "create table restore_ro.restored_operator(a int)")
		require.NoError(t, executor.BRIECreateTable(tk.Session(), ast.NewCIStr("restore_ro"), three, ""))
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		for _, db := range []string{"restore_ro", "other_ro"} {
			require.False(t, privilege.CanWriteReadOnlySchema(tk.Session(), db), "restore authorization must end before returning the session")
		}
		tk.MustGetErrCode("insert into restore_ro.restored_one values (1)", 3989)
		_, err := tk.Session().GetSQLExecutor().ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR), "insert into other_ro.t values (1)")
		require.ErrorContains(t, err, "read only mode")
		// Error paths must restore an existing, differently scoped authorization.
		restore := privilege.WithInternalSchemaReadOnlyBypass(tk.Session(), "other_ro")
		defer restore()
		missing := mockTableInfo(t, tk.Session(), "create table missing_restore_db.restored_missing(a int)")
		err = executor.BRIECreateTable(tk.Session(), ast.NewCIStr("missing_restore_db"), missing, "")
		require.Error(t, err)
		require.True(t, privilege.CanWriteReadOnlySchema(tk.Session(), "other_ro"))
		require.False(t, privilege.CanWriteReadOnlySchema(tk.Session(), "restore_ro"))
	})
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("drop table if exists table_2")

	d := dom.DDL()
	require.NotNil(t, d)

	sctx := tk.Session()
	originQueryString := sctx.Value(sessionctx.QueryString)
	dbName := ast.NewCIStr("test")
	tableInfo := mockTableInfo(t, sctx, "create table test.table_1 (a int primary key, b json, c varchar(20))")
	tableInfo.ID = 1230
	err := executor.BRIECreateTable(sctx, dbName, tableInfo, "/* from test */")
	require.NoError(t, err)

	tableInfo.ID = 1240
	tableInfo.Name = ast.NewCIStr("table_2")
	err = executor.BRIECreateTable(sctx, dbName, tableInfo, "")
	require.NoError(t, err)
	require.Equal(t, originQueryString, sctx.Value(sessionctx.QueryString))
	tk.MustExec("desc table_1")
	tk.MustExec("desc table_2")
}

func TestBRIEGlueSessionReadOnlySequence(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		t.Run(fmt.Sprintf("cycle=%t", cycle), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaReadOnlyDDL", "return")
			require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
			require.False(t, privilege.HasExplicitReplicaWriterAdmin(tk.Session()))

			ctx := context.Background()
			gs, err := executor.NewBRIEGlueForTest(tk.Session()).CreateSession(store)
			require.NoError(t, err)
			defer gs.Close()
			for _, name := range []string{"restore_ro", "other_ro"} {
				require.NoError(t, gs.CreateDatabaseOnExistError(ctx, &model.DBInfo{
					Name: ast.NewCIStr(name), ReadOnly: true,
				}))
				require.NoError(t, gs.CreateTable(ctx, ast.NewCIStr(name), &model.TableInfo{
					Name: ast.NewCIStr("s"), State: model.StatePublic,
					Sequence: &model.SequenceInfo{Start: 1, MinValue: 1, MaxValue: 1000, Increment: 1, Cycle: cycle},
				}))
			}

			func() {
				defer privilege.WithInternalSchemaReadOnlyBypass(gs.GetSessionCtx(), "restore_ro")()
				if cycle {
					// Match restoreSequence's cycle-round initialization before restoring the saved value.
					require.NoError(t, gs.Execute(ctx, "do setval(restore_ro.s, 1000)"))
					require.NoError(t, gs.Execute(ctx, "do nextval(restore_ro.s)"))
				}
				require.NoError(t, gs.Execute(ctx, "do setval(restore_ro.s, 41)"))
				require.ErrorContains(t, gs.Execute(ctx, "do setval(other_ro.s, 99)"), "read only mode")
				require.ErrorContains(t, gs.Execute(ctx, "do nextval(other_ro.s)"), "read only mode")
				// An execution error must not discard the enclosing restore authorization.
				require.NoError(t, gs.Execute(ctx, "do setval(restore_ro.s, 41)"))
			}()

			for _, name := range []string{"restore_ro", "other_ro"} {
				require.False(t, privilege.CanWriteReadOnlySchema(gs.GetSessionCtx(), name))
				require.ErrorContains(t, gs.Execute(ctx, "do setval("+name+".s, 99)"), "read only mode")
				require.ErrorContains(t, tk.QueryToErr("select nextval("+name+".s)"), "read only mode")
				require.ErrorContains(t, tk.QueryToErr("select setval("+name+".s, 99)"), "read only mode")
			}
			tk.MustQuery("select options from information_schema.schemata_extensions where schema_name = 'restore_ro'").Check(testkit.Rows("READ ONLY=1"))
			require.Contains(t, tk.MustQuery("show create database restore_ro").Rows()[0][1], "READ ONLY = 1")
			tk.MustExec("alter database restore_ro read only = 0")
			next, err := strconv.ParseInt(tk.MustQuery("select nextval(restore_ro.s)").Rows()[0][0].(string), 10, 64)
			require.NoError(t, err)
			require.Greater(t, next, int64(41))
		})
	}
}

func TestBRIEGlueSessionAutocommit(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, readOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%t/readOnly=%t", batch, readOnly), func(t *testing.T) {
				store := testkit.CreateMockStore(t)
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("set global autocommit = 0")
				defer tk.MustExec("set global autocommit = 1")
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaReadOnlyDDL", "return")

				ctx := context.Background()
				gs, err := executor.NewBRIEGlueForTest(tk.Session()).CreateSession(store)
				require.NoError(t, err)
				defer gs.Close()
				// Match restore initialization, which loads global session defaults.
				require.NoError(t, gs.Execute(ctx, "set @@sql_mode = ''"))
				dbName := ast.NewCIStr("restore_autocommit")
				require.NoError(t, gs.CreateDatabaseOnExistError(ctx, &model.DBInfo{
					Name: dbName, ReadOnly: readOnly,
				}))

				for _, name := range []string{"s1", "s2"} {
					tbl := &model.TableInfo{
						Name: ast.NewCIStr(name), State: model.StatePublic,
						Sequence: &model.SequenceInfo{Start: 1, MinValue: 1, MaxValue: 1000, Increment: 1},
					}
					if batch {
						batchSession, ok := gs.(glue.BatchCreateTableSession)
						require.True(t, ok)
						require.NoError(t, batchSession.CreateTables(ctx, map[string][]*model.TableInfo{dbName.L: {tbl}}))
					} else {
						require.NoError(t, gs.CreateTable(ctx, dbName, tbl))
					}
					// Interleave creation and SETVAL to detect a stale transaction schema
					// when the same restore worker handles another sequence.
					func() {
						defer privilege.WithInternalSchemaReadOnlyBypass(gs.GetSessionCtx(), dbName.L)()
						require.NoError(t, gs.Execute(ctx, fmt.Sprintf("do setval(%s.%s, 41)", dbName.O, name)))
					}()
				}
				require.True(t, gs.GetSessionCtx().GetSessionVars().IsAutocommit())
				require.False(t, gs.GetSessionCtx().GetSessionVars().InTxn())
			})
		}
	}
}

func TestBRIECreateTables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tableInfos := make([]*model.TableInfo, 100)
	for i := range tableInfos {
		tk.MustExec(fmt.Sprintf("drop table if exists table_%d", i))
	}

	d := dom.DDL()
	require.NotNil(t, d)

	sctx := tk.Session()
	originQueryString := sctx.Value(sessionctx.QueryString)
	for i := range tableInfos {
		tableInfos[i] = mockTableInfo(t, sctx, fmt.Sprintf("create table test.table_%d (a int primary key, b json, c varchar(20))", i))
		tableInfos[i].ID = 1230 + int64(i)
	}
	err := executor.BRIECreateTables(sctx, map[string][]*model.TableInfo{"test": tableInfos}, "/* from test */")
	require.NoError(t, err)

	require.Equal(t, originQueryString, sctx.Value(sessionctx.QueryString))
	for i := range tableInfos {
		tk.MustExec(fmt.Sprintf("desc table_%d", i))
	}
}

type fakeDDLExecutor struct {
	ddl.Executor
	queryList        map[string][]string
	successQueryList map[string][]string
	maxCount         int
}

func (f *fakeDDLExecutor) BatchCreateTableWithInfo(
	sctx sessionctx.Context,
	schema ast.CIStr,
	info []*model.TableInfo,
	cs ...ddl.CreateTableOption,
) error {
	f.queryList[schema.O] = append(f.queryList[schema.O], sctx.Value(sessionctx.QueryString).(string))
	if len(info) > f.maxCount {
		switch rand.Int() % 2 {
		case 0:
			return kv.ErrTxnTooLarge
		case 1:
			return kv.ErrEntryTooLarge
		}
	}
	f.successQueryList[info[0].Name.O] = append(f.successQueryList[info[0].Name.O], sctx.Value(sessionctx.QueryString).(string))
	return nil
}

type fakeSessionContext struct {
	sessionapi.Session
	ddlexecutor *fakeDDLExecutor
	values      map[string]any
	vars        *variable.SessionVars
}

func newFakeSessionContext(ddlexecutor *fakeDDLExecutor) *fakeSessionContext {
	return &fakeSessionContext{
		ddlexecutor: ddlexecutor,
		values:      make(map[string]any),
		vars:        &variable.SessionVars{},
	}
}

func (f *fakeSessionContext) GetDomain() any {
	dom := &domain.Domain{}
	dom.SetDDL(nil, f.ddlexecutor)
	return dom
}

func (f *fakeSessionContext) Value(key fmt.Stringer) any {
	return f.values[key.String()]
}

func (f *fakeSessionContext) SetValue(key fmt.Stringer, value any) {
	f.values[key.String()] = value
}

func (f *fakeSessionContext) GetSessionVars() *variable.SessionVars {
	return f.vars
}

func TestSplitTablesQueryMatch(t *testing.T) {
	ddlexecutor := &fakeDDLExecutor{
		maxCount:         1,
		queryList:        make(map[string][]string),
		successQueryList: make(map[string][]string),
	}
	sctx := newFakeSessionContext(ddlexecutor)
	tables := map[string][]*model.TableInfo{
		"test": {
			{Name: ast.NewCIStr("t1")},
			{Name: ast.NewCIStr("t2")},
		},
		"test2": {
			{Name: ast.NewCIStr("t3")},
		},
	}

	err := executor.BRIECreateTables(sctx, tables, "/*from(br)*/")
	require.NoError(t, err)
	require.Len(t, ddlexecutor.queryList, 2)
	require.Len(t, ddlexecutor.queryList["test"], 3)
	require.Len(t, ddlexecutor.queryList["test2"], 1)
	require.Equal(t, "/*from(br)*/CREATE TABLE `t1` (\n\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;/*from(br)*/CREATE TABLE `t2` (\n\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;", ddlexecutor.queryList["test"][0])
	require.Equal(t, "/*from(br)*/CREATE TABLE `t1` (\n\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;", ddlexecutor.queryList["test"][1])
	require.Equal(t, "/*from(br)*/CREATE TABLE `t2` (\n\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;", ddlexecutor.queryList["test"][2])
	require.Equal(t, "/*from(br)*/CREATE TABLE `t3` (\n\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;", ddlexecutor.queryList["test2"][0])
	require.Len(t, ddlexecutor.successQueryList, 3)
	require.Len(t, ddlexecutor.successQueryList["t1"], 1)
	require.Len(t, ddlexecutor.successQueryList["t2"], 1)
	require.Len(t, ddlexecutor.successQueryList["t3"], 1)
	require.Equal(t, ddlexecutor.queryList["test"][1], ddlexecutor.successQueryList["t1"][0])
	require.Equal(t, ddlexecutor.queryList["test"][2], ddlexecutor.successQueryList["t2"][0])
	require.Equal(t, ddlexecutor.queryList["test2"][0], ddlexecutor.successQueryList["t3"][0])
}
