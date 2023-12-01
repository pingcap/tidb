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
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// batch create table with table id reused
func TestSplitBatchCreateTableWithTableId(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_id_resued1")
	tk.MustExec("drop table if exists table_id_resued2")
	tk.MustExec("drop table if exists table_id_new")

	d := dom.DDL()
	require.NotNil(t, d)

	infos1 := []*model.TableInfo{}
	infos1 = append(infos1, &model.TableInfo{
		ID:   124,
		Name: model.NewCIStr("table_id_resued1"),
	})
	infos1 = append(infos1, &model.TableInfo{
		ID:   125,
		Name: model.NewCIStr("table_id_resued2"),
	})

	sctx := tk.Session()

	// keep/reused table id verification
	sctx.SetValue(sessionctx.QueryString, "skip")
	err := executor.SplitBatchCreateTableForTest(sctx, model.NewCIStr("test"), infos1, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return false
	}))
	require.NoError(t, err)
	require.Equal(t, "skip", sctx.Value(sessionctx.QueryString))

	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'table_id_resued1'").
		Check(testkit.Rows("124"))
	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'table_id_resued2'").
		Check(testkit.Rows("125"))
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)

	// allocate new table id verification
	// query the global id
	var id int64
	err = kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err error
		id, err = m.GenGlobalID()
		return err
	})

	require.NoError(t, err)

	infos2 := []*model.TableInfo{}
	infos2 = append(infos2, &model.TableInfo{
		ID:   124,
		Name: model.NewCIStr("table_id_new"),
	})

	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err = executor.SplitBatchCreateTableForTest(sctx, model.NewCIStr("test"), infos2, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return true
	}))
	require.NoError(t, err)
	require.Equal(t, "skip", sctx.Value(sessionctx.QueryString))

	idGen, ok := tk.MustQuery(
		"select tidb_table_id from information_schema.tables where table_name = 'table_id_new'").
		Rows()[0][0].(string)
	require.True(t, ok)
	idGenNum, err := strconv.ParseInt(idGen, 10, 64)
	require.NoError(t, err)
	require.Greater(t, idGenNum, id)

	// a empty table info with len(info3) = 0
	infos3 := []*model.TableInfo{}

	originQueryString := sctx.Value(sessionctx.QueryString)
	err = executor.SplitBatchCreateTableForTest(sctx, model.NewCIStr("test"), infos3, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return false
	}))
	require.NoError(t, err)
	require.Equal(t, originQueryString, sctx.Value(sessionctx.QueryString))
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
		Name: model.NewCIStr("tables_1"),
	})
	infos = append(infos, &model.TableInfo{
		ID:   1235,
		Name: model.NewCIStr("tables_2"),
	})
	infos = append(infos, &model.TableInfo{
		ID:   1236,
		Name: model.NewCIStr("tables_3"),
	})

	sctx := tk.Session()

	// keep/reused table id verification
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/RestoreBatchCreateTableEntryTooLarge", "return(1)"))
	err := executor.SplitBatchCreateTableForTest(sctx, model.NewCIStr("test"), infos, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return false
	}))
	require.NoError(t, err)
	require.Equal(t, "skip", sctx.Value(sessionctx.QueryString))

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
		Name: model.NewCIStr("tables_1"),
	})
	infos = append(infos, &model.TableInfo{
		Name: model.NewCIStr("tables_2"),
	})
	infos = append(infos, &model.TableInfo{
		Name: model.NewCIStr("tables_3"),
	})

	sctx := tk.Session()

	tk.Session().SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/RestoreBatchCreateTableEntryTooLarge", "return(0)"))
	err := executor.SplitBatchCreateTableForTest(sctx, model.NewCIStr("test"), infos, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return true
	}))
	require.Equal(t, "skip", sctx.Value(sessionctx.QueryString))
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
		Name:    model.NewCIStr("db_1"),
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
		State:   model.StatePublic,
	}
	err := executor.BRIECreateDatabase(sctx, schema1, "/* from test */")
	require.NoError(t, err)

	schema2 := &model.DBInfo{
		ID:      1240,
		Name:    model.NewCIStr("db_2"),
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("drop table if exists table_2")

	d := dom.DDL()
	require.NotNil(t, d)

	sctx := tk.Session()
	originQueryString := sctx.Value(sessionctx.QueryString)
	dbName := model.NewCIStr("test")
	tableInfo := mockTableInfo(t, sctx, "create table test.table_1 (a int primary key, b json, c varchar(20))")
	tableInfo.ID = 1230
	err := executor.BRIECreateTable(sctx, dbName, tableInfo, "/* from test */")
	require.NoError(t, err)

	tableInfo.ID = 1240
	tableInfo.Name = model.NewCIStr("table_2")
	err = executor.BRIECreateTable(sctx, dbName, tableInfo, "")
	require.NoError(t, err)
	require.Equal(t, originQueryString, sctx.Value(sessionctx.QueryString))
	tk.MustExec("desc table_1")
	tk.MustExec("desc table_2")
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
