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

package common

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMiscs(t *testing.T) {
	store := testkit.CreateMockStore(t)

	// TestString
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("select 1")
	// here to check the panic bug in String() when txn is nil after committed.
	t.Log(tk.Session().String())

	// TestLastExecuteDDLFlag
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id int)")
	require.NotNil(t, tk.Session().Value(sessionctx.LastExecuteDDL))
	tk.MustExec("insert into t1 values (1)")
	require.Nil(t, tk.Session().Value(sessionctx.LastExecuteDDL))

	// TestSession
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("ROLLBACK;")
	tk.Session().Close()
}

func TestPrepare(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id TEXT)")
	tk.MustExec(`INSERT INTO t VALUES ("id");`)
	id, ps, _, err := tk.Session().PrepareStmt("select id+? from t")
	ctx := context.Background()
	require.NoError(t, err)
	require.Equal(t, uint32(1), id)
	require.Equal(t, 1, ps)
	tk.MustExec(`set @a=1`)
	rs, err := tk.Session().ExecutePreparedStmt(ctx, id, expression.Args2Expressions4Test("1"))
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	err = tk.Session().DropPreparedStmt(id)
	require.NoError(t, err)

	tk.MustExec("prepare stmt from 'select 1+?'")
	tk.MustExec("set @v1=100")
	tk.MustQuery("execute stmt using @v1").Check(testkit.Rows("101"))

	tk.MustExec("set @v2=200")
	tk.MustQuery("execute stmt using @v2").Check(testkit.Rows("201"))

	tk.MustExec("set @v3=300")
	tk.MustQuery("execute stmt using @v3").Check(testkit.Rows("301"))
	tk.MustExec("deallocate prepare stmt")

	// Execute prepared statements for more than one time.
	tk.MustExec("create table multiexec (a int, b int)")
	tk.MustExec("insert multiexec values (1, 1), (2, 2)")
	id, _, _, err = tk.Session().PrepareStmt("select a from multiexec where b = ? order by b")
	require.NoError(t, err)
	rs, err = tk.Session().ExecutePreparedStmt(ctx, id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	rs, err = tk.Session().ExecutePreparedStmt(ctx, id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	require.NoError(t, rs.Close())
}

func TestIndexColumnLength(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 blob);")
	tk.MustExec("create index idx_c1 on t(c1);")
	tk.MustExec("create index idx_c2 on t(c2(6));")

	is := dom.InfoSchema()
	tab, err2 := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err2)

	idxC1Cols := tables.FindIndexByColName(tab, "c1").Meta().Columns
	require.Equal(t, types.UnspecifiedLength, idxC1Cols[0].Length)

	idxC2Cols := tables.FindIndexByColName(tab, "c2").Meta().Columns
	require.Equal(t, 6, idxC2Cols[0].Length)
}

func TestTableInfoMeta(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	checkResult := func(affectedRows uint64, insertID uint64) {
		gotRows := tk.Session().AffectedRows()
		require.Equal(t, affectedRows, gotRows)

		gotID := tk.Session().LastInsertID()
		require.Equal(t, insertID, gotID)
	}

	// create table
	tk.MustExec("CREATE TABLE tbl_test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// insert data
	tk.MustExec(`INSERT INTO tbl_test VALUES (1, "hello");`)
	checkResult(1, 0)

	tk.MustExec(`INSERT INTO tbl_test VALUES (2, "hello");`)
	checkResult(1, 0)

	tk.MustExec(`UPDATE tbl_test SET name = "abc" where id = 2;`)
	checkResult(1, 0)

	tk.MustExec(`DELETE from tbl_test where id = 2;`)
	checkResult(1, 0)

	// select data
	tk.MustQuery("select * from tbl_test").Check(testkit.Rows("1 hello"))
}

func TestLastMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id TEXT)")

	// Insert
	tk.MustExec(`INSERT INTO t VALUES ("a");`)
	tk.CheckLastMessage("")
	tk.MustExec(`INSERT INTO t VALUES ("b"), ("c");`)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")

	// Update
	tk.MustExec(`UPDATE t set id = 'c' where id = 'a';`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec(`UPDATE t set id = 'a' where id = 'a';`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows matched: 0  Changed: 0  Warnings: 0")

	// Replace
	tk.MustExec(`drop table if exists t, t1;
        create table t (c1 int PRIMARY KEY, c2 int);
        create table t1 (a1 int, a2 int);`)
	tk.MustExec(`INSERT INTO t VALUES (1,1)`)
	tk.MustExec(`REPLACE INTO t VALUES (2,2)`)
	tk.CheckLastMessage("")
	tk.MustExec(`INSERT INTO t1 VALUES (1,10), (3,30);`)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	tk.MustExec(`REPLACE INTO t SELECT * from t1`)
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 0")

	// Check insert with CLIENT_FOUND_ROWS is set
	tk.Session().SetClientCapability(mysql.ClientFoundRows)
	tk.MustExec(`drop table if exists t, t1;
        create table t (c1 int PRIMARY KEY, c2 int);
        create table t1 (a1 int, a2 int);`)
	tk.MustExec(`INSERT INTO t1 VALUES (1, 10), (2, 2), (3, 30);`)
	tk.MustExec(`INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);`)
	tk.MustExec(`INSERT INTO t SELECT * FROM t1 ON DUPLICATE KEY UPDATE c2=a2;`)
	tk.CheckLastMessage("Records: 6  Duplicates: 3  Warnings: 0")
}

func TestQueryString(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table mutil1 (a int);create table multi2 (a int)")
	queryStr := tk.Session().Value(sessionctx.QueryString)
	require.Equal(t, "create table multi2 (a int)", queryStr)

	// Test execution of DDL through the "ExecutePreparedStmt" interface.
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t (id bigint PRIMARY KEY, age int)")
	tk.MustExec("show create table t")
	id, _, _, err := tk.Session().PrepareStmt("CREATE TABLE t2(id bigint PRIMARY KEY, age int)")
	require.NoError(t, err)
	_, err = tk.Session().ExecutePreparedStmt(context.Background(), id, expression.Args2Expressions4Test())
	require.NoError(t, err)
	qs := tk.Session().Value(sessionctx.QueryString)
	require.Equal(t, "CREATE TABLE t2(id bigint PRIMARY KEY, age int)", qs.(string))

	// Test execution of DDL through the "Execute" interface.
	tk.MustExec("use test")
	tk.MustExec("drop table t2")
	tk.MustExec("prepare stmt from 'CREATE TABLE t2(id bigint PRIMARY KEY, age int)'")
	tk.MustExec("execute stmt")
	qs = tk.Session().Value(sessionctx.QueryString)
	require.Equal(t, "CREATE TABLE t2(id bigint PRIMARY KEY, age int)", qs.(string))
}

func TestAffectedRows(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id TEXT)")
	tk.MustExec(`INSERT INTO t VALUES ("a");`)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))
	tk.MustExec(`INSERT INTO t VALUES ("b");`)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))
	tk.MustExec(`UPDATE t set id = 'c' where id = 'a';`)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))
	tk.MustExec(`UPDATE t set id = 'a' where id = 'a';`)
	require.Equal(t, 0, int(tk.Session().AffectedRows()))
	tk.MustQuery(`SELECT * from t`).Check(testkit.Rows("c", "b"))
	require.Equal(t, 0, int(tk.Session().AffectedRows()))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, data int)")
	tk.MustExec(`INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	tk.MustExec(`UPDATE t set id = 1 where data = 0;`)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, c1 timestamp);")
	tk.MustExec(`insert t(id) values(1);`)
	tk.MustExec(`UPDATE t set id = 1 where id = 1;`)
	require.Equal(t, 0, int(tk.Session().AffectedRows()))

	// With ON DUPLICATE KEY UPDATE, the affected-rows value per row is 1 if the row is inserted as a new row,
	// 2 if an existing row is updated, and 0 if an existing row is set to its current values.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int PRIMARY KEY, c2 int);")
	tk.MustExec(`insert t values(1, 1);`)
	tk.MustExec(`insert into t values (1, 1) on duplicate key update c2=2;`)
	require.Equal(t, 2, int(tk.Session().AffectedRows()))
	tk.MustExec(`insert into t values (1, 1) on duplicate key update c2=2;`)
	require.Equal(t, 0, int(tk.Session().AffectedRows()))
	tk.MustExec("drop table if exists test")
	createSQL := `CREATE TABLE test (
	  id        VARCHAR(36) PRIMARY KEY NOT NULL,
	  factor    INTEGER                 NOT NULL                   DEFAULT 2);`
	tk.MustExec(createSQL)
	insertSQL := `INSERT INTO test(id) VALUES('id') ON DUPLICATE KEY UPDATE factor=factor+3;`
	tk.MustExec(insertSQL)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))
	tk.MustExec(insertSQL)
	require.Equal(t, 2, int(tk.Session().AffectedRows()))
	tk.MustExec(insertSQL)
	require.Equal(t, 2, int(tk.Session().AffectedRows()))

	tk.Session().SetClientCapability(mysql.ClientFoundRows)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, data int)")
	tk.MustExec(`INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	tk.MustExec(`UPDATE t set id = 1 where data = 0;`)
	require.Equal(t, 2, int(tk.Session().AffectedRows()))
}
