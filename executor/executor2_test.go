package executor_test

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"testing"
	"time"
)

func TestDebugCs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("create table t (pk int key, val int)")
	tk.MustExec("insert into t values (1,1), (2,2)")
	tk.MustExec("begin")
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("update t set val = val + 1 where pk = 2")
	go func() {
		time.Sleep(time.Millisecond * 10)
		tk2.MustExec("commit")
	}()
	tk.MustQuery("select val from t where pk = 2 for update").Check(testkit.Rows("2"))
	tk.MustExec("rollback")
}

func TestUnionScanCs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_constraint_check_in_place = false")
	/*
		// case-1: duplicate primary key in insert, and primary key is handle.
		tk.MustExec("create table t (pk int key, val int)")
		tk.MustExec("insert into t values (1,1)")
		tk.MustExec("begin optimistic")
		tk.MustExec("insert into t values (1,2)") // MySQL returns error here, so following query result in MySQL is [1 1].
		tk.MustQuery("select * from t").Check(testkit.Rows("1 1")) // TODO: What's the expected result in TiDB? Currently it's [1 2]
		tk.MustGetDBError("commit", kv.ErrKeyExists)
	*/

	/*
		// case-2: duplicate unique key in insert, and primary key is handle.
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (pk int key, val int, unique index (val))")
		tk.MustExec("insert into t values (1,1)")
		tk.MustExec("begin optimistic")
		tk.MustExec("insert into t values (2,1)") // MySQL returns error here, so following query result in MySQL is [1 1].
		tk.MustQuery("select * from t").Check(testkit.Rows("1 1")) // TODO: What's the expected result in TiDB? Currently it's [1 1],[2 1].
		tk.MustGetDBError("commit", kv.ErrKeyExists)
	*/

	// case-3: duplicate primary key in insert, and primary key is not handle.
	/*
		tk.MustExec("set @@tidb_enable_clustered_index = 0;")
		tk.MustExec("create table t (pk varchar(10) key, val int)")
		tk.MustExec("insert into t values (1,1)")
		tk.MustQuery("select _tidb_rowid, pk, val from t").Check(testkit.Rows("1 1 1"))
		tk.MustExec("begin optimistic")
		tk.MustExec("insert into t values (1,2)") // MySQL returns error here, so following query result in MySQL is [1 1].
		tk.MustQuery("select * from t").Check(testkit.Rows("1 1")) // TODO: What's the expected result in TiDB? Currently it's [1 1],[1 2].
		tk.MustGetDBError("commit", kv.ErrKeyExists)
	*/

	// case-4: in pessimistic transaction, duplicate primary key in insert, and primary key is handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1,2)")                  // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2")) // TODO: What's the expected result in TiDB? Currently it's [1 2]
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)

	// case-5: in pessimistic transaction, duplicate unique key in insert, and primary key is handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int, unique index (val))")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (2,1)")                         // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "2 1")) // TODO: What's the expected result in TiDB? Currently it's [1 1],[2 1]
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)

	// case-6: duplicate primary key in insert, and primary key is not handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 0;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk varchar(10) key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustQuery("select _tidb_rowid, pk, val from t").Check(testkit.Rows("1 1 1"))
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1,2)")                         // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "1 2")) // TODO: What's the expected result in TiDB? Currently it's [1 1],[1 2].
	tk.MustGetDBError("commit", kv.ErrKeyExists)

	// case-7: duplicate unique key in insert, and query condition filtered the conflicate row in tikv.

}
