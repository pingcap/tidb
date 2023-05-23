package executor_test

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
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

	// case-1: duplicate primary key in insert, and primary key is handle.
	tk.MustExec("create table t (pk int key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (1,2)")                  // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2")) // TODO: What's the expected result in TiDB?
	tk.MustGetDBError("commit", kv.ErrKeyExists)

	// case-2: duplicate unique key in insert, and primary key is handle.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int, unique index (val))")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (2,1)")                         // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "2 1")) // TODO: What's the expected result in TiDB?
	tk.MustGetDBError("commit", kv.ErrKeyExists)

	// case-3: duplicate primary key in insert, and primary key is not handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 0;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk varchar(10) key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustQuery("select _tidb_rowid, pk, val from t").Check(testkit.Rows("1 1 1"))
	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (1,2)")                         // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "1 2")) // TODO: What's the expected result in TiDB?
	tk.MustGetDBError("commit", kv.ErrKeyExists)

	// case-4: in pessimistic transaction, duplicate primary key in insert, and primary key is handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1,2)")                  // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2")) // TODO: What's the expected result in TiDB?
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)

	// case-5: in pessimistic transaction, duplicate unique key in insert, and primary key is handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int, unique index (val))")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (2,1)")                         // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "2 1")) // TODO: What's the expected result in TiDB?
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
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "1 2")) // TODO: What's the expected result in TiDB?
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)

	// case-7: duplicate unique key in insert, and query condition filtered the conflict row in tikv.
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int, unique index(val))")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (2,1)")                               // MySQL returns error here, so following query result in MySQL is empty.
	tk.MustQuery("select * from t where pk > 1").Check(testkit.Rows("2 1")) // TODO: What's the expected result in TiDB?
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)
}

func TestUnionScanIssue24195(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_constraint_check_in_place = false")
	tk1.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set @@tidb_constraint_check_in_place_pessimistic=on")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("set @@tidb_constraint_check_in_place = false")
	tk2.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk2.MustExec("set @@tidb_constraint_check_in_place_pessimistic=on")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	tk3.MustExec("set @@tidb_constraint_check_in_place = false")
	tk3.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk3.MustExec("set @@tidb_constraint_check_in_place_pessimistic=on")

	/*
		// case-1: when primary key is clustered index
		tk1.MustExec("set @@tidb_enable_clustered_index = 1;")
		tk1.MustExec("drop table if exists t;")
		tk1.MustExec("create table t (c1 varchar(10), c2 int, c3 char(20), primary key (c1, c2));")
		tk1.MustExec("insert into t values ('tag', 10, 't'), ('cat', 20, 'c');")
		tk2.MustExec("begin;")
		tk2.MustExec("update t set c1=reverse(c1) where c1='tag';")
		ch := make(chan struct{}, 0)
		go func() {
			tk3.MustExec("begin")
			tk3.MustExec("insert into t values('dress',40,'d'),('tag', 10, 't');")
			tk3.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "dress 40 d", "tag 10 t"))
			ch <- struct{}{}
		}()
		time.Sleep(time.Second * 2)
		tk2.MustExec("commit")
		_ = <-ch

		// case-2: when primary key is non-clustered index
		tk1.MustExec("rollback")
		tk2.MustExec("rollback")
		tk3.MustExec("rollback")
		tk1.MustExec("set @@tidb_enable_clustered_index = 0;")
		tk1.MustExec("drop table if exists t;")
		tk1.MustExec("create table t (c1 varchar(10), c2 int, c3 char(20), primary key (c1, c2));")
		tk1.MustExec("insert into t values ('tag', 10, 't'), ('cat', 20, 'c');")
		tk2.MustExec("begin;")
		tk2.MustExec("update t set c1=reverse(c1) where c1='tag';")
		tk2.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "gat 10 t"))
		ch = make(chan struct{}, 0)
		go func() {
			tk3.MustExec("begin")
			tk3.MustExec("insert into t values('dress',40,'d'),('tag', 10, 't');")
			tk3.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "dress 40 d", "tag 10 t", "tag 10 t"))
			tk3.MustQuery("select * from t use index(primary) order by c1,c2 for update;").Check(testkit.Rows("cat 20 c", "dress 40 d", "gat 10 t", "tag 10 t"))
			tk3.MustExec("commit")
			tk3.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "dress 40 d", "gat 10 t", "tag 10 t"))
			ch <- struct{}{}
		}()
		time.Sleep(time.Second * 2)
		tk2.MustExec("commit")
		_ = <-ch
	*/

	// case-3: insert some record into the table，union scan can not merge when update clustered index set the value of the first record bigger than the second record;
	tk1.MustExec("rollback")
	tk2.MustExec("rollback")
	tk3.MustExec("rollback")
	tk1.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec("create table t(a int primary key, b int);")
	tk1.MustExec("insert into t values(1,1),(5,5),(10,10);")

	tk1.MustExec("begin pessimistic;")
	tk1.MustExec("update t set a=8 where a=1;")
	ch := make(chan struct{}, 0)
	go func() {
		tk2.MustExec("begin pessimistic;")
		//tk2.MustQuery("select * from t;").Check(testkit.Rows("1 1", "5 5", "10 10"))
		tk2.MustExec("update t set a=a+1;")
		logutil.BgLogger().Info("tk2 after update -----------------------")
		tk2.MustQuery("select * from t;").Check(testkit.Rows("1 1", "6 5", "9 1", "11 10"))
		//tk2.MustQuery("select * from t for update;").Check(testkit.Rows("6 5", "9 1", "11 10"))
		tk2.MustExec("commit")
		tk2.MustQuery("select * from t;").Check(testkit.Rows("6 5", "9 1", "11 10"))
		ch <- struct{}{}
	}()
	time.Sleep(time.Second * 3)
	tk1.MustExec("commit")
	logutil.BgLogger().Info("tk1 after commit -----------------------")
	_ = <-ch
}
