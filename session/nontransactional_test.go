package session_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestNonTransactionalDelete(t *testing.T) {
	if !*withTiKV {
		return
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")

	tables := []string{
		"create table t(a int, b int, primary key(a, b) clustered)",
		"create table t(a int, b int, primary key(a, b) nonclustered)",
		"create table t(a int, b int, primary key(a) clustered)",
		"create table t(a int, b int, primary key(a) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a, b) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a) clustered)",
		"create table t(a varchar(30), b int, primary key(a) nonclustered)",
	}
	for _, table := range tables {
		tk.MustExec("drop table if exists t")
		tk.MustExec(table)
		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		}
		tk.MustExec("split on a limit 3 delete from t")
		tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))

		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		}
		if strings.Contains(table, "a int") {
			tk.MustQuery("split on a limit 3 dry run delete from t").Check(testkit.Rows(
				"DELETE FROM `test`.`t` WHERE `a` BETWEEN 0 AND 34",
				"DELETE FROM `test`.`t` WHERE `a` BETWEEN 35 AND 69",
				"DELETE FROM `test`.`t` WHERE `a` BETWEEN 70 AND 99"),
			)
		}
		tk.MustQuery("split on a limit 3 dry run query delete from t").Check(testkit.Rows(
			"select `a` from `test`.`t` where true order by `a`"))
	}
}

func TestNonTransactionalDeleteErrorMessage(t *testing.T) {
	if !*withTiKV {
		return
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	failpoint.Enable("github.com/pingcap/tidb/session/splitDeleteError", `return`)
	failpoint.Disable("github.com/pingcap/tidb/session/splitDeleteError")
	tk.MustQuery("split on a limit 3 delete from t").Check(testkit.Rows(
		"job id: 1, job size: 35, range: [KindInt64 0, KindInt64 34] DELETE FROM `test`.`t` WHERE `a` BETWEEN 0 AND 34 injected split delete error",
		"job id: 2, job size: 35, range: [KindInt64 35, KindInt64 69] DELETE FROM `test`.`t` WHERE `a` BETWEEN 35 AND 69 injected split delete error",
		"job id: 3, job size: 30, range: [KindInt64 70, KindInt64 99] DELETE FROM `test`.`t` WHERE `a` BETWEEN 70 AND 99 injected split delete error"))
}

func TestNonTransactionalDeleteSplitOnTiDBRowID(t *testing.T) {
	if !*withTiKV {
		return
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("split on _tidb_rowid limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteNull(t *testing.T) {
	if !*withTiKV {
		return
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.MustExec("insert into t values (null, null)")
	}

	tk.MustExec("split on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))

	// all values are null
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (null, null)")
	}
	tk.MustExec("split on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteSmallBatch(t *testing.T) {
	if !*withTiKV {
		return
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=1024")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.MustExec("insert into t values (null, null)")
	}
	require.Equal(t, 1, len(tk.MustQuery("split on a limit 1000 dry run delete from t").Rows()))
	tk.MustExec("split on a limit 1000 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}
