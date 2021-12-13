// Copyright 2021 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestUpdate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	fillData(tk, "update_test")

	updateStr := `UPDATE update_test SET name = "abc" where id > 0;`
	tk.MustExec(updateStr)
	tk.CheckExecResult(2, 0)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")

	// select data
	tk.MustExec("begin")
	r := tk.MustQuery(`SELECT * from update_test limit 2;`)
	r.Check(testkit.Rows("1 abc", "2 abc"))
	tk.MustExec("commit")

	tk.MustExec(`UPDATE update_test SET name = "foo"`)
	tk.CheckExecResult(2, 0)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")

	// table option is auto-increment
	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), primary key(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	tk.MustExec("update update_test set id = 8 where name = 'aa'")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("insert into update_test(name) values ('bb')")
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from update_test;")
	r.Check(testkit.Rows("8 aa", "9 bb"))
	tk.MustExec("commit")

	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), index(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	_, err := tk.Exec("update update_test set id = null where name = 'aa'")
	require.EqualError(t, err, "[table:1048]Column 'id' cannot be null")

	tk.MustExec("drop table update_test")
	tk.MustExec("create table update_test(id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into update_test(id) values (1)")
	tk.MustExec("update update_test set id = 2 where id = 1 limit 1")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("select * from update_test;")
	r.Check(testkit.Rows("2"))
	tk.MustExec("commit")

	// Test that in a transaction, when a constraint failed in an update statement, the record is not inserted.
	tk.MustExec("create table update_unique (id int primary key, name int unique)")
	tk.MustExec("insert update_unique values (1, 1), (2, 2);")
	tk.MustExec("begin")
	_, err = tk.Exec("update update_unique set name = 1 where id = 2")
	require.Error(t, err)
	tk.MustExec("commit")
	tk.MustQuery("select * from update_unique").Check(testkit.Rows("1 1", "2 2"))

	// test update ignore for pimary key
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint, primary key (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	_, err = tk.Exec("update ignore t set a = 1 where a = 2;")
	require.NoError(t, err)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	// test update ignore for truncate as warning
	_, err = tk.Exec("update ignore t set a = 1 where a = (select '2a')")
	require.NoError(t, err)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect DOUBLE value: '2a'", "Warning 1292 Truncated incorrect DOUBLE value: '2a'", "Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))

	tk.MustExec("update ignore t set a = 42 where a = 2;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "42"))

	// test update ignore for unique key
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint, unique key I_uniq (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	_, err = tk.Exec("update ignore t set a = 1 where a = 2;")
	require.NoError(t, err)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'I_uniq'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	// test issue21965
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("create table t (a int) partition by list (a) (partition p0 values in (0,1));")
	tk.MustExec("insert ignore into t values (1);")
	tk.MustExec("update ignore t set a=2 where a=1;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 0")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int key) partition by list (a) (partition p0 values in (0,1));")
	tk.MustExec("insert ignore into t values (1);")
	tk.MustExec("update ignore t set a=2 where a=1;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 0")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id integer auto_increment, t1 datetime, t2 datetime, primary key (id))")
	tk.MustExec("insert into t(t1, t2) values('2000-10-01 01:01:01', '2017-01-01 10:10:10')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2000-10-01 01:01:01 2017-01-01 10:10:10"))
	tk.MustExec("update t set t1 = '2017-10-01 10:10:11', t2 = date_add(t1, INTERVAL 10 MINUTE) where id = 1")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2017-10-01 10:10:11 2000-10-01 01:11:01"))

	// for issue #5132
	tk.MustExec("CREATE TABLE `tt1` (" +
		"`a` int(11) NOT NULL," +
		"`b` varchar(32) DEFAULT NULL," +
		"`c` varchar(32) DEFAULT NULL," +
		"PRIMARY KEY (`a`)," +
		"UNIQUE KEY `b_idx` (`b`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
	tk.MustExec("insert into tt1 values(1, 'a', 'a');")
	tk.MustExec("insert into tt1 values(2, 'd', 'b');")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "2 d b"))
	tk.MustExec("update tt1 set a=5 where c='b';")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "5 d b"))

	// Automatic Updating for TIMESTAMP
	tk.MustExec("CREATE TABLE `tsup` (" +
		"`a` int," +
		"`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
		"KEY `idx` (`ts`)" +
		");")
	tk.MustExec("set @orig_sql_mode=@@sql_mode; set @@sql_mode='';")
	tk.MustExec("insert into tsup values(1, '0000-00-00 00:00:00');")
	tk.MustExec("update tsup set a=5;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	r1 := tk.MustQuery("select ts from tsup use index (idx);")
	r2 := tk.MustQuery("select ts from tsup;")
	r1.Check(r2.Rows())
	tk.MustExec("update tsup set ts='2019-01-01';")
	tk.MustQuery("select ts from tsup;").Check(testkit.Rows("2019-01-01 00:00:00"))
	tk.MustExec("set @@sql_mode=@orig_sql_mode;")

	// issue 5532
	tk.MustExec("create table decimals (a decimal(20, 0) not null)")
	tk.MustExec("insert into decimals values (201)")
	// A warning rather than data truncated error.
	tk.MustExec("update decimals set a = a + 1.23;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect DECIMAL value: '202.23'"))
	r = tk.MustQuery("select * from decimals")
	r.Check(testkit.Rows("202"))

	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (	`c1` year DEFAULT NULL, `c2` year DEFAULT NULL, `c3` date DEFAULT NULL, `c4` datetime DEFAULT NULL,	KEY `idx` (`c1`,`c2`))")
	_, err = tk.Exec("UPDATE t SET c2=16777215 WHERE c1>= -8388608 AND c1 < -9 ORDER BY c1 LIMIT 2")
	require.NoError(t, err)

	tk.MustGetErrCode("update (select * from t) t set c1 = 1111111", mysql.ErrNonUpdatableTable)

	// test update ignore for bad null error
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (i int not null default 10)`)
	tk.MustExec("insert into t values (1)")
	tk.MustExec("update ignore t set i = null;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1048 Column 'i' cannot be null"))
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))

	// issue 7237, update subquery table should be forbidden
	tk.MustExec("drop table t")
	tk.MustExec("create table t (k int, v int)")
	_, err = tk.Exec("update t, (select * from t) as b set b.k = t.k")
	require.EqualError(t, err, "[planner:1288]The target table b of the UPDATE is not updatable")
	tk.MustExec("update t, (select * from t) as b set t.k = b.k")

	// issue 8045
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (c1 float)`)
	tk.MustExec("INSERT INTO t1 SET c1 = 1")
	tk.MustExec("UPDATE t1 SET c1 = 1.2 WHERE c1=1;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")

	// issue 8119
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 float(1,1));")
	tk.MustExec("insert into t values (0.0);")
	_, err = tk.Exec("update t set c1 = 2.0;")
	require.True(t, types.ErrWarnDataOutOfRange.Equal(err))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime not null, b datetime)")
	tk.MustExec("insert into t value('1999-12-12', '1999-12-13')")
	tk.MustExec("set @orig_sql_mode=@@sql_mode; set @@sql_mode='';")
	tk.MustQuery("select * from t").Check(testkit.Rows("1999-12-12 00:00:00 1999-12-13 00:00:00"))
	tk.MustExec("update t set a = ''")
	tk.MustQuery("select * from t").Check(testkit.Rows("0000-00-00 00:00:00 1999-12-13 00:00:00"))
	tk.MustExec("update t set b = ''")
	tk.MustQuery("select * from t").Check(testkit.Rows("0000-00-00 00:00:00 0000-00-00 00:00:00"))
	tk.MustExec("set @@sql_mode=@orig_sql_mode;")

	tk.MustExec("create view v as select * from t")
	_, err = tk.Exec("update v set a = '2000-11-11'")
	require.EqualError(t, err, core.ErrViewInvalid.GenWithStackByArgs("test", "v").Error())
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	tk.MustGetErrCode("update seq set minvalue=1", mysql.ErrBadField)
	tk.MustExec("drop sequence seq")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, d int, e int, index idx(a))")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec("update t1 join t2 on t1.a=t2.a set t1.a=1 where t2.b=1 and t2.c=2")

	// Assign `DEFAULT` in `UPDATE` statement
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int default 1, b int default 2);")
	tk.MustExec("insert into t1 values (10, 10), (20, 20);")
	tk.MustExec("update t1 set a=default where b=10;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "20 20"))
	tk.MustExec("update t1 set a=30, b=default where a=20;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "30 2"))
	tk.MustExec("update t1 set a=default, b=default where a=30;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "1 2"))
	tk.MustExec("insert into t1 values (40, 40)")
	tk.MustExec("update t1 set a=default, b=default")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2", "1 2", "1 2"))
	tk.MustExec("update t1 set a=default(b), b=default(a)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 1", "2 1", "2 1"))
	// With generated columns
	tk.MustExec("create table t2 (a int default 1, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("insert into t2 values (10, default, default), (20, default, default)")
	tk.MustExec("update t2 set b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10", "20 -20 -20"))
	tk.MustExec("update t2 set a=30, b=default where a=10;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("30 -30 -30", "20 -20 -20"))
	tk.MustExec("update t2 set c=default, a=40 where c=-20;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("30 -30 -30", "40 -40 -40"))
	tk.MustExec("update t2 set a=default, b=default, c=default where b=-30;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 -1 -1", "40 -40 -40"))
	tk.MustExec("update t2 set a=default(a), b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 -1 -1", "1 -1 -1"))
	tk.MustGetErrCode("update t2 set b=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("update t2 set a=default(b), b=default(b);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("update t2 set a=default(a), c=default(c);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("update t2 set a=default(a), c=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustExec("drop table t1, t2")
}

func TestListColumnsPartitionWithGlobalIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	// Test generated column with global index
	restoreConfig := config.RestoreFunc()
	defer restoreConfig()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	tableDefs := []string{
		// Test for virtual generated column with global index
		`create table t (a varchar(10), b varchar(1) GENERATED ALWAYS AS (substr(a,1,1)) VIRTUAL) partition by list columns(b) (partition p0 values in ('a','c'), partition p1 values in ('b','d'));`,
		// Test for stored generated column with global index
		`create table t (a varchar(10), b varchar(1) GENERATED ALWAYS AS (substr(a,1,1)) STORED) partition by list columns(b) (partition p0 values in ('a','c'), partition p1 values in ('b','d'));`,
	}
	for _, tbl := range tableDefs {
		tk.MustExec("drop table if exists t")
		tk.MustExec(tbl)
		tk.MustExec("alter table t add unique index (a)")
		tk.MustExec("insert into t (a) values  ('aaa'),('abc'),('acd')")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("aaa", "abc", "acd"))
		tk.MustQuery("select * from t where a = 'abc' order by a").Check(testkit.Rows("abc a"))
		tk.MustExec("update t set a='bbb' where a = 'aaa'")
		tk.MustExec("admin check table t")
		tk.MustQuery("select a from t order by a").Check(testkit.Rows("abc", "acd", "bbb"))
		// TODO: fix below test.
		//tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("abc", "acd"))
		//tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb"))
		tk.MustQuery("select * from t where a = 'bbb' order by a").Check(testkit.Rows("bbb b"))
		// Test insert meet duplicate error.
		_, err := tk.Exec("insert into t (a) values  ('abc')")
		require.Error(t, err)
		// Test insert on duplicate update
		tk.MustExec("insert into t (a) values ('abc') on duplicate key update a='bbc'")
		tk.MustQuery("select a from t order by a").Check(testkit.Rows("acd", "bbb", "bbc"))
		tk.MustQuery("select * from t where a = 'bbc'").Check(testkit.Rows("bbc b"))
		// TODO: fix below test.
		//tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("acd"))
		//tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb", "bbc"))
	}
}

func TestIssue20724(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a varchar(10) collate utf8mb4_general_ci)")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("update t1 set a = 'A'")
	tk.MustQuery("select * from t1").Check(testkit.Rows("A"))
	tk.MustExec("drop table t1")
}

func TestIssue20840(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t1 (i varchar(20) unique key) collate=utf8mb4_general_ci")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("replace into t1 values ('A')")
	tk.MustQuery("select * from t1").Check(testkit.Rows("A"))
	tk.MustExec("drop table t1")
}

func TestIssueInsertPrefixIndexForNonUTF8Collation(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1 ( c_int int, c_str varchar(40) character set ascii collate ascii_bin, primary key(c_int, c_str(8)) clustered , unique key(c_str))")
	tk.MustExec("create table t2 ( c_int int, c_str varchar(40) character set latin1 collate latin1_bin, primary key(c_int, c_str(8)) clustered , unique key(c_str))")
	tk.MustExec("insert into t1 values (3, 'fervent brattain')")
	tk.MustExec("insert into t2 values (3, 'fervent brattain')")
	tk.MustExec("admin check table t1")
	tk.MustExec("admin check table t2")

	tk.MustExec("create table t3 (x varchar(40) CHARACTER SET ascii COLLATE ascii_bin, UNIQUE KEY uk(x(4)))")
	tk.MustExec("insert into t3 select 'abc '")
	tk.MustGetErrCode("insert into t3 select 'abc d'", 1062)
}
