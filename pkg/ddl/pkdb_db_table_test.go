// Copyright 2025 PingCAP, Inc.
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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateTableAsSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create source tables
	tk.MustExec("set @@global.tidb_schema_cache_size= 0")
	tk.MustExec("create table t1 (id int primary key, b int);")
	tk.MustExec("create table t_ref (id int primary key, name varchar(20));")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3);")
	tk.MustExec("insert into t_ref values (1,'apple'),(2,'banana'),(3,'cherry');")

	// Case 1: Basic create table as select
	tk.MustGetErrMsg("create table t2 as select * from t1;", "'CREATE TABLE ... SELECT' is not implemented yet")
	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.EnableCreateTableAsSelect = true
	})

	tk.MustExec("create table t2 as select * from t1;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Case 2: Create table with column definitions as select
	tk.MustExec("create table t3 (id int, b int) as select * from t1;")
	tk.MustQuery("select * from t3;").Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery("show create table t3").Check(testkit.Rows("t3 CREATE TABLE `t3` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Case 3: Create table as select with aggregation
	tk.MustExec("create table t4 as select id, sum(b) from t1 group by id;")
	tk.MustQuery("select * from t4;").Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery("show create table t4").Check(testkit.Rows("t4 CREATE TABLE `t4` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `sum(b)` decimal(32,0) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Case 4: Create table with column definitions as select with aggregation
	tk.MustExec("create table t5 (id int, b int) as select id, sum(b) from t1 group by id;")
	tk.MustQuery("select * from t5;").Check(testkit.Rows("<nil> 1 1", "<nil> 2 2", "<nil> 3 3"))
	tk.MustQuery("show create table t5;").Check(testkit.Rows("t5 CREATE TABLE `t5` (\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `sum(b)` decimal(32,0) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Case 5: Create table with not null constraint as select with aggregation (should fail)
	tk.MustGetErrCode("create table t6 (id int, b int not null) as select id, sum(b) from t1 group by id;", errno.ErrNoDefaultForField)
	tk.MustGetErrCode("show create table t6;", errno.ErrNoSuchTable)

	// Case5-1: Create table with not null constraint as select with aggregation and column alias
	tk.MustExec("create table t61 (id int, b int not null) as select b,sum(b),id from t1 group by  id;")
	tk.MustQuery("show create table t61;").Check(testkit.Rows("t61 CREATE TABLE `t61` (\n" +
		"  `b` int(11) NOT NULL,\n" +
		"  `sum(b)` decimal(32,0) DEFAULT NULL,\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Case 6: Create table with same column definitions as select with aggregation and column alias (sum(b) as b)
	tk.MustExec("create table t7 (id int, b int) as select id, sum(b) as b from t1 group by id;")
	tk.MustQuery("select * from t7;").Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery("show create table t7;").Check(testkit.Rows("t7 CREATE TABLE `t7` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Case 7: Create table with unique index as select with duplicate values (should fail)
	tk.MustExec("update t1 set b=1;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 1", "2 1", "3 1"))
	tk.MustGetErrCode("create table t8 (id int, b int, unique index(b)) as select * from t1;", errno.ErrDupEntry)
	tk.MustGetErrCode("show create table t8;", errno.ErrNoSuchTable)

	// Restore t1 to its original state
	tk.MustExec("update t1 set b=id;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 1", "2 2", "3 3"))

	// Case 8: Create table from simple join query
	tk.MustExec("create table t8 as select t1.id, t1.b, t_ref.name from t1 join t_ref on t1.id = t_ref.id;")
	tk.MustQuery("show create table t8;").Check(testkit.Rows("t8 CREATE TABLE `t8` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `name` varchar(20) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t8;").Check(testkit.Rows("1 1 apple", "2 2 banana", "3 3 cherry"))

	// Case 9: Create table from subquery
	tk.MustExec("create table t9 as select * from (select id, b from t1 where b > 1) as subq;")
	tk.MustQuery("show create table t9;").Check(testkit.Rows("t9 CREATE TABLE `t9` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t9;").Check(testkit.Rows("2 2", "3 3"))

	// Case 10: Create table and add secondary index
	tk.MustExec("create table t10 as select * from t1;")
	tk.MustExec("alter table t10 add index idx_b(b);")
	tk.MustQuery("show create table t10;").Check(testkit.Rows("t10 CREATE TABLE `t10` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  KEY `idx_b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Case 11: Create table with foreign key (should fail as CTAS doesn't support FK)
	tk.MustGetErrCode("create table t11 (id int(11) primary key, b int, name varchar(20), "+
		"constraint fk_name foreign key (id) references t_ref(id)) as "+
		"select t1.id, t1.b, t.name from t1 join t_ref on t1.id = t_ref.id;",
		errno.ErrForeignKeyWithCreateAsSelect)

	// Case 12: Create table with generated columns
	tk.MustExec("create table t12 as select id, b, concat('item-', id) as gen_col from t1;")
	tk.MustQuery("show create table t12;").Check(testkit.Rows("t12 CREATE TABLE `t12` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `gen_col` varchar(25) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t12").Check(testkit.Rows("1 1 item-1", "2 2 item-2", "3 3 item-3"))

	// Case 13: Create table with different column order than source
	tk.MustExec("create table t13 (b int, id int) as select id, b from t1;")
	tk.MustQuery("show create table t13;").Check(testkit.Rows("t13 CREATE TABLE `t13` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t13").Check(testkit.Rows("1 1", "2 2", "3 3"))

	// Case 14: Create table with complex union query  (TODO due to default value)
	// tk.MustExec("create table t14 as select id, b from t1 where id = 1 union select id, b from t1 where id = 2;")
	// tk.MustQuery("show create table t14").Check(testkit.Rows("t14 CREATE TABLE `t14` (\n" +
	// 	"  `id` int(11) NOT NULL DEFAULT '0',\n" +
	// 	"  `b` int(11) DEFAULT NULL\n" +
	// 	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// tk.MustQuery("select * from t14").Check(testkit.Rows("1 1", "2 2"))

	// Case 15: Create table with window function  (TODO due to default value)
	// tk.MustExec("create table t15 as select id, b, rank() over (order by b desc) as rnk from t1;")
	// tk.MustQuery("show create table t15").Check(testkit.Rows("t15 CREATE TABLE `t15` (\n" +
	// 	"  `id` int(11) NOT NULL,\n" +
	// 	"  `b` int(11) DEFAULT NULL,\n" +
	// 	"  `rnk` bigint(21) unsigned NOT NULL DEFAULT '0'\n" +
	// 	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// tk.MustQuery("select * from t15").Check(testkit.Rows("3 3 1", "2 2 2", "1 1 3"))

	// Case 16: Create table with JSON data
	tk.MustExec("create table t16 as select id, b, json_object('id', id, 'value', b) as jdata from t1;")
	tk.MustQuery("show create table t16;").Check(testkit.Rows("t16 CREATE TABLE `t16` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `jdata` json DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// tk.MustQuery("select id, b, json_extract(jdata, '$.id') from t16").Check(testkit.Rows("1 1 1", "2 2 2", "3 3 3"))

	// Case 17: Create table with default value

	// tk.MustExec("create table t200 (id INT NOT NULL DEFAULT 100, name VARCHAR(50) DEFAULT 'unknown', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
	// tk.MustExec("CREATE TABLE t201 AS SELECT * FROM t200;")
	// tk.MustQuery("show create table t201").Check(testkit.Rows("t201 CREATE TABLE `t201` (\n" +
	// 	"  `id` int(11) NOT NULL DEFAULT '100',\n" +
	// 	"  `name` varchar(50) DEFAULT 'unknown',\n" +
	// 	"  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP\n" +
	// 	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestCreateTableAsSelectPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists otherdb")
	tk.MustExec("create database otherdb")
	tk.MustExec("create user 'u1'@'%' identified by '';")

	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.EnableCreateTableAsSelect = true
	})

	// Create source tables
	tk.MustExec("create table t1 (id int primary key, b int);")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3);")
	tk.MustExec("use otherdb")
	tk.MustExec("create table t1 (id int primary key, b int);")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3);")
	tk.MustExec("use test")

	// Part 1: Test regular CREATE TABLE AS SELECT privilege requirements

	// Create connection for u1
	tk2 := testkit.NewTestKit(t, store)
	tk2.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost", CurrentUser: true, AuthUsername: "u1", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)

	// Without any privileges
	err := tk2.ExecToErr("create table test.t2 as select * from test.t1;")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]CREATE command denied to user 'u1'@'%' for table 't2'", err.Error())

	// GRANT CREATE
	tk.MustExec("grant create on test.* to 'u1'@'%';")
	err = tk2.ExecToErr("create table test.t2 as select * from test.t1;")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]INSERT command denied to user 'u1'@'%' for table 't2'", err.Error())

	// GRANT INSERT
	tk.MustExec("grant insert on test.* to 'u1'@'%';")
	err = tk2.ExecToErr("create table test.t2 as select * from test.t1;")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]SELECT command denied to user 'u1'@'%' for table 't1'", err.Error())

	// GRANT SELECT
	tk.MustExec("grant select on test.* to 'u1'@'%';")
	tk2.MustExec("create table test.t2 as select * from test.t1;")
	tk2.MustQuery("select * from test.t2").Check(testkit.Rows("1 1", "2 2", "3 3"))

	// Part 2: Test cross-database CREATE TABLE AS SELECT privilege requirements

	// Clean up previous test tables
	tk.MustExec("drop table if exists test.t2")

	// Test cross-database CREATE TABLE AS SELECT
	// User needs: CREATE privilege on test database, INSERT privilege on the target table,
	// and SELECT privilege on otherdb.t1

	// Without any additional privileges
	err = tk2.ExecToErr("create table test.t3 as select * from otherdb.t1;")
	require.Error(t, err)
	// Already has CREATE and INSERT on test database, but needs SELECT on otherdb.t1
	require.Equal(t, "[planner:1142]SELECT command denied to user 'u1'@'%' for table 't1'", err.Error())

	// Grant SELECT on otherdb.t1
	tk.MustExec("grant select on otherdb.t1 to 'u1'@'%';")
	tk2.MustExec("create table test.t3 as select * from otherdb.t1;")

	// Test querying from the created table
	tk2.MustQuery("select * from test.t3;").Check(testkit.Rows("1 1", "2 2", "3 3"))

	// Clean up
	tk.MustExec("drop database if exists otherdb")
	tk.MustExec("drop table if exists test.t2, test.t3")
	tk.MustExec("drop user 'u1'@'%';")
}

func TestCreateTableAsSelectUnionPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists otherdb")
	tk.MustExec("create database otherdb")
	tk.MustExec("create user 'u1'@'%' identified by '';")

	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.EnableCreateTableAsSelect = true
	})

	// Create source tables in test database
	tk.MustExec("create table t1 (id int primary key, b int);")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3);")
	tk.MustExec("create table t2 (id int primary key, b int);")
	tk.MustExec("insert into t2 values (4,4),(5,5),(6,6);")

	// Create source tables in otherdb
	tk.MustExec("use otherdb")
	tk.MustExec("create table t1 (id int primary key, b int);")
	tk.MustExec("insert into t1 values (7,7),(8,8),(9,9);")
	tk.MustExec("use test")

	// Create connection for u1
	tk2 := testkit.NewTestKit(t, store)
	tk2.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost", CurrentUser: true, AuthUsername: "u1", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)

	// Part 1: Test UNION within same database
	// Without any privileges
	err := tk2.ExecToErr("create table test.t3 as select * from test.t1 union select * from test.t2;")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]CREATE command denied to user 'u1'@'%' for table 't3'", err.Error())

	// GRANT CREATE
	tk.MustExec("grant create on test.* to 'u1'@'%';")
	err = tk2.ExecToErr("create table test.t3 as select * from test.t1 union select * from test.t2;")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]INSERT command denied to user 'u1'@'%' for table 't3'", err.Error())

	// GRANT INSERT
	tk.MustExec("grant insert on test.* to 'u1'@'%';")
	err = tk2.ExecToErr("create table test.t3 as select * from test.t1 union select * from test.t2;")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]SELECT command denied to user 'u1'@'%' for table 't1'", err.Error())

	// GRANT SELECT on both tables
	tk.MustExec("grant select on test.* to 'u1'@'%';")
	tk2.MustExec("create table test.t3 as select * from test.t1 union select * from test.t2;")
	tk2.MustQuery("select * from test.t3 order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6"))

	// Part 2: Test UNION across databases
	// Clean up previous test table
	tk.MustExec("drop table if exists test.t3")

	// Without SELECT on otherdb.t1
	err = tk2.ExecToErr("create table test.t4 as select * from test.t1 union select * from otherdb.t1;")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]SELECT command denied to user 'u1'@'%' for table 't1'", err.Error())

	// Grant SELECT on otherdb.t1
	tk.MustExec("grant select on otherdb.t1 to 'u1'@'%';")
	tk2.MustExec("create table test.t4 as select * from test.t1 union select * from otherdb.t1;")
	tk2.MustQuery("select * from test.t4 order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "7 7", "8 8", "9 9"))

	// Part 3: Test complex UNION with WHERE clauses
	tk.MustExec("drop table if exists test.t5")
	tk2.MustExec("create table test.t5 as select * from test.t1 where id > 1 union select * from otherdb.t1 where id < 9;")
	tk2.MustQuery("select * from test.t5 order by id").Check(testkit.Rows("2 2", "3 3", "7 7", "8 8"))

	// Clean up
	tk.MustExec("drop database if exists otherdb")
	tk.MustExec("drop table if exists test.t3, test.t4, test.t5")
	tk.MustExec("drop user 'u1'@'%';")
}
