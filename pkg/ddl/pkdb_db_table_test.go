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
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateTableAsSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@session.tidb_create_from_select_using_import = false")
	tk.MustExec("use test")

	showCreateTable := func(table string) string {
		return tk.MustQuery("show create table " + table).Rows()[0][1].(string)
	}

	tk.MustExec("create table t1 (id int primary key, b int)")
	tk.MustExec("create table t_ref (id int primary key, name varchar(20))")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(4,1),(5,2),(6,3)")
	tk.MustExec("insert into t_ref values (1,'apple'),(2,'banana'),(3,'cherry')")

	// CREATE TABLE ... [AS] SELECT ...
	tk.MustExec("create table t2 as select * from t1")
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 1", "5 2", "6 3"))
	tk.MustExec("drop table t2")
	tk.MustExec("create table t2 select * from t1")
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 1", "5 2", "6 3"))

	tk.MustExec("create table tdefs(c int) as select * from t1")
	tk.MustQuery("show create table tdefs").Check(testkit.Rows("tdefs CREATE TABLE `tdefs` (\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select column_name, data_type from information_schema.columns where table_schema='test' and table_name='tdefs' order by ordinal_position").
		Check(testkit.Rows("c int", "id int", "b int"))
	tk.MustQuery("select c is null, id, b from tdefs order by id").Check(testkit.Rows("1 1 1", "1 2 2", "1 3 3", "1 4 1", "1 5 2", "1 6 3"))

	tk.MustExec("create table t3 (id tinyint, b varchar(20)) as select * from t1")
	tk.MustQuery("show create table t3").Check(testkit.Rows("t3 CREATE TABLE `t3` (\n" +
		"  `id` tinyint(4) DEFAULT NULL,\n" +
		"  `b` varchar(20) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select column_name, data_type from information_schema.columns where table_schema='test' and table_name='t3' order by ordinal_position").
		Check(testkit.Rows("id tinyint", "b varchar"))
	tk.MustQuery("select * from t3 order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 1", "5 2", "6 3"))

	tk.MustExec("create table t4 as select b, sum(id) as idsum from t1 group by b")
	tk.MustQuery("show create table t4").Check(testkit.Rows("t4 CREATE TABLE `t4` (\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `idsum` decimal(32,0) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t4 order by b").Check(testkit.Rows("1 5", "2 7", "3 9"))

	tk.MustExec("create table t5 (idsum tinyint, b varchar(20)) as select b, sum(id) as idsum from t1 group by b")
	tk.MustQuery("show create table t5").Check(testkit.Rows("t5 CREATE TABLE `t5` (\n" +
		"  `b` varchar(20) DEFAULT NULL,\n" +
		"  `idsum` tinyint(4) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select column_name, data_type from information_schema.columns where table_schema='test' and table_name='t5' order by ordinal_position").
		Check(testkit.Rows("b varchar", "idsum tinyint"))
	tk.MustQuery("select * from t5 order by b").Check(testkit.Rows("1 5", "2 7", "3 9"))

	tk.MustExec("create table t6 (id int, sumb int not null) as select id, sum(b) as sumb from t1 group by id")
	tk.MustQuery("show create table t6").Check(testkit.Rows("t6 CREATE TABLE `t6` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `sumb` int(11) NOT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select column_name, is_nullable from information_schema.columns where table_schema='test' and table_name='t6' order by ordinal_position").
		Check(testkit.Rows("id YES", "sumb NO"))
	tk.MustQuery("select * from t6 order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 1", "5 2", "6 3"))

	tk.MustExec("create table t8 as select t1.id, t1.b, t_ref.name from t1 join t_ref on t1.id = t_ref.id")
	tk.MustQuery("show create table t8").Check(testkit.Rows("t8 CREATE TABLE `t8` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `name` varchar(20) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t8 order by id").Check(testkit.Rows("1 1 apple", "2 2 banana", "3 3 cherry"))
	tk.MustExec("drop table t8")

	tk.MustExec("create table t9 as select * from (select id, b from t1 where b > 1) as subq")
	tk.MustQuery("show create table t9").Check(testkit.Rows("t9 CREATE TABLE `t9` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t9 order by id").Check(testkit.Rows("2 2", "3 3", "5 2", "6 3"))

	tk.MustGetErrCode(`create table t11 (
		id int primary key,
		constraint fk_name foreign key (id) references t_ref(id)
	) as select * from t1`, errno.ErrForeignKeyWithCreateAsSelect)
	tk.MustGetErrCode("show create table t11", errno.ErrNoSuchTable)

	tk.MustExec("create table t14 as select id, b from t1 where id = 1 union select id, b from t1 where id = 2")
	tk.MustQuery("show create table t14").Check(testkit.Rows("t14 CREATE TABLE `t14` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t14 order by id").Check(testkit.Rows("1 1", "2 2"))

	tk.MustExec("create table t200 (id INT NOT NULL DEFAULT 100, name VARCHAR(50) DEFAULT 'unknown', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	tk.MustExec("create table t300 table t200")
	tk.MustQuery("show create table t300").Check(testkit.Rows("t300 CREATE TABLE `t300` (\n" +
		"  `id` int(11) NOT NULL DEFAULT '100',\n" +
		"  `name` varchar(50) DEFAULT 'unknown',\n" +
		"  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	t300DDL := showCreateTable("t300")
	require.Contains(t, t300DDL, "DEFAULT '100'")
	require.Contains(t, t300DDL, "`name` varchar(50) DEFAULT 'unknown'")
	require.Contains(t, t300DDL, "`created_at` timestamp DEFAULT CURRENT_TIMESTAMP")
	tk.MustQuery("show index from t300").Check(testkit.Rows())
	tk.MustExec("drop table t300, t200")

	// CREATE TABLE ... IGNORE/REPLACE [AS] SELECT ...
	tk.MustGetErrCode("create table t8 (id int, b int, unique index(b)) as select * from t1", errno.ErrDupEntry)
	tk.MustGetErrCode("show create table t8", errno.ErrNoSuchTable)

	tk.MustExec("create table tignore (id int, b int, unique index(b)) ignore as select * from t1")
	tk.MustQuery("show create table tignore").Check(testkit.Rows("tignore CREATE TABLE `tignore` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  UNIQUE KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from tignore order by id").Check(testkit.Rows("1 1", "2 2", "3 3"))

	tk.MustExec("create table treplace (id int, b int, unique index(b)) replace as select * from t1")
	tk.MustQuery("show create table treplace").Check(testkit.Rows("treplace CREATE TABLE `treplace` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  UNIQUE KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from treplace order by id").Check(testkit.Rows("4 1", "5 2", "6 3"))

	// CREATE TABLE ... [AS] TABLE ...
	tk.MustExec("create table tt1 table t1")
	tk.MustQuery("show create table tt1").Check(testkit.Rows("tt1 CREATE TABLE `tt1` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from tt1 order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 1", "5 2", "6 3"))
	tk.MustQuery("show index from tt1").Check(testkit.Rows())
	tk.MustExec("drop table tt1")

	tk.MustExec("create table tt1 as table t1")
	tk.MustQuery("show create table tt1").Check(testkit.Rows("tt1 CREATE TABLE `tt1` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from tt1 order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 1", "5 2", "6 3"))
	tk.MustQuery("show index from tt1").Check(testkit.Rows())

	tk.MustExec("create table tt2 (x INT) table t1")
	tk.MustQuery("show create table tt2").Check(testkit.Rows("tt2 CREATE TABLE `tt2` (\n" +
		"  `x` int(11) DEFAULT NULL,\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select column_name, data_type from information_schema.columns where table_schema='test' and table_name='tt2' order by ordinal_position").
		Check(testkit.Rows("x int", "id int", "b int"))
	tk.MustQuery("select x is null, id, b from tt2 order by id").Check(testkit.Rows("1 1 1", "1 2 2", "1 3 3", "1 4 1", "1 5 2", "1 6 3"))

	// CREATE TABLE IF NOT EXISTS t ...
	tk.MustExec("create table if not exists t_exist as select * from t1")
	tk.MustQuery("show create table t_exist").Check(testkit.Rows("t_exist CREATE TABLE `t_exist` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `b` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t_exist order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 1", "5 2", "6 3"))
	tk.MustExec("create table if not exists t_exist as select * from t1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1050 Table 'test.t_exist' already exists"))

	// constraints
	tk.MustExec("create table t200 (" +
		"id INT NOT NULL AUTO_INCREMENT, " +
		"name VARCHAR(50) DEFAULT 'unknown', " +
		"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
		"PRIMARY KEY (id))")
	tk.MustExec("create table t300 as table t200")
	tk.MustQuery("show create table t300").Check(testkit.Rows("t300 CREATE TABLE `t300` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `name` varchar(50) DEFAULT 'unknown',\n" +
		"  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	t300DDL = showCreateTable("t300")
	require.Contains(t, t300DDL, "`name` varchar(50) DEFAULT 'unknown'")
	require.Contains(t, t300DDL, "`created_at` timestamp DEFAULT CURRENT_TIMESTAMP")
	require.NotContains(t, t300DDL, "PRIMARY KEY")
	require.NotContains(t, t300DDL, "AUTO_INCREMENT")
	tk.MustQuery("show index from t300").Check(testkit.Rows())

	// can't use for update
	tk.MustGetErrCode("create table tupdate as select * from t1 for update", errno.ErrCantUpdateTableInCreateTableSelect)
	tk.MustGetErrCode("show create table tupdate", errno.ErrNoSuchTable)

	// test window function
	tk.MustExec("create table twin as select id, row_number() over() as rn from t1;")
	tk.MustQuery("select * from twin order by id").Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6"))

	// test column default value when union operator is used
	tk.MustExec("create table tunion as select id, name from t200 where id = 1 union select id, name from t200 where id = 2;")
	tk.MustQuery("select * from tunion order by id").Check(testkit.Rows())
	tk.MustQuery("show create table tunion").Check(testkit.Rows("tunion CREATE TABLE `tunion` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `name` varchar(50) DEFAULT 'unknown'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestCreateTableAsSelectNoWarning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	originValue := parsertypes.TiDBStrictIntegerDisplayWidth
	parsertypes.TiDBStrictIntegerDisplayWidth = true
	defer func() {
		parsertypes.TiDBStrictIntegerDisplayWidth = originValue
	}()

	tk.MustExec("create table t1 (id int, b int)")
	tk.MustExec("create table t2 as select * from t1")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int DEFAULT NULL,\n" +
		"  `b` int DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("create table t3 as select count(*) as c from t1")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("show create table t3").Check(testkit.Rows("t3 CREATE TABLE `t3` (\n" +
		"  `c` bigint NOT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestCreateTableAsSelectWithStoredFunction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()

	tk.MustExec("create database BG00DSCB00")
	tk.MustExec("use BG00DSCB00")
	tk.MustExec("create table src(id2 varchar(36), extend_1 varchar(400))")
	tk.MustExec("insert into src values ('row1', 'ABCDEFGFXZZ')")
	tk.MustExec("set global tidb_enable_procedure = ON")
	tk.MustExec("set @@session.sql_mode = 'ANSI_QUOTES'")
	tk.MustExec(`create function instr_n(str varchar(1000), sub varchar(255), start_pos int, occurrence int)
returns int
deterministic
begin
	declare pos int default 0;
	declare i int default 1;
	declare search_start int default start_pos;
	if occurrence <= 0 then
		return 0;
	end if;
	while i <= occurrence do
		set pos = locate(sub, str, search_start);
		if pos = 0 then
			return 0;
		end if;
		set search_start = pos + length(sub);
		set i = i + 1;
	end while;
	return pos;
end`)
	tk.MustExec("set @@session.sql_mode = 'NO_ENGINE_SUBSTITUTION'")
	tk.MustQuery("select @@session.sql_mode").Check(testkit.Rows("NO_ENGINE_SUBSTITUTION"))

	tk.MustExec("create table t_sf_u as " +
		"select id2, substring(extend_1, 1, 7) as extend_fx " +
		"from BG00DSCB00.src " +
		"where length(extend_1) > 8 and instr_n(extend_1, 'FX', 1, 1) > 0")
	tk.MustQuery("select * from t_sf_u").Check(testkit.Rows("row1 ABCDEFG"))
	tk.MustQuery("select @@session.sql_mode").Check(testkit.Rows("NO_ENGINE_SUBSTITUTION"))

	tk.MustExec("create table t_sf_q as " +
		"select id2, substring(extend_1, 1, 7) as extend_fx " +
		"from BG00DSCB00.src " +
		"where length(extend_1) > 8 and BG00DSCB00.instr_n(extend_1, 'FX', 1, 1) > 0")
	tk.MustQuery("select * from t_sf_q").Check(testkit.Rows("row1 ABCDEFG"))
	tk.MustQuery("select @@session.sql_mode").Check(testkit.Rows("NO_ENGINE_SUBSTITUTION"))
}

func TestCreateTableAsSelectDoesNotCorruptInfoSchemaColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	defer config.RestoreFunc()

	tk.MustExec("create database crash_test")
	tk.MustExec("use crash_test")
	tk.MustExec("create table t1 (id int, val decimal(10,2))")
	tk.MustExec("create table t2 (id int, val decimal(10,2))")
	tk.MustExec(`create view v_crash as
		select a.id, a.val, count(*) as cnt
		from t1 a join t2 b on a.id = b.id
		group by a.id`)

	tk.MustExec("create table t_ctas as select 1 as x")
	tk.MustQuery(`select table_name, column_name from information_schema.columns
		where table_schema = 'crash_test' and data_type = 'decimal'
		order by table_name, ordinal_position`).Check(testkit.Rows(
		"t1 val",
		"t2 val",
		"v_crash val",
	))
}

func TestCreateTableAsSelectPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists otherdb")
	tk.MustExec("create database otherdb")
	tk.MustExec("create user 'u1'@'%' identified by '';")

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
