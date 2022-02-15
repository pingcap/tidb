// Copyright 2020 PingCAP, Inc.
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
	"crypto/tls"
	"fmt"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"google.golang.org/grpc"
)

var _ = Suite(&testInfoschemaTableSuite{})

// this SerialSuites is used to solve the data race caused by TableStatsCacheExpiry,
// if your test not change the TableStatsCacheExpiry variable, please use testInfoschemaTableSuite for test.
var _ = SerialSuites(&testInfoschemaTableSerialSuite{})

var _ = SerialSuites(&inspectionSuite{})

type testInfoschemaTableSuiteBase struct {
	store kv.Storage
	dom   *domain.Domain
}

type testInfoschemaTableSuite struct {
	testInfoschemaTableSuiteBase
}

type testInfoschemaTableSerialSuite struct {
	testInfoschemaTableSuiteBase
}

type inspectionSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testInfoschemaTableSuiteBase) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionLog
	})
}

func (s *testInfoschemaTableSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *inspectionSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *inspectionSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *inspectionSuite) TestInspectionTables(c *C) {
	c.Skip("unstable, skip it and fix it before 20210624")
	tk := testkit.NewTestKit(c, s.store)
	instances := []string{
		"pd,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash,0",
		"tidb,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash,1001",
		"tikv,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash,0",
	}
	fpName := "github.com/pingcap/tidb/infoschema/mockClusterInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	c.Assert(failpoint.Enable(fpName, fpExpr), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		"pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 1001",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
	))

	// enable inspection mode
	inspectionTableCache := map[string]variable.TableSnapshot{}
	tk.Se.GetSessionVars().InspectionTableCache = inspectionTableCache
	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		"pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 1001",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
	))
	c.Assert(inspectionTableCache["cluster_info"].Err, IsNil)
	c.Assert(len(inspectionTableCache["cluster_info"].Rows), DeepEquals, 3)

	// check whether is obtain data from cache at the next time
	inspectionTableCache["cluster_info"].Rows[0][0].SetString("modified-pd", mysql.DefaultCollationName)
	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		"modified-pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 1001",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
	))
	tk.Se.GetSessionVars().InspectionTableCache = nil
}

func (s *testInfoschemaTableSuite) TestProfiling(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select * from information_schema.profiling").Check(testkit.Rows())
	tk.MustExec("set @@profiling=1")
	tk.MustQuery("select * from information_schema.profiling").Check(testkit.Rows("0 0  0 0 0 0 0 0 0 0 0 0 0 0   0"))
}

func (s *testInfoschemaTableSuite) TestSchemataTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustQuery("select * from information_schema.SCHEMATA where schema_name='mysql';").Check(
		testkit.Rows("def mysql utf8mb4 utf8mb4_bin <nil> <nil>"))

	// Test the privilege of new user for information_schema.schemata.
	tk.MustExec("create user schemata_tester")
	schemataTester := testkit.NewTestKit(c, s.store)
	schemataTester.MustExec("use information_schema")
	c.Assert(schemataTester.Se.Auth(&auth.UserIdentity{
		Username: "schemata_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	schemataTester.MustQuery("select count(*) from information_schema.SCHEMATA;").Check(testkit.Rows("1"))
	schemataTester.MustQuery("select * from information_schema.SCHEMATA where schema_name='mysql';").Check(
		[][]interface{}{})
	schemataTester.MustQuery("select * from information_schema.SCHEMATA where schema_name='INFORMATION_SCHEMA';").Check(
		testkit.Rows("def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil> <nil>"))

	// Test the privilege of user with privilege of mysql for information_schema.schemata.
	tk.MustExec("CREATE ROLE r_mysql_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.* TO r_mysql_priv;")
	tk.MustExec("GRANT r_mysql_priv TO schemata_tester;")
	schemataTester.MustExec("set role r_mysql_priv")
	schemataTester.MustQuery("select count(*) from information_schema.SCHEMATA;").Check(testkit.Rows("2"))
	schemataTester.MustQuery("select * from information_schema.SCHEMATA;").Check(
		testkit.Rows("def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil> <nil>", "def mysql utf8mb4 utf8mb4_bin <nil> <nil>"))
}

func (s *testInfoschemaTableSuite) TestTableIDAndIndexID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop table if exists test.t")
	tk.MustExec("create table test.t (a int, b int, primary key(a), key k1(b))")
	tk.MustQuery("select index_id from information_schema.tidb_indexes where table_schema = 'test' and table_name = 't'").Check(testkit.Rows("0", "1"))
	tblID, err := strconv.Atoi(tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 't'").Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(tblID, Greater, 0)
}

func (s *testInfoschemaTableSuite) TestSchemataCharacterSet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE DATABASE `foo` DEFAULT CHARACTER SET = 'utf8mb4'")
	tk.MustQuery("select default_character_set_name, default_collation_name FROM information_schema.SCHEMATA  WHERE schema_name = 'foo'").Check(
		testkit.Rows("utf8mb4 utf8mb4_bin"))
	tk.MustExec("drop database `foo`")
}

func (s *testInfoschemaTableSuite) TestViews(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE DEFINER='root'@'localhost' VIEW test.v1 AS SELECT 1")
	tk.MustQuery("select TABLE_COLLATION is null from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='VIEW'").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT * FROM information_schema.views WHERE table_schema='test' AND table_name='v1'").Check(testkit.Rows("def test v1 SELECT 1 AS `1` CASCADED NO root@localhost DEFINER utf8mb4 utf8mb4_bin"))
	tk.MustQuery("SELECT table_catalog, table_schema, table_name, table_type, engine, version, row_format, table_rows, avg_row_length, data_length, max_data_length, index_length, data_free, auto_increment, update_time, check_time, table_collation, checksum, create_options, table_comment FROM information_schema.tables WHERE table_schema='test' AND table_name='v1'").Check(testkit.Rows("def test v1 VIEW <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> VIEW"))
}

func (s *testInfoschemaTableSuite) TestEngines(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select * from information_schema.ENGINES;").Check(testkit.Rows("InnoDB DEFAULT Supports transactions, row-level locking, and foreign keys YES YES YES"))
}

// https://github.com/pingcap/tidb/issues/25467.
func (s *testInfoschemaTableSuite) TestDataTypesMaxLengthAndOctLength(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_oct_length;")
	tk.MustExec("create database test_oct_length;")
	tk.MustExec("use test_oct_length;")

	testCases := []struct {
		colTp  string
		maxLen int
		octLen int
	}{
		{"varchar(255) collate ascii_bin", 255, 255},
		{"varchar(255) collate utf8mb4_bin", 255, 255 * 4},
		{"varchar(255) collate utf8_bin", 255, 255 * 3},
		{"char(10) collate ascii_bin", 10, 10},
		{"char(10) collate utf8mb4_bin", 10, 10 * 4},
		{"set('a', 'b', 'cccc') collate ascii_bin", 8, 8},
		{"set('a', 'b', 'cccc') collate utf8mb4_bin", 8, 8 * 4},
		{"enum('a', 'b', 'cccc') collate ascii_bin", 4, 4},
		{"enum('a', 'b', 'cccc') collate utf8mb4_bin", 4, 4 * 4},
	}
	for _, tc := range testCases {
		createSQL := fmt.Sprintf("create table t (a %s);", tc.colTp)
		tk.MustExec(createSQL)
		result := tk.MustQuery("select character_maximum_length, character_octet_length " +
			"from information_schema.columns " +
			"where table_schema=(select database()) and table_name='t';")
		expectedRows := testkit.Rows(fmt.Sprintf("%d %d", tc.maxLen, tc.octLen))
		result.Check(expectedRows)
		tk.MustExec("drop table t;")
	}
}

func (s *testInfoschemaTableSuite) TestDDLJobs(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_ddl_jobs")
	tk.MustQuery("select db_name, job_type from information_schema.DDL_JOBS limit 1").Check(
		testkit.Rows("test_ddl_jobs create schema"))

	tk.MustExec("use test_ddl_jobs")
	tk.MustExec("create table t (a int);")
	tk.MustQuery("select db_name, table_name, job_type from information_schema.DDL_JOBS where table_name = 't'").Check(
		testkit.Rows("test_ddl_jobs t create table"))

	tk.MustQuery("select job_type from information_schema.DDL_JOBS group by job_type having job_type = 'create table'").Check(
		testkit.Rows("create table"))

	// Test the START_TIME and END_TIME field.
	tk.MustQuery("select distinct job_type from information_schema.DDL_JOBS where job_type = 'create table' and start_time > str_to_date('20190101','%Y%m%d%H%i%s')").Check(
		testkit.Rows("create table"))

	// Test the privilege of new user for information_schema.DDL_JOBS.
	tk.MustExec("create user DDL_JOBS_tester")
	DDLJobsTester := testkit.NewTestKit(c, s.store)
	DDLJobsTester.MustExec("use information_schema")
	c.Assert(DDLJobsTester.Se.Auth(&auth.UserIdentity{
		Username: "DDL_JOBS_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)

	// Test the privilege of user for information_schema.ddl_jobs.
	DDLJobsTester.MustQuery("select DB_NAME, TABLE_NAME from information_schema.DDL_JOBS where DB_NAME = 'test_ddl_jobs' and TABLE_NAME = 't';").Check(
		[][]interface{}{})
	tk.MustExec("CREATE ROLE r_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON test_ddl_jobs.* TO r_priv;")
	tk.MustExec("GRANT r_priv TO DDL_JOBS_tester;")
	DDLJobsTester.MustExec("set role r_priv")
	DDLJobsTester.MustQuery("select DB_NAME, TABLE_NAME from information_schema.DDL_JOBS where DB_NAME = 'test_ddl_jobs' and TABLE_NAME = 't';").Check(
		testkit.Rows("test_ddl_jobs t"))
}

func (s *testInfoschemaTableSuite) TestKeyColumnUsage(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustQuery("select * from information_schema.KEY_COLUMN_USAGE where TABLE_NAME='stats_meta' and COLUMN_NAME='table_id';").Check(
		testkit.Rows("def mysql tbl def mysql stats_meta table_id 1 <nil> <nil> <nil> <nil>"))

	// test the privilege of new user for information_schema.table_constraints
	tk.MustExec("create user key_column_tester")
	keyColumnTester := testkit.NewTestKit(c, s.store)
	keyColumnTester.MustExec("use information_schema")
	c.Assert(keyColumnTester.Se.Auth(&auth.UserIdentity{
		Username: "key_column_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	keyColumnTester.MustQuery("select * from information_schema.KEY_COLUMN_USAGE where TABLE_NAME != 'CLUSTER_SLOW_QUERY';").Check([][]interface{}{})

	// test the privilege of user with privilege of mysql.gc_delete_range for information_schema.table_constraints
	tk.MustExec("CREATE ROLE r_stats_meta ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.stats_meta TO r_stats_meta;")
	tk.MustExec("GRANT r_stats_meta TO key_column_tester;")
	keyColumnTester.MustExec("set role r_stats_meta")
	c.Assert(len(keyColumnTester.MustQuery("select * from information_schema.KEY_COLUMN_USAGE where TABLE_NAME='stats_meta';").Rows()), Greater, 0)
}

func (s *testInfoschemaTableSuite) TestUserPrivileges(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// test the privilege of new user for information_schema.table_constraints
	tk.MustExec("create user constraints_tester")
	constraintsTester := testkit.NewTestKit(c, s.store)
	constraintsTester.MustExec("use information_schema")
	c.Assert(constraintsTester.Se.Auth(&auth.UserIdentity{
		Username: "constraints_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS WHERE TABLE_NAME != 'CLUSTER_SLOW_QUERY';").Check([][]interface{}{})

	// test the privilege of user with privilege of mysql.gc_delete_range for information_schema.table_constraints
	tk.MustExec("CREATE ROLE r_gc_delete_range ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.gc_delete_range TO r_gc_delete_range;")
	tk.MustExec("GRANT r_gc_delete_range TO constraints_tester;")
	constraintsTester.MustExec("set role r_gc_delete_range")
	c.Assert(len(constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='gc_delete_range';").Rows()), Greater, 0)
	constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='tables_priv';").Check([][]interface{}{})

	// test the privilege of new user for information_schema
	tk.MustExec("create user tester1")
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use information_schema")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{
		Username: "tester1",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	tk1.MustQuery("select * from information_schema.STATISTICS WHERE TABLE_NAME != 'CLUSTER_SLOW_QUERY';").Check([][]interface{}{})

	// test the privilege of user with some privilege for information_schema
	tk.MustExec("create user tester2")
	tk.MustExec("CREATE ROLE r_columns_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.columns_priv TO r_columns_priv;")
	tk.MustExec("GRANT r_columns_priv TO tester2;")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use information_schema")
	c.Assert(tk2.Se.Auth(&auth.UserIdentity{
		Username: "tester2",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	tk2.MustExec("set role r_columns_priv")
	result := tk2.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='columns_priv' and COLUMN_NAME='Host';")
	c.Assert(len(result.Rows()), Greater, 0)
	tk2.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='tables_priv' and COLUMN_NAME='Host';").Check(
		[][]interface{}{})

	// test the privilege of user with all privilege for information_schema
	tk.MustExec("create user tester3")
	tk.MustExec("CREATE ROLE r_all_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.* TO r_all_priv;")
	tk.MustExec("GRANT r_all_priv TO tester3;")
	tk3 := testkit.NewTestKit(c, s.store)
	tk3.MustExec("use information_schema")
	c.Assert(tk3.Se.Auth(&auth.UserIdentity{
		Username: "tester3",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	tk3.MustExec("set role r_all_priv")
	result = tk3.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='columns_priv' and COLUMN_NAME='Host';")
	c.Assert(len(result.Rows()), Greater, 0)
	result = tk3.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='tables_priv' and COLUMN_NAME='Host';")
	c.Assert(len(result.Rows()), Greater, 0)
}

func (s *testInfoschemaTableSuite) TestUserPrivilegesTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk1 := testkit.NewTestKit(c, s.store)

	// test the privilege of new user for information_schema.user_privileges
	tk.MustExec("create user usageuser")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "usageuser",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee="'usageuser'@'%'"`).Check(testkit.Rows("'usageuser'@'%' def USAGE NO"))
	// the usage row disappears when there is a non-dynamic privilege added
	tk1.MustExec("GRANT SELECT ON *.* to usageuser")
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee="'usageuser'@'%'"`).Check(testkit.Rows("'usageuser'@'%' def SELECT NO"))
	// test grant privilege
	tk1.MustExec("GRANT SELECT ON *.* to usageuser WITH GRANT OPTION")
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee="'usageuser'@'%'"`).Check(testkit.Rows("'usageuser'@'%' def SELECT YES"))
	// test DYNAMIC privs
	tk1.MustExec("GRANT BACKUP_ADMIN ON *.* to usageuser")
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee="'usageuser'@'%'" ORDER BY privilege_type`).Check(testkit.Rows("'usageuser'@'%' def BACKUP_ADMIN NO", "'usageuser'@'%' def SELECT YES"))
}

func (s *testInfoschemaTableSerialSuite) TestDataForTableStatsField(c *C) {
	s.dom.SetStatsUpdating(true)
	oldExpiryTime := executor.TableStatsCacheExpiry
	executor.TableStatsCacheExpiry = 0
	defer func() { executor.TableStatsCacheExpiry = oldExpiryTime }()
	do := s.dom
	h := do.StatsHandle()
	h.Clear()
	is := do.InfoSchema()
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int, e char(5), index idx(e))")
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("0 0 0 0"))
	tk.MustExec(`insert into t(c, d, e) values(1, 2, "c"), (2, 3, "d"), (3, 4, "e")`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("3 18 54 6"))
	tk.MustExec(`insert into t(c, d, e) values(4, 5, "f")`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("4 18 72 8"))
	tk.MustExec("delete from t where c >= 3")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2 18 36 4"))
	tk.MustExec("delete from t where c=3")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2 18 36 4"))

	// Test partition table.
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t (a int, b int, c varchar(5), primary key(a), index idx(c)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16))`)
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	tk.MustExec(`insert into t(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e")`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("3 18 54 6"))
}

func (s *testInfoschemaTableSerialSuite) TestPartitionsTable(c *C) {
	s.dom.SetStatsUpdating(true)
	oldExpiryTime := executor.TableStatsCacheExpiry
	executor.TableStatsCacheExpiry = 0
	defer func() { executor.TableStatsCacheExpiry = oldExpiryTime }()
	do := s.dom
	h := do.StatsHandle()
	h.Clear()
	is := do.InfoSchema()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	testkit.WithPruneMode(tk, variable.Static, func() {
		c.Assert(h.RefreshVars(), IsNil)
		tk.MustExec("DROP TABLE IF EXISTS `test_partitions`;")
		tk.MustExec(`CREATE TABLE test_partitions (a int, b int, c varchar(5), primary key(a), index idx(c)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16));`)
		err := h.HandleDDLEvent(<-h.DDLEventCh())
		c.Assert(err, IsNil)
		tk.MustExec(`insert into test_partitions(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e");`)

		tk.MustQuery("select PARTITION_NAME, PARTITION_DESCRIPTION from information_schema.PARTITIONS where table_name='test_partitions';").Check(
			testkit.Rows("" +
				"p0 6]\n" +
				"[p1 11]\n" +
				"[p2 16"))

		tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.PARTITIONS where table_name='test_partitions';").Check(
			testkit.Rows("" +
				"0 0 0 0]\n" +
				"[0 0 0 0]\n" +
				"[0 0 0 0"))
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		c.Assert(h.Update(is), IsNil)
		tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.PARTITIONS where table_name='test_partitions';").Check(
			testkit.Rows("" +
				"1 18 18 2]\n" +
				"[1 18 18 2]\n" +
				"[1 18 18 2"))
	})

	// Test for table has no partitions.
	tk.MustExec("DROP TABLE IF EXISTS `test_partitions_1`;")
	tk.MustExec(`CREATE TABLE test_partitions_1 (a int, b int, c varchar(5), primary key(a), index idx(c));`)
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	tk.MustExec(`insert into test_partitions_1(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e");`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustQuery("select PARTITION_NAME, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH from information_schema.PARTITIONS where table_name='test_partitions_1';").Check(
		testkit.Rows("<nil> 3 18 54 6"))

	tk.MustExec("DROP TABLE IF EXISTS `test_partitions`;")
	tk.MustExec(`CREATE TABLE test_partitions1 (id int, b int, c varchar(5), primary key(id), index idx(c)) PARTITION BY RANGE COLUMNS(id) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16));`)
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions1';").Check(testkit.Rows("p0 RANGE COLUMNS id", "p1 RANGE COLUMNS id", "p2 RANGE COLUMNS id"))
	tk.MustExec("DROP TABLE test_partitions1")

	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("create table test_partitions (a int) partition by list (a) (partition p0 values in (1), partition p1 values in (2));")
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions';").Check(testkit.Rows("p0 LIST `a`", "p1 LIST `a`"))
	tk.MustExec("drop table test_partitions")

	tk.MustExec("create table test_partitions (a date) partition by list (year(a)) (partition p0 values in (1), partition p1 values in (2));")
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions';").Check(testkit.Rows("p0 LIST YEAR(`a`)", "p1 LIST YEAR(`a`)"))
	tk.MustExec("drop table test_partitions")

	tk.MustExec("create table test_partitions (a bigint, b date) partition by list columns (a,b) (partition p0 values in ((1,'2020-09-28'),(1,'2020-09-29')));")
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions';").Check(testkit.Rows("p0 LIST COLUMNS a,b"))
	pid, err := strconv.Atoi(tk.MustQuery("select TIDB_PARTITION_ID from information_schema.partitions where table_name = 'test_partitions';").Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(pid, Greater, 0)
	tk.MustExec("drop table test_partitions")
}

func (s *testInfoschemaTableSuite) TestMetricTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use information_schema")
	tk.MustQuery("select count(*) > 0 from `METRICS_TABLES`").Check(testkit.Rows("1"))
	tk.MustQuery("select * from `METRICS_TABLES` where table_name='tidb_qps'").
		Check(testutil.RowsWithSep("|", "tidb_qps|sum(rate(tidb_server_query_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (result,type,instance)|instance,type,result|0|TiDB query processing numbers per second"))
}

func (s *testInfoschemaTableSuite) TestTableConstraintsTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='gc_delete_range';").Check(testkit.Rows("def mysql delete_range_index mysql gc_delete_range UNIQUE"))
}

func (s *testInfoschemaTableSuite) TestTableSessionVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select * from information_schema.SESSION_VARIABLES where VARIABLE_NAME='tidb_retry_limit';").Check(testkit.Rows("tidb_retry_limit 10"))
}

func (s *testInfoschemaTableSuite) TestForAnalyzeStatus(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("delete from mysql.analyze_jobs")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists analyze_test")
	tk.MustExec("create table analyze_test (a int, b int, index idx(a))")
	tk.MustExec("insert into analyze_test values (1,2),(3,4)")

	tk.MustQuery("select distinct TABLE_NAME from information_schema.analyze_status where TABLE_NAME='analyze_test'").Check([][]interface{}{})
	tk.MustExec("analyze table analyze_test")
	tk.MustQuery("select distinct TABLE_NAME from information_schema.analyze_status where TABLE_NAME='analyze_test'").Check(testkit.Rows("analyze_test"))

	// test the privilege of new user for information_schema.analyze_status
	tk.MustExec("create user analyze_tester")
	analyzeTester := testkit.NewTestKit(c, s.store)
	analyzeTester.MustExec("use information_schema")
	c.Assert(analyzeTester.Se.Auth(&auth.UserIdentity{
		Username: "analyze_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	analyzeTester.MustQuery("show analyze status").Check([][]interface{}{})
	analyzeTester.MustQuery("select * from information_schema.ANALYZE_STATUS;").Check([][]interface{}{})

	// test the privilege of user with privilege of test.t1 for information_schema.analyze_status
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,2),(3,4)")
	tk.MustExec("analyze table t1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t1.")) // 1 note.
	c.Assert(s.dom.StatsHandle().LoadNeededHistograms(), IsNil)
	tk.MustExec("CREATE ROLE r_t1 ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON test.t1 TO r_t1;")
	tk.MustExec("GRANT r_t1 TO analyze_tester;")
	analyzeTester.MustExec("set role r_t1")
	resultT1 := tk.MustQuery("select * from information_schema.analyze_status where TABLE_NAME='t1'").Sort()
	c.Assert(len(resultT1.Rows()), Greater, 0)
	for _, row := range resultT1.Rows() {
		c.Assert(len(row), Equals, 8) // test length of row
		c.Assert(row[6], NotNil)      // test `End_time` field
	}
}

func (s *testInfoschemaTableSerialSuite) TestForServersInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	result := tk.MustQuery("select * from information_schema.TIDB_SERVERS_INFO")
	c.Assert(len(result.Rows()), Equals, 1)

	info, err := infosync.GetServerInfo()
	c.Assert(err, IsNil)
	c.Assert(info, NotNil)
	c.Assert(result.Rows()[0][0], Equals, info.ID)
	c.Assert(result.Rows()[0][1], Equals, info.IP)
	c.Assert(result.Rows()[0][2], Equals, strconv.FormatInt(int64(info.Port), 10))
	c.Assert(result.Rows()[0][3], Equals, strconv.FormatInt(int64(info.StatusPort), 10))
	c.Assert(result.Rows()[0][4], Equals, info.Lease)
	c.Assert(result.Rows()[0][5], Equals, info.Version)
	c.Assert(result.Rows()[0][6], Equals, info.GitHash)
	c.Assert(result.Rows()[0][7], Equals, info.BinlogStatus)
	c.Assert(result.Rows()[0][8], Equals, stringutil.BuildStringFromLabels(info.Labels))
}

func (s *testInfoschemaTableSerialSuite) TestForTableTiFlashReplica(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("alter table t set tiflash replica 2 location labels 'a','b';")
	tk.MustQuery("select TABLE_SCHEMA,TABLE_NAME,REPLICA_COUNT,LOCATION_LABELS,AVAILABLE, PROGRESS from information_schema.tiflash_replica").Check(testkit.Rows("test t 2 a,b 0 0"))
	tbl, err := domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tbl.Meta().TiFlashReplica.Available = true
	tk.MustQuery("select TABLE_SCHEMA,TABLE_NAME,REPLICA_COUNT,LOCATION_LABELS,AVAILABLE, PROGRESS from information_schema.tiflash_replica").Check(testkit.Rows("test t 2 a,b 1 1"))
}

var _ = SerialSuites(&testInfoschemaClusterTableSuite{testInfoschemaTableSuiteBase: &testInfoschemaTableSuiteBase{}})

type testInfoschemaClusterTableSuite struct {
	*testInfoschemaTableSuiteBase
	rpcserver  *grpc.Server
	httpServer *httptest.Server
	mockAddr   string
	listenAddr string
	startTime  time.Time
}

func (s *testInfoschemaClusterTableSuite) SetUpSuite(c *C) {
	s.testInfoschemaTableSuiteBase.SetUpSuite(c)
	s.rpcserver, s.listenAddr = s.setUpRPCService(c, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
}

func (s *testInfoschemaClusterTableSuite) setUpRPCService(c *C, addr string) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	// Fix issue 9836
	sm := &mockSessionManager{
		processInfoMap: make(map[uint64]*util.ProcessInfo, 1),
		serverID:       1,
	}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
	}
	srv := server.NewRPCServer(config.GetGlobalConfig(), s.dom, sm)
	port := lis.Addr().(*net.TCPAddr).Port
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	go func() {
		err = srv.Serve(lis)
		c.Assert(err, IsNil)
	}()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
	})
	return srv, addr
}

func (s *testInfoschemaClusterTableSuite) setUpMockPDHTTPServer() (*httptest.Server, string) {
	// mock PD http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	router.Handle(pdapi.Stores, fn.Wrap(func() (*helper.StoresStat, error) {
		return &helper.StoresStat{
			Count: 1,
			Stores: []helper.StoreStat{
				{
					Store: helper.StoreBaseStat{
						ID:             1,
						Address:        "127.0.0.1:20160",
						State:          0,
						StateName:      "Up",
						Version:        "4.0.0-alpha",
						StatusAddress:  mockAddr,
						GitHash:        "mock-tikv-githash",
						StartTimestamp: s.startTime.Unix(),
					},
				},
			},
		}, nil
	}))
	// mock PD API
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			Version        string `json:"version"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			Version:        "4.0.0-alpha",
			GitHash:        "mock-pd-githash",
			StartTimestamp: s.startTime.Unix(),
		}, nil
	}))
	var mockConfig = func() (map[string]interface{}, error) {
		configuration := map[string]interface{}{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
			"key3": map[string]interface{}{
				"nest1": "n-value1",
				"nest2": "n-value2",
				"key4": map[string]string{
					"nest3": "n-value4",
					"nest4": "n-value5",
				},
			},
		}
		return configuration, nil
	}
	// PD config.
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config.
	router.Handle("/config", fn.Wrap(mockConfig))
	// PD region.
	router.Handle("/pd/api/v1/stats/region", fn.Wrap(func() (*helper.PDRegionStats, error) {
		return &helper.PDRegionStats{
			Count:            1,
			EmptyCount:       1,
			StorageSize:      1,
			StorageKeys:      1,
			StoreLeaderCount: map[uint64]int{1: 1},
			StorePeerCount:   map[uint64]int{1: 1},
		}, nil
	}))
	return server, mockAddr
}

func (s *testInfoschemaClusterTableSuite) TearDownSuite(c *C) {
	if s.rpcserver != nil {
		s.rpcserver.Stop()
		s.rpcserver = nil
	}
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	s.testInfoschemaTableSuiteBase.TearDownSuite(c)
}

type mockSessionManager struct {
	processInfoMap map[uint64]*util.ProcessInfo
	serverID       uint64
}

func (sm *mockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	panic("unimplemented!")
}

func (sm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	return sm.processInfoMap
}

func (sm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	rs, ok := sm.processInfoMap[id]
	return rs, ok
}

func (sm *mockSessionManager) Kill(connectionID uint64, query bool) {}

func (sm *mockSessionManager) KillAllConnections() {}

func (sm *mockSessionManager) UpdateTLSConfig(cfg *tls.Config) {}

func (sm *mockSessionManager) ServerID() uint64 {
	return sm.serverID
}

func (sm *mockSessionManager) SetServerID(serverID uint64) {
	sm.serverID = serverID
}

type mockStore struct {
	helper.Storage
	host string
}

func (s *mockStore) EtcdAddrs() ([]string, error) { return []string{s.host}, nil }
func (s *mockStore) TLSConfig() *tls.Config       { panic("not implemented") }
func (s *mockStore) StartGCWorker() error         { panic("not implemented") }
func (s *mockStore) Name() string                 { return "mockStore" }
func (s *mockStore) Describe() string             { return "" }

func (s *testInfoschemaClusterTableSuite) TestTiDBClusterInfo(c *C) {
	mockAddr := s.mockAddr
	store := &mockStore{
		s.store.(helper.Storage),
		mockAddr,
	}

	// information_schema.cluster_info
	tk := testkit.NewTestKit(c, store)
	tidbStatusAddr := fmt.Sprintf(":%d", config.GetGlobalConfig().Status.StatusPort)
	row := func(cols ...string) string { return strings.Join(cols, " ") }
	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Rows(
		row("tidb", ":4000", tidbStatusAddr, "None", "None"),
		row("pd", mockAddr, mockAddr, "4.0.0-alpha", "mock-pd-githash"),
		row("tikv", "store1", "", "", ""),
	))
	startTime := s.startTime.Format(time.RFC3339)
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type != 'tidb'").Check(testkit.Rows(
		row("pd", mockAddr, startTime),
		row("tikv", "store1", ""),
	))

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockStoreTombstone", `return(true)`), IsNil)
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type = 'tikv'").Check(testkit.Rows())
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/infoschema/mockStoreTombstone"), IsNil)

	// information_schema.cluster_config
	instances := []string{
		"pd,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,0",
		"tidb,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,1001",
		"tikv,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,0",
	}
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockClusterInfo", fpExpr), IsNil)
	defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/infoschema/mockClusterInfo"), IsNil) }()
	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		row("pd", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "0"),
		row("tidb", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "1001"),
		row("tikv", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "0"),
	))
	tk.MustQuery("select * from information_schema.cluster_config").Check(testkit.Rows(
		"pd 127.0.0.1:11080 key1 value1",
		"pd 127.0.0.1:11080 key2.nest1 n-value1",
		"pd 127.0.0.1:11080 key2.nest2 n-value2",
		"pd 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"pd 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"pd 127.0.0.1:11080 key3.nest1 n-value1",
		"pd 127.0.0.1:11080 key3.nest2 n-value2",
		"tidb 127.0.0.1:11080 key1 value1",
		"tidb 127.0.0.1:11080 key2.nest1 n-value1",
		"tidb 127.0.0.1:11080 key2.nest2 n-value2",
		"tidb 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"tidb 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"tidb 127.0.0.1:11080 key3.nest1 n-value1",
		"tidb 127.0.0.1:11080 key3.nest2 n-value2",
		"tikv 127.0.0.1:11080 key1 value1",
		"tikv 127.0.0.1:11080 key2.nest1 n-value1",
		"tikv 127.0.0.1:11080 key2.nest2 n-value2",
		"tikv 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"tikv 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"tikv 127.0.0.1:11080 key3.nest1 n-value1",
		"tikv 127.0.0.1:11080 key3.nest2 n-value2",
	))
	tk.MustQuery("select TYPE, `KEY`, VALUE from information_schema.cluster_config where `key`='key3.key4.nest4' order by type").Check(testkit.Rows(
		"pd key3.key4.nest4 n-value5",
		"tidb key3.key4.nest4 n-value5",
		"tikv key3.key4.nest4 n-value5",
	))
}

func (s *testInfoschemaClusterTableSuite) TestTableStorageStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	err := tk.QueryToErr("select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test'")
	c.Assert(err.Error(), Equals, "pd unavailable")
	mockAddr := s.mockAddr
	store := &mockStore{
		s.store.(helper.Storage),
		mockAddr,
	}

	// Test information_schema.TABLE_STORAGE_STATS.
	tk = testkit.NewTestKit(c, store)

	// Test not set the schema.
	err = tk.QueryToErr("select * from information_schema.TABLE_STORAGE_STATS")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Please specify the 'table_schema'")

	// Test it would get null set when get the sys schema.
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema';").Check([][]interface{}{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA in ('information_schema', 'metrics_schema');").Check([][]interface{}{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema' and TABLE_NAME='schemata';").Check([][]interface{}{})

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustQuery("select TABLE_NAME, TABLE_SIZE from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' and TABLE_NAME='t';").Check(testkit.Rows("t 1"))

	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustQuery("select TABLE_NAME, sum(TABLE_SIZE) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' group by TABLE_NAME;").Sort().Check(testkit.Rows(
		"t 1",
		"t1 1",
	))
	tk.MustQuery("select TABLE_SCHEMA, sum(TABLE_SIZE) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' group by TABLE_SCHEMA;").Check(testkit.Rows(
		"test 2",
	))
	c.Assert(len(tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql';").Rows()), Equals, 29)

	// More tests about the privileges.
	tk.MustExec("create user 'testuser'@'localhost'")
	tk.MustExec("create user 'testuser2'@'localhost'")
	tk.MustExec("create user 'testuser3'@'localhost'")
	tk1 := testkit.NewTestKit(c, store)
	defer tk1.MustExec("drop user 'testuser'@'localhost'")
	defer tk1.MustExec("drop user 'testuser2'@'localhost'")
	defer tk1.MustExec("drop user 'testuser3'@'localhost'")

	tk.MustExec("grant all privileges on *.* to 'testuser2'@'localhost'")
	tk.MustExec("grant select on *.* to 'testuser3'@'localhost'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil), Equals, true)

	// User has no access to this schema, so the result set is empty.
	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows("0"))

	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil), Equals, true)

	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows("29"))

	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser3",
		Hostname: "localhost",
	}, nil, nil), Equals, true)

	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows("29"))
}

func (s *testInfoschemaTableSuite) TestSequences(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE SEQUENCE test.seq maxvalue 10000000")
	tk.MustQuery("SELECT * FROM information_schema.sequences WHERE sequence_schema='test' AND sequence_name='seq'").Check(testkit.Rows("def test seq 1 1000 0 1 10000000 1 1 "))
	tk.MustExec("DROP SEQUENCE test.seq")
	tk.MustExec("CREATE SEQUENCE test.seq start = -1 minvalue -1 maxvalue 10 increment 1 cache 10")
	tk.MustQuery("SELECT * FROM information_schema.sequences WHERE sequence_schema='test' AND sequence_name='seq'").Check(testkit.Rows("def test seq 1 10 0 1 10 -1 -1 "))
	tk.MustExec("CREATE SEQUENCE test.seq2 start = -9 minvalue -10 maxvalue 10 increment -1 cache 15")
	tk.MustQuery("SELECT * FROM information_schema.sequences WHERE sequence_schema='test' AND sequence_name='seq2'").Check(testkit.Rows("def test seq2 1 15 0 -1 10 -10 -9 "))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME , TABLE_TYPE, ENGINE, TABLE_ROWS FROM information_schema.tables WHERE TABLE_TYPE='SEQUENCE' AND TABLE_NAME='seq2'").Check(testkit.Rows("def test seq2 SEQUENCE InnoDB 1"))
}

func (s *testInfoschemaTableSuite) TestTiFlashSystemTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	err := tk.QueryToErr("select * from information_schema.TIFLASH_TABLES;")
	c.Assert(err, Equals, nil)
	err = tk.QueryToErr("select * from information_schema.TIFLASH_SEGMENTS;")
	c.Assert(err, Equals, nil)
}

func (s *testInfoschemaTableSuite) TestTablesPKType(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t_int (a int primary key, b int)")
	tk.MustQuery("SELECT TIDB_PK_TYPE FROM information_schema.tables where table_schema = 'test' and table_name = 't_int'").Check(testkit.Rows("CLUSTERED"))
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t_implicit (a varchar(64) primary key, b int)")
	tk.MustQuery("SELECT TIDB_PK_TYPE FROM information_schema.tables where table_schema = 'test' and table_name = 't_implicit'").Check(testkit.Rows("NONCLUSTERED"))
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t_common (a varchar(64) primary key, b int)")
	tk.MustQuery("SELECT TIDB_PK_TYPE FROM information_schema.tables where table_schema = 'test' and table_name = 't_common'").Check(testkit.Rows("CLUSTERED"))
	tk.MustQuery("SELECT TIDB_PK_TYPE FROM information_schema.tables where table_schema = 'INFORMATION_SCHEMA' and table_name = 'TABLES'").Check(testkit.Rows("NONCLUSTERED"))
}
