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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
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
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/pdapi"
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
	originCfg := config.GetGlobalConfig()
	newConf := *originCfg
	newConf.OOMAction = config.OOMActionLog
	config.StoreGlobalConfig(&newConf)
}

func (s *testInfoschemaTableSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *inspectionSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, err = mockstore.NewMockTikvStore()
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
	tk := testkit.NewTestKit(c, s.store)
	instances := []string{
		"pd,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
		"tidb,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
		"tikv,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
	}
	fpName := "github.com/pingcap/tidb/infoschema/mockClusterInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	c.Assert(failpoint.Enable(fpName, fpExpr), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Rows(
		"pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
	))

	// enable inspection mode
	inspectionTableCache := map[string]variable.TableSnapshot{}
	tk.Se.GetSessionVars().InspectionTableCache = inspectionTableCache
	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Rows(
		"pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
	))
	c.Assert(inspectionTableCache["cluster_info"].Err, IsNil)
	c.Assert(len(inspectionTableCache["cluster_info"].Rows), DeepEquals, 3)

	// check whether is obtain data from cache at the next time
	inspectionTableCache["cluster_info"].Rows[0][0].SetString("modified-pd", mysql.DefaultCollationName)
	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Rows(
		"modified-pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
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
		testkit.Rows("def mysql utf8mb4 utf8mb4_bin <nil>"))

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
		testkit.Rows("def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil>"))

	// Test the privilege of user with privilege of mysql for information_schema.schemata.
	tk.MustExec("CREATE ROLE r_mysql_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.* TO r_mysql_priv;")
	tk.MustExec("GRANT r_mysql_priv TO schemata_tester;")
	schemataTester.MustExec("set role r_mysql_priv")
	schemataTester.MustQuery("select count(*) from information_schema.SCHEMATA;").Check(testkit.Rows("2"))
	schemataTester.MustQuery("select * from information_schema.SCHEMATA;").Check(
		testkit.Rows("def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil>", "def mysql utf8mb4 utf8mb4_bin <nil>"))
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
	tk.MustQuery("SELECT * FROM information_schema.views WHERE table_schema='test' AND table_name='v1'").Check(testkit.Rows("def test v1 SELECT 1 CASCADED NO root@localhost DEFINER utf8mb4 utf8mb4_bin"))
	tk.MustQuery("SELECT table_catalog, table_schema, table_name, table_type, engine, version, row_format, table_rows, avg_row_length, data_length, max_data_length, index_length, data_free, auto_increment, update_time, check_time, table_collation, checksum, create_options, table_comment FROM information_schema.tables WHERE table_schema='test' AND table_name='v1'").Check(testkit.Rows("def test v1 VIEW <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> VIEW"))
}

func (s *testInfoschemaTableSuite) TestEngines(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select * from information_schema.ENGINES;").Check(testkit.Rows("InnoDB DEFAULT Supports transactions, row-level locking, and foreign keys YES YES YES"))
}

func (s *testInfoschemaTableSuite) TestCharacterSetCollations(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// The description column is not important
	tk.MustQuery("SELECT default_collate_name, maxlen FROM information_schema.character_sets ORDER BY character_set_name").Check(
		testkit.Rows("ascii_bin 1", "binary 1", "latin1_bin 1", "utf8_bin 3", "utf8mb4_bin 4"))

	// The is_default column is not important
	// but the id's are used by client libraries and must be stable
	tk.MustQuery("SELECT character_set_name, id, sortlen FROM information_schema.collations ORDER BY collation_name").Check(
		testkit.Rows("ascii 65 1", "binary 63 1", "latin1 47 1", "utf8 83 1", "utf8mb4 46 1"))

	tk.MustQuery("select * from information_schema.COLLATION_CHARACTER_SET_APPLICABILITY where COLLATION_NAME='utf8mb4_bin';").Check(
		testkit.Rows("utf8mb4_bin utf8mb4"))
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

	//test the privilege of new user for information_schema.table_constraints
	tk.MustExec("create user key_column_tester")
	keyColumnTester := testkit.NewTestKit(c, s.store)
	keyColumnTester.MustExec("use information_schema")
	c.Assert(keyColumnTester.Se.Auth(&auth.UserIdentity{
		Username: "key_column_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	keyColumnTester.MustQuery("select * from information_schema.KEY_COLUMN_USAGE;").Check([][]interface{}{})

	//test the privilege of user with privilege of mysql.gc_delete_range for information_schema.table_constraints
	tk.MustExec("CREATE ROLE r_stats_meta ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.stats_meta TO r_stats_meta;")
	tk.MustExec("GRANT r_stats_meta TO key_column_tester;")
	keyColumnTester.MustExec("set role r_stats_meta")
	c.Assert(len(keyColumnTester.MustQuery("select * from information_schema.KEY_COLUMN_USAGE where TABLE_NAME='stats_meta';").Rows()), Greater, 0)
}

func (s *testInfoschemaTableSuite) TestUserPrivileges(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	//test the privilege of new user for information_schema.table_constraints
	tk.MustExec("create user constraints_tester")
	constraintsTester := testkit.NewTestKit(c, s.store)
	constraintsTester.MustExec("use information_schema")
	c.Assert(constraintsTester.Se.Auth(&auth.UserIdentity{
		Username: "constraints_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS;").Check([][]interface{}{})

	//test the privilege of user with privilege of mysql.gc_delete_range for information_schema.table_constraints
	tk.MustExec("CREATE ROLE r_gc_delete_range ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.gc_delete_range TO r_gc_delete_range;")
	tk.MustExec("GRANT r_gc_delete_range TO constraints_tester;")
	constraintsTester.MustExec("set role r_gc_delete_range")
	c.Assert(len(constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='gc_delete_range';").Rows()), Greater, 0)
	constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='tables_priv';").Check([][]interface{}{})

	//test the privilege of new user for information_schema
	tk.MustExec("create user tester1")
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use information_schema")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{
		Username: "tester1",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	tk1.MustQuery("select * from information_schema.STATISTICS;").Check([][]interface{}{})

	//test the privilege of user with some privilege for information_schema
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

	//test the privilege of user with all privilege for information_schema
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
	h.HandleDDLEvent(<-h.DDLEventCh())
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
	h.HandleDDLEvent(<-h.DDLEventCh())
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

	// Test for table has no partitions.
	tk.MustExec("DROP TABLE IF EXISTS `test_partitions_1`;")
	tk.MustExec(`CREATE TABLE test_partitions_1 (a int, b int, c varchar(5), primary key(a), index idx(c));`)
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	tk.MustExec(`insert into test_partitions_1(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e");`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustQuery("select PARTITION_NAME, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH from information_schema.PARTITIONS where table_name='test_partitions_1';").Check(
		testkit.Rows("<nil> 3 18 54 6"))

	tk.MustExec("DROP TABLE `test_partitions`;")
}

func (s *testInfoschemaTableSuite) TestMetricTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	statistics.ClearHistoryJobs()
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
	statistics.ClearHistoryJobs()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists analyze_test")
	tk.MustExec("create table analyze_test (a int, b int, index idx(a))")
	tk.MustExec("insert into analyze_test values (1,2),(3,4)")

	tk.MustQuery("select distinct TABLE_NAME from information_schema.analyze_status where TABLE_NAME='analyze_test'").Check([][]interface{}{})
	tk.MustExec("analyze table analyze_test")
	tk.MustQuery("select distinct TABLE_NAME from information_schema.analyze_status where TABLE_NAME='analyze_test'").Check(testkit.Rows("analyze_test"))

	//test the privilege of new user for information_schema.analyze_status
	tk.MustExec("create user analyze_tester")
	analyzeTester := testkit.NewTestKit(c, s.store)
	analyzeTester.MustExec("use information_schema")
	c.Assert(analyzeTester.Se.Auth(&auth.UserIdentity{
		Username: "analyze_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	analyzeTester.MustQuery("show analyze status").Check([][]interface{}{})
	analyzeTester.MustQuery("select * from information_schema.ANALYZE_STATUS;").Check([][]interface{}{})

	//test the privilege of user with privilege of test.t1 for information_schema.analyze_status
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,2),(3,4)")
	tk.MustExec("analyze table t1")
	tk.MustExec("CREATE ROLE r_t1 ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON test.t1 TO r_t1;")
	tk.MustExec("GRANT r_t1 TO analyze_tester;")
	analyzeTester.MustExec("set role r_t1")
	resultT1 := tk.MustQuery("select * from information_schema.analyze_status where TABLE_NAME='t1'").Sort()
	c.Assert(len(resultT1.Rows()), Greater, 0)
}

func (s *testInfoschemaTableSuite) TestForServersInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	result := tk.MustQuery("select * from information_schema.TIDB_SERVERS_INFO")
	c.Assert(len(result.Rows()), Equals, 1)

	serversInfo, err := infosync.GetAllServerInfo(context.Background())
	c.Assert(err, IsNil)
	c.Assert(len(serversInfo), Equals, 1)

	for _, info := range serversInfo {
		c.Assert(result.Rows()[0][0], Equals, info.ID)
		c.Assert(result.Rows()[0][1], Equals, info.IP)
		c.Assert(result.Rows()[0][2], Equals, strconv.FormatInt(int64(info.Port), 10))
		c.Assert(result.Rows()[0][3], Equals, strconv.FormatInt(int64(info.StatusPort), 10))
		c.Assert(result.Rows()[0][4], Equals, info.Lease)
		c.Assert(result.Rows()[0][5], Equals, info.Version)
		c.Assert(result.Rows()[0][6], Equals, info.GitHash)
	}
}

func (s *testInfoschemaTableSuite) TestForTableTiFlashReplica(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	statistics.ClearHistoryJobs()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("alter table t set tiflash replica 2 location labels 'a','b';")
	tk.MustQuery("select TABLE_SCHEMA,TABLE_NAME,REPLICA_COUNT,LOCATION_LABELS,AVAILABLE, PROGRESS from information_schema.tiflash_replica").Check(testkit.Rows("test t 2 a,b 0 0"))
	tbl, err := domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"), false)
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
	s.rpcserver, s.listenAddr = s.setUpRPCService(c, ":0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
}

func (s *testInfoschemaClusterTableSuite) setUpRPCService(c *C, addr string) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	// Fix issue 9836
	sm := &mockSessionManager{make(map[uint64]*util.ProcessInfo, 1)}
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
	cfg := config.GetGlobalConfig()
	cfg.Status.StatusPort = uint(port)
	config.StoreGlobalConfig(cfg)
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
	router.Handle(pdapi.ClusterVersion, fn.Wrap(func() (string, error) { return "4.0.0-alpha", nil }))
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
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
	// pd config
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config
	router.Handle("/config", fn.Wrap(mockConfig))
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
}

func (sm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	return sm.processInfoMap
}

func (sm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	rs, ok := sm.processInfoMap[id]
	return rs, ok
}

func (sm *mockSessionManager) Kill(connectionID uint64, query bool) {}

func (sm *mockSessionManager) UpdateTLSConfig(cfg *tls.Config) {}

type mockStore struct {
	tikv.Storage
	host string
}

func (s *mockStore) EtcdAddrs() []string    { return []string{s.host} }
func (s *mockStore) TLSConfig() *tls.Config { panic("not implemented") }
func (s *mockStore) StartGCWorker() error   { panic("not implemented") }

func (s *testInfoschemaClusterTableSuite) TestTiDBClusterInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	err := tk.QueryToErr("select * from information_schema.cluster_info")
	c.Assert(err, NotNil)
	mockAddr := s.mockAddr
	store := &mockStore{
		s.store.(tikv.Storage),
		mockAddr,
	}

	// information_schema.cluster_info
	tk = testkit.NewTestKit(c, store)
	tidbStatusAddr := fmt.Sprintf(":%d", config.GetGlobalConfig().Status.StatusPort)
	row := func(cols ...string) string { return strings.Join(cols, " ") }
	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Rows(
		row("tidb", ":4000", tidbStatusAddr, "5.7.25-TiDB-None", "None"),
		row("pd", mockAddr, mockAddr, "4.0.0-alpha", "mock-pd-githash"),
		row("tikv", "127.0.0.1:20160", mockAddr, "4.0.0-alpha", "mock-tikv-githash"),
	))
	startTime := s.startTime.Format(time.RFC3339)
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type != 'tidb'").Check(testkit.Rows(
		row("pd", mockAddr, startTime),
		row("tikv", "127.0.0.1:20160", startTime),
	))

	// information_schema.cluster_config
	instances := []string{
		"pd,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash",
		"tidb,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash",
		"tikv,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash",
	}
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockClusterInfo", fpExpr), IsNil)
	defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/infoschema/mockClusterInfo"), IsNil) }()
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

func (s *testInfoschemaTableSuite) TestSequences(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE SEQUENCE test.seq maxvalue 10000000")
	tk.MustQuery("SELECT * FROM information_schema.sequences WHERE sequence_schema='test' AND sequence_name='seq'").Check(testkit.Rows("def test seq 1 1000 0 1 10000000 1 0 1 "))
}
