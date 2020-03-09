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
	"strconv"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testInfoschemaTableSuite{})

type testInfoschemaTableSuite struct {
	store kv.Storage
	dom   *domain.Domain
	// mu is used to protect the TableStatsCacheExpiry global variable.
	mu sync.Mutex
}

func (s *testInfoschemaTableSuite) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *testInfoschemaTableSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}
func (s *testInfoschemaTableSuite) TestSchemataTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustQuery("select * from information_schema.SCHEMATA where schema_name='mysql';").Check(
		testkit.Rows("def mysql utf8mb4 utf8mb4_bin <nil>"))

	//test the privilege of new user for information_schema.schemata
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

	//test the privilege of user with privilege of mysql for information_schema.schemata
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

func (s *testInfoschemaTableSuite) TestDataForTableStatsField(c *C) {
	s.mu.Lock()
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
	s.mu.Unlock()
}

func (s *testInfoschemaTableSuite) TestPartitionsTable(c *C) {
	s.mu.Lock()
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
	s.mu.Unlock()
}
