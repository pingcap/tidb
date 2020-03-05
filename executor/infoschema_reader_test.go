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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testInfoschemaTableSuite{})

type testInfoschemaTableSuite struct {
	store kv.Storage
	dom   *domain.Domain
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
