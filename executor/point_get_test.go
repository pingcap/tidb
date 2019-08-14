// Copyright 2018 PingCAP, Inc.
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
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/testkit"
)

type testPointGetSuite struct {
	store kv.Storage
	dom   *domain.Domain
	cli   *checkRequestClient
}

func (s *testPointGetSuite) SetUpSuite(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithHijackClient(hijackClient),
	)
	c.Assert(err, IsNil)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom.SetStatsUpdating(true)
}

func (s *testPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testPointGetSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testPointGetSuite) TestPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")
	tk.MustQuery("select * from point where id = 1 and c = 0").Check(testkit.Rows())
	tk.MustQuery("select * from point where id < 0 and c = 1 and d = 'b'").Check(testkit.Rows())
	result, err := tk.Exec("select id as ident from point where id = 1")
	c.Assert(err, IsNil)
	fields := result.Fields()
	c.Assert(fields[0].ColumnAsName.O, Equals, "ident")
	result.Close()

	tk.MustExec("CREATE TABLE tab3(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);")
	tk.MustExec("CREATE UNIQUE INDEX idx_tab3_0 ON tab3 (col4);")
	tk.MustExec("INSERT INTO tab3 VALUES(0,854,111.96,'mguub',711,966.36,'snwlo');")
	tk.MustQuery("SELECT ALL * FROM tab3 WHERE col4 = 85;").Check(testkit.Rows())

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint primary key, b bigint, c bigint);`)
	tk.MustExec(`insert into t values(1, NULL, NULL), (2, NULL, 2), (3, 3, NULL), (4, 4, 4), (5, 6, 7);`)
	tk.MustQuery(`select * from t where a = 1;`).Check(testkit.Rows(
		`1 <nil> <nil>`,
	))
	tk.MustQuery(`select * from t where a = 2;`).Check(testkit.Rows(
		`2 <nil> 2`,
	))
	tk.MustQuery(`select * from t where a = 3;`).Check(testkit.Rows(
		`3 3 <nil>`,
	))
	tk.MustQuery(`select * from t where a = 4;`).Check(testkit.Rows(
		`4 4 4`,
	))
	tk.MustQuery(`select a, a, b, a, b, c, b, c, c from t where a = 5;`).Check(testkit.Rows(
		`5 5 6 5 6 7 6 7 7`,
	))
	tk.MustQuery(`select b, b from t where a = 1`).Check(testkit.Rows(
		"<nil> <nil>"))
}

func (s *testPointGetSuite) TestPointGetCharPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) primary key, b char(2));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	// Test truncate without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	// Test truncate with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// // Test CHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) binary primary key, b char(2));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "   ";`).Check(testkit.Rows(` `))

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "   ";`).Check(testkit.Rows(` `))
}

func (s *testPointGetSuite) TestPointGetAliasTableCharPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) primary key, b char(2));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	// Test truncate without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t tmp where a = "aab";`).Check(testkit.Rows())

	// Test truncate with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t tmp where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())

	// Test CHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) binary primary key, b char(2));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = " ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t tmp where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t tmp where a = "   ";`).Check(testkit.Rows(` `))

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = " ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t tmp where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t tmp where a = "   ";`).Check(testkit.Rows(` `))

	// Test both wildcard and column name exist in select field list
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) primary key, b char(2));`)
	tk.MustExec(`insert into t values("aa", "bb");`)
	tk.MustPointGet(`select *, a from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb aa`))

	// Test using table alias in field list
	tk.MustPointGet(`select tmp.* from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select tmp.a, tmp.b from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select tmp.*, tmp.a, tmp.b from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb aa bb`))
	tk.MustPointGet(`select tmp.* from t tmp where a = "aab";`).Check(testkit.Rows())
	tk.MustPointGet(`select tmp.a, tmp.b from t tmp where a = "aab";`).Check(testkit.Rows())
	tk.MustPointGet(`select tmp.*, tmp.a, tmp.b from t tmp where a = "aab";`).Check(testkit.Rows())

	// Test using table alias in where clause
	tk.MustPointGet(`select * from t tmp where tmp.a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select a, b from t tmp where tmp.a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select *, a, b from t tmp where tmp.a = "aa";`).Check(testkit.Rows(`aa bb aa bb`))

	// Unknown table name in where clause and field list
	err := tk.ExecToErr(`select a from t where xxxxx.a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown column 'xxxxx.a' in 'where clause'")
	err = tk.ExecToErr(`select xxxxx.a from t where a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown column 'xxxxx.a' in 'field list'")

	// When an alias is provided, it completely hides the actual name of the table.
	err = tk.ExecToErr(`select a from t tmp where t.a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown column 't.a' in 'where clause'")
	err = tk.ExecToErr(`select t.a from t tmp where a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown column 't.a' in 'field list'")
	err = tk.ExecToErr(`select t.* from t tmp where a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown table 't'")
}

func (s *testPointGetSuite) TestIndexLookupChar(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2), b char(2), index idx_1(a));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	// Test truncate without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustIndexLookup(`select * from t where a = "aab";`).Check(testkit.Rows())

	// Test query with table alias
	tk.MustIndexLookup(`select * from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustIndexLookup(`select * from t tmp where a = "aab";`).Check(testkit.Rows())

	// Test truncate with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustIndexLookup(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustIndexLookup(`select * from t where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test CHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) binary, b char(2), index idx_1(a));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = " ";`).Check(testkit.Rows(` `))
	tk.MustIndexLookup(`select * from t where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustIndexLookup(`select * from t where a = "   ";`).Check(testkit.Rows(` `))

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = " ";`).Check(testkit.Rows(` `))
	tk.MustIndexLookup(`select * from t where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustIndexLookup(`select * from t where a = "   ";`).Check(testkit.Rows(` `))

	// Test query with table alias in `PAD_CHAR_TO_FULL_LENGTH` mode
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustIndexLookup(`select * from t tmp where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t tmp where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t tmp where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t tmp where a = " ";`).Check(testkit.Rows(` `))
	tk.MustIndexLookup(`select * from t tmp where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustIndexLookup(`select * from t tmp where a = "   ";`).Check(testkit.Rows(` `))
}

func (s *testPointGetSuite) TestPointGetVarcharPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a varchar(2) primary key, b varchar(2));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	// Test truncate without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	// Test truncate with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// // Test VARCHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a varchar(2) binary primary key, b varchar(2));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows(`     `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(`     `))
	tk.MustPointGet(`select * from t where a = "   ";`).Check(testkit.Rows(`     `))

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows(`     `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(`     `))
	tk.MustPointGet(`select * from t where a = "   ";`).Check(testkit.Rows(`     `))
}

func (s *testPointGetSuite) TestPointGetBinaryPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a binary(2) primary key, b binary(2));`)
	tk.MustExec(`insert into t values("a", "b");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a\0";`).Check(testkit.Rows("a\x00 b\x00"))

	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a\0";`).Check(testkit.Rows("a\x00 b\x00"))

	tk.MustExec(`insert into t values("a ", "b ");`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())
}

func (s *testPointGetSuite) TestPointGetAliasTableBinaryPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a binary(2) primary key, b binary(2));`)
	tk.MustExec(`insert into t values("a", "b");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a\0";`).Check(testkit.Rows("a\x00 b\x00"))

	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a\0";`).Check(testkit.Rows("a\x00 b\x00"))

	tk.MustExec(`insert into t values("a ", "b ");`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())

	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())
}

func (s *testPointGetSuite) TestIndexLookupBinary(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a binary(2), b binary(2), index idx_1(a));`)
	tk.MustExec(`insert into t values("a", "b");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a\0";`).Check(testkit.Rows("a\x00 b\x00"))

	// Test query with table alias
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t tmp where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t tmp where a = "a ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t tmp where a = "a\0";`).Check(testkit.Rows("a\x00 b\x00"))

	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a\0";`).Check(testkit.Rows("a\x00 b\x00"))

	tk.MustExec(`insert into t values("a ", "b ");`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test query with table alias in `PAD_CHAR_TO_FULL_LENGTH` mode
	tk.MustIndexLookup(`select * from t tmp where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t tmp where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustIndexLookup(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())
}

func (s *testPointGetSuite) TestIssue10448(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int1 primary key)")
	tk.MustExec("insert into t values(125)")
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 128").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int8 primary key)")
	tk.MustExec("insert into t values(9223372036854775807)")
	tk.MustQuery("select * from t where pk = 9223372036854775807").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Rows("Point_Get_1 1.00 root table:t, handle:9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int1 unsigned primary key)")
	tk.MustExec("insert into t values(255)")
	tk.MustQuery("select * from t where pk = 255").Check(testkit.Rows("255"))
	tk.MustQuery("desc select * from t where pk = 256").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int8 unsigned primary key)")
	tk.MustExec("insert into t value(18446744073709551615)")
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Rows("Point_Get_1 1.00 root table:t, handle:18446744073709551615"))
	tk.MustQuery("select * from t where pk = 18446744073709551615").Check(testkit.Rows("18446744073709551615"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Rows("Point_Get_1 1.00 root table:t, handle:9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Rows("Point_Get_1 1.00 root table:t, handle:9223372036854775808"))
}

func (s *testPointGetSuite) TestIssue10677(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int1 primary key)")
	tk.MustExec("insert into t values(1)")
	tk.MustQuery("desc select * from t where pk = 1.1").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("select * from t where pk = 1.1").Check(testkit.Rows())
	tk.MustQuery("desc select * from t where pk = '1.1'").Check(testkit.Rows("TableDual_2 0.00 root rows:0"))
	tk.MustQuery("select * from t where pk = '1.1'").Check(testkit.Rows())
	tk.MustQuery("desc select * from t where pk = 1").Check(testkit.Rows("Point_Get_1 1.00 root table:t, handle:1"))
	tk.MustQuery("select * from t where pk = 1").Check(testkit.Rows("1"))
	tk.MustQuery("desc select * from t where pk = '1'").Check(testkit.Rows("Point_Get_1 1.00 root table:t, handle:1"))
	tk.MustQuery("select * from t where pk = '1'").Check(testkit.Rows("1"))
	tk.MustQuery("desc select * from t where pk = '1.0'").Check(testkit.Rows("Point_Get_1 1.00 root table:t, handle:1"))
	tk.MustQuery("select * from t where pk = '1.0'").Check(testkit.Rows("1"))
}

func (s *testPointGetSuite) TestForUpdateRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.Exec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, c int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustQuery("select * from t where pk = 1 for update")
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustExec("update t set c = c + 1 where pk = 1")
	tk.MustExec("update t set c = c + 1 where pk = 2")
	_, err := tk.Exec("commit")
	c.Assert(session.ErrForUpdateCantRetry.Equal(err), IsTrue)
}
