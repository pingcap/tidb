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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/tikv/client-go/v2/tikv"
)

type testPointGetSuite struct {
	store    kv.Storage
	dom      *domain.Domain
	cli      *checkRequestClient
	testData testutil.TestData
}

func (s *testPointGetSuite) SetUpSuite(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	h := s.dom.StatsHandle()
	h.SetLease(0)
	s.testData, err = testutil.LoadTestSuiteData("testdata", "point_get_suite")
	c.Assert(err, IsNil)
}

func (s *testPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
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
	c.Assert(result.Close(), IsNil)

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

func (s *testPointGetSuite) TestPointGetOverflow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c1 BOOL UNIQUE)")
	tk.MustExec("INSERT INTO t0(c1) VALUES (-128)")
	tk.MustExec("INSERT INTO t0(c1) VALUES (127)")
	tk.MustQuery("SELECT t0.c1 FROM t0 WHERE t0.c1=-129").Check(testkit.Rows()) // no result
	tk.MustQuery("SELECT t0.c1 FROM t0 WHERE t0.c1=-128").Check(testkit.Rows("-128"))
	tk.MustQuery("SELECT t0.c1 FROM t0 WHERE t0.c1=128").Check(testkit.Rows())
	tk.MustQuery("SELECT t0.c1 FROM t0 WHERE t0.c1=127").Check(testkit.Rows("127"))

	tk.MustExec("CREATE TABLE `PK_S_MULTI_31_1` (`COL1` tinyint(11) NOT NULL, `COL2` tinyint(11) NOT NULL, `COL3` tinyint(11) DEFAULT NULL, PRIMARY KEY (`COL1`,`COL2`) CLUSTERED)")
	tk.MustQuery("select * from PK_S_MULTI_31_1 where col2 = -129 and col1 = 1").Check(testkit.Rows())
	tk.MustExec("insert into PK_S_MULTI_31_1 select 1, 1, 1")
	tk.MustQuery("select * from PK_S_MULTI_31_1 where (col1, col2) in ((1, -129),(1, 1))").Check(testkit.Rows("1 1 1"))
}

// Close issue #22839
func (s *testPointGetSuite) TestPointGetDataTooLong(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists PK_1389;")
	tk.MustExec("CREATE TABLE `PK_1389` ( " +
		"  `COL1` bit(1) NOT NULL," +
		"  `COL2` varchar(20) DEFAULT NULL," +
		"  `COL3` datetime DEFAULT NULL," +
		"  `COL4` bigint(20) DEFAULT NULL," +
		"  `COL5` float DEFAULT NULL," +
		"  PRIMARY KEY (`COL1`)" +
		");")
	tk.MustExec("insert into PK_1389 values(0, \"皟钹糁泅埞礰喾皑杏灚暋蛨歜檈瓗跾咸滐梀揉\", \"7701-12-27 23:58:43\", 4806951672419474695, -1.55652e38);")
	tk.MustQuery("select count(1) from PK_1389 where col1 = 0x30;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(1) from PK_1389 where col1 in ( 0x30);").Check(testkit.Rows("0"))
	tk.MustExec("drop table if exists PK_1389;")
}

// issue #25489
func (s *testPointGetSuite) TestIssue25489(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists UK_RP16939;")
	// range partition
	tk.MustExec(`CREATE TABLE UK_RP16939 (
		COL1 tinyint(16) DEFAULT '108' COMMENT 'NUMERIC UNIQUE INDEX',
		COL2 varchar(20) DEFAULT NULL,
		COL4 datetime DEFAULT NULL,
		COL3 bigint(20) DEFAULT NULL,
		COL5 float DEFAULT NULL,
		UNIQUE KEY UK_COL1 (COL1)
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	  PARTITION BY RANGE ( COL1+13 ) (
		PARTITION P0 VALUES LESS THAN (-44),
		PARTITION P1 VALUES LESS THAN (-23),
		PARTITION P2 VALUES LESS THAN (-22),
		PARTITION P3 VALUES LESS THAN (63),
		PARTITION P4 VALUES LESS THAN (75),
		PARTITION P5 VALUES LESS THAN (90),
		PARTITION PMX VALUES LESS THAN (MAXVALUE)
	  ) ;`)
	query := "select col1, col2 from UK_RP16939 where col1 in (116, 48, -30);"
	c.Assert(tk.HasPlan(query, "Batch_Point_Get"), IsFalse)
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustExec("drop table if exists UK_RP16939;")

	// list parition
	tk.MustExec(`CREATE TABLE UK_RP16939 (
		COL1 tinyint(16) DEFAULT '108' COMMENT 'NUMERIC UNIQUE INDEX',
		COL2 varchar(20) DEFAULT NULL,
		COL4 datetime DEFAULT NULL,
		COL3 bigint(20) DEFAULT NULL,
		COL5 float DEFAULT NULL,
		UNIQUE KEY UK_COL1 (COL1)
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	  PARTITION BY LIST ( COL1+13 ) (
		PARTITION P0 VALUES IN (-44, -23),
		PARTITION P1 VALUES IN (-22, 63),
		PARTITION P2 VALUES IN (75, 90)
	  ) ;`)
	c.Assert(tk.HasPlan(query, "Batch_Point_Get"), IsFalse)
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustExec("drop table if exists UK_RP16939;")
}

// issue #25320
func (s *testPointGetSuite) TestDistinctPlan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_distinct;")
	tk.MustExec(`CREATE TABLE test_distinct (
		id bigint(18) NOT NULL COMMENT '主键',
		b bigint(18) NOT NULL COMMENT '用户ID',
		PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;`)
	tk.MustExec("insert into test_distinct values (123456789101112131,223456789101112131),(123456789101112132,223456789101112131);")
	tk.MustQuery("select distinct b from test_distinct where id in (123456789101112131,123456789101112132);").Check(testkit.Rows("223456789101112131"))
}

func (s *testPointGetSuite) TestPointGetCharPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(4) primary key, b char(4));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	// Test CHAR type.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test CHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) binary primary key, b char(2));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustTableDual(`select * from t where a = "a  ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows())
	tk.MustTableDual(`select * from t where a = "   ";`).Check(testkit.Rows())

}

func (s *testPointGetSuite) TestPointGetAliasTableCharPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) primary key, b char(2));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustTableDual(`select * from t tmp where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows())
	tk.MustTableDual(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())

	// Test CHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) binary primary key, b char(2));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows())
	tk.MustTableDual(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t tmp where a = "  ";`).Check(testkit.Rows())
	tk.MustTableDual(`select * from t tmp where a = "   ";`).Check(testkit.Rows())

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
	tk.MustTableDual(`select tmp.* from t tmp where a = "aab";`).Check(testkit.Rows())
	tk.MustTableDual(`select tmp.a, tmp.b from t tmp where a = "aab";`).Check(testkit.Rows())
	tk.MustTableDual(`select tmp.*, tmp.a, tmp.b from t tmp where a = "aab";`).Check(testkit.Rows())

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

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustIndexLookup(`select * from t where a = "aab";`).Check(testkit.Rows())

	// Test query with table alias
	tk.MustIndexLookup(`select * from t tmp where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustIndexLookup(`select * from t tmp where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test CHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) binary, b char(2), index idx_1(a));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "";`).Check(testkit.Rows(` `))
	tk.MustIndexLookup(`select * from t where a = " ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "  ";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "   ";`).Check(testkit.Rows())

}

func (s *testPointGetSuite) TestPointGetVarcharPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a varchar(2) primary key, b varchar(2));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustTableDual(`select * from t where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustTableDual(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// // Test VARCHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a varchar(2) binary primary key, b varchar(2));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustTableDual(`select * from t where a = "a  ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(`     `))
	tk.MustTableDual(`select * from t where a = "   ";`).Check(testkit.Rows())

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

	tk.MustExec(`insert into t values("a ", "b ");`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

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

	tk.MustExec(`insert into t values("a ", "b ");`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Rows())

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

	tk.MustExec(`insert into t values("a ", "b ");`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())

	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Rows())

}

func (s *testPointGetSuite) TestOverflowOrTruncated(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t6 (id bigint, a bigint, primary key(id), unique key(a));")
	tk.MustExec("insert into t6 values(9223372036854775807, 9223372036854775807);")
	tk.MustExec("insert into t6 values(1, 1);")
	var nilVal []string
	// for unique key
	tk.MustQuery("select * from t6 where a = 9223372036854775808").Check(testkit.Rows(nilVal...))
	tk.MustQuery("select * from t6 where a = '1.123'").Check(testkit.Rows(nilVal...))
	// for primary key
	tk.MustQuery("select * from t6 where id = 9223372036854775808").Check(testkit.Rows(nilVal...))
	tk.MustQuery("select * from t6 where id = '1.123'").Check(testkit.Rows(nilVal...))
}

func (s *testPointGetSuite) TestIssue10448(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int1 primary key)")
	tk.MustExec("insert into t values(125)")
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 128").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int8 primary key)")
	tk.MustExec("insert into t values(9223372036854775807)")
	tk.MustQuery("select * from t where pk = 9223372036854775807").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int1 unsigned primary key)")
	tk.MustExec("insert into t values(255)")
	tk.MustQuery("select * from t where pk = 255").Check(testkit.Rows("255"))
	tk.MustQuery("desc select * from t where pk = 256").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int8 unsigned primary key)")
	tk.MustExec("insert into t value(18446744073709551615)")
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:18446744073709551615"))
	tk.MustQuery("select * from t where pk = 18446744073709551615").Check(testkit.Rows("18446744073709551615"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:9223372036854775808"))
}

func (s *testPointGetSuite) TestIssue10677(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int1 primary key)")
	tk.MustExec("insert into t values(1)")
	tk.MustQuery("desc select * from t where pk = 1.1").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("select * from t where pk = 1.1").Check(testkit.Rows())
	tk.MustQuery("desc select * from t where pk = '1.1'").Check(testkit.Rows("TableDual_2 0.00 root  rows:0"))
	tk.MustQuery("select * from t where pk = '1.1'").Check(testkit.Rows())
	tk.MustQuery("desc select * from t where pk = 1").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:1"))
	tk.MustQuery("select * from t where pk = 1").Check(testkit.Rows("1"))
	tk.MustQuery("desc select * from t where pk = '1'").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:1"))
	tk.MustQuery("select * from t where pk = '1'").Check(testkit.Rows("1"))
	tk.MustQuery("desc select * from t where pk = '1.0'").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:1"))
	tk.MustQuery("select * from t where pk = '1.0'").Check(testkit.Rows("1"))
}

func (s *testPointGetSuite) TestForUpdateRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	_, err := tk.Exec("drop table if exists t")
	c.Assert(err, IsNil)
	tk.MustExec("create table t(pk int primary key, c int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustQuery("select * from t where pk = 1 for update")
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustExec("update t set c = c + 1 where pk = 1")
	tk.MustExec("update t set c = c + 1 where pk = 2")
	_, err = tk.Exec("commit")
	c.Assert(session.ErrForUpdateCantRetry.Equal(err), IsTrue)
}

func (s *testPointGetSuite) TestPointGetByRowID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(20), b int)")
	tk.MustExec("insert into t values(\"aaa\", 12)")
	tk.MustQuery("explain format = 'brief' select * from t where t._tidb_rowid = 1").Check(testkit.Rows(
		"Point_Get 1.00 root table:t handle:1"))
	tk.MustQuery("select * from t where t._tidb_rowid = 1").Check(testkit.Rows("aaa 12"))
}

func (s *testPointGetSuite) TestSelectCheckVisibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) key, b int,index idx(b))")
	tk.MustExec("insert into t values('1',1)")
	tk.MustExec("begin")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	ts := txn.StartTS()
	store := tk.Se.GetStore().(tikv.Storage)
	// Update gc safe time for check data visibility.
	store.UpdateSPCache(ts+1, time.Now())
	checkSelectResultError := func(sql string, expectErr *terror.Error) {
		re, err := tk.Exec(sql)
		c.Assert(err, IsNil)
		_, err = session.ResultSetToStringSlice(context.Background(), tk.Se, re)
		c.Assert(err, NotNil)
		c.Assert(expectErr.Equal(err), IsTrue)
	}
	// Test point get.
	checkSelectResultError("select * from t where a='1'", storeerr.ErrGCTooEarly)
	// Test batch point get.
	checkSelectResultError("select * from t where a in ('1','2')", storeerr.ErrGCTooEarly)
	// Test Index look up read.
	checkSelectResultError("select * from t where b > 0 ", storeerr.ErrGCTooEarly)
	// Test Index read.
	checkSelectResultError("select b from t where b > 0 ", storeerr.ErrGCTooEarly)
	// Test table read.
	checkSelectResultError("select * from t", storeerr.ErrGCTooEarly)
}

func (s *testPointGetSuite) TestReturnValues(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t (a varchar(64) primary key, b int)")
	tk.MustExec("insert t values ('a', 1), ('b', 2), ('c', 3)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t where a = 'b' for update").Check(testkit.Rows("b 2"))
	tid := tk.GetTableID("t")
	idxVal, err := codec.EncodeKey(tk.Se.GetSessionVars().StmtCtx, nil, types.NewStringDatum("b"))
	c.Assert(err, IsNil)
	pk := tablecodec.EncodeIndexSeekKey(tid, 1, idxVal)
	txnCtx := tk.Se.GetSessionVars().TxnCtx
	val, ok := txnCtx.GetKeyInPessimisticLockCache(pk)
	c.Assert(ok, IsTrue)
	handle, err := tablecodec.DecodeHandleInUniqueIndexValue(val, false)
	c.Assert(err, IsNil)
	rowKey := tablecodec.EncodeRowKeyWithHandle(tid, handle)
	_, ok = txnCtx.GetKeyInPessimisticLockCache(rowKey)
	c.Assert(ok, IsTrue)
	tk.MustExec("rollback")
}

func (s *testPointGetSuite) TestClusterIndexPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists pgt")
	tk.MustExec("create table pgt (a varchar(64), b varchar(64), uk int, v int, primary key(a, b), unique key uuk(uk))")
	tk.MustExec("insert pgt values ('a', 'a1', 1, 11), ('b', 'b1', 2, 22), ('c', 'c1', 3, 33)")
	tk.MustQuery(`select * from pgt where (a, b) in (('a', 'a1'), ('c', 'c1'))`).Check(testkit.Rows("a a1 1 11", "c c1 3 33"))
	tk.MustQuery(`select * from pgt where a = 'b' and b = 'b1'`).Check(testkit.Rows("b b1 2 22"))
	tk.MustQuery(`select * from pgt where uk = 1`).Check(testkit.Rows("a a1 1 11"))
	tk.MustQuery(`select * from pgt where uk in (1, 2, 3)`).Check(testkit.Rows("a a1 1 11", "b b1 2 22", "c c1 3 33"))
	tk.MustExec(`admin check table pgt`)

	tk.MustExec(`drop table if exists snp`)
	tk.MustExec(`create table snp(id1 int, id2 int, v int, primary key(id1, id2))`)
	tk.MustExec(`insert snp values (1, 1, 1), (2, 2, 2), (2, 3, 3)`)
	tk.MustQuery(`explain format = 'brief' select * from snp where id1 = 1`).Check(testkit.Rows("TableReader 10.00 root  data:TableRangeScan",
		"└─TableRangeScan 10.00 cop[tikv] table:snp range:[1,1], keep order:false, stats:pseudo"))
	tk.MustQuery(`explain format = 'brief' select * from snp where id1 in (1, 100)`).Check(testkit.Rows("TableReader 20.00 root  data:TableRangeScan",
		"└─TableRangeScan 20.00 cop[tikv] table:snp range:[1,1], [100,100], keep order:false, stats:pseudo"))
	tk.MustQuery("select * from snp where id1 = 2").Check(testkit.Rows("2 2 2", "2 3 3"))
}

func (s *testPointGetSuite) TestClusterIndexCBOPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`create table t1 (a int, b decimal(10,0), c int, primary key(a,b))`)
	tk.MustExec(`create table t2 (a varchar(20), b int, primary key(a), unique key(b))`)
	tk.MustExec(`insert into t1 values(1,1,1),(2,2,2),(3,3,3)`)
	tk.MustExec(`insert into t2 values('111',1),('222',2),('333',3)`)
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		plan := tk.MustQuery("explain format = 'brief' " + tt)
		res := tk.MustQuery(tt).Sort()
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(plan.Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(res.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
		res.Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testSerialSuite) mustExecDDL(tk *testkit.TestKit, c *C, sql string) {
	tk.MustExec(sql)
	c.Assert(s.domain.Reload(), IsNil)
}

func (s *testSerialSuite) TestMemCacheReadLock(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.Se.GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Se.GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")

	// Simply check the cached results.
	s.mustExecDDL(tk, c, "lock tables point read")
	tk.MustQuery("select id from point where id = 1").Check(testkit.Rows("1"))
	tk.MustQuery("select id from point where id = 1").Check(testkit.Rows("1"))
	s.mustExecDDL(tk, c, "unlock tables")

	cases := []struct {
		sql string
		r1  bool
		r2  bool
	}{
		{"explain analyze select * from point where id = 1", false, false},
		{"explain analyze select * from point where id in (1, 2)", false, false},

		// Cases for not exist keys.
		{"explain analyze select * from point where id = 3", true, true},
		{"explain analyze select * from point where id in (1, 3)", true, true},
		{"explain analyze select * from point where id in (3, 4)", true, true},
	}

	for _, ca := range cases {
		s.mustExecDDL(tk, c, "lock tables point read")

		rows := tk.MustQuery(ca.sql).Rows()
		c.Assert(len(rows), Equals, 1, Commentf("%v", ca.sql))
		explain := fmt.Sprintf("%v", rows[0])
		c.Assert(explain, Matches, ".*num_rpc.*")

		rows = tk.MustQuery(ca.sql).Rows()
		c.Assert(len(rows), Equals, 1)
		explain = fmt.Sprintf("%v", rows[0])
		ok := strings.Contains(explain, "num_rpc")
		c.Assert(ok, Equals, ca.r1, Commentf("%v", ca.sql))
		s.mustExecDDL(tk, c, "unlock tables")

		rows = tk.MustQuery(ca.sql).Rows()
		c.Assert(len(rows), Equals, 1)
		explain = fmt.Sprintf("%v", rows[0])
		c.Assert(explain, Matches, ".*num_rpc.*")

		// Test cache release after unlocking tables.
		s.mustExecDDL(tk, c, "lock tables point read")
		rows = tk.MustQuery(ca.sql).Rows()
		c.Assert(len(rows), Equals, 1)
		explain = fmt.Sprintf("%v", rows[0])
		c.Assert(explain, Matches, ".*num_rpc.*")

		rows = tk.MustQuery(ca.sql).Rows()
		c.Assert(len(rows), Equals, 1)
		explain = fmt.Sprintf("%v", rows[0])
		ok = strings.Contains(explain, "num_rpc")
		c.Assert(ok, Equals, ca.r2, Commentf("%v", ca.sql))

		s.mustExecDDL(tk, c, "unlock tables")
		s.mustExecDDL(tk, c, "lock tables point read")

		rows = tk.MustQuery(ca.sql).Rows()
		c.Assert(len(rows), Equals, 1)
		explain = fmt.Sprintf("%v", rows[0])
		c.Assert(explain, Matches, ".*num_rpc.*")

		s.mustExecDDL(tk, c, "unlock tables")
	}
}

func (s *testSerialSuite) TestPartitionMemCacheReadLock(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.Se.GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Se.GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int unique key, c int, d varchar(10)) partition by hash (id) partitions 4")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")

	// Confirm _tidb_rowid will not be duplicated.
	tk.MustQuery("select distinct(_tidb_rowid) from point order by _tidb_rowid").Check(testkit.Rows("1", "2"))

	s.mustExecDDL(tk, c, "lock tables point read")

	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows("1"))
	s.mustExecDDL(tk, c, "unlock tables")

	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows("1"))
	tk.MustExec("update point set id = -id")

	// Test cache release after unlocking tables.
	s.mustExecDDL(tk, c, "lock tables point read")
	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows())

	tk.MustQuery("select _tidb_rowid from point where id = -1").Check(testkit.Rows("1"))
	tk.MustQuery("select _tidb_rowid from point where id = -1").Check(testkit.Rows("1"))
	tk.MustQuery("select _tidb_rowid from point where id = -2").Check(testkit.Rows("2"))

	s.mustExecDDL(tk, c, "unlock tables")
}

func (s *testPointGetSuite) TestPointGetWriteLock(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")
	tk.MustExec("lock tables point write")
	tk.MustQuery(`select * from point where id = 1;`).Check(testkit.Rows(
		`1 1 a`,
	))
	rows := tk.MustQuery("explain analyze select * from point where id = 1").Rows()
	c.Assert(len(rows), Equals, 1)
	explain := fmt.Sprintf("%v", rows[0])
	c.Assert(explain, Matches, ".*num_rpc.*")
	tk.MustExec("unlock tables")

	tk.MustExec("update point set c = 3 where id = 1")
	tk.MustExec("lock tables point write")
	tk.MustQuery(`select * from point where id = 1;`).Check(testkit.Rows(
		`1 3 a`,
	))
	rows = tk.MustQuery("explain analyze select * from point where id = 1").Rows()
	c.Assert(len(rows), Equals, 1)
	explain = fmt.Sprintf("%v", rows[0])
	c.Assert(explain, Matches, ".*num_rpc.*")
	tk.MustExec("unlock tables")
}

func (s *testPointGetSuite) TestPointGetLockExistKey(c *C) {
	var wg sync.WaitGroup
	errCh := make(chan error)

	testLock := func(rc bool, key string, tableName string) {
		doneCh := make(chan struct{}, 1)
		tk1, tk2 := testkit.NewTestKit(c, s.store), testkit.NewTestKit(c, s.store)

		errCh <- tk1.ExecToErr("use test")
		errCh <- tk2.ExecToErr("use test")
		tk1.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly

		errCh <- tk1.ExecToErr(fmt.Sprintf("drop table if exists %s", tableName))
		errCh <- tk1.ExecToErr(fmt.Sprintf("create table %s(id int, v int, k int, %s key0(id, v))", tableName, key))
		errCh <- tk1.ExecToErr(fmt.Sprintf("insert into %s values(1, 1, 1)", tableName))

		if rc {
			errCh <- tk1.ExecToErr("set tx_isolation = 'READ-COMMITTED'")
			errCh <- tk2.ExecToErr("set tx_isolation = 'READ-COMMITTED'")
		}

		// select for update
		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")
		// lock exist key
		errCh <- tk1.ExecToErr(fmt.Sprintf("select * from %s where id = 1 and v = 1 for update", tableName))
		// read committed will not lock non-exist key
		if rc {
			errCh <- tk1.ExecToErr(fmt.Sprintf("select * from %s where id = 2 and v = 2 for update", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(2, 2, 2)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(1, 1, 10)", tableName))
			// tk2.MustExec(fmt.Sprintf("insert into %s values(1, 1, 10)", tableName))
			doneCh <- struct{}{}
		}()
		time.Sleep(150 * time.Millisecond)
		errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v = 2 where id = 1 and v = 1", tableName))
		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 2 2",
			"1 1 10",
		))

		// update
		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")
		// lock exist key
		errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v = 3 where id = 2 and v = 2", tableName))
		// read committed will not lock non-exist key
		if rc {
			errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v =4 where id = 3 and v = 3", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(3, 3, 3)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(2, 2, 20)", tableName))
			doneCh <- struct{}{}
		}()
		time.Sleep(150 * time.Millisecond)
		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"1 1 10",
			"3 3 3",
			"2 2 20",
		))

		// delete
		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")
		// lock exist key
		errCh <- tk1.ExecToErr(fmt.Sprintf("delete from %s where id = 3 and v = 3", tableName))
		// read committed will not lock non-exist key
		if rc {
			errCh <- tk1.ExecToErr(fmt.Sprintf("delete from %s where id = 4 and v = 4", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(4, 4, 4)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(3, 3, 30)", tableName))
			doneCh <- struct{}{}
		}()
		time.Sleep(50 * time.Millisecond)
		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"1 1 10",
			"2 2 20",
			"4 4 4",
			"3 3 30",
		))
		wg.Done()
	}

	for i, one := range []struct {
		rc  bool
		key string
	}{
		{rc: false, key: "primary key"},
		{rc: false, key: "unique key"},
		{rc: true, key: "primary key"},
		{rc: true, key: "unique key"},
	} {
		wg.Add(1)
		tableName := fmt.Sprintf("t_%d", i)
		go testLock(one.rc, one.key, tableName)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()
	for err := range errCh {
		c.Assert(err, IsNil)
	}
}

func (s *testPointGetSuite) TestWithTiDBSnapshot(c *C) {
	// Fix issue https://github.com/pingcap/tidb/issues/22436
	// Point get should not use math.MaxUint64 when variable @@tidb_snapshot is set.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists xx")
	tk.MustExec(`create table xx (id int key)`)
	tk.MustExec(`insert into xx values (1), (7)`)

	// Unrelated code, to make this test pass in the unit test.
	// The `tikv_gc_safe_point` global variable must be there, otherwise the 'set @@tidb_snapshot' operation fails.
	timeSafe := time.Now().Add(-48 * 60 * 60 * time.Second).Format("20060102-15:04:05 -0700 MST")
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, timeSafe))

	// Record the current tso.
	tk.MustExec("begin")
	tso := tk.Se.GetSessionVars().TxnCtx.StartTS
	tk.MustExec("rollback")
	c.Assert(tso > 0, IsTrue)

	// Insert data.
	tk.MustExec("insert into xx values (8)")

	// Change the snapshot before the tso, the inserted data should not be seen.
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%d'", tso))
	tk.MustQuery("select * from xx where id = 8").Check(testkit.Rows())

	tk.MustQuery("select * from xx").Check(testkit.Rows("1", "7"))
}

func (s *testPointGetSuite) TestPointGetIssue25167(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	defer func() {
		tk.MustExec("drop table if exists t")
	}()
	time.Sleep(50 * time.Millisecond)
	tk.MustExec("set @a=(select current_timestamp(3))")
	tk.MustExec("insert into t values (1)")
	tk.MustQuery("select * from t as of timestamp @a where a = 1").Check(testkit.Rows())
}
