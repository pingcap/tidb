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

package ddl_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testIntegrationSuite9) TestCreateTableWithPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tp;")
	tk.MustExec(`CREATE TABLE tp (a int) PARTITION BY RANGE(a) (
	PARTITION p0 VALUES LESS THAN (10),
	PARTITION p1 VALUES LESS THAN (20),
	PARTITION p2 VALUES LESS THAN (MAXVALUE)
	);`)
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Partition, NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, model.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`a`")
	for _, pdef := range part.Definitions {
		c.Assert(pdef.ID, Greater, int64(0))
	}
	c.Assert(part.Definitions, HasLen, 3)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "10")
	c.Assert(part.Definitions[0].Name.L, Equals, "p0")
	c.Assert(part.Definitions[1].LessThan[0], Equals, "20")
	c.Assert(part.Definitions[1].Name.L, Equals, "p1")
	c.Assert(part.Definitions[2].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[2].Name.L, Equals, "p2")

	tk.MustExec("drop table if exists employees;")
	sql1 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p2 values less than (2001)
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrSameNamePartition)

	sql2 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`
	tk.MustGetErrCode(sql2, tmysql.ErrRangeNotIncreasing)

	sql3 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than maxvalue,
		partition p3 values less than (2001)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrPartitionMaxvalue)

	sql4 := `create table t4 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than maxvalue,
		partition p2 values less than (1991),
		partition p3 values less than (1995)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrPartitionMaxvalue)

	_, err = tk.Exec(`CREATE TABLE rc (
		a INT NOT NULL,
		b INT NOT NULL,
		c INT NOT NULL
	)
	partition by range columns(a,b,c) (
	partition p0 values less than (10,5,1),
	partition p2 values less than (50,maxvalue,10),
	partition p3 values less than (65,30,13),
	partition p4 values less than (maxvalue,30,40)
	);`)
	c.Assert(err, IsNil)

	sql6 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		 partition p0 values less than (6 , 10)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrTooManyValues)

	sql7 := `create table t7 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than (1991),
		partition p2 values less than maxvalue,
		partition p3 values less than maxvalue,
		partition p4 values less than (1995),
		partition p5 values less than maxvalue
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrPartitionMaxvalue)

	_, err = tk.Exec(`create table t8 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than (19xx91),
		partition p2 values less than maxvalue
	);`)
	c.Assert(ddl.ErrNotAllowedTypeInPartition.Equal(err), IsTrue)

	sql9 := `create TABLE t9 (
	col1 int
	)
	partition by range( case when col1 > 0 then 10 else 20 end ) (
		partition p0 values less than (2),
		partition p1 values less than (6)
	);`
	tk.MustGetErrCode(sql9, tmysql.ErrPartitionFunctionIsNotAllowed)

	tk.MustGetErrCode(`create TABLE t10 (c1 int,c2 int) partition by range(c1 / c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFunctionIsNotAllowed)
	_, err = tk.Exec(`CREATE TABLE t9 (
		a INT NOT NULL,
		b INT NOT NULL,
		c INT NOT NULL
	)
	partition by range columns(a) (
	partition p0 values less than (10),
	partition p2 values less than (20),
	partition p3 values less than (20)
	);`)
	c.Assert(ddl.ErrRangeNotIncreasing.Equal(err), IsTrue)

	tk.MustGetErrCode(`create TABLE t10 (c1 int,c2 int) partition by range(c1 / c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFunctionIsNotAllowed)

	tk.MustExec(`create TABLE t11 (c1 int,c2 int) partition by range(c1 div c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t12 (c1 int,c2 int) partition by range(c1 + c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t13 (c1 int,c2 int) partition by range(c1 - c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t14 (c1 int,c2 int) partition by range(c1 * c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t15 (c1 int,c2 int) partition by range( abs(c1) ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t16 (c1 int) partition by range( c1) (partition p0 values less than (10));`)

	tk.MustGetErrCode(`create TABLE t17 (c1 int,c2 float) partition by range(c1 + c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	tk.MustGetErrCode(`create TABLE t18 (c1 int,c2 float) partition by range( floor(c2) ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	tk.MustExec(`create TABLE t19 (c1 int,c2 float) partition by range( floor(c1) ) (partition p0 values less than (2));`)

	tk.MustExec(`create TABLE t20 (c1 int,c2 bit(10)) partition by range(c2) (partition p0 values less than (10));`)
	tk.MustExec(`create TABLE t21 (c1 int,c2 year) partition by range( c2 ) (partition p0 values less than (2000));`)

	tk.MustGetErrCode(`create TABLE t24 (c1 float) partition by range( c1 ) (partition p0 values less than (2000));`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)

	// test check order. The sql below have 2 problem: 1. ErrFieldTypeNotAllowedAsPartitionField  2. ErrPartitionMaxvalue , mysql will return ErrPartitionMaxvalue.
	tk.MustGetErrCode(`create TABLE t25 (c1 float) partition by range( c1 ) (partition p1 values less than maxvalue,partition p0 values less than (2000));`, tmysql.ErrPartitionMaxvalue)

	// Fix issue 7362.
	tk.MustExec("create table test_partition(id bigint, name varchar(255), primary key(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 PARTITION BY RANGE  COLUMNS(id) (PARTITION p1 VALUES LESS THAN (10) ENGINE = InnoDB);")

	// 'Less than' in partition expression could be a constant expression, notice that
	// the SHOW result changed.
	tk.MustExec(`create table t26 (a date)
			  partition by range(to_seconds(a))(
			  partition p0 values less than (to_seconds('2004-01-01')),
			  partition p1 values less than (to_seconds('2005-01-01')));`)
	tk.MustQuery("show create table t26").Check(
		testkit.Rows("t26 CREATE TABLE `t26` (\n  `a` date DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE ( to_seconds(`a`) ) (\n  PARTITION p0 VALUES LESS THAN (63240134400),\n  PARTITION p1 VALUES LESS THAN (63271756800)\n)"))
	tk.MustExec(`create table t27 (a bigint unsigned not null)
		  partition by range(a) (
		  partition p0 values less than (10),
		  partition p1 values less than (100),
		  partition p2 values less than (1000),
		  partition p3 values less than (18446744073709551000),
		  partition p4 values less than (18446744073709551614)
		);`)
	tk.MustExec(`create table t28 (a bigint unsigned not null)
		  partition by range(a) (
		  partition p0 values less than (10),
		  partition p1 values less than (100),
		  partition p2 values less than (1000),
		  partition p3 values less than (18446744073709551000 + 1),
		  partition p4 values less than (18446744073709551000 + 10)
		);`)

	tk.MustExec("set @@tidb_enable_table_partition = 1")
	_, err = tk.Exec(`create table t30 (
		  a int,
		  b float,
		  c varchar(30))
		  partition by range columns (a, b)
		  (partition p0 values less than (10, 10.0))`)
	c.Assert(ddl.ErrNotAllowedTypeInPartition.Equal(err), IsTrue)

	tk.MustGetErrCode(`create table t31 (a int not null) partition by range( a );`, tmysql.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create table t32 (a int not null) partition by range columns( a );`, tmysql.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create table t33 (a int, b int) partition by hash(a) partitions 0;`, tmysql.ErrNoParts)
	tk.MustGetErrCode(`create table t33 (a timestamp, b int) partition by hash(a) partitions 30;`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)
	// TODO: fix this one
	// tk.MustGetErrCode(`create table t33 (a timestamp, b int) partition by hash(unix_timestamp(a)) partitions 30;`, tmysql.ErrPartitionFuncNotAllowed)

	// Fix issue 8647
	tk.MustGetErrCode(`CREATE TABLE trb8 (
		id int(11) DEFAULT NULL,
		name varchar(50) DEFAULT NULL,
		purchased date DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
	PARTITION BY RANGE ( year(notexist.purchased) - 1 ) (
		PARTITION p0 VALUES LESS THAN (1990),
		PARTITION p1 VALUES LESS THAN (1995),
		PARTITION p2 VALUES LESS THAN (2000),
		PARTITION p3 VALUES LESS THAN (2005)
	);`, tmysql.ErrBadField)
}

func (s *testIntegrationSuite7) TestCreateTableWithHashPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists employees;")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec(`
	create table employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash(store_id) partitions 4;`)

	tk.MustExec("drop table if exists employees;")
	tk.MustExec(`
	create table employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash( year(hired) ) partitions 4;`)

	// This query makes tidb OOM without partition count check.
	tk.MustGetErrCode(`CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL DEFAULT '1970-01-01',
    separated DATE NOT NULL DEFAULT '9999-12-31',
    job_code INT,
    store_id INT
) PARTITION BY HASH(store_id) PARTITIONS 102400000000;`, tmysql.ErrTooManyPartitions)
}

func (s *testIntegrationSuite10) TestCreateTableWithRangeColumnPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists log_message_1;")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec(`
create table log_message_1 (
    add_time datetime not null default '2000-01-01 00:00:00',
    log_level int unsigned not null default '0',
    log_host varchar(32) not null,
    service_name varchar(32) not null,
    message varchar(2000)
) partition by range columns(add_time)(
    partition p201403 values less than ('2014-04-01'),
    partition p201404 values less than ('2014-05-01'),
    partition p201405 values less than ('2014-06-01'),
    partition p201406 values less than ('2014-07-01'),
    partition p201407 values less than ('2014-08-01'),
    partition p201408 values less than ('2014-09-01'),
    partition p201409 values less than ('2014-10-01'),
    partition p201410 values less than ('2014-11-01')
)`)
	tk.MustExec("drop table if exists log_message_1;")
	tk.MustExec(`
	create table log_message_1 (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash( year(hired) ) partitions 4;`)

	tk.MustExec("drop table if exists t")

	type testCase struct {
		sql string
		err *terror.Error
	}

	cases := []testCase{
		{
			"create table t (id int) partition by range columns (id);",
			ast.ErrPartitionsMustBeDefined,
		},
		{
			"create table t (id int) partition by range columns (id) (partition p0 values less than (1, 2));",
			ast.ErrPartitionColumnList,
		},
		{
			"create table t (a int) partition by range columns (b) (partition p0 values less than (1, 2));",
			ast.ErrPartitionColumnList,
		},
		{
			"create table t (a int) partition by range columns (b) (partition p0 values less than (1));",
			ddl.ErrFieldNotFoundPart,
		},
		{
			"create table t (id timestamp) partition by range columns (id) (partition p0 values less than ('2019-01-09 11:23:34'));",
			ddl.ErrNotAllowedTypeInPartition,
		},
		{
			`create table t29 (
				a decimal
			)
			partition by range columns (a)
			(partition p0 values less than (0));`,
			ddl.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id text) partition by range columns (id) (partition p0 values less than ('abc'));",
			ddl.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
				"partition p0 values less than (1, 'a')," +
				"partition p1 values less than (1, 'a'))",
			ddl.ErrRangeNotIncreasing,
		},
		{
			"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
				"partition p0 values less than (1, 'b')," +
				"partition p1 values less than (1, 'a'))",
			ddl.ErrRangeNotIncreasing,
		},
		{
			"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
				"partition p0 values less than (1, maxvalue)," +
				"partition p1 values less than (1, 'a'))",
			ddl.ErrRangeNotIncreasing,
		},
		{
			"create table t (col datetime not null default '2000-01-01')" +
				"partition by range columns (col) (" +
				"PARTITION p0 VALUES LESS THAN (20190905)," +
				"PARTITION p1 VALUES LESS THAN (20190906));",
			ddl.ErrWrongTypeColumnValue,
		},
	}
	for i, t := range cases {
		_, err := tk.Exec(t.sql)
		c.Assert(t.err.Equal(err), IsTrue, Commentf(
			"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, t.sql, t.err, err,
		))
	}

	tk.MustExec("create table t1 (a int, b char(3)) partition by range columns (a, b) (" +
		"partition p0 values less than (1, 'a')," +
		"partition p1 values less than (2, maxvalue))")
}

func (s *testIntegrationSuite8) TestCreateTableWithKeyPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tm1;")
	tk.MustExec(`create table tm1
	(
		s1 char(32) primary key
	)
	partition by key(s1) partitions 10;`)
}

func (s *testIntegrationSuite5) TestAlterTableAddPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists employees;")
	tk.MustExec(`create table employees (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	tk.MustExec(`alter table employees add partition (
    partition p4 values less than (2010),
    partition p5 values less than MAXVALUE
	);`)

	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("employees"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Partition, NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, model.PartitionTypeRange)

	c.Assert(part.Expr, Equals, "year(`hired`)")
	c.Assert(part.Definitions, HasLen, 5)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "1991")
	c.Assert(part.Definitions[0].Name, Equals, model.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "1996")
	c.Assert(part.Definitions[1].Name, Equals, model.NewCIStr("p2"))
	c.Assert(part.Definitions[2].LessThan[0], Equals, "2001")
	c.Assert(part.Definitions[2].Name, Equals, model.NewCIStr("p3"))
	c.Assert(part.Definitions[3].LessThan[0], Equals, "2010")
	c.Assert(part.Definitions[3].Name, Equals, model.NewCIStr("p4"))
	c.Assert(part.Definitions[4].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[4].Name, Equals, model.NewCIStr("p5"))

	tk.MustExec("drop table if exists table1;")
	tk.MustExec("create table table1(a int)")
	sql1 := `alter table table1 add partition (
		partition p1 values less than (2010),
		partition p2 values less than maxvalue
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)
	tk.MustExec(`create table table_MustBeDefined (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	sql2 := "alter table table_MustBeDefined add partition"
	tk.MustGetErrCode(sql2, tmysql.ErrPartitionsMustBeDefined)
	tk.MustExec("drop table if exists table2;")
	tk.MustExec(`create table table2 (

	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p2 values less than maxvalue
	);`)

	sql3 := `alter table table2 add partition (
		partition p3 values less than (2010)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrPartitionMaxvalue)

	tk.MustExec("drop table if exists table3;")
	tk.MustExec(`create table table3 (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p2 values less than (2001)
	);`)

	sql4 := `alter table table3 add partition (
		partition p3 values less than (1993)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrRangeNotIncreasing)

	sql5 := `alter table table3 add partition (
		partition p1 values less than (1993)
	);`
	tk.MustGetErrCode(sql5, tmysql.ErrSameNamePartition)

	sql6 := `alter table table3 add partition (
		partition p1 values less than (1993),
		partition p1 values less than (1995)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrSameNamePartition)

	sql7 := `alter table table3 add partition (
		partition p4 values less than (1993),
		partition p1 values less than (1995),
		partition p5 values less than maxvalue
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrSameNamePartition)

	sql8 := "alter table table3 add partition (partition p6);"
	tk.MustGetErrCode(sql8, tmysql.ErrPartitionRequiresValues)

	sql9 := "alter table table3 add partition (partition p7 values in (2018));"
	tk.MustGetErrCode(sql9, tmysql.ErrPartitionWrongValues)

	sql10 := "alter table table3 add partition partitions 4;"
	tk.MustGetErrCode(sql10, tmysql.ErrPartitionsMustBeDefined)

	tk.MustExec("alter table table3 add partition (partition p3 values less than (2001 + 10))")

	// less than value can be negative or expression.
	tk.MustExec(`CREATE TABLE tt5 (
		c3 bigint(20) NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	PARTITION BY RANGE ( c3 ) (
		PARTITION p0 VALUES LESS THAN (-3),
		PARTITION p1 VALUES LESS THAN (-2)
	);`)
	tk.MustExec(`ALTER TABLE tt5 add partition ( partition p2 values less than (-1) );`)
	tk.MustExec(`ALTER TABLE tt5 add partition ( partition p3 values less than (5-1) );`)

	// Test add partition for the table partition by range columns.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime) partition by range columns (a) (partition p1 values less than ('2019-06-01'), partition p2 values less than ('2019-07-01'));")
	sql := "alter table t add partition ( partition p3 values less than ('2019-07-01'));"
	tk.MustGetErrCode(sql, tmysql.ErrRangeNotIncreasing)
	tk.MustExec("alter table t add partition ( partition p3 values less than ('2019-08-01'));")

	// Add partition value's type should be the same with the column's type.
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (
		col date not null default '2000-01-01')
                partition by range columns (col) (
		PARTITION p0 VALUES LESS THAN ('20190905'),
		PARTITION p1 VALUES LESS THAN ('20190906'));`)
	sql = "alter table t add partition (partition p2 values less than (20190907));"
	tk.MustGetErrCode(sql, tmysql.ErrWrongTypeColumnValue)
}

func (s *testIntegrationSuite6) TestAlterTableDropPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists employees")
	tk.MustExec(`create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)

	tk.MustExec("alter table employees drop partition p3;")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("employees"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().GetPartitionInfo(), NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, model.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`hired`")
	c.Assert(part.Definitions, HasLen, 2)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "1991")
	c.Assert(part.Definitions[0].Name, Equals, model.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "1996")
	c.Assert(part.Definitions[1].Name, Equals, model.NewCIStr("p2"))

	tk.MustExec("drop table if exists table1;")
	tk.MustExec("create table table1 (a int);")
	sql1 := "alter table table1 drop partition p10;"
	tk.MustGetErrCode(sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)

	tk.MustExec("drop table if exists table2;")
	tk.MustExec(`create table table2 (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	sql2 := "alter table table2 drop partition p10;"
	tk.MustGetErrCode(sql2, tmysql.ErrDropPartitionNonExistent)

	tk.MustExec("drop table if exists table3;")
	tk.MustExec(`create table table3 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (1991)
	);`)
	sql3 := "alter table table3 drop partition p1;"
	tk.MustGetErrCode(sql3, tmysql.ErrDropLastPartition)

	tk.MustExec("drop table if exists table4;")
	tk.MustExec(`create table table4 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than MAXVALUE
	);`)

	tk.MustExec("alter table table4 drop partition p2;")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("table4"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().GetPartitionInfo(), NotNil)
	part = tbl.Meta().Partition
	c.Assert(part.Type, Equals, model.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`id`")
	c.Assert(part.Definitions, HasLen, 2)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "10")
	c.Assert(part.Definitions[0].Name, Equals, model.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[1].Name, Equals, model.NewCIStr("p3"))

	tk.MustExec("drop table if exists tr;")
	tk.MustExec(` create table tr(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	tk.MustExec(`INSERT INTO tr VALUES
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2014-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result := tk.MustQuery("select * from tr where purchased between '1995-01-01' and '1999-12-31';")
	result.Check(testkit.Rows(`2 alarm clock 1997-11-05`, `10 lava lamp 1998-12-25`))
	tk.MustExec("alter table tr drop partition p2;")
	result = tk.MustQuery("select * from tr where purchased between '1995-01-01' and '1999-12-31';")
	result.Check(testkit.Rows())

	result = tk.MustQuery("select * from tr where purchased between '2010-01-01' and '2014-12-31';")
	result.Check(testkit.Rows(`5 exercise bike 2014-05-09`, `7 espresso maker 2011-11-22`))
	tk.MustExec("alter table tr drop partition p5;")
	result = tk.MustQuery("select * from tr where purchased between '2010-01-01' and '2014-12-31';")
	result.Check(testkit.Rows())

	tk.MustExec("alter table tr drop partition p4;")
	result = tk.MustQuery("select * from tr where purchased between '2005-01-01' and '2009-12-31';")
	result.Check(testkit.Rows())

	tk.MustExec("drop table if exists table4;")
	tk.MustExec(`create table table4 (
		id int not null
	)
	partition by range( id ) (
		partition Par1 values less than (1991),
		partition pAR2 values less than (1992),
		partition Par3 values less than (1995),
		partition PaR5 values less than (1996)
	);`)
	tk.MustExec("alter table table4 drop partition Par2;")
	tk.MustExec("alter table table4 drop partition PAR5;")
	sql4 := "alter table table4 drop partition PAR0;"
	tk.MustGetErrCode(sql4, tmysql.ErrDropPartitionNonExistent)

	tk.MustExec("CREATE TABLE t1 (a int(11), b varchar(64)) PARTITION BY HASH(a) PARTITIONS 3")
	tk.MustGetErrCode("alter table t1 drop partition p2", tmysql.ErrOnlyOnRangeListPartition)
}

func (s *testIntegrationSuite11) TestAddPartitionTooManyPartitions(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	count := ddl.PartitionCountLimit
	tk.MustExec("drop table if exists p1;")
	sql1 := `create table p1 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i <= count; i++ {
		sql1 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql1 += "partition p1025 values less than (1025) );"
	tk.MustGetErrCode(sql1, tmysql.ErrTooManyPartitions)

	tk.MustExec("drop table if exists p2;")
	sql2 := `create table p2 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i < count; i++ {
		sql2 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql2 += "partition p1024 values less than (1024) );"

	tk.MustExec(sql2)
	sql3 := `alter table p2 add partition (
	partition p1025 values less than (1025)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrTooManyPartitions)
}

func checkPartitionDelRangeDone(c *C, s *testIntegrationSuite, partitionPrefix kv.Key) bool {
	hasOldPartitionData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		err := kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
			it, err := txn.Iter(partitionPrefix, nil)
			if err != nil {
				return err
			}
			if !it.Valid() {
				hasOldPartitionData = false
			} else {
				hasOldPartitionData = it.Key().HasPrefix(partitionPrefix)
			}
			it.Close()
			return nil
		})
		c.Assert(err, IsNil)
		if !hasOldPartitionData {
			break
		}
		time.Sleep(waitForCleanDataInterval)
	}
	return hasOldPartitionData
}

func (s *testIntegrationSuite6) TestTruncatePartitionAndDropTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	// Test truncate common table.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (id int(11));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t1 values (?)", i)
	}
	result := tk.MustQuery("select count(*) from t1;")
	result.Check(testkit.Rows("100"))
	tk.MustExec("truncate table t1;")
	result = tk.MustQuery("select count(*) from t1")
	result.Check(testkit.Rows("0"))

	// Test drop common table.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (id int(11));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t2 values (?)", i)
	}
	result = tk.MustQuery("select count(*) from t2;")
	result.Check(testkit.Rows("100"))
	tk.MustExec("drop table t2;")
	tk.MustGetErrCode("select * from t2;", tmysql.ErrNoSuchTable)

	// Test truncate table partition.
	tk.MustExec("drop table if exists t3;")
	tk.MustExec(`create table t3(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	tk.MustExec(`insert into t3 values
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2014-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result = tk.MustQuery("select count(*) from t3;")
	result.Check(testkit.Rows("10"))
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	oldPID := oldTblInfo.Meta().Partition.Definitions[0].ID
	tk.MustExec("truncate table t3;")
	partitionPrefix := tablecodec.EncodeTablePrefix(oldPID)
	hasOldPartitionData := checkPartitionDelRangeDone(c, s.testIntegrationSuite, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)

	// Test drop table partition.
	tk.MustExec("drop table if exists t4;")
	tk.MustExec(`create table t4(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	tk.MustExec(`insert into t4 values
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2014-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result = tk.MustQuery("select count(*) from t4; ")
	result.Check(testkit.Rows("10"))
	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	oldPID = oldTblInfo.Meta().Partition.Definitions[1].ID
	tk.MustExec("drop table t4;")
	partitionPrefix = tablecodec.EncodeTablePrefix(oldPID)
	hasOldPartitionData = checkPartitionDelRangeDone(c, s.testIntegrationSuite, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)
	tk.MustGetErrCode("select * from t4;", tmysql.ErrNoSuchTable)

	// Test truncate table partition reassigns new partitionIDs.
	tk.MustExec("drop table if exists t5;")
	tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	tk.MustExec(`create table t5(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t5"))
	c.Assert(err, IsNil)
	oldPID = oldTblInfo.Meta().Partition.Definitions[0].ID

	tk.MustExec("truncate table t5;")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t5"))
	c.Assert(err, IsNil)
	newPID := newTblInfo.Meta().Partition.Definitions[0].ID
	c.Assert(oldPID != newPID, IsTrue)

	tk.MustExec("set @@session.tidb_enable_table_partition = 1;")
	tk.MustExec("drop table if exists clients;")
	tk.MustExec(`create table clients (
		id int,
		fname varchar(30),
		lname varchar(30),
		signed date
	)
	partition by hash( month(signed) )
	partitions 12;`)
	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("clients"))
	c.Assert(err, IsNil)
	oldDefs := oldTblInfo.Meta().Partition.Definitions

	// Test truncate `hash partitioned table` reassigns new partitionIDs.
	tk.MustExec("truncate table clients;")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("clients"))
	c.Assert(err, IsNil)
	newDefs := newTblInfo.Meta().Partition.Definitions
	for i := 0; i < len(oldDefs); i++ {
		c.Assert(oldDefs[i].ID != newDefs[i].ID, IsTrue)
	}
}

func (s *testIntegrationSuite5) TestPartitionUniqueKeyNeedAllFieldsInPf(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists part1;")
	tk.MustExec(`create table part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1, col2)
	)
	partition by range( col1 + col2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop table if exists part2;")
	tk.MustExec(`create table part2 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1, col2, col3),
		unique key (col3)
	)
	partition by range( col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop table if exists part3;")
	tk.MustExec(`create table part3 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col2)
	)
	partition by range( col1 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop table if exists part4;")
	tk.MustExec(`create table part4 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col2),
		unique key(col2)
	)
	partition by range( year(col2)  ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop table if exists part5;")
	tk.MustExec(`create table part5 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col2, col4),
		unique key(col2, col1)
	)
	partition by range( col1 + col2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop table if exists Part1;")
	sql1 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1, col2)
	)
	partition by range( col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop table if exists Part1;")
	sql2 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1),
		unique key (col3)
	)
	partition by range( col1 + col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql2, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop table if exists Part1;")
	sql3 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1),
		unique key (col3)
	)
	partition by range( col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop table if exists Part1;")
	sql4 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1, col2, col3),
		unique key (col3)
	)
	partition by range( col1 + col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop table if exists Part1;")
	sql5 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col2)
	)
	partition by range( col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql5, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop table if exists Part1;")
	sql6 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col3),
		unique key(col2)
	)
	partition by range( year(col2)  ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop table if exists Part1;")
	sql7 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col3, col4),
		unique key(col2, col1)
	)
	partition by range( col1 + col2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop table if exists part6;")
	sql8 := `create table part6 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		col5 int not null,
		unique key(col1, col2),
		unique key(col1, col3)
	)
	partition by range( col1 + col2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql8, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql9 := `create table part7 (
		col1 int not null,
		col2 int not null,
		col3 int not null unique,
		unique key(col1, col2)
	)
	partition by range (col1 + col2) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	)`
	tk.MustGetErrCode(sql9, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql10 := `create table part8 (
                 a int not null,
                 b int not null,
                 c int default null,
                 d int default null,
                 e int default null,
                 primary key (a, b),
                 unique key (c, d)
        )
        partition by range columns (b) (
               partition p0 values less than (4),
               partition p1 values less than (7),
               partition p2 values less than (11)
        )`
	tk.MustGetErrCode(sql10, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql11 := `create table part9 (
                 a int not null,
                 b int not null,
                 c int default null,
                 d int default null,
                 e int default null,
                 primary key (a, b),
                 unique key (b, c, d)
        )
        partition by range columns (b, c) (
               partition p0 values less than (4, 5),
               partition p1 values less than (7, 9),
               partition p2 values less than (11, 22)
        )`
	tk.MustGetErrCode(sql11, tmysql.ErrUniqueKeyNeedAllFieldsInPf)
}

func (s *testIntegrationSuite2) TestPartitionDropPrimaryKey(c *C) {
	idxName := "primary"
	addIdxSQL := "alter table partition_drop_idx add primary key idx1 (c1);"
	dropIdxSQL := "alter table partition_drop_idx drop primary key;"
	testPartitionDropIndex(c, s.store, s.lease, idxName, addIdxSQL, dropIdxSQL)
}

func (s *testIntegrationSuite3) TestPartitionDropIndex(c *C) {
	idxName := "idx1"
	addIdxSQL := "alter table partition_drop_idx add index idx1 (c1);"
	dropIdxSQL := "alter table partition_drop_idx drop index idx1;"
	testPartitionDropIndex(c, s.store, s.lease, idxName, addIdxSQL, dropIdxSQL)
}

func testPartitionDropIndex(c *C, store kv.Storage, lease time.Duration, idxName, addIdxSQL, dropIdxSQL string) {
	tk := testkit.NewTestKit(c, store)
	done := make(chan error, 1)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists partition_drop_idx;")
	tk.MustExec(`create table partition_drop_idx (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (3),
    	partition p1 values less than (5),
    	partition p2 values less than (7),
    	partition p3 values less than (11),
    	partition p4 values less than (15),
    	partition p5 values less than (20),
		partition p6 values less than (maxvalue)
   	);`)

	num := 20
	for i := 0; i < num; i++ {
		tk.MustExec("insert into partition_drop_idx values (?, ?, ?)", i, i, i)
	}
	tk.MustExec(addIdxSQL)

	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)

	var idx1 table.Index
	for _, pidx := range t.Indices() {
		if pidx.Meta().Name.L == idxName {
			idx1 = pidx
			break
		}
	}
	c.Assert(idx1, NotNil)

	testutil.SessionExecInGoroutine(c, store, dropIdxSQL, done)
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err = <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			step := 10
			rand.Seed(time.Now().Unix())
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustExec("update partition_drop_idx set c2 = 1 where c1 = ?", n)
				tk.MustExec("insert into partition_drop_idx values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	is = domain.GetDomain(ctx).InfoSchema()
	t, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	var idxn table.Index
	t.Indices()
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == idxName {
			idxn = idx
			break
		}
	}
	c.Assert(idxn, IsNil)
	idx := tables.NewIndex(pid, t.Meta(), idx1.Meta())
	checkDelRangeDone(c, ctx, idx)
	tk.MustExec("drop table partition_drop_idx;")
}

func (s *testIntegrationSuite2) TestPartitionCancelAddPrimaryKey(c *C) {
	idxName := "primary"
	addIdxSQL := "alter table t1 add primary key c3_index (c1);"
	testPartitionCancelAddIndex(c, s.store, s.dom.DDL(), s.lease, idxName, addIdxSQL)
}

func (s *testIntegrationSuite3) TestPartitionCancelAddIndex(c *C) {
	idxName := "idx1"
	addIdxSQL := "create unique index c3_index on t1 (c1)"
	testPartitionCancelAddIndex(c, s.store, s.dom.DDL(), s.lease, idxName, addIdxSQL)
}

func testPartitionCancelAddIndex(c *C, store kv.Storage, d ddl.DDL, lease time.Duration, idxName, addIdxSQL string) {
	tk := testkit.NewTestKit(c, store)

	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`create table t1 (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (1024),
    	partition p1 values less than (2048),
    	partition p2 values less than (3072),
    	partition p3 values less than (4096),
		partition p4 values less than (maxvalue)
   	);`)
	base := defaultBatchSize * 2
	count := base
	// add some rows
	batchInsert(tk, "t1", 0, count)

	var checkErr error
	var c3IdxInfo *model.IndexInfo
	hook := &ddl.TestDDLCallback{}
	originBatchSize := tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	// Set batch size to lower try to slow down add-index reorganization, This if for hook to cancel this ddl job.
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 32")
	ctx := tk.Se.(sessionctx.Context)
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_batch_size = %v", originBatchSize.Rows()[0][0]))
	hook.OnJobUpdatedExported, c3IdxInfo, checkErr = backgroundExecOnJobUpdatedExported(c, store, ctx, hook, idxName)
	originHook := d.GetHook()
	defer d.(ddl.DDLForTest).SetHook(originHook)
	d.(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundExec(store, addIdxSQL, done)

	times := 0
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 10
			rand.Seed(time.Now().Unix())
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				tk.MustExec("delete from t1 where c1 = ?", n)
				tk.MustExec("insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	t := testGetTableByName(c, ctx, "test_db", "t1")
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, "c3_index"), IsFalse)
	}

	idx := tables.NewIndex(pid, t.Meta(), c3IdxInfo)
	checkDelRangeDone(c, ctx, idx)

	tk.MustExec("drop table t1")
}

func backgroundExecOnJobUpdatedExported(c *C, store kv.Storage, ctx sessionctx.Context, hook *ddl.TestDDLCallback, idxName string) (
	func(*model.Job), *model.IndexInfo, error) {
	var checkErr error
	first := true
	c3IdxInfo := &model.IndexInfo{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		addIndexNotFirstReorg := (job.Type == model.ActionAddIndex || job.Type == model.ActionAddPrimaryKey) &&
			job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0
		// If the action is adding index and the state is writing reorganization, it want to test the case of cancelling the job when backfilling indexes.
		// When the job satisfies this case of addIndexNotFirstReorg, the worker will start to backfill indexes.
		if !addIndexNotFirstReorg {
			// Get the index's meta.
			if c3IdxInfo != nil {
				return
			}
			t := testGetTableByName(c, ctx, "test_db", "t1")
			for _, index := range t.WritableIndices() {
				if index.Meta().Name.L == idxName {
					c3IdxInfo = index.Meta()
				}
			}
			return
		}
		// The job satisfies the case of addIndexNotFirst for the first time, the worker hasn't finished a batch of backfill indexes.
		if first {
			first = false
			return
		}
		if checkErr != nil {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.Store = store
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		jobIDs := []int64{job.ID}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		errs, err := admin.CancelJobs(txn, jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		// It only tests cancel one DDL job.
		if errs[0] != nil {
			checkErr = errors.Trace(errs[0])
			return
		}
		txn, err = hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}
	return hook.OnJobUpdatedExported, c3IdxInfo, checkErr
}

func (s *testIntegrationSuite5) TestPartitionAddPrimaryKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	testPartitionAddIndexOrPK(c, tk, "primary key")
}

func (s *testIntegrationSuite1) TestPartitionAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	testPartitionAddIndexOrPK(c, tk, "index")
}

func testPartitionAddIndexOrPK(c *C, tk *testkit.TestKit, key string) {
	tk.MustExec("use test")
	tk.MustExec(`create table partition_add_idx (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p3 values less than (2001),
	partition p4 values less than (2004),
	partition p5 values less than (2008),
	partition p6 values less than (2012),
	partition p7 values less than (2018)
	);`)
	testPartitionAddIndex(tk, c, key)

	// test hash partition table.
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec("drop table if exists partition_add_idx")
	tk.MustExec(`create table partition_add_idx (
	id int not null,
	hired date not null
	) partition by hash( year(hired) ) partitions 4;`)
	testPartitionAddIndex(tk, c, key)

	// Test hash partition for pr 10475.
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec("create table t1 (a int, b int, unique key(a)) partition by hash(a) partitions 5;")
	tk.MustExec("insert into t1 values (0,0),(1,1),(2,2),(3,3);")
	tk.MustExec(fmt.Sprintf("alter table t1 add %s idx(a)", key))
	tk.MustExec("admin check table t1;")

	// Test range partition for pr 10475.
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int, b int, unique key(a)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20));")
	tk.MustExec("insert into t1 values (0,0);")
	tk.MustExec(fmt.Sprintf("alter table t1 add %s idx(a)", key))
	tk.MustExec("admin check table t1;")

}

func testPartitionAddIndex(tk *testkit.TestKit, c *C, key string) {
	idxName1 := "idx1"
	if key == "primary key" {
		idxName1 = "primary"
		// For the primary key, hired must be unique.
		for i := 0; i < 500; i++ {
			tk.MustExec(fmt.Sprintf("insert into partition_add_idx values (%d, '%d-01-01')", i, 1518+i))
		}
	} else {
		for i := 0; i < 500; i++ {
			tk.MustExec(fmt.Sprintf("insert into partition_add_idx values (%d, '%d-01-01')", i, 1988+rand.Intn(30)))
		}
	}

	tk.MustExec(fmt.Sprintf("alter table partition_add_idx add %s idx1 (hired)", key))
	tk.MustExec("alter table partition_add_idx add index idx2 (id, hired)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("partition_add_idx"))
	c.Assert(err, IsNil)
	var idx1 table.Index
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == idxName1 {
			idx1 = idx
			break
		}
	}
	c.Assert(idx1, NotNil)

	tk.MustQuery(fmt.Sprintf("select count(hired) from partition_add_idx use index(%s)", idxName1)).Check(testkit.Rows("500"))
	tk.MustQuery("select count(id) from partition_add_idx use index(idx2)").Check(testkit.Rows("500"))

	tk.MustExec("admin check table partition_add_idx")
	tk.MustExec("drop table partition_add_idx")
}

func (s *testIntegrationSuite5) TestDropSchemaWithPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_db_with_partition")
	tk.MustExec("create database test_db_with_partition")
	tk.MustExec("use test_db_with_partition")
	tk.MustExec(`create table t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	tk.MustExec("insert into t_part values (1),(2),(11),(12);")
	ctx := s.ctx
	tbl := testGetTableByName(c, ctx, "test_db_with_partition", "t_part")

	// check records num before drop database.
	recordsNum := getPartitionTableRecordsNum(c, ctx, tbl.(table.PartitionedTable))
	c.Assert(recordsNum, Equals, 4)

	tk.MustExec("drop database if exists test_db_with_partition")

	// check job args.
	rs, err := tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	rows, err := session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(err, IsNil)
	row := rows[0]
	c.Assert(row.GetString(3), Equals, "drop schema")
	jobID := row.GetInt64(0)
	kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(jobID)
		c.Assert(err, IsNil)
		var tableIDs []int64
		err = historyJob.DecodeArgs(&tableIDs)
		c.Assert(err, IsNil)
		// There is 2 partitions.
		c.Assert(len(tableIDs), Equals, 3)
		return nil
	})

	// check records num after drop database.
	for i := 0; i < waitForCleanDataRound; i++ {
		recordsNum = getPartitionTableRecordsNum(c, ctx, tbl.(table.PartitionedTable))
		if recordsNum != 0 {
			time.Sleep(waitForCleanDataInterval)
		} else {
			break
		}
	}
	c.Assert(recordsNum, Equals, 0)
}

func getPartitionTableRecordsNum(c *C, ctx sessionctx.Context, tbl table.PartitionedTable) int {
	num := 0
	info := tbl.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.(table.PartitionedTable).GetPartition(pid)
		startKey := partition.RecordKey(math.MinInt64)
		c.Assert(ctx.NewTxn(context.Background()), IsNil)
		err := partition.IterRecords(ctx, startKey, partition.Cols(),
			func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
				num++
				return true, nil
			})
		c.Assert(err, IsNil)
	}
	return num
}

func (s *testIntegrationSuite4) TestPartitionErrorCode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// add partition
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec("drop database if exists test_db_with_partition")
	tk.MustExec("create database test_db_with_partition")
	tk.MustExec("use test_db_with_partition")
	tk.MustExec(`create table employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash(store_id)
	partitions 4;`)
	_, err := tk.Exec("alter table employees add partition partitions 8;")
	c.Assert(ddl.ErrUnsupportedAddPartition.Equal(err), IsTrue)

	_, err = tk.Exec("alter table employees add partition (partition p5 values less than (42));")
	c.Assert(ddl.ErrUnsupportedAddPartition.Equal(err), IsTrue)

	// coalesce partition
	tk.MustExec(`create table clients (
		id int,
		fname varchar(30),
		lname varchar(30),
		signed date
	)
	partition by hash( month(signed) )
	partitions 12;`)
	_, err = tk.Exec("alter table clients coalesce partition 4;")
	c.Assert(ddl.ErrUnsupportedCoalescePartition.Equal(err), IsTrue)

	tk.MustExec(`create table t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	_, err = tk.Exec("alter table t_part coalesce partition 4;")
	c.Assert(ddl.ErrCoalesceOnlyOnHashPartition.Equal(err), IsTrue)
}

func (s *testIntegrationSuite3) TestUnsupportedPartitionManagementDDLs(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists test_1465;")
	tk.MustExec(`
		create table test_1465 (a int)
		partition by range(a) (
			partition p1 values less than (10),
			partition p2 values less than (20),
			partition p3 values less than (30)
		);
	`)

	_, err := tk.Exec("alter table test_1465 truncate partition p1, p2")
	c.Assert(err, ErrorMatches, ".*can't run multi schema change")
	_, err = tk.Exec("alter table test_1465 drop partition p1, p2")
	c.Assert(err, ErrorMatches, ".*can't run multi schema change")

	_, err = tk.Exec("alter table test_1465 partition by hash(a)")
	c.Assert(err, ErrorMatches, ".*alter table partition is unsupported")
}

func (s *testIntegrationSuite8) TestTruncateTableWithPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists test_4414;")
	tk.MustExec("create table test_4414(a int, b int) partition by hash(a) partitions 10;")
	tk.MustExec("truncate table test_4414;")
	tk.MustQuery("select * from test_4414 partition (p0)").Check(testkit.Rows())
}

func (s *testIntegrationSuite3) TestCommitWhenSchemaChange(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table schema_change (a int, b timestamp)
			partition by range(a) (
			    partition p0 values less than (4),
			    partition p1 values less than (7),
			    partition p2 values less than (11)
			)`)
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	tk.MustExec("begin")
	tk.MustExec("insert into schema_change values (1, '2019-12-25 13:27:42')")
	tk.MustExec("insert into schema_change values (3, '2019-12-25 13:27:43')")

	tk2.MustExec("alter table schema_change add index idx(b)")

	tk.MustExec("insert into schema_change values (5, '2019-12-25 13:27:43')")
	tk.MustExec("insert into schema_change values (9, '2019-12-25 13:27:44')")
	atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 1)
	_, err := tk.Se.Execute(context.Background(), "commit")
	atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 0)
	c.Assert(domain.ErrInfoSchemaChanged.Equal(err), IsTrue)

	// Cover a bug that schema validator does not prevent transaction commit when
	// the schema has changeed on the partitioned table.
	// That bug will cause data and index inconsistency!
	tk.MustExec("admin check table schema_change")
	tk.MustQuery("select * from schema_change").Check(testkit.Rows())
}
