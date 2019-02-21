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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	tmysql "github.com/pingcap/parser/mysql"
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

func (s *testIntegrationSuite) TestCreateTableWithPartition(c *C) {
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
	s.testErrorCode(c, tk, sql1, tmysql.ErrSameNamePartition)

	sql2 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`
	s.testErrorCode(c, tk, sql2, tmysql.ErrRangeNotIncreasing)

	sql3 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than maxvalue,
		partition p3 values less than (2001)
	);`
	s.testErrorCode(c, tk, sql3, tmysql.ErrPartitionMaxvalue)

	sql4 := `create table t4 (
	a int not null,
  	b int not null
	)
	partition by range( id ) (
		partition p1 values less than maxvalue,
  		partition p2 values less than (1991),
  		partition p3 values less than (1995)
	);`
	s.testErrorCode(c, tk, sql4, tmysql.ErrPartitionMaxvalue)

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
	s.testErrorCode(c, tk, sql6, tmysql.ErrTooManyValues)

	sql7 := `create table t7 (
	a int not null,
  	b int not null
	)
	partition by range( id ) (
		partition p1 values less than (1991),
		partition p2 values less than maxvalue,
  		partition p3 values less than maxvalue,
  		partition p4 values less than (1995),
		partition p5 values less than maxvalue
	);`
	s.testErrorCode(c, tk, sql7, tmysql.ErrPartitionMaxvalue)

	_, err = tk.Exec(`create table t8 (
	a int not null,
	b int not null
	)
	partition by range( id ) (
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
	s.testErrorCode(c, tk, sql9, tmysql.ErrPartitionFunctionIsNotAllowed)

	s.testErrorCode(c, tk, `create TABLE t10 (c1 int,c2 int) partition by range(c1 / c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFunctionIsNotAllowed)

	tk.MustExec(`create TABLE t11 (c1 int,c2 int) partition by range(c1 div c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t12 (c1 int,c2 int) partition by range(c1 + c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t13 (c1 int,c2 int) partition by range(c1 - c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t14 (c1 int,c2 int) partition by range(c1 * c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t15 (c1 int,c2 int) partition by range( abs(c1) ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t16 (c1 int) partition by range( c1) (partition p0 values less than (10));`)

	s.testErrorCode(c, tk, `create TABLE t17 (c1 int,c2 float) partition by range(c1 + c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	s.testErrorCode(c, tk, `create TABLE t18 (c1 int,c2 float) partition by range( floor(c2) ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	tk.MustExec(`create TABLE t19 (c1 int,c2 float) partition by range( floor(c1) ) (partition p0 values less than (2));`)

	tk.MustExec(`create TABLE t20 (c1 int,c2 bit(10)) partition by range(c2) (partition p0 values less than (10));`)
	tk.MustExec(`create TABLE t21 (c1 int,c2 year) partition by range( c2 ) (partition p0 values less than (2000));`)

	s.testErrorCode(c, tk, `create TABLE t24 (c1 float) partition by range( c1 ) (partition p0 values less than (2000));`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)

	// test check order. The sql below have 2 problem: 1. ErrFieldTypeNotAllowedAsPartitionField  2. ErrPartitionMaxvalue , mysql will return ErrPartitionMaxvalue.
	s.testErrorCode(c, tk, `create TABLE t25 (c1 float) partition by range( c1 ) (partition p1 values less than maxvalue,partition p0 values less than (2000));`, tmysql.ErrPartitionMaxvalue)

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

	// Only range type partition is now supported, range columns partition only implements the parser, so it will not be checked.
	// So the following SQL statements can be executed successfully.
	tk.MustExec(`create table t29 (
		a decimal
	)
	partition by range columns (a)
	(partition p0 values less than (0));`)

	tk.MustExec("set @@tidb_enable_table_partition = 1")
	_, err = tk.Exec(`create table t30 (
		  a int,
		  b float,
		  c varchar(30))
		  partition by range columns (a, b)
		  (partition p0 values less than (10, 10.0))`)
	c.Assert(ddl.ErrUnsupportedPartitionByRangeColumns.Equal(err), IsTrue)
}

func (s *testIntegrationSuite) TestCreateTableWithHashPartition(c *C) {
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
}

func (s *testIntegrationSuite) TestCreateTableWithRangeColumnPartition(c *C) {
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
}

func (s *testIntegrationSuite) TestCreateTableWithKeyPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tm1;")
	tk.MustExec(`create table tm1
	(
		s1 char(32) primary key
	)
	partition by key(s1) partitions 10;`)
}

func (s *testIntegrationSuite) TestAlterTableAddPartition(c *C) {
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
	s.testErrorCode(c, tk, sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)
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
	s.testErrorCode(c, tk, sql2, tmysql.ErrPartitionsMustBeDefined)
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
	s.testErrorCode(c, tk, sql3, tmysql.ErrPartitionMaxvalue)

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
	s.testErrorCode(c, tk, sql4, tmysql.ErrRangeNotIncreasing)

	sql5 := `alter table table3 add partition (
		partition p1 values less than (1993)
	);`
	s.testErrorCode(c, tk, sql5, tmysql.ErrSameNamePartition)

	sql6 := `alter table table3 add partition (
		partition p1 values less than (1993),
		partition p1 values less than (1995)
	);`
	s.testErrorCode(c, tk, sql6, tmysql.ErrSameNamePartition)

	sql7 := `alter table table3 add partition (
		partition p4 values less than (1993),
		partition p1 values less than (1995),
		partition p5 values less than maxvalue
	);`
	s.testErrorCode(c, tk, sql7, tmysql.ErrSameNamePartition)
}

func (s *testIntegrationSuite) TestAlterTableDropPartition(c *C) {
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
	s.testErrorCode(c, tk, sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)

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
	s.testErrorCode(c, tk, sql2, tmysql.ErrDropPartitionNonExistent)

	tk.MustExec("drop table if exists table3;")
	tk.MustExec(`create table table3 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (1991)
	);`)
	sql3 := "alter table table3 drop partition p1;"
	s.testErrorCode(c, tk, sql3, tmysql.ErrDropLastPartition)

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
	s.testErrorCode(c, tk, sql4, tmysql.ErrDropPartitionNonExistent)
}

func (s *testIntegrationSuite) TestAddPartitionTooManyPartitions(c *C) {
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
	s.testErrorCode(c, tk, sql1, tmysql.ErrTooManyPartitions)

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
	s.testErrorCode(c, tk, sql3, tmysql.ErrTooManyPartitions)
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

func (s *testIntegrationSuite) TestTruncatePartitionAndDropTable(c *C) {
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
	s.testErrorCode(c, tk, "select * from t2;", tmysql.ErrNoSuchTable)

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
	hasOldPartitionData := checkPartitionDelRangeDone(c, s, partitionPrefix)
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
	hasOldPartitionData = checkPartitionDelRangeDone(c, s, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)
	s.testErrorCode(c, tk, "select * from t4;", tmysql.ErrNoSuchTable)

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

func (s *testIntegrationSuite) TestPartitionUniqueKeyNeedAllFieldsInPf(c *C) {
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
	s.testErrorCode(c, tk, sql1, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

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
	s.testErrorCode(c, tk, sql2, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

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
	s.testErrorCode(c, tk, sql3, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

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
	s.testErrorCode(c, tk, sql4, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

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
	s.testErrorCode(c, tk, sql5, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

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
	s.testErrorCode(c, tk, sql6, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

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
	s.testErrorCode(c, tk, sql7, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

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
	s.testErrorCode(c, tk, sql8, tmysql.ErrUniqueKeyNeedAllFieldsInPf)
}

func (s *testIntegrationSuite) TestPartitionDropIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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
	tk.MustExec("alter table partition_drop_idx add index idx1 (c1)")

	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)

	var idx1 table.Index
	for _, pidx := range t.Indices() {
		if pidx.Meta().Name.L == "idx1" {
			idx1 = pidx
			break
		}
	}
	c.Assert(idx1, NotNil)

	testutil.SessionExecInGoroutine(c, s.store, "drop index idx1 on partition_drop_idx;", done)
	ticker := time.NewTicker(s.lease / 2)
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
		if idx.Meta().Name.L == "idx1" {
			idxn = idx
			break
		}
	}
	c.Assert(idxn, IsNil)
	idx := tables.NewIndex(pid, t.Meta(), idx1.Meta())
	checkDelRangeDone(c, ctx, idx)
	tk.MustExec("drop table partition_drop_idx;")
}

func (s *testIntegrationSuite) TestPartitionCancelAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

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
	for i := 0; i < count; i++ {
		tk.MustExec("insert into t1 values (?, ?, ?)", i, i, i)
	}

	var checkErr error
	var c3IdxInfo *model.IndexInfo
	hook := &ddl.TestDDLCallback{}
	hook.OnJobUpdatedExported, c3IdxInfo, checkErr = backgroundExecOnJobUpdatedExported(c, s.store, s.ctx, hook)
	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundExec(s.store, "create index c3_index on t1 (c3)", done)

	times := 0
	ticker := time.NewTicker(s.lease / 2)
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

	t := testGetTableByName(c, s.ctx, "test_db", "t1")
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, "c3_index"), IsFalse)
	}

	idx := tables.NewIndex(pid, t.Meta(), c3IdxInfo)
	checkDelRangeDone(c, s.ctx, idx)

	tk.MustExec("drop table t1")
}

func backgroundExecOnJobUpdatedExported(c *C, store kv.Storage, ctx sessionctx.Context, hook *ddl.TestDDLCallback) (func(*model.Job), *model.IndexInfo, error) {
	var checkErr error
	first := true
	c3IdxInfo := &model.IndexInfo{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		addIndexNotFirstReorg := job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0
		// If the action is adding index and the state is writing reorganization, it want to test the case of cancelling the job when backfilling indexes.
		// When the job satisfies this case of addIndexNotFirstReorg, the worker will start to backfill indexes.
		if !addIndexNotFirstReorg {
			// Get the index's meta.
			if c3IdxInfo != nil {
				return
			}
			t := testGetTableByName(c, ctx, "test", "t1")
			for _, index := range t.WritableIndices() {
				if index.Meta().Name.L == "c3_index" {
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

func (s *testIntegrationSuite) TestPartitionAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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
	testPartitionAddIndex(tk, c)

	// test hash partition table.
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec("drop table if exists partition_add_idx")
	tk.MustExec(`create table partition_add_idx (
	id int not null,
	hired date not null
	) partition by hash( year(hired) ) partitions 4;`)
	testPartitionAddIndex(tk, c)
}

func testPartitionAddIndex(tk *testkit.TestKit, c *C) {
	for i := 0; i < 500; i++ {
		tk.MustExec(fmt.Sprintf("insert into partition_add_idx values (%d, '%d-01-01')", i, 1988+rand.Intn(30)))
	}

	tk.MustExec("alter table partition_add_idx add index idx1 (hired)")
	tk.MustExec("alter table partition_add_idx add index idx2 (id, hired)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("partition_add_idx"))
	c.Assert(err, IsNil)
	var idx1 table.Index
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == "idx1" {
			idx1 = idx
			break
		}
	}
	c.Assert(idx1, NotNil)

	tk.MustQuery("select count(hired) from partition_add_idx use index(idx1)").Check(testkit.Rows("500"))
	tk.MustQuery("select count(id) from partition_add_idx use index(idx2)").Check(testkit.Rows("500"))

	tk.MustExec("admin check table partition_add_idx")
	tk.MustExec("drop table partition_add_idx")
}

func (s *testIntegrationSuite) TestDropSchemaWithPartitionTable(c *C) {
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

func (s *testIntegrationSuite) TestPartitionErrorCode(c *C) {
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
