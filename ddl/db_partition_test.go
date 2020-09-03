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
	"github.com/pingcap/parser/mysql"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	tmysql "github.com/pingcap/tidb/errno"
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

func (s *testIntegrationSuite3) TestCreateTableWithPartition(c *C) {
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

	sql18 := `create table t8 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than (19xx91),
		partition p2 values less than maxvalue
	);`
	tk.MustGetErrCode(sql18, mysql.ErrBadField)

	sql9 := `create TABLE t9 (
	col1 int
	)
	partition by range( case when col1 > 0 then 10 else 20 end ) (
		partition p0 values less than (2),
		partition p1 values less than (6)
	);`
	tk.MustGetErrCode(sql9, tmysql.ErrPartitionFunctionIsNotAllowed)

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
		testkit.Rows("t26 CREATE TABLE `t26` (\n  `a` date DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE ( TO_SECONDS(`a`) ) (\n  PARTITION `p0` VALUES LESS THAN (63240134400),\n  PARTITION `p1` VALUES LESS THAN (63271756800)\n)"))
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
	tk.MustExec("set @@tidb_enable_table_partition = 1")
	tk.MustExec(`create table t30 (
		  a int,
		  b float,
		  c varchar(30))
		  partition by range columns (a, b)
		  (partition p0 values less than (10, 10.0))`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8200 Unsupported partition type, treat as normal table"))

	tk.MustGetErrCode(`create table t31 (a int not null) partition by range( a );`, tmysql.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create table t32 (a int not null) partition by range columns( a );`, tmysql.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create table t33 (a int, b int) partition by hash(a) partitions 0;`, tmysql.ErrNoParts)
	tk.MustGetErrCode(`create table t33 (a timestamp, b int) partition by hash(a) partitions 30;`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)
	tk.MustGetErrCode(`CREATE TABLE t34 (c0 INT) PARTITION BY HASH((CASE WHEN 0 THEN 0 ELSE c0 END )) PARTITIONS 1;`, tmysql.ErrPartitionFunctionIsNotAllowed)
	tk.MustGetErrCode(`CREATE TABLE t0(c0 INT) PARTITION BY HASH((c0<CURRENT_USER())) PARTITIONS 1;`, tmysql.ErrPartitionFunctionIsNotAllowed)
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

	// Fix a timezone dependent check bug introduced in https://github.com/pingcap/tidb/pull/10655
	tk.MustExec(`create table t34 (dt timestamp(3)) partition by range (floor(unix_timestamp(dt))) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`)

	tk.MustGetErrCode(`create table t34 (dt timestamp(3)) partition by range (unix_timestamp(date(dt))) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`, tmysql.ErrWrongExprInPartitionFunc)

	tk.MustGetErrCode(`create table t34 (dt datetime) partition by range (unix_timestamp(dt)) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`, tmysql.ErrWrongExprInPartitionFunc)

	// Fix https://github.com/pingcap/tidb/issues/16333
	tk.MustExec(`create table t35 (dt timestamp) partition by range (unix_timestamp(dt))
(partition p0 values less than (unix_timestamp('2020-04-15 00:00:00')));`)

	tk.MustExec(`drop table if exists too_long_identifier`)
	tk.MustGetErrCode(`create table too_long_identifier(a int)
partition by range (a)
(partition p0pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp values less than (10));`, tmysql.ErrTooLongIdent)

	tk.MustExec(`drop table if exists too_long_identifier`)
	tk.MustExec("create table too_long_identifier(a int) partition by range(a) (partition p0 values less than(10))")
	tk.MustGetErrCode("alter table too_long_identifier add partition "+
		"(partition p0pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp values less than(20))", tmysql.ErrTooLongIdent)

	tk.MustExec(`create table t36 (a date, b datetime) partition by range (EXTRACT(YEAR_MONTH FROM a)) (
    partition p0 values less than (200),
    partition p1 values less than (300),
    partition p2 values less than maxvalue)`)
}

func (s *testIntegrationSuite2) TestCreateTableWithHashPartition(c *C) {
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

	tk.MustExec("CREATE TABLE t_linear (a int, b varchar(128)) PARTITION BY LINEAR HASH(a) PARTITIONS 4")
	tk.MustGetErrCode("select * from t_linear partition (p0)", tmysql.ErrPartitionClauseOnNonpartitioned)

	tk.MustExec(`CREATE TABLE t_sub (a int, b varchar(128)) PARTITION BY RANGE( a ) SUBPARTITION BY HASH( a )
                                   SUBPARTITIONS 2 (
                                       PARTITION p0 VALUES LESS THAN (100),
                                       PARTITION p1 VALUES LESS THAN (200),
                                       PARTITION p2 VALUES LESS THAN MAXVALUE)`)
	tk.MustGetErrCode("select * from t_sub partition (p0)", tmysql.ErrPartitionClauseOnNonpartitioned)

	// Fix create partition table using extract() function as partition key.
	tk.MustExec("create table t2 (a date, b datetime) partition by hash (EXTRACT(YEAR_MONTH FROM a)) partitions 7")
}

func (s *testIntegrationSuite1) TestCreateTableWithRangeColumnPartition(c *C) {
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
		// create as normal table, warning.
		//	{
		//		"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
		//			"partition p0 values less than (1, 'a')," +
		//			"partition p1 values less than (1, 'a'))",
		//		ddl.ErrRangeNotIncreasing,
		//	},
		{
			"create table t (a int, b varchar(64)) partition by range columns ( b) (" +
				"partition p0 values less than ( 'a')," +
				"partition p1 values less than ('a'))",
			ddl.ErrRangeNotIncreasing,
		},
		// create as normal table, warning.
		//	{
		//		"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
		//			"partition p0 values less than (1, 'b')," +
		//			"partition p1 values less than (1, 'a'))",
		//		ddl.ErrRangeNotIncreasing,
		//	},
		{
			"create table t (a int, b varchar(64)) partition by range columns (b) (" +
				"partition p0 values less than ('b')," +
				"partition p1 values less than ('a'))",
			ddl.ErrRangeNotIncreasing,
		},
		// create as normal table, warning.
		//		{
		//			"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
		//				"partition p0 values less than (1, maxvalue)," +
		//				"partition p1 values less than (1, 'a'))",
		//			ddl.ErrRangeNotIncreasing,
		//		},
		{
			"create table t (a int, b varchar(64)) partition by range columns ( b) (" +
				"partition p0 values less than (  maxvalue)," +
				"partition p1 values less than ('a'))",
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

	tk.MustExec("create table t2 (a int, b char(3)) partition by range columns (b) (" +
		"partition p0 values less than ( 'a')," +
		"partition p1 values less than (maxvalue))")
}

func (s *testIntegrationSuite3) TestCreateTableWithKeyPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tm1;")
	tk.MustExec(`create table tm1
	(
		s1 char(32) primary key
	)
	partition by key(s1) partitions 10;`)

	tk.MustExec(`drop table if exists tm2`)
	tk.MustExec(`create table tm2 (a char(5), unique key(a(5))) partition by key() partitions 5;`)
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

	c.Assert(part.Expr, Equals, "YEAR(`hired`)")
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

func (s *testIntegrationSuite5) TestAlterTableDropPartition(c *C) {
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

func (s *testIntegrationSuite5) TestMultiPartitionDropAndTruncate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists employees")
	tk.MustExec(`create table employees (
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001),
		partition p4 values less than (2006),
		partition p5 values less than (2011)
	);`)
	tk.MustExec(`INSERT INTO employees VALUES (1990), (1995), (2000), (2005), (2010)`)

	tk.MustExec("alter table employees drop partition p1, p2;")
	result := tk.MustQuery("select * from employees;")
	result.Sort().Check(testkit.Rows(`2000`, `2005`, `2010`))

	tk.MustExec("alter table employees truncate partition p3, p4")
	result = tk.MustQuery("select * from employees;")
	result.Check(testkit.Rows(`2010`))
}

func (s *testIntegrationSuite7) TestAlterTableExchangePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists e")
	tk.MustExec("drop table if exists e2")
	tk.MustExec(`CREATE TABLE e (
		id INT NOT NULL
	)
    PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (50),
        PARTITION p1 VALUES LESS THAN (100),
        PARTITION p2 VALUES LESS THAN (150),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
	);`)
	tk.MustExec(`CREATE TABLE e2 (
		id INT NOT NULL
	);`)
	tk.MustExec(`INSERT INTO e VALUES (1669),(337),(16),(2005)`)
	tk.MustExec("ALTER TABLE e EXCHANGE PARTITION p0 WITH TABLE e2")
	tk.MustQuery("select * from e2").Check(testkit.Rows("16"))
	tk.MustQuery("select * from e").Check(testkit.Rows("1669", "337", "2005"))
	// validation test for range partition
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p2 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p3 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)

	tk.MustExec("drop table if exists e3")

	tk.MustExec(`CREATE TABLE e3 (
		id int not null
	) PARTITION BY HASH (id)
	PARTITIONS 4;`)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e3;", tmysql.ErrPartitionExchangePartTable)
	tk.MustExec("truncate table e2")
	tk.MustExec(`INSERT INTO e3 VALUES (1),(5)`)

	tk.MustExec("ALTER TABLE e3 EXCHANGE PARTITION p1 WITH TABLE e2;")
	tk.MustQuery("select * from e3 partition(p0)").Check(testkit.Rows())
	tk.MustQuery("select * from e2").Check(testkit.Rows("1", "5"))

	// validation test for hash partition
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p0 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p2 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p3 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)

	// without validation test
	tk.MustExec("ALTER TABLE e3 EXCHANGE PARTITION p0 with TABLE e2 WITHOUT VALIDATION")

	tk.MustQuery("select * from e3 partition(p0)").Check(testkit.Rows("1", "5"))
	tk.MustQuery("select * from e2").Check(testkit.Rows())

	// more boundary test of range partition
	// for partition p0
	tk.MustExec(`create table e4 (a int) partition by range(a) (
		partition p0 values less than (3),
		partition p1 values less than (6),
        PARTITION p2 VALUES LESS THAN (9),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
		);`)
	tk.MustExec(`create table e5(a int);`)

	tk.MustExec("insert into e5 values (1)")

	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p2 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p0 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p0)").Check(testkit.Rows("1"))

	// for partition p1
	tk.MustExec("insert into e5 values (3)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p2 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p1 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p1)").Check(testkit.Rows("3"))

	// for partition p2
	tk.MustExec("insert into e5 values (6)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p2 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p2)").Check(testkit.Rows("6"))

	// for partition p3
	tk.MustExec("insert into e5 values (9)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("alter table e4 exchange partition p2 with table e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p3 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p3)").Check(testkit.Rows("9"))

	// for columns range partition
	tk.MustExec(`create table e6 (a varchar(3)) partition by range columns (a) (
		partition p0 values less than ('3'),
		partition p1 values less than ('6')
	);`)
	tk.MustExec(`create table e7 (a varchar(3));`)
	tk.MustExec(`insert into e6 values ('1');`)
	tk.MustExec(`insert into e7 values ('2');`)
	tk.MustExec("alter table e6 exchange partition p0 with table e7")

	tk.MustQuery("select * from e6 partition(p0)").Check(testkit.Rows("2"))
	tk.MustQuery("select * from e7").Check(testkit.Rows("1"))
	tk.MustGetErrCode("alter table e6 exchange partition p1 with table e7", tmysql.ErrRowDoesNotMatchPartition)

	// test exchange partition from different databases
	tk.MustExec("create table e8 (a int) partition by hash(a) partitions 2;")
	tk.MustExec("create database if not exists exchange_partition")
	tk.MustExec("insert into e8 values (1), (3), (5)")
	tk.MustExec("use exchange_partition;")
	tk.MustExec("create table e9 (a int);")
	tk.MustExec("insert into e9 values (7), (9)")
	tk.MustExec("alter table test.e8 exchange partition p1 with table e9")

	tk.MustExec("insert into e9 values (11)")
	tk.MustQuery("select * from e9").Check(testkit.Rows("1", "3", "5", "11"))
	tk.MustExec("insert into test.e8 values (11)")
	tk.MustQuery("select * from test.e8").Check(testkit.Rows("7", "9", "11"))

	tk.MustExec("use test")
	tk.MustExec("create table e10 (a int) partition by hash(a) partitions 2")
	tk.MustExec("insert into e10 values (0), (2), (4)")
	tk.MustExec("create table e11 (a int)")
	tk.MustExec("insert into e11 values (1), (3)")
	tk.MustExec("alter table e10 exchange partition p1 with table e11")
	tk.MustExec("insert into e11 values (5)")
	tk.MustQuery("select * from e11").Check(testkit.Rows("5"))
	tk.MustExec("insert into e10 values (5), (6)")
	tk.MustQuery("select * from e10 partition(p0)").Check(testkit.Rows("0", "2", "4", "6"))
	tk.MustQuery("select * from e10 partition(p1)").Check(testkit.Rows("1", "3", "5"))

	// test for column id
	tk.MustExec("create table e12 (a int(1), b int, index (a)) partition by hash(a) partitions 3")
	tk.MustExec("create table e13 (a int(8), b int, index (a));")
	tk.MustExec("alter table e13 drop column b")
	tk.MustExec("alter table e13 add column b int")
	tk.MustGetErrCode("alter table e12 exchange partition p0 with table e13", tmysql.ErrPartitionExchangeDifferentOption)
	// test for index id
	tk.MustExec("create table e14 (a int, b int, index(a));")
	tk.MustExec("alter table e12 drop index a")
	tk.MustExec("alter table e12 add index (a);")
	tk.MustGetErrCode("alter table e12 exchange partition p0 with table e14", tmysql.ErrPartitionExchangeDifferentOption)

	// test for tiflash replica
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")

	tk.MustExec("create table e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create table e16 (a int)")
	tk.MustExec("alter table e15 set tiflash replica 1;")
	tk.MustExec("alter table e16 set tiflash replica 2;")

	e15 := testGetTableByName(c, s.ctx, "test", "e15")
	partition := e15.Meta().Partition

	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 := testGetTableByName(c, s.ctx, "test", "e16")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustGetErrCode("alter table e15 exchange partition p0 with table e16", tmysql.ErrTablesDifferentMetadata)
	tk.MustExec("drop table e15, e16")

	tk.MustExec("create table e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create table e16 (a int)")
	tk.MustExec("alter table e15 set tiflash replica 1;")
	tk.MustExec("alter table e16 set tiflash replica 1;")

	e15 = testGetTableByName(c, s.ctx, "test", "e15")
	partition = e15.Meta().Partition

	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 = testGetTableByName(c, s.ctx, "test", "e16")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("alter table e15 exchange partition p0 with table e16")

	e15 = testGetTableByName(c, s.ctx, "test", "e15")

	partition = e15.Meta().Partition

	c.Assert(e15.Meta().TiFlashReplica, NotNil)
	c.Assert(e15.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(e15.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID})

	e16 = testGetTableByName(c, s.ctx, "test", "e16")
	c.Assert(e16.Meta().TiFlashReplica, NotNil)
	c.Assert(e16.Meta().TiFlashReplica.Available, IsTrue)

	tk.MustExec("drop table e15, e16")
	tk.MustExec("create table e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create table e16 (a int)")
	tk.MustExec("alter table e16 set tiflash replica 1;")

	tk.MustExec("alter table e15 set tiflash replica 1 location labels 'a', 'b';")

	tk.MustGetErrCode("alter table e15 exchange partition p0 with table e16", tmysql.ErrTablesDifferentMetadata)

	tk.MustExec("alter table e16 set tiflash replica 1 location labels 'a', 'b';")

	e15 = testGetTableByName(c, s.ctx, "test", "e15")
	partition = e15.Meta().Partition

	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 = testGetTableByName(c, s.ctx, "test", "e16")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("alter table e15 exchange partition p0 with table e16")
}

func (s *testIntegrationSuite4) TestExchangePartitionTableCompatiable(c *C) {
	type testCase struct {
		ptSQL       string
		ntSQL       string
		exchangeSQL string
		err         *terror.Error
	}
	cases := []testCase{
		{
			"create table pt (id int not null) partition by hash (id) partitions 4;",
			"create table nt (id int(1) not null);",
			"alter table pt exchange partition p0 with table nt;",
			nil,
		},
		{
			"create table pt1 (id int not null, fname varchar(3)) partition by hash (id) partitions 4;",
			"create table nt1 (id int not null, fname varchar(4));",
			"alter table pt1 exchange partition p0 with table nt1;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt2 (id int not null, salary decimal) partition by hash(id) partitions 4;",
			"create table nt2 (id int not null, salary decimal(3,2));",
			"alter table pt2 exchange partition p0 with table nt2;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt3 (id int not null, salary decimal) partition by hash(id) partitions 1;",
			"create table nt3 (id int not null, salary decimal(10, 1));",
			"alter table pt3 exchange partition p0 with table nt3",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt4 (id int not null) partition by hash(id) partitions 1;",
			"create table nt4 (id1 int not null);",
			"alter table pt4 exchange partition p0 with table nt4;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt5 (id int not null, primary key (id)) partition by hash(id) partitions 1;",
			"create table nt5 (id int not null);",
			"alter table pt5 exchange partition p0 with table nt5;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt6 (id int not null, salary decimal, index idx (id, salary)) partition by hash(id) partitions 1;",
			"create table nt6 (id int not null, salary decimal, index idx (salary, id));",
			"alter table pt6 exchange partition p0 with table nt6;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt7 (id int not null, index idx (id) invisible) partition by hash(id) partitions 1;",
			"create table nt7 (id int not null, index idx (id));",
			"alter table pt7 exchange partition p0 with table nt7;",
			nil,
		},
		{
			"create table pt8 (id int not null, index idx (id)) partition by hash(id) partitions 1;",
			"create table nt8 (id int not null, index id_idx (id));",
			"alter table pt8 exchange partition p0 with table nt8;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			// foreign key test
			// Partition table doesn't support to add foreign keys in mysql
			"create table pt9 (id int not null primary key auto_increment,t_id int not null) partition by hash(id) partitions 1;",
			"create table nt9 (id int not null primary key auto_increment, t_id int not null,foreign key fk_id (t_id) references pt5(id));",
			"alter table pt9 exchange partition p0 with table nt9;",
			ddl.ErrPartitionExchangeForeignKey,
		},
		{
			// Generated column (virtual)
			"create table pt10 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname,' ')) virtual) partition by hash(id) partitions 1;",
			"create table nt10 (id int not null, lname varchar(30), fname varchar(100));",
			"alter table pt10 exchange partition p0 with table nt10;",
			ddl.ErrUnsupportedOnGeneratedColumn,
		},
		{
			"create table pt11 (id int not null, lname varchar(30), fname varchar(100)) partition by hash(id) partitions 1;",
			"create table nt11 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter table pt11 exchange partition p0 with table nt11;",
			ddl.ErrUnsupportedOnGeneratedColumn,
		},
		{

			"create table pt12 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname,' ')) stored) partition by hash(id) partitions 1;",
			"create table nt12 (id int not null, lname varchar(30), fname varchar(100));",
			"alter table pt12 exchange partition p0 with table nt12;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt13 (id int not null, lname varchar(30), fname varchar(100)) partition by hash(id) partitions 1;",
			"create table nt13 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) stored);",
			"alter table pt13 exchange partition p0 with table nt13;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt14 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual) partition by hash(id) partitions 1;",
			"create table nt14 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter table pt14 exchange partition p0 with table nt14;",
			nil,
		},
		{
			// unique index
			"create table pt15 (id int not null, unique index uk_id (id)) partition by hash(id) partitions 1;",
			"create table nt15 (id int not null, index uk_id (id));",
			"alter table pt15 exchange partition p0 with table nt15",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			// auto_increment
			"create table pt16 (id int not null primary key auto_increment) partition by hash(id) partitions 1;",
			"create table nt16 (id int not null primary key);",
			"alter table pt16 exchange partition p0 with table nt16;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			// default
			"create table pt17 (id int not null default 1) partition by hash(id) partitions 1;",
			"create table nt17 (id int not null);",
			"alter table pt17 exchange partition p0 with table nt17;",
			nil,
		},
		{
			// view test
			"create table pt18 (id int not null) partition by hash(id) partitions 1;",
			"create view nt18 as select id from nt17;",
			"alter table pt18 exchange partition p0 with table nt18",
			ddl.ErrCheckNoSuchTable,
		},
		{
			"create table pt19 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) stored) partition by hash(id) partitions 1;",
			"create table nt19 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter table pt19 exchange partition p0 with table nt19;",
			ddl.ErrUnsupportedOnGeneratedColumn,
		},
		{
			"create table pt20 (id int not null) partition by hash(id) partitions 1;",
			"create table nt20 (id int default null);",
			"alter table pt20 exchange partition p0 with table nt20;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			// unsigned
			"create table pt21 (id int unsigned) partition by hash(id) partitions 1;",
			"create table nt21 (id int);",
			"alter table pt21 exchange partition p0 with table nt21;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			// zerofill
			"create table pt22 (id int) partition by hash(id) partitions 1;",
			"create table nt22 (id int zerofill);",
			"alter table pt22 exchange partition p0 with table nt22;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt23 (id int, lname varchar(10) charset binary) partition by hash(id) partitions 1;",
			"create table nt23 (id int, lname varchar(10));",
			"alter table pt23 exchange partition p0 with table nt23;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt25 (id int, a datetime on update current_timestamp) partition by hash(id) partitions 1;",
			"create table nt25 (id int, a datetime);",
			"alter table pt25 exchange partition p0 with table nt25;",
			nil,
		},
		{
			"create table pt26 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual) partition by hash(id) partitions 1;",
			"create table nt26 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(id, ' ')) virtual);",
			"alter table pt26 exchange partition p0 with table nt26;",
			ddl.ErrTablesDifferentMetadata,
		},
		{
			"create table pt27 (a int key, b int, index(a)) partition by hash(a) partitions 1;",
			"create table nt27 (a int not null, b int, index(a));",
			"alter table pt27 exchange partition p0 with table nt27;",
			ddl.ErrTablesDifferentMetadata,
		},
	}

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	for i, t := range cases {
		tk.MustExec(t.ptSQL)
		tk.MustExec(t.ntSQL)
		if t.err != nil {
			_, err := tk.Exec(t.exchangeSQL)
			c.Assert(terror.ErrorEqual(err, t.err), IsTrue, Commentf(
				"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
				i, t.exchangeSQL, t.err, err,
			))
		} else {
			tk.MustExec(t.exchangeSQL)
		}
	}
}

func (s *testIntegrationSuite7) TestExchangePartitionExpressIndex(c *C) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists pt1;")
	tk.MustExec("create table pt1(a int, b int, c int) PARTITION BY hash (a) partitions 1;")
	tk.MustExec("alter table pt1 add index idx((a+c));")

	tk.MustExec("drop table if exists nt1;")
	tk.MustExec("create table nt1(a int, b int, c int);")
	tk.MustGetErrCode("alter table pt1 exchange partition p0 with table nt1;", tmysql.ErrTablesDifferentMetadata)

	tk.MustExec("alter table nt1 add column (`_V$_idx_0` bigint(20) generated always as (a+b) virtual);")
	tk.MustGetErrCode("alter table pt1 exchange partition p0 with table nt1;", tmysql.ErrTablesDifferentMetadata)

	// test different expression index when expression returns same field type
	tk.MustExec("alter table nt1 drop column `_V$_idx_0`;")
	tk.MustExec("alter table nt1 add index idx((b-c));")
	tk.MustGetErrCode("alter table pt1 exchange partition p0 with table nt1;", tmysql.ErrTablesDifferentMetadata)

	// test different expression index when expression returns different field type
	tk.MustExec("alter table nt1 drop index idx;")
	tk.MustExec("alter table nt1 add index idx((concat(a, b)));")
	tk.MustGetErrCode("alter table pt1 exchange partition p0 with table nt1;", tmysql.ErrTablesDifferentMetadata)

	tk.MustExec("drop table if exists nt2;")
	tk.MustExec("create table nt2 (a int, b int, c int)")
	tk.MustExec("alter table nt2 add index idx((a+c))")
	tk.MustExec("alter table pt1 exchange partition p0 with table nt2")

}

func (s *testIntegrationSuite4) TestAddPartitionTooManyPartitions(c *C) {
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

func (s *testIntegrationSuite4) TestTruncatePartitionAndDropTable(c *C) {
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

	sql12 := `create table part12 (a varchar(20), b binary, unique index (a(5))) partition by range columns (a) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`
	tk.MustGetErrCode(sql12, tmysql.ErrUniqueKeyNeedAllFieldsInPf)
	tk.MustExec(`create table part12 (a varchar(20), b binary) partition by range columns (a) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`)
	tk.MustGetErrCode("alter table part12 add unique index (a(5))", tmysql.ErrUniqueKeyNeedAllFieldsInPf)
	sql13 := `create table part13 (a varchar(20), b varchar(10), unique index (a(5),b)) partition by range columns (b) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`
	tk.MustExec(sql13)
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

func (s *testIntegrationSuite4) TestPartitionCancelAddIndex(c *C) {
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
	count := defaultBatchSize * 32
	// add some rows
	for i := 0; i < count; i += defaultBatchSize {
		batchInsert(tk, "t1", i, i+defaultBatchSize)
	}

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
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
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

	f := func(end int, isPK bool) string {
		dml := fmt.Sprintf("insert into partition_add_idx values")
		for i := 0; i < end; i++ {
			dVal := 1988 + rand.Intn(30)
			if isPK {
				dVal = 1518 + i
			}
			dml += fmt.Sprintf("(%d, '%d-01-01')", i, dVal)
			if i != end-1 {
				dml += ","
			}
		}
		return dml
	}
	var dml string
	if key == "primary key" {
		idxName1 = "primary"
		// For the primary key, hired must be unique.
		dml = f(500, true)
	} else {
		dml = f(500, false)
	}
	tk.MustExec(dml)

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
		startKey := partition.RecordKey(kv.IntHandle(math.MinInt64))
		c.Assert(ctx.NewTxn(context.Background()), IsNil)
		err := partition.IterRecords(ctx, startKey, partition.Cols(),
			func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
				num++
				return true, nil
			})
		c.Assert(err, IsNil)
	}
	return num
}

func (s *testIntegrationSuite3) TestPartitionErrorCode(c *C) {
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

	tk.MustGetErrCode(`alter table t_part reorganize partition p0, p1 into (
			partition p0 values less than (1980));`, tmysql.ErrUnsupportedDDLOperation)

	tk.MustGetErrCode("alter table t_part check partition p0, p1;", tmysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part optimize partition p0,p1;", tmysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part rebuild partition p0,p1;", tmysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part remove partitioning;", tmysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part repair partition p1;", tmysql.ErrUnsupportedDDLOperation)
}

func (s *testIntegrationSuite5) TestConstAndTimezoneDepent(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// add partition
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec("drop database if exists test_db_with_partition_const")
	tk.MustExec("create database test_db_with_partition_const")
	tk.MustExec("use test_db_with_partition_const")

	sql1 := `create table t1 ( id int )
		partition by range(4) (
		partition p1 values less than (10)
		);`
	tk.MustGetErrCode(sql1, tmysql.ErrWrongExprInPartitionFunc)

	sql2 := `create table t2 ( time_recorded timestamp )
		partition by range(TO_DAYS(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql2, tmysql.ErrWrongExprInPartitionFunc)

	sql3 := `create table t3 ( id int )
		partition by range(DAY(id)) (
		partition p1 values less than (1)
		);`
	tk.MustGetErrCode(sql3, tmysql.ErrWrongExprInPartitionFunc)

	sql4 := `create table t4 ( id int )
		partition by hash(4) partitions 4
		;`
	tk.MustGetErrCode(sql4, tmysql.ErrWrongExprInPartitionFunc)

	sql5 := `create table t5 ( time_recorded timestamp )
		partition by range(to_seconds(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql5, tmysql.ErrWrongExprInPartitionFunc)

	sql6 := `create table t6 ( id int )
		partition by range(to_seconds(id)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql6, tmysql.ErrWrongExprInPartitionFunc)

	sql7 := `create table t7 ( time_recorded timestamp )
		partition by range(abs(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql7, tmysql.ErrWrongExprInPartitionFunc)

	sql8 := `create table t2332 ( time_recorded time )
         partition by range(TO_DAYS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql8, tmysql.ErrWrongExprInPartitionFunc)

	sql9 := `create table t1 ( id int )
		partition by hash(4) partitions 4;`
	tk.MustGetErrCode(sql9, tmysql.ErrWrongExprInPartitionFunc)

	sql10 := `create table t1 ( id int )
		partition by hash(ed) partitions 4;`
	tk.MustGetErrCode(sql10, tmysql.ErrBadField)

	sql11 := `create table t2332 ( time_recorded time )
         partition by range(TO_SECONDS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql11, tmysql.ErrWrongExprInPartitionFunc)

	sql12 := `create table t2332 ( time_recorded time )
         partition by range(TO_SECONDS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql12, tmysql.ErrWrongExprInPartitionFunc)

	sql13 := `create table t2332 ( time_recorded time )
         partition by range(day(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql13, tmysql.ErrWrongExprInPartitionFunc)

	sql14 := `create table t2332 ( time_recorded timestamp )
         partition by range(day(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql14, tmysql.ErrWrongExprInPartitionFunc)
}

func (s *testIntegrationSuite5) TestConstAndTimezoneDepent2(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// add partition
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec("drop database if exists test_db_with_partition_const")
	tk.MustExec("create database test_db_with_partition_const")
	tk.MustExec("use test_db_with_partition_const")

	tk.MustExec(`create table t1 ( time_recorded datetime )
	partition by range(TO_DAYS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustExec(`create table t2 ( time_recorded date )
	partition by range(TO_DAYS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustExec(`create table t3 ( time_recorded date )
	partition by range(TO_SECONDS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustExec(`create table t4 ( time_recorded date )
	partition by range(TO_SECONDS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustExec(`create table t5 ( time_recorded timestamp )
	partition by range(unix_timestamp(time_recorded)) (
		partition p1 values less than (1559192604)
	);`)
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

	_, err := tk.Exec("alter table test_1465 partition by hash(a)")
	c.Assert(err, ErrorMatches, ".*alter table partition is unsupported")
}

func (s *testIntegrationSuite7) TestCommitWhenSchemaChange(c *C) {
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
	defer func() {
		atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 0)
	}()
	_, err := tk.Se.Execute(context.Background(), "commit")
	c.Assert(domain.ErrInfoSchemaChanged.Equal(err), IsTrue)

	// Cover a bug that schema validator does not prevent transaction commit when
	// the schema has changed on the partitioned table.
	// That bug will cause data and index inconsistency!
	tk.MustExec("admin check table schema_change")
	tk.MustQuery("select * from schema_change").Check(testkit.Rows())

	// Check inconsistency when exchanging partition
	tk.MustExec(`drop table if exists pt, nt;`)
	tk.MustExec(`create table pt (a int) partition by hash(a) partitions 2;`)
	tk.MustExec(`create table nt (a int);`)

	tk.MustExec("begin")
	tk.MustExec("insert into nt values (1), (3), (5);")
	tk2.MustExec("alter table pt exchange partition p1 with table nt;")
	tk.MustExec("insert into nt values (7), (9);")
	_, err = tk.Se.Execute(context.Background(), "commit")
	c.Assert(domain.ErrInfoSchemaChanged.Equal(err), IsTrue)

	tk.MustExec("admin check table pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustExec("admin check table nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustExec("insert into pt values (1), (3), (5);")
	tk2.MustExec("alter table pt exchange partition p1 with table nt;")
	tk.MustExec("insert into pt values (7), (9);")
	_, err = tk.Se.Execute(context.Background(), "commit")
	c.Assert(domain.ErrInfoSchemaChanged.Equal(err), IsTrue)

	tk.MustExec("admin check table pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustExec("admin check table nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())
}
