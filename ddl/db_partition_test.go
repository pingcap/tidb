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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	tmysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func checkGlobalIndexCleanUpDone(t *testing.T, ctx sessionctx.Context, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, pid int64) int {
	require.NoError(t, sessiontxn.NewTxn(context.Background(), ctx))
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	defer func() {
		err := txn.Rollback()
		require.NoError(t, err)
	}()

	cnt := 0
	prefix := tablecodec.EncodeTableIndexPrefix(tblInfo.ID, idxInfo.ID)
	it, err := txn.Iter(prefix, nil)
	require.NoError(t, err)
	for it.Valid() {
		if !it.Key().HasPrefix(prefix) {
			break
		}
		segs := tablecodec.SplitIndexValue(it.Value())
		require.NotNil(t, segs.PartitionID)
		_, pi, err := codec.DecodeInt(segs.PartitionID)
		require.NoError(t, err)
		require.NotEqual(t, pid, pi)
		cnt++
		err = it.Next()
		require.NoError(t, err)
	}
	return cnt
}

func TestCreateTableWithPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tp;")
	tk.MustExec(`CREATE TABLE tp (a int) PARTITION BY RANGE(a) (
	PARTITION p0 VALUES LESS THAN (10),
	PARTITION p1 VALUES LESS THAN (20),
	PARTITION p2 VALUES LESS THAN (MAXVALUE)
	);`)
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().Partition)
	part := tbl.Meta().Partition
	require.Equal(t, model.PartitionTypeRange, part.Type)
	require.Equal(t, "`a`", part.Expr)
	for _, pdef := range part.Definitions {
		require.Greater(t, pdef.ID, int64(0))
	}
	require.Len(t, part.Definitions, 3)
	require.Equal(t, "10", part.Definitions[0].LessThan[0])
	require.Equal(t, "p0", part.Definitions[0].Name.L)
	require.Equal(t, "20", part.Definitions[1].LessThan[0])
	require.Equal(t, "p1", part.Definitions[1].Name.L)
	require.Equal(t, "MAXVALUE", part.Definitions[2].LessThan[0])
	require.Equal(t, "p2", part.Definitions[2].Name.L)

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
	require.NoError(t, err)

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
	require.True(t, dbterror.ErrRangeNotIncreasing.Equal(err))

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
		testkit.Rows("t26 CREATE TABLE `t26` (\n  `a` date DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE (TO_SECONDS(`a`))\n(PARTITION `p0` VALUES LESS THAN (63240134400),\n PARTITION `p1` VALUES LESS THAN (63271756800))"))
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8200 Unsupported partition type RANGE, treat as normal table"))

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

func TestCreateTableWithHashPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	tk.MustExec("create table t3 (a int, b int) partition by hash(ceiling(a-b)) partitions 10")
	tk.MustExec("create table t4 (a int, b int) partition by hash(floor(a-b)) partitions 10")
}

func TestCreateTableWithRangeColumnPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists log_message_1;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
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
			"create table t(a datetime) partition by range columns (a) (partition p1 values less than ('2000-02-01'), partition p2 values less than ('20000102'));",
			dbterror.ErrRangeNotIncreasing,
		},
		{
			"create table t(a time) partition by range columns (a) (partition p1 values less than ('202020'), partition p2 values less than ('20:20:10'));",
			dbterror.ErrRangeNotIncreasing,
		},
		{
			"create table t(a time) partition by range columns (a) (partition p1 values less than ('202090'));",
			dbterror.ErrWrongTypeColumnValue,
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
			dbterror.ErrFieldNotFoundPart,
		},
		{
			"create table t (a date) partition by range (to_days(to_days(a))) (partition p0 values less than (1));",
			dbterror.ErrWrongExprInPartitionFunc,
		},
		{
			"create table t (id timestamp) partition by range columns (id) (partition p0 values less than ('2019-01-09 11:23:34'));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			`create table t29 (
				a decimal
			)
			partition by range columns (a)
			(partition p0 values less than (0));`,
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id text) partition by range columns (id) (partition p0 values less than ('abc'));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		// create as normal table, warning.
		//	{
		//		"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
		//			"partition p0 values less than (1, 'a')," +
		//			"partition p1 values less than (1, 'a'))",
		//		dbterror.ErrRangeNotIncreasing,
		//	},
		{
			"create table t (a int, b varchar(64)) partition by range columns ( b) (" +
				"partition p0 values less than ( 'a')," +
				"partition p1 values less than ('a'))",
			dbterror.ErrRangeNotIncreasing,
		},
		// create as normal table, warning.
		//	{
		//		"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
		//			"partition p0 values less than (1, 'b')," +
		//			"partition p1 values less than (1, 'a'))",
		//		dbterror.ErrRangeNotIncreasing,
		//	},
		{
			"create table t (a int, b varchar(64)) partition by range columns (b) (" +
				"partition p0 values less than ('b')," +
				"partition p1 values less than ('a'))",
			dbterror.ErrRangeNotIncreasing,
		},
		// create as normal table, warning.
		//		{
		//			"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
		//				"partition p0 values less than (1, maxvalue)," +
		//				"partition p1 values less than (1, 'a'))",
		//			dbterror.ErrRangeNotIncreasing,
		//		},
		{
			"create table t (a int, b varchar(64)) partition by range columns ( b) (" +
				"partition p0 values less than (  maxvalue)," +
				"partition p1 values less than ('a'))",
			dbterror.ErrRangeNotIncreasing,
		},
		{
			"create table t (col datetime not null default '2000-01-01')" +
				"partition by range columns (col) (" +
				"PARTITION p0 VALUES LESS THAN (20190905)," +
				"PARTITION p1 VALUES LESS THAN (20190906));",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t(a char(10) collate utf8mb4_bin) " +
				"partition by range columns (a) (" +
				"partition p0 values less than ('a'), " +
				"partition p1 values less than ('G'));",
			dbterror.ErrRangeNotIncreasing,
		},
		{
			"create table t(a char(10) collate utf8mb4_bin) " +
				"partition by range columns (a) (" +
				"partition p0 values less than ('g'), " +
				"partition p1 values less than ('A'));",
			dbterror.ErrRangeNotIncreasing,
		},
		{
			"CREATE TABLE t1(c0 INT) PARTITION BY HASH((NOT c0)) PARTITIONS 2;",
			dbterror.ErrPartitionFunctionIsNotAllowed,
		},
		{
			"CREATE TABLE t1(c0 INT) PARTITION BY HASH((!c0)) PARTITIONS 2;",
			dbterror.ErrPartitionFunctionIsNotAllowed,
		},
		{
			"CREATE TABLE t1(c0 INT) PARTITION BY LIST((NOT c0)) (partition p0 values in (0), partition p1 values in (1));",
			dbterror.ErrPartitionFunctionIsNotAllowed,
		},
		{
			"CREATE TABLE t1(c0 INT) PARTITION BY LIST((!c0)) (partition p0 values in (0), partition p1 values in (1));",
			dbterror.ErrPartitionFunctionIsNotAllowed,
		},
		{
			"CREATE TABLE t1 (a TIME, b DATE) PARTITION BY range(DATEDIFF(a, b)) (partition p1 values less than (20));",
			dbterror.ErrWrongExprInPartitionFunc,
		},
		{
			"CREATE TABLE t1 (a DATE, b VARCHAR(10)) PARTITION BY range(DATEDIFF(a, b)) (partition p1 values less than (20));",
			dbterror.ErrWrongExprInPartitionFunc,
		},
		{
			"create table t1 (a bigint unsigned) partition by list (a) (partition p0 values in (10, 20, 30, -1));",
			dbterror.ErrPartitionConstDomain,
		},
		{
			"create table t1 (a bigint unsigned) partition by range (a) (partition p0 values less than (-1));",
			dbterror.ErrPartitionConstDomain,
		},
		{
			"create table t1 (a int unsigned) partition by range (a) (partition p0 values less than (-1));",
			dbterror.ErrPartitionConstDomain,
		},
		{
			"create table t1 (a tinyint(20) unsigned) partition by range (a) (partition p0 values less than (-1));",
			dbterror.ErrPartitionConstDomain,
		},
		{
			"CREATE TABLE new (a TIMESTAMP NOT NULL PRIMARY KEY) PARTITION BY RANGE (a % 2) (PARTITION p VALUES LESS THAN (20080819));",
			dbterror.ErrWrongExprInPartitionFunc,
		},
		{
			"CREATE TABLE new (a TIMESTAMP NOT NULL PRIMARY KEY) PARTITION BY RANGE (a+2) (PARTITION p VALUES LESS THAN (20080819));",
			dbterror.ErrWrongExprInPartitionFunc,
		},
	}
	for i, tt := range cases {
		_, err := tk.Exec(tt.sql)
		require.Truef(t, tt.err.Equal(err),
			"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, tt.sql, tt.err, err,
		)
	}

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b char(3)) partition by range columns (a, b) (" +
		"partition p0 values less than (1, 'a')," +
		"partition p1 values less than (2, maxvalue))")

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int, b char(3)) partition by range columns (b) (" +
		"partition p0 values less than ( 'a')," +
		"partition p1 values less than (maxvalue))")

	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t(a char(10) collate utf8mb4_unicode_ci) partition by range columns (a) (
    	partition p0 values less than ('a'),
    	partition p1 values less than ('G'));`)

	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (a varchar(255) charset utf8mb4 collate utf8mb4_bin) ` +
		`partition by range columns (a) ` +
		`(partition pnull values less than (""),` +
		`partition puppera values less than ("AAA"),` +
		`partition plowera values less than ("aaa"),` +
		`partition pmax values less than (MAXVALUE))`)

	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t(a int) partition by range columns (a) (
    	partition p0 values less than (10),
    	partition p1 values less than (20));`)

	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t(a int) partition by range (a) (partition p0 values less than (18446744073709551615));`)

	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t(a binary) partition by range columns (a) (partition p0 values less than (X'0C'));`)
	tk.MustExec(`alter table t add partition (partition p1 values less than (X'0D'), partition p2 values less than (X'0E'));`)
	tk.MustExec(`insert into t values (X'0B'), (X'0C'), (X'0D')`)
	tk.MustQuery(`select * from t where a < X'0D' order by a`).Check(testkit.Rows("\x0B", "\x0C"))
}

func TestPartitionRangeColumnsCollate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create schema PartitionRangeColumnsCollate")
	tk.MustExec("use PartitionRangeColumnsCollate")
	tk.MustExec(`create table t (a varchar(255) charset utf8mb4 collate utf8mb4_bin) partition by range columns (a)
 (partition p0A values less than ("A"),
 partition p1AA values less than ("AA"),
 partition p2Aa values less than ("Aa"),
 partition p3BB values less than ("BB"),
 partition p4Bb values less than ("Bb"),
 partition p5aA values less than ("aA"),
 partition p6aa values less than ("aa"),
 partition p7bB values less than ("bB"),
 partition p8bb values less than ("bb"),
 partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values ("A"),("a"),("b"),("B"),("aa"),("AA"),("aA"),("Aa"),("BB"),("Bb"),("bB"),("bb"),("AB"),("BA"),("Ab"),("Ba"),("aB"),("bA"),("ab"),("ba")`)
	tk.MustQuery(`explain select * from t where a = "AA" collate utf8mb4_general_ci`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  eq(partitionrangecolumnscollate.t.a, "AA")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a = "AA" collate utf8mb4_general_ci`).Sort().Check(testkit.Rows("AA", "Aa", "aA", "aa"))
	tk.MustQuery(`explain select * from t where a = "aa" collate utf8mb4_general_ci`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  eq(partitionrangecolumnscollate.t.a, "aa")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a = "aa" collate utf8mb4_general_ci`).Sort().Check(testkit.Rows("AA", "Aa", "aA", "aa"))
	tk.MustQuery(`explain select * from t where a >= "aa" collate utf8mb4_general_ci`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  ge(partitionrangecolumnscollate.t.a, "aa")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a >= "aa" collate utf8mb4_general_ci`).Sort().Check(testkit.Rows(
		"AA", "AB", "Aa", "Ab", "B", "BA", "BB", "Ba", "Bb", "aA", "aB", "aa", "ab", "b", "bA", "bB", "ba", "bb"))
	tk.MustQuery(`explain select * from t where a > "aa" collate utf8mb4_general_ci`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  gt(partitionrangecolumnscollate.t.a, "aa")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a > "aa" collate utf8mb4_general_ci`).Sort().Check(testkit.Rows(
		"AB", "Ab", "B", "BA", "BB", "Ba", "Bb", "aB", "ab", "b", "bA", "bB", "ba", "bb"))
	tk.MustQuery(`explain select * from t where a <= "aa" collate utf8mb4_general_ci`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  le(partitionrangecolumnscollate.t.a, "aa")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a <= "aa" collate utf8mb4_general_ci`).Sort().Check(testkit.Rows(
		"A", "AA", "Aa", "a", "aA", "aa"))
	tk.MustQuery(`explain select * from t where a < "aa" collate utf8mb4_general_ci`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  lt(partitionrangecolumnscollate.t.a, "aa")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a < "aa" collate utf8mb4_general_ci`).Sort().Check(testkit.Rows(
		"A", "a"))

	tk.MustExec("drop table t")
	tk.MustExec(` create table t (a varchar(255) charset utf8mb4 collate utf8mb4_general_ci) partition by range columns (a)
(partition p0 values less than ("A"),
 partition p1 values less than ("aa"),
 partition p2 values less than ("AAA"),
 partition p3 values less than ("aaaa"),
 partition p4 values less than ("B"),
 partition p5 values less than ("bb"),
 partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values ("A"),("a"),("b"),("B"),("aa"),("AA"),("aA"),("Aa"),("BB"),("Bb"),("bB"),("bb"),("AB"),("BA"),("Ab"),("Ba"),("aB"),("bA"),("ab"),("ba"),("ä"),("ÄÄÄ")`)
	tk.MustQuery(`explain select * from t where a = "aa" collate utf8mb4_general_ci`).Check(testkit.Rows(
		`TableReader_7 10.00 root partition:p2 data:Selection_6`,
		`└─Selection_6 10.00 cop[tikv]  eq(partitionrangecolumnscollate.t.a, "aa")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a = "aa" collate utf8mb4_general_ci`).Sort().Check(testkit.Rows(
		"AA", "Aa", "aA", "aa"))
	tk.MustQuery(`explain select * from t where a = "aa" collate utf8mb4_bin`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:p2 data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  eq(partitionrangecolumnscollate.t.a, "aa")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a = "aa" collate utf8mb4_bin`).Sort().Check(testkit.Rows("aa"))
	// 'a' < 'b' < 'ä' in _bin
	tk.MustQuery(`explain select * from t where a = "ä" collate utf8mb4_bin`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:p1 data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  eq(partitionrangecolumnscollate.t.a, "ä")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a = "ä" collate utf8mb4_bin`).Sort().Check(testkit.Rows("ä"))
	tk.MustQuery(`explain select * from t where a = "b" collate utf8mb4_bin`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:p5 data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  eq(partitionrangecolumnscollate.t.a, "b")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a = "b" collate utf8mb4_bin`).Sort().Check(testkit.Rows("b"))
	tk.MustQuery(`explain select * from t where a <= "b" collate utf8mb4_bin`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  le(partitionrangecolumnscollate.t.a, "b")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a <= "b" collate utf8mb4_bin`).Sort().Check(testkit.Rows("A", "AA", "AB", "Aa", "Ab", "B", "BA", "BB", "Ba", "Bb", "a", "aA", "aB", "aa", "ab", "b"))
	tk.MustQuery(`explain select * from t where a < "b" collate utf8mb4_bin`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  lt(partitionrangecolumnscollate.t.a, "b")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	// Missing upper case B if not p5 is included!
	tk.MustQuery(`select * from t where a < "b" collate utf8mb4_bin`).Sort().Check(testkit.Rows("A", "AA", "AB", "Aa", "Ab", "B", "BA", "BB", "Ba", "Bb", "a", "aA", "aB", "aa", "ab"))
	tk.MustQuery(`explain select * from t where a >= "b" collate utf8mb4_bin`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  ge(partitionrangecolumnscollate.t.a, "b")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a >= "b" collate utf8mb4_bin`).Sort().Check(testkit.Rows("b", "bA", "bB", "ba", "bb", "ÄÄÄ", "ä"))
	tk.MustQuery(`explain select * from t where a > "b" collate utf8mb4_bin`).Check(testkit.Rows(
		`TableReader_7 8000.00 root partition:all data:Selection_6`,
		`└─Selection_6 8000.00 cop[tikv]  gt(partitionrangecolumnscollate.t.a, "b")`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from t where a > "b" collate utf8mb4_bin`).Sort().Check(testkit.Rows("bA", "bB", "ba", "bb", "ÄÄÄ", "ä"))
}

func TestDisableTablePartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	for _, v := range []string{"'AUTO'", "'OFF'", "0", "'ON'"} {
		tk.MustExec("set @@session.tidb_enable_table_partition = " + v)
		tk.MustExec("set @@session.tidb_enable_list_partition = OFF")
		tk.MustExec("drop table if exists t")
		tk.MustExec(`create table t (id int) partition by list  (id) (
	    partition p0 values in (1,2),partition p1 values in (3,4));`)
		tbl := external.GetTableByName(t, tk, "test", "t")
		require.Nil(t, tbl.Meta().Partition)
		_, err := tk.Exec(`alter table t add partition (
		partition p4 values in (7),
		partition p5 values in (8,9));`)
		require.True(t, dbterror.ErrPartitionMgmtOnNonpartitioned.Equal(err))
		tk.MustExec("insert into t values (1),(3),(5),(100),(null)")
	}
}

func generatePartitionTableByNum(num int) string {
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024))
	buf.WriteString("create table gen_t (id int) partition by list  (id) (")
	for i := 0; i < num; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("partition p%v values in (%v)", i, i))
	}
	buf.WriteString(")")
	return buf.String()
}

func TestCreateTableWithListPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	type errorCase struct {
		sql string
		err *terror.Error
	}
	cases := []errorCase{
		{
			"create table t (id int) partition by list (id);",
			ast.ErrPartitionsMustBeDefined,
		},
		{
			"create table t (a int) partition by list (b) (partition p0 values in (1));",
			dbterror.ErrBadField,
		},
		{
			"create table t (id timestamp) partition by list (id) (partition p0 values in ('2019-01-09 11:23:34'));",
			dbterror.ErrValuesIsNotIntType,
		},
		{
			"create table t (id decimal) partition by list (id) (partition p0 values in ('2019-01-09 11:23:34'));",
			dbterror.ErrValuesIsNotIntType,
		},
		{
			"create table t (id float) partition by list (id) (partition p0 values in (1));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id double) partition by list (id) (partition p0 values in (1));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id text) partition by list (id) (partition p0 values in ('abc'));",
			dbterror.ErrValuesIsNotIntType,
		},
		{
			"create table t (id blob) partition by list (id) (partition p0 values in ('abc'));",
			dbterror.ErrValuesIsNotIntType,
		},
		{
			"create table t (id enum('a','b')) partition by list (id) (partition p0 values in ('a'));",
			dbterror.ErrValuesIsNotIntType,
		},
		{
			"create table t (id set('a','b')) partition by list (id) (partition p0 values in ('a'));",
			dbterror.ErrValuesIsNotIntType,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (1), partition p0 values in (2));",
			dbterror.ErrSameNamePartition,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (1), partition P0 values in (2));",
			dbterror.ErrSameNamePartition,
		},
		{
			"create table t (id bigint) partition by list (cast(id as unsigned)) (partition p0 values in (1))",
			dbterror.ErrPartitionFunctionIsNotAllowed,
		},
		{
			"create table t (id float) partition by list (ceiling(id)) (partition p0 values in (1))",
			dbterror.ErrPartitionFuncNotAllowed,
		},
		{
			"create table t(b char(10)) partition by range columns (b) (partition p1 values less than ('G' collate utf8mb4_unicode_ci));",
			dbterror.ErrPartitionFunctionIsNotAllowed,
		},
		{
			"create table t (a date) partition by list (to_days(to_days(a))) (partition p0 values in (1), partition P1 values in (2));",
			dbterror.ErrWrongExprInPartitionFunc,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (1), partition p1 values in (1));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (1), partition p1 values in (+1));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (null), partition p1 values in (NULL));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			`create table t1 (id int key, name varchar(10), unique index idx(name)) partition by list  (id) (
				    partition p0 values in (3,5,6,9,17),
				    partition p1 values in (1,2,10,11,19,20),
				    partition p2 values in (4,12,13,14,18),
				    partition p3 values in (7,8,15,16)
				);`,
			dbterror.ErrUniqueKeyNeedAllFieldsInPf,
		},
		{
			generatePartitionTableByNum(ddl.PartitionCountLimit + 1),
			dbterror.ErrTooManyPartitions,
		},
	}
	for i, tt := range cases {
		_, err := tk.Exec(tt.sql)
		require.Truef(t, tt.err.Equal(err),
			"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, tt.sql, tt.err, err,
		)
	}

	validCases := []string{
		"create table t (a int) partition by list (a) (partition p0 values in (1));",
		"create table t (a bigint unsigned) partition by list (a) (partition p0 values in (18446744073709551615));",
		"create table t (a bigint unsigned) partition by list (a) (partition p0 values in (18446744073709551615 - 1));",
		"create table t (a int) partition by list (a) (partition p0 values in (1,null));",
		"create table t (a int) partition by list (a) (partition p0 values in (1), partition p1 values in (2));",
		`create table t (id int, name varchar(10), age int) partition by list (id) (
			partition p0 values in (3,5,6,9,17),
			partition p1 values in (1,2,10,11,19,20),
			partition p2 values in (4,12,13,-14,18),
			partition p3 values in (7,8,15,+16)
		);`,
		"create table t (id year) partition by list (id) (partition p0 values in (2000));",
		"create table t (a tinyint) partition by list (a) (partition p0 values in (65536));",
		"create table t (a tinyint) partition by list (a*100) (partition p0 values in (65536));",
		"create table t (a bigint) partition by list (a) (partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')));",
		"create table t (a datetime) partition by list (to_seconds(a)) (partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')));",
		"create table t (a int, b int generated always as (a+1) virtual) partition by list (b + 1) (partition p0 values in (1));",
		"create table t(a binary) partition by list columns (a) (partition p0 values in (X'0C'));",
		generatePartitionTableByNum(ddl.PartitionCountLimit),
	}

	for id, sql := range validCases {
		tk.MustExec("drop table if exists t")
		tk.MustExec(sql)
		tblName := "t"
		if id == len(validCases)-1 {
			tblName = "gen_t"
		}
		tbl := external.GetTableByName(t, tk, "test", tblName)
		tblInfo := tbl.Meta()
		require.NotNil(t, tblInfo.Partition)
		require.True(t, tblInfo.Partition.Enable)
		require.Equal(t, model.PartitionTypeList, tblInfo.Partition.Type)
	}
}

func TestCreateTableWithListColumnsPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	type errorCase struct {
		sql string
		err *terror.Error
	}
	cases := []errorCase{
		{
			"create table t (id int) partition by list columns (id);",
			ast.ErrPartitionsMustBeDefined,
		},
		{
			"create table t (a int) partition by list columns (b) (partition p0 values in (1));",
			dbterror.ErrFieldNotFoundPart,
		},
		{
			"create table t (id timestamp) partition by list columns (id) (partition p0 values in ('2019-01-09 11:23:34'));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id decimal) partition by list columns (id) (partition p0 values in ('2019-01-09 11:23:34'));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id year) partition by list columns (id) (partition p0 values in (2000));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id float) partition by list columns (id) (partition p0 values in (1));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id double) partition by list columns (id) (partition p0 values in (1));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id text) partition by list columns (id) (partition p0 values in ('abc'));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id blob) partition by list columns (id) (partition p0 values in ('abc'));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id enum('a','b')) partition by list columns (id) (partition p0 values in ('a'));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (id set('a','b')) partition by list columns (id) (partition p0 values in ('a'));",
			dbterror.ErrNotAllowedTypeInPartition,
		},
		{
			"create table t (a varchar(2)) partition by list columns (a) (partition p0 values in ('abc'));",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a tinyint) partition by list columns (a) (partition p0 values in (65536));",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a bigint) partition by list columns (a) (partition p0 values in (18446744073709551615));",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a bigint unsigned) partition by list columns (a) (partition p0 values in (-1));",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a char) partition by list columns (a) (partition p0 values in ('abc'));",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a datetime) partition by list columns (a) (partition p0 values in ('2020-11-31 12:00:00'));",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition p0 values in (2));",
			dbterror.ErrSameNamePartition,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition P0 values in (2));",
			dbterror.ErrSameNamePartition,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition p1 values in (1));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition p1 values in (+1));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a tinyint) partition by list columns (a) (partition p0 values in (1), partition p1 values in (+1));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a mediumint) partition by list columns (a) (partition p0 values in (1), partition p1 values in (+1));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a bigint) partition by list columns (a) (partition p0 values in (1), partition p1 values in (+1));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a bigint) partition by list columns (a) (partition p0 values in (1,+1))",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (null), partition p1 values in (NULL));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a bigint, b int) partition by list columns (a,b) (partition p0 values in ((1,2),(1,2)))",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a bigint, b int) partition by list columns (a,b) (partition p0 values in ((1,1),(2,2)), partition p1 values in ((+1,1)));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t1 (a int, b int) partition by list columns(a,a) ( partition p values in ((1,1)));",
			dbterror.ErrSameNamePartitionField,
		},
		{
			"create table t1 (a int, b int) partition by list columns(a,b,b) ( partition p values in ((1,1,1)));",
			dbterror.ErrSameNamePartitionField,
		},
		{
			`create table t1 (id int key, name varchar(10), unique index idx(name)) partition by list columns (id) (
				    partition p0 values in (3,5,6,9,17),
				    partition p1 values in (1,2,10,11,19,20),
				    partition p2 values in (4,12,13,14,18),
				    partition p3 values in (7,8,15,16)
				);`,
			dbterror.ErrUniqueKeyNeedAllFieldsInPf,
		},
		{
			"create table t (a date) partition by list columns (a) (partition p0 values in ('2020-02-02'), partition p1 values in ('20200202'));",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int, b varchar(10)) partition by list columns (a,b) (partition p0 values in (1));",
			ast.ErrPartitionColumnList,
		},
		{
			"create table t (a int, b varchar(10)) partition by list columns (a,b) (partition p0 values in (('ab','ab')));",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a int, b datetime) partition by list columns (a,b) (partition p0 values in ((1)));",
			ast.ErrPartitionColumnList,
		},
		{
			"create table t(b int) partition by hash ( b ) partitions 3 (partition p1, partition p2, partition p2);",
			dbterror.ErrSameNamePartition,
		},
	}
	for i, tt := range cases {
		_, err := tk.Exec(tt.sql)
		require.Truef(t, tt.err.Equal(err),
			"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, tt.sql, tt.err, err,
		)
	}

	validCases := []string{
		"create table t (a int) partition by list columns (a) (partition p0 values in (1));",
		"create table t (a bigint unsigned) partition by list columns (a) (partition p0 values in (18446744073709551615));",
		"create table t (a bigint unsigned) partition by list columns (a) (partition p0 values in (18446744073709551615 - 1));",
		"create table t (a int) partition by list columns (a) (partition p0 values in (1,null));",
		"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition p1 values in (2));",
		`create table t (id int, name varchar(10), age int) partition by list columns (id) (
			partition p0 values in (3,5,6,9,17),
			partition p1 values in (1,2,10,11,19,20),
			partition p2 values in (4,12,13,-14,18),
			partition p3 values in (7,8,15,+16)
		);`,
		"create table t (a datetime) partition by list columns (a) (partition p0 values in ('2020-09-28 17:03:38','2020-09-28 17:03:39'));",
		"create table t (a date) partition by list columns (a) (partition p0 values in ('2020-09-28','2020-09-29'));",
		"create table t (a bigint, b date) partition by list columns (a,b) (partition p0 values in ((1,'2020-09-28'),(1,'2020-09-29')));",
		"create table t (a bigint)   partition by list columns (a) (partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')));",
		"create table t (a varchar(10)) partition by list columns (a) (partition p0 values in ('abc'));",
		"create table t (a char) partition by list columns (a) (partition p0 values in ('a'));",
		"create table t (a bool) partition by list columns (a) (partition p0 values in (1));",
		"create table t (c1 bool, c2 tinyint, c3 int, c4 bigint, c5 datetime, c6 date,c7 varchar(10), c8 char) " +
			"partition by list columns (c1,c2,c3,c4,c5,c6,c7,c8) (" +
			"partition p0 values in ((1,2,3,4,'2020-11-30 00:00:01', '2020-11-30','abc','a')));",
		"create table t (a int, b int generated always as (a+1) virtual) partition by list columns (b) (partition p0 values in (1));",
		"create table t(a int,b char(10)) partition by list columns (a, b) (partition p1 values in ((2, 'a'), (1, 'b')), partition p2 values in ((2, 'b')));",
	}

	for _, sql := range validCases {
		tk.MustExec("drop table if exists t")
		tk.MustExec(sql)
		tbl := external.GetTableByName(t, tk, "test", "t")
		tblInfo := tbl.Meta()
		require.NotNil(t, tblInfo.Partition)
		require.Equal(t, true, tblInfo.Partition.Enable)
		require.True(t, tblInfo.Partition.Type == model.PartitionTypeList)
	}
}

func TestAlterTableAddPartitionByList(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`create table t (id int) partition by list  (id) (
	    partition p0 values in (1,2),
	    partition p1 values in (3,4),
	    partition p3 values in (5,null)
	);`)
	tk.MustExec(`alter table t add partition (
		partition p4 values in (7),
		partition p5 values in (8,9));`)

	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().Partition)
	part := tbl.Meta().Partition
	require.True(t, part.Type == model.PartitionTypeList)

	require.Equal(t, "`id`", part.Expr)
	require.Len(t, part.Definitions, 5)
	require.Equal(t, [][]string{{"1"}, {"2"}}, part.Definitions[0].InValues)
	require.Equal(t, model.NewCIStr("p0"), part.Definitions[0].Name)
	require.Equal(t, [][]string{{"3"}, {"4"}}, part.Definitions[1].InValues)
	require.Equal(t, model.NewCIStr("p1"), part.Definitions[1].Name)
	require.Equal(t, [][]string{{"5"}, {"NULL"}}, part.Definitions[2].InValues)
	require.Equal(t, model.NewCIStr("p3"), part.Definitions[2].Name)
	require.Equal(t, [][]string{{"7"}}, part.Definitions[3].InValues)
	require.Equal(t, model.NewCIStr("p4"), part.Definitions[3].Name)
	require.Equal(t, [][]string{{"8"}, {"9"}}, part.Definitions[4].InValues)
	require.Equal(t, model.NewCIStr("p5"), part.Definitions[4].Name)

	errorCases := []struct {
		sql string
		err *terror.Error
	}{
		{"alter table t add partition (partition p4 values in (7))",
			dbterror.ErrSameNamePartition,
		},
		{"alter table t add partition (partition p6 values less than (7))",
			ast.ErrPartitionWrongValues,
		},
		{"alter table t add partition (partition p6 values in (null))",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{"alter table t add partition (partition p6 values in (7))",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{"alter table t add partition (partition p6 values in ('a'))",
			dbterror.ErrValuesIsNotIntType,
		},
		{"alter table t add partition (partition p5 values in (10),partition p6 values in (7))",
			dbterror.ErrSameNamePartition,
		},
	}

	for i, tt := range errorCases {
		_, err := tk.Exec(tt.sql)
		require.Truef(t, tt.err.Equal(err),
			"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, tt.sql, tt.err, err,
		)
	}

	errorCases2 := []struct {
		create string
		alter  string
		err    *terror.Error
	}{
		{
			"create table t (a bigint unsigned) partition by list columns (a) (partition p0 values in (1));",
			"alter table t add partition (partition p1 values in (-1))",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a varchar(2)) partition by list columns (a) (partition p0 values in ('a','b'));",
			"alter table t add partition (partition p1 values in ('abc'))",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a tinyint) partition by list columns (a) (partition p0 values in (1,2,3));",
			"alter table t add partition (partition p1 values in (65536))",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a bigint) partition by list columns (a) (partition p0 values in (1,2,3));",
			"alter table t add partition (partition p1 values in (18446744073709551615))",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a char) partition by list columns (a) (partition p0 values in ('a','b'));",
			"alter table t add partition (partition p1 values in ('abc'))",
			dbterror.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a datetime) partition by list columns (a) (partition p0 values in ('2020-11-30 12:00:00'));",
			"alter table t add partition (partition p1 values in ('2020-11-31 12:00:00'))",
			dbterror.ErrWrongTypeColumnValue,
		},
	}

	for i, tt := range errorCases2 {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(tt.create)
		_, err := tk.Exec(tt.alter)
		require.Truef(t, tt.err.Equal(err),
			"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, tt.alter, tt.err, err,
		)
	}
}

func TestAlterTableAddPartitionByListColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`create table t (id int, name varchar(10)) partition by list columns (id,name) (
	    partition p0 values in ((1,'a'),(2,'b')),
	    partition p1 values in ((3,'a'),(4,'b')),
	    partition p3 values in ((5,null))
	);`)
	tk.MustExec(`alter table t add partition (
		partition p4 values in ((7,'a')),
		partition p5 values in ((8,'a')));`)

	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().Partition)
	part := tbl.Meta().Partition
	require.True(t, part.Type == model.PartitionTypeList)

	require.Equal(t, "", part.Expr)
	require.Equal(t, "id", part.Columns[0].O)
	require.Equal(t, "name", part.Columns[1].O)
	require.Len(t, part.Definitions, 5)
	require.Equal(t, [][]string{{"1", `"a"`}, {"2", `"b"`}}, part.Definitions[0].InValues)
	require.Equal(t, model.NewCIStr("p0"), part.Definitions[0].Name)
	require.Equal(t, [][]string{{"3", `"a"`}, {"4", `"b"`}}, part.Definitions[1].InValues)
	require.Equal(t, model.NewCIStr("p1"), part.Definitions[1].Name)
	require.Equal(t, [][]string{{"5", `NULL`}}, part.Definitions[2].InValues)
	require.Equal(t, model.NewCIStr("p3"), part.Definitions[2].Name)
	require.Equal(t, [][]string{{"7", `"a"`}}, part.Definitions[3].InValues)
	require.Equal(t, model.NewCIStr("p4"), part.Definitions[3].Name)
	require.Equal(t, [][]string{{"8", `"a"`}}, part.Definitions[4].InValues)
	require.Equal(t, model.NewCIStr("p5"), part.Definitions[4].Name)

	errorCases := []struct {
		sql string
		err *terror.Error
	}{
		{"alter table t add partition (partition p4 values in ((7,'b')))",
			dbterror.ErrSameNamePartition,
		},
		{"alter table t add partition (partition p6 values less than ((7,'a')))",
			ast.ErrPartitionWrongValues,
		},
		{"alter table t add partition (partition p6 values in ((5,null)))",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{"alter table t add partition (partition p6 values in ((7,'a')))",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{"alter table t add partition (partition p6 values in (('a','a')))",
			dbterror.ErrWrongTypeColumnValue,
		},
	}

	for i, tt := range errorCases {
		_, err := tk.Exec(tt.sql)
		require.Truef(t, tt.err.Equal(err),
			"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, tt.sql, tt.err, err,
		)
	}
}

func TestAlterTableDropPartitionByList(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`create table t (id int) partition by list  (id) (
	    partition p0 values in (1,2),
	    partition p1 values in (3,4),
	    partition p3 values in (5,null)
	);`)
	tk.MustExec(`insert into t values (1),(3),(5),(null)`)
	tk.MustExec(`alter table t drop partition p1`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "5", "<nil>"))
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().Partition)
	part := tbl.Meta().Partition
	require.True(t, part.Type == model.PartitionTypeList)
	require.Equal(t, "`id`", part.Expr)
	require.Len(t, part.Definitions, 2)
	require.Equal(t, [][]string{{"1"}, {"2"}}, part.Definitions[0].InValues)
	require.Equal(t, model.NewCIStr("p0"), part.Definitions[0].Name)
	require.Equal(t, [][]string{{"5"}, {"NULL"}}, part.Definitions[1].InValues)
	require.Equal(t, model.NewCIStr("p3"), part.Definitions[1].Name)

	sql := "alter table t drop partition p10;"
	tk.MustGetErrCode(sql, tmysql.ErrDropPartitionNonExistent)
	tk.MustExec(`alter table t drop partition p3`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	sql = "alter table t drop partition p0;"
	tk.MustGetErrCode(sql, tmysql.ErrDropLastPartition)
}

func TestAlterTableDropPartitionByListColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`create table t (id int, name varchar(10)) partition by list columns (id,name) (
	    partition p0 values in ((1,'a'),(2,'b')),
	    partition p1 values in ((3,'a'),(4,'b')),
	    partition p3 values in ((5,'a'),(null,null))
	);`)
	tk.MustExec(`insert into t values (1,'a'),(3,'a'),(5,'a'),(null,null)`)
	tk.MustExec(`alter table t drop partition p1`)
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 a", "5 a", "<nil> <nil>"))
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().Partition)
	part := tbl.Meta().Partition
	require.True(t, part.Type == model.PartitionTypeList)
	require.Equal(t, "", part.Expr)
	require.Equal(t, "id", part.Columns[0].O)
	require.Equal(t, "name", part.Columns[1].O)
	require.Len(t, part.Definitions, 2)
	require.Equal(t, [][]string{{"1", `"a"`}, {"2", `"b"`}}, part.Definitions[0].InValues)
	require.Equal(t, model.NewCIStr("p0"), part.Definitions[0].Name)
	require.Equal(t, [][]string{{"5", `"a"`}, {"NULL", "NULL"}}, part.Definitions[1].InValues)
	require.Equal(t, model.NewCIStr("p3"), part.Definitions[1].Name)

	sql := "alter table t drop partition p10;"
	tk.MustGetErrCode(sql, tmysql.ErrDropPartitionNonExistent)
	tk.MustExec(`alter table t drop partition p3`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 a"))
	sql = "alter table t drop partition p0;"
	tk.MustGetErrCode(sql, tmysql.ErrDropLastPartition)
}

func TestAlterTableTruncatePartitionByList(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`create table t (id int) partition by list  (id) (
	    partition p0 values in (1,2),
	    partition p1 values in (3,4),
	    partition p3 values in (5,null)
	);`)
	tk.MustExec(`insert into t values (1),(3),(5),(null)`)
	oldTbl := external.GetTableByName(t, tk, "test", "t")
	tk.MustExec(`alter table t truncate partition p1`)
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1", "5", "<nil>"))
	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl.Meta().Partition)
	part := tbl.Meta().Partition
	require.True(t, part.Type == model.PartitionTypeList)
	require.Len(t, part.Definitions, 3)
	require.Equal(t, [][]string{{"3"}, {"4"}}, part.Definitions[1].InValues)
	require.Equal(t, model.NewCIStr("p1"), part.Definitions[1].Name)
	require.False(t, part.Definitions[1].ID == oldTbl.Meta().Partition.Definitions[1].ID)

	sql := "alter table t truncate partition p10;"
	tk.MustGetErrCode(sql, tmysql.ErrUnknownPartition)
	tk.MustExec(`alter table t truncate partition p3`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec(`alter table t truncate partition p0`)
	tk.MustQuery("select * from t").Check(testkit.Rows())
}

func TestAlterTableTruncatePartitionByListColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`create table t (id int, name varchar(10)) partition by list columns (id,name) (
	    partition p0 values in ((1,'a'),(2,'b')),
	    partition p1 values in ((3,'a'),(4,'b')),
	    partition p3 values in ((5,'a'),(null,null))
	);`)
	tk.MustExec(`insert into t values (1,'a'),(3,'a'),(5,'a'),(null,null)`)
	oldTbl := external.GetTableByName(t, tk, "test", "t")
	tk.MustExec(`alter table t truncate partition p1`)
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 a", "5 a", "<nil> <nil>"))
	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl.Meta().Partition)
	part := tbl.Meta().Partition
	require.True(t, part.Type == model.PartitionTypeList)
	require.Len(t, part.Definitions, 3)
	require.Equal(t, [][]string{{"3", `"a"`}, {"4", `"b"`}}, part.Definitions[1].InValues)
	require.Equal(t, model.NewCIStr("p1"), part.Definitions[1].Name)
	require.False(t, part.Definitions[1].ID == oldTbl.Meta().Partition.Definitions[1].ID)

	sql := "alter table t truncate partition p10;"
	tk.MustGetErrCode(sql, tmysql.ErrUnknownPartition)
	tk.MustExec(`alter table t truncate partition p3`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 a"))
	tk.MustExec(`alter table t truncate partition p0`)
	tk.MustQuery("select * from t").Check(testkit.Rows())
}

func TestCreateTableWithKeyPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestAlterTableAddPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("employees"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().Partition)
	part := tbl.Meta().Partition
	require.Equal(t, model.PartitionTypeRange, part.Type)

	require.Equal(t, "YEAR(`hired`)", part.Expr)
	require.Len(t, part.Definitions, 5)
	require.Equal(t, "1991", part.Definitions[0].LessThan[0])
	require.Equal(t, model.NewCIStr("p1"), part.Definitions[0].Name)
	require.Equal(t, "1996", part.Definitions[1].LessThan[0])
	require.Equal(t, model.NewCIStr("p2"), part.Definitions[1].Name)
	require.Equal(t, "2001", part.Definitions[2].LessThan[0])
	require.Equal(t, model.NewCIStr("p3"), part.Definitions[2].Name)
	require.Equal(t, "2010", part.Definitions[3].LessThan[0])
	require.Equal(t, model.NewCIStr("p4"), part.Definitions[3].Name)
	require.Equal(t, "MAXVALUE", part.Definitions[4].LessThan[0])
	require.Equal(t, model.NewCIStr("p5"), part.Definitions[4].Name)

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

func TestAlterTableDropPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("employees"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().GetPartitionInfo())
	part := tbl.Meta().Partition
	require.Equal(t, model.PartitionTypeRange, part.Type)
	require.Equal(t, "`hired`", part.Expr)
	require.Len(t, part.Definitions, 2)
	require.Equal(t, "1991", part.Definitions[0].LessThan[0])
	require.Equal(t, model.NewCIStr("p1"), part.Definitions[0].Name)
	require.Equal(t, "1996", part.Definitions[1].LessThan[0])
	require.Equal(t, model.NewCIStr("p2"), part.Definitions[1].Name)

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
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().GetPartitionInfo())
	part = tbl.Meta().Partition
	require.Equal(t, model.PartitionTypeRange, part.Type)
	require.Equal(t, "`id`", part.Expr)
	require.Len(t, part.Definitions, 2)
	require.Equal(t, "10", part.Definitions[0].LessThan[0])
	require.Equal(t, model.NewCIStr("p1"), part.Definitions[0].Name)
	require.Equal(t, "MAXVALUE", part.Definitions[1].LessThan[0])
	require.Equal(t, model.NewCIStr("p3"), part.Definitions[1].Name)

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

func TestMultiPartitionDropAndTruncate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestDropPartitionWithGlobalIndex(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20)
	);`)
	tt := external.GetTableByName(t, tk, "test", "test_global")
	pid := tt.Meta().Partition.Definitions[1].ID

	tk.MustExec("Alter Table test_global Add Unique Index idx_b (b);")
	tk.MustExec("Alter Table test_global Add Unique Index idx_c (c);")
	tk.MustExec(`INSERT INTO test_global VALUES (1, 1, 1), (2, 2, 2), (11, 3, 3), (12, 4, 4)`)

	tk.MustExec("alter table test_global drop partition p2;")
	result := tk.MustQuery("select * from test_global;")
	result.Sort().Check(testkit.Rows(`1 1 1`, `2 2 2`))

	tt = external.GetTableByName(t, tk, "test", "test_global")
	idxInfo := tt.Meta().FindIndexByName("idx_b")
	require.NotNil(t, idxInfo)
	cnt := checkGlobalIndexCleanUpDone(t, tk.Session(), tt.Meta(), idxInfo, pid)
	require.Equal(t, 2, cnt)

	idxInfo = tt.Meta().FindIndexByName("idx_c")
	require.NotNil(t, idxInfo)
	cnt = checkGlobalIndexCleanUpDone(t, tk.Session(), tt.Meta(), idxInfo, pid)
	require.Equal(t, 2, cnt)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = false
	})
}

func TestAlterTableExchangePartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	// test disable exchange partition
	tk.MustExec("ALTER TABLE e EXCHANGE PARTITION p0 WITH TABLE e2")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8200 Exchange Partition is disabled, please set 'tidb_enable_exchange_partition' if you need to need to enable it"))
	tk.MustQuery("select * from e").Check(testkit.Rows("16", "1669", "337", "2005"))
	tk.MustQuery("select * from e2").Check(testkit.Rows())

	// enable exchange partition
	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition=0")
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
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		require.NoError(t, err)
	}()

	tk.MustExec("create table e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create table e16 (a int)")
	tk.MustExec("alter table e15 set tiflash replica 1;")
	tk.MustExec("alter table e16 set tiflash replica 2;")

	e15 := external.GetTableByName(t, tk, "test", "e15")
	partition := e15.Meta().Partition

	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)

	e16 := external.GetTableByName(t, tk, "test", "e16")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), e16.Meta().ID, true)
	require.NoError(t, err)

	tk.MustGetErrCode("alter table e15 exchange partition p0 with table e16", tmysql.ErrTablesDifferentMetadata)
	tk.MustExec("drop table e15, e16")

	tk.MustExec("create table e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create table e16 (a int)")
	tk.MustExec("alter table e15 set tiflash replica 1;")
	tk.MustExec("alter table e16 set tiflash replica 1;")

	e15 = external.GetTableByName(t, tk, "test", "e15")
	partition = e15.Meta().Partition

	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)

	e16 = external.GetTableByName(t, tk, "test", "e16")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), e16.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("alter table e15 exchange partition p0 with table e16")

	e15 = external.GetTableByName(t, tk, "test", "e15")

	partition = e15.Meta().Partition

	require.NotNil(t, e15.Meta().TiFlashReplica)
	require.True(t, e15.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[0].ID}, e15.Meta().TiFlashReplica.AvailablePartitionIDs)

	e16 = external.GetTableByName(t, tk, "test", "e16")
	require.NotNil(t, e16.Meta().TiFlashReplica)
	require.True(t, e16.Meta().TiFlashReplica.Available)

	tk.MustExec("drop table e15, e16")
	tk.MustExec("create table e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create table e16 (a int)")
	tk.MustExec("alter table e16 set tiflash replica 1;")

	tk.MustExec("alter table e15 set tiflash replica 1 location labels 'a', 'b';")

	tk.MustGetErrCode("alter table e15 exchange partition p0 with table e16", tmysql.ErrTablesDifferentMetadata)

	tk.MustExec("alter table e16 set tiflash replica 1 location labels 'a', 'b';")

	e15 = external.GetTableByName(t, tk, "test", "e15")
	partition = e15.Meta().Partition

	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)

	e16 = external.GetTableByName(t, tk, "test", "e16")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), e16.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("alter table e15 exchange partition p0 with table e16")
}

func TestExchangePartitionTableCompatiable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			"create table pt2 (id int not null, salary decimal) partition by hash(id) partitions 4;",
			"create table nt2 (id int not null, salary decimal(3,2));",
			"alter table pt2 exchange partition p0 with table nt2;",
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			"create table pt3 (id int not null, salary decimal) partition by hash(id) partitions 1;",
			"create table nt3 (id int not null, salary decimal(10, 1));",
			"alter table pt3 exchange partition p0 with table nt3",
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			"create table pt4 (id int not null) partition by hash(id) partitions 1;",
			"create table nt4 (id1 int not null);",
			"alter table pt4 exchange partition p0 with table nt4;",
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			"create table pt5 (id int not null, primary key (id)) partition by hash(id) partitions 1;",
			"create table nt5 (id int not null);",
			"alter table pt5 exchange partition p0 with table nt5;",
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			"create table pt6 (id int not null, salary decimal, index idx (id, salary)) partition by hash(id) partitions 1;",
			"create table nt6 (id int not null, salary decimal, index idx (salary, id));",
			"alter table pt6 exchange partition p0 with table nt6;",
			dbterror.ErrTablesDifferentMetadata,
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
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			// foreign key test
			// Partition table doesn't support to add foreign keys in mysql
			"create table pt9 (id int not null primary key auto_increment,t_id int not null) partition by hash(id) partitions 1;",
			"create table nt9 (id int not null primary key auto_increment, t_id int not null,foreign key fk_id (t_id) references pt5(id));",
			"alter table pt9 exchange partition p0 with table nt9;",
			dbterror.ErrPartitionExchangeForeignKey,
		},
		{
			// Generated column (virtual)
			"create table pt10 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname,' ')) virtual) partition by hash(id) partitions 1;",
			"create table nt10 (id int not null, lname varchar(30), fname varchar(100));",
			"alter table pt10 exchange partition p0 with table nt10;",
			dbterror.ErrUnsupportedOnGeneratedColumn,
		},
		{
			"create table pt11 (id int not null, lname varchar(30), fname varchar(100)) partition by hash(id) partitions 1;",
			"create table nt11 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter table pt11 exchange partition p0 with table nt11;",
			dbterror.ErrUnsupportedOnGeneratedColumn,
		},
		{

			"create table pt12 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname,' ')) stored) partition by hash(id) partitions 1;",
			"create table nt12 (id int not null, lname varchar(30), fname varchar(100));",
			"alter table pt12 exchange partition p0 with table nt12;",
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			"create table pt13 (id int not null, lname varchar(30), fname varchar(100)) partition by hash(id) partitions 1;",
			"create table nt13 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) stored);",
			"alter table pt13 exchange partition p0 with table nt13;",
			dbterror.ErrTablesDifferentMetadata,
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
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			// auto_increment
			"create table pt16 (id int not null primary key auto_increment) partition by hash(id) partitions 1;",
			"create table nt16 (id int not null primary key);",
			"alter table pt16 exchange partition p0 with table nt16;",
			dbterror.ErrTablesDifferentMetadata,
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
			dbterror.ErrCheckNoSuchTable,
		},
		{
			"create table pt19 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) stored) partition by hash(id) partitions 1;",
			"create table nt19 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter table pt19 exchange partition p0 with table nt19;",
			dbterror.ErrUnsupportedOnGeneratedColumn,
		},
		{
			"create table pt20 (id int not null) partition by hash(id) partitions 1;",
			"create table nt20 (id int default null);",
			"alter table pt20 exchange partition p0 with table nt20;",
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			// unsigned
			"create table pt21 (id int unsigned) partition by hash(id) partitions 1;",
			"create table nt21 (id int);",
			"alter table pt21 exchange partition p0 with table nt21;",
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			// zerofill
			"create table pt22 (id int) partition by hash(id) partitions 1;",
			"create table nt22 (id int zerofill);",
			"alter table pt22 exchange partition p0 with table nt22;",
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			"create table pt23 (id int, lname varchar(10) charset binary) partition by hash(id) partitions 1;",
			"create table nt23 (id int, lname varchar(10));",
			"alter table pt23 exchange partition p0 with table nt23;",
			dbterror.ErrTablesDifferentMetadata,
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
			dbterror.ErrTablesDifferentMetadata,
		},
		{
			"create table pt27 (a int key, b int, index(a)) partition by hash(a) partitions 1;",
			"create table nt27 (a int not null, b int, index(a));",
			"alter table pt27 exchange partition p0 with table nt27;",
			dbterror.ErrTablesDifferentMetadata,
		},
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	err := tk.Session().GetSessionVars().SetSystemVar("tidb_enable_exchange_partition", "1")
	require.NoError(t, err)
	for i, tt := range cases {
		tk.MustExec(tt.ptSQL)
		tk.MustExec(tt.ntSQL)
		if tt.err != nil {
			_, err := tk.Exec(tt.exchangeSQL)
			require.Truef(t, terror.ErrorEqual(err, tt.err),
				"case %d fail, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
				i, tt.exchangeSQL, tt.err, err,
			)
		} else {
			tk.MustExec(tt.exchangeSQL)
		}
	}
	err = tk.Session().GetSessionVars().SetSystemVar("tidb_enable_exchange_partition", "0")
	require.NoError(t, err)
}

func TestExchangePartitionExpressIndex(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		// Test for table lock.
		conf.EnableTableLock = true
		conf.Instance.SlowThreshold = 10000
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition=0")
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

func TestAddPartitionTooManyPartitions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	sql1 += "partition p8193 values less than (8193) );"
	tk.MustGetErrCode(sql1, tmysql.ErrTooManyPartitions)

	tk.MustExec("drop table if exists p2;")
	sql2 := `create table p2 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i < count; i++ {
		sql2 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql2 += "partition p8192 values less than (8192) );"

	tk.MustExec(sql2)
	sql3 := `alter table p2 add partition (
	partition p8193 values less than (8193)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrTooManyPartitions)
}

func waitGCDeleteRangeDone(t *testing.T, tk *testkit.TestKit, physicalID int64) bool {
	var i int
	for i = 0; i < waitForCleanDataRound; i++ {
		rs, err := tk.Exec("select count(1) from mysql.gc_delete_range_done where element_id = ?", physicalID)
		require.NoError(t, err)
		rows, err := session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
		require.NoError(t, err)
		val := rows[0][0]
		if val != "0" {
			return true
		}
		time.Sleep(waitForCleanDataInterval)
	}

	return false
}

func checkPartitionDelRangeDone(t *testing.T, tk *testkit.TestKit, store kv.Storage, oldPID int64) {
	startTime := time.Now()
	partitionPrefix := tablecodec.EncodeTablePrefix(oldPID)

	done := waitGCDeleteRangeDone(t, tk, oldPID)
	if !done {
		// Takes too long, give up the check.
		logutil.BgLogger().Info("truncate partition table",
			zap.Int64("id", oldPID),
			zap.Stringer("duration", time.Since(startTime)),
		)
		return
	}

	hasOldPartitionData := true
	err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
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
	require.NoError(t, err)
	require.False(t, hasOldPartitionData)
}

func TestTruncatePartitionAndDropTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	// Only one partition id test is taken here.
	tk.MustExec("truncate table t3;")
	oldPID := oldTblInfo.Meta().Partition.Definitions[0].ID
	checkPartitionDelRangeDone(t, tk, store, oldPID)

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
	require.NoError(t, err)
	// Only one partition id test is taken here.
	oldPID = oldTblInfo.Meta().Partition.Definitions[1].ID
	tk.MustExec("drop table t4;")
	checkPartitionDelRangeDone(t, tk, store, oldPID)
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
	require.NoError(t, err)
	oldPID = oldTblInfo.Meta().Partition.Definitions[0].ID

	tk.MustExec("truncate table t5;")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t5"))
	require.NoError(t, err)
	newPID := newTblInfo.Meta().Partition.Definitions[0].ID
	require.True(t, oldPID != newPID)

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
	require.NoError(t, err)
	oldDefs := oldTblInfo.Meta().Partition.Definitions

	// Test truncate `hash partitioned table` reassigns new partitionIDs.
	tk.MustExec("truncate table clients;")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("clients"))
	require.NoError(t, err)
	newDefs := newTblInfo.Meta().Partition.Definitions
	for i := 0; i < len(oldDefs); i++ {
		require.True(t, oldDefs[i].ID != newDefs[i].ID)
	}
}

func TestPartitionUniqueKeyNeedAllFieldsInPf(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

	// after we support multiple columns partition, this sql should fail. For now, it will be a normal table.
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
	tk.MustExec(sql11)

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

func TestPartitionDropPrimaryKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	idxName := "primary"
	addIdxSQL := "alter table partition_drop_idx add primary key idx1 (c1);"
	dropIdxSQL := "alter table partition_drop_idx drop primary key;"
	testPartitionDropIndex(t, store, 50*time.Millisecond, idxName, addIdxSQL, dropIdxSQL)
}

func TestPartitionDropIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	idxName := "idx1"
	addIdxSQL := "alter table partition_drop_idx add index idx1 (c1);"
	dropIdxSQL := "alter table partition_drop_idx drop index idx1;"
	testPartitionDropIndex(t, store, 50*time.Millisecond, idxName, addIdxSQL, dropIdxSQL)
}

func testPartitionDropIndex(t *testing.T, store kv.Storage, lease time.Duration, idxName, addIdxSQL, dropIdxSQL string) {
	tk := testkit.NewTestKit(t, store)
	done := make(chan error, 1)
	tk.MustExec("use test")
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

	testutil.ExecMultiSQLInGoroutine(store, "test", []string{dropIdxSQL}, done)
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			require.NoError(t, err)
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
	tk.MustExec("drop table partition_drop_idx;")
}

func TestPartitionAddPrimaryKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	testPartitionAddIndexOrPK(t, tk, "primary key")
}

func TestPartitionAddIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	testPartitionAddIndexOrPK(t, tk, "index")
}

func testPartitionAddIndexOrPK(t *testing.T, tk *testkit.TestKit, key string) {
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
	testPartitionAddIndex(tk, t, key)

	// test hash partition table.
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec("drop table if exists partition_add_idx")
	tk.MustExec(`create table partition_add_idx (
	id int not null,
	hired date not null
	) partition by hash( year(hired) ) partitions 4;`)
	testPartitionAddIndex(tk, t, key)

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

func testPartitionAddIndex(tk *testkit.TestKit, t *testing.T, key string) {
	idxName1 := "idx1"

	f := func(end int, isPK bool) string {
		dml := "insert into partition_add_idx values"
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
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tt, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("partition_add_idx"))
	require.NoError(t, err)
	var idx1 table.Index
	for _, idx := range tt.Indices() {
		if idx.Meta().Name.L == idxName1 {
			idx1 = idx
			break
		}
	}
	require.NotNil(t, idx1)

	tk.MustQuery(fmt.Sprintf("select count(hired) from partition_add_idx use index(%s)", idxName1)).Check(testkit.Rows("500"))
	tk.MustQuery("select count(id) from partition_add_idx use index(idx2)").Check(testkit.Rows("500"))

	tk.MustExec("admin check table partition_add_idx")
	tk.MustExec("drop table partition_add_idx")
}

func TestDropSchemaWithPartitionTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_db_with_partition")
	tk.MustExec("create database test_db_with_partition")
	tk.MustExec("use test_db_with_partition")
	tk.MustExec(`create table t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	tk.MustExec("insert into t_part values (1),(2),(11),(12);")
	ctx := tk.Session()
	tbl := external.GetTableByName(t, tk, "test_db_with_partition", "t_part")

	// check records num before drop database.
	recordsNum := getPartitionTableRecordsNum(t, ctx, tbl.(table.PartitionedTable))
	require.Equal(t, 4, recordsNum)

	tk.MustExec("drop database if exists test_db_with_partition")

	// check job args.
	rs, err := tk.Exec("admin show ddl jobs")
	require.NoError(t, err)
	rows, err := session.GetRows4Test(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	row := rows[0]
	require.Equal(t, "drop schema", row.GetString(3))
	jobID := row.GetInt64(0)

	var tableIDs []int64
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMeta(txn)
		historyJob, err := tt.GetHistoryDDLJob(jobID)
		require.NoError(t, err)
		err = historyJob.DecodeArgs(&tableIDs)
		require.NoError(t, err)
		// There is 2 partitions.
		require.Equal(t, 3, len(tableIDs))
		return nil
	})
	require.NoError(t, err)

	startTime := time.Now()
	done := waitGCDeleteRangeDone(t, tk, tableIDs[2])
	if !done {
		// Takes too long, give up the check.
		logutil.BgLogger().Info("drop schema",
			zap.Int64("id", tableIDs[0]),
			zap.Stringer("duration", time.Since(startTime)),
		)
		return
	}

	// check records num after drop database.
	for i := 0; i < waitForCleanDataRound; i++ {
		recordsNum = getPartitionTableRecordsNum(t, ctx, tbl.(table.PartitionedTable))
		if recordsNum != 0 {
			time.Sleep(waitForCleanDataInterval)
		} else {
			break
		}
	}
	require.Equal(t, 0, recordsNum)
}

func getPartitionTableRecordsNum(t *testing.T, ctx sessionctx.Context, tbl table.PartitionedTable) int {
	num := 0
	info := tbl.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.GetPartition(pid)
		require.Nil(t, sessiontxn.NewTxn(context.Background(), ctx))
		err := tables.IterRecords(partition, ctx, partition.Cols(),
			func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
				num++
				return true, nil
			})
		require.NoError(t, err)
	}
	return num
}

func TestPartitionErrorCode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	require.True(t, dbterror.ErrUnsupportedAddPartition.Equal(err))

	_, err = tk.Exec("alter table employees add partition (partition p5 values less than (42));")
	require.True(t, dbterror.ErrUnsupportedAddPartition.Equal(err))

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
	require.True(t, dbterror.ErrUnsupportedCoalescePartition.Equal(err))

	tk.MustExec(`create table t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	_, err = tk.Exec("alter table t_part coalesce partition 4;")
	require.True(t, dbterror.ErrCoalesceOnlyOnHashPartition.Equal(err))

	tk.MustGetErrCode(`alter table t_part reorganize partition p0, p1 into (
			partition p0 values less than (1980));`, tmysql.ErrUnsupportedDDLOperation)

	tk.MustGetErrCode("alter table t_part check partition p0, p1;", tmysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part optimize partition p0,p1;", tmysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part rebuild partition p0,p1;", tmysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part remove partitioning;", tmysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part repair partition p1;", tmysql.ErrUnsupportedDDLOperation)

	// Reduce the impact on DML when executing partition DDL
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec(`create table t(id int primary key)
		partition by hash(id) partitions 4;`)
	tk1.MustExec("begin")
	tk1.MustExec("insert into t values(1);")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("alter table t truncate partition p0;")

	_, err = tk1.Exec("commit")
	require.NoError(t, err)
}

func TestConstAndTimezoneDepent(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestConstAndTimezoneDepent2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestUnsupportedPartitionManagementDDLs(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	require.Regexp(t, ".*alter table partition is unsupported", err.Error())
}

func TestCommitWhenSchemaChange(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		// Test for table lock.
		conf.EnableTableLock = true
		conf.Instance.SlowThreshold = 10000
		conf.Experimental.AllowsExpressionIndex = true
	})
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, time.Second)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_max_delta_schema_count= 4096")
	tk.MustExec("use test")
	tk.MustExec(`create table schema_change (a int, b timestamp)
			partition by range(a) (
			    partition p0 values less than (4),
			    partition p1 values less than (7),
			    partition p2 values less than (11)
			)`)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk2.MustExec("set @@tidb_enable_exchange_partition=0")

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
	_, err := tk.Exec("commit")
	require.Error(t, err)
	require.Truef(t, domain.ErrInfoSchemaChanged.Equal(err), err.Error())

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
	_, err = tk.Session().Execute(context.Background(), "commit")
	require.True(t, domain.ErrInfoSchemaChanged.Equal(err))

	tk.MustExec("admin check table pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustExec("admin check table nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustExec("insert into pt values (1), (3), (5);")
	tk2.MustExec("alter table pt exchange partition p1 with table nt;")
	tk.MustExec("insert into pt values (7), (9);")
	_, err = tk.Session().Execute(context.Background(), "commit")
	require.True(t, domain.ErrInfoSchemaChanged.Equal(err))

	tk.MustExec("admin check table pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustExec("admin check table nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())
}

func TestCreatePartitionTableWithWrongType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	var err error
	_, err = tk.Exec(`create table t(
	b int(10)
	) partition by range columns (b) (
		partition p0 values less than (0x10),
		partition p3 values less than (0x20)
	)`)
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))

	_, err = tk.Exec(`create table t(
	b int(10)
	) partition by range columns (b) (
		partition p0 values less than ('g'),
		partition p3 values less than ('k')
	)`)
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))

	_, err = tk.Exec(`create table t(
	b char(10)
	) partition by range columns (b) (
		partition p0 values less than (30),
		partition p3 values less than (60)
	)`)
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))

	_, err = tk.Exec(`create table t(
	b datetime
	) partition by range columns (b) (
		partition p0 values less than ('g'),
		partition p3 values less than ('m')
	)`)
	require.Error(t, err)
}

func TestAddPartitionForTableWithWrongType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop tables if exists t_int, t_char, t_date")
	tk.MustExec(`create table t_int(b int(10))
	partition by range columns (b) (
		partition p0 values less than (10)
	)`)

	tk.MustExec(`create table t_char(b char(10))
	partition by range columns (b) (
		partition p0 values less than ('a')
	)`)

	tk.MustExec(`create table t_date(b datetime)
	partition by range columns (b) (
		partition p0 values less than ('2020-09-01')
	)`)

	var err error

	_, err = tk.Exec("alter table t_int add partition (partition p1 values less than ('g'))")
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))

	_, err = tk.Exec("alter table t_int add partition (partition p1 values less than (0x20))")
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))

	_, err = tk.Exec("alter table t_char add partition (partition p1 values less than (0x20))")
	require.Error(t, err)
	require.True(t, dbterror.ErrRangeNotIncreasing.Equal(err))

	_, err = tk.Exec("alter table t_char add partition (partition p1 values less than (10))")
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))

	_, err = tk.Exec("alter table t_date add partition (partition p1 values less than ('m'))")
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))

	_, err = tk.Exec("alter table t_date add partition (partition p1 values less than (0x20))")
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))

	_, err = tk.Exec("alter table t_date add partition (partition p1 values less than (20))")
	require.Error(t, err)
	require.True(t, dbterror.ErrWrongTypeColumnValue.Equal(err))
}

func TestPartitionListWithTimeType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("create table t_list1(a date) partition by list columns (a) (partition p0 values in ('2010-02-02', '20180203'), partition p1 values in ('20200202'));")
	tk.MustExec("insert into t_list1(a) values (20180203);")
	tk.MustQuery(`select * from t_list1 partition (p0);`).Check(testkit.Rows("2018-02-03"))
}

func TestPartitionListWithNewCollation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustGetErrCode(`create table t (a char(10) collate utf8mb4_general_ci) partition by list columns (a) (partition p0 values in ('a', 'A'));`, mysql.ErrMultipleDefConstInListPart)
	tk.MustExec("create table t11(a char(10) collate utf8mb4_general_ci) partition by list columns (a) (partition p0 values in ('a', 'b'), partition p1 values in ('C', 'D'));")
	tk.MustExec("insert into t11(a) values ('A'), ('c'), ('C'), ('d'), ('B');")
	tk.MustQuery(`select * from t11 order by a;`).Check(testkit.Rows("A", "B", "c", "C", "d"))
	tk.MustQuery(`select * from t11 partition (p0);`).Check(testkit.Rows("A", "B"))
	tk.MustQuery(`select * from t11 partition (p1);`).Check(testkit.Rows("c", "C", "d"))
	str := tk.MustQuery(`desc select * from t11 where a = 'b';`).Rows()[0][3].(string)
	require.True(t, strings.Contains(str, "partition:p0"))
}

func TestAddTableWithPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// for global temporary table
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists global_partition_table;")
	tk.MustGetErrCode("create global temporary table global_partition_table (a int, b int) partition by hash(a) partitions 3 ON COMMIT DELETE ROWS;", errno.ErrPartitionNoTemporary)
	tk.MustExec("drop table if exists global_partition_table;")
	tk.MustExec("drop table if exists partition_table;")
	_, err := tk.Exec("create table partition_table (a int, b int) partition by hash(a) partitions 3;")
	require.NoError(t, err)
	tk.MustExec("drop table if exists partition_table;")
	tk.MustExec("drop table if exists partition_range_table;")
	tk.MustGetErrCode(`create global temporary table partition_range_table (c1 smallint(6) not null, c2 char(5) default null) partition by range ( c1 ) (
			partition p0 values less than (10),
			partition p1 values less than (20),
			partition p2 values less than (30),
			partition p3 values less than (MAXVALUE)
	) ON COMMIT DELETE ROWS;`, errno.ErrPartitionNoTemporary)
	tk.MustExec("drop table if exists partition_range_table;")
	tk.MustExec("drop table if exists partition_list_table;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustGetErrCode(`create global temporary table partition_list_table (id int) partition by list  (id) (
	    partition p0 values in (1,2),
	    partition p1 values in (3,4),
	    partition p3 values in (5,null)
	) ON COMMIT DELETE ROWS;`, errno.ErrPartitionNoTemporary)
	tk.MustExec("drop table if exists partition_list_table;")

	// for local temporary table
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists local_partition_table;")
	tk.MustGetErrCode("create temporary table local_partition_table (a int, b int) partition by hash(a) partitions 3;", errno.ErrPartitionNoTemporary)
	tk.MustExec("drop table if exists local_partition_table;")
	tk.MustExec("drop table if exists partition_table;")
	_, err = tk.Exec("create table partition_table (a int, b int) partition by hash(a) partitions 3;")
	require.NoError(t, err)
	tk.MustExec("drop table if exists partition_table;")
	tk.MustExec("drop table if exists local_partition_range_table;")
	tk.MustGetErrCode(`create temporary table local_partition_range_table (c1 smallint(6) not null, c2 char(5) default null) partition by range ( c1 ) (
			partition p0 values less than (10),
			partition p1 values less than (20),
			partition p2 values less than (30),
			partition p3 values less than (MAXVALUE)
	);`, errno.ErrPartitionNoTemporary)
	tk.MustExec("drop table if exists local_partition_range_table;")
	tk.MustExec("drop table if exists local_partition_list_table;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustGetErrCode(`create temporary table local_partition_list_table (id int) partition by list  (id) (
	    partition p0 values in (1,2),
	    partition p1 values in (3,4),
	    partition p3 values in (5,null)
	);`, errno.ErrPartitionNoTemporary)
	tk.MustExec("drop table if exists local_partition_list_table;")
}

func TestTruncatePartitionMultipleTimes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop table if exists test.t;")
	tk.MustExec(`create table test.t (a int primary key) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (maxvalue));`)
	dom := domain.GetDomain(tk.Session())
	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	hook := &ddl.TestDDLCallback{}
	dom.DDL().SetHook(hook)
	injected := false
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionTruncateTablePartition && job.SnapshotVer == 0 && !injected {
			injected = true
			time.Sleep(30 * time.Millisecond)
		}
	}
	var errCount int32
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.Type == model.ActionTruncateTablePartition && job.Error != nil {
			atomic.AddInt32(&errCount, 1)
		}
	}
	done1 := make(chan error, 1)
	go backgroundExec(store, "alter table test.t truncate partition p0;", done1)
	done2 := make(chan error, 1)
	go backgroundExec(store, "alter table test.t truncate partition p0;", done2)
	<-done1
	<-done2
	require.LessOrEqual(t, errCount, int32(1))
}

func TestAddPartitionReplicaBiggerThanTiFlashStores(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_partition2")
	tk.MustExec("use test_partition2")
	tk.MustExec("drop table if exists t1")
	// Build a tableInfo with replica count = 1 while there is no real tiFlash store.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`))
	tk.MustExec(`create table t1 (c int) partition by range(c) (
			partition p0 values less than (100),
			partition p1 values less than (200))`)
	tk.MustExec("alter table t1 set tiflash replica 1")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount"))
	// Mock partitions replica as available.
	t1 := external.GetTableByName(t, tk, "test_partition2", "t1")
	partition := t1.Meta().Partition
	require.Equal(t, 2, len(partition.Definitions))
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[1].ID, true)
	require.NoError(t, err)
	t1 = external.GetTableByName(t, tk, "test_partition2", "t1")
	require.True(t, t1.Meta().TiFlashReplica.Available)
	// Since there is no real TiFlash store (less than replica count), adding a partition will error here.
	err = tk.ExecToErr("alter table t1 add partition (partition p2 values less than (300));")
	require.Error(t, err)
	require.EqualError(t, err, "[ddl:-1][ddl] the tiflash replica count: 1 should be less than the total tiflash server count: 0")
	// Test `add partition` waiting TiFlash replica can exit when its retry count is beyond the limitation.
	originErrCountLimit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 3")
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %v", originErrCountLimit))
	}()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockWaitTiFlashReplica", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockWaitTiFlashReplica"))
	}()
	require.True(t, t1.Meta().TiFlashReplica.Available)
	err = tk.ExecToErr("alter table t1 add partition (partition p3 values less than (300));")
	require.Error(t, err)
	require.Equal(t, "[ddl:-1]DDL job rollback, error msg: [ddl] add partition wait for tiflash replica to complete", err.Error())
}

func TestDuplicatePartitionNames(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database DuplicatePartitionNames")
	defer tk.MustExec("drop database DuplicatePartitionNames")
	tk.MustExec("use DuplicatePartitionNames")

	tk.MustExec("set @@tidb_enable_list_partition=on")
	tk.MustExec("create table t1 (a int) partition by list (a) (partition p1 values in (1), partition p2 values in (2), partition p3 values in (3))")
	tk.MustExec("insert into t1 values (1),(2),(3)")
	tk.MustExec("alter table t1 truncate partition p1,p1")
	tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("2", "3"))
	tk.MustExec("insert into t1 values (1)")
	err := tk.ExecToErr("alter table t1 drop partition p1,p1")
	require.Error(t, err)
	require.Equal(t, "[ddl:1507]Error in list of partitions to DROP", err.Error())
	err = tk.ExecToErr("alter table t1 drop partition p1,p9")
	require.Error(t, err)
	require.Equal(t, "[ddl:1507]Error in list of partitions to DROP", err.Error())
	err = tk.ExecToErr("alter table t1 drop partition p1,p1,p1")
	require.Error(t, err)
	require.Equal(t, "[ddl:1508]Cannot remove all partitions, use DROP TABLE instead", err.Error())
	err = tk.ExecToErr("alter table t1 drop partition p1,p9,p1")
	require.Error(t, err)
	require.Equal(t, "[ddl:1508]Cannot remove all partitions, use DROP TABLE instead", err.Error())
	tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("alter table t1 drop partition p1")
	tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("2", "3"))
	tk.MustQuery("Show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY LIST (`a`)\n" +
		"(PARTITION `p2` VALUES IN (2),\n" +
		" PARTITION `p3` VALUES IN (3))"))
}
