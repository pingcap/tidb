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

package partition

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/assert"
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
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

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
	tk.MustGetErrCode(sql1, errno.ErrSameNamePartition)

	sql2 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`
	tk.MustGetErrCode(sql2, errno.ErrRangeNotIncreasing)

	sql3 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than maxvalue,
		partition p3 values less than (2001)
	);`
	tk.MustGetErrCode(sql3, errno.ErrPartitionMaxvalue)

	sql4 := `create table t4 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than maxvalue,
		partition p2 values less than (1991),
		partition p3 values less than (1995)
	);`
	tk.MustGetErrCode(sql4, errno.ErrPartitionMaxvalue)

	tk.MustExec(`CREATE TABLE rc (
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
	tk.MustQuery("show warnings").Check(testkit.Rows())

	sql6 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		 partition p0 values less than (6 , 10)
	);`
	tk.MustGetErrCode(sql6, errno.ErrTooManyValues)

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
	tk.MustGetErrCode(sql7, errno.ErrPartitionMaxvalue)

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
	tk.MustGetErrCode(sql9, errno.ErrPartitionFunctionIsNotAllowed)

	tk.MustGetDBError(`CREATE TABLE t9 (
		a INT NOT NULL,
		b INT NOT NULL,
		c INT NOT NULL
	)
	partition by range columns(a) (
	partition p0 values less than (10),
	partition p2 values less than (20),
	partition p3 values less than (20)
	);`, dbterror.ErrRangeNotIncreasing)

	tk.MustGetErrCode(`create TABLE t10 (c1 int,c2 int) partition by range(c1 / c2 ) (partition p0 values less than (2));`, errno.ErrPartitionFunctionIsNotAllowed)

	tk.MustExec(`create TABLE t11 (c1 int,c2 int) partition by range(c1 div c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t12 (c1 int,c2 int) partition by range(c1 + c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t13 (c1 int,c2 int) partition by range(c1 - c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t14 (c1 int,c2 int) partition by range(c1 * c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t15 (c1 int,c2 int) partition by range( abs(c1) ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t16 (c1 int) partition by range( c1) (partition p0 values less than (10));`)

	tk.MustGetErrCode(`create TABLE t17 (c1 int,c2 float) partition by range(c1 + c2 ) (partition p0 values less than (2));`, errno.ErrPartitionFuncNotAllowed)
	tk.MustGetErrCode(`create TABLE t18 (c1 int,c2 float) partition by range( floor(c2) ) (partition p0 values less than (2));`, errno.ErrPartitionFuncNotAllowed)
	tk.MustExec(`create TABLE t19 (c1 int,c2 float) partition by range( floor(c1) ) (partition p0 values less than (2));`)

	tk.MustExec(`create TABLE t20 (c1 int,c2 bit(10)) partition by range(c2) (partition p0 values less than (10));`)
	tk.MustExec(`create TABLE t21 (c1 int,c2 year) partition by range( c2 ) (partition p0 values less than (2000));`)

	tk.MustGetErrCode(`create TABLE t24 (c1 float) partition by range( c1 ) (partition p0 values less than (2000));`, errno.ErrFieldTypeNotAllowedAsPartitionField)

	// test check order. The sql below have 2 problem: 1. ErrFieldTypeNotAllowedAsPartitionField  2. ErrPartitionMaxvalue , mysql will return ErrPartitionMaxvalue.
	tk.MustGetErrCode(`create TABLE t25 (c1 float) partition by range( c1 ) (partition p1 values less than maxvalue,partition p0 values less than (2000));`, errno.ErrPartitionMaxvalue)

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
	tk.MustExec(`create table t30 (
		  a int,
		  b varchar(20),
		  c varchar(30))
		  partition by range columns (a, b)
		  (partition p0 values less than (10, '10.0'))`)
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustGetErrCode(`create table t31 (a int not null) partition by range( a );`, errno.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create table t32 (a int not null) partition by range columns( a );`, errno.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create table t33 (a int, b int) partition by hash(a) partitions 0;`, errno.ErrNoParts)
	tk.MustGetErrCode(`create table t33 (a timestamp, b int) partition by hash(a) partitions 30;`, errno.ErrFieldTypeNotAllowedAsPartitionField)
	tk.MustGetErrCode(`CREATE TABLE t34 (c0 INT) PARTITION BY HASH((CASE WHEN 0 THEN 0 ELSE c0 END )) PARTITIONS 1;`, errno.ErrPartitionFunctionIsNotAllowed)
	tk.MustGetErrCode(`CREATE TABLE t0(c0 INT) PARTITION BY HASH((c0<CURRENT_USER())) PARTITIONS 1;`, errno.ErrPartitionFunctionIsNotAllowed)
	// TODO: fix this one
	// tk.MustGetErrCode(`create table t33 (a timestamp, b int) partition by hash(unix_timestamp(a)) partitions 30;`, errno.ErrPartitionFuncNotAllowed)

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
	);`, errno.ErrBadField)

	// Fix a timezone dependent check bug introduced in https://github.com/pingcap/tidb/pull/10655
	tk.MustExec(`create table t34 (dt timestamp(3)) partition by range (floor(unix_timestamp(dt))) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`)

	tk.MustGetErrCode(`create table t34 (dt timestamp(3)) partition by range (unix_timestamp(date(dt))) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`, errno.ErrWrongExprInPartitionFunc)

	tk.MustGetErrCode(`create table t34 (dt datetime) partition by range (unix_timestamp(dt)) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`, errno.ErrWrongExprInPartitionFunc)

	// Fix https://github.com/pingcap/tidb/issues/16333
	tk.MustExec(`create table t35 (dt timestamp) partition by range (unix_timestamp(dt))
(partition p0 values less than (unix_timestamp('2020-04-15 00:00:00')));`)

	tk.MustExec(`drop table if exists too_long_identifier`)
	tk.MustGetErrCode(`create table too_long_identifier(a int)
partition by range (a)
(partition p0pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp values less than (10));`, errno.ErrTooLongIdent)

	tk.MustExec(`drop table if exists too_long_identifier`)
	tk.MustExec("create table too_long_identifier(a int) partition by range(a) (partition p0 values less than(10))")
	tk.MustGetErrCode("alter table too_long_identifier add partition "+
		"(partition p0pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp values less than(20))", errno.ErrTooLongIdent)

	tk.MustExec(`create table t36 (a date, b datetime) partition by range (EXTRACT(YEAR_MONTH FROM a)) (
    partition p0 values less than (200),
    partition p1 values less than (300),
    partition p2 values less than maxvalue)`)

	// Fix https://github.com/pingcap/tidb/issues/35827
	tk.MustExec(`create table t37 (id tinyint unsigned, idpart tinyint, i varchar(255)) partition by range (idpart) (partition p1 values less than (-1));`)
	tk.MustGetErrCode(`create table t38 (id tinyint unsigned, idpart tinyint unsigned, i varchar(255)) partition by range (idpart) (partition p1 values less than (-1));`, errno.ErrPartitionConstDomain)
}

func TestCreateTableWithHashPartition(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

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
) PARTITION BY HASH(store_id) PARTITIONS 102400000000;`, errno.ErrTooManyPartitions)

	tk.MustExec("CREATE TABLE t_linear (a int, b varchar(128)) PARTITION BY LINEAR HASH(a) PARTITIONS 4")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 8200 LINEAR HASH is not supported, using non-linear HASH instead"))
	tk.MustQuery(`show create table t_linear`).Check(testkit.Rows("" +
		"t_linear CREATE TABLE `t_linear` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` varchar(128) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY HASH (`a`) PARTITIONS 4"))
	tk.MustQuery("select * from t_linear partition (p0)").Check(testkit.Rows())

	tk.MustExec(`CREATE TABLE t_sub (a int, b varchar(128)) PARTITION BY RANGE( a ) SUBPARTITION BY HASH( a )
                                   SUBPARTITIONS 2 (
                                       PARTITION p0 VALUES LESS THAN (100),
                                       PARTITION p1 VALUES LESS THAN (200),
                                       PARTITION p2 VALUES LESS THAN MAXVALUE)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 8200 Unsupported subpartitioning, only using RANGE partitioning"))
	tk.MustQuery("select * from t_sub partition (p0)").Check(testkit.Rows())
	tk.MustQuery("show create table t_sub").Check(testkit.Rows("" +
		"t_sub CREATE TABLE `t_sub` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` varchar(128) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (200),\n" +
		" PARTITION `p2` VALUES LESS THAN (MAXVALUE))"))

	// Fix create partition table using extract() function as partition key.
	tk.MustExec("create table t2 (a date, b datetime) partition by hash (EXTRACT(YEAR_MONTH FROM a)) partitions 7")
	tk.MustExec("create table t3 (a int, b int) partition by hash(ceiling(a-b)) partitions 10")
	tk.MustExec("create table t4 (a int, b int) partition by hash(floor(a-b)) partitions 10")
}

func TestSubPartitioning(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int) partition by range (a) subpartition by hash (a) subpartitions 2 (partition pMax values less than (maxvalue))`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 8200 Unsupported subpartitioning, only using RANGE partitioning"))
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	tk.MustExec(`drop table t`)

	tk.MustExec(`create table t (a int) partition by list (a) subpartition by key (a) subpartitions 2 (partition pMax values in (1,3,4))`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 8200 Unsupported subpartitioning, only using LIST partitioning"))
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY LIST (`a`)\n" +
		"(PARTITION `pMax` VALUES IN (1,3,4))"))
	tk.MustExec(`drop table t`)

	tk.MustGetErrMsg(`create table t (a int) partition by hash (a) partitions 2 subpartition by key (a) subpartitions 2`, "[ddl:1500]It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning")
	tk.MustGetErrMsg(`create table t (a int) partition by key (a) partitions 2 subpartition by hash (a) subpartitions 2`, "[ddl:1500]It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning")

	tk.MustGetErrMsg(`CREATE TABLE t ( col1 INT NOT NULL, col2 INT NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL, primary KEY (col1,col3) ) PARTITION BY HASH(col1) PARTITIONS 4 SUBPARTITION BY HASH(col3) SUBPARTITIONS 2`, "[ddl:1500]It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning")
	tk.MustGetErrMsg(`CREATE TABLE t ( col1 INT NOT NULL, col2 INT NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL, primary KEY (col1,col3) ) PARTITION BY KEY(col1) PARTITIONS 4 SUBPARTITION BY KEY(col3) SUBPARTITIONS 2`, "[ddl:1500]It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning")
}

func TestCreateTableWithRangeColumnPartition(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists log_message_1;")
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

	tk.MustExec("create table t (a varchar(255), b varchar(255)) partition by range columns (a,b)" +
		`(partition pNull values less than ("",""), partition p0 values less than ("A",""),` +
		`partition p1 values less than ("A","A"), partition p2 values less than ("A","b"),` +
		`partition p3 values less than ("A",maxvalue), partition p4 values less than ("B",""),` +
		`partition pMax values less than (maxvalue,""))`)
	err := tk.ExecToErr("create table t (a varchar(255), b varchar(255)) partition by range columns (a,b)" +
		`(partition pNull values less than ("",""), partition p0 values less than ("A",""),` +
		`partition p1 values less than ("A","A"), partition p2 values less than ("A","b"),` +
		`partition p3 values less than ("A",maxvalue), partition p4 values less than ("B",""),` +
		// If one column has maxvalue set, the next column does not matter, so we should not allow it!
		`partition pMax values less than (maxvalue,""), partition pMax2 values less than (maxvalue,"a"))`)
	require.Error(t, err)
	require.EqualError(t, err, "[ddl:1493]VALUES LESS THAN value must be strictly increasing for each partition")
	err = tk.ExecToErr("create table t (a varchar(255), b varchar(255)) partition by range columns (a,b)" +
		`(partition pNull values less than ("",""), partition p0 values less than ("A",""),` +
		`partition p1 values less than ("A","A"), partition p2 values less than ("A","b"),` +
		`partition p3 values less than ("A",maxvalue), partition p4 values less than ("B",""),` +
		// If one column has maxvalue set, the next column does not matter, so we should not allow it!
		`partition pMax values less than ("b",MAXVALUE), partition pMax2 values less than ("b","a"))`)
	require.Error(t, err)
	require.EqualError(t, err, "[ddl:1493]VALUES LESS THAN value must be strictly increasing for each partition")
	err = tk.ExecToErr("create table t (a varchar(255), b varchar(255)) partition by range columns (a,b)" +
		`(partition pNull values less than ("",""), partition p0 values less than ("A",""),` +
		`partition p1 values less than ("A","A"), partition p2 values less than ("A","b"),` +
		`partition p3 values less than ("A",maxvalue), partition p4 values less than ("B",""),` +
		// If one column has maxvalue set, the next column does not matter, so we should not allow it!
		`partition pMax values less than ("b",MAXVALUE), partition pMax2 values less than ("b",MAXVALUE))`)
	require.Error(t, err)
	require.EqualError(t, err, "[ddl:1493]VALUES LESS THAN value must be strictly increasing for each partition")

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
		{
			"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
				"partition p0 values less than (1, 'a')," +
				"partition p1 values less than (1, 'a'))",
			dbterror.ErrRangeNotIncreasing,
		},
		{
			"create table t (a int, b varchar(64)) partition by range columns ( b) (" +
				"partition p0 values less than ( 'a')," +
				"partition p1 values less than ('a'))",
			dbterror.ErrRangeNotIncreasing,
		},
		// create as normal table, warning.
		{
			"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
				"partition p0 values less than (1, 'b')," +
				"partition p1 values less than (1, 'a'))",
			dbterror.ErrRangeNotIncreasing,
		},
		{
			"create table t (a int, b varchar(64)) partition by range columns (b) (" +
				"partition p0 values less than ('b')," +
				"partition p1 values less than ('a'))",
			dbterror.ErrRangeNotIncreasing,
		},
		// create as normal table, warning.
		{
			"create table t (a int, b varchar(64)) partition by range columns (a, b) (" +
				"partition p0 values less than (1, maxvalue)," +
				"partition p1 values less than (1, 'a'))",
			dbterror.ErrRangeNotIncreasing,
		},
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
			"create table t(d datetime)" +
				"partition by range columns (d) (" +
				"partition p0 values less than ('2022-01-01')," +
				"partition p1 values less than (MAXVALUE), " +
				"partition p2 values less than (MAXVALUE));",
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
	tk.MustQuery("show warnings").Check(testkit.Rows())

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
	tk.MustExec(`drop table t`)

	tk.MustExec(`create table t(a time) partition by range columns (a) (partition p1 values less than ('2020'))`)
	tk.MustExec(`insert into t values ('2019')`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` time DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
			"PARTITION BY RANGE COLUMNS(`a`)\n" +
			"(PARTITION `p1` VALUES LESS THAN ('00:20:20'))"))
	tk.MustExec(`drop table t`)
	tk.MustExec(`create table t (a time, b time) partition by range columns (a) (partition p1 values less than ('2020'), partition p2 values less than ('20:20:10'))`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` time DEFAULT NULL,\n" +
			"  `b` time DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
			"PARTITION BY RANGE COLUMNS(`a`)\n" +
			"(PARTITION `p1` VALUES LESS THAN ('00:20:20'),\n" +
			" PARTITION `p2` VALUES LESS THAN ('20:20:10'))"))
	tk.MustExec(`insert into t values ('2019','2019'),('20:20:09','20:20:09')`)
	tk.MustExec(`drop table t`)
	tk.MustExec(`create table t (a time, b time) partition by range columns (a,b) (partition p1 values less than ('2020','2020'), partition p2 values less than ('20:20:10','20:20:10'))`)
	tk.MustExec(`insert into t values ('2019','2019'),('20:20:09','20:20:09')`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` time DEFAULT NULL,\n" +
			"  `b` time DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
			"PARTITION BY RANGE COLUMNS(`a`,`b`)\n" +
			"(PARTITION `p1` VALUES LESS THAN ('00:20:20','00:20:20'),\n" +
			" PARTITION `p2` VALUES LESS THAN ('20:20:10','20:20:10'))"))
}

func generatePartitionTableByNum(num int) string {
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024))
	buf.WriteString("create table gen_t (id int) partition by list  (id) (")
	for i := 0; i < num; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(buf, "partition p%v values in (%v)", i, i)
	}
	buf.WriteString(")")
	return buf.String()
}

func TestCreateTableWithListPartition(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

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
			"create table t (a int) partition by list (a) (partition p0 values in (null), partition p1 values in (NULL))",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int, b varchar(33)) partition by list columns (a,b) (partition p0 values in ((1,null)), partition p1 values in ((1,NULL)))",
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
			generatePartitionTableByNum(mysql.PartitionCountLimit + 1),
			dbterror.ErrTooManyPartitions,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (null), partition p1 values in (null))",
			dbterror.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (default), partition p1 values in (default))",
			dbterror.ErrMultipleDefConstInListPart,
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
		"create table t (a varchar(39)) partition by list columns (a) (partition pNull values in (null), partition pEmptyString values in (''))",
		"create table t (a varchar(39), b varchar(44)) partition by list columns (a,b) (partition pNull values in (('1',null),('2','NULL'),('','1'),(null,null)), partition pEmptyString values in (('2',''),('1',''),(NULL,''),('','')))",
		"create table t (a bigint) partition by list (a) (partition p0 values in (1, default),partition p1 values in (0, 22,3))",
		generatePartitionTableByNum(mysql.PartitionCountLimit),
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
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

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
		{
			"create table t (a int) partition by list (a) (partition p1 values in (1), partition p2 values in (2, default), partition p3 values in (3, default));",
			dbterror.ErrMultipleDefConstInListPart,
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

func TestAlterTableTruncatePartitionByList(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	tk.MustGetErrCode(sql, errno.ErrUnknownPartition)
	tk.MustExec(`alter table t truncate partition p3`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec(`alter table t truncate partition p0`)
	tk.MustQuery("select * from t").Check(testkit.Rows())
}

func TestAlterTableTruncatePartitionByListColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	require.Equal(t, [][]string{{"3", `'a'`}, {"4", `'b'`}}, part.Definitions[1].InValues)
	require.Equal(t, model.NewCIStr("p1"), part.Definitions[1].Name)
	require.False(t, part.Definitions[1].ID == oldTbl.Meta().Partition.Definitions[1].ID)

	sql := "alter table t truncate partition p10;"
	tk.MustGetErrCode(sql, errno.ErrUnknownPartition)
	tk.MustExec(`alter table t truncate partition p3`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 a"))
	tk.MustExec(`alter table t truncate partition p0`)
	tk.MustQuery("select * from t").Check(testkit.Rows())
}

func TestAlterTableTruncatePartitionPreSplitRegion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`CREATE TABLE t1 (id int, c varchar(128), key c(c)) partition by range (id) (
		partition p0 values less than (10),
		partition p1 values less than MAXVALUE)`)
	re := tk.MustQuery("show table t1 regions")
	rows := re.Rows()
	require.Len(t, rows, 2)
	tk.MustExec(`alter table t1 truncate partition p0`)
	re = tk.MustQuery("show table t1 regions")
	rows = re.Rows()
	require.Len(t, rows, 2)

	tk.MustExec("drop table if exists t2;")
	tk.MustExec(`CREATE TABLE t2(id bigint(20) NOT NULL AUTO_INCREMENT, PRIMARY KEY (id) NONCLUSTERED) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 PARTITION BY RANGE (id) (
		PARTITION p1 VALUES LESS THAN (10),
		PARTITION p2 VALUES LESS THAN (20),
		PARTITION p3 VALUES LESS THAN (MAXVALUE))`)
	re = tk.MustQuery("show table t2 regions")
	rows = re.Rows()
	require.Len(t, rows, 24)
	tk.MustExec(`alter table t2 truncate partition p3`)
	re = tk.MustQuery("show table t2 regions")
	rows = re.Rows()
	require.Len(t, rows, 24)
}

func TestCreateTableWithKeyPartition(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tm1;")
	tk.MustExec(`create table tm1
	(
		s1 char(32) primary key
	)
	partition by key(s1) partitions 10;`)

	tk.MustExec(`drop table if exists tm2`)
	tk.MustGetErrMsg(`create table tm2 (a char(5), unique key(a(5))) partition by key() partitions 5`,
		"Table partition metadata not correct, neither partition expression or list of partition columns")
	tk.MustExec(`create table tm2 (a char(5) not null, unique key(a(5))) partition by key() partitions 5`)
}

func TestDropPartitionWithGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
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
}

func TestDropMultiPartitionWithGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30)
	);`)
	tt := external.GetTableByName(t, tk, "test", "test_global")
	pid := tt.Meta().Partition.Definitions[1].ID

	tk.MustExec("Alter Table test_global Add Unique Index idx_b (b);")
	tk.MustExec("Alter Table test_global Add Unique Index idx_c (c);")
	tk.MustExec(`INSERT INTO test_global VALUES (1, 1, 1), (2, 2, 2), (11, 3, 3), (12, 4, 4), (21, 21, 21), (29, 29, 29)`)

	tk.MustExec("alter table test_global drop partition p1, p2;")
	result := tk.MustQuery("select * from test_global;")
	result.Sort().Check(testkit.Rows("21 21 21", "29 29 29"))

	tt = external.GetTableByName(t, tk, "test", "test_global")
	idxInfo := tt.Meta().FindIndexByName("idx_b")
	require.NotNil(t, idxInfo)
	cnt := checkGlobalIndexCleanUpDone(t, tk.Session(), tt.Meta(), idxInfo, pid)
	require.Equal(t, 2, cnt)

	idxInfo = tt.Meta().FindIndexByName("idx_c")
	require.NotNil(t, idxInfo)
	cnt = checkGlobalIndexCleanUpDone(t, tk.Session(), tt.Meta(), idxInfo, pid)
	require.Equal(t, 2, cnt)
}

func TestGlobalIndexInsertInDropPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30)
	);`)
	tk.MustExec("alter table test_global add unique index idx_b (b);")
	tk.MustExec("insert into test_global values (1, 1, 1), (8, 8, 8), (11, 11, 11), (12, 12, 12);")

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionDropTablePartition, job.Type)
		if job.SchemaState == model.StateDeleteOnly {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			tk2.MustExec("insert into test_global values (9, 9, 9)")
		}
	}
	dom.DDL().SetHook(hook)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("alter table test_global drop partition p1")

	tk.MustExec("analyze table test_global")
	tk.MustQuery("select * from test_global use index(idx_b) order by a").Check(testkit.Rows("9 9 9", "11 11 11", "12 12 12"))
}

func TestGlobalIndexUpdateInDropPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30)
	);`)
	tk.MustExec("alter table test_global add unique index idx_b (b);")
	tk.MustExec("insert into test_global values (1, 1, 1), (8, 8, 8), (11, 11, 11), (12, 12, 12);")

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionDropTablePartition, job.Type)
		if job.SchemaState == model.StateDeleteOnly {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			tk2.MustExec("update test_global set a = 2 where a = 11")
		}
	}
	dom.DDL().SetHook(hook)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("alter table test_global drop partition p1")

	tk.MustExec("analyze table test_global")
	tk.MustQuery("select * from test_global use index(idx_b) order by a").Check(testkit.Rows("2 11 11", "12 12 12"))
}

func TestTruncatePartitionWithGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
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
	tk.MustExec(`INSERT INTO test_global VALUES (1, 1, 1), (2, 2, 2), (11, 3, 3), (12, 4, 4), (15, 15, 15)`)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use test`)
	tk2.MustExec(`begin`)
	tk2.MustExec(`insert into test_global values (5,5,5)`)
	syncChan := make(chan bool)
	go func() {
		tk.MustExec("alter table test_global truncate partition p2;")
		syncChan <- true
	}()
	waitFor := func(i int, s string) {
		for {
			tk4 := testkit.NewTestKit(t, store)
			tk4.MustExec(`use test`)
			res := tk4.MustQuery(`admin show ddl jobs where db_name = 'test' and table_name = 'test_global' and job_type = 'truncate partition'`).Rows()
			if len(res) == 1 && res[0][i] == s {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor(4, "delete only")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec(`begin`)
	tk3.MustExec(`use test`)
	tk3.MustQuery(`explain format='brief' select b from test_global use index(idx_b) where b = 15`).CheckContain("IndexRangeScan")
	tk3.MustQuery(`explain format='brief' select b from test_global use index(idx_b) where b = 15`).CheckContain("Selection")
	tk3.MustQuery(`explain format='brief' select c from test_global use index(idx_c) where c = 15`).CheckContain("IndexRangeScan")
	tk3.MustQuery(`explain format='brief' select c from test_global use index(idx_c) where c = 15`).CheckContain("Selection")
	tk3.MustQuery(`select b from test_global use index(idx_b) where b = 15`).Check(testkit.Rows())
	tk3.MustQuery(`select c from test_global use index(idx_c) where c = 15`).Check(testkit.Rows())
	// Here it will fail with
	// the partition is not in public.
	err := tk3.ExecToErr(`insert into test_global values (15,15,15)`)
	require.Error(t, err)
	require.ErrorContains(t, err, "the partition is in not in public")
	tk2.MustExec(`commit`)
	tk3.MustExec(`commit`)
	<-syncChan
	result := tk.MustQuery("select * from test_global;")
	result.Sort().Check(testkit.Rows(`1 1 1`, `2 2 2`, `5 5 5`))

	tt = external.GetTableByName(t, tk, "test", "test_global")
	idxInfo := tt.Meta().FindIndexByName("idx_b")
	require.NotNil(t, idxInfo)
	cnt := checkGlobalIndexCleanUpDone(t, tk.Session(), tt.Meta(), idxInfo, pid)
	require.Equal(t, 3, cnt)

	idxInfo = tt.Meta().FindIndexByName("idx_c")
	require.NotNil(t, idxInfo)
	cnt = checkGlobalIndexCleanUpDone(t, tk.Session(), tt.Meta(), idxInfo, pid)
	require.Equal(t, 3, cnt)
	tk.MustQuery(`select b from test_global use index(idx_b) where b = 15`).Check(testkit.Rows())
	tk.MustQuery(`select c from test_global use index(idx_c) where c = 15`).Check(testkit.Rows())
	tk3.MustQuery(`explain format='brief' select b from test_global use index(idx_b) where b = 15`).CheckContain("Point_Get")
	tk3.MustQuery(`explain format='brief' select c from test_global use index(idx_c) where c = 15`).CheckContain("Point_Get")
}

func TestGlobalIndexUpdateInTruncatePartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30)
	);`)
	tk.MustExec("alter table test_global add unique index idx_b (b);")
	tk.MustExec("insert into test_global values (1, 1, 1), (8, 8, 8), (11, 11, 11), (12, 12, 12);")
	tk.MustExec("analyze table test_global")

	originalHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originalHook)

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionTruncateTablePartition, job.Type)
		if job.SchemaState == model.StateDeleteOnly {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			err := tk1.ExecToErr("update test_global set a = 2 where a = 11")
			assert.NotNil(t, err)
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table test_global truncate partition p1")
	tk.MustQuery("select * from test_global use index(idx_b) order by a").Check(testkit.Rows("11 11 11", "12 12 12"))
}

func TestGlobalIndexUpdateInTruncatePartition4Hash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by hash(a) partitions 4;`)
	tk.MustExec("alter table test_global add unique index idx_b (b);")
	tk.MustExec("insert into test_global values (1, 1, 1), (8, 8, 8), (11, 11, 11), (12, 12, 12);")
	tk.MustExec("analyze table test_global")

	originalHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originalHook)

	var err error
	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionTruncateTablePartition, job.Type)
		if job.SchemaState == model.StateDeleteOnly {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			err = tk1.ExecToErr("update test_global set a = 1 where a = 12")
			assert.NotNil(t, err)
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table test_global truncate partition p1")
}

func TestGlobalIndexReaderAndIndexLookUpInTruncatePartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30)
	);`)
	tk.MustExec("alter table test_global add unique index idx_b (b);")
	tk.MustExec("insert into test_global values (1, 1, 1), (8, 8, 8), (11, 11, 11), (12, 12, 12);")
	tk.MustExec("analyze table test_global")

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionTruncateTablePartition, job.Type)
		if job.SchemaState == model.StateDeleteOnly {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")

			tk1.MustQuery("select b from test_global use index(idx_b)").Sort().Check(testkit.Rows("11", "12"))
			tk1.MustQuery("select * from test_global use index(idx_b)").Sort().Check(testkit.Rows("11 11 11", "12 12 12"))
			tk1.MustQuery("select * from test_global use index(idx_b) order by a").Check(testkit.Rows("11 11 11", "12 12 12"))
			tk1.MustQuery("select * from test_global use index(idx_b) order by b").Check(testkit.Rows("11 11 11", "12 12 12"))
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table test_global truncate partition p1")
}

func TestGlobalIndexInsertInTruncatePartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30)
	);`)
	tk.MustExec("alter table test_global add unique index idx_b (b);")
	tk.MustExec("insert into test_global values (1, 1, 1), (8, 8, 8), (11, 11, 11), (12, 12, 12);")
	tk.MustExec("analyze table test_global")

	var err error
	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionTruncateTablePartition, job.Type)
		if job.SchemaState == model.StateDeleteOnly {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			err = tk1.ExecToErr("insert into test_global values(2, 2, 2)")
			assert.NotNil(t, err)
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table test_global truncate partition p1")
}

func TestGlobalIndexReaderInDropPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30)
	);`)
	tk.MustExec("alter table test_global add unique index idx_b (b);")
	tk.MustExec("insert into test_global values (1, 1, 1), (8, 8, 8), (11, 11, 11), (12, 12, 12);")

	var indexScanResult *testkit.Result
	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionDropTablePartition, job.Type)
		if job.SchemaState == model.StateDeleteOnly {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")

			indexScanResult = tk1.MustQuery("select b from test_global use index(idx_b)").Sort()
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table test_global drop partition p1")

	indexScanResult.Check(testkit.Rows("11", "12"))
}

func TestGlobalIndexLookUpInDropPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("drop table if exists test_global")
	tk.MustExec(`create table test_global ( a int, b int, c int)
	partition by range( a ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30)
	);`)
	tk.MustExec("alter table test_global add unique index idx_b (b);")
	tk.MustExec("insert into test_global values (1, 1, 1), (8, 8, 8), (11, 11, 11), (12, 12, 12);")

	var indexLookupResult *testkit.Result
	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionDropTablePartition, job.Type)
		if job.SchemaState == model.StateDeleteOnly {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			tk1.MustExec("analyze table test_global")
			indexLookupResult = tk1.MustQuery("select * from test_global use index(idx_b)").Sort()
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table test_global drop partition p1")

	indexLookupResult.Check(testkit.Rows("11 11 11", "12 12 12"))
}

func TestGlobalIndexShowTableRegions(t *testing.T) {
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("drop table if exists p")
	tk.MustExec("set @@global.tidb_scatter_region = on")
	tk.MustExec(`create table p (id int, c int, d int, unique key uidx(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	rs := tk.MustQuery("show table p regions").Rows()
	require.Equal(t, len(rs), 3)
	rs = tk.MustQuery("show table p index uidx regions").Rows()
	require.Equal(t, len(rs), 3)

	tk.MustExec("alter table p add unique idx(id)")
	rs = tk.MustQuery("show table p regions").Rows()
	require.Equal(t, len(rs), 4)
	rs = tk.MustQuery("show table p index idx regions").Rows()
	require.Equal(t, len(rs), 1)
	rs = tk.MustQuery("show table p index uidx regions").Rows()
	require.Equal(t, len(rs), 3)
}

func TestAlterTableExchangePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

	// enable exchange partition
	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition=0")
	tk.MustExec("ALTER TABLE e EXCHANGE PARTITION p0 WITH TABLE e2")
	tk.MustQuery("select * from e2").Check(testkit.Rows("16"))
	tk.MustQuery("select * from e").Check(testkit.Rows("1669", "337", "2005"))
	// validation test for range partition
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e2", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p2 WITH TABLE e2", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p3 WITH TABLE e2", errno.ErrRowDoesNotMatchPartition)

	tk.MustExec("drop table if exists e3")

	tk.MustExec(`CREATE TABLE e3 (
		id int not null
	) PARTITION BY HASH (id)
	PARTITIONS 4;`)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e3;", errno.ErrPartitionExchangePartTable)
	tk.MustExec("truncate table e2")
	tk.MustExec(`INSERT INTO e3 VALUES (1),(5)`)

	tk.MustExec("ALTER TABLE e3 EXCHANGE PARTITION p1 WITH TABLE e2;")
	tk.MustQuery("select * from e3 partition(p0)").Check(testkit.Rows())
	tk.MustQuery("select * from e2").Check(testkit.Rows("1", "5"))

	// validation test for hash partition
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p0 WITH TABLE e2", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p2 WITH TABLE e2", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p3 WITH TABLE e2", errno.ErrRowDoesNotMatchPartition)

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

	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p2 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p0 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p0)").Check(testkit.Rows("1"))

	// for partition p1
	tk.MustExec("insert into e5 values (3)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p2 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p1 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p1)").Check(testkit.Rows("3"))

	// for partition p2
	tk.MustExec("insert into e5 values (6)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p2 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p2)").Check(testkit.Rows("6"))

	// for partition p3
	tk.MustExec("insert into e5 values (9)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", errno.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("alter table e4 exchange partition p2 with table e5", errno.ErrRowDoesNotMatchPartition)
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
	tk.MustGetErrCode("alter table e6 exchange partition p1 with table e7", errno.ErrRowDoesNotMatchPartition)

	// validation test for list partition
	tk.MustExec("set @@tidb_enable_list_partition=true")
	tk.MustExec(`CREATE TABLE t1 (store_id int)
	PARTITION BY LIST (store_id) (
		PARTITION pNorth VALUES IN (1, 2, 3, 4, 5),
		PARTITION pEast VALUES IN (6, 7, 8, 9, 10),
		PARTITION pWest VALUES IN (11, 12, 13, 14, 15),
		PARTITION pCentral VALUES IN (16, 17, 18, 19, 20)
	);`)
	tk.MustExec(`create table t2 (store_id int);`)
	tk.MustExec(`insert into t1 values (1);`)
	tk.MustExec(`insert into t1 values (6);`)
	tk.MustExec(`insert into t1 values (11);`)
	tk.MustExec(`insert into t2 values (3);`)
	tk.MustExec("alter table t1 exchange partition pNorth with table t2")

	tk.MustQuery("select * from t1 partition(pNorth)").Check(testkit.Rows("3"))
	tk.MustGetErrCode("alter table t1 exchange partition pEast with table t2", errno.ErrRowDoesNotMatchPartition)

	// validation test for list columns partition
	tk.MustExec(`CREATE TABLE t3 (id int, store_id int)
	PARTITION BY LIST COLUMNS (id, store_id) (
		PARTITION p0 VALUES IN ((1, 1), (2, 2)),
		PARTITION p1 VALUES IN ((3, 3), (4, 4))
	);`)
	tk.MustExec(`create table t4 (id int, store_id int);`)
	tk.MustExec(`insert into t3 values (1, 1);`)
	tk.MustExec(`insert into t4 values (2, 2);`)
	tk.MustExec("alter table t3 exchange partition p0 with table t4")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 after the exchange, please analyze related table of the exchange to update statistics"))

	tk.MustQuery("select * from t3 partition(p0)").Check(testkit.Rows("2 2"))
	tk.MustGetErrCode("alter table t3 exchange partition p1 with table t4", errno.ErrRowDoesNotMatchPartition)

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
	tk.MustGetErrCode("alter table e12 exchange partition p0 with table e13", errno.ErrPartitionExchangeDifferentOption)
	// test for index id
	tk.MustExec("create table e14 (a int, b int, index(a));")
	tk.MustExec("alter table e12 drop index a")
	tk.MustExec("alter table e12 add index (a);")
	tk.MustGetErrCode("alter table e12 exchange partition p0 with table e14", errno.ErrPartitionExchangeDifferentOption)

	// test for tiflash replica
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount")
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

	tk.MustGetErrCode("alter table e15 exchange partition p0 with table e16", errno.ErrTablesDifferentMetadata)
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

	tk.MustGetErrCode("alter table e15 exchange partition p0 with table e16", errno.ErrTablesDifferentMetadata)

	tk.MustExec("alter table e16 set tiflash replica 1 location labels 'a', 'b';")

	e15 = external.GetTableByName(t, tk, "test", "e15")
	partition = e15.Meta().Partition

	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)

	e16 = external.GetTableByName(t, tk, "test", "e16")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), e16.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("alter table e15 exchange partition p0 with table e16")

	tk.MustExec("create table e17 (a int)")
	tk.MustExec("alter table e17 set tiflash replica 1")
	tk.MustExec("insert into e17 values (1)")

	tk.MustExec("create table e18 (a int) partition by range (a) (partition p0 values less than (4), partition p1 values less than (10))")
	tk.MustExec("alter table e18 set tiflash replica 1")
	tk.MustExec("insert into e18 values (2)")

	tk.MustExec("alter table e18 exchange partition p0 with table e17")
	tk.MustQuery("select * /*+ read_from_storage(tiflash[e18]) */ from e18").Check(testkit.Rows("1"))
	tk.MustQuery("select * /*+ read_from_storage(tiflash[e17]) */ from e17").Check(testkit.Rows("2"))

	tk.MustExec("create table e19 (a int) partition by hash(a) partitions 1")
	tk.MustExec("create temporary table e20 (a int)")
	tk.MustGetErrCode("alter table e19 exchange partition p0 with table e20", errno.ErrPartitionExchangeTempTable)
}

func TestExchangePartitionMultiTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)

	dbName := "ExchangeMultiTable"
	tk1.MustExec(`create schema ` + dbName)
	tk1.MustExec(`use ` + dbName)
	tk1.MustExec(`CREATE TABLE t1 (a int)`)
	tk1.MustExec(`CREATE TABLE t2 (a int)`)
	tk1.MustExec(`CREATE TABLE tp (a int) partition by hash(a) partitions 3`)
	tk1.MustExec(`insert into t1 values (0)`)
	tk1.MustExec(`insert into t2 values (3)`)
	tk1.MustExec(`insert into tp values (6)`)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use ` + dbName)
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec(`use ` + dbName)
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec(`use ` + dbName)
	waitFor := func(col int, tableName, s string) {
		for {
			tk4 := testkit.NewTestKit(t, store)
			tk4.MustExec(`use test`)
			sql := `admin show ddl jobs where db_name = '` + strings.ToLower(dbName) + `' and table_name = '` + tableName + `' and job_type = 'exchange partition'`
			res := tk4.MustQuery(sql).Rows()
			if len(res) == 1 && res[0][col] == s {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	alterChan1 := make(chan error)
	alterChan2 := make(chan error)
	tk3.MustExec(`BEGIN`)
	tk3.MustExec(`insert into tp values (1)`)
	go func() {
		alterChan1 <- tk1.ExecToErr(`alter table tp exchange partition p0 with table t1`)
	}()
	waitFor(11, "t1", "running")
	go func() {
		alterChan2 <- tk2.ExecToErr(`alter table tp exchange partition p0 with table t2`)
	}()
	waitFor(11, "t2", "queueing")
	tk3.MustExec(`rollback`)
	require.NoError(t, <-alterChan1)
	err := <-alterChan2
	tk3.MustQuery(`select * from t1`).Check(testkit.Rows("6"))
	tk3.MustQuery(`select * from t2`).Check(testkit.Rows("0"))
	tk3.MustQuery(`select * from tp`).Check(testkit.Rows("3"))
	require.NoError(t, err)
}

func TestExchangePartitionHook(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	// why use tkCancel, not tk.
	tkCancel := testkit.NewTestKit(t, store)

	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition=0")

	tk.MustExec("use test")
	tk.MustExec(`create table pt (a int) partition by range(a) (
		partition p0 values less than (3),
		partition p1 values less than (6),
        PARTITION p2 VALUES LESS THAN (9),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
		);`)
	tk.MustExec(`create table nt(a int);`)

	tk.MustExec(`insert into pt values (0), (4), (7)`)
	tk.MustExec("insert into nt values (1)")

	hook := &callback.TestDDLCallback{Do: dom}
	dom.DDL().SetHook(hook)

	hookFunc := func(job *model.Job) {
		if job.Type == model.ActionExchangeTablePartition && job.SchemaState != model.StateNone {
			tkCancel.MustExec("use test")
			tkCancel.MustGetErrCode("insert into nt values (5)", errno.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}
	hook.OnJobUpdatedExported.Store(&hookFunc)

	tk.MustExec("alter table pt exchange partition p0 with table nt")
	tk.MustQuery("select * from pt partition(p0)").Check(testkit.Rows("1"))
}

func TestExchangePartitionAutoID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition=0")

	tk.MustExec("use test")
	tk.MustExec(`create table pt (a int primary key auto_increment) partition by range(a) (
		partition p0 values less than (3),
		partition p1 values less than (6),
        PARTITION p2 values less than (9),
        PARTITION p3 values less than (50000000)
		);`)
	tk.MustExec(`create table nt(a int primary key auto_increment);`)
	tk.MustExec(`insert into pt values (0), (4)`)
	tk.MustExec("insert into nt values (1)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/exchangePartitionAutoID", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/exchangePartitionAutoID"))
	}()

	tk.MustExec("alter table pt exchange partition p0 with table nt")
	tk.MustExec("insert into nt values (NULL)")
	tk.MustQuery("select count(*) from nt where a >= 4000000").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from pt where a >= 4000000").Check(testkit.Rows("1"))
}

func TestAddPartitionTooManyPartitions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	count := mysql.PartitionCountLimit
	tk.MustExec("drop table if exists p1;")
	sql1 := `create table p1 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i <= count; i++ {
		sql1 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql1 += "partition p8193 values less than (8193) );"
	tk.MustGetErrCode(sql1, errno.ErrTooManyPartitions)

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
	tk.MustGetErrCode(sql3, errno.ErrTooManyPartitions)
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
		logutil.DDLLogger().Info("truncate partition table",
			zap.Int64("id", oldPID),
			zap.Stringer("duration", time.Since(startTime)),
		)
		return
	}

	hasOldPartitionData := true
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
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
	store := testkit.CreateMockStore(t)
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
	tk.MustGetErrCode("select * from t2;", errno.ErrNoSuchTable)

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
	tk.MustGetErrCode("select * from t4;", errno.ErrNoSuchTable)

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

func TestPartitionDropPrimaryKeyAndDropIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	// Drop Primary Key
	idxName := "primary"
	addIdxSQL := "alter table partition_drop_idx add primary key idx1 (c1);"
	dropIdxSQL := "alter table partition_drop_idx drop primary key;"
	testPartitionDropIndex(t, store, 50*time.Millisecond, idxName, addIdxSQL, dropIdxSQL)

	// Drop Index
	idxName = "idx1"
	addIdxSQL = "alter table partition_drop_idx add index idx1 (c1);"
	dropIdxSQL = "alter table partition_drop_idx drop index idx1;"
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

func TestPartitionAddPrimaryKeyAndAddIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// Add Primary Key
	testPartitionAddIndexOrPK(t, tk, "primary key")
	// Add Index
	testPartitionAddIndexOrPK(t, tk, "index")
}

func testPartitionAddIndexOrPK(t *testing.T, tk *testkit.TestKit, key string) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists partition_add_idx")
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
	store := testkit.CreateMockStore(t)
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
	historyJob, err := ddl.GetHistoryJobByID(tk.Session(), jobID)
	require.NoError(t, err)
	err = historyJob.DecodeArgs(&tableIDs)
	require.NoError(t, err)
	// There is 2 partitions.
	require.Equal(t, 3, len(tableIDs))

	startTime := time.Now()
	done := waitGCDeleteRangeDone(t, tk, tableIDs[2])
	if !done {
		// Takes too long, give up the check.
		logutil.DDLLogger().Info("drop schema",
			zap.Int64("id", tableIDs[0]),
			zap.Stringer("duration", time.Since(startTime)),
		)
		return
	}

	// check records num after drop database.
	for i := 0; i < waitForCleanDataRound; i++ {
		recordsNum = getPartitionTableRecordsNum(t, ctx, tbl.(table.PartitionedTable))
		if recordsNum == 0 {
			break
		}
		time.Sleep(waitForCleanDataInterval)
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
	store := testkit.CreateMockStore(t)
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
	tk.MustExec("alter table employees add partition partitions 8")
	tk.MustGetDBError("alter table employees add partition (partition pNew values less than (42))", ast.ErrPartitionWrongValues)
	tk.MustGetDBError("alter table employees add partition (partition pNew values in (42))", ast.ErrPartitionWrongValues)

	// coalesce partition
	tk.MustExec(`create table clients (
		id int,
		fname varchar(30),
		lname varchar(30),
		signed date
	)
	partition by hash( month(signed) )
	partitions 12`)
	tk.MustContainErrMsg("alter table clients coalesce partition 12", "[ddl:1508]Cannot remove all partitions, use DROP TABLE instead")

	tk.MustExec(`create table t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	tk.MustGetDBError("alter table t_part coalesce partition 4;", dbterror.ErrCoalesceOnlyOnHashPartition)

	tk.MustGetErrCode("alter table t_part check partition p0, p1;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part optimize partition p0,p1;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part rebuild partition p0,p1;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_part repair partition p1;", errno.ErrUnsupportedDDLOperation)

	// Reduce the impact on DML when executing partition DDL
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set global tidb_enable_metadata_lock=0")
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec(`create table t(id int primary key)
		partition by hash(id) partitions 4;`)
	tk1.MustExec("begin")
	tk1.MustExec("insert into t values(1);")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("alter table t truncate partition p0;")
	tk1.MustExec("commit")
}

func TestCommitWhenSchemaChange(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, time.Second)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_metadata_lock=0")
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
	tk.MustGetDBError("commit", domain.ErrInfoSchemaChanged)

	tk.MustExec("admin check table pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustExec("admin check table nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustExec("insert into pt values (1), (3), (5);")
	tk2.MustExec("alter table pt exchange partition p1 with table nt;")
	tk.MustExec("insert into pt values (7), (9);")
	tk.MustGetDBError("commit", domain.ErrInfoSchemaChanged)

	tk.MustExec("admin check table pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustExec("admin check table nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())
}

func TestTruncatePartitionMultipleTimes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop table if exists test.t;")
	tk.MustExec(`create table test.t (a int primary key) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (maxvalue));`)
	dom := domain.GetDomain(tk.Session())
	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	hook := &callback.TestDDLCallback{}
	dom.DDL().SetHook(hook)
	injected := false
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionTruncateTablePartition && job.SnapshotVer == 0 && !injected {
			injected = true
			time.Sleep(30 * time.Millisecond)
		}
	}
	var errCount atomic.Int32
	onJobUpdatedExportedFunc := func(job *model.Job) {
		if job.Type == model.ActionTruncateTablePartition && job.Error != nil {
			errCount.Add(1)
		}
	}
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	done1 := make(chan error, 1)
	go backgroundExec(store, "test", "alter table test.t truncate partition p0;", done1)
	done2 := make(chan error, 1)
	go backgroundExec(store, "test", "alter table test.t truncate partition p0;", done2)
	<-done1
	<-done2
	require.LessOrEqual(t, errCount.Load(), int32(1))
}

func TestAddPartitionReplicaBiggerThanTiFlashStores(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_partition2")
	tk.MustExec("use test_partition2")
	tk.MustExec("drop table if exists t1")
	// Build a tableInfo with replica count = 1 while there is no real tiFlash store.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	tk.MustExec(`create table t1 (c int) partition by range(c) (
			partition p0 values less than (100),
			partition p1 values less than (200))`)
	tk.MustExec("alter table t1 set tiflash replica 1")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockWaitTiFlashReplica", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockWaitTiFlashReplica"))
	}()
	require.True(t, t1.Meta().TiFlashReplica.Available)
	err = tk.ExecToErr("alter table t1 add partition (partition p3 values less than (300));")
	require.Error(t, err)
	require.Equal(t, "[ddl:-1]DDL job rollback, error msg: [ddl] add partition wait for tiflash replica to complete", err.Error())
}

func TestReorgPartitionTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "ReorgPartTiFlash"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk.MustExec(`create table t (a int unsigned PRIMARY KEY, b varchar(255), c int, key (b), key (c,b))` +
		` partition by list columns (a) ` +
		`(partition p0 values in (10,11,45),` +
		` partition p1 values in (20,1,23,56),` +
		` partition p2 values in (12,34,9))`)
	tk.MustExec(`insert into t values (1,"1",1), (12,"12",21),(23,"23",32),(34,"34",43),(45,"45",54),(56,"56",65)`)

	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount")
		require.NoError(t, err)
	}()

	tk.MustExec(`alter table t set tiflash replica 1`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY LIST COLUMNS(`a`)\n" +
		"(PARTITION `p0` VALUES IN (10,11,45),\n" +
		" PARTITION `p1` VALUES IN (20,1,23,56),\n" +
		" PARTITION `p2` VALUES IN (12,34,9))"))

	tbl := external.GetTableByName(t, tk, schemaName, "t")
	p := tbl.GetPartitionedTable()
	for _, pid := range p.GetAllPartitionIDs() {
		require.NoError(t, domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), pid, true))
	}
	// Reload
	tbl = external.GetTableByName(t, tk, schemaName, "t")
	p = tbl.GetPartitionedTable()
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.True(t, tbl.Meta().TiFlashReplica.Available)
	pids := p.GetAllPartitionIDs()
	sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
	availablePids := tbl.Meta().TiFlashReplica.AvailablePartitionIDs
	sort.Slice(availablePids, func(i, j int) bool { return availablePids[i] < availablePids[j] })
	require.Equal(t, pids, availablePids)
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockWaitTiFlashReplicaOK", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockWaitTiFlashReplicaOK")
		require.NoError(t, err)
	}()
	tk.MustExec(`alter table t reorganize partition p1, p2 into (partition p1 values in (34,2,23), partition p2 values in (12,56,9),partition p3 values in (1,8,19))`)
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY LIST COLUMNS(`a`)\n" +
		"(PARTITION `p0` VALUES IN (10,11,45),\n" +
		" PARTITION `p1` VALUES IN (34,2,23),\n" +
		" PARTITION `p2` VALUES IN (12,56,9),\n" +
		" PARTITION `p3` VALUES IN (1,8,19))"))

	// TODO: Check how to properly test TiFlash, since this will just change the actual configuration
	tbl = external.GetTableByName(t, tk, schemaName, "t")
	p = tbl.GetPartitionedTable()
	for _, pid := range p.GetAllPartitionIDs() {
		require.NoError(t, domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), pid, true))
	}
	tbl = external.GetTableByName(t, tk, schemaName, "t")
	p = tbl.GetPartitionedTable()
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.True(t, tbl.Meta().TiFlashReplica.Available)
	for _, pid := range p.GetAllPartitionIDs() {
		require.True(t, tbl.Meta().TiFlashReplica.IsPartitionAvailable(pid))
	}
	tk.MustExec(`alter table t remove partitioning`)
	tbl = external.GetTableByName(t, tk, schemaName, "t")
	require.Nil(t, tbl.GetPartitionedTable())
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.True(t, tbl.Meta().TiFlashReplica.Available)
	tk.MustExec(`alter table t set tiflash replica 0`)
	tbl = external.GetTableByName(t, tk, schemaName, "t")
	require.Nil(t, tbl.GetPartitionedTable())
	require.Nil(t, tbl.Meta().TiFlashReplica)
	tk.MustExec(`alter table t set tiflash replica 1`)
	tbl = external.GetTableByName(t, tk, schemaName, "t")
	require.NoError(t, domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tbl.Meta().ID, true))
	tbl = external.GetTableByName(t, tk, schemaName, "t")
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.True(t, tbl.Meta().TiFlashReplica.Available)
	require.Nil(t, tbl.GetPartitionedTable())
	tk.MustExec(`alter table t partition by key(a) partitions 3`)
	tbl = external.GetTableByName(t, tk, schemaName, "t")
	p = tbl.GetPartitionedTable()
	for _, pid := range p.GetAllPartitionIDs() {
		require.NoError(t, domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), pid, true))
	}
	p = tbl.GetPartitionedTable()
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.True(t, tbl.Meta().TiFlashReplica.Available)
	for _, pid := range p.GetAllPartitionIDs() {
		require.True(t, tbl.Meta().TiFlashReplica.IsPartitionAvailable(pid))
	}
	for _, pid := range p.GetAllPartitionIDs() {
		require.True(t, tbl.Meta().TiFlashReplica.IsPartitionAvailable(pid))
	}
}

func TestIssue40135Ver2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")

	tk.MustExec("CREATE TABLE t40135 ( a int DEFAULT NULL, b varchar(32) DEFAULT 'md', index(a)) PARTITION BY HASH (a) PARTITIONS 6")
	tk.MustExec("insert into t40135 values (1, 'md'), (2, 'ma'), (3, 'md'), (4, 'ma'), (5, 'md'), (6, 'ma')")
	one := true
	hook := &callback.TestDDLCallback{Do: dom}
	var checkErr error
	var wg sync.WaitGroup
	wg.Add(1)
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateDeleteOnly {
			tk3.MustExec("delete from t40135 where a = 1")
		}
		if one {
			one = false
			go func() {
				_, checkErr = tk1.Exec("alter table t40135 modify column a int NULL")
				wg.Done()
			}()
		}
	}
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t40135 modify column a bigint NULL DEFAULT '6243108' FIRST")
	wg.Wait()
	require.ErrorContains(t, checkErr, "[ddl:8200]Unsupported modify column: table is partition table")
	tk.MustExec("admin check table t40135")
}

func TestAlterModifyPartitionColTruncateWarning(t *testing.T) {
	t.Skip("waiting for supporting Modify Partition Column again")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "truncWarn"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk.MustExec(`set sql_mode = default`)
	tk.MustExec(`create table t (a varchar(255)) partition by range columns (a) (partition p1 values less than ("0"), partition p2 values less than ("zzzz"))`)
	tk.MustExec(`insert into t values ("123456"),(" 654321")`)
	tk.MustContainErrMsg(`alter table t modify a varchar(5)`, "[types:1265]Data truncated for column 'a', value is '")
	tk.MustExec(`set sql_mode = ''`)
	tk.MustExec(`alter table t modify a varchar(5)`)
	// Fix the duplicate warning, see https://github.com/pingcap/tidb/issues/38699
	tk.MustQuery(`show warnings`).Check(testkit.Rows(""+
		"Warning 1265 Data truncated for column 'a', value is ' 654321'",
		"Warning 1265 Data truncated for column 'a', value is ' 654321'"))
	tk.MustExec(`admin check table t`)
}

func TestRemoveKeyPartitioning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("create database RemovePartitioning")
	tk.MustExec("use RemovePartitioning")
	tk.MustExec(`create table t (a varchar(255), b varchar(255), key (a,b), key (b)) partition by key (a) partitions 7`)
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	// Fill the data with ascii strings
	for i := 32; i <= 126; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (char(%d,%d,%d),char(%d,%d,%d,%d))`, i, i, i, i, i, i, i))
	}
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show stats_meta where db_name = 'RemovePartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemovePartitioning", "t", "global", "0", "95"},
		{"RemovePartitioning", "t", "p0", "0", "9"},
		{"RemovePartitioning", "t", "p1", "0", "11"},
		{"RemovePartitioning", "t", "p2", "0", "12"},
		{"RemovePartitioning", "t", "p3", "0", "13"},
		{"RemovePartitioning", "t", "p4", "0", "16"},
		{"RemovePartitioning", "t", "p5", "0", "23"},
		{"RemovePartitioning", "t", "p6", "0", "11"}})
	tk.MustQuery(`select partition_name, table_rows from information_schema.partitions where table_schema = 'RemovePartitioning' and table_name = 't'`).Sort().Check(testkit.Rows(""+
		"p0 9",
		"p1 11",
		"p2 12",
		"p3 13",
		"p4 16",
		"p5 23",
		"p6 11"))
	tk.MustExec(`alter table t remove partitioning`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` varchar(255) DEFAULT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  KEY `a` (`a`,`b`),\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Statistics are updated asynchronously
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	// And also cached and lazy loaded
	h.Clear()
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustQuery(`show stats_meta where db_name = 'RemovePartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemovePartitioning", "t", "", "0", "95"}})
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show stats_meta where db_name = 'RemovePartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemovePartitioning", "t", "", "0", "95"}})
}

func TestRemoveListPartitioning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("create database RemoveListPartitioning")
	tk.MustExec("use RemoveListPartitioning")
	tk.MustExec(`create table t (a int, b varchar(255), key (a,b), key (b)) partition by list (a) (partition p0 values in (0), partition p1 values in (1), partition p2 values in (2), partition p3 values in (3), partition p4 values in (4))`)
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	// Fill the data with ascii strings
	for i := 32; i <= 126; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d,char(%d,%d,%d,%d))`, i%5, i, i, i, i))
	}
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "global", "0", "95"},
		{"RemoveListPartitioning", "t", "p0", "0", "19"},
		{"RemoveListPartitioning", "t", "p1", "0", "19"},
		{"RemoveListPartitioning", "t", "p2", "0", "19"},
		{"RemoveListPartitioning", "t", "p3", "0", "19"},
		{"RemoveListPartitioning", "t", "p4", "0", "19"}})
	tk.MustQuery(`select partition_name, table_rows from information_schema.partitions where table_schema = 'RemoveListPartitioning' and table_name = 't'`).Sort().Check(testkit.Rows(""+
		"p0 19",
		"p1 19",
		"p2 19",
		"p3 19",
		"p4 19"))
	tk.MustExec(`alter table t remove partitioning`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  KEY `a` (`a`,`b`),\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Statistics are updated asynchronously
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	// And also cached and lazy loaded
	h.Clear()
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "", "0", "95"}})
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "", "0", "95"}})
}

func TestRemoveListColumnPartitioning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("create database RemoveListPartitioning")
	tk.MustExec("use RemoveListPartitioning")
	tk.MustExec(`create table t (a varchar(255), b varchar(255), key (a,b), key (b)) partition by list columns (a) (partition p0 values in ("0"), partition p1 values in ("1"), partition p2 values in ("2"), partition p3 values in ("3"), partition p4 values in ("4"))`)
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	// Fill the data with ascii strings
	for i := 32; i <= 126; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values ("%d",char(%d,%d,%d,%d))`, i%5, i, i, i, i))
	}
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "global", "0", "95"},
		{"RemoveListPartitioning", "t", "p0", "0", "19"},
		{"RemoveListPartitioning", "t", "p1", "0", "19"},
		{"RemoveListPartitioning", "t", "p2", "0", "19"},
		{"RemoveListPartitioning", "t", "p3", "0", "19"},
		{"RemoveListPartitioning", "t", "p4", "0", "19"}})
	tk.MustQuery(`select partition_name, table_rows from information_schema.partitions where table_schema = 'RemoveListPartitioning' and table_name = 't'`).Sort().Check(testkit.Rows(""+
		"p0 19",
		"p1 19",
		"p2 19",
		"p3 19",
		"p4 19"))
	tk.MustExec(`alter table t remove partitioning`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` varchar(255) DEFAULT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  KEY `a` (`a`,`b`),\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Statistics are updated asynchronously
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	// And also cached and lazy loaded
	h.Clear()
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "", "0", "95"}})
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "", "0", "95"}})
}

func TestRemoveListColumnsPartitioning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("create database RemoveListPartitioning")
	tk.MustExec("use RemoveListPartitioning")
	tk.MustExec(`create table t (a int, b varchar(255), key (a,b), key (b)) partition by list columns (a,b) (partition p0 values in ((0,"0")), partition p1 values in ((1,"1")), partition p2 values in ((2,"2")), partition p3 values in ((3,"3")), partition p4 values in ((4,"4")))`)
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	// Fill the data
	for i := 32; i <= 126; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d,"%d")`, i%5, i%5))
	}
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "global", "0", "95"},
		{"RemoveListPartitioning", "t", "p0", "0", "19"},
		{"RemoveListPartitioning", "t", "p1", "0", "19"},
		{"RemoveListPartitioning", "t", "p2", "0", "19"},
		{"RemoveListPartitioning", "t", "p3", "0", "19"},
		{"RemoveListPartitioning", "t", "p4", "0", "19"}})
	tk.MustQuery(`select partition_name, table_rows from information_schema.partitions where table_schema = 'RemoveListPartitioning' and table_name = 't'`).Sort().Check(testkit.Rows(""+
		"p0 19",
		"p1 19",
		"p2 19",
		"p3 19",
		"p4 19"))
	tk.MustExec(`alter table t remove partitioning`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  KEY `a` (`a`,`b`),\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Statistics are updated asynchronously
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	// And also cached and lazy loaded
	h.Clear()
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "", "0", "95"}})
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show stats_meta where db_name = 'RemoveListPartitioning' and table_name = 't'`).Sort().CheckAt([]int{0, 1, 2, 4, 5}, [][]any{
		{"RemoveListPartitioning", "t", "", "0", "95"}})
}

func TestRemovePartitioningAutoIDs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	dbName := "RemovePartAutoIDs"
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)

	tk1.MustExec(`create schema ` + dbName)
	tk1.MustExec(`use ` + dbName)
	tk2.MustExec(`use ` + dbName)
	tk3.MustExec(`use ` + dbName)

	tk1.MustExec(`CREATE TABLE t (a int auto_increment primary key nonclustered, b varchar(255), key (b)) partition by hash(a) partitions 3`)
	tk1.MustExec(`insert into t values (11,11),(2,2),(null,12)`)
	tk1.MustExec(`insert into t values (null,18)`)
	tk1.MustQuery(`select _tidb_rowid, a, b from t`).Sort().Check(testkit.Rows("13 11 11", "14 2 2", "15 12 12", "17 16 18"))

	waitFor := func(col int, tableName, s string) {
		for {
			tk4 := testkit.NewTestKit(t, store)
			tk4.MustExec(`use test`)
			sql := `admin show ddl jobs where db_name = '` + strings.ToLower(dbName) + `' and table_name = '` + tableName + `' and job_type = 'alter table remove partitioning'`
			res := tk4.MustQuery(sql).Rows()
			if len(res) == 1 && res[0][col] == s {
				break
			}
			for i := range res {
				strs := make([]string, 0, len(res[i]))
				for j := range res[i] {
					strs = append(strs, res[i][j].(string))
				}
				logutil.DDLLogger().Info("ddl jobs", zap.Strings("jobs", strs))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	alterChan := make(chan error)
	tk2.MustExec(`BEGIN`)
	tk2.MustExec(`insert into t values (null, 4)`)
	go func() {
		alterChan <- tk1.ExecToErr(`alter table t remove partitioning`)
	}()
	waitFor(4, "t", "delete only")
	tk3.MustExec(`BEGIN`)
	tk3.MustExec(`insert into t values (null, 5)`)

	tk2.MustExec(`insert into t values (null, 6)`)
	tk3.MustExec(`insert into t values (null, 7)`)
	tk2.MustExec(`COMMIT`)

	waitFor(4, "t", "write only")
	tk2.MustExec(`BEGIN`)
	tk2.MustExec(`insert into t values (null, 8)`)

	tk3.MustExec(`insert into t values (null, 9)`)
	tk2.MustExec(`insert into t values (null, 10)`)
	tk3.MustExec(`COMMIT`)
	tk3.MustQuery(`select _tidb_rowid, a, b from t`).Sort().Check(testkit.Rows(
		"13 11 11", "14 2 2", "15 12 12", "17 16 18",
		"19 18 4", "21 20 5", "23 22 6", "25 24 7", "30 29 9"))
	tk2.MustQuery(`select _tidb_rowid, a, b from t`).Sort().Check(testkit.Rows(
		"13 11 11", "14 2 2", "15 12 12", "17 16 18",
		"19 18 4", "23 22 6", "27 26 8", "32 31 10"))

	waitFor(4, "t", "write reorganization")
	tk3.MustExec(`BEGIN`)
	tk3.MustExec(`insert into t values (null, 21)`)

	tk2.MustExec(`insert into t values (null, 22)`)
	tk3.MustExec(`insert into t values (null, 23)`)
	tk2.MustExec(`COMMIT`)

	/*
		waitFor(4, "t", "delete reorganization")
		tk2.MustExec(`BEGIN`)
		tk2.MustExec(`insert into t values (null, 24)`)

		tk3.MustExec(`insert into t values (null, 25)`)
		tk2.MustExec(`insert into t values (null, 26)`)
	*/
	tk3.MustExec(`COMMIT`)
	tk3.MustQuery(`select _tidb_rowid, a, b from t`).Sort().Check(testkit.Rows(
		"13 11 11", "14 2 2", "15 12 12", "17 16 18",
		"19 18 4", "21 20 5", "23 22 6", "25 24 7", "27 26 8", "30 29 9",
		"32 31 10", "35 34 21", "38 37 22", "41 40 23"))

	//waitFor(4, "t", "public")
	//tk2.MustExec(`commit`)
	// TODO: Investigate and fix, but it is also related to https://github.com/pingcap/tidb/issues/46904
	require.ErrorContains(t, <-alterChan, "[kv:1062]Duplicate entry '26' for key 't.PRIMARY'")
	tk3.MustQuery(`select _tidb_rowid, a, b from t`).Sort().Check(testkit.Rows(
		"13 11 11", "14 2 2", "15 12 12", "17 16 18",
		"19 18 4", "21 20 5", "23 22 6", "25 24 7", "27 26 8", "30 29 9",
		"32 31 10", "35 34 21", "38 37 22", "41 40 23"))
}

func TestAlterLastIntervalPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (id int, create_time datetime)
		partition by range columns (create_time)
		interval (1 day)
		first partition less than ('2023-01-01')
		last partition less than ('2023-01-03');`)
	ctx := tk.Session()
	tbl, err := domain.GetDomain(ctx).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pd := tbl.Meta().Partition.Definitions
	require.Equal(t, 3, len(pd))
	require.Equal(t, "'2023-01-01 00:00:00'", pd[0].LessThan[0])
	require.Equal(t, "'2023-01-02 00:00:00'", pd[1].LessThan[0])
	require.Equal(t, "'2023-01-03 00:00:00'", pd[2].LessThan[0])
	tk.MustExec("alter table t last partition less than ('2024-01-04')")
	tk.MustExec("alter table t last partition less than ('2025-01-01 00:00:00')")
	tbl, err = domain.GetDomain(ctx).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pd = tbl.Meta().Partition.Definitions
	require.Equal(t, 732, len(pd))
	require.Equal(t, "'2023-01-01 00:00:00'", pd[0].LessThan[0])
	require.Equal(t, "'2023-01-02 00:00:00'", pd[1].LessThan[0])
	require.Equal(t, "'2023-01-03 00:00:00'", pd[2].LessThan[0])
	require.Equal(t, "'2024-12-31 00:00:00'", pd[730].LessThan[0])
	require.Equal(t, "'2025-01-01 00:00:00'", pd[731].LessThan[0])

	// Test for interval 2 days.
	tk.MustExec(`create table t2 (id int, create_time datetime)
		partition by range columns (create_time)
		interval (2 day)
		first partition less than ('2023-01-01')
		last partition less than ('2023-01-05');`)
	tbl, err = domain.GetDomain(ctx).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	pd = tbl.Meta().Partition.Definitions
	require.Equal(t, 3, len(pd))
	require.Equal(t, "'2023-01-01 00:00:00'", pd[0].LessThan[0])
	require.Equal(t, "'2023-01-03 00:00:00'", pd[1].LessThan[0])
	require.Equal(t, "'2023-01-05 00:00:00'", pd[2].LessThan[0])
	tk.MustExec("alter table t2 last partition less than ('2023-01-09')")
	tk.MustExec("alter table t2 last partition less than ('2023-01-11 00:00:00')")
	tbl, err = domain.GetDomain(ctx).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	pd = tbl.Meta().Partition.Definitions
	require.Equal(t, 6, len(pd))
	require.Equal(t, "'2023-01-01 00:00:00'", pd[0].LessThan[0])
	require.Equal(t, "'2023-01-03 00:00:00'", pd[1].LessThan[0])
	require.Equal(t, "'2023-01-05 00:00:00'", pd[2].LessThan[0])
	require.Equal(t, "'2023-01-07 00:00:00'", pd[3].LessThan[0])
	require.Equal(t, "'2023-01-09 00:00:00'", pd[4].LessThan[0])
	require.Equal(t, "'2023-01-11 00:00:00'", pd[5].LessThan[0])

	// Test for day with time.
	tk.MustExec(`create table t3 (id int, create_time datetime)
		partition by range columns (create_time)
		interval (2 day)
		first partition less than ('2023-01-01 12:01:02')
		last partition less than ('2023-01-05 12:01:02');`)
	tbl, err = domain.GetDomain(ctx).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	pd = tbl.Meta().Partition.Definitions
	require.Equal(t, 3, len(pd))
	require.Equal(t, "'2023-01-01 12:01:02'", pd[0].LessThan[0])
	require.Equal(t, "'2023-01-03 12:01:02'", pd[1].LessThan[0])
	require.Equal(t, "'2023-01-05 12:01:02'", pd[2].LessThan[0])
	tk.MustExec("alter table t3 last partition less than ('2023-01-09 12:01:02')")
	tbl, err = domain.GetDomain(ctx).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	pd = tbl.Meta().Partition.Definitions
	require.Equal(t, 5, len(pd))
	require.Equal(t, "'2023-01-01 12:01:02'", pd[0].LessThan[0])
	require.Equal(t, "'2023-01-03 12:01:02'", pd[1].LessThan[0])
	require.Equal(t, "'2023-01-05 12:01:02'", pd[2].LessThan[0])
	require.Equal(t, "'2023-01-07 12:01:02'", pd[3].LessThan[0])
	require.Equal(t, "'2023-01-09 12:01:02'", pd[4].LessThan[0])

	// Some other test.
	tk.MustExec(`create table t4 (id int, create_time datetime)
		partition by range columns (create_time)
		interval (48 hour)
		first partition less than ('2023-01-01')
		last partition less than ('2023-01-05');`)
	tbl, err = domain.GetDomain(ctx).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	require.NoError(t, err)
	pd = tbl.Meta().Partition.Definitions
	require.Equal(t, 3, len(pd))
	require.Equal(t, "'2023-01-01 00:00:00'", pd[0].LessThan[0])
	require.Equal(t, "'2023-01-03 00:00:00'", pd[1].LessThan[0])
	require.Equal(t, "'2023-01-05 00:00:00'", pd[2].LessThan[0])
	tk.MustExec("alter table t4 last partition less than ('2023-01-09 00:00:00')")
	tbl, err = domain.GetDomain(ctx).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	require.NoError(t, err)
	pd = tbl.Meta().Partition.Definitions
	require.Equal(t, 5, len(pd))
	require.Equal(t, "'2023-01-01 00:00:00'", pd[0].LessThan[0])
	require.Equal(t, "'2023-01-03 00:00:00'", pd[1].LessThan[0])
	require.Equal(t, "'2023-01-05 00:00:00'", pd[2].LessThan[0])
	require.Equal(t, "'2023-01-07 00:00:00'", pd[3].LessThan[0])
	require.Equal(t, "'2023-01-09 00:00:00'", pd[4].LessThan[0])
	tk.MustQuery("show create table t4").Check(testkit.Rows("t4 CREATE TABLE `t4` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `create_time` datetime DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`create_time`)\n" +
		"(PARTITION `P_LT_2023-01-01 00:00:00` VALUES LESS THAN ('2023-01-01 00:00:00'),\n" +
		" PARTITION `P_LT_2023-01-03 00:00:00` VALUES LESS THAN ('2023-01-03 00:00:00'),\n" +
		" PARTITION `P_LT_2023-01-05 00:00:00` VALUES LESS THAN ('2023-01-05 00:00:00'),\n" +
		" PARTITION `P_LT_2023-01-07 00:00:00` VALUES LESS THAN ('2023-01-07 00:00:00'),\n" +
		" PARTITION `P_LT_2023-01-09 00:00:00` VALUES LESS THAN ('2023-01-09 00:00:00'))"))

	tk.MustExec(`create table t5 (id int, create_time datetime)
		partition by range columns (create_time)
		interval (1 month)
		first partition less than ('2023-01-01')
		last partition less than ('2023-05-01');`)
	tk.MustQuery("show create table t5").Check(testkit.Rows("t5 CREATE TABLE `t5` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `create_time` datetime DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`create_time`)\n" +
		"(PARTITION `P_LT_2023-01-01 00:00:00` VALUES LESS THAN ('2023-01-01 00:00:00'),\n" +
		" PARTITION `P_LT_2023-02-01 00:00:00` VALUES LESS THAN ('2023-02-01 00:00:00'),\n" +
		" PARTITION `P_LT_2023-03-01 00:00:00` VALUES LESS THAN ('2023-03-01 00:00:00'),\n" +
		" PARTITION `P_LT_2023-04-01 00:00:00` VALUES LESS THAN ('2023-04-01 00:00:00'),\n" +
		" PARTITION `P_LT_2023-05-01 00:00:00` VALUES LESS THAN ('2023-05-01 00:00:00'))"))

	tk.MustExec("CREATE TABLE `t6` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `create_time` datetime DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`create_time`)\n" +
		"(PARTITION `P_LT_2023-01-01` VALUES LESS THAN ('2023-01-01'),\n" +
		" PARTITION `P_LT_2023-01-02` VALUES LESS THAN ('2023-01-02'))")
	tk.MustExec("alter table t6 last partition less than ('2023-01-04')")
	tk.MustQuery("show create table t6").Check(testkit.Rows("t6 CREATE TABLE `t6` (\n" +
		"  `id` int(11) DEFAULT NULL,\n" +
		"  `create_time` datetime DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`create_time`)\n" +
		"(PARTITION `P_LT_2023-01-01` VALUES LESS THAN ('2023-01-01 00:00:00'),\n" +
		" PARTITION `P_LT_2023-01-02` VALUES LESS THAN ('2023-01-02 00:00:00'),\n" +
		" PARTITION `P_LT_2023-01-03 00:00:00` VALUES LESS THAN ('2023-01-03 00:00:00'),\n" +
		" PARTITION `P_LT_2023-01-04 00:00:00` VALUES LESS THAN ('2023-01-04 00:00:00'))"))
}

// TODO: check EXCHANGE how it handles null (for all types of partitioning!!!)
func TestExchangeValidateHandleNullValue(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`CREATE TABLE t1 (id int, c varchar(128)) PARTITION BY HASH (id) PARTITIONS 3`)
	tk.MustExec(`CREATE TABLE t2 (id int, c varchar(128))`)
	tk.MustExec(`insert into t1 values(null, 'a1')`)
	tk.MustExec(`insert into t2 values(null, 'b2')`)
	tk.MustQuery(`select id, c from t1 partition(p0)`).Check(testkit.Rows("<nil> a1"))
	tk.MustContainErrMsg(`alter table t1 EXCHANGE PARTITION p1 WITH TABLE t2`,
		"[ddl:1737]Found a row that does not match the partition")
	tk.MustExec(`alter table t1 EXCHANGE PARTITION p0 WITH TABLE t2`)

	tk.MustExec(`CREATE TABLE t3 (id int, c date) PARTITION BY HASH (year(c)) PARTITIONS 12`)
	tk.MustExec(`CREATE TABLE t4 (id int, c date)`)
	tk.MustExec(`insert into t3 values(1, null)`)
	tk.MustExec(`insert into t4 values(2, null)`)
	tk.MustQuery(`select id, c from t3 partition(p0)`).Check(testkit.Rows("1 <nil>"))
	tk.MustContainErrMsg(`alter table t3 EXCHANGE PARTITION p1 WITH TABLE t4`,
		"[ddl:1737]Found a row that does not match the partition")
	tk.MustExec(`alter table t3 EXCHANGE PARTITION p0 WITH TABLE t4`)

	tk.MustExec(`CREATE TABLE t5 (id int, c varchar(128)) partition by range (id)(
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (maxvalue))`)
	tk.MustExec(`CREATE TABLE t6 (id int, c varchar(128))`)
	tk.MustExec(`insert into t5 values(null, 'a5')`)
	tk.MustExec(`insert into t6 values(null, 'b6')`)
	tk.MustQuery(`select id, c from t5 partition(p0)`).Check(testkit.Rows("<nil> a5"))
	tk.MustContainErrMsg(`alter table t5 EXCHANGE PARTITION p1 WITH TABLE t6`,
		"[ddl:1737]Found a row that does not match the partition")
	tk.MustContainErrMsg(`alter table t5 EXCHANGE PARTITION p2 WITH TABLE t6`,
		"[ddl:1737]Found a row that does not match the partition")
	tk.MustExec(`alter table t5 EXCHANGE PARTITION p0 WITH TABLE t6`)
	// TODO: add "partition by range columns(a, b, c)" test cases.
}
