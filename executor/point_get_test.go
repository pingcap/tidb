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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")
	tk.MustQuery("select * from point where id = 1 and c = 0").Check(testkit.Rows())
	tk.MustQuery("select * from point where id < 0 and c = 1 and d = 'b'").Check(testkit.Rows())
	result, err := tk.Exec("select id as ident from point where id = 1")
	require.NoError(t, err)
	fields := result.Fields()
	require.Equal(t, "ident", fields[0].ColumnAsName.O)
	require.NoError(t, result.Close())

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

func TestPointGetOverflow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
func TestPointGetDataTooLong(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
func TestIssue25489(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	require.False(t, tk.HasPlan(query, "Batch_Point_Get"))
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
	require.False(t, tk.HasPlan(query, "Batch_Point_Get"))
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustExec("drop table if exists UK_RP16939;")
}

// issue #25320
func TestDistinctPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestPointGetBinaryPK(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestPointGetAliasTableBinaryPK(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestIndexLookupBinary(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestOverflowOrTruncated(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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

func TestIssue10448(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestIssue10677(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int1 primary key)")
	tk.MustExec("insert into t values(1)")
	tk.MustQuery("desc select * from t where pk = 1.1").Check(testkit.Rows("TableDual_6 0.00 root  rows:0"))
	tk.MustQuery("select * from t where pk = 1.1").Check(testkit.Rows())
	tk.MustQuery("desc select * from t where pk = '1.1'").Check(testkit.Rows("TableDual_6 0.00 root  rows:0"))
	tk.MustQuery("select * from t where pk = '1.1'").Check(testkit.Rows())
	tk.MustQuery("desc select * from t where pk = 1").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:1"))
	tk.MustQuery("select * from t where pk = 1").Check(testkit.Rows("1"))
	tk.MustQuery("desc select * from t where pk = '1'").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:1"))
	tk.MustQuery("select * from t where pk = '1'").Check(testkit.Rows("1"))
	tk.MustQuery("desc select * from t where pk = '1.0'").Check(testkit.Rows("Point_Get_1 1.00 root table:t handle:1"))
	tk.MustQuery("select * from t where pk = '1.0'").Check(testkit.Rows("1"))
}

func TestForUpdateRetry(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	_, err := tk.Exec("drop table if exists t")
	require.NoError(t, err)
	tk.MustExec("create table t(pk int primary key, c int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustQuery("select * from t where pk = 1 for update")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("update t set c = c + 1 where pk = 1")
	tk.MustExec("update t set c = c + 1 where pk = 2")
	_, err = tk.Exec("commit")
	require.True(t, session.ErrForUpdateCantRetry.Equal(err))
}

func TestPointGetByRowID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(20), b int)")
	tk.MustExec("insert into t values(\"aaa\", 12)")
	tk.MustQuery("explain format = 'brief' select * from t where t._tidb_rowid = 1").Check(testkit.Rows(
		"Point_Get 1.00 root table:t handle:1"))
	tk.MustQuery("select * from t where t._tidb_rowid = 1").Check(testkit.Rows("aaa 12"))
}

func TestPointGetBinaryLiteralString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(255) charset gbk primary key /*T![clustered_index] clustered */, b int);")
	tk.MustExec("insert into t values ('你好', 1);")
	tk.MustPointGet("select * from t where a = 0xC4E3BAC3;").Check(testkit.Rows("你好 1"))
}

func TestSelectCheckVisibility(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) key, b int,index idx(b))")
	tk.MustExec("insert into t values('1',1)")
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	ts := txn.StartTS()
	sessionStore := tk.Session().GetStore().(tikv.Storage)
	// Update gc safe time for check data visibility.
	sessionStore.UpdateSPCache(ts+1, time.Now())
	checkSelectResultError := func(sql string, expectErr *terror.Error) {
		re, err := tk.Exec(sql)
		require.NoError(t, err)
		_, err = session.ResultSetToStringSlice(context.Background(), tk.Session(), re)
		require.Error(t, err)
		require.True(t, expectErr.Equal(err))
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

func TestReturnValues(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t (a varchar(64) primary key, b int)")
	tk.MustExec("insert t values ('a', 1), ('b', 2), ('c', 3)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t where a = 'b' for update").Check(testkit.Rows("b 2"))
	tid := external.GetTableByName(t, tk, "test", "t").Meta().ID
	idxVal, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, nil, types.NewStringDatum("b"))
	require.NoError(t, err)
	pk := tablecodec.EncodeIndexSeekKey(tid, 1, idxVal)
	txnCtx := tk.Session().GetSessionVars().TxnCtx
	val, ok := txnCtx.GetKeyInPessimisticLockCache(pk)
	require.True(t, ok)
	handle, err := tablecodec.DecodeHandleInUniqueIndexValue(val, false)
	require.NoError(t, err)
	rowKey := tablecodec.EncodeRowKeyWithHandle(tid, handle)
	_, ok = txnCtx.GetKeyInPessimisticLockCache(rowKey)
	require.True(t, ok)
	tk.MustExec("rollback")
}

func TestClusterIndexPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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

func TestClusterIndexCBOPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
	pointGetSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		plan := tk.MustQuery("explain format = 'brief' " + tt)
		res := tk.MustQuery(tt).Sort()
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			output[i].Res = testdata.ConvertRowsToStrings(res.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
		res.Check(testkit.Rows(output[i].Res...))
	}
}

func mustExecDDL(tk *testkit.TestKit, t *testing.T, sql string, dom *domain.Domain) {
	tk.MustExec(sql)
	require.NoError(t, dom.Reload())
}

func TestMemCacheReadLock(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Session().GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")

	// Simply check the cached results.
	mustExecDDL(tk, t, "lock tables point read", dom)
	tk.MustQuery("select id from point where id = 1").Check(testkit.Rows("1"))
	tk.MustQuery("select id from point where id = 1").Check(testkit.Rows("1"))
	mustExecDDL(tk, t, "unlock tables", dom)

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
		mustExecDDL(tk, t, "lock tables point read", dom)

		rows := tk.MustQuery(ca.sql).Rows()
		require.Lenf(t, rows, 1, "%v", ca.sql)
		explain := fmt.Sprintf("%v", rows[0])
		require.Regexp(t, ".*num_rpc.*", explain)

		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		ok := strings.Contains(explain, "num_rpc")
		require.Equalf(t, ok, ca.r1, "%v", ca.sql)
		mustExecDDL(tk, t, "unlock tables", dom)

		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		require.Regexp(t, ".*num_rpc.*", explain)

		// Test cache release after unlocking tables.
		mustExecDDL(tk, t, "lock tables point read", dom)
		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		require.Regexp(t, ".*num_rpc.*", explain)

		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		ok = strings.Contains(explain, "num_rpc")
		require.Equal(t, ok, ca.r2, "%v", ca.sql)

		mustExecDDL(tk, t, "unlock tables", dom)
		mustExecDDL(tk, t, "lock tables point read", dom)

		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		require.Regexp(t, ".*num_rpc.*", explain)

		mustExecDDL(tk, t, "unlock tables", dom)
	}
}

func TestPartitionMemCacheReadLock(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Session().GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int unique key, c int, d varchar(10)) partition by hash (id) partitions 4")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")

	// Confirm _tidb_rowid will not be duplicated.
	tk.MustQuery("select distinct(_tidb_rowid) from point order by _tidb_rowid").Check(testkit.Rows("1", "2"))

	mustExecDDL(tk, t, "lock tables point read", dom)

	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows("1"))
	mustExecDDL(tk, t, "unlock tables", dom)

	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows("1"))
	tk.MustExec("update point set id = -id")

	// Test cache release after unlocking tables.
	mustExecDDL(tk, t, "lock tables point read", dom)
	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows())

	tk.MustQuery("select _tidb_rowid from point where id = -1").Check(testkit.Rows("1"))
	tk.MustQuery("select _tidb_rowid from point where id = -1").Check(testkit.Rows("1"))
	tk.MustQuery("select _tidb_rowid from point where id = -2").Check(testkit.Rows("2"))

	mustExecDDL(tk, t, "unlock tables", dom)
}

func TestPointGetWriteLock(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")
	tk.MustExec("lock tables point write")
	tk.MustQuery(`select * from point where id = 1;`).Check(testkit.Rows(
		`1 1 a`,
	))
	rows := tk.MustQuery("explain analyze select * from point where id = 1").Rows()
	require.Len(t, rows, 1)
	explain := fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*num_rpc.*", explain)
	tk.MustExec("unlock tables")

	tk.MustExec("update point set c = 3 where id = 1")
	tk.MustExec("lock tables point write")
	tk.MustQuery(`select * from point where id = 1;`).Check(testkit.Rows(
		`1 3 a`,
	))
	rows = tk.MustQuery("explain analyze select * from point where id = 1").Rows()
	require.Len(t, rows, 1)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*num_rpc.*", explain)
	tk.MustExec("unlock tables")
}

func TestPointGetLockExistKey(t *testing.T) {
	testLock := func(rc bool, key string, tableName string) {
		store, clean := testkit.CreateMockStore(t)
		defer clean()
		tk1, tk2 := testkit.NewTestKit(t, store), testkit.NewTestKit(t, store)

		tk1.MustExec("use test")
		tk2.MustExec("use test")
		tk1.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly

		tk1.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
		tk1.MustExec(fmt.Sprintf("create table %s(id int, v int, k int, %s key0(id, v))", tableName, key))
		tk1.MustExec(fmt.Sprintf("insert into %s values(1, 1, 1)", tableName))

		if rc {
			tk1.MustExec("set tx_isolation = 'READ-COMMITTED'")
			tk2.MustExec("set tx_isolation = 'READ-COMMITTED'")
		}

		// select for update
		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		// lock exist key
		tk1.MustExec(fmt.Sprintf("select * from %s where id = 1 and v = 1 for update", tableName))
		// read committed will not lock non-exist key
		if rc {
			tk1.MustExec(fmt.Sprintf("select * from %s where id = 2 and v = 2 for update", tableName))
		}
		tk2.MustExec(fmt.Sprintf("insert into %s values(2, 2, 2)", tableName))
		var wg3 util.WaitGroupWrapper
		wg3.Run(func() {
			tk2.MustExec(fmt.Sprintf("insert into %s values(1, 1, 10)", tableName))
			// tk2.MustExec(fmt.Sprintf("insert into %s values(1, 1, 10)", tableName))
		})
		time.Sleep(150 * time.Millisecond)
		tk1.MustExec(fmt.Sprintf("update %s set v = 2 where id = 1 and v = 1", tableName))
		tk1.MustExec("commit")
		wg3.Wait()
		tk2.MustExec("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 2 2",
			"1 1 10",
		))

		// update
		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		// lock exist key
		tk1.MustExec(fmt.Sprintf("update %s set v = 3 where id = 2 and v = 2", tableName))
		// read committed will not lock non-exist key
		if rc {
			tk1.MustExec(fmt.Sprintf("update %s set v =4 where id = 3 and v = 3", tableName))
		}
		tk2.MustExec(fmt.Sprintf("insert into %s values(3, 3, 3)", tableName))
		var wg2 util.WaitGroupWrapper
		wg2.Run(func() {
			tk2.MustExec(fmt.Sprintf("insert into %s values(2, 2, 20)", tableName))
		})
		time.Sleep(150 * time.Millisecond)
		tk1.MustExec("commit")
		wg2.Wait()
		tk2.MustExec("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"1 1 10",
			"3 3 3",
			"2 2 20",
		))

		// delete
		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		// lock exist key
		tk1.MustExec(fmt.Sprintf("delete from %s where id = 3 and v = 3", tableName))
		// read committed will not lock non-exist key
		if rc {
			tk1.MustExec(fmt.Sprintf("delete from %s where id = 4 and v = 4", tableName))
		}
		tk2.MustExec(fmt.Sprintf("insert into %s values(4, 4, 4)", tableName))
		var wg1 util.WaitGroupWrapper
		wg1.Run(func() {
			tk2.MustExec(fmt.Sprintf("insert into %s values(3, 3, 30)", tableName))
		})
		time.Sleep(50 * time.Millisecond)
		tk1.MustExec("commit")
		wg1.Wait()
		tk2.MustExec("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"1 1 10",
			"2 2 20",
			"4 4 4",
			"3 3 30",
		))
	}

	var wg sync.WaitGroup
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
		go func(rc bool, key string, tableName string) {
			defer wg.Done()
			testLock(rc, key, tableName)
		}(one.rc, one.key, tableName)
	}
	wg.Wait()
}

func TestWithTiDBSnapshot(t *testing.T) {
	// Fix issue https://github.com/pingcap/tidb/issues/22436
	// Point get should not use math.MaxUint64 when variable @@tidb_snapshot is set.
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	tso := tk.Session().GetSessionVars().TxnCtx.StartTS
	tk.MustExec("rollback")
	require.True(t, tso > 0)

	// Insert data.
	tk.MustExec("insert into xx values (8)")

	// Change the snapshot before the tso, the inserted data should not be seen.
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%d'", tso))
	tk.MustQuery("select * from xx where id = 8").Check(testkit.Rows())

	tk.MustQuery("select * from xx").Check(testkit.Rows("1", "7"))
}

func TestPointGetIssue25167(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
