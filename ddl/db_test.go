// Copyright 2022 PingCAP, Inc.
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
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	parsertypes "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	// waitForCleanDataRound indicates how many times should we check data is cleaned or not.
	waitForCleanDataRound = 150
	// waitForCleanDataInterval is a min duration between 2 check for data clean.
	waitForCleanDataInterval = time.Millisecond * 100
)

const defaultBatchSize = 1024
const defaultReorgBatchSize = 256

const dbTestLease = 600 * time.Millisecond

// Close issue #24580
// See https://github.com/pingcap/tidb/issues/24580
func TestIssue24580(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(250) default null);")
	tk.MustExec("insert into t values();")
	tk.MustGetErrMsg("alter table t modify a char not null;", "[ddl:1265]Data truncated for column 'a' at row 1")
	tk.MustExec("drop table if exists t")
}

// Close issue #27862 https://github.com/pingcap/tidb/issues/27862
// Ref: https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html#data-types-storage-reqs-strings
// text(100) in utf8mb4 charset needs max 400 byte length, thus tinytext is not enough.
func TestCreateTextAdjustLen(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a text(100));")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` text DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("alter table t add b text(100);")
	tk.MustExec("alter table t add c text;")
	tk.MustExec("alter table t add d text(50);")
	tk.MustExec("alter table t change column a a text(50);")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` tinytext DEFAULT NULL,\n"+
		"  `b` text DEFAULT NULL,\n"+
		"  `c` text DEFAULT NULL,\n"+
		"  `d` tinytext DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Ref: https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
	// TINYBLOB, TINYTEXT	L + 1 bytes, where L < 2^8
	// BLOB, TEXT	L + 2 bytes, where L < 2^16
	// MEDIUMBLOB, MEDIUMTEXT	L + 3 bytes, where L < 2^24
	// LONGBLOB, LONGTEXT	L + 4 bytes, where L < 2^32
	tk.MustExec("alter table t change column d d text(100);")
	tk.MustExec("alter table t change column c c text(30000);")
	tk.MustExec("alter table t change column b b text(10000000);")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` tinytext DEFAULT NULL,\n"+
		"  `b` longtext DEFAULT NULL,\n"+
		"  `c` mediumtext DEFAULT NULL,\n"+
		"  `d` text DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table if exists t")
}

func TestGetTimeZone(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	testCases := []struct {
		tzSQL  string
		tzStr  string
		tzName string
		offset int
		err    string
	}{
		{"set time_zone = '+00:00'", "", "UTC", 0, ""},
		{"set time_zone = '-00:00'", "", "UTC", 0, ""},
		{"set time_zone = 'UTC'", "UTC", "UTC", 0, ""},
		{"set time_zone = '+05:00'", "", "UTC", 18000, ""},
		{"set time_zone = '-08:00'", "", "UTC", -28800, ""},
		{"set time_zone = '+08:00'", "", "UTC", 28800, ""},
		{"set time_zone = 'Asia/Shanghai'", "Asia/Shanghai", "Asia/Shanghai", 0, ""},
		{"set time_zone = 'SYSTEM'", "Asia/Shanghai", "Asia/Shanghai", 0, ""},
		{"set time_zone = DEFAULT", "Asia/Shanghai", "Asia/Shanghai", 0, ""},
		{"set time_zone = 'GMT'", "GMT", "GMT", 0, ""},
		{"set time_zone = 'GMT+1'", "GMT", "GMT", 0, "[variable:1298]Unknown or incorrect time zone: 'GMT+1'"},
		{"set time_zone = 'Etc/GMT+12'", "Etc/GMT+12", "Etc/GMT+12", 0, ""},
		{"set time_zone = 'Etc/GMT-12'", "Etc/GMT-12", "Etc/GMT-12", 0, ""},
		{"set time_zone = 'EST'", "EST", "EST", 0, ""},
		{"set time_zone = 'Australia/Lord_Howe'", "Australia/Lord_Howe", "Australia/Lord_Howe", 0, ""},
	}
	for _, tc := range testCases {
		if tc.err != "" {
			tk.MustGetErrMsg(tc.tzSQL, tc.err)
		} else {
			tk.MustExec(tc.tzSQL)
		}
		require.Equal(t, tc.tzStr, tk.Session().GetSessionVars().TimeZone.String(), fmt.Sprintf("sql: %s", tc.tzSQL))
		tz, offset := ddlutil.GetTimeZone(tk.Session())
		require.Equal(t, tz, tc.tzName, fmt.Sprintf("sql: %s, offset: %d", tc.tzSQL, offset))
		require.Equal(t, offset, tc.offset, fmt.Sprintf("sql: %s", tc.tzSQL))
	}
}

// for issue #30328
func TestTooBigFieldLengthAutoConvert(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	err := tk.ExecToErr("create table i30328_1(a varbinary(70000), b varchar(70000000))")
	require.True(t, types.ErrTooBigFieldLength.Equal(err))

	// save previous sql_mode and change
	r := tk.MustQuery("select @@sql_mode")
	defer func(sqlMode string) {
		tk.MustExec("set @@sql_mode= '" + sqlMode + "'")
		tk.MustExec("drop table if exists i30328_1")
		tk.MustExec("drop table if exists i30328_2")
	}(r.Rows()[0][0].(string))
	tk.MustExec("set @@sql_mode='NO_ENGINE_SUBSTITUTION'")

	tk.MustExec("create table i30328_1(a varbinary(70000), b varchar(70000000))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1246 Converting column 'a' from VARBINARY to BLOB", "Warning 1246 Converting column 'b' from VARCHAR to TEXT"))
	tk.MustExec("create table i30328_2(a varchar(200))")
	tk.MustExec("alter table i30328_2 modify a varchar(70000000);")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1246 Converting column 'a' from VARCHAR to TEXT"))
}

// For Close issue #24288
// see https://github.com/pingcap/tidb/issues/24288
func TestDdlMaxLimitOfIdentifier(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	// create/drop database test
	longDbName := strings.Repeat("库", mysql.MaxDatabaseNameLength-1)
	tk.MustExec(fmt.Sprintf("create database %s", longDbName))
	defer func() {
		tk.MustExec(fmt.Sprintf("drop database %s", longDbName))
	}()
	tk.MustExec(fmt.Sprintf("use %s", longDbName))

	// create/drop table,index test
	longTblName := strings.Repeat("表", mysql.MaxTableNameLength-1)
	longColName := strings.Repeat("三", mysql.MaxColumnNameLength-1)
	longIdxName := strings.Repeat("索", mysql.MaxIndexIdentifierLen-1)
	tk.MustExec(fmt.Sprintf("create table %s(f1 int primary key,f2 int, %s varchar(50))", longTblName, longColName))
	tk.MustExec(fmt.Sprintf("create index %s on %s(%s)", longIdxName, longTblName, longColName))
	defer func() {
		tk.MustExec(fmt.Sprintf("drop index %s on %s", longIdxName, longTblName))
		tk.MustExec(fmt.Sprintf("drop table %s", longTblName))
	}()

	// alter table
	tk.MustExec(fmt.Sprintf("alter table %s change f2 %s int", longTblName, strings.Repeat("二", mysql.MaxColumnNameLength-1)))

}

// Close issue #23321.
// See https://github.com/pingcap/tidb/issues/23321
func TestJsonUnmarshalErrWhenPanicInCancellingPath(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists test_add_index_after_add_col")
	tk.MustExec("create table test_add_index_after_add_col(a int, b int not null default '0');")
	tk.MustExec("insert into test_add_index_after_add_col values(1, 2),(2,2);")
	tk.MustExec("alter table test_add_index_after_add_col add column c int not null default '0';")
	tk.MustGetErrMsg("alter table test_add_index_after_add_col add unique index cc(c);", "[kv:1062]Duplicate entry '0' for key 'cc'")
}

func TestIssue22819(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test;")
	tk1.MustExec("create table t1 (v int) partition by hash (v) partitions 2")
	tk1.MustExec("insert into t1 values (1)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test;")
	tk1.MustExec("begin")
	tk1.MustExec("update t1 set v = 2 where v = 1")

	tk2.MustExec("alter table t1 truncate partition p0")

	err := tk1.ExecToErr("commit")
	require.Error(t, err)
	require.Regexp(t, ".*8028.*Information schema is changed during the execution of the statement.*", err.Error())
}

func TestIssue22307(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values(1, 1);")

	hook := &ddl.TestDDLCallback{Do: dom}
	var checkErr1, checkErr2 error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		_, checkErr1 = tk.Exec("update t set a = 3 where b = 1;")
		_, checkErr2 = tk.Exec("update t set a = 3 order by b;")
	}
	dom.DDL().SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(store, "alter table t drop column b;", done)
	err := <-done
	require.NoError(t, err)
	require.EqualError(t, checkErr1, "[planner:1054]Unknown column 'b' in 'where clause'")
	require.EqualError(t, checkErr2, "[planner:1054]Unknown column 'b' in 'order clause'")
}

func TestIssue9100(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table employ (a int, b int) partition by range (b) (partition p0 values less than (1));")
	tk.MustGetErrMsg("alter table employ add unique index p_a (a);", "[ddl:1503]A UNIQUE INDEX must include all columns in the table's partitioning function")
	tk.MustGetErrMsg("alter table employ add primary key p_a (a);", "[ddl:1503]A PRIMARY must include all columns in the table's partitioning function")

	tk.MustExec("create table issue9100t1 (col1 int not null, col2 date not null, col3 int not null, unique key (col1, col2)) partition by range( col1 ) (partition p1 values less than (11))")
	tk.MustExec("alter table issue9100t1 add unique index p_col1 (col1)")
	tk.MustExec("alter table issue9100t1 add primary key p_col1 (col1)")

	tk.MustExec("create table issue9100t2 (col1 int not null, col2 date not null, col3 int not null, unique key (col1, col3)) partition by range( col1 + col3 ) (partition p1 values less than (11))")
	tk.MustGetErrMsg("alter table issue9100t2 add unique index p_col1 (col1)", "[ddl:1503]A UNIQUE INDEX must include all columns in the table's partitioning function")
	tk.MustGetErrMsg("alter table issue9100t2 add primary key p_col1 (col1)", "[ddl:1503]A PRIMARY must include all columns in the table's partitioning function")
}

func TestIssue22207(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("set @@session.tidb_enable_exchange_partition = 1;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t1(id char(10)) partition by list columns(id) (partition p0 values in ('a'), partition p1 values in ('b'));")
	tk.MustExec("insert into t1 VALUES('a')")
	tk.MustExec("create table t2(id char(10));")
	tk.MustExec("ALTER TABLE t1 EXCHANGE PARTITION p0 WITH TABLE t2;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("a"))
	require.Len(t, tk.MustQuery("select * from t1").Rows(), 0)

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t1 (id int) partition by list (id) (partition p0 values in (1,2,3), partition p1 values in (4,5,6));")
	tk.MustExec("insert into t1 VALUES(1);")
	tk.MustExec("insert into t1 VALUES(2);")
	tk.MustExec("insert into t1 VALUES(3);")
	tk.MustExec("create table t2(id int);")
	tk.MustExec("ALTER TABLE t1 EXCHANGE PARTITION p0 WITH TABLE t2;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "2", "3"))
	require.Len(t, tk.MustQuery("select * from t1").Rows(), 0)
	tk.MustExec("set @@session.tidb_enable_exchange_partition = 0;")
}

func TestIssue23473(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_23473;")
	tk.MustExec("create table t_23473 (k int primary key, v int)")
	tk.MustExec("alter table t_23473 change column k k bigint")

	tbl := external.GetTableByName(t, tk, "test", "t_23473")
	require.True(t, mysql.HasNoDefaultValueFlag(tbl.Cols()[0].GetFlag()))
}

func TestDropCheck(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists drop_check")
	tk.MustExec("create table drop_check (pk int primary key)")
	defer tk.MustExec("drop table if exists drop_check")
	tk.MustExec("alter table drop_check drop check crcn")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|8231|DROP CHECK is not supported"))
}

func TestAlterOrderBy(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table ob (pk int primary key, c int default 1, c1 int default 1, KEY cl(c1))")

	// Test order by with primary key
	tk.MustExec("alter table ob order by c")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1105|ORDER BY ignored as there is a user-defined clustered index in the table 'ob'"))

	// Test order by with no primary key
	tk.MustExec("drop table if exists ob")
	tk.MustExec("create table ob (c int default 1, c1 int default 1, KEY cl(c1))")
	tk.MustExec("alter table ob order by c")
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustExec("drop table if exists ob")
}

func TestFKOnGeneratedColumns(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// test add foreign key to generated column

	// foreign key constraint cannot be defined on a virtual generated column.
	tk.MustExec("create table t1 (a int primary key);")
	tk.MustGetErrCode("create table t2 (a int, b int as (a+1) virtual, foreign key (b) references t1(a));", errno.ErrCannotAddForeign)
	tk.MustExec("create table t2 (a int, b int generated always as (a+1) virtual);")
	tk.MustGetErrCode("alter table t2 add foreign key (b) references t1(a);", errno.ErrCannotAddForeign)
	tk.MustExec("drop table t1, t2;")

	// foreign key constraint can be defined on a stored generated column.
	tk.MustExec("create table t2 (a int primary key);")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored, foreign key (b) references t2(a));")
	tk.MustExec("create table t3 (a int, b int generated always as (a+1) stored);")
	tk.MustExec("alter table t3 add foreign key (b) references t2(a);")
	tk.MustExec("drop table t1, t2, t3;")

	// foreign key constraint can reference a stored generated column.
	tk.MustExec("create table t1 (a int, b int generated always as (a+1) stored primary key);")
	tk.MustExec("create table t2 (a int, foreign key (a) references t1(b));")
	tk.MustExec("create table t3 (a int);")
	tk.MustExec("alter table t3 add foreign key (a) references t1(b);")
	tk.MustExec("drop table t1, t2, t3;")

	// rejected FK options on stored generated columns
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on update set null);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on update cascade);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on update set default);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on delete set null);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on delete set default);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustExec("create table t2 (a int primary key);")
	tk.MustExec("create table t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update set null;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update cascade;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update set default;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on delete set null;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on delete set default;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustExec("drop table t1, t2;")
	// column name with uppercase characters
	tk.MustGetErrCode("create table t1 (A int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on update set null);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustExec("create table t2 (a int primary key);")
	tk.MustExec("create table t1 (A int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update set null;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustExec("drop table t1, t2;")

	// special case: TiDB error different from MySQL 8.0
	// MySQL: ERROR 3104 (HY000): Cannot define foreign key with ON UPDATE SET NULL clause on a generated column.
	// TiDB:  ERROR 1146 (42S02): Table 'test.t2' doesn't exist
	tk.MustExec("create table t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update set null;", errno.ErrNoSuchTable)
	tk.MustExec("drop table t1;")

	// allowed FK options on stored generated columns
	tk.MustExec("create table t1 (a int primary key, b char(5));")
	tk.MustExec("create table t2 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on update restrict);")
	tk.MustExec("create table t3 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on update no action);")
	tk.MustExec("create table t4 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete restrict);")
	tk.MustExec("create table t5 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete cascade);")
	tk.MustExec("create table t6 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete no action);")
	tk.MustExec("drop table t2,t3,t4,t5,t6;")
	tk.MustExec("create table t2 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t2 add foreign key (b) references t1(a) on update restrict;")
	tk.MustExec("create table t3 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t3 add foreign key (b) references t1(a) on update no action;")
	tk.MustExec("create table t4 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t4 add foreign key (b) references t1(a) on delete restrict;")
	tk.MustExec("create table t5 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t5 add foreign key (b) references t1(a) on delete cascade;")
	tk.MustExec("create table t6 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t6 add foreign key (b) references t1(a) on delete no action;")
	tk.MustExec("drop table t1,t2,t3,t4,t5,t6;")

	// rejected FK options on the base columns of a stored generated columns
	tk.MustExec("create table t2 (a int primary key);")
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on update set null);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on update cascade);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on update set default);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete set null);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete cascade);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete set default);", errno.ErrCannotAddForeign)
	tk.MustExec("create table t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on update set null;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on update cascade;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on update set default;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on delete set null;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on delete cascade;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on delete set default;", errno.ErrCannotAddForeign)
	tk.MustExec("drop table t1, t2;")

	// allowed FK options on the base columns of a stored generated columns
	tk.MustExec("create table t1 (a int primary key, b char(5));")
	tk.MustExec("create table t2 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on update restrict);")
	tk.MustExec("create table t3 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on update no action);")
	tk.MustExec("create table t4 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on delete restrict);")
	tk.MustExec("create table t5 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on delete no action);")
	tk.MustExec("drop table t2,t3,t4,t5")
	tk.MustExec("create table t2 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t2 add foreign key (a) references t1(a) on update restrict;")
	tk.MustExec("create table t3 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t3 add foreign key (a) references t1(a) on update no action;")
	tk.MustExec("create table t4 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t4 add foreign key (a) references t1(a) on delete restrict;")
	tk.MustExec("create table t5 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t5 add foreign key (a) references t1(a) on delete no action;")
	tk.MustExec("drop table t1,t2,t3,t4,t5;")
}

func TestSelectInViewFromAnotherDB(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_db2")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int)")
	tk.MustExec("use test_db2")
	tk.MustExec("create sql security invoker view v as select * from test.t")
	tk.MustExec("use test")
	tk.MustExec("select test_db2.v.a from test_db2.v")
}

func TestAddConstraintCheck(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists add_constraint_check")
	tk.MustExec("create table add_constraint_check (pk int primary key, a int)")
	defer tk.MustExec("drop table if exists add_constraint_check")
	tk.MustExec("alter table add_constraint_check add constraint crn check (a > 1)")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|8231|ADD CONSTRAINT CHECK is not supported"))
}

func TestCreateTableIgnoreCheckConstraint(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_constraint_check")
	tk.MustExec("CREATE TABLE admin_user (enable bool, CHECK (enable IN (0, 1)));")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|8231|CONSTRAINT CHECK is not supported"))
	tk.MustQuery("show create table admin_user").Check(testkit.RowsWithSep("|", ""+
		"admin_user CREATE TABLE `admin_user` (\n"+
		"  `enable` tinyint(1) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestAutoConvertBlobTypeByLength(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	sql := fmt.Sprintf("create table t0(c0 Blob(%d), c1 Blob(%d), c2 Blob(%d), c3 Blob(%d))",
		255-1, 65535-1, 16777215-1, 4294967295-1)
	tk.MustExec(sql)

	var tableID int64
	rs := tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where table_name='t0' and table_schema='test';")
	tableIDi, _ := strconv.Atoi(rs.Rows()[0][0].(string))
	tableID = int64(tableIDi)

	tbl, exist := dom.InfoSchema().TableByID(tableID)
	require.True(t, exist)

	require.Equal(t, tbl.Cols()[0].GetType(), mysql.TypeTinyBlob)
	require.Equal(t, tbl.Cols()[0].GetFlen(), 255)
	require.Equal(t, tbl.Cols()[1].GetType(), mysql.TypeBlob)
	require.Equal(t, tbl.Cols()[1].GetFlen(), 65535)
	require.Equal(t, tbl.Cols()[2].GetType(), mysql.TypeMediumBlob)
	require.Equal(t, tbl.Cols()[2].GetFlen(), 16777215)
	require.Equal(t, tbl.Cols()[3].GetType(), mysql.TypeLongBlob)
	require.Equal(t, tbl.Cols()[3].GetFlen(), 4294967295)
}

func TestAddExpressionIndexRollback(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
	tk.MustExec("insert into t1 values (20, 20, 20), (40, 40, 40), (80, 80, 80), (160, 160, 160);")

	var checkErr error
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	d := dom.DDL()
	hook := &ddl.TestDDLCallback{Do: dom}
	var currJob *model.Job
	ctx := mock.NewContext()
	ctx.Store = store
	times := 0
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (6, 3, 3) on duplicate key update c1 = 10")
			if checkErr == nil {
				_, checkErr = tk1.Exec("update t1 set c1 = 7 where c2=6;")
			}
			if checkErr == nil {
				_, checkErr = tk1.Exec("delete from t1 where c1 = 40;")
			}
		case model.StateWriteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (2, 2, 2)")
			if checkErr == nil {
				_, checkErr = tk1.Exec("update t1 set c1 = 3 where c2 = 80")
			}
		case model.StateWriteReorganization:
			if checkErr == nil && job.SchemaState == model.StateWriteReorganization && times == 0 {
				_, checkErr = tk1.Exec("insert into t1 values (4, 4, 4)")
				if checkErr != nil {
					return
				}
				_, checkErr = tk1.Exec("update t1 set c1 = 5 where c2 = 80")
				if checkErr != nil {
					return
				}
				currJob = job
				times++
			}
		}
	}
	d.SetHook(hook)

	tk.MustGetErrMsg("alter table t1 add index expr_idx ((pow(c1, c2)));", "[ddl:8202]Cannot decode index value, because [types:1690]DOUBLE value is out of range in 'pow(160, 160)'")
	require.NoError(t, checkErr)
	tk.MustQuery("select * from t1 order by c1;").Check(testkit.Rows("2 2 2", "4 4 4", "5 80 80", "10 3 3", "20 20 20", "160 160 160"))

	// Check whether the reorg information is cleaned up.
	err := sessiontxn.NewTxn(context.Background(), ctx)
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	element, start, end, physicalID, err := m.GetDDLReorgHandle(currJob)
	require.True(t, meta.ErrDDLReorgElementNotExist.Equal(err))
	require.Nil(t, element)
	require.Nil(t, start)
	require.Nil(t, end)
	require.Equal(t, int64(0), physicalID)
}

func TestDropTableOnTiKVDiskFull(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test_disk_full_drop_table(a int);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcTiKVAllowedOnAlmostFull", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcTiKVAllowedOnAlmostFull"))
	}()
	tk.MustExec("drop table test_disk_full_drop_table;")
}

func TestComment(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ct, ct1")

	validComment := strings.Repeat("a", 1024)
	invalidComment := strings.Repeat("b", 1025)
	validTableComment := strings.Repeat("a", 2048)
	invalidTableComment := strings.Repeat("b", 2049)

	// test table comment
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (c integer) COMMENT = '" + validTableComment + "'")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (c integer)")
	tk.MustExec("ALTER TABLE t COMMENT = '" + validTableComment + "'")

	// test column comment
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (c integer NOT NULL COMMENT '" + validComment + "')")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (c integer)")
	tk.MustExec("ALTER TABLE t ADD COLUMN c1 integer COMMENT '" + validComment + "'")

	// test index comment
	tk.MustExec("create table ct (c int, d int, e int, key (c) comment '" + validComment + "')")
	tk.MustExec("create index i on ct (d) comment '" + validComment + "'")
	tk.MustExec("alter table ct add key (e) comment '" + validComment + "'")

	// test table partition comment
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (0) COMMENT '" + validComment + "')")
	tk.MustExec("ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (1000000) COMMENT '" + validComment + "')")

	// test table comment
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustGetErrCode("CREATE TABLE t (c integer) COMMENT = '"+invalidTableComment+"'", errno.ErrTooLongTableComment)
	tk.MustExec("CREATE TABLE t (c integer)")
	tk.MustGetErrCode("ALTER TABLE t COMMENT = '"+invalidTableComment+"'", errno.ErrTooLongTableComment)

	// test column comment
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustGetErrCode("CREATE TABLE t (c integer NOT NULL COMMENT '"+invalidComment+"')", errno.ErrTooLongFieldComment)
	tk.MustExec("CREATE TABLE t (c integer)")
	tk.MustGetErrCode("ALTER TABLE t ADD COLUMN c1 integer COMMENT '"+invalidComment+"'", errno.ErrTooLongFieldComment)

	// test index comment
	tk.MustGetErrCode("create table ct1 (c int, key (c) comment '"+invalidComment+"')", errno.ErrTooLongIndexComment)
	tk.MustGetErrCode("create index i1 on ct (d) comment '"+invalidComment+"'", errno.ErrTooLongIndexComment)
	tk.MustGetErrCode("alter table ct add key (e) comment '"+invalidComment+"'", errno.ErrTooLongIndexComment)

	// test table partition comment
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustGetErrCode("CREATE TABLE t (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (0) COMMENT '"+invalidComment+"')", errno.ErrTooLongTablePartitionComment)
	tk.MustExec("CREATE TABLE t (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (0) COMMENT '" + validComment + "')")
	tk.MustGetErrCode("ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (1000000) COMMENT '"+invalidComment+"')", errno.ErrTooLongTablePartitionComment)

	tk.MustExec("set @@sql_mode=''")

	// test table comment
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (c integer) COMMENT = '" + invalidTableComment + "'")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1628|Comment for table 't' is too long (max = 2048)"))
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (c integer)")
	tk.MustExec("ALTER TABLE t COMMENT = '" + invalidTableComment + "'")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1628|Comment for table 't' is too long (max = 2048)"))

	// test column comment
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (c integer NOT NULL COMMENT '" + invalidComment + "')")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1629|Comment for field 'c' is too long (max = 1024)"))
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (c integer)")
	tk.MustExec("ALTER TABLE t ADD COLUMN c1 integer COMMENT '" + invalidComment + "'")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1629|Comment for field 'c1' is too long (max = 1024)"))

	// test index comment
	tk.MustExec("create table ct1 (c int, d int, e int, key (c) comment '" + invalidComment + "')")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1688|Comment for index 'c' is too long (max = 1024)"))
	tk.MustExec("create index i1 on ct1 (d) comment '" + invalidComment + "b" + "'")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1688|Comment for index 'i1' is too long (max = 1024)"))
	tk.MustExec("alter table ct1 add key (e) comment '" + invalidComment + "'")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1688|Comment for index 'e' is too long (max = 1024)"))

	tk.MustExec("drop table if exists ct, ct1")
}

func TestRebaseAutoID(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange")) }()

	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists tidb;")
	tk.MustExec("create database tidb;")
	tk.MustExec("use tidb;")
	tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1"))
	tk.MustExec("alter table tidb.test auto_increment = 6000;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1"))
	tk.MustExec("alter table tidb.test auto_increment = 5;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1"))

	// Current range for table test is [11000, 15999].
	// Though it does not have a tuple "a = 15999", its global next auto increment id should be 16000.
	// Anyway it is not compatible with MySQL.
	tk.MustExec("alter table tidb.test auto_increment = 12000;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1", "16000 1"))

	tk.MustExec("create table tidb.test2 (a int);")
	tk.MustGetErrCode("alter table tidb.test2 add column b int auto_increment key, auto_increment=10;", errno.ErrUnsupportedDDLOperation)
}

func TestProcessColumnFlags(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// check `processColumnFlags()`
	tk.MustExec("create table t(a year(4) comment 'xxx', b year, c bit)")
	defer tk.MustExec("drop table t;")

	check := func(n string, f func(uint) bool) {
		tbl := external.GetTableByName(t, tk, "test", "t")
		for _, col := range tbl.Cols() {
			if strings.EqualFold(col.Name.L, n) {
				require.True(t, f(col.GetFlag()))
				break
			}
		}
	}

	yearcheck := func(f uint) bool {
		return mysql.HasUnsignedFlag(f) && mysql.HasZerofillFlag(f) && !mysql.HasBinaryFlag(f)
	}

	tk.MustExec("alter table t modify a year(4)")
	check("a", yearcheck)

	tk.MustExec("alter table t modify a year(4) unsigned")
	check("a", yearcheck)

	tk.MustExec("alter table t modify a year(4) zerofill")

	tk.MustExec("alter table t modify b year")
	check("b", yearcheck)

	tk.MustExec("alter table t modify c bit")
	check("c", func(f uint) bool {
		return mysql.HasUnsignedFlag(f) && !mysql.HasBinaryFlag(f)
	})
}

func TestForbidCacheTableForSystemTable(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	sysTables := make([]string, 0, 24)
	memOrSysDB := []string{"MySQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "METRICS_SCHEMA"}
	for _, db := range memOrSysDB {
		tk.MustExec("use " + db)
		tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)
		rows := tk.MustQuery("show tables").Rows()
		for i := 0; i < len(rows); i++ {
			sysTables = append(sysTables, rows[i][0].(string))
		}
		for _, one := range sysTables {
			err := tk.ExecToErr(fmt.Sprintf("alter table `%s` cache", one))
			if db == "MySQL" {
				require.EqualError(t, err, "[ddl:8200]ALTER table cache for tables in system database is currently unsupported")
			} else {
				require.EqualError(t, err, fmt.Sprintf("[planner:1142]ALTER command denied to user 'root'@'%%' for table '%s'", strings.ToLower(one)))
			}

		}
		sysTables = sysTables[:0]
	}
}

func TestAlterShardRowIDBits(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
	}()

	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	// Test alter shard_row_id_bits
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 5")
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	tk.MustExec("insert into t1 set a=1;")

	// Test increase shard_row_id_bits failed by overflow global auto ID.
	tk.MustGetErrMsg("alter table t1 SHARD_ROW_ID_BITS = 10;", "[autoid:1467]shard_row_id_bits 10 will cause next global auto ID 72057594037932936 overflow")

	// Test reduce shard_row_id_bits will be ok.
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 3;")
	checkShardRowID := func(maxShardRowIDBits, shardRowIDBits uint64) {
		tbl := external.GetTableByName(t, tk, "test", "t1")
		require.True(t, tbl.Meta().MaxShardRowIDBits == maxShardRowIDBits)
		require.True(t, tbl.Meta().ShardRowIDBits == shardRowIDBits)
	}
	checkShardRowID(5, 3)

	// Test reduce shard_row_id_bits but calculate overflow should use the max record shard_row_id_bits.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 10")
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 5;")
	checkShardRowID(10, 5)
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	tk.MustGetErrMsg("insert into t1 set a=1;", "[autoid:1467]Failed to read auto-increment value from storage engine")
}

func TestShardRowIDBitsOnTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// for global temporary table
	tk.MustExec("drop table if exists shard_row_id_temporary")
	tk.MustGetErrMsg(
		"create global temporary table shard_row_id_temporary (a int) shard_row_id_bits = 5 on commit delete rows;",
		core.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	tk.MustExec("create global temporary table shard_row_id_temporary (a int) on commit delete rows;")
	tk.MustGetErrMsg(
		"alter table shard_row_id_temporary shard_row_id_bits = 4;",
		dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	// for local temporary table
	tk.MustExec("drop table if exists local_shard_row_id_temporary")
	tk.MustGetErrMsg(
		"create temporary table local_shard_row_id_temporary (a int) shard_row_id_bits = 5;",
		core.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	tk.MustExec("create temporary table local_shard_row_id_temporary (a int);")
	tk.MustGetErrMsg(
		"alter table local_shard_row_id_temporary shard_row_id_bits = 4;",
		dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ALTER TABLE").Error())
}

func TestDDLJobErrorCount(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddl_error_table, new_ddl_error_table")
	tk.MustExec("create table ddl_error_table(a int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockErrEntrySizeTooLarge", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockErrEntrySizeTooLarge"))
	}()

	var jobID int64
	hook := &ddl.TestDDLCallback{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		jobID = job.ID
	}
	originHook := dom.DDL().GetHook()
	dom.DDL().SetHook(hook)
	defer dom.DDL().SetHook(originHook)

	tk.MustGetErrCode("rename table ddl_error_table to new_ddl_error_table", errno.ErrEntryTooLarge)

	historyJob, err := getHistoryDDLJob(store, jobID)
	require.NoError(t, err)
	require.NotNil(t, historyJob)
	require.Equal(t, int64(1), historyJob.ErrorCount)
	require.True(t, kv.ErrEntryTooLarge.Equal(historyJob.Error))
}

func TestCommitTxnWithIndexChange(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()
	// Prepare work.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
	tk.MustExec("insert t1 values (1, 10, 100), (2, 20, 200)")
	tk.MustExec("alter table t1 add index k2(c2)")
	tk.MustExec("alter table t1 drop index k2")
	tk.MustExec("alter table t1 add index k2(c2)")
	tk.MustExec("alter table t1 drop index k2")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	// tkSQLs are the sql statements for the pessimistic transaction.
	// tk2DDL are the ddl statements executed before the pessimistic transaction.
	// idxDDL is the DDL statement executed between pessimistic transaction begin and commit.
	// failCommit means the pessimistic transaction commit should fail not.
	type caseUnit struct {
		tkSQLs     []string
		tk2DDL     []string
		idxDDL     string
		checkSQLs  []string
		rowsExps   [][]string
		failCommit bool
		stateEnd   model.SchemaState
	}

	cases := []caseUnit{
		// Test secondary index
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t2 values(11, 11, 11)"},
			[]string{"alter table t1 add index k2(c2)",
				"alter table t1 drop index k2",
				"alter table t1 add index kk2(c2, c1)",
				"alter table t1 add index k2(c2)",
				"alter table t1 drop index k2"},
			"alter table t1 add index k2(c2)",
			[]string{"select c3, c2 from t1 use index(k2) where c2 = 20",
				"select c3, c2 from t1 use index(k2) where c2 = 10",
				"select * from t1",
				"select * from t2 where c1 = 11"},
			[][]string{{"200 20"},
				{"100 10"},
				{"1 10 100", "2 20 200", "3 30 300"},
				{"11 11 11"}},
			false,
			model.StateNone},
		// Test secondary index
		{[]string{"insert into t2 values(5, 50, 500)",
			"insert into t2 values(11, 11, 11)",
			"delete from t2 where c2 = 11",
			"update t2 set c2 = 110 where c1 = 11"},
			// "update t2 set c1 = 10 where c3 = 100"},
			[]string{"alter table t1 add index k2(c2)",
				"alter table t1 drop index k2",
				"alter table t1 add index kk2(c2, c1)",
				"alter table t1 add index k2(c2)",
				"alter table t1 drop index k2"},
			"alter table t1 add index k2(c2)",
			[]string{"select c3, c2 from t1 use index(k2) where c2 = 20",
				"select c3, c2 from t1 use index(k2) where c2 = 10",
				"select * from t1",
				"select * from t2 where c1 = 11",
				"select * from t2 where c3 = 100"},
			[][]string{{"200 20"},
				{"100 10"},
				{"1 10 100", "2 20 200"},
				{},
				{"1 10 100"}},
			false,
			model.StateNone},
		// Test unique index
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t1 values(4, 40, 400)",
			"insert into t2 values(11, 11, 11)",
			"insert into t2 values(12, 12, 11)"},
			[]string{"alter table t1 add unique index uk3(c3)",
				"alter table t1 drop index uk3",
				"alter table t2 add unique index ukc1c3(c1, c3)",
				"alter table t2 add unique index ukc3(c3)",
				"alter table t2 drop index ukc1c3",
				"alter table t2 drop index ukc3",
				"alter table t2 add index kc3(c3)"},
			"alter table t1 add unique index uk3(c3)",
			[]string{"select c3, c2 from t1 use index(uk3) where c3 = 200",
				"select c3, c2 from t1 use index(uk3) where c3 = 300",
				"select c3, c2 from t1 use index(uk3) where c3 = 400",
				"select * from t1",
				"select * from t2"},
			[][]string{{"200 20"},
				{"300 30"},
				{"400 40"},
				{"1 10 100", "2 20 200", "3 30 300", "4 40 400"},
				{"1 10 100", "2 20 200", "11 11 11", "12 12 11"}},
			false, model.StateNone},
		// Test unique index fail to commit, this case needs the new index could be inserted
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t1 values(4, 40, 300)",
			"insert into t2 values(11, 11, 11)",
			"insert into t2 values(12, 11, 12)"},
			//[]string{"alter table t1 add unique index uk3(c3)", "alter table t1 drop index uk3"},
			[]string{},
			"alter table t1 add unique index uk3(c3)",
			[]string{"select c3, c2 from t1 use index(uk3) where c3 = 200",
				"select c3, c2 from t1 use index(uk3) where c3 = 300",
				"select c3, c2 from t1 where c1 = 4",
				"select * from t1",
				"select * from t2"},
			[][]string{{"200 20"},
				{},
				{},
				{"1 10 100", "2 20 200"},
				{"1 10 100", "2 20 200"}},
			true,
			model.StateWriteOnly},
	}
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))

	// Test add index state change
	do := dom.DDL()
	startStates := []model.SchemaState{model.StateNone, model.StateDeleteOnly}
	for _, startState := range startStates {
		endStatMap := session.ConstOpAddIndex[startState]
		var endStates []model.SchemaState
		for st := range endStatMap {
			endStates = append(endStates, st)
		}
		sort.Slice(endStates, func(i, j int) bool { return endStates[i] < endStates[j] })
		for _, endState := range endStates {
			for _, curCase := range cases {
				if endState < curCase.stateEnd {
					break
				}
				tk2.MustExec("drop table if exists t1")
				tk2.MustExec("drop table if exists t2")
				tk2.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
				tk2.MustExec("create table t2 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
				tk2.MustExec("insert t1 values (1, 10, 100), (2, 20, 200)")
				tk2.MustExec("insert t2 values (1, 10, 100), (2, 20, 200)")
				tk2.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))
				tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))
				tk.MustQuery("select * from t2;").Check(testkit.Rows("1 10 100", "2 20 200"))

				for _, DDLSQL := range curCase.tk2DDL {
					tk2.MustExec(DDLSQL)
				}
				hook := &ddl.TestDDLCallback{}
				prepared := false
				committed := false
				hook.OnJobUpdatedExported = func(job *model.Job) {
					if job.SchemaState == startState {
						if !prepared {
							tk.MustExec("begin pessimistic")
							for _, tkSQL := range curCase.tkSQLs {
								tk.MustExec(tkSQL)
							}
							prepared = true
						}
					} else if job.SchemaState == endState {
						if !committed {
							if curCase.failCommit {
								err := tk.ExecToErr("commit")
								require.Error(t, err)
							} else {
								tk.MustExec("commit")
							}
						}
						committed = true
					}
				}
				originalCallback := do.GetHook()
				do.SetHook(hook)
				tk2.MustExec(curCase.idxDDL)
				do.SetHook(originalCallback)
				tk2.MustExec("admin check table t1")
				for i, checkSQL := range curCase.checkSQLs {
					if len(curCase.rowsExps[i]) > 0 {
						tk2.MustQuery(checkSQL).Check(testkit.Rows(curCase.rowsExps[i]...))
					} else {
						tk2.MustQuery(checkSQL).Check(nil)
					}
				}
			}
		}
	}
	tk.MustExec("admin check table t1")
}

// TestAddIndexFailOnCaseWhenCanExit is used to close #19325.
func TestAddIndexFailOnCaseWhenCanExit(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockCaseWhenParseFailure", `return(true)`))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockCaseWhenParseFailure")) }()
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", originalVal))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustGetErrMsg("alter table t add index idx(b)", "[ddl:-1]DDL job rollback, error msg: job.ErrCount:1, mock unknown type: ast.whenClause.")
	tk.MustExec("drop table if exists t")
}

func TestCreateTableWithIntegerLengthWaring(t *testing.T) {
	// Inject the strict-integer-display-width variable in parser directly.
	parsertypes.TiDBStrictIntegerDisplayWidth = true
	defer func() { parsertypes.TiDBStrictIntegerDisplayWidth = false }()
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a tinyint(1))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a smallint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a mediumint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a integer(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int1(1))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int2(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int3(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int4(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int8(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
}

// Close issue #24172.
// See https://github.com/pingcap/tidb/issues/24172
func TestCancelJobWriteConflict(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")

	tk1.MustExec("create table t(id int)")

	var cancelErr error
	var rs []sqlexec.RecordSet
	hook := &ddl.TestDDLCallback{}
	d := dom.DDL()
	originalHook := d.GetHook()
	d.SetHook(hook)
	defer d.SetHook(originalHook)

	// Test when cancelling cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			stmt := fmt.Sprintf("admin cancel ddl jobs %d", job.ID)
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn", `return("no_retry")`))
			defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn")) }()
			rs, cancelErr = tk2.Session().Execute(context.Background(), stmt)
		}
	}
	tk1.MustExec("alter table t add index (id)")
	require.EqualError(t, cancelErr, "mock commit error")

	// Test when cancelling is retried only once and adding index is cancelled in the end.
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			jobID = job.ID
			stmt := fmt.Sprintf("admin cancel ddl jobs %d", job.ID)
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn", `return("retry_once")`))
			defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn")) }()
			rs, cancelErr = tk2.Session().Execute(context.Background(), stmt)
		}
	}
	tk1.MustGetErrCode("alter table t add index (id)", errno.ErrCancelledDDLJob)
	require.NoError(t, cancelErr)
	result := tk2.ResultSetToResultWithCtx(context.Background(), rs[0], "cancel ddl job fails")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID)))
}

func TestSnapshotVersion(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	dd := dom.DDL()
	ddl.DisableTiFlashPoll(dd)
	require.Equal(t, dbTestLease, dd.GetLease())

	snapTS := oracle.GoTimeToTS(time.Now())
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create table t(a int)")

	is := dom.InfoSchema()
	require.NotNil(t, is)

	// For updating the self schema version.
	goCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	require.NoError(t, err)

	snapIs, err := dom.GetSnapshotInfoSchema(snapTS)
	require.NotNil(t, snapIs)
	require.NoError(t, err)

	// Make sure that the self schema version doesn't be changed.
	goCtx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	require.NoError(t, err)

	// for GetSnapshotInfoSchema
	currSnapTS := oracle.GoTimeToTS(time.Now())
	currSnapIs, err := dom.GetSnapshotInfoSchema(currSnapTS)
	require.NoError(t, err)
	require.NotNil(t, currSnapTS)
	require.Equal(t, is.SchemaMetaVersion(), currSnapIs.SchemaMetaVersion())

	// for GetSnapshotMeta
	dbInfo, ok := currSnapIs.SchemaByName(model.NewCIStr("test2"))
	require.True(t, ok)

	tbl, err := currSnapIs.TableByName(model.NewCIStr("test2"), model.NewCIStr("t"))
	require.NoError(t, err)

	m, err := dom.GetSnapshotMeta(snapTS)
	require.NoError(t, err)

	tblInfo1, err := m.GetTable(dbInfo.ID, tbl.Meta().ID)
	require.True(t, meta.ErrDBNotExists.Equal(err))
	require.Nil(t, tblInfo1)

	m, err = dom.GetSnapshotMeta(currSnapTS)
	require.NoError(t, err)

	tblInfo2, err := m.GetTable(dbInfo.ID, tbl.Meta().ID)
	require.NoError(t, err)
	require.Equal(t, tblInfo2, tbl.Meta())
}

func TestSchemaValidator(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	dd := dom.DDL()
	ddl.DisableTiFlashPoll(dd)
	require.Equal(t, dbTestLease, dd.GetLease())

	tk.MustExec("create table test.t(a int)")

	err := dom.Reload()
	require.NoError(t, err)
	schemaVer := dom.InfoSchema().SchemaMetaVersion()
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)

	ts := ver.Ver
	_, res := dom.SchemaValidator.Check(ts, schemaVer, nil)
	require.Equal(t, domain.ResultSucc, res)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed", `return(true)`))

	err = dom.Reload()
	require.Error(t, err)
	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil)
	require.Equal(t, domain.ResultSucc, res)
	time.Sleep(dbTestLease)

	ver, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	ts = ver.Ver
	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil)
	require.Equal(t, domain.ResultUnknown, res)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed"))
	err = dom.Reload()
	require.NoError(t, err)

	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil)
	require.Equal(t, domain.ResultSucc, res)

	// For schema check, it tests for getting the result of "ResultUnknown".
	is := dom.InfoSchema()
	schemaChecker := domain.NewSchemaChecker(dom, is.SchemaMetaVersion(), nil)
	// Make sure it will retry one time and doesn't take a long time.
	domain.SchemaOutOfDateRetryTimes.Store(1)
	domain.SchemaOutOfDateRetryInterval.Store(time.Millisecond * 1)
	dom.SchemaValidator.Stop()
	_, err = schemaChecker.Check(uint64(123456))
	require.EqualError(t, err, domain.ErrInfoSchemaExpired.Error())
}

func TestLogAndShowSlowLog(t *testing.T) {
	_, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()

	dom.LogSlowQuery(&domain.SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})
	dom.LogSlowQuery(&domain.SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})
	dom.LogSlowQuery(&domain.SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	// Collecting slow queries is asynchronous, wait a while to ensure it's done.
	time.Sleep(5 * time.Millisecond)

	result := dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2})
	require.Len(t, result, 2)
	require.Equal(t, "bbb", result[0].SQL)
	require.Equal(t, 3*time.Second, result[0].Duration)
	require.Equal(t, "ccc", result[1].SQL)
	require.Equal(t, 2*time.Second, result[1].Duration)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2, Kind: ast.ShowSlowKindInternal})
	require.Len(t, result, 1)
	require.Equal(t, "aaa", result[0].SQL)
	require.Equal(t, time.Second, result[0].Duration)
	require.True(t, result[0].Internal)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 4, Kind: ast.ShowSlowKindAll})
	require.Len(t, result, 3)
	require.Equal(t, "bbb", result[0].SQL)
	require.Equal(t, 3*time.Second, result[0].Duration)
	require.Equal(t, "ccc", result[1].SQL)
	require.Equal(t, 2*time.Second, result[1].Duration)
	require.Equal(t, "aaa", result[2].SQL)
	require.Equal(t, time.Second, result[2].Duration)
	require.True(t, result[2].Internal)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowRecent, Count: 2})
	require.Len(t, result, 2)
	require.Equal(t, "ccc", result[0].SQL)
	require.Equal(t, 2*time.Second, result[0].Duration)
	require.Equal(t, "bbb", result[1].SQL)
	require.Equal(t, 3*time.Second, result[1].Duration)
}

func TestReportingMinStartTimestamp(t *testing.T) {
	_, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()

	infoSyncer := dom.InfoSyncer()
	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	infoSyncer.SetSessionManager(sm)
	beforeTS := oracle.GoTimeToTS(time.Now())
	infoSyncer.ReportMinStartTS(dom.Store())
	afterTS := oracle.GoTimeToTS(time.Now())
	require.False(t, infoSyncer.GetMinStartTS() > beforeTS && infoSyncer.GetMinStartTS() < afterTS)

	now := time.Now()
	validTS := oracle.GoTimeToLowerLimitStartTS(now.Add(time.Minute), tikv.MaxTxnTimeUse)
	lowerLimit := oracle.GoTimeToLowerLimitStartTS(now, tikv.MaxTxnTimeUse)
	sm.PS = []*util.ProcessInfo{
		{CurTxnStartTS: 0},
		{CurTxnStartTS: math.MaxUint64},
		{CurTxnStartTS: lowerLimit},
		{CurTxnStartTS: validTS},
	}
	infoSyncer.SetSessionManager(sm)
	infoSyncer.ReportMinStartTS(dom.Store())
	require.Equal(t, validTS, infoSyncer.GetMinStartTS())
}
