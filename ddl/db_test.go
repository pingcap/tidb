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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/stretchr/testify/require"
)

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
	tk1.MustExec("drop table if exists t1;")
	defer func() {
		tk1.MustExec("drop table if exists t1;")
	}()

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
	go backgroundExecT(store, "alter table t drop column b;", done)
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
	require.True(t, mysql.HasNoDefaultValueFlag(tbl.Cols()[0].Flag))
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

func TestAlterLock(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_index_lock (c1 int, c2 int, C3 int)")
	tk.MustExec("alter table t_index_lock add index (c1, c2), lock=none")
}

func TestComment(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	validComment := strings.Repeat("a", 1024)
	invalidComment := strings.Repeat("b", 1025)

	tk.MustExec("create table ct (c int, d int, e int, key (c) comment '" + validComment + "')")
	tk.MustExec("create index i on ct (d) comment '" + validComment + "'")
	tk.MustExec("alter table ct add key (e) comment '" + validComment + "'")

	tk.MustGetErrCode("create table ct1 (c int, key (c) comment '"+invalidComment+"')", errno.ErrTooLongIndexComment)
	tk.MustGetErrCode("create index i1 on ct (d) comment '"+invalidComment+"b"+"'", errno.ErrTooLongIndexComment)
	tk.MustGetErrCode("alter table ct add key (e) comment '"+invalidComment+"'", errno.ErrTooLongIndexComment)

	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("create table ct1 (c int, d int, e int, key (c) comment '" + invalidComment + "')")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1688|Comment for index 'c' is too long (max = 1024)"))
	tk.MustExec("create index i1 on ct1 (d) comment '" + invalidComment + "b" + "'")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1688|Comment for index 'i1' is too long (max = 1024)"))
	tk.MustExec("alter table ct1 add key (e) comment '" + invalidComment + "'")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1688|Comment for index 'e' is too long (max = 1024)"))
}

func TestIfNotExists(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int key)")

	// ADD COLUMN
	sql := "alter table t1 add column b int"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	tk.MustExec("alter table t1 add column if not exists b int")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1060|Duplicate column name 'b'"))

	// ADD INDEX
	sql = "alter table t1 add index idx_b (b)"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	tk.MustExec("alter table t1 add index if not exists idx_b (b)")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// CREATE INDEX
	sql = "create index idx_b on t1 (b)"
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	tk.MustExec("create index if not exists idx_b on t1 (b)")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// ADD PARTITION
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a int key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	sql = "alter table t2 add partition (partition p2 values less than (30))"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrSameNamePartition)
	tk.MustExec("alter table t2 add partition if not exists (partition p2 values less than (30))")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1517|Duplicate partition name p2"))
}

func TestIfExists(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int key, b int);")

	// DROP COLUMN
	sql := "alter table t1 drop column b"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	tk.MustExec("alter table t1 drop column if exists b") // only `a` exists now
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1091|Can't DROP 'b'; check that column/key exists"))

	// CHANGE COLUMN
	sql = "alter table t1 change column b c int"
	tk.MustGetErrCode(sql, errno.ErrBadField)
	tk.MustExec("alter table t1 change column if exists b c int")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1054|Unknown column 'b' in 't1'"))
	tk.MustExec("alter table t1 change column if exists a c int") // only `c` exists now

	// MODIFY COLUMN
	sql = "alter table t1 modify column a bigint"
	tk.MustGetErrCode(sql, errno.ErrBadField)
	tk.MustExec("alter table t1 modify column if exists a bigint")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1054|Unknown column 'a' in 't1'"))
	tk.MustExec("alter table t1 modify column if exists c bigint") // only `c` exists now

	// DROP INDEX
	tk.MustExec("alter table t1 add index idx_c (c)")
	sql = "alter table t1 drop index idx_c"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	tk.MustExec("alter table t1 drop index if exists idx_c")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1091|index idx_c doesn't exist"))

	// DROP PARTITION
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a int key) partition by range(a) (partition pNeg values less than (0), partition p0 values less than (10), partition p1 values less than (20))")
	sql = "alter table t2 drop partition p1"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrDropPartitionNonExistent)
	tk.MustExec("alter table t2 drop partition if exists p1")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1507|Error in list of partitions to DROP"))
}

func TestCheckTooBigFieldLength(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tr_01 (id int, name varchar(20000), purchased date )  default charset=utf8 collate=utf8_bin;")

	tk.MustExec("drop table if exists tr_02;")
	tk.MustExec("create table tr_02 (id int, name varchar(16000), purchased date )  default charset=utf8mb4 collate=utf8mb4_bin;")

	tk.MustExec("drop table if exists tr_03;")
	tk.MustExec("create table tr_03 (id int, name varchar(65534), purchased date ) default charset=latin1;")

	tk.MustExec("drop table if exists tr_04;")
	tk.MustExec("create table tr_04 (a varchar(20000) ) default charset utf8;")
	tk.MustGetErrCode("alter table tr_04 add column b varchar(20000) charset utf8mb4;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("alter table tr_04 convert to character set utf8mb4;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create table tr (id int, name varchar(30000), purchased date )  default charset=utf8 collate=utf8_bin;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create table tr (id int, name varchar(20000) charset utf8mb4, purchased date ) default charset=utf8 collate=utf8_bin;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create table tr (id int, name varchar(65536), purchased date ) default charset=latin1;", errno.ErrTooBigFieldlength)

	tk.MustExec("drop table if exists tr_05;")
	tk.MustExec("create table tr_05 (a varchar(16000) charset utf8);")
	tk.MustExec("alter table tr_05 modify column a varchar(16000) charset utf8;")
	tk.MustExec("alter table tr_05 modify column a varchar(16000) charset utf8mb4;")
}

func TestGeneratedColumnWindowFunction(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrCode("CREATE TABLE t (a INT , b INT as (ROW_NUMBER() OVER (ORDER BY a)))", errno.ErrWindowInvalidWindowFuncUse)
	tk.MustGetErrCode("CREATE TABLE t (a INT , index idx ((ROW_NUMBER() OVER (ORDER BY a))))", errno.ErrWindowInvalidWindowFuncUse)
}

func TestCreateTableWithDecimalWithDoubleZero(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	checkType := func(db, table, field string) {
		ctx := tk.Session().(sessionctx.Context)
		is := domain.GetDomain(ctx).InfoSchema()
		tableInfo, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(table))
		require.NoError(t, err)
		tblInfo := tableInfo.Meta()
		for _, col := range tblInfo.Columns {
			if col.Name.L == field {
				require.Equal(t, 10, col.Flen)
			}
		}
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt(d decimal(0, 0))")
	checkType("test", "tt", "d")

	tk.MustExec("drop table tt")
	tk.MustExec("create table tt(a int)")
	tk.MustExec("alter table tt add column d decimal(0, 0)")
	checkType("test", "tt", "d")

	tk.MustExec("drop table tt")
	tk.MustExec("create table tt(d int)")
	tk.MustExec("alter table tt change column d d decimal(0, 0)")
	checkType("test", "tt", "d")
}

func TestAlterCheck(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table alter_check (pk int primary key)")
	tk.MustExec("alter table alter_check alter check crcn ENFORCED")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|8231|ALTER CHECK is not supported"))
}

func TestDefaultSQLFunction(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// For issue #13189
	// Use `DEFAULT()` in `INSERT` / `INSERT ON DUPLICATE KEY UPDATE` statement
	tk.MustExec("create table t1 (a int primary key, b int default 20, c int default 30, d int default 40);")
	tk.MustExec("SET @@time_zone = '+00:00'")
	defer tk.MustExec("SET @@time_zone = DEFAULT")
	tk.MustQuery("SELECT @@time_zone").Check(testkit.Rows("+00:00"))
	tk.MustExec("create table t2 (a int primary key, b timestamp DEFAULT CURRENT_TIMESTAMP, c timestamp DEFAULT '2000-01-01 00:00:00')")
	tk.MustExec("insert into t1 set a = 1, b = default(c);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40"))
	tk.MustExec("insert into t1 set a = 2, b = default(c), c = default(d), d = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40", "2 30 40 20"))
	tk.MustExec("insert into t1 values (2, 3, 4, 5) on duplicate key update b = default(d), c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40", "2 40 20 20"))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a = default(b) + default(c) - default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20 30 40"))
	tk.MustExec("set @@timestamp = 1321009871")
	defer tk.MustExec("set @@timestamp = DEFAULT")
	tk.MustQuery("SELECT NOW()").Check(testkit.Rows("2011-11-11 11:11:11"))
	tk.MustExec("insert into t2 set a = 1, b = default(c)")
	tk.MustExec("insert into t2 set a = 2, c = default(b)")
	tk.MustGetErrCode("insert into t2 set a = 3, b = default(a)", errno.ErrNoDefaultForField)
	tk.MustExec("insert into t2 set a = 4, b = default(b), c = default(c)")
	tk.MustExec("insert into t2 set a = 5, b = default, c = default")
	tk.MustExec("insert into t2 set a = 6")
	tk.MustQuery("select * from t2").Sort().Check(testkit.Rows(
		"1 2000-01-01 00:00:00 2000-01-01 00:00:00",
		"2 2011-11-11 11:11:11 2011-11-11 11:11:11",
		"4 2011-11-11 11:11:11 2000-01-01 00:00:00",
		"5 2011-11-11 11:11:11 2000-01-01 00:00:00",
		"6 2011-11-11 11:11:11 2000-01-01 00:00:00"))
	// Use `DEFAULT()` in `UPDATE` statement
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 value (1, 2, 3, 4);")
	tk.MustExec("update t1 set a = 1, c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2 20 4"))
	tk.MustExec("insert into t1 value (2, 2, 3, 4);")
	tk.MustExec("update t1 set c = default(b), b = default(c) where a = 2;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2 20 4", "2 30 20 4"))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a = 10")
	tk.MustExec("update t1 set a = 10, b = default(c) + default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40"))
	tk.MustExec("set @@timestamp = 1671747742")
	tk.MustExec("update t2 set b = default(c) WHERE a = 6")
	tk.MustExec("update t2 set c = default(b) WHERE a = 5")
	tk.MustGetErrCode("update t2 set b = default(a) WHERE a = 4", errno.ErrNoDefaultForField)
	tk.MustExec("update t2 set b = default(b), c = default(c) WHERE a = 4")
	// Non existing row!
	tk.MustExec("update t2 set b = default(b), c = default(c) WHERE a = 3")
	tk.MustExec("update t2 set b = default, c = default WHERE a = 2")
	tk.MustExec("update t2 set b = default(b) WHERE a = 1")
	tk.MustQuery("select * from t2;").Sort().Check(testkit.Rows(
		"1 2022-12-22 22:22:22 2000-01-01 00:00:00",
		"2 2022-12-22 22:22:22 2000-01-01 00:00:00",
		"4 2022-12-22 22:22:22 2000-01-01 00:00:00",
		"5 2011-11-11 11:11:11 2022-12-22 22:22:22",
		"6 2000-01-01 00:00:00 2000-01-01 00:00:00"))
	// Use `DEFAULT()` in `REPLACE` statement
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 value (1, 2, 3, 4);")
	tk.MustExec("replace into t1 set a = 1, c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 20 40"))
	tk.MustExec("insert into t1 value (2, 2, 3, 4);")
	tk.MustExec("replace into t1 set a = 2, d = default(b), c = default(d);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 20 40", "2 20 40 20"))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a = 10, c = 3")
	tk.MustExec("replace into t1 set a = 10, b = default(c) + default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40"))
	tk.MustExec("replace into t1 set a = 20, d = default(c) + default(b)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40", "20 20 30 50"))

	// Use `DEFAULT()` in expression of generate columns, issue #12471
	tk.MustExec("DROP TABLE t2")
	tk.MustExec("create table t2(a int default 9, b int as (1 + default(a)));")
	tk.MustExec("insert into t2 values(1, default);")
	tk.MustExec("insert into t2 values(2, default(b))")
	tk.MustQuery("select * from t2").Sort().Check(testkit.Rows("1 10", "2 10"))

	// Use `DEFAULT()` with subquery, issue #13390
	tk.MustExec("create table t3(f1 int default 11);")
	tk.MustExec("insert into t3 value ();")
	tk.MustQuery("select default(f1) from (select * from t3) t1;").Check(testkit.Rows("11"))
	tk.MustQuery("select default(f1) from (select * from (select * from t3) t1 ) t1;").Check(testkit.Rows("11"))

	tk.MustExec("create table t4(a int default 4);")
	tk.MustExec("insert into t4 value (2);")
	tk.MustQuery("select default(c) from (select b as c from (select a as b from t4) t3) t2;").Check(testkit.Rows("4"))
	tk.MustGetErrCode("select default(a) from (select a from (select 1 as a) t4) t4;", errno.ErrNoDefaultForField)

	tk.MustExec("drop table t1, t2, t3, t4;")
}

func TestCreateIndexType(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_index (
		price int(5) DEFAULT '0' NOT NULL,
		area varchar(40) DEFAULT '' NOT NULL,
		type varchar(40) DEFAULT '' NOT NULL,
		transityes set('a','b'),
		shopsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		schoolsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		petsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		KEY price (price,area,type,transityes,shopsyes,schoolsyes,petsyes));`)
}

func TestAlterPrimaryKey(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test_add_pk(a int, b int unsigned , c varchar(255) default 'abc', d int as (a+b), e int as (a+1) stored, index idx(b))")

	// for generated columns
	tk.MustGetErrCode("alter table test_add_pk add primary key(d);", errno.ErrUnsupportedOnGeneratedColumn)
	// The primary key name is the same as the existing index name.
	tk.MustExec("alter table test_add_pk add primary key idx(e)")
	tk.MustExec("drop index `primary` on test_add_pk")

	// for describing table
	tk.MustExec("create table test_add_pk1(a int, index idx(a))")
	tk.MustQuery("desc test_add_pk1").Check(testutil.RowsWithSep(",", `a,int(11),YES,MUL,<nil>,`))
	tk.MustExec("alter table test_add_pk1 add primary key idx(a)")
	tk.MustQuery("desc test_add_pk1").Check(testutil.RowsWithSep(",", `a,int(11),NO,PRI,<nil>,`))
	tk.MustExec("alter table test_add_pk1 drop primary key")
	tk.MustQuery("desc test_add_pk1").Check(testutil.RowsWithSep(",", `a,int(11),NO,MUL,<nil>,`))
	tk.MustExec("create table test_add_pk2(a int, b int, index idx(a))")
	tk.MustExec("alter table test_add_pk2 add primary key idx(a, b)")
	tk.MustQuery("desc test_add_pk2").Check(testutil.RowsWithSep(",", ""+
		"a int(11) NO PRI <nil> ]\n"+
		"[b int(11) NO PRI <nil> "))
	tk.MustQuery("show create table test_add_pk2").Check(testutil.RowsWithSep("|", ""+
		"test_add_pk2 CREATE TABLE `test_add_pk2` (\n"+
		"  `a` int(11) NOT NULL,\n"+
		"  `b` int(11) NOT NULL,\n"+
		"  KEY `idx` (`a`),\n"+
		"  PRIMARY KEY (`a`,`b`) /*T![clustered_index] NONCLUSTERED */\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("alter table test_add_pk2 drop primary key")
	tk.MustQuery("desc test_add_pk2").Check(testutil.RowsWithSep(",", ""+
		"a int(11) NO MUL <nil> ]\n"+
		"[b int(11) NO  <nil> "))

	// Check if the primary key exists before checking the table's pkIsHandle.
	tk.MustGetErrCode("alter table test_add_pk drop primary key", errno.ErrCantDropFieldOrKey)

	// for the limit of name
	validName := strings.Repeat("a", mysql.MaxIndexIdentifierLen)
	invalidName := strings.Repeat("b", mysql.MaxIndexIdentifierLen+1)
	tk.MustGetErrCode("alter table test_add_pk add primary key "+invalidName+"(a)", errno.ErrTooLongIdent)
	// for valid name
	tk.MustExec("alter table test_add_pk add primary key " + validName + "(a)")
	// for multiple primary key
	tk.MustGetErrCode("alter table test_add_pk add primary key (a)", errno.ErrMultiplePriKey)
	tk.MustExec("alter table test_add_pk drop primary key")
	// for not existing primary key
	tk.MustGetErrCode("alter table test_add_pk drop primary key", errno.ErrCantDropFieldOrKey)
	tk.MustGetErrCode("drop index `primary` on test_add_pk", errno.ErrCantDropFieldOrKey)

	// for too many key parts specified
	tk.MustGetErrCode("alter table test_add_pk add primary key idx_test(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17);",
		errno.ErrTooManyKeyParts)

	// for the limit of comment's length
	validComment := "'" + strings.Repeat("a", ddl.MaxCommentLength) + "'"
	invalidComment := "'" + strings.Repeat("b", ddl.MaxCommentLength+1) + "'"
	tk.MustGetErrCode("alter table test_add_pk add primary key(a) comment "+invalidComment, errno.ErrTooLongIndexComment)
	// for empty sql_mode
	r := tk.MustQuery("select @@sql_mode")
	sqlMode := r.Rows()[0][0].(string)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("alter table test_add_pk add primary key(a) comment " + invalidComment)
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'PRIMARY' is too long (max = 1024)"))
	tk.MustExec("set @@sql_mode= '" + sqlMode + "'")
	tk.MustExec("alter table test_add_pk drop primary key")
	// for valid comment
	tk.MustExec("alter table test_add_pk add primary key(a, b, c) comment " + validComment)
	require.NoError(t, tk.Session().NewTxn(context.Background()))
	tbl := external.GetTableByName(t, tk, "test", "test_add_pk")
	col1Flag := tbl.Cols()[0].Flag
	col2Flag := tbl.Cols()[1].Flag
	col3Flag := tbl.Cols()[2].Flag
	require.True(t, mysql.HasNotNullFlag(col1Flag) && !mysql.HasPreventNullInsertFlag(col1Flag))
	require.True(t, mysql.HasNotNullFlag(col2Flag) && !mysql.HasPreventNullInsertFlag(col2Flag) && mysql.HasUnsignedFlag(col2Flag))
	require.True(t, mysql.HasNotNullFlag(col3Flag) && !mysql.HasPreventNullInsertFlag(col3Flag) && !mysql.HasNoDefaultValueFlag(col3Flag))
	tk.MustExec("alter table test_add_pk drop primary key")

	// for null values in primary key
	tk.MustExec("drop table test_add_pk")
	tk.MustExec("create table test_add_pk(a int, b int unsigned , c varchar(255) default 'abc', index idx(b))")
	tk.MustExec("insert into test_add_pk set a = 0, b = 0, c = 0")
	tk.MustExec("insert into test_add_pk set a = 1")
	tk.MustGetErrCode("alter table test_add_pk add primary key (b)", errno.ErrInvalidUseOfNull)
	tk.MustExec("insert into test_add_pk set a = 2, b = 2")
	tk.MustGetErrCode("alter table test_add_pk add primary key (a, b)", errno.ErrInvalidUseOfNull)
	tk.MustExec("insert into test_add_pk set a = 3, c = 3")
	tk.MustGetErrCode("alter table test_add_pk add primary key (c, b, a)", errno.ErrInvalidUseOfNull)
}

func TestParallelDropSchemaAndDropTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database if not exists test_drop_schema_table")
	tk1.MustExec("use test_drop_schema_table")
	tk1.MustExec("create table t(c1 int, c2 int)")
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: dom}

	dbInfo := external.GetSchemaByName(t, tk1, "test_drop_schema_table")
	done := false
	var wg sync.WaitGroup
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test_drop_schema_table")
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.Type == model.ActionDropSchema && job.State == model.JobStateRunning &&
			job.SchemaState == model.StateWriteOnly && job.SchemaID == dbInfo.ID && done == false {
			wg.Add(1)
			done = true
			go func() {
				_, checkErr = tk2.Exec("drop table t")
				wg.Done()
			}()
			time.Sleep(5 * time.Millisecond)
		}
	}
	originalHook := dom.DDL().GetHook()
	dom.DDL().SetHook(hook)
	tk1.MustExec("drop database test_drop_schema_table")
	dom.DDL().SetHook(originalHook)
	wg.Wait()
	require.True(t, done)
	require.Error(t, checkErr)
	// There are two possible assert result because:
	// 1: If drop-database is finished before drop-table being put into the ddl job queue, it will return "unknown table" error directly in the previous check.
	// 2: If drop-table has passed the previous check and been put into the ddl job queue, then drop-database finished, it will return schema change error.
	assertRes := checkErr.Error() == "[domain:8028]Information schema is changed during the execution of the"+
		" statement(for example, table definition may be updated by other DDL ran in parallel). "+
		"If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]" ||
		checkErr.Error() == "[schema:1051]Unknown table 'test_drop_schema_table.t'"
	require.True(t, assertRes)

	// Below behaviour is use to mock query `curl "http://$IP:10080/tiflash/replica"`
	fn := func(jobs []*model.Job) (bool, error) {
		return executor.GetDropOrTruncateTableInfoFromJobs(jobs, 0, dom, func(job *model.Job, info *model.TableInfo) (bool, error) {
			return false, nil
		})
	}
	require.NoError(t, tk1.Session().NewTxn(context.Background()))
	txn, err := tk1.Session().Txn(true)
	require.NoError(t, err)
	require.NoError(t, admin.IterHistoryDDLJobs(txn, fn))
}

// TestCancelDropIndex tests cancel ddl job which type is drop primary key.
func TestCancelDropPrimaryKey(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()
	idxName := "primary"
	addIdxSQL := "alter table t add primary key idx_c2 (c2);"
	dropIdxSQL := "alter table t drop primary key;"
	testCancelDropIndex(t, store, dom.DDL(), idxName, addIdxSQL, dropIdxSQL, dom)
}

// TestCancelDropIndex tests cancel ddl job which type is drop index.
func TestCancelDropIndex(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()
	idxName := "idx_c2"
	addIdxSQL := "alter table t add index idx_c2 (c2);"
	dropIdxSQL := "alter table t drop index idx_c2;"
	testCancelDropIndex(t, store, dom.DDL(), idxName, addIdxSQL, dropIdxSQL, dom)
}

// testCancelDropIndex tests cancel ddl job which type is drop index.
func testCancelDropIndex(t *testing.T, store kv.Storage, d ddl.DDL, idxName, addIdxSQL, dropIdxSQL string, dom *domain.Domain) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tests := []struct {
		needAddIndex   bool
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		// model.JobStateNone means the jobs is canceled before the first run.
		// if we cancel successfully, we need to set needAddIndex to false in the next test case. Otherwise, set needAddIndex to true.
		{true, model.JobStateNone, model.StateNone, true},
		{false, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: dom}
	var jobID int64
	test := &tests[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if (job.Type == model.ActionDropIndex || job.Type == model.ActionDropPrimaryKey) &&
			job.State == test.jobState && job.SchemaState == test.JobSchemaState {
			jobID = job.ID
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = store
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
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
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := d.GetHook()
	d.SetHook(hook)
	for i := range tests {
		test = &tests[i]
		if test.needAddIndex {
			tk.MustExec(addIdxSQL)
		}
		err := tk.ExecToErr(dropIdxSQL)
		tbl := external.GetTableByName(t, tk, "test", "t")
		indexInfo := tbl.Meta().FindIndexByName(idxName)
		if test.cancelSucc {
			require.NoError(t, checkErr)
			require.EqualError(t, err, "[ddl:8214]Cancelled DDL job")
			require.NotNil(t, indexInfo)
			require.Equal(t, model.StatePublic, indexInfo.State)
		} else {
			err1 := admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID)
			require.NoError(t, err)
			require.EqualError(t, checkErr, err1.Error())
			require.Nil(t, indexInfo)
		}
	}
	d.SetHook(originalHook)
	tk.MustExec(addIdxSQL)
	tk.MustExec(dropIdxSQL)
}

// TestCancelTruncateTable tests cancel ddl job which type is truncate table.
func TestCancelTruncateTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionTruncateTable && job.State == model.JobStateNone {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = store
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
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
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	dom.DDL().SetHook(hook)
	err := tk.ExecToErr("truncate table t")
	require.NoError(t, checkErr)
	require.EqualError(t, err, "[ddl:8214]Cancelled DDL job")
}
