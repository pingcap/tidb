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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
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

	tbl := testutil.GetTableByName(t, tk, "test", "t_23473")
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
