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
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	goctx "golang.org/x/net/context"
)

func TestNoZeroDateMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	defer tk.MustExec("set session sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';")

	tk.MustExec("use test;")
	tk.MustExec("set session sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION';")
	tk.MustGetErrCode("create table test_zero_date(agent_start_time date NOT NULL DEFAULT '0000-00-00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table test_zero_date(agent_start_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table test_zero_date(agent_start_time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table test_zero_date(a timestamp default '0000-00-00 00');", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table test_zero_date(a timestamp default 0);", errno.ErrInvalidDefault)
	defer tk.MustExec(`drop table if exists test_zero_date`)
	tk.MustExec("set session sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("create table test_zero_date (a timestamp default 0)")
	tk.MustExec(`insert into test_zero_date values (0)`)
	tk.MustQuery(`select a, unix_timestamp(a) from test_zero_date`).Check(testkit.Rows("0000-00-00 00:00:00 0"))
	tk.MustExec(`update test_zero_date set a = '2001-01-01 11:11:11' where a = 0`)
	tk.MustExec(`replace into test_zero_date values (0)`)
	tk.MustExec(`delete from test_zero_date where a = 0`)
	tk.MustExec(`update test_zero_date set a = 0 where a = '2001-01-01 11:11:11'`)
	tk.MustExec("set session sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';")
	tk.MustGetErrCode(`insert into test_zero_date values (0)`, errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode(`replace into test_zero_date values (0)`, errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode(`update test_zero_date set a = 0 where a = 0`, errno.ErrTruncatedWrongValue)
	tk.MustExec(`delete from test_zero_date where a = 0`)
	tk.MustQuery(`select a, unix_timestamp(a) from test_zero_date`).Check(testkit.Rows())
	tk.MustExec(`drop table test_zero_date`)
	tk.MustExec("set session sql_mode=''")
	tk.MustExec("create table test_zero_date (a timestamp default 0)")
	tk.MustExec(`drop table test_zero_date`)
	tk.MustExec(`create table test_zero_date (a int)`)
	tk.MustExec(`insert into test_zero_date values (0)`)
	tk.MustExec(`alter table test_zero_date modify a date`)
	tk.MustExec("set session sql_mode='NO_ZERO_DATE'")
	tk.MustExec(`drop table test_zero_date`)
	tk.MustExec("create table test_zero_date (a timestamp default 0)")
	tk.MustExec(`drop table test_zero_date`)
	tk.MustExec(`create table test_zero_date (a int)`)
	tk.MustExec(`insert into test_zero_date values (0)`)
	tk.MustExec(`alter table test_zero_date modify a date`)
	tk.MustExec("set session sql_mode='STRICT_TRANS_TABLES'")
	tk.MustExec(`drop table test_zero_date`)
	tk.MustExec("create table test_zero_date (a timestamp default 0)")
	tk.MustExec(`drop table test_zero_date`)
	tk.MustExec(`create table test_zero_date (a int)`)
	tk.MustExec(`insert into test_zero_date values (0)`)
	tk.MustGetErrCode(`alter table test_zero_date modify a date`, errno.ErrTruncatedWrongValue)
	tk.MustExec("set session sql_mode='NO_ZERO_DATE,STRICT_TRANS_TABLES'")
	tk.MustExec(`drop table test_zero_date`)
	tk.MustGetErrCode("create table test_zero_date (a timestamp default 0)", errno.ErrInvalidDefault)
	tk.MustExec(`create table test_zero_date (a int)`)
	tk.MustExec(`insert into test_zero_date values (0)`)
	tk.MustGetErrCode(`alter table test_zero_date modify a date`, errno.ErrTruncatedWrongValue)
}

func TestInvalidDefault(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t(c1 decimal default 1.7976931348623157E308)")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, types.ErrInvalidDefault), "err %v", err)

	_, err = tk.Exec("create table t( c1 varchar(2) default 'TiDB');")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, types.ErrInvalidDefault), "err %v", err)
}

// TestKeyWithoutLength for issue #13452
func TestKeyWithoutLengthCreateTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test")

	_, err := tk.Exec("create table t_without_length (a text primary key)")
	require.Error(t, err)
	require.Regexp(t, ".*BLOB/TEXT column 'a' used in key specification without a key length", err.Error())
}

// TestInvalidNameWhenCreateTable for issue #3848
func TestInvalidNameWhenCreateTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test;")

	tk.MustGetErrCode("create table t(xxx.t.a bigint)", errno.ErrWrongDBName)
	tk.MustGetErrCode("create table t(test.tttt.a bigint)", errno.ErrWrongTableName)
	tk.MustGetErrCode("create table t(t.tttt.a bigint)", errno.ErrWrongDBName)
}

// TestCreateTableIfNotExists for issue #6879
func TestCreateTableIfNotExists(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test;")

	tk.MustExec("create table ct1(a bigint)")
	tk.MustExec("create table ct(a bigint)")

	// Test duplicate create-table with `LIKE` clause
	tk.MustExec("create table if not exists ct like ct1;")
	warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.GreaterOrEqual(t, len(warnings), 1)
	lastWarn := warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(infoschema.ErrTableExists, lastWarn.Err), "err %v", lastWarn.Err)
	require.Equal(t, stmtctx.WarnLevelNote, lastWarn.Level)

	// Test duplicate create-table without `LIKE` clause
	tk.MustExec("create table if not exists ct(b bigint, c varchar(60));")
	warnings = tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.GreaterOrEqual(t, len(warnings), 1)
	lastWarn = warnings[len(warnings)-1]
	require.True(t, terror.ErrorEqual(infoschema.ErrTableExists, lastWarn.Err))
}

// for issue #9910
func TestCreateTableWithKeyWord(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t1(pump varchar(20), drainer varchar(20), node_id varchar(20), node_state varchar(20));")
	require.NoError(t, err)
}

func TestUniqueKeyNullValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(255))")

	tk.MustExec("insert into t values(1, NULL)")
	tk.MustExec("insert into t values(2, NULL)")
	tk.MustExec("alter table t add unique index b(b);")
	res := tk.MustQuery("select count(*) from t use index(b);")
	res.Check(testkit.Rows("2"))
	tk.MustExec("admin check table t")
	tk.MustExec("admin check index t b")
}

func TestUniqueKeyNullValueClusterIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists unique_null_val;")
	tk.MustExec("create database unique_null_val;")
	tk.MustExec("use unique_null_val;")
	tk.MustExec("create table t (a varchar(10), b float, c varchar(255), primary key (a, b));")
	tk.MustExec("insert into t values ('1', 1, NULL);")
	tk.MustExec("insert into t values ('2', 2, NULL);")
	tk.MustExec("alter table t add unique index c(c);")
	tk.MustQuery("select count(*) from t use index(c);").Check(testkit.Rows("2"))
	tk.MustExec("admin check table t;")
	tk.MustExec("admin check index t c;")
}

// TestModifyColumnAfterAddIndex Issue 5134
func TestModifyColumnAfterAddIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table city (city VARCHAR(2) KEY);")
	tk.MustExec("alter table city change column city city varchar(50);")
	tk.MustExec(`insert into city values ("abc"), ("abd");`)
}

func TestIssue2293(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_issue_2293 (a int)")
	tk.MustGetErrCode("alter table t_issue_2293 add b int not null default 'a'", errno.ErrInvalidDefault)
	tk.MustExec("insert into t_issue_2293 value(1)")
	tk.MustQuery("select * from t_issue_2293").Check(testkit.Rows("1"))
}

func TestIssue6101(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (quantity decimal(2) unsigned);")
	_, err := tk.Exec("insert into t1 values (500), (-500), (~0), (-1);")
	terr := errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrWarnDataOutOfRange), terr.Code())
	tk.MustExec("drop table t1")

	tk.MustExec("set sql_mode=''")
	tk.MustExec("create table t1 (quantity decimal(2) unsigned);")
	tk.MustExec("insert into t1 values (500), (-500), (~0), (-1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("99", "0", "99", "0"))
	tk.MustExec("drop table t1")
}

func TestIssue19229(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE enumt (type enum('a', 'b') );")
	_, err := tk.Exec("insert into enumt values('xxx');")
	terr := errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.WarnDataTruncated), terr.Code())
	_, err = tk.Exec("insert into enumt values(-1);")
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.WarnDataTruncated), terr.Code())
	tk.MustExec("drop table enumt")

	tk.MustExec("CREATE TABLE sett (type set('a', 'b') );")
	_, err = tk.Exec("insert into sett values('xxx');")
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.WarnDataTruncated), terr.Code())
	_, err = tk.Exec("insert into sett values(-1);")
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.WarnDataTruncated), terr.Code())
	tk.MustExec("drop table sett")
}

func TestIndexLength(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table idx_len(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0))")
	tk.MustExec("create index idx on idx_len(a)")
	tk.MustExec("alter table idx_len add index idxa(a)")
	tk.MustExec("create index idx1 on idx_len(b)")
	tk.MustExec("alter table idx_len add index idxb(b)")
	tk.MustExec("create index idx2 on idx_len(c)")
	tk.MustExec("alter table idx_len add index idxc(c)")
	tk.MustExec("create index idx3 on idx_len(d)")
	tk.MustExec("alter table idx_len add index idxd(d)")
	tk.MustExec("create index idx4 on idx_len(f)")
	tk.MustExec("alter table idx_len add index idxf(f)")
	tk.MustExec("create index idx5 on idx_len(g)")
	tk.MustExec("alter table idx_len add index idxg(g)")
	tk.MustExec("create table idx_len1(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0), index(a), index(b), index(c), index(d), index(f), index(g))")

	tk.MustExec("drop table idx_len;")
	tk.MustExec("create table idx_len(a text, b text charset ascii, c blob, index(a(768)), index (b(3072)), index (c(3072)));")
	tk.MustExec("drop table idx_len;")
	tk.MustExec("create table idx_len(a text, b text charset ascii, c blob);")
	tk.MustExec("alter table idx_len add index (a(768))")
	tk.MustExec("alter table idx_len add index (b(3072))")
	tk.MustExec("alter table idx_len add index (c(3072))")
	tk.MustExec("drop table idx_len;")
}

func TestIssue3833(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table issue3833 (b char(0), c binary(0), d  varchar(0))")
	tk.MustGetErrCode("create index idx on issue3833 (b)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (b)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(b))", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create index idx on issue3833 (c)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (c)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(c))", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create index idx on issue3833 (d)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (d)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(d))", errno.ErrWrongKeyColumn)
}

func TestIssue2858And2717(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_issue_2858_bit (a bit(64) default b'0')")
	tk.MustExec("insert into t_issue_2858_bit value ()")
	tk.MustExec(`insert into t_issue_2858_bit values (100), ('10'), ('\0')`)
	tk.MustQuery("select a+0 from t_issue_2858_bit").Check(testkit.Rows("0", "100", "12592", "0"))
	tk.MustExec(`alter table t_issue_2858_bit alter column a set default '\0'`)

	tk.MustExec("create table t_issue_2858_hex (a int default 0x123)")
	tk.MustExec("insert into t_issue_2858_hex value ()")
	tk.MustExec("insert into t_issue_2858_hex values (123), (0x321)")
	tk.MustQuery("select a from t_issue_2858_hex").Check(testkit.Rows("291", "123", "801"))
	tk.MustExec(`alter table t_issue_2858_hex alter column a set default 0x321`)
}

func TestIssue4432(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table tx (col bit(10) default 'a')")
	tk.MustExec("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustExec("drop table tx")

	tk.MustExec("create table tx (col bit(10) default 0x61)")
	tk.MustExec("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustExec("drop table tx")

	tk.MustExec("create table tx (col bit(10) default 97)")
	tk.MustExec("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustExec("drop table tx")

	tk.MustExec("create table tx (col bit(10) default 0b1100001)")
	tk.MustExec("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustExec("drop table tx")
}

func TestIssue5092(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_issue_5092 (a int)")
	tk.MustExec("alter table t_issue_5092 add column (b int, c int)")
	tk.MustExec("alter table t_issue_5092 add column if not exists (b int, c int)")
	tk.MustExec("alter table t_issue_5092 add column b1 int after b, add column c1 int after c")
	tk.MustExec("alter table t_issue_5092 add column d int after b, add column e int first, add column f int after c1, add column g int, add column h int first")
	tk.MustQuery("show create table t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `h` int(11) DEFAULT NULL,\n" +
		"  `e` int(11) DEFAULT NULL,\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `d` int(11) DEFAULT NULL,\n" +
		"  `b1` int(11) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `c1` int(11) DEFAULT NULL,\n" +
		"  `f` int(11) DEFAULT NULL,\n" +
		"  `g` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// The following two statements are consistent with MariaDB.
	tk.MustGetErrCode("alter table t_issue_5092 add column if not exists d int, add column d int", errno.ErrDupFieldName)
	tk.MustExec("alter table t_issue_5092 add column dd int, add column if not exists dd int")
	tk.MustExec("alter table t_issue_5092 add column if not exists (d int, e int), add column ff text")
	tk.MustExec("alter table t_issue_5092 add column b2 int after b1, add column c2 int first")
	tk.MustQuery("show create table t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `c2` int(11) DEFAULT NULL,\n" +
		"  `h` int(11) DEFAULT NULL,\n" +
		"  `e` int(11) DEFAULT NULL,\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `d` int(11) DEFAULT NULL,\n" +
		"  `b1` int(11) DEFAULT NULL,\n" +
		"  `b2` int(11) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `c1` int(11) DEFAULT NULL,\n" +
		"  `f` int(11) DEFAULT NULL,\n" +
		"  `g` int(11) DEFAULT NULL,\n" +
		"  `dd` int(11) DEFAULT NULL,\n" +
		"  `ff` text DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_issue_5092")

	tk.MustExec("create table t_issue_5092 (a int default 1)")
	tk.MustExec("alter table t_issue_5092 add column (b int default 2, c int default 3)")
	tk.MustExec("alter table t_issue_5092 add column b1 int default 22 after b, add column c1 int default 33 after c")
	tk.MustExec("insert into t_issue_5092 value ()")
	tk.MustQuery("select * from t_issue_5092").Check(testkit.Rows("1 2 22 3 33"))
	tk.MustExec("alter table t_issue_5092 add column d int default 4 after c1, add column aa int default 0 first")
	tk.MustQuery("select * from t_issue_5092").Check(testkit.Rows("0 1 2 22 3 33 4"))
	tk.MustQuery("show create table t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `aa` int(11) DEFAULT '0',\n" +
		"  `a` int(11) DEFAULT '1',\n" +
		"  `b` int(11) DEFAULT '2',\n" +
		"  `b1` int(11) DEFAULT '22',\n" +
		"  `c` int(11) DEFAULT '3',\n" +
		"  `c1` int(11) DEFAULT '33',\n" +
		"  `d` int(11) DEFAULT '4'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_issue_5092")

	tk.MustExec("create table t_issue_5092 (a int)")
	tk.MustExec("alter table t_issue_5092 add column (b int, c int)")
	tk.MustExec("alter table t_issue_5092 drop column b,drop column c")
	tk.MustGetErrCode("alter table t_issue_5092 drop column c, drop column c", errno.ErrCantDropFieldOrKey)
	tk.MustExec("alter table t_issue_5092 drop column if exists b,drop column if exists c")
	tk.MustGetErrCode("alter table t_issue_5092 drop column g, drop column d", errno.ErrCantDropFieldOrKey)
	tk.MustExec("drop table t_issue_5092")

	tk.MustExec("create table t_issue_5092 (a int)")
	tk.MustExec("alter table t_issue_5092 add column (b int, c int)")
	tk.MustGetErrCode("alter table t_issue_5092 drop column if exists a, drop column b, drop column c", errno.ErrCantRemoveAllFields)
	tk.MustGetErrCode("alter table t_issue_5092 drop column if exists c, drop column c", errno.ErrCantDropFieldOrKey)
	tk.MustExec("alter table t_issue_5092 drop column c, drop column if exists c")
	tk.MustExec("drop table t_issue_5092")
}

func TestErrnoErrorCode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// create database
	sql := "create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "create database test"
	tk.MustGetErrCode(sql, errno.ErrDBCreateExists)
	sql = "create database test1 character set uft8;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create database test2 character set gkb;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create database test3 character set laitn1;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	// drop database
	sql = "drop database db_not_exist"
	tk.MustGetErrCode(sql, errno.ErrDBDropExists)
	// create table
	tk.MustExec("create table test_error_code_succ (c1 int, c2 int, c3 int, primary key(c3))")
	sql = "create table test_error_code_succ (c1 int, c2 int, c3 int)"
	tk.MustGetErrCode(sql, errno.ErrTableExists)
	sql = "create table test_error_code1 (c1 int, c2 int, c2 int)"
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	sql = "create table test_error_code1 (c1 int, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "create table test_error_code1 (c1 int, `_tidb_rowid` int)"
	tk.MustGetErrCode(sql, errno.ErrWrongColumnName)
	sql = "create table aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa(a int)"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "create table test_error_code1 (c1 int, c2 int, key aa (c1, c2), key aa (c1))"
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	sql = "create table test_error_code1 (c1 int, c2 int, c3 int, key(c_not_exist))"
	tk.MustGetErrCode(sql, errno.ErrKeyColumnDoesNotExits)
	sql = "create table test_error_code1 (c1 int, c2 int, c3 int, primary key(c_not_exist))"
	tk.MustGetErrCode(sql, errno.ErrKeyColumnDoesNotExits)
	sql = "create table test_error_code1 (c1 int not null default '')"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT 2.0 DEFAULT now());"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "CREATE TABLE `t` (`a` double DEFAULT now());"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "create table t1(a int) character set uft8;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create table t1(a int) character set gkb;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create table t1(a int) character set laitn1;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create table test_error_code (a int not null ,b int not null,c int not null, d int not null, foreign key (b, c) references product(id));"
	tk.MustGetErrCode(sql, errno.ErrWrongFkDef)
	sql = "create table test_error_code_2;"
	tk.MustGetErrCode(sql, errno.ErrTableMustHaveColumns)
	sql = "create table test_error_code_2 (unique(c1));"
	tk.MustGetErrCode(sql, errno.ErrTableMustHaveColumns)
	sql = "create table test_error_code_2(c1 int, c2 int, c3 int, primary key(c1), primary key(c2));"
	tk.MustGetErrCode(sql, errno.ErrMultiplePriKey)
	sql = "create table test_error_code_3(pt blob ,primary key (pt));"
	tk.MustGetErrCode(sql, errno.ErrBlobKeyWithoutLength)
	sql = "create table test_error_code_3(a text, unique (a(769)));"
	tk.MustGetErrCode(sql, errno.ErrTooLongKey)
	sql = "create table test_error_code_3(a text charset ascii, unique (a(3073)));"
	tk.MustGetErrCode(sql, errno.ErrTooLongKey)
	sql = "create table test_error_code_3(`id` int, key `primary`(`id`));"
	tk.MustGetErrCode(sql, errno.ErrWrongNameForIndex)
	sql = "create table t2(c1.c2 blob default null);"
	tk.MustGetErrCode(sql, errno.ErrWrongTableName)
	sql = "create table t2 (id int default null primary key , age int);"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "create table t2 (id int null primary key , age int);"
	tk.MustGetErrCode(sql, errno.ErrPrimaryCantHaveNull)
	sql = "create table t2 (id int default null, age int, primary key(id));"
	tk.MustGetErrCode(sql, errno.ErrPrimaryCantHaveNull)
	sql = "create table t2 (id int null, age int, primary key(id));"
	tk.MustGetErrCode(sql, errno.ErrPrimaryCantHaveNull)
	sql = "create table t2 (id int auto_increment);"
	tk.MustGetErrCode(sql, errno.ErrWrongAutoKey)
	sql = "create table t2 (id int auto_increment, a int key);"
	tk.MustGetErrCode(sql, errno.ErrWrongAutoKey)
	sql = "create table t2 (a datetime(2) default current_timestamp(3));"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "create table t2 (a datetime(2) default current_timestamp(2) on update current_timestamp);"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)
	sql = "create table t2 (a datetime default current_timestamp on update current_timestamp(2));"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)
	sql = "create table t2 (a datetime(2) default current_timestamp(2) on update current_timestamp(3));"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)
	sql = "create table t(a blob(10), index(a(0)));"
	tk.MustGetErrCode(sql, errno.ErrKeyPart0)
	sql = "create table t(a char(10), index(a(0)));"
	tk.MustGetErrCode(sql, errno.ErrKeyPart0)

	sql = "create table t2 (id int primary key , age int);"
	tk.MustExec(sql)

	// add column
	sql = "alter table test_error_code_succ add column c1 int"
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	sql = "alter table test_error_code_succ add column aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "alter table test_comment comment 'test comment'"
	tk.MustGetErrCode(sql, errno.ErrNoSuchTable)
	sql = "alter table test_error_code_succ add column `a ` int ;"
	tk.MustGetErrCode(sql, errno.ErrWrongColumnName)
	sql = "alter table test_error_code_succ add column `_tidb_rowid` int ;"
	tk.MustGetErrCode(sql, errno.ErrWrongColumnName)
	tk.MustExec("create table test_on_update (c1 int, c2 int);")
	sql = "alter table test_on_update add column c3 int on update current_timestamp;"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)
	sql = "create table test_on_update_2(c int on update current_timestamp);"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)

	// add columns
	sql = "alter table test_error_code_succ add column c1 int, add column c1 int"
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	sql = "alter table test_error_code_succ add column (aa int, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "alter table test_error_code_succ add column `a ` int, add column `b ` int;"
	tk.MustGetErrCode(sql, errno.ErrWrongColumnName)
	tk.MustExec("create table test_add_columns_on_update (c1 int, c2 int);")
	sql = "alter table test_add_columns_on_update add column cc int, add column c3 int on update current_timestamp;"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)

	// drop column
	sql = "alter table test_error_code_succ drop c_not_exist"
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	tk.MustExec("create table test_drop_column (c1 int );")
	sql = "alter table test_drop_column drop column c1;"
	tk.MustGetErrCode(sql, errno.ErrCantRemoveAllFields)
	// drop columns
	sql = "alter table test_error_code_succ drop c_not_exist, drop cc_not_exist"
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	tk.MustExec("create table test_drop_columns (c1 int);")
	tk.MustExec("alter table test_drop_columns add column c2 int first, add column c3 int after c1")
	sql = "alter table test_drop_columns drop column c1, drop column c2, drop column c3;"
	tk.MustGetErrCode(sql, errno.ErrCantRemoveAllFields)
	sql = "alter table test_drop_columns drop column c1, add column c2 int;"
	tk.MustGetErrCode(sql, errno.ErrUnsupportedDDLOperation)
	sql = "alter table test_drop_columns drop column c1, drop column c1;"
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	// add index
	sql = "alter table test_error_code_succ add index idx (c_not_exist)"
	tk.MustGetErrCode(sql, errno.ErrKeyColumnDoesNotExits)
	tk.MustExec("alter table test_error_code_succ add index idx (c1)")
	sql = "alter table test_error_code_succ add index idx (c1)"
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	// drop index
	sql = "alter table test_error_code_succ drop index idx_not_exist"
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	sql = "alter table test_error_code_succ drop column c3"
	tk.MustGetErrCode(sql, errno.ErrUnsupportedDDLOperation)
	// modify column
	sql = "alter table test_error_code_succ modify testx.test_error_code_succ.c1 bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongDBName)
	sql = "alter table test_error_code_succ modify t.c1 bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongTableName)
	sql = "alter table test_error_code_succ change c1 _tidb_rowid bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongColumnName)
	sql = "alter table test_error_code_succ rename column c1 to _tidb_rowid"
	tk.MustGetErrCode(sql, errno.ErrWrongColumnName)
	// insert value
	tk.MustExec("create table test_error_code_null(c1 char(100) not null);")
	sql = "insert into test_error_code_null (c1) values(null);"
	tk.MustGetErrCode(sql, errno.ErrBadNull)
	// disable tidb_enable_change_multi_schema
	tk.MustExec("set global tidb_enable_change_multi_schema = false")
	sql = "alter table test_error_code_null add column (x1 int, x2 int)"
	tk.MustGetErrCode(sql, errno.ErrUnsupportedDDLOperation)
	sql = "alter table test_error_code_null add column (x1 int, x2 int)"
	tk.MustGetErrCode(sql, errno.ErrUnsupportedDDLOperation)
	tk.MustExec("set global tidb_enable_change_multi_schema = true")
}

func TestTableDDLWithFloatType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustGetErrCode("create table t (a decimal(1, 2))", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("create table t (a float(1, 2))", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("create table t (a double(1, 2))", errno.ErrMBiggerThanD)
	tk.MustExec("create table t (a double(1, 1))")
	tk.MustGetErrCode("alter table t add column b decimal(1, 2)", errno.ErrMBiggerThanD)
	// add multi columns now not support, so no case.
	tk.MustGetErrCode("alter table t modify column a float(1, 4)", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("alter table t change column a aa float(1, 4)", errno.ErrMBiggerThanD)
	tk.MustExec("drop table t")
}

func TestTableDDLWithTimeType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustGetErrCode("create table t (a time(7))", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("create table t (a datetime(7))", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("create table t (a timestamp(7))", errno.ErrTooBigPrecision)
	_, err := tk.Exec("create table t (a time(-1))")
	require.Error(t, err)
	tk.MustExec("create table t (a datetime)")
	tk.MustGetErrCode("alter table t add column b time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t add column b datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t add column b timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t modify column a time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t modify column a datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t modify column a timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t change column a aa time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t change column a aa datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t change column a aa timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustExec("alter table t change column a aa datetime(0)")
	tk.MustExec("drop table t")
}

func TestUpdateMultipleTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("insert t1 values (1, 1), (2, 2)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert t2 values (1, 3), (2, 5)")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	d := dom.DDL()
	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			tk2.MustExec("update t1, t2 set t1.c1 = 8, t2.c2 = 10 where t1.c2 = t2.c1")
			tk2.MustQuery("select * from t1").Check(testkit.Rows("8 1", "8 2"))
			tk2.MustQuery("select * from t2").Check(testkit.Rows("1 10", "2 10"))
		}
	}
	d.SetHook(hook)

	tk.MustExec("alter table t1 add column c3 bigint default 9")

	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1 9", "8 2 9"))
}

func TestNullGeneratedColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"`a` int(11) DEFAULT NULL," +
		"`b` int(11) DEFAULT NULL," +
		"`c` int(11) GENERATED ALWAYS AS (`a` + `b`) VIRTUAL," +
		"`h` varchar(10) DEFAULT NULL," +
		"`m` int(11) DEFAULT NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	tk.MustExec("insert into t values()")
	tk.MustExec("alter table t add index idx_c(c)")
	tk.MustExec("drop table t")
}

func TestDependedGeneratedColumnPrior2GeneratedColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"`a` int(11) DEFAULT NULL," +
		"`b` int(11) GENERATED ALWAYS AS (`a` + 1) VIRTUAL," +
		"`c` int(11) GENERATED ALWAYS AS (`b` + 1) VIRTUAL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	// should check unknown column first, then the prior ones.
	sql := "alter table t add column d int as (c + f + 1) first"
	tk.MustGetErrCode(sql, errno.ErrBadField)

	// depended generated column should be prior to generated column self
	sql = "alter table t add column d int as (c+1) first"
	tk.MustGetErrCode(sql, errno.ErrGeneratedColumnNonPrior)

	// correct case
	tk.MustExec("alter table t add column d int as (c+1) after c")

	// check position nil case
	tk.MustExec("alter table t add column(e int as (c+1))")
}

func TestChangingTableCharset(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test")
	tk.MustExec("create table t(a char(10), index i(a)) charset latin1 collate latin1_bin")

	tk.MustGetErrCode("alter table t charset gbk", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t charset ''", errno.ErrUnknownCharacterSet)

	tk.MustGetErrCode("alter table t charset utf8mb4 collate '' collate utf8mb4_bin;", errno.ErrUnknownCollation)

	tk.MustGetErrCode("alter table t charset utf8 collate latin1_bin", errno.ErrCollationCharsetMismatch)
	tk.MustGetErrCode("alter table t charset utf8 collate utf8mb4_bin;", errno.ErrCollationCharsetMismatch)
	tk.MustGetErrCode("alter table t charset utf8 collate utf8_bin collate utf8mb4_bin collate utf8_bin;", errno.ErrCollationCharsetMismatch)
	tk.MustGetErrCode("alter table t charset utf8", errno.ErrUnsupportedDDLOperation)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), index i(a)) charset latin1 collate latin1_bin")
	tk.MustExec("alter table t charset utf8mb4")
	tk.MustExec("admin check table t")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), index i(a)) charset latin1 collate latin1_bin")
	tk.MustExec("alter table t charset utf8mb4 collate utf8mb4_bin")
	tk.MustExec("admin check table t")

	tk.MustGetErrCode("alter table t charset latin1 charset utf8 charset utf8mb4 collate utf8_bin;", errno.ErrConflictingDeclarations)

	// Test change column charset when changing table charset.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10)) charset utf8")
	tk.MustExec("alter table t convert to charset utf8mb4;")
	checkCharset := func(chs, coll string) {
		tbl := external.GetTableByName(t, tk, "test", "t")
		require.NotNil(t, tbl)
		require.Equal(t, chs, tbl.Meta().Charset)
		require.Equal(t, coll, tbl.Meta().Collate)
		for _, col := range tbl.Meta().Columns {
			require.Equal(t, chs, col.GetCharset())
			require.Equal(t, coll, col.GetCollate())
		}
	}
	checkCharset(charset.CharsetUTF8MB4, charset.CollationUTF8MB4)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(20), key i(a)) charset=latin1")
	tk.MustGetErrCode("alter table t convert to charset utf8 collate utf8_unicode_ci", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t convert to charset utf8 collate utf8_general_ci", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t convert to charset utf8 collate utf8_bin", errno.ErrUnsupportedDDLOperation)

	// Test when column charset can not convert to the target charset.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10) character set ascii) charset utf8mb4")
	tk.MustGetErrCode("alter table t convert to charset utf8mb4;", errno.ErrUnsupportedDDLOperation)

	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10) character set utf8) charset utf8")
	tk.MustExec("alter table t convert to charset utf8 collate utf8_general_ci;")
	checkCharset(charset.CharsetUTF8, "utf8_general_ci")

	// Test when table charset is equal to target charset but column charset is not equal.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10) character set utf8) charset utf8mb4")
	tk.MustExec("alter table t convert to charset utf8mb4 collate utf8mb4_general_ci;")
	checkCharset(charset.CharsetUTF8MB4, "utf8mb4_general_ci")

	// Mock table info with charset is "". Old TiDB maybe create table with charset is "".
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)
	tbl := external.GetTableByName(t, tk, "test", "t")
	tblInfo := tbl.Meta().Clone()
	tblInfo.Charset = ""
	tblInfo.Collate = ""
	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = store
		err := sessiontxn.NewTxn(goctx.Background(), mockCtx)
		require.NoError(t, err)
		txn, err := mockCtx.Txn(true)
		require.NoError(t, err)
		mt := meta.NewMeta(txn)

		err = mt.UpdateTable(db.ID, tblInfo)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
	}
	updateTableInfo(tblInfo)

	// check table charset is ""
	tk.MustExec("alter table t add column b varchar(10);") //  load latest schema.
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, "", tbl.Meta().Charset)
	require.Equal(t, "", tbl.Meta().Collate)
	// Test when table charset is "", this for compatibility.
	tk.MustExec("alter table t convert to charset utf8mb4;")
	checkCharset(charset.CharsetUTF8MB4, charset.CollationUTF8MB4)

	// Test when column charset is "".
	tbl = external.GetTableByName(t, tk, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Columns[0].SetCharset("")
	tblInfo.Columns[0].SetCollate("")
	updateTableInfo(tblInfo)
	// check table charset is ""
	tk.MustExec("alter table t drop column b;") //  load latest schema.
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, "", tbl.Meta().Columns[0].GetCharset())
	require.Equal(t, "", tbl.Meta().Columns[0].GetCollate())
	tk.MustExec("alter table t convert to charset utf8mb4;")
	checkCharset(charset.CharsetUTF8MB4, charset.CollationUTF8MB4)

	tk.MustExec("drop table t")
	tk.MustExec("create table t (a blob) character set utf8;")
	tk.MustExec("alter table t charset=utf8mb4 collate=utf8mb4_bin;")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` blob DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a varchar(5) charset utf8) charset utf8")
	tk.MustExec("alter table t charset utf8mb4")
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, "utf8mb4", tbl.Meta().Charset)
	require.Equal(t, "utf8mb4_bin", tbl.Meta().Collate)
	for _, col := range tbl.Meta().Columns {
		// Column charset and collate should remain unchanged.
		require.Equal(t, "utf8", col.GetCharset())
		require.Equal(t, "utf8_bin", col.GetCollate())
	}

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a varchar(5) charset utf8 collate utf8_unicode_ci) charset utf8 collate utf8_unicode_ci")
	tk.MustExec("alter table t collate utf8_general_ci")
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, "utf8", tbl.Meta().Charset)
	require.Equal(t, "utf8_general_ci", tbl.Meta().Collate)
	for _, col := range tbl.Meta().Columns {
		require.Equal(t, "utf8", col.GetCharset())
		// Column collate should remain unchanged.
		require.Equal(t, "utf8_unicode_ci", col.GetCollate())
	}
}

func TestModifyColumnOption(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	errMsg := "[ddl:8200]" // unsupported modify column with references
	assertErrCode := func(sql string, errCodeStr string) {
		_, err := tk.Exec(sql)
		require.Error(t, err)
		require.Equal(t, errCodeStr, err.Error()[:len(errCodeStr)])
	}

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (b char(1) default null) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_general_ci")
	tk.MustExec("alter table t1 modify column b char(1) character set utf8mb4 collate utf8mb4_general_ci")

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (b char(1) collate utf8mb4_general_ci)")
	tk.MustExec("alter table t1 modify b char(1) character set utf8mb4 collate utf8mb4_general_ci")

	tk.MustExec("drop table t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int(11) default null)")
	tk.MustExec("create table t2 (b char, c int)")
	assertErrCode("alter table t2 modify column c int references t1(a)", errMsg)
	_, err := tk.Exec("alter table t1 change a a varchar(16)")
	require.NoError(t, err)
	_, err = tk.Exec("alter table t1 change a a varchar(10)")
	require.NoError(t, err)
	_, err = tk.Exec("alter table t1 change a a datetime")
	require.NoError(t, err)
	_, err = tk.Exec("alter table t1 change a a int(11) unsigned")
	require.NoError(t, err)
	_, err = tk.Exec("alter table t2 change b b int(11) unsigned")
	require.NoError(t, err)
}

func TestIndexOnMultipleGeneratedColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int as (a + 1), c int as (b + 1))")
	tk.MustExec("insert into t (a) values (1)")
	tk.MustExec("create index idx on t (c)")
	tk.MustQuery("select * from t where c > 1").Check(testkit.Rows("1 2 3"))
	res := tk.MustQuery("select * from t use index(idx) where c > 1")
	tk.MustQuery("select * from t ignore index(idx) where c > 1").Check(res.Rows())
	tk.MustExec("admin check table t")
}

func TestIndexOnMultipleGeneratedColumn1(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int as (a + 1), c int as (b + 1), d int as (c + 1))")
	tk.MustExec("insert into t (a) values (1)")
	tk.MustExec("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 3 4"))
	res := tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")
}

func TestIndexOnMultipleGeneratedColumn2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint, b decimal as (a+1), c varchar(20) as (b*2), d float as (a*23+b-1+length(c)))")
	tk.MustExec("insert into t (a) values (1)")
	tk.MustExec("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 4 25"))
	res := tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")
}

func TestIndexOnMultipleGeneratedColumn3(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10), b float as (length(a)+123), c varchar(20) as (right(a, 2)), d float as (b+b-7+1-3+3*ASCII(c)))")
	tk.MustExec("insert into t (a) values ('adorable')")
	tk.MustExec("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("adorable 131 le 577")) // 131+131-7+1-3+3*108
	res := tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")
}

func TestIndexOnMultipleGeneratedColumn4(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint, b decimal as (a), c int(10) as (a+b), d float as (a+b+c), e decimal as (a+b+c+d))")
	tk.MustExec("insert into t (a) values (1)")
	tk.MustExec("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 1 2 4 8"))
	res := tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")
}

func TestIndexOnMultipleGeneratedColumn5(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint, b bigint as (a+1) virtual, c bigint as (b+1) virtual)")
	tk.MustExec("alter table t add index idx_b(b)")
	tk.MustExec("alter table t add index idx_c(c)")
	tk.MustExec("insert into t(a) values(1)")
	tk.MustExec("alter table t add column(d bigint as (c+1) virtual)")
	tk.MustExec("alter table t add index idx_d(d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 3 4"))
	res := tk.MustQuery("select * from t use index(idx_d) where d > 2")
	tk.MustQuery("select * from t ignore index(idx_d) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")
}

func TestCaseInsensitiveCharsetAndCollate(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database if not exists test_charset_collate")
	defer tk.MustExec("drop database test_charset_collate")
	tk.MustExec("use test_charset_collate")
	tk.MustExec("create table t(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=UTF8_BIN;")
	tk.MustExec("create table t1(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=uTF8_BIN;")
	tk.MustExec("create table t2(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8 COLLATE=utf8_BIN;")
	tk.MustExec("create table t3(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_BIN;")
	tk.MustExec("create table t4(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_general_ci;")

	tk.MustExec("create table t5(a varchar(20)) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI;")
	tk.MustExec("insert into t5 values ('特克斯和凯科斯群岛')")

	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr("test_charset_collate"))
	require.True(t, ok)
	tbl := external.GetTableByName(t, tk, "test_charset_collate", "t5")
	tblInfo := tbl.Meta().Clone()
	require.Equal(t, "utf8mb4", tblInfo.Charset)
	require.Equal(t, "utf8mb4", tblInfo.Columns[0].GetCharset())

	tblInfo.Version = model.TableInfoVersion2
	tblInfo.Charset = "UTF8MB4"

	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = store
		err := sessiontxn.NewTxn(goctx.Background(), mockCtx)
		require.NoError(t, err)
		txn, err := mockCtx.Txn(true)
		require.NoError(t, err)
		mt := meta.NewMeta(txn)
		require.True(t, ok)
		err = mt.UpdateTable(db.ID, tblInfo)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
	}
	updateTableInfo(tblInfo)
	tk.MustExec("alter table t5 add column b varchar(10);") //  load latest schema.

	tblInfo = external.GetTableByName(t, tk, "test_charset_collate", "t5").Meta()
	require.Equal(t, "utf8mb4", tblInfo.Charset)
	require.Equal(t, "utf8mb4", tblInfo.Columns[0].GetCharset())

	// For model.TableInfoVersion3, it is believed that all charsets / collations are lower-cased, do not do case-convert
	tblInfo = tblInfo.Clone()
	tblInfo.Version = model.TableInfoVersion3
	tblInfo.Charset = "UTF8MB4"
	updateTableInfo(tblInfo)
	tk.MustExec("alter table t5 add column c varchar(10);") //  load latest schema.

	tblInfo = external.GetTableByName(t, tk, "test_charset_collate", "t5").Meta()
	require.Equal(t, "UTF8MB4", tblInfo.Charset)
	require.Equal(t, "utf8mb4", tblInfo.Columns[0].GetCharset())
}

func TestZeroFillCreateTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists abc;")
	tk.MustExec("create table abc(y year, z tinyint(10) zerofill, primary key(y));")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("abc"))
	require.NoError(t, err)
	var yearCol, zCol *model.ColumnInfo
	for _, col := range tbl.Meta().Columns {
		if col.Name.String() == "y" {
			yearCol = col
		}
		if col.Name.String() == "z" {
			zCol = col
		}
	}
	require.NotNil(t, yearCol)
	require.Equal(t, mysql.TypeYear, yearCol.GetType())
	require.True(t, mysql.HasUnsignedFlag(yearCol.GetFlag()))

	require.NotNil(t, zCol)
	require.True(t, mysql.HasUnsignedFlag(zCol.GetFlag()))
}

func TestBitDefaultValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_bit (c1 bit(10) default 250, c2 int);")
	tk.MustExec("insert into t_bit set c2=1;")
	tk.MustQuery("select bin(c1),c2 from t_bit").Check(testkit.Rows("11111010 1"))
	tk.MustExec("drop table t_bit")

	tk.MustExec("create table t_bit (a int)")
	tk.MustExec("insert into t_bit value (1)")
	tk.MustExec("alter table t_bit add column c bit(16) null default b'1100110111001'")
	tk.MustQuery("select c from t_bit").Check(testkit.Rows("\x19\xb9"))
	tk.MustExec("update t_bit set c = b'11100000000111'")
	tk.MustQuery("select c from t_bit").Check(testkit.Rows("\x38\x07"))
	tk.MustExec("drop table t_bit")

	tk.MustExec("create table t_bit (a int)")
	tk.MustExec("insert into t_bit value (1)")
	tk.MustExec("alter table t_bit add column b bit(1) default b'0';")
	tk.MustExec("alter table t_bit modify column b bit(1) default b'1';")
	tk.MustQuery("select b from t_bit").Check(testkit.Rows("\x00"))
	tk.MustExec("drop table t_bit")

	tk.MustExec("create table t_bit (a bit);")
	tk.MustExec("insert into t_bit values (null);")
	tk.MustQuery("select count(*) from t_bit where a is null;").Check(testkit.Rows("1"))

	tk.MustExec(`create table testalltypes1 (
    field_1 bit default 1,
    field_2 tinyint null default null
	);`)
	tk.MustExec(`create table testalltypes2 (
    field_1 bit null default null,
    field_2 tinyint null default null,
    field_3 tinyint unsigned null default null,
    field_4 bigint null default null,
    field_5 bigint unsigned null default null,
    field_6 mediumblob null default null,
    field_7 longblob null default null,
    field_8 blob null default null,
    field_9 tinyblob null default null,
    field_10 varbinary(255) null default null,
    field_11 binary(255) null default null,
    field_12 mediumtext null default null,
    field_13 longtext null default null,
    field_14 text null default null,
    field_15 tinytext null default null,
    field_16 char(255) null default null,
    field_17 numeric null default null,
    field_18 decimal null default null,
    field_19 integer null default null,
    field_20 integer unsigned null default null,
    field_21 int null default null,
    field_22 int unsigned null default null,
    field_23 mediumint null default null,
    field_24 mediumint unsigned null default null,
    field_25 smallint null default null,
    field_26 smallint unsigned null default null,
    field_27 float null default null,
    field_28 double null default null,
    field_29 double precision null default null,
    field_30 real null default null,
    field_31 varchar(255) null default null,
    field_32 date null default null,
    field_33 time null default null,
    field_34 datetime null default null,
    field_35 timestamp null default null
	);`)
}

func backgroundExec(s kv.Storage, sql string, done chan error) {
	se, err := session.CreateSession4Test(s)
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	defer se.Close()
	_, err = se.Execute(context.Background(), "use test")
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	_, err = se.Execute(context.Background(), sql)
	done <- errors.Trace(err)
}

func getHistoryDDLJob(store kv.Storage, id int64) (*model.Job, error) {
	var job *model.Job

	err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDDLJob(id)
		return errors.Trace(err1)
	})

	return job, errors.Trace(err)
}

func TestCreateTableTooLarge(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	sql := "create table t_too_large ("
	cnt := 3000
	for i := 1; i <= cnt; i++ {
		sql += fmt.Sprintf("a%d double, b%d double, c%d double, d%d double", i, i, i, i)
		if i != cnt {
			sql += ","
		}
	}
	sql += ");"
	tk.MustGetErrCode(sql, errno.ErrTooManyFields)

	originLimit := config.GetGlobalConfig().TableColumnCountLimit
	atomic.StoreUint32(&config.GetGlobalConfig().TableColumnCountLimit, uint32(cnt*4))
	_, err := tk.Exec(sql)
	require.Truef(t, kv.ErrEntryTooLarge.Equal(err), "err:%v", err)
	atomic.StoreUint32(&config.GetGlobalConfig().TableColumnCountLimit, originLimit)
}

func TestCreateTableTooManyIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	sql := "create table t_too_many_indexes ("
	for i := 0; i < 100; i++ {
		if i != 0 {
			sql += ","
		}
		sql += fmt.Sprintf("c%d int", i)
	}
	for i := 0; i < 100; i++ {
		sql += ","
		sql += fmt.Sprintf("key k%d(c%d)", i, i)
	}
	sql += ");"

	tk.MustGetErrCode(sql, errno.ErrTooManyKeys)
}

func TestChangeColumnPosition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table position (a int default 1, b int default 2)")
	tk.MustExec("insert into position value ()")
	tk.MustExec("insert into position values (3,4)")
	tk.MustQuery("select * from position").Check(testkit.Rows("1 2", "3 4"))
	tk.MustExec("alter table position modify column b int first")
	tk.MustQuery("select * from position").Check(testkit.Rows("2 1", "4 3"))
	tk.MustExec("insert into position value ()")
	tk.MustQuery("select * from position").Check(testkit.Rows("2 1", "4 3", "<nil> 1"))

	tk.MustExec("create table position1 (a int, b int, c double, d varchar(5))")
	tk.MustExec(`insert into position1 value (1, 2, 3.14, 'TiDB')`)
	tk.MustExec("alter table position1 modify column d varchar(5) after a")
	tk.MustQuery("select * from position1").Check(testkit.Rows("1 TiDB 2 3.14"))
	tk.MustExec("alter table position1 modify column a int after c")
	tk.MustQuery("select * from position1").Check(testkit.Rows("TiDB 2 3.14 1"))
	tk.MustExec("alter table position1 modify column c double first")
	tk.MustQuery("select * from position1").Check(testkit.Rows("3.14 TiDB 2 1"))
	tk.MustGetErrCode("alter table position1 modify column b int after b", errno.ErrBadField)

	tk.MustExec("create table position2 (a int, b int)")
	tk.MustExec("alter table position2 add index t(a, b)")
	tk.MustExec("alter table position2 modify column b int first")
	tk.MustExec("insert into position2 value (3, 5)")
	tk.MustQuery("select a from position2 where a = 3").Check(testkit.Rows())
	tk.MustExec("alter table position2 change column b c int first")
	tk.MustQuery("select * from position2 where c = 3").Check(testkit.Rows("3 5"))
	tk.MustGetErrCode("alter table position2 change column c b int after c", errno.ErrBadField)

	tk.MustExec("create table position3 (a int default 2)")
	tk.MustExec("alter table position3 modify column a int default 5 first")
	tk.MustExec("insert into position3 value ()")
	tk.MustQuery("select * from position3").Check(testkit.Rows("5"))

	tk.MustExec("create table position4 (a int, b int)")
	tk.MustExec("alter table position4 add index t(b)")
	tk.MustExec("alter table position4 change column b c int first")
	createSQL := tk.MustQuery("show create table position4").Rows()[0][1]
	expectedSQL := []string{
		"CREATE TABLE `position4` (",
		"  `c` int(11) DEFAULT NULL,",
		"  `a` int(11) DEFAULT NULL,",
		"  KEY `t` (`c`)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	}
	require.Equal(t, strings.Join(expectedSQL, "\n"), createSQL)
}

func TestAddIndexAfterAddColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table test_add_index_after_add_col(a int, b int not null default '0')")
	tk.MustExec("insert into test_add_index_after_add_col values(1, 2),(2,2)")
	tk.MustExec("alter table test_add_index_after_add_col add column c int not null default '0'")
	sql := "alter table test_add_index_after_add_col add unique index cc(c) "
	tk.MustGetErrCode(sql, errno.ErrDupEntry)
	sql = "alter table test_add_index_after_add_col add index idx_test(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17);"
	tk.MustGetErrCode(sql, errno.ErrTooManyKeyParts)
}

func TestResolveCharset(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists resolve_charset")
	tk.MustExec(`CREATE TABLE resolve_charset (a varchar(255) DEFAULT NULL) DEFAULT CHARSET=latin1`)
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("resolve_charset"))
	require.NoError(t, err)
	require.Equal(t, "latin1", tbl.Cols()[0].GetCharset())
	tk.MustExec("INSERT INTO resolve_charset VALUES('鰈')")

	tk.MustExec("create database resolve_charset charset binary")
	tk.MustExec("use resolve_charset")
	tk.MustExec(`CREATE TABLE resolve_charset (a varchar(255) DEFAULT NULL) DEFAULT CHARSET=latin1`)

	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("resolve_charset"), model.NewCIStr("resolve_charset"))
	require.NoError(t, err)
	require.Equal(t, "latin1", tbl.Cols()[0].GetCharset())
	require.Equal(t, "latin1", tbl.Meta().Charset)

	tk.MustExec(`CREATE TABLE resolve_charset1 (a varchar(255) DEFAULT NULL)`)
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("resolve_charset"), model.NewCIStr("resolve_charset1"))
	require.NoError(t, err)
	require.Equal(t, "binary", tbl.Cols()[0].GetCharset())
	require.Equal(t, "binary", tbl.Meta().Charset)
}

func TestAddColumnDefaultNow(t *testing.T) {
	// Related Issue: https://github.com/pingcap/tidb/issues/31968
	mockStore, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, mockStore)
	const dateHourLength = len("yyyy-mm-dd hh:")

	tk.MustExec("SET timestamp = 1000")
	tk.MustExec("USE test")
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1 ( i INT )")
	tk.MustExec("INSERT INTO t1 VALUES (1)")

	// test datetime
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c4 DATETIME(6) DEFAULT NOW(6) ON UPDATE NOW(6) FIRST")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c3 DATETIME(6) DEFAULT NOW(6) FIRST")

	// test timestamp
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c2 TIMESTAMP(6) DEFAULT NOW(6) ON UPDATE NOW(6) FIRST")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c1 TIMESTAMP(6) DEFAULT NOW(6) FIRST")

	tk.MustExec("SET time_zone = 'Europe/Amsterdam'")
	rows := tk.MustQuery("select * from t1").Sort().Rows()
	require.Len(t, rows, 1, "Result should has only 1 row")
	row := rows[0]
	if c3, ok := row[0].(string); ok {
		require.Equal(t, "19", c3[:len("19")], "The datetime should start with '19'")
	}
	if c4, ok := row[1].(string); ok {
		require.Equal(t, "19", c4[:len("19")], "The datetime should start with '19'")
	}

	var amsterdamC1YMDH, shanghaiC1YMDH, amsterdamC1MS, shanghaiC1MS string
	var amsterdamC2YMDH, shanghaiC2YMDH, amsterdamC2MS, shanghaiC2MS string

	if c1, ok := row[0].(string); ok {
		require.Equal(t, "19", c1[:len("19")], "The timestamp should start with '19'")

		amsterdamC1YMDH = c1[:dateHourLength] // YMDH means "yyyy-mm-dd hh"
		amsterdamC1MS = c1[dateHourLength:]   // MS means "mm-ss.fractional"
	}
	if c2, ok := row[1].(string); ok {
		require.Equal(t, "19", c2[:len("19")], "The timestamp should start with '19'")

		amsterdamC2YMDH = c2[:dateHourLength] // YMDH means "yyyy-mm-dd hh"
		amsterdamC2MS = c2[dateHourLength:]   // MS means "mm-ss.fractional"
	}

	tk.MustExec("SET time_zone = 'Asia/Shanghai'")
	rows = tk.MustQuery("select * from t1").Sort().Rows()
	require.Len(t, rows, 1, "Result should has only 1 row")
	row = rows[0]

	if c1, ok := row[0].(string); ok {
		require.Equal(t, "19", c1[:len("19")], "The timestamp should start with '19'")

		shanghaiC1YMDH = c1[:dateHourLength] // YMDH means "yyyy-mm-dd hh"
		shanghaiC1MS = c1[dateHourLength:]   // MS means "mm-ss.fractional"
	}
	if c2, ok := row[1].(string); ok {
		require.Equal(t, "19", c2[:len("19")], "The timestamp should start with '19'")

		shanghaiC2YMDH = c2[:dateHourLength] // YMDH means "yyyy-mm-dd hh"
		shanghaiC2MS = c2[dateHourLength:]   // MS means "mm-ss.fractional"
	}

	require.True(t, amsterdamC1YMDH != shanghaiC1YMDH, "The timestamp before 'hour' should not be equivalent")
	require.True(t, amsterdamC2YMDH != shanghaiC2YMDH, "The timestamp before 'hour' should not be equivalent")
	require.Equal(t, amsterdamC1MS, shanghaiC1MS, "The timestamp after 'hour' should be equivalent")
	require.Equal(t, amsterdamC2MS, shanghaiC2MS, "The timestamp after 'hour' should be equivalent")

}

func TestAddColumnTooMany(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	count := int(atomic.LoadUint32(&config.GetGlobalConfig().TableColumnCountLimit) - 1)
	var cols []string
	for i := 0; i < count; i++ {
		cols = append(cols, fmt.Sprintf("a%d int", i))
	}
	createSQL := fmt.Sprintf("create table t_column_too_many (%s)", strings.Join(cols, ","))
	tk.MustExec(createSQL)
	tk.MustExec("alter table t_column_too_many add column a_512 int")
	alterSQL := "alter table t_column_too_many add column a_513 int"
	tk.MustGetErrCode(alterSQL, errno.ErrTooManyFields)
}

func TestCreateTooManyIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	count := config.GetGlobalConfig().IndexLimit - 1
	sql := "create table t_index_too_many ("
	for i := 0; i < 100; i++ {
		if i != 0 {
			sql += ","
		}
		sql += fmt.Sprintf("c%d int", i)
	}
	for i := 0; i < count; i++ {
		sql += ","
		sql += fmt.Sprintf("key k%d(c%d)", i, i)
	}
	sql += ");"
	tk.MustExec(sql)
	tk.MustExec("create index idx1 on t_index_too_many (c62)")
	alterSQL := "create index idx2 on t_index_too_many (c63)"
	tk.MustGetErrCode(alterSQL, errno.ErrTooManyKeys)
}

func TestCreateSecondaryIndexInCluster(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test create table with non-unique key
	tk.MustExec(`
CREATE TABLE t (
  c01 varchar(255) NOT NULL,
  c02 varchar(255) NOT NULL,
  c03 varchar(255) NOT NULL,
  c04 varchar(255) DEFAULT NULL,
  c05 varchar(255) DEFAULT NULL,
  c06 varchar(255) DEFAULT NULL,
  PRIMARY KEY (c01,c02,c03) clustered,
  KEY c04 (c04)
)`)
	tk.MustExec("drop table t")

	// test create long clustered primary key.
	tk.MustGetErrCode(`
CREATE TABLE t (
  c01 varchar(255) NOT NULL,
  c02 varchar(255) NOT NULL,
  c03 varchar(255) NOT NULL,
  c04 varchar(255) NOT NULL,
  c05 varchar(255) DEFAULT NULL,
  c06 varchar(255) DEFAULT NULL,
  PRIMARY KEY (c01,c02,c03,c04) clustered
)`, errno.ErrTooLongKey)

	// test create table with unique key
	tk.MustExec(`
CREATE TABLE t (
  c01 varchar(255) NOT NULL,
  c02 varchar(255) NOT NULL,
  c03 varchar(255) NOT NULL,
  c04 varchar(255) DEFAULT NULL,
  c05 varchar(255) DEFAULT NULL,
  c06 varchar(255) DEFAULT NULL,
  PRIMARY KEY (c01,c02,c03) clustered,
  unique key c04 (c04)
)`)
	tk.MustExec("drop table t")

	// test create index
	tk.MustExec(`
CREATE TABLE t (
  c01 varchar(255) NOT NULL,
  c02 varchar(255) NOT NULL,
  c03 varchar(255) NOT NULL,
  c04 varchar(255) DEFAULT NULL,
  c05 varchar(255) DEFAULT NULL,
  c06 varchar(255) DEFAULT NULL,
  PRIMARY KEY (c01,c02) clustered
)`)
	tk.MustExec("create index idx1 on t(c03)")
	tk.MustExec("create index idx2 on t(c03, c04)")
	tk.MustExec("create unique index uk2 on t(c03, c04)")
	tk.MustExec("drop table t")

	// test change/modify column
	tk.MustExec(`
CREATE TABLE t (
  c01 varchar(255) NOT NULL,
  c02 varchar(255) NOT NULL,
  c03 varchar(255) NOT NULL,
  c04 varchar(255) DEFAULT NULL,
  c05 varchar(255) DEFAULT NULL,
  c06 varchar(255) DEFAULT NULL,
  Index idx1(c03),
  PRIMARY KEY (c01,c02) clustered,
  unique index uk1(c06)
)`)
	tk.MustExec("alter table t change c03 c10 varchar(256) default null")
	tk.MustGetErrCode("alter table t change c10 c100 varchar(1024) default null", errno.ErrTooLongKey)
	tk.MustExec("alter table t modify c10 varchar(600) default null")
	tk.MustExec("alter table t modify c06 varchar(600) default null")
	tk.MustExec("alter table t modify c01 varchar(510)")
	tk.MustExec("create table t2 like t")
}

func TestAlterColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table test_alter_column (a int default 111, b varchar(8), c varchar(8) not null, d timestamp on update current_timestamp)")
	tk.MustExec("insert into test_alter_column set b = 'a', c = 'aa'")
	tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111"))
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_alter_column"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	colA := tblInfo.Columns[0]
	hasNoDefault := mysql.HasNoDefaultValueFlag(colA.GetFlag())
	require.False(t, hasNoDefault)
	tk.MustExec("alter table test_alter_column alter column a set default 222")
	tk.MustExec("insert into test_alter_column set b = 'b', c = 'bb'")
	tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_alter_column"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	colA = tblInfo.Columns[0]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colA.GetFlag())
	require.False(t, hasNoDefault)
	tk.MustExec("alter table test_alter_column alter column b set default null")
	tk.MustExec("insert into test_alter_column set c = 'cc'")
	tk.MustQuery("select b from test_alter_column").Check(testkit.Rows("a", "b", "<nil>"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_alter_column"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	colC := tblInfo.Columns[2]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colC.GetFlag())
	require.True(t, hasNoDefault)
	tk.MustExec("alter table test_alter_column alter column c set default 'xx'")
	tk.MustExec("insert into test_alter_column set a = 123")
	tk.MustQuery("select c from test_alter_column").Check(testkit.Rows("aa", "bb", "cc", "xx"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_alter_column"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	colC = tblInfo.Columns[2]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colC.GetFlag())
	require.False(t, hasNoDefault)
	// TODO: After fix issue 2606.
	// tk.MustExec( "alter table test_alter_column alter column d set default null")
	tk.MustExec("alter table test_alter_column alter column a drop default")
	tk.MustExec("insert into test_alter_column set b = 'd', c = 'dd'")
	tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222", "222", "123", "<nil>"))

	// for failing tests
	sql := "alter table db_not_exist.test_alter_column alter column b set default 'c'"
	tk.MustGetErrCode(sql, errno.ErrNoSuchTable)
	sql = "alter table test_not_exist alter column b set default 'c'"
	tk.MustGetErrCode(sql, errno.ErrNoSuchTable)
	sql = "alter table test_alter_column alter column col_not_exist set default 'c'"
	tk.MustGetErrCode(sql, errno.ErrBadField)
	sql = "alter table test_alter_column alter column c set default null"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)

	// The followings tests whether adding constraints via change / modify column
	// is forbidden as expected.
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(a int key nonclustered, b int, c int)")
	_, err = tk.Exec("alter table mc modify column a int key") // Adds a new primary key
	require.Error(t, err)
	_, err = tk.Exec("alter table mc modify column c int unique") // Adds a new unique key
	require.Error(t, err)
	result := tk.MustQuery("show create table mc")
	createSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	require.Equal(t, expected, createSQL)

	// Change / modify column should preserve index options.
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(a int key nonclustered, b int, c int unique)")
	tk.MustExec("alter table mc modify column a bigint") // NOT NULL & PRIMARY KEY should be preserved
	tk.MustExec("alter table mc modify column b bigint")
	tk.MustExec("alter table mc modify column c bigint") // Unique should be preserved
	result = tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` bigint(20) DEFAULT NULL,\n  `c` bigint(20) DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */,\n  UNIQUE KEY `c` (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	require.Equal(t, expected, createSQL)

	// Dropping or keeping auto_increment is allowed, however adding is not allowed.
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(a int key nonclustered auto_increment, b int)")
	tk.MustExec("alter table mc modify column a bigint auto_increment") // Keeps auto_increment
	result = tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL AUTO_INCREMENT,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	require.Equal(t, expected, createSQL)
	_, err = tk.Exec("alter table mc modify column a bigint") // Droppping auto_increment is not allow when @@tidb_allow_remove_auto_inc == 'off'
	require.Error(t, err)
	tk.MustExec("set @@tidb_allow_remove_auto_inc = on")
	tk.MustExec("alter table mc modify column a bigint") // Dropping auto_increment is ok when @@tidb_allow_remove_auto_inc == 'on'
	result = tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	require.Equal(t, expected, createSQL)

	_, err = tk.Exec("alter table mc modify column a bigint auto_increment") // Adds auto_increment should throw error
	require.Error(t, err)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1 (a varchar(10),b varchar(100),c tinyint,d varchar(3071),index(a),index(a,b),index (c,d)) charset = ascii;")
	tk.MustGetErrCode("alter table t1 modify column a varchar(3000);", errno.ErrTooLongKey)
	// check modify column with rename column.
	tk.MustGetErrCode("alter table t1 change column a x varchar(3000);", errno.ErrTooLongKey)
	tk.MustGetErrCode("alter table t1 modify column c bigint;", errno.ErrTooLongKey)

	tk.MustExec("drop table if exists multi_unique")
	tk.MustExec("create table multi_unique (a int unique unique)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a int key primary key unique unique)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a int key unique unique key unique)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a serial serial default value)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a serial serial default value serial default value)")
	tk.MustExec("drop table multi_unique")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("insert into t1(id) values (1);")
	tk.MustExec("alter table t1 add column update_time3 datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3);")
	tk.MustExec("alter table t1 add column update_time6 datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);")
	rows := tk.MustQuery("select * from t1").Rows()
	require.Equal(t, "1", rows[0][0])
	updateTime3 := rows[0][1].(string)
	require.NotEqual(t, "000", updateTime3[len(updateTime3)-3:])
	updateTime6 := rows[0][2].(string)
	require.NotEqual(t, "000000", updateTime6[len(updateTime6)-6:])
}

func assertWarningExec(tk *testkit.TestKit, t *testing.T, sql string, expectedWarn *terror.Error) {
	_, err := tk.Exec(sql)
	require.NoError(t, err)
	st := tk.Session().GetSessionVars().StmtCtx
	require.Equal(t, uint16(1), st.WarningCount())
	require.Truef(t, expectedWarn.Equal(st.GetWarnings()[0].Err), "error:%v", err)
}

func assertAlterWarnExec(tk *testkit.TestKit, t *testing.T, sql string) {
	assertWarningExec(tk, t, sql, dbterror.ErrAlterOperationNotSupported)
}

func TestAlterAlgorithm(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec(`create table t(
	a int,
	b varchar(100),
	c int,
	INDEX idx_c(c)) PARTITION BY RANGE ( a ) (
	PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	)`)
	assertAlterWarnExec(tk, t, "alter table t modify column a bigint, ALGORITHM=INPLACE;")
	tk.MustExec("alter table t modify column a bigint, ALGORITHM=INPLACE, ALGORITHM=INSTANT;")
	tk.MustExec("alter table t modify column a bigint, ALGORITHM=DEFAULT;")

	// Test add/drop index
	tk.MustGetErrCode("alter table t add index idx_b(b), ALGORITHM=INSTANT", errno.ErrAlterOperationNotSupportedReason)
	assertAlterWarnExec(tk, t, "alter table t add index idx_b1(b), ALGORITHM=COPY")
	tk.MustExec("alter table t add index idx_b2(b), ALGORITHM=INPLACE")
	tk.MustExec("alter table t add index idx_b3(b), ALGORITHM=DEFAULT")
	assertAlterWarnExec(tk, t, "alter table t drop index idx_b3, ALGORITHM=INPLACE")
	assertAlterWarnExec(tk, t, "alter table t drop index idx_b1, ALGORITHM=COPY")
	tk.MustExec("alter table t drop index idx_b2, ALGORITHM=INSTANT")

	// Test rename
	assertAlterWarnExec(tk, t, "alter table t rename to t1, ALGORITHM=COPY")
	assertAlterWarnExec(tk, t, "alter table t1 rename to t2, ALGORITHM=INPLACE")
	tk.MustExec("alter table t2 rename to t, ALGORITHM=INSTANT")
	tk.MustExec("alter table t rename to t1, ALGORITHM=DEFAULT")
	tk.MustExec("alter table t1 rename to t")

	// Test rename index
	assertAlterWarnExec(tk, t, "alter table t rename index idx_c to idx_c1, ALGORITHM=COPY")
	assertAlterWarnExec(tk, t, "alter table t rename index idx_c1 to idx_c2, ALGORITHM=INPLACE")
	tk.MustExec("alter table t rename index idx_c2 to idx_c, ALGORITHM=INSTANT")
	tk.MustExec("alter table t rename index idx_c to idx_c1, ALGORITHM=DEFAULT")

	// partition.
	assertAlterWarnExec(tk, t, "alter table t ALGORITHM=COPY, truncate partition p1")
	assertAlterWarnExec(tk, t, "alter table t ALGORITHM=INPLACE, truncate partition p2")
	tk.MustExec("alter table t ALGORITHM=INSTANT, truncate partition p3")

	assertAlterWarnExec(tk, t, "alter table t add partition (partition p4 values less than (2002)), ALGORITHM=COPY")
	assertAlterWarnExec(tk, t, "alter table t add partition (partition p5 values less than (3002)), ALGORITHM=INPLACE")
	tk.MustExec("alter table t add partition (partition p6 values less than (4002)), ALGORITHM=INSTANT")

	assertAlterWarnExec(tk, t, "alter table t ALGORITHM=COPY, drop partition p4")
	assertAlterWarnExec(tk, t, "alter table t ALGORITHM=INPLACE, drop partition p5")
	tk.MustExec("alter table t ALGORITHM=INSTANT, drop partition p6")

	// Table options
	assertAlterWarnExec(tk, t, "alter table t comment = 'test', ALGORITHM=COPY")
	assertAlterWarnExec(tk, t, "alter table t comment = 'test', ALGORITHM=INPLACE")
	tk.MustExec("alter table t comment = 'test', ALGORITHM=INSTANT")

	assertAlterWarnExec(tk, t, "alter table t default charset = utf8mb4, ALGORITHM=COPY")
	assertAlterWarnExec(tk, t, "alter table t default charset = utf8mb4, ALGORITHM=INPLACE")
	tk.MustExec("alter table t default charset = utf8mb4, ALGORITHM=INSTANT")
}

func TestAlterTableAddUniqueOnPartionRangeColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec(`create table t(
	a int,
	b varchar(100),
	c int,
	INDEX idx_c(c))
	PARTITION BY RANGE COLUMNS( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	)`)
	tk.MustExec("insert into t values (4, 'xxx', 4)")
	tk.MustExec("insert into t values (4, 'xxx', 9)") // Note the repeated 4
	tk.MustExec("insert into t values (17, 'xxx', 12)")
	tk.MustGetErrCode("alter table t add unique index idx_a(a)", errno.ErrDupEntry)

	tk.MustExec("delete from t where a = 4")
	tk.MustExec("alter table t add unique index idx_a(a)")
	tk.MustExec("alter table t add unique index idx_ac(a, c)")
	tk.MustGetErrCode("alter table t add unique index idx_b(b)", errno.ErrUniqueKeyNeedAllFieldsInPf)
}

func TestFulltextIndexIgnore(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_ft")
	defer tk.MustExec("drop table if exists t_ft")
	// Make sure that creating and altering to add a fulltext key gives the correct warning
	assertWarningExec(tk, t, "create table t_ft (a text, fulltext key (a))", dbterror.ErrTableCantHandleFt)
	assertWarningExec(tk, t, "alter table t_ft add fulltext key (a)", dbterror.ErrTableCantHandleFt)

	// Make sure table t_ft still has no indexes even after it was created and altered
	r := tk.MustQuery("show index from t_ft")
	require.Len(t, r.Rows(), 0)
	r = tk.MustQuery("select * from information_schema.statistics where table_schema='test' and table_name='t_ft'")
	require.Len(t, r.Rows(), 0)

}

func TestTreatOldVersionUTF8AsUTF8MB4(t *testing.T) {
	restoreFunc := config.RestoreFunc()
	defer restoreFunc()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a varchar(10) character set utf8, b varchar(10) character set ascii) charset=utf8mb4;")
	tk.MustGetErrCode("insert into t set a= x'f09f8c80';", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Mock old version table info with column charset is utf8.
	db, ok := domain.GetDomain(tk.Session()).InfoSchema().SchemaByName(model.NewCIStr("test"))
	tbl := external.GetTableByName(t, tk, "test", "t")
	tblInfo := tbl.Meta().Clone()
	tblInfo.Version = model.TableInfoVersion0
	tblInfo.Columns[0].Version = model.ColumnInfoVersion0
	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = store
		err := sessiontxn.NewTxn(goctx.Background(), mockCtx)
		require.NoError(t, err)
		txn, err := mockCtx.Txn(true)
		require.NoError(t, err)
		mt := meta.NewMeta(txn)
		require.True(t, ok)
		err = mt.UpdateTable(db.ID, tblInfo)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
	}
	updateTableInfo(tblInfo)
	tk.MustExec("alter table t add column c varchar(10) character set utf8;") // load latest schema.
	require.True(t, config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4)
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n" +
		"  `c` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustExec("alter table t drop column c;") //  reload schema.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Mock old version table info with table and column charset is utf8.
	tbl = external.GetTableByName(t, tk, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Charset = charset.CharsetUTF8
	tblInfo.Collate = charset.CollationUTF8
	tblInfo.Version = model.TableInfoVersion0
	tblInfo.Columns[0].Version = model.ColumnInfoVersion0
	updateTableInfo(tblInfo)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustExec("alter table t add column c varchar(10);") //  load latest schema.
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n" +
		"  `c` varchar(10) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustExec("alter table t drop column c;") //  reload schema.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Test modify column charset.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustExec("alter table t modify column a varchar(10) character set utf8mb4") //  change column charset.
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, charset.CharsetUTF8MB4, tbl.Meta().Columns[0].GetCharset())
	require.Equal(t, charset.CollationUTF8MB4, tbl.Meta().Columns[0].GetCollate())
	require.Equal(t, model.ColumnInfoVersion0, tbl.Meta().Columns[0].Version)
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Test for change column should not modify the column version.
	tk.MustExec("alter table t change column a a varchar(20)") //  change column.
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, charset.CharsetUTF8MB4, tbl.Meta().Columns[0].GetCharset())
	require.Equal(t, charset.CollationUTF8MB4, tbl.Meta().Columns[0].GetCollate())
	require.Equal(t, model.ColumnInfoVersion0, tbl.Meta().Columns[0].Version)

	// Test for v2.1.5 and v2.1.6 that table version is 1 but column version is 0.
	tbl = external.GetTableByName(t, tk, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Charset = charset.CharsetUTF8
	tblInfo.Collate = charset.CollationUTF8
	tblInfo.Version = model.TableInfoVersion1
	tblInfo.Columns[0].Version = model.ColumnInfoVersion0
	tblInfo.Columns[0].SetCharset(charset.CharsetUTF8)
	tblInfo.Columns[0].SetCollate(charset.CollationUTF8)
	updateTableInfo(tblInfo)
	require.True(t, config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4)
	tk.MustExec("alter table t change column b b varchar(20) character set ascii") // reload schema.
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(20) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustExec("alter table t change column b b varchar(30) character set ascii") // reload schema.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(30) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Test for alter table convert charset
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustExec("alter table t drop column b") // reload schema.
	tk.MustExec("alter table t convert to charset utf8mb4;")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustExec("alter table t add column b varchar(50);") // reload schema.
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(50) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestDefaultValueIsString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default b'1');")
	tbl := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, "1", tbl.Meta().Columns[0].DefaultValue)
}

func TestDefaultColumnWithRand(t *testing.T) {
	// Related issue: https://github.com/pingcap/tidb/issues/10377
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, testLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2")

	// create table
	tk.MustExec("create table t (c int(10), c1 int default (rand()))")
	tk.MustExec("create table t1 (c int, c1 double default (rand()))")
	tk.MustExec("create table t2 (c int, c1 double default (rand(1)))")

	// add column with default rand() for table t is forbidden in MySQL 8.0
	tk.MustGetErrCode("alter table t add column c2 double default (rand(2))", errno.ErrBinlogUnsafeSystemFunction)
	tk.MustGetErrCode("alter table t add column c3 int default ((rand()))", errno.ErrBinlogUnsafeSystemFunction)
	tk.MustGetErrCode("alter table t add column c4 int default (((rand(3))))", errno.ErrBinlogUnsafeSystemFunction)

	// insert records
	tk.MustExec("insert into t(c) values (1),(2),(3)")
	tk.MustExec("insert into t1(c) values (1),(2),(3)")
	tk.MustExec("insert into t2(c) values (1),(2),(3)")

	queryStmts := []string{
		"SELECT c1 from t",
		"SELECT c1 from t1",
		"SELECT c1 from t2",
	}
	for _, queryStmt := range queryStmts {
		r := tk.MustQuery(queryStmt).Rows()
		for _, row := range r {
			d, ok := row[0].(float64)
			if ok {
				require.True(t, 0.0 <= d && d < 1.0, "rand() return a random floating-point value in the range 0 <= v < 1.0.")
			}
		}
	}

	tk.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `c` int(10) DEFAULT NULL,\n" +
			"  `c1` int(11) DEFAULT rand()\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table t1").Check(testkit.Rows(
		"t1 CREATE TABLE `t1` (\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `c1` double DEFAULT rand()\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table t2").Check(testkit.Rows(
		"t2 CREATE TABLE `t2` (\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `c1` double DEFAULT rand(1)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// use a non-existent function name
	tk.MustGetErrCode("CREATE TABLE t3 (c int, c1 int default a_function_not_supported_yet());", errno.ErrDefValGeneratedNamedFunctionIsNotAllowed)
}

func TestDefaultColumnWithUUID(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, testLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (c int(10), c1 varchar(256) default (uuid()))")
	// add column with default uuid() for table t is forbidden in MySQL 8.0
	tk.MustGetErrCode("alter table t add column c2 varchar(256) default (uuid())", errno.ErrBinlogUnsafeSystemFunction)
	tk.MustExec("insert into t(c) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
	// each value of UUID should differ
	r := tk.MustQuery("select c1 from t").Rows()
	set := make(map[string]bool, 10)
	for _, row := range r {
		str, _ := row[0].(string)
		_, ok := set[str]
		require.True(t, !ok, "Existing two same UUID values.")
		set[str] = true
	}
	tk.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `c` int(10) DEFAULT NULL,\n" +
			"  `c1` varchar(256) DEFAULT uuid()\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestChangingDBCharset(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("DROP DATABASE IF EXISTS alterdb1")
	tk.MustExec("CREATE DATABASE alterdb1 CHARSET=utf8 COLLATE=utf8_unicode_ci")

	// No default DB errors.
	noDBFailedCases := []struct {
		stmt   string
		errMsg string
	}{
		{
			"ALTER DATABASE CHARACTER SET = 'utf8'",
			"[planner:1046]No database selected",
		},
		{
			"ALTER SCHEMA `` CHARACTER SET = 'utf8'",
			"[ddl:1102]Incorrect database name ''",
		},
	}
	for _, fc := range noDBFailedCases {
		require.EqualError(t, tk.ExecToErr(fc.stmt), fc.errMsg)
	}

	verifyDBCharsetAndCollate := func(dbName, chs string, coll string) {
		// check `SHOW CREATE SCHEMA`.
		r := tk.MustQuery("SHOW CREATE SCHEMA " + dbName).Rows()[0][1].(string)
		require.True(t, strings.Contains(r, "CHARACTER SET "+chs))

		template := `SELECT
					DEFAULT_CHARACTER_SET_NAME,
					DEFAULT_COLLATION_NAME
				FROM INFORMATION_SCHEMA.SCHEMATA
				WHERE SCHEMA_NAME = '%s'`
		sql := fmt.Sprintf(template, dbName)
		tk.MustQuery(sql).Check(testkit.Rows(fmt.Sprintf("%s %s", chs, coll)))

		dom := domain.GetDomain(tk.Session())
		// Make sure the table schema is the new schema.
		err := dom.Reload()
		require.NoError(t, err)
		dbInfo, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(dbName))
		require.Equal(t, true, ok)
		require.Equal(t, chs, dbInfo.Charset)
		require.Equal(t, coll, dbInfo.Collate)
	}

	tk.MustExec("ALTER SCHEMA alterdb1 COLLATE = utf8mb4_general_ci")
	verifyDBCharsetAndCollate("alterdb1", "utf8mb4", "utf8mb4_general_ci")

	tk.MustExec("DROP DATABASE IF EXISTS alterdb2")
	tk.MustExec("CREATE DATABASE alterdb2 CHARSET=utf8 COLLATE=utf8_unicode_ci")
	tk.MustExec("USE alterdb2")

	failedCases := []struct {
		stmt   string
		errMsg string
	}{
		{
			"ALTER SCHEMA `` CHARACTER SET = 'utf8'",
			"[ddl:1102]Incorrect database name ''",
		},
		{
			"ALTER DATABASE CHARACTER SET = ''",
			"[parser:1115]Unknown character set: ''",
		},
		{
			"ALTER DATABASE CHARACTER SET = 'INVALID_CHARSET'",
			"[parser:1115]Unknown character set: 'INVALID_CHARSET'",
		},
		{
			"ALTER SCHEMA COLLATE = ''",
			"[ddl:1273]Unknown collation: ''",
		},
		{
			"ALTER DATABASE COLLATE = 'INVALID_COLLATION'",
			"[ddl:1273]Unknown collation: 'INVALID_COLLATION'",
		},
		{
			"ALTER DATABASE CHARACTER SET = 'utf8' DEFAULT CHARSET = 'utf8mb4'",
			"[ddl:1302]Conflicting declarations: 'CHARACTER SET utf8' and 'CHARACTER SET utf8mb4'",
		},
		{
			"ALTER SCHEMA CHARACTER SET = 'utf8' COLLATE = 'utf8mb4_bin'",
			"[ddl:1302]Conflicting declarations: 'CHARACTER SET utf8' and 'CHARACTER SET utf8mb4'",
		},
		{
			"ALTER DATABASE COLLATE = 'utf8mb4_bin' COLLATE = 'utf8_bin'",
			"[ddl:1302]Conflicting declarations: 'CHARACTER SET utf8mb4' and 'CHARACTER SET utf8'",
		},
	}

	for _, fc := range failedCases {
		require.EqualError(t, tk.ExecToErr(fc.stmt), fc.errMsg)
	}
	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8' COLLATE = 'utf8_unicode_ci'")
	verifyDBCharsetAndCollate("alterdb2", "utf8", "utf8_unicode_ci")

	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8mb4'")
	verifyDBCharsetAndCollate("alterdb2", "utf8mb4", "utf8mb4_bin")

	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8mb4' COLLATE = 'utf8mb4_general_ci'")
	verifyDBCharsetAndCollate("alterdb2", "utf8mb4", "utf8mb4_general_ci")

	// Test changing charset of schema with uppercase name. See https://github.com/pingcap/tidb/issues/19273.
	tk.MustExec("drop database if exists TEST_UPPERCASE_DB_CHARSET;")
	tk.MustExec("create database TEST_UPPERCASE_DB_CHARSET;")
	tk.MustExec("use TEST_UPPERCASE_DB_CHARSET;")
	tk.MustExec("alter database TEST_UPPERCASE_DB_CHARSET default character set utf8;")
	tk.MustExec("alter database TEST_UPPERCASE_DB_CHARSET default character set utf8mb4;")
}

func TestDropAutoIncrementIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int(11) not null auto_increment key, b int(11), c bigint, unique key (a, b, c))")
	tk.MustExec("alter table t1 drop index a")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int auto_increment, unique key (a))")
	dropIndexSQL := "alter table t1 drop index a"
	tk.MustGetErrCode(dropIndexSQL, errno.ErrWrongAutoKey)

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int(11) not null auto_increment, b int(11), c bigint, unique key (a, b, c))")
	dropIndexSQL = "alter table t1 drop index a"
	tk.MustGetErrCode(dropIndexSQL, errno.ErrWrongAutoKey)
}

func TestInsertIntoGeneratedColumnWithDefaultExpr(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	// insert into virtual / stored columns
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int as (-a) virtual, c int as (-a) stored)")
	tk.MustExec("insert into t1 values (1, default, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 -1 -1"))
	tk.MustExec("delete from t1")

	// insert multiple rows
	tk.MustExec("insert into t1(a,b) values (1, default), (2, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 -1 -1", "2 -2 -2"))
	tk.MustExec("delete from t1")

	// insert into generated columns only
	tk.MustExec("insert into t1(b) values (default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1(c) values (default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec("delete from t1")

	// generated columns with index
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 like t1")
	tk.MustExec("alter table t2 add index idx1(a)")
	tk.MustExec("alter table t2 add index idx2(b)")
	tk.MustExec("insert into t2 values (1, default, default)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 -1 -1"))
	tk.MustExec("delete from t2")
	tk.MustExec("alter table t2 drop index idx1")
	tk.MustExec("alter table t2 drop index idx2")
	tk.MustExec("insert into t2 values (1, default, default)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 -1 -1"))

	// generated columns in different position
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (gc1 int as (r+1), gc2 int as (r+1) stored, gc3 int as (gc2+1), gc4 int as (gc1+1) stored, r int)")
	tk.MustExec("insert into t3 values (default, default, default, default, 1)")
	tk.MustQuery("select * from t3").Check(testkit.Rows("2 2 3 3 1"))

	// generated columns in replace statement
	tk.MustExec("create table t4 (a int key, b int, c int as (a+1), d int as (b+1) stored)")
	tk.MustExec("insert into t4 values (1, 10, default, default)")
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 10 2 11"))
	tk.MustExec("replace into t4 values (1, 20, default, default)")
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 20 2 21"))

	// generated columns with default function is not allowed
	tk.MustExec("create table t5 (a int default 10, b int as (a+1))")
	tk.MustGetErrCode("insert into t5 values (20, default(a))", errno.ErrBadGeneratedColumn)

	tk.MustExec("drop table t1, t2, t3, t4, t5")
}

func TestSqlFunctionsInGeneratedColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")

	// In generated columns expression, these items are not allowed:
	// 1. Blocked function (for full function list, please visit https://github.com/mysql/mysql-server/blob/5.7/errno-test/suite/gcol/inc/gcol_blocked_sql_funcs_main.inc)
	// Note: This list is not complete, if you need a complete list, please refer to MySQL 5.7 source code.
	tk.MustGetErrCode("create table t (a int, b int as (sysdate()))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 2. Non-builtin function
	tk.MustGetErrCode("create table t (a int, b int as (non_exist_funcA()))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 3. values(x) function
	tk.MustGetErrCode("create table t (a int, b int as (values(a)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 4. Subquery
	tk.MustGetErrCode("create table t (a int, b int as ((SELECT 1 FROM t1 UNION SELECT 1 FROM t1)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 5. Variable & functions related to variable
	tk.MustExec("set @x = 1")
	tk.MustGetErrCode("create table t (a int, b int as (@x))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("create table t (a int, b int as (@@max_connections))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("create table t (a int, b int as (@y:=1))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode(`create table t (a int, b int as (getvalue("x")))`, errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode(`create table t (a int, b int as (setvalue("y", 1)))`, errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 6. Aggregate function
	tk.MustGetErrCode("create table t1 (a int, b int as (avg(a)));", errno.ErrInvalidGroupFuncUse)

	// Determinate functions are allowed:
	tk.MustExec("create table t1 (a int, b int generated always as (abs(a)) virtual)")
	tk.MustExec("insert into t1 values (-1, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("-1 1"))

	// Functions added in MySQL 8.0, but now not supported in TiDB
	// They will be deal with non-exists function, and throw error.git
	tk.MustGetErrCode("create table t (a int, b int as (updatexml(1, 1, 1)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("create table t (a int, b int as (statement_digest(1)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("create table t (a int, b int as (statement_digest_text(1)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)

	// NOTE (#18150): In creating generated column, row value is not allowed.
	tk.MustGetErrCode("create table t (a int, b int as ((a, a)))", errno.ErrGeneratedColumnRowValueIsNotAllowed)
	tk.MustExec("create table t (a int, b int as ((a)))")
}

func TestParserIssue284(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test.t_parser_issue_284(c1 int not null primary key)")
	_, err := tk.Exec("create table test.t_parser_issue_284_2(id int not null primary key, c1 int not null, constraint foreign key (c1) references t_parser_issue_284(c1))")
	require.NoError(t, err)

	tk.MustExec("drop table test.t_parser_issue_284")
	tk.MustExec("drop table test.t_parser_issue_284_2")
}

func TestAddExpressionIndex(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		// Test for table lock.
		conf.EnableTableLock = true
		conf.Instance.SlowThreshold = 10000
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")

	tk.MustExec("create table t (a int, b real);")
	tk.MustExec("insert into t values (1, 2.1);")
	tk.MustExec("alter table t add index idx((a+b));")
	tk.MustQuery("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE table_name = 't'").Check(testkit.Rows())

	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	columns := tblInfo.Meta().Columns
	require.Equal(t, 3, len(columns))
	require.True(t, columns[2].Hidden)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustExec("alter table t add index idx_multi((a+b),(a+1), b);")
	tblInfo, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	columns = tblInfo.Meta().Columns
	require.Equal(t, 5, len(columns))
	require.True(t, columns[3].Hidden)
	require.True(t, columns[4].Hidden)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustExec("alter table t drop index idx;")
	tblInfo, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	columns = tblInfo.Meta().Columns
	require.Equal(t, 4, len(columns))

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustExec("alter table t drop index idx_multi;")
	tblInfo, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	columns = tblInfo.Meta().Columns
	require.Equal(t, 2, len(columns))

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	// Issue #26371
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, primary key(a, b) clustered)")
	tk.MustExec("alter table t1 add index idx((a+1))")

	// Issue #17111
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a varchar(10), b varchar(10));")
	tk.MustExec("alter table t1 add unique index ei_ab ((concat(a, b)));")
	tk.MustExec("alter table t1 alter index ei_ab invisible;")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, key((a+1)), key((a+2)), key idx((a+3)), key((a+4)));")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (A INT, B INT, UNIQUE KEY ((A * 2)));")

	// Test experiment switch.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = false
	})
	tk.MustGetErrMsg("create index d on t((repeat(a, 2)))", "[ddl:8200]Unsupported creating expression index containing unsafe functions without allow-expression-index in config")
	tk.MustGetErrMsg("create table t(a char(10), key ((repeat(a, 2))));", "[ddl:8200]Unsupported creating expression index containing unsafe functions without allow-expression-index in config")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), key ((lower(a))))")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = true
	})
}

func TestCreateExpressionIndexError(t *testing.T) {
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
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b real);")
	tk.MustGetErrCode("alter table t add primary key ((a+b)) nonclustered;", errno.ErrFunctionalIndexPrimaryKey)

	tk.MustGetErrCode("create table t(a int, index((cast(a as JSON))))", errno.ErrFunctionalIndexOnJSONOrGeometryFunction)

	// Test for error
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b real);")
	tk.MustGetErrCode("alter table t add primary key ((a+b)) nonclustered;", errno.ErrFunctionalIndexPrimaryKey)
	tk.MustGetErrCode("alter table t add index ((rand()));", errno.ErrFunctionalIndexFunctionIsNotAllowed)
	tk.MustGetErrCode("alter table t add index ((now()+1));", errno.ErrFunctionalIndexFunctionIsNotAllowed)

	tk.MustExec("alter table t add column (_V$_idx_0 int);")
	tk.MustGetErrCode("alter table t add index idx((a+1));", errno.ErrDupFieldName)
	tk.MustExec("alter table t drop column _V$_idx_0;")
	tk.MustExec("alter table t add index idx((a+1));")
	tk.MustGetErrCode("alter table t add column (_V$_idx_0 int);", errno.ErrDupFieldName)
	tk.MustExec("alter table t drop index idx;")
	tk.MustExec("alter table t add column (_V$_idx_0 int);")

	tk.MustExec("alter table t add column (_V$_expression_index_0 int);")
	tk.MustGetErrCode("alter table t add index ((a+1));", errno.ErrDupFieldName)
	tk.MustExec("alter table t drop column _V$_expression_index_0;")
	tk.MustExec("alter table t add index ((a+1));")
	tk.MustGetErrCode("alter table t drop column _V$_expression_index_0;", errno.ErrCantDropFieldOrKey)
	tk.MustGetErrCode("alter table t add column e int as (_V$_expression_index_0 + 1);", errno.ErrBadField)

	// NOTE (#18150): In creating expression index, row value is not allowed.
	tk.MustExec("drop table if exists t;")
	tk.MustGetErrCode("create table t (j json, key k (((j,j))))", errno.ErrFunctionalIndexRowValueIsNotAllowed)
	tk.MustExec("create table t (j json, key k ((j+1),(j+1)))")

	tk.MustGetErrCode("create table t1 (col1 int, index ((concat(''))));", errno.ErrWrongKeyColumnFunctionalIndex)
	tk.MustGetErrCode("CREATE TABLE t1 (col1 INT, PRIMARY KEY ((ABS(col1))) NONCLUSTERED);", errno.ErrFunctionalIndexPrimaryKey)

	// For issue 26349
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(id char(10) primary key, short_name char(10), name char(10), key n((upper(`name`))));")
	tk.MustExec("update t t1 set t1.short_name='a' where t1.id='1';")
}

func TestAddExpressionIndexOnPartition(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t(
	a int,
	b varchar(100),
	c int)
	PARTITION BY RANGE ( a ) (
	PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	);`)
	tk.MustExec("insert into t values (1, 'test', 2), (12, 'test', 3), (15, 'test', 10), (20, 'test', 20);")
	tk.MustExec("alter table t add index idx((a+c));")

	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	columns := tblInfo.Meta().Columns
	require.Equal(t, 4, len(columns))
	require.True(t, columns[3].Hidden)

	tk.MustQuery("select * from t order by a;").Check(testkit.Rows("1 test 2", "12 test 3", "15 test 10", "20 test 20"))
}

// TestCreateTableWithAutoIdCache test the auto_id_cache table option.
// `auto_id_cache` take effects on handle too when `PKIshandle` is false,
// or even there is no auto_increment column at all.
func TestCreateTableWithAutoIdCache(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")

	// Test primary key is handle.
	tk.MustExec("create table t(a int auto_increment key clustered) auto_id_cache 100")
	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, int64(100), tblInfo.Meta().AutoIdCache)
	tk.MustExec("insert into t values()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache
	tk.MustExec("rename table t to t1;")
	tk.MustExec("insert into t1 values()")
	tk.MustQuery("select * from t1").Check(testkit.Rows("101"))

	// Test primary key is not handle.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(a int) auto_id_cache 100")
	_, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	tk.MustExec("insert into t values()")
	tk.MustQuery("select _tidb_rowid from t").Check(testkit.Rows("1"))
	tk.MustExec("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache
	tk.MustExec("rename table t to t1;")
	tk.MustExec("insert into t1 values()")
	tk.MustQuery("select _tidb_rowid from t1").Check(testkit.Rows("101"))

	// Test both auto_increment and rowid exist.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(a int null, b int auto_increment unique) auto_id_cache 100")
	_, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	tk.MustExec("insert into t(b) values(NULL)")
	tk.MustQuery("select b, _tidb_rowid from t").Check(testkit.Rows("1 2"))
	tk.MustExec("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustExec("rename table t to t1;")
	tk.MustExec("insert into t1(b) values(NULL)")
	tk.MustQuery("select b, _tidb_rowid from t1").Check(testkit.Rows("101 102"))
	tk.MustExec("delete from t1")

	// Test alter auto_id_cache.
	tk.MustExec("alter table t1 auto_id_cache 200")
	tblInfo, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, int64(200), tblInfo.Meta().AutoIdCache)

	tk.MustExec("insert into t1(b) values(NULL)")
	tk.MustQuery("select b, _tidb_rowid from t1").Check(testkit.Rows("201 202"))
	tk.MustExec("delete from t1")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustExec("rename table t1 to t;")
	tk.MustExec("insert into t(b) values(NULL)")
	tk.MustQuery("select b, _tidb_rowid from t").Check(testkit.Rows("401 402"))
	tk.MustExec("delete from t")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(a int auto_increment key clustered) auto_id_cache 3")
	tblInfo, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, int64(3), tblInfo.Meta().AutoIdCache)

	// Test insert batch size(4 here) greater than the customized autoid step(3 here).
	tk.MustExec("insert into t(a) values(NULL),(NULL),(NULL)")
	// Cache 3 more values. We can't merge this two lines because the batch allocation overrides autoid step.
	tk.MustExec("insert into t(a) values(NULL)")
	tk.MustQuery("select a from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustExec("rename table t to t1;")
	tk.MustExec("insert into t1(a) values(NULL)")
	next := tk.MustQuery("select a from t1").Rows()[0][0].(string)
	nextInt, err := strconv.Atoi(next)
	require.NoError(t, err)
	require.Greater(t, nextInt, 5)

	// Test auto_id_cache overflows int64.
	tk.MustExec("drop table if exists t;")
	_, err = tk.Exec("create table t(a int) auto_id_cache = 9223372036854775808")
	require.Error(t, err)
	require.Equal(t, "table option auto_id_cache overflows int64", err.Error())

	tk.MustExec("create table t(a int) auto_id_cache = 9223372036854775807")
	_, err = tk.Exec("alter table t auto_id_cache = 9223372036854775808")
	require.Error(t, err)
	require.Equal(t, "table option auto_id_cache overflows int64", err.Error())
}

func TestAlterIndexVisibility(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists alter_index_test")
	tk.MustExec("USE alter_index_test;")
	tk.MustExec("drop table if exists t, t1, t2, t3;")

	tk.MustExec("create table t(a int NOT NULL, b int, key(a), unique(b) invisible)")
	query := queryIndexOnTable("alter_index_test", "t")
	tk.MustQuery(query).Check(testkit.Rows("a YES", "b NO"))

	tk.MustExec("alter table t alter index a invisible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b NO"))

	tk.MustExec("alter table t alter index b visible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b YES"))

	tk.MustExec("alter table t alter index b invisible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b NO"))

	tk.MustGetErrMsg("alter table t alter index non_exists_idx visible", "[schema:1176]Key 'non_exists_idx' doesn't exist in table 't'")

	// Alter implicit primary key to invisible index should throw error
	tk.MustExec("create table t1(a int NOT NULL, unique(a))")
	tk.MustGetErrMsg("alter table t1 alter index a invisible", "[ddl:3522]A primary key index cannot be invisible")

	// Alter explicit primary key to invisible index should throw error
	tk.MustExec("create table t2(a int, primary key(a))")
	tk.MustGetErrMsg("alter table t2 alter index PRIMARY invisible", `[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 34 near "PRIMARY invisible" `)

	// Alter expression index
	tk.MustExec("create table t3(a int NOT NULL, b int)")
	tk.MustExec("alter table t3 add index idx((a+b));")
	query = queryIndexOnTable("alter_index_test", "t3")
	tk.MustQuery(query).Check(testkit.Rows("idx YES"))

	tk.MustExec("alter table t3 alter index idx invisible")
	tk.MustQuery(query).Check(testkit.Rows("idx NO"))
}

func queryIndexOnTable(dbName, tableName string) string {
	return fmt.Sprintf("select distinct index_name, is_visible from information_schema.statistics where table_schema = '%s' and table_name = '%s' order by index_name", dbName, tableName)
}

func TestDropColumnWithCompositeIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	query := queryIndexOnTable("drop_composite_index_test", "t_drop_column_with_comp_idx")
	tk.MustExec("create database if not exists drop_composite_index_test")
	tk.MustExec("use drop_composite_index_test")
	tk.MustExec("create table t_drop_column_with_comp_idx(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t_drop_column_with_comp_idx")
	tk.MustExec("create index idx_bc on t_drop_column_with_comp_idx(b, c)")
	tk.MustExec("create index idx_b on t_drop_column_with_comp_idx(b)")
	tk.MustGetErrMsg("alter table t_drop_column_with_comp_idx drop column b", "[ddl:8200]can't drop column b with composite index covered or Primary Key covered now")
	tk.MustQuery(query).Check(testkit.Rows("idx_b YES", "idx_bc YES"))
	tk.MustExec("alter table t_drop_column_with_comp_idx alter index idx_bc invisible")
	tk.MustExec("alter table t_drop_column_with_comp_idx alter index idx_b invisible")
	tk.MustGetErrMsg("alter table t_drop_column_with_comp_idx drop column b", "[ddl:8200]can't drop column b with composite index covered or Primary Key covered now")
	tk.MustQuery(query).Check(testkit.Rows("idx_b NO", "idx_bc NO"))
}

func TestDropColumnWithIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_column_with_idx(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t_drop_column_with_idx")
	tk.MustExec("create index idx on t_drop_column_with_idx(b)")
	tk.MustExec("alter table t_drop_column_with_idx drop column b")
	query := queryIndexOnTable("test", "t_drop_column_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func TestDropColumnWithMultiIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_column_with_idx(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t_drop_column_with_idx")
	tk.MustExec("create index idx_1 on t_drop_column_with_idx(b)")
	tk.MustExec("create index idx_2 on t_drop_column_with_idx(b)")
	tk.MustExec("alter table t_drop_column_with_idx drop column b")
	query := queryIndexOnTable("test", "t_drop_column_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func TestDropColumnsWithMultiIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_columns_with_idx(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t_drop_columns_with_idx")
	tk.MustExec("create index idx_1 on t_drop_columns_with_idx(b)")
	tk.MustExec("create index idx_2 on t_drop_columns_with_idx(b)")
	tk.MustExec("create index idx_3 on t_drop_columns_with_idx(c)")
	tk.MustExec("alter table t_drop_columns_with_idx drop column b, drop column c")
	query := queryIndexOnTable("test", "t_drop_columns_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func TestDropLastVisibleColumnOrColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_last_column(x int, key((1+1)))")
	_, err := tk.Exec("alter table t_drop_last_column drop column x")
	require.Error(t, err)
	require.Equal(t, "[ddl:1113]A table must have at least 1 column", err.Error())
	// for visible columns
	tk.MustExec("create table t_drop_last_columns(x int, y int, key((1+1)))")
	_, err = tk.Exec("alter table t_drop_last_columns drop column x, drop column y")
	require.Error(t, err)
	require.Equal(t, "[ddl:1113]A table must have at least 1 column", err.Error())

	tk.MustExec("drop table if exists t_drop_last_column, t_drop_last_columns")
}

func TestAutoIncrementTableOption(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_auto_inc_table_opt;")
	tk.MustExec("create database test_auto_inc_table_opt;")
	tk.MustExec("use test_auto_inc_table_opt;")

	// Empty auto_inc allocator should not cause error.
	tk.MustExec("create table t (a bigint primary key clustered) auto_increment = 10;")
	tk.MustExec("alter table t auto_increment = 10;")
	tk.MustExec("alter table t auto_increment = 12345678901234567890;")

	// Rebase the auto_inc allocator to a large integer should work.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a bigint unsigned auto_increment, unique key idx(a));")
	tk.MustExec("alter table t auto_increment = 12345678901234567890;")
	tk.MustExec("insert into t values ();")
	tk.MustQuery("select * from t;").Check(testkit.Rows("12345678901234567890"))
}

func TestAutoIncrementForce(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists auto_inc_force;")
	tk.MustExec("create database auto_inc_force;")
	tk.MustExec("use auto_inc_force;")
	getNextGlobalID := func() uint64 {
		gidStr := tk.MustQuery("show table t next_row_id").Rows()[0][3]
		gid, err := strconv.ParseUint(gidStr.(string), 10, 64)
		require.NoError(t, err)
		return gid
	}
	// Rebase _tidb_row_id.
	tk.MustExec("create table t (a int);")
	tk.MustExec("alter table t force auto_increment = 2;")
	tk.MustExec("insert into t values (1),(2);")
	tk.MustQuery("select a, _tidb_rowid from t;").Check(testkit.Rows("1 2", "2 3"))
	// Cannot set next global ID to 0.
	tk.MustGetErrCode("alter table t force auto_increment = 0;", errno.ErrAutoincReadFailed)
	tk.MustExec("alter table t force auto_increment = 1;")
	require.Equal(t, uint64(1), getNextGlobalID())
	// inserting new rows can overwrite the existing data.
	tk.MustExec("insert into t values (3);")
	require.Equal(t, "[kv:1062]Duplicate entry '2' for key 'PRIMARY'", tk.ExecToErr("insert into t values (3);").Error())
	tk.MustQuery("select a, _tidb_rowid from t;").Check(testkit.Rows("3 1", "1 2", "2 3"))

	// Rebase auto_increment.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int primary key auto_increment, b int);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (100000000, 1);")
	tk.MustExec("delete from t where a = 100000000;")
	require.Greater(t, getNextGlobalID(), uint64(100000000))
	// Cannot set next global ID to 0.
	tk.MustGetErrCode("alter table t /*T![force_inc] force */ auto_increment = 0;", errno.ErrAutoincReadFailed)
	tk.MustExec("alter table t /*T![force_inc] force */ auto_increment = 2;")
	require.Equal(t, uint64(2), getNextGlobalID())
	tk.MustExec("insert into t(b) values (2);")
	tk.MustQuery("select a, b from t;").Check(testkit.Rows("1 1", "2 2"))

	// Rebase auto_random.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint primary key auto_random(5));")
	tk.MustExec("insert into t values ();")
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")
	tk.MustExec("insert into t values (100000000);")
	tk.MustExec("delete from t where a = 100000000;")
	require.Greater(t, getNextGlobalID(), uint64(100000000))
	// Cannot set next global ID to 0.
	tk.MustGetErrCode("alter table t force auto_random_base = 0;", errno.ErrAutoincReadFailed)
	tk.MustExec("alter table t force auto_random_base = 2;")
	require.Equal(t, uint64(2), getNextGlobalID())
	tk.MustExec("insert into t values ();")
	tk.MustQuery("select (a & 3) from t order by 1;").Check(testkit.Rows("1", "2"))

	// Change next global ID.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint primary key auto_increment);")
	tk.MustExec("insert into t values (1);")
	bases := []uint64{1, 65535, 10, math.MaxUint64, math.MaxInt64 + 1, 1, math.MaxUint64, math.MaxInt64, 2}
	lastBase := fmt.Sprintf("%d", bases[len(bases)-1])
	for _, b := range bases {
		tk.MustExec(fmt.Sprintf("alter table t force auto_increment = %d;", b))
		require.Equal(t, b, getNextGlobalID())
	}
	tk.MustExec("insert into t values ();")
	tk.MustQuery("select a from t;").Check(testkit.Rows("1", lastBase))
	// Force alter unsigned int auto_increment column.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint unsigned primary key auto_increment);")
	for _, b := range bases {
		tk.MustExec(fmt.Sprintf("alter table t force auto_increment = %d;", b))
		require.Equal(t, b, getNextGlobalID())
		tk.MustExec("insert into t values ();")
		tk.MustQuery("select a from t;").Check(testkit.Rows(fmt.Sprintf("%d", b)))
		tk.MustExec("delete from t;")
	}

	// Force alter with @@auto_increment_increment and @@auto_increment_offset.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int key auto_increment);")
	tk.MustExec("set @@auto_increment_offset=2;")
	tk.MustExec("set @@auto_increment_increment = 11;")
	tk.MustExec("insert into t values (500);")
	tk.MustExec("alter table t force auto_increment=100;")
	tk.MustExec("insert into t values (), ();")
	tk.MustQuery("select * from t;").Check(testkit.Rows("101", "112", "500"))
	tk.MustQuery("select * from t order by a;").Check(testkit.Rows("101", "112", "500"))
	tk.MustExec("drop table if exists t;")

	// Check for warning in case we can't set the auto_increment to the desired value
	tk.MustExec("create table t(a int primary key auto_increment)")
	tk.MustExec("insert into t values (200)")
	tk.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=5201"))
	tk.MustExec("alter table t auto_increment=100;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Can't reset AUTO_INCREMENT to 100 without FORCE option, using 5201 instead"))
	tk.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=5201"))
	tk.MustExec("drop table t")
}

func TestIssue20490(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table issue20490 (a int);")
	tk.MustExec("insert into issue20490(a) values(1);")
	tk.MustExec("alter table issue20490 add b int not null default 1;")
	tk.MustExec("insert into issue20490(a) values(2);")
	tk.MustExec("alter table issue20490 modify b int null;")
	tk.MustExec("insert into issue20490(a) values(3);")

	tk.MustQuery("select b from issue20490 order by a;").Check(testkit.Rows("1", "1", "<nil>"))
}

func TestIssue20741WithEnumField(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists issue20741")
	tk.MustExec("create table issue20741(id int primary key, c int)")
	tk.MustExec("insert into issue20741(id, c) values(1, 2), (2, 2)")
	tk.MustExec("alter table issue20741 add column cc enum('a', 'b', 'c', 'd') not null")
	tk.MustExec("update issue20741 set c=2 where id=1")
	tk.MustQuery("select * from issue20741").Check(testkit.Rows("1 2 a", "2 2 a"))
	tk.MustQuery("select * from issue20741 where cc = 0").Check(testkit.Rows())
	tk.MustQuery("select * from issue20741 where cc = 1").Check(testkit.Rows("1 2 a", "2 2 a"))
}

func TestIssue20741WithSetField(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists issue20741_2")
	tk.MustExec("create table issue20741_2(id int primary key, c int)")
	tk.MustExec("insert into issue20741_2(id, c) values(1, 2), (2, 2)")
	tk.MustExec("alter table issue20741_2 add column cc set('a', 'b', 'c', 'd') not null")
	tk.MustExec("update issue20741_2 set c=2 where id=1")
	tk.MustQuery("select * from issue20741_2").Check(testkit.Rows("1 2 ", "2 2 "))
	tk.MustQuery("select * from issue20741_2 where cc = 0").Check(testkit.Rows("1 2 ", "2 2 "))
	tk.MustQuery("select * from issue20741_2 where cc = 1").Check(testkit.Rows())
	_, err := tk.Exec("insert into issue20741_2(id, c) values (3, 3)")
	require.Error(t, err)
	require.Equal(t, "[table:1364]Field 'cc' doesn't have a default value", err.Error())
}

// TestDefaultValueIsLatin1 for issue #18977
func TestEnumAndSetDefaultValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a enum(0x61, 'b') not null default 0x61, b set(0x61, 'b') not null default 0x61) character set latin1")
	tbl := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, "a", tbl.Meta().Columns[0].DefaultValue)
	require.Equal(t, "a", tbl.Meta().Columns[1].DefaultValue)

	tk.MustExec("drop table t")
	tk.MustExec("create table t (a enum(0x61, 'b') not null default 0x61, b set(0x61, 'b') not null default 0x61) character set utf8mb4")
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, "a", tbl.Meta().Columns[0].DefaultValue)
	require.Equal(t, "a", tbl.Meta().Columns[1].DefaultValue)
}

func TestStrictDoubleTypeCheck(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_strict_double_type_check = 'ON'")
	sql := "create table double_type_check(id int, c double(10));"
	_, err := tk.Exec(sql)
	require.Error(t, err)
	require.Equal(t, "[parser:1149]You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use", err.Error())
	tk.MustExec("set @@tidb_enable_strict_double_type_check = 'OFF'")
	defer tk.MustExec("set @@tidb_enable_strict_double_type_check = 'ON'")
	tk.MustExec(sql)
}

func TestDuplicateErrorMessage(t *testing.T) {
	defer collate.SetNewCollationEnabledForTest(true)
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	type testdata struct {
		types  []string
		values []string
	}
	tests := []testdata{
		{[]string{"int"}, []string{"1"}},
		{[]string{"datetime"}, []string{"'2020-01-01 00:00:00'"}},
		{[]string{"varchar(10)"}, []string{"'qwe'"}},
		{[]string{"enum('r', 'g', 'b')"}, []string{"'r'"}},
		{[]string{"int", "datetime", "varchar(10)", "enum('r', 'g', 'b')"}, []string{"1", "'2020-01-01 00:00:00'", "'qwe'", "'r'"}},
	}

	for _, newCollate := range []bool{false, true} {
		collate.SetNewCollationEnabledForTest(newCollate)
		for _, globalIndex := range []bool{false, true} {
			restoreConfig := config.RestoreFunc()
			config.UpdateGlobal(func(conf *config.Config) {
				conf.EnableGlobalIndex = globalIndex
			})
			for _, clusteredIndex := range []variable.ClusteredIndexDefMode{variable.ClusteredIndexDefModeOn, variable.ClusteredIndexDefModeOff, variable.ClusteredIndexDefModeIntOnly} {
				tk.Session().GetSessionVars().EnableClusteredIndex = clusteredIndex
				for _, t := range tests {
					tk.MustExec("drop table if exists t;")
					fields := make([]string, len(t.types))

					for i, tp := range t.types {
						fields[i] = fmt.Sprintf("a%d %s", i, tp)
					}
					tk.MustExec("create table t (id1 int, id2 varchar(10), " + strings.Join(fields, ",") + ",primary key(id1, id2)) " +
						"collate utf8mb4_general_ci " +
						"partition by range (id1) (partition p1 values less than (2), partition p2 values less than (maxvalue))")

					vals := strings.Join(t.values, ",")
					tk.MustExec(fmt.Sprintf("insert into t values (1, 'asd', %s), (1, 'dsa', %s)", vals, vals))
					for i := range t.types {
						fields[i] = fmt.Sprintf("a%d", i)
					}
					index := strings.Join(fields, ",")
					for i, val := range t.values {
						fields[i] = strings.Replace(val, "'", "", -1)
					}
					tk.MustGetErrMsg("alter table t add unique index t_idx(id1,"+index+")",
						fmt.Sprintf("[kv:1062]Duplicate entry '1-%s' for key 't_idx'", strings.Join(fields, "-")))
				}
			}
			restoreConfig()
		}
	}
}

func TestIssue22028(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	_, err := tk.Exec("create table t(a double(0, 0));")
	require.Equal(t, "[types:1439]Display width out of range for column 'a' (max = 255)", err.Error())

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a double);")
	_, err = tk.Exec("ALTER TABLE t MODIFY COLUMN a DOUBLE(0,0);")
	require.Equal(t, "[types:1439]Display width out of range for column 'a' (max = 255)", err.Error())
}

func TestIssue21835(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	_, err := tk.Exec("create table t( col decimal(1,2) not null default 0);")
	require.Equal(t, "[types:1427]For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'col').", err.Error())
}

func TestCreateTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")

	// Grammar error.
	tk.MustGetErrCode("create global temporary table t(a double(0, 0))", errno.ErrParse)
	tk.MustGetErrCode("create temporary table t(id int) on commit delete rows", errno.ErrParse)
	tk.MustGetErrCode("create temporary table t(id int) on commit preserve rows", errno.ErrParse)
	tk.MustGetErrCode("create table t(id int) on commit delete rows", errno.ErrParse)
	tk.MustGetErrCode("create table t(id int) on commit preserve rows", errno.ErrParse)

	// Not support yet.
	tk.MustGetErrCode("create global temporary table t (id int) on commit preserve rows", errno.ErrUnsupportedDDLOperation)

	// Engine type can be anyone, see https://github.com/pingcap/tidb/issues/28541.
	tk.MustExec("drop table if exists tengine")
	tk.MustExec("create global temporary table tengine (id int) engine = 'innodb' on commit delete rows")
	tk.MustExec("drop table if exists tengine")
	tk.MustExec("create global temporary table tengine (id int) engine = 'memory' on commit delete rows")
	tk.MustExec("drop table if exists tengine")
	tk.MustExec("create global temporary table tengine (id int) engine = 'myisam' on commit delete rows")
	tk.MustExec("drop table if exists tengine")
	tk.MustExec("create temporary table tengine (id int) engine = 'innodb'")
	tk.MustExec("drop table if exists tengine")
	tk.MustExec("create temporary table tengine (id int) engine = 'memory'")
	tk.MustExec("drop table if exists tengine")
	tk.MustExec("create temporary table tengine (id int) engine = 'myisam'")
	tk.MustExec("drop table if exists tengine")

	// Create local temporary table.
	tk.MustExec("create database tmp_db")
	tk.MustExec("use tmp_db")
	tk.MustExec("create temporary table t1 (id int)")
	// Create a normal table with the same name is ok.
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create temporary table tmp_db.t2 (id int)")
	tk.MustQuery("select * from t1") // No error
	tk.MustExec("drop database tmp_db")
	_, err := tk.Exec("select * from t1")
	require.Error(t, err)
	// In MySQL, drop DB does not really drop the table, it's back!
	tk.MustExec("create database tmp_db")
	tk.MustExec("use tmp_db")
	tk.MustQuery("select * from t1") // No error

	// When local temporary table overlap the normal table, it takes a higher priority.
	tk.MustExec("create table overlap (id int)")
	tk.MustExec("create temporary table overlap (a int, b int)")
	_, err = tk.Exec("insert into overlap values (1)") // column not match
	require.Error(t, err)
	tk.MustExec("insert into overlap values (1, 1)")

	// Check create local temporary table does not auto commit the transaction.
	// Normal DDL implies a commit, but create temporary does not.
	tk.MustExec("create table check_data (id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into check_data values (1)")
	tk.MustExec("create temporary table a_local_temp_table (id int)")
	// Although "begin" take a infoschema snapshot, local temporary table inside txn should be always visible.
	tk.MustExec("show create table tmp_db.a_local_temp_table")
	tk.MustExec("rollback")
	tk.MustQuery("select * from check_data").Check(testkit.Rows())

	// Check create temporary table for if not exists
	tk.MustExec("create temporary table b_local_temp_table (id int)")
	_, err = tk.Exec("create temporary table b_local_temp_table (id int)")
	require.True(t, infoschema.ErrTableExists.Equal(err))
	tk.MustExec("create temporary table if not exists b_local_temp_table (id int)")

	// Stale read see the local temporary table but can't read on it.
	tk.MustExec("START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3)")
	tk.MustGetErrMsg("select * from overlap", "can not stale read temporary table")
	tk.MustExec("rollback")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
}

func TestAccessLocalTmpTableAfterDropDB(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists tmpdb")
	tk.MustExec("create temporary table tmpdb.tmp(id int)")
	tk.MustExec("drop database tmpdb")

	tests := []struct {
		sql         string
		errcode     int
		result      []string
		queryResult []string
	}{
		{
			sql:    "insert into tmpdb.tmp values(1)",
			result: []string{"1"},
		},
		{
			sql:         "select * from tmpdb.tmp t1 join tmpdb.tmp t2 where t1.id=t2.id",
			queryResult: []string{"1 1"},
		},
		{
			sql:         "select (select id from tmpdb.tmp) id1, t1.id id2 from (select * from tmpdb.tmp) t1 where t1.id=1",
			queryResult: []string{"1 1"},
		},
		{
			sql:    "update tmpdb.tmp set id=2 where id=1",
			result: []string{"2"},
		},
		{
			sql:    "delete from tmpdb.tmp where id=2",
			result: []string{},
		},
		{
			sql:    "insert into tmpdb.tmp select 1 from dual",
			result: []string{"1"},
		},
		{
			sql:    "update tmpdb.tmp t1, tmpdb.tmp t2 set t1.id=2 where t1.id=t2.id",
			result: []string{"2"},
		},
		{
			sql:    "delete t1 from tmpdb.tmp t1 join tmpdb.tmp t2 where t1.id=t2.id",
			result: []string{},
		},
		{
			sql:     "admin check table tmpdb.tmp",
			errcode: errno.ErrOptOnTemporaryTable,
		},
		{
			sql:     "alter table tmpdb.tmp add column name char(10)",
			errcode: errno.ErrUnsupportedDDLOperation,
		},
	}

	executeTests := func() {
		tk.MustExec("truncate table tmpdb.tmp")
		for _, test := range tests {
			switch {
			case test.errcode != 0:
				tk.MustGetErrCode(test.sql, test.errcode)
			case test.queryResult != nil:
				tk.MustQuery(test.sql).Check(testkit.Rows(test.queryResult...))
			case test.result != nil:
				tk.MustExec(test.sql)
				tk.MustQuery("select * from tmpdb.tmp").Check(testkit.Rows(test.result...))
			default:
				tk.MustExec(test.sql)
			}
		}
	}

	executeTests()

	// Create the database again.
	tk.MustExec("create database tmpdb")
	executeTests()

	// Create another table in the database and drop the database again.
	tk.MustExec("create temporary table tmpdb.tmp2(id int)")
	tk.MustExec("drop database tmpdb")
	executeTests()
}

func TestPlacementOnTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.tplacement1, db2.t1, db2.tplacement3, db2.tplacement5")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("create placement policy x primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy x")

	// Cannot create temporary table with placement options
	tk.MustGetErrCode("create temporary table tplacement2 (id int) placement policy='x'", errno.ErrOptOnTemporaryTable)

	// Cannot alter temporary table with placement options
	tk.MustExec("create global temporary table tplacement1 (id int) on commit delete rows")
	defer tk.MustExec("drop table tplacement1")
	tk.MustGetErrCode("alter table tplacement1  placement policy='x'", errno.ErrOptOnTemporaryTable)

	tk.MustExec("create temporary table tplacement2 (id int)")
	tk.MustGetErrCode("alter table tplacement2  placement policy='x'", errno.ErrUnsupportedDDLOperation)

	// Temporary table will not inherit placement from db
	tk.MustExec("create database db2 placement policy x")
	defer tk.MustExec("drop database db2")

	tk.MustExec("create global temporary table db2.tplacement3 (id int) on commit delete rows")
	defer tk.MustExec("drop table db2.tplacement3")
	tk.MustQuery("show create table db2.tplacement3").Check(testkit.Rows(
		"tplacement3 CREATE GLOBAL TEMPORARY TABLE `tplacement3` (\n" +
			"  `id` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ON COMMIT DELETE ROWS",
	))

	tk.MustExec("create temporary table db2.tplacement4 (id int)")
	tk.MustQuery("show create table db2.tplacement4").Check(testkit.Rows(
		"tplacement4 CREATE TEMPORARY TABLE `tplacement4` (\n" +
			"  `id` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("create table db2.t1 (a int) placement policy 'default'")
	defer tk.MustExec("drop table db2.t1")

	tk.MustExec("create global temporary table db2.tplacement5 like db2.t1 on commit delete rows")
	defer tk.MustExec("drop table db2.tplacement5")
	tk.MustQuery("show create table db2.tplacement5").Check(testkit.Rows(
		"tplacement5 CREATE GLOBAL TEMPORARY TABLE `tplacement5` (\n" +
			"  `a` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ON COMMIT DELETE ROWS",
	))

	tk.MustExec("create temporary table db2.tplacement6 like db2.t1")
	defer tk.MustExec("drop table db2.tplacement6")
	tk.MustQuery("show create table db2.tplacement6").Check(testkit.Rows(
		"tplacement6 CREATE TEMPORARY TABLE `tplacement6` (\n" +
			"  `a` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

func TestAvoidCreateViewOnLocalTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tk.MustExec("drop table if exists tt0")
	tk.MustExec("drop table if exists tt1")
	tk.MustExec("drop table if exists tt2")

	tk.MustExec("create table tt0 (a int, b int)")
	tk.MustExec("create view v0 as select * from tt0")
	tk.MustExec("create temporary table tt1 (a int, b int)")
	tk.MustExec("create temporary table tt2 (c int, d int)")

	checkCreateView := func() {
		_, err := tk.Exec("create view v1 as select * from tt1")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
		_, err = tk.Exec("select * from v1")
		require.Error(t, err)
		require.Equal(t, "[schema:1146]Table 'test.v1' doesn't exist", err.Error())

		_, err = tk.Exec("create view v1 as select * from (select * from tt1) as tt")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
		_, err = tk.Exec("select * from v1")
		require.Error(t, err)
		require.Equal(t, "[schema:1146]Table 'test.v1' doesn't exist", err.Error())

		_, err = tk.Exec("create view v2 as select * from tt0 union select * from tt1")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
		_, err = tk.Exec("select * from v2")
		require.Error(t, err)
		require.Equal(t, "[schema:1146]Table 'test.v2' doesn't exist", err.Error())

		_, err = tk.Exec("create view v3 as select * from tt0, tt1 where tt0.a = tt1.a")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
		_, err = tk.Exec("select * from v3")
		require.Error(t, err)
		require.Equal(t, "[schema:1146]Table 'test.v3' doesn't exist", err.Error())

		_, err = tk.Exec("create view v4 as select a, (select count(1) from tt1 where tt1.a = tt0.a) as tt1a from tt0")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
		_, err = tk.Exec("select * from v4")
		require.Error(t, err)
		require.Equal(t, "[schema:1146]Table 'test.v4' doesn't exist", err.Error())

		_, err = tk.Exec("create view v5 as select a, (select count(1) from tt1 where tt1.a = 1) as tt1a from tt0")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
		_, err = tk.Exec("select * from v5")
		require.Error(t, err)
		require.Equal(t, "[schema:1146]Table 'test.v5' doesn't exist", err.Error())

		_, err = tk.Exec("create view v6 as select * from tt0 where tt0.a=(select max(tt1.b) from tt1)")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
		_, err = tk.Exec("select * from v6")
		require.Error(t, err)
		require.Equal(t, "[schema:1146]Table 'test.v6' doesn't exist", err.Error())

		_, err = tk.Exec("create view v7 as select * from tt0 where tt0.b=(select max(tt1.b) from tt1 where tt0.a=tt1.a)")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
		_, err = tk.Exec("select * from v7")
		require.Error(t, err)
		require.Equal(t, "[schema:1146]Table 'test.v7' doesn't exist", err.Error())

		_, err = tk.Exec("create or replace view v0 as select * from tt1")
		require.True(t, core.ErrViewSelectTemporaryTable.Equal(err))
	}

	checkCreateView()
	tk.MustExec("create temporary table tt0 (a int, b int)")
	tk.MustExec("create table tt1 (a int, b int)")
	tk.MustExec("create table tt2 (c int, d int)")
	tk.MustExec("create view vv as select * from v0")
	checkCreateView()
}

func TestDropTemporaryTable(t *testing.T) {
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

	// Check drop temporary table(include meta data and real data.
	tk.MustExec("create temporary table if not exists b_local_temp_table (id int)")
	tk.MustQuery("select * from b_local_temp_table").Check(testkit.Rows())
	tk.MustExec("drop table b_local_temp_table")
	_, err := tk.Exec("select * from b_local_temp_table")
	require.Equal(t, "[schema:1146]Table 'test.b_local_temp_table' doesn't exist", err.Error())
	// TODO: test drop real data

	// Check if we have a normal and local temporary table in the same db with the same name,
	// local temporary table should be dropped instead of the normal table.
	tk.MustExec("drop table if exists b_table_local_and_normal")
	tk.MustExec("create table if not exists b_table_local_and_normal (id int)")
	tk.MustExec("create temporary table if not exists b_table_local_and_normal (id int)")
	tk.MustQuery("select * from b_table_local_and_normal").Check(testkit.Rows())
	tk.MustExec("drop table b_table_local_and_normal")
	sequenceTable := external.GetTableByName(t, tk, "test", "b_table_local_and_normal")
	require.Equal(t, model.TempTableNone, sequenceTable.Meta().TempTableType)
	tk.MustExec("drop table if exists b_table_local_and_normal")
	_, err = tk.Exec("select * from b_table_local_and_normal")
	require.Equal(t, "[schema:1146]Table 'test.b_table_local_and_normal' doesn't exist", err.Error())

	// Check dropping local temporary tables should not commit current transaction implicitly.
	tk.MustExec("drop table if exists check_data_normal_table")
	tk.MustExec("create table check_data_normal_table (id int)")
	defer tk.MustExec("drop table if exists check_data_normal_table")
	tk.MustExec("begin")
	tk.MustExec("insert into check_data_normal_table values (1)")
	tk.MustExec("create temporary table a_local_temp_table (id int)")
	tk.MustExec("create temporary table a_local_temp_table_1 (id int)")
	tk.MustExec("create temporary table a_local_temp_table_2 (id int)")
	tk.MustExec("show create table a_local_temp_table")
	tk.MustExec("show create table a_local_temp_table_1")
	tk.MustExec("show create table a_local_temp_table_2")
	tk.MustExec("drop table a_local_temp_table, a_local_temp_table_1, a_local_temp_table_2")
	tk.MustExec("rollback")
	tk.MustQuery("select * from check_data_normal_table").Check(testkit.Rows())

	// Check dropping local temporary and normal tables should commit current transaction implicitly.
	tk.MustExec("drop table if exists check_data_normal_table_1")
	tk.MustExec("create table check_data_normal_table_1 (id int)")
	defer tk.MustExec("drop table if exists check_data_normal_table_1")
	tk.MustExec("begin")
	tk.MustExec("insert into check_data_normal_table_1 values (1)")
	tk.MustExec("create temporary table a_local_temp_table (id int)")
	tk.MustExec("create temporary table a_local_temp_table_1 (id int)")
	tk.MustExec("create temporary table a_local_temp_table_2 (id int)")
	tk.MustExec("drop table if exists a_normal_table")
	tk.MustExec("create table a_normal_table (id int)")
	defer tk.MustExec("drop table if exists a_normal_table")
	tk.MustExec("show create table a_local_temp_table")
	tk.MustExec("show create table a_local_temp_table_1")
	tk.MustExec("show create table a_local_temp_table_2")
	tk.MustExec("show create table a_normal_table")
	tk.MustExec("drop table a_local_temp_table, a_local_temp_table_1, a_local_temp_table_2, a_normal_table")
	tk.MustExec("rollback")
	tk.MustQuery("select * from check_data_normal_table_1").Check(testkit.Rows("1"))

	// Check drop not exists table.
	tk.MustExec("create temporary table a_local_temp_table_3 (id int)")
	tk.MustExec("create temporary table a_local_temp_table_4 (id int)")
	tk.MustExec("create temporary table a_local_temp_table_5 (id int)")
	tk.MustExec("drop table if exists a_normal_table_2")
	tk.MustExec("create table a_normal_table_2 (id int)")
	defer tk.MustExec("drop table if exists a_normal_table_2")
	_, err = tk.Exec("drop table a_local_temp_table_3, a_local_temp_table_4, a_local_temp_table_5, a_normal_table_2, a_local_temp_table_6")
	require.Equal(t, "[schema:1051]Unknown table 'test.a_local_temp_table_6'", err.Error())

	tk.MustExec("drop table if exists check_data_normal_table_3")
	tk.MustExec("create table check_data_normal_table_3 (id int)")
	defer tk.MustExec("drop table if exists check_data_normal_table_3")
	tk.MustExec("create temporary table a_local_temp_table_6 (id int)")
	_, err = tk.Exec("drop table check_data_normal_table_3, check_data_normal_table_7, a_local_temp_table_6")
	require.Equal(t, "[schema:1051]Unknown table 'test.check_data_normal_table_7'", err.Error())

	// Check filter out data from removed local temp tables
	tk.MustExec("create temporary table a_local_temp_table_7 (id int)")
	ctx := tk.Session()
	require.Nil(t, sessiontxn.NewTxn(context.Background(), ctx))
	_, err = ctx.Txn(true)
	require.NoError(t, err)
	sessionVars := tk.Session().GetSessionVars()
	sessVarsTempTable := sessionVars.LocalTemporaryTables
	localTemporaryTable := sessVarsTempTable.(*infoschema.LocalTemporaryTables)
	tbl, exist := localTemporaryTable.TableByName(model.NewCIStr("test"), model.NewCIStr("a_local_temp_table_7"))
	require.True(t, exist)
	tblInfo := tbl.Meta()
	tablePrefix := tablecodec.EncodeTablePrefix(tblInfo.ID)
	endTablePrefix := tablecodec.EncodeTablePrefix(tblInfo.ID + 1)

	tk.MustExec("insert into a_local_temp_table_7 values (0)")
	tk.MustExec("insert into a_local_temp_table_7 values (2)")
	tk.MustExec("begin")
	tk.MustExec("insert into a_local_temp_table_7 values (1)")
	tk.MustExec("drop table if exists a_local_temp_table_7")
	tk.MustExec("commit")

	_, err = tk.Exec("select * from a_local_temp_table_7")
	require.Equal(t, "[schema:1146]Table 'test.a_local_temp_table_7' doesn't exist", err.Error())
	memData := sessionVars.TemporaryTableData
	iter, err := memData.Iter(tablePrefix, endTablePrefix)
	require.NoError(t, err)
	for iter.Valid() {
		key := iter.Key()
		if !bytes.HasPrefix(key, tablePrefix) {
			break
		}
		value := iter.Value()
		require.Equal(t, 0, len(value))
		_ = iter.Next()
	}
	require.False(t, iter.Valid())

	// Check drop not exists table in transaction.
	tk.MustExec("begin")
	tk.MustExec("create temporary table a_local_temp_table_8 (id int)")
	_, err = tk.Exec("drop table a_local_temp_table_8, a_local_temp_table_9_not_exist")
	require.Equal(t, "[schema:1051]Unknown table 'test.a_local_temp_table_9_not_exist'", err.Error())
	tk.MustQuery("select * from a_local_temp_table_8").Check(testkit.Rows())
}

func TestDropWithGlobalTemporaryTableKeyWord(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	clearSQL := "drop table if exists tb, tb2, temp, temp1, ltemp1, ltemp2"
	tk.MustExec(clearSQL)
	defer tk.MustExec(clearSQL)
	// two normal table test.tb, test.tb2
	tk.MustExec("create table tb(id int)")
	tk.MustExec("create table tb2(id int)")
	// two global temporary table test.temp, test.temp1
	tk.MustExec("create global temporary table temp(id int) on commit delete rows")
	tk.MustExec("create global temporary table temp1(id int) on commit delete rows")
	// two local temporary table test.ltemp1, test.ltemp2
	tk.MustExec("create temporary table ltemp1(id int)")
	tk.MustExec("create temporary table ltemp2(id int)")

	// testing for drop table which is not global temporary
	err := tk.ExecToErr("drop global temporary table tb")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table test.tb")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table ltemp1")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table test.ltemp1")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table ltemp1, temp")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table temp, ltemp1")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table xxx, ltemp1")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table xxx")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))

	// testing for drop table if exists which is not global temporary
	err = tk.ExecToErr("drop global temporary table if exists tb")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table if exists ltemp1")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	tk.MustExec("drop global temporary table if exists xxx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.xxx'"))
	err = tk.ExecToErr("drop global temporary table if exists xxx,tb")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))
	err = tk.ExecToErr("drop global temporary table if exists test.tb")
	require.True(t, core.ErrDropTableOnTemporaryTable.Equal(err))

	// testing for drop global temporary table successfully
	tk.MustExec("drop global temporary table temp")
	err = tk.ExecToErr("select * from temp")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	tk.MustExec("drop global temporary table test.temp1")
	err = tk.ExecToErr("select * from temp2")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	tk.MustExec("create global temporary table temp (id int) on commit delete rows")
	tk.MustExec("create global temporary table temp1 (id int) on commit delete rows")

	tk.MustExec("drop global temporary table temp, temp1")
	err = tk.ExecToErr("select * from temp")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	err = tk.ExecToErr("select * from temp1")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	tk.MustExec("create global temporary table temp (id int) on commit delete rows")
	tk.MustExec("create global temporary table temp1 (id int) on commit delete rows")

	tk.MustExec("drop global temporary table if exists temp")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	err = tk.ExecToErr("select * from temp")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
}

func TestDropWithLocalTemporaryTableKeyWord(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	clearSQL := "drop table if exists tb, tb2, temp, temp1, ltemp1, ltemp2, testt.ltemp3"
	tk.MustExec(clearSQL)
	defer tk.MustExec(clearSQL)
	// two normal table test.tb, test.tb2, a temporary table with name test.tb2
	tk.MustExec("create table tb(id int)")
	tk.MustExec("create table tb2(id int)")
	tk.MustExec("insert into tb2 values(1)")
	tk.MustExec("create temporary table tb2(id int)")
	// two global temporary table test.temp, test.temp1
	tk.MustExec("create global temporary table temp(id int) on commit delete rows")
	tk.MustExec("create global temporary table temp1(id int) on commit delete rows")
	// two local temporary table test.ltemp1, test.ltemp2
	tk.MustExec("create temporary table ltemp1(id int)")
	tk.MustExec("create temporary table ltemp2(id int)")
	// a local temporary table test.ltemp3
	tk.MustExec("create database if not exists testt")
	tk.MustExec("create temporary table testt.ltemp3(id int)")

	// testing for drop table which is not local temporary
	err := tk.ExecToErr("drop temporary table tb")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))
	err = tk.ExecToErr("drop temporary table test.tb")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))
	err = tk.ExecToErr("drop temporary table temp1")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))
	err = tk.ExecToErr("drop temporary table test.temp1")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))
	err = tk.ExecToErr("drop temporary table ltemp1, tb")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))
	err = tk.ExecToErr("drop temporary table temp, ltemp1")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))
	err = tk.ExecToErr("drop temporary table xxx, ltemp1")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))
	err = tk.ExecToErr("drop temporary table xxx")
	require.True(t, infoschema.ErrTableDropExists.Equal(err))

	// testing for drop table if exists which is not local temporary
	tk.MustExec("drop temporary table if exists xxx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.xxx'"))
	tk.MustExec("drop temporary table if exists ltemp1, xxx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.xxx'"))
	tk.MustExec("drop temporary table if exists tb1, xxx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.tb1,test.xxx'"))
	tk.MustExec("drop temporary table if exists temp1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.temp1'"))
	tk.MustExec("drop temporary table if exists temp1, xxx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.temp1,test.xxx'"))
	tk.MustExec("drop temporary table if exists testt.ltemp4")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'testt.ltemp4'"))
	tk.MustExec("drop temporary table if exists testt.ltemp3, tb1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.tb1'"))

	// testing for drop temporary table successfully
	tk.MustExec("drop temporary table ltemp1")
	err = tk.ExecToErr("select * from ltemp1")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	tk.MustExec("drop temporary table test.ltemp2")
	err = tk.ExecToErr("select * from ltemp2")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	tk.MustExec("drop temporary table tb2")
	tk.MustQuery("select * from tb2").Check(testkit.Rows("1"))

	tk.MustExec("create temporary table ltemp1 (id int)")
	tk.MustExec("create temporary table ltemp2 (id int)")

	tk.MustExec("drop temporary table testt.ltemp3, ltemp1")
	err = tk.ExecToErr("select * from testt.ltemp3")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	err = tk.ExecToErr("select * from ltemp1")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	tk.MustExec("drop temporary table if exists ltemp2")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	err = tk.ExecToErr("select * from ltemp2")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
}

func TestTruncateLocalTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, tn")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table tn (id int)")
	tk.MustExec("insert into t1 values(10), (11), (12)")
	tk.MustExec("create temporary table t1 (id int primary key auto_increment)")
	tk.MustExec("create temporary table t2 (id int primary key)")
	tk.MustExec("create database if not exists testt")
	tk.MustExec("create temporary table testt.t2 (id int)")

	// truncate table out of txn
	tk.MustExec("insert into t1 values(1), (2), (3)")
	tk.MustExec("insert into t2 values(4), (5), (6)")
	tk.MustExec("insert into testt.t2 values(7), (8), (9)")
	tk.MustExec("truncate table t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	tk.MustExec("insert into t1 values()")
	// auto_increment will be reset for truncate
	tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("4", "5", "6"))
	tk.MustExec("truncate table t2")
	tk.MustQuery("select * from t2").Check(testkit.Rows())
	tk.MustQuery("select * from testt.t2").Check(testkit.Rows("7", "8", "9"))
	tk.MustExec("drop table t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("10", "11", "12"))
	tk.MustExec("create temporary table t1 (id int primary key auto_increment)")

	// truncate table with format dbName.tableName
	tk.MustExec("insert into t2 values(4), (5), (6)")
	tk.MustExec("insert into testt.t2 values(7), (8), (9)")
	tk.MustExec("truncate table testt.t2")
	tk.MustQuery("select * from testt.t2").Check(testkit.Rows())
	tk.MustQuery("select * from t2").Check(testkit.Rows("4", "5", "6"))
	tk.MustExec("truncate table test.t2")
	tk.MustQuery("select * from t2").Check(testkit.Rows())

	// truncate table in txn
	tk.MustExec("insert into t1 values(1), (2), (3)")
	tk.MustExec("insert into t2 values(4), (5), (6)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(11), (12)")
	tk.MustExec("insert into t2 values(24), (25)")
	tk.MustExec("delete from t1 where id=2")
	tk.MustExec("delete from t2 where id=4")
	tk.MustExec("truncate table t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	tk.MustExec("insert into t1 values()")
	// auto_increment will be reset for truncate
	tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("5", "6", "24", "25"))

	// since transaction already committed by truncate, so query after rollback will get same result
	tk.MustExec("rollback")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("5", "6", "24", "25"))

	// truncate a temporary table will not effect the normal table with the same name
	tk.MustExec("drop table t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("10", "11", "12"))
	tk.MustExec("create temporary table t1 (id int primary key auto_increment)")

	// truncate temporary table will clear session data
	localTemporaryTables := tk.Session().GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables)
	tb1, exist := localTemporaryTables.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	tbl1Info := tb1.Meta()
	tablePrefix := tablecodec.EncodeTablePrefix(tbl1Info.ID)
	endTablePrefix := tablecodec.EncodeTablePrefix(tbl1Info.ID + 1)
	require.True(t, exist)
	tk.MustExec("insert into t1 values(1), (2), (3)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(5), (6), (7)")
	tk.MustExec("truncate table t1")
	iter, err := tk.Session().GetSessionVars().TemporaryTableData.Iter(tablePrefix, endTablePrefix)
	require.NoError(t, err)
	for iter.Valid() {
		key := iter.Key()
		if !bytes.HasPrefix(key, tablePrefix) {
			break
		}
		value := iter.Value()
		require.Equal(t, 0, len(value))
		_ = iter.Next()
	}
	require.False(t, iter.Valid())

	// truncate after drop database should be successful
	tk.MustExec("create temporary table testt.t3 (id int)")
	tk.MustExec("insert into testt.t3 values(1)")
	tk.MustExec("drop database testt")
	tk.MustExec("truncate table testt.t3")
	tk.MustQuery("select * from testt.t3").Check(testkit.Rows())
}

func TestIssue29282(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists issue29828_t")
	tk.MustExec("create table issue29828_t (id int)")
	tk.MustExec("create temporary table issue29828_tmp(id int);")
	tk.MustExec("insert into issue29828_tmp values(1)")
	tk.MustExec("prepare stmt from 'insert into issue29828_t select * from issue29828_tmp';")
	tk.MustExec("execute stmt")
	tk.MustQuery("select *from issue29828_t").Check(testkit.Rows("1"))

	tk.MustExec("create temporary table issue29828_tmp1(id int);")
	tk.MustExec("insert into issue29828_tmp1 values(1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("prepare stmt1 from 'select * from issue29828_t for update union select * from issue29828_tmp1';")
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	ch := make(chan struct{}, 1)
	tk1.MustExec("use test")
	tk1.MustExec("begin pessimistic")
	go func() {
		// This query should block.
		tk1.MustQuery("select * from issue29828_t where id = 1 for update;").Check(testkit.Rows("1"))
		ch <- struct{}{}
		tk1.MustExec("rollback")
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		// Expected, query blocked, not finish within 100ms.
		tk.MustExec("rollback")
	case <-ch:
		// Unexpected, test fail.
		t.Fail()
	}

	// Wait the background query rollback
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fail()
	case <-ch:
	}
}

// See https://github.com/pingcap/tidb/issues/29327
func TestEnumDefaultValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE `t1` (   `a` enum('','a','b') NOT NULL DEFAULT 'b' ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` enum('','a','b') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'b'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE `t1` (   `a` enum('','a','b') NOT NULL DEFAULT 'b ' ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` enum('','a','b') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'b'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
}

func TestIssue29326(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("insert into t1 values(1)")
	defer tk.MustExec("drop table t1")

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (id int)")
	tk.MustExec("insert into t2 values(1)")
	defer tk.MustExec("drop table t2")

	tk.MustExec("drop view if exists v1")
	defer tk.MustExec("drop view if exists v1")

	tk.MustExec("create view v1 as select 1,1")
	rs, err := tk.Exec("select * from v1")
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1"))
	require.Equal(t, "1", rs.Fields()[0].Column.Name.O)
	require.Equal(t, "Name_exp_1", rs.Fields()[1].Column.Name.O)

	tk.MustExec("drop view if exists v1")
	tk.MustExec("create view v1 as select 1, 2, 1, 2, 1, 2, 1, 2")
	rs, err = tk.Exec("select * from v1")
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 2 1 2 1 2 1 2"))
	require.Equal(t, "1", rs.Fields()[0].Column.Name.O)
	require.Equal(t, "2", rs.Fields()[1].Column.Name.O)
	require.Equal(t, "Name_exp_1", rs.Fields()[2].Column.Name.O)
	require.Equal(t, "Name_exp_2", rs.Fields()[3].Column.Name.O)
	require.Equal(t, "Name_exp_1_1", rs.Fields()[4].Column.Name.O)
	require.Equal(t, "Name_exp_1_2", rs.Fields()[5].Column.Name.O)
	require.Equal(t, "Name_exp_2_1", rs.Fields()[6].Column.Name.O)
	require.Equal(t, "Name_exp_2_2", rs.Fields()[7].Column.Name.O)

	tk.MustExec("drop view if exists v1")
	tk.MustExec("create view v1 as select 't', 't', 1 as t")
	rs, err = tk.Exec("select * from v1")
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("t t 1"))
	require.Equal(t, "Name_exp_t", rs.Fields()[0].Column.Name.O)
	require.Equal(t, "Name_exp_1_t", rs.Fields()[1].Column.Name.O)
	require.Equal(t, "t", rs.Fields()[2].Column.Name.O)

	tk.MustExec("drop view if exists v1")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v1 as select 1, 1 union all select 1, 1")
	tk.MustQuery("show create view v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` " +
		"SQL SECURITY DEFINER VIEW `v1` (`1`, `Name_exp_1`) " +
		"AS SELECT 1 AS `1`,1 AS `Name_exp_1` UNION ALL SELECT 1 AS `1`,1 AS `1` " +
		"utf8mb4 utf8mb4_bin"))
	rs, err = tk.Exec("select * from v1")
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1", "1 1"))
	require.Equal(t, "1", rs.Fields()[0].Column.Name.O)
	require.Equal(t, "Name_exp_1", rs.Fields()[1].Column.Name.O)

	tk.MustExec("drop view if exists v1")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v1 as select 'id', id from t1")
	tk.MustQuery("show create view v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` " +
		"SQL SECURITY DEFINER VIEW `v1` (`Name_exp_id`, `id`) " +
		"AS SELECT _UTF8MB4'id' AS `Name_exp_id`,`id` AS `id` FROM `test`.`t1` " +
		"utf8mb4 utf8mb4_bin"))
	rs, err = tk.Exec("select * from v1")
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("id 1"))
	require.Equal(t, "Name_exp_id", rs.Fields()[0].Column.Name.O)
	require.Equal(t, "id", rs.Fields()[1].Column.Name.O)

	tk.MustExec("drop view if exists v1")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v1 as select 1, (select id from t1 where t1.id=t2.id) as '1' from t2")
	tk.MustQuery("show create view v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` " +
		"SQL SECURITY DEFINER VIEW `v1` (`Name_exp_1`, `1`) " +
		"AS SELECT 1 AS `Name_exp_1`,(SELECT `id` AS `id` FROM `test`.`t1` WHERE `t1`.`id`=`t2`.`id`) AS `1` FROM `test`.`t2` " +
		"utf8mb4 utf8mb4_bin"))
	rs, err = tk.Exec("select * from v1")
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1"))
	require.Equal(t, "Name_exp_1", rs.Fields()[0].Column.Name.O)
	require.Equal(t, "1", rs.Fields()[1].Column.Name.O)

	tk.MustExec("drop view if exists v1")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v1 as select 1 as 'abs(t1.id)', abs(t1.id) from t1")
	tk.MustQuery("show create view v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` " +
		"SQL SECURITY DEFINER VIEW `v1` (`abs(t1.id)`, `Name_exp_abs(t1.id)`) " +
		"AS SELECT 1 AS `abs(t1.id)`,ABS(`t1`.`id`) AS `Name_exp_abs(t1.id)` FROM `test`.`t1` " +
		"utf8mb4 utf8mb4_bin"))
	rs, err = tk.Exec("select * from v1")
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1"))
	require.Equal(t, "abs(t1.id)", rs.Fields()[0].Column.Name.O)
	require.Equal(t, "Name_exp_abs(t1.id)", rs.Fields()[1].Column.Name.O)

	tk.MustExec("drop view if exists v1")
	err = tk.ExecToErr("create definer=`root`@`127.0.0.1` view v1 as select 1 as t,1 as t")
	require.True(t, infoschema.ErrColumnExists.Equal(err))

	tk.MustExec("drop view if exists v1")
	err = tk.ExecToErr("create definer=`root`@`127.0.0.1` view v1 as select 1 as id, id from t1")
	require.True(t, infoschema.ErrColumnExists.Equal(err))

	tk.MustExec("drop view if exists v1")
	err = tk.ExecToErr("create definer=`root`@`127.0.0.1` view v1 as select * from t1 left join t2 on t1.id=t2.id")
	require.True(t, infoschema.ErrColumnExists.Equal(err))

	tk.MustExec("drop view if exists v1")
	err = tk.ExecToErr("create definer=`root`@`127.0.0.1` view v1 as select t1.id, t2.id from t1,t2 where t1.id=t2.id")
	require.True(t, infoschema.ErrColumnExists.Equal(err))
}

func TestInvalidPartitionNameWhenCreateTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database invalidPartitionNames")
	defer tk.MustExec("drop database invalidPartitionNames")
	tk.MustExec("USE invalidPartitionNames")

	_, err := tk.Exec("create table t(a int) partition by range (a) (partition p0 values less than (0), partition `p1 ` values less than (3))")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, dbterror.ErrWrongPartitionName), "err %v", err)

	_, err = tk.Exec("create table t(a int) partition by range (a) (partition `` values less than (0), partition `p1` values less than (3))")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, dbterror.ErrWrongPartitionName), "err %v", err)

	tk.MustExec("create table t(a int) partition by range (a) (partition `p0` values less than (0), partition `p1` values less than (3))")
	_, err = tk.Exec("alter table t add partition (partition `p2 ` values less than (5))")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, dbterror.ErrWrongPartitionName), "err %v", err)
}

func TestDDLLastInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.query'), json_extract(@@tidb_last_ddl_info, '$.seq_num')").Check(testkit.Rows("\"\" 0"))
	tk.MustExec("create table t(a int)")
	firstSequence := 0
	res := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.query'), json_extract(@@tidb_last_ddl_info, '$.seq_num')")
	require.Len(t, res.Rows(), 1)
	require.Equal(t, "\"create table t(a int)\"", res.Rows()[0][0])
	var err error
	firstSequence, err = strconv.Atoi(fmt.Sprintf("%v", res.Rows()[0][1]))
	require.NoError(t, err)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use test;`)
	tk.MustExec("create table t2(a int)")
	tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.query'), json_extract(@@tidb_last_ddl_info, '$.seq_num')").Check(testkit.Rows(fmt.Sprintf("\"create table t2(a int)\" %d", firstSequence+1)))

	tk.MustExec("drop table t, t2")
	tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.query'), json_extract(@@tidb_last_ddl_info, '$.seq_num')").Check(testkit.Rows(fmt.Sprintf("\"drop table t, t2\" %d", firstSequence+3)))
}
