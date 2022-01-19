// Copyright 2021 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestAlterTableCache(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	session.SetSchemaLease(600 * time.Millisecond)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	dom.SetStatsUpdating(true)

	clean := func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk2.MustExec("use test")
	/* Test of cache table */
	tk.MustExec("create table t1 ( n int auto_increment primary key)")
	tk.MustGetErrCode("alter table t1 ca", errno.ErrParse)
	tk.MustGetErrCode("alter table t2 cache", errno.ErrNoSuchTable)
	tk.MustExec("alter table t1 cache")
	checkTableCacheStatus(t, tk.Session(), "test", "t1", model.TableCacheStatusEnable)
	tk.MustExec("drop table if exists t1")
	/*Test can't skip schema checker*/
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 cache;")
	_, err = tk.Exec("commit")
	require.True(t, terror.ErrorEqual(domain.ErrInfoSchemaChanged, err))
	/* Test can skip schema checker */
	tk.MustExec("begin")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("insert into t1 set a=2;")
	tk2.MustExec("alter table t2 cache")
	tk.MustExec("commit")
	// Test if a table is not exists
	tk.MustExec("drop table if exists t")
	tk.MustGetErrCode("alter table t cache", errno.ErrNoSuchTable)
	tk.MustExec("create table t (a int)")
	tk.MustExec("alter table t cache")
	// Multiple alter cache is okay
	tk.MustExec("alter table t cache")
	tk.MustExec("alter table t cache")
	// Test a temporary table
	tk.MustExec("drop table if exists t")
	tk.MustExec("create temporary table t (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("drop table if exists tmp1")
	// local temporary table alter is not supported
	tk.MustGetErrCode("alter table t cache", errno.ErrUnsupportedDDLOperation)
	// test global temporary table
	tk.MustExec("create global temporary table tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))" +
		"on commit delete rows")
	tk.MustGetErrMsg("alter table tmp1 cache", ddl.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache").Error())

}

func TestRestrainDropColumnWithIndexX(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	session.SetSchemaLease(600 * time.Millisecond)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	dom.SetStatsUpdating(true)

	clean := func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}
	defer clean()

	tk := testkit.NewTestKit(t, store)
	sysTZ, _ := time.LoadLocation(tk.Session().GetSessionVars().TimeZone.String())
	sysL, _ := time.LoadLocation(tk.Session().GetSessionVars().Location().String())
	n0 := time.Now().In(time.UTC)
	n1 := time.Now().In(sysTZ)
	n2 := time.Now().In(sysL)
	str := fmt.Sprintf("---------------------xxxc-------------------time tz val:%v, systz:%+v, val:%v, sysl val:%v", n0, sysTZ, n1, n2)
	logutil.BgLogger().Warn("xxx000============================" + str)

	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	// change timezone
	tk.MustExec(`set time_zone = 'Asia/Shanghai'`)
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`insert into t set a=1`)
	logutil.BgLogger().Warn("xxx*************************************** 010")
	tk.MustExec(`alter table t add column b timestamp not null default current_timestamp;`)
	logutil.BgLogger().Warn("xxx*************************************** 011")
	timeIn8 := tk.MustQuery("select b from t").Rows()[0][0]
	// change timezone
	tk.MustExec(`set time_zone = '+00:00'`)
	tk.MustExec(`insert into t set b=now()`)
	timeIn0 := tk.MustQuery("select b from t").Rows()[0][0]
	require.True(t, timeIn8 != timeIn0)
	datumTimeIn8, err := expression.GetTimeValue(tk.Session(), timeIn8, mysql.TypeTimestamp, 0)
	require.Nil(t, err)
	tIn8To0 := datumTimeIn8.GetMysqlTime()
	timeZoneIn8, err := time.LoadLocation("Asia/Shanghai")
	require.Nil(t, err)
	err = tIn8To0.ConvertTimeZone(timeZoneIn8, time.UTC)
	require.Nil(t, err)
	require.True(t, timeIn0 == tIn8To0.String())

	logutil.BgLogger().Warn("xxx*************************************** 02")
	tk.MustExec(`alter table t add index(b);`)
	logutil.BgLogger().Warn("xxx*************************************** 03")
	tk.MustExec("admin check table t")
	tk.MustExec(`set time_zone = '+05:00'`)
	tk.MustExec("admin check table t")

	// general column
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	// change timezone
	tk.MustExec(`set time_zone = 'Asia/Shanghai'`)
	tk.MustExec(`create table t(a timestamp default current_timestamp)`)
	tk.MustExec(`insert into t set a=now()`)
	logutil.BgLogger().Warn("xxx*************************************** 010")
	tk.MustExec(`alter table t add column b timestamp as (a+1) virtual;`)
	logutil.BgLogger().Warn("xxx*************************************** 011")
	// change timezone
	tk.MustExec(`set time_zone = '+05:00'`)
	tk.MustExec(`insert into t set a=now()`)
	logutil.BgLogger().Warn("xxx*************************************** 02")
	tk.MustExec(`alter table t add index(b);`)
	logutil.BgLogger().Warn("xxx*************************************** 03")
	tk.MustExec("admin check table t")
	tk.MustExec(`set time_zone = '+00:00'`)
	tk.MustExec("admin check table t")

	// set @@session.time_zone='+00:00'
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key auto_increment, c1 timestamp default '2020-07-10 01:05:08', index idx(c1));")
	tk.MustExec("insert into t values();")
	logutil.BgLogger().Warn("xxx*************************************** 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	logutil.BgLogger().Warn("xxx*************************************** 1")
	tk.MustExec("alter table t modify column c1 bigint;")
	tk.MustExec("alter table t add index idx1(id, c1);")
	logutil.BgLogger().Warn("xxx*************************************** 2")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 20200710010508"))
	tk.MustExec("admin check table t")
	str = fmt.Sprintf("tz:%v, ctxTZ:%v", tk.Session().GetSessionVars().TimeZone.String(), tk.Session().GetSessionVars().StmtCtx.TimeZone)
	logutil.BgLogger().Warn("xxx111============================" + str)
	tk.MustExec("alter table t modify c1 timestamp")
	tk.MustExec("set @@session.time_zone='-8:00'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-09 17:05:08"))
	tk.MustExec("admin check table t")

	// set @@session.time_zone='-8:00'
	tk.MustExec("set @@session.time_zone='-8:00'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key auto_increment, c1 timestamp default '2020-07-10 01:05:08', index idx(c1));")
	tk.MustExec("insert into t values();")
	logutil.BgLogger().Warn("xxx*************************************** 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	logutil.BgLogger().Warn("xxx*************************************** 10")
	tk.MustExec("alter table t modify column c1 bigint;")
	logutil.BgLogger().Warn("xxx*************************************** 11")
	tk.MustExec("alter table t add index idx1(id, c1);")
	logutil.BgLogger().Warn("xxx*************************************** 2")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 20200710010508"))
	tk.MustExec("admin check table t")
	logutil.BgLogger().Warn("xxx222============================" + str)
	tk.MustExec("alter table t modify c1 timestamp")
	tk.MustExec("set @@session.time_zone='SYSTEM'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 17:05:08"))
	tk.MustExec("admin check table t")

	// set @@session.time_zone='SYSTEM'
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key auto_increment, c1 timestamp default '2020-07-10 01:05:08', index idx(c1));")
	tk.MustExec("insert into t values();")
	logutil.BgLogger().Warn("xxx*************************************** 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	logutil.BgLogger().Warn("xxx*************************************** 1")
	tk.MustExec("alter table t modify column c1 bigint;")
	tk.MustExec("alter table t add index idx1(id, c1);")
	logutil.BgLogger().Warn("xxx*************************************** 2")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 20200710010508"))
	tk.MustExec("admin check table t")
	logutil.BgLogger().Warn("xxx333============================" + str)
	tk.MustExec("alter table t modify c1 timestamp")
	tk.MustExec("set @@session.time_zone='Asia/Shanghai'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	tk.MustExec("admin check table t")
}
