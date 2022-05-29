// Copyright 2020 PingCAP, Inc.
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
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestColumnTypeChangeBetweenInteger(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Modify column from null to not null.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int null, b int null)")
	tk.MustExec("alter table t modify column b int not null")

	tk.MustExec("insert into t(a, b) values (null, 1)")
	// Modify column from null to not null in same type will cause ErrWarnDataTruncated
	_, err := tk.Exec("alter table t modify column a int not null")
	require.Equal(t, "[ddl:1265]Data truncated for column 'a' at row 1", err.Error())

	// Modify column from null to not null in different type will cause WarnDataTruncated.
	tk.MustGetErrCode("alter table t modify column a tinyint not null", errno.WarnDataTruncated)
	tk.MustGetErrCode("alter table t modify column a bigint not null", errno.WarnDataTruncated)

	// Modify column not null to null.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("alter table t modify column b int null")

	tk.MustExec("insert into t(a, b) values (1, null)")
	tk.MustExec("alter table t modify column a int null")

	// Modify column from unsigned to signed and from signed to unsigned.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unsigned, b int signed)")
	tk.MustExec("insert into t(a, b) values (1, 1)")
	tk.MustExec("alter table t modify column a int signed")
	tk.MustExec("alter table t modify column b int unsigned")

	// Modify column from small type to big type.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a tinyint)")
	tk.MustExec("alter table t modify column a smallint")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a tinyint)")
	tk.MustExec("insert into t(a) values (127)")
	tk.MustExec("alter table t modify column a smallint")
	tk.MustExec("alter table t modify column a mediumint")
	tk.MustExec("alter table t modify column a int")
	tk.MustExec("alter table t modify column a bigint")

	// Modify column from big type to small type.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint)")
	tk.MustExec("alter table t modify column a int")
	tk.MustExec("alter table t modify column a mediumint")
	tk.MustExec("alter table t modify column a smallint")
	tk.MustExec("alter table t modify column a tinyint")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint)")
	tk.MustExec("insert into t(a) values (9223372036854775807)")
	tk.MustGetErrCode("alter table t modify column a int", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify column a mediumint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify column a smallint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify column a tinyint", errno.ErrDataOutOfRange)
	_, err = tk.Exec("admin check table t")
	require.NoError(t, err)
}

func TestColumnTypeChangeStateBetweenInteger(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert into t(c1, c2) values (1, 1)")

	// use new session to check meta in callback function.
	internalTK := testkit.NewTestKit(t, store)
	internalTK.MustExec("use test")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))
	require.NotNil(t, external.GetModifyColumn(t, tk, "test", "t", "c2", false))

	hook := &ddl.TestDDLCallback{Do: dom}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}
		switch job.SchemaState {
		case model.StateNone:
			tbl = external.GetTableByName(t, internalTK, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			} else if len(tbl.Cols()) != 2 {
				checkErr = errors.New("len(cols) is not right")
			}
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			tbl = external.GetTableByName(t, internalTK, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			} else if len(tbl.(*tables.TableCommon).Columns) != 3 {
				// changingCols has been added into meta.
				checkErr = errors.New("len(cols) is not right")
			} else if external.GetModifyColumn(t, internalTK, "test", "t", "c2", true).GetFlag()&mysql.PreventNullInsertFlag == uint(0) {
				checkErr = errors.New("old col's flag is not right")
			} else if external.GetModifyColumn(t, internalTK, "test", "t", "_Col$_c2_0", true) == nil {
				checkErr = errors.New("changingCol is nil")
			}
		}
	}
	dom.DDL().SetHook(hook)
	// Alter sql will modify column c2 to tinyint not null.
	SQL := "alter table t modify column c2 tinyint not null"
	tk.MustExec(SQL)
	// Assert the checkErr in the job of every state.
	require.NoError(t, checkErr)

	// Check the col meta after the column type change.
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))
	col := external.GetModifyColumn(t, tk, "test", "t", "c2", false)
	require.NotNil(t, col)
	require.Equal(t, true, mysql.HasNotNullFlag(col.GetFlag()))
	require.NotEqual(t, 0, col.GetFlag()&mysql.NoDefaultValueFlag)
	require.Equal(t, mysql.TypeTiny, col.GetType())
	require.Nil(t, col.ChangeStateInfo)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

func TestRollbackColumnTypeChangeBetweenInteger(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bigint, c2 bigint)")
	tk.MustExec("insert into t(c1, c2) values (1, 1)")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))
	require.NotNil(t, external.GetModifyColumn(t, tk, "test", "t", "c2", false))

	hook := &ddl.TestDDLCallback{Do: dom}
	// Mock roll back at model.StateNone.
	customizeHookRollbackAtState(hook, tbl, model.StateNone)
	dom.DDL().SetHook(hook)
	// Alter sql will modify column c2 to bigint not null.
	SQL := "alter table t modify column c2 int not null"
	err := tk.ExecToErr(SQL)
	require.EqualError(t, err, "[ddl:1]MockRollingBackInCallBack-none")
	assertRollBackedColUnchanged(t, tk)

	// Mock roll back at model.StateDeleteOnly.
	customizeHookRollbackAtState(hook, tbl, model.StateDeleteOnly)
	dom.DDL().SetHook(hook)
	err = tk.ExecToErr(SQL)
	require.EqualError(t, err, "[ddl:1]MockRollingBackInCallBack-delete only")
	assertRollBackedColUnchanged(t, tk)

	// Mock roll back at model.StateWriteOnly.
	customizeHookRollbackAtState(hook, tbl, model.StateWriteOnly)
	dom.DDL().SetHook(hook)
	err = tk.ExecToErr(SQL)
	require.EqualError(t, err, "[ddl:1]MockRollingBackInCallBack-write only")
	assertRollBackedColUnchanged(t, tk)

	// Mock roll back at model.StateWriteReorg.
	customizeHookRollbackAtState(hook, tbl, model.StateWriteReorganization)
	dom.DDL().SetHook(hook)
	err = tk.ExecToErr(SQL)
	require.EqualError(t, err, "[ddl:1]MockRollingBackInCallBack-write reorganization")
	assertRollBackedColUnchanged(t, tk)
}

func customizeHookRollbackAtState(hook *ddl.TestDDLCallback, tbl table.Table, state model.SchemaState) {
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		if job.SchemaState == state {
			job.State = model.JobStateRollingback
			job.Error = mockTerrorMap[state.String()]
		}
	}
}

func assertRollBackedColUnchanged(t *testing.T, tk *testkit.TestKit) {
	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))
	col := external.GetModifyColumn(t, tk, "test", "t", "c2", false)
	require.NotNil(t, col)
	require.Equal(t, uint(0), col.GetFlag())
	require.Equal(t, mysql.TypeLonglong, col.GetType())
	require.Nil(t, col.ChangeStateInfo)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

var mockTerrorMap = make(map[string]*terror.Error)

func init() {
	// Since terror new action will cause data race with other test suite (getTerrorCode) in parallel, we init it all here.
	mockTerrorMap[model.StateNone.String()] = dbterror.ClassDDL.New(1, "MockRollingBackInCallBack-"+model.StateNone.String())
	mockTerrorMap[model.StateDeleteOnly.String()] = dbterror.ClassDDL.New(1, "MockRollingBackInCallBack-"+model.StateDeleteOnly.String())
	mockTerrorMap[model.StateWriteOnly.String()] = dbterror.ClassDDL.New(1, "MockRollingBackInCallBack-"+model.StateWriteOnly.String())
	mockTerrorMap[model.StateWriteReorganization.String()] = dbterror.ClassDDL.New(1, "MockRollingBackInCallBack-"+model.StateWriteReorganization.String())
}

func TestColumnTypeChangeFromIntegerToOthers(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	prepare := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a tinyint, b smallint, c mediumint, d int, e bigint, f bigint)")
		tk.MustExec("insert into t values(1, 11, 111, 1111, 11111, 111111)")
	}
	prepareForEnumSet := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a tinyint, b smallint, c mediumint, d int, e bigint)")
		tk.MustExec("insert into t values(1, 1, 1, 11111, 11111)")
	}

	// integer to string
	prepare(tk)
	tk.MustExec("alter table t modify a varchar(10)")
	modifiedColumn := external.GetModifyColumn(t, tk, "test", "t", "a", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeVarchar, modifiedColumn.GetType())
	tk.MustQuery("select a from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify b char(10)")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "b", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeString, modifiedColumn.GetType())
	tk.MustQuery("select b from t").Check(testkit.Rows("11"))

	tk.MustExec("alter table t modify c binary(10)")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "c", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeString, modifiedColumn.GetType())
	tk.MustQuery("select c from t").Check(testkit.Rows("111\x00\x00\x00\x00\x00\x00\x00"))

	tk.MustExec("alter table t modify d varbinary(10)")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "d", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeVarchar, modifiedColumn.GetType())
	tk.MustQuery("select d from t").Check(testkit.Rows("1111"))

	tk.MustExec("alter table t modify e blob(10)")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "e", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeTinyBlob, modifiedColumn.GetType())
	tk.MustQuery("select e from t").Check(testkit.Rows("11111"))

	tk.MustExec("alter table t modify f text(10)")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "f", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeTinyBlob, modifiedColumn.GetType())
	tk.MustQuery("select f from t").Check(testkit.Rows("111111"))

	// integer to decimal
	prepare(tk)
	tk.MustExec("alter table t modify a decimal(2,1)")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "a", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeNewDecimal, modifiedColumn.GetType())
	tk.MustQuery("select a from t").Check(testkit.Rows("1.0"))

	// integer to year
	// For year(2), MySQL converts values in the ranges '0' to '69' and '70' to '99' to YEAR values in the ranges 2000 to 2069 and 1970 to 1999.
	tk.MustExec("alter table t modify b year")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "b", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeYear, modifiedColumn.GetType())
	tk.MustQuery("select b from t").Check(testkit.Rows("2011"))

	// integer to time
	tk.MustExec("alter table t modify c time")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "c", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeDuration, modifiedColumn.GetType())
	tk.MustQuery("select c from t").Check(testkit.Rows("00:01:11"))

	// integer to date (mysql will throw `Incorrect date value: '1111' for column 'd' at row 1` error)
	tk.MustExec("alter table t modify d date")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "d", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeDate, modifiedColumn.GetType())
	tk.MustQuery("select d from t").Check(testkit.Rows("2000-11-11")) // the given number will be left-forward used.

	// integer to timestamp (according to what timezone you have set)
	tk.MustExec("alter table t modify e timestamp")
	tk.MustExec("set @@session.time_zone=UTC")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "e", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeTimestamp, modifiedColumn.GetType())
	tk.MustQuery("select e from t").Check(testkit.Rows("2001-11-10 16:00:00")) // the given number will be left-forward used.

	// integer to datetime
	tk.MustExec("alter table t modify f datetime")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "f", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeDatetime, modifiedColumn.GetType())
	tk.MustQuery("select f from t").Check(testkit.Rows("2011-11-11 00:00:00")) // the given number will be left-forward used.

	// integer to floating-point values
	prepare(tk)
	tk.MustExec("alter table t modify a float")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "a", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeFloat, modifiedColumn.GetType())
	tk.MustQuery("select a from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify b double")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "b", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeDouble, modifiedColumn.GetType())
	tk.MustQuery("select b from t").Check(testkit.Rows("11"))

	// integer to bit
	tk.MustExec("alter table t modify c bit(10)")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "c", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeBit, modifiedColumn.GetType())
	// 111 will be stored ad 0x00,0110,1111 = 0x6F, which will be shown as ASCII('o')=111 as well.
	tk.MustQuery("select c from t").Check(testkit.Rows("\x00o"))

	// integer to json
	tk.MustExec("alter table t modify d json")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "d", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeJSON, modifiedColumn.GetType())
	tk.MustQuery("select d from t").Check(testkit.Rows("1111"))

	// integer to enum
	prepareForEnumSet(tk)
	// TiDB take integer as the enum element offset to cast.
	tk.MustExec("alter table t modify a enum(\"a\", \"b\")")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "a", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeEnum, modifiedColumn.GetType())
	tk.MustQuery("select a from t").Check(testkit.Rows("a"))

	// TiDB take integer as the set element offset to cast.
	tk.MustExec("alter table t modify b set(\"a\", \"b\")")
	modifiedColumn = external.GetModifyColumn(t, tk, "test", "t", "b", false)
	require.NotNil(t, modifiedColumn)
	require.Equal(t, mysql.TypeSet, modifiedColumn.GetType())
	tk.MustQuery("select b from t").Check(testkit.Rows("a"))

	// TiDB can't take integer as the enum element string to cast, while the MySQL can.
	tk.MustGetErrCode("alter table t modify d enum(\"11111\", \"22222\")", errno.WarnDataTruncated)

	// TiDB can't take integer as the set element string to cast, while the MySQL can.
	tk.MustGetErrCode("alter table t modify e set(\"11111\", \"22222\")", errno.WarnDataTruncated)
}

func TestColumnTypeChangeBetweenVarcharAndNonVarchar(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists col_type_change_char;")
	tk.MustExec("create database col_type_change_char;")
	tk.MustExec("use col_type_change_char;")

	// https://github.com/pingcap/tidb/issues/23624
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(b varchar(10));")
	tk.MustExec("insert into t values ('aaa   ');")
	tk.MustExec("alter table t change column b b char(10);")
	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("alter table t change column b b varchar(10);")
	tk.MustQuery("select b from t use index(idx);").Check(testkit.Rows("aaa"))
	tk.MustQuery("select b from t ignore index(idx);").Check(testkit.Rows("aaa"))
	tk.MustExec("admin check table t;")

	// https://github.com/pingcap/tidb/pull/23688#issuecomment-810166597
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(10));")
	tk.MustExec("insert into t values ('aaa   ');")
	tk.MustQuery("select a from t;").Check(testkit.Rows("aaa   "))
	tk.MustExec("alter table t modify column a char(10);")
	tk.MustQuery("select a from t;").Check(testkit.Rows("aaa"))
}

func TestColumnTypeChangeFromStringToOthers(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Set time zone to UTC.
	originalTz := tk.Session().GetSessionVars().TimeZone
	tk.Session().GetSessionVars().TimeZone = time.UTC
	defer func() {
		tk.Session().GetSessionVars().TimeZone = originalTz
	}()

	// Init string date type table.
	reset := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec(`
			create table t (
				c char(8),
				vc varchar(8),
				bny binary(8),
				vbny varbinary(8),
				bb blob,
				txt text,
				e enum('123', '2020-07-15 18:32:17.888', 'str', '{"k1": "value"}'),
				s set('123', '2020-07-15 18:32:17.888', 'str', '{"k1": "value"}')
			)
		`)
	}

	// To numeric data types.
	// tinyint
	reset(tk)
	tk.MustExec("insert into t values ('123', '123', '123', '123', '123', '123', '123', '123')")
	tk.MustExec("alter table t modify c tinyint")
	tk.MustExec("alter table t modify vc tinyint")
	tk.MustExec("alter table t modify bny tinyint")
	tk.MustExec("alter table t modify vbny tinyint")
	tk.MustExec("alter table t modify bb tinyint")
	tk.MustExec("alter table t modify txt tinyint")
	tk.MustExec("alter table t modify e tinyint")
	tk.MustExec("alter table t modify s tinyint")
	tk.MustQuery("select * from t").Check(testkit.Rows("123 123 123 123 123 123 1 1"))
	// int
	reset(tk)
	tk.MustExec("insert into t values ('17305', '17305', '17305', '17305', '17305', '17305', '123', '123')")
	tk.MustExec("alter table t modify c int")
	tk.MustExec("alter table t modify vc int")
	tk.MustExec("alter table t modify bny int")
	tk.MustExec("alter table t modify vbny int")
	tk.MustExec("alter table t modify bb int")
	tk.MustExec("alter table t modify txt int")
	tk.MustExec("alter table t modify e int")
	tk.MustExec("alter table t modify s int")
	tk.MustQuery("select * from t").Check(testkit.Rows("17305 17305 17305 17305 17305 17305 1 1"))
	// bigint
	reset(tk)
	tk.MustExec("insert into t values ('17305867', '17305867', '17305867', '17305867', '17305867', '17305867', '123', '123')")
	tk.MustExec("alter table t modify c bigint")
	tk.MustExec("alter table t modify vc bigint")
	tk.MustExec("alter table t modify bny bigint")
	tk.MustExec("alter table t modify vbny bigint")
	tk.MustExec("alter table t modify bb bigint")
	tk.MustExec("alter table t modify txt bigint")
	tk.MustExec("alter table t modify e bigint")
	tk.MustExec("alter table t modify s bigint")
	tk.MustQuery("select * from t").Check(testkit.Rows("17305867 17305867 17305867 17305867 17305867 17305867 1 1"))
	// bit
	reset(tk)
	tk.MustExec("insert into t values ('1', '1', '1', '1', '1', '1', '123', '123')")
	tk.MustGetErrCode("alter table t modify c bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify vc bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify bny bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify vbny bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify bb bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify txt bit", errno.ErrUnsupportedDDLOperation)
	tk.MustExec("alter table t modify e bit")
	tk.MustExec("alter table t modify s bit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1\x00\x00\x00\x00\x00\x00\x00 1 1 1 \x01 \x01"))
	// decimal
	reset(tk)
	tk.MustExec("insert into t values ('123.45', '123.45', '123.45', '123.45', '123.45', '123.45', '123', '123')")
	tk.MustExec("alter table t modify c decimal(7, 4)")
	tk.MustExec("alter table t modify vc decimal(7, 4)")
	tk.MustGetErrCode("alter table t modify bny decimal(7, 4)", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify vbny decimal(7, 4)")
	tk.MustExec("alter table t modify bb decimal(7, 4)")
	tk.MustExec("alter table t modify txt decimal(7, 4)")
	tk.MustExec("alter table t modify e decimal(7, 4)")
	tk.MustExec("alter table t modify s decimal(7, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("123.4500 123.4500 123.45\x00\x00 123.4500 123.4500 123.4500 1.0000 1.0000"))
	// double
	reset(tk)
	tk.MustExec("insert into t values ('123.45', '123.45', '123.45', '123.45', '123.45', '123.45', '123', '123')")
	tk.MustExec("alter table t modify c double(7, 4)")
	tk.MustExec("alter table t modify vc double(7, 4)")
	tk.MustExec("alter table t modify bny double(7, 4)")
	tk.MustExec("alter table t modify vbny double(7, 4)")
	tk.MustExec("alter table t modify bb double(7, 4)")
	tk.MustExec("alter table t modify txt double(7, 4)")
	tk.MustExec("alter table t modify e double(7, 4)")
	tk.MustExec("alter table t modify s double(7, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("123.45 123.45 123.45 123.45 123.45 123.45 1 1"))

	// To date and time data types.
	// date
	reset(tk)
	tk.MustExec("insert into t values ('20200826', '2008261', '20200826', '200826', '2020-08-26', '08-26 19:35:41', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888')")
	tk.MustExec("alter table t modify c date")
	tk.MustExec("alter table t modify vc date")
	tk.MustExec("alter table t modify bny date")
	tk.MustExec("alter table t modify vbny date")
	tk.MustExec("alter table t modify bb date")
	// Alter text '08-26 19:35:41' to date will error. (same as mysql does)
	tk.MustGetErrCode("alter table t modify txt date", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify e date", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify s date", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-08-26 2020-08-26 2020-08-26 2020-08-26 2020-08-26 08-26 19:35:41 2020-07-15 18:32:17.888 2020-07-15 18:32:17.888"))
	// time
	reset(tk)
	tk.MustExec("insert into t values ('19:35:41', '19:35:41', '19:35:41', '19:35:41', '19:35:41.45678', '19:35:41.45678', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888')")
	tk.MustExec("alter table t modify c time")
	tk.MustExec("alter table t modify vc time")
	tk.MustExec("alter table t modify bny time")
	tk.MustExec("alter table t modify vbny time")
	tk.MustExec("alter table t modify bb time")
	tk.MustExec("alter table t modify txt time")
	tk.MustGetErrCode("alter table t modify e time", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify s time", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("19:35:41 19:35:41 19:35:41 19:35:41 19:35:41 19:35:41 2020-07-15 18:32:17.888 2020-07-15 18:32:17.888"))
	// datetime
	reset(tk)
	tk.MustExec("alter table t modify c char(23)")
	tk.MustExec("alter table t modify vc varchar(23)")
	tk.MustExec("alter table t modify bny binary(23)")
	tk.MustExec("alter table t modify vbny varbinary(23)")
	tk.MustExec("insert into t values ('2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888')")
	tk.MustExec("alter table t modify c datetime")
	tk.MustExec("alter table t modify vc datetime")
	tk.MustExec("alter table t modify bny datetime")
	tk.MustExec("alter table t modify vbny datetime")
	tk.MustExec("alter table t modify bb datetime")
	tk.MustExec("alter table t modify txt datetime")
	tk.MustGetErrCode("alter table t modify e datetime", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify s datetime", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:17.888 2020-07-15 18:32:17.888"))
	// timestamp
	reset(tk)
	tk.MustExec("alter table t modify c char(23)")
	tk.MustExec("alter table t modify vc varchar(23)")
	tk.MustExec("alter table t modify bny binary(23)")
	tk.MustExec("alter table t modify vbny varbinary(23)")
	tk.MustExec("insert into t values ('2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888')")
	tk.MustExec("alter table t modify c timestamp")
	tk.MustExec("alter table t modify vc timestamp")
	tk.MustExec("alter table t modify bny timestamp")
	tk.MustExec("alter table t modify vbny timestamp")
	tk.MustExec("alter table t modify bb timestamp")
	tk.MustExec("alter table t modify txt timestamp")
	tk.MustGetErrCode("alter table t modify e timestamp", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify s timestamp", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:18 2020-07-15 18:32:17.888 2020-07-15 18:32:17.888"))
	// year
	reset(tk)
	tk.MustExec("insert into t values ('2020', '91', '2', '2020', '20', '99', '2020-07-15 18:32:17.888', '2020-07-15 18:32:17.888')")
	tk.MustExec("alter table t modify c year")
	tk.MustExec("alter table t modify vc year")
	tk.MustExec("alter table t modify bny year")
	tk.MustExec("alter table t modify vbny year")
	tk.MustExec("alter table t modify bb year")
	tk.MustExec("alter table t modify txt year")
	tk.MustExec("alter table t modify e year")
	tk.MustExec("alter table t modify s year")
	tk.MustQuery("select * from t").Check(testkit.Rows("2020 1991 2002 2020 2020 1999 2002 2002"))

	// To json data type.
	reset(tk)
	tk.MustExec("alter table t modify c char(15)")
	tk.MustExec("alter table t modify vc varchar(15)")
	tk.MustExec("alter table t modify bny binary(15)")
	tk.MustExec("alter table t modify vbny varbinary(15)")
	tk.MustExec("insert into t values ('{\"k1\": \"value\"}', '{\"k1\": \"value\"}', '{\"k1\": \"value\"}', '{\"k1\": \"value\"}', '{\"k1\": \"value\"}', '{\"k1\": \"value\"}', '{\"k1\": \"value\"}', '{\"k1\": \"value\"}')")
	tk.MustExec("alter table t modify c json")
	tk.MustExec("alter table t modify vc json")
	tk.MustExec("alter table t modify bny json")
	tk.MustExec("alter table t modify vbny json")
	tk.MustExec("alter table t modify bb json")
	tk.MustExec("alter table t modify txt json")
	tk.MustExec("alter table t modify e json")
	tk.MustExec("alter table t modify s json")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"k1\": \"value\"} {\"k1\": \"value\"} {\"k1\": \"value\"} {\"k1\": \"value\"} {\"k1\": \"value\"} {\"k1\": \"value\"} \"{\\\"k1\\\": \\\"value\\\"}\" \"{\\\"k1\\\": \\\"value\\\"}\""))

	reset(tk)
	tk.MustExec("insert into t values ('123x', 'x123', 'abc', 'datetime', 'timestamp', 'date', '123', '123')")
	tk.MustGetErrCode("alter table t modify c int", errno.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify vc smallint", errno.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify bny bigint", errno.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify vbny datetime", errno.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify bb timestamp", errno.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify txt date", errno.ErrTruncatedWrongValue)

	reset(tk)
	tk.MustExec("alter table t modify vc varchar(20)")
	tk.MustExec("insert into t(c, vc) values ('1x', '20200915110836')")
	tk.MustGetErrCode("alter table t modify c year", errno.ErrTruncatedWrongValue)

	// Special cases about different behavior between TiDB and errno.
	// MySQL will get warning but TiDB not.
	// MySQL will get "Warning 1292 Incorrect time value: '20200915110836' for column 'vc'"
	tk.MustExec("alter table t modify vc time")
	tk.MustQuery("select vc from t").Check(testkit.Rows("11:08:36"))

	// Both error but different error message.
	// MySQL will get "ERROR 3140 (22032): Invalid JSON text: "The document root must not be followed by other values." at position 1 in value for column '#sql-5b_42.c'." error.
	reset(tk)
	tk.MustExec("alter table t modify c char(15)")
	tk.MustExec("insert into t(c) values ('{\"k1\": \"value\"')")
	tk.MustGetErrCode("alter table t modify c json", errno.ErrInvalidJSONText)

	// MySQL will get "ERROR 1366 (HY000): Incorrect DECIMAL value: '0' for column '' at row -1" error.
	tk.MustExec("insert into t(vc) values ('abc')")
	tk.MustGetErrCode("alter table t modify vc decimal(5,3)", errno.ErrBadNumber)
}

func TestColumnTypeChangeFromNumericToOthers(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Set time zone to UTC.
	originalTz := tk.Session().GetSessionVars().TimeZone
	tk.Session().GetSessionVars().TimeZone = time.UTC
	defer func() {
		tk.Session().GetSessionVars().TimeZone = originalTz
	}()

	// Init string date type table.
	reset := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec(`
			create table t (
				d decimal(13, 7),
				n numeric(5, 2),
				r real(20, 12),
				db real(32, 11),
				f32 float(23),
				f64 double,
				b bit(5)
			)
		`)
	}

	// To integer data types.
	// tinyint
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustGetErrCode("alter table t modify d tinyint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify n tinyint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify r tinyint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify db tinyint", errno.ErrDataOutOfRange)
	tk.MustExec("alter table t modify f32 tinyint")
	tk.MustGetErrCode("alter table t modify f64 tinyint", errno.ErrDataOutOfRange)
	tk.MustExec("alter table t modify b tinyint")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33 2000000.20000002 323232323.32323235 -111 -222222222222.22223 21"))
	// int
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustExec("alter table t modify d int")
	tk.MustExec("alter table t modify n int")
	tk.MustExec("alter table t modify r int")
	tk.MustExec("alter table t modify db int")
	tk.MustExec("alter table t modify f32 int")
	tk.MustGetErrCode("alter table t modify f64 int", errno.ErrDataOutOfRange)
	tk.MustExec("alter table t modify b int")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258 333 2000000 323232323 -111 -222222222222.22223 21"))
	// bigint
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustExec("alter table t modify d bigint")
	tk.MustExec("alter table t modify n bigint")
	tk.MustExec("alter table t modify r bigint")
	tk.MustExec("alter table t modify db bigint")
	tk.MustExec("alter table t modify f32 bigint")
	tk.MustExec("alter table t modify f64 bigint")
	tk.MustExec("alter table t modify b bigint")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258 333 2000000 323232323 -111 -222222222222 21"))
	// unsigned bigint
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	// MySQL will get "ERROR 1264 (22001): Data truncation: Out of range value for column 'd' at row 1".
	tk.MustGetErrCode("alter table t modify d bigint unsigned", errno.ErrDataOutOfRange)
	tk.MustExec("alter table t modify n bigint unsigned")
	tk.MustExec("alter table t modify r bigint unsigned")
	tk.MustExec("alter table t modify db bigint unsigned")
	// MySQL will get "ERROR 1264 (22001): Data truncation: Out of range value for column 'f32' at row 1".
	tk.MustGetErrCode("alter table t modify f32 bigint unsigned", errno.ErrDataOutOfRange)
	// MySQL will get "ERROR 1264 (22001): Data truncation: Out of range value for column 'f64' at row 1".
	tk.MustGetErrCode("alter table t modify f64 bigint unsigned", errno.ErrDataOutOfRange)
	tk.MustExec("alter table t modify b int")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333 2000000 323232323 -111.111115 -222222222222.22223 21"))

	// To string data types.
	// char
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustExec("alter table t modify d char(20)")
	tk.MustExec("alter table t modify n char(20)")
	tk.MustExec("alter table t modify r char(20)")
	// MySQL will get "ERROR 1406 (22001): Data truncation: Data too long for column 'db' at row 1".
	tk.MustExec("alter table t modify db char(20)")
	// MySQL will get "-111.111" rather than "-111.111115" at TiDB.
	tk.MustExec("alter table t modify f32 char(20)")
	// MySQL will get "ERROR 1406 (22001): Data truncation: Data too long for column 'f64' at row 1".
	tk.MustExec("alter table t modify f64 char(20)")
	tk.MustExec("alter table t modify b char(20)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33 2000000.20000002 323232323.32323235 -111.111115 -222222222222.22223 21"))

	// varchar
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustExec("alter table t modify d varchar(30)")
	tk.MustExec("alter table t modify n varchar(30)")
	tk.MustExec("alter table t modify r varchar(30)")
	tk.MustExec("alter table t modify db varchar(30)")
	// MySQL will get "-111.111" rather than "-111.111115" at TiDB.
	tk.MustExec("alter table t modify f32 varchar(30)")
	// MySQL will get "ERROR 1406 (22001): Data truncation: Data too long for column 'f64' at row 1".
	tk.MustExec("alter table t modify f64 varchar(30)")
	tk.MustExec("alter table t modify b varchar(30)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33 2000000.20000002 323232323.32323235 -111.111115 -222222222222.22223 21"))

	// binary
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustGetErrCode("alter table t modify d binary(10)", errno.WarnDataTruncated)
	tk.MustExec("alter table t modify n binary(10)")
	tk.MustGetErrCode("alter table t modify r binary(10)", errno.WarnDataTruncated)
	tk.MustGetErrCode("alter table t modify db binary(10)", errno.WarnDataTruncated)
	// MySQL will run with no error.
	tk.MustGetErrCode("alter table t modify f32 binary(10)", errno.WarnDataTruncated)
	tk.MustGetErrCode("alter table t modify f64 binary(10)", errno.WarnDataTruncated)
	tk.MustExec("alter table t modify b binary(10)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33\x00\x00\x00\x00 2000000.20000002 323232323.32323235 -111.111115 -222222222222.22223 21\x00\x00\x00\x00\x00\x00\x00\x00"))

	// varbinary
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustExec("alter table t modify d varbinary(30)")
	tk.MustExec("alter table t modify n varbinary(30)")
	tk.MustExec("alter table t modify r varbinary(30)")
	tk.MustExec("alter table t modify db varbinary(30)")
	// MySQL will get "-111.111" rather than "-111.111115" at TiDB.
	tk.MustExec("alter table t modify f32 varbinary(30)")
	// MySQL will get "ERROR 1406 (22001): Data truncation: Data too long for column 'f64' at row 1".
	tk.MustExec("alter table t modify f64 varbinary(30)")
	tk.MustExec("alter table t modify b varbinary(30)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33 2000000.20000002 323232323.32323235 -111.111115 -222222222222.22223 21"))

	// blob
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustExec("alter table t modify d blob")
	tk.MustExec("alter table t modify n blob")
	tk.MustExec("alter table t modify r blob")
	tk.MustExec("alter table t modify db blob")
	// MySQL will get "-111.111" rather than "-111.111115" at TiDB.
	tk.MustExec("alter table t modify f32 blob")
	tk.MustExec("alter table t modify f64 blob")
	tk.MustExec("alter table t modify b blob")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33 2000000.20000002 323232323.32323235 -111.111115 -222222222222.22223 21"))

	// text
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustExec("alter table t modify d text")
	tk.MustExec("alter table t modify n text")
	tk.MustExec("alter table t modify r text")
	tk.MustExec("alter table t modify db text")
	// MySQL will get "-111.111" rather than "-111.111115" at TiDB.
	tk.MustExec("alter table t modify f32 text")
	tk.MustExec("alter table t modify f64 text")
	tk.MustExec("alter table t modify b text")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33 2000000.20000002 323232323.32323235 -111.111115 -222222222222.22223 21"))

	// enum
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.111, -222222222222.222222222222222, b'10101')")
	tk.MustGetErrCode("alter table t modify d enum('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify n enum('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify r enum('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify db enum('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f32 enum('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f64 enum('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify b enum('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33 2000000.20000002 323232323.32323235 -111.111 -222222222222.22223 \x15"))

	// set
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.111, -222222222222.222222222222222, b'10101')")
	tk.MustGetErrCode("alter table t modify d set('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify n set('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify r set('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify db set('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f32 set('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f64 set('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify b set('-258.12345', '333.33', '2000000.20000002', '323232323.3232323232', '-111.111', '-222222222222.222222222222222', b'10101')", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.1234500 333.33 2000000.20000002 323232323.32323235 -111.111 -222222222222.22223 \x15"))

	// To date and time data types.
	// datetime
	reset(tk)
	tk.MustExec("insert into t values (200805.11, 307.333, 20200805.11111111, 20200805111307.11111111, 200805111307.11111111, 20200805111307.11111111, b'10101')")
	// MySQL will get "ERROR 1292 (22001) Data truncation: Incorrect datetime value: '200805.1100000' for column 'd' at row 1".
	tk.MustGetErrCode("alter table t modify d datetime", errno.ErrUnsupportedDDLOperation)
	// MySQL will get "ERROR 1292 (22001) Data truncation: Incorrect datetime value: '307.33' for column 'n' at row 1".
	tk.MustGetErrCode("alter table t modify n datetime", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify r datetime", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify db datetime", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f32 datetime", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f64 datetime", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify b datetime", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("200805.1100000 307.33 20200805.11111111 20200805111307.11 200805100000 20200805111307.11 \x15"))
	// time
	reset(tk)
	tk.MustExec("insert into t values (200805.11, 307.333, 20200805.11111111, 20200805111307.11111111, 200805111307.11111111, 20200805111307.11111111, b'10101')")
	tk.MustExec("alter table t modify d time")
	tk.MustExec("alter table t modify n time")
	tk.MustGetErrCode("alter table t modify r time", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify db time")
	tk.MustExec("alter table t modify f32 time")
	tk.MustExec("alter table t modify f64 time")
	tk.MustGetErrCode("alter table t modify b time", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("20:08:05 00:03:07 20200805.11111111 11:13:07 10:00:00 11:13:07 \x15"))
	// date
	reset(tk)
	tk.MustExec("insert into t values (200805.11, 307.333, 20200805.11111111, 20200805111307.11111111, 200805111307.11111111, 20200805111307.11111111, b'10101')")
	// MySQL will get "ERROR 1292 (22001) Data truncation: Incorrect date value: '200805.1100000' for column 'd' at row 1".
	tk.MustGetErrCode("alter table t modify d date", errno.ErrUnsupportedDDLOperation)
	// MySQL will get "ERROR 1292 (22001) Data truncation: Incorrect date value: '307.33' for column 'n' at row 1".
	tk.MustGetErrCode("alter table t modify n date", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify r date", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify db date", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f32 date", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f64 date", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify b date", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("200805.1100000 307.33 20200805.11111111 20200805111307.11 200805100000 20200805111307.11 \x15"))
	// timestamp
	reset(tk)
	tk.MustExec("insert into t values (200805.11, 307.333, 20200805.11111111, 20200805111307.11111111, 200805111307.11111111, 20200805111307.11111111, b'10101')")
	// MySQL will get "ERROR 1292 (22001) Data truncation: Incorrect datetime value: '200805.1100000' for column 'd' at row 1".
	tk.MustGetErrCode("alter table t modify d timestamp", errno.ErrUnsupportedDDLOperation)
	// MySQL will get "ERROR 1292 (22001) Data truncation: Incorrect datetime value: '307.33' for column 'n' at row 1".
	tk.MustGetErrCode("alter table t modify n timestamp", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify r timestamp", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify db timestamp", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f32 timestamp", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f64 timestamp", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify b timestamp", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("200805.1100000 307.33 20200805.11111111 20200805111307.11 200805100000 20200805111307.11 \x15"))
	// year
	reset(tk)
	tk.MustExec("insert into t values (200805.11, 307.333, 2.55555, 98.1111111, 2154.00001, 20200805111307.11111111, b'10101')")
	tk.MustGetErrMsg("alter table t modify d year", "[types:1264]Out of range value for column 'd', the value is '200805.1100000'")
	tk.MustGetErrCode("alter table t modify n year", errno.ErrWarnDataOutOfRange)
	// MySQL will get "ERROR 1264 (22001) Data truncation: Out of range value for column 'r' at row 1".
	tk.MustExec("alter table t modify r year")
	// MySQL will get "ERROR 1264 (22001) Data truncation: Out of range value for column 'db' at row 1".
	tk.MustExec("alter table t modify db year")
	// MySQL will get "ERROR 1264 (22001) Data truncation: Out of range value for column 'f32' at row 1".
	tk.MustExec("alter table t modify f32 year")
	tk.MustGetErrMsg("alter table t modify f64 year", "[types:1264]Out of range value for column 'f64', the value is '20200805111307.11'")
	tk.MustExec("alter table t modify b year")
	tk.MustQuery("select * from t").Check(testkit.Rows("200805.1100000 307.33 2003 1998 2154 20200805111307.11 2021"))

	// To json data type.
	reset(tk)
	tk.MustExec("insert into t values (-258.12345, 333.33, 2000000.20000002, 323232323.3232323232, -111.11111111, -222222222222.222222222222222, b'10101')")
	tk.MustExec("alter table t modify d json")
	tk.MustExec("alter table t modify n json")
	tk.MustExec("alter table t modify r json")
	tk.MustExec("alter table t modify db json")
	tk.MustExec("alter table t modify f32 json")
	tk.MustExec("alter table t modify f64 json")
	tk.MustExec("alter table t modify b json")
	tk.MustQuery("select * from t").Check(testkit.Rows("-258.12345 333.33 2000000.20000002 323232323.32323235 -111.11111450195312 -222222222222.22223 \"\\u0015\""))
}

// Test issue #20529.
func TestColumnTypeChangeIgnoreDisplayLength(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var assertResult bool
	assertHasAlterWriteReorg := func(tbl table.Table) {
		// Restore assertResult to false.
		assertResult = false
		hook := &ddl.TestDDLCallback{Do: dom}
		hook.OnJobRunBeforeExported = func(job *model.Job) {
			if tbl.Meta().ID != job.TableID {
				return
			}
			if job.SchemaState == model.StateWriteReorganization {
				assertResult = true
			}
		}
		dom.DDL().SetHook(hook)
	}

	// Change int to tinyint.
	// Although display length is increased, the default flen is decreased, reorg is needed.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(1))")
	tbl := external.GetTableByName(t, tk, "test", "t")
	assertHasAlterWriteReorg(tbl)
	tk.MustExec("alter table t modify column a tinyint(3)")
	require.True(t, assertResult)

	// Change tinyint to tinyint
	// Although display length is decreased, default flen is the same, reorg is not needed.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a tinyint(3))")
	tbl = external.GetTableByName(t, tk, "test", "t")
	assertHasAlterWriteReorg(tbl)
	tk.MustExec("alter table t modify column a tinyint(1)")
	require.False(t, assertResult)
	tk.MustExec("drop table if exists t")
}

func TestColumnTypeChangeFromDateTimeTypeToOthers(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Set time zone to UTC.
	originalTz := tk.Session().GetSessionVars().TimeZone
	tk.Session().GetSessionVars().TimeZone = time.UTC
	defer func() {
		tk.Session().GetSessionVars().TimeZone = originalTz
	}()

	// Init string date type table.
	reset := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec(`
			create table t (
                   d date,
                   t time(3),
                   dt datetime(6),
                   tmp timestamp(6),
                   y year
			)
		`)
	}

	// To numeric data types.
	// tinyint
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustGetErrCode("alter table t modify d tinyint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify t tinyint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify dt tinyint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify tmp tinyint", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify y tinyint", errno.ErrDataOutOfRange)
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))
	// int
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d int")
	tk.MustExec("alter table t modify t int")
	tk.MustGetErrCode("alter table t modify dt int", errno.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify tmp int", errno.ErrDataOutOfRange)
	tk.MustExec("alter table t modify y int")
	tk.MustQuery("select * from t").Check(testkit.Rows("20201030 193825 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))
	// bigint
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d bigint")
	tk.MustExec("alter table t modify t bigint")
	tk.MustExec("alter table t modify dt bigint")
	tk.MustExec("alter table t modify tmp bigint")
	tk.MustExec("alter table t modify y bigint")
	tk.MustQuery("select * from t").Check(testkit.Rows("20201030 193825 20201030082133 20201030082133 2020"))
	// bit
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustGetErrCode("alter table t modify d bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify t bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify dt bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify tmp bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify y bit", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))
	// decimal
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d decimal")
	tk.MustExec("alter table t modify t decimal(10, 4)")
	tk.MustExec("alter table t modify dt decimal(20, 6)")
	tk.MustExec("alter table t modify tmp decimal(22, 8)")
	tk.MustExec("alter table t modify y decimal")
	tk.MustQuery("select * from t").Check(testkit.Rows("20201030 193825.0010 20201030082133.455555 20201030082133.45555500 2020"))
	// double
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d double")
	tk.MustExec("alter table t modify t double(10, 4)")
	tk.MustExec("alter table t modify dt double(20, 6)")
	tk.MustExec("alter table t modify tmp double(22, 8)")
	tk.MustExec("alter table t modify y double")
	tk.MustQuery("select * from t").Check(testkit.Rows("20201030 193825.001 20201030082133.457 20201030082133.457 2020"))

	// To string data types.
	// char
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d char(30)")
	tk.MustExec("alter table t modify t char(30)")
	tk.MustExec("alter table t modify dt char(30)")
	tk.MustExec("alter table t modify tmp char(30)")
	tk.MustExec("alter table t modify y char(30)")
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))

	// varchar
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d varchar(30)")
	tk.MustExec("alter table t modify t varchar(30)")
	tk.MustExec("alter table t modify dt varchar(30)")
	tk.MustExec("alter table t modify tmp varchar(30)")
	tk.MustExec("alter table t modify y varchar(30)")
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))

	// binary
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d binary(30)")
	tk.MustExec("alter table t modify t binary(30)")
	tk.MustExec("alter table t modify dt binary(30)")
	tk.MustExec("alter table t modify tmp binary(30)")
	tk.MustExec("alter table t modify y binary(30)")
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 " +
		"19:38:25.001\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 " +
		"2020-10-30 08:21:33.455555\x00\x00\x00\x00 " +
		"2020-10-30 08:21:33.455555\x00\x00\x00\x00 " +
		"2020\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))

	// varbinary
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d varbinary(30)")
	tk.MustExec("alter table t modify t varbinary(30)")
	tk.MustExec("alter table t modify dt varbinary(30)")
	tk.MustExec("alter table t modify tmp varbinary(30)")
	tk.MustExec("alter table t modify y varbinary(30)")
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))

	// text
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d text")
	tk.MustExec("alter table t modify t text")
	tk.MustExec("alter table t modify dt text")
	tk.MustExec("alter table t modify tmp text")
	tk.MustExec("alter table t modify y text")
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))

	// blob
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d blob")
	tk.MustExec("alter table t modify t blob")
	tk.MustExec("alter table t modify dt blob")
	tk.MustExec("alter table t modify tmp blob")
	tk.MustExec("alter table t modify y blob")
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))

	// enum
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustGetErrCode("alter table t modify d enum('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify t enum('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify dt enum('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify tmp enum('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify y enum('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))

	// set
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustGetErrCode("alter table t modify d set('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify t set('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify dt set('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify tmp set('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify y set('2020-10-30', '19:38:25.001', '20201030082133.455555', '2020')", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("2020-10-30 19:38:25.001 2020-10-30 08:21:33.455555 2020-10-30 08:21:33.455555 2020"))

	// To json data type.
	reset(tk)
	tk.MustExec("insert into t values ('2020-10-30', '19:38:25.001', 20201030082133.455555, 20201030082133.455555, 2020)")
	tk.MustExec("alter table t modify d json")
	tk.MustExec("alter table t modify t json")
	tk.MustExec("alter table t modify dt json")
	tk.MustExec("alter table t modify tmp json")
	tk.MustExec("alter table t modify y json")
	tk.MustQuery("select * from t").Check(testkit.Rows("\"2020-10-30\" \"19:38:25.001\" \"2020-10-30 08:21:33.455555\" \"2020-10-30 08:21:33.455555\" 2020"))
}

func TestColumnTypeChangeFromJsonToOthers(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Set time zone to UTC.
	originalTz := tk.Session().GetSessionVars().TimeZone
	tk.Session().GetSessionVars().TimeZone = time.UTC
	defer func() {
		tk.Session().GetSessionVars().TimeZone = originalTz
	}()

	// Init string date type table.
	reset := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec(`
			create table t (
				obj json,
				arr json,
				nil json,
				t json,
				f json,
				i json,
				ui json,
				f64 json,
				str json,
				nul json
			)
		`)
	}

	// To numeric data types.
	// tinyint
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '{"obj": 100}' for column 'obj' at row 1".
	tk.MustGetErrCode("alter table t modify obj tinyint", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '[-1, 0, 1]' for column 'arr' at row 1".
	tk.MustGetErrCode("alter table t modify arr tinyint", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'null' for column 'nil' at row 1".
	tk.MustGetErrCode("alter table t modify nil tinyint", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'true' for column 't' at row 1".
	tk.MustExec("alter table t modify t tinyint")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'false' for column 'f' at row 1".
	tk.MustExec("alter table t modify f tinyint")
	tk.MustExec("alter table t modify i tinyint")
	tk.MustExec("alter table t modify ui tinyint")
	tk.MustGetErrCode("alter table t modify f64 tinyint", errno.ErrDataOutOfRange)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '"json string"' for column 'str' at row 1".
	tk.MustGetErrCode("alter table t modify str tinyint", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify nul tinyint")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null 1 0 -22 22 323232323.32323235 \"json string\" <nil>"))

	// int
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '{"obj": 100}' for column 'obj' at row 1".
	tk.MustGetErrCode("alter table t modify obj int", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '[-1, 0, 1]' for column 'arr' at row 1".
	tk.MustGetErrCode("alter table t modify arr int", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'null' for column 'nil' at row 1".
	tk.MustGetErrCode("alter table t modify nil int", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'true' for column 't' at row 1".
	tk.MustExec("alter table t modify t int")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'false' for column 'f' at row 1".
	tk.MustExec("alter table t modify f int")
	tk.MustExec("alter table t modify i int")
	tk.MustExec("alter table t modify ui int")
	tk.MustExec("alter table t modify f64 int")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '"json string"' for column 'str' at row 1".
	tk.MustGetErrCode("alter table t modify str int", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify nul int")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null 1 0 -22 22 323232323 \"json string\" <nil>"))

	// bigint
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '{"obj": 100}' for column 'obj' at row 1".
	tk.MustGetErrCode("alter table t modify obj bigint", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '[-1, 0, 1]' for column 'arr' at row 1".
	tk.MustGetErrCode("alter table t modify arr bigint", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'null' for column 'nil' at row 1".
	tk.MustGetErrCode("alter table t modify nil bigint", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'true' for column 't' at row 1".
	tk.MustExec("alter table t modify t bigint")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'false' for column 'f' at row 1".
	tk.MustExec("alter table t modify f bigint")
	tk.MustExec("alter table t modify i bigint")
	tk.MustExec("alter table t modify ui bigint")
	tk.MustExec("alter table t modify f64 bigint")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '"json string"' for column 'str' at row 1".
	tk.MustGetErrCode("alter table t modify str bigint", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify nul bigint")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null 1 0 -22 22 323232323 \"json string\" <nil>"))

	// unsigned bigint
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '{"obj": 100}' for column 'obj' at row 1".
	tk.MustGetErrCode("alter table t modify obj bigint unsigned", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '[-1, 0, 1]' for column 'arr' at row 1".
	tk.MustGetErrCode("alter table t modify arr bigint unsigned", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'null' for column 'nil' at row 1".
	tk.MustGetErrCode("alter table t modify nil bigint unsigned", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'true' for column 't' at row 1".
	tk.MustExec("alter table t modify t bigint unsigned")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: 'false' for column 'f' at row 1".
	tk.MustExec("alter table t modify f bigint unsigned")
	// MySQL will get "ERROR 1264 (22003) Out of range value for column 'i' at row 1".
	tk.MustGetErrCode("alter table t modify i bigint unsigned", errno.ErrDataOutOfRange)
	tk.MustExec("alter table t modify ui bigint unsigned")
	tk.MustExec("alter table t modify f64 bigint unsigned")
	// MySQL will get "ERROR 1366 (HY000) Incorrect integer value: '"json string"' for column 'str' at row 1".
	tk.MustGetErrCode("alter table t modify str bigint unsigned", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify nul bigint unsigned")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null 1 0 -22 22 323232323 \"json string\" <nil>"))

	// bit
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustGetErrCode("alter table t modify obj bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify arr bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify nil bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify t bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify i bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify ui bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f64 bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify str bit", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify nul bit", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false -22 22 323232323.32323235 \"json string\" <nil>"))

	// decimal
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	// MySQL will get "ERROR 3156 (22001) Invalid JSON value for CAST to DECIMAL from column obj at row 1".
	tk.MustGetErrCode("alter table t modify obj decimal(20, 10)", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 3156 (22001) Invalid JSON value for CAST to DECIMAL from column arr at row 1".
	tk.MustGetErrCode("alter table t modify arr decimal(20, 10)", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 3156 (22001) Invalid JSON value for CAST to DECIMAL from column nil at row 1".
	tk.MustGetErrCode("alter table t modify nil decimal(20, 10)", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify t decimal(20, 10)")
	tk.MustExec("alter table t modify f decimal(20, 10)")
	tk.MustExec("alter table t modify i decimal(20, 10)")
	tk.MustExec("alter table t modify ui decimal(20, 10)")
	tk.MustExec("alter table t modify f64 decimal(20, 10)")
	// MySQL will get "ERROR 1366 (HY000): Incorrect DECIMAL value: '0' for column '' at row -1".
	tk.MustGetErrCode("alter table t modify str decimal(20, 10)", errno.ErrBadNumber)
	tk.MustExec("alter table t modify nul decimal(20, 10)")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null 1.0000000000 0.0000000000 -22.0000000000 22.0000000000 323232323.3232323500 \"json string\" <nil>"))

	// double
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	// MySQL will get "ERROR 1265 (01000): Data truncated for column 'obj' at row 1".
	tk.MustGetErrCode("alter table t modify obj double", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1265 (01000): Data truncated for column 'arr' at row 1".
	tk.MustGetErrCode("alter table t modify arr double", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1265 (01000): Data truncated for column 'nil' at row 1".
	tk.MustGetErrCode("alter table t modify nil double", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1265 (01000): Data truncated for column 't' at row 1".
	tk.MustExec("alter table t modify t double")
	// MySQL will get "ERROR 1265 (01000): Data truncated for column 'f' at row 1".
	tk.MustExec("alter table t modify f double")
	tk.MustExec("alter table t modify i double")
	tk.MustExec("alter table t modify ui double")
	tk.MustExec("alter table t modify f64 double")
	// MySQL will get "ERROR 1265 (01000): Data truncated for column 'str' at row 1".
	tk.MustGetErrCode("alter table t modify str double", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify nul double")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null 1 0 -22 22 323232323.32323235 \"json string\" <nil>"))

	// To string data types.
	// char
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustExec("alter table t modify obj char(20)")
	tk.MustExec("alter table t modify arr char(20)")
	tk.MustExec("alter table t modify nil char(20)")
	tk.MustExec("alter table t modify t char(20)")
	tk.MustExec("alter table t modify f char(20)")
	tk.MustExec("alter table t modify i char(20)")
	tk.MustExec("alter table t modify ui char(20)")
	tk.MustExec("alter table t modify f64 char(20)")
	tk.MustExec("alter table t modify str char(20)")
	tk.MustExec("alter table t modify nil char(20)")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false -22 22 323232323.32323235 \"json string\" <nil>"))

	// varchar
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustExec("alter table t modify obj varchar(20)")
	tk.MustExec("alter table t modify arr varchar(20)")
	tk.MustExec("alter table t modify nil varchar(20)")
	tk.MustExec("alter table t modify t varchar(20)")
	tk.MustExec("alter table t modify f varchar(20)")
	tk.MustExec("alter table t modify i varchar(20)")
	tk.MustExec("alter table t modify ui varchar(20)")
	tk.MustExec("alter table t modify f64 varchar(20)")
	tk.MustExec("alter table t modify str varchar(20)")
	tk.MustExec("alter table t modify nul varchar(20)")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false -22 22 323232323.32323235 \"json string\" <nil>"))

	// binary
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustExec("alter table t modify obj binary(20)")
	tk.MustExec("alter table t modify arr binary(20)")
	tk.MustExec("alter table t modify nil binary(20)")
	tk.MustExec("alter table t modify t binary(20)")
	tk.MustExec("alter table t modify f binary(20)")
	tk.MustExec("alter table t modify i binary(20)")
	tk.MustExec("alter table t modify ui binary(20)")
	tk.MustExec("alter table t modify f64 binary(20)")
	tk.MustExec("alter table t modify str binary(20)")
	tk.MustExec("alter table t modify nul binary(20)")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"{\"obj\": 100}\x00\x00\x00\x00\x00\x00\x00\x00 " +
			"[-1, 0, 1]\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 " +
			"null\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 " +
			"true\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 " +
			"false\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 " +
			"-22\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 " +
			"22\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 " +
			"323232323.32323235\x00\x00 " +
			"\"json string\"\x00\x00\x00\x00\x00\x00\x00 <nil>"))
	// varbinary
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustExec("alter table t modify obj varbinary(20)")
	tk.MustExec("alter table t modify arr varbinary(20)")
	tk.MustExec("alter table t modify nil varbinary(20)")
	tk.MustExec("alter table t modify t varbinary(20)")
	tk.MustExec("alter table t modify f varbinary(20)")
	tk.MustExec("alter table t modify i varbinary(20)")
	tk.MustExec("alter table t modify ui varbinary(20)")
	tk.MustExec("alter table t modify f64 varbinary(20)")
	tk.MustExec("alter table t modify str varbinary(20)")
	tk.MustExec("alter table t modify nul varbinary(20)")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false -22 22 323232323.32323235 \"json string\" <nil>"))

	// blob
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustExec("alter table t modify obj blob")
	tk.MustExec("alter table t modify arr blob")
	tk.MustExec("alter table t modify nil blob")
	tk.MustExec("alter table t modify t blob")
	tk.MustExec("alter table t modify f blob")
	tk.MustExec("alter table t modify i blob")
	tk.MustExec("alter table t modify ui blob")
	tk.MustExec("alter table t modify f64 blob")
	tk.MustExec("alter table t modify str blob")
	tk.MustExec("alter table t modify nul blob")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false -22 22 323232323.32323235 \"json string\" <nil>"))

	// text
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustExec("alter table t modify obj text")
	tk.MustExec("alter table t modify arr text")
	tk.MustExec("alter table t modify nil text")
	tk.MustExec("alter table t modify t text")
	tk.MustExec("alter table t modify f text")
	tk.MustExec("alter table t modify i text")
	tk.MustExec("alter table t modify ui text")
	tk.MustExec("alter table t modify f64 text")
	tk.MustExec("alter table t modify str text")
	tk.MustExec("alter table t modify nul text")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false -22 22 323232323.32323235 \"json string\" <nil>"))

	// enum
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustGetErrCode("alter table t modify obj enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify arr enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify nil enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify t enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify i enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify ui enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f64 enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify str enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify nul enum('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false -22 22 323232323.32323235 \"json string\" <nil>"))

	// set
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"', null)")
	tk.MustGetErrCode("alter table t modify obj set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify arr set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify nil set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify t set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify i set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify ui set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify f64 set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify str set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify nul set('{\"obj\": 100}', '[-1]', 'null', 'true', 'false', '-22', '22', '323232323.3232323232', '\"json string\"')", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1] null true false -22 22 323232323.32323235 \"json string\" <nil>"))

	// To date and time data types.
	// datetime
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '20200826173501', '20201123', '20200826173501.123456', '\"2020-08-26 17:35:01.123456\"', null)")
	tk.MustGetErrCode("alter table t modify obj datetime", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify arr datetime", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify nil datetime", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify t datetime", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify f datetime", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify i datetime")
	tk.MustExec("alter table t modify ui datetime")
	tk.MustExec("alter table t modify f64 datetime")
	// MySQL will get "ERROR 1292 (22007): Incorrect datetime value: '"2020-08-26 17:35:01.123456"' for column 'str' at row 1".
	tk.MustExec("alter table t modify str datetime")
	tk.MustExec("alter table t modify nul datetime")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false 2020-08-26 17:35:01 2020-11-23 00:00:00 2020-08-26 17:35:01 2020-08-26 17:35:01 <nil>"))

	// time
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '200805', '1111', '200805.11', '\"19:35:41\"', null)")
	// MySQL will get "ERROR 1366 (HY000): Incorrect time value: '{"obj": 100}' for column 'obj' at row 1".
	tk.MustGetErrCode("alter table t modify obj time", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000): Incorrect time value: '[-1, 0, 1]' for column 'arr' at row 11".
	tk.MustGetErrCode("alter table t modify arr time", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000): Incorrect time value: 'null' for column 'nil' at row 1".
	tk.MustGetErrCode("alter table t modify nil time", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000): Incorrect time value: 'true' for column 't' at row 1".
	tk.MustGetErrCode("alter table t modify t time", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000): Incorrect time value: 'true' for column 't' at row 1".
	tk.MustGetErrCode("alter table t modify f time", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify i time")
	tk.MustExec("alter table t modify ui time")
	tk.MustExec("alter table t modify f64 time")
	// MySQL will get "ERROR 1292 (22007): Incorrect time value: '"19:35:41"' for column 'str' at row 1".
	tk.MustExec("alter table t modify str time")
	tk.MustExec("alter table t modify nul time")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false 20:08:05 00:11:11 20:08:05 19:35:41 <nil>"))

	// date
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '20200826173501', '20201123', '20200826173501.123456', '\"2020-08-26 17:35:01.123456\"', null)")
	tk.MustGetErrCode("alter table t modify obj date", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify arr date", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify nil date", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify t date", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify f date", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify i date")
	tk.MustExec("alter table t modify ui date")
	tk.MustExec("alter table t modify f64 date")
	// MySQL will get "ERROR 1292 (22007): Incorrect date value: '"2020-08-26 17:35:01.123456"' for column 'str' at row 1".
	tk.MustExec("alter table t modify str date")
	tk.MustExec("alter table t modify nul date")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false 2020-08-26 2020-11-23 2020-08-26 2020-08-26 <nil>"))

	// timestamp
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '20200826173501', '20201123', '20200826173501.123456', '\"2020-08-26 17:35:01.123456\"', null)")
	tk.MustGetErrCode("alter table t modify obj timestamp", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify arr timestamp", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify nil timestamp", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify t timestamp", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify f timestamp", errno.ErrTruncatedWrongValue)
	tk.MustExec("alter table t modify i timestamp")
	tk.MustExec("alter table t modify ui timestamp")
	tk.MustExec("alter table t modify f64 timestamp")
	// MySQL will get "ERROR 1292 (22007): Incorrect timestamptime value: '"2020-08-26 17:35:01.123456"' for column 'str' at row 1".
	tk.MustExec("alter table t modify str timestamp")
	tk.MustExec("alter table t modify nul timestamp")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null true false 2020-08-26 17:35:01 2020-11-23 00:00:00 2020-08-26 17:35:01 2020-08-26 17:35:01 <nil>"))

	// year
	reset(tk)
	tk.MustExec("insert into t values ('{\"obj\": 100}', '[-1, 0, 1]', 'null', 'true', 'false', '2020', '91', '9', '\"2020\"', null)")
	// MySQL will get "ERROR 1366 (HY000): Incorrect integer value: '{"obj": 100}' for column 'obj' at row 1".
	tk.MustGetErrCode("alter table t modify obj year", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000): Incorrect integer value: '[-1, 0, 1]' for column 'arr' at row 11".
	tk.MustGetErrCode("alter table t modify arr year", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000): Incorrect integer value: 'null' for column 'nil' at row 1".
	tk.MustGetErrCode("alter table t modify nil year", errno.ErrTruncatedWrongValue)
	// MySQL will get "ERROR 1366 (HY000): Incorrect integer value: 'true' for column 't' at row 1".
	tk.MustExec("alter table t modify t year")
	// MySQL will get "ERROR 1366 (HY000): Incorrect integer value: 'false' for column 'f' at row 1".
	tk.MustExec("alter table t modify f year")
	tk.MustExec("alter table t modify i year")
	tk.MustExec("alter table t modify ui year")
	tk.MustExec("alter table t modify f64 year")
	// MySQL will get "ERROR 1366 (HY000): Incorrect integer value: '"2020"' for column 'str' at row 1".
	tk.MustExec("alter table t modify str year")
	tk.MustExec("alter table t modify nul year")
	tk.MustQuery("select * from t").Check(testkit.Rows("{\"obj\": 100} [-1, 0, 1] null 2001 0 2020 1991 2009 2020 <nil>"))
}

func TestUpdateDataAfterChangeTimestampToDate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (col timestamp default '1971-06-09' not null, col1 int default 1, unique key(col1));")
	tk.MustExec("alter table t modify column col date not null;")
	tk.MustExec("update t set col = '2002-12-31';")
	// point get
	tk.MustExec("update t set col = '2002-12-31' where col1 = 1;")

	// Make sure the original default value isn't rewritten.
	tk.MustExec("create table t1 (col timestamp default '1971-06-09' not null, col1 int default 1, unique key(col1));")
	tk.MustExec("insert into t1 value('2001-01-01', 1);")
	tk.MustExec("alter table t1 add column col2 timestamp default '2020-06-02' not null;")
	tk.MustExec("alter table t1 modify column col2 date not null;")
	tk.MustExec("update t1 set col = '2002-11-22';")
	// point get
	tk.MustExec("update t1 set col = '2002-12-31' where col1 = 1;")
}

// TestRowFormat is used to close issue #21391, the encoded row in column type change should be aware of the new row format.
func TestRowFormat(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v varchar(10))")
	tk.MustExec("insert into t values (1, \"123\");")
	tk.MustExec("alter table t modify column v varchar(5);")

	tbl := external.GetTableByName(t, tk, "test", "t")
	encodedKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))

	h := helper.NewHelper(store.(helper.Storage))
	data, err := h.GetMvccByEncodedKey(encodedKey)
	require.NoError(t, err)
	// The new format will start with CodecVer = 128 (0x80).
	require.Equal(t, []byte{0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x1, 0x0, 0x4, 0x0, 0x7, 0x0, 0x1, 0x31, 0x32, 0x33, 0x31, 0x32, 0x33}, data.Info.Writes[0].ShortValue)
	tk.MustExec("drop table if exists t")
}

// Close issue #22395
// Background:
// Since the changing column is implemented as adding a new column and substitute the old one when it finished.
// The added column with NOT-NULL option will be fetched with error when it's origin default value is not set.
// It's good because the insert / update logic will cast the related column to changing column rather than use
// origin default value directly.
func TestChangingColOriginDefaultValue(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int not null, unique key(a))")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("insert into t values(2, 2)")

	tbl := external.GetTableByName(t, tk, "test", "t")
	originalHook := dom.DDL().GetHook()
	hook := &ddl.TestDDLCallback{Do: dom}
	var (
		once     bool
		checkErr error
	)
	i := 0
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}
		if (job.SchemaState == model.StateWriteOnly || job.SchemaState == model.StateWriteReorganization) && i < 3 {
			if !once {
				once = true
				tbl := external.GetTableByName(t, tk, "test", "t")
				if len(tbl.WritableCols()) != 3 {
					checkErr = errors.New("assert the writable column number error")
					return
				}
				if tbl.WritableCols()[2].OriginDefaultValue.(string) != "0" {
					checkErr = errors.New("assert the write only column origin default value error")
					return
				}
			}
			// For writable column:
			// Insert/ Update should set the column with the casted-related column value.
			sql := fmt.Sprintf("insert into t values(%d, %d)", i+3, i+3)
			_, err := tk1.Exec(sql)
			if err != nil {
				checkErr = err
				return
			}
			if job.SchemaState == model.StateWriteOnly {
				// The casted value will be inserted into changing column too.
				_, err := tk1.Exec("update t set b = -1 where a = 1")
				if err != nil {
					checkErr = err
					return
				}
			} else {
				// The casted value will be inserted into changing column too.
				_, err := tk1.Exec("update t set b = -2 where a = 2")
				if err != nil {
					checkErr = err
					return
				}
			}
			i++
		}
	}
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t modify column b tinyint NOT NULL")
	dom.DDL().SetHook(originalHook)
	require.NoError(t, checkErr)
	// Since getReorgInfo will stagnate StateWriteReorganization for a ddl round, so insert should exec 3 times.
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1 -1", "2 -2", "3 3", "4 4", "5 5"))
	tk.MustExec("drop table if exists t")
}

func TestChangingColOriginDefaultValueAfterAddColAndCastSucc(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("set time_zone = 'UTC'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int not null, unique key(a))")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("insert into t values(2, 2)")
	tk.MustExec("alter table t add column c timestamp default '1971-06-09' not null")

	tbl := external.GetTableByName(t, tk, "test", "t")
	originalHook := dom.DDL().GetHook()
	hook := &ddl.TestDDLCallback{Do: dom}
	var (
		once     bool
		checkErr error
	)
	i, stableTimes := 0, 0
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}
		if (job.SchemaState == model.StateWriteOnly || job.SchemaState == model.StateWriteReorganization) && stableTimes < 3 {
			if !once {
				once = true
				tbl := external.GetTableByName(t, tk, "test", "t")
				if len(tbl.WritableCols()) != 4 {
					checkErr = errors.New("assert the writable column number error")
					return
				}
				originalDV := fmt.Sprintf("%v", tbl.WritableCols()[3].OriginDefaultValue)
				expectVal := "1971-06-09"
				if originalDV != expectVal {
					errMsg := fmt.Sprintf("expect: %v, got: %v", expectVal, originalDV)
					checkErr = errors.New("assert the write only column origin default value error" + errMsg)
					return
				}
			}
			// For writable column:
			// Insert / Update should set the column with the casted-related column value.
			sql := fmt.Sprintf("insert into t values(%d, %d, '2021-06-06 12:13:14')", i+3, i+3)
			_, err := tk1.Exec(sql)
			if err != nil {
				checkErr = err
				return
			}
			if job.SchemaState == model.StateWriteOnly {
				// The casted value will be inserted into changing column too.
				// for point get
				_, err := tk1.Exec("update t set b = -1 where a = 1")
				if err != nil {
					checkErr = err
					return
				}
			} else {
				// The casted value will be inserted into changing column too.
				// for point get
				_, err := tk1.Exec("update t set b = -2 where a = 2")
				if err != nil {
					checkErr = err
					return
				}
			}
			stableTimes++
		}
		i++
	}

	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t modify column c date NOT NULL")
	dom.DDL().SetHook(originalHook)
	require.NoError(t, checkErr)
	// Since getReorgInfo will stagnate StateWriteReorganization for a ddl round, so insert should exec 3 times.
	tk.MustQuery("select * from t order by a").Check(
		testkit.Rows("1 -1 1971-06-09", "2 -2 1971-06-09", "5 5 2021-06-06", "6 6 2021-06-06", "7 7 2021-06-06"))
	tk.MustExec("drop table if exists t")
}

// TestChangingColOriginDefaultValueAfterAddColAndCastFail tests #25383.
func TestChangingColOriginDefaultValueAfterAddColAndCastFail(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("set time_zone = 'UTC'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a VARCHAR(31) NULL DEFAULT 'wwrzfwzb01j6ddj', b DECIMAL(12,0) NULL DEFAULT '-729850476163')")
	tk.MustExec("ALTER TABLE t ADD COLUMN x CHAR(218) NULL DEFAULT 'lkittuae'")

	tbl := external.GetTableByName(t, tk, "test", "t")
	originalHook := dom.DDL().GetHook()
	hook := &ddl.TestDDLCallback{Do: dom}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}

		if job.SchemaState == model.StateWriteOnly || job.SchemaState == model.StateWriteReorganization {
			tbl := external.GetTableByName(t, tk, "test", "t")
			if len(tbl.WritableCols()) != 4 {
				errMsg := fmt.Sprintf("cols len:%v", len(tbl.WritableCols()))
				checkErr = errors.New("assert the writable column number error" + errMsg)
				return
			}
			originalDV := fmt.Sprintf("%v", tbl.WritableCols()[3].OriginDefaultValue)
			expectVal := "0000-00-00 00:00:00"
			if originalDV != expectVal {
				errMsg := fmt.Sprintf("expect: %v, got: %v", expectVal, originalDV)
				checkErr = errors.New("assert the write only column origin default value error" + errMsg)
				return
			}
			// The casted value will be inserted into changing column too.
			_, err := tk1.Exec("UPDATE t SET a = '18apf' WHERE x = '' AND a = 'mul'")
			if err != nil {
				checkErr = err
				return
			}
		}
	}

	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t modify column x DATETIME NULL DEFAULT '3771-02-28 13:00:11' AFTER b;")
	dom.DDL().SetHook(originalHook)
	require.NoError(t, checkErr)
	tk.MustQuery("select * from t order by a").Check(testkit.Rows())
	tk.MustExec("drop table if exists t")
}

// Close issue #22820
func TestChangingAttributeOfColumnWithFK(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	prepare := func() {
		tk.MustExec("drop table if exists users")
		tk.MustExec("drop table if exists orders")
		tk.MustExec("CREATE TABLE users (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, doc JSON);")
		tk.MustExec("CREATE TABLE orders (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, user_id INT NOT NULL, doc JSON, FOREIGN KEY fk_user_id (user_id) REFERENCES users(id));")
	}

	prepare()
	// For column with FK, alter action can be performed for changing null/not null, default value, comment and so on, but column type.
	tk.MustExec("alter table orders modify user_id int null;")
	tbl := external.GetTableByName(t, tk, "test", "orders")
	require.Equal(t, false, mysql.HasNotNullFlag(tbl.Meta().Columns[1].GetFlag()))

	prepare()
	tk.MustExec("alter table orders change user_id user_id2 int null")
	tbl = external.GetTableByName(t, tk, "test", "orders")
	require.Equal(t, "user_id2", tbl.Meta().Columns[1].Name.L)
	require.Equal(t, false, mysql.HasNotNullFlag(tbl.Meta().Columns[1].GetFlag()))

	prepare()
	tk.MustExec("alter table orders modify user_id int default -1 comment \"haha\"")
	tbl = external.GetTableByName(t, tk, "test", "orders")
	require.Equal(t, "haha", tbl.Meta().Columns[1].Comment)
	require.Equal(t, "-1", tbl.Meta().Columns[1].DefaultValue.(string))

	prepare()
	tk.MustGetErrCode("alter table orders modify user_id bigint", errno.ErrFKIncompatibleColumns)

	tk.MustExec("drop table if exists orders, users")
}

func TestAlterPrimaryKeyToNull(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int not null, b int not null, primary key(a, b));")
	tk.MustGetErrCode("alter table t modify a bigint null;", errno.ErrPrimaryCantHaveNull)
	tk.MustGetErrCode("alter table t change column a a bigint null;", errno.ErrPrimaryCantHaveNull)
	tk.MustExec("create table t1(a int not null, b int not null, primary key(a));")
	tk.MustGetErrCode("alter table t modify a bigint null;", errno.ErrPrimaryCantHaveNull)
	tk.MustGetErrCode("alter table t change column a a bigint null;", errno.ErrPrimaryCantHaveNull)
}

// Close https://github.com/pingcap/tidb/issues/24839.
func TestChangeUnsignedIntToDatetime(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int(10) unsigned default null, b bigint unsigned, c tinyint unsigned);")
	tk.MustExec("insert into t values (1, 1, 1);")
	tk.MustGetErrCode("alter table t modify column a datetime;", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify column b datetime;", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify column c datetime;", errno.ErrTruncatedWrongValue)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int(10) unsigned default null, b bigint unsigned, c tinyint unsigned);")
	tk.MustExec("insert into t values (4294967295, 18446744073709551615, 255);")
	tk.MustGetErrCode("alter table t modify column a datetime;", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify column b datetime;", errno.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify column c datetime;", errno.ErrTruncatedWrongValue)
}

// Close issue #23202
func TestDDLExitWhenCancelMeetPanic(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	tk.MustExec("alter table t add index(b)")
	tk.MustExec("set @@global.tidb_ddl_error_count_limit=3")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit"))
	}()

	hook := &ddl.TestDDLCallback{Do: dom}
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if jobID != 0 {
			return
		}
		if job.Type == model.ActionDropIndex {
			jobID = job.ID
		}
	}
	dom.DDL().SetHook(hook)

	// when it panics in write-reorg state, the job will be pulled up as a cancelling job. Since drop-index with
	// write-reorg can't be cancelled, so it will be converted to running state and try again (dead loop).
	err := tk.ExecToErr("alter table t drop index b")
	require.EqualError(t, err, "[ddl:-1]panic in handling DDL logic and error count beyond the limitation 3, cancelled")
	require.Less(t, int64(0), jobID)

	// Verification of the history job state.
	var job *model.Job
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err1 error
		job, err1 = m.GetHistoryDDLJob(jobID)
		return errors2.Trace(err1)
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), job.ErrorCount)
	require.Equal(t, "[ddl:-1]panic in handling DDL logic and error count beyond the limitation 3, cancelled", job.Error.Error())
}

// Close issue #24253
func TestChangeIntToBitWillPanicInBackfillIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"  `a` int(11) DEFAULT NULL," +
		"  `b` varchar(10) DEFAULT NULL," +
		"  `c` decimal(10,2) DEFAULT NULL," +
		"  KEY `idx1` (`a`)," +
		"  UNIQUE KEY `idx2` (`a`)," +
		"  KEY `idx3` (`a`,`b`)," +
		"  KEY `idx4` (`a`,`b`,`c`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("insert into t values(19,1,1),(17,2,2)")
	tk.MustExec("alter table t modify a bit(5) not null")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` bit(5) NOT NULL,\n" +
		"  `b` varchar(10) DEFAULT NULL,\n" +
		"  `c` decimal(10,2) DEFAULT NULL,\n" +
		"  KEY `idx1` (`a`),\n" +
		"  UNIQUE KEY `idx2` (`a`),\n" +
		"  KEY `idx3` (`a`,`b`),\n" +
		"  KEY `idx4` (`a`,`b`,`c`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t").Check(testkit.Rows("\x13 1 1.00", "\x11 2 2.00"))
}

// Close issue #24584
func TestCancelCTCInReorgStateWillCauseGoroutineLeak(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockInfiniteReorgLogic", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockInfiniteReorgLogic"))
	}()

	// set ddl hook
	originalHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originalHook)

	tk.MustExec("drop table if exists ctc_goroutine_leak")
	tk.MustExec("create table ctc_goroutine_leak (a int)")
	tk.MustExec("insert into ctc_goroutine_leak values(1),(2),(3)")
	tbl := external.GetTableByName(t, tk, "test", "ctc_goroutine_leak")

	hook := &ddl.TestDDLCallback{Do: dom}
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if jobID != 0 {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}
		if job.Query == "alter table ctc_goroutine_leak modify column a tinyint" {
			jobID = job.ID
		}
	}
	dom.DDL().SetHook(hook)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	var (
		wg       util.WaitGroupWrapper
		alterErr error
	)
	wg.Run(func() {
		// This ddl will be hang over in the failpoint loop, waiting for outside cancel.
		_, alterErr = tk1.Exec("alter table ctc_goroutine_leak modify column a tinyint")
	})

	<-ddl.TestReorgGoroutineRunning
	tk.MustExec("admin cancel ddl jobs " + strconv.Itoa(int(jobID)))
	wg.Wait()
	require.Equal(t, "[ddl:8214]Cancelled DDL job", alterErr.Error())
}

// Close issue #24971, #24973, #24974
func TestCTCShouldCastTheDefaultValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("alter table t add column b bit(51) default 1512687856625472")                 // virtual fill the column data
	tk.MustGetErrCode("alter table t modify column b decimal(30,18)", errno.ErrDataOutOfRange) // because 1512687856625472 is out of range.

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table tbl_1 (col int)")
	tk.MustExec("insert into tbl_1 values (9790)")
	tk.MustExec("alter table tbl_1 add column col1 blob(6) collate binary not null")
	tk.MustQuery("select col1 from tbl_1").Check(testkit.Rows(""))
	tk.MustGetErrCode("alter table tbl_1 change column col1 col2 int", errno.ErrTruncatedWrongValue)
	tk.MustQuery("select col1 from tbl_1").Check(testkit.Rows(""))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table tbl(col_214 decimal(30,8))")
	tk.MustExec("replace into tbl values (89687.448)")
	tk.MustExec("alter table tbl add column col_279 binary(197) collate binary default 'RAWTdm' not null")
	tk.MustQuery("select col_279 from tbl").Check(testkit.Rows("RAWTdm\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
	tk.MustGetErrCode("alter table tbl change column col_279 col_287 int", errno.ErrTruncatedWrongValue)
	tk.MustQuery("select col_279 from tbl").Check(testkit.Rows("RAWTdm\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
}

// Close issue #25037
// 1: for default value of binary of create-table, it should append the \0 as the suffix to meet flen.
// 2: when cast the bit to binary, we should consider to convert it to uint then cast uint to string, rather than taking the bit to string directly.
func TestCTCCastBitToBinary(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// For point 1:
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(10) default 't')")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` binary(10) DEFAULT 't\\0\\0\\0\\0\\0\\0\\0\\0\\0'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// For point 2 with binary:
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bit(13) not null) collate utf8mb4_general_ci")
	tk.MustExec("insert into t values ( 4047 )")
	tk.MustExec("alter table t change column a a binary(248) collate binary default 't'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` binary(248) DEFAULT 't\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
	tk.MustQuery("select * from t").Check(testkit.Rows("4047\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))

	// For point 2 with varbinary:
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bit(13) not null) collate utf8mb4_general_ci")
	tk.MustExec("insert into t values ( 4047 )")
	tk.MustExec("alter table t change column a a varbinary(248) collate binary default 't'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` varbinary(248) DEFAULT 't'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
	tk.MustQuery("select * from t").Check(testkit.Rows("4047"))
}

func TestChangePrefixedIndexColumnToNonPrefixOne(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a text, unique index idx(a(2)));")
	tk.MustExec("alter table t modify column a int;")
	showCreateTable := tk.MustQuery("show create table t").Rows()[0][1].(string)
	require.Containsf(t, showCreateTable, "UNIQUE KEY `idx` (`a`)", showCreateTable)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a char(255), unique index idx(a(2)));")
	tk.MustExec("alter table t modify column a float;")
	showCreateTable = tk.MustQuery("show create table t").Rows()[0][1].(string)
	require.Containsf(t, showCreateTable, "UNIQUE KEY `idx` (`a`)", showCreateTable)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a char(255), b text, unique index idx(a(2), b(10)));")
	tk.MustExec("alter table t modify column b int;")
	showCreateTable = tk.MustQuery("show create table t").Rows()[0][1].(string)
	require.Containsf(t, showCreateTable, "UNIQUE KEY `idx` (`a`(2),`b`)", showCreateTable)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a char(250), unique key idx(a(10)));")
	tk.MustExec("alter table t modify a char(9);")
	showCreateTable = tk.MustQuery("show create table t").Rows()[0][1].(string)
	require.Containsf(t, showCreateTable, "UNIQUE KEY `idx` (`a`)", showCreateTable)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a varchar(700), key(a(700)));")
	tk.MustGetErrCode("alter table t change column a a tinytext;", errno.ErrBlobKeyWithoutLength)
}

// Fix issue https://github.com/pingcap/tidb/issues/25469
func TestCastToTimeStampDecodeError(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"  `a` datetime DEFAULT '1764-06-11 02:46:14'" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin COMMENT='7b84832e-f857-4116-8872-82fc9dcc4ab3'")
	tk.MustExec("insert into `t` values();")
	tk.MustGetErrCode("alter table `t` change column `a` `b` TIMESTAMP NULL DEFAULT '2015-11-14 07:12:24';", errno.ErrTruncatedWrongValue)

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"  `a` date DEFAULT '1764-06-11 02:46:14'" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin COMMENT='7b84832e-f857-4116-8872-82fc9dcc4ab3'")
	tk.MustExec("insert into `t` values();")
	tk.MustGetErrCode("alter table `t` change column `a` `b` TIMESTAMP NULL DEFAULT '2015-11-14 07:12:24';", errno.ErrTruncatedWrongValue)
	tk.MustExec("drop table if exists t")

	// Normal cast datetime to timestamp can succeed.
	tk.MustQuery("select timestamp(cast('1000-11-11 12-3-1' as date));").Check(testkit.Rows("1000-11-11 00:00:00"))
}

// https://github.com/pingcap/tidb/issues/25285.
func TestCastFromZeroIntToTimeError(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	prepare := func() {
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int);")
		tk.MustExec("insert into t values (0);")
	}
	const errCodeNone = -1
	testCases := []struct {
		sqlMode string
		errCode int
	}{
		{"STRICT_TRANS_TABLES", errno.ErrTruncatedWrongValue},
		{"STRICT_ALL_TABLES", errno.ErrTruncatedWrongValue},
		{"NO_ZERO_IN_DATE", errCodeNone},
		{"NO_ZERO_DATE", errCodeNone},
		{"ALLOW_INVALID_DATES", errCodeNone},
		{"", errCodeNone},
	}
	for _, tc := range testCases {
		prepare()
		tk.MustExec(fmt.Sprintf("set @@sql_mode = '%s';", tc.sqlMode))
		if tc.sqlMode == "NO_ZERO_DATE" {
			tk.MustQuery(`select date(0);`).Check(testkit.Rows("<nil>"))
		} else {
			tk.MustQuery(`select date(0);`).Check(testkit.Rows("0000-00-00"))
		}
		tk.MustQuery(`select time(0);`).Check(testkit.Rows("00:00:00"))
		if tc.errCode == errCodeNone {
			tk.MustExec("alter table t modify column a date;")
			prepare()
			tk.MustExec("alter table t modify column a datetime;")
			prepare()
			tk.MustExec("alter table t modify column a timestamp;")
		} else {
			tk.MustGetErrCode("alter table t modify column a date;", errno.ErrTruncatedWrongValue)
			tk.MustGetErrCode("alter table t modify column a datetime;", errno.ErrTruncatedWrongValue)
			tk.MustGetErrCode("alter table t modify column a timestamp;", errno.ErrTruncatedWrongValue)
		}
	}
	tk.MustExec("drop table if exists t;")
}

func TestChangeFromTimeToYear(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a time default 0);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("insert into t values (NULL);")
	tk.MustExec("insert into t values ('12');")
	tk.MustExec("insert into t values ('00:19:59');")
	tk.MustExec("insert into t values ('00:20:13');")
	tk.MustExec("alter table t modify column a year;")
	tk.MustQuery("select a from t;").Check(testkit.Rows("0", "<nil>", "2012", "1959", "2013"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id bigint primary key, a time);")
	tk.MustExec("replace into t values (1, '10:10:10');")
	tk.MustGetErrCode("alter table t modify column a year;", errno.ErrWarnDataOutOfRange)
	tk.MustExec("replace into t values (1, '12:13:14');")
	tk.MustGetErrCode("alter table t modify column a year;", errno.ErrWarnDataOutOfRange)
	tk.MustExec("set @@sql_mode = '';")
	tk.MustExec("alter table t modify column a year;")
	tk.MustQuery("show warnings").Check(
		testkit.Rows("Warning 1264 Out of range value for column 'a', the value is '12:13:14'"))
}

// Fix issue: https://github.com/pingcap/tidb/issues/26292
// Cast date to timestamp has two kind behavior: cast("3977-02-22" as date)
// For select statement, it truncates the string and return no errors. (which is 3977-02-22 00:00:00 here)
// For ddl reorging or changing column in ctc, it needs report some errors.
func TestCastDateToTimestampInReorgAttribute(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 600*time.Millisecond)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("CREATE TABLE `t` (`a` DATE NULL DEFAULT '8497-01-06')")
	tk.MustExec("insert into t values(now())")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Len(t, tbl.Cols(), 1)
	var checkErr1 error
	var checkErr2 error

	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr1 != nil || checkErr2 != nil || tbl.Meta().ID != job.TableID {
			return
		}
		switch job.SchemaState {
		case model.StateWriteOnly:
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			checkErr1 = tk.ExecToErr("insert into `t` set  `a` = '3977-02-22'") // this(string) will be cast to a as date, then cast a(date) as timestamp to changing column.
			checkErr2 = tk.ExecToErr("update t set `a` = '3977-02-22'")
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table t modify column a  TIMESTAMP NULL DEFAULT '2021-04-28 03:35:11' FIRST")
	require.EqualError(t, checkErr1, "[types:1292]Incorrect timestamp value: '3977-02-22'")
	require.EqualError(t, checkErr2, "[types:1292]Incorrect timestamp value: '3977-02-22'")
}

// https://github.com/pingcap/tidb/issues/25282.
func TestChangeFromUnsignedIntToTime(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a mediumint unsigned);")
	tk.MustExec("insert into t values (180857);")
	tk.MustExec("alter table t modify column a time;")
	tk.MustQuery("select a from t;").Check(testkit.Rows("18:08:57"))
	tk.MustExec("drop table if exists t;")
}

// See https://github.com/pingcap/tidb/issues/25287.
// Revised according to https://github.com/pingcap/tidb/pull/31031#issuecomment-1001404832.
func TestChangeFromBitToStringInvalidUtf8ErrMsg(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bit(45));")
	tk.MustExec("insert into t values (1174717);")
	tk.MustExec("alter table t modify column a varchar(31) collate utf8mb4_general_ci;")
	tk.MustQuery("select a from t;").Check(testkit.Rows("1174717"))
}

func TestForIssue24621(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(250));")
	tk.MustExec("insert into t values('0123456789abc');")
	errMsg := "[types:1265]Data truncated for column 'a', value is '0123456789abc'"
	tk.MustGetErrMsg("alter table t modify a char(12) null;", errMsg)
}

func TestChangeNullValueFromOtherTypeToTimestamp(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Some ddl cases.
	prepare := func() {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int null)")
		tk.MustExec("insert into t values()")
		tk.MustQuery("select * from t").Check(testkit.Rows("<nil>"))
	}

	prepare()
	tk.MustExec("alter table t modify column a timestamp NOT NULL")
	tk.MustQuery("select count(*) from t where a = null").Check(testkit.Rows("0"))

	prepare()
	// only from other type NULL to timestamp type NOT NULL, it should be successful.
	_, err := tk.Exec("alter table t change column a a1 time NOT NULL")
	require.Error(t, err)
	require.Equal(t, "[ddl:1265]Data truncated for column 'a1' at row 1", err.Error())

	prepare2 := func() {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a timestamp null)")
		tk.MustExec("insert into t values()")
		tk.MustQuery("select * from t").Check(testkit.Rows("<nil>"))
	}

	prepare2()
	// only from other type NULL to timestamp type NOT NULL, it should be successful. (timestamp to timestamp excluded)
	_, err = tk.Exec("alter table t modify column a timestamp NOT NULL")
	require.Error(t, err)
	require.Equal(t, "[ddl:1265]Data truncated for column 'a' at row 1", err.Error())

	// Some dml cases.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a timestamp NOT NULL)")
	_, err = tk.Exec("insert into t values()")
	require.Error(t, err)
	require.Equal(t, "[table:1364]Field 'a' doesn't have a default value", err.Error())

	_, err = tk.Exec("insert into t values(null)")
	require.Equal(t, "[table:1048]Column 'a' cannot be null", err.Error())
}

func TestColumnTypeChangeBetweenFloatAndDouble(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// issue #31372
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	prepare := func(createTableStmt string) {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(createTableStmt)
		tk.MustExec("insert into t values (36.4), (24.1);")
	}

	prepare("create table t (a float(6,2));")
	tk.MustExec("alter table t modify a double(6,2)")
	tk.MustQuery("select a from t;").Check(testkit.Rows("36.4", "24.1"))

	prepare("create table t (a double(6,2));")
	tk.MustExec("alter table t modify a double(6,1)")
	tk.MustQuery("select a from t;").Check(testkit.Rows("36.4", "24.1"))
	tk.MustExec("alter table t modify a float(6,1)")
	tk.MustQuery("select a from t;").Check(testkit.Rows("36.4", "24.1"))
}

func TestColumnTypeChangeTimestampToInt(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// 1. modify a timestamp column to bigint
	// 2. modify the bigint column to timestamp
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key auto_increment, c1 timestamp default '2020-07-10 01:05:08');")
	tk.MustExec("insert into t values();")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	tk.MustExec("alter table t modify column c1 bigint;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 20200710010508"))
	tk.MustExec("alter table t modify c1 timestamp")
	tk.MustExec("set @@session.time_zone=UTC")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-09 17:05:08"))

	// 1. modify a timestamp column to bigint
	// 2. add the index
	// 3. modify the bigint column to timestamp
	// The current session.time_zone is '+00:00'.
	tk.MustExec(`set time_zone = '+00:00'`)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key auto_increment, c1 timestamp default '2020-07-10 01:05:08', index idx(c1));")
	tk.MustExec("insert into t values();")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	tk.MustExec("alter table t modify column c1 bigint;")
	tk.MustExec("alter table t add index idx1(id, c1);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 20200710010508"))
	tk.MustExec("admin check table t")
	// change timezone
	tk.MustExec("set @@session.time_zone='+5:00'")
	tk.MustExec("alter table t modify c1 timestamp")
	// change timezone
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	tk.MustExec("set @@session.time_zone='-8:00'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-09 12:05:08"))
	tk.MustExec("admin check table t")
	// test the timezone of "default" and "system"
	// The current session.time_zone is '-8:00'.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key auto_increment, c1 timestamp default '2020-07-10 01:05:08', index idx(c1));")
	tk.MustExec("insert into t values();")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	tk.MustExec("alter table t modify column c1 bigint;")
	tk.MustExec("alter table t add index idx1(id, c1);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 20200710010508"))
	tk.MustExec("admin check table t")
	// change timezone
	tk.MustExec("set @@session.time_zone= default")
	tk.MustExec("alter table t modify c1 timestamp")
	// change timezone
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	tk.MustExec("set @@session.time_zone='SYSTEM'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-07-10 01:05:08"))
	tk.MustExec("admin check table t")

	// tests DST
	// 1. modify a timestamp column to bigint
	// 2. modify the bigint column to timestamp
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.time_zone=UTC")
	tk.MustExec("create table t(id int primary key auto_increment, c1 timestamp default '1990-04-15 18:00:00');")
	tk.MustExec("insert into t values();")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1990-04-15 18:00:00"))
	tk.MustExec("set @@session.time_zone='Asia/Shanghai'")
	tk.MustExec("alter table t modify column c1 bigint;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 19900416030000"))
	tk.MustExec("alter table t modify c1 timestamp default '1990-04-15 18:00:00'")
	tk.MustExec("set @@session.time_zone=UTC")
	tk.MustExec("insert into t values();")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1990-04-15 18:00:00", "2 1990-04-15 09:00:00"))
	// 1. modify a timestamp column to bigint
	// 2. add the index
	// 3. modify the bigint column to timestamp
	// The current session.time_zone is '+00:00'.
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.time_zone='-8:00'")
	tk.MustExec("create table t(id int primary key auto_increment, c1 timestamp default '2016-03-13 02:30:00', index idx(c1));")
	tk.MustExec("insert into t values();")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2016-03-13 02:30:00"))
	tk.MustExec("set @@session.time_zone='America/Los_Angeles'")
	tk.MustExec("alter table t modify column c1 bigint;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 20160313033000"))
	tk.MustExec("alter table t add index idx1(id, c1);")
	tk.MustExec("admin check table t")
}
