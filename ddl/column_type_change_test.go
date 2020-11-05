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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"errors"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	parser_mysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testColumnTypeChangeSuite{})

type testColumnTypeChangeSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo
	dom    *domain.Domain
}

func (s *testColumnTypeChangeSuite) SetUpSuite(c *C) {
	var err error
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	s.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testColumnTypeChangeSuite) TearDownSuite(c *C) {
	s.dom.Close()
	c.Assert(s.store.Close(), IsNil)
}

func (s *testColumnTypeChangeSuite) TestColumnTypeChangeBetweenInteger(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Enable column change variable.
	tk.Se.GetSessionVars().EnableChangeColumnType = true
	defer func() {
		tk.Se.GetSessionVars().EnableChangeColumnType = false
	}()

	// Modify column from null to not null.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int null, b int null)")
	tk.MustExec("alter table t modify column b int not null")

	tk.MustExec("insert into t(a, b) values (null, 1)")
	// Modify column from null to not null in same type will cause ErrInvalidUseOfNull
	tk.MustGetErrCode("alter table t modify column a int not null", mysql.ErrInvalidUseOfNull)

	// Modify column from null to not null in different type will cause WarnDataTruncated.
	tk.MustGetErrCode("alter table t modify column a tinyint not null", mysql.WarnDataTruncated)
	tk.MustGetErrCode("alter table t modify column a bigint not null", mysql.WarnDataTruncated)

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
	tk.MustGetErrCode("alter table t modify column a int", mysql.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify column a mediumint", mysql.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify column a smallint", mysql.ErrDataOutOfRange)
	tk.MustGetErrCode("alter table t modify column a tinyint", mysql.ErrDataOutOfRange)
	_, err := tk.Exec("admin check table t")
	c.Assert(err, IsNil)
}

func (s *testColumnTypeChangeSuite) TestColumnTypeChangeStateBetweenInteger(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert into t(c1, c2) values (1, 1)")
	// Enable column change variable.
	tk.Se.GetSessionVars().EnableChangeColumnType = true
	defer func() {
		tk.Se.GetSessionVars().EnableChangeColumnType = false
	}()

	// use new session to check meta in callback function.
	internalTK := testkit.NewTestKit(c, s.store)
	internalTK.MustExec("use test")

	tbl := testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.Cols()), Equals, 2)
	c.Assert(getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c2", false), NotNil)

	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)

	hook := &ddl.TestDDLCallback{}
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
			tbl = testGetTableByName(c, internalTK.Se, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			} else if len(tbl.Cols()) != 2 {
				checkErr = errors.New("len(cols) is not right")
			}
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			tbl = testGetTableByName(c, internalTK.Se, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			} else if len(tbl.(*tables.TableCommon).Columns) != 3 {
				// changingCols has been added into meta.
				checkErr = errors.New("len(cols) is not right")
			} else if getModifyColumn(c, internalTK.Se.(sessionctx.Context), "test", "t", "c2", true).Flag&parser_mysql.PreventNullInsertFlag == uint(0) {
				checkErr = errors.New("old col's flag is not right")
			} else if getModifyColumn(c, internalTK.Se.(sessionctx.Context), "test", "t", "_Col$_c2_0", true) == nil {
				checkErr = errors.New("changingCol is nil")
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	// Alter sql will modify column c2 to tinyint not null.
	SQL := "alter table t modify column c2 tinyint not null"
	tk.MustExec(SQL)
	// Assert the checkErr in the job of every state.
	c.Assert(checkErr, IsNil)

	// Check the col meta after the column type change.
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.Cols()), Equals, 2)
	col := getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c2", false)
	c.Assert(col, NotNil)
	c.Assert(parser_mysql.HasNotNullFlag(col.Flag), Equals, true)
	c.Assert(col.Flag&parser_mysql.NoDefaultValueFlag, Not(Equals), uint(0))
	c.Assert(col.Tp, Equals, parser_mysql.TypeTiny)
	c.Assert(col.ChangeStateInfo, IsNil)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

func (s *testColumnTypeChangeSuite) TestRollbackColumnTypeChangeBetweenInteger(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bigint, c2 bigint)")
	tk.MustExec("insert into t(c1, c2) values (1, 1)")
	// Enable column change variable.
	tk.Se.GetSessionVars().EnableChangeColumnType = true
	defer func() {
		tk.Se.GetSessionVars().EnableChangeColumnType = false
	}()

	tbl := testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.Cols()), Equals, 2)
	c.Assert(getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c2", false), NotNil)

	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)

	hook := &ddl.TestDDLCallback{}
	// Mock roll back at model.StateNone.
	customizeHookRollbackAtState(hook, tbl, model.StateNone)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	// Alter sql will modify column c2 to bigint not null.
	SQL := "alter table t modify column c2 int not null"
	_, err := tk.Exec(SQL)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1]MockRollingBackInCallBack-none")
	assertRollBackedColUnchanged(c, tk)

	// Mock roll back at model.StateDeleteOnly.
	customizeHookRollbackAtState(hook, tbl, model.StateDeleteOnly)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	_, err = tk.Exec(SQL)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1]MockRollingBackInCallBack-delete only")
	assertRollBackedColUnchanged(c, tk)

	// Mock roll back at model.StateWriteOnly.
	customizeHookRollbackAtState(hook, tbl, model.StateWriteOnly)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	_, err = tk.Exec(SQL)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1]MockRollingBackInCallBack-write only")
	assertRollBackedColUnchanged(c, tk)

	// Mock roll back at model.StateWriteReorg.
	customizeHookRollbackAtState(hook, tbl, model.StateWriteReorganization)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	_, err = tk.Exec(SQL)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1]MockRollingBackInCallBack-write reorganization")
	assertRollBackedColUnchanged(c, tk)
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

func assertRollBackedColUnchanged(c *C, tk *testkit.TestKit) {
	tbl := testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.Cols()), Equals, 2)
	col := getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c2", false)
	c.Assert(col, NotNil)
	c.Assert(col.Flag, Equals, uint(0))
	c.Assert(col.Tp, Equals, parser_mysql.TypeLonglong)
	c.Assert(col.ChangeStateInfo, IsNil)
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

func (s *testColumnTypeChangeSuite) TestColumnTypeChangeFromIntegerToOthers(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Enable column change variable.
	tk.Se.GetSessionVars().EnableChangeColumnType = true
	defer func() {
		tk.Se.GetSessionVars().EnableChangeColumnType = false
	}()

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
	modifiedColumn := getModifyColumn(c, tk.Se, "test", "t", "a", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeVarchar)
	tk.MustQuery("select a from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify b char(10)")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "b", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeString)
	tk.MustQuery("select b from t").Check(testkit.Rows("11"))

	tk.MustExec("alter table t modify c binary(10)")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "c", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeString)
	tk.MustQuery("select c from t").Check(testkit.Rows("111\x00\x00\x00\x00\x00\x00\x00"))

	tk.MustExec("alter table t modify d varbinary(10)")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "d", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeVarchar)
	tk.MustQuery("select d from t").Check(testkit.Rows("1111"))

	tk.MustExec("alter table t modify e blob(10)")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "e", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeBlob)
	tk.MustQuery("select e from t").Check(testkit.Rows("11111"))

	tk.MustExec("alter table t modify f text(10)")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "f", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeBlob)
	tk.MustQuery("select f from t").Check(testkit.Rows("111111"))

	// integer to decimal
	prepare(tk)
	tk.MustExec("alter table t modify a decimal(2,1)")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "a", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeNewDecimal)
	tk.MustQuery("select a from t").Check(testkit.Rows("1.0"))

	// integer to year
	// For year(2), MySQL converts values in the ranges '0' to '69' and '70' to '99' to YEAR values in the ranges 2000 to 2069 and 1970 to 1999.
	tk.MustExec("alter table t modify b year")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "b", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeYear)
	tk.MustQuery("select b from t").Check(testkit.Rows("2011"))

	// integer to time
	tk.MustExec("alter table t modify c time")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "c", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeDuration) // mysql.TypeTime has rename to TypeDuration.
	tk.MustQuery("select c from t").Check(testkit.Rows("00:01:11"))

	// integer to date (mysql will throw `Incorrect date value: '1111' for column 'd' at row 1` error)
	tk.MustExec("alter table t modify d date")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "d", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeDate)
	tk.MustQuery("select d from t").Check(testkit.Rows("2000-11-11")) // the given number will be left-forward used.

	// integer to timestamp (according to what timezone you have set)
	tk.MustExec("alter table t modify e timestamp")
	tk.MustExec("set @@session.time_zone=UTC")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "e", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeTimestamp)
	tk.MustQuery("select e from t").Check(testkit.Rows("2001-11-11 00:00:00")) // the given number will be left-forward used.

	// integer to datetime
	tk.MustExec("alter table t modify f datetime")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "f", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeDatetime)
	tk.MustQuery("select f from t").Check(testkit.Rows("2011-11-11 00:00:00")) // the given number will be left-forward used.

	// integer to floating-point values
	prepare(tk)
	tk.MustExec("alter table t modify a float")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "a", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeFloat)
	tk.MustQuery("select a from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify b double")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "b", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeDouble)
	tk.MustQuery("select b from t").Check(testkit.Rows("11"))

	// integer to bit
	tk.MustExec("alter table t modify c bit(10)")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "c", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeBit)
	// 111 will be stored ad 0x00,0110,1111 = 0x6F, which will be shown as ASCII('o')=111 as well.
	tk.MustQuery("select c from t").Check(testkit.Rows("\x00o"))

	// integer to json
	tk.MustExec("alter table t modify d json")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "d", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeJSON)
	tk.MustQuery("select d from t").Check(testkit.Rows("1111"))

	// integer to enum
	prepareForEnumSet(tk)
	// TiDB take integer as the enum element offset to cast.
	tk.MustExec("alter table t modify a enum(\"a\", \"b\")")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "a", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeEnum)
	tk.MustQuery("select a from t").Check(testkit.Rows("a"))

	// TiDB take integer as the set element offset to cast.
	tk.MustExec("alter table t modify b set(\"a\", \"b\")")
	modifiedColumn = getModifyColumn(c, tk.Se, "test", "t", "b", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeSet)
	tk.MustQuery("select b from t").Check(testkit.Rows("a"))

	// TiDB can't take integer as the enum element string to cast, while the MySQL can.
	tk.MustGetErrCode("alter table t modify d enum(\"11111\", \"22222\")", mysql.WarnDataTruncated)

	// TiDB can't take integer as the set element string to cast, while the MySQL can.
	tk.MustGetErrCode("alter table t modify e set(\"11111\", \"22222\")", mysql.WarnDataTruncated)
}

func (s *testColumnTypeChangeSuite) TestColumnTypeChangeFromStringToOthers(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Enable column change variable.
	tk.Se.GetSessionVars().EnableChangeColumnType = true

	// Set time zone to UTC.
	originalTz := tk.Se.GetSessionVars().TimeZone
	tk.Se.GetSessionVars().TimeZone = time.UTC
	defer func() {
		tk.Se.GetSessionVars().EnableChangeColumnType = false
		tk.Se.GetSessionVars().TimeZone = originalTz
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
	tk.MustGetErrCode("alter table t modify c bit", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify vc bit", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify bny bit", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify vbny bit", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify bb bit", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify txt bit", mysql.ErrUnsupportedDDLOperation)
	tk.MustExec("alter table t modify e bit")
	tk.MustExec("alter table t modify s bit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1\x00\x00\x00\x00\x00\x00\x00 1 1 1 \x01 \x01"))
	// decimal
	reset(tk)
	tk.MustExec("insert into t values ('123.45', '123.45', '123.45', '123.45', '123.45', '123.45', '123', '123')")
	tk.MustExec("alter table t modify c decimal(7, 4)")
	tk.MustExec("alter table t modify vc decimal(7, 4)")
	tk.MustExec("alter table t modify bny decimal(7, 4)")
	tk.MustExec("alter table t modify vbny decimal(7, 4)")
	tk.MustExec("alter table t modify bb decimal(7, 4)")
	tk.MustExec("alter table t modify txt decimal(7, 4)")
	tk.MustExec("alter table t modify e decimal(7, 4)")
	tk.MustExec("alter table t modify s decimal(7, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("123.4500 123.4500 123.4500 123.4500 123.4500 123.4500 1.0000 1.0000"))
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
	tk.MustGetErrCode("alter table t modify txt date", mysql.ErrTruncatedWrongValue)
	tk.MustGetErrCode("alter table t modify e date", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify s date", mysql.ErrUnsupportedDDLOperation)
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
	tk.MustGetErrCode("alter table t modify e time", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify s time", mysql.ErrUnsupportedDDLOperation)
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
	tk.MustGetErrCode("alter table t modify e datetime", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify s datetime", mysql.ErrUnsupportedDDLOperation)
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
	tk.MustGetErrCode("alter table t modify e timestamp", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify s timestamp", mysql.ErrUnsupportedDDLOperation)
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
	tk.MustGetErrCode("alter table t modify c int", mysql.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify vc smallint", mysql.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify bny bigint", mysql.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify vbny datetime", mysql.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify bb timestamp", mysql.ErrTruncatedWrongValue)

	tk.MustGetErrCode("alter table t modify txt date", mysql.ErrTruncatedWrongValue)

	reset(tk)
	tk.MustExec("alter table t modify vc varchar(20)")
	tk.MustExec("insert into t(c, vc) values ('1x', '20200915110836')")
	tk.MustGetErrCode("alter table t modify c year", mysql.ErrTruncatedWrongValue)

	// Special cases about different behavior between TiDB and MySQL.
	// MySQL will get warning but TiDB not.
	// MySQL will get "Warning 1292 Incorrect time value: '20200915110836' for column 'vc'"
	tk.MustExec("alter table t modify vc time")
	tk.MustQuery("select vc from t").Check(testkit.Rows("11:08:36"))

	// Both error but different error message.
	// MySQL will get "ERROR 3140 (22032): Invalid JSON text: "The document root must not be followed by other values." at position 1 in value for column '#sql-5b_42.c'." error.
	reset(tk)
	tk.MustExec("alter table t modify c char(15)")
	tk.MustExec("insert into t(c) values ('{\"k1\": \"value\"')")
	tk.MustGetErrCode("alter table t modify c json", mysql.ErrInvalidJSONText)

	// MySQL will get "ERROR 1366 (HY000): Incorrect DECIMAL value: '0' for column '' at row -1" error.
	tk.MustExec("insert into t(vc) values ('abc')")
	tk.MustGetErrCode("alter table t modify vc decimal(5,3)", mysql.ErrBadNumber)
}
