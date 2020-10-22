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
	defer func() {
		tk.Se.GetSessionVars().EnableChangeColumnType = false
	}()

	prepare := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a char(10), b varchar(10), c varbinary(10), d blob(10), e text(10), f text(10))")
		tk.MustExec("insert into t values(\"1\", \"1\", \"1\", \"1\", \"1\", \"1\")")
	}
	prepareSpecialString := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a char(10), b varchar(10), c varbinary(10), d blob(10), e text(10), f text(10))")
		tk.MustExec("insert into t values(\"1a\", \"a1a\", \"a1\", \"a\", \"1111111111\",  \"1111111111\")")
	}
	prepareSpecialTimeString := func(tk *testkit.TestKit) {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a char(50), b varchar(50), c varbinary(50), d blob(50), e text(50))")
		tk.MustExec("insert into t values(\"2020-10-21 06:08:28\", \"2020-10-21 06:08:28\", \"0x323032302D31302D32312030363A30383A3238\", \"0x323032302D31302D32312030363A30383A3238\", \"2020-10-21 06:08:28\")")
	}

	// string to integer
	prepare(tk)
	tk.MustExec("alter table t modify a tinyint")
	modifiedColumn := getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "a", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeTiny)
	tk.MustQuery("select a from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify b smallint")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "b", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeShort)
	tk.MustQuery("select b from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify c mediumint")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeInt24)
	tk.MustQuery("select c from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify d int")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "d", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeLong)
	tk.MustQuery("select d from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify e bigint")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "e", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeLonglong)
	tk.MustQuery("select e from t").Check(testkit.Rows("1"))

	prepareSpecialString(tk)
	// special string to integer
	// MySQL fails to alter table with error `Data truncated for column 'a' at row 1`.
	// TiDB succeed to alter table with warning.
	tk.MustExec("alter table t modify a tinyint")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '1a'"))
	tk.MustQuery("select a from t").Check(testkit.Rows("1")) // TiDB will truncate the valid prefix to integer.

	// MySQL fails to alter table with error `Incorrect integer value: 'a1a' for column 'b' at row 1`.
	// TiDB succeed to alter table with warning.
	tk.MustExec("alter table t modify b smallint")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: 'a1a'"))
	tk.MustQuery("select b from t").Check(testkit.Rows("0"))

	// MySQL fails to alter table with error `Incorrect integer value: 'a1' for column 'c' at row 1`.
	// TiDB succeed to alter table with warning.
	tk.MustExec("alter table t modify c mediumint")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: 'a1'"))
	tk.MustQuery("select c from t").Check(testkit.Rows("0"))

	// MySQL fails to alter table with error `Incorrect integer value: 'a' for column 'd' at row 1`.
	// TiDB succeed to alter table (zero value) with warning.
	tk.MustExec("alter table t modify d int")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: 'a'"))
	tk.MustQuery("select d from t").Check(testkit.Rows("0"))

	// cast a long valid string to integer will cause overflow.
	tk.MustGetErrCode("alter table t modify e smallint", parser_mysql.ErrDataOutOfRange)

	tk.MustExec("alter table t modify f bigint")
	tk.MustQuery("select f from t").Check(testkit.Rows("1111111111"))

	// string to decimal
	prepare(tk)
	tk.MustExec("alter table t modify a decimal(2,1)")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "a", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeNewDecimal)
	tk.MustQuery("select a from t").Check(testkit.Rows("1.0"))

	// string to year
	// MySQL converts values in the ranges '0' to '69' and '70' to '99' to YEAR values in the ranges 2000 to 2069 and 1970 to 1999.
	tk.MustExec("alter table t modify b year")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "b", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeYear)
	tk.MustQuery("select b from t").Check(testkit.Rows("2001"))

	// string to time
	tk.MustExec("alter table t modify c time")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeDuration)
	tk.MustQuery("select c from t").Check(testkit.Rows("00:00:01"))

	// string to date
	// MySQL fails to alter table with `Incorrect date value: '1' for column 'd' at row 1`, while TiDB does.
	tk.MustExec("alter table t modify d date")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "d", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeDate)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Incorrect time value: '1'"))
	tk.MustQuery("select d from t").Check(testkit.Rows("0000-00-00"))

	tk.MustExec("alter table t modify e timestamp")
	// MySQL fails to alter table with `Incorrect datetime value: '1' for column 'e' at row 1`, while TiDB does with warnings.
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "e", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeTimestamp)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Incorrect time value: '1'"))
	tk.MustQuery("select e from t").Check(testkit.Rows("0000-00-00 00:00:00"))

	tk.MustExec("alter table t modify f datetime")
	// MySQL fails to alter table with `Incorrect datetime value: '1' for column 'e' at row 1`, while TiDB does with warnings.
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "f", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeDatetime)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Incorrect time value: '1'"))
	tk.MustQuery("select f from t").Check(testkit.Rows("0000-00-00 00:00:00"))

	prepareSpecialTimeString(tk)
	// MySQL fails to alter table with `Data truncated for column 'a' at row 1`, while TiDB does with warnings.
	tk.MustExec("alter table t modify a year")
	tk.MustQuery("select a from t").Check(testkit.Rows("0"))

	// MySQL succeeds with warnings `Incorrect time value: '2020-10-21 06:08:28' for column 'b' at row 1`, while TiDB doesn't have warning.
	tk.MustExec("alter table t modify b time")
	tk.MustQuery("select b from t").Check(testkit.Rows("06:08:28"))

	// TiDB and MySQL both couldn't identify the binary string "0x323032302D31302D32312030363A30383A3238" as date type,
	// TiDB will fill zero date with warning, while MySQL will fail.
	tk.MustExec("alter table t modify c date")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '0x323032302D31302D32312030363A30383A3238'"))
	tk.MustQuery("select c from t").Check(testkit.Rows("0000-00-00"))

	// TiDB and MySQL both couldn't identify the binary string "0x323032302D31302D32312030363A30383A3238" as timestamp type,
	// TiDB will fill zero date with warning, while MySQL will fail.
	tk.MustExec("alter table t modify d timestamp")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '0x323032302D31302D32312030363A30383A3238'"))
	tk.MustQuery("select d from t").Check(testkit.Rows("0000-00-00 00:00:00"))

	tk.MustExec("alter table t modify e datetime")
	tk.MustQuery("select e from t").Check(testkit.Rows("2020-10-21 06:08:28"))

	// string to float-point
	prepare(tk)
	tk.MustExec("alter table t modify a float")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "a", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeFloat)
	tk.MustQuery("select a from t").Check(testkit.Rows("1"))

	tk.MustExec("alter table t modify b double")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "b", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeDouble)
	tk.MustQuery("select b from t").Check(testkit.Rows("1"))

	// string to json
	// MySQL fails to alter with error `Cannot create a JSON value from a string with CHARACTER SET 'binary'.`
	// TiDB succeeds to identify binary string `0x31` as a ascii number.
	tk.MustExec("alter table t modify c json")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeJSON)
	tk.MustQuery("select c from t").Check(testkit.Rows("1"))

	// MySQL and TiDB both can cast normal string `1` to json type.
	tk.MustExec("alter table t modify f json")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "f", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeJSON)
	tk.MustQuery("select f from t").Check(testkit.Rows("1"))

	// string to enum
	// MySQL and TiDB both can cast binary string `0x31` as a offset number of enum type.
	tk.MustExec("alter table t modify d enum(\"a\", \"b\")")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "d", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeEnum)
	tk.MustQuery("select d from t").Check(testkit.Rows("a"))

	// MySQL and TiDB both can cast normal string `1` as element string of enum type.
	tk.MustExec("alter table t modify e enum(\"2\", \"1\")")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "e", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeEnum)
	tk.MustQuery("select e from t").Check(testkit.Rows("1"))

	prepare(tk)
	// string to set
	// MySQL and TiDB both can cast binary string `0x31` as a offset number of enum type.
	tk.MustExec("alter table t modify d set(\"a\", \"b\")")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "d", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeSet)
	tk.MustQuery("select d from t").Check(testkit.Rows("a"))

	// MySQL and TiDB both can cast normal string `1` as element string of enum type.
	tk.MustExec("alter table t modify e set(\"2\", \"1\")")
	modifiedColumn = getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "e", false)
	c.Assert(modifiedColumn, NotNil)
	c.Assert(modifiedColumn.Tp, Equals, parser_mysql.TypeSet)
	tk.MustQuery("select e from t").Check(testkit.Rows("1"))
}
