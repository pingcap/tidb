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
	"strings"
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
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testColumnTypeChangeSuite{})

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
	c.Assert(getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c2"), NotNil)

	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)

	hook := &ddl.TestDDLCallback{}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		switch job.SchemaState {
		case model.StateNone:
			tbl = testGetTableByName(c, internalTK.Se, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			}
			if len(tbl.Cols()) != 2 {
				checkErr = errors.New("len(cols) is not right")
			}
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			tbl = testGetTableByName(c, internalTK.Se, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			}
			// changingCols has been added into meta.
			if len(tbl.(*tables.TableCommon).Columns) != 3 {
				checkErr = errors.New("len(cols) is not right")
			}
			if getModifyColumnInAllCols(c, internalTK.Se.(sessionctx.Context), "test", "t", "c2").Flag&parser_mysql.PreventNullInsertFlag == uint(0) {
				checkErr = errors.New("old col's flag is not right")
			}
			if getModifyColumnInAllCols(c, internalTK.Se.(sessionctx.Context), "test", "t", "_Col$_c2") == nil {
				checkErr = errors.New("changingCol is nil")
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	// Alter sql will modify column c2 to tinyint not null.
	SQL := "alter table t modify column c2 tinyint not null"
	_, err := tk.Exec(SQL)
	c.Assert(err, IsNil)
	// Assert the checkErr in the job of every state.
	c.Assert(checkErr, IsNil)

	// Check the col meta after the column type change.
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.Cols()), Equals, 2)
	col := getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c2")
	c.Assert(col, NotNil)
	c.Assert(col.Flag&parser_mysql.NotNullFlag, Not(Equals), uint(0))
	c.Assert(col.Flag&parser_mysql.NoDefaultValueFlag, Not(Equals), uint(0))
	c.Assert(col.Tp, Equals, parser_mysql.TypeTiny)
	c.Assert(col.ChangeStateInfo, IsNil)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

func getModifyColumnInAllCols(c *C, ctx sessionctx.Context, db, tbl, colName string) *table.Column {
	colLowName := strings.ToLower(colName)
	t := testGetTableByName(c, ctx, db, tbl)
	// tableCommon's columns includes all the cols of all states.
	for _, col := range t.(*tables.TableCommon).Columns {
		if col.Name.L == colLowName {
			return col
		}
	}
	return nil
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
	c.Assert(getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c2"), NotNil)

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
			job.Error = terror.ClassDDL.New(1, "MockRollingBackInCallBack-"+state.String())
		}
	}
}

func assertRollBackedColUnchanged(c *C, tk *testkit.TestKit) {
	tbl := testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.Cols()), Equals, 2)
	col := getModifyColumn(c, tk.Se.(sessionctx.Context), "test", "t", "c2")
	c.Assert(col, NotNil)
	c.Assert(col.Flag, Equals, uint(0))
	c.Assert(col.Tp, Equals, parser_mysql.TypeLonglong)
	c.Assert(col.ChangeStateInfo, IsNil)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}
