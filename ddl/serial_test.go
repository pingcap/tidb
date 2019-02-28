// Copyright 2019 PingCAP, Inc.
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
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	gofail "github.com/pingcap/gofail/runtime"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testSerialSuite{})

type testSerialSuite struct {
	store     kv.Storage
	dom       *domain.Domain
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
}

func (s *testSerialSuite) SetUpSuite(c *C) {
	session.SetSchemaLease(200 * time.Millisecond)
	session.SetStatsLease(0)

	ddl.WaitTimeWhenErrorOccured = 1 * time.Microsecond
	var err error
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testSerialSuite) TearDownSuite(c *C) {
	if s.dom != nil {
		s.dom.Close()
	}
	if s.store != nil {
		s.store.Close()
	}
}

// TestCancelAddIndex1 tests canceling ddl job when the add index worker is not started.
func (s *testSerialSuite) TestCancelAddIndexPanic(c *C) {
	gofail.Enable("github.com/pingcap/tidb/ddl/errorMockPanic", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/ddl/errorMockPanic")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	var checkErr error
	oldReorgWaitTimeout := ddl.ReorgWaitTimeout
	ddl.ReorgWaitTimeout = 50 * time.Millisecond
	defer func() { ddl.ReorgWaitTimeout = oldReorgWaitTimeout }()
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
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
			txn, err = hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	origHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	rs, err := tk.Exec("alter table t add index idx_c2(c2)")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
}

func (s *testSerialSuite) TestRestoreTableByJobID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_restore")
	tk.MustExec("use test_restore")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int);")
	defer func(originGC bool) {
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	ddl.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - time.Duration(48*60*60*time.Second)).Format(gcTimeFormat)
	timeAfterDrop := time.Now().Add(time.Duration(48 * 60 * 60 * time.Second)).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	rs, err := tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	rows, err := session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(err, IsNil)
	row := rows[0]
	c.Assert(row.GetString(1), Equals, "test_restore")
	c.Assert(row.GetString(3), Equals, "drop table")
	jobID := row.GetInt64(0)

	// if GC safe point is not exists in mysql.tidb
	_, err = tk.Exec(fmt.Sprintf("admin restore table by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "can not get 'tikv_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// if GC enable is not exists in mysql.tidb
	_, err = tk.Exec(fmt.Sprintf("admin restore table by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:-1]can not get 'tikv_gc_enable'")

	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeAfterDrop))
	_, err = tk.Exec(fmt.Sprintf("admin restore table by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "snapshot is older than GC safe point"), Equals, true)

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// if there is a new table with the same name, should return failed.
	tk.MustExec("create table t_recover (a int);")
	_, err = tk.Exec(fmt.Sprintf("admin restore table by job %d", jobID))
	c.Assert(err.Error(), Equals, infoschema.ErrTableExists.GenWithStackByArgs("t_recover").Error())

	// drop the new table with the same name, then restore table.
	tk.MustExec("drop table t_recover")

	// do restore table.
	tk.MustExec(fmt.Sprintf("admin restore table by job %d", jobID))

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))

	// restore table by none exits job.
	_, err = tk.Exec(fmt.Sprintf("admin restore table by job %d", 10000000))
	c.Assert(err, NotNil)

	// Disable GC by manual first, then after recover table, the GC enable status should also be disabled.
	err = gcutil.DisableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop table t_recover")
	rs, err = tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	rows, err = session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(err, IsNil)
	row = rows[0]
	c.Assert(row.GetString(1), Equals, "test_restore")
	c.Assert(row.GetString(3), Equals, "drop table")
	jobID = row.GetInt64(0)

	tk.MustExec(fmt.Sprintf("admin restore table by job %d", jobID))

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9"))

	gcEnable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(gcEnable, Equals, false)
}

func (s *testSerialSuite) TestRestoreTableByTableName(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_restore")
	tk.MustExec("use test_restore")
	tk.MustExec("drop table if exists t_recover, t_recover2")
	tk.MustExec("create table t_recover (a int);")
	defer func(originGC bool) {
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	ddl.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - time.Duration(48*60*60*time.Second)).Format(gcTimeFormat)
	timeAfterDrop := time.Now().Add(time.Duration(48 * 60 * 60 * time.Second)).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	// if GC safe point is not exists in mysql.tidb
	_, err := tk.Exec("admin restore table t_recover")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "can not get 'tikv_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// if GC enable is not exists in mysql.tidb
	_, err = tk.Exec("admin restore table t_recover")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:-1]can not get 'tikv_gc_enable'")

	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeAfterDrop))
	_, err = tk.Exec("admin restore table t_recover")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "snapshot is older than GC safe point"), Equals, true)

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// if there is a new table with the same name, should return failed.
	tk.MustExec("create table t_recover (a int);")
	_, err = tk.Exec("admin restore table t_recover")
	c.Assert(err.Error(), Equals, infoschema.ErrTableExists.GenWithStackByArgs("t_recover").Error())

	// drop the new table with the same name, then restore table.
	tk.MustExec("rename table t_recover to t_recover2")

	// do restore table.
	tk.MustExec("admin restore table t_recover")

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_recover;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003"))

	// restore table by none exits job.
	_, err = tk.Exec(fmt.Sprintf("admin restore table by job %d", 10000000))
	c.Assert(err, NotNil)

	// Disable GC by manual first, then after recover table, the GC enable status should also be disabled.
	err = gcutil.DisableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop table t_recover")

	tk.MustExec("admin restore table t_recover")

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9"))

	gcEnable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(gcEnable, Equals, false)
}

func (s *testSerialSuite) TestRestoreTableByJobIDFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_restore")
	tk.MustExec("use test_restore")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int);")
	defer func(originGC bool) {
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	ddl.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - time.Duration(48*60*60*time.Second)).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	rs, err := tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	rows, err := session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(err, IsNil)
	row := rows[0]
	c.Assert(row.GetString(1), Equals, "test_restore")
	c.Assert(row.GetString(3), Equals, "drop table")
	jobID := row.GetInt64(0)

	// enableGC first
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// set hook
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionRestoreTable {
			gofail.Enable("github.com/pingcap/tidb/store/tikv/mockCommitError", `return(true)`)
			gofail.Enable("github.com/pingcap/tidb/ddl/mockRestoreTableCommitErr", `return(true)`)
		}
	}
	origHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	// do restore table.
	tk.MustExec(fmt.Sprintf("admin restore table by job %d", jobID))
	gofail.Disable("github.com/pingcap/tidb/store/tikv/mockCommitError")
	gofail.Disable("github.com/pingcap/tidb/ddl/mockRestoreTableCommitErr")

	// make sure enable GC after restore table.
	enable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(enable, Equals, true)

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func (s *testSerialSuite) TestRestoreTableByTableNameFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_restore")
	tk.MustExec("use test_restore")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int);")
	defer func(originGC bool) {
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	ddl.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - time.Duration(48*60*60*time.Second)).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	// enableGC first
	err := gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// set hook
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionRestoreTable {
			gofail.Enable("github.com/pingcap/tidb/store/tikv/mockCommitError", `return(true)`)
			gofail.Enable("github.com/pingcap/tidb/ddl/mockRestoreTableCommitErr", `return(true)`)
		}
	}
	origHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	// do restore table.
	tk.MustExec("admin restore table t_recover")
	gofail.Disable("github.com/pingcap/tidb/store/tikv/mockCommitError")
	gofail.Disable("github.com/pingcap/tidb/ddl/mockRestoreTableCommitErr")

	// make sure enable GC after restore table.
	enable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(enable, Equals, true)

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func (s *testSerialSuite) TestAddIndexSplitTableRangesPanic(c *C) {
	gofail.Enable("github.com/pingcap/tidb/ddl/mockSplitTableRangesPanic", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/ddl/mockSplitTableRangesPanic")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_split_table")
	defer tk.MustExec("drop database test_split_table")
	tk.MustExec("use test_split_table")

	tk.MustExec("create table t_split_table(a bigint primary key, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t_split_table values(%v, %v)", i, i))
	}

	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_split_table"), model.NewCIStr("t_split_table"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID
	s.cluster.SplitTable(s.mvccStore, tblID, 100)

	tk.MustExec("alter table t_split_table add index idx_b(b)")
	tk.MustExec("admin check index t_split_table idx_b")
	tk.MustExec("admin check table t_split_table")
}

func (s *testSerialSuite) TestDropSchemaFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_schema")
	tk.MustExec("use test_schema")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values (1),(2),(3)")

	hook := &ddl.TestDDLCallback{}
	first := true
	stateCnt := 0

	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			stateCnt++
		} else if job.SchemaState == model.StateDeleteOnly {
			if first {
				gofail.Enable("github.com/pingcap/tidb/ddl/dropSchemaErr", `return(true)`)
				first = false
			} else {
				gofail.Disable("github.com/pingcap/tidb/ddl/dropSchemaErr")
			}
		}
	}
	origHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	tk.MustExec("drop database test_schema;")
	c.Assert(stateCnt, Equals, 1)
	tk.MustExec("create database test_schema")
}

func (s *testSerialSuite) TestDropTableOrViewFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_drop_table")
	tk.MustExec("use test_drop_table")
	tk.MustExec("drop table if exists t_table")
	tk.MustExec("create table t_table (a int);")
	tk.MustExec("insert into t_table values (1);")

	hook := &ddl.TestDDLCallback{}
	first := true
	stateCnt := 0

	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			stateCnt++
		} else if job.SchemaState == model.StateDeleteOnly {
			if first {
				gofail.Enable("github.com/pingcap/tidb/ddl/dropTableOrViewErr", `return(true)`)
				first = false
			} else {
				gofail.Disable("github.com/pingcap/tidb/ddl/dropTableOrViewErr")
			}
		}
	}
	origHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	tk.MustExec("drop table t_table;")
	c.Assert(stateCnt, Equals, 1)
	tk.MustExec("create table t_table (a int);")
}
