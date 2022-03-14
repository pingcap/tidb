// Copyright 2015 PingCAP, Inc.
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
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	parsertypes "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	ntestkit "github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true

	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

const (
	// waitForCleanDataRound indicates how many times should we check data is cleaned or not.
	waitForCleanDataRound = 150
	// waitForCleanDataInterval is a min duration between 2 check for data clean.
	waitForCleanDataInterval = time.Millisecond * 100
)

var _ = Suite(&testDBSuite4{&testDBSuite{}})
var _ = Suite(&testDBSuite5{&testDBSuite{}})
var _ = SerialSuites(&testSerialDBSuite{&testDBSuite{}})

const defaultBatchSize = 1024
const defaultReorgBatchSize = 256

type testDBSuite struct {
	cluster    testutils.Cluster
	store      kv.Storage
	dom        *domain.Domain
	schemaName string
	s          session.Session
	lease      time.Duration
	autoIDStep int64
	ctx        sessionctx.Context
}

func setUpSuite(s *testDBSuite, c *C) {
	var err error

	s.lease = 600 * time.Millisecond
	session.SetSchemaLease(s.lease)
	session.DisableStats4Test()
	s.schemaName = "test_db"
	s.autoIDStep = autoid.GetStep()
	ddl.SetWaitTimeWhenErrorOccurred(0)

	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.s, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.ctx = s.s.(sessionctx.Context)

	_, err = s.s.Execute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)
	_, err = s.s.Execute(context.Background(), "set @@global.tidb_max_delta_schema_count= 4096")
	c.Assert(err, IsNil)
}

func tearDownSuite(s *testDBSuite, c *C) {
	_, err := s.s.Execute(context.Background(), "drop database if exists test_db")
	c.Assert(err, IsNil)
	s.s.Close()
	s.dom.Close()
	err = s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testDBSuite) SetUpSuite(c *C) {
	setUpSuite(s, c)
}

func (s *testDBSuite) TearDownSuite(c *C) {
	tearDownSuite(s, c)
}

type testDBSuite4 struct{ *testDBSuite }
type testDBSuite5 struct{ *testDBSuite }
type testSerialDBSuite struct{ *testDBSuite }

func (s *testDBSuite5) TestAddIndexWithDupIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

	err1 := dbterror.ErrDupKeyName.GenWithStack("index already exist %s", "idx")
	err2 := dbterror.ErrDupKeyName.GenWithStack("index already exist %s; "+
		"a background job is trying to add the same index, "+
		"please check by `ADMIN SHOW DDL JOBS`", "idx")

	// When there is already an duplicate index, show error message.
	tk.MustExec("create table test_add_index_with_dup (a int, key idx (a))")
	_, err := tk.Exec("alter table test_add_index_with_dup add index idx (a)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)
	c.Assert(errors.Cause(err1).Error() == err.Error(), IsTrue)

	// When there is another session adding duplicate index with state other than
	// StatePublic, show explicit error message.
	t := s.testGetTable(c, "test_add_index_with_dup")
	indexInfo := t.Meta().FindIndexByName("idx")
	indexInfo.State = model.StateNone
	_, err = tk.Exec("alter table test_add_index_with_dup add index idx (a)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)
	c.Assert(errors.Cause(err2).Error() == err.Error(), IsTrue)

	tk.MustExec("drop table test_add_index_with_dup")
}

func testGetTableByName(c *C, ctx sessionctx.Context, db, table string) table.Table {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	c.Assert(err, IsNil)
	return tbl
}

func testGetSchemaByName(c *C, ctx sessionctx.Context, db string) *model.DBInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	dbInfo, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(db))
	c.Assert(ok, IsTrue)
	return dbInfo
}

func (s *testDBSuite) testGetTable(c *C, name string) table.Table {
	ctx := s.s.(sessionctx.Context)
	return testGetTableByName(c, ctx, s.schemaName, name)
}

func backgroundExecT(s kv.Storage, sql string, done chan error) {
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

func (s *testSerialDBSuite) TestWriteReorgForColumnTypeChangeOnAmendTxn(c *C) {
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test_db")
	tk2.MustExec("set global tidb_enable_amend_pessimistic_txn = ON;")
	defer func() {
		tk2.MustExec("set global tidb_enable_amend_pessimistic_txn = OFF;")
	}()

	d := s.dom.DDL()
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	testInsertOnModifyColumn := func(sql string, startColState, commitColState model.SchemaState, retStrs []string, retErr error) {
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test_db")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
		tk.MustExec("insert into t1 values (20, 20, 20);")

		var checkErr error
		tk1 := testkit.NewTestKit(c, s.store)
		defer func() {
			if tk1.Se != nil {
				tk1.Se.Close()
			}
		}()
		hook := &ddl.TestDDLCallback{Do: s.dom}
		times := 0
		hook.OnJobUpdatedExported = func(job *model.Job) {
			if job.Type != model.ActionModifyColumn || checkErr != nil ||
				(job.SchemaState != startColState && job.SchemaState != commitColState) {
				return
			}

			if job.SchemaState == startColState {
				tk1.MustExec("use test_db")
				tk1.MustExec("begin pessimistic;")
				tk1.MustExec("insert into t1 values(101, 102, 103)")
				return
			}
			if times == 0 {
				_, checkErr = tk1.Exec("commit;")
			}
			times++
		}
		d.SetHook(hook)

		tk.MustExec(sql)
		if retErr == nil {
			c.Assert(checkErr, IsNil)
		} else {
			c.Assert(strings.Contains(checkErr.Error(), retErr.Error()), IsTrue)
		}
		tk.MustQuery("select * from t1;").Check(testkit.Rows(retStrs...))

		tk.MustExec("admin check table t1")
	}

	// Testing it needs reorg data.
	ddlStatement := "alter table t1 change column c2 cc smallint;"
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateDeleteOnly, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateDeleteOnly, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)

	// Testing it needs not reorg data. This case only have two state: none, public.
	ddlStatement = "alter table t1 change column c2 cc bigint;"
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StateWriteReorganization, []string{"20 20 20"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StateWriteReorganization, []string{"20 20 20"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StatePublic, []string{"20 20 20", "101 102 103"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StatePublic, []string{"20 20 20"}, nil)
}

func (s *testSerialDBSuite) TestAddExpressionIndexRollback(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
	tk.MustExec("insert into t1 values (20, 20, 20), (40, 40, 40), (80, 80, 80), (160, 160, 160);")

	var checkErr error
	tk1 := testkit.NewTestKit(c, s.store)
	_, checkErr = tk1.Exec("use test_db")

	d := s.dom.DDL()
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var currJob *model.Job
	ctx := mock.NewContext()
	ctx.Store = s.store
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
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select * from t1 order by c1;").Check(testkit.Rows("2 2 2", "4 4 4", "5 80 80", "10 3 3", "20 20 20", "160 160 160"))

	// Check whether the reorg information is cleaned up.
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	element, start, end, physicalID, err := m.GetDDLReorgHandle(currJob)
	c.Assert(meta.ErrDDLReorgElementNotExist.Equal(err), IsTrue)
	c.Assert(element, IsNil)
	c.Assert(start, IsNil)
	c.Assert(end, IsNil)
	c.Assert(physicalID, Equals, int64(0))
}

func (s *testSerialDBSuite) TestDropTableOnTiKVDiskFull(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table test_disk_full_drop_table(a int);")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcTiKVAllowedOnAlmostFull", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcTiKVAllowedOnAlmostFull")
	tk.MustExec("drop table test_disk_full_drop_table;")
}

// TestCancelDropIndex tests cancel ddl job which type is drop primary key.
func (s *testDBSuite4) TestCancelDropPrimaryKey(c *C) {
	idxName := "primary"
	addIdxSQL := "alter table t add primary key idx_c2 (c2);"
	dropIdxSQL := "alter table t drop primary key;"
	testCancelDropIndex(c, s.store, s.dom.DDL(), idxName, addIdxSQL, dropIdxSQL, s.dom)
}

// TestCancelDropIndex tests cancel ddl job which type is drop index.
func (s *testDBSuite5) TestCancelDropIndex(c *C) {
	idxName := "idx_c2"
	addIdxSQL := "alter table t add index idx_c2 (c2);"
	dropIdxSQL := "alter table t drop index idx_c2;"
	testCancelDropIndex(c, s.store, s.dom.DDL(), idxName, addIdxSQL, dropIdxSQL, s.dom)
}

// testCancelDropIndex tests cancel ddl job which type is drop index.
func testCancelDropIndex(c *C, store kv.Storage, d ddl.DDL, idxName, addIdxSQL, dropIdxSQL string, dom *domain.Domain) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	testCases := []struct {
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
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if (job.Type == model.ActionDropIndex || job.Type == model.ActionDropPrimaryKey) &&
			job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
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
	ctx := tk.Se.(sessionctx.Context)
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddIndex {
			tk.MustExec(addIdxSQL)
		}
		rs, err := tk.Exec(dropIdxSQL)
		if rs != nil {
			rs.Close()
		}
		t := testGetTableByName(c, ctx, "test_db", "t")
		indexInfo := t.Meta().FindIndexByName(idxName)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			c.Assert(indexInfo, NotNil)
			c.Assert(indexInfo.State, Equals, model.StatePublic)
		} else {
			err1 := admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID)
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, err1.Error())
			c.Assert(indexInfo, IsNil)
		}
	}
	d.SetHook(originalHook)
	tk.MustExec(addIdxSQL)
	tk.MustExec(dropIdxSQL)
}

// TestCancelTruncateTable tests cancel ddl job which type is truncate table.
func (s *testDBSuite5) TestCancelTruncateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create database if not exists test_truncate_table")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionTruncateTable && job.State == model.JobStateNone {
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
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().SetHook(hook)
	_, err := tk.Exec("truncate table t")
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
	s.dom.DDL().SetHook(originalHook)
}

func (s *testDBSuite5) TestParallelDropSchemaAndDropTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_drop_schema_table")
	tk.MustExec("use test_drop_schema_table")
	tk.MustExec("create table t(c1 int, c2 int)")
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	dbInfo := testGetSchemaByName(c, tk.Se, "test_drop_schema_table")
	done := false
	var wg sync.WaitGroup
	tk2 := testkit.NewTestKit(c, s.store)
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
	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().SetHook(hook)
	tk.MustExec("drop database test_drop_schema_table")
	s.dom.DDL().SetHook(originalHook)
	wg.Wait()
	c.Assert(done, IsTrue)
	c.Assert(checkErr, NotNil)
	// There are two possible assert result because:
	// 1: If drop-database is finished before drop-table being put into the ddl job queue, it will return "unknown table" error directly in the previous check.
	// 2: If drop-table has passed the previous check and been put into the ddl job queue, then drop-database finished, it will return schema change error.
	assertRes := checkErr.Error() == "[domain:8028]Information schema is changed during the execution of the"+
		" statement(for example, table definition may be updated by other DDL ran in parallel). "+
		"If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]" ||
		checkErr.Error() == "[schema:1051]Unknown table 'test_drop_schema_table.t'"

	c.Assert(assertRes, Equals, true)

	// Below behaviour is use to mock query `curl "http://$IP:10080/tiflash/replica"`
	fn := func(jobs []*model.Job) (bool, error) {
		return executor.GetDropOrTruncateTableInfoFromJobs(jobs, 0, s.dom, func(job *model.Job, info *model.TableInfo) (bool, error) {
			return false, nil
		})
	}
	err := tk.Se.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	err = admin.IterHistoryDDLJobs(txn, fn)
	c.Assert(err, IsNil)
}

func (s *testDBSuite4) TestAlterLock(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("create table t_index_lock (c1 int, c2 int, C3 int)")
	tk.MustExec("alter table t_index_lock add index (c1, c2), lock=none")
}

func (s *testDBSuite5) TestAddMultiColumnsIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

	tk.MustExec("drop database if exists tidb;")
	tk.MustExec("create database tidb;")
	tk.MustExec("use tidb;")
	tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	tk.MustExec("insert tidb.test values (1, 1);")
	tk.MustExec("update tidb.test set b = b + 1 where a = 1;")
	tk.MustExec("insert into tidb.test values (2, 2);")
	// Test that the b value is nil.
	tk.MustExec("insert into tidb.test (a) values (3);")
	tk.MustExec("insert into tidb.test values (4, 4);")
	// Test that the b value is nil again.
	tk.MustExec("insert into tidb.test (a) values (5);")
	tk.MustExec("insert tidb.test values (6, 6);")
	tk.MustExec("alter table tidb.test add index idx1 (a, b);")
	tk.MustExec("admin check table test")
}

func testGetIndexID(t *testing.T, ctx sessionctx.Context, dbName, tblName, idxName string) int64 {
	is := domain.GetDomain(ctx).InfoSchema()
	tt, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
	require.NoError(t, err)

	for _, idx := range tt.Indices() {
		if idx.Meta().Name.L == idxName {
			return idx.Meta().ID
		}
	}
	require.FailNowf(t, "index %s not found(db: %s, tbl: %s)", idxName, dbName, tblName)
	return -1
}

type testDDLJobIDCallback struct {
	ddl.Callback
	jobID int64
}

func (t *testDDLJobIDCallback) OnJobUpdated(job *model.Job) {
	if t.jobID == 0 {
		t.jobID = job.ID
	}
	if t.Callback != nil {
		t.Callback.OnJobUpdated(job)
	}
}

func wrapJobIDExtCallback(oldCallback ddl.Callback) *testDDLJobIDCallback {
	return &testDDLJobIDCallback{
		Callback: oldCallback,
		jobID:    0,
	}
}

func setupJobIDExtCallback(ctx sessionctx.Context) (jobExt *testDDLJobIDCallback, tearDown func()) {
	dom := domain.GetDomain(ctx)
	originHook := dom.DDL().GetHook()
	jobIDExt := wrapJobIDExtCallback(originHook)
	dom.DDL().SetHook(jobIDExt)
	return jobIDExt, func() {
		dom.DDL().SetHook(originHook)
	}
}

func checkDelRangeAdded(tk *ntestkit.TestKit, jobID int64, elemID int64) {
	query := `select sum(cnt) from
	(select count(1) cnt from mysql.gc_delete_range where job_id = ? and element_id = ? union
	select count(1) cnt from mysql.gc_delete_range_done where job_id = ? and element_id = ?) as gdr;`
	tk.MustQuery(query, jobID, elemID, jobID, elemID).Check(testkit.Rows("1"))
}

func (s *testDBSuite5) TestAlterPrimaryKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table test_add_pk(a int, b int unsigned , c varchar(255) default 'abc', d int as (a+b), e int as (a+1) stored, index idx(b))")
	defer tk.MustExec("drop table test_add_pk")

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
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'PRIMARY' is too long (max = 1024)"))
	tk.MustExec("set @@sql_mode= '" + sqlMode + "'")
	tk.MustExec("alter table test_add_pk drop primary key")
	// for valid comment
	tk.MustExec("alter table test_add_pk add primary key(a, b, c) comment " + validComment)
	ctx := tk.Se.(sessionctx.Context)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)
	t := testGetTableByName(c, ctx, "test", "test_add_pk")
	col1Flag := t.Cols()[0].Flag
	col2Flag := t.Cols()[1].Flag
	col3Flag := t.Cols()[2].Flag
	c.Assert(mysql.HasNotNullFlag(col1Flag) && !mysql.HasPreventNullInsertFlag(col1Flag), IsTrue)
	c.Assert(mysql.HasNotNullFlag(col2Flag) && !mysql.HasPreventNullInsertFlag(col2Flag) && mysql.HasUnsignedFlag(col2Flag), IsTrue)
	c.Assert(mysql.HasNotNullFlag(col3Flag) && !mysql.HasPreventNullInsertFlag(col3Flag) && !mysql.HasNoDefaultValueFlag(col3Flag), IsTrue)
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

func (s *testDBSuite4) TestAddIndexWithDupCols(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	err1 := infoschema.ErrColumnExists.GenWithStackByArgs("b")
	err2 := infoschema.ErrColumnExists.GenWithStackByArgs("B")

	tk.MustExec("create table test_add_index_with_dup (a int, b int)")
	_, err := tk.Exec("create index c on test_add_index_with_dup(b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.Exec("create index c on test_add_index_with_dup(b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.Exec("alter table test_add_index_with_dup add index c (b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.Exec("alter table test_add_index_with_dup add index c (b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	tk.MustExec("drop table test_add_index_with_dup")
}

func (s *testDBSuite5) TestCreateIndexType(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	sql := `CREATE TABLE test_index (
		price int(5) DEFAULT '0' NOT NULL,
		area varchar(40) DEFAULT '' NOT NULL,
		type varchar(40) DEFAULT '' NOT NULL,
		transityes set('a','b'),
		shopsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		schoolsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		petsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		KEY price (price,area,type,transityes,shopsyes,schoolsyes,petsyes));`
	tk.MustExec(sql)
}

func oldBackgroundExec(s kv.Storage, sql string, done chan error) {
	se, err := session.CreateSession4Test(s)
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	defer se.Close()
	_, err = se.Execute(context.Background(), "use test_db")
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	_, err = se.Execute(context.Background(), sql)
	done <- errors.Trace(err)
}

// TestCreateTableWithLike2 tests create table with like when refer table have non-public column/index.
func (s *testSerialDBSuite) TestCreateTableWithLike2(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1,t2;")
	defer tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("create table t1 (a int, b int, c int, index idx1(c));")

	tbl1 := testGetTableByName(c, s.s, "test_db", "t1")
	doneCh := make(chan error, 2)
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var onceChecker sync.Map
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type != model.ActionAddColumn && job.Type != model.ActionDropColumn &&
			job.Type != model.ActionAddColumns && job.Type != model.ActionDropColumns &&
			job.Type != model.ActionAddIndex && job.Type != model.ActionDropIndex {
			return
		}
		if job.TableID != tbl1.Meta().ID {
			return
		}

		if job.SchemaState == model.StateDeleteOnly {
			if _, ok := onceChecker.Load(job.ID); ok {
				return
			}

			onceChecker.Store(job.ID, true)
			go oldBackgroundExec(s.store, "create table t2 like t1", doneCh)
		}
	}
	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().SetHook(originalHook)
	s.dom.DDL().SetHook(hook)

	// create table when refer table add column
	tk.MustExec("alter table t1 add column d int")
	checkTbl2 := func() {
		err := <-doneCh
		c.Assert(err, IsNil)
		tk.MustExec("alter table t2 add column e int")
		t2Info := testGetTableByName(c, s.s, "test_db", "t2")
		c.Assert(len(t2Info.Meta().Columns), Equals, len(t2Info.Cols()))
	}
	checkTbl2()

	// create table when refer table drop column
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 drop column b;")
	checkTbl2()

	// create table when refer table add index
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 add index idx2(a);")
	checkTbl2 = func() {
		err := <-doneCh
		c.Assert(err, IsNil)
		tk.MustExec("alter table t2 add column e int")
		tbl2 := testGetTableByName(c, s.s, "test_db", "t2")
		c.Assert(len(tbl2.Meta().Columns), Equals, len(tbl2.Cols()))

		for i := 0; i < len(tbl2.Meta().Indices); i++ {
			c.Assert(tbl2.Meta().Indices[i].State, Equals, model.StatePublic)
		}
	}
	checkTbl2()

	// create table when refer table drop index.
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 drop index idx2;")
	checkTbl2()

	// Test for table has tiflash  replica.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	s.dom.DDL().SetHook(originalHook)
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("create table t1 (a int) partition by hash(a) partitions 2;")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 := testGetTableByName(c, s.s, "test_db", "t1")
	// Mock for all partitions replica was available.
	partition := t1.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 2)
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	t1 = testGetTableByName(c, s.s, "test_db", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})

	tk.MustExec("create table t2 like t1")
	t2 := testGetTableByName(c, s.s, "test_db", "t2")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)
	// Test for not affecting the original table.
	t1 = testGetTableByName(c, s.s, "test_db", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})
}

func (s *testSerialDBSuite) TestTruncateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table truncate_table (c1 int, c2 int)")
	tk.MustExec("insert truncate_table values (1, 1), (2, 2)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("truncate_table"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID

	tk.MustExec("truncate table truncate_table")

	tk.MustExec("insert truncate_table values (3, 3), (4, 4)")
	tk.MustQuery("select * from truncate_table").Check(testkit.Rows("3 3", "4 4"))

	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("truncate_table"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Greater, oldTblID)

	// Verify that the old table data has been deleted by background worker.
	tablePrefix := tablecodec.EncodeTablePrefix(oldTblID)
	hasOldTableData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		err = kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
			it, err1 := txn.Iter(tablePrefix, nil)
			if err1 != nil {
				return err1
			}
			if !it.Valid() {
				hasOldTableData = false
			} else {
				hasOldTableData = it.Key().HasPrefix(tablePrefix)
			}
			it.Close()
			return nil
		})
		c.Assert(err, IsNil)
		if !hasOldTableData {
			break
		}
		time.Sleep(waitForCleanDataInterval)
	}
	c.Assert(hasOldTableData, IsFalse)

	// Test for truncate table should clear the tiflash available status.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err = failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 := testGetTableByName(c, s.s, "test", "t1")
	// Mock for table tiflash replica was available.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, t1.Meta().ID, true)
	c.Assert(err, IsNil)
	t1 = testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)

	tk.MustExec("truncate table t1")
	t2 := testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)

	// Test for truncate partition should clear the tiflash available status.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int) partition by hash(a) partitions 2;")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 = testGetTableByName(c, s.s, "test", "t1")
	// Mock for all partitions replica was available.
	partition := t1.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 2)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	t1 = testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})

	tk.MustExec("alter table t1 truncate partition p0")
	t2 = testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[1].ID})
	// Test for truncate twice.
	tk.MustExec("alter table t1 truncate partition p0")
	t2 = testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[1].ID})

}

func (s *testDBSuite4) TestComment(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists ct, ct1")

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
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'c' is too long (max = 1024)"))
	tk.MustExec("create index i1 on ct1 (d) comment '" + invalidComment + "b" + "'")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'i1' is too long (max = 1024)"))
	tk.MustExec("alter table ct1 add key (e) comment '" + invalidComment + "'")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'e' is too long (max = 1024)"))

	tk.MustExec("drop table if exists ct, ct1")
}

func (s *testSerialDBSuite) TestRebaseAutoID(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

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

func (s *testDBSuite5) TestCheckColumnDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists text_default_text;")
	tk.MustGetErrCode("create table text_default_text(c1 text not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_text(c1 text not null default 'scds');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("drop table if exists text_default_json;")
	tk.MustGetErrCode("create table text_default_json(c1 json not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_json(c1 json not null default 'dfew555');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("drop table if exists text_default_blob;")
	tk.MustGetErrCode("create table text_default_blob(c1 blob not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_blob(c1 blob not null default 'scds54');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("set sql_mode='';")
	tk.MustExec("create table text_default_text(c1 text not null default '');")
	tk.MustQuery(`show create table text_default_text`).Check(testutil.RowsWithSep("|",
		"text_default_text CREATE TABLE `text_default_text` (\n"+
			"  `c1` text NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_text"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, "")

	tk.MustExec("create table text_default_blob(c1 blob not null default '');")
	tk.MustQuery(`show create table text_default_blob`).Check(testutil.RowsWithSep("|",
		"text_default_blob CREATE TABLE `text_default_blob` (\n"+
			"  `c1` blob NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_blob"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, "")

	tk.MustExec("create table text_default_json(c1 json not null default '');")
	tk.MustQuery(`show create table text_default_json`).Check(testutil.RowsWithSep("|",
		"text_default_json CREATE TABLE `text_default_json` (\n"+
			"  `c1` json NOT NULL DEFAULT 'null'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_json"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, `null`)
}

func (s *testDBSuite4) TestCheckTooBigFieldLength(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tr_01;")
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

func (s *testDBSuite5) TestCheckConvertToCharacter(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table t")
	tk.MustExec("create table t(a varchar(10) charset binary);")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset utf8 collate utf8_bin", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset utf8mb4 collate utf8mb4_bin", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset latin1 collate latin1_bin", errno.ErrUnsupportedDDLOperation)
	c.Assert(t.Cols()[0].Charset, Equals, "binary")
}

func (s *testDBSuite4) TestIfNotExists(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int key);")

	// ADD COLUMN
	sql := "alter table t1 add column b int"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	tk.MustExec("alter table t1 add column if not exists b int")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1060|Duplicate column name 'b'"))

	// ADD INDEX
	sql = "alter table t1 add index idx_b (b)"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	tk.MustExec("alter table t1 add index if not exists idx_b (b)")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// CREATE INDEX
	sql = "create index idx_b on t1 (b)"
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	tk.MustExec("create index if not exists idx_b on t1 (b)")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// ADD PARTITION
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a int key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	sql = "alter table t2 add partition (partition p2 values less than (30))"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrSameNamePartition)
	tk.MustExec("alter table t2 add partition if not exists (partition p2 values less than (30))")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1517|Duplicate partition name p2"))
}

func (s *testDBSuite4) TestIfExists(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int key, b int);")

	// DROP COLUMN
	sql := "alter table t1 drop column b"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	tk.MustExec("alter table t1 drop column if exists b") // only `a` exists now
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1091|Can't DROP 'b'; check that column/key exists"))

	// CHANGE COLUMN
	sql = "alter table t1 change column b c int"
	tk.MustGetErrCode(sql, errno.ErrBadField)
	tk.MustExec("alter table t1 change column if exists b c int")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1054|Unknown column 'b' in 't1'"))
	tk.MustExec("alter table t1 change column if exists a c int") // only `c` exists now

	// MODIFY COLUMN
	sql = "alter table t1 modify column a bigint"
	tk.MustGetErrCode(sql, errno.ErrBadField)
	tk.MustExec("alter table t1 modify column if exists a bigint")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1054|Unknown column 'a' in 't1'"))
	tk.MustExec("alter table t1 modify column if exists c bigint") // only `c` exists now

	// DROP INDEX
	tk.MustExec("alter table t1 add index idx_c (c)")
	sql = "alter table t1 drop index idx_c"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	tk.MustExec("alter table t1 drop index if exists idx_c")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1091|index idx_c doesn't exist"))

	// DROP PARTITION
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a int key) partition by range(a) (partition pNeg values less than (0), partition p0 values less than (10), partition p1 values less than (20))")
	sql = "alter table t2 drop partition p1"
	tk.MustExec(sql)
	tk.MustGetErrCode(sql, errno.ErrDropPartitionNonExistent)
	tk.MustExec("alter table t2 drop partition if exists p1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1507|Error in list of partitions to DROP"))
}

func (s *testDBSuite5) TestModifyGeneratedColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test;")
	tk.MustExec("use test")
	modIdxColErrMsg := "[ddl:3106]'modifying an indexed column' is not supported for generated columns."
	modStoredColErrMsg := "[ddl:3106]'modifying a stored column' is not supported for generated columns."

	// Modify column with single-col-index.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(b));")
	tk.MustExec("insert into t1 set a=1;")
	_, err := tk.Exec("alter table t1 modify column b int as (a+2);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modIdxColErrMsg)
	tk.MustExec("drop index idx on t1;")
	tk.MustExec("alter table t1 modify b int as (a+2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 3"))

	// Modify column with multi-col-index.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(a, b));")
	tk.MustExec("insert into t1 set a=1;")
	_, err = tk.Exec("alter table t1 modify column b int as (a+2);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modIdxColErrMsg)
	tk.MustExec("drop index idx on t1;")
	tk.MustExec("alter table t1 modify b int as (a+2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 3"))

	// Modify column with stored status to a different expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	_, err = tk.Exec("alter table t1 modify column b int as (a+2) stored;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modStoredColErrMsg)

	// Modify column with stored status to the same expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b bigint as (a+1) stored;")
	tk.MustExec("alter table t1 modify column b bigint as (a + 1) stored;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))

	// Modify column with index to the same expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(b));")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b bigint as (a+1);")
	tk.MustExec("alter table t1 modify column b bigint as (a + 1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))

	// Modify column from non-generated to stored generated.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int);")
	_, err = tk.Exec("alter table t1 modify column b bigint as (a+1) stored;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modStoredColErrMsg)

	// Modify column from stored generated to non-generated.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b int;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))
}

func (s *testDBSuite5) TestDefaultSQLFunction(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test;")
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2, t3, t4;")

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

func (s *testSerialDBSuite) TestProcessColumnFlags(c *C) {
	// check `processColumnFlags()`
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table t(a year(4) comment 'xxx', b year, c bit)")
	defer tk.MustExec("drop table t;")

	check := func(n string, f func(uint) bool) {
		t := testGetTableByName(c, tk.Se, "test_db", "t")
		for _, col := range t.Cols() {
			if strings.EqualFold(col.Name.L, n) {
				c.Assert(f(col.Flag), IsTrue)
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

func (s *testSerialDBSuite) TestSetTableFlashReplica(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t_flash;")
	tk.MustExec("create table t_flash(a int, b int)")
	defer tk.MustExec("drop table t_flash;")

	t := s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, IsNil)

	tk.MustExec("alter table t_flash set tiflash replica 2 location labels 'a','b';")
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Count, Equals, uint64(2))
	c.Assert(strings.Join(t.Meta().TiFlashReplica.LocationLabels, ","), Equals, "a,b")

	tk.MustExec("alter table t_flash set tiflash replica 0")
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, IsNil)

	// Test set tiflash replica for partition table.
	tk.MustExec("drop table if exists t_flash;")
	tk.MustExec("create table t_flash(a int, b int) partition by hash(a) partitions 3")
	tk.MustExec("alter table t_flash set tiflash replica 2 location labels 'a','b';")
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Count, Equals, uint64(2))
	c.Assert(strings.Join(t.Meta().TiFlashReplica.LocationLabels, ","), Equals, "a,b")

	// Use table ID as physical ID, mock for partition feature was not enabled.
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, t.Meta().ID, true)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, true)
	c.Assert(len(t.Meta().TiFlashReplica.AvailablePartitionIDs), Equals, 0)

	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, t.Meta().ID, false)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)

	// Mock for partition 0 replica was available.
	partition := t.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 3)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID})

	// Mock for partition 0 replica become unavailable.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, false)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)

	// Mock for partition 0, 1,2 replica was available.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[2].ID, true)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, true)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID, partition.Definitions[2].ID})

	// Mock for partition 1 replica was unavailable.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[1].ID, false)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[2].ID})

	// Test for update table replica with unknown table ID.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, math.MaxInt64, false)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schema:1146]Table which ID = 9223372036854775807 does not exist.")

	// Test for FindTableByPartitionID.
	is := domain.GetDomain(tk.Se).InfoSchema()
	t, dbInfo, _ := is.FindTableByPartitionID(partition.Definitions[0].ID)
	c.Assert(t, NotNil)
	c.Assert(dbInfo, NotNil)
	c.Assert(t.Meta().Name.L, Equals, "t_flash")
	t, dbInfo, _ = is.FindTableByPartitionID(t.Meta().ID)
	c.Assert(t, IsNil)
	c.Assert(dbInfo, IsNil)
	err = failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
	c.Assert(err, IsNil)

	// Test for set replica count more than the tiflash store count.
	tk.MustExec("drop table if exists t_flash;")
	tk.MustExec("create table t_flash(a int, b int)")
	_, err = tk.Exec("alter table t_flash set tiflash replica 2 location labels 'a','b';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "the tiflash replica count: 2 should be less than the total tiflash server count: 0")
}

func (s *testSerialDBSuite) TestForbitCacheTableForSystemTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	sysTables := make([]string, 0, 24)
	memOrSysDB := []string{"MySQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "METRICS_SCHEMA"}
	for _, db := range memOrSysDB {
		tk.MustExec("use " + db)
		tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)
		rows := tk.MustQuery("show tables").Rows()
		for i := 0; i < len(rows); i++ {
			sysTables = append(sysTables, rows[i][0].(string))
		}
		for _, one := range sysTables {
			_, err := tk.Exec(fmt.Sprintf("alter table `%s` cache", one))
			if db == "MySQL" {
				c.Assert(err.Error(), Equals, "[ddl:8200]ALTER table cache for tables in system database is currently unsupported")
			} else {
				c.Assert(err.Error(), Equals, fmt.Sprintf("[planner:1142]ALTER command denied to user 'root'@'%%' for table '%s'", strings.ToLower(one)))
			}

		}
		sysTables = sysTables[:0]
	}
}

func (s *testSerialDBSuite) TestSetTableFlashReplicaForSystemTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	sysTables := make([]string, 0, 24)
	memOrSysDB := []string{"MySQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "METRICS_SCHEMA"}
	for _, db := range memOrSysDB {
		tk.MustExec("use " + db)
		tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)
		rows := tk.MustQuery("show tables").Rows()
		for i := 0; i < len(rows); i++ {
			sysTables = append(sysTables, rows[i][0].(string))
		}
		for _, one := range sysTables {
			_, err := tk.Exec(fmt.Sprintf("alter table `%s` set tiflash replica 1", one))
			if db == "MySQL" {
				c.Assert(err.Error(), Equals, "[ddl:8200]ALTER table replica for tables in system database is currently unsupported")
			} else {
				c.Assert(err.Error(), Equals, fmt.Sprintf("[planner:1142]ALTER command denied to user 'root'@'%%' for table '%s'", strings.ToLower(one)))
			}

		}
		sysTables = sysTables[:0]
	}
}

func (s *testSerialDBSuite) TestSetTiFlashReplicaForTemporaryTable(c *C) {
	// test for tiflash replica
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists temp, temp2")
	tk.MustExec("drop table if exists temp")
	tk.MustExec("create global temporary table temp(id int) on commit delete rows")
	tk.MustExec("create temporary table temp2(id int)")
	tk.MustGetErrCode("alter table temp set tiflash replica 1", errno.ErrOptOnTemporaryTable)
	tk.MustGetErrCode("alter table temp2 set tiflash replica 1", errno.ErrUnsupportedDDLOperation)
	tk.MustExec("drop table temp, temp2")

	tk.MustExec("drop table if exists normal")
	tk.MustExec("create table normal(id int)")
	defer tk.MustExec("drop table normal")
	tk.MustExec("alter table normal set tiflash replica 1")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='normal'").Check(testkit.Rows("1"))
	tk.MustExec("create global temporary table temp like normal on commit delete rows")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='temp'").Check(testkit.Rows())
	tk.MustExec("drop table temp")
	tk.MustExec("create temporary table temp like normal")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='temp'").Check(testkit.Rows())
}

func (s *testSerialDBSuite) TestAlterShardRowIDBits(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()

	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	// Test alter shard_row_id_bits
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 5")
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	tk.MustExec("insert into t1 set a=1;")

	// Test increase shard_row_id_bits failed by overflow global auto ID.
	_, err := tk.Exec("alter table t1 SHARD_ROW_ID_BITS = 10;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:1467]shard_row_id_bits 10 will cause next global auto ID 72057594037932936 overflow")

	// Test reduce shard_row_id_bits will be ok.
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 3;")
	checkShardRowID := func(maxShardRowIDBits, shardRowIDBits uint64) {
		tbl := testGetTableByName(c, tk.Se, "test", "t1")
		c.Assert(tbl.Meta().MaxShardRowIDBits == maxShardRowIDBits, IsTrue)
		c.Assert(tbl.Meta().ShardRowIDBits == shardRowIDBits, IsTrue)
	}
	checkShardRowID(5, 3)

	// Test reduce shard_row_id_bits but calculate overflow should use the max record shard_row_id_bits.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 10")
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 5;")
	checkShardRowID(10, 5)
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	_, err = tk.Exec("insert into t1 set a=1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:1467]Failed to read auto-increment value from storage engine")
}

func (s *testSerialDBSuite) TestShardRowIDBitsOnTemporaryTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// for global temporary table
	tk.MustExec("drop table if exists shard_row_id_temporary")
	_, err := tk.Exec("create global temporary table shard_row_id_temporary (a int) shard_row_id_bits = 5 on commit delete rows;")
	c.Assert(err.Error(), Equals, core.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	tk.MustExec("create global temporary table shard_row_id_temporary (a int) on commit delete rows;")
	defer tk.MustExec("drop table if exists shard_row_id_temporary")
	_, err = tk.Exec("alter table shard_row_id_temporary shard_row_id_bits = 4;")
	c.Assert(err.Error(), Equals, dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	// for local temporary table
	tk.MustExec("drop table if exists local_shard_row_id_temporary")
	_, err = tk.Exec("create temporary table local_shard_row_id_temporary (a int) shard_row_id_bits = 5;")
	c.Assert(err.Error(), Equals, core.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	tk.MustExec("create temporary table local_shard_row_id_temporary (a int);")
	defer tk.MustExec("drop table if exists local_shard_row_id_temporary")
	_, err = tk.Exec("alter table local_shard_row_id_temporary shard_row_id_bits = 4;")
	c.Assert(err.Error(), Equals, dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ALTER TABLE").Error())
}

func (s *testSerialDBSuite) TestSkipSchemaChecker(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	// Test skip schema checker for ActionSetTiFlashReplica.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 set tiflash replica 2 location labels 'a','b';")
	tk.MustExec("commit")

	// Test skip schema checker for ActionUpdateTiFlashReplicaStatus.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tb := testGetTableByName(c, tk.Se, "test", "t1")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("commit")

	// Test can't skip schema checker.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 add column b int;")
	_, err = tk.Exec("commit")
	c.Assert(terror.ErrorEqual(domain.ErrInfoSchemaChanged, err), IsTrue)
}

// TestConcurrentLockTables test concurrent lock/unlock tables.
func (s *testDBSuite4) TestConcurrentLockTables(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk2.MustExec("use test")

	// Test concurrent lock tables read.
	sql1 := "lock tables t1 read"
	sql2 := "lock tables t1 read"
	s.testParallelExecSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
	})
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test concurrent lock tables write.
	sql1 = "lock tables t1 write"
	sql2 = "lock tables t1 write"
	s.testParallelExecSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(terror.ErrorEqual(err2, infoschema.ErrTableLocked), IsTrue)
	})
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test concurrent lock tables write local.
	sql1 = "lock tables t1 write local"
	sql2 = "lock tables t1 write local"
	s.testParallelExecSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(terror.ErrorEqual(err2, infoschema.ErrTableLocked), IsTrue)
	})

	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func (s *testDBSuite4) TestLockTableReadOnly(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	defer func() {
		tk.MustExec("alter table t1 read write")
		tk.MustExec("alter table t2 read write")
		tk.MustExec("drop table if exists t1,t2")
	}()
	tk.MustExec("create table t1 (a int key, b int)")
	tk.MustExec("create table t2 (a int key)")

	tk.MustExec("alter table t1 read only")
	tk.MustQuery("select * from t1")
	tk2.MustQuery("select * from t1")
	_, err := tk.Exec("insert into t1 set a=1, b=2")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk.Exec("update t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk.Exec("delete from t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)

	_, err = tk2.Exec("insert into t1 set a=1, b=2")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("update t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("delete from t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk2.MustExec("alter table t1 read only")
	_, err = tk2.Exec("insert into t1 set a=1, b=2")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("alter table t1 read write")

	tk.MustExec("lock tables t1 read")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("lock tables t1 write")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("lock tables t1 write local")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("unlock tables")

	tk.MustExec("alter table t1 read only")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("lock tables t1 read"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("lock tables t1 read"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk.ExecToErr("lock tables t1 write"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("lock tables t1 write"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk.ExecToErr("lock tables t1 write local"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("lock tables t1 write local"), infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("admin cleanup table lock t1")
	tk2.MustExec("insert into t1 set a=1, b=2")

	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 2"))
	tk2.MustExec("update t1 set b = 3")
	tk2.MustExec("alter table t1 read only")
	tk2.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 3"))
	tk.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 2"))
	tk.MustExec("update t1 set b = 4")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("commit"), domain.ErrInfoSchemaChanged), IsTrue)
	tk2.MustExec("alter table t1 read write")
}

type checkRet func(c *C, err1, err2 error)

func (s *testDBSuite4) testParallelExecSQL(c *C, sql1, sql2 string, se1, se2 session.Session, f checkRet) {
	callback := &ddl.TestDDLCallback{}
	times := 0
	callback.OnJobRunBeforeExported = func(job *model.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			err := kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
				jobs, err1 := admin.GetDDLJobs(txn)
				if err1 != nil {
					return err1
				}
				qLen = len(jobs)
				return nil
			})
			c.Assert(err, IsNil)
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	d.SetHook(callback)

	var wg util.WaitGroupWrapper
	var err1 error
	var err2 error
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DDLJobQueue.
	go func() {
		var qLen int
		for {
			err := kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
				jobs, err3 := admin.GetDDLJobs(txn)
				if err3 != nil {
					return err3
				}
				qLen = len(jobs)
				return nil
			})
			c.Assert(err, IsNil)
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	wg.Run(func() {
		_, err1 = se1.Execute(context.Background(), sql1)
	})
	wg.Run(func() {
		<-ch
		_, err2 = se2.Execute(context.Background(), sql2)
	})

	wg.Wait()
	f(c, err1, err2)
}

func (s *testDBSuite4) TestColumnCheck(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists column_check")
	tk.MustExec("create table column_check (pk int primary key, a int check (a > 1))")
	defer tk.MustExec("drop table if exists column_check")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8231|CONSTRAINT CHECK is not supported"))
}

func (s *testDBSuite5) TestAlterCheck(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists alter_check")
	tk.MustExec("create table alter_check (pk int primary key)")
	defer tk.MustExec("drop table if exists alter_check")
	tk.MustExec("alter table alter_check alter check crcn ENFORCED")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8231|ALTER CHECK is not supported"))
}

func (s *testSerialDBSuite) TestDDLJobErrorCount(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddl_error_table, new_ddl_error_table")
	tk.MustExec("create table ddl_error_table(a int)")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockErrEntrySizeTooLarge", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockErrEntrySizeTooLarge"), IsNil)
	}()

	var jobID int64
	hook := &ddl.TestDDLCallback{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		jobID = job.ID
	}
	originHook := s.dom.DDL().GetHook()
	s.dom.DDL().SetHook(hook)
	defer s.dom.DDL().SetHook(originHook)

	tk.MustGetErrCode("rename table ddl_error_table to new_ddl_error_table", errno.ErrEntryTooLarge)

	historyJob, err := getHistoryDDLJob(s.store, jobID)
	c.Assert(err, IsNil)
	c.Assert(historyJob, NotNil)
	c.Assert(historyJob.ErrorCount, Equals, int64(1), Commentf("%v", historyJob))
	kv.ErrEntryTooLarge.Equal(historyJob.Error)
}

func (s *testSerialDBSuite) TestCommitTxnWithIndexChange(c *C) {
	// Prepare work.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
	tk.MustExec("insert t1 values (1, 10, 100), (2, 20, 200)")
	tk.MustExec("alter table t1 add index k2(c2)")
	tk.MustExec("alter table t1 drop index k2")
	tk.MustExec("alter table t1 add index k2(c2)")
	tk.MustExec("alter table t1 drop index k2")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test_db")

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
	do := s.dom.DDL()
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
								_, err := tk.Exec("commit")
								c.Assert(err, NotNil)
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
func (s *testSerialDBSuite) TestAddIndexFailOnCaseWhenCanExit(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/MockCaseWhenParseFailure", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/MockCaseWhenParseFailure"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	originalVal := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", originalVal))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 1)")
	_, err := tk.Exec("alter table t add index idx(b)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:-1]DDL job rollback, error msg: job.ErrCount:1, mock unknown type: ast.whenClause.")
	tk.MustExec("drop table if exists t")
}

func (s *testSerialDBSuite) TestCreateTableWithIntegerLengthWaring(c *C) {
	// Inject the strict-integer-display-width variable in parser directly.
	parsertypes.TiDBStrictIntegerDisplayWidth = true
	defer func() {
		parsertypes.TiDBStrictIntegerDisplayWidth = false
	}()
	tk := testkit.NewTestKit(c, s.store)
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

func (s *testSerialDBSuite) TestColumnTypeChangeGenUniqueChangingName(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	hook := &ddl.TestDDLCallback{}
	var checkErr error
	assertChangingColName := "_col$_c2_0"
	assertChangingIdxName := "_idx$_idx_0"
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionModifyColumn {
			var (
				newCol                *model.ColumnInfo
				oldColName            *model.CIStr
				modifyColumnTp        byte
				updatedAutoRandomBits uint64
				changingCol           *model.ColumnInfo
				changingIdxs          []*model.IndexInfo
			)
			pos := &ast.ColumnPosition{}
			err := job.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
			if err != nil {
				checkErr = err
				return
			}
			if changingCol.Name.L != assertChangingColName {
				checkErr = errors.New("changing column name is incorrect")
			} else if changingIdxs[0].Name.L != assertChangingIdxName {
				checkErr = errors.New("changing index name is incorrect")
			}
		}
	}
	d := s.dom.DDL()
	originHook := d.GetHook()
	d.SetHook(hook)
	defer d.SetHook(originHook)

	tk.MustExec("create table if not exists t(c1 varchar(256), c2 bigint, `_col$_c2` varchar(10), unique _idx$_idx(c1), unique idx(c2));")
	tk.MustExec("alter table test.t change column c2 cC2 tinyint after `_col$_c2`")
	c.Assert(checkErr, IsNil)

	t := testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(len(t.Meta().Columns), Equals, 3)
	c.Assert(t.Meta().Columns[0].Name.O, Equals, "c1")
	c.Assert(t.Meta().Columns[0].Offset, Equals, 0)
	c.Assert(t.Meta().Columns[1].Name.O, Equals, "_col$_c2")
	c.Assert(t.Meta().Columns[1].Offset, Equals, 1)
	c.Assert(t.Meta().Columns[2].Name.O, Equals, "cC2")
	c.Assert(t.Meta().Columns[2].Offset, Equals, 2)

	c.Assert(len(t.Meta().Indices), Equals, 2)
	c.Assert(t.Meta().Indices[0].Name.O, Equals, "_idx$_idx")
	c.Assert(t.Meta().Indices[1].Name.O, Equals, "idx")

	c.Assert(len(t.Meta().Indices[0].Columns), Equals, 1)
	c.Assert(t.Meta().Indices[0].Columns[0].Name.O, Equals, "c1")
	c.Assert(t.Meta().Indices[0].Columns[0].Offset, Equals, 0)

	c.Assert(len(t.Meta().Indices[1].Columns), Equals, 1)
	c.Assert(t.Meta().Indices[1].Columns[0].Name.O, Equals, "cC2")
	c.Assert(t.Meta().Indices[1].Columns[0].Offset, Equals, 2)

	assertChangingColName1 := "_col$__col$_c1_1"
	assertChangingColName2 := "_col$__col$__col$_c1_0_1"
	query1 := "alter table t modify column _col$_c1 tinyint"
	query2 := "alter table t modify column _col$__col$_c1_0 tinyint"
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if (job.Query == query1 || job.Query == query2) && job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionModifyColumn {
			var (
				newCol                *model.ColumnInfo
				oldColName            *model.CIStr
				modifyColumnTp        byte
				updatedAutoRandomBits uint64
				changingCol           *model.ColumnInfo
				changingIdxs          []*model.IndexInfo
			)
			pos := &ast.ColumnPosition{}
			err := job.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
			if err != nil {
				checkErr = err
				return
			}
			if job.Query == query1 && changingCol.Name.L != assertChangingColName1 {
				checkErr = errors.New("changing column name is incorrect")
			}
			if job.Query == query2 && changingCol.Name.L != assertChangingColName2 {
				checkErr = errors.New("changing column name is incorrect")
			}
		}
	}
	d.SetHook(hook)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table if not exists t(c1 bigint, _col$_c1 bigint, _col$__col$_c1_0 bigint, _col$__col$__col$_c1_0_0 bigint)")
	tk.MustExec("alter table t modify column c1 tinyint")
	tk.MustExec("alter table t modify column _col$_c1 tinyint")
	c.Assert(checkErr, IsNil)
	tk.MustExec("alter table t modify column _col$__col$_c1_0 tinyint")
	c.Assert(checkErr, IsNil)
	tk.MustExec("alter table t change column _col$__col$__col$_c1_0_0  _col$__col$__col$_c1_0_0 tinyint")

	t = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(len(t.Meta().Columns), Equals, 4)
	c.Assert(t.Meta().Columns[0].Name.O, Equals, "c1")
	c.Assert(t.Meta().Columns[0].Tp, Equals, mysql.TypeTiny)
	c.Assert(t.Meta().Columns[0].Offset, Equals, 0)
	c.Assert(t.Meta().Columns[1].Name.O, Equals, "_col$_c1")
	c.Assert(t.Meta().Columns[1].Tp, Equals, mysql.TypeTiny)
	c.Assert(t.Meta().Columns[1].Offset, Equals, 1)
	c.Assert(t.Meta().Columns[2].Name.O, Equals, "_col$__col$_c1_0")
	c.Assert(t.Meta().Columns[2].Tp, Equals, mysql.TypeTiny)
	c.Assert(t.Meta().Columns[2].Offset, Equals, 2)
	c.Assert(t.Meta().Columns[3].Name.O, Equals, "_col$__col$__col$_c1_0_0")
	c.Assert(t.Meta().Columns[3].Tp, Equals, mysql.TypeTiny)
	c.Assert(t.Meta().Columns[3].Offset, Equals, 3)

	tk.MustExec("drop table if exists t")
}

func (s *testDBSuite4) TestGeneratedColumnWindowFunction(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustGetErrCode("CREATE TABLE t (a INT , b INT as (ROW_NUMBER() OVER (ORDER BY a)))", errno.ErrWindowInvalidWindowFuncUse)
	tk.MustGetErrCode("CREATE TABLE t (a INT , index idx ((ROW_NUMBER() OVER (ORDER BY a))))", errno.ErrWindowInvalidWindowFuncUse)
}

func (s *testDBSuite4) TestAnonymousIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("create table t(bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb int, b int);")
	tk.MustExec("alter table t add index bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb(b);")
	tk.MustExec("alter table t add index (bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb);")
	res := tk.MustQuery("show index from t where key_name='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';")
	c.Assert(len(res.Rows()), Equals, 1)
	res = tk.MustQuery("show index from t where key_name='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb_2';")
	c.Assert(len(res.Rows()), Equals, 1)
}

func (s *testDBSuite4) TestUnsupportedAlterTableOption(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("create table t(a char(10) not null,b char(20)) shard_row_id_bits=6;")
	tk.MustGetErrCode("alter table t pre_split_regions=6;", errno.ErrUnsupportedDDLOperation)
}

func (s *testDBSuite4) TestCreateTableWithDecimalWithDoubleZero(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	checkType := func(db, table, field string) {
		ctx := tk.Se.(sessionctx.Context)
		is := domain.GetDomain(ctx).InfoSchema()
		tableInfo, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(table))
		c.Assert(err, IsNil)
		tblInfo := tableInfo.Meta()
		for _, col := range tblInfo.Columns {
			if col.Name.L == field {
				c.Assert(col.Flen, Equals, 10)
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

	/*
		Currently not support change column to decimal
		tk.MustExec("drop table tt")
		tk.MustExec("create table tt(d int)")
		tk.MustExec("alter table tt change column d d decimal(0, 0)")
		checkType("test", "tt", "d")
	*/
}

// Close issue #24172.
// See https://github.com/pingcap/tidb/issues/24172
func (s *testSerialDBSuite) TestCancelJobWriteConflict(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int)")

	var cancelErr error
	var rs []sqlexec.RecordSet
	hook := &ddl.TestDDLCallback{}
	d := s.dom.DDL()
	originalHook := d.GetHook()
	d.SetHook(hook)
	defer d.SetHook(originalHook)

	// Test when cancelling cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			stmt := fmt.Sprintf("admin cancel ddl jobs %d", job.ID)
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn", `return("no_retry")`), IsNil)
			defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn"), IsNil) }()
			rs, cancelErr = tk1.Se.Execute(context.Background(), stmt)
		}
	}
	tk.MustExec("alter table t add index (id)")
	c.Assert(cancelErr.Error(), Equals, "mock commit error")

	// Test when cancelling is retried only once and adding index is cancelled in the end.
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			jobID = job.ID
			stmt := fmt.Sprintf("admin cancel ddl jobs %d", job.ID)
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn", `return("retry_once")`), IsNil)
			defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn"), IsNil) }()
			rs, cancelErr = tk1.Se.Execute(context.Background(), stmt)
		}
	}
	tk.MustGetErrCode("alter table t add index (id)", errno.ErrCancelledDDLJob)
	c.Assert(cancelErr, IsNil)
	result := tk1.ResultSetToResultWithCtx(context.Background(), rs[0], Commentf("cancel ddl job fails"))
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID)))
}

func (s *testSerialDBSuite) TestAddGeneratedColumnAndInsert(c *C) {
	// For issue #31735.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, unique kye(a))")
	tk.MustExec("insert into t1 value (1), (10)")

	var checkErr error
	tk1 := testkit.NewTestKit(c, s.store)
	_, checkErr = tk1.Exec("use test_db")

	d := s.dom.DDL()
	hook := &ddl.TestDDLCallback{Do: s.dom}
	ctx := mock.NewContext()
	ctx.Store = s.store
	times := 0
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (1) on duplicate key update a=a+1")
			if checkErr == nil {
				_, checkErr = tk1.Exec("replace into t1 values (2)")
			}
		case model.StateWriteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (2) on duplicate key update a=a+1")
			if checkErr == nil {
				_, checkErr = tk1.Exec("replace into t1 values (3)")
			}
		case model.StateWriteReorganization:
			if checkErr == nil && job.SchemaState == model.StateWriteReorganization && times == 0 {
				_, checkErr = tk1.Exec("insert into t1 values (3) on duplicate key update a=a+1")
				if checkErr == nil {
					_, checkErr = tk1.Exec("replace into t1 values (4)")
				}
				times++
			}
		}
	}
	d.SetHook(hook)

	tk.MustExec("alter table t1 add column gc int as ((a+1))")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("4 5", "10 11"))
	c.Assert(checkErr, IsNil)
}
