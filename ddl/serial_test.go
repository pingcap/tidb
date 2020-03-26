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
	"github.com/pingcap/tidb/config"
	"math"
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
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
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	dom       *domain.Domain
}

func (s *testSerialSuite) SetUpSuite(c *C) {
	session.SetSchemaLease(200 * time.Millisecond)
	session.DisableStats4Test()

	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	// Test for add/drop primary key.
	newCfg.AlterPrimaryKey = false
	config.StoreGlobalConfig(&newCfg)

	s.cluster = mocktikv.NewCluster()
	s.mvccStore = mocktikv.MustNewMVCCStore()

	ddl.WaitTimeWhenErrorOccured = 1 * time.Microsecond
	var err error
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

func (s *testSerialSuite) TestChangeMaxIndexLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	originalMaxIndexLen := cfg.MaxIndexLength
	newCfg.MaxIndexLength = config.DefMaxOfMaxIndexLength
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		newCfg.MaxIndexLength = originalMaxIndexLen
		config.StoreGlobalConfig(&newCfg)
	}()

	tk.MustExec("create table t (c1 varchar(3073), index(c1)) charset = ascii;")
	tk.MustExec(fmt.Sprintf("create table t1 (c1 varchar(%d), index(c1)) charset = ascii;", config.DefMaxOfMaxIndexLength))
	_, err := tk.Exec(fmt.Sprintf("create table t2 (c1 varchar(%d), index(c1)) charset = ascii;", config.DefMaxOfMaxIndexLength+1))
	c.Assert(err.Error(), Equals, "[ddl:1071]Specified key was too long; max key length is 12288 bytes")
	tk.MustExec("drop table t, t1")
}

func (s *testSerialSuite) TestPrimaryKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table primary_key_test (a int, b varchar(10))")
	tk.MustExec("create table primary_key_test_1 (a int, b varchar(10), primary key(a))")
	_, err := tk.Exec("alter table primary_key_test add primary key(a)")
	c.Assert(ddl.ErrUnsupportedModifyPrimaryKey.Equal(err), IsTrue)
	_, err = tk.Exec("alter table primary_key_test drop primary key")
	c.Assert(err.Error(), Equals, "[ddl:206]Unsupported drop primary key when alter-primary-key is false")
	// for "drop index `primary` on ..." syntax
	_, err = tk.Exec("drop index `primary` on primary_key_test")
	c.Assert(err.Error(), Equals, "[ddl:206]Unsupported drop primary key when alter-primary-key is false")
	_, err = tk.Exec("drop index `primary` on primary_key_test_1")
	c.Assert(err.Error(), Equals, "[ddl:206]Unsupported drop primary key when alter-primary-key is false")

	// Change the value of AlterPrimaryKey.
	tk.MustExec("create table primary_key_test1 (a int, b varchar(10), primary key(a))")
	tk.MustExec("create table primary_key_test2 (a int, b varchar(10), primary key(b))")
	tk.MustExec("create table primary_key_test3 (a int, b varchar(10))")
	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	orignalAlterPrimaryKey := newCfg.AlterPrimaryKey
	newCfg.AlterPrimaryKey = true
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		newCfg.AlterPrimaryKey = orignalAlterPrimaryKey
		config.StoreGlobalConfig(&newCfg)
	}()

	_, err = tk.Exec("alter table primary_key_test1 drop primary key")
	c.Assert(err.Error(), Equals, "[ddl:206]Unsupported drop primary key when the table's pkIsHandle is true")
	tk.MustExec("alter table primary_key_test2 drop primary key")
	_, err = tk.Exec("alter table primary_key_test3 drop primary key")
	c.Assert(err.Error(), Equals, "[ddl:1091]Can't DROP 'PRIMARY'; check that column/key exists")

	// for "drop index `primary` on ..." syntax
	tk.MustExec("create table primary_key_test4 (a int, b varchar(10), primary key(a))")
	newCfg.AlterPrimaryKey = false
	config.StoreGlobalConfig(&newCfg)
	_, err = tk.Exec("drop index `primary` on primary_key_test4")
	c.Assert(err.Error(), Equals, "[ddl:206]Unsupported drop primary key when alter-primary-key is false")
	// for the index name is `primary`
	tk.MustExec("create table tt(`primary` int);")
	tk.MustExec("alter table tt add index (`primary`);")
	_, err = tk.Exec("drop index `primary` on tt")
	c.Assert(err.Error(), Equals, "[ddl:206]Unsupported drop primary key when alter-primary-key is false")
}

func (s *testSerialSuite) TestMultiRegionGetTableEndHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")

	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DDL()
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, 100)

	maxID, emptyTable := getMaxTableRowID(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(999))

	tk.MustExec("insert into t values(10000, 1000)")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(10000))

	tk.MustExec("insert into t values(-1, 1000)")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(10000))
}

func (s *testSerialSuite) TestGetTableEndHandle(c *C) {
	// TestGetTableEndHandle test ddl.GetTableMaxRowID method, which will return the max row id of the table.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	// Test PK is handle.
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")

	is := s.dom.InfoSchema()
	d := s.dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	testCtx := newTestMaxTableRowIDContext(c, d, tbl)
	// test empty table
	checkGetMaxTableRowID(testCtx, s.store, true, int64(math.MaxInt64))

	tk.MustExec("insert into t values(-1, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, int64(-1))

	tk.MustExec("insert into t values(9223372036854775806, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, int64(9223372036854775806))

	tk.MustExec("insert into t values(9223372036854775807, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, int64(9223372036854775807))

	tk.MustExec("insert into t values(10, 1)")
	tk.MustExec("insert into t values(102149142, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, int64(9223372036854775807))

	tk.MustExec("create table t1(a bigint PRIMARY KEY, b int)")

	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
	}
	is = s.dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	checkGetMaxTableRowID(testCtx, s.store, false, int64(999))

	// Test PK is not handle
	tk.MustExec("create table t2(a varchar(255))")

	is = s.dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	checkGetMaxTableRowID(testCtx, s.store, true, int64(math.MaxInt64))

	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", i))
	}

	result := tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable := getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec("insert into t2 values(100000)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64-1))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec("insert into t2 values(100)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)
}

func (s *testSerialSuite) TestCreateTableWithLike(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// for the same database
	tk.MustExec("create database ctwl_db")
	tk.MustExec("use ctwl_db")
	tk.MustExec("create table tt(id int primary key)")
	tk.MustExec("create table t (c1 int not null auto_increment, c2 int, constraint cc foreign key (c2) references tt(id), primary key(c1)) auto_increment = 10")
	tk.MustExec("insert into t set c2=1")
	tk.MustExec("create table t1 like ctwl_db.t")
	tk.MustExec("insert into t1 set c2=11")
	tk.MustExec("create table t2 (like ctwl_db.t1)")
	tk.MustExec("insert into t2 set c2=12")
	tk.MustQuery("select * from t").Check(testkit.Rows("10 1"))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 12"))
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("ctwl_db"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tbl1Info := tbl1.Meta()
	c.Assert(tbl1Info.ForeignKeys, IsNil)
	c.Assert(tbl1Info.PKIsHandle, Equals, true)
	col := tbl1Info.Columns[0]
	hasNotNull := mysql.HasNotNullFlag(col.Flag)
	c.Assert(hasNotNull, IsTrue)
	tbl2, err := is.TableByName(model.NewCIStr("ctwl_db"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tbl2Info := tbl2.Meta()
	c.Assert(tbl2Info.ForeignKeys, IsNil)
	c.Assert(tbl2Info.PKIsHandle, Equals, true)
	c.Assert(mysql.HasNotNullFlag(tbl2Info.Columns[0].Flag), IsTrue)

	// for different databases
	tk.MustExec("create database ctwl_db1")
	tk.MustExec("use ctwl_db1")
	tk.MustExec("create table t1 like ctwl_db.t")
	tk.MustExec("insert into t1 set c2=11")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl1, err = is.TableByName(model.NewCIStr("ctwl_db1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(tbl1.Meta().ForeignKeys, IsNil)

	// for table partition
	tk.MustExec("use ctwl_db")
	tk.MustExec("create table pt1 (id int) partition by range columns (id) (partition p0 values less than (10))")
	tk.MustExec("insert into pt1 values (1),(2),(3),(4);")
	tk.MustExec("create table ctwl_db1.pt1 like ctwl_db.pt1;")
	tk.MustQuery("select * from ctwl_db1.pt1").Check(testkit.Rows())

	// for failure cases
	failSQL := fmt.Sprintf("create table t1 like test_not_exist.t")
	tk.MustGetErrCode(failSQL, mysql.ErrNoSuchTable)
	failSQL = fmt.Sprintf("create table t1 like test.t_not_exist")
	tk.MustGetErrCode(failSQL, mysql.ErrNoSuchTable)
	failSQL = fmt.Sprintf("create table t1 (like test_not_exist.t)")
	tk.MustGetErrCode(failSQL, mysql.ErrNoSuchTable)
	failSQL = fmt.Sprintf("create table test_not_exis.t1 like ctwl_db.t")
	tk.MustGetErrCode(failSQL, mysql.ErrBadDB)
	failSQL = fmt.Sprintf("create table t1 like ctwl_db.t")
	tk.MustGetErrCode(failSQL, mysql.ErrTableExists)

	tk.MustExec("drop database ctwl_db")
	tk.MustExec("drop database ctwl_db1")
}

// TestCancelAddIndex1 tests canceling ddl job when the add index worker is not started.
func (s *testSerialSuite) TestCancelAddIndexPanic(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/errorMockPanic", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/errorMockPanic"), IsNil)
	}()
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

func (s *testSerialSuite) TestRecoverTableByJobID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
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

	getDDLJobID := func(table, tp string) int64 {
		rs, err := tk.Exec("admin show ddl jobs")
		c.Assert(err, IsNil)
		rows, err := session.GetRows4Test(context.Background(), tk.Se, rs)
		c.Assert(err, IsNil)
		for _, row := range rows {
			if row.GetString(1) == table && row.GetString(3) == tp {
				return row.GetInt64(0)
			}
		}
		c.Errorf("can't find %s table of %s", tp, table)
		return -1
	}
	jobID := getDDLJobID("test_recover", "drop table")

	// if GC safe point is not exists in mysql.tidb
	_, err := tk.Exec(fmt.Sprintf("recover table by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "can not get 'tikv_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// if GC enable is not exists in mysql.tidb
	_, err = tk.Exec(fmt.Sprintf("recover table by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:-1]can not get 'tikv_gc_enable'")

	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeAfterDrop))
	_, err = tk.Exec(fmt.Sprintf("recover table by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "snapshot is older than GC safe point"), Equals, true)

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// if there is a new table with the same name, should return failed.
	tk.MustExec("create table t_recover (a int);")
	_, err = tk.Exec(fmt.Sprintf("recover table by job %d", jobID))
	c.Assert(err.Error(), Equals, infoschema.ErrTableExists.GenWithStackByArgs("t_recover").Error())

	// drop the new table with the same name, then recover table.
	tk.MustExec("drop table t_recover")

	// do recover table.
	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))

	// recover table by none exits job.
	_, err = tk.Exec(fmt.Sprintf("recover table by job %d", 10000000))
	c.Assert(err, NotNil)

	// Disable GC by manual first, then after recover table, the GC enable status should also be disabled.
	err = gcutil.DisableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop table t_recover")
	jobID = getDDLJobID("test_recover", "drop table")

	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9"))

	// Test for recover truncate table.
	tk.MustExec("truncate table t_recover")
	tk.MustExec("rename table t_recover to t_recover_new2")
	jobID = getDDLJobID("test_recover", "truncate table")
	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))
	tk.MustExec("insert into t_recover values (10)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9", "10"))

	gcEnable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(gcEnable, Equals, false)
}

func (s *testSerialSuite) TestRecoverTableByTableName(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
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
	_, err := tk.Exec("recover table t_recover")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "can not get 'tikv_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// if GC enable is not exists in mysql.tidb
	_, err = tk.Exec("recover table t_recover")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:-1]can not get 'tikv_gc_enable'")

	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeAfterDrop))
	_, err = tk.Exec("recover table t_recover")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "snapshot is older than GC safe point"), Equals, true)

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// if there is a new table with the same name, should return failed.
	tk.MustExec("create table t_recover (a int);")
	_, err = tk.Exec("recover table t_recover")
	c.Assert(err.Error(), Equals, infoschema.ErrTableExists.GenWithStackByArgs("t_recover").Error())

	// drop the new table with the same name, then recover table.
	tk.MustExec("rename table t_recover to t_recover2")

	// do recover table.
	tk.MustExec("recover table t_recover")

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_recover;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003"))

	// recover table by none exits job.
	_, err = tk.Exec(fmt.Sprintf("recover table by job %d", 10000000))
	c.Assert(err, NotNil)

	// Disable GC by manual first, then after recover table, the GC enable status should also be disabled.
	err = gcutil.DisableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop table t_recover")

	tk.MustExec("recover table t_recover")

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9"))

	// Recover truncate table.
	tk.MustExec("truncate table t_recover")
	tk.MustExec("rename table t_recover to t_recover_new1")
	tk.MustExec("recover table t_recover")
	tk.MustExec("insert into t_recover values (10)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9", "10"))

	gcEnable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(gcEnable, Equals, false)
}

func (s *testSerialSuite) TestRecoverTableByJobIDFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
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
	c.Assert(row.GetString(1), Equals, "test_recover")
	c.Assert(row.GetString(3), Equals, "drop table")
	jobID := row.GetInt64(0)

	// enableGC first
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// set hook
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionRecoverTable {
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockCommitError", `return(true)`), IsNil)
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockRecoverTableCommitErr", `return(true)`), IsNil)
		}
	}
	origHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	// do recover table.
	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockCommitError"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockRecoverTableCommitErr"), IsNil)

	// make sure enable GC after recover table.
	enable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(enable, Equals, true)

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func (s *testSerialSuite) TestRecoverTableByTableNameFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
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
		if job.Type == model.ActionRecoverTable {
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockCommitError", `return(true)`), IsNil)
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockRecoverTableCommitErr", `return(true)`), IsNil)
		}
	}
	origHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	// do recover table.
	tk.MustExec("recover table t_recover")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockCommitError"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockRecoverTableCommitErr"), IsNil)

	// make sure enable GC after recover table.
	enable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(enable, Equals, true)

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func (s *testSerialSuite) TestCancelJobByErrorCountLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit"), IsNil)
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	_, err := tk.Exec("create table t (a int)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
}

func (s *testSerialSuite) TestCanceledJobTakeTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_cjtt(a int)")

	hook := &ddl.TestDDLCallback{}
	once := sync.Once{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		once.Do(func() {
			err := kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
				t := meta.NewMeta(txn)
				return t.DropTableOrView(job.SchemaID, job.TableID, true)
			})
			c.Assert(err, IsNil)
		})
	}
	origHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)

	originalWT := ddl.WaitTimeWhenErrorOccured
	ddl.WaitTimeWhenErrorOccured = 1 * time.Second
	defer func() { ddl.WaitTimeWhenErrorOccured = originalWT }()
	startTime := time.Now()
	tk.MustGetErrCode("alter table t_cjtt add column b int", mysql.ErrNoSuchTable)
	sub := time.Since(startTime)
	c.Assert(sub, Less, ddl.WaitTimeWhenErrorOccured)
}
