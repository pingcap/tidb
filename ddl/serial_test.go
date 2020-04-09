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
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

// Make it serial because config is modified in test cases.
var _ = SerialSuites(&testSerialSuite{})

type testSerialSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testSerialSuite) SetUpSuite(c *C) {
	session.SetSchemaLease(200 * time.Millisecond)
	session.DisableStats4Test()

	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	// Test for add/drop primary key.
	newCfg.AlterPrimaryKey = false
	config.StoreGlobalConfig(&newCfg)

	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
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
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported drop primary key when alter-primary-key is false")
	// for "drop index `primary` on ..." syntax
	_, err = tk.Exec("drop index `primary` on primary_key_test")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported drop primary key when alter-primary-key is false")
	_, err = tk.Exec("drop index `primary` on primary_key_test_1")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported drop primary key when alter-primary-key is false")

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
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported drop primary key when the table's pkIsHandle is true")
	tk.MustExec("alter table primary_key_test2 drop primary key")
	_, err = tk.Exec("alter table primary_key_test3 drop primary key")
	c.Assert(err.Error(), Equals, "[ddl:1091]Can't DROP 'PRIMARY'; check that column/key exists")

	// for "drop index `primary` on ..." syntax
	tk.MustExec("create table primary_key_test4 (a int, b varchar(10), primary key(a))")
	newCfg.AlterPrimaryKey = false
	config.StoreGlobalConfig(&newCfg)
	_, err = tk.Exec("drop index `primary` on primary_key_test4")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported drop primary key when alter-primary-key is false")
	// for the index name is `primary`
	tk.MustExec("create table tt(`primary` int);")
	tk.MustExec("alter table tt add index (`primary`);")
	_, err = tk.Exec("drop index `primary` on tt")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported drop primary key when alter-primary-key is false")
}

func (s *testSerialSuite) TestDropAutoIncrementIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int(11) not null auto_increment key, b int(11), c bigint, unique key (a, b, c))")
	tk.MustExec("alter table t1 drop index a")
}

func (s *testSerialSuite) TestMultiRegionGetTableEndHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")

	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DDL()
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// Split the table.
	cluster := mocktikv.NewCluster()
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()
	cluster.SplitTable(mvccStore, tblID, 100)

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

	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t1 values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

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

	builder.Reset()
	fmt.Fprintf(&builder, "insert into t2 values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v),", i)
	}
	sql = builder.String()
	tk.MustExec(sql[:len(sql)-1])

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

	// Test create table like for partition table.
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	tk.MustExec("drop table if exists partition_t;")
	tk.MustExec("create table partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 like partition_t")
	re := tk.MustQuery("show table t1 regions")
	rows := re.Rows()
	c.Assert(len(rows), Equals, 3)
	tbl := testGetTableByName(c, tk.Se, "test", "t1")
	partitionDef := tbl.Meta().GetPartitionInfo().Definitions
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[0].ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[1].ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[2].ID))

	// Test pre-split table region when create table like.
	tk.MustExec("drop table if exists t_pre")
	tk.MustExec("create table t_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 like t_pre")
	re = tk.MustQuery("show table t2 regions")
	rows = re.Rows()
	// Table t2 which create like t_pre should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	tbl = testGetTableByName(c, tk.Se, "test", "t2")
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID))
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)

	// for failure cases
	tk.MustExec("use ctwl_db")
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
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
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
	tk.MustExec("rename table t_recover to t_recover_new")
	jobID = getDDLJobID("test_recover", "truncate table")
	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))
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

	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 16")
	err := ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))

	_, err = tk.Exec("create table t (a int)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:-1]current error msg: Cancelled DDL job, original error msg: mock do job error")
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

	originalWT := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Second)
	defer func() { ddl.SetWaitTimeWhenErrorOccurred(originalWT) }()
	startTime := time.Now()
	tk.MustGetErrCode("alter table t_cjtt add column b int", mysql.ErrNoSuchTable)
	sub := time.Since(startTime)
	c.Assert(sub, Less, ddl.GetWaitTimeWhenErrorOccurred())
}

func (s *testSerialSuite) TestTableLocksEnable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")

	// Test for enable table lock config.
	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	newCfg.EnableTableLock = false
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(cfg)
	}()

	tk.MustExec("lock tables t1 write")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockNone)
}

func (s *testSerialSuite) TestAutoRandom(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists auto_random_db")
	defer tk.MustExec("drop database if exists auto_random_db")
	tk.MustExec("use auto_random_db")
	tk.MustExec("drop table if exists t")

	assertInvalidAutoRandomErr := func(sql string, errMsg string, args ...interface{}) {
		_, err := tk.Exec(sql)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, ddl.ErrInvalidAutoRandom.GenWithStackByArgs(fmt.Sprintf(errMsg, args...)).Error())
	}

	assertPKIsNotHandle := func(sql, errCol string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomPKisNotHandleErrMsg, errCol)
	}
	assertExperimentDisabled := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomExperimentalDisabledErrMsg)
	}
	assertAlterValue := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomAlterErrMsg)
	}
	assertWithAutoInc := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
	}
	assertOverflow := func(sql, colType string, autoRandBits, maxFieldLength uint64) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomOverflowErrMsg, colType, maxFieldLength, autoRandBits, colType, maxFieldLength-1)
	}
	assertModifyColType := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomModifyColTypeErrMsg)
	}
	assertDefault := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
	}
	assertNonPositive := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomNonPositive)
	}
	mustExecAndDrop := func(sql string, fns ...func()) {
		tk.MustExec(sql)
		for _, f := range fns {
			f()
		}
		tk.MustExec("drop table t")
	}

	testutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer testutil.ConfigTestUtils.RestoreAutoRandomTestConfig()
	// PKIsHandle, but auto_random is defined on non-primary key.
	assertPKIsNotHandle("create table t (a bigint auto_random (3) primary key, b int auto_random (3))", "b")
	assertPKIsNotHandle("create table t (a bigint auto_random (3), b int auto_random(3), primary key(a))", "b")
	assertPKIsNotHandle("create table t (a bigint auto_random (3), b int auto_random(3) primary key)", "a")

	// PKIsNotHandle: no primary key.
	assertPKIsNotHandle("create table t (a int auto_random(3), b int)", "a")
	assertPKIsNotHandle("create table t (a bigint auto_random(3), b int)", "a")
	// PKIsNotHandle: primary key is not integer column.
	assertPKIsNotHandle("create table t (a char primary key auto_random(3), b int)", "a")
	assertPKIsNotHandle("create table t (a varchar(255) primary key auto_random(3), b int)", "a")
	assertPKIsNotHandle("create table t (a timestamp primary key auto_random(3), b int)", "a")
	// PKIsNotHandle: primary key is not a single column.
	assertPKIsNotHandle("create table t (a bigint auto_random(3), b int, primary key (a, b))", "a")
	assertPKIsNotHandle("create table t (a int auto_random(3), b int, c char, primary key (a, c))", "a")

	// Can not set auto_random along with auto_increment.
	assertWithAutoInc("create table t (a bigint auto_random(3) primary key auto_increment)")
	assertWithAutoInc("create table t (a bigint primary key auto_increment auto_random(3))")
	assertWithAutoInc("create table t (a bigint auto_increment primary key auto_random(3))")
	assertWithAutoInc("create table t (a bigint auto_random(3) auto_increment, primary key (a))")

	// Overflow data type max length.
	assertOverflow("create table t (a bigint auto_random(65) primary key)", "a", 65, 64)
	assertOverflow("create table t (a int auto_random(33) primary key)", "a", 33, 32)
	assertOverflow("create table t (a mediumint auto_random(25) primary key)", "a", 25, 24)
	assertOverflow("create table t (a smallint auto_random(17) primary key)", "a", 17, 16)
	assertOverflow("create table t (a tinyint auto_random(9) primary key)", "a", 9, 8)

	assertNonPositive("create table t (a bigint auto_random(0) primary key)")

	// Can not set auto_random along with default.
	assertDefault("create table t (a int auto_random primary key default 3)")
	assertDefault("create table t (a bigint auto_random(2) primary key default 5)")
	mustExecAndDrop("create table t (a int auto_random primary key)", func() {
		assertDefault("alter table t modify column a int auto_random default 3")
	})

	// Basic usage.
	mustExecAndDrop("create table t (a bigint auto_random(4) primary key, b varchar(255))")
	mustExecAndDrop("create table t (a bigint primary key auto_random(4), b varchar(255))")
	mustExecAndDrop("create table t (a bigint auto_random(4), b varchar(255), primary key (a))")

	// Different primary key field types.
	mustExecAndDrop("create table t (a bigint auto_random(4) primary key)")
	mustExecAndDrop("create table t (a int auto_random(4) primary key)")
	mustExecAndDrop("create table t (a mediumint auto_random(4) primary key)")
	mustExecAndDrop("create table t (a smallint auto_random(4) primary key)")
	mustExecAndDrop("create table t (a tinyint auto_random(4) primary key)")

	// Auto_random can occur multiple times like other column attributes.
	mustExecAndDrop("create table t (a bigint auto_random(3) auto_random(2) primary key)")
	mustExecAndDrop("create table t (a int, b bigint auto_random(3) primary key auto_random(2))")
	mustExecAndDrop("create table t (a int auto_random(1) auto_random(2) auto_random(3), primary key (a))")

	// Add/drop the auto_random attribute is not allowed.
	mustExecAndDrop("create table t (a bigint auto_random(3) primary key)", func() {
		assertAlterValue("alter table t modify column a bigint")
		assertAlterValue("alter table t change column a b bigint")
	})
	mustExecAndDrop("create table t (a int, b char, c int auto_random(3), primary key(c))", func() {
		assertAlterValue("alter table t modify column c bigint")
		assertAlterValue("alter table t change column c d bigint")
	})
	mustExecAndDrop("create table t (a bigint primary key)", func() {
		assertAlterValue("alter table t modify column a bigint auto_random(3)")
		assertAlterValue("alter table t change column a b bigint auto_random(3)")
	})

	// Modifying the field type of a auto_random column is not allowed.
	mustExecAndDrop("create table t (a tinyint primary key auto_random(3))", func() {
		assertModifyColType("alter table t modify column a int auto_random(3)")
		assertModifyColType("alter table t modify column a mediumint auto_random(3)")
		assertModifyColType("alter table t modify column a smallint auto_random(3)")
	})

	// Test show warnings when create auto_random table.
	assertShowWarningCorrect := func(sql string, times int) {
		mustExecAndDrop(sql, func() {
			note := fmt.Sprintf(autoid.AutoRandomAvailableAllocTimesNote, times)
			result := fmt.Sprintf("Note|1105|%s", note)
			tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", result))
			c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))
		})
	}
	assertShowWarningCorrect("create table t (a tinyint unsigned auto_random(6) primary key)", 1)
	assertShowWarningCorrect("create table t (a tinyint unsigned auto_random(5) primary key)", 3)
	assertShowWarningCorrect("create table t (a tinyint auto_random(4) primary key)", 7)
	assertShowWarningCorrect("create table t (a bigint auto_random(62) primary key)", 1)
	assertShowWarningCorrect("create table t (a bigint unsigned auto_random(61) primary key)", 3)
	assertShowWarningCorrect("create table t (a int auto_random(30) primary key)", 1)
	assertShowWarningCorrect("create table t (a int auto_random(29) primary key)", 3)

	// Disallow using it when allow-auto-random is not enabled.
	config.GetGlobalConfig().Experimental.AllowAutoRandom = false
	assertExperimentDisabled("create table auto_random_table (a int primary key auto_random(3))")
}

func (s *testSerialSuite) TestModifyingColumn4NewCollations(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database dct")
	tk.MustExec("use dct")
	tk.MustExec("create table t(b varchar(10) collate utf8_bin, c varchar(10) collate utf8_general_ci) collate utf8_bin")
	// Column collation can be changed as long as there is no index defined.
	tk.MustExec("alter table t modify b varchar(10) collate utf8_general_ci")
	tk.MustExec("alter table t modify c varchar(10) collate utf8_bin")
	tk.MustExec("alter table t charset utf8 collate utf8_general_ci")
	tk.MustExec("alter table t convert to charset utf8 collate utf8_bin")
	tk.MustExec("alter table t convert to charset utf8 collate utf8_general_ci")
	tk.MustExec("alter table t modify b varchar(10) collate utf8_bin")

	tk.MustExec("alter table t add index b_idx(b)")
	tk.MustExec("alter table t add index c_idx(c)")
	tk.MustGetErrMsg("alter table t modify b varchar(10) collate utf8_general_ci", "[ddl:8200]Unsupported modifying collation of column 'b' from 'utf8_bin' to 'utf8_general_ci' when index is defined on it.")
	tk.MustGetErrMsg("alter table t modify c varchar(10) collate utf8_bin", "[ddl:8200]Unsupported modifying collation of column 'c' from 'utf8_general_ci' to 'utf8_bin' when index is defined on it.")
	tk.MustGetErrMsg("alter table t convert to charset utf8 collate utf8_general_ci", "[ddl:8200]Unsupported converting collation of column 'b' from 'utf8_bin' to 'utf8_general_ci' when index is defined on it.")
	// Change to a compatible collation is allowed.
	tk.MustExec("alter table t modify c varchar(10) collate utf8mb4_general_ci")
	// Change the default collation of table is allowed.
	tk.MustExec("alter table t collate utf8mb4_general_ci")
	tk.MustExec("alter table t charset utf8mb4 collate utf8mb4_bin")
	// Change the default collation of database is allowed.
	tk.MustExec("alter database dct charset utf8mb4 collate utf8mb4_general_ci")
}

func (s *testSerialSuite) TestForbidUnsupportedCollations(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.store)

	mustGetUnsupportedCollation := func(sql string, coll string) {
		tk.MustGetErrMsg(sql, fmt.Sprintf("[ddl:1273]Unsupported collation when new collation is enabled: '%s'", coll))
	}
	// Test default collation of database.
	mustGetUnsupportedCollation("create database ucd charset utf8mb4 collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
	mustGetUnsupportedCollation("create database ucd charset utf8 collate utf8_unicode_ci", "utf8_unicode_ci")
	tk.MustExec("create database ucd")
	mustGetUnsupportedCollation("alter database ucd charset utf8mb4 collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
	mustGetUnsupportedCollation("alter database ucd collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")

	// Test default collation of table.
	tk.MustExec("use ucd")
	mustGetUnsupportedCollation("create table t(a varchar(20)) charset utf8mb4 collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
	mustGetUnsupportedCollation("create table t(a varchar(20)) collate utf8_unicode_ci", "utf8_unicode_ci")
	tk.MustExec("create table t(a varchar(20)) collate utf8mb4_general_ci")
	mustGetUnsupportedCollation("alter table t default collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
	mustGetUnsupportedCollation("alter table t convert to charset utf8mb4 collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")

	// Test collation of columns.
	mustGetUnsupportedCollation("create table t1(a varchar(20)) collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
	mustGetUnsupportedCollation("create table t1(a varchar(20)) charset utf8 collate utf8_unicode_ci", "utf8_unicode_ci")
	tk.MustExec("create table t1(a varchar(20))")
	mustGetUnsupportedCollation("alter table t1 modify a varchar(20) collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
	mustGetUnsupportedCollation("alter table t1 modify a varchar(20) charset utf8 collate utf8_unicode_ci", "utf8_unicode_ci")
	mustGetUnsupportedCollation("alter table t1 modify a varchar(20) charset utf8 collate utf8_unicode_ci", "utf8_unicode_ci")

	// TODO(bb7133): fix the following cases by setting charset from collate firstly.
	// mustGetUnsupportedCollation("create database ucd collate utf8mb4_unicode_ci", errMsgUnsupportedUnicodeCI)
	// mustGetUnsupportedCollation("alter table t convert to collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
}
