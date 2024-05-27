// Copyright 2022 PingCAP, Inc.
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

package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

func TestSchemaCheckerSQL(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_enable_metadata_lock=0")
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1.MustExec("use test")

	// create table
	tk.MustExec(`create table t (id int, c int);`)
	tk.MustExec(`create table t1 (id int, c int);`)
	// insert data
	tk.MustExec(`insert into t values(1, 1);`)

	// The schema version is out of date in the first transaction, and the SQL can't be retried.
	atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 1)
	defer func() {
		atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 0)
	}()
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table t modify column c bigint;`)
	tk.MustExec(`insert into t values(3, 3);`)
	err := tk.ExecToErr(`commit;`)
	require.True(t, terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), fmt.Sprintf("err %v", err))

	// But the transaction related table IDs aren't in the updated table IDs.
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table t add index idx2(c);`)
	tk.MustExec(`insert into t1 values(4, 4);`)
	tk.MustExec(`commit;`)

	// Test for "select for update".
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table t add index idx3(c);`)
	tk.MustQuery(`select * from t for update`)
	require.Error(t, tk.ExecToErr(`commit;`))

	// Repeated tests for partitioned table
	tk.MustExec(`create table pt (id int, c int) partition by hash (id) partitions 3`)
	tk.MustExec(`insert into pt values(1, 1);`)
	// The schema version is out of date in the first transaction, and the SQL can't be retried.
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table pt modify column c bigint;`)
	tk.MustExec(`insert into pt values(3, 3);`)
	err = tk.ExecToErr(`commit;`)
	require.True(t, terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), fmt.Sprintf("err %v", err))

	// But the transaction related table IDs aren't in the updated table IDs.
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table pt add index idx2(c);`)
	tk.MustExec(`insert into t1 values(4, 4);`)
	tk.MustExec(`commit;`)

	// Test for "select for update".
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table pt add index idx3(c);`)
	tk.MustQuery(`select * from pt for update`)
	require.Error(t, tk.ExecToErr(`commit;`))

	// Test for "select for update".
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table pt add index idx4(c);`)
	tk.MustQuery(`select * from pt partition (p1) for update`)
	require.Error(t, tk.ExecToErr(`commit;`))
}

func TestLoadSchemaFailed(t *testing.T) {
	originalRetryTime := domain.SchemaOutOfDateRetryTimes.Load()
	originalRetryInterval := domain.SchemaOutOfDateRetryInterval.Load()
	domain.SchemaOutOfDateRetryTimes.Store(3)
	domain.SchemaOutOfDateRetryInterval.Store(20 * time.Millisecond)
	defer func() {
		domain.SchemaOutOfDateRetryTimes.Store(originalRetryTime)
		domain.SchemaOutOfDateRetryInterval.Store(originalRetryInterval)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second)

	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("create table t (a int);")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("create table t2 (a int);")

	tk1.MustExec("begin")
	tk2.MustExec("begin")

	// Make sure loading information schema is failed and server is invalid.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/ErrorMockReloadFailed", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/ErrorMockReloadFailed"))
	}()
	require.Error(t, domain.GetDomain(tk.Session()).Reload())

	lease := domain.GetDomain(tk.Session()).DDL().GetLease()
	time.Sleep(lease * 2)

	// Make sure executing insert statement is failed when server is invalid.
	require.Error(t, tk.ExecToErr("insert t values (100);"))

	tk1.MustExec("insert t1 values (100);")
	tk2.MustExec("insert t2 values (100);")

	require.Error(t, tk1.ExecToErr("commit"))

	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	require.NotNil(t, ver)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/ErrorMockReloadFailed"))
	time.Sleep(lease * 2)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert t values (100);")
	// Make sure insert to table t2 transaction executes.
	tk2.MustExec("commit")
}

func TestWriteOnMultipleCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ct1, ct2")
	tk.MustExec("create table ct1 (id int, c int)")
	tk.MustExec("create table ct2 (id int, c int)")
	tk.MustExec("alter table ct1 cache")
	tk.MustExec("alter table ct2 cache")
	tk.MustQuery("select * from ct1").Check(testkit.Rows())
	tk.MustQuery("select * from ct2").Check(testkit.Rows())

	lastReadFromCache := func(tk *testkit.TestKit) bool {
		return tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
	}

	cached := false
	for i := 0; i < 50; i++ {
		tk.MustQuery("select * from ct1")
		if lastReadFromCache(tk) {
			cached = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, cached)

	tk.MustExec("begin")
	tk.MustExec("insert into ct1 values (3, 4)")
	tk.MustExec("insert into ct2 values (5, 6)")
	tk.MustExec("commit")

	tk.MustQuery("select * from ct1").Check(testkit.Rows("3 4"))
	tk.MustQuery("select * from ct2").Check(testkit.Rows("5 6"))

	// cleanup
	tk.MustExec("alter table ct1 nocache")
	tk.MustExec("alter table ct2 nocache")
}

func TestFixSetTiDBSnapshotTS(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("create database t123")
	time.Sleep(time.Second)
	ts := time.Now().Format("2006-1-2 15:04:05")
	time.Sleep(time.Second)
	tk.MustExec("drop database t123")
	tk.MustMatchErrMsg("use t123", ".*Unknown database.*")
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot='%s'", ts))
	tk.MustExec("use t123")
	// update any session variable and assert whether infoschema is changed
	tk.MustExec("SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER';")
	tk.MustExec("use t123")
}

func TestPrepareZero(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(v timestamp)")
	tk.MustExec("prepare s1 from 'insert into t (v) values (?)'")
	tk.MustExec("set @v1='0'")
	require.Error(t, tk.ExecToErr("execute s1 using @v1"))
	tk.MustExec("set @v2='" + types.ZeroDatetimeStr + "'")
	tk.MustExec("set @orig_sql_mode=@@sql_mode; set @@sql_mode='';")
	tk.MustExec("execute s1 using @v2")
	tk.MustQuery("select v from t").Check(testkit.Rows("0000-00-00 00:00:00"))
	tk.MustExec("set @@sql_mode=@orig_sql_mode;")
}

func TestPrimaryKeyAutoIncrement(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL, name varchar(255) UNIQUE NOT NULL, status int)")
	tk.MustExec("insert t (name) values (?)", "abc")
	id := tk.Session().LastInsertID()
	require.NotZero(t, id)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("%d abc <nil>", id)))

	tk.MustExec("update t set name = 'abc', status = 1 where id = ?", id)
	tk1.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("%d abc 1", id)))

	// Check for pass bool param to tidb prepared statement
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id tinyint)")
	tk.MustExec("insert t values (?)", true)
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
}

func TestParseWithParams(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	exec := se.GetRestrictedSQLExecutor()

	// test compatibility with ExecuteInternal
	_, err := exec.ParseWithParams(context.TODO(), "SELECT 4")
	require.NoError(t, err)

	// test charset attack
	stmt, err := exec.ParseWithParams(context.TODO(), "SELECT * FROM test WHERE name = %? LIMIT 1", "\xbf\x27 OR 1=1 /*")
	require.NoError(t, err)

	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.RestoreStringDoubleQuotes, &sb)
	err = stmt.Restore(ctx)
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM test WHERE name=_utf8mb4\"\xbf' OR 1=1 /*\" LIMIT 1", sb.String())

	// test invalid sql
	_, err = exec.ParseWithParams(context.TODO(), "SELECT")
	require.Regexp(t, ".*You have an error in your SQL syntax.*", err)

	// test invalid arguments to escape
	_, err = exec.ParseWithParams(context.TODO(), "SELECT %?, %?", 3)
	require.Regexp(t, "missing arguments.*", err)

	// test noescape
	stmt, err = exec.ParseWithParams(context.TODO(), "SELECT 3")
	require.NoError(t, err)

	sb.Reset()
	ctx = format.NewRestoreCtx(0, &sb)
	err = stmt.Restore(ctx)
	require.NoError(t, err)
	require.Equal(t, "SELECT 3", sb.String())
}

func TestDoDDLJobQuit(t *testing.T) {
	// This is required since mock tikv does not support paging.
	failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/DisablePaging", `return`)
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/DisablePaging"))
	}()

	// test https://github.com/pingcap/tidb/issues/18714, imitate DM's use environment
	// use isolated store, because in below failpoint we will cancel its context
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.MockTiKV))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	se, err := session.CreateSession(store)
	require.NoError(t, err)
	defer se.Close()

	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/storeCloseInLoop", `return`))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/storeCloseInLoop")) }()

	// this DDL call will enter deadloop before this fix
	err = dom.DDL().CreateSchema(se, &ast.CreateDatabaseStmt{Name: model.NewCIStr("testschema")})
	require.Equal(t, "context canceled", err.Error())
}

func TestProcessInfoIssue22068(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		tk.MustQuery("select 1 from t where a = (select sleep(5));").Check(testkit.Rows())
	})
	time.Sleep(2 * time.Second)
	pi := tk.Session().ShowProcess()
	require.NotNil(t, pi)
	require.Equal(t, "select 1 from t where a = (select sleep(5));", pi.Info)
	require.Nil(t, pi.Plan)
	wg.Wait()
}

func TestPerStmtTaskID(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table task_id (v int)")

	tk.MustExec("begin")
	tk.MustExec("select * from task_id where v > 10")
	taskID1 := tk.Session().GetSessionVars().StmtCtx.TaskID
	tk.MustExec("select * from task_id where v < 5")
	taskID2 := tk.Session().GetSessionVars().StmtCtx.TaskID
	tk.MustExec("commit")

	require.NotEqual(t, taskID1, taskID2)
}

func TestStmtHints(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test MEMORY_QUOTA hint
	tk.MustExec("select /*+ MEMORY_QUOTA(1 MB) */ 1;")
	val := int64(1) * 1024 * 1024
	require.True(t, tk.Session().GetSessionVars().MemTracker.CheckBytesLimit(val))
	tk.MustExec("select /*+ MEMORY_QUOTA(1 GB) */ 1;")
	val = int64(1) * 1024 * 1024 * 1024
	require.True(t, tk.Session().GetSessionVars().MemTracker.CheckBytesLimit(val))
	tk.MustExec("select /*+ MEMORY_QUOTA(1 GB), MEMORY_QUOTA(1 MB) */ 1;")
	val = int64(1) * 1024 * 1024
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.True(t, tk.Session().GetSessionVars().MemTracker.CheckBytesLimit(val))
	tk.MustExec("select /*+ MEMORY_QUOTA(0 GB) */ 1;")
	val = int64(0)
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.True(t, tk.Session().GetSessionVars().MemTracker.CheckBytesLimit(val))
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "Setting the MEMORY_QUOTA to 0 means no memory limit")

	tk.MustExec("use test")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("insert /*+ MEMORY_QUOTA(1 MB) */ into t1 (a) values (1);")
	val = int64(1) * 1024 * 1024
	require.True(t, tk.Session().GetSessionVars().MemTracker.CheckBytesLimit(val))

	tk.MustExec("insert /*+ MEMORY_QUOTA(1 MB) */  into t1 select /*+ MEMORY_QUOTA(3 MB) */ * from t1;")
	val = int64(1) * 1024 * 1024
	require.True(t, tk.Session().GetSessionVars().MemTracker.CheckBytesLimit(val))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "[planner:3126]Hint MEMORY_QUOTA(`3145728`) is ignored as conflicting/duplicated.")

	// Test NO_INDEX_MERGE hint
	tk.Session().GetSessionVars().SetEnableIndexMerge(true)
	tk.MustExec("select /*+ NO_INDEX_MERGE() */ 1;")
	require.True(t, tk.Session().GetSessionVars().StmtCtx.NoIndexMergeHint)
	tk.MustExec("select /*+ NO_INDEX_MERGE(), NO_INDEX_MERGE() */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.True(t, tk.Session().GetSessionVars().GetEnableIndexMerge())

	// Test STRAIGHT_JOIN hint
	tk.MustExec("select /*+ straight_join() */ 1;")
	require.True(t, tk.Session().GetSessionVars().StmtCtx.StraightJoinOrder)
	tk.MustExec("select /*+ straight_join(), straight_join() */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)

	// Test USE_TOJA hint
	tk.Session().GetSessionVars().SetAllowInSubqToJoinAndAgg(true)
	tk.MustExec("select /*+ USE_TOJA(false) */ 1;")
	require.False(t, tk.Session().GetSessionVars().GetAllowInSubqToJoinAndAgg())
	tk.Session().GetSessionVars().SetAllowInSubqToJoinAndAgg(false)
	tk.MustExec("select /*+ USE_TOJA(true) */ 1;")
	require.True(t, tk.Session().GetSessionVars().GetAllowInSubqToJoinAndAgg())
	tk.MustExec("select /*+ USE_TOJA(false), USE_TOJA(true) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.True(t, tk.Session().GetSessionVars().GetAllowInSubqToJoinAndAgg())

	// Test USE_CASCADES hint
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(true)
	tk.MustExec("select /*+ USE_CASCADES(false) */ 1;")
	require.False(t, tk.Session().GetSessionVars().GetEnableCascadesPlanner())
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(false)
	tk.MustExec("select /*+ USE_CASCADES(true) */ 1;")
	require.True(t, tk.Session().GetSessionVars().GetEnableCascadesPlanner())
	tk.MustExec("select /*+ USE_CASCADES(false), USE_CASCADES(true) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(true)")
	require.True(t, tk.Session().GetSessionVars().GetEnableCascadesPlanner())

	// Test READ_CONSISTENT_REPLICA hint
	tk.Session().GetSessionVars().SetReplicaRead(kv.ReplicaReadLeader)
	tk.MustExec("select /*+ READ_CONSISTENT_REPLICA() */ 1;")
	require.Equal(t, kv.ReplicaReadFollower, tk.Session().GetSessionVars().GetReplicaRead())
	tk.MustExec("select /*+ READ_CONSISTENT_REPLICA(), READ_CONSISTENT_REPLICA() */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.Equal(t, kv.ReplicaReadFollower, tk.Session().GetSessionVars().GetReplicaRead())
}

func TestRollbackOnCompileError(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert t values (1)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("rename table t to t2")
	var meetErr bool
	for i := 0; i < 100; i++ {
		_, err := tk2.Exec("insert t values (1)")
		if err != nil {
			meetErr = true
			break
		}
	}
	require.True(t, meetErr)

	tk.MustExec("rename table t2 to t")
	var recoverErr bool
	for i := 0; i < 100; i++ {
		_, err := tk2.Exec("insert t values (1)")
		if err == nil {
			recoverErr = true
			break
		}
	}
	require.True(t, recoverErr)
}

func TestResultField(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int);")

	tk.MustExec(`INSERT INTO t VALUES (1);`)
	tk.MustExec(`INSERT INTO t VALUES (2);`)
	r, err := tk.Exec(`SELECT count(*) from t;`)
	require.NoError(t, err)
	defer r.Close()
	fields := r.Fields()
	require.NoError(t, err)
	require.Len(t, fields, 1)
	field := fields[0].Column
	require.Equal(t, mysql.TypeLonglong, field.GetType())
	require.Equal(t, 21, field.GetFlen())
}

// Testcase for https://github.com/pingcap/tidb/issues/325
func TestResultType(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	rs, err := tk.Exec(`select cast(null as char(30))`)
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	err = rs.Next(context.Background(), req)
	require.NoError(t, err)
	require.True(t, req.GetRow(0).IsNull(0))
	require.Equal(t, mysql.TypeVarString, rs.Fields()[0].Column.FieldType.GetType())
}

func TestFieldText(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tests := []struct {
		sql   string
		field string
	}{
		{"select distinct(a) from t", "a"},
		{"select (1)", "1"},
		{"select (1+1)", "(1+1)"},
		{"select a from t", "a"},
		{"select        ((a+1))     from t", "((a+1))"},
		{"select 1 /*!32301 +1 */;", "1  +1 "},
		{"select /*!32301 1  +1 */;", "1  +1 "},
		{"/*!32301 select 1  +1 */;", "1  +1 "},
		{"select 1 + /*!32301 1 +1 */;", "1 +  1 +1 "},
		{"select 1 /*!32301 + 1, 1 */;", "1  + 1"},
		{"select /*!32301 1, 1 +1 */;", "1"},
		{"select /*!32301 1 + 1, */ +1;", "1 + 1"},
	}
	for _, tt := range tests {
		result, err := tk.Exec(tt.sql)
		require.NoError(t, err)
		require.Equal(t, tt.field, result.Fields()[0].ColumnAsName.O)
		result.Close()
	}
}

func TestMatchIdentity(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE USER `useridentity`@`%`")
	tk.MustExec("CREATE USER `useridentity`@`localhost`")
	tk.MustExec("CREATE USER `useridentity`@`192.168.1.1`")
	tk.MustExec("CREATE USER `useridentity`@`example.com`")

	// The MySQL matching rule is most specific to least specific.
	// So if I log in from 192.168.1.1 I should match that entry always.
	identity, err := tk.Session().MatchIdentity("useridentity", "192.168.1.1")
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	require.Equal(t, "192.168.1.1", identity.Hostname)

	// If I log in from localhost, I should match localhost
	identity, err = tk.Session().MatchIdentity("useridentity", "localhost")
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	require.Equal(t, "localhost", identity.Hostname)

	// If I log in from 192.168.1.2 I should match wildcard.
	identity, err = tk.Session().MatchIdentity("useridentity", "192.168.1.2")
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	require.Equal(t, "%", identity.Hostname)

	identity, err = tk.Session().MatchIdentity("useridentity", "127.0.0.1")
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	require.Equal(t, "localhost", identity.Hostname)

	// This uses the lookup of example.com to get an IP address.
	// We then login with that IP address, but expect it to match the example.com
	// entry in the privileges table (by reverse lookup).
	ips, err := net.LookupHost("example.com")
	require.NoError(t, err)
	identity, err = tk.Session().MatchIdentity("useridentity", ips[0])
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	// FIXME: we *should* match example.com instead
	// as long as skip-name-resolve is not set (DEFAULT)
	require.Equal(t, "%", identity.Hostname)
}

func TestHandleAssertionFailureForPartitionedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	se.SetConnectionID(1)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, primary key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	failpoint.Enable("github.com/pingcap/tidb/pkg/table/tables/addRecordForceAssertExist", "return")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/table/tables/addRecordForceAssertExist")

	ctx, hook := testutil.WithLogHook(context.TODO(), t, "table")
	_, err := tk.ExecWithContext(ctx, "insert into t values (1, 1, 1)")
	require.ErrorContains(t, err, "assertion")
	hook.CheckLogCount(t, 0)
}

func TestRandomBinary(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	allBytes := [][]byte{
		{4, 0, 0, 0, 0, 0, 0, 4, '2'},
		{4, 0, 0, 0, 0, 0, 0, 4, '.'},
		{4, 0, 0, 0, 0, 0, 0, 4, '*'},
		{4, 0, 0, 0, 0, 0, 0, 4, '('},
		{4, 0, 0, 0, 0, 0, 0, 4, '\''},
		{4, 0, 0, 0, 0, 0, 0, 4, '!'},
		{4, 0, 0, 0, 0, 0, 0, 4, 29},
		{4, 0, 0, 0, 0, 0, 0, 4, 28},
		{4, 0, 0, 0, 0, 0, 0, 4, 23},
		{4, 0, 0, 0, 0, 0, 0, 4, 16},
	}
	sql := "insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values "
	var val string
	for i, bytes := range allBytes {
		if i == 0 {
			val += sqlescape.MustEscapeSQL("(874, 0, 1, %?, 3)", bytes)
		} else {
			val += sqlescape.MustEscapeSQL(",(874, 0, 1, %?, 3)", bytes)
		}
	}
	sql += val
	tk.MustExec("set sql_mode = 'NO_BACKSLASH_ESCAPES';")
	_, err := tk.Session().ExecuteInternal(ctx, sql)
	require.NoError(t, err)
}

func TestSQLModeOp(t *testing.T) {
	s := mysql.ModeNoBackslashEscapes | mysql.ModeOnlyFullGroupBy
	d := mysql.DelSQLMode(s, mysql.ModeANSIQuotes)
	require.Equal(t, s, d)

	d = mysql.DelSQLMode(s, mysql.ModeNoBackslashEscapes)
	require.Equal(t, mysql.ModeOnlyFullGroupBy, d)

	s = mysql.ModeNoBackslashEscapes | mysql.ModeOnlyFullGroupBy
	a := mysql.SetSQLMode(s, mysql.ModeOnlyFullGroupBy)
	require.Equal(t, s, a)

	a = mysql.SetSQLMode(s, mysql.ModeAllowInvalidDates)
	require.Equal(t, mysql.ModeNoBackslashEscapes|mysql.ModeOnlyFullGroupBy|mysql.ModeAllowInvalidDates, a)
}

func TestRequestSource(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.MockTiKV))
	tk := testkit.NewTestKit(t, store)
	withCheckInterceptor := func(source string) interceptor.RPCInterceptor {
		return interceptor.NewRPCInterceptor("kv-request-source-verify", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
			return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
				requestSource := ""
				readType := ""
				switch r := req.Req.(type) {
				case *kvrpcpb.PrewriteRequest:
					requestSource = r.GetContext().GetRequestSource()
				case *kvrpcpb.CommitRequest:
					requestSource = r.GetContext().GetRequestSource()
				case *coprocessor.Request:
					readType = "leader_" // read request will be attached with read type
					requestSource = r.GetContext().GetRequestSource()
				case *kvrpcpb.GetRequest:
					readType = "leader_" // read request will be attached with read type
					requestSource = r.GetContext().GetRequestSource()
				case *kvrpcpb.BatchGetRequest:
					readType = "leader_" // read request will be attached with read type
					requestSource = r.GetContext().GetRequestSource()
				}
				require.Equal(t, readType+source, requestSource)
				return next(target, req)
			}
		})
	}
	ctx := context.Background()
	tk.MustExecWithContext(ctx, "use test")
	tk.MustExecWithContext(ctx, "create table t(a int primary key, b int)")
	tk.MustExecWithContext(ctx, "set @@tidb_request_source_type = 'lightning'")
	tk.MustQueryWithContext(ctx, "select @@tidb_request_source_type").Check(testkit.Rows("lightning"))
	insertCtx := interceptor.WithRPCInterceptor(context.Background(), withCheckInterceptor("external_Insert_lightning"))
	tk.MustExecWithContext(insertCtx, "insert into t values(1, 1)")
	selectCtx := interceptor.WithRPCInterceptor(context.Background(), withCheckInterceptor("external_Select_lightning"))
	tk.MustExecWithContext(selectCtx, "select count(*) from t;")
	tk.MustQueryWithContext(selectCtx, "select b from t where a = 1;")
	tk.MustQueryWithContext(selectCtx, "select b from t where a in (1, 2, 3);")
}

func TestEmptyInitSQLFile(t *testing.T) {
	// A non-existent sql file would stop the bootstrap of the tidb cluster
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	config.GetGlobalConfig().InitializeSQLFile = "non-existent.sql"
	defer func() {
		require.NoError(t, store.Close())
		config.GetGlobalConfig().InitializeSQLFile = ""
	}()

	dom, err := session.BootstrapSession(store)
	require.Nil(t, dom)
	require.Error(t, err)
}

func TestInitSystemVariable(t *testing.T) {
	// We create an initialize-sql-file and then bootstrap the server with it.
	// The observed behavior should be that tidb_enable_noop_variables is now
	// disabled, and the feature works as expected.
	initializeSQLFile, err := os.CreateTemp("", "init.sql")
	require.NoError(t, err)
	defer func() {
		path := initializeSQLFile.Name()
		err = initializeSQLFile.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()
	// Implicitly test multi-line init files
	_, err = initializeSQLFile.WriteString(
		"CREATE DATABASE initsqlfiletest;\n" +
			"SET GLOBAL tidb_enable_noop_variables = OFF;\n")
	require.NoError(t, err)

	// Create a mock store
	// Set the config parameter for initialize sql file
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	config.GetGlobalConfig().InitializeSQLFile = initializeSQLFile.Name()
	defer func() {
		require.NoError(t, store.Close())
		config.GetGlobalConfig().InitializeSQLFile = ""
	}()

	// Bootstrap with the InitializeSQLFile config option
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	se := session.CreateSessionAndSetID(t, store)
	ctx := context.Background()
	r := session.MustExecToRecodeSet(t, se, `SHOW VARIABLES LIKE 'query_cache_type'`)
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows()) // not shown in noopvariables mode
	require.NoError(t, r.Close())

	r = session.MustExecToRecodeSet(t, se, `SHOW VARIABLES LIKE 'tidb_enable_noop_variables'`)
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, []byte("OFF"), row.GetBytes(1))
	require.NoError(t, r.Close())
}

func TestInitUsers(t *testing.T) {
	// Two sql files are set to 'initialize-sql-file' one after another,
	// and only the first one is executed.
	var err error
	sqlFiles := make([]*os.File, 2)
	for i, name := range []string{"1.sql", "2.sql"} {
		sqlFiles[i], err = os.CreateTemp("", name)
		require.NoError(t, err)
	}
	defer func() {
		for _, sqlFile := range sqlFiles {
			path := sqlFile.Name()
			err = sqlFile.Close()
			require.NoError(t, err)
			err = os.Remove(path)
			require.NoError(t, err)
		}
	}()
	_, err = sqlFiles[0].WriteString(`
CREATE USER cloud_admin;
GRANT BACKUP_ADMIN, RESTORE_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT DASHBOARD_CLIENT on *.* TO 'cloud_admin'@'%';
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT CONNECTION_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_VARIABLES_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_STATUS_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_CONNECTION_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_USER_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_TABLES_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT RESTRICTED_REPLICA_WRITER_ADMIN ON *.* TO 'cloud_admin'@'%';
GRANT CREATE USER ON *.* TO 'cloud_admin'@'%';
GRANT RELOAD ON *.* TO 'cloud_admin'@'%';
GRANT PROCESS ON *.* TO 'cloud_admin'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON mysql.* TO 'cloud_admin'@'%';
GRANT SELECT ON information_schema.* TO 'cloud_admin'@'%';
GRANT SELECT ON performance_schema.* TO 'cloud_admin'@'%';
GRANT SHOW DATABASES on *.* TO 'cloud_admin'@'%';
GRANT REFERENCES ON *.* TO 'cloud_admin'@'%';
GRANT SELECT ON *.* TO 'cloud_admin'@'%';
GRANT INDEX ON *.* TO 'cloud_admin'@'%';
GRANT INSERT ON *.* TO 'cloud_admin'@'%';
GRANT UPDATE ON *.* TO 'cloud_admin'@'%';
GRANT DELETE ON *.* TO 'cloud_admin'@'%';
GRANT CREATE ON *.* TO 'cloud_admin'@'%';
GRANT DROP ON *.* TO 'cloud_admin'@'%';
GRANT ALTER ON *.* TO 'cloud_admin'@'%';
GRANT CREATE VIEW ON *.* TO 'cloud_admin'@'%';
GRANT SHUTDOWN, CONFIG ON *.* TO 'cloud_admin'@'%';
REVOKE SHUTDOWN, CONFIG ON *.* FROM root;

DROP USER root;
`)
	require.NoError(t, err)
	_, err = sqlFiles[1].WriteString("drop user cloud_admin;")
	require.NoError(t, err)

	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	config.GetGlobalConfig().InitializeSQLFile = sqlFiles[0].Name()
	defer func() {
		require.NoError(t, store.Close())
		config.GetGlobalConfig().InitializeSQLFile = ""
	}()

	// Bootstrap with the first sql file
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	se := session.CreateSessionAndSetID(t, store)
	ctx := context.Background()
	// 'cloud_admin' has been created successfully
	r := session.MustExecToRecodeSet(t, se, `select user from mysql.user where user = 'cloud_admin'`)
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, "cloud_admin", row.GetString(0))
	require.NoError(t, r.Close())
	// 'root' has been deleted successfully
	r = session.MustExecToRecodeSet(t, se, `select user from mysql.user where user = 'root'`)
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	require.NoError(t, r.Close())
	dom.Close()

	session.DisableRunBootstrapSQLFileInTest()

	// Bootstrap with the second sql file, which would not been executed.
	config.GetGlobalConfig().InitializeSQLFile = sqlFiles[1].Name()
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	se = session.CreateSessionAndSetID(t, store)
	r = session.MustExecToRecodeSet(t, se, `select user from mysql.user where user = 'cloud_admin'`)
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row = req.GetRow(0)
	require.Equal(t, "cloud_admin", row.GetString(0))
	require.NoError(t, r.Close())
	dom.Close()
}

func TestErrorHappenWhileInit(t *testing.T) {
	// 1. parser error in sql file (1.sql) makes the bootstrap panic
	// 2. other errors in sql file (2.sql) will be ignored
	var err error
	sqlFiles := make([]*os.File, 2)
	for i, name := range []string{"1.sql", "2.sql"} {
		sqlFiles[i], err = os.CreateTemp("", name)
		require.NoError(t, err)
	}
	defer func() {
		for _, sqlFile := range sqlFiles {
			path := sqlFile.Name()
			err = sqlFile.Close()
			require.NoError(t, err)
			err = os.Remove(path)
			require.NoError(t, err)
		}
	}()
	_, err = sqlFiles[0].WriteString("create table test.t (c in);")
	require.NoError(t, err)
	_, err = sqlFiles[1].WriteString(`
create table test.t (c int);
insert into test.t values ("abc"); -- invalid statement
`)
	require.NoError(t, err)

	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	config.GetGlobalConfig().InitializeSQLFile = sqlFiles[0].Name()
	defer func() {
		config.GetGlobalConfig().InitializeSQLFile = ""
	}()

	// Bootstrap with the first sql file
	dom, err := session.BootstrapSession(store)
	require.Nil(t, dom)
	require.Error(t, err)
	require.NoError(t, store.Close())

	session.DisableRunBootstrapSQLFileInTest()

	// Bootstrap with the second sql file, which would not been executed.
	store, err = mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	config.GetGlobalConfig().InitializeSQLFile = sqlFiles[1].Name()
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	se := session.CreateSessionAndSetID(t, store)
	ctx := context.Background()
	_ = session.MustExecToRecodeSet(t, se, `use test;`)
	require.NoError(t, err)
	// Table t has been created.
	r := session.MustExecToRecodeSet(t, se, `show tables;`)
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, "t", row.GetString(0))
	require.NoError(t, r.Close())
	// But data is failed to inserted since the error
	r = session.MustExecToRecodeSet(t, se, `select * from test.t`)
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	require.NoError(t, r.Close())
	dom.Close()
}
