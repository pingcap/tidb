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

package session_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestSchemaCheckerSQL(t *testing.T) {
	store, clean := createMockStoreForSchemaTest(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1.MustExec("use test")

	// create table
	tk.MustExec(`create table t (id int, c int);`)
	tk.MustExec(`create table t1 (id int, c int);`)
	// insert data
	tk.MustExec(`insert into t values(1, 1);`)

	// The schema version is out of date in the first transaction, but the SQL can be retried.
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table t add index idx(c);`)
	tk.MustExec(`insert into t values(2, 2);`)
	tk.MustExec(`commit;`)

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

func TestSchemaCheckerTempTable(t *testing.T) {
	store, clean := createMockStoreForSchemaTest(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk2.MustExec("use test")

	// create table
	tk1.MustExec(`drop table if exists normal_table`)
	tk1.MustExec(`create table normal_table (id int, c int);`)
	defer tk1.MustExec(`drop table if exists normal_table`)
	tk1.MustExec(`drop table if exists temp_table`)
	tk1.MustExec(`create global temporary table temp_table (id int primary key, c int) on commit delete rows;`)
	defer tk1.MustExec(`drop table if exists temp_table`)

	// The schema version is out of date in the first transaction, and the SQL can't be retried.
	atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 1)
	defer func() {
		atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 0)
	}()

	// It's fine to change the schema of temporary tables.
	tk1.MustExec(`begin;`)
	tk2.MustExec(`alter table temp_table modify column c tinyint;`)
	tk1.MustExec(`insert into temp_table values(3, 3);`)
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c int;`)
	tk1.MustQuery(`select * from temp_table for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c smallint;`)
	tk1.MustExec(`insert into temp_table values(3, 4);`)
	tk1.MustQuery(`select * from temp_table for update;`).Check(testkit.Rows("3 4"))
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c bigint;`)
	tk1.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c smallint;`)
	tk1.MustExec("insert into temp_table values (1, 2), (2, 3), (4, 5)")
	tk1.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows("1 2"))
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c int;`)
	tk1.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c bigint;`)
	tk1.MustQuery(`select * from temp_table where id in (1, 2, 3) for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c int;`)
	tk1.MustExec("insert into temp_table values (1, 2), (2, 3), (4, 5)")
	tk1.MustQuery(`select * from temp_table where id in (1, 2, 3) for update;`).Check(testkit.Rows("1 2", "2 3"))
	tk1.MustExec(`commit;`)

	tk1.MustExec("insert into normal_table values(1, 2)")
	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c int;`)
	tk1.MustExec(`insert into temp_table values(1, 5);`)
	tk1.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows("1 5 1 2"))
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table normal_table modify column c bigint;`)
	tk1.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	// Truncate will modify table ID.
	tk1.MustExec(`begin;`)
	tk2.MustExec(`truncate table temp_table;`)
	tk1.MustExec(`insert into temp_table values(3, 3);`)
	tk1.MustExec(`commit;`)

	// It reports error when also changing the schema of a normal table.
	tk1.MustExec(`begin;`)
	tk2.MustExec(`alter table normal_table modify column c bigint;`)
	tk1.MustExec(`insert into temp_table values(3, 3);`)
	tk1.MustExec(`insert into normal_table values(3, 3);`)
	err := tk1.ExecToErr(`commit;`)
	require.True(t, terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), fmt.Sprintf("err %v", err))

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table normal_table modify column c int;`)
	tk1.MustExec(`insert into temp_table values(1, 6);`)
	tk1.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows("1 6 1 2"))
	err = tk1.ExecToErr(`commit;`)
	require.True(t, terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), fmt.Sprintf("err %v", err))
}

func TestDisableTxnAutoRetry(t *testing.T) {
	store, clean := createMockStoreForSchemaTest(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 1")

	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 2")

	tk2.MustExec("begin")
	tk2.MustExec("update no_retry set id = 3")
	tk2.MustExec("commit")

	// No auto retry because tidb_disable_txn_auto_retry is set to 1.
	_, err := tk1.Session().Execute(context.Background(), "commit")
	require.Error(t, err)

	// session 1 starts a transaction early.
	// execute a select statement to clear retry history.
	tk1.MustExec("select 1")
	err = tk1.Session().PrepareTxnCtx(context.Background())
	require.NoError(t, err)
	// session 2 update the value.
	tk2.MustExec("update no_retry set id = 4")
	// AutoCommit update will retry, so it would not fail.
	tk1.MustExec("update no_retry set id = 5")

	// RestrictedSQL should retry.
	tk1.Session().GetSessionVars().InRestrictedSQL = true
	tk1.MustExec("begin")

	tk2.MustExec("update no_retry set id = 6")

	tk1.MustExec("update no_retry set id = 7")
	tk1.MustExec("commit")

	// test for disable transaction local latch
	tk1.Session().GetSessionVars().InRestrictedSQL = false
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = false
	})
	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 9")

	tk2.MustExec("update no_retry set id = 8")

	_, err = tk1.Session().Execute(context.Background(), "commit")
	require.Error(t, err)
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("err %v", err))
	require.Contains(t, err.Error(), kv.TxnRetryableMark)
	tk1.MustExec("rollback")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = true
	})
	tk1.MustExec("begin")
	tk2.MustExec("alter table no_retry add index idx(id)")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("8"))
	tk1.MustExec("update no_retry set id = 10")
	_, err = tk1.Session().Execute(context.Background(), "commit")
	require.Error(t, err)

	// set autocommit to begin and commit
	tk1.MustExec("set autocommit = 0")
	tk1.MustQuery("select * from no_retry").Check(testkit.Rows("8"))
	tk2.MustExec("update no_retry set id = 11")
	tk1.MustExec("update no_retry set id = 12")
	_, err = tk1.Session().Execute(context.Background(), "set autocommit = 1")
	require.Error(t, err)
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("err %v", err))
	require.Contains(t, err.Error(), kv.TxnRetryableMark)
	tk1.MustExec("rollback")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("11"))

	tk1.MustExec("set autocommit = 0")
	tk1.MustQuery("select * from no_retry").Check(testkit.Rows("11"))
	tk2.MustExec("update no_retry set id = 13")
	tk1.MustExec("update no_retry set id = 14")
	_, err = tk1.Session().Execute(context.Background(), "commit")
	require.Error(t, err)
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("err %v", err))
	require.Contains(t, err.Error(), kv.TxnRetryableMark)
	tk1.MustExec("rollback")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("13"))
}

// The Read-only flags are checked in the planning stage of queries,
// but this test checks we check them again at commit time.
// The main use case for this is a long-running auto-commit statement.
func TestAutoCommitRespectsReadOnly(t *testing.T) {
	store, clean := createMockStoreForSchemaTest(t)
	defer clean()
	var wg sync.WaitGroup
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	require.True(t, tk2.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

	tk1.MustExec("create table test.auto_commit_test (a int)")
	wg.Add(1)
	go func() {
		err := tk1.ExecToErr("INSERT INTO test.auto_commit_test VALUES (SLEEP(1))")
		require.True(t, terror.ErrorEqual(err, plannercore.ErrSQLInReadOnlyMode), fmt.Sprintf("err %v", err))
		wg.Done()
	}()
	tk2.MustExec("SET GLOBAL tidb_restricted_read_only = 1")
	err := tk2.ExecToErr("INSERT INTO test.auto_commit_test VALUES (0)") // should also be an error
	require.True(t, terror.ErrorEqual(err, plannercore.ErrSQLInReadOnlyMode), fmt.Sprintf("err %v", err))
	// Reset and check with the privilege to ignore the readonly flag and continue to insert.
	wg.Wait()
	tk1.MustExec("SET GLOBAL tidb_restricted_read_only = 0")
	tk1.MustExec("SET GLOBAL tidb_super_read_only = 0")
	tk1.MustExec("GRANT RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'")

	wg.Add(1)
	go func() {
		tk1.MustExec("INSERT INTO test.auto_commit_test VALUES (SLEEP(1))")
		wg.Done()
	}()
	tk2.MustExec("SET GLOBAL tidb_restricted_read_only = 1")
	tk2.MustExec("INSERT INTO test.auto_commit_test VALUES (0)")

	// wait for go routines
	wg.Wait()
	tk1.MustExec("SET GLOBAL tidb_restricted_read_only = 0")
	tk1.MustExec("SET GLOBAL tidb_super_read_only = 0")
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

	store, clean := createMockStoreForSchemaTest(t)
	defer clean()

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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed", `return(true)`))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed")) }()
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

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed"))
	time.Sleep(lease * 2)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert t values (100);")
	// Make sure insert to table t2 transaction executes.
	tk2.MustExec("commit")
}
