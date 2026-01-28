// Copyright 2025 PingCAP, Inc.
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

package txntest

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func prepareForeignKeyTables(tk *testkit.TestKit) {
	tk.MustExec("drop table if exists child, parent")
	tk.MustExec("create table parent (id int primary key)")
	tk.MustExec("create table child (id int primary key, pid int, foreign key (pid) references parent(id))")
	tk.MustExec("insert into parent values (1), (2)")
}

func TestSharedLockBlockedByExclusiveLock(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk3.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")

	prepareForeignKeyTables(tk1)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk3.MustExec("begin pessimistic")

	tk1.MustExec("select * from parent where id=1 for update")
	tk2Done := make(chan struct{})
	go func() {
		tk2.MustExec("insert into child values(1, 1)")
		close(tk2Done)
	}()
	tk3Done := make(chan struct{})
	go func() {
		tk3.MustExec("insert into child values(2, 1)")
		close(tk3Done)
	}()

	select {
	case <-time.After(500 * time.Millisecond):
	case <-tk2Done:
		require.FailNow(t, "tk2 should be blocked")
	case <-tk3Done:
		require.FailNow(t, "tk3 should be blocked")
	}
	tk1.MustExec("commit")
	<-tk2Done
	<-tk3Done

	tk1.MustQuery("select * from child").Check(testkit.Rows())
	tk2.MustExec("commit")
	tk1.MustQuery("select * from child").Check(testkit.Rows("1 1"))
	tk3.MustExec("commit")
	tk1.MustQuery("select * from child").Check(testkit.Rows("1 1", "2 1"))
	tk1.MustExec("admin check table parent, child")
}

func TestSharedLockBlockExclusiveLock(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk3.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")

	prepareForeignKeyTables(tk1)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk3.MustExec("begin pessimistic")

	tk2.MustExec("insert into child values(1, 1)")
	tk3.MustExec("insert into child values(2, 1)")
	tk1Done := make(chan struct{})
	go func() {
		tk1.MustExec("select * from parent where id=1 for update")
		close(tk1Done)
	}()

	select {
	case <-time.After(500 * time.Millisecond):
	case <-tk1Done:
		require.FailNow(t, "tk1 should be blocked")
	}
	tk2.MustExec("commit")
	tk2.MustQuery("select * from child").Check(testkit.Rows("1 1"))
	select {
	case <-time.After(500 * time.Millisecond):
	case <-tk1Done:
		require.FailNow(t, "tk1 should be blocked")
	}
	tk3.MustExec("commit")
	tk3.MustQuery("select * from child").Check(testkit.Rows("1 1", "2 1"))

	<-tk1Done

	tk1.MustExec("commit")
	tk1.MustExec("admin check table parent, child")
}

func TestSharedLockChildTableConflict(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk3.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")

	prepareForeignKeyTables(tk1)

	tk2.MustExec("begin pessimistic")
	tk3.MustExec("begin pessimistic")

	tk2.MustExec("insert into child values(1, 1)")
	tk3Done := make(chan struct{})
	go func() {
		tk3.MustExecToErr("insert into child values(1, 2)")
		close(tk3Done)
	}()
	select {
	case <-time.After(500 * time.Millisecond):
	case <-tk3Done:
		require.FailNow(t, "tk3 should be blocked")
	}
	tk2.MustExec("commit")
	<-tk3Done
	tk3.MustExec("commit")

	tk2.MustQuery("select * from child").Check(testkit.Rows("1 1"))
	tk2.MustExec("admin check table parent, child")

	tk1.MustExec("delete from child")

	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk3.MustExec("begin pessimistic")

	tk1.MustExec("select * from parent where id in (1, 2) for update")

	tk2ErrCh := make(chan error)
	go func() {
		_, err := tk2.Exec("insert into child values(1, 1)")
		tk2ErrCh <- err
	}()
	tk3ErrCh := make(chan error)
	go func() {
		_, err := tk3.Exec("insert into child values(1, 2)")
		tk3ErrCh <- err
	}()

	select {
	case <-time.After(500 * time.Millisecond):
	case <-tk2ErrCh:
		require.FailNow(t, "tk2 should be blocked")
	case <-tk3ErrCh:
		require.FailNow(t, "tk3 should be blocked")
	}
	tk1.MustExec("commit")

	var (
		results    [][]any
		anotherErr func() error
	)
	select {
	case err := <-tk2ErrCh:
		results = append(results, []any{"1", "1"})
		require.Nil(t, err)
		tk2.MustExec("commit")
		anotherErr = func() error {
			err := <-tk3ErrCh
			tk3.MustExec("commit")
			return err
		}
	case err := <-tk3ErrCh:
		results = append(results, []any{"1", "2"})
		require.Nil(t, err)
		tk3.MustExec("commit")
		anotherErr = func() error {
			err := <-tk2ErrCh
			tk2.MustExec("commit")
			return err
		}
	}

	require.Error(t, anotherErr())

	tk1.MustQuery("select * from child").Check(results)
	tk1.MustExec("admin check table parent, child")
}

func TestSharedLockLockView(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	testTk := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	testTk.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")

	prepareForeignKeyTables(tk1)

	conn2 := tk2.MustQuery("select connection_id()").Rows()[0][0].(string)

	// Case1: shared lock waits for exclusive lock on parent row
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from parent where id=1 for update") // lock parent row(exclusive)

	insertDoneCh := make(chan error, 1)
	tk2.MustExec("begin pessimistic")
	conn2TxnID := tk2.Session().TxnInfo().StartTS
	go func() {
		_, err := tk2.Exec("insert into child values (1, 1)")
		if err == nil {
			_, err = tk2.Exec("commit")
		}
		insertDoneCh <- err
	}()

	select {
	case <-time.After(500 * time.Millisecond):
	case <-insertDoneCh:
		require.FailNow(t, "insert should be blocked")
		return
	}

	lock_waits := testTk.MustQuery("select `key`, count(*) as `count` from INFORMATION_SCHEMA.DATA_LOCK_WAITS group by `key` order by `count` desc;").Rows()
	require.Len(t, lock_waits, 1)
	key := lock_waits[0][0].(string)
	count := lock_waits[0][1].(string)
	require.Equal(t, count, "1")

	txn_waits := testTk.MustQuery(fmt.Sprintf("select TRX_ID, SESSION_ID from INFORMATION_SCHEMA.DATA_LOCK_WAITS as l left join INFORMATION_SCHEMA.TIDB_TRX as trx on l.trx_id = trx.id where l.key = \"%s\"", key)).Rows()
	require.Len(t, txn_waits, 1)
	waitingTxnID := txn_waits[0][0].(string)
	sessionID := txn_waits[0][1].(string)
	require.Equal(t, waitingTxnID, fmt.Sprintf("%d", conn2TxnID))
	require.Equal(t, sessionID, conn2)

	tk1.MustExec("commit")
	require.NoError(t, <-insertDoneCh)
	tk1.MustQuery("select * from child").Check(testkit.Rows("1 1"))

	// Case2: exclusive lock waits for shared lock on parent row
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("insert into child values (2, 1)") // lock parent row (shared)

	exclusiveLockDoneCh := make(chan error, 1)
	tk2.MustExec("begin pessimistic")
	conn2TxnID = tk2.Session().TxnInfo().StartTS
	go func() {
		_, err := tk2.Exec("select * from parent where id=1 for update")
		if err == nil {
			_, err = tk2.Exec("commit")
		}
		exclusiveLockDoneCh <- err
	}()

	select {
	case <-time.After(500 * time.Millisecond):
	case <-exclusiveLockDoneCh:
		require.FailNow(t, "exclusive lock should be blocked")
		return
	}

	lock_waits = testTk.MustQuery("select `key`, count(*) as `count` from INFORMATION_SCHEMA.DATA_LOCK_WAITS group by `key` order by `count` desc;").Rows()
	require.Len(t, lock_waits, 1)
	key = lock_waits[0][0].(string)
	count = lock_waits[0][1].(string)
	require.Equal(t, count, "1")

	txn_waits = testTk.MustQuery(fmt.Sprintf("select TRX_ID, SESSION_ID from INFORMATION_SCHEMA.DATA_LOCK_WAITS as l left join INFORMATION_SCHEMA.TIDB_TRX as trx on l.trx_id = trx.id where l.key = \"%s\"", key)).Rows()
	require.Len(t, txn_waits, 1)
	waitingTxnID = txn_waits[0][0].(string)
	sessionID = txn_waits[0][1].(string)
	require.Equal(t, waitingTxnID, fmt.Sprintf("%d", conn2TxnID))
	require.Equal(t, sessionID, conn2)

	tk1.MustExec("commit")
	require.NoError(t, <-exclusiveLockDoneCh)
}
