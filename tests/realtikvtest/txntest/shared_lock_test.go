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
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type dropSuccessfulPessimisticLockResponseClient struct {
	tikv.Client
	startTS uint64
	key     []byte
	dropped atomic.Bool
}

type capturedSharedLockRequest struct {
	addr        string
	requestCtx  kvrpcpb.Context
	startTS     uint64
	forUpdateTS uint64
	key         []byte
}

type captureSharedLockClient struct {
	tikv.Client
	targetStartTS uint64
	targetKey     []byte
	mu            sync.Mutex
	captured      *capturedSharedLockRequest
}

func (c *captureSharedLockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdPessimisticLock {
		lockReq := req.PessimisticLock()
		if lockReq.GetStartVersion() == c.targetStartTS {
			for _, mutation := range lockReq.GetMutations() {
				if mutation.GetOp() == kvrpcpb.Op_SharedPessimisticLock && bytes.Equal(mutation.GetKey(), c.targetKey) {
					c.mu.Lock()
					if c.captured == nil {
						c.captured = &capturedSharedLockRequest{
							addr: addr, requestCtx: req.Context,
							startTS: lockReq.GetStartVersion(), forUpdateTS: lockReq.GetForUpdateTs(),
							key: bytes.Clone(mutation.GetKey()),
						}
					}
					c.mu.Unlock()
				}
			}
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *captureSharedLockClient) rollbackCapturedSharedLock(ctx context.Context) error {
	c.mu.Lock()
	if c.captured == nil {
		c.mu.Unlock()
		return errors.New("shared lock request was not captured")
	}
	captured := *c.captured
	captured.key = bytes.Clone(c.captured.key)
	c.mu.Unlock()

	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticRollback, &kvrpcpb.PessimisticRollbackRequest{
		StartVersion: captured.startTS,
		ForUpdateTs:  captured.forUpdateTS,
		Keys:         [][]byte{captured.key},
	}, captured.requestCtx)
	resp, err := c.Client.SendRequest(ctx, captured.addr, req, 10*time.Second)
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("pessimistic rollback returned a nil response")
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return err
	}
	if regionErr != nil {
		return errors.Errorf("pessimistic rollback region error: %s", regionErr)
	}
	rollbackResp, ok := resp.Resp.(*kvrpcpb.PessimisticRollbackResponse)
	if !ok {
		return errors.Errorf("unexpected pessimistic rollback response %T", resp.Resp)
	}
	if len(rollbackResp.Errors) != 0 {
		return errors.Errorf("pessimistic rollback key errors: %v", rollbackResp.Errors)
	}
	return nil
}

func (c *dropSuccessfulPessimisticLockResponseClient) SendRequest(
	ctx context.Context,
	addr string,
	req *tikvrpc.Request,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if err != nil || resp == nil || req.Type != tikvrpc.CmdPessimisticLock {
		return resp, err
	}
	lockReq := req.PessimisticLock()
	if lockReq.GetStartVersion() != c.startTS || len(lockReq.Mutations) != 1 {
		return resp, nil
	}
	mutation := lockReq.Mutations[0]
	if mutation.Op != kvrpcpb.Op_PessimisticLock || !bytes.Equal(mutation.Key, c.key) {
		return resp, nil
	}
	lockResp, ok := resp.Resp.(*kvrpcpb.PessimisticLockResponse)
	if !ok || lockResp.GetRegionError() != nil || len(lockResp.Errors) > 0 || !c.dropped.CompareAndSwap(false, true) {
		return resp, nil
	}
	return &tikvrpc.Response{}, nil
}

func prepareForeignKeyTables(tk *testkit.TestKit) {
	tk.MustExec("drop table if exists child, parent")
	tk.MustExec("create table parent (id int primary key)")
	tk.MustExec("create table child (id int primary key, pid int, foreign key (pid) references parent(id))")
	tk.MustExec("insert into parent values (1), (2)")
}

func allowForeignKeyCheckInSharedLockForTest(t *testing.T) {
	t.Helper()
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowEnableForeignKeyCheckInSharedLock = true
	})
}

func prepareSharedLockUpgradeTables(tk *testkit.TestKit, fkOptions string) {
	tk.MustExec("drop table if exists child, parent")
	tk.MustExec("create table parent (id int primary key, v int)")
	childTableSQL := "create table child (id int primary key, pid int, foreign key (pid) references parent(id)"
	if fkOptions != "" {
		childTableSQL += " " + fkOptions
	}
	childTableSQL += ")"
	tk.MustExec(childTableSQL)
	tk.MustExec("insert into parent values (1, 0), (2, 0)")
}

func enableSharedLockUpgrade(tks ...*testkit.TestKit) {
	for _, tk := range tks {
		tk.MustExec("set @@tidb_enable_shared_lock_upgrade = ON")
	}
}

func requireTxnLockAcquiring(t *testing.T, waitingTk *testkit.TestKit) {
	require.Eventuallyf(t, func() bool {
		info := waitingTk.Session().TxnInfo()
		return info != nil && info.State == txninfo.TxnLockAcquiring && info.BlockStartTime.Valid
	}, 10*time.Second, 100*time.Millisecond, "expected session %d to be waiting on lock acquisition", waitingTk.Session().GetSessionVars().ConnectionID)
}

func requireStorageLockWait(t *testing.T, store kv.Storage, startTS uint64, key []byte) {
	t.Helper()
	require.Eventually(t, func() bool {
		entries, err := store.GetLockWaits()
		if err != nil {
			return false
		}
		for _, entry := range entries {
			if entry.GetTxn() == startTS && bytes.Equal(entry.GetKey(), key) {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)
}

func TestForeignKeySharedLockOptimisticReverseReferenceOrder(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")

	prepareForeignKeyTables(tk1)

	// Foreign-key shared locks do not support optimistic transactions. The checks below reference
	// parent rows in reverse orders, so the second optimistic transaction should fail at commit
	// time with a write conflict.
	tk1.MustExec("begin optimistic")
	tk2.MustExec("begin optimistic")

	tk1.MustExec("insert into child values (1, 1)")
	tk2.MustExec("insert into child values (3, 2)")

	tk1.MustExec("insert into child values (2, 2)")
	tk2.MustExec("insert into child values (4, 1)")

	tk1.MustExec("commit")
	err := tk2.ExecToErr("commit")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Write conflict")

	tk1.MustQuery("select * from child order by id").Check(testkit.Rows("1 1", "2 2"))
}

func TestForeignKeySharedLockPessimisticReverseReferenceOrder(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk1.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")

	prepareForeignKeyTables(tk1)

	// Pessimistic transactions can acquire compatible foreign-key shared locks, so the same
	// reverse reference order should let both transactions commit.
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")

	tk1.MustExec("insert into child values (1, 1)")
	tk2.MustExec("insert into child values (3, 2)")

	tk1.MustExec("insert into child values (2, 2)")
	tk2.MustExec("insert into child values (4, 1)")

	tk1.MustExec("commit")
	tk2.MustExec("commit")

	tk1.MustQuery("select * from child order by id").Check(testkit.Rows("1 1", "2 2", "3 2", "4 1"))
}

func TestSharedLockBlockedByExclusiveLock(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk1.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk3.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk3.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")

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
	tk1.MustExec("admin check table parent")
	tk1.MustExec("admin check table child")
}

func TestSharedLockBlockExclusiveLock(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk1.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk3.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk3.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")

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
	tk1.MustExec("admin check table parent")
	tk1.MustExec("admin check table child")
}

func TestSharedLockUpgrade(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	if !kerneltype.IsNextGen() {
		t.Skip("shared lock upgrade rollout acceptance is only required on next-gen")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)

	t.Run("shared_lock_upgrade_waits_for_last_holder", func(t *testing.T) {
		tk1 := testkit.NewTestKit(t, store)
		tk2 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk2.MustExec("use test")
		tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
		tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
		enableSharedLockUpgrade(tk1, tk2)
		prepareSharedLockUpgradeTables(tk1, "")

		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		tk1.MustExec("insert into child values(1, 1)")
		tk2.MustExec("insert into child values(2, 1)")

		upgraderDone := make(chan error, 1)
		go func() {
			upgraderDone <- tk1.ExecToErr("update parent set v = v + 1 where id = 1")
		}()

		requireTxnLockAcquiring(t, tk1)

		tk2.MustExec("commit")
		require.NoError(t, <-upgraderDone)
		tk1.MustExec("commit")

		tk1.MustQuery("select * from parent order by id").Check(testkit.Rows("1 1", "2 0"))
		tk1.MustQuery("select * from child order by id").Check(testkit.Rows("1 1", "2 1"))
		tk1.MustExec("admin check table parent")
		tk1.MustExec("admin check table child")
	})

	t.Run("shared_lock_lost_rolls_back_transaction", func(t *testing.T) {
		tkU := testkit.NewTestKit(t, store)
		tkH := testkit.NewTestKit(t, store)
		tkVerify := testkit.NewTestKit(t, store)
		for _, tk := range []*testkit.TestKit{tkU, tkH, tkVerify} {
			tk.MustExec("use test")
			tk.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
		}
		enableSharedLockUpgrade(tkU, tkH, tkVerify)
		prepareSharedLockUpgradeTables(tkU, "")
		parentTableID := external.GetTableByName(t, tkU, "test", "parent").Meta().ID
		upgradeKey := tablecodec.EncodeRowKeyWithHandle(parentTableID, kv.IntHandle(1))

		tkU.MustExec("begin pessimistic")
		txnU, err := tkU.Session().Txn(false)
		require.NoError(t, err)
		tikvStore, ok := store.(interface {
			GetTiKVClient() tikv.Client
			SetTiKVClient(tikv.Client)
		})
		require.True(t, ok)
		originalClient := tikvStore.GetTiKVClient()
		capturingClient := &captureSharedLockClient{
			Client: originalClient, targetStartTS: txnU.StartTS(), targetKey: upgradeKey,
		}
		tikvStore.SetTiKVClient(capturingClient)
		t.Cleanup(func() { tikvStore.SetTiKVClient(originalClient) })

		tkU.MustExec("insert into child values(1, 1)")
		tkH.MustExec("begin pessimistic")
		tkH.MustExec("insert into child values(2, 1)")

		upgradeDone := make(chan error, 1)
		go func() {
			upgradeDone <- tkU.ExecToErr("update parent set v = v + 1 where id = 1")
		}()
		requireTxnLockAcquiring(t, tkU)
		requireStorageLockWait(t, store, txnU.StartTS(), upgradeKey)
		require.NoError(t, capturingClient.rollbackCapturedSharedLock(context.Background()))

		var upgradeErr error
		select {
		case upgradeErr = <-upgradeDone:
		case <-time.After(10 * time.Second):
			require.FailNow(t, "shared lock upgrader did not return after holder removal")
		}
		require.Error(t, upgradeErr)
		cause, ok := errors.Cause(upgradeErr).(*errors.Error)
		require.True(t, ok)
		require.Equal(t, errno.ErrSharedLockLost, int(cause.Code()))
		require.False(t, tkU.Session().GetSessionVars().InTxn())
		require.Nil(t, tkU.Session().TxnInfo())
		require.Nil(t, sessiontxn.GetTxnManager(tkU.Session()).GetContextProvider())

		tikvStore.SetTiKVClient(originalClient)
		tkVerify.MustExec("set foreign_key_checks = OFF")
		tkVerify.MustExec("insert into child values(1, 1)")
		tkH.MustExec("rollback")
		tkVerify.MustQuery("select * from parent order by id").Check(testkit.Rows("1 0", "2 0"))
		tkVerify.MustQuery("select * from child order by id").Check(testkit.Rows("1 1"))
		tkVerify.MustExec("admin check table parent")
		tkVerify.MustExec("admin check table child")
	})

	t.Run("upgrade_error_keeps_explicit_transaction_usable", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
		enableSharedLockUpgrade(tk)
		prepareSharedLockUpgradeTables(tk, "")

		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into child values(1, 1)")

		const failpointName = "tikvclient/beforePessimisticLock"
		testfailpoint.Enable(t, failpointName, `return("fail")`)
		err := tk.ExecToErr("update parent set v = v + 1 where id = 1")
		require.Error(t, err)
		require.False(t, terror.ErrResultUndetermined.Equal(err), "unexpected error: %v", err)
		require.ErrorContains(t, err, "injected failure at pessimistic lock")
		require.True(t, tk.Session().GetSessionVars().InTxn())
		require.NotNil(t, tk.Session().TxnInfo())
		testfailpoint.Disable(t, failpointName)

		tk.MustExec("update parent set v = v + 1 where id = 1")
		tk.MustExec("commit")
		tk.MustQuery("select * from parent order by id").Check(testkit.Rows("1 1", "2 0"))
		tk.MustQuery("select * from child order by id").Check(testkit.Rows("1 1"))
		tk.MustExec("admin check table parent")
		tk.MustExec("admin check table child")
	})

	t.Run("applied_upgrade_with_missing_response_keeps_explicit_transaction_usable", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
		enableSharedLockUpgrade(tk)
		prepareSharedLockUpgradeTables(tk, "")

		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into child values(1, 1)")
		txn, err := tk.Session().Txn(false)
		require.NoError(t, err)
		parentTableID := external.GetTableByName(t, tk, "test", "parent").Meta().ID
		upgradeKey := tablecodec.EncodeRowKeyWithHandle(parentTableID, kv.IntHandle(1))

		tikvStore, ok := store.(interface {
			GetTiKVClient() tikv.Client
			SetTiKVClient(tikv.Client)
		})
		require.True(t, ok)
		originalClient := tikvStore.GetTiKVClient()
		droppingClient := &dropSuccessfulPessimisticLockResponseClient{
			Client:  originalClient,
			startTS: txn.StartTS(),
			key:     upgradeKey,
		}
		tikvStore.SetTiKVClient(droppingClient)
		t.Cleanup(func() { tikvStore.SetTiKVClient(originalClient) })
		err = tk.ExecToErr("update parent set v = v + 1 where id = 1")
		tikvStore.SetTiKVClient(originalClient)

		require.Error(t, err)
		require.False(t, terror.ErrResultUndetermined.Equal(err), "unexpected error: %v", err)
		require.ErrorContains(t, err, "response body is missing")
		require.True(t, droppingClient.dropped.Load())
		require.True(t, tk.Session().GetSessionVars().InTxn())
		require.NotNil(t, tk.Session().TxnInfo())

		tk.MustExec("update parent set v = v + 1 where id = 1")
		tk.MustExec("commit")
		tk.MustQuery("select * from parent order by id").Check(testkit.Rows("1 1", "2 0"))
		tk.MustQuery("select * from child order by id").Check(testkit.Rows("1 1"))
		tk.MustExec("admin check table parent")
		tk.MustExec("admin check table child")
	})

	t.Run("second_upgrader_returns_deadlock", func(t *testing.T) {
		tk1 := testkit.NewTestKit(t, store)
		tk2 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk2.MustExec("use test")
		tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
		tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
		enableSharedLockUpgrade(tk1, tk2)
		prepareSharedLockUpgradeTables(tk1, "")

		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		tk1.MustExec("insert into child values(1, 1)")
		tk2.MustExec("insert into child values(2, 1)")

		upgraderDone := make(chan error, 1)
		go func() {
			upgraderDone <- tk1.ExecToErr("update parent set v = v + 1 where id = 1")
		}()

		requireTxnLockAcquiring(t, tk1)

		tk2.MustGetErrCode("update parent set v = v + 2 where id = 1", errno.ErrLockDeadlock)
		require.False(t, tk2.Session().GetSessionVars().InTxn())
		require.Nil(t, tk2.Session().TxnInfo())
		require.NoError(t, <-upgraderDone)
		tk1.MustExec("commit")

		tk1.MustQuery("select * from parent order by id").Check(testkit.Rows("1 1", "2 0"))
		tk1.MustQuery("select * from child order by id").Check(testkit.Rows("1 1"))
		tk1.MustExec("admin check table parent")
		tk1.MustExec("admin check table child")
	})
}

func TestSharedLockChildTableConflict(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk1.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk3.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk3.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")

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
	tk2.MustExec("admin check table parent")
	tk2.MustExec("admin check table child")

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
	tk1.MustExec("admin check table parent")
	tk1.MustExec("admin check table child")
}

func TestSharedLockCascadeUpdateExplicitPessimisticTxn(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)

	for _, constraintCheckInPlace := range []string{"ON", "OFF"} {
		t.Run("constraint_check_in_place_pessimistic_"+constraintCheckInPlace, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set @@global.tidb_enable_foreign_key=1")
			defer tk.MustExec("set @@global.tidb_enable_foreign_key=default")
			tk.MustExec("set @@foreign_key_checks=1")
			tk.MustExec("set @@tidb_foreign_key_check_in_shared_lock=ON")
			tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=" + constraintCheckInPlace)

			tk.MustExec("drop table if exists c, p")
			tk.MustExec("create table p(id int primary key)")
			tk.MustExec("create table c(pid int, foreign key(pid) references p(id) on delete cascade on update cascade)")
			tk.MustExec("insert into p values (1)")
			tk.MustExec("insert into c values (1)")

			tk.MustExec("begin pessimistic")
			tk.MustExec("update p set id = 2 where id = 1")
			tk.MustQuery("select pid from c").Check(testkit.Rows("2"))
			tk.MustExec("commit")

			tk.MustQuery("select pid from c").Check(testkit.Rows("2"))
			tk.MustExec("delete from p where id = 2")
			tk.MustQuery("select count(*) from c").Check(testkit.Rows("0"))
			tk.MustExec("admin check table p")
			tk.MustExec("admin check table c")
		})
	}

	t.Run("insert_child_then_update_parent", func(t *testing.T) {
		if !kerneltype.IsNextGen() {
			t.Skip("shared lock upgrade rollout acceptance is only required on next-gen")
		}

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@global.tidb_enable_foreign_key=1")
		defer tk.MustExec("set @@global.tidb_enable_foreign_key=default")
		tk.MustExec("set @@foreign_key_checks=1")
		tk.MustExec("set @@tidb_foreign_key_check_in_shared_lock=ON")
		enableSharedLockUpgrade(tk)
		prepareSharedLockUpgradeTables(tk, "")

		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into child values (1, 1)")
		tk.MustExec("update parent set v = v + 1 where id = 1")
		tk.MustExec("commit")

		tk.MustQuery("select * from parent order by id").Check(testkit.Rows("1 1", "2 0"))
		tk.MustQuery("select * from child order by id").Check(testkit.Rows("1 1"))
		tk.MustExec("admin check table parent")
		tk.MustExec("admin check table child")
	})

	t.Run("insert_child_then_delete_parent_restrict", func(t *testing.T) {
		if !kerneltype.IsNextGen() {
			t.Skip("shared lock upgrade rollout acceptance is only required on next-gen")
		}

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@global.tidb_enable_foreign_key=1")
		defer tk.MustExec("set @@global.tidb_enable_foreign_key=default")
		tk.MustExec("set @@foreign_key_checks=1")
		tk.MustExec("set @@tidb_foreign_key_check_in_shared_lock=ON")
		enableSharedLockUpgrade(tk)
		prepareSharedLockUpgradeTables(tk, "")

		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into child values (1, 1)")
		tk.MustGetErrCode("delete from parent where id = 1", errno.ErrRowIsReferenced2)
		tk.MustExec("commit")

		tk.MustQuery("select * from parent order by id").Check(testkit.Rows("1 0", "2 0"))
		tk.MustQuery("select * from child order by id").Check(testkit.Rows("1 1"))
		tk.MustExec("admin check table parent")
		tk.MustExec("admin check table child")
	})

	t.Run("insert_child_then_delete_parent_cascade", func(t *testing.T) {
		if !kerneltype.IsNextGen() {
			t.Skip("shared lock upgrade rollout acceptance is only required on next-gen")
		}

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@global.tidb_enable_foreign_key=1")
		defer tk.MustExec("set @@global.tidb_enable_foreign_key=default")
		tk.MustExec("set @@foreign_key_checks=1")
		tk.MustExec("set @@tidb_foreign_key_check_in_shared_lock=ON")
		enableSharedLockUpgrade(tk)
		prepareSharedLockUpgradeTables(tk, "on delete cascade")

		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into child values (1, 1)")
		tk.MustExec("delete from parent where id = 1")
		tk.MustExec("commit")

		tk.MustQuery("select * from parent order by id").Check(testkit.Rows("2 0"))
		tk.MustQuery("select * from child").Check(testkit.Rows())
		tk.MustExec("admin check table parent")
		tk.MustExec("admin check table child")
	})
}

func TestSharedLockLockView(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	testTk := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	testTk.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk1.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")

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

	lockWaits := testTk.MustQuery("select `key`, count(*) as `count` from INFORMATION_SCHEMA.DATA_LOCK_WAITS group by `key` order by `count` desc;").Rows()
	require.Len(t, lockWaits, 1)
	key := lockWaits[0][0].(string)
	count := lockWaits[0][1].(string)
	require.Equal(t, count, "1")

	txnWaits := testTk.MustQuery(fmt.Sprintf("select TRX_ID, SESSION_ID from INFORMATION_SCHEMA.DATA_LOCK_WAITS as l left join INFORMATION_SCHEMA.TIDB_TRX as trx on l.trx_id = trx.id where l.key = \"%s\"", key)).Rows()
	require.Len(t, txnWaits, 1)
	waitingTxnID := txnWaits[0][0].(string)
	sessionID := txnWaits[0][1].(string)
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

	lockWaits = testTk.MustQuery("select `key`, count(*) as `count` from INFORMATION_SCHEMA.DATA_LOCK_WAITS group by `key` order by `count` desc;").Rows()
	require.GreaterOrEqual(t, len(lockWaits), 1)
	key = lockWaits[0][0].(string)

	txnWaits = testTk.MustQuery(fmt.Sprintf("select TRX_ID, SESSION_ID from INFORMATION_SCHEMA.DATA_LOCK_WAITS as l left join INFORMATION_SCHEMA.TIDB_TRX as trx on l.trx_id = trx.id where l.key = \"%s\"", key)).Rows()
	require.GreaterOrEqual(t, len(txnWaits), 1)
	waitingTxnID = txnWaits[0][0].(string)
	sessionID = txnWaits[0][1].(string)
	require.Equal(t, waitingTxnID, fmt.Sprintf("%d", conn2TxnID))
	require.Equal(t, sessionID, conn2)

	tk1.MustExec("commit")
	require.NoError(t, <-exclusiveLockDoneCh)
}

func TestSharedLockDataLockWaitsFromStorageWaitTable(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	allowForeignKeyCheckInSharedLockForTest(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	testTk := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	testTk.MustExec("use test")
	tk1.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk1.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	tk2.MustExec("set @@tidb_foreign_key_check_in_shared_lock = ON")
	tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/dataLockWaitsSkipResolvingLocks", "return(true)")
	prepareForeignKeyTables(tk1)

	conn2 := tk2.MustQuery("select connection_id()").Rows()[0][0].(string)

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from parent where id=1 for update")
	insertDoneCh := make(chan error, 1)
	insertDone := false
	t.Cleanup(func() {
		if _, err := tk1.Exec("rollback"); err != nil {
			t.Errorf("rollback blocker transaction: %v", err)
		}
		if !insertDone {
			select {
			case err := <-insertDoneCh:
				if err != nil {
					t.Errorf("insert goroutine failed after rolling back blocker transaction: %v", err)
				}
			case <-time.After(time.Second):
				t.Errorf("insert goroutine did not finish after rolling back blocker transaction")
			}
		}
	})

	tk2.MustExec("begin pessimistic")
	conn2TxnID := tk2.Session().TxnInfo().StartTS
	go func() {
		_, err := tk2.Exec("insert into child values (1, 1)")
		if err == nil {
			_, err = tk2.Exec("commit")
		}
		insertDoneCh <- err
	}()

	var (
		insertErr            error
		insertFinishedEarly  bool
		waitingTxnAndSession [][]any
	)
	require.Eventually(t, func() bool {
		select {
		case insertErr = <-insertDoneCh:
			insertDone = true
			insertFinishedEarly = true
			return true
		default:
		}

		waitingTxnAndSession = testTk.MustQuery(fmt.Sprintf(
			"select TRX_ID, SESSION_ID from INFORMATION_SCHEMA.DATA_LOCK_WAITS as l left join INFORMATION_SCHEMA.TIDB_TRX as trx on l.trx_id = trx.id where l.trx_id = %d and trx.session_id = %s",
			conn2TxnID, conn2,
		)).Rows()
		return len(waitingTxnAndSession) > 0
	}, 10*time.Second, 100*time.Millisecond)
	require.Falsef(t, insertFinishedEarly, "insert should be blocked before DATA_LOCK_WAITS row is observed, err: %v", insertErr)
	require.Len(t, waitingTxnAndSession, 1)
	waitingTxnID := waitingTxnAndSession[0][0].(string)
	sessionID := waitingTxnAndSession[0][1].(string)
	require.Equal(t, waitingTxnID, fmt.Sprintf("%d", conn2TxnID))
	require.Equal(t, sessionID, conn2)

	tk1.MustExec("commit")
	insertErr = <-insertDoneCh
	insertDone = true
	require.NoError(t, insertErr)
	tk1.MustQuery("select * from child").Check(testkit.Rows("1 1"))
}
