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

package session_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testPessimisticSuite{})

type testPessimisticSuite struct {
	testSessionSuiteBase
}

func (s *testPessimisticSuite) SetUpSuite(c *C) {
	s.testSessionSuiteBase.SetUpSuite(c)
	// Set it to 300ms for testing lock resolve.
	atomic.StoreUint64(&tikv.ManagedLockTTL, 300)
	tikv.PrewriteMaxBackoff = 500
}

func (s *testPessimisticSuite) TearDownSuite(c *C) {
	s.testSessionSuiteBase.TearDownSuite(c)
	tikv.PrewriteMaxBackoff = 20000
}

func (s *testPessimisticSuite) TestPessimisticTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	// Make the name has different indent for easier read.
	tk1 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists pessimistic")
	tk.MustExec("create table pessimistic (k int, v int)")
	tk.MustExec("insert into pessimistic values (1, 1)")

	// t1 lock, t2 update, t1 update and retry statement.
	tk1.MustExec("begin pessimistic")

	tk.MustExec("update pessimistic set v = 2 where v = 1")

	// Update can see the change, so this statement affects 0 rows.
	tk1.MustExec("update pessimistic set v = 3 where v = 1")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(0))
	c.Assert(session.GetHistory(tk1.Se).Count(), Equals, 0)
	// select for update can see the change of another transaction.
	tk1.MustQuery("select * from pessimistic for update").Check(testkit.Rows("1 2"))
	// plain select can not see the change of another transaction.
	tk1.MustQuery("select * from pessimistic").Check(testkit.Rows("1 1"))
	tk1.MustExec("update pessimistic set v = 3 where v = 2")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(1))

	// pessimistic lock doesn't block read operation of other transactions.
	tk.MustQuery("select * from pessimistic").Check(testkit.Rows("1 2"))

	tk1.MustExec("commit")
	tk1.MustQuery("select * from pessimistic").Check(testkit.Rows("1 3"))

	// t1 lock, t1 select for update, t2 wait t1.
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from pessimistic where k = 1 for update")
	finishCh := make(chan struct{})
	go func() {
		tk.MustExec("update pessimistic set v = 5 where k = 1")
		finishCh <- struct{}{}
	}()
	time.Sleep(time.Millisecond * 10)
	tk1.MustExec("update pessimistic set v = 3 where k = 1")
	tk1.MustExec("commit")
	<-finishCh
	tk.MustQuery("select * from pessimistic").Check(testkit.Rows("1 5"))
}

func (s *testPessimisticSuite) TestTxnMode(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tests := []struct {
		beginStmt     string
		txnMode       string
		isPessimistic bool
	}{
		{"pessimistic", "pessimistic", true},
		{"pessimistic", "optimistic", true},
		{"pessimistic", "", true},
		{"optimistic", "pessimistic", false},
		{"optimistic", "optimistic", false},
		{"optimistic", "", false},
		{"", "pessimistic", true},
		{"", "optimistic", false},
		{"", "", false},
	}
	for _, tt := range tests {
		tk.MustExec(fmt.Sprintf("set @@tidb_txn_mode = '%s'", tt.txnMode))
		tk.MustExec("begin " + tt.beginStmt)
		c.Check(tk.Se.GetSessionVars().TxnCtx.IsPessimistic, Equals, tt.isPessimistic)
		tk.MustExec("rollback")
	}

	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("create table if not exists txn_mode (a int)")
	tests2 := []struct {
		txnMode       string
		isPessimistic bool
	}{
		{"pessimistic", true},
		{"optimistic", false},
		{"", false},
	}
	for _, tt := range tests2 {
		tk.MustExec(fmt.Sprintf("set @@tidb_txn_mode = '%s'", tt.txnMode))
		tk.MustExec("rollback")
		tk.MustExec("insert txn_mode values (1)")
		c.Check(tk.Se.GetSessionVars().TxnCtx.IsPessimistic, Equals, tt.isPessimistic)
		tk.MustExec("rollback")
	}
	tk.MustExec("set @@global.tidb_txn_mode = 'pessimistic'")
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustQuery("select @@tidb_txn_mode").Check(testkit.Rows("pessimistic"))
	tk1.MustExec("set @@autocommit = 0")
	tk1.MustExec("insert txn_mode values (2)")
	c.Check(tk1.Se.GetSessionVars().TxnCtx.IsPessimistic, IsTrue)
	tk1.MustExec("set @@tidb_txn_mode = ''")
	tk1.MustExec("rollback")
	tk1.MustExec("insert txn_mode values (2)")
	c.Check(tk1.Se.GetSessionVars().TxnCtx.IsPessimistic, IsFalse)
	tk1.MustExec("rollback")
}

func (s *testPessimisticSuite) TestDeadlock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists deadlock")
	tk.MustExec("create table deadlock (k int primary key, v int)")
	tk.MustExec("insert into deadlock values (1, 1), (2, 1)")

	syncCh := make(chan error)
	go func() {
		tk1 := testkit.NewTestKitWithInit(c, s.store)
		tk1.MustExec("begin pessimistic")
		tk1.MustExec("update deadlock set v = v + 1 where k = 2")
		syncCh <- nil
		_, err := tk1.Exec("update deadlock set v = v + 1 where k = 1")
		syncCh <- err
	}()
	tk.MustExec("begin pessimistic")
	tk.MustExec("update deadlock set v = v + 1 where k = 1")
	<-syncCh
	_, err1 := tk.Exec("update deadlock set v = v + 1 where k = 2")
	err2 := <-syncCh
	// Either err1 or err2 is deadlock error.
	var err error
	if err1 != nil {
		c.Assert(err2, IsNil)
		err = err1
	} else {
		err = err2
	}
	e, ok := errors.Cause(err).(*terror.Error)
	c.Assert(ok, IsTrue)
	c.Assert(int(e.Code()), Equals, mysql.ErrLockDeadlock)
}

func (s *testPessimisticSuite) TestSingleStatementRollback(c *C) {
	if *withTiKV {
		c.Skip("skip with tikv because cluster manipulate is not available")
	}
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists pessimistic")
	tk.MustExec("create table single_statement (id int primary key, v int)")
	tk.MustExec("insert into single_statement values (1, 1), (2, 1), (3, 1), (4, 1)")
	tblID := tk.GetTableID("single_statement")
	s.cluster.SplitTable(tblID, 2)
	region1Key := codec.EncodeBytes(nil, tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(1)))
	region1, _ := s.cluster.GetRegionByKey(region1Key)
	region1ID := region1.Id
	region2Key := codec.EncodeBytes(nil, tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(3)))
	region2, _ := s.cluster.GetRegionByKey(region2Key)
	region2ID := region2.Id

	syncCh := make(chan bool)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/SingleStmtDeadLockRetrySleep", "return"), IsNil)
	go func() {
		tk2.MustExec("begin pessimistic")
		<-syncCh
		// tk2 will go first, so tk will meet deadlock and retry, tk2 will resolve pessimistic rollback
		// lock on key 3 after lock ttl
		s.cluster.ScheduleDelay(tk2.Se.GetSessionVars().TxnCtx.StartTS, region2ID, time.Millisecond*3)
		tk2.MustExec("update single_statement set v = v + 1")
		tk2.MustExec("commit")
		<-syncCh
	}()
	tk.MustExec("begin pessimistic")
	syncCh <- true
	s.cluster.ScheduleDelay(tk.Se.GetSessionVars().TxnCtx.StartTS, region1ID, time.Millisecond*10)
	tk.MustExec("update single_statement set v = v + 1")
	tk.MustExec("commit")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/SingleStmtDeadLockRetrySleep"), IsNil)
	syncCh <- true
}

func (s *testPessimisticSuite) TestFirstStatementFail(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists first")
	tk.MustExec("create table first (k int unique)")
	tk.MustExec("insert first values (1)")
	tk.MustExec("begin pessimistic")
	_, err := tk.Exec("insert first values (1)")
	c.Assert(err, NotNil)
	tk.MustExec("insert first values (2)")
	tk.MustExec("commit")
}

func (s *testPessimisticSuite) TestKeyExistsCheck(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists chk")
	tk.MustExec("create table chk (k int primary key)")
	tk.MustExec("insert chk values (1)")
	tk.MustExec("delete from chk where k = 1")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert chk values (1)")
	tk.MustExec("commit")

	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("begin optimistic")
	tk1.MustExec("insert chk values (1), (2), (3)")
	_, err := tk1.Exec("commit")
	c.Assert(err, NotNil)

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert chk values (2)")
	tk.MustExec("commit")
}

func (s *testPessimisticSuite) TestInsertOnDup(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists dup")
	tk.MustExec("create table dup (id int primary key, c int)")
	tk.MustExec("begin pessimistic")

	tk2.MustExec("insert dup values (1, 1)")
	tk.MustExec("insert dup values (1, 1) on duplicate key update c = c + 1")
	tk.MustExec("commit")
	tk.MustQuery("select * from dup").Check(testkit.Rows("1 2"))
}

func (s *testPessimisticSuite) TestPointGetKeyLock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int primary key, u int unique, c int)")
	syncCh := make(chan struct{})

	tk.MustExec("begin pessimistic")
	tk.MustExec("update point set c = c + 1 where id = 1")
	tk.MustExec("delete from point where u = 2")
	go func() {
		tk2.MustExec("begin pessimistic")
		_, err1 := tk2.Exec("insert point values (1, 1, 1)")
		c.Check(kv.ErrKeyExists.Equal(err1), IsTrue)
		_, err1 = tk2.Exec("insert point values (2, 2, 2)")
		c.Check(kv.ErrKeyExists.Equal(err1), IsTrue)
		tk2.MustExec("rollback")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustExec("insert point values (1, 1, 1)")
	tk.MustExec("insert point values (2, 2, 2)")
	tk.MustExec("commit")
	syncCh <- struct{}{}

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from point where id = 3 for update")
	tk.MustExec("select * from point where u = 4 for update")
	go func() {
		tk2.MustExec("begin pessimistic")
		_, err1 := tk2.Exec("insert point values (3, 3, 3)")
		c.Check(kv.ErrKeyExists.Equal(err1), IsTrue)
		_, err1 = tk2.Exec("insert point values (4, 4, 4)")
		c.Check(kv.ErrKeyExists.Equal(err1), IsTrue)
		tk2.MustExec("rollback")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustExec("insert point values (3, 3, 3)")
	tk.MustExec("insert point values (4, 4, 4)")
	tk.MustExec("commit")
	syncCh <- struct{}{}
}

func (s *testPessimisticSuite) TestBankTransfer(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists accounts")
	tk.MustExec("create table accounts (id int primary key, c int)")
	tk.MustExec("insert accounts values (1, 100), (2, 100), (3, 100)")
	syncCh := make(chan struct{})

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from accounts where id = 1 for update").Check(testkit.Rows("1 100"))
	go func() {
		tk2.MustExec("begin pessimistic")
		tk2.MustExec("select * from accounts where id = 2 for update")
		<-syncCh
		tk2.MustExec("select * from accounts where id = 3 for update")
		tk2.MustExec("update accounts set c = 50 where id = 2")
		tk2.MustExec("update accounts set c = 150 where id = 3")
		tk2.MustExec("commit")
		<-syncCh
	}()
	syncCh <- struct{}{}
	tk.MustQuery("select * from accounts where id = 2 for update").Check(testkit.Rows("2 50"))
	tk.MustExec("update accounts set c = 50 where id = 1")
	tk.MustExec("update accounts set c = 100 where id = 2")
	tk.MustExec("commit")
	syncCh <- struct{}{}
	tk.MustQuery("select sum(c) from accounts").Check(testkit.Rows("300"))
}

func (s *testPessimisticSuite) TestLockUnchangedRowKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists unchanged")
	tk.MustExec("create table unchanged (id int primary key, c int)")
	tk.MustExec("insert unchanged values (1, 1), (2, 2)")

	tk.MustExec("begin pessimistic")
	tk.MustExec("update unchanged set c = 1 where id < 2")

	tk2.MustExec("begin pessimistic")
	err := tk2.ExecToErr("select * from unchanged where id = 1 for update nowait")
	c.Assert(err, NotNil)

	tk.MustExec("rollback")

	tk2.MustQuery("select * from unchanged where id = 1 for update nowait")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert unchanged values (2, 2) on duplicate key update c = values(c)")

	err = tk2.ExecToErr("select * from unchanged where id = 2 for update nowait")
	c.Assert(err, NotNil)

	tk.MustExec("commit")

	tk2.MustQuery("select * from unchanged where id = 1 for update nowait")
	tk2.MustExec("rollback")
}

func (s *testPessimisticSuite) TestOptimisticConflicts(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists conflict")
	tk.MustExec("create table conflict (id int primary key, c int)")
	tk.MustExec("insert conflict values (1, 1)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from conflict where id = 1 for update")
	syncCh := make(chan struct{})
	go func() {
		tk2.MustExec("update conflict set c = 3 where id = 1")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustExec("update conflict set c = 2 where id = 1")
	tk.MustExec("commit")
	syncCh <- struct{}{}
	tk.MustQuery("select c from conflict where id = 1").Check(testkit.Rows("3"))

	// Check pessimistic lock is not resolved.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update conflict set c = 4 where id = 1")
	tk2.MustExec("begin optimistic")
	tk2.MustExec("update conflict set c = 5 where id = 1")
	_, err := tk2.Exec("commit")
	c.Check(err, NotNil)
	tk.MustExec("rollback")

	// Update snapshotTS after a conflict, invalidate snapshot cache.
	tk.MustExec("truncate table conflict")
	tk.MustExec("insert into conflict values (1, 2)")
	tk.MustExec("begin pessimistic")
	// This SQL use BatchGet and cache data in the txn snapshot.
	// It can be changed to other SQLs that use BatchGet.
	tk.MustExec("insert ignore into conflict values (1, 2)")

	tk2.MustExec("update conflict set c = c - 1")

	// Make the txn update its forUpdateTS.
	tk.MustQuery("select * from conflict where id = 1 for update").Check(testkit.Rows("1 1"))
	// Cover a bug that the txn snapshot doesn't invalidate cache after ts change.
	tk.MustExec("insert into conflict values (1, 999) on duplicate key update c = c + 2")
	tk.MustExec("commit")
	tk.MustQuery("select * from conflict").Check(testkit.Rows("1 3"))
}

func (s *testPessimisticSuite) TestSelectForUpdateNoWait(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk3 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")

	tk.MustExec("set @@autocommit = 0")
	tk2.MustExec("set @@autocommit = 0")
	tk3.MustExec("set @@autocommit = 0")

	// point get with no autocommit
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from tk where c1 = 2 for update") // lock succ

	tk2.MustExec("begin pessimistic")
	_, err := tk2.Exec("select * from tk where c1 = 2 for update nowait")
	c.Check(err, NotNil)
	tk.MustExec("commit")
	tk2.MustExec("select * from tk where c1 = 2 for update nowait") // lock succ

	tk3.MustExec("begin pessimistic")
	_, err = tk3.Exec("select * from tk where c1 = 2 for update nowait")
	c.Check(err, NotNil)

	tk2.MustExec("commit")
	tk3.MustExec("select * from tk where c1 = 2 for update")
	tk3.MustExec("commit")
	tk.MustExec("commit")

	tk3.MustExec("begin pessimistic")
	tk3.MustExec("update tk set c2 = c2 + 1 where c1 = 3")
	tk2.MustExec("begin pessimistic")
	_, err = tk2.Exec("select * from tk where c1 = 3 for update nowait")
	c.Check(err, NotNil)
	tk3.MustExec("commit")
	tk2.MustExec("select * from tk where c1 = 3 for update nowait")
	tk2.MustExec("commit")

	tk.MustExec("commit")
	tk2.MustExec("commit")
	tk3.MustExec("commit")

	// scan with no autocommit
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from tk where c1 >= 2 for update")
	tk2.MustExec("begin pessimistic")
	_, err = tk2.Exec("select * from tk where c1 = 2 for update nowait")
	c.Check(err, NotNil)
	_, err = tk2.Exec("select * from tk where c1 > 3 for update nowait")
	c.Check(err, NotNil)
	tk2.MustExec("select * from tk where c1 = 1 for update nowait")
	tk2.MustExec("commit")
	tk.MustQuery("select * from tk where c1 >= 2 for update").Check(testkit.Rows("2 2", "3 4", "4 4", "5 5"))
	tk.MustExec("commit")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update tk set c2 = c2 + 10 where c1 > 3")
	tk3.MustExec("begin pessimistic")
	_, err = tk3.Exec("select * from tk where c1 = 5 for update nowait")
	c.Check(err, NotNil)
	tk3.MustExec("select * from tk where c1 = 1 for update nowait")
	tk.MustExec("commit")
	tk3.MustQuery("select * from tk where c1 > 3 for update nowait").Check(testkit.Rows("4 14", "5 15"))
	tk3.MustExec("commit")

	//delete
	tk3.MustExec("begin pessimistic")
	tk3.MustExec("delete from tk where c1 <= 2")
	tk.MustExec("begin pessimistic")
	_, err = tk.Exec("select * from tk where c1 = 1 for update nowait")
	c.Check(err, NotNil)
	tk3.MustExec("commit")
	tk.MustQuery("select * from tk where c1 > 1 for update nowait").Check(testkit.Rows("3 4", "4 14", "5 15"))
	tk.MustExec("update tk set c2 = c2 + 1 where c1 = 5")
	tk2.MustExec("begin pessimistic")
	_, err = tk2.Exec("select * from tk where c1 = 5 for update nowait")
	c.Check(err, NotNil)
	tk.MustExec("commit")
	tk2.MustQuery("select * from tk where c1 = 5 for update nowait").Check(testkit.Rows("5 16"))
	tk2.MustExec("update tk set c2 = c2 + 1 where c1 = 5")
	tk2.MustQuery("select * from tk where c1 = 5 for update nowait").Check(testkit.Rows("5 17"))
	tk2.MustExec("commit")
}

func (s *testPessimisticSuite) TestAsyncRollBackNoWait(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,17)")

	tk.MustExec("set @@autocommit = 0")
	tk2.MustExec("set @@autocommit = 0")
	tk3.MustExec("set @@autocommit = 0")

	// test get ts failed for handlePessimisticLockError when using nowait
	// even though async rollback for pessimistic lock may rollback later locked key if get ts failed from pd
	// the txn correctness should be ensured
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/ExecStmtGetTsError", "return"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/AsyncRollBackSleep", "return"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/ExecStmtGetTsError"), IsNil)
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/AsyncRollBackSleep"), IsNil)
	}()
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from tk where c1 > 0 for update nowait")
	tk2.MustExec("begin pessimistic")
	// The lock rollback of this statement is delayed by failpoint AsyncRollBackSleep.
	_, err := tk2.Exec("select * from tk where c1 > 0 for update nowait")
	c.Check(err, NotNil)
	tk.MustExec("commit")
	// This statement success for now, but its lock will be rollbacked later by the
	// lingering rollback request, as forUpdateTS doesn't change.
	tk2.MustQuery("select * from tk where c1 > 0 for update nowait")
	tk2.MustQuery("select * from tk where c1 = 5 for update nowait").Check(testkit.Rows("5 17"))
	tk3.MustExec("begin pessimistic")

	c.Skip("tk3 is blocking because tk2 didn't rollback itself")
	// tk3 succ because tk2 rollback itself.
	tk3.MustExec("update tk set c2 = 1 where c1 = 5")
	// This will not take effect because the lock of tk2 was gone.
	tk2.MustExec("update tk set c2 = c2 + 100 where c1 > 0")
	_, err = tk2.Exec("commit")
	c.Check(err, NotNil) // txn abort because pessimistic lock not found
	tk3.MustExec("commit")
	tk3.MustExec("begin pessimistic")
	tk3.MustQuery("select * from tk where c1 = 5 for update nowait").Check(testkit.Rows("5 1"))
	tk3.MustQuery("select * from tk where c1 = 4 for update nowait").Check(testkit.Rows("4 4"))
	tk3.MustQuery("select * from tk where c1 = 3 for update nowait").Check(testkit.Rows("3 3"))
	tk3.MustExec("commit")
}

func (s *testPessimisticSuite) TestWaitLockKill(c *C) {
	// Test kill command works on waiting pessimistic lock.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists test_kill")
	tk.MustExec("create table test_kill (id int primary key, c int)")
	tk.MustExec("insert test_kill values (1, 1)")
	tk.MustExec("begin pessimistic")
	tk2.MustExec("set innodb_lock_wait_timeout = 50")
	tk2.MustExec("begin pessimistic")
	tk.MustQuery("select * from test_kill where id = 1 for update")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		sessVars := tk2.Se.GetSessionVars()
		succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
		c.Assert(succ, IsTrue)
		wg.Wait()
	}()
	_, err := tk2.Exec("update test_kill set c = c + 1 where id = 1")
	wg.Done()
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, tikv.ErrQueryInterrupted), IsTrue)
	tk.MustExec("rollback")
}

func (s *testPessimisticSuite) TestKillStopTTLManager(c *C) {
	// Test killing an idle pessimistic session stop its ttlManager.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists test_kill")
	tk.MustExec("create table test_kill (id int primary key, c int)")
	tk.MustExec("insert test_kill values (1, 1)")
	tk.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk.MustQuery("select * from test_kill where id = 1 for update")
	sessVars := tk.Se.GetSessionVars()
	succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
	c.Assert(succ, IsTrue)

	// This query should success rather than returning a ResolveLock error.
	tk2.MustExec("update test_kill set c = c + 1 where id = 1")
}

func (s *testPessimisticSuite) TestConcurrentInsert(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert tk values (1, 1)")
	tk.MustExec("create table tk1 (c1 int, c2 int)")

	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("begin pessimistic")
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	forUpdateTsA := tk1.Se.GetSessionVars().TxnCtx.GetForUpdateTS()
	tk1.MustQuery("select * from tk where c1 = 1 for update")
	forUpdateTsB := tk1.Se.GetSessionVars().TxnCtx.GetForUpdateTS()
	c.Assert(forUpdateTsA, Equals, forUpdateTsB)
	tk1.MustQuery("select * from tk where c1 > 0 for update")
	forUpdateTsC := tk1.Se.GetSessionVars().TxnCtx.GetForUpdateTS()
	c.Assert(forUpdateTsC, Greater, forUpdateTsB)

	tk2.MustExec("insert tk values (2, 2)")
	tk1.MustQuery("select * from tk for update").Check(testkit.Rows("1 1", "2 2"))
	tk2.MustExec("insert tk values (3, 3)")
	tk1.MustExec("update tk set c2 = c2 + 1")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(3))
	tk2.MustExec("insert tk values (4, 4)")
	tk1.MustExec("delete from tk")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(4))
	tk2.MustExec("insert tk values (5, 5)")
	tk1.MustExec("insert into tk1 select * from tk")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(1))
	tk2.MustExec("insert tk values (6, 6)")
	tk1.MustExec("replace into tk1 select * from tk")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(2))
	tk2.MustExec("insert tk values (7, 7)")
	// This test is used to test when the selectPlan is a PointGetPlan, and we didn't update its forUpdateTS.
	tk1.MustExec("insert into tk1 select * from tk where c1 = 7")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(1))
	tk1.MustExec("commit")
}

func (s *testPessimisticSuite) TestInnodbLockWaitTimeout(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	// tk set global
	tk.MustExec("set global innodb_lock_wait_timeout = 3")
	tk.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 50"))

	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk2.MustExec("set innodb_lock_wait_timeout = 2")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 2"))

	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk3.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk3.MustExec("set innodb_lock_wait_timeout = 1")
	tk3.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	tk2.MustExec("set @@autocommit = 0")
	tk3.MustExec("set @@autocommit = 0")

	tk4 := testkit.NewTestKitWithInit(c, s.store)
	tk4.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk4.MustExec("set @@autocommit = 0")

	// tk2 lock c1 = 1
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("select * from tk where c1 = 1 for update") // lock succ c1 = 1

	// Parallel the blocking tests to accelerate CI.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// tk3 try lock c1 = 1 timeout 1sec
		tk3.MustExec("begin pessimistic")
		_, err := tk3.Exec("select * from tk where c1 = 1 for update")
		c.Check(err.Error(), Equals, tikv.ErrLockWaitTimeout.Error())
		tk3.MustExec("commit")
	}()

	go func() {
		defer wg.Done()
		// tk5 try lock c1 = 1 timeout 2sec
		tk5 := testkit.NewTestKitWithInit(c, s.store)
		tk5.MustExec("set innodb_lock_wait_timeout = 2")
		tk5.MustExec("begin pessimistic")
		_, err := tk5.Exec("update tk set c2 = c2 - 1 where c1 = 1")
		c.Check(err.Error(), Equals, tikv.ErrLockWaitTimeout.Error())
		tk5.MustExec("rollback")
	}()

	// tk4 lock c1 = 2
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("update tk set c2 = c2 + 1 where c1 = 2") // lock succ c1 = 2 by update

	tk2.MustExec("set innodb_lock_wait_timeout = 1")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	start := time.Now()
	_, err := tk2.Exec("delete from tk where c1 = 2")
	c.Check(time.Since(start), GreaterEqual, 1000*time.Millisecond)
	c.Check(time.Since(start), Less, 3000*time.Millisecond) // unit test diff should not be too big
	c.Check(err.Error(), Equals, tikv.ErrLockWaitTimeout.Error())

	tk4.MustExec("commit")

	tk.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 50"))
	tk.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 3")) // tk4 update commit work, tk2 delete should be rollbacked

	// test stmtRollBack caused by timeout but not the whole transaction
	tk2.MustExec("update tk set c2 = c2 + 2 where c1 = 2")                    // tk2 lock succ c1 = 2 by update
	tk2.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 5")) // tk2 update c2 succ

	tk3.MustExec("begin pessimistic")
	tk3.MustExec("select * from tk where c1 = 3 for update") // tk3  lock c1 = 3 succ

	start = time.Now()
	_, err = tk2.Exec("delete from tk where c1 = 3") // tk2 tries to lock c1 = 3 fail, this delete should be rollback, but previous update should be keeped
	c.Check(time.Since(start), GreaterEqual, 1000*time.Millisecond)
	c.Check(time.Since(start), Less, 3000*time.Millisecond) // unit test diff should not be too big
	c.Check(err.Error(), Equals, tikv.ErrLockWaitTimeout.Error())

	tk2.MustExec("commit")
	tk3.MustExec("commit")

	tk.MustQuery(`select * from tk where c1 = 1`).Check(testkit.Rows("1 1"))
	tk.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 5")) // tk2 update succ
	tk.MustQuery(`select * from tk where c1 = 3`).Check(testkit.Rows("3 3")) // tk2 delete should fail
	tk.MustQuery(`select * from tk where c1 = 4`).Check(testkit.Rows("4 4"))
	tk.MustQuery(`select * from tk where c1 = 5`).Check(testkit.Rows("5 5"))

	// clean
	tk.MustExec("drop table if exists tk")
	tk4.MustExec("commit")

	wg.Wait()
}

func (s *testPessimisticSuite) TestPushConditionCheckForPessimisticTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (i int key)")
	tk.MustExec("insert into t values (1)")

	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk.MustExec("begin")
	tk1.MustExec("delete from t where i = 1")
	tk.MustExec("insert into t values (1) on duplicate key update i = values(i)")
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
}

func (s *testPessimisticSuite) TestInnodbLockWaitTimeoutWaitStart(c *C) {
	// prepare work
	tk := testkit.NewTestKitWithInit(c, s.store)
	defer tk.MustExec("drop table if exists tk")
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("set global innodb_lock_wait_timeout = 1")

	// raise pessimistic transaction in tk2 and trigger failpoint returning ErrWriteConflict
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	// tk3 gets the pessimistic lock
	tk3.MustExec("begin pessimistic")
	tk3.MustQuery("select * from tk where c1 = 1 for update")

	tk2.MustExec("begin pessimistic")
	done := make(chan error)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/PessimisticLockErrWriteConflict", "return"), IsNil)
	var duration time.Duration
	go func() {
		var err error
		start := time.Now()
		defer func() {
			duration = time.Since(start)
			done <- err
		}()
		_, err = tk2.Exec("select * from tk where c1 = 1 for update")
	}()
	time.Sleep(time.Millisecond * 100)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/PessimisticLockErrWriteConflict"), IsNil)
	waitErr := <-done
	c.Assert(waitErr, NotNil)
	c.Check(waitErr.Error(), Equals, tikv.ErrLockWaitTimeout.Error())
	c.Check(duration, GreaterEqual, 1000*time.Millisecond)
	c.Check(duration, LessEqual, 3000*time.Millisecond)
	tk2.MustExec("rollback")
	tk3.MustExec("commit")
}

func (s *testPessimisticSuite) TestBatchPointGetWriteConflict(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (i int primary key, c int);")
	tk.MustExec("insert t values (1, 1), (2, 2), (3, 3)")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select * from t where i = 1").Check(testkit.Rows("1 1"))
	tk.MustExec("update t set c = c + 1")
	tk1.MustQuery("select * from t where i in (1, 3) for update").Check(testkit.Rows("1 2", "3 4"))
	tk1.MustExec("commit")
}

func (s *testPessimisticSuite) TestPessimisticReadCommitted(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("use test")

	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set tidb_txn_mode = 'pessimistic'")

	// test SI
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int key);")
	tk.MustExec("insert into t values (1);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustExec("update t set i = -i;")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tk1.MustExec("update t set i = -i;")
		wg.Done()
	}()
	tk.MustExec("commit;")
	wg.Wait()

	tk1.MustExec("commit;")

	// test RC
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk1.MustExec("set tx_isolation = 'READ-COMMITTED'")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int key);")
	tk.MustExec("insert into t values (1);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustExec("update t set i = -i;")

	wg.Add(1)
	go func() {
		tk1.MustExec("update t set i = -i;")
		wg.Done()
	}()
	tk.MustExec("commit;")
	wg.Wait()

	tk1.MustExec("commit;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int key, j int unique key, k int, l int, m int, key (k));")
	tk.MustExec("insert into t values (1, 1, 1, 1, 1);")

	// Set it back to RR to test set transaction statement.
	tk.MustExec("set tx_isolation = 'REPEATABLE-READ'")
	tk1.MustExec("set tx_isolation = 'REPEATABLE-READ'")

	// test one shot and some reads.
	tk.MustExec("set transaction isolation level read committed")
	tk.MustExec("begin;")

	// test table reader
	tk.MustQuery("select l from t where l = 1").Check(testkit.Rows("1"))
	tk1.MustExec("update t set l = l + 1 where l = 1;")
	tk.MustQuery("select l from t where l = 2").Check(testkit.Rows("2"))
	tk1.MustExec("update t set l = l + 1 where l = 2;")
	tk.MustQuery("select l from t where l = 3").Check(testkit.Rows("3"))

	// test index reader
	tk.MustQuery("select k from t where k = 1").Check(testkit.Rows("1"))
	tk1.MustExec("update t set k = k + 1 where k = 1;")
	tk.MustQuery("select k from t where k = 2").Check(testkit.Rows("2"))
	tk1.MustExec("update t set k = k + 1 where k = 2;")
	tk.MustQuery("select k from t where k = 3").Check(testkit.Rows("3"))

	// test double read
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("1"))
	tk1.MustExec("update t set m = m + 1 where k = 3;")
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("2"))
	tk1.MustExec("update t set m = m + 1 where k = 3;")
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("3"))

	// test point get plan
	tk.MustQuery("select m from t where i = 1").Check(testkit.Rows("3"))
	tk1.MustExec("update t set m = m + 1 where i = 1;")
	tk.MustQuery("select m from t where i = 1").Check(testkit.Rows("4"))
	tk1.MustExec("update t set m = m + 1 where j = 1;")
	tk.MustQuery("select m from t where j = 1").Check(testkit.Rows("5"))

	// test batch point get plan
	tk1.MustExec("insert into t values (2, 2, 2, 2, 2);")
	tk.MustQuery("select m from t where i in (1, 2)").Check(testkit.Rows("5", "2"))
	tk1.MustExec("update t set m = m + 1 where i in (1, 2);")
	tk.MustQuery("select m from t where i in (1, 2)").Check(testkit.Rows("6", "3"))
	tk1.MustExec("update t set m = m + 1 where j in (1, 2);")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))

	tk.MustExec("commit;")

	// test for NewTxn()
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("begin optimistic;")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))
	tk.MustExec("begin pessimistic;")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))
	tk.MustExec("commit;")
}

func (s *testPessimisticSuite) TestPessimisticLockNonExistsKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (k int primary key, c int)")
	tk.MustExec("insert t values (1, 1), (3, 3), (5, 5)")

	// verify that select with project and filter on a non exists key still locks the key.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert t values (8, 8)") // Make the transaction dirty.
	tk.MustQuery("select c + 1 from t where k = 2 and c = 2 for update").Check(testkit.Rows())
	explainStr := tk.MustQuery("explain select c + 1 from t where k = 2 and c = 2 for update").Rows()[0][0].(string)
	c.Assert(strings.Contains(explainStr, "UnionScan"), IsFalse)
	tk.MustQuery("select * from t where k in (4, 5, 7) for update").Check(testkit.Rows("5 5"))

	tk1.MustExec("begin pessimistic")
	err := tk1.ExecToErr("select * from t where k = 2 for update nowait")
	c.Check(tikv.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	err = tk1.ExecToErr("select * from t where k = 4 for update nowait")
	c.Check(tikv.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	err = tk1.ExecToErr("select * from t where k = 7 for update nowait")
	c.Check(tikv.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	tk.MustExec("rollback")
	tk1.MustExec("rollback")

	// verify update and delete non exists keys still locks the key.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert t values (8, 8)") // Make the transaction dirty.
	tk.MustExec("update t set c = c + 1 where k in (2, 3, 4) and c > 0")
	tk.MustExec("delete from t where k in (5, 6, 7) and c > 0")

	tk1.MustExec("begin pessimistic")
	err = tk1.ExecToErr("select * from t where k = 2 for update nowait")
	c.Check(tikv.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	err = tk1.ExecToErr("select * from t where k = 6 for update nowait")
	c.Check(tikv.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	tk.MustExec("rollback")
	tk1.MustExec("rollback")
}

func (s *testPessimisticSuite) TestPessimisticCommitReadLock(c *C) {
	// set lock ttl to 3s, tk1 lock wait timeout is 2s
	atomic.StoreUint64(&tikv.ManagedLockTTL, 3000)
	defer atomic.StoreUint64(&tikv.ManagedLockTTL, 300)
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("use test")

	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set innodb_lock_wait_timeout = 2")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int key primary key, j int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))

	// tk lock one row
	tk.MustExec("begin;")
	tk.MustQuery("select * from t for update").Check(testkit.Rows("1 2"))
	tk1.MustExec("begin;")
	done := make(chan error)
	go func() {
		var err error
		defer func() {
			done <- err
		}()
		// let txn not found could be checked by lock wait timeout utility
		err = failpoint.Enable("github.com/pingcap/tidb/store/tikv/txnNotFoundRetTTL", "return")
		if err != nil {
			return
		}
		_, err = tk1.Exec("update t set j = j + 1 where i = 1")
		if err != nil {
			return
		}
		err = failpoint.Disable("github.com/pingcap/tidb/store/tikv/txnNotFoundRetTTL")
		if err != nil {
			return
		}
		_, err = tk1.Exec("commit")
	}()
	// let the lock be hold for a while
	time.Sleep(time.Millisecond * 50)
	tk.MustExec("commit")
	waitErr := <-done
	c.Assert(waitErr, IsNil)
}

func (s *testPessimisticSuite) TestPessimisticLockReadValue(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int, j int, k int, unique key uk(j));")
	tk.MustExec("insert into t values (1, 1, 1);")

	// tk1 will left op_lock record
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("use test")
	tk1.MustExec("begin optimistic")
	tk1.MustQuery("select * from t where j = 1 for update").Check(testkit.Rows("1 1 1"))
	tk1.MustQuery("select * from t where j = 1 for update").Check(testkit.Rows("1 1 1"))
	tk1.MustExec("commit")

	// tk2 pessimistic lock read value
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustExec("begin pessimistic")
	tk2.MustQuery("select * from t where j = 1 for update").Check(testkit.Rows("1 1 1"))
	tk2.MustQuery("select * from t where j = 1 for update").Check(testkit.Rows("1 1 1"))
	tk2.MustExec("commit")
}

func (s *testPessimisticSuite) TestRCWaitTSOTwice(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (i int key)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk.MustExec("set tx_isolation = 'read-committed'")
	tk.MustExec("set autocommit = 0")
	tk.MustQuery("select * from t where i = 1").Check(testkit.Rows("1"))
	tk.MustExec("rollback")
}

func (s *testPessimisticSuite) TestNonAutoCommitWithPessimisticMode(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int)")
	tk.MustExec("insert into t1 values (1, 1)")
	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk.MustExec("set autocommit = 0")
	tk.MustQuery("select * from t1 where c2 = 1 for update").Check(testkit.Rows("1 1"))
	tk2.MustExec("insert into t1 values(2, 1)")
	tk.MustQuery("select * from t1 where c2 = 1 for update").Check(testkit.Rows("1 1", "2 1"))
	tk.MustExec("commit")
	tk2.MustExec("insert into t1 values(3, 1)")
	tk.MustExec("set tx_isolation = 'read-committed'")
	tk.MustQuery("select * from t1 where c2 = 1 for update").Check(testkit.Rows("1 1", "2 1", "3 1"))
	tk2.MustExec("insert into t1 values(4, 1)")
	tk.MustQuery("select * from t1 where c2 = 1 for update").Check(testkit.Rows("1 1", "2 1", "3 1", "4 1"))
	tk.MustExec("commit")
}

func (s *testPessimisticSuite) TestBatchPointGetLockIndex(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustExec("use test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (5, 5, 5)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	tk.MustExec("begin pessimistic")
	// the handle does not exist and the index key should be locked as point get executor did
	tk.MustQuery("select * from t1 where c2 in (2, 3) for update").Check(testkit.Rows())
	tk2.MustExec("set innodb_lock_wait_timeout = 1")
	tk2.MustExec("begin pessimistic")
	err := tk2.ExecToErr("insert into t1 values(2, 2, 2)")
	c.Assert(tikv.ErrLockWaitTimeout.Equal(err), IsTrue)
	err = tk2.ExecToErr("select * from t1 where c2 = 3 for update nowait")
	c.Assert(tikv.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
}

func (s *testPessimisticSuite) TestLockGotKeysInRC(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk2.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (5, 5, 5)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in (2, 3) for update").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c2 in (2, 3) for update").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c1 = 2 for update").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c2 = 2 for update").Check(testkit.Rows())
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("insert into t1 values(2, 2, 2)")
	tk2.MustExec("select * from t1 where c1 = 3 for update nowait")
	tk2.MustExec("select * from t1 where c2 = 3 for update nowait")
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
}

func (s *testPessimisticSuite) TestBatchPointGetAlreadyLocked(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int, c3 int, primary key(c1, c2))")
	tk.MustExec("insert t values (1, 1, 1), (2, 2, 2)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t where c1 > 1 for update").Check(testkit.Rows("2 2 2"))
	tk.MustQuery("select * from t where (c1, c2) in ((2,2)) for update").Check(testkit.Rows("2 2 2"))
	tk.MustExec("commit")
}

func (s *testPessimisticSuite) TestRollbackWakeupBlockedTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustExec("use test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (5, 5, 5)")
	tk.MustExec("insert into t1 values (10, 10, 10)")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/txnExpireRetTTL", "return"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/getTxnStatusDelay", "return"), IsNil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("set innodb_lock_wait_timeout = 1")
	tk2.MustExec("begin pessimistic")
	tk.MustExec("update t1 set c3 = c3 + 1")
	errCh := make(chan error)
	go func() {
		var err error
		defer func() {
			errCh <- err
		}()
		_, err = tk2.Exec("update t1 set c3 = 100 where c1 = 1")
		if err != nil {
			return
		}
	}()
	time.Sleep(time.Millisecond * 30)
	tk.MustExec("rollback")
	err := <-errCh
	c.Assert(err, IsNil)
	tk2.MustExec("rollback")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/txnExpireRetTTL"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/getTxnStatusDelay"), IsNil)
}

func (s *testPessimisticSuite) TestRCSubQuery(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table `t` ( `c1` int(11) not null, `c2` int(11) default null, primary key (`c1`) )")
	tk.MustExec("insert into t values(1, 3)")
	tk.MustExec("create table `t1` ( `c1` int(11) not null, `c2` int(11) default null, primary key (`c1`) )")
	tk.MustExec("insert into t1 values(1, 3)")
	tk.MustExec("set transaction isolation level read committed")
	tk.MustExec("begin pessimistic")

	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustExec("update t1 set c2 = c2 + 1")

	tk.MustQuery("select * from t1 where c1 = (select 1) and 1=1;").Check(testkit.Rows("1 4"))
	tk.MustQuery("select * from t1 where c1 = (select c1 from t where c1 = 1) and 1=1;").Check(testkit.Rows("1 4"))
	tk.MustExec("rollback")
}

func (s *testPessimisticSuite) TestGenerateColPointGet(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_row_format_version = %d", variable.DefTiDBRowFormatV2))
	}()
	tests2 := []int{variable.DefTiDBRowFormatV1, variable.DefTiDBRowFormatV2}
	for _, rowFormat := range tests2 {
		tk.MustExec(fmt.Sprintf("set global tidb_row_format_version = %d", rowFormat))
		tk.MustExec("drop table if exists tu")
		tk.MustExec("CREATE TABLE `tu`(`x` int, `y` int, `z` int GENERATED ALWAYS AS (x + y) VIRTUAL, PRIMARY KEY (`x`), UNIQUE KEY `idz` (`z`))")
		tk.MustExec("insert into tu(x, y) values(1, 2);")

		// test point get lock
		tk.MustExec("begin pessimistic")
		tk.MustQuery("select * from tu where z = 3 for update").Check(testkit.Rows("1 2 3"))
		tk2 := testkit.NewTestKitWithInit(c, s.store)
		tk2.MustExec("begin pessimistic")
		err := tk2.ExecToErr("select * from tu where z = 3 for update nowait")
		c.Assert(terror.ErrorEqual(err, tikv.ErrLockAcquireFailAndNoWaitSet), IsTrue)
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into tu(x, y) values(2, 2);")
		err = tk2.ExecToErr("select * from tu where z = 4 for update nowait")
		c.Assert(terror.ErrorEqual(err, tikv.ErrLockAcquireFailAndNoWaitSet), IsTrue)

		// test batch point get lock
		tk.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		tk.MustQuery("select * from tu where z in (1, 3, 5) for update").Check(testkit.Rows("1 2 3"))
		tk2.MustExec("begin pessimistic")
		err = tk2.ExecToErr("select x from tu where z in (3, 7, 9) for update nowait")
		c.Assert(terror.ErrorEqual(err, tikv.ErrLockAcquireFailAndNoWaitSet), IsTrue)
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into tu(x, y) values(5, 6);")
		err = tk2.ExecToErr("select * from tu where z = 11 for update nowait")
		c.Assert(terror.ErrorEqual(err, tikv.ErrLockAcquireFailAndNoWaitSet), IsTrue)

		tk.MustExec("commit")
		tk2.MustExec("commit")
	}
}

func (s *testPessimisticSuite) TestTxnWithExpiredPessimisticLocks(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (5, 5, 5)")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in(1, 5) for update").Check(testkit.Rows("1 1 1", "5 5 5"))
	atomic.StoreUint32(&tk.Se.GetSessionVars().TxnCtx.LockExpire, 1)
	err := tk.ExecToErr("select * from t1 where c1 in(1, 5)")
	c.Assert(terror.ErrorEqual(err, tikv.ErrLockExpire), IsTrue)
	tk.MustExec("commit")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in(1, 5) for update").Check(testkit.Rows("1 1 1", "5 5 5"))
	atomic.StoreUint32(&tk.Se.GetSessionVars().TxnCtx.LockExpire, 1)
	err = tk.ExecToErr("update t1 set c2 = c2 + 1")
	c.Assert(terror.ErrorEqual(err, tikv.ErrLockExpire), IsTrue)
	atomic.StoreUint32(&tk.Se.GetSessionVars().TxnCtx.LockExpire, 0)
	tk.MustExec("update t1 set c2 = c2 + 1")
	tk.MustExec("rollback")
}

func (s *testPessimisticSuite) TestKillWaitLockTxn(c *C) {
	// Test kill command works on waiting pessimistic lock.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists test_kill")
	tk.MustExec("create table test_kill (id int primary key, c int)")
	tk.MustExec("insert test_kill values (1, 1)")

	tk.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")

	tk.MustQuery("select * from test_kill where id = 1 for update")
	errCh := make(chan error)
	go func() {
		var err error
		defer func() {
			errCh <- err
		}()
		time.Sleep(20 * time.Millisecond)
		_, err = tk2.Exec("update test_kill set c = c + 1 where id = 1")
		if err != nil {
			return
		}
	}()
	time.Sleep(100 * time.Millisecond)
	sessVars := tk.Se.GetSessionVars()
	// lock query in tk is killed, the ttl manager will stop
	succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
	c.Assert(succ, IsTrue)
	err := <-errCh
	c.Assert(err, IsNil)
	tk.Exec("rollback")
	// reset kill
	atomic.CompareAndSwapUint32(&sessVars.Killed, 1, 0)
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
}

func (s *testPessimisticSuite) TestDupLockInconsistency(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index b (b))")
	tk.MustExec("insert t (a) values (1), (1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t, (select a from t) s set t.b = s.a")
	tk.MustExec("commit")
	tk.MustExec("admin check table t")
}

func (s *testPessimisticSuite) TestUseLockCacheInRCMode(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists test_kill")
	tk.MustExec("CREATE TABLE SEQUENCE_VALUE_ITEM(SEQ_NAME varchar(60) NOT NULL, SEQ_ID decimal(18,0) DEFAULT NULL, " +
		"PRIMARY KEY (SEQ_NAME))")
	tk.MustExec("create table t1(c1 int, c2 int, unique key(c1))")
	tk.MustExec(`insert into sequence_value_item values("OSCurrentStep", 0)`)
	tk.MustExec("insert into t1 values(1, 1)")
	tk.MustExec("insert into t1 values(2, 2)")

	// tk2 uses RC isolation level
	tk2.MustExec("set @@tx_isolation='READ-COMMITTED'")
	tk2.MustExec("set autocommit = 0")

	// test point get
	tk2.MustExec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME='OSCurrentStep' FOR UPDATE")
	tk2.MustExec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME='OSCurrentStep'")
	tk2.MustExec("rollback")
	tk2.MustQuery("select * from t1 where c1 = 1 for update").Check(testkit.Rows("1 1"))
	tk2.MustExec("update t1 set c2 = c2 + 10 where c1 = 1")
	tk2.MustQuery("select * from t1 where c1 in (1, 2) for update").Check(testkit.Rows("1 11", "2 2"))
	tk2.MustExec("update t1 set c2 = c2 + 10 where c1 in (2)")
	tk2.MustQuery("select * from t1 where c1 in (1, 2) for update").Check(testkit.Rows("1 11", "2 12"))
	tk2.MustExec("commit")

	// tk3 uses begin with RC isolation level
	tk3.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 0"))
	tk3.MustExec("set @@tx_isolation='READ-COMMITTED'")
	tk3.MustExec("begin")
	tk3.MustExec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME='OSCurrentStep' FOR UPDATE")
	tk3.MustExec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME='OSCurrentStep'")
	tk3.MustQuery("select * from t1 where c1 = 1 for update").Check(testkit.Rows("1 11"))
	tk3.MustExec("update t1 set c2 = c2 + 10 where c1 = 1")
	tk3.MustQuery("select * from t1 where c1 in (1, 2) for update").Check(testkit.Rows("1 21", "2 12"))
	tk3.MustExec("update t1 set c2 = c2 + 10 where c1 in (2)")
	tk3.MustQuery("select * from t1 where c1 in (1, 2) for update").Check(testkit.Rows("1 21", "2 22"))
	tk3.MustExec("commit")

	// verify
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 100"))
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM where SEQ_ID = 100").Check(testkit.Rows("OSCurrentStep 100"))
	tk.MustQuery("select * from t1 where c1 = 2").Check(testkit.Rows("2 22"))
	tk.MustQuery("select * from t1 where c1 in (1, 2, 3)").Check(testkit.Rows("1 21", "2 22"))

	// test batch point get
	tk2.MustExec("set autocommit = 1")
	tk2.MustExec("set autocommit = 0")
	tk2.MustExec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME in ('OSCurrentStep') FOR UPDATE")
	tk2.MustExec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME in ('OSCurrentStep')")
	tk2.MustQuery("select * from t1 where c1 in (1, 2, 3, 4, 5) for update").Check(testkit.Rows("1 21", "2 22"))
	tk2.MustExec("update t1 set c2 = c2 + 10 where c1 in (1, 2, 3, 4, 5)")
	tk2.MustQuery("select * from t1 where c1 in (1, 2, 3, 4, 5) for update").Check(testkit.Rows("1 31", "2 32"))
	tk2.MustExec("commit")
	tk2.MustExec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME in ('OSCurrentStep') FOR UPDATE")
	tk2.MustExec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME in ('OSCurrentStep')")
	tk2.MustExec("rollback")

	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 200"))
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM where SEQ_NAME in ('OSCurrentStep')").Check(testkit.Rows("OSCurrentStep 200"))
	tk.MustQuery("select * from t1 where c1 in (1, 2, 3)").Check(testkit.Rows("1 31", "2 32"))
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
	tk3.MustExec("rollback")
}

func (s *testPessimisticSuite) TestPointGetWithDeleteInMem(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists uk")
	tk.MustExec("create table uk (c1 int primary key, c2 int, unique key uk(c2))")
	tk.MustExec("insert uk values (1, 77), (2, 88), (3, 99)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("delete from uk where c1 = 1")
	tk.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows())
	tk.MustQuery("select * from uk where c2 in(77, 88, 99)").Check(testkit.Rows("2 88", "3 99"))
	tk.MustQuery("select * from uk").Check(testkit.Rows("2 88", "3 99"))
	tk.MustQuery("select * from uk where c2 = 77 for update").Check(testkit.Rows())
	tk.MustQuery("select * from uk where c2 in(77, 88, 99) for update").Check(testkit.Rows("2 88", "3 99"))
	tk.MustExec("rollback")
	tk2.MustQuery("select * from uk where c1 = 1").Check(testkit.Rows("1 77"))
	tk.MustExec("begin")
	tk.MustExec("update uk set c1 = 10 where c1 = 1")
	tk.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows("10 77"))
	tk.MustQuery("select * from uk where c2 in(77, 88, 99)").Check(testkit.Rows("10 77", "2 88", "3 99"))
	tk.MustExec("commit")
	tk2.MustQuery("select * from uk where c1 = 1").Check(testkit.Rows())
	tk2.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows("10 77"))
	tk2.MustQuery("select * from uk where c1 = 10").Check(testkit.Rows("10 77"))
	tk.MustExec("drop table if exists uk")
}

func (s *testPessimisticSuite) TestPessimisticTxnWithDDLAddDropColumn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int)")
	tk.MustExec("insert t1 values (1, 77), (2, 88)")
	tk.MustExec("alter table t1 add index k2(c2)")
	tk.MustExec("alter table t1 drop index k2")

	// tk2 starts a pessimistic transaction and make some changes on table t1.
	// tk executes some ddl statements add/drop column on table t1.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t1 set c2 = c1 * 10")
	tk2.MustExec("alter table t1 add column c3 int after c1")
	tk.MustExec("commit")
	tk.MustExec("admin check table t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 <nil> 10", "2 <nil> 20"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(5, 5, 5)")
	tk2.MustExec("alter table t1 drop column c3")
	tk2.MustExec("alter table t1 drop column c2")
	tk.MustExec("commit")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "2", "5"))
}
func (s *testPessimisticSuite) TestPessimisticTxnWithDDLChangeColumn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 varchar(10))")
	tk.MustExec("insert t1 values (1, 77, 'a'), (2, 88, 'b')")

	// Extend column field length is acceptable.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t1 set c2 = c1 * 10")
	tk2.MustExec("alter table t1 modify column c2 bigint")
	tk.MustExec("commit")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t1 set c3 = 'aba'")
	tk2.MustExec("alter table t1 modify column c3 varchar(30)")
	tk.MustExec("commit")
	tk2.MustExec("admin check table t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 10 aba", "2 20 aba"))

	// Change column from nullable to not null is not allowed by now.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1(c1) values(100)")
	tk2.MustExec("alter table t1 change column c2 cc2 bigint not null")
	err := tk.ExecToErr("commit")
	c.Assert(err, NotNil)

	// Change default value is rejected.
	tk2.MustExec("create table ta(a bigint primary key auto_random(3), b varchar(255) default 'old');")
	tk2.MustExec("insert into ta(b) values('a')")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into ta values()")
	tk2.MustExec("alter table ta modify column b varchar(300) default 'new';")
	err = tk.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk2.MustQuery("select b from ta").Check(testkit.Rows("a"))

	// Change default value with add index. There is a new MultipleKeyFlag flag on the index key, and the column is changed,
	// the flag check will fail.
	tk2.MustExec("insert into ta values()")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into ta(b) values('inserted_value')")
	tk.MustExec("insert into ta values()")
	tk.MustExec("insert into ta values()")
	tk2.MustExec("alter table ta add index i1(b)")
	tk2.MustExec("alter table ta change column b b varchar(301) default 'newest'")
	tk2.MustExec("alter table ta modify column b varchar(301) default 'new'")
	c.Assert(tk.ExecToErr("commit"), NotNil)
	tk2.MustExec("admin check table ta")
	tk2.MustQuery("select count(b) from ta use index(i1) where b = 'new'").Check(testkit.Rows("1"))

	// Change default value to now().
	tk2.MustExec("create table tbl_time(c1 int, c_time timestamp)")
	tk2.MustExec("insert into tbl_time(c1) values(1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into tbl_time(c1) values(2)")
	tk2.MustExec("alter table tbl_time modify column c_time timestamp default now()")
	tk2.MustExec("insert into tbl_time(c1) values(3)")
	tk2.MustExec("insert into tbl_time(c1) values(4)")
	c.Assert(tk.ExecToErr("commit"), NotNil)
	tk2.MustQuery("select count(1) from tbl_time where c_time is not null").Check(testkit.Rows("2"))

	tk2.MustExec("drop database if exists test_db")
}

func (s *testPessimisticSuite) TestPessimisticUnionForUpdate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, v int, k int, primary key (id), key kk(k))")
	tk.MustExec("insert into t select 1, 1, 1")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("(select * from t where id between 0 and 1 for update) union all (select * from t where id between 0 and 1 for update)")
	tk.MustExec("update t set k = 2 where k = 1")
	tk.MustExec("commit")
	tk.MustExec("admin check table t")
}

func (s *testPessimisticSuite) TestInsertDupKeyAfterLock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 2, 3);")
	tk2.MustExec("insert into t1 values(10, 20, 30);")

	// Test insert after lock.
	tk.MustExec("begin pessimistic")
	err := tk.ExecToErr("update t1 set c2 = 20 where c1 = 1;")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 for update")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 where c2 = 2 for update")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	// Test insert after insert.
	tk.MustExec("begin pessimistic")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("insert into t1 values(5, 6, 7)")
	err = tk.ExecToErr("insert into t1 values(6, 6, 7);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 30"))

	// Test insert after delete.
	tk.MustExec("begin pessimistic")
	tk.MustExec("delete from t1 where c2 > 2")
	tk.MustExec("insert into t1 values(10, 20, 500);")
	err = tk.ExecToErr("insert into t1 values(20, 20, 30);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("insert into t1 values(1, 20, 30);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test range.
	tk.MustExec("begin pessimistic")
	err = tk.ExecToErr("update t1 set c2 = 20 where c1 >= 1 and c1 < 5;")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("update t1 set c2 = 20 where c1 >= 1 and c1 < 50;")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test select for update after dml.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(5, 6, 7)")
	tk.MustExec("select * from t1 where c1 = 5 for update")
	tk.MustExec("select * from t1 where c1 = 6 for update")
	tk.MustExec("select * from t1 for update")
	err = tk.ExecToErr("insert into t1 values(7, 6, 7)")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("insert into t1 values(5, 8, 6)")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("select * from t1 where c1 = 5 for update")
	tk.MustExec("select * from t1 where c2 = 8 for update")
	tk.MustExec("select * from t1 for update")
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 500"))

	// Test optimistic for update.
	tk.MustExec("begin optimistic")
	tk.MustQuery("select * from t1 where c1 = 1 for update").Check(testkit.Rows("1 2 3"))
	tk.MustExec("insert into t1 values(10, 10, 10)")
	err = tk.ExecToErr("commit")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
}

func (s *testPessimisticSuite) TestInsertDupKeyAfterLockBatchPointGet(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 2, 3);")
	tk2.MustExec("insert into t1 values(10, 20, 30);")

	// Test insert after lock.
	tk.MustExec("begin pessimistic")
	err := tk.ExecToErr("update t1 set c2 = 20 where c1 in (1);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 for update")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 where c2 in (2) for update")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	// Test insert after insert.
	tk.MustExec("begin pessimistic")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("insert into t1 values(5, 6, 7)")
	err = tk.ExecToErr("insert into t1 values(6, 6, 7);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 30"))

	// Test insert after delete.
	tk.MustExec("begin pessimistic")
	tk.MustExec("delete from t1 where c2 > 2")
	tk.MustExec("insert into t1 values(10, 20, 500);")
	err = tk.ExecToErr("insert into t1 values(20, 20, 30);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("insert into t1 values(1, 20, 30);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test range.
	tk.MustExec("begin pessimistic")
	err = tk.ExecToErr("update t1 set c2 = 20 where c1 >= 1 and c1 < 5;")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("update t1 set c2 = 20 where c1 >= 1 and c1 < 50;")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test select for update after dml.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(5, 6, 7)")
	tk.MustExec("select * from t1 where c1 in (5, 6) for update")
	tk.MustExec("select * from t1 where c1 = 6 for update")
	tk.MustExec("select * from t1 for update")
	err = tk.ExecToErr("insert into t1 values(7, 6, 7)")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	err = tk.ExecToErr("insert into t1 values(5, 8, 6)")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustExec("select * from t1 where c2 = 8 for update")
	tk.MustExec("select * from t1 where c1 in (5, 8) for update")
	tk.MustExec("select * from t1 for update")
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 500"))

	// Test optimistic for update.
	tk.MustExec("begin optimistic")
	tk.MustQuery("select * from t1 where c1 in (1) for update").Check(testkit.Rows("1 2 3"))
	tk.MustExec("insert into t1 values(10, 10, 10)")
	err = tk.ExecToErr("commit")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
}

func (s *testPessimisticSuite) TestAmendTxnVariable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 1, 1);")
	tk2.MustExec("insert into t1 values(2, 2, 2);")
	tk3.MustExec("use test_db")

	// Set off the session variable.
	tk3.MustExec("set tidb_enable_amend_pessimistic_txn = 0;")
	tk3.MustExec("begin pessimistic")
	tk3.MustExec("insert into t1 values(3, 3, 3)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(4, 4, 4)")
	tk2.MustExec("alter table t1 add column new_col int")
	err := tk3.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 1 1 <nil>", "2 2 2 <nil>", "4 4 4 <nil>"))

	// Set off the global variable.
	tk2.MustExec("set global tidb_enable_amend_pessimistic_txn = 0;")
	tk4 := testkit.NewTestKitWithInit(c, s.store)
	tk4.MustQuery(`show variables like "tidb_enable_amend_pessimistic_txn"`).Check(testkit.Rows("tidb_enable_amend_pessimistic_txn 0"))
	tk4.MustExec("use test_db")
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("insert into t1 values(5, 5, 5, 5)")
	tk2.MustExec("alter table t1 drop column new_col")
	err = tk4.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk4.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("insert into t1 values(5, 5, 5)")
	tk2.MustExec("alter table t1 add column new_col2 int")
	tk4.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 1 1 <nil>", "2 2 2 <nil>", "4 4 4 <nil>", "5 5 5 <nil>"))

	// Restore.
	tk2.MustExec("set global tidb_enable_amend_pessimistic_txn = 1;")
}

func (s *testPessimisticSuite) Test19336(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`create table t (a int)`)
	tk.MustQuery(`select * from t lock in share mode`).Check(testkit.Rows())
}
