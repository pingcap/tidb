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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPessimisticSuite{})

type testPessimisticSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	dom       *domain.Domain
}

func (s *testPessimisticSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	config.GetGlobalConfig().PessimisticTxn.Enable = true
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	s.store = store
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testPessimisticSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	config.GetGlobalConfig().PessimisticTxn.Enable = false
	testleak.AfterTest(c)()
}

func (s *testPessimisticSuite) TestPessimisticTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	// Make the name has different indent for easier read.
	tk1 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists pessimistic")
	tk.MustExec("create table pessimistic (k int, v int)")
	tk.MustExec("insert into pessimistic values (1, 1)")

	// t1 lock, t2 update, t1 update and retry statement.
	tk1.MustExec("begin lock")

	tk.MustExec("update pessimistic set v = 2 where v = 1")

	// Update can see the change, so this statement affects 0 roews.
	tk1.MustExec("update pessimistic set v = 3 where v = 1")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(0))
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
	tk1.MustExec("begin lock")
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
