// Copyright 2021 PingCAP, Inc.
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
package telemetry_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testFeatureInfoSuite{})

type testFeatureInfoSuite struct {
	cluster cluster.Cluster
	store   kv.Storage
	dom     *domain.Domain
	se      session.Session
}

func (s *testFeatureInfoSuite) SetUpTest(c *C) {
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)
	c.Assert(err, IsNil)
	s.store = store
	dom, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom = dom
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.se = se
}

func (s *testFeatureInfoSuite) TearDownSuite(c *C) {
	s.se.Close()
	s.dom.Close()
	s.store.Close()
}

func (s *testFeatureInfoSuite) TestTxnUsageInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(fmt.Sprintf("set global %s = 0", variable.TiDBEnableAsyncCommit))
	tk.MustExec(fmt.Sprintf("set global %s = 0", variable.TiDBEnable1PC))
	txnUsage := telemetry.GetTxnUsageInfo(s.se)
	c.Assert(txnUsage.AsyncCommitUsed, IsFalse)
	c.Assert(txnUsage.OnePCUsed, IsFalse)
	tk.MustExec(fmt.Sprintf("set global %s = 1", variable.TiDBEnableAsyncCommit))
	tk.MustExec(fmt.Sprintf("set global %s = 1", variable.TiDBEnable1PC))
	txnUsage = telemetry.GetTxnUsageInfo(s.se)
	c.Assert(txnUsage.AsyncCommitUsed, IsTrue)
	c.Assert(txnUsage.OnePCUsed, IsTrue)

	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists txn_usage_info")
	tk1.MustExec("create table txn_usage_info (a int)")
	tk1.MustExec(fmt.Sprintf("set %s = 1", variable.TiDBEnableAsyncCommit))
	tk1.MustExec(fmt.Sprintf("set %s = 1", variable.TiDBEnable1PC))
	tk1.MustExec("insert into txn_usage_info values (1)")
	tk1.MustExec(fmt.Sprintf("set %s = 0", variable.TiDBEnable1PC))
	tk1.MustExec("insert into txn_usage_info values (2)")
	tk1.MustExec(fmt.Sprintf("set %s = 0", variable.TiDBEnableAsyncCommit))
	tk1.MustExec("insert into txn_usage_info values (3)")
	txnUsage = telemetry.GetTxnUsageInfo(tk1.Se)
	c.Assert(txnUsage.AsyncCommitUsed, IsTrue)
	c.Assert(txnUsage.OnePCUsed, IsTrue)
	c.Assert(txnUsage.TxnCommitCounter.AsyncCommit, Greater, int64(0))
	c.Assert(txnUsage.TxnCommitCounter.OnePC, Greater, int64(0))
	c.Assert(txnUsage.TxnCommitCounter.TwoPC, Greater, int64(0))
}
