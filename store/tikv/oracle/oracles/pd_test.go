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

package oracles

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/testutil"
	"go.uber.org/goleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&clientTestSuite{})

type clientTestSuite struct {
	pdClient pd.Client
	pd       *pdOracle
}

var oracleUpdateInterval time.Duration = 2000

func (s *clientTestSuite) SetUpSuite(c *C) {
	mvccStore, err := mocktikv.NewMVCCLevelDB("")
	c.Assert(err, IsNil)
	cluster := mocktikv.NewCluster(mvccStore)
	pdCli := mocktikv.NewPDClient(cluster)
	o, err := NewPdOracle(pdCli, oracleUpdateInterval*time.Millisecond)
	s.pd = o.(*pdOracle)
	s.pdClient = pdCli
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TearDownSuite(c *C) {
	s.pdClient.Close()
	s.pd.Close()
}

func TestPDOracle_UntilExpired(t *testing.T) {
	lockAfter, lockExp := 10, 15
	o := NewEmptyPDOracle()
	start := time.Now()
	SetEmptyPDOracleLastTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
	lockTs := oracle.ComposeTS(oracle.GetPhysical(start.Add(time.Duration(lockAfter)*time.Millisecond)), 1)
	waitTs := o.UntilExpired(lockTs, uint64(lockExp), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	if waitTs != int64(lockAfter+lockExp) {
		t.Errorf("waitTs shoulb be %d but got %d", int64(lockAfter+lockExp), waitTs)
	}
}

func (s *clientTestSuite) TestGetStaleTimestamp(c *C) {
	ctx := context.Background()
	t0Tidb, t1Pd, t2Tidb, err := s.pd.getTransferTimeline(ctx)
	c.Assert(err, IsNil)
	physical := s.pd.getStaleTimestamp(ctx, t0Tidb, t1Pd, t2Tidb, 10)
	c.Assert(err, IsNil)
	now := oracle.GetPhysical(time.Now())
	c.Assert(now-physical, LessEqual, time.Second.Milliseconds()*10)
	c.Assert(now-physical, GreaterEqual, time.Second.Milliseconds()*9)

	t := time.Now()
	t0Tidb, t1Pd, t2Tidb = mockDelay(t, time.Second*1, time.Second*2)
	physical = s.pd.getStaleTimestamp(ctx, t0Tidb, t1Pd, t2Tidb, 10)
	c.Assert(oracle.GetPhysical(t)-physical, Equals, time.Second.Milliseconds()*10)

	// g_t = 0, tb = g_t + 0, tp = g_t + 1 send tb -> tp
	// g_t = 1, tb = g_t + 0 = 1, tp = g_t + 1 = 2 send tp -> tb
	// g_t = 2, tb = g_t + 0 = 2, tp = g_t + 1 = 3 tb receive
	// g_t = -10 tp = -9 tb = -10
	t = time.Now()
	t0Tidb, t1Pd, t2Tidb = mockDelay(t, time.Second*2, time.Second*2)
	physical = s.pd.getStaleTimestamp(ctx, t0Tidb, t1Pd, t2Tidb, 10)
	c.Assert(oracle.GetPhysical(t)-physical, Equals, time.Second.Milliseconds()*9)
}

func mockDelay(now time.Time, duration0, duration1 time.Duration) (t0Tidb, t1Pd, t2Tidb int64) {
	t0Tidb = oracle.GetPhysical(now)
	t1Pd = t0Tidb + duration0.Milliseconds()
	t2Tidb = t0Tidb + duration1.Milliseconds()
	return t0Tidb, t1Pd, t2Tidb
}
