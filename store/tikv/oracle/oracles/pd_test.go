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
	"github.com/tikv/pd/pkg/testutil"
	"go.uber.org/goleak"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pd "github.com/tikv/pd/client"
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

func TestPdOracle_GetTimestamp(t *testing.T) {

}
