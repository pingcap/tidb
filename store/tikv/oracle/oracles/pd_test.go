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

package oracles_test

import (
	"context"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPDSuite{})

type testPDSuite struct{}

func (s *testPDSuite) TestPDOracle_UntilExpired(c *C) {
	lockAfter, lockExp := 10, 15
	o := oracles.NewEmptyPDOracle()
	start := time.Now()
	oracles.SetEmptyPDOracleLastTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
	lockTs := oracle.ComposeTS(oracle.GetPhysical(start.Add(time.Duration(lockAfter)*time.Millisecond)), 1)
	waitTs := o.UntilExpired(lockTs, uint64(lockExp), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(waitTs, Equals, int64(lockAfter+lockExp), Commentf("waitTs shoulb be %d but got %d", int64(lockAfter+lockExp), waitTs))
}

func (s *testPDSuite) TestPdOracle_GetStaleTimestamp(c *C) {
	o := oracles.NewEmptyPDOracle()
	start := time.Now()
	oracles.SetEmptyPDOracleLastTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
	ts, err := o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 10)
	c.Assert(err, IsNil)

	duration := start.Sub(oracle.GetTimeFromTS(ts))
	c.Assert(duration <= 12*time.Second && duration >= 8*time.Second, IsTrue, Commentf("stable TS have accuracy err, expect: %d +-2, obtain: %d", 10, duration))

	_, err = o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 1e12)
	c.Assert(err, NotNil, Commentf("expect exceed err but get nil"))

	testcases := []struct {
		name      string
		preSec    uint64
		expectErr string
	}{
		{
			name:      "normal case",
			preSec:    6,
			expectErr: "",
		},
		{
			name:      "preSec too large",
			preSec:    math.MaxUint64,
			expectErr: ".*invalid prevSecond.*",
		},
	}

	for _, testcase := range testcases {
		comment := Commentf("%s", testcase.name)
		start = time.Now()
		oracles.SetEmptyPDOracleLastTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
		ts, err = o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, testcase.preSec)
		if testcase.expectErr == "" {
			c.Assert(err, IsNil, comment)
			duration = start.Sub(oracle.GetTimeFromTS(ts))
			c.Assert(duration <= time.Duration(testcase.preSec+2)*time.Second && duration >= time.Duration(testcase.preSec-2)*time.Second, IsTrue, Commentf("%s: stable TS have accuracy err, expect: %d +-2, obtain: %d", comment.CheckCommentString(), testcase.preSec, duration))
		} else {
			c.Assert(err, ErrorMatches, testcase.expectErr, comment)
		}
	}
}
