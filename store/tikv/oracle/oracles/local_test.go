// Copyright 2016 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
)

var _ = Suite(&testOraclesSuite{})

type testOraclesSuite struct{}

func (s *testOraclesSuite) TestLocalOracle(c *C) {
	l := oracles.NewLocalOracle()
	defer l.Close()
	m := map[uint64]struct{}{}
	for i := 0; i < 100000; i++ {
		ts, err := l.GetTimestamp(context.Background(), &oracle.Option{})
		c.Assert(err, IsNil)
		m[ts] = struct{}{}
	}

	c.Assert(len(m) == 100000, IsTrue, Commentf("should generate same ts"))
}

func (s *testOraclesSuite) TestIsExpired(c *C) {
	o := oracles.NewLocalOracle()
	defer o.Close()
	start := time.Now()
	oracles.SetOracleHookCurrentTime(o, start)
	ts, _ := o.GetTimestamp(context.Background(), &oracle.Option{})
	oracles.SetOracleHookCurrentTime(o, start.Add(10*time.Millisecond))
	expire := o.IsExpired(ts, 5, &oracle.Option{})
	c.Assert(expire, IsTrue, Commentf("should expire"))
	expire = o.IsExpired(ts, 200, &oracle.Option{})
	c.Assert(expire, IsFalse, Commentf("should not expire"))
}

func (s *testOraclesSuite) TestLocalOracle_UntilExpired(c *C) {
	o := oracles.NewLocalOracle()
	defer o.Close()
	start := time.Now()
	oracles.SetOracleHookCurrentTime(o, start)
	ts, _ := o.GetTimestamp(context.Background(), &oracle.Option{})
	oracles.SetOracleHookCurrentTime(o, start.Add(10*time.Millisecond))
	c.Assert(o.UntilExpired(ts, 5, &oracle.Option{}) == -5 && o.UntilExpired(ts, 15, &oracle.Option{}) == 5, IsTrue, Commentf("before it is expired, it should be +-5"))
}
