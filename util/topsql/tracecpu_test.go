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

package topsql_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/topsql"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tidb/util/topsql/tracecpu/mock"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = SerialSuites(&testSuite{})

type testSuite struct{}

func (s *testSuite) SetUpSuite(c *C) {
	variable.TopSQLVariable.Enable.Store(true)
	variable.TopSQLVariable.AgentAddress.Store("mock")
	variable.TopSQLVariable.PrecisionSeconds.Store(1)
	tracecpu.GlobalSQLCPUProfiler.Run()
}

func (s *testSuite) TestTopSQLCPUProfile(c *C) {
	collector := mock.NewTopSQLCollector()
	tracecpu.GlobalSQLCPUProfiler.SetCollector(collector)
	reqs := []struct {
		sql  string
		plan string
	}{
		{"select * from t where a=?", "point-get"},
		{"select * from t where a>?", "table-scan"},
		{"insert into t values (?)", ""},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, req := range reqs {
		go func(sql, plan string) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					s.mockExecuteSQL(sql, plan)
				}
			}
		}(req.sql, req.plan)
	}

	// test for StartCPUProfile.
	buf := bytes.NewBuffer(nil)
	err := tracecpu.StartCPUProfile(buf)
	c.Assert(err, IsNil)
	collector.WaitCollectCnt(2)
	err = tracecpu.StopCPUProfile()
	c.Assert(err, IsNil)
	_, err = profile.Parse(buf)
	c.Assert(err, IsNil)

	for _, req := range reqs {
		stats := collector.GetSQLStatsBySQLWithRetry(req.sql, len(req.plan) > 0)
		c.Assert(len(stats), Equals, 1)
		sql := collector.GetSQL(stats[0].SQLDigest)
		plan := collector.GetPlan(stats[0].PlanDigest)
		c.Assert(sql, Equals, req.sql)
		c.Assert(plan, Equals, req.plan)
	}
}

func (s *testSuite) TestIsEnabled(c *C) {
	s.setTopSQLEnable(false)
	c.Assert(tracecpu.GlobalSQLCPUProfiler.IsEnabled(), IsFalse)

	s.setTopSQLEnable(true)
	err := tracecpu.StartCPUProfile(bytes.NewBuffer(nil))
	c.Assert(err, IsNil)
	c.Assert(tracecpu.GlobalSQLCPUProfiler.IsEnabled(), IsTrue)
	s.setTopSQLEnable(false)
	c.Assert(tracecpu.GlobalSQLCPUProfiler.IsEnabled(), IsTrue)
	err = tracecpu.StopCPUProfile()
	c.Assert(err, IsNil)

	s.setTopSQLEnable(false)
	c.Assert(tracecpu.GlobalSQLCPUProfiler.IsEnabled(), IsFalse)
	s.setTopSQLEnable(true)
	c.Assert(tracecpu.GlobalSQLCPUProfiler.IsEnabled(), IsTrue)
}

func (s *testSuite) setTopSQLEnable(enabled bool) {
	variable.TopSQLVariable.Enable.Store(enabled)
}

func (s *testSuite) mockExecuteSQL(sql, plan string) {
	ctx := context.Background()
	sqlDigest := mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, "", nil)
	s.mockExecute(time.Millisecond * 100)
	planDigest := genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, plan, planDigest)
	s.mockExecute(time.Millisecond * 300)
}

func genDigest(str string) *parser.Digest {
	if str == "" {
		return parser.NewDigest(nil)
	}
	return parser.DigestNormalized(str)
}

func (s *testSuite) mockExecute(d time.Duration) {
	start := time.Now()
	for {
		for i := 0; i < 10e5; i++ {
		}
		if time.Since(start) > d {
			return
		}
	}
}
