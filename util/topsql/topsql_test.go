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
	"github.com/pingcap/tidb/util/topsql/reporter"
	mockServer "github.com/pingcap/tidb/util/topsql/reporter/mock"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tidb/util/topsql/tracecpu/mock"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = SerialSuites(&testSuite{})

type testSuite struct{}

type collectorWrapper struct {
	reporter.TopSQLReporter
}

func (s *testSuite) SetUpSuite(c *C) {
	variable.TopSQLVariable.Enable.Store(true)
	variable.TopSQLVariable.AgentAddress.Store("mock")
	variable.TopSQLVariable.PrecisionSeconds.Store(1)
	tracecpu.GlobalSQLCPUProfiler.Run()
}

func (s *testSuite) TestTopSQLCPUProfile(c *C) {
	collector := mock.NewTopSQLCollector()
	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{collector})
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

func mockPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
}

func (s *testSuite) TestTopSQLReporter(c *C) {
	server, err := mockServer.StartMockAgentServer()
	c.Assert(err, IsNil)
	variable.TopSQLVariable.MaxStatementCount.Store(200)
	variable.TopSQLVariable.ReportIntervalSeconds.Store(1)
	variable.TopSQLVariable.AgentAddress.Store(server.Address())

	client := reporter.NewGRPCReportClient(mockPlanBinaryDecoderFunc)
	report := reporter.NewRemoteTopSQLReporter(client)
	defer report.Close()

	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{report})
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

	sqlMap := make(map[string]string)
	sql2plan := make(map[string]string)
	for _, req := range reqs {
		sql2plan[req.sql] = req.plan
		sqlDigest := mock.GenSQLDigest(req.sql)
		sqlMap[string(sqlDigest.Bytes())] = req.sql

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

	server.WaitCollectCnt(1, time.Second*5)
	records := server.GetLatestRecords()
	checkSQLPlanMap := map[string]struct{}{}
	for _, req := range records {
		c.Assert(len(req.RecordListCpuTimeMs) > 0, IsTrue)
		c.Assert(req.RecordListCpuTimeMs[0] > 0, IsTrue)
		c.Assert(req.RecordListCpuTimeMs[0] > 0, IsTrue)
		normalizedSQL, exist := server.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		c.Assert(exist, IsTrue)
		expectedNormalizedSQL, exist := sqlMap[string(req.SqlDigest)]
		c.Assert(exist, IsTrue)
		c.Assert(normalizedSQL, Equals, expectedNormalizedSQL)

		expectedNormalizedPlan := sql2plan[expectedNormalizedSQL]
		if expectedNormalizedPlan == "" || len(req.PlanDigest) == 0 {
			c.Assert(len(req.PlanDigest), Equals, 0)
			continue
		}
		normalizedPlan, exist := server.GetPlanMetaByDigestBlocking(req.PlanDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(normalizedPlan, Equals, expectedNormalizedPlan)
		checkSQLPlanMap[expectedNormalizedSQL] = struct{}{}
	}
	c.Assert(len(checkSQLPlanMap) == 2, IsTrue)
}

func (s *testSuite) TestMaxSQLAndPlanTest(c *C) {
	collector := mock.NewTopSQLCollector()
	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{collector})

	ctx := context.Background()

	// Test for normal sql and plan
	sql := "select * from t"
	sqlDigest := mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, "", nil)
	plan := "TableReader table:t"
	planDigest := genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, plan, planDigest)

	cSQL := collector.GetSQL(sqlDigest.Bytes())
	c.Assert(cSQL, Equals, sql)
	cPlan := collector.GetPlan(planDigest.Bytes())
	c.Assert(cPlan, Equals, plan)

	// Test for huge sql and plan
	sql = genStr(topsql.MaxSQLTextSize + 10)
	sqlDigest = mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, "", nil)
	plan = genStr(topsql.MaxPlanTextSize + 10)
	planDigest = genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, plan, planDigest)

	cSQL = collector.GetSQL(sqlDigest.Bytes())
	c.Assert(cSQL, Equals, sql[:topsql.MaxSQLTextSize])
	cPlan = collector.GetPlan(planDigest.Bytes())
	c.Assert(cPlan, Equals, "")
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

func genDigest(str string) *parser.Digest {
	if str == "" {
		return parser.NewDigest(nil)
	}
	return parser.DigestNormalized(str)
}

func genStr(n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = 'a' + byte(i%25)
	}
	return string(buf)
}
