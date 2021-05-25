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

package tracecpu_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/tracecpu"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = SerialSuites(&testSuite{})

type testSuite struct{}

func (s *testSuite) SetUpSuite(c *C) {
	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	newCfg.TopSQL.Enable = true
	newCfg.TopSQL.RefreshInterval = 1
	config.StoreGlobalConfig(&newCfg)
	tracecpu.GlobalSQLStatsProfiler.Run()
}

func (s *testSuite) TestSQLStatsProfile(c *C) {
	collector := newMockStatsCollector()
	tracecpu.GlobalSQLStatsProfiler.SetCollector(collector)
	reqs := []struct {
		sql  string
		plan string
	}{
		{"select * from t where a=?", "point-get"},
		{"select * from t where a>?", "table-scan"},
		{"insert into t values (?)", ""},
	}
	var wg sync.WaitGroup
	for _, req := range reqs {
		wg.Add(1)
		go func(sql, plan string) {
			defer wg.Done()
			s.mockExecuteSQL(sql, plan)
		}(req.sql, req.plan)
	}

	// test for StartCPUProfile.
	s.testFetchProfile(c, time.Second+time.Millisecond*200)

	// test for collect SQL stats.
	wg.Wait()
	for _, req := range reqs {
		stats := collector.getSQLStats(req.sql, req.plan)
		c.Assert(stats, NotNil)
		sql := collector.getSQL(stats.SQLDigest)
		plan := collector.getPlan(stats.PlanDigest)
		c.Assert(sql, Equals, req.sql)
		c.Assert(plan, Equals, req.plan)
	}
}

func (s *testSuite) TestIsEnabled(c *C) {
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsTrue)
	config.GetGlobalConfig().TopSQL.Enable = false
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsFalse)

	config.GetGlobalConfig().TopSQL.Enable = true
	err := tracecpu.StartCPUProfile(bytes.NewBuffer(nil))
	c.Assert(err, IsNil)
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsTrue)
	config.GetGlobalConfig().TopSQL.Enable = false
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsTrue)
	err = tracecpu.StopCPUProfile()
	c.Assert(err, IsNil)

	config.GetGlobalConfig().TopSQL.Enable = false
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsFalse)
	config.GetGlobalConfig().TopSQL.Enable = true
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsTrue)
}

func (s *testSuite) testFetchProfile(c *C, d time.Duration) {
	buf := bytes.NewBuffer(nil)
	err := tracecpu.StartCPUProfile(buf)
	c.Assert(err, IsNil)
	time.Sleep(d)
	err = tracecpu.StopCPUProfile()
	c.Assert(err, IsNil)
	_, err = profile.Parse(buf)
	c.Assert(err, IsNil)
}

func (s *testSuite) mockExecuteSQL(sql, plan string) {
	ctx := context.Background()
	sqlDigest := genDigest(sql)
	ctx = tracecpu.SetGoroutineLabelsWithSQL(ctx, sql, sqlDigest)
	s.mockExecute(time.Millisecond * 100)
	planDigest := genDigest(plan)
	tracecpu.SetGoroutineLabelsWithSQLAndPlan(ctx, sqlDigest, planDigest, plan)
	s.mockExecute(time.Millisecond * 200)
}

func genDigest(str string) string {
	if str == "" {
		return ""
	}
	hasher := sha256.New()
	hasher.Write(hack.Slice(str))
	return hex.EncodeToString(hasher.Sum(nil))
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

type mockStatsCollector struct {
	// sql_digest -> normalized SQL
	sqlmu  sync.Mutex
	sqlMap map[string]string
	// plan_digest -> normalized plan
	planMu  sync.Mutex
	planMap map[string]string
	// sql -> sql stats
	sqlStatsMap map[string]*tracecpu.SQLStats
}

func newMockStatsCollector() *mockStatsCollector {
	return &mockStatsCollector{
		sqlMap:      make(map[string]string),
		planMap:     make(map[string]string),
		sqlStatsMap: make(map[string]*tracecpu.SQLStats),
	}
}

func (c *mockStatsCollector) hash(stat tracecpu.SQLStats) string {
	return stat.SQLDigest + stat.PlanDigest
}

func (c *mockStatsCollector) Collect(ts int64, stats []tracecpu.SQLStats) {
	if len(stats) == 0 {
		return
	}

	for _, stmt := range stats {
		hash := c.hash(stmt)
		stats, ok := c.sqlStatsMap[hash]
		if !ok {
			tmp := stmt
			stats = &tmp
			c.sqlStatsMap[hash] = stats
		}
		stats.CPUTimeMs += stmt.CPUTimeMs
	}
}

func (c *mockStatsCollector) getSQLStats(sql, plan string) *tracecpu.SQLStats {
	sqlDigest, planDigest := genDigest(sql), genDigest(plan)
	hash := c.hash(tracecpu.SQLStats{SQLDigest: sqlDigest, PlanDigest: planDigest})
	return c.sqlStatsMap[hash]
}

func (c *mockStatsCollector) getSQL(sqlDigest string) string {
	c.sqlmu.Lock()
	sql := c.sqlMap[sqlDigest]
	c.sqlmu.Unlock()
	return sql
}

func (c *mockStatsCollector) getPlan(planDigest string) string {
	c.planMu.Lock()
	plan := c.planMap[planDigest]
	c.planMu.Unlock()
	return plan
}

func (c *mockStatsCollector) RegisterSQL(sqlDigest, normalizedSQL string) {
	c.sqlmu.Lock()
	_, ok := c.sqlMap[sqlDigest]
	if !ok {
		c.sqlMap[sqlDigest] = normalizedSQL
	}
	c.sqlmu.Unlock()

}

func (c *mockStatsCollector) RegisterPlan(planDigest string, normalizedPlan string) {
	c.planMu.Lock()
	_, ok := c.planMap[planDigest]
	if !ok {
		c.planMap[planDigest] = normalizedPlan
	}
	c.planMu.Unlock()
}
