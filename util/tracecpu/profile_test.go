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
	"github.com/uber-go/atomic"
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

	cnt := collector.getCollectCnt()
	// test for StartCPUProfile.
	buf := bytes.NewBuffer(nil)
	err := tracecpu.StartCPUProfile(buf)
	c.Assert(err, IsNil)
	s.waitCollectCnt(collector, cnt+2)
	err = tracecpu.StopCPUProfile()
	c.Assert(err, IsNil)
	_, err = profile.Parse(buf)
	c.Assert(err, IsNil)

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
	s.setTopSQLEnable(false)
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsFalse)

	s.setTopSQLEnable(true)
	err := tracecpu.StartCPUProfile(bytes.NewBuffer(nil))
	c.Assert(err, IsNil)
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsTrue)
	s.setTopSQLEnable(false)
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsTrue)
	err = tracecpu.StopCPUProfile()
	c.Assert(err, IsNil)

	s.setTopSQLEnable(false)
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsFalse)
	s.setTopSQLEnable(true)
	c.Assert(tracecpu.GlobalSQLStatsProfiler.IsEnabled(), IsTrue)
}

func (s *testSuite) waitCollectCnt(collector *mockCollector, cnt int64) {
	timeout := time.After(time.Second * 5)
	for {
		// Wait for collector collect sql stats count >= expected count
		if collector.getCollectCnt() >= cnt {
			break
		}
		select {
		case <-timeout:
			break
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (s *testSuite) setTopSQLEnable(enabled bool) {
	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	newCfg.TopSQL.Enable = enabled
	config.StoreGlobalConfig(&newCfg)
}

func (s *testSuite) mockExecuteSQL(sql, plan string) {
	ctx := context.Background()
	sqlDigest := genDigest(sql)
	ctx = tracecpu.SetGoroutineLabelsWithSQL(ctx, sql, sqlDigest)
	s.mockExecute(time.Millisecond * 100)
	planDigest := genDigest(plan)
	tracecpu.SetGoroutineLabelsWithSQLAndPlan(ctx, sqlDigest, planDigest, plan)
	s.mockExecute(time.Millisecond * 300)
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

type mockCollector struct {
	sync.Mutex
	// sql_digest -> normalized SQL
	sqlMap map[string]string
	// plan_digest -> normalized plan
	planMap map[string]string
	// (sql + plan_digest) -> sql stats
	sqlStatsMap map[string]*tracecpu.SQLStats
	collectCnt  atomic.Int64
}

func newMockStatsCollector() *mockCollector {
	return &mockCollector{
		sqlMap:      make(map[string]string),
		planMap:     make(map[string]string),
		sqlStatsMap: make(map[string]*tracecpu.SQLStats),
	}
}

func (c *mockCollector) hash(stat tracecpu.SQLStats) string {
	return stat.SQLDigest + stat.PlanDigest
}

func (c *mockCollector) Collect(ts int64, stats []tracecpu.SQLStats) {
	defer c.collectCnt.Inc()
	if len(stats) == 0 {
		return
	}
	c.Lock()
	defer c.Unlock()
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

func (c *mockCollector) getCollectCnt() int64 {
	return c.collectCnt.Load()
}

func (c *mockCollector) getSQLStats(sql, plan string) *tracecpu.SQLStats {
	c.Lock()
	sqlDigest, planDigest := genDigest(sql), genDigest(plan)
	hash := c.hash(tracecpu.SQLStats{SQLDigest: sqlDigest, PlanDigest: planDigest})
	tmp := c.sqlStatsMap[hash]
	c.Unlock()
	return tmp
}

func (c *mockCollector) getSQL(sqlDigest string) string {
	c.Lock()
	sql := c.sqlMap[sqlDigest]
	c.Unlock()
	return sql
}

func (c *mockCollector) getPlan(planDigest string) string {
	c.Lock()
	plan := c.planMap[planDigest]
	c.Unlock()
	return plan
}

func (c *mockCollector) RegisterSQL(sqlDigest, normalizedSQL string) {
	c.Lock()
	_, ok := c.sqlMap[sqlDigest]
	if !ok {
		c.sqlMap[sqlDigest] = normalizedSQL
	}
	c.Unlock()

}

func (c *mockCollector) RegisterPlan(planDigest string, normalizedPlan string) {
	c.Lock()
	_, ok := c.planMap[planDigest]
	if !ok {
		c.planMap[planDigest] = normalizedPlan
	}
	c.Unlock()
}
