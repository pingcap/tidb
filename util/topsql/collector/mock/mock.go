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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mock

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/topsql/collector"
	"github.com/pingcap/tidb/util/topsql/primitives"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// TopSQLCollector uses for testing.
type TopSQLCollector struct {
	sync.Mutex
	// sql_digest -> normalized SQL
	sqlMap map[parser.RawDigestString]string
	// plan_digest -> normalized plan
	planMap map[parser.RawDigestString]string
	// (sql + plan_digest) -> sql stats
	sqlStatsMap map[primitives.SQLPlanDigest]*collector.SQLCPUTimeRecord
	collectCnt  atomic.Int64
}

// NewTopSQLCollector uses for testing.
func NewTopSQLCollector() *TopSQLCollector {
	return &TopSQLCollector{
		sqlMap:      make(map[parser.RawDigestString]string),
		planMap:     make(map[parser.RawDigestString]string),
		sqlStatsMap: make(map[primitives.SQLPlanDigest]*collector.SQLCPUTimeRecord),
	}
}

// Start implements TopSQLReporter interface.
func (c *TopSQLCollector) Start() {}

// Collect uses for testing.
func (c *TopSQLCollector) Collect(stats []collector.SQLCPUTimeRecord) {
	defer c.collectCnt.Inc()
	if len(stats) == 0 {
		return
	}
	c.Lock()
	defer c.Unlock()
	for _, stmt := range stats {
		stats, ok := c.sqlStatsMap[stmt.SQLAndPlan]
		if !ok {
			stats = &collector.SQLCPUTimeRecord{
				SQLAndPlan: stmt.SQLAndPlan,
			}
			c.sqlStatsMap[stmt.SQLAndPlan] = stats
		}
		stats.CPUTimeMs += stmt.CPUTimeMs
		logutil.BgLogger().Info("mock top sql collector collected sql",
			zap.String("sql", c.sqlMap[stmt.SQLAndPlan.SQLDigest]),
			zap.Bool("has-plan", len(c.planMap[stmt.SQLAndPlan.PlanDigest]) > 0))
	}
}

// CollectStmtStatsMap implements stmtstats.Collector.
func (c *TopSQLCollector) CollectStmtStatsMap(_ stmtstats.StatementStatsMap) {}

// GetSQLStatsBySQLWithRetry uses for testing.
func (c *TopSQLCollector) GetSQLStatsBySQLWithRetry(sql string, planIsNotNull bool) []*collector.SQLCPUTimeRecord {
	after := time.After(time.Second * 10)
	for {
		select {
		case <-after:
			return nil
		default:
		}
		stats := c.GetSQLStatsBySQL(sql, planIsNotNull)
		if len(stats) > 0 {
			return stats
		}
		c.WaitCollectCnt(1)
	}
}

// GetSQLStatsBySQL uses for testing.
func (c *TopSQLCollector) GetSQLStatsBySQL(sql string, planIsNotNull bool) []*collector.SQLCPUTimeRecord {
	stats := make([]*collector.SQLCPUTimeRecord, 0, 2)
	sqlDigest := GenSQLDigest(sql)
	c.Lock()
	for _, stmt := range c.sqlStatsMap {
		if stmt.SQLAndPlan.SQLDigest == sqlDigest.RawAsString() {
			if planIsNotNull {
				plan := c.planMap[stmt.SQLAndPlan.PlanDigest]
				if len(plan) > 0 {
					stats = append(stats, stmt)
				}
			} else {
				stats = append(stats, stmt)
			}
		}
	}
	c.Unlock()
	return stats
}

// GetSQLCPUTimeBySQL uses for testing.
func (c *TopSQLCollector) GetSQLCPUTimeBySQL(sql string) uint32 {
	sqlDigest := GenSQLDigest(sql)
	cpuTime := uint32(0)
	c.Lock()
	for _, stmt := range c.sqlStatsMap {
		if stmt.SQLAndPlan.SQLDigest == sqlDigest.RawAsString() {
			cpuTime += stmt.CPUTimeMs
		}
	}
	c.Unlock()
	return cpuTime
}

// GetSQL uses for testing.
func (c *TopSQLCollector) GetSQL(sqlDigest parser.RawDigestString) string {
	c.Lock()
	sql := c.sqlMap[sqlDigest]
	c.Unlock()
	return sql
}

// GetPlan uses for testing.
func (c *TopSQLCollector) GetPlan(planDigest parser.RawDigestString) string {
	c.Lock()
	plan := c.planMap[planDigest]
	c.Unlock()
	return plan
}

// RegisterSQL uses for testing.
func (c *TopSQLCollector) RegisterSQL(sqlDigest parser.RawDigestString, normalizedSQL string, isInternal bool) {
	c.Lock()
	_, ok := c.sqlMap[sqlDigest]
	if !ok {
		c.sqlMap[sqlDigest] = normalizedSQL
	}
	c.Unlock()

}

// RegisterPlan uses for testing.
func (c *TopSQLCollector) RegisterPlan(planDigest parser.RawDigestString, normalizedPlan string) {
	c.Lock()
	_, ok := c.planMap[planDigest]
	if !ok {
		c.planMap[planDigest] = normalizedPlan
	}
	c.Unlock()
}

// WaitCollectCnt uses for testing.
func (c *TopSQLCollector) WaitCollectCnt(count int64) {
	timeout := time.After(time.Second * 10)
	end := c.collectCnt.Load() + count
	for {
		// Wait for reporter to collect sql stats count >= expected count
		if c.collectCnt.Load() >= end {
			return
		}
		select {
		case <-timeout:
			return
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// Reset cleans all collected data.
func (c *TopSQLCollector) Reset() {
	c.Lock()
	defer c.Unlock()
	c.sqlMap = make(map[parser.RawDigestString]string)
	c.planMap = make(map[parser.RawDigestString]string)
	c.sqlStatsMap = make(map[primitives.SQLPlanDigest]*collector.SQLCPUTimeRecord)
	c.collectCnt.Store(0)
}

// CollectCnt uses for testing.
func (c *TopSQLCollector) CollectCnt() int64 {
	return c.collectCnt.Load()
}

// Close implements the interface.
func (c *TopSQLCollector) Close() {}

// GenSQLDigest uses for testing.
func GenSQLDigest(sql string) *parser.Digest {
	_, digest := parser.NormalizeDigest(sql)
	return digest
}
