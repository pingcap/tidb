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

package mock

import (
	"sync"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/util/tracecpu"
	"github.com/uber-go/atomic"
)

// TopSQLCollector uses for testing.
type TopSQLCollector struct {
	sync.Mutex
	// sql_digest -> normalized SQL
	sqlMap map[string]string
	// plan_digest -> normalized plan
	planMap map[string]string
	// (sql + plan_digest) -> sql stats
	sqlStatsMap map[string]*tracecpu.SQLStats
	collectCnt  atomic.Int64
}

// NewTopSQLCollector uses for testing.
func NewTopSQLCollector() *TopSQLCollector {
	return &TopSQLCollector{
		sqlMap:      make(map[string]string),
		planMap:     make(map[string]string),
		sqlStatsMap: make(map[string]*tracecpu.SQLStats),
	}
}

// Collect uses for testing.
func (c *TopSQLCollector) Collect(ts int64, stats []tracecpu.SQLStats) {
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

// GetSQLStatsBySQLWithRetry uses for testing.
func (c *TopSQLCollector) GetSQLStatsBySQLWithRetry(sql string, planIsNotNull bool) []*tracecpu.SQLStats {
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
func (c *TopSQLCollector) GetSQLStatsBySQL(sql string, planIsNotNull bool) []*tracecpu.SQLStats {
	stats := make([]*tracecpu.SQLStats, 0, 2)
	sqlDigest := GenSQLDigest(sql)
	c.Lock()
	for _, stmt := range c.sqlStatsMap {
		if stmt.SQLDigest == sqlDigest {
			if planIsNotNull {
				plan := c.planMap[stmt.PlanDigest]
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

// GetSQL uses for testing.
func (c *TopSQLCollector) GetSQL(sqlDigest string) string {
	c.Lock()
	sql := c.sqlMap[sqlDigest]
	c.Unlock()
	return sql
}

// GetPlan uses for testing.
func (c *TopSQLCollector) GetPlan(planDigest string) string {
	c.Lock()
	plan := c.planMap[planDigest]
	c.Unlock()
	return plan
}

// RegisterSQL uses for testing.
func (c *TopSQLCollector) RegisterSQL(sqlDigest, normalizedSQL string) {
	c.Lock()
	_, ok := c.sqlMap[sqlDigest]
	if !ok {
		c.sqlMap[sqlDigest] = normalizedSQL
	}
	c.Unlock()

}

// RegisterPlan uses for testing.
func (c *TopSQLCollector) RegisterPlan(planDigest string, normalizedPlan string) {
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
		// Wait for collector collect sql stats count >= expected count
		if c.collectCnt.Load() >= end {
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

func (c *TopSQLCollector) hash(stat tracecpu.SQLStats) string {
	return stat.SQLDigest + stat.PlanDigest
}

// GenSQLDigest uses for testing.
func GenSQLDigest(sql string) string {
	_, digest := parser.NormalizeDigest(sql)
	return digest.String()
}
