package tracecpu

import (
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type SQLStatsCollector interface {
	Collect(ts int64, stats []SQLStats)
	RegisterSQL(sqlDigest, normalizedSQL string)
	RegisterPlan(planDigest string, normalizedPlan string)
}

type SQLStats struct {
	sqlDigest  string
	planDigest string
	cpuTimeMs  uint32
}

type mockStatsCollector struct {
	// sql_digest -> normalized SQL
	sqlmu  sync.Mutex
	sqlMap map[string]string
	// plan_digest -> normalized plan
	planMu  sync.Mutex
	planMap map[string]string

	decodePlanFn func(string) string
}

func NewMockStatsCollector(decodePlanFn func(string) string) SQLStatsCollector {
	return &mockStatsCollector{
		sqlMap:       make(map[string]string),
		planMap:      make(map[string]string),
		decodePlanFn: decodePlanFn,
	}
}

func (c *mockStatsCollector) Collect(ts int64, stats []SQLStats) {
	if len(stats) == 0 {
		return
	}
	total := int64(0)
	logutil.BgLogger().Info("-------- [ BEGIN ] ----------", zap.Int64("ts", ts))
	for _, stmt := range stats {
		logutil.BgLogger().Info(fmt.Sprintf("%s : %v, %v, %v, %v", time.Duration(stmt.cpuTimeMs)*time.Millisecond, shortString(stmt.sqlDigest, 5), shortString(stmt.planDigest, 5), c.getSQL(stmt.sqlDigest), shortString(c.getPlan(stmt.planDigest), 30)))
		total += int64(stmt.cpuTimeMs)
	}
	logutil.BgLogger().Info("-------- [ END ] ", zap.Duration("total", time.Duration(total)*time.Millisecond))
}

func shortString(digest string, n int) string {
	if len(digest) <= n {
		return digest
	}
	return digest[:n]
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
		p := c.decodePlanFn(normalizedPlan)
		c.planMap[planDigest] = p
	}
	c.planMu.Unlock()
}
