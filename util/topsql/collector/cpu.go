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

package collector

import (
	"context"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/cpuprofile"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"go.uber.org/zap"
)

const (
	labelSQLDigest  = "sql_digest"
	labelPlanDigest = "plan_digest"
)

// Collector uses to collect SQL execution cpu time.
type Collector interface {
	// Collect uses to collect the SQL execution cpu time.
	Collect(stats []SQLCPUTimeRecord)
}

// SQLCPUTimeRecord represents a single record of how much cpu time a sql plan consumes in one second.
//
// PlanDigest can be empty, because:
// 1. some sql statements has no plan, like `COMMIT`
// 2. when a sql statement is being compiled, there's no plan yet
type SQLCPUTimeRecord struct {
	SQLDigest  []byte
	PlanDigest []byte
	CPUTimeMs  uint32
}

// SQLCPUCollector uses to consume cpu profile from globalCPUProfiler, then parse the SQL CPU usage from the cpu profile data.
// It is not thread-safe, should only be used in one goroutine.
type SQLCPUCollector struct {
	started bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	collector  Collector
	registered bool
}

// NewSQLCPUCollector create a SQLCPUCollector.
func NewSQLCPUCollector(c Collector) *SQLCPUCollector {
	return &SQLCPUCollector{
		collector: c,
	}
}

// Start uses to start to run SQLCPUCollector.
// This will register a consumer into globalCPUProfiler, then SQLCPUCollector will receive cpu profile data per seconds.
// WARN: this function is not thread-safe.
func (sp *SQLCPUCollector) Start() {
	if sp.started {
		return
	}
	sp.started = true
	sp.ctx, sp.cancel = context.WithCancel(context.Background())
	sp.wg.Add(1)
	go sp.collectSQLCPULoop()
	logutil.BgLogger().Info("sql cpu collector started")
}

// Stop uses to stop the SQLCPUCollector.
// WARN: this function is not thread-safe.
func (sp *SQLCPUCollector) Stop() {
	if !sp.started {
		return
	}
	sp.started = false
	if sp.cancel != nil {
		sp.cancel()
	}

	sp.wg.Wait()
	logutil.BgLogger().Info("sql cpu collector stopped")
}

var defCollectTickerInterval = time.Second

func (sp *SQLCPUCollector) collectSQLCPULoop() {
	profileConsumer := make(cpuprofile.ProfileConsumer, 1)
	ticker := time.NewTicker(defCollectTickerInterval)
	defer func() {
		util.Recover("top-sql", "startAnalyzeProfileWorker", nil, false)
		sp.wg.Done()
		sp.doUnregister(profileConsumer)
		ticker.Stop()
	}()

	for {
		if topsqlstate.TopSQLEnabled() {
			sp.doRegister(profileConsumer)
		} else {
			sp.doUnregister(profileConsumer)
		}

		select {
		case <-sp.ctx.Done():
			return
		case <-ticker.C:
		case data := <-profileConsumer:
			sp.handleProfileData(data)
		}
	}
}

func (sp *SQLCPUCollector) handleProfileData(data *cpuprofile.ProfileData) {
	if data.Error != nil {
		return
	}

	p, err := profile.ParseData(data.Data.Bytes())
	if err != nil {
		logutil.BgLogger().Error("parse profile error", zap.Error(err))
		return
	}
	stats := sp.parseCPUProfileBySQLLabels(p)
	sp.collector.Collect(stats)
}

func (sp *SQLCPUCollector) doRegister(profileConsumer cpuprofile.ProfileConsumer) {
	if sp.registered {
		return
	}
	sp.registered = true
	cpuprofile.Register(profileConsumer)
}

func (sp *SQLCPUCollector) doUnregister(profileConsumer cpuprofile.ProfileConsumer) {
	if !sp.registered {
		return
	}
	sp.registered = false
	cpuprofile.Unregister(profileConsumer)
}

// parseCPUProfileBySQLLabels uses to aggregate the cpu-profile sample data by sql_digest and plan_digest labels,
// output the TopSQLCPUTimeRecord slice. Want to know more information about profile labels, see https://rakyll.org/profiler-labels/
// The sql_digest label is been set by `SetSQLLabels` function after parse the SQL.
// The plan_digest label is been set by `SetSQLAndPlanLabels` function after build the SQL plan.
// Since `SQLCPUCollector` only care about the cpu time that consume by (sql_digest,plan_digest), the other sample data
// without those label will be ignore.
func (sp *SQLCPUCollector) parseCPUProfileBySQLLabels(p *profile.Profile) []SQLCPUTimeRecord {
	sqlMap := make(map[string]*sqlStats)
	idx := len(p.SampleType) - 1
	for _, s := range p.Sample {
		digests, ok := s.Label[labelSQLDigest]
		if !ok || len(digests) == 0 {
			continue
		}
		for _, digest := range digests {
			stmt, ok := sqlMap[digest]
			if !ok {
				stmt = &sqlStats{
					plans: make(map[string]int64),
					total: 0,
				}
				sqlMap[digest] = stmt
			}
			stmt.total += s.Value[idx]

			plans := s.Label[labelPlanDigest]
			for _, plan := range plans {
				stmt.plans[plan] += s.Value[idx]
			}
		}
	}
	return sp.createSQLStats(sqlMap)
}

func (sp *SQLCPUCollector) createSQLStats(sqlMap map[string]*sqlStats) []SQLCPUTimeRecord {
	stats := make([]SQLCPUTimeRecord, 0, len(sqlMap))
	for sqlDigest, stmt := range sqlMap {
		stmt.tune()
		for planDigest, val := range stmt.plans {
			stats = append(stats, SQLCPUTimeRecord{
				SQLDigest:  []byte(sqlDigest),
				PlanDigest: []byte(planDigest),
				CPUTimeMs:  uint32(time.Duration(val).Milliseconds()),
			})
		}
	}
	return stats
}

type sqlStats struct {
	plans map[string]int64
	total int64
}

// tune use to adjust sql stats. Consider following situation:
// The `sqlStats` maybe:
//     plans: {
//         "table_scan": 200ms, // The cpu time of the sql that plan with `table_scan` is 200ms.
//         "index_scan": 300ms, // The cpu time of the sql that plan with `index_scan` is 300ms.
//       },
//     total:      600ms,       // The total cpu time of the sql is 600ms.
// total_time - table_scan_time - index_scan_time = 100ms, and this 100ms means those sample data only contain the
// sql_digest label, doesn't contain the plan_digest label. This is cause by the `pprof profile` is base on sample,
// and the plan digest can only be set after optimizer generated execution plan. So the remain 100ms means the plan
// optimizer takes time to generated plan.
// After this tune function, the `sqlStats` become to:
//     plans: {
//         ""          : 100ms,  // 600 - 200 - 300 = 100ms, indicate the optimizer generated plan time cost.
//         "table_scan": 200ms,
//         "index_scan": 300ms,
//       },
//     total:      600ms,
func (s *sqlStats) tune() {
	if len(s.plans) == 0 {
		s.plans[""] = s.total
		return
	}
	if len(s.plans) == 1 {
		for k := range s.plans {
			s.plans[k] = s.total
			return
		}
	}
	planTotal := int64(0)
	for _, v := range s.plans {
		planTotal += v
	}
	optimize := s.total - planTotal
	if optimize <= 0 {
		return
	}
	s.plans[""] += optimize
}

// CtxWithSQLDigest wrap the ctx with sql digest.
func CtxWithSQLDigest(ctx context.Context, sqlDigest []byte) context.Context {
	return pprof.WithLabels(ctx, pprof.Labels(labelSQLDigest, string(hack.String(sqlDigest))))
}

// CtxWithSQLAndPlanDigest wrap the ctx with sql digest and plan digest.
func CtxWithSQLAndPlanDigest(ctx context.Context, sqlDigest, planDigest []byte) context.Context {
	return pprof.WithLabels(ctx, pprof.Labels(labelSQLDigest, string(hack.String(sqlDigest)),
		labelPlanDigest, string(hack.String(planDigest))))
}
