// Copyright 2024 PingCAP, Inc.
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

package profileProcess

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/cpuprofile"
	"github.com/pingcap/tidb/pkg/util/logutil"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"go.uber.org/zap"
)

var (
	profiler *ProcessCPUProfiler
	updater  ProcessCPUTimeUpdater
)

// ProcessCPUTimeUpdater Introduce this interface due to the dependency cycle
type ProcessCPUTimeUpdater interface {
	UpdateProcessCPUTime(connID uint64, sqlID uint64, cpuTime time.Duration)
}

// SetupProcessProfiling sets up the process cpu profile worker.
func SetupProcessProfiling(ud ProcessCPUTimeUpdater) {
	profiler = NewProcessCPUProfiler()
	updater = ud
	profiler.Start()
}

// Close uses to close and release related resource.
func Close() {
	profiler.Stop()
}

// AttachAndRegisterProcessInfo attach the ProcessInfo into Goroutine labels.
func AttachAndRegisterProcessInfo(ctx context.Context, connID uint64, sqlID uint64) context.Context {
	processLabel := fmt.Sprintf("%d_%d", connID, sqlID)
	ctx = pprof.WithLabels(ctx, pprof.Labels(labelSQLUID, processLabel))
	pprof.SetGoroutineLabels(ctx)
	return ctx
}

const (
	labelSQLUID = "sql_global_uid"
)

// sqlCPUTimeRecord represents a single record of how much cpu time a sql consumes in one second.
type sqlCPUTimeRecord struct {
	sqlGlobalUID uint64
	total        int64
}

// ProcessCPUProfiler uses to consume cpu profile from globalCPUProfiler, then parse the Process CPU usage from the cpu profile data.
// It is not thread-safe, should only be used in one goroutine.
type ProcessCPUProfiler struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	started    bool
	registered bool
}

// NewProcessCPUProfiler create a ProcessCPUProfiler.
func NewProcessCPUProfiler() *ProcessCPUProfiler {
	return &ProcessCPUProfiler{}
}

// Start uses to start to run SQLCPUCollector.
// This will register a consumer into globalCPUProfiler, then SQLCPUCollector will receive cpu profile data per seconds.
// WARN: this function is not thread-safe.
func (sp *ProcessCPUProfiler) Start() {
	if sp.started {
		return
	}
	sp.started = true
	sp.ctx, sp.cancel = context.WithCancel(context.Background())
	sp.wg.Add(1)
	go sp.collectSQLCPULoop()
	logutil.BgLogger().Info("ProcessCPUProfiler sql cpu collector started")
}

// Stop uses to stop the SQLCPUCollector.
// WARN: this function is not thread-safe.
func (sp *ProcessCPUProfiler) Stop() {
	if !sp.started {
		return
	}
	sp.started = false
	if sp.cancel != nil {
		sp.cancel()
	}

	sp.wg.Wait()
	logutil.BgLogger().Info("ProcessCPUProfiler sql cpu collector stopped")
}

var defCollectTickerInterval = time.Second

func (sp *ProcessCPUProfiler) collectSQLCPULoop() {
	profileConsumer := make(cpuprofile.ProfileConsumer, 1)
	ticker := time.NewTicker(defCollectTickerInterval)
	defer func() {
		sp.wg.Done()
		sp.doUnregister(profileConsumer)
		ticker.Stop()
	}()
	defer util.Recover("profileProcessCpu", "startAnalyzeProfileWorker", nil, false)

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

func (sp *ProcessCPUProfiler) handleProfileData(data *cpuprofile.ProfileData) {
	if data.Error != nil {
		return
	}

	// TODO: maybe we can move profile.ParseData upper to globalCPUProfiler side, and provide just Parsed data for all consumers
	p, err := profile.ParseData(data.Data.Bytes())
	if err != nil {
		logutil.BgLogger().Error("parse profile error", zap.Error(err))
		return
	}
	sp.parseCPUProfile(p)
}

func (sp *ProcessCPUProfiler) doRegister(profileConsumer cpuprofile.ProfileConsumer) {
	if sp.registered {
		return
	}
	sp.registered = true
	cpuprofile.Register(profileConsumer)
}

func (sp *ProcessCPUProfiler) doUnregister(profileConsumer cpuprofile.ProfileConsumer) {
	if !sp.registered {
		return
	}
	sp.registered = false
	cpuprofile.Unregister(profileConsumer)
}

// parseCPUProfile uses to aggregate the cpu-profile sample data by sql_global_uid labels,
// Want to know more information about profile labels, see https://rakyll.org/profiler-labels/
// Since `SQLCPUCollector` only care about the cpu time that consume by (sql_global_uid), the other sample data
// without those label will be ignore.
func (sp *ProcessCPUProfiler) parseCPUProfile(p *profile.Profile) {
	sqlMap := make(map[uint64]sqlCPUTimeRecord)
	idx := len(p.SampleType) - 1
	// Reverse traverse sample data, since only the latest sqlID for each connection is usable
	for i := len(p.Sample) - 1; i >= 0; i-- {
		s := p.Sample[i]
		sqlUIDs, ok := s.Label[labelSQLUID]
		if !ok || len(sqlUIDs) == 0 {
			continue
		}
		for _, sqlUID := range sqlUIDs {
			keys := strings.Split(sqlUID, `_`)
			connID, _ := strconv.ParseUint(keys[0], 10, 64)
			sqlID, _ := strconv.ParseUint(keys[1], 10, 64)
			if timeRecord, ok := sqlMap[connID]; ok {
				if sqlID != sqlMap[connID].sqlGlobalUID {
					// Ignore previous sql's cpu profile data inside the same connection
					continue
				}
				timeRecord.total += s.Value[idx]
				sqlMap[connID] = timeRecord
			} else {
				sqlMap[connID] = sqlCPUTimeRecord{sqlID, 0}
			}
		}
	}
	for key, val := range sqlMap {
		updater.UpdateProcessCPUTime(key, val.sqlGlobalUID, time.Duration(val.total))
	}
}
