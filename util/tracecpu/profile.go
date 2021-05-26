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

package tracecpu

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	labelSQL        = "sql"
	labelSQLDigest  = "sql_digest"
	labelPlanDigest = "plan_digest"
)

// GlobalSQLStatsProfiler is the global SQL stats profiler.
var GlobalSQLStatsProfiler = NewSQLStatsProfiler()

type sqlStatsProfiler struct {
	taskCh     chan *profileTask
	cacheBufCh chan *profileTask

	mu struct {
		sync.Mutex
		ept *exportProfileTask
	}
	collector TopSQLCollector
}

// NewSQLStatsProfiler create a sqlStatsProfiler.
func NewSQLStatsProfiler() *sqlStatsProfiler {
	return &sqlStatsProfiler{
		taskCh:     make(chan *profileTask, 128),
		cacheBufCh: make(chan *profileTask, 128),
	}
}

func (sp *sqlStatsProfiler) Run() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	logutil.BgLogger().Info("cpu profiler started")
	go sp.startCPUProfileWorker()
	go sp.startAnalyzeProfileWorker()
}

func (sp *sqlStatsProfiler) SetCollector(c TopSQLCollector) {
	sp.collector = c
}

func (sp *sqlStatsProfiler) RegisterSQL(sqlDigest, normalizedSQL string) {
	if sp.collector == nil {
		return
	}
	sp.collector.RegisterSQL(sqlDigest, normalizedSQL)
}

func (sp *sqlStatsProfiler) RegisterPlan(planDigest string, normalizedPlan string) {
	if sp.collector == nil {
		return
	}
	sp.collector.RegisterPlan(planDigest, normalizedPlan)
}

func (sp *sqlStatsProfiler) startCPUProfileWorker() {
	defer util.Recover("top-sql", "profileWorker", nil, false)
	for {
		if sp.IsEnabled() {
			sp.doCPUProfile()
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (sp *sqlStatsProfiler) doCPUProfile() {
	interval := config.GetGlobalConfig().TopSQL.RefreshInterval
	task := sp.newProfileTask()
	if err := pprof.StartCPUProfile(task.buf); err != nil {
		return
	}
	ns := int(time.Second)*interval - time.Now().Nanosecond()
	time.Sleep(time.Nanosecond * time.Duration(ns))
	pprof.StopCPUProfile()
	sp.sendProfileTask(task)
}

func (sp *sqlStatsProfiler) sendProfileTask(task *profileTask) {
	task.end = time.Now().Unix()
	sp.taskCh <- task
}

func (sp *sqlStatsProfiler) startAnalyzeProfileWorker() {
	defer util.Recover("top-sql", "analyzeProfileWorker", nil, false)
	for {
		task := <-sp.taskCh
		reader := bytes.NewReader(task.buf.Bytes())
		p, err := profile.Parse(reader)
		if err != nil {
			logutil.BgLogger().Error("parse profile error", zap.Error(err))
			continue
		}
		stats := sp.parseCPUProfileTags(p)
		sp.handleExportProfileTask(p)
		if sp.collector != nil {
			sp.collector.Collect(task.end, stats)
		}
		sp.putTaskToBuffer(task)
	}
}

type profileTask struct {
	buf *bytes.Buffer
	end int64
}

func (sp *sqlStatsProfiler) newProfileTask() *profileTask {
	var task *profileTask
	select {
	case task = <-sp.cacheBufCh:
		task.buf.Reset()
	default:
		task = &profileTask{
			buf: bytes.NewBuffer(make([]byte, 0, 100*1024)),
		}
	}
	return task
}

func (sp *sqlStatsProfiler) putTaskToBuffer(task *profileTask) {
	select {
	case sp.cacheBufCh <- task:
	default:
	}
}

func (sp *sqlStatsProfiler) parseCPUProfileTags(p *profile.Profile) []SQLStats {
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
					plans:      make(map[string]int64),
					total:      0,
					isInternal: false,
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

func (sp *sqlStatsProfiler) createSQLStats(sqlMap map[string]*sqlStats) []SQLStats {
	stats := make([]SQLStats, 0, len(sqlMap))
	for sqlDigest, stmt := range sqlMap {
		stmt.tune()
		for planDigest, val := range stmt.plans {
			stats = append(stats, SQLStats{
				SQLDigest:  sqlDigest,
				PlanDigest: planDigest,
				CPUTimeMs:  uint32(time.Duration(val).Milliseconds()),
			})
		}
	}
	return stats
}

type sqlStats struct {
	plans      map[string]int64
	total      int64
	isInternal bool
}

// tune use to adjust stats
func (s *sqlStats) tune() {
	if len(s.plans) == 0 {
		s.plans[""] = s.total
		return
	}
	planTotal := int64(0)
	for _, v := range s.plans {
		planTotal += v
	}
	remain := s.total - planTotal
	if remain <= 0 {
		return
	}
	for k, v := range s.plans {
		s.plans[k] = v + (v/planTotal)*remain
	}
}

func (sp *sqlStatsProfiler) handleExportProfileTask(p *profile.Profile) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.mu.ept == nil {
		return
	}
	sp.mu.ept.mergeProfile(p)
}

func (sp *sqlStatsProfiler) hasExportProfileTask() bool {
	sp.mu.Lock()
	has := sp.mu.ept != nil
	sp.mu.Unlock()
	return has
}

// IsEnabled return true if it is(should be) enabled. It exports for tests.
func (sp *sqlStatsProfiler) IsEnabled() bool {
	return config.GetGlobalConfig().TopSQL.Enable || sp.hasExportProfileTask()
}

// StartCPUProfile same like pprof.StartCPUProfile.
// Because the GlobalSQLStatsProfiler keep calling pprof.StartCPUProfile to fetch SQL cpu stats, other place (such pprof profile HTTP API handler) call pprof.StartCPUProfile will be failed,
// other place should call tracecpu.StartCPUProfile instead of pprof.StartCPUProfile.
func StartCPUProfile(w io.Writer) error {
	if GlobalSQLStatsProfiler.IsEnabled() {
		return GlobalSQLStatsProfiler.startExportCPUProfile(w)
	}
	return pprof.StartCPUProfile(w)
}

// StopCPUProfile same like pprof.StopCPUProfile.
// other place should call tracecpu.StopCPUProfile instead of pprof.StopCPUProfile.
func StopCPUProfile() error {
	if GlobalSQLStatsProfiler.IsEnabled() {
		return GlobalSQLStatsProfiler.stopExportCPUProfile()
	}
	pprof.StopCPUProfile()
	return nil
}

// SetGoroutineLabelsWithSQL sets the SQL digest label into the goroutine.
func SetGoroutineLabelsWithSQL(ctx context.Context, normalizedSQL, sqlDigest string) context.Context {
	if len(normalizedSQL) == 0 || len(sqlDigest) == 0 {
		return ctx
	}
	if variable.EnablePProfSQLCPU.Load() {
		ctx = pprof.WithLabels(context.Background(), pprof.Labels(labelSQLDigest, sqlDigest, labelSQL, util.QueryStrForLog(normalizedSQL)))
	} else {
		ctx = pprof.WithLabels(context.Background(), pprof.Labels(labelSQLDigest, sqlDigest))
	}
	pprof.SetGoroutineLabels(ctx)
	GlobalSQLStatsProfiler.RegisterSQL(sqlDigest, normalizedSQL)
	return ctx
}

// SetGoroutineLabelsWithSQLAndPlan sets the SQL and plan digest label into the goroutine.
func SetGoroutineLabelsWithSQLAndPlan(ctx context.Context, sqlDigest, planDigest, normalizedPlan string) context.Context {
	ctx = pprof.WithLabels(ctx, pprof.Labels(labelSQLDigest, sqlDigest, labelPlanDigest, planDigest))
	pprof.SetGoroutineLabels(ctx)
	GlobalSQLStatsProfiler.RegisterPlan(planDigest, normalizedPlan)
	return ctx
}

func (sp *sqlStatsProfiler) startExportCPUProfile(w io.Writer) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.mu.ept != nil {
		return errors.New("cpu profiling already in use")
	}
	sp.mu.ept = &exportProfileTask{w: w}
	return nil
}

func (sp *sqlStatsProfiler) stopExportCPUProfile() error {
	sp.mu.Lock()
	ept := sp.mu.ept
	sp.mu.ept = nil
	sp.mu.Unlock()
	if ept.err != nil {
		return ept.err
	}
	if w := ept.w; w != nil && ept.cpuProfile != nil {
		sp.removeLabel(ept.cpuProfile)
		return ept.cpuProfile.Write(w)
	}
	return nil
}

func (sp *sqlStatsProfiler) removeLabel(p *profile.Profile) {
	if p == nil {
		return
	}
	keepLabelSQL := variable.EnablePProfSQLCPU.Load()
	for _, s := range p.Sample {
		for k := range s.Label {
			if keepLabelSQL && k == labelSQL {
				continue
			}
			delete(s.Label, k)
		}
	}
}

type exportProfileTask struct {
	cpuProfile *profile.Profile
	err        error
	w          io.Writer
}

func (t *exportProfileTask) mergeProfile(p *profile.Profile) {
	if t.err != nil || p == nil {
		return
	}
	ps := make([]*profile.Profile, 0, 2)
	if t.cpuProfile != nil {
		ps = append(ps, t.cpuProfile)
	}
	ps = append(ps, p)
	t.cpuProfile, t.err = profile.Merge(ps)
}

// ProfileHTTPHandler is same as pprof.Profile.
// The difference is ProfileHTTPHandler uses tracecpu.StartCPUProfile/StopCPUProfile to fetch profile data.
func ProfileHTTPHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	sec, err := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	if sec <= 0 || err != nil {
		sec = 30
	}

	if durationExceedsWriteTimeout(r, float64(sec)) {
		serveError(w, http.StatusBadRequest, "profile duration exceeds server's WriteTimeout")
		return
	}

	// Set Content Type assuming StartCPUProfile will work,
	// because if it does it starts writing.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="profile"`)

	err = StartCPUProfile(w)
	if err != nil {
		serveError(w, http.StatusInternalServerError, "Could not enable CPU profiling: "+err.Error())
		return
	}
	time.Sleep(time.Second * time.Duration(sec))
	err = StopCPUProfile()
	if err != nil {
		serveError(w, http.StatusInternalServerError, "Could not enable CPU profiling: "+err.Error())
		return
	}
}

func durationExceedsWriteTimeout(r *http.Request, seconds float64) bool {
	srv, ok := r.Context().Value(http.ServerContextKey).(*http.Server)
	return ok && srv.WriteTimeout != 0 && seconds >= srv.WriteTimeout.Seconds()
}

func serveError(w http.ResponseWriter, status int, txt string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.Header().Del("Content-Disposition")
	w.WriteHeader(status)
	_, err := fmt.Fprintln(w, txt)
	if err != nil {
		logutil.BgLogger().Info("write http response error", zap.Error(err))
	}
}
