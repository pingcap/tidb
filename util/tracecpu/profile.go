package tracecpu

import (
	"bytes"
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
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	LabelSQL        = "sql"
	LabelSQLDigest  = "sql_digest"
	LabelPlanDigest = "plan_digest"
)

var GlobalStmtProfiler = NewStmtProfiler()

type StmtProfiler struct {
	taskCh     chan *profileTask
	cacheBufCh chan *profileTask

	mu struct {
		sync.Mutex
		ept *exportProfileTask
	}
}

func NewStmtProfiler() *StmtProfiler {
	return &StmtProfiler{
		taskCh:     make(chan *profileTask, 128),
		cacheBufCh: make(chan *profileTask, 128),
	}
}

func (sp *StmtProfiler) Run() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	logutil.BgLogger().Info("cpu profiler started")
	go sp.startCPUProfileWorker()
	go sp.startAnalyzeProfileWorker()
}

func (sp *StmtProfiler) startCPUProfileWorker() {
	for {
		if sp.isEnabled() {
			sp.doCPUProfile()
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (sp *StmtProfiler) doCPUProfile() {
	interval := config.GetGlobalConfig().TopStmt.RefreshInterval
	task := sp.newProfileTask()
	if err := pprof.StartCPUProfile(task.buf); err != nil {
		return
	}
	ns := int(time.Second)*interval - time.Now().Nanosecond()
	time.Sleep(time.Nanosecond * time.Duration(ns))
	pprof.StopCPUProfile()
	sp.sendProfileTask(task)
}

func (sp *StmtProfiler) sendProfileTask(task *profileTask) {
	task.end = time.Now().Unix()
	sp.taskCh <- task
}

func (sp *StmtProfiler) startAnalyzeProfileWorker() {
	for {
		task := <-sp.taskCh
		reader := bytes.NewReader(task.buf.Bytes())
		p, err := profile.Parse(reader)
		if err != nil {
			logutil.BgLogger().Error("parse profile error", zap.Error(err))
			continue
		}
		sp.handleExportProfileTask(p)
		stmtMap := sp.parseCPUProfileTags(p)
		if len(stmtMap) == 0 {
			continue
		}
		total := int64(0)
		logutil.BgLogger().Info("-------- [ BEGIN ] ----------")
		for digest, stmt := range stmtMap {
			logutil.BgLogger().Info(fmt.Sprintf("%s , %v", stmt.normalizedSQL, digest))
			for p, v := range stmt.plans {
				logutil.BgLogger().Info(fmt.Sprintf("    %s : %s", time.Duration(v), p))
				total += v
			}
		}
		sp.putTaskToBuffer(task)
	}
}

type profileTask struct {
	buf *bytes.Buffer
	end int64
}

func (sp *StmtProfiler) newProfileTask() *profileTask {
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

func (sp *StmtProfiler) putTaskToBuffer(task *profileTask) {
	select {
	case sp.cacheBufCh <- task:
	default:
	}
}

func (sp *StmtProfiler) parseCPUProfileTags(p *profile.Profile) (stmtMap map[string]*stmtStats) {
	stmtMap = make(map[string]*stmtStats)
	idx := len(p.SampleType) - 1
	for _, s := range p.Sample {
		digests, ok := s.Label[LabelSQLDigest]
		if !ok || len(digests) == 0 {
			continue
		}
		sqls, ok := s.Label[LabelSQL]
		if !ok || len(sqls) != len(digests) {
			continue
		}
		for i, digest := range digests {
			stmt, ok := stmtMap[digest]
			if !ok {
				stmt = &stmtStats{
					plans:         make(map[string]int64),
					total:         0,
					isInternal:    false,
					normalizedSQL: sqls[i],
				}
				stmtMap[digest] = stmt
			}
			stmt.total += s.Value[idx]

			plans := s.Label[LabelPlanDigest]
			for _, plan := range plans {
				stmt.plans[plan] += s.Value[idx]
			}
		}
	}
	for _, stmt := range stmtMap {
		stmt.tune()
	}
	return stmtMap
}

type stmtStats struct {
	plans         map[string]int64
	total         int64
	isInternal    bool
	normalizedSQL string
}

// tune use to adjust stats
func (s *stmtStats) tune() {
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

func (sp *StmtProfiler) handleExportProfileTask(p *profile.Profile) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.mu.ept == nil {
		return
	}
	sp.mu.ept.mergeProfile(p)
}

func (sp *StmtProfiler) hasExportProfileTask() bool {
	sp.mu.Lock()
	has := sp.mu.ept != nil
	sp.mu.Unlock()
	return has
}

func (sp *StmtProfiler) isEnabled() bool {
	return config.GetGlobalConfig().TopStmt.Enable || sp.hasExportProfileTask()
}

func StartCPUProfile(w io.Writer) error {
	if GlobalStmtProfiler.isEnabled() {
		return GlobalStmtProfiler.startExportCPUProfile(w)
	}
	return pprof.StartCPUProfile(w)
}

func StopCPUProfile() error {
	if GlobalStmtProfiler.isEnabled() {
		return GlobalStmtProfiler.stopExportCPUProfile()
	}
	pprof.StopCPUProfile()
	return nil
}

func (sp *StmtProfiler) startExportCPUProfile(w io.Writer) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.mu.ept != nil {
		return errors.New("cpu profiling already in use")
	}
	sp.mu.ept = &exportProfileTask{w: w}
	return nil
}

func (sp *StmtProfiler) stopExportCPUProfile() error {
	sp.mu.Lock()
	ept := sp.mu.ept
	sp.mu.ept = nil
	sp.mu.Unlock()
	if ept.err != nil {
		return ept.err
	}
	if w := ept.w; w != nil {
		sp.removeLabel(ept.cpuProfile)
		return ept.cpuProfile.Write(w)
	}
	return nil
}

func (sp *StmtProfiler) removeLabel(p *profile.Profile) {
	if p == nil {
		return
	}
	keepLabelSQL := variable.EnablePProfSQLCPU.Load()
	for _, s := range p.Sample {
		for k := range s.Label {
			if keepLabelSQL && k == LabelSQL {
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
	if t.err != nil {
		return
	}
	if t.cpuProfile == nil {
		t.cpuProfile = p
	} else {
		t.cpuProfile, t.err = profile.Merge([]*profile.Profile{t.cpuProfile, p})
	}
}

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
	fmt.Fprintln(w, txt)
}
