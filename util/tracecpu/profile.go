package tracecpu

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	LabelSQL        = "sql"
	LabelSQLDigest  = "sql_digest"
	LabelPlanDigest = "plan_digest"
)

type StmtProfiler struct {
	taskCh     chan *profileTask
	cacheBufCh chan *profileTask
}

type profileTask struct {
	buf *bytes.Buffer
	end int64
}

func NewStmtProfiler() *StmtProfiler {
	return &StmtProfiler{
		taskCh:     make(chan *profileTask, 128),
		cacheBufCh: make(chan *profileTask, 128),
	}
}

func (sp *StmtProfiler) Run() {
	logutil.BgLogger().Info("profiler started")
	go sp.startCPUProfileWorker()
	go sp.startAnalyzeProfileWorker()
}

func (sp *StmtProfiler) startCPUProfileWorker() {
	for {
		cfg := config.GetGlobalConfig()
		if cfg.TopStmt.Enable {
			sp.doCPUProfile(cfg.TopStmt.RefreshInterval)
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (sp *StmtProfiler) doCPUProfile(interval int) {
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
		if config.GetGlobalConfig().TopStmt.Debug && total > (500*int64(time.Millisecond)) {
			ioutil.WriteFile("cpu.profile."+strconv.Itoa(int(task.end)), task.buf.Bytes(), 0644)
		}
		sp.putTaskToBuffer(task)
	}
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
