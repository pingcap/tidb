package tracecpu

import (
	"bytes"
	"fmt"
	"runtime/pprof"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
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
		tagMap := sp.parseCPUProfileTags(p)
		if len(tagMap) == 0 {
			continue
		}
		logutil.BgLogger().Info("-------- [ BEGIN ] ----------")
		for k, tags := range tagMap {
			if k != "sql" {
				continue
			}
			for t, v := range tags {
				fmt.Printf("%s : %s\n", time.Duration(v), t)
			}
		}
		fmt.Printf("\n\n")
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

func (sp *StmtProfiler) parseCPUProfileTags(p *profile.Profile) map[string]map[string]int64 {
	tagMap := make(map[string]map[string]int64)
	idx := len(p.SampleType) - 1
	for _, s := range p.Sample {
		for key, vals := range s.Label {
			for _, val := range vals {
				valueMap, ok := tagMap[key]
				if !ok {
					valueMap = make(map[string]int64)
					tagMap[key] = valueMap
				}
				valueMap[val] += s.Value[idx]
			}
		}
	}
	return tagMap
}
