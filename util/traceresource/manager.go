package traceresource

import (
	"bytes"
	"fmt"
	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"runtime/pprof"
	"time"
)

type StmtProfiler struct {
	taskCh     chan *bytes.Buffer
	cacheBufCh chan *bytes.Buffer
}

func NewStmtProfiler() *StmtProfiler {
	return &StmtProfiler{
		taskCh:     make(chan *bytes.Buffer, 128),
		cacheBufCh: make(chan *bytes.Buffer, 128),
	}
}

func (sp *StmtProfiler) Run() {
	logutil.BgLogger().Info("profiler started")
	go sp.startCPUProfileWorker()
	go sp.startAnalyzeProfileWorker()
}

func (sp *StmtProfiler) startCPUProfileWorker() {
	for {
		buf := sp.getBuffer()
		if err := pprof.StartCPUProfile(buf); err != nil {
			return
		}
		sp.sleep(time.Second * 10)
		pprof.StopCPUProfile()
		sp.taskCh <- buf
	}
}

func (sp *StmtProfiler) startAnalyzeProfileWorker() {
	var buf *bytes.Buffer
	for {
		buf = <-sp.taskCh
		reader := bytes.NewReader(buf.Bytes())
		p, err := profile.Parse(reader)
		if err != nil {
			logutil.BgLogger().Error("parse profile error", zap.Error(err))
			continue
		}
		tagMap := sp.parseCPUProfileTags(p)
		if len(tagMap) == 0 {
			continue
		}
		fmt.Printf("-------- [ BEGIN ] ----------\n")
		for k, tags := range tagMap {
			fmt.Println(k)
			for t, v := range tags {
				fmt.Printf("%s : %s, %s\n", time.Duration(v), k, t)
			}
		}
		fmt.Printf("-------- [ END ] ----------\n\n")
		sp.putBuffer(buf)
	}
}

func (sp *StmtProfiler) getBuffer() *bytes.Buffer {
	select {
	case buf := <-sp.cacheBufCh:
		buf.Reset()
		return buf
	default:
		return bytes.NewBuffer(make([]byte, 0, 100*1024))
	}
}

func (sp *StmtProfiler) putBuffer(buf *bytes.Buffer) {
	select {
	case sp.cacheBufCh <- buf:
	default:
	}
}

func (sp *StmtProfiler) sleep(d time.Duration) {
	timer := time.NewTimer(d)
	select {
	case <-timer.C:
	}
	timer.Stop()
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
