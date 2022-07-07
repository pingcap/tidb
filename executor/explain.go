// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"strconv"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

// ExplainExec represents an explain executor.
type ExplainExec struct {
	baseExecutor

	explain     *core.Explain
	analyzeExec Executor
	executed    bool
	rows        [][]string
	cursor      int
}

// Open implements the Executor Open interface.
func (e *ExplainExec) Open(ctx context.Context) error {
	if e.analyzeExec != nil {
		return e.analyzeExec.Open(ctx)
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	e.rows = nil
	if e.analyzeExec != nil && !e.executed {
		// Open(), but Next() is not called.
		return e.analyzeExec.Close()
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *ExplainExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		var err error
		e.rows, err = e.generateExplainInfo(ctx)
		if err != nil {
			return err
		}
	}

	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := mathutil.Min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		for j := range e.rows[i] {
			req.AppendString(j, e.rows[i][j])
		}
	}
	e.cursor += numCurRows
	return nil
}

func (e *ExplainExec) executeAnalyzeExec(ctx context.Context) (err error) {
	if e.analyzeExec != nil && !e.executed {
		defer func() {
			err1 := e.analyzeExec.Close()
			if err1 != nil {
				if err != nil {
					err = errors.New(err.Error() + ", " + err1.Error())
				} else {
					err = err1
				}
			}
		}()
		if e.ctx.GetSessionVars().MemoryDebugMode > 0 {
			exit := make(chan bool)
			e.runMemoryDebugGoroutine(exit)
			defer func() {
				select {
				case <-exit:
				}
			}()
			defer func() { exit <- false }()
		}
		e.executed = true
		chk := newFirstChunk(e.analyzeExec)
		for {
			err = Next(ctx, e.analyzeExec, chk)
			if err != nil || chk.NumRows() == 0 {
				break
			}
		}
	}
	return err
}

func (e *ExplainExec) generateExplainInfo(ctx context.Context) (rows [][]string, err error) {
	if err = e.executeAnalyzeExec(ctx); err != nil {
		return nil, err
	}
	if err = e.explain.RenderResult(); err != nil {
		return nil, err
	}
	return e.explain.Rows, nil
}

// getAnalyzeExecToExecutedNoDelay gets the analyze DML executor to execute in handleNoDelay function.
// For explain analyze insert/update/delete statement, the analyze executor should be executed in handleNoDelay
// function and then commit transaction if needed.
// Otherwise, in autocommit transaction, the table record change of analyze executor(insert/update/delete...)
// will not be committed.
func (e *ExplainExec) getAnalyzeExecToExecutedNoDelay() Executor {
	if e.analyzeExec != nil && !e.executed && e.analyzeExec.Schema().Len() == 0 {
		e.executed = true
		return e.analyzeExec
	}
	return nil
}

func (e *ExplainExec) runMemoryDebugGoroutine(exit chan bool) {
	debugMode := e.ctx.GetSessionVars().MemoryDebugMode
	ticker := time.NewTicker(5 * time.Second)
	tracker := e.ctx.GetSessionVars().StmtCtx.MemTracker
	instanceStats := &runtime.MemStats{}
	var heapInUse, trackedMem uint64
	const GB = 1024 * 1024 * 1024
	go func() {
		defer func() {
			runtime.GC()
			runtime.ReadMemStats(instanceStats)
			heapInUse = instanceStats.HeapInuse
			trackedMem = uint64(tracker.BytesConsumed())
			logutil.BgLogger().Warn("Memory Debug Mode",
				zap.String("sql", "finished"),
				zap.String("heap in use", memory.FormatBytes(int64(heapInUse))),
				zap.String("tracked memory", memory.FormatBytes(int64(trackedMem))),
				zap.String("heap profile", e.getHeapProfile(false)))
			close(exit)
		}()
		times := 0
		for {
			select {
			case <-exit:
				return
			case <-ticker.C:
				if debugMode == 2 {
					runtime.GC()
				}
				runtime.ReadMemStats(instanceStats)
				heapInUse = instanceStats.HeapInuse
				trackedMem = uint64(tracker.BytesConsumed())

				times++
				if times%6 == 0 {
					logutil.BgLogger().Warn("Memory Debug Mode",
						zap.String("sql", "running"),
						zap.String("heap in use", memory.FormatBytes(int64(heapInUse))),
						zap.String("tracked memory", memory.FormatBytes(int64(trackedMem))))
				}

				if debugMode == 1 {
					if heapInUse > 10*GB && trackedMem/10*15 < heapInUse {
						logutil.BgLogger().Warn("Memory Debug Mode",
							zap.String("debug mode 1 alarm", "trackedMem * 150% < heapInUse, maybe some allocation and free frequently"),
							zap.String("heap in use", memory.FormatBytes(int64(heapInUse))),
							zap.String("tracked memory", memory.FormatBytes(int64(trackedMem))),
							zap.String("heap profile", e.getHeapProfile(true)))
					}
				} else {
					if heapInUse > 10*GB && trackedMem/10*11 < heapInUse {
						logutil.BgLogger().Warn("Memory Debug Mode",
							zap.String("debug mode 2 alarm", "trackedMem * 110% < heapInUse after GC, maybe some memory not be tracked"),
							zap.String("heap in use", memory.FormatBytes(int64(heapInUse))),
							zap.String("tracked memory", memory.FormatBytes(int64(trackedMem))),
							zap.String("heap profile", e.getHeapProfile(false)))
						ts := tracker.SearchTrackerConsumedMoreThanNBytes(GB)
						logs := make([]zap.Field, 0, len(ts))
						for _, t := range ts {
							logs = append(logs, zap.String("Executor_"+strconv.Itoa(t.Label()), memory.FormatBytes(t.BytesConsumed())))
						}
						logutil.BgLogger().Warn("Memory Debug Mode, Log all trackers that consumes more than 1GB", logs...)
					}
				}
			}
		}
	}()
}

func (e *ExplainExec) getHeapProfile(gc bool) (fileName string) {
	tempDir := filepath.Join(config.GetGlobalConfig().TempStoragePath, "record")
	timeString := time.Now().Format(time.RFC3339)
	if gc {
		runtime.GC()
	}
	fileName = filepath.Join(tempDir, "heapGC"+timeString)
	f, _ := os.Create(fileName)
	p := rpprof.Lookup("heap")
	_ = p.WriteTo(f, 0)
	_ = f.Close()
	return fileName
}
