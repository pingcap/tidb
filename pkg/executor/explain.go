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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
	clientutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// ExplainExec represents an explain executor.
type ExplainExec struct {
	exec.BaseExecutor

	explain     *core.Explain
	analyzeExec exec.Executor
	executed    bool
	rows        [][]string
	cursor      int
}

// Open implements the Executor Open interface.
func (e *ExplainExec) Open(ctx context.Context) error {
	if e.explain.Analyze && e.analyzeExec != nil {
		return exec.Open(ctx, e.analyzeExec)
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	e.rows = nil
	if e.explain.Analyze && e.analyzeExec != nil && !e.executed {
		// Open(), but Next() is not called.
		return exec.Close(e.analyzeExec)
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

	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		for j := range e.rows[i] {
			req.AppendString(j, e.rows[i][j])
		}
	}
	e.cursor += numCurRows
	return nil
}

func (e *ExplainExec) executeAnalyzeExec(ctx context.Context) (err error) {
	if e.explain.Analyze && e.analyzeExec != nil && !e.executed {
		defer func() {
			err1 := exec.Close(e.analyzeExec)
			if err1 != nil {
				if err != nil {
					err = errors.New(err.Error() + ", " + err1.Error())
				} else {
					err = err1
				}
			}
		}()
		if minHeapInUse, alarmRatio := e.Ctx().GetSessionVars().MemoryDebugModeMinHeapInUse, e.Ctx().GetSessionVars().MemoryDebugModeAlarmRatio; minHeapInUse != 0 && alarmRatio != 0 {
			memoryDebugModeCtx, cancel := context.WithCancel(ctx)
			waitGroup := sync.WaitGroup{}
			waitGroup.Add(1)
			defer func() {
				// Notify and wait debug goroutine exit.
				cancel()
				waitGroup.Wait()
			}()
			go (&memoryDebugModeHandler{
				ctx:          memoryDebugModeCtx,
				minHeapInUse: mathutil.Abs(minHeapInUse),
				alarmRatio:   alarmRatio,
				autoGC:       minHeapInUse > 0,
				memTracker:   e.Ctx().GetSessionVars().MemTracker,
				wg:           &waitGroup,
			}).run()
		}
		e.executed = true
		chk := exec.TryNewCacheChunk(e.analyzeExec)
		for {
			err = exec.Next(ctx, e.analyzeExec, chk)
			if err != nil || chk.NumRows() == 0 {
				break
			}
		}
	}
	// Register the RU runtime stats to the runtime stats collection after the analyze executor has been executed.
	if e.explain.Analyze && e.analyzeExec != nil && e.executed {
		ruDetailsRaw := ctx.Value(clientutil.RUDetailsCtxKey)
		if coll := e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl; coll != nil && ruDetailsRaw != nil {
			ruDetails := ruDetailsRaw.(*clientutil.RUDetails)
			coll.RegisterStats(e.explain.TargetPlan.ID(), &ruRuntimeStats{ruDetails})
		}
	}
	return err
}

func (e *ExplainExec) generateExplainInfo(ctx context.Context) (rows [][]string, err error) {
	if e.explain.Analyze {
		if err = e.executeAnalyzeExec(ctx); err != nil {
			return nil, err
		}
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
func (e *ExplainExec) getAnalyzeExecToExecutedNoDelay() exec.Executor {
	if e.explain.Analyze && e.analyzeExec != nil && !e.executed && e.analyzeExec.Schema().Len() == 0 {
		e.executed = true
		return e.analyzeExec
	}
	return nil
}

type memoryDebugModeHandler struct {
	ctx          context.Context
	minHeapInUse int64
	alarmRatio   int64
	autoGC       bool
	wg           *sync.WaitGroup
	memTracker   *memory.Tracker

	infoField []zap.Field
}

func (h *memoryDebugModeHandler) fetchCurrentMemoryUsage(gc bool) (heapInUse, trackedMem uint64) {
	if gc {
		runtime.GC() //nolint: revive
	}
	instanceStats := memory.ForceReadMemStats()
	heapInUse = instanceStats.HeapInuse
	trackedMem = uint64(h.memTracker.BytesConsumed())
	return
}

func (h *memoryDebugModeHandler) genInfo(status string, needProfile bool, heapInUse, trackedMem int64) (fields []zap.Field, err error) {
	var fileName string
	h.infoField = h.infoField[:0]
	h.infoField = append(h.infoField, zap.String("sql", status))
	h.infoField = append(h.infoField, zap.String("heap in use", memory.FormatBytes(heapInUse)))
	h.infoField = append(h.infoField, zap.String("tracked memory", memory.FormatBytes(trackedMem)))
	if needProfile {
		fileName, err = getHeapProfile()
		h.infoField = append(h.infoField, zap.String("heap profile", fileName))
	}
	return h.infoField, err
}

func (h *memoryDebugModeHandler) getTrackerTreeMemUseLogs() []zap.Field {
	trackerMemUseMap := h.memTracker.CountAllChildrenMemUse()
	logs := make([]zap.Field, 0, len(trackerMemUseMap))
	keys := make([]string, 0, len(trackerMemUseMap))
	for k := range trackerMemUseMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		logs = append(logs, zap.String("TrackerTree "+k, memory.FormatBytes(trackerMemUseMap[k])))
	}
	return logs
}

func updateTriggerIntervalByHeapInUse(heapInUse uint64) (time.Duration, int) {
	if heapInUse < 30*size.GB {
		return 5 * time.Second, 6
	} else if heapInUse < 40*size.GB {
		return 15 * time.Second, 2
	}
	return 30 * time.Second, 1
}

func (h *memoryDebugModeHandler) run() {
	var err error
	var fields []zap.Field
	defer func() {
		heapInUse, trackedMem := h.fetchCurrentMemoryUsage(true)
		if err == nil {
			fields, err := h.genInfo("finished", true, int64(heapInUse), int64(trackedMem))
			logutil.BgLogger().Info("Memory Debug Mode", fields...)
			if err != nil {
				logutil.BgLogger().Error("Memory Debug Mode Exit", zap.Error(err))
			}
		} else {
			fields, err := h.genInfo("debug_mode_error", false, int64(heapInUse), int64(trackedMem))
			logutil.BgLogger().Error("Memory Debug Mode", fields...)
			logutil.BgLogger().Error("Memory Debug Mode Exit", zap.Error(err))
		}
		h.wg.Done()
	}()

	logutil.BgLogger().Info("Memory Debug Mode",
		zap.String("sql", "started"),
		zap.Bool("autoGC", h.autoGC),
		zap.String("minHeapInUse", memory.FormatBytes(h.minHeapInUse)),
		zap.Int64("alarmRatio", h.alarmRatio),
	)
	triggerInterval := 5 * time.Second
	printMod := 6
	ticker, loop := time.NewTicker(triggerInterval), 0
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			heapInUse, trackedMem := h.fetchCurrentMemoryUsage(h.autoGC)
			loop++
			if loop%printMod == 0 {
				fields, err = h.genInfo("running", false, int64(heapInUse), int64(trackedMem))
				logutil.BgLogger().Info("Memory Debug Mode", fields...)
				if err != nil {
					return
				}
			}
			triggerInterval, printMod = updateTriggerIntervalByHeapInUse(heapInUse)
			ticker.Reset(triggerInterval)

			if !h.autoGC {
				if heapInUse > uint64(h.minHeapInUse) && trackedMem/100*uint64(100+h.alarmRatio) < heapInUse {
					fields, err = h.genInfo("warning", true, int64(heapInUse), int64(trackedMem))
					logutil.BgLogger().Warn("Memory Debug Mode", fields...)
					if err != nil {
						return
					}
				}
			} else {
				if heapInUse > uint64(h.minHeapInUse) && trackedMem/100*uint64(100+h.alarmRatio) < heapInUse {
					fields, err = h.genInfo("warning", true, int64(heapInUse), int64(trackedMem))
					logutil.BgLogger().Warn("Memory Debug Mode", fields...)
					if err != nil {
						return
					}
					ts := h.memTracker.SearchTrackerConsumedMoreThanNBytes(h.minHeapInUse / 5)
					logs := make([]zap.Field, 0, len(ts))
					for _, t := range ts {
						logs = append(logs, zap.String("Executor_"+strconv.Itoa(t.Label()), memory.FormatBytes(t.BytesConsumed())))
					}
					logutil.BgLogger().Warn("Memory Debug Mode, Log all executors that consumes more than threshold * 20%", logs...)
					logutil.BgLogger().Warn("Memory Debug Mode, Log the tracker tree", h.getTrackerTreeMemUseLogs()...)
				}
			}
		}
	}
}

func getHeapProfile() (fileName string, err error) {
	tempDir := filepath.Join(config.GetGlobalConfig().TempStoragePath, "record")
	timeString := time.Now().Format(time.RFC3339)
	fileName = filepath.Join(tempDir, "heapGC"+timeString)
	f, err := os.Create(fileName)
	if err != nil {
		return "", err
	}
	p := rpprof.Lookup("heap")
	err = p.WriteTo(f, 0)
	if err != nil {
		return "", err
	}
	err = f.Close()
	if err != nil {
		return "", err
	}
	return fileName, nil
}

// ruRuntimeStats is a wrapper of clientutil.RUDetails,
// which implements the RuntimeStats interface.
type ruRuntimeStats struct {
	*clientutil.RUDetails
}

// String implements the RuntimeStats interface.
func (e *ruRuntimeStats) String() string {
	if e.RUDetails != nil {
		return fmt.Sprintf("RU:%f", e.RRU()+e.WRU())
	}
	return ""
}

// Clone implements the RuntimeStats interface.
func (e *ruRuntimeStats) Clone() execdetails.RuntimeStats {
	return &ruRuntimeStats{RUDetails: e.RUDetails.Clone()}
}

// Merge implements the RuntimeStats interface.
func (e *ruRuntimeStats) Merge(other execdetails.RuntimeStats) {
	if tmp, ok := other.(*ruRuntimeStats); ok {
		if e.RUDetails != nil {
			e.RUDetails.Merge(tmp.RUDetails)
		} else {
			e.RUDetails = tmp.RUDetails.Clone()
		}
	}
}

// Tp implements the RuntimeStats interface.
func (*ruRuntimeStats) Tp() int {
	return execdetails.TpRURuntimeStats
}
