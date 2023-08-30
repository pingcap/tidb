// Copyright 2023 PingCAP, Inc.
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

package aggregate

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/executor/internal/exec"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

// getPartialResultBatch fetches a batch of partial results from HashAggIntermData.
func (d *HashAggIntermData) getPartialResultBatch(_ *stmtctx.StatementContext, prs [][]aggfuncs.PartialResult, _ []aggfuncs.AggFunc, maxChunkSize int) (_ [][]aggfuncs.PartialResult, groupKeys []string, reachEnd bool) {
	keyStart := d.cursor
	for ; d.cursor < len(d.groupKeys) && len(prs) < maxChunkSize; d.cursor++ {
		prs = append(prs, d.partialResultMap[d.groupKeys[d.cursor]])
	}
	if d.cursor == len(d.groupKeys) {
		reachEnd = true
	}
	return prs, d.groupKeys[keyStart:d.cursor], reachEnd
}

func closeBaseExecutor(b *exec.BaseExecutor) {
	if r := recover(); r != nil {
		// Release the resource, but throw the panic again and let the top level handle it.
		terror.Log(b.Close())
		logutil.BgLogger().Warn("panic in Open(), close base executor and throw exception again")
		panic(r)
	}
}

func recoveryHashAgg(output chan *AfFinalResult, r interface{}) {
	err := errors.Errorf("%v", r)
	output <- &AfFinalResult{err: errors.Errorf("%v", r)}
	logutil.BgLogger().Error("parallel hash aggregation panicked", zap.Error(err), zap.Stack("stack"))
}

func getGroupKeyMemUsage(groupKey [][]byte) int64 {
	mem := int64(0)
	for _, key := range groupKey {
		mem += int64(cap(key))
	}
	mem += aggfuncs.DefSliceSize * int64(cap(groupKey))
	return mem
}

// GetGroupKey evaluates the group items and args of aggregate functions.
func GetGroupKey(ctx sessionctx.Context, input *chunk.Chunk, groupKey [][]byte, groupByItems []expression.Expression) ([][]byte, error) {
	numRows := input.NumRows()
	avlGroupKeyLen := mathutil.Min(len(groupKey), numRows)
	for i := 0; i < avlGroupKeyLen; i++ {
		groupKey[i] = groupKey[i][:0]
	}
	for i := avlGroupKeyLen; i < numRows; i++ {
		groupKey = append(groupKey, make([]byte, 0, 10*len(groupByItems)))
	}

	for _, item := range groupByItems {
		tp := item.GetType()

		buf, err := expression.GetColumn(tp.EvalType(), numRows)
		if err != nil {
			return nil, err
		}

		// In strict sql mode like ‘STRICT_TRANS_TABLES’，can not insert an invalid enum value like 0.
		// While in sql mode like '', can insert an invalid enum value like 0,
		// then the enum value 0 will have the enum name '', which maybe conflict with user defined enum ''.
		// Ref to issue #26885.
		// This check is used to handle invalid enum name same with user defined enum name.
		// Use enum value as groupKey instead of enum name.
		if item.GetType().GetType() == mysql.TypeEnum {
			newTp := *tp
			newTp.AddFlag(mysql.EnumSetAsIntFlag)
			tp = &newTp
		}

		if err := expression.EvalExpr(ctx, item, tp.EvalType(), input, buf); err != nil {
			expression.PutColumn(buf)
			return nil, err
		}
		// This check is used to avoid error during the execution of `EncodeDecimal`.
		if item.GetType().GetType() == mysql.TypeNewDecimal {
			newTp := *tp
			newTp.SetFlen(0)
			tp = &newTp
		}

		groupKey, err = codec.HashGroupKey(ctx.GetSessionVars().StmtCtx, input.NumRows(), buf, groupKey, tp)
		if err != nil {
			expression.PutColumn(buf)
			return nil, err
		}
		expression.PutColumn(buf)
	}
	return groupKey, nil
}

// HashAggRuntimeStats record the HashAggExec runtime stat
type HashAggRuntimeStats struct {
	PartialConcurrency int
	PartialWallTime    int64
	FinalConcurrency   int
	FinalWallTime      int64
	PartialStats       []*AggWorkerStat
	FinalStats         []*AggWorkerStat
}

func (*HashAggRuntimeStats) workerString(buf *bytes.Buffer, prefix string, concurrency int, wallTime int64, workerStats []*AggWorkerStat) {
	var totalTime, totalWait, totalExec, totalTaskNum int64
	for _, w := range workerStats {
		totalTime += w.WorkerTime
		totalWait += w.WaitTime
		totalExec += w.ExecTime
		totalTaskNum += w.TaskNum
	}
	buf.WriteString(prefix)
	fmt.Fprintf(buf, "_worker:{wall_time:%s, concurrency:%d, task_num:%d, tot_wait:%s, tot_exec:%s, tot_time:%s",
		time.Duration(wallTime), concurrency, totalTaskNum, time.Duration(totalWait), time.Duration(totalExec), time.Duration(totalTime))
	n := len(workerStats)
	if n > 0 {
		slices.SortFunc(workerStats, func(i, j *AggWorkerStat) int { return cmp.Compare(i.WorkerTime, j.WorkerTime) })
		fmt.Fprintf(buf, ", max:%v, p95:%v",
			time.Duration(workerStats[n-1].WorkerTime), time.Duration(workerStats[n*19/20].WorkerTime))
	}
	buf.WriteString("}")
}

// String implements the RuntimeStats interface.
func (e *HashAggRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	e.workerString(buf, "partial", e.PartialConcurrency, atomic.LoadInt64(&e.PartialWallTime), e.PartialStats)
	buf.WriteString(", ")
	e.workerString(buf, "final", e.FinalConcurrency, atomic.LoadInt64(&e.FinalWallTime), e.FinalStats)
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *HashAggRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := &HashAggRuntimeStats{
		PartialConcurrency: e.PartialConcurrency,
		PartialWallTime:    atomic.LoadInt64(&e.PartialWallTime),
		FinalConcurrency:   e.FinalConcurrency,
		FinalWallTime:      atomic.LoadInt64(&e.FinalWallTime),
		PartialStats:       make([]*AggWorkerStat, 0, e.PartialConcurrency),
		FinalStats:         make([]*AggWorkerStat, 0, e.FinalConcurrency),
	}
	for _, s := range e.PartialStats {
		newRs.PartialStats = append(newRs.PartialStats, s.Clone())
	}
	for _, s := range e.FinalStats {
		newRs.FinalStats = append(newRs.FinalStats, s.Clone())
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *HashAggRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*HashAggRuntimeStats)
	if !ok {
		return
	}
	atomic.AddInt64(&e.PartialWallTime, atomic.LoadInt64(&tmp.PartialWallTime))
	atomic.AddInt64(&e.FinalWallTime, atomic.LoadInt64(&tmp.FinalWallTime))
	e.PartialStats = append(e.PartialStats, tmp.PartialStats...)
	e.FinalStats = append(e.FinalStats, tmp.FinalStats...)
}

// Tp implements the RuntimeStats interface.
func (*HashAggRuntimeStats) Tp() int {
	return execdetails.TpHashAggRuntimeStat
}

// AggWorkerInfo contains the agg worker information.
type AggWorkerInfo struct {
	Concurrency int
	WallTime    int64
}

// AggWorkerStat record the AggWorker runtime stat
type AggWorkerStat struct {
	TaskNum    int64
	WaitTime   int64
	ExecTime   int64
	WorkerTime int64
}

// Clone implements the RuntimeStats interface.
func (w *AggWorkerStat) Clone() *AggWorkerStat {
	return &AggWorkerStat{
		TaskNum:    w.TaskNum,
		WaitTime:   w.WaitTime,
		ExecTime:   w.ExecTime,
		WorkerTime: w.WorkerTime,
	}
}

// ActionSpill returns a AggSpillDiskAction for spilling intermediate data for hashAgg.
func (e *HashAggExec) ActionSpill() *AggSpillDiskAction {
	if e.spillAction == nil {
		e.spillAction = &AggSpillDiskAction{
			e: e,
		}
	}
	return e.spillAction
}
