// Copyright 2017 PingCAP, Inc.
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

package executor

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	ctx      context.Context
	pkOffset int // The first pkOffset executors is for pk.
	Srcs     []Executor
}

const (
	maxSampleCount     = 10000
	maxSketchSize      = 1000
	defaultBucketCount = 256
)

// Schema implements the Executor Schema interface.
func (e *AnalyzeExec) Schema() *expression.Schema {
	return expression.NewSchema()
}

// Close implements the Executor Close interface.
func (e *AnalyzeExec) Close() error {
	for _, src := range e.Srcs {
		err := src.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next() (*Row, error) {
	var tasks []*analyzeTask
	for i, src := range e.Srcs {
		switch v := src.(type) {
		case *XSelectTableExec:
			if i < e.pkOffset {
				tasks = append(tasks, e.buildAnalyzePKTask(v))
			} else {
				t, err := e.buildAnalyzeColumnsTask(v)
				if err != nil {
					return nil, errors.Trace(err)
				}
				tasks = append(tasks, t...)
			}
		case *XSelectIndexExec:
			tasks = append(tasks, e.buildAnalyzeIndexTask(v))
		}
	}
	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	taskCh := make(chan *analyzeTask, len(tasks))
	resultCh := make(chan *analyzeResult, len(tasks))
	for i := 0; i < concurrency; i++ {
		go analyzeWorker(taskCh, resultCh)
	}
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	for i := 0; i < len(tasks); i++ {
		result := <-resultCh
		if result.err != nil {
			return nil, errors.Trace(err)
		}
		err = result.hist.SaveToStorage(e.ctx, result.tableID, result.count, result.isIndex)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	dom := sessionctx.GetDomain(e.ctx)
	lease := dom.DDL().GetLease()
	if lease > 0 {
		// We sleep two lease to make sure other tidb node has updated this node.
		time.Sleep(lease * 2)
	} else {
		err := dom.StatsHandle().Update(GetInfoSchema(e.ctx))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return nil, nil
}

func getBuildStatsConcurrency(ctx context.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := varsutil.GetSessionSystemVar(sessionVars, variable.TiDBBuildStatsConcurrency)
	if err != nil {
		return 0, errors.Trace(err)
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), errors.Trace(err)
}

type taskType int

const (
	pkTask taskType = iota
	idxTask
	colTask
)

type analyzeTask struct {
	statsBuilder *statistics.Builder
	taskType     taskType
	id           int64
	record       ast.RecordSet
	ndv          int64
	count        int64
	samples      []types.Datum
}

type analyzeResult struct {
	tableID int64
	hist    *statistics.Histogram
	count   int64
	isIndex int
	err     error
}

func analyzeWorker(taskCh <-chan *analyzeTask, resultCh chan<- *analyzeResult) {
	for task := range taskCh {
		switch task.taskType {
		case pkTask:
			count, hg, err := task.statsBuilder.BuildIndex(task.id, task.record, 0)
			resultCh <- &analyzeResult{tableID: task.statsBuilder.TblInfo.ID, hist: hg, count: count, isIndex: 0, err: err}
		case idxTask:
			count, hg, err := task.statsBuilder.BuildIndex(task.id, task.record, 1)
			resultCh <- &analyzeResult{tableID: task.statsBuilder.TblInfo.ID, hist: hg, count: count, isIndex: 1, err: err}
		case colTask:
			hg, err := task.statsBuilder.BuildColumn(task.id, task.ndv, task.count, task.samples)
			resultCh <- &analyzeResult{tableID: task.statsBuilder.TblInfo.ID, hist: hg, count: task.count, isIndex: 0, err: err}
		}
	}
}

func (e *AnalyzeExec) buildAnalyzePKTask(exec *XSelectTableExec) *analyzeTask {
	b := &statistics.Builder{
		Ctx:        exec.ctx,
		TblInfo:    exec.tableInfo,
		NumBuckets: defaultBucketCount,
	}
	return &analyzeTask{
		statsBuilder: b,
		taskType:     pkTask,
		id:           exec.Columns[0].ID,
		record:       &recordSet{executor: exec},
	}
}

func (e *AnalyzeExec) buildAnalyzeColumnsTask(exec *XSelectTableExec) ([]*analyzeTask, error) {
	count, sampleRows, colNDVs, err := CollectSamplesAndEstimateNDVs(&recordSet{executor: exec}, len(exec.Columns))
	if err != nil {
		return nil, errors.Trace(err)
	}
	b := &statistics.Builder{
		Ctx:        exec.ctx,
		TblInfo:    exec.tableInfo,
		NumBuckets: defaultBucketCount,
	}
	columnSamples := rowsToColumnSamples(sampleRows)
	if columnSamples == nil {
		columnSamples = make([][]types.Datum, len(exec.Columns))
	}
	var tasks []*analyzeTask
	for i, col := range exec.Columns {
		t := &analyzeTask{
			statsBuilder: b,
			taskType:     colTask,
			id:           col.ID,
			ndv:          colNDVs[i],
			count:        count,
			samples:      columnSamples[i],
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (e *AnalyzeExec) buildAnalyzeIndexTask(exec *XSelectIndexExec) *analyzeTask {
	b := &statistics.Builder{
		Ctx:        exec.ctx,
		TblInfo:    exec.tableInfo,
		NumBuckets: defaultBucketCount,
	}
	return &analyzeTask{
		statsBuilder: b,
		taskType:     idxTask,
		id:           exec.indexPlan.Index.ID,
		record:       &recordSet{executor: exec},
	}
}

// CollectSamplesAndEstimateNDVs collects sample from the result set using Reservoir Sampling algorithm,
// and estimates NDVs using FM Sketch during the collecting process.
// See https://en.wikipedia.org/wiki/Reservoir_sampling
// Exported for test.
func CollectSamplesAndEstimateNDVs(e ast.RecordSet, numCols int) (count int64, samples []*ast.Row, ndvs []int64, err error) {
	var sketches []*statistics.FMSketch
	for i := 0; i < numCols; i++ {
		sketches = append(sketches, statistics.NewFMSketch(maxSketchSize))
	}
	for {
		row, err := e.Next()
		if err != nil {
			return count, samples, ndvs, errors.Trace(err)
		}
		if row == nil {
			break
		}
		for i, val := range row.Data {
			err = sketches[i].InsertValue(val)
			if err != nil {
				return count, samples, ndvs, errors.Trace(err)
			}
		}
		if len(samples) < maxSampleCount {
			samples = append(samples, row)
		} else {
			shouldAdd := rand.Int63n(count) < maxSampleCount
			if shouldAdd {
				idx := rand.Intn(maxSampleCount)
				samples[idx] = row
			}
		}
		count++
	}
	for _, sketch := range sketches {
		ndvs = append(ndvs, sketch.NDV())
	}
	return count, samples, ndvs, nil
}

func rowsToColumnSamples(rows []*ast.Row) [][]types.Datum {
	if len(rows) == 0 {
		return nil
	}
	columnSamples := make([][]types.Datum, len(rows[0].Data))
	for i := range columnSamples {
		columnSamples[i] = make([]types.Datum, len(rows))
	}
	for j, row := range rows {
		for i, val := range row.Data {
			columnSamples[i][j] = val
		}
	}
	return columnSamples
}
