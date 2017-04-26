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
	ctx   context.Context
	tasks []analyzeTask
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
	for _, task := range e.tasks {
		err := task.src.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next() (*Row, error) {
	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	taskCh := make(chan analyzeTask, len(e.tasks))
	resultCh := make(chan analyzeResult, len(e.tasks))
	for i := 0; i < concurrency; i++ {
		go analyzeWorker(taskCh, resultCh)
	}
	for _, task := range e.tasks {
		taskCh <- task
	}
	close(taskCh)
	results := make([]analyzeResult, 0, len(e.tasks))
	for i := 0; i < len(e.tasks); i++ {
		result := <-resultCh
		results = append(results, result)
		if result.err != nil {
			return nil, errors.Trace(err)
		}
	}
	for _, result := range results {
		for _, hg := range result.hist {
			err = hg.SaveToStorage(e.ctx, result.tableID, result.count, result.isIndex)
			if err != nil {
				return nil, errors.Trace(err)
			}
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
	colTask
	idxTask
)

type analyzeTask struct {
	taskType taskType
	src      Executor
}

type analyzeResult struct {
	tableID int64
	hist    []*statistics.Histogram
	count   int64
	isIndex int
	err     error
}

func analyzeWorker(taskCh <-chan analyzeTask, resultCh chan<- analyzeResult) {
	for task := range taskCh {
		switch task.taskType {
		case pkTask:
			resultCh <- analyzePK(task.src.(*XSelectTableExec))
		case colTask:
			resultCh <- analyzeColumns(task.src.(*XSelectTableExec))
		case idxTask:
			resultCh <- analyzeIndex(task.src.(*XSelectIndexExec))
		}
	}
}

func analyzePK(exec *XSelectTableExec) analyzeResult {
	count, hg, err := statistics.BuildPK(exec.ctx, defaultBucketCount, exec.Columns[0].ID, &recordSet{executor: exec})
	return analyzeResult{tableID: exec.tableInfo.ID, hist: []*statistics.Histogram{hg}, count: count, isIndex: 0, err: err}
}

func analyzeColumns(exec *XSelectTableExec) analyzeResult {
	count, sampleRows, colNDVs, err := CollectSamplesAndEstimateNDVs(&recordSet{executor: exec}, len(exec.Columns))
	if err != nil {
		return analyzeResult{err: err}
	}
	columnSamples := rowsToColumnSamples(sampleRows)
	if columnSamples == nil {
		columnSamples = make([][]types.Datum, len(exec.Columns))
	}
	result := analyzeResult{tableID: exec.tableInfo.ID, count: count, isIndex: 0}
	for i, col := range exec.Columns {
		hg, err := statistics.BuildColumn(exec.ctx, defaultBucketCount, col.ID, colNDVs[i], count, columnSamples[i])
		result.hist = append(result.hist, hg)
		if err != nil && result.err == nil {
			result.err = err
		}
	}
	return result
}

func analyzeIndex(exec *XSelectIndexExec) analyzeResult {
	count, hg, err := statistics.BuildIndex(exec.ctx, defaultBucketCount, exec.index.ID, &recordSet{executor: exec})
	return analyzeResult{tableID: exec.tableInfo.ID, hist: []*statistics.Histogram{hg}, count: count, isIndex: 1, err: err}
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
