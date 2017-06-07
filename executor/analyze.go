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
	"github.com/pingcap/tidb/model"
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
	tasks []*analyzeTask
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

// Open implements the Executor Open interface.
func (e *AnalyzeExec) Open() error {
	for _, task := range e.tasks {
		err := task.src.Open()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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
	taskCh := make(chan *analyzeTask, len(e.tasks))
	resultCh := make(chan analyzeResult, len(e.tasks))
	for i := 0; i < concurrency; i++ {
		go e.analyzeWorker(taskCh, resultCh)
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
	lease := dom.StatsHandle().Lease
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
	taskType  taskType
	tableInfo *model.TableInfo
	indexInfo *model.IndexInfo
	Columns   []*model.ColumnInfo
	src       Executor
}

type analyzeResult struct {
	tableID int64
	hist    []*statistics.Histogram
	count   int64
	isIndex int
	err     error
}

func (e *AnalyzeExec) analyzeWorker(taskCh <-chan *analyzeTask, resultCh chan<- analyzeResult) {
	for task := range taskCh {
		switch task.taskType {
		case pkTask:
			resultCh <- e.analyzePK(task)
		case colTask:
			resultCh <- e.analyzeColumns(task)
		case idxTask:
			resultCh <- e.analyzeIndex(task)
		}
	}
}

func (e *AnalyzeExec) analyzePK(task *analyzeTask) analyzeResult {
	count, hg, err := statistics.BuildPK(e.ctx, defaultBucketCount, task.Columns[0].ID, &recordSet{executor: task.src})
	return analyzeResult{tableID: task.tableInfo.ID, hist: []*statistics.Histogram{hg}, count: count, isIndex: 0, err: err}
}

func (e *AnalyzeExec) analyzeColumns(task *analyzeTask) analyzeResult {
	collectors, err := CollectSamplesAndEstimateNDVs(&recordSet{executor: task.src}, len(task.Columns))
	if err != nil {
		return analyzeResult{err: err}
	}
	result := analyzeResult{tableID: task.tableInfo.ID, count: collectors[0].Count + collectors[0].NullCount, isIndex: 0}
	for i, col := range task.Columns {
		hg, err := statistics.BuildColumn(e.ctx, defaultBucketCount, col.ID, collectors[i].Sketch.NDV(), collectors[i].Count, collectors[i].NullCount, collectors[i].samples)
		result.hist = append(result.hist, hg)
		if err != nil && result.err == nil {
			result.err = err
		}
	}
	return result
}

func (e *AnalyzeExec) analyzeIndex(task *analyzeTask) analyzeResult {
	count, hg, err := statistics.BuildIndex(e.ctx, defaultBucketCount, task.indexInfo.ID, &recordSet{executor: task.src})
	return analyzeResult{tableID: task.tableInfo.ID, hist: []*statistics.Histogram{hg}, count: count, isIndex: 1, err: err}
}

// SampleCollector will collect samples and calculate the count and ndv of an attribute.
type SampleCollector struct {
	samples   []types.Datum
	NullCount int64
	Count     int64
	Sketch    *statistics.FMSketch
}

func (c *SampleCollector) collect(d types.Datum) error {
	if d.IsNull() {
		c.NullCount++
		return nil
	}
	c.Count++
	if len(c.samples) < maxSampleCount {
		c.samples = append(c.samples, d)
	} else {
		shouldAdd := rand.Int63n(c.Count) < maxSampleCount
		if shouldAdd {
			idx := rand.Intn(maxSampleCount)
			c.samples[idx] = d
		}
	}
	return errors.Trace(c.Sketch.InsertValue(d))
}

// CollectSamplesAndEstimateNDVs collects sample from the result set using Reservoir Sampling algorithm,
// and estimates NDVs using FM Sketch during the collecting process. It returns the sample collectors which contain total
// count, null count and distinct values count.
// See https://en.wikipedia.org/wiki/Reservoir_sampling
// Exported for test.
func CollectSamplesAndEstimateNDVs(e ast.RecordSet, numCols int) ([]*SampleCollector, error) {
	collectors := make([]*SampleCollector, numCols)
	for i := range collectors {
		collectors[i] = &SampleCollector{
			Sketch: statistics.NewFMSketch(maxSketchSize),
		}
	}
	for {
		row, err := e.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			return collectors, nil
		}
		for i, val := range row.Data {
			err = collectors[i].collect(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
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
