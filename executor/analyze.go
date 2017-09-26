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
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/statistics"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	ctx   context.Context
	tasks []*analyzeTask
}

const (
	maxSampleSize       = 10000
	maxRegionSampleSize = 1000
	maxSketchSize       = 10000
	maxBucketSize       = 256
)

// Schema implements the Executor Schema interface.
func (e *AnalyzeExec) Schema() *expression.Schema {
	return expression.NewSchema()
}

// Open implements the Executor Open interface.
func (e *AnalyzeExec) Open() error {
	return nil
}

// Close implements the Executor Close interface.
func (e *AnalyzeExec) Close() error {
	return nil
}

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next() (Row, error) {
	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	taskCh := make(chan *analyzeTask, len(e.tasks))
	resultCh := make(chan statistics.AnalyzeResult, len(e.tasks))
	for i := 0; i < concurrency; i++ {
		go e.analyzeWorker(taskCh, resultCh)
	}
	for _, task := range e.tasks {
		taskCh <- task
	}
	close(taskCh)
	dom := sessionctx.GetDomain(e.ctx)
	lease := dom.StatsHandle().Lease
	if lease > 0 {
		var err1 error
		for i := 0; i < len(e.tasks); i++ {
			result := <-resultCh
			if result.Err != nil {
				err1 = result.Err
				log.Error(errors.ErrorStack(err1))
				continue
			}
			dom.StatsHandle().AnalyzeResultCh() <- &result
		}
		// We sleep two lease to make sure other tidb node has updated this node.
		time.Sleep(lease * 2)
		return nil, errors.Trace(err1)
	}
	results := make([]statistics.AnalyzeResult, 0, len(e.tasks))
	var err1 error
	for i := 0; i < len(e.tasks); i++ {
		result := <-resultCh
		if result.Err != nil {
			err1 = result.Err
			log.Error(errors.ErrorStack(err1))
			continue
		}
		results = append(results, result)
	}
	if err1 != nil {
		return nil, errors.Trace(err1)
	}
	for _, result := range results {
		for _, hg := range result.Hist {
			err = hg.SaveToStorage(e.ctx, result.TableID, result.Count, result.IsIndex)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	err = dom.StatsHandle().Update(GetInfoSchema(e.ctx))
	if err != nil {
		return nil, errors.Trace(err)
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
	colTask taskType = iota
	idxTask
)

type analyzeTask struct {
	taskType  taskType
	tableInfo *model.TableInfo
	indexInfo *model.IndexInfo
	Columns   []*model.ColumnInfo
	PKInfo    *model.ColumnInfo
	src       Executor
	idxExec   *AnalyzeIndexExec
	colExec   *AnalyzeColumnsExec
}

func (e *AnalyzeExec) analyzeWorker(taskCh <-chan *analyzeTask, resultCh chan<- statistics.AnalyzeResult) {
	for task := range taskCh {
		switch task.taskType {
		case colTask:
			if task.colExec != nil {
				resultCh <- analyzeColumnsPushdown(task.colExec)
			} else {
				resultCh <- e.analyzeColumns(task)
			}
		case idxTask:
			if task.idxExec != nil {
				resultCh <- analyzeIndexPushdown(task.idxExec)
			} else {
				resultCh <- e.analyzeIndex(task)
			}
		}
	}
}

func (e *AnalyzeExec) analyzeColumns(task *analyzeTask) statistics.AnalyzeResult {
	if e := task.src.Open(); e != nil {
		return statistics.AnalyzeResult{Err: e}
	}
	pkID := int64(-1)
	if task.PKInfo != nil {
		pkID = task.PKInfo.ID
	}
	builder := statistics.SampleBuilder{
		Sc:            e.ctx.GetSessionVars().StmtCtx,
		RecordSet:     &recordSet{executor: task.src},
		ColLen:        len(task.Columns),
		PkID:          pkID,
		MaxBucketSize: maxBucketSize,
		MaxSketchSize: maxSketchSize,
		MaxSampleSize: maxSampleSize,
	}
	collectors, pkBuilder, err := builder.CollectSamplesAndEstimateNDVs()
	if e := task.src.Close(); e != nil {
		return statistics.AnalyzeResult{Err: e}
	}
	if err != nil {
		return statistics.AnalyzeResult{Err: err}
	}
	result := statistics.AnalyzeResult{TableID: task.tableInfo.ID, IsIndex: 0}
	if task.PKInfo != nil {
		result.Count = pkBuilder.Count
		result.Hist = []*statistics.Histogram{pkBuilder.Hist()}
	} else {
		result.Count = collectors[0].Count + collectors[0].NullCount
	}
	for i, col := range task.Columns {
		hg, err := statistics.BuildColumn(e.ctx, maxBucketSize, col.ID, collectors[i])
		result.Hist = append(result.Hist, hg)
		if err != nil && result.Err == nil {
			result.Err = err
		}
	}
	return result
}

func (e *AnalyzeExec) analyzeIndex(task *analyzeTask) statistics.AnalyzeResult {
	if e := task.src.Open(); e != nil {
		return statistics.AnalyzeResult{Err: e}
	}
	count, hg, err := statistics.BuildIndex(e.ctx, maxBucketSize, task.indexInfo.ID, &recordSet{executor: task.src})
	if e := task.src.Close(); e != nil {
		return statistics.AnalyzeResult{Err: e}
	}
	return statistics.AnalyzeResult{TableID: task.tableInfo.ID, Hist: []*statistics.Histogram{hg}, Count: count, IsIndex: 1, Err: err}
}
