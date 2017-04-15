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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	ctx     context.Context
	pkCount int
	Srcs    []Executor
}

const (
	maxSampleCount     = 10000
	maxSketchSize      = 1000
	defaultBucketCount = 256
)

// Schema implements the Executor Schema interface.
func (e *AnalyzeExec) Schema() *expression.Schema {
	return nil
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
	for i, src := range e.Srcs {
		switch src.(type) {
		case *XSelectTableExec:
			var err error
			if i < e.pkCount {
				err = e.analyzePK(src.(*XSelectTableExec))
			} else {
				err = e.analyzeColumns(src.(*XSelectTableExec))
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
		case *XSelectIndexExec:
			err := e.analyzeIndex(src.(*XSelectIndexExec))
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return nil, nil
}

func (e *AnalyzeExec) analyzePK(exec *XSelectTableExec) error {
	statBuilder := &statistics.Builder{
		Ctx:        exec.ctx,
		TblInfo:    exec.tableInfo,
		Count:      -1,
		NumBuckets: defaultBucketCount,
		PkRecords:  &recordSet{executor: exec},
		PkID:       exec.Columns[0].ID,
	}
	err := e.buildStatisticsAndSaveToKV(statBuilder)
	return errors.Trace(err)
}

func (e *AnalyzeExec) analyzeColumns(exec *XSelectTableExec) error {
	count, sampleRows, colNDVs, err := CollectSamplesAndEstimateNDVs(&recordSet{executor: exec}, len(exec.Columns))
	if err != nil {
		return errors.Trace(err)
	}
	columnSamples := rowsToColumnSamples(sampleRows)
	var colIDs []int64
	for _, col := range exec.Columns {
		colIDs = append(colIDs, col.ID)
	}
	statBuilder := &statistics.Builder{
		Ctx:           exec.ctx,
		TblInfo:       exec.tableInfo,
		Count:         count,
		NumBuckets:    defaultBucketCount,
		ColumnSamples: columnSamples,
		ColIDs:        colIDs,
		ColNDVs:       colNDVs,
	}
	err = e.buildStatisticsAndSaveToKV(statBuilder)
	return errors.Trace(err)
}

func (e *AnalyzeExec) analyzeIndex(exec *XSelectIndexExec) error {
	statBuilder := &statistics.Builder{
		Ctx:        exec.ctx,
		TblInfo:    exec.tableInfo,
		Count:      -1,
		NumBuckets: defaultBucketCount,
		IdxRecords: []ast.RecordSet{&recordSet{executor: exec}},
		IdxIDs:     []int64{exec.indexPlan.Index.ID},
	}
	err := e.buildStatisticsAndSaveToKV(statBuilder)
	return errors.Trace(err)
}

func (e *AnalyzeExec) buildStatisticsAndSaveToKV(statBuilder *statistics.Builder) error {
	t, err := statBuilder.NewTable()
	if err != nil {
		return errors.Trace(err)
	}
	dom := sessionctx.GetDomain(e.ctx)
	err = dom.StatsHandle().SaveToStorage(e.ctx, t)
	if err != nil {
		return errors.Trace(err)
	}
	lease := dom.DDL().GetLease()
	if lease > 0 {
		// We sleep two lease to make sure other tidb node has updated this node.
		time.Sleep(lease * 2)
	} else {
		err = dom.StatsHandle().Update(GetInfoSchema(e.ctx))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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
