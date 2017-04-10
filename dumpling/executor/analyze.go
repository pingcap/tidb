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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	schema  *expression.Schema
	tblInfo *model.TableInfo
	ctx     context.Context
	idxIDs  []int64
	colIDs  []int64
	pkID    int64
	Srcs    []Executor
}

const (
	maxSampleCount     = 10000
	maxSketchSize      = 1000
	defaultBucketCount = 256
)

// Schema implements the Executor Schema interface.
func (e *AnalyzeExec) Schema() *expression.Schema {
	return e.schema
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
	for _, src := range e.Srcs {
		ae := src.(*AnalyzeExec)
		var count int64 = -1
		var sampleRows []*ast.Row
		var colNDVs []int64
		if len(ae.colIDs) != 0 {
			rs := &recordSet{executor: ae.Srcs[len(ae.Srcs)-1]}
			var err error
			count, sampleRows, colNDVs, err = CollectSamplesAndEstimateNDVs(rs, len(ae.colIDs))
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		columnSamples := rowsToColumnSamples(sampleRows)
		var pkRS ast.RecordSet
		if ae.pkID != 0 {
			offset := len(ae.Srcs) - 1
			if len(ae.colIDs) != 0 {
				offset--
			}
			pkRS = &recordSet{executor: ae.Srcs[offset]}
		}
		idxRS := make([]ast.RecordSet, 0, len(ae.idxIDs))
		for i := range ae.idxIDs {
			idxRS = append(idxRS, &recordSet{executor: ae.Srcs[i]})
		}
		err := ae.buildStatisticsAndSaveToKV(count, columnSamples, colNDVs, idxRS, pkRS)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return nil, nil
}

func (e *AnalyzeExec) buildStatisticsAndSaveToKV(count int64, columnSamples [][]types.Datum, colNDVs []int64, idxRS []ast.RecordSet, pkRS ast.RecordSet) error {
	statBuilder := &statistics.Builder{
		Ctx:           e.ctx,
		TblInfo:       e.tblInfo,
		Count:         count,
		NumBuckets:    defaultBucketCount,
		ColumnSamples: columnSamples,
		ColIDs:        e.colIDs,
		ColNDVs:       colNDVs,
		IdxRecords:    idxRS,
		IdxIDs:        e.idxIDs,
		PkRecords:     pkRS,
		PkID:          e.pkID,
	}
	t, err := statBuilder.NewTable()
	if err != nil {
		return errors.Trace(err)
	}
	err = t.SaveToStorage(e.ctx)
	return errors.Trace(err)
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
