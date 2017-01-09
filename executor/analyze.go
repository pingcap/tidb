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
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/plan/statscache"
	"github.com/pingcap/tidb/util/types"
)

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	schema     expression.Schema
	table      *ast.TableName
	ctx        context.Context
	indOffsets []int
	colOffsets []int
	pkOffset   int
	Srcs       []Executor
}

const (
	maxSampleCount     = 10000
	defaultBucketCount = 256
)

// Schema implements the Executor Schema interface.
func (e *AnalyzeExec) Schema() expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *AnalyzeExec) Close() error {
	return nil
}

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next() (*Row, error) {
	for _, ana := range e.Srcs {
		e, _ := ana.(*AnalyzeExec)
		var count int64 = -1
		var sampleRows []*ast.Row
		if e.colOffsets != nil {
			rs := &recordSet{executor: e.Srcs[len(e.Srcs)-1]}
			var err error
			count, sampleRows, err = collectSamples(rs)
			rs.Close()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		columnSamples := rowsToColumnSamples(sampleRows)
		var pkRes ast.RecordSet
		if e.pkOffset != -1 {
			offset := len(e.Srcs) - 1
			if e.colOffsets != nil {
				offset--
			}
			pkRes = &recordSet{executor: e.Srcs[offset]}
		}
		var indRes []ast.RecordSet
		for i := range e.indOffsets {
			indRes = append(indRes, &recordSet{executor: e.Srcs[i]})
		}
		err := e.buildStatisticsAndSaveToKV(count, columnSamples, indRes, pkRes)
		for _, ir := range indRes {
			ir.Close()
		}
		if e.pkOffset != -1 {
			pkRes.Close()
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.Close()
	}
	return nil, nil
}

func (e *AnalyzeExec) buildStatisticsAndSaveToKV(count int64, columnSamples [][]types.Datum, indRes []ast.RecordSet, pkRes ast.RecordSet) error {
	txn := e.ctx.Txn()
	sc := e.ctx.GetSessionVars().StmtCtx
	t, err := statistics.NewTable(sc, e.table.TableInfo, int64(txn.StartTS()), count, defaultBucketCount, columnSamples)
	if err != nil {
		return errors.Trace(err)
	}
	statscache.SetStatisticsTableCache(e.table.TableInfo.ID, t)
	tpb, err := t.ToPB()
	if err != nil {
		return errors.Trace(err)
	}
	m := meta.NewMeta(txn)
	err = m.SetTableStats(e.table.TableInfo.ID, tpb)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// collectSamples collects sample from the result set, using Reservoir Sampling algorithm.
// See https://en.wikipedia.org/wiki/Reservoir_sampling
func collectSamples(e ast.RecordSet) (count int64, samples []*ast.Row, err error) {
	for {
		row, err := e.Next()
		if err != nil {
			return count, samples, errors.Trace(err)
		}
		if row == nil {
			break
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
	return count, samples, nil
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
