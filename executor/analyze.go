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
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/plan/statscache"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	schema     *expression.Schema
	tblInfo    *model.TableInfo
	ctx        context.Context
	idxOffsets []int
	colOffsets []int
	pkOffset   int
	Srcs       []Executor
}

const (
	maxSampleCount     = 10000
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
		if ae.colOffsets != nil {
			rs := &recordSet{executor: ae.Srcs[len(ae.Srcs)-1]}
			var err error
			count, sampleRows, err = collectSamples(rs)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		columnSamples := rowsToColumnSamples(sampleRows)
		var pkRS ast.RecordSet
		if ae.pkOffset != -1 {
			offset := len(ae.Srcs) - 1
			if ae.colOffsets != nil {
				offset--
			}
			pkRS = &recordSet{executor: ae.Srcs[offset]}
		}
		idxRS := make([]ast.RecordSet, 0, len(ae.idxOffsets))
		for i := range ae.idxOffsets {
			idxRS = append(idxRS, &recordSet{executor: ae.Srcs[i]})
		}
		err := ae.buildStatisticsAndSaveToKV(count, columnSamples, idxRS, pkRS)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return nil, nil
}

func (e *AnalyzeExec) buildStatisticsAndSaveToKV(count int64, columnSamples [][]types.Datum, idxRS []ast.RecordSet, pkRS ast.RecordSet) error {
	txn := e.ctx.Txn()
	statBuilder := &statistics.Builder{
		Sc:            e.ctx.GetSessionVars().StmtCtx,
		TblInfo:       e.tblInfo,
		StartTS:       int64(txn.StartTS()),
		Count:         count,
		NumBuckets:    defaultBucketCount,
		ColumnSamples: columnSamples,
		ColOffsets:    e.colOffsets,
		IdxRecords:    idxRS,
		IdxOffsets:    e.idxOffsets,
		PkRecords:     pkRS,
		PkOffset:      e.pkOffset,
	}
	t, err := statBuilder.NewTable()
	if err != nil {
		return errors.Trace(err)
	}
	version := e.ctx.Txn().StartTS()
	statscache.SetStatisticsTableCache(e.tblInfo.ID, t, version)
	tpb, err := t.ToPB()
	if err != nil {
		return errors.Trace(err)
	}
	m := meta.NewMeta(txn)
	err = m.SetTableStats(e.tblInfo.ID, tpb)
	if err != nil {
		return errors.Trace(err)
	}
	insertSQL := fmt.Sprintf("insert into mysql.stats_meta (version, table_id) values (%d, %d) on duplicate key update version = %d", version, e.tblInfo.ID, version)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, insertSQL)
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
