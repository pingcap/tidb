// Copyright 2020 PingCAP, Inc.
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
	"context"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
)

type RowSampler interface {
	WriteChunk(req *chunk.Chunk) error
}

type TableRegionSampler struct {
	ctx        sessionctx.Context
	table      table.Table
	startTS    uint64
	partTables []table.PartitionedTable
	schema     *expression.Schema
	fullSchema *expression.Schema
	isDesc     bool

	retTypes []*types.FieldType
	rowMap   map[int64]types.Datum
	finished bool
}

func NewTableRegionSampler(ctx sessionctx.Context, t table.Table, startTs uint64, partTables []table.PartitionedTable,
	schema *expression.Schema, fullSchema *expression.Schema, retTypes []*types.FieldType, desc bool) *TableRegionSampler {
	schemaFts := make(map[int64]*types.FieldType, len(schema.Columns))
	for _, c := range schema.Columns {
		schemaFts[c.ID] = c.RetType
	}
	return &TableRegionSampler{
		ctx:        ctx,
		table:      t,
		startTS:    startTs,
		partTables: partTables,
		schema:     schema,
		fullSchema: fullSchema,
		isDesc: desc,
		retTypes:   retTypes,
		rowMap:     make(map[int64]types.Datum),
	}
}

func (ts *TableRegionSampler) WriteChunk(req *chunk.Chunk) error {
	req.Reset()
	if ts.finished {
		return nil
	}
	regionKeyRanges, err := ts.splitTableRanges()
	if err != nil {
		return err
	}
	sort.Slice(regionKeyRanges, func(i, j int) bool {
		ir, jr := regionKeyRanges[i].StartKey, regionKeyRanges[j].StartKey
		if !ts.isDesc {
			return ir.Cmp(jr) < 0
		}
		return ir.Cmp(jr) > 0
	})

	decLoc, sysLoc := ts.ctx.GetSessionVars().TimeZone, time.UTC
	cols, decColMap, err := ts.buildSampleColAndDecodeColMap()
	if err != nil {
		return err
	}
	rowDecoder := decoder.NewRowDecoder(ts.table, cols, decColMap)
	err = ts.scanFirstKVForEachRange(regionKeyRanges, func(handle kv.Handle, value []byte) error {
		_, err := rowDecoder.DecodeAndEvalRowWithMap(ts.ctx, handle, value, decLoc, sysLoc, ts.rowMap)
		if err != nil {
			return err
		}
		currentRow := rowDecoder.CurrentRowWithDefaultVal()
		mutRow := chunk.MutRowFromTypes(ts.retTypes)
		for i, col := range ts.schema.Columns {
			offset := decColMap[col.ID].Col.Offset
			target := currentRow.GetDatum(offset, ts.retTypes[i])
			mutRow.SetDatum(i, target)
		}
		req.AppendRow(mutRow.ToRow())
		ts.resetRowMap()
		return nil
	})
	ts.finished = true
	return err
}

func (ts *TableRegionSampler) splitTableRanges() ([]kv.KeyRange, error) {
	if len(ts.partTables) != 0 {
		var ranges []kv.KeyRange
		for _, t := range ts.partTables {
			for _, pid := range t.GetAllPartitionIDs() {
				start := tablecodec.GenTableRecordPrefix(pid)
				end := start.PrefixNext()
				rs, err := splitTableRanges(ts.ctx.GetStore(), start, end)
				if err != nil {
					return nil, err
				}
				ranges = append(ranges, rs...)
			}
		}
		return ranges, nil
	}
	startKey, endKey := ts.table.RecordPrefix(), ts.table.RecordPrefix().PrefixNext()
	return splitTableRanges(ts.ctx.GetStore(), startKey, endKey)
}

func splitTableRanges(store kv.Storage, startKey, endKey kv.Key) ([]kv.KeyRange, error) {
	kvRange := kv.KeyRange{StartKey: startKey, EndKey: endKey}

	s, ok := store.(tikv.Storage)
	if !ok {
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := tikv.NewBackofferWithVars(context.Background(), maxSleep, nil)
	var ranges []kv.KeyRange
	regions, err := s.GetRegionCache().LoadRegionsInKeyRange(bo, startKey, endKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, r := range regions {
		ranges = append(ranges, kv.KeyRange{StartKey: r.StartKey(), EndKey: r.EndKey()})
	}
	if len(ranges) == 0 {
		return nil, errors.Trace(errors.Errorf("no regions found"))
	}
	return ranges, nil
}

func (ts *TableRegionSampler) buildSampleColAndDecodeColMap() ([]*table.Column, map[int64]decoder.Column, error) {
	schema := ts.schema
	cols := make([]*table.Column, 0, len(schema.Columns))
	colMap := make(map[int64]decoder.Column, len(schema.Columns))
	tableCols := ts.table.Cols()

	for offset, schemaCol := range schema.Columns {
		for _, tableCol := range tableCols {
			if tableCol.ID != schemaCol.ID {
				continue
			}
			if schemaCol.VirtualExpr != nil {
				var err error
				schemaCol.VirtualExpr, err = schemaCol.VirtualExpr.ResolveIndices(ts.fullSchema)
				if err != nil {
					return nil, nil, err
				}
			}
			colMap[tableCol.ID] = decoder.Column{
				Col:     tableCol,
				GenExpr: schemaCol.VirtualExpr,
			}
			cols = append(cols, tableCol)
		}
		if schemaCol.ID == model.ExtraHandleID {
			extraHandle := model.NewExtraHandleColInfo()
			extraHandle.Offset = offset
			tableCol := &table.Column{ColumnInfo: extraHandle}
			colMap[schemaCol.ID] = decoder.Column{
				Col: tableCol,
			}
			cols = append(cols, tableCol)
		}
	}
	return cols, colMap, nil
}

func (ts *TableRegionSampler) scanFirstKVForEachRange(
	ranges []kv.KeyRange, fn func(handle kv.Handle, value []byte) error) error {
	ver := kv.Version{Ver: ts.startTS}
	snap := ts.ctx.GetStore().GetSnapshot(ver)
	for _, r := range ranges {
		it, err := snap.Iter(r.StartKey, r.EndKey)
		if err != nil {
			return errors.Trace(err)
		}
		for it.Valid() {
			if !tablecodec.IsRecordKey(it.Key()) {
				if err := it.Next(); err != nil {
					return err
				}
				continue
			}
			var handle kv.Handle
			handle, err = tablecodec.DecodeRowKey(it.Key())
			if err != nil {
				return errors.Trace(err)
			}

			if err = fn(handle, it.Value()); err != nil {
				return errors.Trace(err)
			}
			break
		}
		it.Close()
	}
	return nil
}

func (ts *TableRegionSampler) resetRowMap() {
	if ts.rowMap == nil {
		colLen := len(ts.schema.Columns)
		ts.rowMap = make(map[int64]types.Datum, colLen)
		return
	}
	for id := range ts.rowMap {
		delete(ts.rowMap, id)
	}
}
