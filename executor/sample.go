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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"sort"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/tikv/client-go/v2/tikv"
)

var _ Executor = &TableSampleExecutor{}

const sampleMethodRegionConcurrency = 5

// TableSampleExecutor fetches a few rows through kv.Scan
// according to the specific sample method.
type TableSampleExecutor struct {
	baseExecutor

	table   table.Table
	startTS uint64

	sampler rowSampler
}

// Open initializes necessary variables for using this executor.
func (e *TableSampleExecutor) Open(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("TableSampleExecutor.Open", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	return nil
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by sampler.
func (e *TableSampleExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.sampler.finished() {
		return nil
	}
	// TODO(tangenta): add runtime stat & memory tracing
	return e.sampler.writeChunk(req)
}

// Close implements the Executor Close interface.
func (e *TableSampleExecutor) Close() error {
	return nil
}

type rowSampler interface {
	writeChunk(req *chunk.Chunk) error
	finished() bool
}

type tableRegionSampler struct {
	ctx        sessionctx.Context
	table      table.Table
	startTS    uint64
	partTables []table.PartitionedTable
	schema     *expression.Schema
	fullSchema *expression.Schema
	isDesc     bool
	retTypes   []*types.FieldType

	rowMap       map[int64]types.Datum
	restKVRanges []kv.KeyRange
	isFinished   bool
}

func newTableRegionSampler(ctx sessionctx.Context, t table.Table, startTs uint64, partTables []table.PartitionedTable,
	schema *expression.Schema, fullSchema *expression.Schema, retTypes []*types.FieldType, desc bool) *tableRegionSampler {
	return &tableRegionSampler{
		ctx:        ctx,
		table:      t,
		startTS:    startTs,
		partTables: partTables,
		schema:     schema,
		fullSchema: fullSchema,
		isDesc:     desc,
		retTypes:   retTypes,
		rowMap:     make(map[int64]types.Datum),
	}
}

func (s *tableRegionSampler) writeChunk(req *chunk.Chunk) error {
	err := s.initRanges()
	if err != nil {
		return err
	}
	expectedRowCount := req.RequiredRows()
	for expectedRowCount > 0 && len(s.restKVRanges) > 0 {
		ranges, err := s.pickRanges(expectedRowCount)
		if err != nil {
			return err
		}
		err = s.writeChunkFromRanges(ranges, req)
		if err != nil {
			return err
		}
		expectedRowCount = req.RequiredRows() - req.NumRows()
	}
	if len(s.restKVRanges) == 0 {
		s.isFinished = true
	}
	return nil
}

func (s *tableRegionSampler) initRanges() error {
	if s.restKVRanges == nil {
		var err error
		s.restKVRanges, err = s.splitTableRanges()
		if err != nil {
			return err
		}
		sortRanges(s.restKVRanges, s.isDesc)
	}
	return nil
}

func (s *tableRegionSampler) pickRanges(count int) ([]kv.KeyRange, error) {
	var regionKeyRanges []kv.KeyRange
	cutPoint := count
	if len(s.restKVRanges) < cutPoint {
		cutPoint = len(s.restKVRanges)
	}
	regionKeyRanges, s.restKVRanges = s.restKVRanges[:cutPoint], s.restKVRanges[cutPoint:]
	return regionKeyRanges, nil
}

func (s *tableRegionSampler) writeChunkFromRanges(ranges []kv.KeyRange, req *chunk.Chunk) error {
	decLoc := s.ctx.GetSessionVars().Location()
	cols, decColMap, err := s.buildSampleColAndDecodeColMap()
	if err != nil {
		return err
	}
	rowDecoder := decoder.NewRowDecoder(s.table, cols, decColMap)
	err = s.scanFirstKVForEachRange(ranges, func(handle kv.Handle, value []byte) error {
		_, err := rowDecoder.DecodeAndEvalRowWithMap(s.ctx, handle, value, decLoc, s.rowMap)
		if err != nil {
			return err
		}
		currentRow := rowDecoder.CurrentRowWithDefaultVal()
		mutRow := chunk.MutRowFromTypes(s.retTypes)
		for i, col := range s.schema.Columns {
			offset := decColMap[col.ID].Col.Offset
			target := currentRow.GetDatum(offset, s.retTypes[i])
			mutRow.SetDatum(i, target)
		}
		req.AppendRow(mutRow.ToRow())
		s.resetRowMap()
		return nil
	})
	return err
}

func (s *tableRegionSampler) splitTableRanges() ([]kv.KeyRange, error) {
	if len(s.partTables) != 0 {
		var ranges []kv.KeyRange
		for _, t := range s.partTables {
			for _, pid := range t.GetAllPartitionIDs() {
				start := tablecodec.GenTableRecordPrefix(pid)
				end := start.PrefixNext()
				rs, err := splitIntoMultiRanges(s.ctx.GetStore(), start, end)
				if err != nil {
					return nil, err
				}
				ranges = append(ranges, rs...)
			}
		}
		return ranges, nil
	}
	startKey, endKey := s.table.RecordPrefix(), s.table.RecordPrefix().PrefixNext()
	return splitIntoMultiRanges(s.ctx.GetStore(), startKey, endKey)
}

func splitIntoMultiRanges(store kv.Storage, startKey, endKey kv.Key) ([]kv.KeyRange, error) {
	kvRange := kv.KeyRange{StartKey: startKey, EndKey: endKey}

	s, ok := store.(tikv.Storage)
	if !ok {
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := tikv.NewBackofferWithVars(context.Background(), maxSleep, nil)
	regions, err := s.GetRegionCache().LoadRegionsInKeyRange(bo, startKey, endKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var ranges = make([]kv.KeyRange, 0, len(regions))
	for _, r := range regions {
		start, end := r.StartKey(), r.EndKey()
		if kv.Key(start).Cmp(startKey) < 0 {
			start = startKey
		}
		if end == nil || kv.Key(end).Cmp(endKey) > 0 {
			end = endKey
		}
		ranges = append(ranges, kv.KeyRange{StartKey: start, EndKey: end})
	}
	if len(ranges) == 0 {
		return nil, errors.Trace(errors.Errorf("no regions found"))
	}
	return ranges, nil
}

func sortRanges(ranges []kv.KeyRange, isDesc bool) {
	sort.Slice(ranges, func(i, j int) bool {
		ir, jr := ranges[i].StartKey, ranges[j].StartKey
		if !isDesc {
			return ir.Cmp(jr) < 0
		}
		return ir.Cmp(jr) > 0
	})
}

func (s *tableRegionSampler) buildSampleColAndDecodeColMap() ([]*table.Column, map[int64]decoder.Column, error) {
	schemaCols := s.schema.Columns
	cols := make([]*table.Column, 0, len(schemaCols))
	colMap := make(map[int64]decoder.Column, len(schemaCols))
	tableCols := s.table.Cols()

	for _, schemaCol := range schemaCols {
		for _, tableCol := range tableCols {
			if tableCol.ID != schemaCol.ID {
				continue
			}
			// The `MutRow` produced by `DecodeAndEvalRowWithMap` used `ColumnInfo.Offset` as indices.
			// To evaluate the columns in virtual generated expression properly,
			// indices of column(Column.Index) needs to be resolved against full column's schema.
			if schemaCol.VirtualExpr != nil {
				var err error
				schemaCol.VirtualExpr, err = schemaCol.VirtualExpr.ResolveIndices(s.fullSchema)
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
	}
	// Schema columns contain _tidb_rowid, append extra handle column info.
	if len(cols) < len(schemaCols) && schemaCols[len(schemaCols)-1].ID == model.ExtraHandleID {
		extraHandle := model.NewExtraHandleColInfo()
		extraHandle.Offset = len(cols)
		tableCol := &table.Column{ColumnInfo: extraHandle}
		colMap[model.ExtraHandleID] = decoder.Column{
			Col: tableCol,
		}
		cols = append(cols, tableCol)
	}
	return cols, colMap, nil
}

func (s *tableRegionSampler) scanFirstKVForEachRange(ranges []kv.KeyRange,
	fn func(handle kv.Handle, value []byte) error) error {
	ver := kv.Version{Ver: s.startTS}
	snap := s.ctx.GetStore().GetSnapshot(ver)
	setOptionForTopSQL(s.ctx.GetSessionVars().StmtCtx, snap)
	concurrency := sampleMethodRegionConcurrency
	if len(ranges) < concurrency {
		concurrency = len(ranges)
	}

	fetchers := make([]*sampleFetcher, concurrency)
	for i := 0; i < concurrency; i++ {
		fetchers[i] = &sampleFetcher{
			workerID:    i,
			concurrency: concurrency,
			kvChan:      make(chan *sampleKV),
			snapshot:    snap,
			ranges:      ranges,
		}
		go fetchers[i].run()
	}
	syncer := sampleSyncer{
		fetchers:   fetchers,
		totalCount: len(ranges),
		consumeFn:  fn,
	}
	return syncer.sync()
}

func (s *tableRegionSampler) resetRowMap() {
	if s.rowMap == nil {
		colLen := len(s.schema.Columns)
		s.rowMap = make(map[int64]types.Datum, colLen)
		return
	}
	for id := range s.rowMap {
		delete(s.rowMap, id)
	}
}

func (s *tableRegionSampler) finished() bool {
	return s.isFinished
}

type sampleKV struct {
	handle kv.Handle
	value  []byte
}

type sampleFetcher struct {
	workerID    int
	concurrency int
	kvChan      chan *sampleKV
	err         error
	snapshot    kv.Snapshot
	ranges      []kv.KeyRange
}

func (s *sampleFetcher) run() {
	defer close(s.kvChan)
	for i, r := range s.ranges {
		if i%s.concurrency != s.workerID {
			continue
		}
		it, err := s.snapshot.Iter(r.StartKey, r.EndKey)
		if err != nil {
			s.err = err
			return
		}
		hasValue := false
		for it.Valid() {
			if !tablecodec.IsRecordKey(it.Key()) {
				if err = it.Next(); err != nil {
					s.err = err
					return
				}
				continue
			}
			handle, err := tablecodec.DecodeRowKey(it.Key())
			if err != nil {
				s.err = err
				return
			}
			hasValue = true
			s.kvChan <- &sampleKV{handle: handle, value: it.Value()}
			break
		}
		if !hasValue {
			s.kvChan <- nil
		}
	}
}

type sampleSyncer struct {
	fetchers   []*sampleFetcher
	totalCount int
	consumeFn  func(handle kv.Handle, value []byte) error
}

func (s *sampleSyncer) sync() error {
	defer func() {
		for _, f := range s.fetchers {
			// Cleanup channels to terminate fetcher goroutines.
			for _, ok := <-f.kvChan; ok; {
			}
		}
	}()
	for i := 0; i < s.totalCount; i++ {
		f := s.fetchers[i%len(s.fetchers)]
		v, ok := <-f.kvChan
		if f.err != nil {
			return f.err
		}
		if ok && v != nil {
			err := s.consumeFn(v.handle, v.value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
