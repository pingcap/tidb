// Copyright 2019 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

type memIndexLookUpReader struct {
	ctx           sessionctx.Context
	index         *model.IndexInfo
	columns       []*model.ColumnInfo
	table         table.Table
	conditions    []expression.Expression
	retFieldTypes []*types.FieldType
	schema        *expression.Schema

	idxReader *memIndexReader

	groupedKVRanges []*kvRangesWithPhysicalTblID

	cacheTable kv.MemBuffer

	keepOrder bool
	compareExec
}

func buildMemIndexLookUpReader(ctx context.Context, us *UnionScanExec, idxLookUpReader *IndexLookUpExecutor) *memIndexLookUpReader {
	defer tracing.StartRegion(ctx, "buildMemIndexLookUpReader").End()

	outputOffset := []int{len(idxLookUpReader.index.Columns)}
	memIdxReader := &memIndexReader{
		ctx:            us.Ctx(),
		index:          idxLookUpReader.index,
		table:          idxLookUpReader.table.Meta(),
		retFieldTypes:  exec.RetTypes(us),
		outputOffset:   outputOffset,
		cacheTable:     us.cacheTable,
		partitionIDMap: us.partitionIDMap,
		resultRows:     make([]types.Datum, 0, len(outputOffset)),
	}

	return &memIndexLookUpReader{
		ctx:           us.Ctx(),
		index:         idxLookUpReader.index,
		columns:       idxLookUpReader.columns,
		table:         idxLookUpReader.table,
		conditions:    us.conditions,
		retFieldTypes: exec.RetTypes(us),
		schema:        us.Schema(),
		idxReader:     memIdxReader,

		groupedKVRanges: idxLookUpReader.groupedKVRanges,
		cacheTable:      us.cacheTable,

		keepOrder:   idxLookUpReader.keepOrder,
		compareExec: us.compareExec,
	}
}

func (m *memIndexLookUpReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
	kvRanges := [][]kv.KeyRange{m.idxReader.kvRanges}
	physicalTableIDs := []int64{getPhysicalTableID(m.table)}
	if len(m.groupedKVRanges) > 0 {
		kvRanges = make([][]kv.KeyRange, 0, len(m.groupedKVRanges))
		physicalTableIDs = make([]int64, 0, len(m.groupedKVRanges))
		for _, partitionRange := range m.groupedKVRanges {
			kvRanges = append(kvRanges, partitionRange.KeyRanges)
			physicalTableIDs = append(physicalTableIDs, partitionRange.PhysicalTableID)
		}
	}

	tblKVRanges := make([]kv.KeyRange, 0, 16)
	numHandles := 0
	for i, ranges := range kvRanges {
		m.idxReader.kvRanges = ranges
		handles, err := m.idxReader.getMemRowsHandle()
		if err != nil {
			return nil, err
		}
		if len(handles) == 0 {
			continue
		}
		numHandles += len(handles)
		physicalTableID := physicalTableIDs[i]
		ranges, _ := distsql.TableHandlesToKVRanges(physicalTableID, handles)
		tblKVRanges = append(tblKVRanges, ranges...)
	}
	if numHandles == 0 {
		return &defaultRowsIter{}, nil
	}

	if m.desc {
		slices.Reverse(tblKVRanges)
	}

	cd := NewRowDecoder(m.ctx, m.schema, m.table.Meta())
	colIDs, pkColIDs, rd := getColIDAndPkColIDs(m.ctx, m.table, m.columns)
	memTblReader := &memTableReader{
		ctx:           m.ctx,
		table:         m.table.Meta(),
		columns:       m.columns,
		kvRanges:      tblKVRanges,
		conditions:    m.conditions,
		addedRows:     make([][]types.Datum, 0, numHandles),
		retFieldTypes: m.retFieldTypes,
		colIDs:        colIDs,
		pkColIDs:      pkColIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
			cd:          cd,
		},
		cacheTable:  m.cacheTable,
		keepOrder:   m.keepOrder,
		compareExec: m.compareExec,
	}

	return memTblReader.getMemRowsIter(ctx)
}

func (*memIndexLookUpReader) getMemRowsHandle() ([]kv.Handle, error) {
	return nil, errors.New("getMemRowsHandle has not been implemented for memIndexLookUpReader")
}

type memIndexMergeReader struct {
	ctx              sessionctx.Context
	columns          []*model.ColumnInfo
	table            table.Table
	conditions       []expression.Expression
	retFieldTypes    []*types.FieldType
	indexMergeReader *IndexMergeReaderExecutor
	memReaders       []memReader
	isIntersection   bool

	// partition mode
	partitionMode     bool                  // if it is accessing a partition table
	partitionTables   []table.PhysicalTable // partition tables to access
	partitionKVRanges [][][]kv.KeyRange     // kv ranges for these partition tables

	keepOrder bool
	compareExec
}

func buildMemIndexMergeReader(ctx context.Context, us *UnionScanExec, indexMergeReader *IndexMergeReaderExecutor) *memIndexMergeReader {
	defer tracing.StartRegion(ctx, "buildMemIndexMergeReader").End()
	indexCount := len(indexMergeReader.indexes)
	memReaders := make([]memReader, 0, indexCount)
	for i := range indexCount {
		if indexMergeReader.indexes[i] == nil {
			colIDs, pkColIDs, rd := getColIDAndPkColIDs(indexMergeReader.Ctx(), indexMergeReader.table, indexMergeReader.columns)
			memReaders = append(memReaders, &memTableReader{
				ctx:           us.Ctx(),
				table:         indexMergeReader.table.Meta(),
				columns:       indexMergeReader.columns,
				kvRanges:      nil,
				conditions:    us.conditions,
				addedRows:     make([][]types.Datum, 0),
				retFieldTypes: exec.RetTypes(us),
				colIDs:        colIDs,
				pkColIDs:      pkColIDs,
				buffer: allocBuf{
					handleBytes: make([]byte, 0, 16),
					rd:          rd,
				},
			})
		} else {
			outputOffset := []int{len(indexMergeReader.indexes[i].Columns)}
			memReaders = append(memReaders, &memIndexReader{
				ctx:            us.Ctx(),
				index:          indexMergeReader.indexes[i],
				table:          indexMergeReader.table.Meta(),
				kvRanges:       nil,
				compareExec:    compareExec{desc: indexMergeReader.descs[i]},
				retFieldTypes:  exec.RetTypes(us),
				outputOffset:   outputOffset,
				partitionIDMap: indexMergeReader.partitionIDMap,
				resultRows:     make([]types.Datum, 0, len(outputOffset)),
			})
		}
	}

	return &memIndexMergeReader{
		ctx:              us.Ctx(),
		table:            indexMergeReader.table,
		columns:          indexMergeReader.columns,
		conditions:       us.conditions,
		retFieldTypes:    exec.RetTypes(us),
		indexMergeReader: indexMergeReader,
		memReaders:       memReaders,
		isIntersection:   indexMergeReader.isIntersection,

		partitionMode:     indexMergeReader.partitionTableMode,
		partitionTables:   indexMergeReader.prunedPartitions,
		partitionKVRanges: indexMergeReader.partitionKeyRanges,

		keepOrder:   us.keepOrder,
		compareExec: us.compareExec,
	}
}

type memRowsIter interface {
	Next() ([]types.Datum, error)
	// Close will release the snapshot it holds, so be sure to call Close.
	Close()
}

type defaultRowsIter struct {
	data   [][]types.Datum
	cursor int
}

func (iter *defaultRowsIter) Next() ([]types.Datum, error) {
	if iter.cursor < len(iter.data) {
		ret := iter.data[iter.cursor]
		iter.cursor++
		return ret, nil
	}
	return nil, nil
}

func (*defaultRowsIter) Close() {}

// memRowsIterForTable combine a kv.Iterator and a kv decoder to get a memRowsIter.
type memRowsIterForTable struct {
	kvIter   *txnMemBufferIter // txnMemBufferIter is the kv.Iterator
	cd       *rowcodec.ChunkDecoder
	chk      *chunk.Chunk
	datumRow []types.Datum
	*memTableReader
}

func (iter *memRowsIterForTable) Next() ([]types.Datum, error) {
	curr := iter.kvIter
	var ret []types.Datum
	for curr.Valid() {
		key := curr.Key()
		value := curr.Value()
		if err := curr.Next(); err != nil {
			return nil, errors.Trace(err)
		}

		// check whether the key was been deleted.
		if len(value) == 0 {
			continue
		}
		handle, err := tablecodec.DecodeRowKey(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		iter.chk.Reset()

		if !rowcodec.IsNewFormat(value) {
			// TODO: remove the legacy code!
			// fallback to the old way.
			iter.datumRow, err = iter.memTableReader.decodeRecordKeyValue(key, value, &iter.datumRow)
			if err != nil {
				return nil, errors.Trace(err)
			}

			mutableRow := chunk.MutRowFromTypes(iter.retFieldTypes)
			mutableRow.SetDatums(iter.datumRow...)
			matched, _, err := expression.EvalBool(iter.ctx.GetExprCtx().GetEvalCtx(), iter.conditions, mutableRow.ToRow())
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !matched {
				continue
			}
			return iter.datumRow, nil
		}

		err = iter.cd.DecodeToChunk(value, 0, handle, iter.chk)
		if err != nil {
			return nil, errors.Trace(err)
		}

		row := iter.chk.GetRow(0)
		matched, _, err := expression.EvalBool(iter.ctx.GetExprCtx().GetEvalCtx(), iter.conditions, row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !matched {
			continue
		}
		ret = row.GetDatumRowWithBuffer(iter.retFieldTypes, iter.datumRow)
		break
	}
	return ret, nil
}

func (iter *memRowsIterForTable) Close() {
	if iter.kvIter != nil {
		iter.kvIter.Close()
	}
}

