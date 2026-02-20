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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

type memRowsIterForIndex struct {
	kvIter     *txnMemBufferIter
	tps        []*types.FieldType
	mutableRow chunk.MutRow
	*memIndexReader
	colInfos []rowcodec.ColInfo
}

func (iter *memRowsIterForIndex) Next() ([]types.Datum, error) {
	var ret []types.Datum
	curr := iter.kvIter
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

		// filter key/value by partitition id
		if iter.index.Global {
			_, pid, err := codec.DecodeInt(tablecodec.SplitIndexValue(value).PartitionID)
			if err != nil {
				return nil, err
			}
			if _, exists := iter.partitionIDMap[pid]; !exists {
				continue
			}
		}

		data, err := iter.memIndexReader.decodeIndexKeyValue(key, value, iter.tps, iter.colInfos)
		if err != nil {
			return nil, err
		}

		iter.mutableRow.SetDatums(data...)
		matched, _, err := expression.EvalBool(iter.memIndexReader.ctx.GetExprCtx().GetEvalCtx(), iter.memIndexReader.conditions, iter.mutableRow.ToRow())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !matched {
			continue
		}
		ret = data
		break
	}
	return ret, nil
}

func (iter *memRowsIterForIndex) Close() {
	if iter.kvIter != nil {
		iter.kvIter.Close()
	}
}

func (m *memIndexMergeReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
	data, err := m.getMemRows(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &defaultRowsIter{data: data}, nil
}

func (m *memIndexMergeReader) getHandles() (handles []kv.Handle, err error) {
	hMap := kv.NewHandleMap()
	// loop each memReaders and fill handle map
	for i, reader := range m.memReaders {
		// [partitionNum][rangeNum]
		var readerKvRanges [][]kv.KeyRange
		if m.partitionMode {
			readerKvRanges = m.partitionKVRanges[i]
		} else {
			readerKvRanges = [][]kv.KeyRange{m.indexMergeReader.keyRanges[i]}
		}
		for j, kr := range readerKvRanges {
			switch r := reader.(type) {
			case *memTableReader:
				r.kvRanges = kr
			case *memIndexReader:
				r.kvRanges = kr
			default:
				return nil, errors.New("memReader have to be memTableReader or memIndexReader")
			}
			handles, err := reader.getMemRowsHandle()
			if err != nil {
				return nil, err
			}
			// Filter same row.
			for _, handle := range handles {
				if _, ok := handle.(kv.PartitionHandle); !ok && m.partitionMode {
					pid := m.partitionTables[j].GetPhysicalID()
					handle = kv.NewPartitionHandle(pid, handle)
				}
				if v, ok := hMap.Get(handle); !ok {
					cnt := 1
					hMap.Set(handle, &cnt)
				} else {
					*(v.(*int))++
				}
			}
		}
	}

	// process handle map, return handles meets the requirements (union or intersection)
	hMap.Range(func(h kv.Handle, val any) bool {
		if m.isIntersection {
			if *(val.(*int)) == len(m.memReaders) {
				handles = append(handles, h)
			}
		} else {
			handles = append(handles, h)
		}
		return true
	})

	return handles, nil
}

func (m *memIndexMergeReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	r, ctx := tracing.StartRegionEx(ctx, "memIndexMergeReader.getMemRows")
	defer r.End()

	handles, err := m.getHandles()
	if err != nil || len(handles) == 0 {
		return nil, err
	}

	var tblKVRanges []kv.KeyRange
	if m.partitionMode {
		// `tid` for partition handle is useless, so use 0 here.
		tblKVRanges, _ = distsql.TableHandlesToKVRanges(0, handles)
	} else {
		tblKVRanges, _ = distsql.TableHandlesToKVRanges(getPhysicalTableID(m.table), handles)
	}

	colIDs, pkColIDs, rd := getColIDAndPkColIDs(m.ctx, m.table, m.columns)

	memTblReader := &memTableReader{
		ctx:           m.ctx,
		table:         m.table.Meta(),
		columns:       m.columns,
		kvRanges:      tblKVRanges,
		conditions:    m.conditions,
		addedRows:     make([][]types.Datum, 0, len(handles)),
		retFieldTypes: m.retFieldTypes,
		colIDs:        colIDs,
		pkColIDs:      pkColIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
		},
	}

	rows, err := memTblReader.getMemRows(ctx)
	if err != nil {
		return nil, err
	}

	// Didn't set keepOrder = true for memTblReader,
	// In indexMerge, non-partitioned tables are also need reordered.
	if m.keepOrder {
		slices.SortFunc(rows, func(a, b []types.Datum) int {
			ret, err1 := m.compare(m.ctx.GetSessionVars().StmtCtx, a, b)
			if err1 != nil {
				err = err1
			}
			return ret
		})
	}

	return rows, err
}

func (*memIndexMergeReader) getMemRowsHandle() ([]kv.Handle, error) {
	return nil, errors.New("getMemRowsHandle has not been implemented for memIndexMergeReader")
}

func getColIDAndPkColIDs(ctx sessionctx.Context, tbl table.Table, columns []*model.ColumnInfo) (map[int64]int, []int64, *rowcodec.BytesDecoder) {
	colIDs := make(map[int64]int, len(columns))
	for i, col := range columns {
		colIDs[col.ID] = i
	}

	tblInfo := tbl.Meta()
	colInfos := make([]rowcodec.ColInfo, 0, len(columns))
	for i := range columns {
		col := columns[i]
		colInfos = append(colInfos, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()),
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		})
	}
	pkColIDs := tables.TryGetCommonPkColumnIds(tblInfo)
	if len(pkColIDs) == 0 {
		pkColIDs = []int64{-1}
	}
	defVal := func(i int) ([]byte, error) {
		d, err := table.GetColOriginDefaultValueWithoutStrictSQLMode(ctx.GetExprCtx(), columns[i])
		if err != nil {
			return nil, err
		}
		buf, err := tablecodec.EncodeValue(ctx.GetSessionVars().StmtCtx.TimeZone(), nil, d)
		return buf, ctx.GetSessionVars().StmtCtx.HandleError(err)
	}
	rd := rowcodec.NewByteDecoder(colInfos, pkColIDs, defVal, ctx.GetSessionVars().Location())
	return colIDs, pkColIDs, rd
}
