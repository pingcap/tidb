// Copyright 2026 PingCAP, Inc.
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
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// looseScanInfo is the executor side of physicalop.LooseScanInfo, with the
// prefix columns resolved to offsets in the reader's output rows.
type looseScanInfo struct {
	prefixOffsets  []int
	prefixTypes    []*types.FieldType
	nullSkipOffset int
	nullSkipType   *types.FieldType
	batchSize      int
}

func buildLooseScanInfo(schema *expression.Schema, ls *physicalop.LooseScanInfo) (*looseScanInfo, error) {
	info := &looseScanInfo{
		nullSkipOffset: -1,
		batchSize:      int(ls.BatchSize),
	}
	for _, col := range ls.PrefixCols {
		offset := schema.ColumnIndex(col)
		if offset < 0 {
			return nil, errors.Errorf("loose index scan prefix column %s is not in the reader schema", col)
		}
		info.prefixOffsets = append(info.prefixOffsets, offset)
		info.prefixTypes = append(info.prefixTypes, schema.Columns[offset].RetType)
	}
	if col := ls.NullSkipCol; col != nil {
		info.nullSkipOffset = schema.ColumnIndex(col)
		if info.nullSkipOffset < 0 {
			return nil, errors.Errorf("loose index scan column %s is not in the reader schema", col)
		}
		info.nullSkipType = schema.Columns[info.nullSkipOffset].RetType
	}
	return info, nil
}

// looseScanResult implements distsql.SelectResult for a loose index scan. For
// each key range it repeatedly reads the first batch of rows in what is left
// of the range, then moves the range start past the key prefix of the last row
// read (or the range end before it, for a descending scan). A request that
// returns no rows finishes the range.
type looseScanResult struct {
	info     *looseScanInfo
	kvRanges []kv.KeyRange
	desc     bool
	loc      *time.Location
	retTypes []*types.FieldType
	// keyPrefix is the key prefix of the index, or of the table's rows, that
	// the encoded key prefix of a row is appended to.
	keyPrefix kv.Key
	// selectRange sends one request over a key range.
	selectRange func(ctx context.Context, r kv.KeyRange) (distsql.SelectResult, error)
	// cur is what is left of kvRanges[rangeIdx].
	rangeIdx int
	cur      kv.KeyRange
	started  bool
	batch    *chunk.Chunk
	datums   []types.Datum
	keyBuf   []byte
}

func newLooseScanResult(info *looseScanInfo, kvRanges []kv.KeyRange, desc bool, loc *time.Location,
	retTypes []*types.FieldType, keyPrefix kv.Key,
	selectRange func(ctx context.Context, r kv.KeyRange) (distsql.SelectResult, error)) *looseScanResult {
	return &looseScanResult{
		info:        info,
		kvRanges:    kvRanges,
		desc:        desc,
		loc:         loc,
		retTypes:    retTypes,
		keyPrefix:   keyPrefix,
		selectRange: selectRange,
		batch:       chunk.New(retTypes, info.batchSize, info.batchSize),
		datums:      make([]types.Datum, 0, len(info.prefixOffsets)+1),
	}
}

// newIndexLooseScanResult returns the loose scan result of an index reader.
func newIndexLooseScanResult(e *IndexReaderExecutor, kvRanges []kv.KeyRange) *looseScanResult {
	return newLooseScanResult(e.looseScan, kvRanges, e.desc, e.dctx.Location, exec.RetTypes(e),
		tablecodec.EncodeIndexSeekKey(e.physicalTableID, e.index.ID, nil),
		func(ctx context.Context, r kv.KeyRange) (distsql.SelectResult, error) {
			kvReq, err := e.buildKVReq([]kv.KeyRange{r})
			if err != nil {
				return nil, err
			}
			kvReq.Concurrency = 1
			return e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
		})
}

// newTableLooseScanResult returns the loose scan result of a table reader over
// a clustered primary key, whose row keys are the table's record prefix
// followed by the encoded primary key.
func newTableLooseScanResult(ctx context.Context, e *TableReaderExecutor) (*looseScanResult, error) {
	kvReq, err := e.buildKVReq(ctx, e.ranges)
	if err != nil {
		return nil, err
	}
	kvRanges := slices.Clone(kvReq.KeyRanges.FirstPartitionRange())
	slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})
	if e.desc {
		slices.Reverse(kvRanges)
	}
	e.kvRanges = kvRanges
	physicalTableID := getPhysicalTableID(e.table)
	return newLooseScanResult(e.looseScan, kvRanges, e.desc, e.dctx.Location, exec.RetTypes(e),
		tablecodec.GenTableRecordPrefix(physicalTableID),
		func(ctx context.Context, r kv.KeyRange) (distsql.SelectResult, error) {
			var builder distsql.RequestBuilder
			kvReq, err := e.finishKVReq(ctx, builder.SetKeyRanges([]kv.KeyRange{r}))
			if err != nil {
				return nil, err
			}
			kvReq.Concurrency = 1
			return e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
		}), nil
}

// Next implements distsql.SelectResult. It returns at most one batch per call.
func (r *looseScanResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for chk.NumRows() == 0 {
		if !r.started {
			if r.rangeIdx >= len(r.kvRanges) {
				return nil
			}
			r.cur = r.kvRanges[r.rangeIdx]
			r.started = true
		}
		if bytes.Compare(r.cur.StartKey, r.cur.EndKey) >= 0 {
			r.nextRange()
			continue
		}
		if err := r.readBatch(ctx); err != nil {
			return err
		}
		if r.batch.NumRows() == 0 {
			r.nextRange()
			continue
		}
		chk.Append(r.batch, 0, r.batch.NumRows())
		seekKey, err := r.seekKey(r.batch.GetRow(r.batch.NumRows() - 1))
		if err != nil {
			return err
		}
		if r.desc {
			r.cur.EndKey = seekKey
		} else {
			r.cur.StartKey = seekKey.PrefixNext()
		}
	}
	return nil
}

func (r *looseScanResult) nextRange() {
	r.rangeIdx++
	r.started = false
}

// readBatch reads the first batchSize rows of r.cur into r.batch. The pushed
// down Limit caps each region's response, and the request runs with
// concurrency 1, so the first non-empty region response is enough.
func (r *looseScanResult) readBatch(ctx context.Context) error {
	r.batch.Reset()
	result, err := r.selectRange(ctx, r.cur)
	if err != nil {
		return err
	}
	tmp := chunk.New(r.retTypes, r.info.batchSize, r.info.batchSize)
	for r.batch.NumRows() < r.info.batchSize {
		if err = result.Next(ctx, tmp); err != nil {
			break
		}
		if tmp.NumRows() == 0 {
			break
		}
		need := min(tmp.NumRows(), r.info.batchSize-r.batch.NumRows())
		r.batch.Append(tmp, 0, need)
	}
	if closeErr := result.Close(); err == nil {
		err = closeErr
	}
	return err
}

// seekKey returns the key of the prefix of row. Every key that starts with the
// prefix sorts at or after it and before its PrefixNext().
func (r *looseScanResult) seekKey(row chunk.Row) (kv.Key, error) {
	r.datums = r.datums[:0]
	for i, offset := range r.info.prefixOffsets {
		r.datums = append(r.datums, row.GetDatum(offset, r.info.prefixTypes[i]))
	}
	// Ascending MIN over the next key column: skip only the NULLs at the head
	// of the group, so the next request returns its first non-NULL value.
	if r.info.nullSkipOffset >= 0 && row.IsNull(r.info.nullSkipOffset) {
		r.datums = append(r.datums, row.GetDatum(r.info.nullSkipOffset, r.info.nullSkipType))
	}
	var err error
	r.keyBuf = append(r.keyBuf[:0], r.keyPrefix...)
	r.keyBuf, err = codec.EncodeKey(r.loc, r.keyBuf, r.datums...)
	if err != nil {
		return nil, err
	}
	return kv.Key(r.keyBuf).Clone(), nil
}

// NextRaw implements distsql.SelectResult.
func (*looseScanResult) NextRaw(context.Context) ([]byte, error) {
	return nil, errors.New("loose index scan doesn't support NextRaw")
}

// IntoIter implements distsql.SelectResult.
func (*looseScanResult) IntoIter([][]*types.FieldType) (distsql.SelectResultIter, error) {
	return nil, errors.New("loose index scan doesn't support IntoIter")
}

// Close implements distsql.SelectResult.
func (*looseScanResult) Close() error {
	return nil
}
