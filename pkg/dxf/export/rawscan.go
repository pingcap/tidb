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

package export

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	kvutil "github.com/tikv/client-go/v2/util"
)

// errOldRowFormat reports a row written in the pre-2.0 encoding, which the
// batched scan cannot decode. The caller re-exports the batch one table at a
// time through the coprocessor, which decodes either format server-side.
var errOldRowFormat = errors.New("export: row is in the old format")

// exportBatchedChunk reads every table of a batched chunk in one scan instead of
// one scan each, which is what makes exporting a schema of many tiny tables
// affordable for TiKV: a region holding thousands of them costs a request per
// region rather than a request per table.
//
// The spans are whole tables in key order, so exactly one table is open at a
// time and rows never have to be routed between decoders.
func (e *dumpStepExecutor) exportBatchedChunk(ctx context.Context, ce *chunkExporter, c Chunk) error {
	snap := e.store.GetSnapshot(kv.NewVersion(e.taskMeta.SnapshotTS))
	snap.SetOption(kv.RequestSourceInternal, true)
	snap.SetOption(kv.RequestSourceType, kvutil.ExplicitTypeDumpling)
	snap.SetOption(kv.ExplicitRequestSourceType, kvutil.ExplicitTypeDumpling)
	// Exported data is read once and never read again, so keeping it would only
	// evict rows that user traffic still needs.
	snap.SetOption(kv.NotFillCache, true)

	var it kv.Iterator
	defer func() {
		if it != nil {
			it.Close()
		}
	}()
	// Reopening costs a round trip, so it happens only when tables outside this
	// export sit between two spans; adjacent spans keep the same scan.
	openAt := func(start kv.Key) error {
		if it != nil {
			it.Close()
		}
		var err error
		it, err = snap.Iter(start, kv.Key(c.End))
		return errors.Trace(err)
	}
	if err := openAt(kv.Key(c.Start)); err != nil {
		return err
	}

	for i := range c.Spans {
		span := &c.Spans[i]
		if !it.Valid() {
			break
		}
		if it.Key().Cmp(kv.Key(span.Start)) < 0 {
			if err := openAt(kv.Key(span.Start)); err != nil {
				return err
			}
		}
		if err := e.exportSpan(ctx, ce, it, span); err != nil {
			return err
		}
	}
	return nil
}

// exportSpan writes one table's rows out of a batched scan, advancing the shared
// iterator to the span's end.
func (e *dumpStepExecutor) exportSpan(ctx context.Context, ce *chunkExporter, it kv.Iterator, span *TableSpan) error {
	ref := e.tableRefs[span.TableIdx]
	tblInfo := ref.tableInfo
	colInfos, fieldTps := exportColumns(tblInfo)
	dec := newExportRowDecoder(ce.exprCtx, tblInfo, colInfos)
	enc := newRowEncoder(tblInfo.Name.O, colInfos)
	w := &chunkWriter{
		ctx:      ctx,
		objStore: e.objStore,
		fileSize: e.taskMeta.FileSize,
		db:       ref.dbName,
		table:    tblInfo.Name.O,
		ordinal:  span.Ordinal,
		kinds:    fieldKinds(colInfos),
	}
	chk := chunk.NewChunkWithCapacity(fieldTps, readChunkSize)
	flush := func() error {
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rawRow, err := enc.encode(row)
			if err != nil {
				return err
			}
			if err := w.writeRow(rawRow); err != nil {
				return err
			}
		}
		chk.Reset()
		return nil
	}

	end := kv.Key(span.End)
	var rows int64
	for it.Valid() && it.Key().Cmp(end) < 0 {
		key := it.Key()
		if !tablecodec.IsRecordKey(key) {
			if err := it.Next(); err != nil {
				return errors.Trace(err)
			}
			continue
		}
		_, handle, err := tablecodec.DecodeRecordKey(key)
		if err != nil {
			return errors.Trace(err)
		}
		value := it.Value()
		if !rowcodec.IsNewFormat(value) {
			return errOldRowFormat
		}
		if err := dec.DecodeToChunk(value, 0, handle, chk); err != nil {
			return errors.Trace(err)
		}
		rows++
		if chk.IsFull() {
			if err := flush(); err != nil {
				return err
			}
		}
		if err := it.Next(); err != nil {
			return errors.Trace(err)
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := w.close(); err != nil {
		return err
	}
	e.summary.RowCnt.Add(rows)
	e.summary.Processed.Add(w.written)
	return nil
}

// newExportRowDecoder builds the decoder that turns a raw row value into the
// columns the exporter writes. The coprocessor does this server-side for the
// unbatched path; a batched scan returns raw bytes, so it is done here instead.
func newExportRowDecoder(exprCtx expression.BuildContext, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) *rowcodec.ChunkDecoder {
	var pkCols []int64
	reqCols := make([]rowcodec.ColInfo, len(colInfos))
	for i, col := range colInfos {
		if (tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag())) || col.ID == model.ExtraHandleID {
			pkCols = append(pkCols, col.ID)
		}
		reqCols[i] = rowcodec.ColInfo{
			ID:            col.ID,
			VirtualGenCol: col.IsVirtualGenerated(),
			Ft:            &colInfos[i].FieldType,
		}
	}
	if len(pkCols) == 0 {
		// A clustered primary key lives in the key rather than the value, so the
		// decoder needs its column ids to rebuild those columns.
		pkCols = tables.TryGetCommonPkColumnIds(tblInfo)
		if len(pkCols) == 0 {
			pkCols = []int64{-1}
		}
	}
	// Columns added after a row was written are absent from it and take their
	// original default instead.
	defVal := func(i int, chk *chunk.Chunk) error {
		if reqCols[i].ID < 0 {
			chk.AppendNull(i)
			return nil
		}
		d, err := table.GetColOriginDefaultValue(exprCtx, colInfos[i])
		if err != nil {
			return err
		}
		chk.AppendDatum(i, &d)
		return nil
	}
	return rowcodec.NewChunkDecoder(reqCols, pkCols, defVal, exprCtx.GetEvalCtx().Location())
}
