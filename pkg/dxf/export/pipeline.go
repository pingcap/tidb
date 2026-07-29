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
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const (
	// readChunkSize is the row capacity of the read buffer chunk.
	readChunkSize = 1024
	// chunkScanConcurrency is the per-chunk cop-scan concurrency. It is 1, as in
	// add-index's keep-order read (index_cop.go), so keep-order results are
	// reliably in handle order; read parallelism comes from the worker pool
	// running many chunks at once, not from within one chunk.
	chunkScanConcurrency = 1
)

// chunkExporter carries the per-worker, table-independent scan context so it is
// built once per worker rather than once per chunk.
type chunkExporter struct {
	exprCtx *exprstatic.ExprContext
	distCtx *distsqlctx.DistSQLContext
}

func (e *dumpStepExecutor) newChunkExporter() *chunkExporter {
	return &chunkExporter{
		exprCtx: newExportExprCtx(),
		distCtx: newExportDistSQLCtx(e.store.GetClient()),
	}
}

// exportChunk reads one chunk's key range at the snapshot in handle order,
// encodes the rows to CSV and uploads them as a group of files named by the
// chunk's ordinal.
func (e *dumpStepExecutor) exportChunk(ctx context.Context, ce *chunkExporter, c Chunk) error {
	tbl := e.taskMeta.Tables[c.TableIdx]
	tblInfo := tbl.TableInfo
	colInfos, fieldTps := exportColumns(tblInfo)

	rs, err := buildScan(ctx, ce.exprCtx, ce.distCtx, tblInfo, c.PhysicalID, colInfos, fieldTps,
		e.taskMeta.SnapshotTS, chunkScanConcurrency, c.Start, c.End)
	if err != nil {
		return err
	}
	defer func() {
		_ = rs.Close()
	}()

	fileSize := e.taskMeta.FileSize
	if fileSize <= 0 {
		fileSize = defaultFileSize
	}
	enc := newRowEncoder(tblInfo.Name.O, colInfos)
	w := &chunkWriter{
		ctx:      ctx,
		objStore: e.objStore,
		fileSize: fileSize,
		db:       tbl.DBName,
		table:    tblInfo.Name.O,
		ordinal:  c.Ordinal,
		kinds:    fieldKinds(colInfos),
	}

	chk := chunk.NewChunkWithCapacity(fieldTps, readChunkSize)
	var rows int64
	for {
		chk.Reset()
		if err := rs.Next(ctx, chk); err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		it := chunk.NewIterator4Chunk(chk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			rawRow, err := enc.encode(row)
			if err != nil {
				return err
			}
			if err := w.writeRow(rawRow); err != nil {
				return err
			}
		}
		rows += int64(chk.NumRows())
	}
	if err := w.close(); err != nil {
		return err
	}
	e.summary.RowCnt.Add(rows)
	e.summary.Processed.Add(w.written)
	e.logger.Debug("export chunk done",
		zap.Int("table-idx", c.TableIdx), zap.Int("ordinal", c.Ordinal),
		zap.Int64("rows", rows), zap.Int64("bytes", w.written))
	return nil
}
