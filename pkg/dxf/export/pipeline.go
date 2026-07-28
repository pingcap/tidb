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
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const (
	// readChunkSize is the row capacity of the read buffer chunk.
	readChunkSize = 1024
	// chunkScanConcurrency is the cop-scan concurrency for one chunk; the whole
	// subtask's read parallelism is this times the worker count.
	chunkScanConcurrency = 4
)

// exportChunk reads one chunk's key range at the snapshot in handle order,
// encodes the rows to CSV and uploads them as a group of files named by the
// chunk's ordinal.
func (e *dumpStepExecutor) exportChunk(ctx context.Context, c Chunk) error {
	tbl := e.taskMeta.Tables[c.TableIdx]
	tblInfo := tbl.TableInfo
	colInfos, fieldTps := exportColumns(tblInfo)

	exprCtx := newExportExprCtx()
	distCtx := newExportDistSQLCtx(e.store.GetClient())
	rs, err := buildScan(ctx, exprCtx, distCtx, tblInfo, c.PhysicalID, colInfos, fieldTps,
		e.taskMeta.SnapshotTS, chunkScanConcurrency, c.Start, c.End)
	if err != nil {
		return err
	}
	defer func() {
		_ = rs.Close()
	}()

	enc := newRowEncoder(tblInfo.Name.O, colInfos)
	w := &chunkWriter{
		ctx:      ctx,
		objStore: e.objStore,
		fileSize: e.taskMeta.FileSize,
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
	e.logger.Debug("export chunk done",
		zap.Int("table-idx", c.TableIdx), zap.Int("ordinal", c.Ordinal), zap.Int64("rows", rows))
	return nil
}
