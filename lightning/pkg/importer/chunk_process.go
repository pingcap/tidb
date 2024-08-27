// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/driver/txn"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/extsort"
	"go.uber.org/zap"
)

// chunkProcessor process data chunk
// for local backend it encodes and writes KV to local disk
// for tidb backend it transforms data into sql and executes them.
type chunkProcessor struct {
	parser mydump.Parser
	index  int
	chunk  *checkpoints.ChunkCheckpoint
}

func newChunkProcessor(
	ctx context.Context,
	index int,
	cfg *config.Config,
	chunk *checkpoints.ChunkCheckpoint,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
	tableInfo *model.TableInfo,
) (*chunkProcessor, error) {
	parser, err := openParser(ctx, cfg, chunk, ioWorkers, store, tableInfo)
	if err != nil {
		return nil, err
	}
	return &chunkProcessor{
		parser: parser,
		index:  index,
		chunk:  chunk,
	}, nil
}

func openParser(
	ctx context.Context,
	cfg *config.Config,
	chunk *checkpoints.ChunkCheckpoint,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
	tblInfo *model.TableInfo,
) (mydump.Parser, error) {
	blockBufSize := int64(cfg.Mydumper.ReadBlockSize)
	reader, err := mydump.OpenReader(ctx, &chunk.FileMeta, store, storage.DecompressConfig{
		ZStdDecodeConcurrency: 1,
	})
	if err != nil {
		return nil, err
	}

	var parser mydump.Parser
	switch chunk.FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := cfg.Mydumper.CSV.Header && chunk.Chunk.Offset == 0
		// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
		charsetConvertor, err := mydump.NewCharsetConvertor(cfg.Mydumper.DataCharacterSet, cfg.Mydumper.DataInvalidCharReplace)
		if err != nil {
			return nil, err
		}
		parser, err = mydump.NewCSVParser(ctx, &cfg.Mydumper.CSV, reader, blockBufSize, ioWorkers, hasHeader, charsetConvertor)
		if err != nil {
			return nil, err
		}
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(ctx, cfg.TiDB.SQLMode, reader, blockBufSize, ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, store, reader, chunk.FileMeta.Path)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("file '%s' with unknown source type '%s'", chunk.Key.Path, chunk.FileMeta.Type.String())
	}

	if chunk.FileMeta.Compression == mydump.CompressionNone {
		if err = parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax); err != nil {
			_ = parser.Close()
			return nil, err
		}
	} else {
		if err = mydump.ReadUntil(parser, chunk.Chunk.Offset); err != nil {
			_ = parser.Close()
			return nil, err
		}
		parser.SetRowID(chunk.Chunk.PrevRowIDMax)
	}
	if len(chunk.ColumnPermutation) > 0 {
		parser.SetColumns(getColumnNames(tblInfo, chunk.ColumnPermutation))
	}

	return parser, nil
}

func getColumnNames(tableInfo *model.TableInfo, permutation []int) []string {
	colIndexes := make([]int, 0, len(permutation))
	for i := 0; i < len(permutation); i++ {
		colIndexes = append(colIndexes, -1)
	}
	colCnt := 0
	for i, p := range permutation {
		if p >= 0 {
			colIndexes[p] = i
			colCnt++
		}
	}

	names := make([]string, 0, colCnt)
	for _, idx := range colIndexes {
		// skip columns with index -1
		if idx >= 0 {
			// original fields contains _tidb_rowid field
			if idx == len(tableInfo.Columns) {
				names = append(names, model.ExtraHandleName.O)
			} else {
				names = append(names, tableInfo.Columns[idx].Name.O)
			}
		}
	}
	return names
}

func (cr *chunkProcessor) process(
	ctx context.Context,
	t *TableImporter,
	engineID int32,
	dataEngine, indexEngine backend.EngineWriter,
	rc *Controller,
) error {
	logger := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
	)
	// Create the encoder.
	kvEncoder, err := rc.encBuilder.NewEncoder(ctx, &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{
			SQLMode:   rc.cfg.TiDB.SQLMode,
			Timestamp: cr.chunk.Timestamp,
			SysVars:   rc.sysVars,
			// use chunk.PrevRowIDMax as the auto random seed, so it can stay the same value after recover from checkpoint.
			AutoRandomSeed: cr.chunk.Chunk.PrevRowIDMax,
		},
		Path:   cr.chunk.Key.Path,
		Table:  t.encTable,
		Logger: logger,
	})
	if err != nil {
		return err
	}
	defer kvEncoder.Close()

	kvsCh := make(chan []deliveredKVs, maxKVQueueSize)
	deliverCompleteCh := make(chan deliverResult)

	go func() {
		defer close(deliverCompleteCh)
		dur, err := cr.deliverLoop(ctx, kvsCh, t, engineID, dataEngine, indexEngine, rc)
		select {
		case <-ctx.Done():
		case deliverCompleteCh <- deliverResult{dur, err}:
		}
	}()

	logTask := logger.Begin(zap.InfoLevel, "restore file")

	readTotalDur, encodeTotalDur, encodeErr := cr.encodeLoop(
		ctx,
		kvsCh,
		t,
		logger,
		kvEncoder,
		deliverCompleteCh,
		rc,
	)
	var deliverErr error
	select {
	case deliverResult, ok := <-deliverCompleteCh:
		if ok {
			logTask.End(zap.ErrorLevel, deliverResult.err,
				zap.Duration("readDur", readTotalDur),
				zap.Duration("encodeDur", encodeTotalDur),
				zap.Duration("deliverDur", deliverResult.totalDur),
				zap.Object("checksum", &cr.chunk.Checksum),
			)
			deliverErr = deliverResult.err
		} else {
			// else, this must cause by ctx cancel
			deliverErr = ctx.Err()
		}
	case <-ctx.Done():
		deliverErr = ctx.Err()
	}
	return errors.Trace(firstErr(encodeErr, deliverErr))
}

//nolint:nakedret // TODO: refactor
func (cr *chunkProcessor) encodeLoop(
	ctx context.Context,
	kvsCh chan<- []deliveredKVs,
	t *TableImporter,
	logger log.Logger,
	kvEncoder encode.Encoder,
	deliverCompleteCh <-chan deliverResult,
	rc *Controller,
) (readTotalDur time.Duration, encodeTotalDur time.Duration, err error) {
	defer close(kvsCh)

	// when AddIndexBySQL, we use all PK and UK to run pre-deduplication, and then we
	// strip almost all secondary index to run encodeLoop. In encodeLoop when we meet
	// a duplicated row marked by pre-deduplication, we need original table structure
	// to generate the duplicate error message, so here create a new encoder with
	// original table structure.
	originalTableEncoder := kvEncoder
	if rc.cfg.TikvImporter.AddIndexBySQL {
		encTable, err := tables.TableFromMeta(t.alloc, t.tableInfo.Desired)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}

		originalTableEncoder, err = rc.encBuilder.NewEncoder(ctx, &encode.EncodingConfig{
			SessionOptions: encode.SessionOptions{
				SQLMode:   rc.cfg.TiDB.SQLMode,
				Timestamp: cr.chunk.Timestamp,
				SysVars:   rc.sysVars,
				// use chunk.PrevRowIDMax as the auto random seed, so it can stay the same value after recover from checkpoint.
				AutoRandomSeed: cr.chunk.Chunk.PrevRowIDMax,
			},
			Path:   cr.chunk.Key.Path,
			Table:  encTable,
			Logger: logger,
		})
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
		defer originalTableEncoder.Close()
	}

	send := func(kvs []deliveredKVs) error {
		select {
		case kvsCh <- kvs:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case deliverResult, ok := <-deliverCompleteCh:
			if deliverResult.err == nil && !ok {
				deliverResult.err = ctx.Err()
			}
			if deliverResult.err == nil {
				deliverResult.err = errors.New("unexpected premature fulfillment")
				logger.DPanic("unexpected: deliverCompleteCh prematurely fulfilled with no error", zap.Bool("chIsOpen", ok))
			}
			return errors.Trace(deliverResult.err)
		}
	}

	pauser, maxKvPairsCnt := rc.pauser, rc.cfg.TikvImporter.MaxKVPairs
	initializedColumns, reachEOF := false, false
	// filteredColumns is column names that excluded ignored columns
	// WARN: this might be not correct when different SQL statements contains different fields,
	// but since ColumnPermutation also depends on the hypothesis that the columns in one source file is the same
	// so this should be ok.
	var (
		filteredColumns []string
		extendVals      []types.Datum
	)
	ignoreColumns, err1 := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(t.dbInfo.Name, t.tableInfo.Core.Name.O, rc.cfg.Mydumper.CaseSensitive)
	if err1 != nil {
		err = err1
		return
	}

	var dupIgnoreRowsIter extsort.Iterator
	if t.dupIgnoreRows != nil {
		dupIgnoreRowsIter, err = t.dupIgnoreRows.NewIterator(ctx)
		if err != nil {
			return 0, 0, err
		}
		defer func() {
			_ = dupIgnoreRowsIter.Close()
		}()
	}

	for !reachEOF {
		if err = pauser.Wait(ctx); err != nil {
			return
		}
		offset, _ := cr.parser.Pos()
		if offset >= cr.chunk.Chunk.EndOffset {
			break
		}

		var readDur, encodeDur time.Duration
		canDeliver := false
		kvPacket := make([]deliveredKVs, 0, maxKvPairsCnt)
		curOffset := offset
		var newOffset, rowID, newScannedOffset int64
		var scannedOffset int64 = -1
		var kvSize uint64
		var scannedOffsetErr error
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			err = cr.parser.ReadRow()
			columnNames := cr.parser.Columns()
			newOffset, rowID = cr.parser.Pos()
			if cr.chunk.FileMeta.Compression != mydump.CompressionNone || cr.chunk.FileMeta.Type == mydump.SourceTypeParquet {
				newScannedOffset, scannedOffsetErr = cr.parser.ScannedPos()
				if scannedOffsetErr != nil {
					logger.Warn("fail to get data engine ScannedPos, progress may not be accurate",
						log.ShortError(scannedOffsetErr), zap.String("file", cr.chunk.FileMeta.Path))
				}
				if scannedOffset == -1 {
					scannedOffset = newScannedOffset
				}
			}

			switch errors.Cause(err) {
			case nil:
				if !initializedColumns {
					if len(cr.chunk.ColumnPermutation) == 0 {
						if err = t.initializeColumns(columnNames, cr.chunk); err != nil {
							return
						}
					}
					filteredColumns = columnNames
					ignoreColsMap := ignoreColumns.ColumnsMap()
					if len(ignoreColsMap) > 0 || len(cr.chunk.FileMeta.ExtendData.Columns) > 0 {
						filteredColumns, extendVals = filterColumns(columnNames, cr.chunk.FileMeta.ExtendData, ignoreColsMap, t.tableInfo.Core)
					}
					lastRow := cr.parser.LastRow()
					lastRowLen := len(lastRow.Row)
					extendColsMap := make(map[string]int)
					for i, c := range cr.chunk.FileMeta.ExtendData.Columns {
						extendColsMap[c] = lastRowLen + i
					}
					for i, col := range t.tableInfo.Core.Columns {
						if p, ok := extendColsMap[col.Name.O]; ok {
							cr.chunk.ColumnPermutation[i] = p
						}
					}
					initializedColumns = true

					if dupIgnoreRowsIter != nil {
						dupIgnoreRowsIter.Seek(common.EncodeIntRowID(lastRow.RowID))
					}
				}
			case io.EOF:
				reachEOF = true
				break outLoop
			default:
				err = common.ErrEncodeKV.Wrap(err).GenWithStackByArgs(&cr.chunk.Key, newOffset)
				return
			}
			readDur += time.Since(readDurStart)
			encodeDurStart := time.Now()
			lastRow := cr.parser.LastRow()
			lastRow.Row = append(lastRow.Row, extendVals...)

			// Skip duplicated rows.
			if dupIgnoreRowsIter != nil {
				rowIDKey := common.EncodeIntRowID(lastRow.RowID)
				isDupIgnored := false
			dupDetectLoop:
				for dupIgnoreRowsIter.Valid() {
					switch bytes.Compare(rowIDKey, dupIgnoreRowsIter.UnsafeKey()) {
					case 0:
						isDupIgnored = true
						break dupDetectLoop
					case 1:
						dupIgnoreRowsIter.Next()
					case -1:
						break dupDetectLoop
					}
				}
				if dupIgnoreRowsIter.Error() != nil {
					err = dupIgnoreRowsIter.Error()
					return
				}
				if isDupIgnored {
					cr.parser.RecycleRow(lastRow)
					lastOffset := curOffset
					curOffset = newOffset

					if rc.errorMgr.ConflictRecordsRemain() <= 0 {
						continue
					}

					dupMsg := cr.getDuplicateMessage(
						originalTableEncoder,
						lastRow,
						lastOffset,
						dupIgnoreRowsIter.UnsafeValue(),
						t.tableInfo.Desired,
						logger,
					)
					rowText := tidb.EncodeRowForRecord(ctx, t.encTable, rc.cfg.TiDB.SQLMode, lastRow.Row, cr.chunk.ColumnPermutation)
					err = rc.errorMgr.RecordDuplicate(
						ctx,
						logger,
						t.tableName,
						cr.chunk.Key.Path,
						newOffset,
						dupMsg,
						lastRow.RowID,
						rowText,
					)
					if err != nil {
						return 0, 0, err
					}
					continue
				}
			}

			// sql -> kv
			kvs, encodeErr := kvEncoder.Encode(lastRow.Row, lastRow.RowID, cr.chunk.ColumnPermutation, curOffset)
			encodeDur += time.Since(encodeDurStart)

			hasIgnoredEncodeErr := false
			if encodeErr != nil {
				rowText := tidb.EncodeRowForRecord(ctx, t.encTable, rc.cfg.TiDB.SQLMode, lastRow.Row, cr.chunk.ColumnPermutation)
				encodeErr = rc.errorMgr.RecordTypeError(ctx, logger, t.tableName, cr.chunk.Key.Path, newOffset, rowText, encodeErr)
				if encodeErr != nil {
					err = common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(&cr.chunk.Key, newOffset)
				}
				hasIgnoredEncodeErr = true
			}
			cr.parser.RecycleRow(lastRow)
			curOffset = newOffset

			if err != nil {
				return
			}
			if hasIgnoredEncodeErr {
				continue
			}

			kvPacket = append(kvPacket, deliveredKVs{kvs: kvs, columns: filteredColumns, offset: newOffset,
				rowID: rowID, realOffset: newScannedOffset})
			kvSize += kvs.Size()
			failpoint.Inject("mock-kv-size", func(val failpoint.Value) {
				kvSize += uint64(val.(int))
			})
			// pebble cannot allow > 4.0G kv in one batch.
			// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
			// so add this check.
			if kvSize >= minDeliverBytes || len(kvPacket) >= maxKvPairsCnt || newOffset == cr.chunk.Chunk.EndOffset {
				canDeliver = true
				kvSize = 0
			}
		}
		encodeTotalDur += encodeDur
		readTotalDur += readDur
		if m, ok := metric.FromContext(ctx); ok {
			m.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
			m.RowReadSecondsHistogram.Observe(readDur.Seconds())
			if cr.chunk.FileMeta.Type == mydump.SourceTypeParquet {
				m.RowReadBytesHistogram.Observe(float64(newScannedOffset - scannedOffset))
			} else {
				m.RowReadBytesHistogram.Observe(float64(newOffset - offset))
			}
		}

		if len(kvPacket) != 0 {
			deliverKvStart := time.Now()
			if err = send(kvPacket); err != nil {
				return
			}
			if m, ok := metric.FromContext(ctx); ok {
				m.RowKVDeliverSecondsHistogram.Observe(time.Since(deliverKvStart).Seconds())
			}
		}
	}

	err = send([]deliveredKVs{{offset: cr.chunk.Chunk.EndOffset, realOffset: cr.chunk.FileMeta.FileSize}})
	return
}

// getDuplicateMessage gets the duplicate message like a SQL error. When it meets
// internal error, the error message will be returned instead of the duplicate message.
// If the index is not found (which is not expected), an empty string will be returned.
func (cr *chunkProcessor) getDuplicateMessage(
	kvEncoder encode.Encoder,
	lastRow mydump.Row,
	lastOffset int64,
	encodedIdxID []byte,
	tableInfo *model.TableInfo,
	logger log.Logger,
) string {
	_, idxID, err := codec.DecodeVarint(encodedIdxID)
	if err != nil {
		return err.Error()
	}
	kvs, err := kvEncoder.Encode(lastRow.Row, lastRow.RowID, cr.chunk.ColumnPermutation, lastOffset)
	if err != nil {
		return err.Error()
	}

	if idxID == conflictOnHandle {
		for _, kv := range kvs.(*kv.Pairs).Pairs {
			if tablecodec.IsRecordKey(kv.Key) {
				dupErr := txn.ExtractKeyExistsErrFromHandle(kv.Key, kv.Val, tableInfo)
				return dupErr.Error()
			}
		}
		// should not happen
		logger.Warn("fail to find conflict record key",
			zap.String("file", cr.chunk.FileMeta.Path),
			zap.Any("row", lastRow.Row))
	} else {
		for _, kv := range kvs.(*kv.Pairs).Pairs {
			_, decodedIdxID, isRecordKey, err := tablecodec.DecodeKeyHead(kv.Key)
			if err != nil {
				return err.Error()
			}
			if !isRecordKey && decodedIdxID == idxID {
				dupErr := txn.ExtractKeyExistsErrFromIndex(kv.Key, kv.Val, tableInfo, idxID)
				return dupErr.Error()
			}
		}
		// should not happen
		logger.Warn("fail to find conflict index key",
			zap.String("file", cr.chunk.FileMeta.Path),
			zap.Int64("idxID", idxID),
			zap.Any("row", lastRow.Row))
	}
	return ""
}

//nolint:nakedret // TODO: refactor
func (cr *chunkProcessor) deliverLoop(
	ctx context.Context,
	kvsCh <-chan []deliveredKVs,
	t *TableImporter,
	engineID int32,
	dataEngine, indexEngine backend.EngineWriter,
	rc *Controller,
) (deliverTotalDur time.Duration, err error) {
	deliverLogger := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
		zap.String("task", "deliver"),
	)
	// Fetch enough KV pairs from the source.
	dataKVs := rc.encBuilder.MakeEmptyRows()
	indexKVs := rc.encBuilder.MakeEmptyRows()

	dataSynced := true
	hasMoreKVs := true
	var startRealOffset, currRealOffset int64 // save to 0 at first

	keyspace := keyspace.CodecV1.GetKeyspace()
	if t.kvStore != nil {
		keyspace = t.kvStore.GetCodec().GetKeyspace()
	}
	for hasMoreKVs {
		var (
			dataChecksum  = verify.NewKVChecksumWithKeyspace(keyspace)
			indexChecksum = verify.NewKVChecksumWithKeyspace(keyspace)
		)
		var columns []string
		var kvPacket []deliveredKVs
		// init these two field as checkpoint current value, so even if there are no kv pairs delivered,
		// chunk checkpoint should stay the same
		startOffset := cr.chunk.Chunk.Offset
		currOffset := startOffset
		startRealOffset = cr.chunk.Chunk.RealOffset
		currRealOffset = startRealOffset
		rowID := cr.chunk.Chunk.PrevRowIDMax

	populate:
		for dataChecksum.SumSize()+indexChecksum.SumSize() < minDeliverBytes {
			select {
			case kvPacket = <-kvsCh:
				if len(kvPacket) == 0 {
					hasMoreKVs = false
					break populate
				}
				for _, p := range kvPacket {
					if p.kvs == nil {
						// This is the last message.
						currOffset = p.offset
						currRealOffset = p.realOffset
						hasMoreKVs = false
						break populate
					}
					p.kvs.ClassifyAndAppend(&dataKVs, dataChecksum, &indexKVs, indexChecksum)
					columns = p.columns
					currOffset = p.offset
					currRealOffset = p.realOffset
					rowID = p.rowID
				}
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}

		err = func() error {
			// We use `TryRLock` with sleep here to avoid blocking current goroutine during importing when disk-quota is
			// triggered, so that we can save chunkCheckpoint as soon as possible after `FlushEngine` is called.
			// This implementation may not be very elegant or even completely correct, but it is currently a relatively
			// simple and effective solution.
			for !rc.diskQuotaLock.TryRLock() {
				// try to update chunk checkpoint, this can help save checkpoint after importing when disk-quota is triggered
				if !dataSynced {
					dataSynced = cr.maybeSaveCheckpoint(rc, t, engineID, cr.chunk, dataEngine, indexEngine)
				}
				time.Sleep(time.Millisecond)
			}
			defer rc.diskQuotaLock.RUnlock()

			// Write KVs into the engine
			start := time.Now()

			if err = dataEngine.AppendRows(ctx, columns, dataKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					deliverLogger.Error("write to data engine failed", log.ShortError(err))
				}

				return errors.Trace(err)
			}
			if err = indexEngine.AppendRows(ctx, columns, indexKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					deliverLogger.Error("write to index engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}

			if m, ok := metric.FromContext(ctx); ok {
				deliverDur := time.Since(start)
				deliverTotalDur += deliverDur
				m.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())
				m.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumSize()))
				m.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumSize()))
				m.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumKVS()))
				m.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumKVS()))
			}
			return nil
		}()
		if err != nil {
			return
		}
		dataSynced = false

		dataKVs = dataKVs.Clear()
		indexKVs = indexKVs.Clear()

		// Update the table, and save a checkpoint.
		// (the write to the importer is effective immediately, thus update these here)
		// No need to apply a lock since this is the only thread updating `cr.chunk.**`.
		// In local mode, we should write these checkpoints after engine flushed.
		lastOffset := cr.chunk.Chunk.Offset
		cr.chunk.Checksum.Add(dataChecksum)
		cr.chunk.Checksum.Add(indexChecksum)
		cr.chunk.Chunk.Offset = currOffset
		cr.chunk.Chunk.RealOffset = currRealOffset
		cr.chunk.Chunk.PrevRowIDMax = rowID

		if m, ok := metric.FromContext(ctx); ok {
			// value of currOffset comes from parser.pos which increase monotonically. the init value of parser.pos
			// comes from chunk.Chunk.Offset. so it shouldn't happen that currOffset - startOffset < 0.
			// but we met it one time, but cannot reproduce it now, we add this check to make code more robust
			// TODO: reproduce and find the root cause and fix it completely
			var lowOffset, highOffset int64
			if cr.chunk.FileMeta.Compression != mydump.CompressionNone {
				lowOffset, highOffset = startRealOffset, currRealOffset
			} else {
				lowOffset, highOffset = startOffset, currOffset
			}
			delta := highOffset - lowOffset
			if delta >= 0 {
				if cr.chunk.FileMeta.Type == mydump.SourceTypeParquet {
					if currRealOffset > startRealOffset {
						m.BytesCounter.WithLabelValues(metric.StateRestored).Add(float64(currRealOffset - startRealOffset))
					}
					m.RowsCounter.WithLabelValues(metric.StateRestored, t.tableName).Add(float64(delta))
				} else {
					m.BytesCounter.WithLabelValues(metric.StateRestored).Add(float64(delta))
					m.RowsCounter.WithLabelValues(metric.StateRestored, t.tableName).Add(float64(dataChecksum.SumKVS()))
				}
				if rc.status != nil && rc.status.backend == config.BackendTiDB {
					rc.status.FinishedFileSize.Add(delta)
				}
			} else {
				deliverLogger.Error("offset go back", zap.Int64("curr", highOffset),
					zap.Int64("start", lowOffset))
			}
		}

		if currOffset > lastOffset || dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0 {
			// No need to save checkpoint if nothing was delivered.
			dataSynced = cr.maybeSaveCheckpoint(rc, t, engineID, cr.chunk, dataEngine, indexEngine)
		}
		failpoint.Inject("SlowDownWriteRows", func() {
			deliverLogger.Warn("Slowed down write rows")
			finished := rc.status.FinishedFileSize.Load()
			total := rc.status.TotalFileSize.Load()
			deliverLogger.Warn("PrintStatus Failpoint",
				zap.Int64("finished", finished),
				zap.Int64("total", total))
		})
		failpoint.Inject("FailAfterWriteRows", nil)
		// TODO: for local backend, we may save checkpoint more frequently, e.g. after written
		// 10GB kv pairs to data engine, we can do a flush for both data & index engine, then we
		// can safely update current checkpoint.

		failpoint.Inject("LocalBackendSaveCheckpoint", func() {
			if !isLocalBackend(rc.cfg) && (dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0) {
				// No need to save checkpoint if nothing was delivered.
				saveCheckpoint(rc, t, engineID, cr.chunk)
			}
		})
	}

	return
}

func (*chunkProcessor) maybeSaveCheckpoint(
	rc *Controller,
	t *TableImporter,
	engineID int32,
	chunk *checkpoints.ChunkCheckpoint,
	data, index backend.EngineWriter,
) bool {
	if data.IsSynced() && index.IsSynced() {
		saveCheckpoint(rc, t, engineID, chunk)
		return true
	}
	return false
}

func (cr *chunkProcessor) close() {
	_ = cr.parser.Close()
}
