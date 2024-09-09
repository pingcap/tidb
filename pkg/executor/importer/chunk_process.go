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
	"context"
	"io"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// constants, make it a variable for test
var (
	maxKVQueueSize         = 32             // Cache at most this number of rows before blocking the encode loop
	MinDeliverBytes uint64 = 96 * units.KiB // 96 KB (data + index). batch at least this amount of bytes to reduce number of messages
	// MinDeliverRowCnt see default for tikv-importer.max-kv-pairs
	MinDeliverRowCnt = 4096
)

type rowToEncode struct {
	row   []types.Datum
	rowID int64
	// endOffset represents the offset after the current row in encode reader.
	// it will be negative if the data source is not file.
	endOffset int64
	resetFn   func()
}

type encodeReaderFn func(ctx context.Context) (data rowToEncode, closed bool, err error)

// parserEncodeReader wraps a mydump.Parser as a encodeReaderFn.
func parserEncodeReader(parser mydump.Parser, endOffset int64, filename string) encodeReaderFn {
	return func(context.Context) (data rowToEncode, closed bool, err error) {
		readPos, _ := parser.Pos()
		if readPos >= endOffset {
			closed = true
			return
		}

		err = parser.ReadRow()
		// todo: we can implement a ScannedPos which don't return error, will change it later.
		currOffset, _ := parser.ScannedPos()
		switch errors.Cause(err) {
		case nil:
		case io.EOF:
			closed = true
			err = nil
			return
		default:
			err = common.ErrEncodeKV.Wrap(err).GenWithStackByArgs(filename, currOffset)
			return
		}
		lastRow := parser.LastRow()
		data = rowToEncode{
			row:       lastRow.Row,
			rowID:     lastRow.RowID,
			endOffset: currOffset,
			resetFn:   func() { parser.RecycleRow(lastRow) },
		}
		return
	}
}

// queryRowEncodeReader wraps a QueryRow channel as a encodeReaderFn.
func queryRowEncodeReader(rowCh <-chan QueryRow) encodeReaderFn {
	return func(ctx context.Context) (data rowToEncode, closed bool, err error) {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case row, ok := <-rowCh:
			if !ok {
				closed = true
				return
			}
			data = rowToEncode{
				row:       row.Data,
				rowID:     row.ID,
				endOffset: -1,
				resetFn:   func() {},
			}
			return
		}
	}
}

type encodedKVGroupBatch struct {
	dataKVs  []common.KvPair
	indexKVs map[int64][]common.KvPair // indexID -> pairs

	bytesBuf *kv.BytesBuf
	memBuf   *kv.MemBuf

	groupChecksum *verify.KVGroupChecksum
}

func (b *encodedKVGroupBatch) reset() {
	if b.memBuf == nil {
		return
	}
	// mimic kv.Pairs.Clear
	b.memBuf.Recycle(b.bytesBuf)
	b.bytesBuf = nil
	b.memBuf = nil
}

func newEncodedKVGroupBatch(keyspace []byte) *encodedKVGroupBatch {
	return &encodedKVGroupBatch{
		indexKVs:      make(map[int64][]common.KvPair, 8),
		groupChecksum: verify.NewKVGroupChecksumWithKeyspace(keyspace),
	}
}

// add must be called with `kvs` from the same session for a encodedKVGroupBatch.
func (b *encodedKVGroupBatch) add(kvs *kv.Pairs) error {
	for _, pair := range kvs.Pairs {
		if tablecodec.IsRecordKey(pair.Key) {
			b.dataKVs = append(b.dataKVs, pair)
			b.groupChecksum.UpdateOneDataKV(pair)
		} else {
			indexID, err := tablecodec.DecodeIndexID(pair.Key)
			if err != nil {
				return errors.Trace(err)
			}
			b.indexKVs[indexID] = append(b.indexKVs[indexID], pair)
			b.groupChecksum.UpdateOneIndexKV(indexID, pair)
		}
	}

	// the related buf is shared, so we only need to record any one of them.
	if b.bytesBuf == nil && kvs.BytesBuf != nil {
		b.bytesBuf = kvs.BytesBuf
		b.memBuf = kvs.MemBuf
	}
	return nil
}

// chunkEncoder encodes data from readFn and sends encoded data to sendFn.
type chunkEncoder struct {
	readFn encodeReaderFn
	offset int64
	sendFn func(ctx context.Context, batch *encodedKVGroupBatch) error

	chunkName string
	logger    *zap.Logger
	encoder   KVEncoder
	keyspace  []byte

	// total duration takes by read/encode.
	readTotalDur   time.Duration
	encodeTotalDur time.Duration

	groupChecksum *verify.KVGroupChecksum
}

func newChunkEncoder(
	chunkName string,
	readFn encodeReaderFn,
	offset int64,
	sendFn func(ctx context.Context, batch *encodedKVGroupBatch) error,
	logger *zap.Logger,
	encoder KVEncoder,
	keyspace []byte,
) *chunkEncoder {
	return &chunkEncoder{
		chunkName:     chunkName,
		readFn:        readFn,
		offset:        offset,
		sendFn:        sendFn,
		logger:        logger,
		encoder:       encoder,
		keyspace:      keyspace,
		groupChecksum: verify.NewKVGroupChecksumWithKeyspace(keyspace),
	}
}

func (p *chunkEncoder) encodeLoop(ctx context.Context) error {
	var (
		encodedBytesCounter, encodedRowsCounter prometheus.Counter
		readDur, encodeDur                      time.Duration
		rowCount                                int
		rowBatch                                = make([]*kv.Pairs, 0, MinDeliverRowCnt)
		rowBatchByteSize                        uint64
		currOffset                              int64
	)
	metrics, _ := metric.GetCommonMetric(ctx)
	if metrics != nil {
		encodedBytesCounter = metrics.BytesCounter.WithLabelValues(metric.StateRestored)
		// table name doesn't matter here, all those metrics will have task-id label.
		encodedRowsCounter = metrics.RowsCounter.WithLabelValues(metric.StateRestored, "")
	}

	recordSendReset := func() error {
		if len(rowBatch) == 0 {
			return nil
		}

		if currOffset >= 0 && metrics != nil {
			delta := currOffset - p.offset
			p.offset = currOffset
			// if we're using split_file, this metric might larger than total
			// source file size, as the offset we're using is the reader offset,
			// not parser offset, and we'll buffer data.
			encodedBytesCounter.Add(float64(delta))
		}

		if metrics != nil {
			metrics.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
			metrics.RowReadSecondsHistogram.Observe(readDur.Seconds())
			encodedRowsCounter.Add(float64(rowCount))
		}
		p.encodeTotalDur += encodeDur
		p.readTotalDur += readDur

		kvGroupBatch := newEncodedKVGroupBatch(p.keyspace)

		for _, kvs := range rowBatch {
			if err := kvGroupBatch.add(kvs); err != nil {
				return errors.Trace(err)
			}
		}

		p.groupChecksum.Add(kvGroupBatch.groupChecksum)

		if err := p.sendFn(ctx, kvGroupBatch); err != nil {
			return err
		}

		// the ownership of rowBatch is transferred to the receiver of sendFn, we should
		// not touch it anymore.
		rowBatch = make([]*kv.Pairs, 0, MinDeliverRowCnt)
		rowBatchByteSize = 0
		rowCount = 0
		readDur = 0
		encodeDur = 0
		return nil
	}

	for {
		readDurStart := time.Now()
		data, closed, err := p.readFn(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if closed {
			break
		}
		readDur += time.Since(readDurStart)

		encodeDurStart := time.Now()
		kvs, encodeErr := p.encoder.Encode(data.row, data.rowID)
		currOffset = data.endOffset
		data.resetFn()
		if encodeErr != nil {
			// todo: record and ignore encode error if user set max-errors param
			return common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(p.chunkName, data.endOffset)
		}
		encodeDur += time.Since(encodeDurStart)

		rowCount++
		rowBatch = append(rowBatch, kvs)
		rowBatchByteSize += kvs.Size()
		// pebble cannot allow > 4.0G kv in one batch.
		// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
		// so add this check.
		if rowBatchByteSize >= MinDeliverBytes || len(rowBatch) >= MinDeliverRowCnt {
			if err := recordSendReset(); err != nil {
				return err
			}
		}
	}

	return recordSendReset()
}

func (p *chunkEncoder) summaryFields() []zap.Field {
	mergedChecksum := p.groupChecksum.MergedChecksum()
	return []zap.Field{
		zap.Duration("readDur", p.readTotalDur),
		zap.Duration("encodeDur", p.encodeTotalDur),
		zap.Object("checksum", &mergedChecksum),
	}
}

// ChunkProcessor is used to process a chunk of data, include encode data to KV
// and deliver KV to local or global storage.
type ChunkProcessor interface {
	Process(ctx context.Context) error
}

type baseChunkProcessor struct {
	sourceType    DataSourceType
	enc           *chunkEncoder
	deliver       *dataDeliver
	logger        *zap.Logger
	groupChecksum *verify.KVGroupChecksum
}

func (p *baseChunkProcessor) Process(ctx context.Context) (err error) {
	task := log.BeginTask(p.logger, "process chunk")
	defer func() {
		logFields := append(p.enc.summaryFields(), p.deliver.summaryFields()...)
		logFields = append(logFields, zap.Stringer("type", p.sourceType))
		task.End(zap.ErrorLevel, err, logFields...)
		if metrics, ok := metric.GetCommonMetric(ctx); ok && err == nil {
			metrics.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Inc()
		}
	}()

	group, gCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return p.deliver.deliverLoop(gCtx)
	})
	group.Go(func() error {
		defer p.deliver.encodeDone()
		return p.enc.encodeLoop(gCtx)
	})

	err2 := group.Wait()
	// in some unit tests it's nil
	if c := p.groupChecksum; c != nil {
		c.Add(p.enc.groupChecksum)
	}
	return err2
}

// NewFileChunkProcessor creates a new local sort chunk processor.
// exported for test.
func NewFileChunkProcessor(
	parser mydump.Parser,
	encoder KVEncoder,
	keyspace []byte,
	chunk *checkpoints.ChunkCheckpoint,
	logger *zap.Logger,
	diskQuotaLock *syncutil.RWMutex,
	dataWriter backend.EngineWriter,
	indexWriter backend.EngineWriter,
	groupChecksum *verify.KVGroupChecksum,
) ChunkProcessor {
	chunkLogger := logger.With(zap.String("key", chunk.GetKey()))
	deliver := &dataDeliver{
		logger:        chunkLogger,
		diskQuotaLock: diskQuotaLock,
		kvBatch:       make(chan *encodedKVGroupBatch, maxKVQueueSize),
		dataWriter:    dataWriter,
		indexWriter:   indexWriter,
	}
	return &baseChunkProcessor{
		sourceType: DataSourceTypeFile,
		deliver:    deliver,
		enc: newChunkEncoder(
			chunk.GetKey(),
			parserEncodeReader(parser, chunk.Chunk.EndOffset, chunk.GetKey()),
			chunk.Chunk.Offset,
			deliver.sendEncodedData,
			chunkLogger,
			encoder,
			keyspace,
		),
		logger:        chunkLogger,
		groupChecksum: groupChecksum,
	}
}

type dataDeliver struct {
	logger        *zap.Logger
	kvBatch       chan *encodedKVGroupBatch
	diskQuotaLock *syncutil.RWMutex
	dataWriter    backend.EngineWriter
	indexWriter   backend.EngineWriter

	deliverTotalDur time.Duration
}

func (p *dataDeliver) encodeDone() {
	close(p.kvBatch)
}

func (p *dataDeliver) sendEncodedData(ctx context.Context, batch *encodedKVGroupBatch) error {
	select {
	case p.kvBatch <- batch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *dataDeliver) deliverLoop(ctx context.Context) error {
	var (
		dataKVBytesHist, indexKVBytesHist prometheus.Observer
		dataKVPairsHist, indexKVPairsHist prometheus.Observer
		deliverBytesCounter               prometheus.Counter
	)

	metrics, _ := metric.GetCommonMetric(ctx)
	if metrics != nil {
		dataKVBytesHist = metrics.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData)
		indexKVBytesHist = metrics.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex)
		dataKVPairsHist = metrics.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData)
		indexKVPairsHist = metrics.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex)
		deliverBytesCounter = metrics.BytesCounter.WithLabelValues(metric.StateRestoreWritten)
	}

	for {
		var (
			kvBatch *encodedKVGroupBatch
			ok      bool
		)

		select {
		case kvBatch, ok = <-p.kvBatch:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		err := func() error {
			p.diskQuotaLock.RLock()
			defer p.diskQuotaLock.RUnlock()

			start := time.Now()
			if err := p.dataWriter.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvBatch.dataKVs)); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to data engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}
			if err := p.indexWriter.AppendRows(ctx, nil, kv.GroupedPairs(kvBatch.indexKVs)); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to index engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}

			deliverDur := time.Since(start)
			p.deliverTotalDur += deliverDur
			if metrics != nil {
				metrics.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())

				dataSize, indexSize := kvBatch.groupChecksum.DataAndIndexSumSize()
				dataKVCnt, indexKVCnt := kvBatch.groupChecksum.DataAndIndexSumKVS()
				dataKVBytesHist.Observe(float64(dataSize))
				dataKVPairsHist.Observe(float64(dataKVCnt))
				indexKVBytesHist.Observe(float64(indexSize))
				indexKVPairsHist.Observe(float64(indexKVCnt))
				deliverBytesCounter.Add(float64(dataSize + indexSize))
			}
			return nil
		}()
		if err != nil {
			return err
		}

		kvBatch.reset()
	}
}

func (p *dataDeliver) summaryFields() []zap.Field {
	return []zap.Field{
		zap.Duration("deliverDur", p.deliverTotalDur),
	}
}

// QueryRow is a row from query result.
type QueryRow struct {
	ID   int64
	Data []types.Datum
}

func newQueryChunkProcessor(
	rowCh chan QueryRow,
	encoder KVEncoder,
	keyspace []byte,
	logger *zap.Logger,
	diskQuotaLock *syncutil.RWMutex,
	dataWriter backend.EngineWriter,
	indexWriter backend.EngineWriter,
	groupChecksum *verify.KVGroupChecksum,
) ChunkProcessor {
	chunkName := "import-from-select"
	chunkLogger := logger.With(zap.String("key", chunkName))
	deliver := &dataDeliver{
		logger:        chunkLogger,
		diskQuotaLock: diskQuotaLock,
		kvBatch:       make(chan *encodedKVGroupBatch, maxKVQueueSize),
		dataWriter:    dataWriter,
		indexWriter:   indexWriter,
	}
	return &baseChunkProcessor{
		sourceType: DataSourceTypeQuery,
		deliver:    deliver,
		enc: newChunkEncoder(
			chunkName,
			queryRowEncodeReader(rowCh),
			-1,
			deliver.sendEncodedData,
			chunkLogger,
			encoder,
			keyspace,
		),
		logger:        chunkLogger,
		groupChecksum: groupChecksum,
	}
}

// IndexRouteWriter is a writer for index when using global sort.
// we route kvs of different index to different writer in order to make
// merge sort easier, else kv data of all subtasks will all be overlapped.
type IndexRouteWriter struct {
	writers       map[int64]*external.Writer
	logger        *zap.Logger
	writerFactory func(int64) *external.Writer
}

// NewIndexRouteWriter creates a new IndexRouteWriter.
func NewIndexRouteWriter(logger *zap.Logger, writerFactory func(int64) *external.Writer) *IndexRouteWriter {
	return &IndexRouteWriter{
		writers:       make(map[int64]*external.Writer),
		logger:        logger,
		writerFactory: writerFactory,
	}
}

// AppendRows implements backend.EngineWriter interface.
func (w *IndexRouteWriter) AppendRows(ctx context.Context, _ []string, rows encode.Rows) error {
	groupedKvs, ok := rows.(kv.GroupedPairs)
	if !ok {
		return errors.Errorf("invalid kv pairs type for IndexRouteWriter: %T", rows)
	}
	for indexID, kvs := range groupedKvs {
		for _, item := range kvs {
			writer, ok := w.writers[indexID]
			if !ok {
				writer = w.writerFactory(indexID)
				w.writers[indexID] = writer
			}
			if err := writer.WriteRow(ctx, item.Key, item.Val, nil); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

// IsSynced implements backend.EngineWriter interface.
func (*IndexRouteWriter) IsSynced() bool {
	return true
}

// Close implements backend.EngineWriter interface.
func (w *IndexRouteWriter) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	var firstErr error
	for _, writer := range w.writers {
		if err := writer.Close(ctx); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			w.logger.Error("close index writer failed", zap.Error(err))
		}
	}
	return nil, firstErr
}
