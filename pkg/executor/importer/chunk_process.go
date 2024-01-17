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
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/tikv"
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

type deliveredRow struct {
	kvs *kv.Pairs // if kvs is nil, this indicated we've got the last message.
	// offset is the end offset in data file after encode this row.
	offset int64
}

type deliverKVBatch struct {
	dataKVs  kv.Pairs
	indexKVs kv.Pairs

	dataChecksum  *verify.KVChecksum
	indexChecksum *verify.KVChecksum

	codec tikv.Codec
}

func newDeliverKVBatch(codec tikv.Codec) *deliverKVBatch {
	return &deliverKVBatch{
		dataChecksum:  verify.NewKVChecksumWithKeyspace(codec),
		indexChecksum: verify.NewKVChecksumWithKeyspace(codec),
		codec:         codec,
	}
}

func (b *deliverKVBatch) reset() {
	b.dataKVs.Clear()
	b.indexKVs.Clear()
	b.dataChecksum = verify.NewKVChecksumWithKeyspace(b.codec)
	b.indexChecksum = verify.NewKVChecksumWithKeyspace(b.codec)
}

func (b *deliverKVBatch) size() uint64 {
	return b.dataChecksum.SumSize() + b.indexChecksum.SumSize()
}

func (b *deliverKVBatch) add(kvs *kv.Pairs) {
	for _, pair := range kvs.Pairs {
		if tablecodec.IsRecordKey(pair.Key) {
			b.dataKVs.Pairs = append(b.dataKVs.Pairs, pair)
			b.dataChecksum.UpdateOne(pair)
		} else {
			b.indexKVs.Pairs = append(b.indexKVs.Pairs, pair)
			b.indexChecksum.UpdateOne(pair)
		}
	}

	// the related buf is shared, so we only need to set it into one of the kvs so it can be released
	if kvs.BytesBuf != nil {
		b.dataKVs.BytesBuf = kvs.BytesBuf
		b.dataKVs.MemBuf = kvs.MemBuf
	}
}

type chunkEncoder interface {
	init() error
	encodeLoop(ctx context.Context) error
	summaryFields() []zap.Field
}

// fileChunkEncoder encode data chunk(either a data file or part of a file).
type fileChunkEncoder struct {
	parser    mydump.Parser
	chunkInfo *checkpoints.ChunkCheckpoint
	logger    *zap.Logger
	encoder   KVEncoder
	kvCodec   tikv.Codec
	sendFn    func(ctx context.Context, kvs []deliveredRow) error

	// startOffset is the offset of current source file reader. it might be
	// larger than the pos that has been parsed due to reader buffering.
	// some rows before startOffset might be skipped if skip_rows > 0.
	startOffset int64
	// total duration takes by read/encode/deliver.
	readTotalDur   time.Duration
	encodeTotalDur time.Duration
}

var _ chunkEncoder = (*fileChunkEncoder)(nil)

func (p *fileChunkEncoder) init() error {
	// we might skip N rows or start from checkpoint
	offset, err := p.parser.ScannedPos()
	if err != nil {
		return errors.Trace(err)
	}
	p.startOffset = offset
	return nil
}

func (p *fileChunkEncoder) encodeLoop(ctx context.Context) error {
	var err error
	reachEOF := false
	prevOffset, currOffset := p.startOffset, p.startOffset

	var encodedBytesCounter, encodedRowsCounter prometheus.Counter
	metrics, _ := metric.GetCommonMetric(ctx)
	if metrics != nil {
		encodedBytesCounter = metrics.BytesCounter.WithLabelValues(metric.StateRestored)
		// table name doesn't matter here, all those metrics will have task-id label.
		encodedRowsCounter = metrics.RowsCounter.WithLabelValues(metric.StateRestored, "")
	}

	for !reachEOF {
		readPos, _ := p.parser.Pos()
		if readPos >= p.chunkInfo.Chunk.EndOffset {
			break
		}
		var readDur, encodeDur time.Duration
		canDeliver := false
		rowBatch := make([]deliveredRow, 0, MinDeliverRowCnt)
		var rowCount, kvSize uint64
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			err = p.parser.ReadRow()
			readPos, _ = p.parser.Pos()
			// todo: we can implement a ScannedPos which don't return error, will change it later.
			currOffset, _ = p.parser.ScannedPos()

			switch errors.Cause(err) {
			case nil:
			case io.EOF:
				reachEOF = true
				break outLoop
			default:
				return common.ErrEncodeKV.Wrap(err).GenWithStackByArgs(p.chunkInfo.GetKey(), currOffset)
			}
			readDur += time.Since(readDurStart)
			encodeDurStart := time.Now()
			lastRow := p.parser.LastRow()
			// sql -> kv
			kvs, encodeErr := p.encoder.Encode(lastRow.Row, lastRow.RowID)
			encodeDur += time.Since(encodeDurStart)

			if encodeErr != nil {
				// todo: record and ignore encode error if user set max-errors param
				err = common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(p.chunkInfo.GetKey(), currOffset)
			}
			p.parser.RecycleRow(lastRow)

			if err != nil {
				return err
			}

			rowBatch = append(rowBatch, deliveredRow{kvs: kvs, offset: currOffset})
			kvSize += kvs.Size()
			rowCount++
			// pebble cannot allow > 4.0G kv in one batch.
			// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
			// so add this check.
			if kvSize >= MinDeliverBytes || len(rowBatch) >= MinDeliverRowCnt || readPos == p.chunkInfo.Chunk.EndOffset {
				canDeliver = true
			}
		}

		delta := currOffset - prevOffset
		prevOffset = currOffset

		p.encodeTotalDur += encodeDur
		p.readTotalDur += readDur
		if metrics != nil {
			metrics.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
			metrics.RowReadSecondsHistogram.Observe(readDur.Seconds())
			// if we're using split_file, this metric might larger than total
			// source file size, as the offset we're using is the reader offset,
			// not parser offset, and we'll buffer data.
			encodedBytesCounter.Add(float64(delta))
			encodedRowsCounter.Add(float64(rowCount))
		}

		if len(rowBatch) > 0 {
			if err = p.sendFn(ctx, rowBatch); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *fileChunkEncoder) summaryFields() []zap.Field {
	return []zap.Field{
		zap.Duration("readDur", p.readTotalDur),
		zap.Duration("encodeDur", p.encodeTotalDur),
	}
}

// ChunkProcessor is used to process a chunk of data, include encode data to KV
// and deliver KV to local or global storage.
type ChunkProcessor interface {
	Process(ctx context.Context) error
}

type baseChunkProcessor struct {
	sourceType DataSourceType
	enc        chunkEncoder
	deliver    *dataDeliver
	logger     *zap.Logger
	chunkInfo  *checkpoints.ChunkCheckpoint
}

func (p *baseChunkProcessor) Process(ctx context.Context) (err error) {
	task := log.BeginTask(p.logger, "process chunk")
	defer func() {
		logFields := append(p.enc.summaryFields(), p.deliver.logFields()...)
		logFields = append(logFields, zap.Stringer("type", p.sourceType))
		task.End(zap.ErrorLevel, err, logFields...)
		if metrics, ok := metric.GetCommonMetric(ctx); ok && err == nil {
			metrics.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Inc()
		}
	}()
	if err2 := p.enc.init(); err2 != nil {
		return err2
	}

	group, gCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return p.deliver.deliverLoop(gCtx)
	})
	group.Go(func() error {
		defer p.deliver.encodeDone()
		return p.enc.encodeLoop(gCtx)
	})

	err2 := group.Wait()
	p.chunkInfo.Checksum.Add(&p.deliver.checksum)
	return err2
}

// NewFileChunkProcessor creates a new local sort chunk processor.
// exported for test.
func NewFileChunkProcessor(
	parser mydump.Parser,
	encoder KVEncoder,
	kvCodec tikv.Codec,
	chunk *checkpoints.ChunkCheckpoint,
	logger *zap.Logger,
	diskQuotaLock *syncutil.RWMutex,
	dataWriter backend.EngineWriter,
	indexWriter backend.EngineWriter,
) ChunkProcessor {
	chunkLogger := logger.With(zap.String("key", chunk.GetKey()))
	deliver := &dataDeliver{
		logger:        chunkLogger,
		kvCodec:       kvCodec,
		diskQuotaLock: diskQuotaLock,
		kvsCh:         make(chan []deliveredRow, maxKVQueueSize),
		dataWriter:    dataWriter,
		indexWriter:   indexWriter,
	}
	return &baseChunkProcessor{
		sourceType: DataSourceTypeFile,
		deliver:    deliver,
		enc: &fileChunkEncoder{
			parser:    parser,
			chunkInfo: chunk,
			logger:    chunkLogger,
			encoder:   encoder,
			kvCodec:   kvCodec,
			sendFn:    deliver.sendEncodedData,
		},
		logger:    chunkLogger,
		chunkInfo: chunk,
	}
}

type dataDeliver struct {
	logger        *zap.Logger
	kvCodec       tikv.Codec
	kvsCh         chan []deliveredRow
	diskQuotaLock *syncutil.RWMutex
	dataWriter    backend.EngineWriter
	indexWriter   backend.EngineWriter

	checksum        verify.KVChecksum
	deliverTotalDur time.Duration
}

func (p *dataDeliver) encodeDone() {
	close(p.kvsCh)
}

func (p *dataDeliver) sendEncodedData(ctx context.Context, kvs []deliveredRow) error {
	select {
	case p.kvsCh <- kvs:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *dataDeliver) deliverLoop(ctx context.Context) error {
	kvBatch := newDeliverKVBatch(p.kvCodec)

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
	outer:
		for kvBatch.size() < MinDeliverBytes {
			select {
			case kvPacket, ok := <-p.kvsCh:
				if !ok {
					break outer
				}
				for _, row := range kvPacket {
					kvBatch.add(row.kvs)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if kvBatch.size() == 0 {
			break
		}

		err := func() error {
			p.diskQuotaLock.RLock()
			defer p.diskQuotaLock.RUnlock()

			start := time.Now()
			if err := p.dataWriter.AppendRows(ctx, nil, &kvBatch.dataKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to data engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}
			if err := p.indexWriter.AppendRows(ctx, nil, &kvBatch.indexKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to index engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}

			deliverDur := time.Since(start)
			p.deliverTotalDur += deliverDur
			if metrics != nil {
				metrics.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())
				dataKVBytesHist.Observe(float64(kvBatch.dataChecksum.SumSize()))
				indexKVBytesHist.Observe(float64(kvBatch.indexChecksum.SumSize()))
				dataKVPairsHist.Observe(float64(kvBatch.dataChecksum.SumKVS()))
				indexKVPairsHist.Observe(float64(kvBatch.indexChecksum.SumKVS()))
			}
			return nil
		}()
		if err != nil {
			return err
		}

		if metrics != nil {
			deliverBytesCounter.Add(float64(kvBatch.size()))
		}

		p.checksum.Add(kvBatch.dataChecksum)
		p.checksum.Add(kvBatch.indexChecksum)

		kvBatch.reset()
	}

	return nil
}

func (p *dataDeliver) logFields() []zap.Field {
	return []zap.Field{
		zap.Duration("deliverDur", p.deliverTotalDur),
		zap.Object("checksum", &p.checksum),
	}
}

// fileChunkEncoder encode data chunk(either a data file or part of a file).
type queryChunkEncoder struct {
	rowCh     chan QueryRow
	chunkInfo *checkpoints.ChunkCheckpoint
	logger    *zap.Logger
	encoder   KVEncoder
	sendFn    func(ctx context.Context, kvs []deliveredRow) error

	// total duration takes by read/encode/deliver.
	readTotalDur   time.Duration
	encodeTotalDur time.Duration
}

var _ chunkEncoder = (*queryChunkEncoder)(nil)

func (*queryChunkEncoder) init() error {
	return nil
}

// TODO logic is very similar to fileChunkEncoder, consider merge them.
func (e *queryChunkEncoder) encodeLoop(ctx context.Context) error {
	var err error
	reachEOF := false
	var encodedRowsCounter prometheus.Counter
	metrics, _ := metric.GetCommonMetric(ctx)
	if metrics != nil {
		// table name doesn't matter here, all those metrics will have task-id label.
		encodedRowsCounter = metrics.RowsCounter.WithLabelValues(metric.StateRestored, "")
	}

	for !reachEOF {
		var readDur, encodeDur time.Duration
		canDeliver := false
		rowBatch := make([]deliveredRow, 0, MinDeliverRowCnt)
		var rowCount, kvSize uint64
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			var (
				lastRow QueryRow
				rowID   int64
				ok      bool
			)
			select {
			case lastRow, ok = <-e.rowCh:
				if !ok {
					reachEOF = true
					break outLoop
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			readDur += time.Since(readDurStart)
			encodeDurStart := time.Now()
			// sql -> kv
			kvs, encodeErr := e.encoder.Encode(lastRow.Data, lastRow.ID)
			encodeDur += time.Since(encodeDurStart)

			if encodeErr != nil {
				err = common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(e.chunkInfo.GetKey(), rowID)
			}
			if err != nil {
				return err
			}

			rowBatch = append(rowBatch, deliveredRow{kvs: kvs})
			kvSize += kvs.Size()
			rowCount++
			// pebble cannot allow > 4.0G kv in one batch.
			// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
			// so add this check.
			if kvSize >= MinDeliverBytes || len(rowBatch) >= MinDeliverRowCnt {
				canDeliver = true
			}
		}

		e.encodeTotalDur += encodeDur
		e.readTotalDur += readDur
		if metrics != nil {
			metrics.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
			metrics.RowReadSecondsHistogram.Observe(readDur.Seconds())
			encodedRowsCounter.Add(float64(rowCount))
		}

		if len(rowBatch) > 0 {
			if err = e.sendFn(ctx, rowBatch); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *queryChunkEncoder) summaryFields() []zap.Field {
	return []zap.Field{
		zap.Duration("readDur", e.readTotalDur),
		zap.Duration("encodeDur", e.encodeTotalDur),
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
	kvCodec tikv.Codec,
	chunk *checkpoints.ChunkCheckpoint,
	logger *zap.Logger,
	diskQuotaLock *syncutil.RWMutex,
	dataWriter backend.EngineWriter,
	indexWriter backend.EngineWriter,
) ChunkProcessor {
	chunkLogger := logger.With(zap.String("key", chunk.GetKey()))
	deliver := &dataDeliver{
		logger:        chunkLogger,
		kvCodec:       kvCodec,
		diskQuotaLock: diskQuotaLock,
		kvsCh:         make(chan []deliveredRow, maxKVQueueSize),
		dataWriter:    dataWriter,
		indexWriter:   indexWriter,
	}
	return &baseChunkProcessor{
		sourceType: DataSourceTypeQuery,
		deliver:    deliver,
		enc: &queryChunkEncoder{
			rowCh:     rowCh,
			chunkInfo: chunk,
			logger:    chunkLogger,
			encoder:   encoder,
			sendFn:    deliver.sendEncodedData,
		},
		logger:    chunkLogger,
		chunkInfo: chunk,
	}
}

// IndexRouteWriter is a writer for index when using global sort.
// we route kvs of different index to different writer in order to make
// merge sort easier, else kv data of all subtasks will all be overlapped.
//
// drawback of doing this is that the number of writers need to open will be
// index-count * encode-concurrency, when the table has many indexes, and each
// writer will take 256MiB buffer on default.
// this will take a lot of memory, or even OOM.
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
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}
	for _, item := range kvs {
		indexID, err := tablecodec.DecodeIndexID(item.Key)
		if err != nil {
			return errors.Trace(err)
		}
		writer, ok := w.writers[indexID]
		if !ok {
			writer = w.writerFactory(indexID)
			w.writers[indexID] = writer
		}
		if err = writer.WriteRow(ctx, item.Key, item.Val, nil); err != nil {
			return errors.Trace(err)
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
