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
	"sync"
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
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
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

// chunkEncoder encode data chunk(either a data file or part of a file).
type chunkEncoder struct {
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
	metrics        *metric.Common
}

func (p *chunkEncoder) initProgress() error {
	// we might skip N rows or start from checkpoint
	offset, err := p.parser.ScannedPos()
	if err != nil {
		return errors.Trace(err)
	}
	p.startOffset = offset
	return nil
}

func (p *chunkEncoder) encodeLoop(ctx context.Context) error {
	var err error
	reachEOF := false
	prevOffset, currOffset := p.startOffset, p.startOffset

	var encodedBytesCounter, encodedRowsCounter prometheus.Counter
	if p.metrics != nil {
		encodedBytesCounter = p.metrics.BytesCounter.WithLabelValues(metric.StateRestored)
		// table name doesn't matter here, all those metrics will have task-id label.
		encodedRowsCounter = p.metrics.RowsCounter.WithLabelValues(metric.StateRestored, "")
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
		if p.metrics != nil {
			p.metrics.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
			p.metrics.RowReadSecondsHistogram.Observe(readDur.Seconds())
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

// ChunkProcessor is used to process a chunk of data, include encode data to KV
// and deliver KV to local or global storage.
type ChunkProcessor interface {
	Process(ctx context.Context) error
}

type baseChunkProcessor struct {
	enc         *chunkEncoder
	logger      *zap.Logger
	kvCodec     tikv.Codec
	deliverLoop func(ctx context.Context) error
	encodeDone  func(ctx context.Context)

	// initialized when Process
	metrics *metric.Common

	checksum        verify.KVChecksum
	deliverTotalDur time.Duration
}

func (p *baseChunkProcessor) Process(ctx context.Context) (err error) {
	task := log.BeginTask(p.logger, "process chunk")
	defer func() {
		task.End(zap.ErrorLevel, err,
			zap.Duration("readDur", p.enc.readTotalDur),
			zap.Duration("encodeDur", p.enc.encodeTotalDur),
			zap.Duration("deliverDur", p.deliverTotalDur),
			zap.Object("checksum", &p.checksum),
		)
		if err == nil && p.metrics != nil {
			p.metrics.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Inc()
		}
	}()
	if err2 := p.enc.initProgress(); err2 != nil {
		return err2
	}

	if metrics, ok := metric.GetCommonMetric(ctx); ok {
		p.enc.metrics = metrics
		p.metrics = p.enc.metrics
	}

	group, gCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return p.deliverLoop(gCtx)
	})
	group.Go(func() error {
		defer p.encodeDone(gCtx)
		return p.enc.encodeLoop(gCtx)
	})

	err2 := group.Wait()
	p.enc.chunkInfo.Checksum.Add(&p.checksum)
	return err2
}

// localSortChunkProcessor encode and sort kv, then write to local storage.
// each chunk processor will have a pair of encode and deliver routine.
type localSortChunkProcessor struct {
	*baseChunkProcessor
	kvsCh         chan []deliveredRow
	diskQuotaLock *syncutil.RWMutex
	dataWriter    backend.EngineWriter
	indexWriter   backend.EngineWriter
}

var _ ChunkProcessor = &localSortChunkProcessor{}

// NewLocalSortChunkProcessor creates a new local sort chunk processor.
// exported for test.
func NewLocalSortChunkProcessor(
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
	cp := &localSortChunkProcessor{
		diskQuotaLock: diskQuotaLock,
		kvsCh:         make(chan []deliveredRow, maxKVQueueSize),
		dataWriter:    dataWriter,
		indexWriter:   indexWriter,
	}
	cp.baseChunkProcessor = &baseChunkProcessor{
		enc: &chunkEncoder{
			parser:    parser,
			chunkInfo: chunk,
			logger:    chunkLogger,
			encoder:   encoder,
			kvCodec:   kvCodec,
			sendFn:    cp.sendEncodedData,
		},
		logger:      chunkLogger,
		kvCodec:     kvCodec,
		deliverLoop: cp.deliverLoop,
		encodeDone:  cp.encodeDone,
	}
	return cp
}

func (p *localSortChunkProcessor) encodeDone(context.Context) {
	close(p.kvsCh)
}

func (p *localSortChunkProcessor) sendEncodedData(ctx context.Context, kvs []deliveredRow) error {
	select {
	case p.kvsCh <- kvs:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *localSortChunkProcessor) deliverLoop(ctx context.Context) error {
	kvBatch := newDeliverKVBatch(p.kvCodec)

	var (
		dataKVBytesHist, indexKVBytesHist prometheus.Observer
		dataKVPairsHist, indexKVPairsHist prometheus.Observer
		deliverBytesCounter               prometheus.Counter
	)
	if p.metrics != nil {
		dataKVBytesHist = p.metrics.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData)
		indexKVBytesHist = p.metrics.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex)
		dataKVPairsHist = p.metrics.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData)
		indexKVPairsHist = p.metrics.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex)
		deliverBytesCounter = p.metrics.BytesCounter.WithLabelValues(metric.StateRestoreWritten)
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
			if p.metrics != nil {
				p.metrics.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())
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

		if p.metrics != nil {
			deliverBytesCounter.Add(float64(kvBatch.size()))
		}

		p.checksum.Add(kvBatch.dataChecksum)
		p.checksum.Add(kvBatch.indexChecksum)

		kvBatch.reset()
	}

	return nil
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
	// this writer and all wrappedWriters are shared by all deliver routines,
	// so we need to synchronize them.
	sync.RWMutex
	writers       map[int64]*wrappedWriter
	logger        *zap.Logger
	writerFactory func(int64) *external.Writer
}

type wrappedWriter struct {
	sync.Mutex
	*external.Writer
}

func (w *wrappedWriter) WriteRow(ctx context.Context, idxKey, idxVal []byte, handle tidbkv.Handle) error {
	w.Lock()
	defer w.Unlock()
	return w.Writer.WriteRow(ctx, idxKey, idxVal, handle)
}

func (w *wrappedWriter) Close(ctx context.Context) error {
	w.Lock()
	defer w.Unlock()
	return w.Writer.Close(ctx)
}

// NewIndexRouteWriter creates a new IndexRouteWriter.
func NewIndexRouteWriter(logger *zap.Logger, writerFactory func(int64) *external.Writer) *IndexRouteWriter {
	return &IndexRouteWriter{
		writers:       make(map[int64]*wrappedWriter),
		logger:        logger,
		writerFactory: writerFactory,
	}
}

func (w *IndexRouteWriter) getWriter(indexID int64) *wrappedWriter {
	w.RLock()
	writer, ok := w.writers[indexID]
	w.RUnlock()
	if ok {
		return writer
	}

	w.Lock()
	defer w.Unlock()
	writer, ok = w.writers[indexID]
	if !ok {
		writer = &wrappedWriter{
			Writer: w.writerFactory(indexID),
		}
		w.writers[indexID] = writer
	}
	return writer
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
		writer := w.getWriter(indexID)
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
	w.Lock()
	defer w.Unlock()
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
