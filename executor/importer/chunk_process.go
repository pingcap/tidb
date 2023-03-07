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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type chunkInfo struct {
	FilePath string
	Offset   int64
	mydump.SourceFileMeta
}

var (
	maxKVQueueSize         = 32             // Cache at most this number of rows before blocking the encode loop
	minDeliverBytes uint64 = 96 * units.KiB // 96 KB (data + index). batch at least this amount of bytes to reduce number of messages
	// see default for tikv-importer.max-kv-pairs
	minDeliverKVPairCnt = 4096
)

type deliveredKVs struct {
	kvs    *KvPairs // if kvs is nil, this indicated we've got the last message.
	offset int64
	rowID  int64
}

type deliverResult struct {
	totalDur time.Duration
	err      error
}

func firstErr(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

// chunkProcessor process data chunk, it encodes and writes KV to local disk.
type chunkProcessor struct {
	parser      mydump.Parser
	chunkInfo   *checkpoints.ChunkCheckpoint
	logger      *zap.Logger
	kvsCh       chan []deliveredKVs
	dataWriter  *backend.LocalEngineWriter
	indexWriter *backend.LocalEngineWriter

	checksum verify.KVChecksum
	encoder  KVEncoder
}

func (p *chunkProcessor) process(ctx context.Context) error {
	deliverCompleteCh := make(chan deliverResult)
	go func() {
		defer close(deliverCompleteCh)
		err := p.deliverLoop(ctx)
		select {
		case <-ctx.Done():
		case deliverCompleteCh <- deliverResult{err: err}:
		}
	}()

	p.logger.Info("process chunk")

	encodeErr := p.encodeLoop(ctx, deliverCompleteCh)
	var deliverErr error
	select {
	case result, ok := <-deliverCompleteCh:
		if ok {
			deliverErr = result.err
		} else {
			// else, this must cause by ctx cancel
			deliverErr = ctx.Err()
		}
	case <-ctx.Done():
		deliverErr = ctx.Err()
	}
	return errors.Trace(firstErr(encodeErr, deliverErr))
}

func (p *chunkProcessor) encodeLoop(ctx context.Context, deliverCompleteCh <-chan deliverResult) error {
	defer close(p.kvsCh)

	send := func(kvs []deliveredKVs) error {
		select {
		case p.kvsCh <- kvs:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-deliverCompleteCh:
			if result.err == nil && !ok {
				result.err = ctx.Err()
			}
			if result.err == nil {
				result.err = errors.New("unexpected premature fulfillment")
				p.logger.DPanic("unexpected: deliverCompleteCh prematurely fulfilled with no error", zap.Bool("chIsOpen", ok))
			}
			return errors.Trace(result.err)
		}
	}

	var err error
	reachEOF := false
	for !reachEOF {
		offset, _ := p.parser.Pos()

		var readDur, encodeDur time.Duration
		canDeliver := false
		kvPacket := make([]deliveredKVs, 0, minDeliverKVPairCnt)
		var newOffset, rowID int64
		var kvSize uint64
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			err = p.parser.ReadRow()
			newOffset, rowID = p.parser.Pos()

			switch errors.Cause(err) {
			case nil:
			case io.EOF:
				reachEOF = true
				break outLoop
			default:
				return common.ErrEncodeKV.Wrap(err).GenWithStackByArgs(p.chunkInfo.GetKey(), newOffset)
			}
			readDur += time.Since(readDurStart)
			encodeDurStart := time.Now()
			lastRow := p.parser.LastRow()
			// sql -> kv
			kvs, encodeErr := p.encoder.Encode(lastRow.Row, lastRow.RowID)
			encodeDur += time.Since(encodeDurStart)

			if encodeErr != nil {
				// todo: record and ignore encode error if user set max-errors param
				err = common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(p.chunkInfo.GetKey(), newOffset)
			}
			p.parser.RecycleRow(lastRow)

			if err != nil {
				return err
			}

			kvPacket = append(kvPacket, deliveredKVs{kvs: kvs, offset: newOffset, rowID: rowID})
			kvSize += kvs.Size() // todo: this size doesn't include keyspace prefix
			failpoint.Inject("mock-kv-size", func(val failpoint.Value) {
				kvSize += uint64(val.(int))
			})
			// pebble cannot allow > 4.0G kv in one batch.
			// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
			// so add this check.
			if kvSize >= minDeliverBytes || len(kvPacket) >= minDeliverKVPairCnt {
				canDeliver = true
				kvSize = 0
			}
		}
		if m, ok := metric.FromContext(ctx); ok {
			m.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
			m.RowReadSecondsHistogram.Observe(readDur.Seconds())
			m.RowReadBytesHistogram.Observe(float64(newOffset - offset))
		}

		if len(kvPacket) > 0 {
			deliverKvStart := time.Now()
			if err = send(kvPacket); err != nil {
				return err
			}
			if m, ok := metric.FromContext(ctx); ok {
				m.RowKVDeliverSecondsHistogram.Observe(time.Since(deliverKvStart).Seconds())
			}
		}
	}

	return nil
}

type deliverKVBatch struct {
	dataKVs  KvPairs
	indexKVs KvPairs

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

func (b *deliverKVBatch) add(kvs *KvPairs) {
	for _, pair := range kvs.pairs {
		if pair.Key[tablecodec.TableSplitKeyLen+1] == 'r' {
			b.dataKVs.pairs = append(b.dataKVs.pairs, pair)
			b.dataChecksum.UpdateOne(pair)
		} else {
			b.indexKVs.pairs = append(b.indexKVs.pairs, pair)
			b.indexChecksum.UpdateOne(pair)
		}
	}

	// the related buf is shared, so we only need to set it into one of the kvs so it can be released
	if kvs.bytesBuf != nil {
		b.dataKVs.bytesBuf = kvs.bytesBuf
		b.dataKVs.memBuf = kvs.memBuf
	}
}

func (p *chunkProcessor) deliverLoop(ctx context.Context) error {
	var (
		err                     error
		startOffset, currOffset int64
	)
	c := keyspace.CodecV1
	// todo: use keyspace codec from table importer(not done yet)
	kvBatch := newDeliverKVBatch(c)

	for {
		startOffset = currOffset
	outer:
		for kvBatch.size() < minDeliverBytes {
			select {
			case kvPacket, ok := <-p.kvsCh:
				if !ok {
					break outer
				}
				for _, p := range kvPacket {
					kvBatch.add(p.kvs)
					currOffset = p.offset
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if kvBatch.size() == 0 {
			break
		}

		err = func() error {
			// todo: disk quota related code from lightning, removed temporary
			start := time.Now()

			if err = p.dataWriter.WriteRows(ctx, nil, &kvBatch.dataKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to data engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}
			if err = p.indexWriter.WriteRows(ctx, nil, &kvBatch.indexKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to index engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}

			if m, ok := metric.FromContext(ctx); ok {
				deliverDur := time.Since(start)
				m.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())
				m.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(kvBatch.dataChecksum.SumSize()))
				m.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(kvBatch.indexChecksum.SumSize()))
				m.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(kvBatch.dataChecksum.SumKVS()))
				m.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(kvBatch.indexChecksum.SumKVS()))
			}
			return nil
		}()
		if err != nil {
			return err
		}

		kvBatch.reset()

		p.checksum.Add(kvBatch.dataChecksum)
		p.checksum.Add(kvBatch.indexChecksum)

		if m, ok := metric.FromContext(ctx); ok {
			// value of currOffset comes from parser.pos which increase monotonically. the init value of parser.pos
			// comes from chunk.Chunk.Offset. so it shouldn't happen that currOffset - startOffset < 0.
			// but we met it one time, but cannot reproduce it now, we add this check to make code more robust
			// TODO: reproduce and find the root cause and fix it completely
			delta := currOffset - startOffset
			if delta >= 0 {
				m.BytesCounter.WithLabelValues(metric.BytesStateRestored).Add(float64(delta))
			} else {
				p.logger.Warn("offset go back", zap.Int64("curr", currOffset),
					zap.Int64("start", startOffset))
			}
		}
	}

	return nil
}

func (p *chunkProcessor) close(ctx context.Context) {
	if err2 := p.parser.Close(); err2 != nil {
		p.logger.Error("failed to close parser", zap.Error(err2))
	}
	if _, err2 := p.dataWriter.Close(ctx); err2 != nil {
		p.logger.Error("failed to close data writer", zap.Error(err2))
	}
	if _, err2 := p.indexWriter.Close(ctx); err2 != nil {
		p.logger.Error("failed to close data writer", zap.Error(err2))
	}
}
