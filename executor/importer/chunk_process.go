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
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// constants, make it a variable for test
var (
	maxKVQueueSize         = 32             // Cache at most this number of rows before blocking the encode loop
	MinDeliverBytes uint64 = 96 * units.KiB // 96 KB (data + index). batch at least this amount of bytes to reduce number of messages
	// see default for tikv-importer.max-kv-pairs
	MinDeliverRowCnt = 4096
)

type deliveredRow struct {
	kvs *kv.Pairs // if kvs is nil, this indicated we've got the last message.
	// end offset in data file after encode this row.
	offset int64
}

type deliverResult struct {
	totalDur time.Duration
	err      error
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
	kvsCh       chan []deliveredRow
	dataWriter  backend.EngineWriter
	indexWriter backend.EngineWriter

	checksum    verify.KVChecksum
	encoder     kvEncoder
	kvCodec     tikv.Codec
	progress    *asyncloaddata.Progress
	startOffset int64
}

func (p *chunkProcessor) process(ctx context.Context) error {
	if err := p.initProgress(); err != nil {
		return err
	}
	// todo: use error group pattern to simplify the code
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

func (p *chunkProcessor) initProgress() error {
	// we might skip N rows or start from checkpoint
	offset, err := p.parser.ScannedPos()
	if err != nil {
		return errors.Trace(err)
	}
	p.progress.EncodeFileSize.Add(offset)
	p.startOffset = offset
	return nil
}

func (p *chunkProcessor) encodeLoop(ctx context.Context, deliverCompleteCh <-chan deliverResult) error {
	defer close(p.kvsCh)

	send := func(kvs []deliveredRow) error {
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
		var readDur, encodeDur time.Duration
		canDeliver := false
		rowBatch := make([]deliveredRow, 0, MinDeliverRowCnt)
		var newOffset int64
		var kvSize uint64
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			err = p.parser.ReadRow()
			// todo: we can implement a ScannedPos which don't return error, will change it later.
			newOffset, _ = p.parser.ScannedPos()

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

			rowBatch = append(rowBatch, deliveredRow{kvs: kvs, offset: newOffset})
			kvSize += kvs.Size()
			// pebble cannot allow > 4.0G kv in one batch.
			// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
			// so add this check.
			if kvSize >= MinDeliverBytes || len(rowBatch) >= MinDeliverRowCnt {
				canDeliver = true
				kvSize = 0
			}
		}

		if len(rowBatch) > 0 {
			if err = send(rowBatch); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *chunkProcessor) deliverLoop(ctx context.Context) error {
	kvBatch := newDeliverKVBatch(p.kvCodec)

	prevOffset, currOffset := p.startOffset, p.startOffset
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
					currOffset = row.offset
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if kvBatch.size() == 0 {
			break
		}

		err := func() error {
			// todo: disk quota related code from lightning, removed temporary
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
			return nil
		}()
		if err != nil {
			return err
		}

		p.progress.EncodeFileSize.Add(currOffset - prevOffset)
		prevOffset = currOffset

		p.chunkInfo.Checksum.Add(kvBatch.dataChecksum)
		p.chunkInfo.Checksum.Add(kvBatch.indexChecksum)

		kvBatch.reset()
	}

	return nil
}

func (p *chunkProcessor) close(ctx context.Context) {
	if err2 := p.parser.Close(); err2 != nil {
		p.logger.Error("failed to close parser", zap.Error(err2))
	}
	if err2 := p.encoder.Close(); err2 != nil {
		p.logger.Error("failed to close encoder", zap.Error(err2))
	}
	if _, err2 := p.dataWriter.Close(ctx); err2 != nil {
		p.logger.Error("failed to close data writer", zap.Error(err2))
	}
	if _, err2 := p.indexWriter.Close(ctx); err2 != nil {
		p.logger.Error("failed to close data writer", zap.Error(err2))
	}
}
