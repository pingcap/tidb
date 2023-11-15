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
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
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
	kvs *kv.KvPairs // if kvs is nil, this indicated we've got the last message.
	// offset is the end offset in data file after encode this row.
	offset int64
}

type deliverKVBatch struct {
	dataKVs  kv.KvPairs
	indexKVs kv.KvPairs

	dataChecksum  *verify.KVChecksum
	indexChecksum *verify.KVChecksum
}

func newDeliverKVBatch() *deliverKVBatch {
	return &deliverKVBatch{
		dataChecksum:  &verify.KVChecksum{},
		indexChecksum: &verify.KVChecksum{},
	}
}

func (b *deliverKVBatch) reset() {
	b.dataKVs.Clear()
	b.indexKVs.Clear()
	b.dataChecksum = &verify.KVChecksum{}
	b.indexChecksum = &verify.KVChecksum{}
}

func (b *deliverKVBatch) size() uint64 {
	return b.dataChecksum.SumSize() + b.indexChecksum.SumSize()
}

func (b *deliverKVBatch) add(kvs *kv.KvPairs) {
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

// ChunkProcessor is used to process a chunk of data, include encode data to KV
// and deliver KV to local or global storage.
type ChunkProcessor interface {
	Process(ctx context.Context) error
}

type dataDeliver struct {
	logger *zap.Logger

	checksum        verify.KVChecksum
	deliverTotalDur time.Duration

	kvsCh         chan []deliveredRow
	diskQuotaLock *sync.RWMutex
	dataWriter    backend.EngineWriter
	indexWriter   backend.EngineWriter
}

func (p *dataDeliver) encodeDone(context.Context) {
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
	kvBatch := newDeliverKVBatch()

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
			if err := p.dataWriter.AppendRows(ctx, "", nil, &kvBatch.dataKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to data engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}
			if err := p.indexWriter.AppendRows(ctx, "", nil, &kvBatch.indexKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to index engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}

			deliverDur := time.Since(start)
			p.deliverTotalDur += deliverDur
			return nil
		}()
		if err != nil {
			return err
		}

		p.checksum.Add(kvBatch.dataChecksum)
		p.checksum.Add(kvBatch.indexChecksum)

		kvBatch.reset()
	}

	return nil
}

// chunkEncoder encode data chunk(either a data file or part of a file).
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

func (p *queryChunkEncoder) encodeLoop(ctx context.Context) error {
	var err error
	reachEOF := false

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
			case lastRow, ok = <-p.rowCh:
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
			kvs, encodeErr := p.encoder.Encode(lastRow.Data, lastRow.ID)
			encodeDur += time.Since(encodeDurStart)

			if encodeErr != nil {
				err = common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs("", rowID)
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

		p.encodeTotalDur += encodeDur
		p.readTotalDur += readDur

		if len(rowBatch) > 0 {
			if err = p.sendFn(ctx, rowBatch); err != nil {
				return err
			}
		}
	}

	return nil
}

// QueryRow is a row from query result.
type QueryRow struct {
	ID   int64
	Data []types.Datum
}

type queryChunkProcessor struct {
	enc         *queryChunkEncoder
	deliver     *dataDeliver
	logger      *zap.Logger
	deliverLoop func(ctx context.Context) error
	encodeDone  func(ctx context.Context)
}

func newQueryChunkProcessor(
	rowCh chan QueryRow,
	encoder KVEncoder,
	chunk *checkpoints.ChunkCheckpoint,
	logger *zap.Logger,
	diskQuotaLock *sync.RWMutex,
	dataWriter backend.EngineWriter,
	indexWriter backend.EngineWriter,
) ChunkProcessor {
	chunkLogger := logger
	deliver := &dataDeliver{
		logger:        chunkLogger,
		diskQuotaLock: diskQuotaLock,
		kvsCh:         make(chan []deliveredRow, maxKVQueueSize),
		dataWriter:    dataWriter,
		indexWriter:   indexWriter,
	}
	cp := &queryChunkProcessor{
		deliver: deliver,
		enc: &queryChunkEncoder{
			rowCh:     rowCh,
			chunkInfo: chunk,
			logger:    chunkLogger,
			encoder:   encoder,
			sendFn:    deliver.sendEncodedData,
		},
		logger:      chunkLogger,
		deliverLoop: deliver.deliverLoop,
		encodeDone:  deliver.encodeDone,
	}
	return cp
}

func (p *queryChunkProcessor) Process(ctx context.Context) (err error) {
	task := log.BeginTask(p.logger, "process chunk")
	defer func() {
		task.End(zap.ErrorLevel, err,
			zap.Duration("readDur", p.enc.readTotalDur),
			zap.Duration("encodeDur", p.enc.encodeTotalDur),
			zap.Duration("deliverDur", p.deliver.deliverTotalDur),
			zap.Object("checksum", &p.deliver.checksum),
		)
	}()
	//if err2 := p.enc.initProgress(); err2 != nil {
	//	return err2
	//}

	group, gCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return p.deliverLoop(gCtx)
	})
	group.Go(func() error {
		defer p.encodeDone(gCtx)
		return p.enc.encodeLoop(gCtx)
	})

	err2 := group.Wait()
	p.enc.chunkInfo.Checksum.Add(&p.deliver.checksum)
	return err2
}
