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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	channelBufSize = 2
	readChunkSize  = 128
)

// writerChan is one contiguous sub-range with reader/writer affinity: rows
// read by its reader are always written by its writer, so the writer's output
// files stay in handle order.
type writerChan struct {
	id    int
	start kv.Key
	end   kv.Key
	// encCh carries encoded buffers from the encoder to the writer.
	encCh chan []byte
}

func sendCtx[T any](ctx context.Context, ch chan<- T, v T) error {
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func recvCtx[T any](ctx context.Context, ch <-chan T) (v T, ok bool, err error) {
	select {
	case v, ok = <-ch:
		return v, ok, nil
	case <-ctx.Done():
		return v, false, ctx.Err()
	}
}

// runPipeline runs the read -> encode -> upload pipeline of one subtask. Each
// of the subtask's sub-ranges gets a 1:1:1 reader/encoder/writer chain: the
// reader cop-scans its sub-range in handle order, the encoder turns the chunks
// into CSV, and the writer uploads them. Reader-writer affinity keeps each
// output file in handle order; the channels overlap the slow cross-region
// upload with the read+encode work.
func (e *dumpStepExecutor) runPipeline(ctx context.Context, physicalID int64, ordinal int, bounds []kv.Key) error {
	writerCnt := len(bounds) - 1
	eg, egCtx := errgroup.WithContext(ctx)
	for i := range writerCnt {
		w := &writerChan{
			id:    i,
			start: bounds[i],
			end:   bounds[i+1],
			encCh: make(chan []byte, channelBufSize),
		}
		workCh := make(chan *chunk.Chunk, channelBufSize)
		eg.Go(func() error {
			return e.runReader(egCtx, physicalID, w, workCh)
		})
		eg.Go(func() error {
			return e.runEncoder(egCtx, w, workCh)
		})
		eg.Go(func() error {
			return e.runFileWriter(egCtx, ordinal, w.id, w.encCh)
		})
	}
	return eg.Wait()
}

// runReader cop-scans the writer's sub-range in handle order and feeds raw
// chunks to the encoder, ending with a nil-chunk done marker.
func (e *dumpStepExecutor) runReader(ctx context.Context, physicalID int64, w *writerChan, workCh chan<- *chunk.Chunk) error {
	exprCtx := newExportExprCtx()
	distCtx := newExportDistSQLCtx(e.store.GetClient())
	rs, err := buildScan(ctx, exprCtx, distCtx, e.taskMeta.TableInfo, physicalID,
		e.colInfos, e.fieldTps, e.taskMeta.SnapshotTS, w.start, w.end)
	if err != nil {
		return err
	}
	defer rs.Close()
	rows := 0
	for {
		// the chunk is returned to the pool by the encoder after encoding.
		chk := e.getChunk()
		if err := rs.Next(ctx, chk); err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			e.logger.Info("export sub-range read done", zap.Int("writer", w.id), zap.Int("rows", rows))
			return sendCtx(ctx, workCh, nil)
		}
		rows += chk.NumRows()
		if err := sendCtx(ctx, workCh, chk); err != nil {
			return err
		}
	}
}

// runEncoder encodes the reader's chunks into CSV and forwards them to the
// writer, closing the writer channel when the reader signals done.
func (e *dumpStepExecutor) runEncoder(ctx context.Context, w *writerChan, workCh <-chan *chunk.Chunk) error {
	enc := newCSVEncoder(e.taskMeta.TableInfo.Name.O, e.colInfos)
	for {
		chk, _, err := recvCtx(ctx, workCh)
		if err != nil {
			return err
		}
		if chk == nil {
			close(w.encCh)
			return nil
		}

		buf, _ := e.bufPool.Get().([]byte)
		buf, err = enc.encodeChunk(chk, buf[:0])
		if err != nil {
			return err
		}
		e.summary.RowCnt.Add(int64(chk.NumRows()))
		e.rowsCounter.Add(float64(chk.NumRows()))
		e.chunkPool.Put(chk)

		if err := sendCtx(ctx, w.encCh, buf); err != nil {
			return err
		}
	}
}

// runFileWriter uploads one file's encoded buffers to the object store, cutting
// files at FileSize on chunk (row) boundaries. Buffers arrive on encCh in key
// order, so the output file stays in handle order.
func (e *dumpStepExecutor) runFileWriter(ctx context.Context, ordinal, id int, encCh <-chan []byte) error {
	fw := newFileWriter(ctx, e.objStore, e.taskMeta, ordinal, id, e.filesCounter)
	for {
		buf, ok, err := recvCtx(ctx, encCh)
		if err != nil {
			return err
		}
		if !ok {
			return fw.Close()
		}

		if err := fw.Write(buf); err != nil {
			return err
		}

		e.summary.Processed.Add(int64(len(buf)))
		e.bytesCounter.Add(float64(len(buf)))
		e.bufPool.Put(buf) //nolint:staticcheck
	}
}
