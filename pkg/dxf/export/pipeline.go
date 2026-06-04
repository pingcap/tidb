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
	// freeBufCh recycles encoded buffers from the writer back to the encoder.
	freeBufCh chan []byte
}

// readerChunk is one read chunk tagged with its target writer
type readerChunk struct {
	writer *writerChan
	chk    *chunk.Chunk
}

func sendCtx[T any](ctx context.Context, ch chan<- T, v T) error {
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// recvCtx receives from ch unless ctx is done; ok is false when ch is closed.
func recvCtx[T any](ctx context.Context, ch <-chan T) (v T, ok bool, err error) {
	select {
	case v, ok = <-ch:
		return v, ok, nil
	case <-ctx.Done():
		return v, false, ctx.Err()
	}
}

// runPipeline runs the read -> encode -> upload pipeline of one subtask:
// n encoders (n = task concurrency), each serving m reader/writer pairs.
// The m readers of one encoder share a single channel into it, and the
// encoder routes encoded buffers to the m writer channels.
func (e *dumpStepExecutor) runPipeline(ctx context.Context, physicalID int64, ordinal int, bounds []kv.Key) error {
	writerCnt := len(bounds) - 1
	eg, egCtx := errgroup.WithContext(ctx)
	for wStart := 0; wStart < writerCnt; wStart += writersPerEncoder {
		wEnd := min(wStart+writersPerEncoder, writerCnt)
		writers := make([]*writerChan, 0, wEnd-wStart)
		for i := wStart; i < wEnd; i++ {
			w := &writerChan{
				id:        i,
				start:     bounds[i],
				end:       bounds[i+1],
				encCh:     make(chan []byte, channelBufSize),
				freeBufCh: make(chan []byte, channelBufSize),
			}
			writers = append(writers, w)
		}

		workCh := make(chan readerChunk, len(writers)*channelBufSize)
		for _, w := range writers {
			eg.Go(func() error {
				return e.runReader(egCtx, physicalID, w, workCh)
			})
			eg.Go(func() error {
				return e.runWriter(egCtx, ordinal, w)
			})
		}
		eg.Go(func() error {
			return e.runEncoder(egCtx, writers, workCh)
		})
	}
	return eg.Wait()
}

// runReader cop-scans the writer's sub-range in handle order and feeds raw
// chunks to the encoder.
func (e *dumpStepExecutor) runReader(ctx context.Context, physicalID int64, w *writerChan, workCh chan<- readerChunk) error {
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
		chk := chunk.NewChunkWithCapacity(e.fieldTps, 128)
		if err := rs.Next(ctx, chk); err != nil {
			return errors.Trace(err)
		}

		if chk.NumRows() == 0 {
			e.logger.Info("export sub-range read done", zap.Int("writer", w.id), zap.Int("rows", rows))
			return sendCtx(ctx, workCh, readerChunk{writer: w})
		}
		rows += chk.NumRows()
		if err := sendCtx(ctx, workCh, readerChunk{writer: w, chk: chk}); err != nil {
			return err
		}
	}
}

// runEncoder is the CPU-bound stage: it drains chunks of its m sub-ranges
// from the shared channel and routes encoded buffers to each writer channel.
func (e *dumpStepExecutor) runEncoder(ctx context.Context, writers []*writerChan, workCh <-chan readerChunk) error {
	enc := newCSVEncoder(e.taskMeta.TableInfo.Name.O, e.colInfos)
	remaining := len(writers)
	for remaining > 0 {
		rc, _, err := recvCtx(ctx, workCh)
		if err != nil {
			return err
		}

		if rc.chk == nil {
			close(rc.writer.encCh)
			remaining--
			continue
		}

		var buf []byte
		select {
		case buf = <-rc.writer.freeBufCh:
			buf = buf[:0]
		default:
		}

		buf, err = enc.encodeChunk(rc.chk, buf)
		if err != nil {
			return err
		}
		e.summary.RowCnt.Add(int64(rc.chk.NumRows()))

		if err := sendCtx(ctx, rc.writer.encCh, buf); err != nil {
			return err
		}
	}
	return nil
}

// runWriter uploads the writer's encoded buffers to the object store, cutting
// files at FileSize on chunk (row) boundaries.
func (e *dumpStepExecutor) runWriter(ctx context.Context, ordinal int, w *writerChan) error {
	fw := newFileWriter(ctx, e.objStore, e.taskMeta, ordinal, w.id)
	for {
		buf, ok, err := recvCtx(ctx, w.encCh)
		if err != nil {
			return err
		}
		if !ok {
			return fw.Close()
		}

		if err := fw.Write(buf); err != nil {
			return err
		}

		e.summary.Bytes.Add(int64(len(buf)))
		select {
		case w.freeBufCh <- buf:
		default:
		}
	}
}
