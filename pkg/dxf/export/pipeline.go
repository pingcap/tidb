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
	"sync/atomic"

	"github.com/pingcap/errors"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
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
	if exportReaderPool > 0 {
		return e.runPipelineDecoupled(ctx, physicalID, ordinal, exportReaderPool, bounds)
	}
	writerCnt := len(bounds) - 1
	eg, egCtx := errgroup.WithContext(ctx)
	m := e.taskMeta.effectiveWritersPerEncoder()
	for wStart := 0; wStart < writerCnt; wStart += m {
		wEnd := min(wStart+m, writerCnt)
		writers := make([]*writerChan, 0, wEnd-wStart)
		for i := wStart; i < wEnd; i++ {
			w := &writerChan{
				id:    i,
				start: bounds[i],
				end:   bounds[i+1],
				encCh: make(chan []byte, exportEncBufSize),
			}
			writers = append(writers, w)
		}

		workCh := make(chan readerChunk, len(writers)*exportEncBufSize)
		for _, w := range writers {
			eg.Go(func() error {
				return e.runReader(egCtx, physicalID, w, workCh)
			})
			eg.Go(func() error {
				return e.runFileWriter(egCtx, ordinal, w.id, w.encCh)
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
		// the chunk is returned to the pool by the encoder after encoding.
		chk := e.getChunk()
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

		buf, _ := e.bufPool.Get().([]byte)
		buf, err = enc.encodeChunk(rc.chk, buf[:0])
		if err != nil {
			return err
		}
		e.summary.RowCnt.Add(int64(rc.chk.NumRows()))
		e.rowsCounter.Add(float64(rc.chk.NumRows()))
		e.chunkPool.Put(rc.chk)

		if err := sendCtx(ctx, rc.writer.encCh, buf); err != nil {
			return err
		}
	}
	return nil
}

// runFileWriter uploads one file's encoded buffers to the object store, cutting
// files at FileSize on chunk (row) boundaries. Both pipelines use it: buffers
// arrive on encCh in key order, so the output files stay in handle order.
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

// fileCursor is one ordered output file in the decoupled pipeline. Its key
// range is pre-split into region-sized pages; the shared reader pool advances
// it one page at a time. A cursor lives in the work queue at most once, so only
// one reader holds it at a time and its pages reach encCh in key order, where a
// dedicated writer drains them. This decouples the reader count (concurrent cop
// reads) from the file/writer count.
type fileCursor struct {
	id int
	// pages are region boundaries: page i covers [pages[i], pages[i+1]).
	pages   []kv.Key
	pageIdx int
	encCh   chan []byte
}

// runPipelineDecoupled runs the export with a fixed-size reader pool feeding m
// ordered per-file buffers (m = len(bounds)-1). Readers round-robin region
// pages across all files, so at most readerCnt cop reads are in flight at once
// regardless of how many files/writers there are.
func (e *dumpStepExecutor) runPipelineDecoupled(ctx context.Context, physicalID int64, ordinal, readerCnt int, bounds []kv.Key) error {
	fileCnt := len(bounds) - 1
	files := make([]*fileCursor, 0, fileCnt)
	for i := range fileCnt {
		pages, err := loadRegionBoundaries(ctx, e.store, bounds[i], bounds[i+1])
		if err != nil {
			return errors.Trace(err)
		}
		files = append(files, &fileCursor{
			id:    i,
			pages: pages,
			encCh: make(chan []byte, exportEncBufSize),
		})
	}

	eg, egCtx := errgroup.WithContext(ctx)
	// Cap at fileCnt so the channel never blocks on re-queue (at most fileCnt
	// cursors ever exist) and we do not spawn readers that can never get work.
	queue := make(chan *fileCursor, fileCnt)
	for _, f := range files {
		queue <- f
		eg.Go(func() error {
			return e.runFileWriter(egCtx, ordinal, f.id, f.encCh)
		})
	}
	var remaining atomic.Int64
	remaining.Store(int64(fileCnt))
	readerCnt = min(readerCnt, fileCnt)
	for range readerCnt {
		eg.Go(func() error {
			return e.runReaderPool(egCtx, physicalID, queue, &remaining)
		})
	}
	e.logger.Info("export decoupled pipeline", zap.Int("files", fileCnt), zap.Int("readers", readerCnt))
	return eg.Wait()
}

// runReaderPool is one reader of the shared pool: it pulls a file cursor, reads
// its next region page, encodes it to the file's writer, then re-queues the
// cursor (or closes the file when its last page is done).
func (e *dumpStepExecutor) runReaderPool(ctx context.Context, physicalID int64, queue chan *fileCursor, remaining *atomic.Int64) error {
	exprCtx := newExportExprCtx()
	distCtx := newExportDistSQLCtx(e.store.GetClient())
	enc := newCSVEncoder(e.taskMeta.TableInfo.Name.O, e.colInfos)
	for {
		f, ok, err := recvCtx(ctx, queue)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := e.readPage(ctx, exprCtx, distCtx, enc, physicalID, f); err != nil {
			return err
		}
		f.pageIdx++
		if f.pageIdx < len(f.pages)-1 {
			if err := sendCtx(ctx, queue, f); err != nil {
				return err
			}
			continue
		}
		// last page done: close the file and, when this was the last file,
		// close the queue so the other readers exit. remaining only reaches 0
		// once every cursor is done, so no re-queue can race this close.
		close(f.encCh)
		if remaining.Add(-1) == 0 {
			close(queue)
		}
	}
}

// readPage cop-scans the cursor's current page in handle order, encoding rows
// to CSV and flushing to the file's writer at part-size boundaries (between
// chunks, so a flush never splits a row).
func (e *dumpStepExecutor) readPage(ctx context.Context, exprCtx *exprstatic.ExprContext,
	distCtx *distsqlctx.DistSQLContext, enc *csvEncoder, physicalID int64, f *fileCursor) error {
	start, end := f.pages[f.pageIdx], f.pages[f.pageIdx+1]
	rs, err := buildScan(ctx, exprCtx, distCtx, e.taskMeta.TableInfo, physicalID,
		e.colInfos, e.fieldTps, e.taskMeta.SnapshotTS, start, end)
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	buf, _ := e.bufPool.Get().([]byte)
	buf = buf[:0]
	rows := 0
	for {
		chk := e.getChunk()
		if err := rs.Next(ctx, chk); err != nil {
			e.chunkPool.Put(chk)
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			e.chunkPool.Put(chk)
			break
		}
		rows += chk.NumRows()
		buf, err = enc.encodeChunk(chk, buf)
		e.chunkPool.Put(chk)
		if err != nil {
			return err
		}
		if int64(len(buf)) >= writerPartSize {
			if err := sendCtx(ctx, f.encCh, buf); err != nil {
				return err
			}
			buf, _ = e.bufPool.Get().([]byte)
			buf = buf[:0]
		}
	}
	e.summary.RowCnt.Add(int64(rows))
	e.rowsCounter.Add(float64(rows))
	if len(buf) > 0 {
		return sendCtx(ctx, f.encCh, buf)
	}
	e.bufPool.Put(buf) //nolint:staticcheck
	return nil
}
