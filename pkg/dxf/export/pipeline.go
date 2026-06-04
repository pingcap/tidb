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
	"golang.org/x/sync/errgroup"
)

const (
	readChunkSize = 1024
	// chunksPerLane bounds the chunks in flight between one reader and its
	// encoder.
	chunksPerLane = 2
	// encodedBufsPerLane bounds the encoded buffers in flight between the
	// encoder and one writer.
	encodedBufsPerLane = 2
)

// lane is one contiguous sub-range with reader/writer affinity: rows read by
// the lane's reader are always written by the lane's writer, so each lane's
// output files stay in handle order.
type lane struct {
	id    int
	start kv.Key
	end   kv.Key
	// encCh carries encoded buffers from the encoder to the lane's writer.
	encCh chan []byte
	// freeChunkCh recycles chunks from the encoder back to the reader.
	freeChunkCh chan *chunk.Chunk
	// freeBufCh recycles encoded buffers from the writer back to the encoder.
	freeBufCh chan []byte
}

// laneChunk is one read chunk tagged with its lane; a nil chk marks lane EOF.
type laneChunk struct {
	lane *lane
	chk  *chunk.Chunk
}

func sendCtx[T any](ctx context.Context, ch chan<- T, v T) error {
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runPipeline runs the read -> encode -> upload pipeline of one subtask:
// n encoders (n = task concurrency), each serving m lanes. The m readers of
// one encoder share a single channel into it, and the encoder routes encoded
// buffers to the m writer channels by lane.
func (e *dumpStepExecutor) runPipeline(ctx context.Context, physicalID int64, ordinal int, bounds []kv.Key) error {
	laneCnt := len(bounds) - 1
	m := e.taskMeta.effectiveLanesPerEncoder()
	eg, egCtx := errgroup.WithContext(ctx)
	for laneStart := 0; laneStart < laneCnt; laneStart += m {
		laneEnd := min(laneStart+m, laneCnt)
		lanes := make([]*lane, 0, laneEnd-laneStart)
		for i := laneStart; i < laneEnd; i++ {
			l := &lane{
				id:          i,
				start:       bounds[i],
				end:         bounds[i+1],
				encCh:       make(chan []byte, encodedBufsPerLane),
				freeChunkCh: make(chan *chunk.Chunk, chunksPerLane),
				freeBufCh:   make(chan []byte, encodedBufsPerLane),
			}
			for range chunksPerLane {
				l.freeChunkCh <- chunk.NewChunkWithCapacity(e.fieldTps, readChunkSize)
			}
			lanes = append(lanes, l)
		}
		// readers of this encoder share one channel; per-lane order is kept
		// because each lane has a single sender.
		workCh := make(chan laneChunk, len(lanes)*chunksPerLane)
		for _, l := range lanes {
			eg.Go(func() error {
				return e.runReader(egCtx, physicalID, l, workCh)
			})
			eg.Go(func() error {
				return e.runWriter(egCtx, ordinal, l)
			})
		}
		eg.Go(func() error {
			return e.runEncoder(egCtx, lanes, workCh)
		})
	}
	return eg.Wait()
}

// runReader cop-scans the lane's sub-range in handle order and feeds raw
// chunks to the encoder.
func (e *dumpStepExecutor) runReader(ctx context.Context, physicalID int64, l *lane, workCh chan<- laneChunk) error {
	exprCtx := newExportExprCtx()
	distCtx := newExportDistSQLCtx(e.store.GetClient())
	rs, err := buildScan(ctx, exprCtx, distCtx, e.taskMeta.TableInfo, physicalID,
		e.colInfos, e.fieldTps, e.taskMeta.SnapshotTS, l.start, l.end)
	if err != nil {
		return err
	}
	defer rs.Close()
	for {
		var chk *chunk.Chunk
		select {
		case chk = <-l.freeChunkCh:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := rs.Next(ctx, chk); err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			// EOF sentinel, the encoder closes the lane's writer channel.
			return sendCtx(ctx, workCh, laneChunk{lane: l})
		}
		if err := sendCtx(ctx, workCh, laneChunk{lane: l, chk: chk}); err != nil {
			return err
		}
	}
}

// runEncoder is the CPU-bound stage: it drains chunks of its m lanes from the
// shared channel and routes encoded buffers to each lane's writer.
func (e *dumpStepExecutor) runEncoder(ctx context.Context, lanes []*lane, workCh <-chan laneChunk) error {
	enc := newCSVEncoder(e.taskMeta.TableInfo.Name.O, e.colInfos)
	remaining := len(lanes)
	for remaining > 0 {
		var lc laneChunk
		select {
		case lc = <-workCh:
		case <-ctx.Done():
			return ctx.Err()
		}
		if lc.chk == nil {
			close(lc.lane.encCh)
			remaining--
			continue
		}
		var buf []byte
		select {
		case buf = <-lc.lane.freeBufCh:
			buf = buf[:0]
		default:
		}
		buf, err := enc.encodeChunk(lc.chk, buf)
		if err != nil {
			return err
		}
		e.summary.RowCnt.Add(int64(lc.chk.NumRows()))
		lc.chk.Reset()
		select {
		case lc.lane.freeChunkCh <- lc.chk:
		default:
		}
		if err := sendCtx(ctx, lc.lane.encCh, buf); err != nil {
			return err
		}
	}
	return nil
}

// runWriter uploads the lane's encoded buffers to the object store, cutting
// files at FileSize on chunk (row) boundaries.
func (e *dumpStepExecutor) runWriter(ctx context.Context, ordinal int, l *lane) error {
	fw := newFileWriter(e.objStore, e.taskMeta, ordinal, l.id)
	for {
		var buf []byte
		var ok bool
		select {
		case buf, ok = <-l.encCh:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ok {
			return fw.Close(ctx)
		}
		if err := fw.Write(ctx, buf); err != nil {
			return err
		}
		e.summary.Bytes.Add(int64(len(buf)))
		select {
		case l.freeBufCh <- buf:
		default:
		}
	}
}
