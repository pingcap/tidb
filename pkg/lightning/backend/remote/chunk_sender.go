// Copyright 2025 PingCAP, Inc.
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

package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

// PutChunkResponse is json data that returned by remote server PUT API.
type PutChunkResponse struct {
	// The chunks that remote worker has flushed.
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	// The chunks that remote worker has recevied.
	HandledChunkID uint64 `json:"handled-chunk-id"`
	// When the remote worker terminates with an error, or the client sends a
	// cleanup request, the `Canceled` will be set to true.
	Canceled bool `json:"canceled"`
	// When the remote worker successfully ingests the data into tikv, the `Finished`
	// will be set to true.
	Finished bool   `json:"finished"`
	Error    string `json:"error"`
}

// FlushResponse is json data that returned by remote server POST API.
type FlushResponse struct {
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	Canceled       bool   `json:"canceled"`
	Finished       bool   `json:"finished"`
	Error          string `json:"error"`
}

// chunk is a unit of data that send to remote worker.
type chunk struct {
	id   uint64
	data []byte
}

type chunkTask struct {
	// For flush, resultCh is used to notify the completion of the flush.
	// For put chunk, resultCh is used to receive chunkID allocated by chunkSenderLoop,
	// but receiving a chunkID does not mean that the chunk has been sent to the
	// remote worker.
	resultCh chan uint64
	flush    bool
	data     []byte
}

func newChunkTask(data []byte, flush bool) *chunkTask {
	return &chunkTask{
		resultCh: make(chan uint64, 1),
		flush:    flush,
		data:     data,
	}
}

func (t *chunkTask) done(chunkID uint64) {
	t.resultCh <- chunkID
	close(t.resultCh)
}

type chunkSender struct {
	id uint64

	e          *engine
	httpClient *http.Client
	chunkStore *chunkStore
	logger     log.Logger

	// the chunk id that remote worker has flushed
	flushedChunkID atomic.Uint64
	nextChunkID    atomic.Uint64

	// the following channels should be closed by chunkSenderLoop
	loopDoneChan chan struct{} // notify chunkSenderLoop is done

	taskChan chan *chunkTask
	cancel   context.CancelFunc

	err error
}

func newChunkSender(ctx context.Context, id uint64, engine *engine, chunkStore *chunkStore) *chunkSender {
	// The ctx is used to control the life of the `chunkSenderLoop`.
	ctx, cancel := context.WithCancel(ctx)
	c := &chunkSender{
		id: id,

		e:          engine,
		httpClient: engine.httpClient,
		chunkStore: chunkStore,
		logger:     engine.logger.With(zap.Uint64("sender", id)),

		flushedChunkID: atomic.Uint64{},
		nextChunkID:    atomic.Uint64{},

		loopDoneChan: make(chan struct{}),
		taskChan:     make(chan *chunkTask),
		cancel:       cancel,
	}

	go c.chunkSenderLoop(ctx)

	c.logger.Info("chunk sender started")
	return c
}

func (c *chunkSender) putChunk(ctx context.Context, data []byte) (uint64, error) {
	return c.submit(ctx, data, false)
}

func (c *chunkSender) flush(ctx context.Context) error {
	if c.getFlushedChunkID() == c.getLastChunkID() {
		return nil
	}
	_, err := c.submit(ctx, nil, true)
	return err
}

func (c *chunkSender) submit(ctx context.Context, data []byte, flush bool) (uint64, error) {
	task := newChunkTask(data, flush)

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case c.taskChan <- task:
	case <-c.loopDoneChan:
		return 0, c.err
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case newChunkID := <-task.resultCh:
		return newChunkID, nil
	case <-c.loopDoneChan:
		return 0, c.err
	}
}

func (c *chunkSender) getNextChunkID() uint64 {
	return c.nextChunkID.Add(1)
}

func (c *chunkSender) getLastChunkID() uint64 {
	return c.nextChunkID.Load()
}

func (c *chunkSender) getFlushedChunkID() uint64 {
	return c.flushedChunkID.Load()
}

func (c *chunkSender) chunkSenderLoop(ctx context.Context) {
	defer func() {
		close(c.loopDoneChan)
		err := c.chunkStore.close()
		if err != nil {
			c.logger.Warn("failed to close chunk store", zap.Error(err))
		}
	}()

	ticker := time.NewTicker(updateFlushedChunkDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-c.taskChan:
			if !task.flush {
				newChunkID := c.getNextChunkID()
				task.done(newChunkID)

				chunk := &chunk{
					id:   newChunkID,
					data: task.data,
				}
				c.err = c.putChunkToRemote(ctx, chunk)
				if c.err != nil {
					return
				}
			} else {
				c.err = c.sendFlushToRemote(ctx)
				if c.err != nil {
					return
				}
				task.done(0)
			}
			// Reset the ticker when a new task is received. Avoid sending empty chunks too frequently.
			ticker.Reset(updateFlushedChunkDuration)
		case <-ticker.C:
			// Periodically send empty chunks to get the latest flushed chunkID from remote worker if needed.
			if c.getFlushedChunkID() == c.getLastChunkID() {
				continue
			}
			c.err = c.putEmptyChunk(ctx)
			if c.err != nil {
				return
			}
		}
	}
}

// putEmptyChunk sends an empty chunk to remote worker to get remote worker's state.
//
// The remote worker will not accept the empty chunk and just return the
// `FlushedChunkID` and `HandledChunkID`. If the remote worker restarts, the
// remote worker may return the unexpected `HandledChunkID`, and we need to
// retry sending chunks.
func (c *chunkSender) putEmptyChunk(ctx context.Context) error {
	chunk := &chunk{id: c.getLastChunkID(), data: nil}
	return c.putChunkToRemote(ctx, chunk)
}

func (c *chunkSender) putChunkToRemote(ctx context.Context, chunk *chunk) error {
	if len(chunk.data) != 0 {
		// Cache the chunk data to avoid the data being lost when the remote worker restarts.
		// If the remote worker restarts, we can retry sending chunks from cache.
		err := c.chunkStore.put(chunk.id, chunk.data)
		if err != nil {
			return errors.Trace(err)
		}
	}

	url := fmt.Sprintf(putChunkURL, c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, chunk.id)

	data, err := sendRequest(ctx, c.httpClient, "PUT", url, chunk.data)
	if err != nil {
		return errors.Trace(err)
	}
	resp := new(PutChunkResponse)
	err = json.Unmarshal(data, resp)
	if err != nil {
		return errors.Trace(err)
	}

	err = c.handlePutChunkResponse(ctx, resp, chunk.id)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// handlePutChunkResponse handles the response of put chunk request.
//
// Chunks are sent to remote workers in sequence according to chunk id. The remote
// worker should return the `FlushedChunkID` and `HandledChunkID`. If the remote worker
// restarts, the `HandledChunkID` may be less than the `expectedChunkID`, and remote
// worker will reject the new chunk.
// In this case, we need to retry sending chunks from the `FlushedChunkID` to the `expectedChunkID`.
func (c *chunkSender) handlePutChunkResponse(ctx context.Context, resp *PutChunkResponse, expectedChunkID uint64) error {
	if resp.Canceled {
		c.logger.Error("failed to put chunk, task is canceled", zap.String("error", resp.Error))
		return common.ErrRemoteLoadDataTaskCanceled.FastGenByArgs(c.e.loadDataTaskID, resp.Error)
	}

	if resp.HandledChunkID != expectedChunkID {
		c.logger.Info("remote worker may restart, retry to put chunk",
			zap.Uint64("expected chunkID", expectedChunkID),
			zap.Uint64("handled chunkID", resp.HandledChunkID),
			zap.Uint64("flushed chunkID", resp.FlushedChunkID))

		nextChunkID := resp.HandledChunkID + 1
		for nextChunkID <= expectedChunkID {
			url := fmt.Sprintf(putChunkURL, c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, nextChunkID)

			buf, err := c.chunkStore.get(nextChunkID)
			if err != nil {
				return errors.Trace(err)
			}
			c.logger.Info("retry to put chunk", zap.Uint64("chunkID", nextChunkID))

			data, err := sendRequest(ctx, c.httpClient, "PUT", url, buf)
			if err != nil {
				return errors.Trace(err)
			}
			resp = new(PutChunkResponse)
			err = json.Unmarshal(data, resp)
			if err != nil {
				return errors.Trace(err)
			}
			if resp.Canceled {
				c.logger.Error("failed to put chunk, task is canceled", zap.String("error", resp.Error))
				return common.ErrRemoteLoadDataTaskCanceled.FastGenByArgs(c.e.loadDataTaskID, resp.Error)
			}

			nextChunkID = resp.HandledChunkID + 1
		}
	}

	flushedChunkID := c.flushedChunkID.Load()
	// We can clean the cache of chunks that the remote worker has flushed.
	lastFlushedChunkID := flushedChunkID + 1
	for lastFlushedChunkID <= resp.FlushedChunkID {
		err := c.chunkStore.clean(lastFlushedChunkID)
		if err != nil {
			c.logger.Info("failed to clean chunk cache",
				zap.Uint64("chunkID", lastFlushedChunkID),
				zap.Error(err))
		}
		lastFlushedChunkID++
	}

	c.flushedChunkID.Store(resp.FlushedChunkID)
	return nil
}

// sendFlushToRemote send flush requst to remote worker.
//
// The remote worker should return `FlushedChunkID`, which should be equal to `LastChunkID`.
// If not, it means that the remote worker has restarted and we should resend the chunk from
// `FlushedChunkID` to the `LastChunkID`.
func (c *chunkSender) sendFlushToRemote(ctx context.Context) error {
	for {
		url := fmt.Sprintf(flushURL, c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id)

		data, err := sendRequest(ctx, c.httpClient, "POST", url, nil)
		if err != nil {
			c.logger.Error("failed to flush", zap.Error(err))
			return errors.Trace(err)
		}

		resp := new(FlushResponse)
		err = json.Unmarshal(data, resp)
		if err != nil {
			return errors.Trace(err)
		}

		if resp.Finished {
			c.logger.Info("load data task finished")
			return nil
		}

		if resp.Canceled {
			c.logger.Error("failed to flush",
				zap.Bool("canceled", resp.Canceled),
				zap.String("error", resp.Error))
			return common.ErrRemoteLoadDataTaskCanceled.FastGenByArgs(c.e.loadDataTaskID, resp.Error)
		}

		// Make sure all chunks are flushed in remote worker.
		if resp.FlushedChunkID == c.getLastChunkID() {
			// Clean the cache of chunks that have been flushed and update the state.
			flushedChunkID := c.flushedChunkID.Load()
			lastFlushedChunkID := flushedChunkID + 1
			for lastFlushedChunkID <= resp.FlushedChunkID {
				err := c.chunkStore.clean(lastFlushedChunkID)
				if err != nil {
					c.logger.Warn("failed to clean chunk cache", zap.Uint64("chunkID", lastFlushedChunkID), zap.Error(err))
				}
				lastFlushedChunkID++
			}
			c.flushedChunkID.Store(resp.FlushedChunkID)
			return nil
		}

		// The remote worker missed some chunks, we need to retry putting chunks.
		// We put an empty chunk to get the `PutChunkResponse` to start the put chunk process.
		// Then we flush again.
		err = c.putEmptyChunk(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func (c *chunkSender) close() error {
	// notify chunkSenderLoop to quit
	c.cancel()
	// wait for chunkSenderLoop to quit
	<-c.loopDoneChan

	return c.err
}
