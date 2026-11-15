// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// PutChunkResult is json data that returned by remote server PUT API.
type PutChunkResult struct {
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	HandledChunkID uint64 `json:"handled-chunk-id"`
	Canceled       bool   `json:"canceled"`
	Finished       bool   `json:"finished"`
	Error          string `json:"error"`
	spfreshRecoveryState
	// SPFresh build PUT can be accepted while the worker is recovering a
	// transient transaction. The chunk was not accepted in that response;
	// keep the retry metadata so the sender can wait and resend the same
	// idempotent chunk instead of failing the whole build.
	acceptedForRecovery bool
}

// FlushResult is json data that returned by remote server POST API.
type FlushResult struct {
	FlushedChunkIDs map[uint64]uint64 `json:"flushed-chunk-ids"`
	Canceled        bool              `json:"canceled"`
	Finished        bool              `json:"finished"`
	Error           string            `json:"error"`
	spfreshRecoveryState
}

// chunk is a unit of data that send to remote worker.
type chunk struct {
	id   uint64
	data []byte
}

type chunksState struct {
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	HandledChunkID uint64 `json:"handled-chunk-id"`
}

type chunkTask struct {
	// For flush, resp is used to notify the completion of the flush.
	// For put chunk, resp is used to receive chunkID allocated by chunkSenderLoop.
	resp  chan uint64
	flush bool
	data  []byte
	err   error
}

func newChunkTask(data []byte, flush bool) *chunkTask {
	return &chunkTask{
		resp:  make(chan uint64, 1),
		flush: flush,
		data:  data,
	}
}

func (t *chunkTask) done(chunkID uint64) {
	t.resp <- chunkID
	close(t.resp)
}

type chunkSender struct {
	id uint64
	wg sync.WaitGroup

	e           *engine
	httpClient  *http.Client
	chunksCache *chunkCache

	// state is used to record the state of the chunks in remote worker.
	state       atomic.Value
	nextChunkID atomic.Uint64

	// the following channels should be closed by chunkSenderLoop
	loopDoneChan chan struct{} // notify chunkSenderLoop is done

	// the following channels should be closed by closed method
	taskChan     chan *chunkTask
	loopQuitChan chan struct{} // notify chunkSenderLoop to quit

	err error
}

func newChunkSender(ctx context.Context, id uint64, engine *engine, chunksCache *chunkCache) *chunkSender {
	c := &chunkSender{
		id: id,
		wg: sync.WaitGroup{},

		e:           engine,
		httpClient:  engine.httpClient,
		chunksCache: chunksCache,

		state:       atomic.Value{},
		nextChunkID: atomic.Uint64{},

		loopDoneChan: make(chan struct{}),
		loopQuitChan: make(chan struct{}),
		taskChan:     make(chan *chunkTask),
	}
	c.state.Store(&chunksState{})

	c.wg.Add(1)
	go c.chunkSenderLoop(ctx)

	engine.logger.Info("chunk sender started", zap.Uint64("sender", c.id))
	return c
}

func (c *chunkSender) putChunk(ctx context.Context, data []byte) (uint64, error) {
	data0 := make([]byte, len(data))
	copy(data0, data)
	return c.putOwnedChunk(ctx, data0)
}

// putOwnedChunk transfers ownership of data to the sender, including when it
// returns an error.
func (c *chunkSender) putOwnedChunk(ctx context.Context, data []byte) (uint64, error) {
	task := newChunkTask(data, false)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case c.taskChan <- task:
	case <-c.loopDoneChan:
		return 0, c.err
	}

	newChunkID := uint64(0)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case newChunkID = <-task.resp:
	case <-c.loopDoneChan:
		return 0, c.err
	}

	return newChunkID, nil
}

func (c *chunkSender) flush(ctx context.Context) error {
	if c.getFlushedChunkID() == c.getLastChunkID() {
		return nil
	}

	task := newChunkTask(nil, true)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.taskChan <- task:
	case <-c.loopDoneChan:
		return c.err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-task.resp:
		return task.err
	case <-c.loopDoneChan:
		return c.err
	}

	return nil
}

func (c *chunkSender) getNextChunkID() uint64 {
	return c.nextChunkID.Add(1)
}

func (c *chunkSender) getLastChunkID() uint64 {
	return c.nextChunkID.Load()
}

func (c *chunkSender) loadState() *chunksState {
	return c.state.Load().(*chunksState)
}

func (c *chunkSender) getFlushedChunkID() uint64 {
	return c.loadState().FlushedChunkID
}

func (c *chunkSender) storeState(flushedChunkID, handledChunkID uint64) {
	c.state.Store(&chunksState{
		FlushedChunkID: flushedChunkID,
		HandledChunkID: handledChunkID,
	})
}

func (c *chunkSender) cleanChunkCache(state *chunksState, flushedChunkID uint64) {
	for chunkID := state.FlushedChunkID + 1; chunkID <= flushedChunkID; chunkID++ {
		if err := c.chunksCache.clean(chunkID); err != nil {
			c.e.logger.Info("failed to clean chunk cache",
				zap.Uint64("chunkID", chunkID),
				zap.Error(err))
		}
	}
}

func (c *chunkSender) chunkSenderLoop(ctx context.Context) {
	defer func() {
		close(c.loopDoneChan)
		c.chunksCache.close()
		c.wg.Done()
	}()

	ticker := time.NewTicker(updateFlushedChunkDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.loopQuitChan:
			return
		case task := <-c.taskChan:
			// Reset the ticker when a new task is received. Avoid sending empty chunks too frequently.
			ticker.Reset(updateFlushedChunkDuration)
			if !task.flush {
				newChunkID := c.getNextChunkID()
				task.done(newChunkID)

				chunk := &chunk{
					id:   newChunkID,
					data: task.data,
				}
				c.err = c.putChunkToRemote(ctx, chunk)
				if c.err != nil {
					c.e.logger.Error("put chunk error",
						zap.Uint64("sender", c.id),
						zap.Error(c.err))
					return
				}
			} else {
				if err := c.sendFlushToRemote(ctx); err != nil {
					c.e.logger.Error("flush chunk error",
						zap.Uint64("sender", c.id),
						zap.Error(err))
					task.err = err
					task.done(0)
					continue
				}
				task.done(0)
			}
		case <-ticker.C:
			// Periodically send empty chunks to update the flushed chunkID.
			if c.e.taskType != taskTypeLoadData {
				continue
			}
			if c.getFlushedChunkID() == c.getLastChunkID() {
				continue
			}
			c.err = c.putEmptyChunk(ctx)
			if c.err != nil {
				c.e.logger.Error("flush empty chunk error",
					zap.Uint64("sender", c.id),
					zap.Error(c.err))
				return
			}
		}
	}
}

func (c *chunkSender) putEmptyChunk(ctx context.Context) error {
	if c.e.taskType != taskTypeLoadData {
		return fmt.Errorf("putEmptyChunk is not supported for task type %v", c.e.taskType)
	}
	chunk := &chunk{id: c.getLastChunkID(), data: nil}
	return c.putChunkToRemote(ctx, chunk)
}

func (c *chunkSender) putChunkToRemote(ctx context.Context, chunk *chunk) error {
	// cache chunk data
	if len(chunk.data) != 0 {
		err := c.chunksCache.put(chunk.id, chunk.data)
		if err != nil {
			return err
		}
	}

	result, err := c.sendPutChunk(ctx, chunk.id, chunk.data)
	if err != nil {
		return err
	}
	return c.handlePutChunkResult(ctx, result, chunk.id)
}

func (c *chunkSender) sendPutChunk(ctx context.Context, chunkID uint64, data []byte) (*PutChunkResult, error) {
	path := "/load_data"
	if c.e.taskType == taskTypeSPFreshBuild {
		path = "/spfresh/build"
	}
	url := fmt.Sprintf("%s%s?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
		c.e.addr, path, c.e.clusterID, c.e.loadDataTaskID, c.id, chunkID)
	var body []byte
	var statusCode int
	var err error
	if c.e.taskType == taskTypeSPFreshBuild {
		body, statusCode, err = sendLongOwnerBoundRequestAllowAccepted(
			ctx, c.httpClient, "PUT", url, data, c.e.retryCounter,
		)
	} else {
		body, err = sendLongOwnerBoundRequest(ctx, c.httpClient, "PUT", url, data, c.e.retryCounter)
	}
	if err != nil {
		return nil, err
	}
	result := new(PutChunkResult)
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, err
	}
	result.acceptedForRecovery = statusCode == http.StatusAccepted
	return result, nil
}

func (c *chunkSender) checkPutChunkTaskState(result *PutChunkResult) (bool, error) {
	if result.Canceled {
		c.e.logger.Error("failed to put chunk, task is canceled",
			zap.String("error", result.Error))
		return false, ErrTaskCanceled
	}

	if result.Finished {
		c.e.logger.Error("failed to put chunk, task is finished",
			zap.String("error", result.Error))
		return true, nil
	}
	if c.e.taskType == taskTypeSPFreshBuild && result.Error != "" && !result.acceptedForRecovery {
		c.e.logger.Error("failed to put SPFresh build chunk",
			zap.String("error", result.Error))
		return false, fmt.Errorf("failed to put SPFresh build chunk: %s", result.Error)
	}
	return false, nil
}

func (c *chunkSender) handlePutChunkResult(ctx context.Context, result *PutChunkResult, expectChunkID uint64) error {
	var done bool
	var err error
	result, done, err = c.retrySPFreshBuildChunkRecovery(ctx, result, expectChunkID)
	if err != nil {
		return err
	}
	if done {
		return nil
	}

	if result.HandledChunkID != expectChunkID {
		c.e.logger.Info("remote worker may restart, retry to put chunk",
			zap.Uint64("expectChunkID", expectChunkID),
			zap.Uint64("handledChunkID", result.HandledChunkID),
			zap.Uint64("flushedChunkID", result.FlushedChunkID),
			zap.String("error", result.Error))

		nextChunkID := result.HandledChunkID + 1
		for nextChunkID <= expectChunkID {
			buf, err := c.chunksCache.get(nextChunkID)
			if err != nil {
				return err
			}
			c.e.logger.Info("retry to put chunk",
				zap.Uint64("sender", c.id),
				zap.Uint64("chunkID", nextChunkID))

			result, err = c.sendPutChunk(ctx, nextChunkID, buf)
			if err != nil {
				return err
			}
			result, done, err = c.retrySPFreshBuildChunkRecovery(ctx, result, nextChunkID)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
			nextChunkID = result.HandledChunkID + 1
		}
	}

	state := c.loadState()
	handledChunkID := max(state.HandledChunkID, result.HandledChunkID)
	if c.e.taskType == taskTypeSPFreshBuild {
		// SPFresh build PUT reports in-memory ingest progress. Only POST flush
		// forces a checkpoint, so keep cached chunks until flush commits them.
		c.storeState(state.FlushedChunkID, handledChunkID)
		return nil
	}

	flushedChunkID := max(state.FlushedChunkID, result.FlushedChunkID)
	c.cleanChunkCache(state, flushedChunkID)
	c.storeState(flushedChunkID, handledChunkID)
	return nil
}

func (c *chunkSender) resendChunks(ctx context.Context, startChunkID, endChunkID uint64) error {
	if startChunkID == 0 {
		startChunkID = 1
	}
	for chunkID := startChunkID; chunkID <= endChunkID; {
		buf, err := c.chunksCache.get(chunkID)
		if err != nil {
			return err
		}
		c.e.logger.Info("retry to put chunk",
			zap.Uint64("sender", c.id),
			zap.Uint64("chunkID", chunkID))

		result, err := c.sendPutChunk(ctx, chunkID, buf)
		if err != nil {
			return err
		}
		if err := c.handlePutChunkResult(ctx, result, chunkID); err != nil {
			return err
		}
		nextChunkID := result.HandledChunkID + 1
		if nextChunkID <= chunkID {
			nextChunkID = chunkID + 1
		}
		chunkID = nextChunkID
	}
	return nil
}

func (c *chunkSender) sendFlushToRemote(ctx context.Context) error {
	targetChunkID := c.getLastChunkID()
	missingMetadataCount := 0
	for {
		url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&flush=true&writer_id=%d",
			c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id)
		if c.e.taskType == taskTypeSPFreshBuild {
			url = fmt.Sprintf("%s/spfresh/build?cluster_id=%d&task_id=%s&flush=true&writer_id=%d&target_chunk_id=%d",
				c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, targetChunkID)
		}

		var data []byte
		var statusCode int
		var err error
		if c.e.taskType == taskTypeSPFreshBuild {
			data, statusCode, err = sendLongOwnerBoundRequestAllowAccepted(
				ctx,
				c.httpClient,
				"POST",
				url,
				nil,
				c.e.retryCounter,
			)
		} else {
			data, err = sendLongOwnerBoundRequest(ctx, c.httpClient, "POST", url, nil, c.e.retryCounter)
		}
		if err != nil {
			c.e.logger.Error("failed to flush", zap.Error(err))
			return err
		}

		result := new(FlushResult)
		err = json.Unmarshal(data, result)
		if err != nil {
			if statusCode != http.StatusAccepted {
				return err
			}
			// Flush retains bounded fallback for accepted responses with
			// missing recovery metadata while polling the same barrier.
			result = new(FlushResult)
		}

		if result.Canceled {
			c.e.logger.Error("failed to flush, task is canceled",
				zap.String("error", result.Error))
			return ErrTaskCanceled
		}
		if result.Error != "" && statusCode != http.StatusAccepted {
			c.e.logger.Error("failed to flush",
				zap.String("error", result.Error))
			return ErrFlushRemoteWorker
		}

		if statusCode == http.StatusAccepted ||
			isSPFreshBuildRecoveryStatus(result.Status) {
			c.e.logger.Debug("SPFresh build flush accepted while remote task recovers",
				zap.String("taskID", c.e.loadDataTaskID),
				zap.String("status", result.Status),
				zap.String("retryReason", result.RetryReason),
				zap.Uint64("retryAttempt", result.RetryAttempt))
			var err error
			missingMetadataCount, err = waitForSPFreshBuildRetry(
				ctx,
				result.NextRetryAtMs,
				missingMetadataCount,
			)
			if err != nil {
				return err
			}
			continue
		}

		if result.Finished {
			c.e.logger.Info("remote task finished", zap.String("taskID", c.e.loadDataTaskID))
			return nil
		}

		// make sure all chunks are flushed
		flushedChunkID := result.FlushedChunkIDs[c.id]
		if flushedChunkID >= targetChunkID {
			// all chunks are flushed, clean cache and update state
			state := c.loadState()
			committedChunkID := min(flushedChunkID, targetChunkID)
			c.cleanChunkCache(state, committedChunkID)
			c.storeState(committedChunkID, max(state.HandledChunkID, committedChunkID))
			return nil
		}

		// the flushed chunkID is not equal to the last sent chunkID,
		// because the remote worker may restart, retry to put chunk.
		if c.e.taskType == taskTypeSPFreshBuild {
			if err = c.resendChunks(ctx, flushedChunkID+1, targetChunkID); err != nil {
				return err
			}
		} else if err = c.putEmptyChunk(ctx); err != nil {
			return err
		}
	}
}

func (c *chunkSender) close() error {
	// notify chunkSenderLoop to quit
	close(c.loopQuitChan)
	c.wg.Wait()

	return c.err
}
