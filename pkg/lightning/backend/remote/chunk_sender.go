package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"go.uber.org/zap"
)

// PutChunkResult is json data that returned by remote server PUT API.
type PutChunkResult struct {
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	HandledChunkID uint64 `json:"handled-chunk-id"`
	Canceled       bool   `json:"canceled"`
	Finished       bool   `json:"finished"`
	Error          string `json:"error"`
}

// FlushResult is json data that returned by remote server POST API.
type FlushResult struct {
	FlushedChunkIDs map[uint64]uint64 `json:"flushed-chunk-ids"`
	Canceled        bool              `json:"canceled"`
	Finished        bool              `json:"finished"`
	Error           string            `json:"error"`
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

	task := newChunkTask(data0, false)
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

func (c *chunkSender) getFlushedChunkID() uint64 {
	return c.state.Load().(*chunksState).FlushedChunkID
}

func (c *chunkSender) chunkSenderLoop(ctx context.Context) {
	defer func() {
		close(c.loopDoneChan)
		err := c.chunksCache.close()
		if err != nil {
			c.e.logger.Warn("failed to close chunk cache", zap.Error(err))
		}
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
					return
				}
			} else {
				c.err = c.sendFlushToRemote(ctx)
				if c.err != nil {
					return
				}
				task.done(0)
			}
		case <-ticker.C:
			// Periodically send empty chunks to update the flushed chunkID.
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

func (c *chunkSender) putEmptyChunk(ctx context.Context) error {
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

	url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
		c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, chunk.id)

	data, err := sendRequest(ctx, c.httpClient, "PUT", url, chunk.data)
	if err != nil {
		return err
	}
	result := new(PutChunkResult)
	err = json.Unmarshal(data, result)
	if err != nil {
		return err
	}

	err = c.handlePutChunkResult(ctx, result, chunk.id)
	if err != nil {
		return err
	}

	return nil
}

func (c *chunkSender) handlePutChunkResult(ctx context.Context, result *PutChunkResult, expectChunkID uint64) error {
	if result.Canceled {
		c.e.logger.Error("failed to put chunk, task is canceled",
			zap.String("error", result.Error),
			zap.Bool("finished", result.Finished))
		return common.ErrLoadDataTaskCanceled.FastGenByArgs(c.e.loadDataTaskID, result.Error)
	}

	if result.HandledChunkID != expectChunkID {
		c.e.logger.Info("remote worker may restart, retry to put chunk",
			zap.Uint64("expectChunkID", expectChunkID),
			zap.Uint64("handledChunkID", result.HandledChunkID),
			zap.Uint64("flushedChunkID", result.FlushedChunkID),
			zap.String("error", result.Error))

		nextChunkID := result.HandledChunkID + 1
		for nextChunkID <= expectChunkID {
			url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
				c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, nextChunkID)

			buf, err := c.chunksCache.get(nextChunkID)
			if err != nil {
				return err
			}
			c.e.logger.Info("retry to put chunk",
				zap.Uint64("sender", c.id),
				zap.Uint64("chunkID", nextChunkID))

			data, err := sendRequest(ctx, c.httpClient, "PUT", url, buf)
			if err != nil {
				return err
			}
			result = new(PutChunkResult)
			err = json.Unmarshal(data, result)
			if err != nil {
				return err
			}
			nextChunkID = result.HandledChunkID + 1
		}
	}

	state := c.state.Load().(*chunksState)
	lastFlushedChunkID := state.FlushedChunkID + 1
	for lastFlushedChunkID <= result.FlushedChunkID {
		err := c.chunksCache.clean(lastFlushedChunkID)
		if err != nil {
			c.e.logger.Info("failed to clean chunk cache",
				zap.Uint64("chunkID", lastFlushedChunkID),
				zap.Error(err))
		}
		lastFlushedChunkID++
	}

	c.state.Store(&chunksState{
		FlushedChunkID: result.FlushedChunkID,
		HandledChunkID: result.HandledChunkID,
	})
	return nil
}

func (c *chunkSender) sendFlushToRemote(ctx context.Context) error {
	for {
		url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&flush=true&writer_id=%d",
			c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id)

		data, err := sendRequest(ctx, c.httpClient, "POST", url, nil)
		if err != nil {
			c.e.logger.Error("failed to flush", zap.Error(err))
			return err
		}

		result := new(FlushResult)
		err = json.Unmarshal(data, result)
		if err != nil {
			return err
		}

		if result.Canceled || result.Error != "" {
			c.e.logger.Error("failed to flush",
				zap.Bool("canceled", result.Canceled),
				zap.String("error", result.Error))
			return common.ErrLoadDataTaskCanceled.FastGenByArgs(c.e.loadDataTaskID, result.Error)
		}

		if result.Finished {
			c.e.logger.Info("loadDataTask finished")
			return nil
		}

		// make sure all chunks are flushed
		flushedChunkID := result.FlushedChunkIDs[c.id]
		if flushedChunkID == c.getLastChunkID() {
			// all chunks are flushed, clean cache and update state
			state := c.state.Load().(*chunksState)
			lastFlushedChunkID := state.FlushedChunkID + 1
			for lastFlushedChunkID <= flushedChunkID {
				err := c.chunksCache.clean(lastFlushedChunkID)
				if err != nil {
					c.e.logger.Warn("failed to clean chunk cache", zap.Uint64("chunkID", lastFlushedChunkID), zap.Error(err))
				}
				lastFlushedChunkID++
			}
			state.FlushedChunkID = flushedChunkID

			c.state.Store(state)
			return nil
		}

		// the flushed chunkID is not equal to the last sent chunkID,
		// because the remote worker may restart, retry to put chunk.
		err = c.putEmptyChunk(ctx)
		if err != nil {
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
