// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	rangeTaskDefaultStatLogInterval = time.Minute * 10
	rangeTaskDefaultMaxBackoff      = 20000
)

// RangeTaskRunner splits a range into many ranges to process concurrently, and convenient to send requests to all
// regions in the range. Because of merging and splitting, it's possible that multiple requests for disjoint ranges are
// sent to the same region.
type RangeTaskRunner struct {
	name            string
	store           Storage
	concurrency     int
	handler         RangeTaskHandler
	maxBackoff      int
	statLogInterval time.Duration

	completedRegions int32
}

// RangeTaskHandler is the type of functions that processes a task of a region. It should be able to be invoked multiple
// times.
// If the range can't be processed immediately, returns `nextKey`, which should be in range `r`, to indicate that the
// next task will start from `nextKey`.
type RangeTaskHandler = func(ctx context.Context, bo *Backoffer, r kv.KeyRange, loc *KeyLocation) (nextKey []byte, err error)

// NewRangeTaskRunner creates a RangeTaskRunner.
//
// `requestCreator` is the function used to create RPC request according to the given range.
// `responseHandler` is the function to process responses of errors. If `responseHandler` returns error, the whole job
// will be canceled.
func NewRangeTaskRunner(
	name string,
	store Storage,
	concurrency int,
	handler RangeTaskHandler,
) *RangeTaskRunner {
	return &RangeTaskRunner{
		name:            name,
		store:           store,
		concurrency:     concurrency,
		handler:         handler,
		maxBackoff:      rangeTaskDefaultMaxBackoff,
		statLogInterval: rangeTaskDefaultStatLogInterval,
	}
}

// SetMaxBackoff sets the max backoff for each single task.
func (s *RangeTaskRunner) SetMaxBackoff(backoff int) {
	s.maxBackoff = backoff
}

// SetStatLogInterval sets the time interval to log the stats.
func (s *RangeTaskRunner) SetStatLogInterval(interval time.Duration) {
	s.statLogInterval = interval
}

// RunOnRange runs the task on the given range.
// Empty startKey or endKey means unbounded.
func (s *RangeTaskRunner) RunOnRange(ctx context.Context, startKey []byte, endKey []byte) error {
	s.completedRegions = 0

	if len(endKey) != 0 && bytes.Compare(startKey, endKey) >= 0 {
		logutil.Logger(ctx).Info("empty range task executed. ignored",
			zap.String("name", s.name),
			zap.Binary("startKey", startKey),
			zap.Binary("endKey", endKey))
		return nil
	}

	logutil.Logger(ctx).Info("range task started",
		zap.String("name", s.name),
		zap.Binary("startKey", startKey),
		zap.Binary("endKey", endKey))

	// Periodically log the progress
	statLogTicker := time.NewTicker(s.statLogInterval)

	ctx, cancel := context.WithCancel(ctx)
	taskCh := make(chan *kv.KeyRange, s.concurrency)
	var wg sync.WaitGroup

	// Create workers that concurrently process the whole range.
	for i := 0; i < s.concurrency; i++ {
		w := s.createWorker(taskCh, &wg)
		wg.Add(1)
		go w.run(ctx, cancel)
	}

	defer func() {
		close(taskCh)
		statLogTicker.Stop()
		wg.Wait()
		cancel()
	}()

	startTime := time.Now()

	// Iterate all regions and send each region's range as a task to the workers.
	key := startKey
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-statLogTicker.C:
			logutil.Logger(ctx).Info("range task in progress",
				zap.String("name", s.name),
				zap.Binary("startKey", startKey),
				zap.Binary("endKey", endKey),
				zap.Duration("costTime", time.Since(startTime)),
				zap.Int32("completedRegions", s.CompletedRegions()))
		default:
		}

		bo := NewBackoffer(ctx, locateRegionMaxBackoff)

		loc, err := s.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			logutil.Logger(ctx).Info("range task failed",
				zap.String("name", s.name),
				zap.Binary("startKey", startKey),
				zap.Binary("endKey", endKey),
				zap.Error(err))
			return errors.Trace(err)
		}
		task := &kv.KeyRange{
			StartKey: key,
			EndKey:   loc.EndKey,
		}

		isLast := len(task.EndKey) == 0 || (len(endKey) > 0 && bytes.Compare(task.EndKey, endKey) >= 0)
		// Let task.EndKey = min(endKey, loc.EndKey)
		if isLast {
			task.EndKey = endKey
		}

		taskCh <- task

		if isLast {
			break
		}

		key = task.EndKey
	}

	logutil.Logger(ctx).Info("range task finished",
		zap.String("name", s.name),
		zap.Binary("startKey", startKey),
		zap.Binary("endKey", endKey))

	return nil
}

// createWorker creates a worker that can process tasks from the given channel.
func (s *RangeTaskRunner) createWorker(taskCh chan *kv.KeyRange, wg *sync.WaitGroup) *rangeTaskWorker {
	return &rangeTaskWorker{
		name:       s.name,
		store:      s.store,
		handler:    s.handler,
		maxBackoff: s.maxBackoff,
		taskCh:     taskCh,
		wg:         wg,

		completedRegions: &s.completedRegions,
	}
}

// CompletedRegions returns how many regions has been sent requests.
func (s *RangeTaskRunner) CompletedRegions() int32 {
	return atomic.LoadInt32(&s.completedRegions)
}

// rangeTaskWorker is used by RangeTaskRunner to process tasks concurrently.
type rangeTaskWorker struct {
	name       string
	store      Storage
	handler    RangeTaskHandler
	maxBackoff int
	taskCh     chan *kv.KeyRange
	wg         *sync.WaitGroup

	completedRegions *int32
}

// run starts the worker. It collects all objects from `w.taskCh` and process them one by one.
func (w *rangeTaskWorker) run(ctx context.Context, cancel context.CancelFunc) {
	defer func() {
		w.wg.Done()
	}()

	for r := range w.taskCh {
		select {
		case <-ctx.Done():
			break
		default:
		}

		err := w.runForRange(ctx, r.StartKey, r.EndKey)

		if err != nil {
			logutil.Logger(ctx).Info("canceling range task because of error",
				zap.String("name", w.name),
				zap.Error(err))
			cancel()
			return
		}
	}
}

// runForRange runs task for the given range. The range might contains multiple regions, because after the range sent
// from the master, the region may split.
func (w *rangeTaskWorker) runForRange(ctx context.Context, startKey []byte, endKey []byte) error {
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) >= 0 {
		return nil
	}

	key := startKey

	for {
		bo := NewBackoffer(ctx, w.maxBackoff)
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}

		// Compare only once
		isLast := len(loc.EndKey) == 0 || (len(endKey) > 0 && bytes.Compare(loc.EndKey, endKey) >= 0)

		r := kv.KeyRange{
			StartKey: key,
			EndKey:   endKey,
		}
		if !isLast {
			r.EndKey = loc.EndKey
		}

		nextKey, err := w.handler(ctx, bo, r, loc)
		if err != nil {
			return errors.Trace(err)
		}

		key = r.EndKey
		if len(nextKey) > 0 {
			// The region needs further processing
			key = nextKey
			isLast = false
		} else {
			// If nextKey is set, it means the current region should be processed again. So calculate stats only when
			// nextKey is not set.
			atomic.AddInt32(w.completedRegions, 1)
		}

		if isLast {
			return nil
		}
	}
}
