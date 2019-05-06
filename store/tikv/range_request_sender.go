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
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	rangeTaskDefaultStatLogInterval = time.Minute * 10
	rangeTaskDefaultMaxBackoff      = 20000
	rangeTaskDefaultReqTimeout      = time.Minute

	rangeWorkerUpdateStatInterval = time.Millisecond * 500
)

// RangeRequestSender splits a range into many ranges to process concurrently, and convenient to send requests to all
// regions in the range. Because of merging and splitting, it's possible that multiple requests for disjoint ranges are
// sent to the same region.
type RangeRequestSender struct {
	name            string
	store           Storage
	concurrency     int
	requestCreator  RequestCreator
	responseHandler ResponseHandler
	maxBackoff      int
	reqTimeout      time.Duration
	statLogInterval time.Duration

	completedRegions int32
	failedRegions    int32
}

// RequestCreator is the type of functions that create request from a key range. It should be able to be invoked
// multiple times. The given range is inner a region.
type RequestCreator = func(startKey []byte, endKey []byte) *tikvrpc.Request

// ResponseHandler is the type of functions that process responses or errors from TiKV RPC. It should be able to be
// invoked multiple times.
type ResponseHandler = func(response *tikvrpc.Response, err error) error

// NewRangeRequestSender creates a RangeRequestSender.
//
// `requestCreator` is the function used to create RPC request according to the given range.
// `responseHandler` is the function to process responses of errors. If `responseHandler` returns error, the whole job
// will be canceled.
func NewRangeRequestSender(
	name string,
	store Storage,
	concurrency int,
	requestCreator RequestCreator,
	responseHandler ResponseHandler,
) *RangeRequestSender {
	return &RangeRequestSender{
		name:            name,
		store:           store,
		concurrency:     concurrency,
		requestCreator:  requestCreator,
		responseHandler: responseHandler,
		maxBackoff:      rangeTaskDefaultMaxBackoff,
		reqTimeout:      rangeTaskDefaultReqTimeout,
		statLogInterval: rangeTaskDefaultStatLogInterval,
	}
}

// SetMaxBackoff sets the max backoff for each single request sent by RangeRequestSender
func (s *RangeRequestSender) SetMaxBackoff(backoff int) {
	s.maxBackoff = backoff
}

// SetReqTimeout sets the timeout of each RPC request sent by RangeRequestSender
func (s *RangeRequestSender) SetReqTimeout(timeout time.Duration) {
	s.reqTimeout = timeout
}

// SetStatLogInterval sets the time interval to log the stats.
func (s *RangeRequestSender) SetStatLogInterval(interval time.Duration) {
	s.statLogInterval = interval
}

// RunOnRange runs the task on the given range.
func (s *RangeRequestSender) RunOnRange(ctx context.Context, startKey []byte, endKey []byte) error {
	logutil.Logger(ctx).Info("range task started",
		zap.String("name", s.name),
		zap.Binary("startKey", startKey),
		zap.Binary("endKey", endKey))

	if len(endKey) != 0 && bytes.Compare(startKey, endKey) >= 0 {
		return nil
	}

	s.completedRegions = 0
	s.failedRegions = 0

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
		statLogTicker.Stop()
		close(taskCh)
		cancel()
		wg.Wait()
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
				zap.Int32("completedRegions", s.CompletedRegions()),
				zap.Int32("failedRegions", s.FailedRegions()))
		default:
		}

		bo := NewBackoffer(ctx, locateRegionMaxBackoff)

		loc, err := s.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}
		r := &kv.KeyRange{
			StartKey: key,
			EndKey:   loc.EndKey,
		}

		isLast := len(r.EndKey) == 0 || (len(endKey) > 0 && bytes.Compare(r.EndKey, endKey) >= 0)
		// Let r.EndKey = min(endKey, loc.EndKey)
		if isLast {
			r.EndKey = endKey
		}

		taskCh <- r

		if isLast {
			break
		}

		key = r.EndKey
	}

	return nil
}

// createWorker creates a worker that can process tasks from the given channel.
func (s *RangeRequestSender) createWorker(taskCh chan *kv.KeyRange, wg *sync.WaitGroup) *rangeRequestWorker {
	return &rangeRequestWorker{
		name:            s.name,
		store:           s.store,
		requestCreator:  s.requestCreator,
		responseHandler: s.responseHandler,
		maxBackoff:      s.maxBackoff,
		reqTimeout:      s.reqTimeout,
		taskCh:          taskCh,
		wg:              wg,

		totalCompletedRegions: &s.completedRegions,
		totalFailedRegions:    &s.failedRegions,
	}
}

// CompletedRegions returns how many regions has been sent requests.
func (s *RangeRequestSender) CompletedRegions() int32 {
	return atomic.LoadInt32(&s.completedRegions)
}

// FailedRegions returns how many regions were unable to process the requests successfully.
func (s *RangeRequestSender) FailedRegions() int32 {
	return atomic.LoadInt32(&s.failedRegions)
}

// rangeRequestWorker is used by RangeRequestSender to process tasks concurrently.
type rangeRequestWorker struct {
	name            string
	store           Storage
	requestCreator  RequestCreator
	responseHandler ResponseHandler
	maxBackoff      int
	reqTimeout      time.Duration
	taskCh          chan *kv.KeyRange
	wg              *sync.WaitGroup

	completedRegions int32
	failedRegions    int32

	totalCompletedRegions *int32
	totalFailedRegions    *int32
}

// run runs the worker. It collects all objects from `w.taskCh` and process them one by one.
func (w *rangeRequestWorker) run(ctx context.Context, cancel context.CancelFunc) {
	// Each task might take very short time to finish, so if we update the statistics immediately after sending a
	// request to a region, the contention of atomic operations might reduce the performance. So it's better to
	// calculate only the current thread and update it to the RangeRequestSender.
	ticker := time.NewTicker(rangeWorkerUpdateStatInterval)
	defer func() {
		ticker.Stop()
		w.updateStat()
		w.wg.Done()
	}()

	for r := range w.taskCh {
		select {
		case <-ctx.Done():
			break
		case <-ticker.C:
			w.updateStat()
		default:
		}

		err := w.runForRange(ctx, r.StartKey, r.EndKey)

		if err != nil {
			logutil.Logger(ctx).Info("canceling range request task because of error",
				zap.String("name", w.name),
				zap.Error(err))
			cancel()
			return
		}
	}
}

func (w *rangeRequestWorker) updateStat() {
	atomic.AddInt32(w.totalCompletedRegions, w.completedRegions)
	atomic.AddInt32(w.totalFailedRegions, w.failedRegions)
	w.completedRegions = 0
	w.failedRegions = 0
}

// runForRange runs task for the given range. The range might contains multiple regions, because after the range sent
// from the master, the region may split.
func (w *rangeRequestWorker) runForRange(ctx context.Context, startKey []byte, endKey []byte) error {
	key := startKey

	for {
		bo := NewBackoffer(ctx, w.maxBackoff)
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}

		// Compare only once
		isLast := len(loc.EndKey) == 0 || (len(endKey) > 0 && bytes.Compare(loc.EndKey, endKey) >= 0)

		reqStartKey := key
		reqEndKey := endKey
		if !isLast {
			reqEndKey = loc.EndKey
		}

		req := w.requestCreator(reqStartKey, reqEndKey)
		if req == nil {
			logutil.Logger(ctx).Info("range request task created request, skip sending request to region",
				zap.String("name", w.name),
				zap.Uint64("region", loc.Region.GetID()),
				zap.Binary("rangeStartKey", reqStartKey),
				zap.Binary("rangeEndKey", reqEndKey))
		} else {
			var resp *tikvrpc.Response
			resp, err = w.sendReq(ctx, bo, req, loc.Region)

			if err == nil {
				var regionErr *errorpb.Error
				regionErr, err = resp.GetRegionError()
				if regionErr != nil {
					// If region error occurs, retry it.
					continue
				}
			}

			if err != nil {
				logutil.Logger(ctx).Info("range request task failed sending request to region",
					zap.String("name", w.name),
					zap.Uint64("region", loc.Region.GetID()),
					zap.Binary("rangeStartKey", reqStartKey),
					zap.Binary("rangeEndKey", reqEndKey),
					zap.Error(err))
				w.failedRegions++
			} else {
				w.completedRegions++
			}
			err = w.responseHandler(resp, err)
			if err != nil {
				return errors.Trace(err)
			}
		}

		key = reqEndKey

		if isLast {
			return nil
		}
	}
}

func (w *rangeRequestWorker) sendReq(ctx context.Context, bo *Backoffer, req *tikvrpc.Request, region RegionVerID) (*tikvrpc.Response, error) {
	resp, err := w.store.SendReq(bo, req, region, w.reqTimeout)

	if err != nil {
		return nil, errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if regionErr != nil {
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return resp, nil
}
