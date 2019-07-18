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

// Package tikv provides tcp connection to kvserver.
package tikv

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type batchConn struct {
	index uint32
	// batchCommandsCh used for batch commands.
	batchCommandsCh        chan *batchCommandsEntry
	batchCommandsClients   []*batchCommandsClient
	tikvTransportLayerLoad uint64
	closed                 chan struct{}

	// Notify rpcClient to check the idle flag
	idleNotify *uint32
	idle       bool
	idleDetect *time.Timer

	pendingRequests prometheus.Gauge
}

func newBatchConn(connCount, maxBatchSize uint, idleNotify *uint32) *batchConn {
	return &batchConn{
		batchCommandsCh:        make(chan *batchCommandsEntry, maxBatchSize),
		batchCommandsClients:   make([]*batchCommandsClient, 0, connCount),
		tikvTransportLayerLoad: 0,
		closed:                 make(chan struct{}),

		idleNotify: idleNotify,
		idleDetect: time.NewTimer(idleTimeout),
	}
}

// fetchAllPendingRequests fetches all pending requests from the channel.
func (a *batchConn) fetchAllPendingRequests(
	maxBatchSize int,
	entries *[]*batchCommandsEntry,
	requests *[]*tikvpb.BatchCommandsRequest_Request,
) {
	// Block on the first element.
	var headEntry *batchCommandsEntry
	select {
	case headEntry = <-a.batchCommandsCh:
		if !a.idleDetect.Stop() {
			<-a.idleDetect.C
		}
		a.idleDetect.Reset(idleTimeout)
	case <-a.idleDetect.C:
		a.idleDetect.Reset(idleTimeout)
		a.idle = true
		atomic.CompareAndSwapUint32(a.idleNotify, 0, 1)
		// This batchConn to be recycled
		return
	case <-a.closed:
		return
	}
	if headEntry == nil {
		return
	}
	*entries = append(*entries, headEntry)
	*requests = append(*requests, headEntry.req)

	// This loop is for trying best to collect more requests.
	for len(*entries) < maxBatchSize {
		select {
		case entry := <-a.batchCommandsCh:
			if entry == nil {
				return
			}
			*entries = append(*entries, entry)
			*requests = append(*requests, entry.req)
		default:
			return
		}
	}
}

// fetchMorePendingRequests fetches more pending requests from the channel.
func fetchMorePendingRequests(
	ch chan *batchCommandsEntry,
	maxBatchSize int,
	batchWaitSize int,
	maxWaitTime time.Duration,
	entries *[]*batchCommandsEntry,
	requests *[]*tikvpb.BatchCommandsRequest_Request,
) {
	waitStart := time.Now()

	// Try to collect `batchWaitSize` requests, or wait `maxWaitTime`.
	after := time.NewTimer(maxWaitTime)
	for len(*entries) < batchWaitSize {
		select {
		case entry := <-ch:
			if entry == nil {
				return
			}
			*entries = append(*entries, entry)
			*requests = append(*requests, entry.req)
		case waitEnd := <-after.C:
			metrics.TiKVBatchWaitDuration.Observe(float64(waitEnd.Sub(waitStart)))
			return
		}
	}
	after.Stop()

	// Do an additional non-block try. Here we test the lengh with `maxBatchSize` instead
	// of `batchWaitSize` because trying best to fetch more requests is necessary so that
	// we can adjust the `batchWaitSize` dynamically.
	for len(*entries) < maxBatchSize {
		select {
		case entry := <-ch:
			if entry == nil {
				return
			}
			*entries = append(*entries, entry)
			*requests = append(*requests, entry.req)
		default:
			metrics.TiKVBatchWaitDuration.Observe(float64(time.Since(waitStart)))
			return
		}
	}
}

type batchCommandsClient struct {
	// The target host.
	target string

	conn                   *grpc.ClientConn
	client                 tikvpb.Tikv_BatchCommandsClient
	batched                sync.Map
	idAlloc                uint64
	tikvTransportLayerLoad *uint64

	// closed indicates the batch client is closed explicitly or not.
	closed int32
	// clientLock protects client when re-create the streaming.
	clientLock sync.Mutex
}

func (c *batchCommandsClient) isStopped() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

func (c *batchCommandsClient) send(request *tikvpb.BatchCommandsRequest, entries []*batchCommandsEntry) {
	// Use the lock to protect the stream client won't be replaced by RecvLoop,
	// and new added request won't be removed by `failPendingRequests`.
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	for i, requestID := range request.RequestIds {
		c.batched.Store(requestID, entries[i])
	}
	if err := c.client.Send(request); err != nil {
		logutil.BgLogger().Error(
			"batch commands send error",
			zap.String("target", c.target),
			zap.Error(err),
		)
		c.failPendingRequests(err)
	}
}

func (c *batchCommandsClient) recv() (*tikvpb.BatchCommandsResponse, error) {
	failpoint.Inject("gotErrorInRecvLoop", func(_ failpoint.Value) (*tikvpb.BatchCommandsResponse, error) {
		return nil, errors.New("injected error in batchRecvLoop")
	})
	// When `conn.Close()` is called, `client.Recv()` will return an error.
	return c.client.Recv()
}

// `failPendingRequests` must be called in locked contexts in order to avoid double closing channels.
func (c *batchCommandsClient) failPendingRequests(err error) {
	failpoint.Inject("panicInFailPendingRequests", nil)
	c.batched.Range(func(key, value interface{}) bool {
		id, _ := key.(uint64)
		entry, _ := value.(*batchCommandsEntry)
		entry.err = err
		c.batched.Delete(id)
		close(entry.res)
		return true
	})
}

func (c *batchCommandsClient) reCreateStreamingClient(err error, skipLog bool) error {
	// Hold the lock to forbid batchSendLoop using the old client.
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	c.failPendingRequests(err) // fail all pending requests.

	// Re-establish a application layer stream. TCP layer is handled by gRPC.
	tikvClient := tikvpb.NewTikvClient(c.conn)
	streamClient, err := tikvClient.BatchCommands(context.TODO())
	if err == nil {
		logutil.BgLogger().Info(
			"batchRecvLoop re-create streaming success",
			zap.String("target", c.target),
		)
		c.client = streamClient
		return err
	}
	if !skipLog {
		logutil.BgLogger().Error(
			"batchRecvLoop re-create streaming fail",
			zap.String("target", c.target),
			zap.Error(err),
		)
	}
	return err
}

func (c *batchCommandsClient) batchRecvLoop(cfg config.TiKVClient) {
	defer func() {
		if r := recover(); r != nil {
			metrics.PanicCounter.WithLabelValues(metrics.LabelBatchRecvLoop).Inc()
			logutil.BgLogger().Error("batchRecvLoop",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			logutil.BgLogger().Info("restart batchRecvLoop")
			go c.batchRecvLoop(cfg)
		}
	}()

	for {
		resp, err := c.recv()
		if err != nil {
			b := NewBackoffer(context.Background(), math.MaxInt32)
			now := time.Now()
			skipLog := false
			for count := 0; ; count++ { // try to re-create the streaming in the loop.
				if c.isStopped() {
					return
				}

				if count <= 10 {
					logutil.BgLogger().Error(
						"batchRecvLoop error when receive",
						zap.String("target", c.target),
						zap.Error(err),
					)
					if count == 10 {
						logutil.BgLogger().Error("meet error for 10 times and don't show the same log any more")
						skipLog = true
					}
				}

				err1 := c.reCreateStreamingClient(err, skipLog)
				if err1 == nil {
					break
				}
				if err := b.Backoff(boTiKVRPC, err1); err != nil {
					logutil.BgLogger().Error("backoff error", zap.Error(err))
				}
			}
			metrics.TiKVBatchClientUnavailable.Observe(time.Since(now).Seconds())
			continue
		}

		responses := resp.GetResponses()
		for i, requestID := range resp.GetRequestIds() {
			value, ok := c.batched.Load(requestID)
			if !ok {
				// There shouldn't be any unknown responses because if the old entries
				// are cleaned by `failPendingRequests`, the stream must be re-created
				// so that old responses will be never received.
				panic("batchRecvLoop receives a unknown response")
			}
			entry := value.(*batchCommandsEntry)
			if atomic.LoadInt32(&entry.canceled) == 0 {
				// Put the response only if the request is not canceled.
				entry.res <- responses[i]
			}
			c.batched.Delete(requestID)
		}

		tikvTransportLayerLoad := resp.GetTransportLayerLoad()
		if tikvTransportLayerLoad > 0.0 && cfg.MaxBatchWaitTime > 0 {
			// We need to consider TiKV load only if batch-wait strategy is enabled.
			atomic.StoreUint64(c.tikvTransportLayerLoad, tikvTransportLayerLoad)
		}
	}
}

type batchCommandsEntry struct {
	req *tikvpb.BatchCommandsRequest_Request
	res chan *tikvpb.BatchCommandsResponse_Response

	// canceled indicated the request is canceled or not.
	canceled int32
	err      error
}

func (b *batchCommandsEntry) isCanceled() bool {
	return atomic.LoadInt32(&b.canceled) == 1
}

const idleTimeout = 3 * time.Minute

func (a *batchConn) batchSendLoop(cfg config.TiKVClient) {
	defer func() {
		if r := recover(); r != nil {
			metrics.PanicCounter.WithLabelValues(metrics.LabelBatchSendLoop).Inc()
			logutil.BgLogger().Error("batchSendLoop",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			logutil.BgLogger().Info("restart batchSendLoop")
			go a.batchSendLoop(cfg)
		}
	}()

	entries := make([]*batchCommandsEntry, 0, cfg.MaxBatchSize)
	requests := make([]*tikvpb.BatchCommandsRequest_Request, 0, cfg.MaxBatchSize)
	requestIDs := make([]uint64, 0, cfg.MaxBatchSize)

	var bestBatchWaitSize = cfg.BatchWaitSize
	for {
		// Choose a connection by round-robbin.
		next := atomic.AddUint32(&a.index, 1) % uint32(len(a.batchCommandsClients))
		batchCommandsClient := a.batchCommandsClients[next]

		entries = entries[:0]
		requests = requests[:0]
		requestIDs = requestIDs[:0]

		a.pendingRequests.Set(float64(len(a.batchCommandsCh)))
		a.fetchAllPendingRequests(int(cfg.MaxBatchSize), &entries, &requests)

		if len(entries) < int(cfg.MaxBatchSize) && cfg.MaxBatchWaitTime > 0 {
			tikvTransportLayerLoad := atomic.LoadUint64(batchCommandsClient.tikvTransportLayerLoad)
			// If the target TiKV is overload, wait a while to collect more requests.
			if uint(tikvTransportLayerLoad) >= cfg.OverloadThreshold {
				fetchMorePendingRequests(
					a.batchCommandsCh, int(cfg.MaxBatchSize), int(bestBatchWaitSize),
					cfg.MaxBatchWaitTime, &entries, &requests,
				)
			}
		}
		length := len(requests)
		if uint(length) == 0 {
			// The batch command channel is closed.
			return
		} else if uint(length) < bestBatchWaitSize && bestBatchWaitSize > 1 {
			// Waits too long to collect requests, reduce the target batch size.
			bestBatchWaitSize -= 1
		} else if uint(length) > bestBatchWaitSize+4 && bestBatchWaitSize < cfg.MaxBatchSize {
			bestBatchWaitSize += 1
		}

		length = removeCanceledRequests(&entries, &requests)
		if length == 0 {
			continue // All requests are canceled.
		}
		maxBatchID := atomic.AddUint64(&batchCommandsClient.idAlloc, uint64(length))
		for i := 0; i < length; i++ {
			requestID := uint64(i) + maxBatchID - uint64(length)
			requestIDs = append(requestIDs, requestID)
		}

		req := &tikvpb.BatchCommandsRequest{
			Requests:   requests,
			RequestIds: requestIDs,
		}

		batchCommandsClient.send(req, entries)
	}
}

func (a *batchConn) Close() {
	// Close all batchRecvLoop.
	for _, c := range a.batchCommandsClients {
		// After connections are closed, `batchRecvLoop`s will check the flag.
		atomic.StoreInt32(&c.closed, 1)
	}
	// Don't close(batchCommandsCh) because when Close() is called, someone maybe
	// calling SendRequest and writing batchCommandsCh, if we close it here the
	// writing goroutine will panic.
	close(a.closed)
}

// removeCanceledRequests removes canceled requests before sending.
func removeCanceledRequests(
	entries *[]*batchCommandsEntry,
	requests *[]*tikvpb.BatchCommandsRequest_Request) int {
	validEntries := (*entries)[:0]
	validRequets := (*requests)[:0]
	for _, e := range *entries {
		if !e.isCanceled() {
			validEntries = append(validEntries, e)
			validRequets = append(validRequets, e.req)
		}
	}
	*entries = validEntries
	*requests = validRequets
	return len(*entries)
}

func sendBatchRequest(
	ctx context.Context,
	addr string,
	batchConn *batchConn,
	req *tikvpb.BatchCommandsRequest_Request,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	entry := &batchCommandsEntry{
		req:      req,
		res:      make(chan *tikvpb.BatchCommandsResponse_Response, 1),
		canceled: 0,
		err:      nil,
	}
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case batchConn.batchCommandsCh <- entry:
	case <-ctx1.Done():
		logutil.BgLogger().Warn("send request is cancelled",
			zap.String("to", addr), zap.String("cause", ctx1.Err().Error()))
		return nil, errors.Trace(ctx1.Err())
	}

	select {
	case res, ok := <-entry.res:
		if !ok {
			return nil, errors.Trace(entry.err)
		}
		return tikvrpc.FromBatchCommandsResponse(res), nil
	case <-ctx1.Done():
		atomic.StoreInt32(&entry.canceled, 1)
		logutil.BgLogger().Warn("wait response is cancelled",
			zap.String("to", addr), zap.String("cause", ctx1.Err().Error()))
		return nil, errors.Trace(ctx1.Err())
	}
}

func (c *rpcClient) recycleIdleConnArray() {
	var addrs []string
	c.RLock()
	for _, conn := range c.conns {
		if conn.idle {
			addrs = append(addrs, conn.target)
		}
	}
	c.RUnlock()

	for _, addr := range addrs {
		c.Lock()
		conn, ok := c.conns[addr]
		if ok {
			delete(c.conns, addr)
			logutil.BgLogger().Info("recycle idle connection",
				zap.String("target", addr))
		}
		c.Unlock()
		if conn != nil {
			conn.Close()
		}
	}
}
