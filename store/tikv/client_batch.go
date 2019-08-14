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
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type batchConn struct {
	// batchCommandsCh used for batch commands.
	batchCommandsCh        chan *batchCommandsEntry
	batchCommandsClients   []*batchCommandsClient
	tikvTransportLayerLoad uint64
	closed                 chan struct{}

	// Notify rpcClient to check the idle flag
	idleNotify *uint32
	idleDetect *time.Timer

	pendingRequests prometheus.Gauge

	index uint32
	idle  bool
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

type tryLock struct {
	sync.RWMutex
	reCreating bool
}

func (l *tryLock) tryLockForSend() bool {
	l.RLock()
	if l.reCreating {
		l.RUnlock()
		return false
	}
	return true
}

func (l *tryLock) unlockForSend() {
	l.RUnlock()
}

func (l *tryLock) lockForRecreate() {
	l.Lock()
	l.reCreating = true
	l.Unlock()

}

func (l *tryLock) unlockForRecreate() {
	l.Lock()
	l.reCreating = false
	l.Unlock()
}

type batchCommandsClient struct {
	// The target host.
	target string

	conn    *grpc.ClientConn
	client  tikvpb.Tikv_BatchCommandsClient
	batched sync.Map
	idAlloc uint64

	// closed indicates the batch client is closed explicitly or not.
	closed int32
	// tryLock protects client when re-create the streaming.
	tryLock
}

func (c *batchCommandsClient) isStopped() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

func (c *batchCommandsClient) send(request *tikvpb.BatchCommandsRequest, entries []*batchCommandsEntry) {
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

func (c *batchCommandsClient) reCreateStreamingClientOnce(err error) error {
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

		return nil
	}
	logutil.BgLogger().Error(
		"batchRecvLoop re-create streaming fail",
		zap.String("target", c.target),
		zap.Error(err),
	)
	return err
}

func (c *batchCommandsClient) batchRecvLoop(cfg config.TiKVClient, tikvTransportLayerLoad *uint64) {
	defer func() {
		if r := recover(); r != nil {
			metrics.PanicCounter.WithLabelValues(metrics.LabelBatchRecvLoop).Inc()
			logutil.BgLogger().Error("batchRecvLoop",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			logutil.BgLogger().Info("restart batchRecvLoop")
			go c.batchRecvLoop(cfg, tikvTransportLayerLoad)
		}
	}()

	for {
		resp, err := c.recv()
		if err != nil {
			logutil.BgLogger().Error(
				"batchRecvLoop error when receive",
				zap.String("target", c.target),
				zap.Error(err),
			)

			now := time.Now()
			if stopped := c.reCreateStreamingClient(err); stopped {
				return
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
			logutil.Eventf(entry.ctx, "receive %T response with other %d batched requests from %s", responses[i].GetCmd(), len(responses), c.target)
			if atomic.LoadInt32(&entry.canceled) == 0 {
				// Put the response only if the request is not canceled.
				entry.res <- responses[i]
			}
			c.batched.Delete(requestID)
		}

		transportLayerLoad := resp.GetTransportLayerLoad()
		if transportLayerLoad > 0.0 && cfg.MaxBatchWaitTime > 0 {
			// We need to consider TiKV load only if batch-wait strategy is enabled.
			atomic.StoreUint64(tikvTransportLayerLoad, transportLayerLoad)
		}
	}
}

func (c *batchCommandsClient) reCreateStreamingClient(err error) (stopped bool) {
	// Forbids the batchSendLoop using the old client.
	c.lockForRecreate()
	defer c.unlockForRecreate()

	b := NewBackoffer(context.Background(), math.MaxInt32)
	for { // try to re-create the streaming in the loop.
		if c.isStopped() {
			return true
		}
		err1 := c.reCreateStreamingClientOnce(err)
		if err1 == nil {
			break
		}

		err2 := b.Backoff(boTiKVRPC, err1)
		// As timeout is set to math.MaxUint32, err2 should always be nil.
		// This line is added to make the 'make errcheck' pass.
		terror.Log(err2)
	}
	return false
}

type batchCommandsEntry struct {
	ctx context.Context
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
		entries = entries[:0]
		requests = requests[:0]
		requestIDs = requestIDs[:0]

		a.pendingRequests.Set(float64(len(a.batchCommandsCh)))
		a.fetchAllPendingRequests(int(cfg.MaxBatchSize), &entries, &requests)

		if len(entries) < int(cfg.MaxBatchSize) && cfg.MaxBatchWaitTime > 0 {
			// If the target TiKV is overload, wait a while to collect more requests.
			if atomic.LoadUint64(&a.tikvTransportLayerLoad) >= uint64(cfg.OverloadThreshold) {
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

		entries, requests = removeCanceledRequests(entries, requests)
		if len(entries) == 0 {
			continue // All requests are canceled.
		}

		a.getClientAndSend(entries, requests, requestIDs)
	}
}

func (a *batchConn) getClientAndSend(entries []*batchCommandsEntry, requests []*tikvpb.BatchCommandsRequest_Request, requestIDs []uint64) {
	// Choose a connection by round-robbin.
	var cli *batchCommandsClient = nil
	var target string = ""
	for i := 0; i < len(a.batchCommandsClients); i++ {
		a.index = (a.index + 1) % uint32(len(a.batchCommandsClients))
		target = a.batchCommandsClients[a.index].target
		// The lock protects the batchCommandsClient from been closed while it's inuse.
		if a.batchCommandsClients[a.index].tryLockForSend() {
			cli = a.batchCommandsClients[a.index]
			break
		}
	}
	if cli == nil {
		logutil.BgLogger().Warn("no available connections", zap.String("target", target))
		for _, entry := range entries {
			// Please ensure the error is handled in region cache correctly.
			entry.err = errors.New("no available connections")
			close(entry.res)
		}
		return
	}
	defer cli.unlockForSend()

	maxBatchID := atomic.AddUint64(&cli.idAlloc, uint64(len(requests)))
	for i := 0; i < len(requests); i++ {
		requestID := uint64(i) + maxBatchID - uint64(len(requests))
		requestIDs = append(requestIDs, requestID)
	}
	req := &tikvpb.BatchCommandsRequest{
		Requests:   requests,
		RequestIds: requestIDs,
	}

	cli.send(req, entries)
	return
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
func removeCanceledRequests(entries []*batchCommandsEntry,
	requests []*tikvpb.BatchCommandsRequest_Request) ([]*batchCommandsEntry, []*tikvpb.BatchCommandsRequest_Request) {
	validEntries := entries[:0]
	validRequets := requests[:0]
	for _, e := range entries {
		if !e.isCanceled() {
			validEntries = append(validEntries, e)
			validRequets = append(validRequets, e.req)
		}
	}
	return validEntries, validRequets
}

func sendBatchRequest(
	ctx context.Context,
	addr string,
	batchConn *batchConn,
	req *tikvpb.BatchCommandsRequest_Request,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	entry := &batchCommandsEntry{
		ctx:      ctx,
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
