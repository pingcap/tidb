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
	"runtime/trace"
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
	"google.golang.org/grpc/connectivity"
)

type batchConn struct {
	// An atomic flag indicates whether the batch is idle or not.
	// 0 for busy, others for idle.
	idle uint32

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

func (a *batchConn) isIdle() bool {
	return atomic.LoadUint32(&a.idle) != 0
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
		atomic.AddUint32(&a.idle, 1)
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

	tikvClientCfg config.TiKVClient
	tikvLoad      *uint64
	dialTimeout   time.Duration

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
		if trace.IsEnabled() {
			trace.Log(entries[i].ctx, "rpc", "send")
		}
	}

	if err := c.initBatchClient(); err != nil {
		logutil.BgLogger().Warn(
			"init create streaming fail",
			zap.String("target", c.target),
			zap.Error(err),
		)
		c.failPendingRequests(err)
		return
	}
	if err := c.client.Send(request); err != nil {
		logutil.BgLogger().Info(
			"sending batch commands meets error",
			zap.String("target", c.target),
			zap.Uint64s("requestIDs", request.RequestIds),
			zap.Error(err),
		)
		c.failPendingRequests(err)
	}
}

func (c *batchCommandsClient) recv() (resp *tikvpb.BatchCommandsResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			metrics.PanicCounter.WithLabelValues(metrics.LabelBatchRecvLoop).Inc()
			logutil.BgLogger().Error("batchCommandsClient.recv panic",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			err = errors.SuspendStack(errors.New("batch conn recv paniced"))
		}
	}()
	failpoint.Inject("gotErrorInRecvLoop", func(_ failpoint.Value) (resp *tikvpb.BatchCommandsResponse, err error) {
		err = errors.New("injected error in batchRecvLoop")
		return
	})
	// When `conn.Close()` is called, `client.Recv()` will return an error.
	resp, err = c.client.Recv()
	return
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

func (c *batchCommandsClient) waitConnReady() (err error) {
	if c.conn.GetState() == connectivity.Ready {
		return
	}
	start := time.Now()
	defer func() {
		metrics.TiKVBatchClientWaitEstablish.Observe(time.Since(start).Seconds())
	}()
	dialCtx, cancel := context.WithTimeout(context.Background(), c.dialTimeout)
	for {
		s := c.conn.GetState()
		if s == connectivity.Ready {
			cancel()
			break
		}
		if !c.conn.WaitForStateChange(dialCtx, s) {
			cancel()
			err = dialCtx.Err()
			return
		}
	}
	return
}

func (c *batchCommandsClient) reCreateStreamingClientOnce(perr error) error {
	c.failPendingRequests(perr) // fail all pending requests.

	err := c.waitConnReady()
	// Re-establish a application layer stream. TCP layer is handled by gRPC.
	if err == nil {
		tikvClient := tikvpb.NewTikvClient(c.conn)
		var streamClient tikvpb.Tikv_BatchCommandsClient
		streamClient, err = tikvClient.BatchCommands(context.TODO())
		if err == nil {
			logutil.BgLogger().Info(
				"batchRecvLoop re-create streaming success",
				zap.String("target", c.target),
			)
			c.client = streamClient
			return nil
		}
	}
	logutil.BgLogger().Info(
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
			if c.isStopped() {
				return
			}
			logutil.BgLogger().Info(
				"batchRecvLoop fails when receiving, needs to reconnect",
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
				// this maybe caused by batchCommandsClient#send meets ambiguous error that request has be sent to TiKV but still report a error.
				// then TiKV will send response back though stream and reach here.
				logutil.BgLogger().Warn("batchRecvLoop receives outdated response", zap.Uint64("requestID", requestID))
				continue
			}
			entry := value.(*batchCommandsEntry)

			if trace.IsEnabled() {
				trace.Log(entry.ctx, "rpc", "received")
			}
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

	b := NewBackofferWithVars(context.Background(), math.MaxInt32, nil)
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

func resetEntries(entries []*batchCommandsEntry) []*batchCommandsEntry {
	for i := 0; i < len(entries); i++ {
		entries[i] = nil
	}
	entries = entries[:0]
	return entries
}

func resetRequests(requests []*tikvpb.BatchCommandsRequest_Request) []*tikvpb.BatchCommandsRequest_Request {
	for i := 0; i < len(requests); i++ {
		requests[i] = nil
	}
	requests = requests[:0]
	return requests
}

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
		// NOTE: We can't simply set entries = entries[:0] here.
		// The data in the cap part of the slice would reference the prewrite keys whose
		// underlying memory is borrowed from memdb. The reference cause GC can't release
		// the memdb, leading to serious memory leak problems in the large transaction case.
		entries = resetEntries(entries)
		requests = resetRequests(requests)
		requestIDs = requestIDs[:0]

		a.pendingRequests.Set(float64(len(a.batchCommandsCh)))
		a.fetchAllPendingRequests(int(cfg.MaxBatchSize), &entries, &requests)

		// curl -XPUT -d 'return(true)' http://0.0.0.0:10080/fail/github.com/pingcap/tidb/store/tikv/mockBlockOnBatchClient
		failpoint.Inject("mockBlockOnBatchClient", func(val failpoint.Value) {
			if val.(bool) {
				time.Sleep(1 * time.Hour)
			}
		})

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
			bestBatchWaitSize--
		} else if uint(length) > bestBatchWaitSize+4 && bestBatchWaitSize < cfg.MaxBatchSize {
			bestBatchWaitSize++
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
	var (
		cli    *batchCommandsClient
		target string
	)
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
		metrics.TiKVNoAvailableConnectionCounter.Inc()

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
}

func (c *batchCommandsClient) initBatchClient() error {
	if c.client != nil {
		return nil
	}

	if err := c.waitConnReady(); err != nil {
		return err
	}

	// Initialize batch streaming clients.
	tikvClient := tikvpb.NewTikvClient(c.conn)
	streamClient, err := tikvClient.BatchCommands(context.TODO())
	if err != nil {
		return errors.Trace(err)
	}
	c.client = streamClient
	go c.batchRecvLoop(c.tikvClientCfg, c.tikvLoad)
	return nil
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
	validRequests := requests[:0]
	for _, e := range entries {
		if !e.isCanceled() {
			validEntries = append(validEntries, e)
			validRequests = append(validRequests, e.req)
		}
	}
	return validEntries, validRequests
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
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case batchConn.batchCommandsCh <- entry:
	case <-ctx.Done():
		logutil.BgLogger().Warn("send request is cancelled",
			zap.String("to", addr), zap.String("cause", ctx.Err().Error()))
		return nil, errors.Trace(ctx.Err())
	case <-timer.C:
		return nil, errors.SuspendStack(errors.Annotate(context.DeadlineExceeded, "wait sendLoop"))
	}

	select {
	case res, ok := <-entry.res:
		if !ok {
			return nil, errors.Trace(entry.err)
		}
		return tikvrpc.FromBatchCommandsResponse(res)
	case <-ctx.Done():
		atomic.StoreInt32(&entry.canceled, 1)
		logutil.BgLogger().Warn("wait response is cancelled",
			zap.String("to", addr), zap.String("cause", ctx.Err().Error()))
		return nil, errors.Trace(ctx.Err())
	case <-timer.C:
		return nil, errors.SuspendStack(errors.Annotate(context.DeadlineExceeded, "wait recvLoop"))
	}
}

func (c *rpcClient) recycleIdleConnArray() {
	var addrs []string
	c.RLock()
	for _, conn := range c.conns {
		if conn.batchConn != nil && conn.isIdle() {
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
