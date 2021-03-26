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
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
)

type batchCommandsEntry struct {
	ctx context.Context
	req *tikvpb.BatchCommandsRequest_Request
	res chan *tikvpb.BatchCommandsResponse_Response
	// forwardedHost is the address of a store which will handle the request.
	// It's different from the address the request sent to.
	forwardedHost string
	// canceled indicated the request is canceled or not.
	canceled int32
	err      error
}

func (b *batchCommandsEntry) isCanceled() bool {
	return atomic.LoadInt32(&b.canceled) == 1
}

func (b *batchCommandsEntry) error(err error) {
	b.err = err
	close(b.res)
}

// batchCommandsBuilder collects a batch of `batchCommandsEntry`s to build
// `BatchCommandsRequest`s.
type batchCommandsBuilder struct {
	// Each BatchCommandsRequest_Request sent to a store has a unique identity to
	// distinguish its response.
	idAlloc    uint64
	entries    []*batchCommandsEntry
	requests   []*tikvpb.BatchCommandsRequest_Request
	requestIDs []uint64
	// In most cases, there isn't any forwardingReq.
	forwardingReqs map[string]*tikvpb.BatchCommandsRequest
}

func (b *batchCommandsBuilder) len() int {
	return len(b.entries)
}

func (b *batchCommandsBuilder) push(entry *batchCommandsEntry) {
	b.entries = append(b.entries, entry)
}

// build builds BatchCommandsRequests and calls collect() for each valid entry.
// The first return value is the request that doesn't need forwarding.
// The second is a map that maps forwarded hosts to requests.
func (b *batchCommandsBuilder) build(
	collect func(id uint64, e *batchCommandsEntry),
) (*tikvpb.BatchCommandsRequest, map[string]*tikvpb.BatchCommandsRequest) {
	for _, e := range b.entries {
		if e.isCanceled() {
			continue
		}
		if collect != nil {
			collect(b.idAlloc, e)
		}
		if e.forwardedHost == "" {
			b.requestIDs = append(b.requestIDs, b.idAlloc)
			b.requests = append(b.requests, e.req)
		} else {
			batchReq, ok := b.forwardingReqs[e.forwardedHost]
			if !ok {
				batchReq = &tikvpb.BatchCommandsRequest{}
				b.forwardingReqs[e.forwardedHost] = batchReq
			}
			batchReq.RequestIds = append(batchReq.RequestIds, b.idAlloc)
			batchReq.Requests = append(batchReq.Requests, e.req)
		}
		b.idAlloc++
	}
	var req *tikvpb.BatchCommandsRequest
	if len(b.requests) > 0 {
		req = &tikvpb.BatchCommandsRequest{
			Requests:   b.requests,
			RequestIds: b.requestIDs,
		}
	}
	return req, b.forwardingReqs
}

func (b *batchCommandsBuilder) cancel(e error) {
	for _, entry := range b.entries {
		entry.error(e)
	}
}

// reset resets the builder to the initial state.
// Should call it before collecting a new batch.
func (b *batchCommandsBuilder) reset() {
	// NOTE: We can't simply set entries = entries[:0] here.
	// The data in the cap part of the slice would reference the prewrite keys whose
	// underlying memory is borrowed from memdb. The reference cause GC can't release
	// the memdb, leading to serious memory leak problems in the large transaction case.
	for i := 0; i < len(b.entries); i++ {
		b.entries[i] = nil
	}
	b.entries = b.entries[:0]
	for i := 0; i < len(b.requests); i++ {
		b.requests[i] = nil
	}
	b.requests = b.requests[:0]
	b.requestIDs = b.requestIDs[:0]

	for k := range b.forwardingReqs {
		delete(b.forwardingReqs, k)
	}
}

func newBatchCommandsBuilder(maxBatchSize uint) *batchCommandsBuilder {
	return &batchCommandsBuilder{
		idAlloc:        0,
		entries:        make([]*batchCommandsEntry, 0, maxBatchSize),
		requests:       make([]*tikvpb.BatchCommandsRequest_Request, 0, maxBatchSize),
		requestIDs:     make([]uint64, 0, maxBatchSize),
		forwardingReqs: make(map[string]*tikvpb.BatchCommandsRequest),
	}
}

type batchConn struct {
	// An atomic flag indicates whether the batch is idle or not.
	// 0 for busy, others for idle.
	idle uint32

	// batchCommandsCh used for batch commands.
	batchCommandsCh        chan *batchCommandsEntry
	batchCommandsClients   []*batchCommandsClient
	tikvTransportLayerLoad uint64
	closed                 chan struct{}

	reqBuilder *batchCommandsBuilder

	// Notify rpcClient to check the idle flag
	idleNotify *uint32
	idleDetect *time.Timer

	pendingRequests prometheus.Observer
	batchSize       prometheus.Observer

	index uint32
}

func newBatchConn(connCount, maxBatchSize uint, idleNotify *uint32) *batchConn {
	return &batchConn{
		batchCommandsCh:        make(chan *batchCommandsEntry, maxBatchSize),
		batchCommandsClients:   make([]*batchCommandsClient, 0, connCount),
		tikvTransportLayerLoad: 0,
		closed:                 make(chan struct{}),
		reqBuilder:             newBatchCommandsBuilder(maxBatchSize),
		idleNotify:             idleNotify,
		idleDetect:             time.NewTimer(idleTimeout),
	}
}

func (a *batchConn) isIdle() bool {
	return atomic.LoadUint32(&a.idle) != 0
}

// fetchAllPendingRequests fetches all pending requests from the channel.
func (a *batchConn) fetchAllPendingRequests(
	maxBatchSize int,
) time.Time {
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
		return time.Now()
	case <-a.closed:
		return time.Now()
	}
	if headEntry == nil {
		return time.Now()
	}
	ts := time.Now()
	a.reqBuilder.push(headEntry)

	// This loop is for trying best to collect more requests.
	for a.reqBuilder.len() < maxBatchSize {
		select {
		case entry := <-a.batchCommandsCh:
			if entry == nil {
				return ts
			}
			a.reqBuilder.push(entry)
		default:
			return ts
		}
	}
	return ts
}

// fetchMorePendingRequests fetches more pending requests from the channel.
func (a *batchConn) fetchMorePendingRequests(
	maxBatchSize int,
	batchWaitSize int,
	maxWaitTime time.Duration,
) {
	// Try to collect `batchWaitSize` requests, or wait `maxWaitTime`.
	after := time.NewTimer(maxWaitTime)
	for a.reqBuilder.len() < batchWaitSize {
		select {
		case entry := <-a.batchCommandsCh:
			if entry == nil {
				return
			}
			a.reqBuilder.push(entry)
		case <-after.C:
			return
		}
	}
	after.Stop()

	// Do an additional non-block try. Here we test the lengh with `maxBatchSize` instead
	// of `batchWaitSize` because trying best to fetch more requests is necessary so that
	// we can adjust the `batchWaitSize` dynamically.
	for a.reqBuilder.len() < maxBatchSize {
		select {
		case entry := <-a.batchCommandsCh:
			if entry == nil {
				return
			}
			a.reqBuilder.push(entry)
		default:
			return
		}
	}
}

const idleTimeout = 3 * time.Minute

func (a *batchConn) batchSendLoop(cfg config.TiKVClient) {
	defer func() {
		if r := recover(); r != nil {
			metrics.TiKVPanicCounter.WithLabelValues(metrics.LabelBatchSendLoop).Inc()
			logutil.BgLogger().Error("batchSendLoop",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			logutil.BgLogger().Info("restart batchSendLoop")
			go a.batchSendLoop(cfg)
		}
	}()

	bestBatchWaitSize := cfg.BatchWaitSize
	for {
		a.reqBuilder.reset()

		start := a.fetchAllPendingRequests(int(cfg.MaxBatchSize))
		a.pendingRequests.Observe(float64(len(a.batchCommandsCh)))
		a.batchSize.Observe(float64(a.reqBuilder.len()))

		// curl -XPUT -d 'return(true)' http://0.0.0.0:10080/fail/github.com/pingcap/tidb/store/tikv/mockBlockOnBatchClient
		failpoint.Inject("mockBlockOnBatchClient", func(val failpoint.Value) {
			if val.(bool) {
				time.Sleep(1 * time.Hour)
			}
		})

		if a.reqBuilder.len() < int(cfg.MaxBatchSize) && cfg.MaxBatchWaitTime > 0 {
			// If the target TiKV is overload, wait a while to collect more requests.
			if atomic.LoadUint64(&a.tikvTransportLayerLoad) >= uint64(cfg.OverloadThreshold) {
				metrics.TiKVBatchWaitOverLoad.Inc()
				a.fetchMorePendingRequests(int(cfg.MaxBatchSize), int(bestBatchWaitSize), cfg.MaxBatchWaitTime)
			}
		}
		length := a.reqBuilder.len()
		if uint(length) == 0 {
			// The batch command channel is closed.
			return
		} else if uint(length) < bestBatchWaitSize && bestBatchWaitSize > 1 {
			// Waits too long to collect requests, reduce the target batch size.
			bestBatchWaitSize--
		} else if uint(length) > bestBatchWaitSize+4 && bestBatchWaitSize < cfg.MaxBatchSize {
			bestBatchWaitSize++
		}

		a.getClientAndSend()
		metrics.TiKVBatchSendLatency.Observe(float64(time.Since(start)))
	}
}

func (a *batchConn) getClientAndSend() {
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

		// Please ensure the error is handled in region cache correctly.
		a.reqBuilder.cancel(errors.New("no available connections"))
		return
	}
	defer cli.unlockForSend()

	req, forwardingReqs := a.reqBuilder.build(func(id uint64, e *batchCommandsEntry) {
		cli.batched.Store(id, e)
		if trace.IsEnabled() {
			trace.Log(e.ctx, "rpc", "send")
		}
	})
	if req != nil {
		cli.send("", req)
	}
	for forwardedHost, req := range forwardingReqs {
		cli.send(forwardedHost, req)
	}
}

type tryLock struct {
	*sync.Cond
	reCreating bool
}

func (l *tryLock) tryLockForSend() bool {
	l.L.Lock()
	if l.reCreating {
		l.L.Unlock()
		return false
	}
	return true
}

func (l *tryLock) unlockForSend() {
	l.L.Unlock()
}

func (l *tryLock) lockForRecreate() {
	l.L.Lock()
	for l.reCreating {
		l.Wait()
	}
	l.reCreating = true
	l.L.Unlock()
}

func (l *tryLock) unlockForRecreate() {
	l.L.Lock()
	l.reCreating = false
	l.Broadcast()
	l.L.Unlock()
}

type batchCommandsStream struct {
	tikvpb.Tikv_BatchCommandsClient
	forwardedHost string
}

func (s *batchCommandsStream) recv() (resp *tikvpb.BatchCommandsResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			metrics.TiKVPanicCounter.WithLabelValues(metrics.LabelBatchRecvLoop).Inc()
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
	resp, err = s.Recv()
	return
}

// recreate creates a new BatchCommands stream. The conn should be ready for work.
func (s *batchCommandsStream) recreate(conn *grpc.ClientConn) error {
	tikvClient := tikvpb.NewTikvClient(conn)
	ctx := context.TODO()
	// Set metadata for forwarding stream.
	if s.forwardedHost != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, forwardMetadataKey, s.forwardedHost)
	}
	streamClient, err := tikvClient.BatchCommands(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	s.Tikv_BatchCommandsClient = streamClient
	return nil
}

type batchCommandsClient struct {
	// The target host.
	target string

	conn *grpc.ClientConn
	// client and forwardedClients are protected by tryLock.
	//
	// client is the stream that needn't forwarding.
	client *batchCommandsStream
	// TiDB uses [gRPC-metadata](https://github.com/grpc/grpc-go/blob/master/Documentation/grpc-metadata.md) to
	// indicate a request needs forwarding. gRPC doesn't support setting a metadata for each request in a stream,
	// so we need to create a stream for each forwarded host.
	//
	// forwardedClients are clients that need forwarding. It's a map that maps forwarded hosts to streams
	forwardedClients map[string]*batchCommandsStream
	batched          sync.Map

	tikvClientCfg config.TiKVClient
	tikvLoad      *uint64
	dialTimeout   time.Duration

	// Increased in each reconnection.
	// It's used to prevent the connection from reconnecting multiple times
	// due to one failure because there may be more than 1 `batchRecvLoop`s.
	epoch uint64
	// closed indicates the batch client is closed explicitly or not.
	closed int32
	// tryLock protects client when re-create the streaming.
	tryLock
}

func (c *batchCommandsClient) isStopped() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

func (c *batchCommandsClient) send(forwardedHost string, req *tikvpb.BatchCommandsRequest) {
	err := c.initBatchClient(forwardedHost)
	if err != nil {
		logutil.BgLogger().Warn(
			"init create streaming fail",
			zap.String("target", c.target),
			zap.String("forwardedHost", forwardedHost),
			zap.Error(err),
		)
		c.failPendingRequests(err)
		return
	}

	client := c.client
	if forwardedHost != "" {
		client = c.forwardedClients[forwardedHost]
	}
	if err := client.Send(req); err != nil {
		logutil.BgLogger().Info(
			"sending batch commands meets error",
			zap.String("target", c.target),
			zap.String("forwardedHost", forwardedHost),
			zap.Uint64s("requestIDs", req.RequestIds),
			zap.Error(err),
		)
		c.failPendingRequests(err)
	}
}

// `failPendingRequests` must be called in locked contexts in order to avoid double closing channels.
func (c *batchCommandsClient) failPendingRequests(err error) {
	failpoint.Inject("panicInFailPendingRequests", nil)
	c.batched.Range(func(key, value interface{}) bool {
		id, _ := key.(uint64)
		entry, _ := value.(*batchCommandsEntry)
		c.batched.Delete(id)
		entry.error(err)
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

func (c *batchCommandsClient) recreateStreamingClientOnce(streamClient *batchCommandsStream) error {
	err := c.waitConnReady()
	// Re-establish a application layer stream. TCP layer is handled by gRPC.
	if err == nil {
		err := streamClient.recreate(c.conn)
		if err == nil {
			logutil.BgLogger().Info(
				"batchRecvLoop re-create streaming success",
				zap.String("target", c.target),
				zap.String("forwardedHost", streamClient.forwardedHost),
			)
			return nil
		}
	}
	logutil.BgLogger().Info(
		"batchRecvLoop re-create streaming fail",
		zap.String("target", c.target),
		zap.String("forwardedHost", streamClient.forwardedHost),
		zap.Error(err),
	)
	return err
}

func (c *batchCommandsClient) batchRecvLoop(cfg config.TiKVClient, tikvTransportLayerLoad *uint64, streamClient *batchCommandsStream) {
	defer func() {
		if r := recover(); r != nil {
			metrics.TiKVPanicCounter.WithLabelValues(metrics.LabelBatchRecvLoop).Inc()
			logutil.BgLogger().Error("batchRecvLoop",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			logutil.BgLogger().Info("restart batchRecvLoop")
			go c.batchRecvLoop(cfg, tikvTransportLayerLoad, streamClient)
		}
	}()

	epoch := atomic.LoadUint64(&c.epoch)
	for {
		resp, err := streamClient.recv()
		if err != nil {
			if c.isStopped() {
				return
			}
			logutil.BgLogger().Info(
				"batchRecvLoop fails when receiving, needs to reconnect",
				zap.String("target", c.target),
				zap.String("forwardedHost", streamClient.forwardedHost),
				zap.Error(err),
			)

			now := time.Now()
			if stopped := c.recreateStreamingClient(err, streamClient, &epoch); stopped {
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
				logutil.BgLogger().Warn("batchRecvLoop receives outdated response", zap.Uint64("requestID", requestID), zap.String("forwardedHost", streamClient.forwardedHost))
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

func (c *batchCommandsClient) recreateStreamingClient(err error, streamClient *batchCommandsStream, epoch *uint64) (stopped bool) {
	// Forbids the batchSendLoop using the old client and
	// blocks other streams trying to recreate.
	c.lockForRecreate()
	defer c.unlockForRecreate()

	// Each batchCommandsStream has a batchRecvLoop. There is only one stream waiting for
	// the connection ready in every epoch to prevent the connection from reconnecting
	// multiple times due to one failure.
	//
	// Check it in the locked scope to prevent the stream which gets the token from
	// reconnecting lately, i.e.
	// goroutine 1       | goroutine 2
	// CAS success       |
	//                   | CAS failure
	//                   | lockForRecreate
	//                   | recreate error
	//                   | unlockForRecreate
	// lockForRecreate   |
	// waitConnReady     |
	// recreate          |
	// unlockForRecreate |
	waitConnReady := atomic.CompareAndSwapUint64(&c.epoch, *epoch, *epoch+1)
	if !waitConnReady {
		*epoch = atomic.LoadUint64(&c.epoch)
		if err := streamClient.recreate(c.conn); err != nil {
			logutil.BgLogger().Info(
				"batchRecvLoop re-create streaming fail",
				zap.String("target", c.target),
				zap.String("forwardedHost", streamClient.forwardedHost),
				zap.Error(err),
			)
		}
		return c.isStopped()
	}
	*epoch++

	c.failPendingRequests(err) // fail all pending requests.
	b := NewBackofferWithVars(context.Background(), math.MaxInt32, nil)
	for { // try to re-create the streaming in the loop.
		if c.isStopped() {
			return true
		}
		err1 := c.recreateStreamingClientOnce(streamClient)
		if err1 == nil {
			break
		}

		err2 := b.Backoff(BoTiKVRPC, err1)
		// As timeout is set to math.MaxUint32, err2 should always be nil.
		// This line is added to make the 'make errcheck' pass.
		terror.Log(err2)
	}
	return false
}

func (c *batchCommandsClient) newBatchStream(forwardedHost string) (*batchCommandsStream, error) {
	batchStream := &batchCommandsStream{forwardedHost: forwardedHost}
	if err := batchStream.recreate(c.conn); err != nil {
		return nil, errors.Trace(err)
	}
	return batchStream, nil
}

func (c *batchCommandsClient) initBatchClient(forwardedHost string) error {
	if forwardedHost == "" && c.client != nil {
		return nil
	}
	if _, ok := c.forwardedClients[forwardedHost]; ok {
		return nil
	}

	if err := c.waitConnReady(); err != nil {
		return err
	}

	streamClient, err := c.newBatchStream(forwardedHost)
	if err != nil {
		return errors.Trace(err)
	}
	if forwardedHost == "" {
		c.client = streamClient
	} else {
		c.forwardedClients[forwardedHost] = streamClient
	}
	go c.batchRecvLoop(c.tikvClientCfg, c.tikvLoad, streamClient)
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

func sendBatchRequest(
	ctx context.Context,
	addr string,
	forwardedHost string,
	batchConn *batchConn,
	req *tikvpb.BatchCommandsRequest_Request,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	entry := &batchCommandsEntry{
		ctx:           ctx,
		req:           req,
		res:           make(chan *tikvpb.BatchCommandsResponse_Response, 1),
		forwardedHost: forwardedHost,
		canceled:      0,
		err:           nil,
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	start := time.Now()
	select {
	case batchConn.batchCommandsCh <- entry:
	case <-ctx.Done():
		logutil.BgLogger().Warn("send request is cancelled",
			zap.String("to", addr), zap.String("cause", ctx.Err().Error()))
		return nil, errors.Trace(ctx.Err())
	case <-timer.C:
		return nil, errors.SuspendStack(errors.Annotate(context.DeadlineExceeded, "wait sendLoop"))
	}
	metrics.TiKVBatchWaitDuration.Observe(float64(time.Since(start)))

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

func (c *RPCClient) recycleIdleConnArray() {
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
