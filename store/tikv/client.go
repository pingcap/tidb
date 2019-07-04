// Copyright 2016 PingCAP, Inc.
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
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// MaxRecvMsgSize set max gRPC receive message size received from server. If any message size is larger than
// current value, an error will be reported from gRPC.
var MaxRecvMsgSize = math.MaxInt64

// Timeout durations.
const (
	dialTimeout               = 5 * time.Second
	readTimeoutShort          = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium         = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong           = 150 * time.Second // For requests that may need scan region multiple times.
	GCTimeout                 = 5 * time.Minute
	UnsafeDestroyRangeTimeout = 5 * time.Minute

	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
}

type connArray struct {
	// The target host.
	target string

	index uint32
	v     []*grpc.ClientConn
	// streamTimeout binds with a background goroutine to process coprocessor streaming timeout.
	streamTimeout chan *tikvrpc.Lease

	// batchCommandsCh used for batch commands.
	batchCommandsCh        chan *batchCommandsEntry
	batchCommandsClients   []*batchCommandsClient
	tikvTransportLayerLoad uint64

	// Notify rpcClient to check the idle flag
	idleNotify *uint32
	idle       bool
	idleDetect *time.Timer

	pendingRequests prometheus.Gauge
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
		close(entry.res)
		c.batched.Delete(id)
		return true
	})
}

func (c *batchCommandsClient) reCreateStreamingClient(err error) bool {
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
		return true
	}
	logutil.BgLogger().Error(
		"batchRecvLoop re-create streaming fail",
		zap.String("target", c.target),
		zap.Error(err),
	)
	return false
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
			now := time.Now()
			for { // try to re-create the streaming in the loop.
				if c.isStopped() {
					return
				}
				logutil.BgLogger().Error(
					"batchRecvLoop error when receive",
					zap.String("target", c.target),
					zap.Error(err),
				)

				if c.reCreateStreamingClient(err) {
					break
				}

				// TODO: Use a more smart backoff strategy.
				time.Sleep(time.Second)
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

func newConnArray(maxSize uint, addr string, security config.Security, idleNotify *uint32) (*connArray, error) {
	cfg := config.GetGlobalConfig()

	a := &connArray{
		index:         0,
		v:             make([]*grpc.ClientConn, maxSize),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),

		batchCommandsCh:        make(chan *batchCommandsEntry, cfg.TiKVClient.MaxBatchSize),
		batchCommandsClients:   make([]*batchCommandsClient, 0, maxSize),
		tikvTransportLayerLoad: 0,

		idleNotify: idleNotify,
		idleDetect: time.NewTimer(idleTimeout),
	}
	if err := a.Init(addr, security); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connArray) Init(addr string, security config.Security) error {
	a.target = addr
	a.pendingRequests = metrics.TiKVPendingBatchRequests.WithLabelValues(a.target)

	opt := grpc.WithInsecure()
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	cfg := config.GetGlobalConfig()
	var (
		unaryInterceptor  grpc.UnaryClientInterceptor
		streamInterceptor grpc.StreamClientInterceptor
	)
	if cfg.OpenTracing.Enable {
		unaryInterceptor = grpc_opentracing.UnaryClientInterceptor()
		streamInterceptor = grpc_opentracing.StreamClientInterceptor()
	}

	allowBatch := cfg.TiKVClient.MaxBatchSize > 0
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	keepAliveTimeout := cfg.TiKVClient.GrpcKeepAliveTimeout
	for i := range a.v {
		ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		conn, err := grpc.DialContext(
			ctx,
			addr,
			opt,
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(unaryInterceptor),
			grpc.WithStreamInterceptor(streamInterceptor),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxRecvMsgSize)),
			grpc.WithBackoffMaxDelay(time.Second*3),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Duration(keepAlive) * time.Second,
				Timeout:             time.Duration(keepAliveTimeout) * time.Second,
				PermitWithoutStream: true,
			}),
		)
		cancel()
		if err != nil {
			// Cleanup if the initialization fails.
			a.Close()
			return errors.Trace(err)
		}
		a.v[i] = conn

		if allowBatch {
			// Initialize batch streaming clients.
			tikvClient := tikvpb.NewTikvClient(conn)
			streamClient, err := tikvClient.BatchCommands(context.TODO())
			if err != nil {
				a.Close()
				return errors.Trace(err)
			}
			batchClient := &batchCommandsClient{
				target:                 a.target,
				conn:                   conn,
				client:                 streamClient,
				batched:                sync.Map{},
				idAlloc:                0,
				tikvTransportLayerLoad: &a.tikvTransportLayerLoad,
				closed:                 0,
			}
			a.batchCommandsClients = append(a.batchCommandsClients, batchClient)
			go batchClient.batchRecvLoop(cfg.TiKVClient)
		}
	}
	go tikvrpc.CheckStreamTimeoutLoop(a.streamTimeout)
	if allowBatch {
		go a.batchSendLoop(cfg.TiKVClient)
	}

	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	// Close all batchRecvLoop.
	for _, c := range a.batchCommandsClients {
		// After connections are closed, `batchRecvLoop`s will check the flag.
		atomic.StoreInt32(&c.closed, 1)
	}
	close(a.batchCommandsCh)

	for i, c := range a.v {
		if c != nil {
			err := c.Close()
			terror.Log(errors.Trace(err))
			a.v[i] = nil
		}
	}
	close(a.streamTimeout)
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

// fetchAllPendingRequests fetches all pending requests from the channel.
func (a *connArray) fetchAllPendingRequests(
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
		// This connArray to be recycled
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

func (a *connArray) batchSendLoop(cfg config.TiKVClient) {
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
		next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
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

// rpcClient is RPC client struct.
// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
type rpcClient struct {
	sync.RWMutex
	isClosed bool
	conns    map[string]*connArray
	security config.Security

	// Implement background cleanup.
	// Periodically check whether there is any connection that is idle and then close and remove these idle connections.
	idleNotify uint32
}

func newRPCClient(security config.Security) *rpcClient {
	return &rpcClient{
		conns:    make(map[string]*connArray),
		security: security,
	}
}

// NewTestRPCClient is for some external tests.
func NewTestRPCClient() Client {
	return newRPCClient(config.Security{})
}

func (c *rpcClient) getConnArray(addr string) (*connArray, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	array, ok := c.conns[addr]
	c.RUnlock()
	if !ok {
		var err error
		array, err = c.createConnArray(addr)
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}

func (c *rpcClient) createConnArray(addr string) (*connArray, error) {
	c.Lock()
	defer c.Unlock()
	array, ok := c.conns[addr]
	if !ok {
		var err error
		connCount := config.GetGlobalConfig().TiKVClient.GrpcConnectionCount
		array, err = newConnArray(connCount, addr, c.security, &c.idleNotify)
		if err != nil {
			return nil, err
		}
		c.conns[addr] = array
	}
	return array, nil
}

func (c *rpcClient) closeConns() {
	c.Lock()
	if !c.isClosed {
		c.isClosed = true
		// close all connections
		for _, array := range c.conns {
			array.Close()
		}
	}
	c.Unlock()
}

func sendBatchRequest(
	ctx context.Context,
	addr string,
	connArray *connArray,
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
	case connArray.batchCommandsCh <- entry:
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

// SendRequest sends a Request to server and receives Response.
func (c *rpcClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	start := time.Now()
	reqType := req.Type.String()
	storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
	defer func() {
		metrics.TiKVSendReqHistogram.WithLabelValues(reqType, storeID).Observe(time.Since(start).Seconds())
	}()

	if atomic.CompareAndSwapUint32(&c.idleNotify, 1, 0) {
		c.recycleIdleConnArray()
	}

	connArray, err := c.getConnArray(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if config.GetGlobalConfig().TiKVClient.MaxBatchSize > 0 {
		if batchReq := req.ToBatchCommandsRequest(); batchReq != nil {
			return sendBatchRequest(ctx, addr, connArray, batchReq, timeout)
		}
	}

	if req.IsDebugReq() {
		client := debugpb.NewDebugClient(connArray.Get())
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return tikvrpc.CallDebugRPC(ctx1, client, req)
	}

	client := tikvpb.NewTikvClient(connArray.Get())

	if req.Type != tikvrpc.CmdCopStream {
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return tikvrpc.CallRPC(ctx1, client, req)
	}

	// Coprocessor streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	copStream := resp.CopStream
	copStream.Timeout = timeout
	copStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &copStream.Lease

	// Read the first streaming response to get CopStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *coprocessor.Response
	first, err = copStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Debug("copstream returns nothing for the request.")
	}
	copStream.Response = first
	return resp, nil
}

func (c *rpcClient) Close() error {
	c.closeConns()
	return nil
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
