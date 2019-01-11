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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	gstatus "google.golang.org/grpc/status"
)

// MaxConnectionCount is the max gRPC connections that will be established with
// each tikv-server.
var MaxConnectionCount uint = 16

// GrpcKeepAliveTime is the duration of time after which if the client doesn't see
// any activity it pings the server to see if the transport is still alive.
var GrpcKeepAliveTime = time.Duration(10) * time.Second

// GrpcKeepAliveTimeout is the duration of time for which the client waits after having
// pinged for keepalive check and if no activity is seen even after that the connection
// is closed.
var GrpcKeepAliveTimeout = time.Duration(3) * time.Second

// MaxSendMsgSize set max gRPC request message size sent to server. If any request message size is larger than
// current value, an error will be reported from gRPC.
var MaxSendMsgSize = 1<<31 - 1

// MaxCallMsgSize set max gRPC receive message size received from server. If any message size is larger than
// current value, an error will be reported from gRPC.
var MaxCallMsgSize = 1<<31 - 1

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
	index uint32
	v     []*grpc.ClientConn
	// Bind with a background goroutine to process coprocessor streaming timeout.
	streamTimeout chan *tikvrpc.Lease

	// For batch commands.
	batchCommandsCh        chan *batchCommandsEntry
	batchCommandsClients   []*batchCommandsClient
	tikvTransportLayerLoad uint64
}

type batchCommandsClient struct {
	conn                   *grpc.ClientConn
	client                 tikvpb.Tikv_BatchCommandsClient
	clientLock             sync.Mutex // Protect client when re-create the streaming.
	batched                sync.Map
	idAlloc                uint64
	tikvTransportLayerLoad *uint64
	closed                 chan int
}

func (c *batchCommandsClient) stop() {
	c.closed <- 0
}

func (c *batchCommandsClient) stopped() bool {
	select {
	case <-c.closed:
		return true
	default:
	}
	return false
}

func (c *batchCommandsClient) failPendingRequests(err error) {
	c.batched.Range(func(key, value interface{}) bool {
		id, _ := key.(uint64)
		entry, _ := value.(*batchCommandsEntry)
		entry.err = err
		close(entry.res)
		c.batched.Delete(id)
		return true
	})
}

func (c *batchCommandsClient) batchRecvLoop() {
	for {
		resp, err := c.client.Recv()
		if err != nil {
			if c.stopped() {
				return
			}

			log.Errorf("batchRecvLoop error when receive: %v", err)

			c.clientLock.Lock()
			c.failPendingRequests(err) // fail all pending requests.
			for {                      // try to re-create the streaming in the loop.
				tikvClient := tikvpb.NewTikvClient(c.conn)
				streamClient, err := tikvClient.BatchCommands(context.TODO())
				if err == nil {
					log.Infof("batchRecvLoop re-create streaming success")
					c.client = streamClient
					break
				}
				log.Errorf("batchRecvLoop re-create streaming fail: %v", err)
				time.Sleep(time.Second)
			}
			c.clientLock.Unlock()
			continue
		}

		responses := resp.GetResponses()
		for i, requestID := range resp.GetRequestIds() {
			value, _ := c.batched.Load(requestID)
			entry, _ := value.(*batchCommandsEntry)
			if atomic.LoadInt32(&entry.timeout) == 0 {
				entry.res <- responses[i]
			}
			c.batched.Delete(requestID)
		}

		tikvTransportLayerLoad := resp.GetTransportLayerLoad()
		if tikvTransportLayerLoad > 0.0 {
			atomic.StoreUint64(c.tikvTransportLayerLoad, tikvTransportLayerLoad)
		}
	}
}

func newConnArray(maxSize uint, addr string, security config.Security) (*connArray, error) {
	cfg := config.GetGlobalConfig()
	a := &connArray{
		index:         0,
		v:             make([]*grpc.ClientConn, maxSize),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),

		batchCommandsCh:        make(chan *batchCommandsEntry, cfg.TiKVClient.MaxBatchSize),
		batchCommandsClients:   make([]*batchCommandsClient, 0, maxSize),
		tikvTransportLayerLoad: 0,
	}
	if err := a.Init(addr, security); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connArray) Init(addr string, security config.Security) error {
	opt := grpc.WithInsecure()
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	unaryInterceptor := grpc_prometheus.UnaryClientInterceptor
	streamInterceptor := grpc_prometheus.StreamClientInterceptor
	cfg := config.GetGlobalConfig()
	if cfg.OpenTracing.Enable {
		unaryInterceptor = grpc_middleware.ChainUnaryClient(
			unaryInterceptor,
			grpc_opentracing.UnaryClientInterceptor(),
		)
		streamInterceptor = grpc_middleware.ChainStreamClient(
			streamInterceptor,
			grpc_opentracing.StreamClientInterceptor(),
		)
	}

	allowBatch := cfg.TiKVClient.MaxBatchSize > 0
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
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxCallMsgSize)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(MaxSendMsgSize)),
			grpc.WithBackoffMaxDelay(time.Second*3),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                GrpcKeepAliveTime,
				Timeout:             GrpcKeepAliveTimeout,
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
				conn:                   conn,
				client:                 streamClient,
				batched:                sync.Map{},
				idAlloc:                0,
				tikvTransportLayerLoad: &a.tikvTransportLayerLoad,
				closed:                 make(chan int, 1),
			}
			a.batchCommandsClients = append(a.batchCommandsClients, batchClient)
			go batchClient.batchRecvLoop()
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
		c.stop()
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
	req     *tikvpb.BatchCommandsRequest_Request
	res     chan *tikvpb.BatchCommandsResponse_Response
	timeout int32 // Indicates the request is timeout or not.
	err     error
}

func (a *connArray) batchSendLoop(cfg config.TiKVClient) {
	entries := make([]*batchCommandsEntry, 0, cfg.MaxBatchSize)
	requests := make([]*tikvpb.BatchCommandsRequest_Request, 0, cfg.MaxBatchSize)
	requestIDs := make([]uint64, 0, cfg.MaxBatchSize)

	for {
		metrics.TiKVPendingBatchRequests.Set(float64(len(a.batchCommandsCh)))

		// Choose a connection by round-robbin.
		next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
		batchCommandsClient := a.batchCommandsClients[next]

		tikvTransportLayerLoad := atomic.LoadUint64(batchCommandsClient.tikvTransportLayerLoad)
		inHeavyLoad := uint(tikvTransportLayerLoad) >= cfg.TiKVHeavyLoadToBatch // Need to wait.

		entries = entries[:0]
		requests = requests[:0]
		requestIDs = requestIDs[:0]

		// Block on the first element.
		headEntry := <-a.batchCommandsCh
		if headEntry == nil {
			return
		}
		entries = append(entries, headEntry)

		// This loop is for trying best to collect more requests.
	Loop:
		for {
			select {
			case entry := <-a.batchCommandsCh:
				if entry == nil {
					return
				}
				entries = append(entries, entry)
				if len(entries) >= int(cfg.MaxBatchSize) {
					break Loop
				}
			default:
				break Loop
			}
		}

		// If the target TiKV is overload, wait a while to collect more requests.
		if len(entries) < int(cfg.MaxBatchSize) && inHeavyLoad && cfg.MaxBatchWaitTime > 0 {
			waitStart := time.Now()
			var waitEnd time.Time
			after := time.After(cfg.MaxBatchWaitTime)
			batchIsFilled := false
		WaitLoop:
			for {
				if !batchIsFilled {
					select {
					case entry := <-a.batchCommandsCh:
						if entry == nil {
							return
						}
						entries = append(entries, entry)
						if len(entries) >= int(cfg.BatchWaitSize) {
							batchIsFilled = true
						}
					case now := <-after:
						waitEnd = now
						break WaitLoop
					}
				} else {
					select {
					case entry := <-a.batchCommandsCh:
						if entry == nil {
							return
						}
						entries = append(entries, entry)
						if len(entries) >= int(cfg.MaxBatchSize) {
							waitEnd = time.Now()
							break WaitLoop
						}
					default:
						waitEnd = time.Now()
						break WaitLoop
					}
				}
			}
			metrics.TiKVBatchWaitDuration.Observe(float64(waitEnd.Sub(waitStart)))
		}

		for _, entry := range entries {
			requests = append(requests, entry.req)
		}

		length := len(requests)
		maxBatchID := atomic.AddUint64(&batchCommandsClient.idAlloc, uint64(length))
		for i := 0; i < length; i++ {
			requestID := uint64(i) + maxBatchID - uint64(length)
			requestIDs = append(requestIDs, requestID)
			batchCommandsClient.batched.Store(requestID, entries[i])
		}

		request := &tikvpb.BatchCommandsRequest{
			Requests:   requests,
			RequestIds: requestIDs,
		}

		batchCommandsClient.clientLock.Lock()
		err := batchCommandsClient.client.Send(request)
		batchCommandsClient.clientLock.Unlock()
		if err != nil {
			log.Errorf("batch commands send error: %v", err)
			batchCommandsClient.failPendingRequests(err)
			continue
		}
	}
}

// rpcClient is RPC client struct.
// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
// TODO: Implement background cleanup. It adds a background goroutine to periodically check
// whether there is any connection is idle and then close and remove these idle connections.
type rpcClient struct {
	sync.RWMutex
	isClosed bool
	conns    map[string]*connArray
	security config.Security
}

func newRPCClient(security config.Security) *rpcClient {
	return &rpcClient{
		conns:    make(map[string]*connArray),
		security: security,
	}
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
		array, err = newConnArray(MaxConnectionCount, addr, c.security)
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

// SendRequest sends a Request to server and receives Response.
func (c *rpcClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	start := time.Now()
	reqType := req.Type.String()
	storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
	defer func() {
		metrics.TiKVSendReqHistogram.WithLabelValues(reqType, storeID).Observe(time.Since(start).Seconds())
	}()

	connArray, err := c.getConnArray(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if config.GetGlobalConfig().TiKVClient.MaxBatchSize > 0 {
		if batchCommandsReq := req.ToBatchCommandsRequest(); batchCommandsReq != nil {
			entry := &batchCommandsEntry{
				req:     batchCommandsReq,
				res:     make(chan *tikvpb.BatchCommandsResponse_Response, 1),
				timeout: 0,
				err:     nil,
			}
			connArray.batchCommandsCh <- entry
			ctx1, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			select {
			case res, ok := <-entry.res:
				if !ok {
					return nil, errors.Trace(entry.err)
				}
				return tikvrpc.FromBatchCommandsResponse(res), nil
			case <-ctx1.Done():
				atomic.StoreInt32(&entry.timeout, 1)
				log.Warnf("SendRequest to %s is canceled", addr)
				return nil, errors.Trace(gstatus.Error(gcodes.DeadlineExceeded, "Canceled by caller"))
			}
		}
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
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		cancel() // This line stops the silly lint tool from complaining.
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
		log.Debug("copstream returns nothing for the request.")
	}
	copStream.Response = first
	return resp, nil
}

func (c *rpcClient) Close() error {
	c.closeConns()
	return nil
}
