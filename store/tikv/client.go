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
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/terror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
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
	batchCommandsCh      chan *batchCommandsEntry
	batchStatistics      batchStatistics
	batchCommandsClients []*batchCommandsClient
}

// Some internal flags used in batching.
type batchStatistics struct {
	// 10 slots per second.
	reqCountSlots [10]int
	reqTsSlots    [10]int64
	curSlot       int
}

func newBatchStatus() batchStatistics {
	return batchStatistics{
		reqCountSlots: [10]int{0},
		reqTsSlots:    [10]int64{0},
		curSlot:       0,
	}
}

// Update the batchStatistics with a latest batch. batchStatistics holds 10 slots
// for last 10 0.1s. When update is called, if the current slot is not passed, add
// the latest batch size in place. Otherwise clean stale slots first and then add
// the latest batch size in a new slot.
func (b *batchStatistics) update(batch int) {
	// normalize it with unit 0.1s.
	now := time.Now().UnixNano() / 100000000
	if b.reqTsSlots[b.curSlot] != now {
		// Clean all stale counts.
		for i := range b.reqCountSlots {
			if b.reqTsSlots[i] > 0 && b.reqTsSlots[i] <= int64(now-10) {
				// The slot lives more than 1s, clean it.
				b.reqCountSlots[i] = 0
				b.reqTsSlots[i] = 0
			}
		}
		b.curSlot = int(now % 10)
		b.reqTsSlots[b.curSlot] = now
		b.reqCountSlots[b.curSlot] = 0
	}
	b.reqCountSlots[b.curSlot] += batch
}

func (b *batchStatistics) inHeavyLoad(heavyLoad uint) bool {
	reqSpeed := 0
	for _, count := range b.reqCountSlots {
		reqSpeed += count
	}
	return uint(reqSpeed) >= heavyLoad
}

type batchCommandsClient struct {
	client          tikvpb.Tikv_BatchCommandsClient
	batched         sync.Map
	idAlloc         uint64
	tikvInHeavyLoad int32
}

func (c *batchCommandsClient) batchRecvLoop() {
	for {
		resp, err := c.client.Recv()
		if err != nil {
			log.Errorf("batchRecvLoop error when receive: %v", err)
			c.batched.Range(func(key, value interface{}) bool {
				id, _ := key.(uint64)
				entry, _ := value.(*batchCommandsEntry)
				entry.err = err
				close(entry.res)
				c.batched.Delete(id)
				return true
			})
			continue
		}

		responses := resp.GetResponses()
		for i, requestID := range resp.GetRequestIds() {
			value, _ := c.batched.Load(requestID)
			entry, _ := value.(*batchCommandsEntry)
			entry.res <- responses[i]
			c.batched.Delete(requestID)
		}

		if resp.GetInHeavyLoad() {
			atomic.StoreInt32(&c.tikvInHeavyLoad, 1)
		} else {
			atomic.StoreInt32(&c.tikvInHeavyLoad, 0)
		}
	}
}

func newConnArray(maxSize uint, addr string, security config.Security) (*connArray, error) {
	cfg := config.GetGlobalConfig()
	a := &connArray{
		index:         0,
		v:             make([]*grpc.ClientConn, maxSize),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),

		batchCommandsCh:      make(chan *batchCommandsEntry, cfg.TiKVClient.MaxBatchSize),
		batchCommandsClients: make([]*batchCommandsClient, 0, maxSize),
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
				client:  streamClient,
				idAlloc: 0,
			}
			a.batchCommandsClients = append(a.batchCommandsClients, batchClient)
			go batchClient.batchRecvLoop()
		}
	}
	go tikvrpc.CheckStreamTimeoutLoop(a.streamTimeout)
	if allowBatch {
		a.batchStatistics = newBatchStatus()
		go a.batchSendLoop(cfg.TiKVClient)
	}

	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	for i, c := range a.v {
		if c != nil {
			err := c.Close()
			terror.Log(errors.Trace(err))
			a.v[i] = nil
		}
	}
	close(a.streamTimeout)
	close(a.batchCommandsCh)
}

type batchCommandsEntry struct {
	req *tikvpb.BatchCommandsRequest_Request
	res chan *tikvpb.BatchCommandsResponse_Response
	err error
}

func (a *connArray) batchSendLoop(cfg config.TiKVClient) {
	entries := make([]*batchCommandsEntry, 0, cfg.MaxBatchSize)
	requests := make([]*tikvpb.BatchCommandsRequest_Request, 0, cfg.MaxBatchSize)
	requestIds := make([]uint64, 0, cfg.MaxBatchSize)
	for {
		// Choose a connection by round-robbin.
		next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
		batchCommandsClient := a.batchCommandsClients[next]

		metrics.TiKVPendingBatchRequests.Set(float64(len(a.batchCommandsCh)))

		entries = entries[:0]
		requests = requests[:0]
		requestIds = requestIds[:0]

		// Block on the first element.
		headEntry := <-a.batchCommandsCh
		entries = append(entries, headEntry)
		requests = append(requests, headEntry.req)

		inHeavyLoad := atomic.LoadInt32(&batchCommandsClient.tikvInHeavyLoad) == 1
	Loop:
		for {
			select {
			case entry := <-a.batchCommandsCh:
				entries = append(entries, entry)
				requests = append(requests, entry.req)
				if len(requests) >= int(cfg.MaxBatchSize) {
					break Loop
				}
			default:
				inHeavyLoad = inHeavyLoad || a.batchStatistics.inHeavyLoad(cfg.HeavyLoadToBatch)
				break Loop
			}
		}

		if len(requests) < int(cfg.MaxBatchSize) && inHeavyLoad {
			metrics.TiKVBatchWaitTimes.Inc()
			end := time.After(cfg.BatchWaitTime)
		BackoffLoop:
			for {
				select {
				case entry := <-a.batchCommandsCh:
					entries = append(entries, entry)
					requests = append(requests, entry.req)
					if len(requests) >= int(cfg.BatchWaitSize) {
						break BackoffLoop
					}
				case <-end:
					break BackoffLoop
				}
			}
		}

		a.batchStatistics.update(len(requests))

		length := len(requests)
		maxBatchID := atomic.AddUint64(&batchCommandsClient.idAlloc, uint64(length))
		for i := 0; i < length; i++ {
			requestID := uint64(i) + maxBatchID - uint64(length)
			batchCommandsClient.batched.Store(requestID, entries[i])
			requestIds = append(requestIds, requestID)
		}

		request := &tikvpb.BatchCommandsRequest{
			Requests:   requests,
			RequestIds: requestIds,
		}
		if err := batchCommandsClient.client.Send(request); err != nil {
			log.Errorf("batch commands send error: %v", err)
			return
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
				req: batchCommandsReq,
				res: make(chan *tikvpb.BatchCommandsResponse_Response, 1),
				err: nil,
			}
			connArray.batchCommandsCh <- entry

			res, ok := <-entry.res
			if !ok {
				return nil, errors.Trace(entry.err)
			}
			return tikvrpc.FromBatchCommandsResponse(res), nil
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
