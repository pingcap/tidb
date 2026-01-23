package copr

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const forwardedHostMetadataKey = "tikv-forwarded-host"

type versionedRPCClient struct {
	tlsConfig *tls.Config

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool

	streamTimeout chan *tikvrpc.Lease
	streamDone    chan struct{}
}

func newVersionedRPCClient(tlsConfig *tls.Config) *versionedRPCClient {
	c := &versionedRPCClient{
		tlsConfig: tlsConfig,
		conns:     make(map[string]*grpc.ClientConn, 8),
		// Keep it consistent with client-go connArray.streamTimeout.
		streamTimeout: make(chan *tikvrpc.Lease, 1024),
		streamDone:    make(chan struct{}),
	}
	go tikvrpc.CheckStreamTimeoutLoop(c.streamTimeout, c.streamDone)
	return c
}

func (c *versionedRPCClient) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	conns := c.conns
	c.conns = nil
	close(c.streamDone)
	c.mu.Unlock()

	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (c *versionedRPCClient) CloseAddr(addr string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	conn, ok := c.conns[addr]
	if ok {
		delete(c.conns, addr)
	}
	c.mu.Unlock()

	if ok {
		_ = conn.Close()
	}
}

func (c *versionedRPCClient) getConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("versioned rpc client is closed")
	}
	if conn, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()

	conn, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close()
		return nil, fmt.Errorf("versioned rpc client is closed")
	}
	if old, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		_ = conn.Close()
		return old, nil
	}
	c.conns[addr] = conn
	c.mu.Unlock()
	return conn, nil
}

func (c *versionedRPCClient) dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	cfg := config.GetGlobalConfig()

	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	if c.tlsConfig != nil {
		creds = grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConfig))
	}

	callOpts := []grpc.CallOption{grpc.MaxCallRecvMsgSize(math.MaxInt64 - 1)}
	if cfg.TiKVClient.GrpcCompressionType == gzip.Name {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}

	dialOpts := []grpc.DialOption{
		creds,
		grpc.WithInitialWindowSize(cfg.TiKVClient.GrpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(cfg.TiKVClient.GrpcInitialConnWindowSize),
		grpc.WithDefaultCallOptions(callOpts...),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
			Timeout: cfg.TiKVClient.GetGrpcKeepAliveTimeout(),
		}),
	}

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func isVersionedCoprocessorRequest(req *tikvrpc.Request) bool {
	if req == nil || req.Type != tikvrpc.CmdCop || req.StoreTp != tikvrpc.TiKV {
		return false
	}
	copReq, ok := req.Req.(*coprocessor.Request)
	if !ok || copReq == nil {
		return false
	}
	if len(copReq.RangeVersions) > 0 {
		return true
	}
	for _, t := range copReq.Tasks {
		if len(t.GetRangeVersions()) > 0 {
			return true
		}
	}
	return false
}

func isVersionedBatchCoprocessorRequest(req *tikvrpc.Request) bool {
	if req == nil || req.Type != tikvrpc.CmdBatchCop {
		return false
	}
	batchReq, ok := req.Req.(*coprocessor.BatchRequest)
	if !ok || batchReq == nil {
		return false
	}

	type rangeVersionsGetter interface {
		GetRangeVersions() []uint64
	}
	hasRangeVersions := func(v any) bool {
		rv, ok := v.(rangeVersionsGetter)
		return ok && len(rv.GetRangeVersions()) > 0
	}

	if hasRangeVersions(batchReq) {
		return true
	}
	for _, region := range batchReq.GetRegions() {
		if hasRangeVersions(region) {
			return true
		}
	}
	for _, tableRegion := range batchReq.GetTableRegions() {
		if hasRangeVersions(tableRegion) {
			return true
		}
		for _, region := range tableRegion.GetRegions() {
			if hasRangeVersions(region) {
				return true
			}
		}
	}
	for _, tableShardInfos := range batchReq.GetTableShardInfos() {
		if hasRangeVersions(tableShardInfos) {
			return true
		}
		for _, shardInfo := range tableShardInfos.GetShardInfos() {
			if hasRangeVersions(shardInfo) {
				return true
			}
		}
	}
	return false
}

type versionedBatchRequest struct {
	Request *coprocessor.BatchRequest `protobuf:"bytes,1,opt,name=request,proto3" json:"request,omitempty"`
}

func (m *versionedBatchRequest) Reset()         { *m = versionedBatchRequest{} }
func (m *versionedBatchRequest) String() string { return fmt.Sprintf("%+v", *m) }
func (*versionedBatchRequest) ProtoMessage()    {}
func (m *versionedBatchRequest) GetRequest() *coprocessor.BatchRequest {
	if m != nil {
		return m.Request
	}
	return nil
}

type versionedBatchResponse struct {
	Response *coprocessor.BatchResponse `protobuf:"bytes,1,opt,name=response,proto3" json:"response,omitempty"`
}

func (m *versionedBatchResponse) Reset()         { *m = versionedBatchResponse{} }
func (m *versionedBatchResponse) String() string { return fmt.Sprintf("%+v", *m) }
func (*versionedBatchResponse) ProtoMessage()    {}
func (m *versionedBatchResponse) GetResponse() *coprocessor.BatchResponse {
	if m != nil {
		return m.Response
	}
	return nil
}

type versionedBatchCoprocessorClient struct {
	grpc.ClientStream
}

func (x *versionedBatchCoprocessorClient) Recv() (*coprocessor.BatchResponse, error) {
	m := new(versionedBatchResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m.GetResponse(), nil
}

func (c *versionedRPCClient) registerStreamLease(lease *tikvrpc.Lease) {
	if lease == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	ch := c.streamTimeout
	done := c.streamDone
	c.mu.Unlock()

	select {
	case ch <- lease:
	case <-done:
	}
}

func (c *versionedRPCClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration, codec tikv.Codec) (*tikvrpc.Response, error) {
	encodedReq := req
	useCodec := codec != nil
	decoded := false
	var err error
	if useCodec {
		encodedReq, err = codec.EncodeRequest(req)
		if err != nil {
			return nil, err
		}
		defer func() {
			if !decoded {
				// Return request to codec's pool.
				_, _ = codec.DecodeResponse(encodedReq, &tikvrpc.Response{Resp: &coprocessor.Response{}})
			}
		}()
	}

	// Attach region/store context into the protobuf request.
	tikvrpc.AttachContext(encodedReq, encodedReq.Context)

	copReq := encodedReq.Cop()
	vReq := &coprocessor.VersionedRequest{Request: copReq}

	conn, err := c.getConn(ctx, addr)
	if err != nil {
		return nil, err
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if encodedReq.ForwardedHost != "" {
		callCtx = metadata.AppendToOutgoingContext(callCtx, forwardedHostMetadataKey, encodedReq.ForwardedHost)
	}

	vClient := tikvpb.NewVersionedKvClient(conn)
	vResp, err := vClient.VersionedCoprocessor(callCtx, vReq)
	if err != nil {
		// If gRPC remote cancels the request (e.g. by keepalive), reconnect next time.
		if callCtx.Err() == nil && status.Code(err) == codes.Canceled {
			c.CloseAddr(addr)
		}
		return nil, err
	}

	resp := &tikvrpc.Response{Resp: vResp.GetResponse()}
	if useCodec {
		resp, err = codec.DecodeResponse(encodedReq, resp)
		decoded = true
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (c *versionedRPCClient) SendBatchRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration, codec tikv.Codec) (*tikvrpc.Response, error) {
	encodedReq := req
	useCodec := codec != nil
	decoded := false
	var err error
	if useCodec {
		encodedReq, err = codec.EncodeRequest(req)
		if err != nil {
			return nil, err
		}
		defer func() {
			if !decoded {
				// Return request to codec's pool.
				_, _ = codec.DecodeResponse(encodedReq, &tikvrpc.Response{Resp: &tikvrpc.BatchCopStreamResponse{}})
			}
		}()
	}

	// Attach region/store context into the protobuf request.
	tikvrpc.AttachContext(encodedReq, encodedReq.Context)

	copReq := encodedReq.BatchCop()
	vReq := &versionedBatchRequest{Request: copReq}

	conn, err := c.getConn(ctx, addr)
	if err != nil {
		return nil, err
	}

	callCtx, cancel := context.WithCancel(ctx)
	if encodedReq.ForwardedHost != "" {
		callCtx = metadata.AppendToOutgoingContext(callCtx, forwardedHostMetadataKey, encodedReq.ForwardedHost)
	}

	stream, err := conn.NewStream(callCtx, &grpc.StreamDesc{StreamName: "VersionedBatchCoprocessor", ServerStreams: true}, "/tikvpb.VersionedKv/VersionedBatchCoprocessor")
	if err != nil {
		cancel()
		return nil, err
	}
	streamClient := &versionedBatchCoprocessorClient{ClientStream: stream}
	if err := streamClient.ClientStream.SendMsg(vReq); err != nil {
		cancel()
		return nil, err
	}
	if err := streamClient.ClientStream.CloseSend(); err != nil {
		cancel()
		return nil, err
	}

	resp := &tikvrpc.Response{Resp: &tikvrpc.BatchCopStreamResponse{Tikv_BatchCoprocessorClient: streamClient}}
	copStream := resp.Resp.(*tikvrpc.BatchCopStreamResponse)
	copStream.Timeout = timeout
	copStream.Lease.Cancel = cancel
	c.registerStreamLease(&copStream.Lease)

	var first *coprocessor.BatchResponse
	first, err = copStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			cancel()
			return nil, errors.WithStack(err)
		}
	}
	copStream.BatchResponse = first

	if useCodec {
		resp, err = codec.DecodeResponse(encodedReq, resp)
		decoded = true
		if err != nil {
			cancel()
			return nil, err
		}
	}
	return resp, nil
}
