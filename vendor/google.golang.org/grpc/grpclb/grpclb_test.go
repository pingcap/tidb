/*
 *
 * Copyright 2016 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

//go:generate protoc --go_out=plugins=:$GOPATH/src grpc_lb_v1/messages/messages.proto
//go:generate protoc --go_out=plugins=grpc:$GOPATH/src grpc_lb_v1/service/service.proto

// Package grpclb_test is currently used only for grpclb testing.
package grpclb_test

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	lbmpb "google.golang.org/grpc/grpclb/grpc_lb_v1/messages"
	lbspb "google.golang.org/grpc/grpclb/grpc_lb_v1/service"
	_ "google.golang.org/grpc/grpclog/glogger"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	testpb "google.golang.org/grpc/test/grpc_testing"
	"google.golang.org/grpc/test/leakcheck"
)

var (
	lbServerName = "bar.com"
	beServerName = "foo.com"
	lbToken      = "iamatoken"

	// Resolver replaces localhost with fakeName in Next().
	// Dialer replaces fakeName with localhost when dialing.
	// This will test that custom dialer is passed from Dial to grpclb.
	fakeName = "fake.Name"
)

type serverNameCheckCreds struct {
	mu       sync.Mutex
	sn       string
	expected string
}

func (c *serverNameCheckCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if _, err := io.WriteString(rawConn, c.sn); err != nil {
		fmt.Printf("Failed to write the server name %s to the client %v", c.sn, err)
		return nil, nil, err
	}
	return rawConn, nil, nil
}
func (c *serverNameCheckCreds) ClientHandshake(ctx context.Context, addr string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := make([]byte, len(c.expected))
	errCh := make(chan error, 1)
	go func() {
		_, err := rawConn.Read(b)
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err != nil {
			fmt.Printf("Failed to read the server name from the server %v", err)
			return nil, nil, err
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	if c.expected != string(b) {
		fmt.Printf("Read the server name %s want %s", string(b), c.expected)
		return nil, nil, errors.New("received unexpected server name")
	}
	return rawConn, nil, nil
}
func (c *serverNameCheckCreds) Info() credentials.ProtocolInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return credentials.ProtocolInfo{}
}
func (c *serverNameCheckCreds) Clone() credentials.TransportCredentials {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &serverNameCheckCreds{
		expected: c.expected,
	}
}
func (c *serverNameCheckCreds) OverrideServerName(s string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expected = s
	return nil
}

// fakeNameDialer replaces fakeName with localhost when dialing.
// This will test that custom dialer is passed from Dial to grpclb.
func fakeNameDialer(addr string, timeout time.Duration) (net.Conn, error) {
	addr = strings.Replace(addr, fakeName, "localhost", 1)
	return net.DialTimeout("tcp", addr, timeout)
}

type remoteBalancer struct {
	sls       chan *lbmpb.ServerList
	statsDura time.Duration
	done      chan struct{}
	mu        sync.Mutex
	stats     lbmpb.ClientStats
}

func newRemoteBalancer(intervals []time.Duration) *remoteBalancer {
	return &remoteBalancer{
		sls:  make(chan *lbmpb.ServerList, 1),
		done: make(chan struct{}),
	}
}

func (b *remoteBalancer) stop() {
	close(b.sls)
	close(b.done)
}

func (b *remoteBalancer) BalanceLoad(stream lbspb.LoadBalancer_BalanceLoadServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	initReq := req.GetInitialRequest()
	if initReq.Name != beServerName {
		return status.Errorf(codes.InvalidArgument, "invalid service name: %v", initReq.Name)
	}
	resp := &lbmpb.LoadBalanceResponse{
		LoadBalanceResponseType: &lbmpb.LoadBalanceResponse_InitialResponse{
			InitialResponse: &lbmpb.InitialLoadBalanceResponse{
				ClientStatsReportInterval: &lbmpb.Duration{
					Seconds: int64(b.statsDura.Seconds()),
					Nanos:   int32(b.statsDura.Nanoseconds() - int64(b.statsDura.Seconds())*1e9),
				},
			},
		},
	}
	if err := stream.Send(resp); err != nil {
		return err
	}
	go func() {
		for {
			var (
				req *lbmpb.LoadBalanceRequest
				err error
			)
			if req, err = stream.Recv(); err != nil {
				return
			}
			b.mu.Lock()
			b.stats.NumCallsStarted += req.GetClientStats().NumCallsStarted
			b.stats.NumCallsFinished += req.GetClientStats().NumCallsFinished
			b.stats.NumCallsFinishedWithDropForRateLimiting += req.GetClientStats().NumCallsFinishedWithDropForRateLimiting
			b.stats.NumCallsFinishedWithDropForLoadBalancing += req.GetClientStats().NumCallsFinishedWithDropForLoadBalancing
			b.stats.NumCallsFinishedWithClientFailedToSend += req.GetClientStats().NumCallsFinishedWithClientFailedToSend
			b.stats.NumCallsFinishedKnownReceived += req.GetClientStats().NumCallsFinishedKnownReceived
			b.mu.Unlock()
		}
	}()
	for v := range b.sls {
		resp = &lbmpb.LoadBalanceResponse{
			LoadBalanceResponseType: &lbmpb.LoadBalanceResponse_ServerList{
				ServerList: v,
			},
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	<-b.done
	return nil
}

type testServer struct {
	testpb.TestServiceServer

	addr     string
	fallback bool
}

const testmdkey = "testmd"

func (s *testServer) EmptyCall(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "failed to receive metadata")
	}
	if !s.fallback && (md == nil || md["lb-token"][0] != lbToken) {
		return nil, status.Errorf(codes.Internal, "received unexpected metadata: %v", md)
	}
	grpc.SetTrailer(ctx, metadata.Pairs(testmdkey, s.addr))
	return &testpb.Empty{}, nil
}

func (s *testServer) FullDuplexCall(stream testpb.TestService_FullDuplexCallServer) error {
	return nil
}

func startBackends(sn string, fallback bool, lis ...net.Listener) (servers []*grpc.Server) {
	for _, l := range lis {
		creds := &serverNameCheckCreds{
			sn: sn,
		}
		s := grpc.NewServer(grpc.Creds(creds))
		testpb.RegisterTestServiceServer(s, &testServer{addr: l.Addr().String(), fallback: fallback})
		servers = append(servers, s)
		go func(s *grpc.Server, l net.Listener) {
			s.Serve(l)
		}(s, l)
	}
	return
}

func stopBackends(servers []*grpc.Server) {
	for _, s := range servers {
		s.Stop()
	}
}

type testServers struct {
	lbAddr  string
	ls      *remoteBalancer
	lb      *grpc.Server
	beIPs   []net.IP
	bePorts []int
}

func newLoadBalancer(numberOfBackends int) (tss *testServers, cleanup func(), err error) {
	var (
		beListeners []net.Listener
		ls          *remoteBalancer
		lb          *grpc.Server
		beIPs       []net.IP
		bePorts     []int
	)
	for i := 0; i < numberOfBackends; i++ {
		// Start a backend.
		beLis, e := net.Listen("tcp", "localhost:0")
		if e != nil {
			err = fmt.Errorf("Failed to listen %v", err)
			return
		}
		beIPs = append(beIPs, beLis.Addr().(*net.TCPAddr).IP)
		bePorts = append(bePorts, beLis.Addr().(*net.TCPAddr).Port)

		beListeners = append(beListeners, beLis)
	}
	backends := startBackends(beServerName, false, beListeners...)

	// Start a load balancer.
	lbLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		err = fmt.Errorf("Failed to create the listener for the load balancer %v", err)
		return
	}
	lbCreds := &serverNameCheckCreds{
		sn: lbServerName,
	}
	lb = grpc.NewServer(grpc.Creds(lbCreds))
	if err != nil {
		err = fmt.Errorf("Failed to generate the port number %v", err)
		return
	}
	ls = newRemoteBalancer(nil)
	lbspb.RegisterLoadBalancerServer(lb, ls)
	go func() {
		lb.Serve(lbLis)
	}()

	tss = &testServers{
		lbAddr:  fakeName + ":" + strconv.Itoa(lbLis.Addr().(*net.TCPAddr).Port),
		ls:      ls,
		lb:      lb,
		beIPs:   beIPs,
		bePorts: bePorts,
	}
	cleanup = func() {
		defer stopBackends(backends)
		defer func() {
			ls.stop()
			lb.Stop()
		}()
	}
	return
}

func TestGRPCLB(t *testing.T) {
	defer leakcheck.Check(t)

	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	tss, cleanup, err := newLoadBalancer(1)
	if err != nil {
		t.Fatalf("failed to create new load balancer: %v", err)
	}
	defer cleanup()

	be := &lbmpb.Server{
		IpAddress:        tss.beIPs[0],
		Port:             int32(tss.bePorts[0]),
		LoadBalanceToken: lbToken,
	}
	var bes []*lbmpb.Server
	bes = append(bes, be)
	sl := &lbmpb.ServerList{
		Servers: bes,
	}
	tss.ls.sls <- sl
	creds := serverNameCheckCreds{
		expected: beServerName,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := grpc.DialContext(ctx, r.Scheme()+":///"+beServerName,
		grpc.WithTransportCredentials(&creds), grpc.WithDialer(fakeNameDialer))
	if err != nil {
		t.Fatalf("Failed to dial to the backend %v", err)
	}
	defer cc.Close()
	testC := testpb.NewTestServiceClient(cc)

	r.NewAddress([]resolver.Address{{
		Addr:       tss.lbAddr,
		Type:       resolver.GRPCLB,
		ServerName: lbServerName,
	}})

	if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false)); err != nil {
		t.Fatalf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
	}
}

// The remote balancer sends response with duplicates to grpclb client.
func TestGRPCLBWeighted(t *testing.T) {
	defer leakcheck.Check(t)

	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	tss, cleanup, err := newLoadBalancer(2)
	if err != nil {
		t.Fatalf("failed to create new load balancer: %v", err)
	}
	defer cleanup()

	beServers := []*lbmpb.Server{{
		IpAddress:        tss.beIPs[0],
		Port:             int32(tss.bePorts[0]),
		LoadBalanceToken: lbToken,
	}, {
		IpAddress:        tss.beIPs[1],
		Port:             int32(tss.bePorts[1]),
		LoadBalanceToken: lbToken,
	}}
	portsToIndex := make(map[int]int)
	for i := range beServers {
		portsToIndex[tss.bePorts[i]] = i
	}

	creds := serverNameCheckCreds{
		expected: beServerName,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := grpc.DialContext(ctx, r.Scheme()+":///"+beServerName,
		grpc.WithTransportCredentials(&creds), grpc.WithDialer(fakeNameDialer))
	if err != nil {
		t.Fatalf("Failed to dial to the backend %v", err)
	}
	defer cc.Close()
	testC := testpb.NewTestServiceClient(cc)

	r.NewAddress([]resolver.Address{{
		Addr:       tss.lbAddr,
		Type:       resolver.GRPCLB,
		ServerName: lbServerName,
	}})

	sequences := []string{"00101", "00011"}
	for _, seq := range sequences {
		var (
			bes    []*lbmpb.Server
			p      peer.Peer
			result string
		)
		for _, s := range seq {
			bes = append(bes, beServers[s-'0'])
		}
		tss.ls.sls <- &lbmpb.ServerList{Servers: bes}

		for i := 0; i < 1000; i++ {
			if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false), grpc.Peer(&p)); err != nil {
				t.Fatalf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
			}
			result += strconv.Itoa(portsToIndex[p.Addr.(*net.TCPAddr).Port])
		}
		// The generated result will be in format of "0010100101".
		if !strings.Contains(result, strings.Repeat(seq, 2)) {
			t.Errorf("got result sequence %q, want patten %q", result, seq)
		}
	}
}

func TestDropRequest(t *testing.T) {
	defer leakcheck.Check(t)

	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	tss, cleanup, err := newLoadBalancer(1)
	if err != nil {
		t.Fatalf("failed to create new load balancer: %v", err)
	}
	defer cleanup()
	tss.ls.sls <- &lbmpb.ServerList{
		Servers: []*lbmpb.Server{{
			IpAddress:            tss.beIPs[0],
			Port:                 int32(tss.bePorts[0]),
			LoadBalanceToken:     lbToken,
			DropForLoadBalancing: false,
		}, {
			DropForLoadBalancing: true,
		}},
	}
	creds := serverNameCheckCreds{
		expected: beServerName,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := grpc.DialContext(ctx, r.Scheme()+":///"+beServerName,
		grpc.WithTransportCredentials(&creds), grpc.WithDialer(fakeNameDialer))
	if err != nil {
		t.Fatalf("Failed to dial to the backend %v", err)
	}
	defer cc.Close()
	testC := testpb.NewTestServiceClient(cc)

	r.NewAddress([]resolver.Address{{
		Addr:       tss.lbAddr,
		Type:       resolver.GRPCLB,
		ServerName: lbServerName,
	}})

	// Wait for the 1st, non-fail-fast RPC to succeed. This ensures both server
	// connections are made, because the first one has DropForLoadBalancing set
	// to true.
	var i int
	for i = 0; i < 1000; i++ {
		if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false)); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if i >= 1000 {
		t.Fatalf("%v.SayHello(_, _) = _, %v, want _, <nil>", testC, err)
	}
	for _, failfast := range []bool{true, false} {
		for i := 0; i < 3; i++ {
			// Even RPCs should fail, because the 2st backend has
			// DropForLoadBalancing set to true.
			if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(failfast)); status.Code(err) != codes.Unavailable {
				t.Errorf("%v.EmptyCall(_, _) = _, %v, want _, %s", testC, err, codes.Unavailable)
			}
			// Odd RPCs should succeed since they choose the non-drop-request
			// backend according to the round robin policy.
			if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(failfast)); err != nil {
				t.Errorf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
			}
		}
	}
}

// When the balancer in use disconnects, grpclb should connect to the next address from resolved balancer address list.
func TestBalancerDisconnects(t *testing.T) {
	defer leakcheck.Check(t)

	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	var (
		tests []*testServers
		lbs   []*grpc.Server
	)
	for i := 0; i < 2; i++ {
		tss, cleanup, err := newLoadBalancer(1)
		if err != nil {
			t.Fatalf("failed to create new load balancer: %v", err)
		}
		defer cleanup()

		be := &lbmpb.Server{
			IpAddress:        tss.beIPs[0],
			Port:             int32(tss.bePorts[0]),
			LoadBalanceToken: lbToken,
		}
		var bes []*lbmpb.Server
		bes = append(bes, be)
		sl := &lbmpb.ServerList{
			Servers: bes,
		}
		tss.ls.sls <- sl

		tests = append(tests, tss)
		lbs = append(lbs, tss.lb)
	}

	creds := serverNameCheckCreds{
		expected: beServerName,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := grpc.DialContext(ctx, r.Scheme()+":///"+beServerName,
		grpc.WithTransportCredentials(&creds), grpc.WithDialer(fakeNameDialer))
	if err != nil {
		t.Fatalf("Failed to dial to the backend %v", err)
	}
	defer cc.Close()
	testC := testpb.NewTestServiceClient(cc)

	r.NewAddress([]resolver.Address{{
		Addr:       tests[0].lbAddr,
		Type:       resolver.GRPCLB,
		ServerName: lbServerName,
	}, {
		Addr:       tests[1].lbAddr,
		Type:       resolver.GRPCLB,
		ServerName: lbServerName,
	}})

	var p peer.Peer
	if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false), grpc.Peer(&p)); err != nil {
		t.Fatalf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
	}
	if p.Addr.(*net.TCPAddr).Port != tests[0].bePorts[0] {
		t.Fatalf("got peer: %v, want peer port: %v", p.Addr, tests[0].bePorts[0])
	}

	lbs[0].Stop()
	// Stop balancer[0], balancer[1] should be used by grpclb.
	// Check peer address to see if that happened.
	for i := 0; i < 1000; i++ {
		if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false), grpc.Peer(&p)); err != nil {
			t.Fatalf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
		}
		if p.Addr.(*net.TCPAddr).Port == tests[1].bePorts[0] {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("No RPC sent to second backend after 1 second")
}

type customGRPCLBBuilder struct {
	balancer.Builder
	name string
}

func (b *customGRPCLBBuilder) Name() string {
	return b.name
}

const grpclbCustomFallbackName = "grpclb_with_custom_fallback_timeout"

func init() {
	balancer.Register(&customGRPCLBBuilder{
		Builder: grpc.NewLBBuilderWithFallbackTimeout(100 * time.Millisecond),
		name:    grpclbCustomFallbackName,
	})
}

func TestFallback(t *testing.T) {
	defer leakcheck.Check(t)

	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	tss, cleanup, err := newLoadBalancer(1)
	if err != nil {
		t.Fatalf("failed to create new load balancer: %v", err)
	}
	defer cleanup()

	// Start a standalone backend.
	beLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen %v", err)
	}
	defer beLis.Close()
	standaloneBEs := startBackends(beServerName, true, beLis)
	defer stopBackends(standaloneBEs)

	be := &lbmpb.Server{
		IpAddress:        tss.beIPs[0],
		Port:             int32(tss.bePorts[0]),
		LoadBalanceToken: lbToken,
	}
	var bes []*lbmpb.Server
	bes = append(bes, be)
	sl := &lbmpb.ServerList{
		Servers: bes,
	}
	tss.ls.sls <- sl
	creds := serverNameCheckCreds{
		expected: beServerName,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := grpc.DialContext(ctx, r.Scheme()+":///"+beServerName,
		grpc.WithBalancerName(grpclbCustomFallbackName),
		grpc.WithTransportCredentials(&creds), grpc.WithDialer(fakeNameDialer))
	if err != nil {
		t.Fatalf("Failed to dial to the backend %v", err)
	}
	defer cc.Close()
	testC := testpb.NewTestServiceClient(cc)

	r.NewAddress([]resolver.Address{{
		Addr:       "",
		Type:       resolver.GRPCLB,
		ServerName: lbServerName,
	}, {
		Addr:       beLis.Addr().String(),
		Type:       resolver.Backend,
		ServerName: beServerName,
	}})

	var p peer.Peer
	if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false), grpc.Peer(&p)); err != nil {
		t.Fatalf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
	}
	if p.Addr.String() != beLis.Addr().String() {
		t.Fatalf("got peer: %v, want peer: %v", p.Addr, beLis.Addr())
	}

	r.NewAddress([]resolver.Address{{
		Addr:       tss.lbAddr,
		Type:       resolver.GRPCLB,
		ServerName: lbServerName,
	}, {
		Addr:       beLis.Addr().String(),
		Type:       resolver.Backend,
		ServerName: beServerName,
	}})

	for i := 0; i < 1000; i++ {
		if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false), grpc.Peer(&p)); err != nil {
			t.Fatalf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
		}
		if p.Addr.(*net.TCPAddr).Port == tss.bePorts[0] {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("No RPC sent to backend behind remote balancer after 1 second")
}

type failPreRPCCred struct{}

func (failPreRPCCred) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if strings.Contains(uri[0], failtosendURI) {
		return nil, fmt.Errorf("rpc should fail to send")
	}
	return nil, nil
}

func (failPreRPCCred) RequireTransportSecurity() bool {
	return false
}

func checkStats(stats *lbmpb.ClientStats, expected *lbmpb.ClientStats) error {
	if !proto.Equal(stats, expected) {
		return fmt.Errorf("stats not equal: got %+v, want %+v", stats, expected)
	}
	return nil
}

func runAndGetStats(t *testing.T, dropForLoadBalancing, dropForRateLimiting bool, runRPCs func(*grpc.ClientConn)) lbmpb.ClientStats {
	defer leakcheck.Check(t)

	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	tss, cleanup, err := newLoadBalancer(1)
	if err != nil {
		t.Fatalf("failed to create new load balancer: %v", err)
	}
	defer cleanup()
	tss.ls.sls <- &lbmpb.ServerList{
		Servers: []*lbmpb.Server{{
			IpAddress:            tss.beIPs[0],
			Port:                 int32(tss.bePorts[0]),
			LoadBalanceToken:     lbToken,
			DropForLoadBalancing: dropForLoadBalancing,
			DropForRateLimiting:  dropForRateLimiting,
		}},
	}
	tss.ls.statsDura = 100 * time.Millisecond
	creds := serverNameCheckCreds{expected: beServerName}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := grpc.DialContext(ctx, r.Scheme()+":///"+beServerName,
		grpc.WithTransportCredentials(&creds),
		grpc.WithPerRPCCredentials(failPreRPCCred{}),
		grpc.WithDialer(fakeNameDialer))
	if err != nil {
		t.Fatalf("Failed to dial to the backend %v", err)
	}
	defer cc.Close()

	r.NewAddress([]resolver.Address{{
		Addr:       tss.lbAddr,
		Type:       resolver.GRPCLB,
		ServerName: lbServerName,
	}})

	runRPCs(cc)
	time.Sleep(1 * time.Second)
	tss.ls.mu.Lock()
	stats := tss.ls.stats
	tss.ls.mu.Unlock()
	return stats
}

const (
	countRPC      = 40
	failtosendURI = "failtosend"
	dropErrDesc   = "request dropped by grpclb"
)

func TestGRPCLBStatsUnarySuccess(t *testing.T) {
	defer leakcheck.Check(t)
	stats := runAndGetStats(t, false, false, func(cc *grpc.ClientConn) {
		testC := testpb.NewTestServiceClient(cc)
		// The first non-failfast RPC succeeds, all connections are up.
		if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false)); err != nil {
			t.Fatalf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
		}
		for i := 0; i < countRPC-1; i++ {
			testC.EmptyCall(context.Background(), &testpb.Empty{})
		}
	})

	if err := checkStats(&stats, &lbmpb.ClientStats{
		NumCallsStarted:               int64(countRPC),
		NumCallsFinished:              int64(countRPC),
		NumCallsFinishedKnownReceived: int64(countRPC),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCLBStatsUnaryDropLoadBalancing(t *testing.T) {
	defer leakcheck.Check(t)
	c := 0
	stats := runAndGetStats(t, true, false, func(cc *grpc.ClientConn) {
		testC := testpb.NewTestServiceClient(cc)
		for {
			c++
			if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}); err != nil {
				if strings.Contains(err.Error(), dropErrDesc) {
					break
				}
			}
		}
		for i := 0; i < countRPC; i++ {
			testC.EmptyCall(context.Background(), &testpb.Empty{})
		}
	})

	if err := checkStats(&stats, &lbmpb.ClientStats{
		NumCallsStarted:                          int64(countRPC + c),
		NumCallsFinished:                         int64(countRPC + c),
		NumCallsFinishedWithDropForLoadBalancing: int64(countRPC + 1),
		NumCallsFinishedWithClientFailedToSend:   int64(c - 1),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCLBStatsUnaryDropRateLimiting(t *testing.T) {
	defer leakcheck.Check(t)
	c := 0
	stats := runAndGetStats(t, false, true, func(cc *grpc.ClientConn) {
		testC := testpb.NewTestServiceClient(cc)
		for {
			c++
			if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}); err != nil {
				if strings.Contains(err.Error(), dropErrDesc) {
					break
				}
			}
		}
		for i := 0; i < countRPC; i++ {
			testC.EmptyCall(context.Background(), &testpb.Empty{})
		}
	})

	if err := checkStats(&stats, &lbmpb.ClientStats{
		NumCallsStarted:                         int64(countRPC + c),
		NumCallsFinished:                        int64(countRPC + c),
		NumCallsFinishedWithDropForRateLimiting: int64(countRPC + 1),
		NumCallsFinishedWithClientFailedToSend:  int64(c - 1),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCLBStatsUnaryFailedToSend(t *testing.T) {
	defer leakcheck.Check(t)
	stats := runAndGetStats(t, false, false, func(cc *grpc.ClientConn) {
		testC := testpb.NewTestServiceClient(cc)
		// The first non-failfast RPC succeeds, all connections are up.
		if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.FailFast(false)); err != nil {
			t.Fatalf("%v.EmptyCall(_, _) = _, %v, want _, <nil>", testC, err)
		}
		for i := 0; i < countRPC-1; i++ {
			cc.Invoke(context.Background(), failtosendURI, &testpb.Empty{}, nil)
		}
	})

	if err := checkStats(&stats, &lbmpb.ClientStats{
		NumCallsStarted:                        int64(countRPC),
		NumCallsFinished:                       int64(countRPC),
		NumCallsFinishedWithClientFailedToSend: int64(countRPC - 1),
		NumCallsFinishedKnownReceived:          1,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCLBStatsStreamingSuccess(t *testing.T) {
	defer leakcheck.Check(t)
	stats := runAndGetStats(t, false, false, func(cc *grpc.ClientConn) {
		testC := testpb.NewTestServiceClient(cc)
		// The first non-failfast RPC succeeds, all connections are up.
		stream, err := testC.FullDuplexCall(context.Background(), grpc.FailFast(false))
		if err != nil {
			t.Fatalf("%v.FullDuplexCall(_, _) = _, %v, want _, <nil>", testC, err)
		}
		for {
			if _, err = stream.Recv(); err == io.EOF {
				break
			}
		}
		for i := 0; i < countRPC-1; i++ {
			stream, err = testC.FullDuplexCall(context.Background())
			if err == nil {
				// Wait for stream to end if err is nil.
				for {
					if _, err = stream.Recv(); err == io.EOF {
						break
					}
				}
			}
		}
	})

	if err := checkStats(&stats, &lbmpb.ClientStats{
		NumCallsStarted:               int64(countRPC),
		NumCallsFinished:              int64(countRPC),
		NumCallsFinishedKnownReceived: int64(countRPC),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCLBStatsStreamingDropLoadBalancing(t *testing.T) {
	defer leakcheck.Check(t)
	c := 0
	stats := runAndGetStats(t, true, false, func(cc *grpc.ClientConn) {
		testC := testpb.NewTestServiceClient(cc)
		for {
			c++
			if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}); err != nil {
				if strings.Contains(err.Error(), dropErrDesc) {
					break
				}
			}
		}
		for i := 0; i < countRPC; i++ {
			testC.FullDuplexCall(context.Background())
		}
	})

	if err := checkStats(&stats, &lbmpb.ClientStats{
		NumCallsStarted:                          int64(countRPC + c),
		NumCallsFinished:                         int64(countRPC + c),
		NumCallsFinishedWithDropForLoadBalancing: int64(countRPC + 1),
		NumCallsFinishedWithClientFailedToSend:   int64(c - 1),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCLBStatsStreamingDropRateLimiting(t *testing.T) {
	defer leakcheck.Check(t)
	c := 0
	stats := runAndGetStats(t, false, true, func(cc *grpc.ClientConn) {
		testC := testpb.NewTestServiceClient(cc)
		for {
			c++
			if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}); err != nil {
				if strings.Contains(err.Error(), dropErrDesc) {
					break
				}
			}
		}
		for i := 0; i < countRPC; i++ {
			testC.FullDuplexCall(context.Background())
		}
	})

	if err := checkStats(&stats, &lbmpb.ClientStats{
		NumCallsStarted:                         int64(countRPC + c),
		NumCallsFinished:                        int64(countRPC + c),
		NumCallsFinishedWithDropForRateLimiting: int64(countRPC + 1),
		NumCallsFinishedWithClientFailedToSend:  int64(c - 1),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCLBStatsStreamingFailedToSend(t *testing.T) {
	defer leakcheck.Check(t)
	stats := runAndGetStats(t, false, false, func(cc *grpc.ClientConn) {
		testC := testpb.NewTestServiceClient(cc)
		// The first non-failfast RPC succeeds, all connections are up.
		stream, err := testC.FullDuplexCall(context.Background(), grpc.FailFast(false))
		if err != nil {
			t.Fatalf("%v.FullDuplexCall(_, _) = _, %v, want _, <nil>", testC, err)
		}
		for {
			if _, err = stream.Recv(); err == io.EOF {
				break
			}
		}
		for i := 0; i < countRPC-1; i++ {
			cc.NewStream(context.Background(), &grpc.StreamDesc{}, failtosendURI)
		}
	})

	if err := checkStats(&stats, &lbmpb.ClientStats{
		NumCallsStarted:                        int64(countRPC),
		NumCallsFinished:                       int64(countRPC),
		NumCallsFinishedWithClientFailedToSend: int64(countRPC - 1),
		NumCallsFinishedKnownReceived:          1,
	}); err != nil {
		t.Fatal(err)
	}
}
