/*
 *
 * Copyright 2014 gRPC authors.
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

package grpc

import (
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/leakcheck"
	"google.golang.org/grpc/transport"
)

var (
	expectedRequest  = "ping"
	expectedResponse = "pong"
	weirdError       = "format verbs: %v%s"
	sizeLargeErr     = 1024 * 1024
	canceled         = 0
)

type testCodec struct {
}

func (testCodec) Marshal(v interface{}) ([]byte, error) {
	return []byte(*(v.(*string))), nil
}

func (testCodec) Unmarshal(data []byte, v interface{}) error {
	*(v.(*string)) = string(data)
	return nil
}

func (testCodec) String() string {
	return "test"
}

type testStreamHandler struct {
	port string
	t    transport.ServerTransport
}

func (h *testStreamHandler) handleStream(t *testing.T, s *transport.Stream) {
	p := &parser{r: s}
	for {
		pf, req, err := p.recvMsg(math.MaxInt32)
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		if pf != compressionNone {
			t.Errorf("Received the mistaken message format %d, want %d", pf, compressionNone)
			return
		}
		var v string
		codec := testCodec{}
		if err := codec.Unmarshal(req, &v); err != nil {
			t.Errorf("Failed to unmarshal the received message: %v", err)
			return
		}
		if v == "weird error" {
			h.t.WriteStatus(s, status.New(codes.Internal, weirdError))
			return
		}
		if v == "canceled" {
			canceled++
			h.t.WriteStatus(s, status.New(codes.Internal, ""))
			return
		}
		if v == "port" {
			h.t.WriteStatus(s, status.New(codes.Internal, h.port))
			return
		}

		if v != expectedRequest {
			h.t.WriteStatus(s, status.New(codes.Internal, strings.Repeat("A", sizeLargeErr)))
			return
		}
	}
	// send a response back to end the stream.
	hdr, data, err := encode(testCodec{}, &expectedResponse, nil, nil, nil)
	if err != nil {
		t.Errorf("Failed to encode the response: %v", err)
		return
	}
	h.t.Write(s, hdr, data, &transport.Options{})
	h.t.WriteStatus(s, status.New(codes.OK, ""))
}

type server struct {
	lis        net.Listener
	port       string
	addr       string
	startedErr chan error // sent nil or an error after server starts
	mu         sync.Mutex
	conns      map[transport.ServerTransport]bool
}

func newTestServer() *server {
	return &server{startedErr: make(chan error, 1)}
}

// start starts server. Other goroutines should block on s.startedErr for further operations.
func (s *server) start(t *testing.T, port int, maxStreams uint32) {
	var err error
	if port == 0 {
		s.lis, err = net.Listen("tcp", "localhost:0")
	} else {
		s.lis, err = net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	}
	if err != nil {
		s.startedErr <- fmt.Errorf("failed to listen: %v", err)
		return
	}
	s.addr = s.lis.Addr().String()
	_, p, err := net.SplitHostPort(s.addr)
	if err != nil {
		s.startedErr <- fmt.Errorf("failed to parse listener address: %v", err)
		return
	}
	s.port = p
	s.conns = make(map[transport.ServerTransport]bool)
	s.startedErr <- nil
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			return
		}
		config := &transport.ServerConfig{
			MaxStreams: maxStreams,
		}
		st, err := transport.NewServerTransport("http2", conn, config)
		if err != nil {
			continue
		}
		s.mu.Lock()
		if s.conns == nil {
			s.mu.Unlock()
			st.Close()
			return
		}
		s.conns[st] = true
		s.mu.Unlock()
		h := &testStreamHandler{
			port: s.port,
			t:    st,
		}
		go st.HandleStreams(func(s *transport.Stream) {
			go h.handleStream(t, s)
		}, func(ctx context.Context, method string) context.Context {
			return ctx
		})
	}
}

func (s *server) wait(t *testing.T, timeout time.Duration) {
	select {
	case err := <-s.startedErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(timeout):
		t.Fatalf("Timed out after %v waiting for server to be ready", timeout)
	}
}

func (s *server) stop() {
	s.lis.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.conns = nil
	s.mu.Unlock()
}

func setUp(t *testing.T, port int, maxStreams uint32) (*server, *ClientConn) {
	server := newTestServer()
	go server.start(t, port, maxStreams)
	server.wait(t, 2*time.Second)
	addr := "localhost:" + server.port
	cc, err := Dial(addr, WithBlock(), WithInsecure(), WithCodec(testCodec{}))
	if err != nil {
		t.Fatalf("Failed to create ClientConn: %v", err)
	}
	return server, cc
}

func TestInvoke(t *testing.T) {
	defer leakcheck.Check(t)
	server, cc := setUp(t, 0, math.MaxUint32)
	var reply string
	if err := cc.Invoke(context.Background(), "/foo/bar", &expectedRequest, &reply); err != nil || reply != expectedResponse {
		t.Fatalf("grpc.Invoke(_, _, _, _, _) = %v, want <nil>", err)
	}
	cc.Close()
	server.stop()
}

func TestInvokeLargeErr(t *testing.T) {
	defer leakcheck.Check(t)
	server, cc := setUp(t, 0, math.MaxUint32)
	var reply string
	req := "hello"
	err := cc.Invoke(context.Background(), "/foo/bar", &req, &reply)
	if _, ok := status.FromError(err); !ok {
		t.Fatalf("grpc.Invoke(_, _, _, _, _) receives non rpc error.")
	}
	if status.Code(err) != codes.Internal || len(errorDesc(err)) != sizeLargeErr {
		t.Fatalf("grpc.Invoke(_, _, _, _, _) = %v, want an error of code %d and desc size %d", err, codes.Internal, sizeLargeErr)
	}
	cc.Close()
	server.stop()
}

// TestInvokeErrorSpecialChars checks that error messages don't get mangled.
func TestInvokeErrorSpecialChars(t *testing.T) {
	defer leakcheck.Check(t)
	server, cc := setUp(t, 0, math.MaxUint32)
	var reply string
	req := "weird error"
	err := cc.Invoke(context.Background(), "/foo/bar", &req, &reply)
	if _, ok := status.FromError(err); !ok {
		t.Fatalf("grpc.Invoke(_, _, _, _, _) receives non rpc error.")
	}
	if got, want := errorDesc(err), weirdError; got != want {
		t.Fatalf("grpc.Invoke(_, _, _, _, _) error = %q, want %q", got, want)
	}
	cc.Close()
	server.stop()
}

// TestInvokeCancel checks that an Invoke with a canceled context is not sent.
func TestInvokeCancel(t *testing.T) {
	defer leakcheck.Check(t)
	server, cc := setUp(t, 0, math.MaxUint32)
	var reply string
	req := "canceled"
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cc.Invoke(ctx, "/foo/bar", &req, &reply)
	}
	if canceled != 0 {
		t.Fatalf("received %d of 100 canceled requests", canceled)
	}
	cc.Close()
	server.stop()
}

// TestInvokeCancelClosedNonFail checks that a canceled non-failfast RPC
// on a closed client will terminate.
func TestInvokeCancelClosedNonFailFast(t *testing.T) {
	defer leakcheck.Check(t)
	server, cc := setUp(t, 0, math.MaxUint32)
	var reply string
	cc.Close()
	req := "hello"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cc.Invoke(ctx, "/foo/bar", &req, &reply, FailFast(false)); err == nil {
		t.Fatalf("canceled invoke on closed connection should fail")
	}
	server.stop()
}
