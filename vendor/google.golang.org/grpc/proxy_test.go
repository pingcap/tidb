// +build !race

/*
 *
 * Copyright 2017 gRPC authors.
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
	"bufio"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/test/leakcheck"
)

const (
	envTestAddr  = "1.2.3.4:8080"
	envProxyAddr = "2.3.4.5:7687"
)

// overwriteAndRestore overwrite function httpProxyFromEnvironment and
// returns a function to restore the default values.
func overwrite(hpfe func(req *http.Request) (*url.URL, error)) func() {
	backHPFE := httpProxyFromEnvironment
	httpProxyFromEnvironment = hpfe
	return func() {
		httpProxyFromEnvironment = backHPFE
	}
}

type proxyServer struct {
	t   *testing.T
	lis net.Listener
	in  net.Conn
	out net.Conn
}

func (p *proxyServer) run() {
	in, err := p.lis.Accept()
	if err != nil {
		return
	}
	p.in = in

	req, err := http.ReadRequest(bufio.NewReader(in))
	if err != nil {
		p.t.Errorf("failed to read CONNECT req: %v", err)
		return
	}
	if req.Method != http.MethodConnect || req.UserAgent() != grpcUA {
		resp := http.Response{StatusCode: http.StatusMethodNotAllowed}
		resp.Write(p.in)
		p.in.Close()
		p.t.Errorf("get wrong CONNECT req: %+v", req)
		return
	}

	out, err := net.Dial("tcp", req.URL.Host)
	if err != nil {
		p.t.Errorf("failed to dial to server: %v", err)
		return
	}
	resp := http.Response{StatusCode: http.StatusOK, Proto: "HTTP/1.0"}
	resp.Write(p.in)
	p.out = out
	go io.Copy(p.in, p.out)
	go io.Copy(p.out, p.in)
}

func (p *proxyServer) stop() {
	p.lis.Close()
	if p.in != nil {
		p.in.Close()
	}
	if p.out != nil {
		p.out.Close()
	}
}

func TestHTTPConnect(t *testing.T) {
	defer leakcheck.Check(t)
	plis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	p := &proxyServer{t: t, lis: plis}
	go p.run()
	defer p.stop()

	blis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	msg := []byte{4, 3, 5, 2}
	recvBuf := make([]byte, len(msg))
	done := make(chan struct{})
	go func() {
		in, err := blis.Accept()
		if err != nil {
			t.Errorf("failed to accept: %v", err)
			return
		}
		defer in.Close()
		in.Read(recvBuf)
		close(done)
	}()

	// Overwrite the function in the test and restore them in defer.
	hpfe := func(req *http.Request) (*url.URL, error) {
		return &url.URL{Host: plis.Addr().String()}, nil
	}
	defer overwrite(hpfe)()

	// Dial to proxy server.
	dialer := newProxyDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		if deadline, ok := ctx.Deadline(); ok {
			return net.DialTimeout("tcp", addr, deadline.Sub(time.Now()))
		}
		return net.Dial("tcp", addr)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := dialer(ctx, blis.Addr().String())
	if err != nil {
		t.Fatalf("http connect Dial failed: %v", err)
	}
	defer c.Close()

	// Send msg on the connection.
	c.Write(msg)
	<-done

	// Check received msg.
	if string(recvBuf) != string(msg) {
		t.Fatalf("received msg: %v, want %v", recvBuf, msg)
	}
}

func TestMapAddressEnv(t *testing.T) {
	defer leakcheck.Check(t)
	// Overwrite the function in the test and restore them in defer.
	hpfe := func(req *http.Request) (*url.URL, error) {
		if req.URL.Host == envTestAddr {
			return &url.URL{
				Scheme: "https",
				Host:   envProxyAddr,
			}, nil
		}
		return nil, nil
	}
	defer overwrite(hpfe)()

	// envTestAddr should be handled by ProxyFromEnvironment.
	got, err := mapAddress(context.Background(), envTestAddr)
	if err != nil {
		t.Error(err)
	}
	if got != envProxyAddr {
		t.Errorf("want %v, got %v", envProxyAddr, got)
	}
}
