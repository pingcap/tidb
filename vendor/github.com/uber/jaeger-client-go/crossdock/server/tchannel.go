// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package server

import (
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/net/context"

	"github.com/uber/jaeger-client-go/crossdock/log"
	"github.com/uber/jaeger-client-go/crossdock/thrift/tracetest"
)

func (s *Server) startTChannelServer(tracer opentracing.Tracer) error {
	channelOpts := &tchannel.ChannelOptions{
		Tracer: tracer,
	}
	ch, err := tchannel.NewChannel("go", channelOpts)
	if err != nil {
		return err
	}
	server := thrift.NewServer(ch)

	s.channel = ch

	handler := tracetest.NewTChanTracedServiceServer(s)
	server.Register(handler)

	if err := ch.ListenAndServe(s.HostPortTChannel); err != nil {
		return err
	}
	s.HostPortTChannel = ch.PeerInfo().HostPort
	log.Printf("Started tchannel server at %s\n", s.HostPortTChannel)
	return nil
}

// StartTrace implements StartTrace() of TChanTracedService
func (s *Server) StartTrace(ctx thrift.Context, request *tracetest.StartTraceRequest) (*tracetest.TraceResponse, error) {
	return nil, errCannotStartInTChannel
}

// JoinTrace implements JoinTrace() of TChanTracedService
func (s *Server) JoinTrace(ctx thrift.Context, request *tracetest.JoinTraceRequest) (*tracetest.TraceResponse, error) {
	log.Printf("tchannel server handling JoinTrace")
	return s.prepareResponse(ctx, request.ServerRole, request.Downstream)
}

func (s *Server) callDownstreamTChannel(ctx context.Context, target *tracetest.Downstream) (*tracetest.TraceResponse, error) {
	req := &tracetest.JoinTraceRequest{
		ServerRole: target.ServerRole,
		Downstream: target.Downstream,
	}

	hostPort := fmt.Sprintf("%s:%s", target.Host, target.Port)
	log.Printf("calling downstream '%s' over tchannel:%s", target.ServiceName, hostPort)

	channelOpts := &tchannel.ChannelOptions{
		Tracer: s.Tracer,
	}
	ch, err := tchannel.NewChannel("tchannel-client", channelOpts)
	if err != nil {
		return nil, err
	}

	opts := &thrift.ClientOptions{HostPort: hostPort}
	thriftClient := thrift.NewClient(ch, target.ServiceName, opts)

	client := tracetest.NewTChanTracedServiceClient(thriftClient)
	ctx, cx := context.WithTimeout(ctx, time.Second)
	defer cx()
	return client.JoinTrace(thrift.Wrap(ctx), req)
}
