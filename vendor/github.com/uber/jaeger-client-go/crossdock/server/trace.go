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

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"golang.org/x/net/context"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/crossdock/common"
	"github.com/uber/jaeger-client-go/crossdock/log"
	"github.com/uber/jaeger-client-go/crossdock/thrift/tracetest"
)

func (s *Server) doStartTrace(req *tracetest.StartTraceRequest) (*tracetest.TraceResponse, error) {
	span := s.Tracer.StartSpan(req.ServerRole)
	if req.Sampled {
		ext.SamplingPriority.Set(span, 1)
	}
	span.SetBaggageItem(BaggageKey, req.Baggage)
	defer span.Finish()

	ctx := opentracing.ContextWithSpan(context.Background(), span)

	return s.prepareResponse(ctx, req.ServerRole, req.Downstream)
}

func (s *Server) doJoinTrace(ctx context.Context, req *tracetest.JoinTraceRequest) (*tracetest.TraceResponse, error) {
	return s.prepareResponse(ctx, req.ServerRole, req.Downstream)
}

func (s *Server) prepareResponse(ctx context.Context, role string, reqDwn *tracetest.Downstream) (*tracetest.TraceResponse, error) {
	observedSpan, err := observeSpan(ctx, s.Tracer)
	if err != nil {
		return nil, err
	}

	resp := tracetest.NewTraceResponse()
	resp.Span = observedSpan

	if reqDwn != nil {
		downstreamResp, err := s.callDownstream(ctx, role, reqDwn)
		if err != nil {
			return nil, err
		}
		resp.Downstream = downstreamResp
	}

	return resp, nil
}

func (s *Server) callDownstream(ctx context.Context, role string, downstream *tracetest.Downstream) (*tracetest.TraceResponse, error) {
	switch downstream.Transport {
	case tracetest.Transport_HTTP:
		return s.callDownstreamHTTP(ctx, downstream)
	case tracetest.Transport_TCHANNEL:
		return s.callDownstreamTChannel(ctx, downstream)
	case tracetest.Transport_DUMMY:
		return &tracetest.TraceResponse{NotImplementedError: "DUMMY transport not implemented"}, nil
	default:
		return nil, errUnrecognizedProtocol
	}
}

func (s *Server) callDownstreamHTTP(ctx context.Context, target *tracetest.Downstream) (*tracetest.TraceResponse, error) {
	req := &tracetest.JoinTraceRequest{
		ServerRole: target.ServerRole,
		Downstream: target.Downstream,
	}
	url := fmt.Sprintf("http://%s:%s/join_trace", target.Host, target.Port)
	log.Printf("Calling downstream service '%s' at %s", target.ServiceName, url)
	return common.PostJSON(ctx, url, req)
}

func observeSpan(ctx context.Context, tracer opentracing.Tracer) (*tracetest.ObservedSpan, error) {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return nil, errNoSpanObserved
	}
	sc := span.Context().(jaeger.SpanContext)
	observedSpan := tracetest.NewObservedSpan()
	observedSpan.TraceId = sc.TraceID().String()
	observedSpan.Sampled = sc.IsSampled()
	observedSpan.Baggage = span.BaggageItem(BaggageKey)
	return observedSpan, nil
}
