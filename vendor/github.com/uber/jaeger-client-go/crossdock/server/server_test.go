// Copyright (c) 2017 Uber Technologies, Inc.
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
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/crossdock/common"
	"github.com/uber/jaeger-client-go/crossdock/log"
	"github.com/uber/jaeger-client-go/crossdock/thrift/tracetest"
)

func TestServerJSON(t *testing.T) {
	tracer, tCloser := jaeger.NewTracer(
		"crossdock",
		jaeger.NewConstSampler(false),
		jaeger.NewNullReporter())
	defer tCloser.Close()

	s := &Server{HostPortHTTP: "127.0.0.1:0", HostPortTChannel: "127.0.0.1:0", Tracer: tracer}
	err := s.Start()
	require.NoError(t, err)
	defer s.Close()

	req := tracetest.NewStartTraceRequest()
	req.Sampled = true
	req.Baggage = "Zoidberg"
	req.Downstream = &tracetest.Downstream{
		ServiceName: "go",
		Host:        "localhost",
		Port:        s.GetPortHTTP(),
		Transport:   tracetest.Transport_HTTP,
		Downstream: &tracetest.Downstream{
			ServiceName: "go",
			Host:        "localhost",
			Port:        s.GetPortTChannel(),
			Transport:   tracetest.Transport_TCHANNEL,
		},
	}

	url := fmt.Sprintf("http://%s/start_trace", s.HostPortHTTP)
	result, err := common.PostJSON(context.Background(), url, req)

	require.NoError(t, err)
	log.Printf("response=%+v", &result)
}

func TestObserveSpan(t *testing.T) {
	tracer, tCloser := jaeger.NewTracer(
		"crossdock",
		jaeger.NewConstSampler(true),
		jaeger.NewNullReporter())
	defer tCloser.Close()

	_, err := observeSpan(context.Background(), tracer)
	assert.Error(t, err)

	span := tracer.StartSpan("hi")
	span.SetBaggageItem(BaggageKey, "xyz")
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	s, err := observeSpan(ctx, tracer)
	assert.NoError(t, err)
	assert.True(t, s.Sampled)
	traceID := span.Context().(jaeger.SpanContext).TraceID().String()
	assert.Equal(t, traceID, s.TraceId)
	assert.Equal(t, "xyz", s.Baggage)
}
