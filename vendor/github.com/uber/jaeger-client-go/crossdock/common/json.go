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

package common

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/uber/jaeger-client-go/crossdock/thrift/tracetest"
	"github.com/uber/jaeger-client-go/utils"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"golang.org/x/net/context"
)

// PostJSON sends a POST request to `url` with body containing JSON-serialized `req`.
// It injects tracing span into the headers (if found in the context).
// It returns parsed TraceResponse, or error.
func PostJSON(ctx context.Context, url string, req interface{}) (*tracetest.TraceResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	span, err := injectSpan(ctx, httpReq)
	if span != nil {
		defer span.Finish()
	}
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}

	var result tracetest.TraceResponse
	err = utils.ReadJSON(resp, &result)
	return &result, err
}

func injectSpan(ctx context.Context, req *http.Request) (opentracing.Span, error) {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return nil, nil
	}
	span = span.Tracer().StartSpan("post", opentracing.ChildOf(span.Context()))
	ext.SpanKindRPCClient.Set(span)
	c := opentracing.HTTPHeadersCarrier(req.Header)
	err := span.Tracer().Inject(span.Context(), opentracing.HTTPHeaders, c)
	return span, err
}
