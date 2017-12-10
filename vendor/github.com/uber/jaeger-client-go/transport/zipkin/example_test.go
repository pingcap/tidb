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

package zipkin_test

import (
	"log"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-client-go/transport/zipkin"
)

func ExampleNewHTTPTransport() {
	// assume this is your main()

	transport, err := zipkin.NewHTTPTransport(
		"http://localhost:9411/api/v1/spans",
		zipkin.HTTPBatchSize(10),
		zipkin.HTTPLogger(jlog.StdLogger),
	)
	if err != nil {
		log.Fatalf("Cannot initialize Zipkin HTTP transport: %v", err)
	}
	tracer, closer := jaeger.NewTracer(
		"my-service-name",
		jaeger.NewConstSampler(true),
		jaeger.NewRemoteReporter(transport, nil),
	)
	defer closer.Close()
	opentracing.InitGlobalTracer(tracer)

	// initialize servers
}
