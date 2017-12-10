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

package main

import (
	"io"

	"github.com/opentracing/opentracing-go"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/crossdock/client"
	"github.com/uber/jaeger-client-go/crossdock/common"
	"github.com/uber/jaeger-client-go/crossdock/log"
	"github.com/uber/jaeger-client-go/crossdock/server"
	jlog "github.com/uber/jaeger-client-go/log"
)

func main() {
	log.Enabled = true

	tracer, tCloser := initTracer()
	defer tCloser.Close()

	s := &server.Server{Tracer: tracer}
	if err := s.Start(); err != nil {
		panic(err.Error())
	} else {
		defer s.Close()
	}
	client := &client.Client{}
	if err := client.Start(); err != nil {
		panic(err.Error())
	}
}

func initTracer() (opentracing.Tracer, io.Closer) {
	t, c := jaeger.NewTracer(
		common.DefaultTracerServiceName,
		jaeger.NewConstSampler(false),
		jaeger.NewLoggingReporter(jlog.StdLogger))
	return t, c
}
