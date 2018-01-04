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

package jaeger

import (
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
)

func TestEmptyObserver(t *testing.T) {
	tracer, closer := NewTracer("test", NewConstSampler(true), NewInMemoryReporter())
	defer closer.Close()
	s := tracer.StartSpan("test", ext.RPCServerOption(nil))
	s.Finish()
	assert.Equal(t, s.(*Span).observer, noopSpanObserver)
}

func TestObservers(t *testing.T) {
	tracer, closer := NewTracer(
		"test",
		NewConstSampler(true),
		NewInMemoryReporter(),
		TracerOptions.Observer(testObserver{}),
		TracerOptions.Observer(testObserver{}),
	)
	defer closer.Close()

	s := tracer.StartSpan("test", ext.RPCServerOption(nil))

	forEachObs := func(f func(so *testSpanObserver)) {
		observers := s.(*Span).observer.(spanObserver).observers
		assert.Len(t, observers, 2)
		for _, so := range observers {
			f(so.(*testSpanObserver))
		}
	}

	forEachObs(func(so *testSpanObserver) {
		assert.Equal(t, testSpanObserver{
			operationName: "test",
			tags: map[string]interface{}{
				"span.kind": ext.SpanKindRPCServerEnum,
			},
		}, *so)
	})

	s.SetOperationName("test2")
	s.SetTag("bender", "rodriguez")
	forEachObs(func(so *testSpanObserver) {
		assert.Equal(t, testSpanObserver{
			operationName: "test2",
			tags: map[string]interface{}{
				"span.kind": ext.SpanKindRPCServerEnum,
				"bender":    "rodriguez",
			},
		}, *so)
	})

	s.Finish()
	forEachObs(func(so *testSpanObserver) {
		assert.True(t, so.finished)
	})
}

type testObserver struct{}

type testSpanObserver struct {
	operationName string
	tags          map[string]interface{}
	finished      bool
}

func (o testObserver) OnStartSpan(operationName string, options opentracing.StartSpanOptions) SpanObserver {
	tags := make(map[string]interface{})
	for k, v := range options.Tags {
		tags[k] = v
	}
	return &testSpanObserver{
		operationName: operationName,
		tags:          tags,
	}
}

func (o *testSpanObserver) OnSetOperationName(operationName string) {
	o.operationName = operationName
}

func (o *testSpanObserver) OnSetTag(key string, value interface{}) {
	o.tags[key] = value
}

func (o *testSpanObserver) OnFinish(options opentracing.FinishOptions) {
	o.finished = true
}
