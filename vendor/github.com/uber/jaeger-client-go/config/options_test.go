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

package config

import (
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"

	"github.com/uber/jaeger-client-go"
)

func TestApplyOptions(t *testing.T) {
	metricsFactory := metrics.NewLocalFactory(0)
	observer := fakeObserver{}
	opts := applyOptions(
		Metrics(metricsFactory),
		Logger(jaeger.StdLogger),
		Observer(observer),
		ZipkinSharedRPCSpan(true),
	)
	assert.Equal(t, jaeger.StdLogger, opts.logger)
	assert.Equal(t, metricsFactory, opts.metrics)
	assert.Equal(t, []jaeger.Observer{observer}, opts.observers)
	assert.True(t, opts.zipkinSharedRPCSpan)
}

func TestTraceTagOption(t *testing.T) {
	c := Configuration{}
	tracer, closer, err := c.New("test-service", Tag("tag-key", "tag-value"))
	require.NoError(t, err)
	defer closer.Close()
	assert.Equal(t, opentracing.Tag{Key: "tag-key", Value: "tag-value"}, tracer.(*jaeger.Tracer).Tags()[0])
}

func TestApplyOptionsDefaults(t *testing.T) {
	opts := applyOptions()
	assert.Equal(t, jaeger.NullLogger, opts.logger)
	assert.Equal(t, metrics.NullFactory, opts.metrics)
}

type fakeObserver struct{}

func (o fakeObserver) OnStartSpan(operationName string, options opentracing.StartSpanOptions) jaeger.SpanObserver {
	return nil
}
