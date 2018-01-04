// Copyright (c) 2015 Uber Technologies, Inc.

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
	"io"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/testutils"

	"github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-client-go/utils"
)

type tracerSuite struct {
	suite.Suite
	tracer         opentracing.Tracer
	closer         io.Closer
	metricsFactory *metrics.LocalFactory
}

func (s *tracerSuite) SetupTest() {
	s.metricsFactory = metrics.NewLocalFactory(0)
	metrics := NewMetrics(s.metricsFactory, nil)

	s.tracer, s.closer = NewTracer("DOOP", // respect the classics, man!
		NewConstSampler(true),
		NewNullReporter(),
		TracerOptions.Metrics(metrics),
		TracerOptions.ZipkinSharedRPCSpan(true),
	)
	s.NotNil(s.tracer)
}

func (s *tracerSuite) TearDownTest() {
	if s.tracer != nil {
		s.closer.Close()
		s.tracer = nil
	}
}

func TestTracerSuite(t *testing.T) {
	suite.Run(t, new(tracerSuite))
}

func (s *tracerSuite) TestBeginRootSpan() {
	s.metricsFactory.Clear()
	startTime := time.Now()
	s.tracer.(*Tracer).timeNow = func() time.Time { return startTime }
	someID := uint64(12345)
	s.tracer.(*Tracer).randomNumber = func() uint64 { return someID }

	sp := s.tracer.StartSpan("get_name")
	ext.SpanKindRPCServer.Set(sp)
	ext.PeerService.Set(sp, "peer-service")
	s.NotNil(sp)
	ss := sp.(*Span)
	s.NotNil(ss.tracer, "Tracer must be referenced from span")
	s.Equal("get_name", ss.operationName)
	s.Len(ss.tags, 4, "Span should have 2 sampler tags, span.kind tag and peer.service tag")
	s.EqualValues(Tag{key: "span.kind", value: ext.SpanKindRPCServerEnum}, ss.tags[2], "Span must be server-side")
	s.EqualValues(Tag{key: "peer.service", value: "peer-service"}, ss.tags[3], "Client is 'peer-service'")

	s.EqualValues(someID, ss.context.traceID.Low)
	s.EqualValues(0, ss.context.parentID)

	s.Equal(startTime, ss.startTime)

	sp.Finish()
	s.NotNil(ss.duration)

	testutils.AssertCounterMetrics(s.T(), s.metricsFactory, []testutils.ExpectedMetric{
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "started"}, Value: 1},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "finished"}, Value: 1},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "sampling", "sampled": "y"}, Value: 1},
		{Name: "jaeger.traces", Tags: map[string]string{"sampled": "y", "state": "started"}, Value: 1},
	}...)
}

func (s *tracerSuite) TestStartRootSpanWithOptions() {
	ts := time.Now()
	sp := s.tracer.StartSpan("get_address", opentracing.StartTime(ts))
	ss := sp.(*Span)
	s.Equal("get_address", ss.operationName)
	s.Equal(ts, ss.startTime)
}

func (s *tracerSuite) TestStartChildSpan() {
	s.metricsFactory.Clear()
	sp1 := s.tracer.StartSpan("get_address")
	sp2 := s.tracer.StartSpan("get_street", opentracing.ChildOf(sp1.Context()))
	s.Equal(sp1.(*Span).context.spanID, sp2.(*Span).context.parentID)
	sp2.Finish()
	s.NotNil(sp2.(*Span).duration)
	sp1.Finish()
	testutils.AssertCounterMetrics(s.T(), s.metricsFactory, []testutils.ExpectedMetric{
		{Name: "jaeger.spans", Tags: map[string]string{"group": "sampling", "sampled": "y"}, Value: 2},
		{Name: "jaeger.traces", Tags: map[string]string{"sampled": "y", "state": "started"}, Value: 1},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "started"}, Value: 2},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "finished"}, Value: 2},
	}...)
}

type nonJaegerSpanContext struct{}

func (c nonJaegerSpanContext) ForeachBaggageItem(handler func(k, v string) bool) {}

func (s *tracerSuite) TestStartSpanWithMultipleReferences() {
	s.metricsFactory.Clear()
	sp1 := s.tracer.StartSpan("A")
	sp2 := s.tracer.StartSpan("B")
	sp3 := s.tracer.StartSpan("C")
	sp4 := s.tracer.StartSpan(
		"D",
		opentracing.ChildOf(sp1.Context()),
		opentracing.ChildOf(sp2.Context()),
		opentracing.FollowsFrom(sp3.Context()),
		opentracing.FollowsFrom(nonJaegerSpanContext{}),
		opentracing.FollowsFrom(SpanContext{}), // Empty span context should be excluded
	)
	// Should use the first ChildOf ref span as the parent
	s.Equal(sp1.(*Span).context.spanID, sp4.(*Span).context.parentID)
	sp4.Finish()
	s.NotNil(sp4.(*Span).duration)
	sp3.Finish()
	sp2.Finish()
	sp1.Finish()
	testutils.AssertCounterMetrics(s.T(), s.metricsFactory, []testutils.ExpectedMetric{
		{Name: "jaeger.spans", Tags: map[string]string{"group": "sampling", "sampled": "y"}, Value: 4},
		{Name: "jaeger.traces", Tags: map[string]string{"sampled": "y", "state": "started"}, Value: 3},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "started"}, Value: 4},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "finished"}, Value: 4},
	}...)
	assert.Len(s.T(), sp4.(*Span).references, 3)
}

func (s *tracerSuite) TestStartSpanWithOnlyFollowFromReference() {
	s.metricsFactory.Clear()
	sp1 := s.tracer.StartSpan("A")
	sp2 := s.tracer.StartSpan(
		"B",
		opentracing.FollowsFrom(sp1.Context()),
	)
	// Should use the first ChildOf ref span as the parent
	s.Equal(sp1.(*Span).context.spanID, sp2.(*Span).context.parentID)
	sp2.Finish()
	s.NotNil(sp2.(*Span).duration)
	sp1.Finish()
	testutils.AssertCounterMetrics(s.T(), s.metricsFactory, []testutils.ExpectedMetric{
		{Name: "jaeger.spans", Tags: map[string]string{"group": "sampling", "sampled": "y"}, Value: 2},
		{Name: "jaeger.traces", Tags: map[string]string{"sampled": "y", "state": "started"}, Value: 1},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "started"}, Value: 2},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "finished"}, Value: 2},
	}...)
	assert.Len(s.T(), sp2.(*Span).references, 1)
}

func (s *tracerSuite) TestTraceStartedOrJoinedMetrics() {
	tests := []struct {
		sampled bool
		label   string
	}{
		{true, "y"},
		{false, "n"},
	}
	for _, test := range tests {
		s.metricsFactory.Clear()
		s.tracer.(*Tracer).sampler = NewConstSampler(test.sampled)
		sp1 := s.tracer.StartSpan("parent", ext.RPCServerOption(nil))
		sp2 := s.tracer.StartSpan("child1", opentracing.ChildOf(sp1.Context()))
		sp3 := s.tracer.StartSpan("child2", ext.RPCServerOption(sp2.Context()))
		s.Equal(sp2.(*Span).context.spanID, sp3.(*Span).context.spanID)
		s.Equal(sp2.(*Span).context.parentID, sp3.(*Span).context.parentID)
		sp3.Finish()
		sp2.Finish()
		sp1.Finish()
		s.Equal(test.sampled, sp1.Context().(SpanContext).IsSampled())
		s.Equal(test.sampled, sp2.Context().(SpanContext).IsSampled())

		testutils.AssertCounterMetrics(s.T(), s.metricsFactory, []testutils.ExpectedMetric{
			{Name: "jaeger.spans", Tags: map[string]string{"group": "sampling", "sampled": test.label}, Value: 3},
			{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "started"}, Value: 3},
			{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "finished"}, Value: 3},
			{Name: "jaeger.traces", Tags: map[string]string{"sampled": test.label, "state": "started"}, Value: 1},
			{Name: "jaeger.traces", Tags: map[string]string{"sampled": test.label, "state": "joined"}, Value: 1},
		}...)
	}
}

func (s *tracerSuite) TestSetOperationName() {
	sp1 := s.tracer.StartSpan("get_address")
	sp1.SetOperationName("get_street")
	s.Equal("get_street", sp1.(*Span).operationName)
}

func (s *tracerSuite) TestSamplerEffects() {
	s.tracer.(*Tracer).sampler = NewConstSampler(true)
	sp := s.tracer.StartSpan("test")
	flags := sp.(*Span).context.flags
	s.EqualValues(flagSampled, flags&flagSampled)

	s.tracer.(*Tracer).sampler = NewConstSampler(false)
	sp = s.tracer.StartSpan("test")
	flags = sp.(*Span).context.flags
	s.EqualValues(0, flags&flagSampled)
}

func (s *tracerSuite) TestRandomIDNotZero() {
	val := uint64(0)
	s.tracer.(*Tracer).randomNumber = func() (r uint64) {
		r = val
		val++
		return
	}
	sp := s.tracer.StartSpan("get_name").(*Span)
	s.EqualValues(TraceID{Low: 1}, sp.context.traceID)

	rng := utils.NewRand(0)
	rng.Seed(1) // for test coverage
}

func TestTracerOptions(t *testing.T) {
	t1, e := time.Parse(time.RFC3339, "2012-11-01T22:08:41+00:00")
	assert.NoError(t, e)

	timeNow := func() time.Time {
		return t1
	}
	rnd := func() uint64 {
		return 1
	}

	openTracer, closer := NewTracer("DOOP", // respect the classics, man!
		NewConstSampler(true),
		NewNullReporter(),
		TracerOptions.Logger(log.StdLogger),
		TracerOptions.TimeNow(timeNow),
		TracerOptions.RandomNumber(rnd),
		TracerOptions.PoolSpans(true),
		TracerOptions.Tag("tag_key", "tag_value"),
	)
	defer closer.Close()

	tracer := openTracer.(*Tracer)
	assert.Equal(t, log.StdLogger, tracer.logger)
	assert.Equal(t, t1, tracer.timeNow())
	assert.Equal(t, uint64(1), tracer.randomNumber())
	assert.Equal(t, uint64(1), tracer.randomNumber())
	assert.Equal(t, uint64(1), tracer.randomNumber()) // always 1
	assert.Equal(t, true, tracer.options.poolSpans)
	assert.Equal(t, opentracing.Tag{Key: "tag_key", Value: "tag_value"}, tracer.Tags()[0])
}

func TestInjectorExtractorOptions(t *testing.T) {
	tracer, tc := NewTracer("x", NewConstSampler(true), NewNullReporter(),
		TracerOptions.Injector("dummy", &dummyPropagator{}),
		TracerOptions.Extractor("dummy", &dummyPropagator{}),
	)
	defer tc.Close()

	sp := tracer.StartSpan("x")
	c := &dummyCarrier{}
	err := tracer.Inject(sp.Context(), "dummy", []int{})
	assert.Equal(t, opentracing.ErrInvalidCarrier, err)
	err = tracer.Inject(sp.Context(), "dummy", c)
	assert.NoError(t, err)
	assert.True(t, c.ok)

	c.ok = false
	_, err = tracer.Extract("dummy", []int{})
	assert.Equal(t, opentracing.ErrInvalidCarrier, err)
	_, err = tracer.Extract("dummy", c)
	assert.Equal(t, opentracing.ErrSpanContextNotFound, err)
	c.ok = true
	_, err = tracer.Extract("dummy", c)
	assert.NoError(t, err)
}

func TestEmptySpanContextAsParent(t *testing.T) {
	tracer, tc := NewTracer("x", NewConstSampler(true), NewNullReporter())
	defer tc.Close()

	span := tracer.StartSpan("test", opentracing.ChildOf(emptyContext))
	ctx := span.Context().(SpanContext)
	assert.True(t, ctx.traceID.IsValid())
	assert.True(t, ctx.IsValid())
}

func TestZipkinSharedRPCSpan(t *testing.T) {
	tracer, tc := NewTracer("x", NewConstSampler(true), NewNullReporter(), TracerOptions.ZipkinSharedRPCSpan(false))

	sp1 := tracer.StartSpan("client", ext.SpanKindRPCClient)
	sp2 := tracer.StartSpan("server", opentracing.ChildOf(sp1.Context()), ext.SpanKindRPCServer)
	assert.Equal(t, sp1.(*Span).context.spanID, sp2.(*Span).context.parentID)
	assert.NotEqual(t, sp1.(*Span).context.spanID, sp2.(*Span).context.spanID)
	sp2.Finish()
	sp1.Finish()
	tc.Close()

	tracer, tc = NewTracer("x", NewConstSampler(true), NewNullReporter(), TracerOptions.ZipkinSharedRPCSpan(true))

	sp1 = tracer.StartSpan("client", ext.SpanKindRPCClient)
	sp2 = tracer.StartSpan("server", opentracing.ChildOf(sp1.Context()), ext.SpanKindRPCServer)
	assert.Equal(t, sp1.(*Span).context.spanID, sp2.(*Span).context.spanID)
	assert.Equal(t, sp1.(*Span).context.parentID, sp2.(*Span).context.parentID)
	sp2.Finish()
	sp1.Finish()
	tc.Close()
}

type dummyPropagator struct{}
type dummyCarrier struct {
	ok bool
}

func (p *dummyPropagator) Inject(ctx SpanContext, carrier interface{}) error {
	c, ok := carrier.(*dummyCarrier)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	c.ok = true
	return nil
}

func (p *dummyPropagator) Extract(carrier interface{}) (SpanContext, error) {
	c, ok := carrier.(*dummyCarrier)
	if !ok {
		return emptyContext, opentracing.ErrInvalidCarrier
	}
	if c.ok {
		return emptyContext, nil
	}
	return emptyContext, opentracing.ErrSpanContextNotFound
}
