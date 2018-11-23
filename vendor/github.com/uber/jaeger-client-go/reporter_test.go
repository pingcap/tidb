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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/jaeger-lib/metrics"
	mTestutils "github.com/uber/jaeger-lib/metrics/testutils"

	"github.com/uber/jaeger-client-go/testutils"
	j "github.com/uber/jaeger-client-go/thrift-gen/jaeger"
)

type reporterSuite struct {
	suite.Suite
	tracer         opentracing.Tracer
	closer         io.Closer
	serviceName    string
	reporter       *remoteReporter
	collector      *fakeSender
	metricsFactory *metrics.LocalFactory
}

func (s *reporterSuite) SetupTest() {
	s.metricsFactory = metrics.NewLocalFactory(0)
	metrics := NewMetrics(s.metricsFactory, nil)
	s.serviceName = "DOOP"
	s.collector = &fakeSender{}
	s.reporter = NewRemoteReporter(
		s.collector, ReporterOptions.Metrics(metrics),
	).(*remoteReporter)

	s.tracer, s.closer = NewTracer(
		"reporter-test-service",
		NewConstSampler(true),
		s.reporter,
		TracerOptions.Metrics(metrics))
	s.NotNil(s.tracer)
}

func (s *reporterSuite) TearDownTest() {
	s.closer.Close()
	s.tracer = nil
	s.reporter = nil
	s.collector = nil
}

func TestReporter(t *testing.T) {
	suite.Run(t, new(reporterSuite))
}

func (s *reporterSuite) flushReporter() {
	// Wait for reporter queue to add spans to buffer. We could've called reporter.Close(),
	// but then it fails when the test suite calls close on it again (via tracer's Closer).
	time.Sleep(5 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)
	s.reporter.flushSignal <- &wg
	wg.Wait()
}

func (s *reporterSuite) TestRootSpanTags() {
	s.metricsFactory.Clear()
	sp := s.tracer.StartSpan("get_name")
	ext.SpanKindRPCServer.Set(sp)
	ext.PeerService.Set(sp, s.serviceName)
	sp.Finish()
	s.flushReporter()
	s.Equal(1, len(s.collector.Spans()))
	span := s.collector.Spans()[0]
	s.Len(span.tags, 4)
	s.EqualValues("server", span.tags[2].value, "span.kind should be server")

	mTestutils.AssertCounterMetrics(s.T(), s.metricsFactory,
		mTestutils.ExpectedMetric{
			Name:  "jaeger.reporter-spans",
			Tags:  map[string]string{"state": "success"},
			Value: 1,
		},
	)
}

func (s *reporterSuite) TestClientSpan() {
	s.metricsFactory.Clear()
	sp := s.tracer.StartSpan("get_name")
	ext.SpanKindRPCServer.Set(sp)
	ext.PeerService.Set(sp, s.serviceName)
	sp2 := s.tracer.StartSpan("get_last_name", opentracing.ChildOf(sp.Context()))
	ext.SpanKindRPCClient.Set(sp2)
	ext.PeerService.Set(sp2, s.serviceName)
	sp2.Finish()
	sp.Finish()
	s.flushReporter()
	s.Equal(2, len(s.collector.Spans()))
	span := s.collector.Spans()[0] // child span is reported first
	s.EqualValues(span.context.spanID, sp2.(*Span).context.spanID)
	s.Len(span.tags, 2)
	s.EqualValues("client", span.tags[0].value, "span.kind should be client")

	mTestutils.AssertCounterMetrics(s.T(), s.metricsFactory,
		mTestutils.ExpectedMetric{
			Name:  "jaeger.reporter-spans",
			Tags:  map[string]string{"state": "success"},
			Value: 2,
		},
	)
}

func (s *reporterSuite) TestTagsAndEvents() {
	sp := s.tracer.StartSpan("get_name")
	sp.LogEvent("hello")
	sp.LogEvent(strings.Repeat("long event ", 30))
	expected := []string{"long", "ping", "awake", "awake", "one", "two", "three", "bite me",
		SamplerParamTagKey, SamplerTypeTagKey, "does not compute"}
	sp.SetTag("long", strings.Repeat("x", 300))
	sp.SetTag("ping", "pong")
	sp.SetTag("awake", true)
	sp.SetTag("awake", false)
	sp.SetTag("one", 1)
	sp.SetTag("two", int32(2))
	sp.SetTag("three", int64(3))
	sp.SetTag("bite me", []byte{1})
	sp.SetTag("does not compute", sp) // should be converted to string
	sp.Finish()
	s.flushReporter()
	s.Equal(1, len(s.collector.Spans()))
	span := s.collector.Spans()[0]
	s.Equal(2, len(span.logs), "expecting two logs")
	s.Equal(len(expected), len(span.tags),
		"expecting %d tags", len(expected))
	tags := []string{}
	for _, tag := range span.tags {
		tags = append(tags, string(tag.key))
	}
	sort.Strings(expected)
	sort.Strings(tags)
	s.Equal(expected, tags, "expecting %d tags", len(expected))

	s.NotNil(findDomainLog(span, "hello"), "expecting 'hello' log: %+v", span.logs)
}

func TestUDPReporter(t *testing.T) {
	agent, err := testutils.StartMockAgent()
	require.NoError(t, err)
	defer agent.Close()

	testRemoteReporter(t,
		func(m *Metrics) (Transport, error) {
			return NewUDPTransport(agent.SpanServerAddr(), 0)
		},
		func() []*j.Batch {
			return agent.GetJaegerBatches()
		})
}

func testRemoteReporter(
	t *testing.T,
	factory func(m *Metrics) (Transport, error),
	getBatches func() []*j.Batch,
) {
	metricsFactory := metrics.NewLocalFactory(0)
	metrics := NewMetrics(metricsFactory, nil)

	sender, err := factory(metrics)
	require.NoError(t, err)
	reporter := NewRemoteReporter(sender, ReporterOptions.Metrics(metrics)).(*remoteReporter)

	tracer, closer := NewTracer(
		"reporter-test-service",
		NewConstSampler(true),
		reporter,
		TracerOptions.Metrics(metrics))

	span := tracer.StartSpan("leela")
	ext.SpanKindRPCClient.Set(span)
	ext.PeerService.Set(span, "downstream")
	span.Finish()
	closer.Close() // close the tracer, which also closes and flushes the reporter
	// however, in case of UDP reporter it's fire and forget, so we need to wait a bit
	time.Sleep(5 * time.Millisecond)

	batches := getBatches()
	require.Equal(t, 1, len(batches))
	require.Equal(t, 1, len(batches[0].Spans))
	assert.Equal(t, "leela", batches[0].Spans[0].OperationName)
	assert.Equal(t, "reporter-test-service", batches[0].Process.ServiceName)
	tag := findJaegerTag("peer.service", batches[0].Spans[0].Tags)
	assert.NotNil(t, tag)
	assert.Equal(t, "downstream", *tag.VStr)

	mTestutils.AssertCounterMetrics(t, metricsFactory, []mTestutils.ExpectedMetric{
		{Name: "jaeger.reporter-spans", Tags: map[string]string{"state": "success"}, Value: 1},
		{Name: "jaeger.reporter-spans", Tags: map[string]string{"state": "failure"}, Value: 0},
	}...)
}

func (s *reporterSuite) TestMemoryReporterReport() {
	sp := s.tracer.StartSpan("leela")
	ext.PeerService.Set(sp, s.serviceName)
	reporter := NewInMemoryReporter()
	reporter.Report(sp.(*Span))
	s.Equal(1, reporter.SpansSubmitted(), "expected number of spans submitted")
	reporter.Close()
}

type fakeSender struct {
	spans []*Span
	mutex sync.Mutex
}

func (s *fakeSender) Append(span *Span) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.spans = append(s.spans, span)
	return 1, nil
}

func (s *fakeSender) Flush() (int, error) {
	return 0, nil
}

func (s *fakeSender) Close() error { return nil }

func (s *fakeSender) Spans() []*Span {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	res := make([]*Span, len(s.spans))
	copy(res, s.spans)
	return res
}

func findDomainLog(span *Span, key string) *opentracing.LogRecord {
	for _, log := range span.logs {
		if log.Fields[0].Value().(string) == key {
			return &log
		}
	}
	return nil
}

func findDomainTag(span *Span, key string) *Tag {
	for _, tag := range span.tags {
		if tag.key == key {
			return &tag
		}
	}
	return nil
}
