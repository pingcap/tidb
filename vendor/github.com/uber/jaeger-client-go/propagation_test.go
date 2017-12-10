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
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/testutils"
)

func initMetrics() (*metrics.LocalFactory, *Metrics) {
	factory := metrics.NewLocalFactory(0)
	return factory, NewMetrics(factory, nil)
}

func TestSpanPropagator(t *testing.T) {
	const op = "test"
	reporter := NewInMemoryReporter()
	metricsFactory, metrics := initMetrics()
	tracer, closer := NewTracer("x", NewConstSampler(true), reporter, TracerOptions.Metrics(metrics), TracerOptions.ZipkinSharedRPCSpan(true))

	mapc := opentracing.TextMapCarrier(make(map[string]string))
	httpc := opentracing.HTTPHeadersCarrier(http.Header{})
	tests := []struct {
		format, carrier, formatName interface{}
	}{
		{SpanContextFormat, new(SpanContext), "TraceContextFormat"},
		{opentracing.Binary, new(bytes.Buffer), "Binary"},
		{opentracing.TextMap, mapc, "TextMap"},
		{opentracing.HTTPHeaders, httpc, "HTTPHeaders"},
	}

	sp := tracer.StartSpan(op)
	sp.SetTag("x", "y") // to avoid later comparing nil vs. []
	sp.SetBaggageItem("foo", "bar")
	for _, test := range tests {
		// starting normal child to extract its serialized context
		child := tracer.StartSpan(op, opentracing.ChildOf(sp.Context()))
		err := tracer.Inject(child.Context(), test.format, test.carrier)
		assert.NoError(t, err)
		// Note: we're not finishing the above span
		childCtx, err := tracer.Extract(test.format, test.carrier)
		assert.NoError(t, err)
		child = tracer.StartSpan(test.formatName.(string), ext.RPCServerOption(childCtx))
		child.SetTag("x", "y") // to avoid later comparing nil vs. []
		child.Finish()
	}
	sp.Finish()
	closer.Close()

	otSpans := reporter.GetSpans()
	require.Equal(t, len(tests)+1, len(otSpans), "unexpected number of spans reporter")

	spans := make([]*Span, len(otSpans))
	for i, s := range otSpans {
		spans[i] = s.(*Span)
	}

	// The last span is the original one.
	exp, spans := spans[len(spans)-1], spans[:len(spans)-1]
	exp.duration = time.Duration(123)
	exp.startTime = time.Time{}.Add(1)
	require.Len(t, exp.logs, 1) // The parent span should have baggage logs
	fields := exp.logs[0].Fields
	require.Len(t, fields, 3)
	require.Equal(t, "event", fields[0].Key())
	require.Equal(t, "baggage", fields[0].Value().(string))
	require.Equal(t, "key", fields[1].Key())
	require.Equal(t, "foo", fields[1].Value().(string))
	require.Equal(t, "value", fields[2].Key())
	require.Equal(t, "bar", fields[2].Value().(string))

	if exp.context.ParentID() != 0 {
		t.Fatalf("Root span's ParentID %d is not 0", exp.context.ParentID())
	}

	expTags := exp.tags[2:] // skip two sampler.xxx tags
	for i, sp := range spans {
		formatName := sp.operationName
		if a, e := sp.context.ParentID(), exp.context.SpanID(); a != e {
			t.Fatalf("%d: ParentID %d does not match expectation %d", i, a, e)
		} else {
			// Prepare for comparison.
			sp.context.spanID, sp.context.parentID = exp.context.SpanID(), 0
			sp.duration, sp.startTime = exp.duration, exp.startTime
		}
		assert.Equal(t, exp.context, sp.context, formatName)
		assert.Equal(t, "span.kind", sp.tags[0].key)
		assert.Equal(t, expTags, sp.tags[1:] /*skip span.kind tag*/, formatName)
		assert.Empty(t, sp.logs, formatName)
		// Override collections to avoid tripping comparison on different pointers
		sp.context = exp.context
		sp.tags = exp.tags
		sp.logs = exp.logs
		sp.operationName = op
		sp.references = exp.references
		// Compare the rest of the fields
		assert.Equal(t, exp, sp, formatName)
	}

	testutils.AssertCounterMetrics(t, metricsFactory, []testutils.ExpectedMetric{
		{Name: "jaeger.spans", Tags: map[string]string{"group": "sampling", "sampled": "y"}, Value: 1 + 2*len(tests)},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "started"}, Value: 1 + 2*len(tests)},
		{Name: "jaeger.spans", Tags: map[string]string{"group": "lifecycle", "state": "finished"}, Value: 1 + len(tests)},
		{Name: "jaeger.traces", Tags: map[string]string{"state": "started", "sampled": "y"}, Value: 1},
		{Name: "jaeger.traces", Tags: map[string]string{"state": "joined", "sampled": "y"}, Value: len(tests)},
	}...)
}

func TestSpanIntegrityAfterSerialize(t *testing.T) {
	serializedString := "f6c385a2c57ed8d7:b04a90b7723bdc:76c385a2c57ed8d7:1"

	context, err := ContextFromString(serializedString)
	require.NoError(t, err)
	require.True(t, context.traceID.Low > (uint64(1)<<63))
	require.True(t, int64(context.traceID.Low) < 0)

	newSerializedString := context.String()
	require.Equal(t, serializedString, newSerializedString)
}

func TestDecodingError(t *testing.T) {
	reporter := NewInMemoryReporter()
	metricsFactory, metrics := initMetrics()
	tracer, closer := NewTracer("x", NewConstSampler(true), reporter, TracerOptions.Metrics(metrics))
	defer closer.Close()

	badHeader := "x.x.x.x"
	httpHeader := http.Header{}
	httpHeader.Add(TracerStateHeaderName, badHeader)
	tmc := opentracing.HTTPHeadersCarrier(httpHeader)
	_, err := tracer.Extract(opentracing.HTTPHeaders, tmc)
	assert.Error(t, err)

	testutils.AssertCounterMetrics(t, metricsFactory, testutils.ExpectedMetric{Name: "jaeger.decoding-errors", Value: 1})
}

func TestBaggagePropagationHTTP(t *testing.T) {
	tracer, closer := NewTracer("DOOP", NewConstSampler(true), NewNullReporter())
	defer closer.Close()

	sp1 := tracer.StartSpan("s1")
	sp1.SetBaggageItem("Some_Key", "12345")
	assert.Equal(t, "12345", sp1.BaggageItem("some-KEY"), "baggage: %+v", sp1.(*Span).context.baggage)
	sp1.SetBaggageItem("Some_Key", "98:765") // colon : should be escaped as %3A
	assert.Equal(t, "98:765", sp1.BaggageItem("some-KEY"), "baggage: %+v", sp1.(*Span).context.baggage)

	h := http.Header{}
	h.Add("header1", "value1") // make sure this does not get unmarshalled as baggage
	err := tracer.Inject(sp1.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(h))
	require.NoError(t, err)
	// check that colon : was encoded as %3A
	assert.Equal(t, "98%3A765", h.Get(TraceBaggageHeaderPrefix+"some-key"))

	sp2, err := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(h))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"some-key": "98:765"}, sp2.(SpanContext).baggage)
}

func TestJaegerBaggageHeader(t *testing.T) {
	metricsFactory, metrics := initMetrics()
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter(),
		TracerOptions.Metrics(metrics),
	)
	defer closer.Close()

	h := http.Header{}
	h.Add(JaegerBaggageHeader, "key1=value1, key 2=value two")

	ctx, err := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(h))
	require.NoError(t, err)

	sp := tracer.StartSpan("root", opentracing.ChildOf(ctx)).(*Span)

	assert.Equal(t, "value1", sp.BaggageItem("key1"))
	assert.Equal(t, "value two", sp.BaggageItem("key 2"))

	// ensure that traces.started counter is incremented, not traces.joined
	testutils.AssertCounterMetrics(t, metricsFactory,
		testutils.ExpectedMetric{
			Name: "jaeger.traces", Tags: map[string]string{"state": "started", "sampled": "y"}, Value: 1,
		},
	)
}

func TestParseCommaSeperatedMap(t *testing.T) {
	var testcases = []struct {
		in  string
		out map[string]string
	}{
		{"hobbit=Bilbo Baggins", map[string]string{"hobbit": "Bilbo Baggins"}},
		{"hobbit=Bilbo Baggins, dwarf= Thrain", map[string]string{"hobbit": "Bilbo Baggins", "dwarf": " Thrain"}},
		{"kevin spacey=actor", map[string]string{"kevin spacey": "actor"}},
		{"kevin%20spacey=se7en%3Aactor", map[string]string{"kevin spacey": "se7en:actor"}},
		{"key1=, key2=", map[string]string{"key1": "", "key2": ""}},
		{"malformed", map[string]string{}},
		{"malformed, string", map[string]string{}},
		{"another malformed string", map[string]string{}},
	}

	for _, testcase := range testcases {
		m := parseCommaSeparatedMap(testcase.in)
		assert.Equal(t, testcase.out, m)
	}
}

func TestDebugCorrelationID(t *testing.T) {
	metricsFactory, metrics := initMetrics()
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter(),
		TracerOptions.Metrics(metrics),
	)
	defer closer.Close()

	h := http.Header{}
	h.Add(JaegerDebugHeader, "value1")
	ctx, err := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(h))
	require.NoError(t, err)
	assert.EqualValues(t, 0, ctx.(SpanContext).parentID)
	assert.EqualValues(t, "value1", ctx.(SpanContext).debugID)
	sp := tracer.StartSpan("root", opentracing.ChildOf(ctx)).(*Span)
	assert.EqualValues(t, 0, sp.context.parentID)
	assert.True(t, sp.context.traceID.IsValid())
	assert.True(t, sp.context.IsSampled())
	assert.True(t, sp.context.IsDebug())
	tagFound := false
	for _, tag := range sp.tags {
		if tag.key == JaegerDebugHeader {
			assert.Equal(t, "value1", tag.value)
			tagFound = true
		}
	}
	assert.True(t, tagFound)

	// ensure that traces.started counter is incremented, not traces.joined
	testutils.AssertCounterMetrics(t, metricsFactory,
		testutils.ExpectedMetric{
			Name: "jaeger.traces", Tags: map[string]string{"state": "started", "sampled": "y"}, Value: 1,
		},
	)
}
