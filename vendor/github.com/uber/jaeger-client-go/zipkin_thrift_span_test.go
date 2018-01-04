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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/jaeger-client-go/thrift-gen/zipkincore"
	"github.com/uber/jaeger-client-go/utils"
)

func TestThriftFirstInProcessSpan(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()

	sp1 := tracer.StartSpan("s1").(*Span)
	sp2 := tracer.StartSpan("sp2", opentracing.ChildOf(sp1.Context())).(*Span)
	sp2.Finish()
	sp1.Finish()

	tests := []struct {
		span     *Span
		wantTags bool
	}{
		{sp1, true},
		{sp2, false},
	}

	for _, test := range tests {
		var check func(assert.TestingT, interface{}, ...interface{}) bool
		if test.wantTags {
			check = assert.NotNil
		} else {
			check = assert.Nil
		}
		thriftSpan := BuildZipkinThrift(test.span)
		version := findBinaryAnnotation(thriftSpan, JaegerClientVersionTagKey)
		hostname := findBinaryAnnotation(thriftSpan, TracerHostnameTagKey)
		check(t, version)
		check(t, hostname)
	}
}

func TestThriftForceSampled(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(false), // sample nothing
		NewNullReporter())
	defer closer.Close()

	sp := tracer.StartSpan("s1").(*Span)
	ext.SamplingPriority.Set(sp, 1)
	assert.True(t, sp.context.IsSampled())
	assert.True(t, sp.context.IsDebug())
	thriftSpan := BuildZipkinThrift(sp)
	assert.True(t, thriftSpan.Debug)
}

func TestThriftSpanLogs(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()
	root := tracer.StartSpan("s1")

	someTime := time.Now().Add(-time.Minute)
	someTimeInt64 := utils.TimeToMicrosecondsSinceEpochInt64(someTime)

	fields := func(fields ...log.Field) []log.Field {
		return fields
	}
	tests := []struct {
		fields            []log.Field
		logFunc           func(sp opentracing.Span)
		expected          string
		expectedTimestamp int64
		disableSampling   bool
	}{
		{fields: fields(log.String("event", "happened")), expected: "happened"},
		{fields: fields(log.String("something", "happened")), expected: `{"something":"happened"}`},
		{fields: fields(log.Bool("something", true)), expected: `{"something":"true"}`},
		{fields: fields(log.Int("something", 123)), expected: `{"something":"123"}`},
		{fields: fields(log.Int32("something", 123)), expected: `{"something":"123"}`},
		{fields: fields(log.Int64("something", 123)), expected: `{"something":"123"}`},
		{fields: fields(log.Uint32("something", 123)), expected: `{"something":"123"}`},
		{fields: fields(log.Uint64("something", 123)), expected: `{"something":"123"}`},
		{fields: fields(log.Float32("something", 123)), expected: `{"something":"123.000000"}`},
		{fields: fields(log.Float64("something", 123)), expected: `{"something":"123.000000"}`},
		{fields: fields(log.Error(errors.New("drugs are baaad, m-k"))),
			expected: `{"error":"drugs are baaad, m-k"}`},
		{fields: fields(log.Object("something", 123)), expected: `{"something":"123"}`},
		{
			fields: fields(log.Lazy(func(fv log.Encoder) {
				fv.EmitBool("something", true)
			})),
			expected: `{"something":"true"}`,
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.LogKV("event", "something")
			},
			expected: "something",
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.LogKV("non-even number of arguments")
			},
			// this is a bit fragile, but ¯\_(ツ)_/¯
			expected: `{"error":"non-even keyValues len: 1","function":"LogKV"}`,
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.LogEvent("something")
			},
			expected: "something",
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.LogEventWithPayload("something", "payload")
			},
			expected: `{"event":"something","payload":"payload"}`,
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.Log(opentracing.LogData{Event: "something"})
			},
			expected: "something",
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.Log(opentracing.LogData{Event: "something", Payload: "payload"})
			},
			expected: `{"event":"something","payload":"payload"}`,
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.FinishWithOptions(opentracing.FinishOptions{
					LogRecords: []opentracing.LogRecord{
						{
							Timestamp: someTime,
							Fields:    fields(log.String("event", "happened")),
						},
					},
				})
			},
			expected:          "happened",
			expectedTimestamp: someTimeInt64,
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.FinishWithOptions(opentracing.FinishOptions{
					BulkLogData: []opentracing.LogData{
						{
							Timestamp: someTime,
							Event:     "happened",
						},
					},
				})
			},
			expected:          "happened",
			expectedTimestamp: someTimeInt64,
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.FinishWithOptions(opentracing.FinishOptions{
					BulkLogData: []opentracing.LogData{
						{
							Timestamp: someTime,
							Event:     "happened",
							Payload:   "payload",
						},
					},
				})
			},
			expected:          `{"event":"happened","payload":"payload"}`,
			expectedTimestamp: someTimeInt64,
		},
		{
			disableSampling: true,
			fields:          fields(log.String("event", "happened")),
			expected:        "",
		},
		{
			disableSampling: true,
			logFunc: func(sp opentracing.Span) {
				sp.LogKV("event", "something")
			},
			expected: "",
		},
	}

	for i, test := range tests {
		testName := fmt.Sprintf("test-%02d", i)
		sp := tracer.StartSpan(testName, opentracing.ChildOf(root.Context()))
		if test.disableSampling {
			ext.SamplingPriority.Set(sp, 0)
		}
		if test.logFunc != nil {
			test.logFunc(sp)
		} else if len(test.fields) > 0 {
			sp.LogFields(test.fields...)
		}
		thriftSpan := BuildZipkinThrift(sp.(*Span))
		if test.disableSampling {
			assert.Equal(t, 0, len(thriftSpan.Annotations), testName)
			continue
		}
		assert.Equal(t, 1, len(thriftSpan.Annotations), testName)
		assert.Equal(t, test.expected, thriftSpan.Annotations[0].Value, testName)
		if test.expectedTimestamp != 0 {
			assert.Equal(t, test.expectedTimestamp, thriftSpan.Annotations[0].Timestamp, testName)
		}
	}
}

func TestThriftLocalComponentSpan(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()

	tests := []struct {
		addComponentTag bool
		wantAnnotation  string
	}{
		{false, "DOOP"}, // Without COMPONENT tag the value is the service name
		{true, "c1"},
	}

	for _, test := range tests {
		sp := tracer.StartSpan("s1").(*Span)
		if test.addComponentTag {
			ext.Component.Set(sp, "c1")
		}
		sp.Finish()
		thriftSpan := BuildZipkinThrift(sp)

		anno := findBinaryAnnotation(thriftSpan, "lc")
		require.NotNil(t, anno)
		assert.EqualValues(t, test.wantAnnotation, anno.Value)
	}
}

func TestSpecialTags(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()

	sp := tracer.StartSpan("s1").(*Span)
	ext.SpanKindRPCServer.Set(sp)
	ext.PeerService.Set(sp, "peer")
	ext.PeerPort.Set(sp, 80)
	ext.PeerHostIPv4.Set(sp, 2130706433)
	sp.Finish()

	thriftSpan := BuildZipkinThrift(sp)
	// Special tags should not be copied over to binary annotations
	assert.Nil(t, findBinaryAnnotation(thriftSpan, "span.kind"))
	assert.Nil(t, findBinaryAnnotation(thriftSpan, "peer.service"))
	assert.Nil(t, findBinaryAnnotation(thriftSpan, "peer.port"))
	assert.Nil(t, findBinaryAnnotation(thriftSpan, "peer.ipv4"))
	assert.Nil(t, findBinaryAnnotation(thriftSpan, "ip"))

	anno := findBinaryAnnotation(thriftSpan, "ca")
	assert.NotNil(t, anno)
	assert.NotNil(t, anno.Host)
	assert.EqualValues(t, 80, anno.Host.Port)
	assert.EqualValues(t, 2130706433, anno.Host.Ipv4)
	assert.EqualValues(t, "peer", anno.Host.ServiceName)

	assert.NotNil(t, findAnnotation(thriftSpan, "sr"))
	assert.NotNil(t, findAnnotation(thriftSpan, "ss"))
}

func TestBaggageLogs(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()

	sp := tracer.StartSpan("s1").(*Span)
	sp.SetBaggageItem("auth.token", "token")
	ext.SpanKindRPCServer.Set(sp)
	sp.Finish()

	thriftSpan := BuildZipkinThrift(sp)
	assert.NotNil(t, findAnnotation(thriftSpan, `{"event":"baggage","key":"auth.token","value":"token"}`))
}

func findAnnotation(span *zipkincore.Span, name string) *zipkincore.Annotation {
	for _, a := range span.Annotations {
		if a.Value == name {
			return a
		}
	}
	return nil
}

func findBinaryAnnotation(span *zipkincore.Span, name string) *zipkincore.BinaryAnnotation {
	for _, a := range span.BinaryAnnotations {
		if a.Key == name {
			return a
		}
	}
	return nil
}
