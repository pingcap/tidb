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

	j "github.com/uber/jaeger-client-go/thrift-gen/jaeger"
	"github.com/uber/jaeger-client-go/utils"
)

var (
	someString      = "str"
	someBool        = true
	someLong        = int64(123)
	someDouble      = float64(123)
	someBinary      = []byte("hello")
	someSlice       = []string{"a"}
	someSliceString = "[a]"
)

func TestBuildJaegerThrift(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()

	sp1 := tracer.StartSpan("sp1").(*Span)
	ext.SpanKindRPCServer.Set(sp1)
	ext.PeerService.Set(sp1, "svc")
	sp2 := tracer.StartSpan("sp2", opentracing.ChildOf(sp1.Context())).(*Span)
	ext.SpanKindRPCClient.Set(sp2)
	sp2.Finish()
	sp1.Finish()

	jaegerSpan1 := BuildJaegerThrift(sp1)
	jaegerSpan2 := BuildJaegerThrift(sp2)
	assert.Equal(t, "sp1", jaegerSpan1.OperationName)
	assert.Equal(t, "sp2", jaegerSpan2.OperationName)
	assert.EqualValues(t, 0, jaegerSpan1.ParentSpanId)
	assert.Equal(t, jaegerSpan1.SpanId, jaegerSpan2.ParentSpanId)
	assert.Len(t, jaegerSpan1.Tags, 4)
	tag := findTag(jaegerSpan1, SamplerTypeTagKey)
	assert.Equal(t, SamplerTypeConst, *tag.VStr)
	tag = findTag(jaegerSpan1, string(ext.SpanKind))
	assert.Equal(t, string(ext.SpanKindRPCServerEnum), *tag.VStr)
	tag = findTag(jaegerSpan1, string(ext.PeerService))
	assert.Equal(t, "svc", *tag.VStr)
	assert.Empty(t, jaegerSpan1.References)

	assert.Len(t, jaegerSpan2.References, 1)
	assert.Equal(t, j.SpanRefType_CHILD_OF, jaegerSpan2.References[0].RefType)
	assert.EqualValues(t, jaegerSpan1.TraceIdLow, jaegerSpan2.References[0].TraceIdLow)
	assert.EqualValues(t, jaegerSpan1.TraceIdHigh, jaegerSpan2.References[0].TraceIdHigh)
	assert.EqualValues(t, jaegerSpan1.SpanId, jaegerSpan2.References[0].SpanId)
	tag = findTag(jaegerSpan2, string(ext.SpanKind))
	assert.Equal(t, string(ext.SpanKindRPCClientEnum), *tag.VStr)
}

func TestBuildJaegerProcessThrift(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()

	sp := tracer.StartSpan("sp1").(*Span)
	sp.Finish()

	process := BuildJaegerProcessThrift(sp)
	assert.Equal(t, process.ServiceName, "DOOP")
	require.Len(t, process.Tags, 3)
	assert.NotNil(t, findJaegerTag("jaeger.version", process.Tags))
	assert.NotNil(t, findJaegerTag("hostname", process.Tags))
	assert.NotNil(t, findJaegerTag("ip", process.Tags))
}

func TestBuildLogs(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()
	root := tracer.StartSpan("s1")

	someTime := time.Now().Add(-time.Minute)
	someTimeInt64 := utils.TimeToMicrosecondsSinceEpochInt64(someTime)

	errString := "error"

	tests := []struct {
		field             log.Field
		logFunc           func(sp opentracing.Span)
		expected          []*j.Tag
		expectedTimestamp int64
		disableSampling   bool
	}{
		{field: log.String("event", someString), expected: []*j.Tag{{Key: "event", VType: j.TagType_STRING, VStr: &someString}}},
		{field: log.String("k", someString), expected: []*j.Tag{{Key: "k", VType: j.TagType_STRING, VStr: &someString}}},
		{field: log.Bool("k", someBool), expected: []*j.Tag{{Key: "k", VType: j.TagType_BOOL, VBool: &someBool}}},
		{field: log.Int("k", 123), expected: []*j.Tag{{Key: "k", VType: j.TagType_LONG, VLong: &someLong}}},
		{field: log.Int32("k", 123), expected: []*j.Tag{{Key: "k", VType: j.TagType_LONG, VLong: &someLong}}},
		{field: log.Int64("k", 123), expected: []*j.Tag{{Key: "k", VType: j.TagType_LONG, VLong: &someLong}}},
		{field: log.Uint32("k", 123), expected: []*j.Tag{{Key: "k", VType: j.TagType_LONG, VLong: &someLong}}},
		{field: log.Uint64("k", 123), expected: []*j.Tag{{Key: "k", VType: j.TagType_LONG, VLong: &someLong}}},
		{field: log.Float32("k", 123), expected: []*j.Tag{{Key: "k", VType: j.TagType_DOUBLE, VDouble: &someDouble}}},
		{field: log.Float64("k", 123), expected: []*j.Tag{{Key: "k", VType: j.TagType_DOUBLE, VDouble: &someDouble}}},
		{field: log.Error(errors.New(errString)), expected: []*j.Tag{{Key: "error", VType: j.TagType_STRING, VStr: &errString}}},
		{field: log.Object("k", someSlice), expected: []*j.Tag{{Key: "k", VType: j.TagType_STRING, VStr: &someSliceString}}},
		{
			field: log.Lazy(func(fv log.Encoder) {
				fv.EmitBool("k", someBool)
			}),
			expected: []*j.Tag{{Key: "k", VType: j.TagType_BOOL, VBool: &someBool}},
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.LogKV("event", someString)
			},
			expected: []*j.Tag{{Key: "event", VType: j.TagType_STRING, VStr: &someString}},
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.LogKV("non-even number of arguments")
			},
			// this is a bit fragile, but ¯\_(ツ)_/¯
			expected: []*j.Tag{
				{Key: "error", VType: j.TagType_STRING, VStr: getStringPtr("non-even keyValues len: 1")},
				{Key: "function", VType: j.TagType_STRING, VStr: getStringPtr("LogKV")},
			},
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.LogEvent(someString)
			},
			expected: []*j.Tag{{Key: "event", VType: j.TagType_STRING, VStr: &someString}},
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.LogEventWithPayload(someString, "payload")
			},
			expected: []*j.Tag{
				{Key: "event", VType: j.TagType_STRING, VStr: &someString},
				{Key: "payload", VType: j.TagType_STRING, VStr: getStringPtr("payload")},
			},
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.Log(opentracing.LogData{Event: someString})
			},
			expected: []*j.Tag{{Key: "event", VType: j.TagType_STRING, VStr: &someString}},
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.Log(opentracing.LogData{Event: someString, Payload: "payload"})
			},
			expected: []*j.Tag{
				{Key: "event", VType: j.TagType_STRING, VStr: &someString},
				{Key: "payload", VType: j.TagType_STRING, VStr: getStringPtr("payload")},
			},
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.FinishWithOptions(opentracing.FinishOptions{
					LogRecords: []opentracing.LogRecord{
						{
							Timestamp: someTime,
							Fields:    []log.Field{log.String("event", someString)},
						},
					},
				})
			},
			expected:          []*j.Tag{{Key: "event", VType: j.TagType_STRING, VStr: &someString}},
			expectedTimestamp: someTimeInt64,
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.FinishWithOptions(opentracing.FinishOptions{
					BulkLogData: []opentracing.LogData{
						{
							Timestamp: someTime,
							Event:     someString,
						},
					},
				})
			},
			expected:          []*j.Tag{{Key: "event", VType: j.TagType_STRING, VStr: &someString}},
			expectedTimestamp: someTimeInt64,
		},
		{
			logFunc: func(sp opentracing.Span) {
				sp.FinishWithOptions(opentracing.FinishOptions{
					BulkLogData: []opentracing.LogData{
						{
							Timestamp: someTime,
							Event:     someString,
							Payload:   "payload",
						},
					},
				})
			},
			expected: []*j.Tag{
				{Key: "event", VType: j.TagType_STRING, VStr: &someString},
				{Key: "payload", VType: j.TagType_STRING, VStr: getStringPtr("payload")},
			},
			expectedTimestamp: someTimeInt64,
		},
		{
			disableSampling: true,
			field:           log.String("event", someString),
		},
		{
			disableSampling: true,
			logFunc: func(sp opentracing.Span) {
				sp.LogKV("event", someString)
			},
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
		} else if test.field != (log.Field{}) {
			sp.LogFields(test.field)
		}
		jaegerSpan := BuildJaegerThrift(sp.(*Span))
		if test.disableSampling {
			assert.Equal(t, 0, len(jaegerSpan.Logs), testName)
			continue
		}
		assert.Equal(t, 1, len(jaegerSpan.Logs), testName)
		compareTagSlices(t, test.expected, jaegerSpan.Logs[0].GetFields(), testName)
		if test.expectedTimestamp != 0 {
			assert.Equal(t, test.expectedTimestamp, jaegerSpan.Logs[0].Timestamp, testName)
		}
	}
}

func TestBuildTags(t *testing.T) {
	tests := []struct {
		tag      Tag
		expected *j.Tag
	}{
		{tag: Tag{key: "k", value: someString}, expected: &j.Tag{Key: "k", VType: j.TagType_STRING, VStr: &someString}},
		{tag: Tag{key: "k", value: int(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: uint(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: int8(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: uint8(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: int16(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: uint16(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: int32(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: uint32(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: int64(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: uint64(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_LONG, VLong: &someLong}},
		{tag: Tag{key: "k", value: float32(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_DOUBLE, VDouble: &someDouble}},
		{tag: Tag{key: "k", value: float64(123)}, expected: &j.Tag{Key: "k", VType: j.TagType_DOUBLE, VDouble: &someDouble}},
		{tag: Tag{key: "k", value: someBool}, expected: &j.Tag{Key: "k", VType: j.TagType_BOOL, VBool: &someBool}},
		{tag: Tag{key: "k", value: someBinary}, expected: &j.Tag{Key: "k", VType: j.TagType_BINARY, VBinary: someBinary}},
		{tag: Tag{key: "k", value: someSlice}, expected: &j.Tag{Key: "k", VType: j.TagType_STRING, VStr: &someSliceString}},
	}
	for i, test := range tests {
		testName := fmt.Sprintf("test-%02d", i)
		actual := buildTags([]Tag{test.tag})
		assert.Len(t, actual, 1)
		compareTags(t, test.expected, actual[0], testName)
	}
}

func TestBuildReferences(t *testing.T) {
	references := []Reference{
		{Type: opentracing.ChildOfRef, Context: SpanContext{traceID: TraceID{High: 1, Low: 1}, spanID: SpanID(1)}},
		{Type: opentracing.FollowsFromRef, Context: SpanContext{traceID: TraceID{High: 2, Low: 2}, spanID: SpanID(2)}},
	}
	spanRefs := buildReferences(references)
	assert.Len(t, spanRefs, 2)
	assert.Equal(t, j.SpanRefType_CHILD_OF, spanRefs[0].RefType)
	assert.EqualValues(t, 1, spanRefs[0].SpanId)
	assert.EqualValues(t, 1, spanRefs[0].TraceIdHigh)
	assert.EqualValues(t, 1, spanRefs[0].TraceIdLow)

	assert.Equal(t, j.SpanRefType_FOLLOWS_FROM, spanRefs[1].RefType)
	assert.EqualValues(t, 2, spanRefs[1].SpanId)
	assert.EqualValues(t, 2, spanRefs[1].TraceIdHigh)
	assert.EqualValues(t, 2, spanRefs[1].TraceIdLow)
}

func TestJaegerSpanBaggageLogs(t *testing.T) {
	tracer, closer := NewTracer("DOOP",
		NewConstSampler(true),
		NewNullReporter())
	defer closer.Close()

	sp := tracer.StartSpan("s1").(*Span)
	sp.SetBaggageItem("auth.token", "token")
	ext.SpanKindRPCServer.Set(sp)
	sp.Finish()

	jaegerSpan := BuildJaegerThrift(sp)
	require.Len(t, jaegerSpan.Logs, 1)
	fields := jaegerSpan.Logs[0].Fields
	require.Len(t, fields, 3)
	assertJaegerTag(t, fields, "event", "baggage")
	assertJaegerTag(t, fields, "key", "auth.token")
	assertJaegerTag(t, fields, "value", "token")
}

func assertJaegerTag(t *testing.T, tags []*j.Tag, key string, value string) {
	tag := findJaegerTag(key, tags)
	require.NotNil(t, tag)
	assert.Equal(t, value, tag.GetVStr())
}

func getStringPtr(s string) *string {
	return &s
}

func findTag(span *j.Span, key string) *j.Tag {
	for _, s := range span.Tags {
		if s.Key == key {
			return s
		}
	}
	return nil
}

func findJaegerTag(key string, tags []*j.Tag) *j.Tag {
	for _, tag := range tags {
		if tag.Key == key {
			return tag
		}
	}
	return nil
}

func compareTagSlices(t *testing.T, expectedTags, actualTags []*j.Tag, testName string) {
	assert.Equal(t, len(expectedTags), len(actualTags))
	for _, expectedTag := range expectedTags {
		actualTag := findJaegerTag(expectedTag.Key, actualTags)
		compareTags(t, expectedTag, actualTag, testName)
	}
}

func compareTags(t *testing.T, expected, actual *j.Tag, testName string) {
	if expected == nil && actual == nil {
		return
	}
	if expected == nil || actual == nil {
		assert.Fail(t, "one of the tags is nil", testName)
		return
	}
	assert.Equal(t, expected.Key, actual.Key, testName)
	assert.Equal(t, expected.VType, actual.VType, testName)
	switch expected.VType {
	case j.TagType_STRING:
		assert.Equal(t, *expected.VStr, *actual.VStr, testName)
	case j.TagType_LONG:
		assert.Equal(t, *expected.VLong, *actual.VLong, testName)
	case j.TagType_DOUBLE:
		assert.Equal(t, *expected.VDouble, *actual.VDouble, testName)
	case j.TagType_BOOL:
		assert.Equal(t, *expected.VBool, *actual.VBool, testName)
	case j.TagType_BINARY:
		assert.Equal(t, expected.VBinary, actual.VBinary, testName)
	}
}
