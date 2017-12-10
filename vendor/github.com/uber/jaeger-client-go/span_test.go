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

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaggageIterator(t *testing.T) {
	service := "DOOP"
	tracer, closer := NewTracer(service, NewConstSampler(true), NewNullReporter())
	defer closer.Close()

	sp1 := tracer.StartSpan("s1").(*Span)
	sp1.SetBaggageItem("Some_Key", "12345")
	sp1.SetBaggageItem("Some-other-key", "42")
	expectedBaggage := map[string]string{"some-key": "12345", "some-other-key": "42"}
	assertBaggage(t, sp1, expectedBaggage)
	assertBaggageRecords(t, sp1, expectedBaggage)

	b := extractBaggage(sp1, false) // break out early
	assert.Equal(t, 1, len(b), "only one baggage item should be extracted")

	sp2 := tracer.StartSpan("s2", opentracing.ChildOf(sp1.Context())).(*Span)
	assertBaggage(t, sp2, expectedBaggage) // child inherits the same baggage
	require.Len(t, sp2.logs, 0)            // child doesn't inherit the baggage logs
}

func assertBaggageRecords(t *testing.T, sp *Span, expected map[string]string) {
	require.Len(t, sp.logs, len(expected))
	for _, logRecord := range sp.logs {
		require.Len(t, logRecord.Fields, 3)
		require.Equal(t, "event:baggage", logRecord.Fields[0].String())
		key := logRecord.Fields[1].Value().(string)
		value := logRecord.Fields[2].Value().(string)

		require.Contains(t, expected, key)
		assert.Equal(t, expected[key], value)
	}
}

func assertBaggage(t *testing.T, sp opentracing.Span, expected map[string]string) {
	b := extractBaggage(sp, true)
	assert.Equal(t, expected, b)
}

func extractBaggage(sp opentracing.Span, allItems bool) map[string]string {
	b := make(map[string]string)
	sp.Context().ForeachBaggageItem(func(k, v string) bool {
		b[k] = v
		return allItems
	})
	return b
}

func TestSpanProperties(t *testing.T) {
	tracer, closer := NewTracer("DOOP", NewConstSampler(true), NewNullReporter())
	defer closer.Close()

	sp1 := tracer.StartSpan("s1").(*Span)
	assert.Equal(t, tracer, sp1.Tracer())
	assert.NotNil(t, sp1.Context())
}

func TestSpanOperationName(t *testing.T) {
	tracer, closer := NewTracer("DOOP", NewConstSampler(true), NewNullReporter())
	defer closer.Close()

	sp1 := tracer.StartSpan("s1").(*Span)
	sp1.SetOperationName("s2")
	sp1.Finish()

	assert.Equal(t, "s2", sp1.OperationName())
}
