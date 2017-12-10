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

package zipkin

import (
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/uber/jaeger-client-go"
)

var (
	rootSampled       = newSpanContext(1, 2, 0, true)
	nonRootSampled    = newSpanContext(1, 2, 1, true)
	nonRootNonSampled = newSpanContext(1, 2, 1, false)
)

var (
	rootSampledHeader = opentracing.TextMapCarrier{
		"x-b3-traceid": "1",
		"x-b3-spanid":  "2",
		"x-b3-sampled": "1",
	}
	nonRootSampledHeader = opentracing.TextMapCarrier{
		"x-b3-traceid":      "1",
		"x-b3-spanid":       "2",
		"x-b3-parentspanid": "1",
		"x-b3-sampled":      "1",
	}
	nonRootNonSampledHeader = opentracing.TextMapCarrier{
		"x-b3-traceid":      "1",
		"x-b3-spanid":       "2",
		"x-b3-parentspanid": "1",
		"x-b3-sampled":      "0",
	}
	invalidHeader = opentracing.TextMapCarrier{
		"x-b3-traceid":      "jdkafhsd",
		"x-b3-spanid":       "afsdfsdf",
		"x-b3-parentspanid": "hiagggdf",
		"x-b3-sampled":      "sdfgsdfg",
	}
)

var (
	propagator = NewZipkinB3HTTPHeaderPropagator()
)

func newSpanContext(traceID, spanID, parentID uint64, sampled bool) jaeger.SpanContext {
	return jaeger.NewSpanContext(
		jaeger.TraceID{Low: traceID},
		jaeger.SpanID(spanID),
		jaeger.SpanID(parentID),
		sampled,
		nil,
	)
}

func TestExtractorInvalid(t *testing.T) {
	_, err := propagator.Extract(invalidHeader)
	assert.Error(t, err)
}

func TestExtractorRootSampled(t *testing.T) {
	ctx, err := propagator.Extract(rootSampledHeader)
	assert.Nil(t, err)
	assert.EqualValues(t, rootSampled, ctx)
}

func TestExtractorNonRootSampled(t *testing.T) {
	ctx, err := propagator.Extract(nonRootSampledHeader)
	assert.Nil(t, err)
	assert.EqualValues(t, nonRootSampled, ctx)
}

func TestExtractorNonRootNonSampled(t *testing.T) {
	ctx, err := propagator.Extract(nonRootNonSampledHeader)
	assert.Nil(t, err)
	assert.EqualValues(t, nonRootNonSampled, ctx)
}

func TestInjectorRootSampled(t *testing.T) {
	hdr := opentracing.TextMapCarrier{}
	err := propagator.Inject(rootSampled, hdr)
	assert.Nil(t, err)
	assert.EqualValues(t, rootSampledHeader, hdr)
}

func TestInjectorNonRootSampled(t *testing.T) {
	hdr := opentracing.TextMapCarrier{}
	err := propagator.Inject(nonRootSampled, hdr)
	assert.Nil(t, err)
	assert.EqualValues(t, nonRootSampledHeader, hdr)
}

func TestInjectorNonRootNonSampled(t *testing.T) {
	hdr := opentracing.TextMapCarrier{}
	err := propagator.Inject(nonRootNonSampled, hdr)
	assert.Nil(t, err)
	assert.EqualValues(t, nonRootNonSampledHeader, hdr)
}
