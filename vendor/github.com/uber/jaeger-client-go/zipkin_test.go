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

	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
)

func TestZipkinPropagator(t *testing.T) {
	tracer, tCloser := NewTracer("x", NewConstSampler(true), NewNullReporter(), TracerOptions.ZipkinSharedRPCSpan(true))
	defer tCloser.Close()

	carrier := &TestZipkinSpan{}
	sp := tracer.StartSpan("y")

	// Note: we intentionally use string as format, as that's what TChannel would need to do
	if err := tracer.Inject(sp.Context(), "zipkin-span-format", carrier); err != nil {
		t.Fatalf("Inject failed: %+v", err)
	}
	sp1 := sp.(*Span)
	assert.Equal(t, sp1.context.traceID, TraceID{Low: carrier.traceID})
	assert.Equal(t, sp1.context.spanID, SpanID(carrier.spanID))
	assert.Equal(t, sp1.context.parentID, SpanID(carrier.parentID))
	assert.Equal(t, sp1.context.flags, carrier.flags)

	sp2ctx, err := tracer.Extract("zipkin-span-format", carrier)
	if err != nil {
		t.Fatalf("Extract failed: %+v", err)
	}
	sp2 := tracer.StartSpan("x", ext.RPCServerOption(sp2ctx))
	sp3 := sp2.(*Span)
	assert.Equal(t, sp1.context.traceID, sp3.context.traceID)
	assert.Equal(t, sp1.context.spanID, sp3.context.spanID)
	assert.Equal(t, sp1.context.parentID, sp3.context.parentID)
	assert.Equal(t, sp1.context.flags, sp3.context.flags)
}

// TestZipkinSpan is a mock-up of TChannel's internal Span struct
type TestZipkinSpan struct {
	traceID  uint64
	parentID uint64
	spanID   uint64
	flags    byte
}

func (s TestZipkinSpan) TraceID() uint64              { return s.traceID }
func (s TestZipkinSpan) ParentID() uint64             { return s.parentID }
func (s TestZipkinSpan) SpanID() uint64               { return s.spanID }
func (s TestZipkinSpan) Flags() byte                  { return s.flags }
func (s *TestZipkinSpan) SetTraceID(traceID uint64)   { s.traceID = traceID }
func (s *TestZipkinSpan) SetSpanID(spanID uint64)     { s.spanID = spanID }
func (s *TestZipkinSpan) SetParentID(parentID uint64) { s.parentID = parentID }
func (s *TestZipkinSpan) SetFlags(flags byte)         { s.flags = flags }
