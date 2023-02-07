// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracing

import (
	"context"
	"runtime/trace"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

// TiDBTrace is set as Baggage on traces which are used for tidb tracing.
const TiDBTrace = "tr"

// A CallbackRecorder immediately invokes itself on received trace spans.
type CallbackRecorder func(sp basictracer.RawSpan)

// RecordSpan implements basictracer.SpanRecorder.
func (cr CallbackRecorder) RecordSpan(sp basictracer.RawSpan) {
	cr(sp)
}

// NewRecordedTrace returns a Span which records directly via the specified
// callback.
func NewRecordedTrace(opName string, callback func(sp basictracer.RawSpan)) opentracing.Span {
	tr := basictracer.New(CallbackRecorder(callback))
	opentracing.SetGlobalTracer(tr)
	sp := tr.StartSpan(opName)
	sp.SetBaggageItem(TiDBTrace, "1")
	return sp
}

// noopSpan returns a Span which discards all operations.
func noopSpan() opentracing.Span {
	return (opentracing.NoopTracer{}).StartSpan("DefaultSpan")
}

// SpanFromContext returns the span obtained from the context or, if none is found, a new one started through tracer.
func SpanFromContext(ctx context.Context) (sp opentracing.Span) {
	if sp = opentracing.SpanFromContext(ctx); sp == nil {
		return noopSpan()
	}
	return sp
}

// ChildSpanFromContxt return a non-nil span. If span can be got from ctx, then returned span is
// a child of such span. Otherwise, returned span is a noop span.
func ChildSpanFromContxt(ctx context.Context, opName string) (opentracing.Span, context.Context) {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		if _, ok := sp.Tracer().(opentracing.NoopTracer); !ok {
			child := opentracing.StartSpan(opName, opentracing.ChildOf(sp.Context()))
			return child, opentracing.ContextWithSpan(ctx, child)
		}
	}
	return noopSpan(), ctx
}

// StartRegion provides better API, integrating both opentracing and runtime.trace facilities into one.
// Recommended usage is
//
//	defer tracing.StartRegion(ctx, "myTracedRegion").End()
func StartRegion(ctx context.Context, regionType string) Region {
	r := trace.StartRegion(ctx, regionType)
	var span1 opentracing.Span
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 = span.Tracer().StartSpan(regionType, opentracing.ChildOf(span.Context()))
	}
	return Region{
		Region: r,
		Span:   span1,
	}
}

// StartRegionEx returns Region together with the context.
// Recommended usage is
//
//		r, ctx := tracing.StartRegionEx(ctx, "myTracedRegion")
//	     defer r.End()
func StartRegionEx(ctx context.Context, regionType string) (Region, context.Context) {
	r := StartRegion(ctx, regionType)
	if r.Span != nil {
		ctx = opentracing.ContextWithSpan(ctx, r.Span)
	}
	return r, ctx
}

// Region is a region of code whose execution time interval is traced.
type Region struct {
	*trace.Region
	opentracing.Span
}

// End marks the end of the traced code region.
func (r Region) End() {
	if r.Span != nil {
		r.Span.Finish()
	}
	r.Region.End()
}
