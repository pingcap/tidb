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
	"github.com/pingcap/tidb/pkg/parser/model"
)

// TiDBTrace is set as Baggage on traces which are used for tidb tracing.
const TiDBTrace = "tr"

type sqlTracingCtxKeyType struct{}

var sqlTracingCtxKey = sqlTracingCtxKeyType{}

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

// StartRegionWithNewRootSpan return Region together with the context.
// It create and start a new span by globalTracer and store it into `ctx`.
func StartRegionWithNewRootSpan(ctx context.Context, regionType string) (Region, context.Context) {
	span := opentracing.GlobalTracer().StartSpan(regionType)
	r := Region{
		Region: trace.StartRegion(ctx, regionType),
		Span:   span,
	}
	ctx = opentracing.ContextWithSpan(ctx, span)
	return r, ctx
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

// TraceInfoFromContext returns the `model.TraceInfo` in context
func TraceInfoFromContext(ctx context.Context) *model.TraceInfo {
	val := ctx.Value(sqlTracingCtxKey)
	if info, ok := val.(*model.TraceInfo); ok {
		return info
	}
	return nil
}

// ContextWithTraceInfo creates a new `model.TraceInfo` for context
func ContextWithTraceInfo(ctx context.Context, info *model.TraceInfo) context.Context {
	if info == nil {
		return ctx
	}
	return context.WithValue(ctx, sqlTracingCtxKey, info)
}
