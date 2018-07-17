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
// See the License for the specific language governing permissions and
// limitations under the License.

package tracing

import (
	"context"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

// Snowball is set as Baggage on traces which are used for snowball tracing.
const Snowball = "sb"

// A CallbackRecorder immediately invokes itself on received trace spans.
type CallbackRecorder func(sp basictracer.RawSpan)

// RecordSpan implements basictracer.SpanRecorder.
func (cr CallbackRecorder) RecordSpan(sp basictracer.RawSpan) {
	cr(sp)
}

// NewSnowball returns a span which records directly via the specified
// callback. If the given WireSpan is the zero value, a new trace is created;
// otherwise, the created Span is a child.
func NewSnowball(opName string, callback func(sp basictracer.RawSpan)) opentracing.Span {
	tr := basictracer.New(CallbackRecorder(callback))
	opentracing.SetGlobalTracer(tr)
	sp := tr.StartSpan(opName)
	sp.SetBaggageItem(Snowball, "1")
	return sp
}

// NoopSpan returns a span which discards all operations.
func NoopSpan() opentracing.Span {
	return (opentracing.NoopTracer{}).StartSpan("DefaultSpan")
}

// SpanFromContext returns the span obtained from the context or, if none is found, a new one started through tracer.
func SpanFromContext(ctx context.Context) (sp opentracing.Span) {
	if sp = opentracing.SpanFromContext(ctx); sp == nil {
		return NoopSpan()
	}
	return sp
}

// ChildSpanFromContxt returns a non-nil span. If span can be got from ctx, then returned span is
// a child of such span. Otherwise, returned span is a noop span.
func ChildSpanFromContxt(ctx context.Context, opName string) (sp opentracing.Span) {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		if _, ok := sp.Tracer().(opentracing.NoopTracer); !ok {
			child := opentracing.StartSpan(opName, opentracing.ChildOf(sp.Context()))
			opentracing.ContextWithSpan(ctx, child)
			return child
		}
	}
	return NoopSpan()
}
