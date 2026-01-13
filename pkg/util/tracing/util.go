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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
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

// TraceInfo is the information for trace.
type TraceInfo struct {
	// SessionAlias is the alias of session
	SessionAlias string `json:"session_alias"`
	// TraceID is the trace id for every SQL statement
	TraceID []byte `json:"trace_id"`
	// ConnectionID is the id of the connection
	ConnectionID uint64 `json:"connection_id"`
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

// TraceBuf records trace events.
type TraceBuf interface {
	Record(ctx context.Context, event Event)
}

type traceBufKeyType struct{}

var traceBufKey traceBufKeyType = struct{}{}

// GetTraceBuf returns the TraceBuf from the context.
func GetTraceBuf(ctx context.Context) any {
	return ctx.Value(traceBufKey)
}

// WithTraceBuf returns a context with TraceBuf.
func WithTraceBuf(ctx context.Context, val TraceBuf) context.Context {
	return context.WithValue(ctx, traceBufKey, val)
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
	ret := Region{
		Region: r,
		Span:   span1,
	}
	if IsEnabled(General) {
		if tmp := GetTraceBuf(ctx); tmp != nil {
			traceBuf := tmp.(TraceBuf)
			event := Event{
				Category:  General,
				Name:      regionType,
				Phase:     PhaseBegin,
				Timestamp: time.Now(),
			}
			traceBuf.Record(ctx, event)
			ret.span.event = &event
			ret.span.traceBuf = traceBuf
			ret.span.ctx = ctx
		}
	}
	return ret
}

// enabledCategories stores the currently enabled category mask.
var enabledCategories atomic.Uint64

// Enable enables trace events for the specified categories.
func Enable(categories TraceCategory) {
	for {
		current := enabledCategories.Load()
		next := current | uint64(categories)
		if enabledCategories.CompareAndSwap(current, next) {
			return
		}
	}
}

// Disable disables trace events for the specified categories.
func Disable(categories TraceCategory) {
	for {
		current := enabledCategories.Load()
		next := current &^ uint64(categories)
		if enabledCategories.CompareAndSwap(current, next) {
			return
		}
	}
}

// SetCategories sets the enabled categories to exactly the specified value.
func SetCategories(categories TraceCategory) {
	enabledCategories.Store(uint64(categories))
}

// GetEnabledCategories returns the currently enabled categories.
func GetEnabledCategories() TraceCategory {
	return TraceCategory(enabledCategories.Load())
}

// IsEnabled returns whether the specified category is enabled.
// It delegates to traceevent.IsEnabled
var IsEnabled func(category TraceCategory) bool

// TraceCategory represents different trace event categories.
type TraceCategory uint64

const (
	// TxnLifecycle traces transaction begin/commit/rollback events.
	TxnLifecycle TraceCategory = 1 << iota
	// Txn2PC traces two-phase commit prewrite and commit phases.
	Txn2PC
	// TxnLockResolve traces lock resolution and conflict handling.
	TxnLockResolve
	// StmtLifecycle traces statement start/finish events.
	StmtLifecycle
	// StmtPlan traces statement plan digest and optimization.
	StmtPlan
	// KvRequest traces client-go kv request and responses
	KvRequest
	// UnknownClient is the fallback category for unmapped client-go trace events.
	// Used when client-go emits events with categories not yet mapped in adapter.go.
	// This provides forward compatibility if client-go adds new categories.
	UnknownClient
	// General is used by tracing API
	General
	// DDLJob traces DDL job events.
	DDLJob
	// DevDebug traces development/debugging events.
	DevDebug
	// TiKVRequest maps to client-go's FlagTiKVCategoryRequest.
	// Controls request-level tracing in TiKV.
	TiKVRequest
	// TiKVWriteDetails maps to client-go's FlagTiKVCategoryWriteDetails.
	// Controls detailed write operation tracing in TiKV.
	TiKVWriteDetails
	// TiKVReadDetails maps to client-go's FlagTiKVCategoryReadDetails.
	// Controls detailed read operation tracing in TiKV.
	TiKVReadDetails
	// RegionCache traces region cache events.
	RegionCache

	traceCategorySentinel
)

// AllCategories can be used to enable every known trace category.
const AllCategories = traceCategorySentinel - 1

const defaultEnabledCategories = 0

func init() {
	enabledCategories.Store(uint64(defaultEnabledCategories))
}

// String returns the string representation of a TraceCategory.
func (c TraceCategory) String() string {
	return getCategoryName(c)
}

// ParseTraceCategory parses a trace category string representation and returns the corresponding TraceCategory.
func ParseTraceCategory(category string) TraceCategory {
	for i := 0; (1 << i) < traceCategorySentinel; i++ {
		if getCategoryName(1<<i) == category {
			return TraceCategory(1 << i)
		}
	}
	return TraceCategory(0) // invalid
}

// getCategoryName returns the string name for a category.
func getCategoryName(category TraceCategory) string {
	switch category {
	case TxnLifecycle:
		return "txn_lifecycle"
	case Txn2PC:
		return "txn_2pc"
	case TxnLockResolve:
		return "txn_lock_resolve"
	case StmtLifecycle:
		return "stmt_lifecycle"
	case StmtPlan:
		return "stmt_plan"
	case KvRequest:
		return "kv_request"
	case UnknownClient:
		return "unknown_client"
	case General:
		return "general"
	case DDLJob:
		return "ddl_job"
	case DevDebug:
		return "dev_debug"
	case TiKVRequest:
		return "tikv_request"
	case TiKVWriteDetails:
		return "tikv_write_details"
	case TiKVReadDetails:
		return "tikv_read_details"
	case RegionCache:
		return "region_cache"
	default:
		return "unknown(" + strconv.FormatUint(uint64(category), 10) + ")"
	}
}

// Constants used in event fields.
// See https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
// for more details.

// Phase represents the phase of an event.
type Phase string

// The value definition for Phase.
const (
	PhaseBegin      Phase = "B"
	PhaseEnd        Phase = "E"
	PhaseAsyncBegin Phase = "b"
	PhaseAsyncEnd   Phase = "e"
	PhaseFlowBegin  Phase = "s"
	PhaseFlowEnd    Phase = "f"
	PhaseInstant    Phase = "i"
)

// Event represents a traced event.
// INVARIANT: Event.Fields must be treated as immutable once created, as the
// underlying array may be shared across multiple goroutines (e.g., flight
// recorder, log sink, context-specific sinks). Modifications to Fields must
// allocate a new slice to avoid data races.
type Event struct {
	Timestamp time.Time
	Name      string
	Phase
	TraceID  []byte
	Fields   []zap.Field
	Category TraceCategory
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
	span struct {
		event    *Event
		traceBuf TraceBuf
		ctx      context.Context
	}
}

// End marks the end of the traced code region.
func (r Region) End() {
	if r.Span != nil {
		r.Span.Finish()
	}
	r.Region.End()
	if r.span.event != nil {
		r.span.event.Phase = PhaseEnd
		r.span.event.Timestamp = time.Now()
		r.span.traceBuf.Record(r.span.ctx, *r.span.event)
	}
}

// TraceInfoFromContext returns the `model.TraceInfo` in context
func TraceInfoFromContext(ctx context.Context) *TraceInfo {
	val := ctx.Value(sqlTracingCtxKey)
	if info, ok := val.(*TraceInfo); ok {
		return info
	}
	return nil
}

// ContextWithTraceInfo creates a new `model.TraceInfo` for context
func ContextWithTraceInfo(ctx context.Context, info *TraceInfo) context.Context {
	if info == nil {
		return ctx
	}
	return context.WithValue(ctx, sqlTracingCtxKey, info)
}
