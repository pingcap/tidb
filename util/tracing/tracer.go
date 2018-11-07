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
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
)

const maxLogsPerSpan = 1000

// These constants are used to form keys to represent tracing context
// information in carriers supporting opentracing.HTTPHeaders format.
const (
	prefixTracerState = "tidb-tracer-"
	prefixBaggage     = "tidb-baggage-"
	// prefixShadow is prepended to the keys for the context of the shadow tracer
	// (e.g. LightStep).
	prefixShadow = "tidb-shadow-"

	fieldNameTraceID = prefixTracerState + "traceid"
	fieldNameSpanID  = prefixTracerState + "spanid"
	// fieldNameShadow is the name of the shadow tracer.
	fieldNameShadowType = prefixTracerState + "shadowtype"

	TiDBQueryLevelTracing = "tidb_query_level_trace"
)

// Tracer is our own custom implementation of opentracing.Tracer. It supports:
//
//  * forwarding events to x/net/trace instances
//
//  * recording traces. It has two types: query-level tracing and session-level tracing.
//
//  * lightstep traces. This is implemented by maintaining a "shadow" lightstep
//    span inside each of our spans.
//
// Even when tracing is disabled, we still use this Tracer (with x/net/trace and
// lightstep disabled) because of its recording capability (query-level
// tracing needs to work in all cases).
//
// Tracer is currently stateless so we could have a single instance and it is currently careated
// at final stage of creating a tidb-server.
type Tracer struct {
	mu sync.RWMutex
	// Pre-allocated noopSpan, used to avoid creating spans when we are not using
	// lightstep and we are not recording.
	noopSpan noopSpan

	// If forceRealSpans is true, this Tracer will always create real spans (never
	// noopSpans), regardless of the recording or lightstep configuration. Used
	// by tests for situations when they need to indirectly create spans and don't
	// have the option of passing the Recordable option to their constructor.
	forceRealSpans bool

	// Pointer to shadowTracer, if using one.
	shadowTracer *shadowTracer
}

var _ opentracing.Tracer = &Tracer{}

// NewTracer creates a Tracer. Tracer create by this method is running with minimal overhead
// and collects essentially nothing.
func NewTracer() *Tracer {
	t := &Tracer{}
	t.noopSpan.tracer = t
	return t
}

// Close cleans up any resources associated with a Tracer.
func (t *Tracer) Close() {
	// Clean up any shadow tracer.
	t.setShadowTracer(nil, nil)
}

// SetForceRealSpans sets forceRealSpans option to v and returns the previous
// value.
func (t *Tracer) SetForceRealSpans(v bool) bool {
	prevVal := t.forceRealSpans
	t.forceRealSpans = v
	return prevVal
}

func (t *Tracer) setShadowTracer(manager shadowTracerManager, tr opentracing.Tracer) {
	var shadow *shadowTracer
	if manager != nil {
		shadow = &shadowTracer{
			Tracer:  tr,
			manager: manager,
		}
	}
	t.mu.Lock()
	old := t.shadowTracer
	old.Close()
	t.shadowTracer = shadow
	t.mu.Unlock()
}

func (t *Tracer) getShadowTracer() *shadowTracer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.shadowTracer
}

type recordableOption struct{}

// Recordable is a StartSpanOption that forces creation of a real span.
//
// When tracing is disabled all spans are noopSpans; these spans aren't
// capable of recording, so this option should be passed to StartSpan if the
// caller wants to be able to call StartRecording on the resulting span.
var Recordable opentracing.StartSpanOption = recordableOption{}

func (recordableOption) Apply(*opentracing.StartSpanOptions) {}

// StartSpan is part of the opentracing.Tracer interface.
func (t *Tracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	// Fast paths to avoid the allocation of StartSpanOptions below when tracing
	// is disabled: if we have no options or a single SpanReference (the common
	// case) with a noop context, return a noop span now.
	if len(opts) == 1 {
		if o, ok := opts[0].(opentracing.SpanReference); ok {
			if IsNoopContext(o.ReferencedContext) {
				return &t.noopSpan
			}
		}
	}

	shadowTr := t.getShadowTracer()

	if len(opts) == 0 && shadowTr == nil && !t.forceRealSpans {
		return &t.noopSpan
	}

	var sso opentracing.StartSpanOptions
	var recordable bool
	for _, o := range opts {
		o.Apply(&sso)
		if _, ok := o.(recordableOption); ok {
			recordable = true
		}
	}

	var hasParent bool
	var parentType opentracing.SpanReferenceType
	var parentCtx *spanContext
	var recordingGroup *spanGroup
	var recordingType RecordingType

	for _, r := range sso.References {
		if r.Type != opentracing.ChildOfRef && r.Type != opentracing.FollowsFromRef {
			continue
		}
		if r.ReferencedContext == nil {
			continue
		}
		if IsNoopContext(r.ReferencedContext) {
			continue
		}
		hasParent = true
		parentType = r.Type
		parentCtx = r.ReferencedContext.(*spanContext)
		if parentCtx.recordingGroup != nil {
			recordingGroup = parentCtx.recordingGroup
			recordingType = parentCtx.recordingType
		} else if parentCtx.Baggage[TiDBQueryLevelTracing] != "" {
			// Automatically enable recording if we have the query-level tracing baggage item.
			recordingGroup = new(spanGroup)
			recordingType = TiDBRecording
		}
		break
	}
	if hasParent {
		// We use the parent's shadow tracer, to avoid inconsistency inside a
		// trace when the shadow tracer changes.
		shadowTr = parentCtx.shadowTr
	}

	// If tracing is disabled, the Recordable option wasn't passed, and we're not
	// part of a recording or query-level trace, avoid overhead and return a noop
	// span.
	if !recordable && recordingGroup == nil && shadowTr == nil && !t.forceRealSpans {
		return &t.noopSpan
	}

	s := &span{
		tracer:    t,
		operation: operationName,
		startTime: sso.StartTime,
	}
	if s.startTime.IsZero() {
		s.startTime = time.Now()
	}
	s.mu.duration = -1

	if !hasParent {
		// No parent Span; allocate new trace id.
		s.TraceID = uint64(rand.Int63())
	} else {
		s.TraceID = parentCtx.TraceID
	}
	s.SpanID = uint64(rand.Int63())

	if shadowTr != nil {
		var parentShadowCtx opentracing.SpanContext
		if hasParent {
			parentShadowCtx = parentCtx.shadowCtx
		}
		linkShadowSpan(s, shadowTr, parentShadowCtx, parentType)
	}

	// Start recording if necessary.
	if recordingGroup != nil {
		s.enableRecording(recordingGroup, recordingType)
	}

	if hasParent {
		s.parentSpanID = parentCtx.SpanID
		// Copy baggage from parent.
		if l := len(parentCtx.Baggage); l > 0 {
			s.mu.Baggage = make(map[string]string, l)
			for k, v := range parentCtx.Baggage {
				s.mu.Baggage[k] = v
			}
		}
	}

	for k, v := range sso.Tags {
		s.SetTag(k, v)
	}

	// Copy baggage items to tags so they show up in the shadow tracer UI,
	// x/net/trace, or recordings.
	for k, v := range s.mu.Baggage {
		s.SetTag(k, v)
	}

	return s
}

type textMapWriterFn func(key, val string)

var _ opentracing.TextMapWriter = textMapWriterFn(nil)

// Set is part of the opentracing.TextMapWriter interface.
func (fn textMapWriterFn) Set(key, val string) {
	fn(key, val)
}

// Inject is part of the opentracing.Tracer interface.
func (t *Tracer) Inject(
	osc opentracing.SpanContext, format interface{}, carrier interface{},
) error {
	if IsNoopContext(osc) {
		// Fast path when tracing is disabled. Extract will accept an empty map as a
		// noop context.
		return nil
	}

	// We only support the HTTPHeaders/TextMap format.
	if format != opentracing.HTTPHeaders && format != opentracing.TextMap {
		return opentracing.ErrUnsupportedFormat
	}

	mapWriter, ok := carrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}

	sc, ok := osc.(*spanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}

	mapWriter.Set(fieldNameTraceID, strconv.FormatUint(sc.TraceID, 16))
	mapWriter.Set(fieldNameSpanID, strconv.FormatUint(sc.SpanID, 16))

	for k, v := range sc.Baggage {
		mapWriter.Set(prefixBaggage+k, v)
	}

	if sc.shadowTr != nil {
		mapWriter.Set(fieldNameShadowType, sc.shadowTr.Typ())
		// Encapsulate the shadow text map, prepending a prefix to the keys.
		if err := sc.shadowTr.Inject(sc.shadowCtx, format, textMapWriterFn(func(key, val string) {
			mapWriter.Set(prefixShadow+key, val)
		})); err != nil {
			return err
		}
	}

	return nil
}

type textMapReaderFn func(handler func(key, val string) error) error

var _ opentracing.TextMapReader = textMapReaderFn(nil)

// ForeachKey is part of the opentracing.TextMapReader interface.
func (fn textMapReaderFn) ForeachKey(handler func(key, val string) error) error {
	return fn(handler)
}

// Extract is part of the opentracing.Tracer interface.
// It always returns a valid context, even in error cases (this is assumed by the
// grpc-opentracing interceptor).
func (t *Tracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	// We only support the HTTPHeaders/TextMap format.
	if format != opentracing.HTTPHeaders && format != opentracing.TextMap {
		return noopSpanContext{}, opentracing.ErrUnsupportedFormat
	}

	mapReader, ok := carrier.(opentracing.TextMapReader)
	if !ok {
		return noopSpanContext{}, opentracing.ErrInvalidCarrier
	}

	var sc spanContext
	var shadowType string
	var shadowCarrier opentracing.TextMapCarrier

	err := mapReader.ForeachKey(func(k, v string) error {
		switch k = strings.ToLower(k); k {
		case fieldNameTraceID:
			var err error
			sc.TraceID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return opentracing.ErrSpanContextCorrupted
			}
		case fieldNameSpanID:
			var err error
			sc.SpanID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return opentracing.ErrSpanContextCorrupted
			}
		case fieldNameShadowType:
			shadowType = v
		default:
			if strings.HasPrefix(k, prefixBaggage) {
				if sc.Baggage == nil {
					sc.Baggage = make(map[string]string)
				}
				sc.Baggage[strings.TrimPrefix(k, prefixBaggage)] = v
			} else if strings.HasPrefix(k, prefixShadow) {
				if shadowCarrier == nil {
					shadowCarrier = make(opentracing.TextMapCarrier)
				}
				// We build a shadow textmap with the original shadow keys.
				shadowCarrier.Set(strings.TrimPrefix(k, prefixShadow), v)
			}
		}
		return nil
	})
	if err != nil {
		return noopSpanContext{}, err
	}
	if sc.TraceID == 0 && sc.SpanID == 0 {
		return noopSpanContext{}, nil
	}

	if shadowType != "" {
		// Using a shadow tracer only works if all hosts use the same shadow tracer.
		// If that's not the case, ignore the shadow context.
		if shadowTr := t.getShadowTracer(); shadowTr != nil &&
			strings.ToLower(shadowType) == strings.ToLower(shadowTr.Typ()) {
			sc.shadowTr = shadowTr
			// Extract the shadow context using the un-encapsulated textmap.
			sc.shadowCtx, err = shadowTr.Extract(format, shadowCarrier)
			if err != nil {
				return noopSpanContext{}, err
			}
		}
	}

	return &sc, nil
}
