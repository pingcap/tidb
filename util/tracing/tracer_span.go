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
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	opentracing "github.com/opentracing/opentracing-go"

	otlog "github.com/opentracing/opentracing-go/log"
	"golang.org/x/net/trace"
)

// spanMeta stores span information that is common to span and spanContext.
type spanMeta struct {
	// A probabilistically unique identifier for a [multi-span] trace.
	TraceID uint64

	// A probabilistically unique identifier for a span.
	SpanID uint64
}

type spanContext struct {
	spanMeta

	shadowTr  *shadowTracer
	shadowCtx opentracing.SpanContext

	// If set, all spans derived from this context are being recorded as a group.
	recordingGroup *spanGroup
	recordingType  RecordingType

	// The span's associated baggage.
	Baggage map[string]string
}

const (
	// TagPrefix is prefixed to all tags that should be output in SHOW TRACE.
	TagPrefix = "tidb."
	// StatTagPrefix is prefixed to all stats output in span tags.
	StatTagPrefix = TagPrefix + "stat."
)

// SpanStats are stats that can be added to a span.
type SpanStats interface {
	proto.Message
	// Stats returns the stats that the object represents as a map from stat name
	// to value to be added to span tags. The keys will be prefixed with
	// StatTagPrefix.
	Stats() map[string]string
}

var _ opentracing.SpanContext = &spanContext{}

// ForeachBaggageItem is part of the opentracing.SpanContext interface.
func (sc *spanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	for k, v := range sc.Baggage {
		if !handler(k, v) {
			break
		}
	}
}

// RecordingType is the type of recording that a span might be performing.
type RecordingType int

const (
	// NoRecording means that the span isn't recording.
	NoRecording RecordingType = iota
	// TiDBRecording means that remote child spans (generally opened through
	// RPCs) are also recorded.
	TiDBRecording
	// SingleNodeRecording means that only spans on the current node are recorded.
	SingleNodeRecording
)

type span struct {
	spanMeta

	parentSpanID uint64

	tracer *Tracer

	// x/net/trace.Trace instance; nil if not tracing to x/net/trace.
	netTr trace.Trace
	// Shadow tracer and span; nil if not using a shadow tracer.
	shadowTr   *shadowTracer
	shadowSpan opentracing.Span

	operation string
	startTime time.Time

	// Atomic flag used to avoid taking the mutex in the hot path.
	recording int32

	mu struct {
		sync.RWMutex
		// duration is initialized to -1 and set on Finish().
		duration time.Duration

		recordingGroup *spanGroup
		recordingType  RecordingType
		recordedLogs   []opentracing.LogRecord
		// tags are only set when recording.
		tags opentracing.Tags

		stats SpanStats

		// The span's associated baggage.
		Baggage map[string]string
	}
}

var _ opentracing.Span = &span{}

func (s *span) isRecording() bool {
	return atomic.LoadInt32(&s.recording) != 0
}

// IsRecording returns true if the span is recording its events.
func IsRecording(s opentracing.Span) bool {
	if _, noop := s.(*noopSpan); noop {
		return false
	}
	return s.(*span).isRecording()
}

func (s *span) enableRecording(group *spanGroup, recType RecordingType) {
	if group == nil {
		panic("no spanGroup")
	}
	s.mu.Lock()
	atomic.StoreInt32(&s.recording, 1)
	s.mu.recordingGroup = group
	s.mu.recordingType = recType
	if recType == TiDBRecording {
		s.setBaggageItemLocked(TiDBQueryLevelTracing, "1")
	}
	// Clear any previously recorded logs.
	s.mu.recordedLogs = nil
	s.mu.Unlock()

	group.addSpan(s)
}

// StartRecording enables recording on the span. Events from this point forward
// are recorded; also, all direct and indirect child spans started from now on
// will be part of the same recording.
//
// Recording is not supported by noop spans; to ensure a real span is always
// created, use the Force option to StartSpan.
//
// If recording was already started on this span (either directly or because a
// parent span is recording), the old recording is lost.
func StartRecording(os opentracing.Span, recType RecordingType) {
	if recType == NoRecording {
		panic("StartRecording called with NoRecording")
	}
	if _, noop := os.(*noopSpan); noop {
		panic("StartRecording called on NoopSpan; use the Force option for StartSpan")
	}
	os.(*span).enableRecording(new(spanGroup), recType)
}

// StopRecording disables recording on this span. Child spans that were created
// since recording was started will continue to record until they finish.
//
// Calling this after StartRecording is not required; the recording will go away
// when all the spans finish.
//
// StopRecording() can be called on a Finish()ed span.
func StopRecording(os opentracing.Span) {
	os.(*span).disableRecording()
}

func (s *span) disableRecording() {
	s.mu.Lock()
	atomic.StoreInt32(&s.recording, 0)
	s.mu.recordingGroup = nil
	// We test the duration as a way to check if the span has been finished. If it
	// has, we don't want to do the call below as it might crash (at least if
	// there's a netTr).
	if (s.mu.duration == -1) && (s.mu.recordingType == TiDBRecording) {
		// Clear the Snowball baggage item, assuming that it was set by
		// enableRecording().
		s.setBaggageItemLocked(TiDBQueryLevelTracing, "")
	}
	s.mu.Unlock()
}

// IsRecordable returns true if {Start,Stop}Recording() can be called on this
// span.
//
// In other words, this tests if the span is our custom type, and not a noopSpan
// or anything else.
func IsRecordable(os opentracing.Span) bool {
	_, customizedSpan := os.(*span)
	return customizedSpan
}

// GetRecording retrieves the current recording, if the span has
// recording enabled. This can be called while spans that are part of the
// record are still open; it can run concurrently with operations on those
// spans.
func GetRecording(os opentracing.Span) []RecordedSpan {
	if _, noop := os.(*noopSpan); noop {
		return nil
	}
	s := os.(*span)
	if !s.isRecording() {
		return nil
	}
	s.mu.Lock()
	group := s.mu.recordingGroup
	s.mu.Unlock()
	if group == nil {
		return nil
	}
	return group.getSpans()
}

// IsBlackHoleSpan returns true if events for this span are just dropped. This
// is the case when tracing is disabled and we're not recording. Tracing clients
// can use this method to figure out if they can short-circuit some
// tracing-related work that would be discarded anyway.
func IsBlackHoleSpan(s opentracing.Span) bool {
	// There are two types of black holes: instances of noopSpan and, when tracing
	// is disabled, real spans that are not recording.
	if _, noop := s.(*noopSpan); noop {
		return true
	}
	sp := s.(*span)
	return !sp.isRecording() && sp.netTr == nil
	// && sp.shadowTr == nil
}

// IsNoopContext returns true if the span context is from a "no-op" span. If
// this is true, any span derived from this context will be a "black hole span".
func IsNoopContext(spanCtx opentracing.SpanContext) bool {
	_, noop := spanCtx.(noopSpanContext)
	return noop
}

// SetSpanStats sets the stats on a span. stats.Stats() will also be added to
// the span tags.
func SetSpanStats(os opentracing.Span, stats SpanStats) {
	s := os.(*span)
	s.mu.Lock()
	s.mu.stats = stats
	for name, value := range stats.Stats() {
		s.setTagInner(StatTagPrefix+name, value, true /* locked */)
	}
	s.mu.Unlock()
}

// Finish is part of the opentracing.Span interface.
func (s *span) Finish() {
	s.FinishWithOptions(opentracing.FinishOptions{})
}

// FinishWithOptions is part of the opentracing.Span interface.
func (s *span) FinishWithOptions(opts opentracing.FinishOptions) {
	finishTime := opts.FinishTime
	if finishTime.IsZero() {
		finishTime = time.Now()
	}
	s.mu.Lock()
	s.mu.duration = finishTime.Sub(s.startTime)
	s.mu.Unlock()
	if s.shadowTr != nil {
		s.shadowSpan.Finish()
	}
	if s.netTr != nil {
		s.netTr.Finish()
	}
}

// Context is part of the opentracing.Span interface.
func (s *span) Context() opentracing.SpanContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	baggageCopy := make(map[string]string, len(s.mu.Baggage))
	for k, v := range s.mu.Baggage {
		baggageCopy[k] = v
	}
	sc := &spanContext{
		spanMeta: s.spanMeta,
		Baggage:  baggageCopy,
	}
	if s.shadowTr != nil {
		sc.shadowTr = s.shadowTr
		sc.shadowCtx = s.shadowSpan.Context()
	}

	if s.isRecording() {
		sc.recordingGroup = s.mu.recordingGroup
		sc.recordingType = s.mu.recordingType
	}
	return sc
}

// SetOperationName is part of the opentracing.Span interface.
func (s *span) SetOperationName(operationName string) opentracing.Span {
	if s.shadowTr != nil {
		s.shadowSpan.SetOperationName(operationName)
	}
	s.operation = operationName
	return s
}

// SetTag is part of the opentracing.Span interface.
func (s *span) SetTag(key string, value interface{}) opentracing.Span {
	return s.setTagInner(key, value, false /* locked */)
}

func (s *span) setTagInner(key string, value interface{}, locked bool) opentracing.Span {
	if s.shadowTr != nil {
		s.shadowSpan.SetTag(key, value)
	}
	if s.netTr != nil {
		s.netTr.LazyPrintf("%s:%v", key, value)
	}
	if s.isRecording() {
		if !locked {
			s.mu.Lock()
		}
		if s.mu.tags == nil {
			s.mu.tags = make(opentracing.Tags)
		}
		s.mu.tags[key] = value
		if !locked {
			s.mu.Unlock()
		}
	}
	return s
}

// LogFields is part of the opentracing.Span interface.
func (s *span) LogFields(fields ...otlog.Field) {
	if s.shadowTr != nil {
		s.shadowSpan.LogFields(fields...)
	}
	if s.netTr != nil {
		// lights step does not support arbitrary event
		if len(fields) == 1 && fields[0].Key() == "event" {
			s.netTr.LazyPrintf("%s", fields[0].Value())
		} else {
			var buf bytes.Buffer
			for i, f := range fields {
				if i > 0 {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%s:%v", f.Key(), f.Value())
			}

			s.netTr.LazyPrintf("%s", buf.String())
		}
	}
	if s.isRecording() {
		s.mu.Lock()
		if len(s.mu.recordedLogs) < maxLogsPerSpan {
			s.mu.recordedLogs = append(s.mu.recordedLogs, opentracing.LogRecord{
				Timestamp: time.Now(),
				Fields:    fields,
			})
		}
		s.mu.Unlock()
	}
}

// LogKV is part of the opentracing.Span interface.
func (s *span) LogKV(alternatingKeyValues ...interface{}) {
	fields, err := otlog.InterleavedKVToFields(alternatingKeyValues...)
	if err != nil {
		s.LogFields(otlog.Error(err), otlog.String("function", "LogKV"))
		return
	}
	s.LogFields(fields...)
}

// SetBaggageItem is part of the opentracing.Span interface.
func (s *span) SetBaggageItem(restrictedKey, value string) opentracing.Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setBaggageItemLocked(restrictedKey, value)
}

func (s *span) setBaggageItemLocked(restrictedKey, value string) opentracing.Span {
	if oldVal, ok := s.mu.Baggage[restrictedKey]; ok && oldVal == value {
		// No-op.
		return s
	}
	if s.mu.Baggage == nil {
		s.mu.Baggage = make(map[string]string)
	}
	s.mu.Baggage[restrictedKey] = value

	if s.shadowTr != nil {
		s.shadowSpan.SetBaggageItem(restrictedKey, value)
	}
	// Also set a tag so it shows up in the Lightstep UI or x/net/trace.
	s.setTagInner(restrictedKey, value, true /* locked */)
	return s
}

// BaggageItem is part of the opentracing.Span interface.
func (s *span) BaggageItem(restrictedKey string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.Baggage[restrictedKey]
}

// Tracer is part of the opentracing.Span interface.
func (s *span) Tracer() opentracing.Tracer {
	return s.tracer
}

// LogEvent is part of the opentracing.Span interface. Deprecated.
func (s *span) LogEvent(event string) {
	s.LogFields(otlog.String("event", event))
}

// LogEventWithPayload is part of the opentracing.Span interface. Deprecated.
func (s *span) LogEventWithPayload(event string, payload interface{}) {
	s.LogFields(otlog.String("event", event), otlog.Object("payload", payload))
}

// Log is part of the opentracing.Span interface. Deprecated.
func (s *span) Log(data opentracing.LogData) {
	panic("unimplemented")
}

// spanGroup keeps track of all the spans that are being recorded as a group (i.e.
// the span for which recording was enabled and all direct or indirect child
// spans since then).
type spanGroup struct {
	sync.Mutex
	// spans keeps track of all the local spans. A span is inserted in this slice
	// as soon as it is opened; the first element is the span passed to
	// StartRecording().
	spans []*span
}

func (ss *spanGroup) addSpan(s *span) {
	ss.Lock()
	ss.spans = append(ss.spans, s)
	ss.Unlock()
}

// RecordedSpan is just an alias of span which can be used outside the current package.
type RecordedSpan struct {
	// ID of the trace; spans that are part of the same hierarchy share
	// the same trace ID.
	TraceID uint64
	// ID of the span.
	SpanID uint64
	// Span ID of the parent span.
	ParentSpanID uint64
	// Operation name.
	Operation string
	// Baggage items get passed from parent to child spans (even through gRPC).
	// Notably, snowball tracing uses a special `sb` baggage item.
	Baggage map[string]string
	// Tags associated with the span.
	Tags map[string]string
	// Time when the span was started.
	StartTime time.Time
	// Duration in nanoseconds; 0 if the span is not finished.
	Duration time.Duration
	// Events logged in the span.
	Logs []opentracing.LogRecord
}

// getSpans returns all the local and remote spans accumulated in this group.
// The first result is the first local span - i.e. the span originally passed to
// StartRecording().
func (ss *spanGroup) getSpans() []RecordedSpan {
	ss.Lock()
	spans := ss.spans
	ss.Unlock()

	result := make([]RecordedSpan, 0, len(spans))
	for _, s := range spans {
		s.mu.Lock()
		rs := RecordedSpan{
			TraceID:      s.TraceID,
			SpanID:       s.SpanID,
			ParentSpanID: s.parentSpanID,
			Operation:    s.operation,
			StartTime:    s.startTime,
			Duration:     s.mu.duration,
		}
		switch rs.Duration {
		case -1:
			// -1 indicates an unfinished span.
			rs.Duration = 0
		case 0:
			// 0 is a special value for unfinished spans. Change to 1ns.
			rs.Duration = time.Nanosecond
		}

		if len(s.mu.Baggage) > 0 {
			rs.Baggage = make(map[string]string)
			for k, v := range s.mu.Baggage {
				rs.Baggage[k] = v
			}
		}
		if len(s.mu.tags) > 0 {
			rs.Tags = make(map[string]string)
			for k, v := range s.mu.tags {
				// We encode the tag values as strings.
				rs.Tags[k] = fmt.Sprint(v)
			}
		}
		rs.Logs = s.mu.recordedLogs
		s.mu.Unlock()
		result = append(result, rs)
	}
	return result
}

type noopSpanContext struct{}

var _ opentracing.SpanContext = noopSpanContext{}

func (n noopSpanContext) ForeachBaggageItem(handler func(k, v string) bool) {}

type noopSpan struct {
	tracer *Tracer
}

var _ opentracing.Span = &noopSpan{}

func (n *noopSpan) Context() opentracing.SpanContext                       { return noopSpanContext{} }
func (n *noopSpan) BaggageItem(key string) string                          { return "" }
func (n *noopSpan) SetTag(key string, value interface{}) opentracing.Span  { return n }
func (n *noopSpan) Finish()                                                {}
func (n *noopSpan) FinishWithOptions(opts opentracing.FinishOptions)       {}
func (n *noopSpan) SetOperationName(operationName string) opentracing.Span { return n }
func (n *noopSpan) Tracer() opentracing.Tracer                             { return n.tracer }
func (n *noopSpan) LogFields(fields ...otlog.Field)                        {}
func (n *noopSpan) LogKV(keyVals ...interface{})                           {}
func (n *noopSpan) LogEvent(event string)                                  {}
func (n *noopSpan) LogEventWithPayload(event string, payload interface{})  {}
func (n *noopSpan) Log(data opentracing.LogData)                           {}

func (n *noopSpan) SetBaggageItem(key, val string) opentracing.Span {
	if key == TiDBQueryLevelTracing {
		panic("attempting to set query-level tracing on a noop span; use the Force option to StartSpan")
	}
	return n
}
