// Copyright 2025 PingCAP, Inc.
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

package traceevent

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// TraceCategory represents different trace event categories.
type TraceCategory uint64

const (
	// CoprRegionCache traces region cache and coprocessor region info flow.
	CoprRegionCache TraceCategory = 1 << iota
	traceCategorySentinel
)

// AllCategories can be used to enable every known trace category.
const AllCategories = traceCategorySentinel - 1

const (
	// ModeOff disables log emission (flight recorder still captures allowed categories).
	ModeOff = "off"
	// ModeLogging enables log emission for allowed categories.
	ModeLogging = "logging"
)

// enabledCategories stores the currently enabled category mask and loggingEnabled controls the log sink.
var (
	enabledCategories atomic.Uint64
	loggingEnabled    atomic.Bool
)

// DefaultFlightRecorderCapacity controls the number of events retained in the in-memory recorder.
const DefaultFlightRecorderCapacity = 1024

// eventSink stores the global sink used to record events.
type sinkHolder struct {
	sink Sink
}

var eventSink atomic.Value // of type sinkHolder

// flightRecorder keeps a rolling buffer with recent events for post-mortem analysis.
var flightRecorder = NewRingBufferSink(DefaultFlightRecorderCapacity)

// init sets up the default sink configuration.
func init() {
	defaultSink := &LogSink{}
	eventSink.Store(sinkHolder{sink: defaultSink})
	enabledCategories.Store(uint64(AllCategories))
	loggingEnabled.Store(false)
}

// Enable enables trace events for the specified categories.
// Multiple categories can be combined with bitwise OR.
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

// NormalizeMode converts a user-supplied tracing mode string into its canonical representation.
func NormalizeMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case ModeOff:
		return ModeOff, nil
	case ModeLogging:
		return ModeLogging, nil
	default:
		return "", fmt.Errorf("unsupported trace event mode %q", mode)
	}
}

// SetMode applies the requested tracing mode and returns the canonical value.
func SetMode(mode string) (string, error) {
	normalized, err := NormalizeMode(mode)
	if err != nil {
		return "", err
	}
	switch normalized {
	case ModeOff:
		loggingEnabled.Store(false)
	case ModeLogging:
		loggingEnabled.Store(true)
	}
	return normalized, nil
}

// CurrentMode reports the canonical tracing mode string.
func CurrentMode() string {
	if loggingEnabled.Load() {
		return ModeLogging
	}
	return ModeOff
}

// GetEnabledCategories returns the currently enabled categories.
func GetEnabledCategories() TraceCategory {
	return TraceCategory(enabledCategories.Load())
}

// IsEnabled returns whether the specified category is enabled.
// This function is inline-friendly for hot paths.
// Trace events only work for next-gen kernel.
func IsEnabled(category TraceCategory) bool {
	// Fast path: check kernel type first
	if kerneltype.IsClassic() {
		return false
	}
	return enabledCategories.Load()&uint64(category) != 0
}

// Event captures the raw information describing a trace event. This structure
// is intentionally generic so that it can later be transformed into the Trace
// Event Format (TEF) once the full design is finalized.
type Event struct {
	Category  TraceCategory
	Name      string
	Timestamp time.Time
	TraceID   string
	Fields    []zap.Field
}

// Sink records trace events.
type Sink interface {
	Record(ctx context.Context, event Event)
}

// SetSink replaces the global sink. Passing nil restores the default sink.
func SetSink(s Sink) {
	if s == nil {
		s = &LogSink{}
	}
	eventSink.Store(sinkHolder{sink: s})
}

// CurrentSink returns the sink currently used for trace events.
func CurrentSink() Sink {
	value := eventSink.Load()
	if value == nil {
		return nil
	}
	return value.(sinkHolder).sink
}

// FlightRecorder returns the always-on in-memory recorder.
func FlightRecorder() *RingBufferSink {
	return flightRecorder
}

// TraceEvent records a trace event if the category is enabled.
// The caller is responsible for applying any necessary redaction to the supplied fields.
//
// Usage:
//
//	traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "region split detected",
//		zap.Uint64("regionID", regionID),
//		zap.String("key", formatKey(key)))
func TraceEvent(ctx context.Context, category TraceCategory, name string, fields ...zap.Field) {
	if !IsEnabled(category) {
		return
	}

	event := Event{
		Category:  category,
		Name:      name,
		Timestamp: time.Now(),
		TraceID:   extractTraceID(ctx), // TODO(trace-id): populate once tracing ID is available.
		Fields:    copyFields(fields),
	}

	// Flight recorder is always on to provide post-mortem visibility even when logging is disabled.
	if recorder := FlightRecorder(); recorder != nil {
		recorder.Record(ctx, event)
	}

	if sink := CurrentSink(); sink != nil {
		sink.Record(ctx, event)
	}
}

// extractTraceID returns the trace identifier encoded in ctx if present.
func extractTraceID(_ context.Context) string {
	// TODO(trace-id): populate once tracing context propagation is implemented.
	return ""
}

// LogSink serializes trace events to the global zap logger. The output structure
// stays simple for now, but the Event data contains enough information to build
// a Trace Event Format record when the format is finalized.
type LogSink struct{}

// Record implements the Sink interface.
func (*LogSink) Record(ctx context.Context, event Event) {
	if !loggingEnabled.Load() {
		return
	}
	baseFields := make([]zap.Field, 0, len(event.Fields)+3)
	baseFields = append(baseFields, zap.String("category", getCategoryName(event.Category)))
	baseFields = append(baseFields, zap.Time("event_ts", event.Timestamp))
	if event.TraceID != "" {
		baseFields = append(baseFields, zap.String("trace_id", event.TraceID))
	}
	baseFields = append(baseFields, event.Fields...)
	logutil.Logger(ctx).Info("[trace-event] "+event.Name, baseFields...)
}

func copyFields(fields []zap.Field) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	out := make([]zap.Field, len(fields))
	copy(out, fields)
	return out
}

// MultiSink distributes events to multiple sinks.
type MultiSink struct {
	sinks []Sink
}

// NewMultiSink constructs a MultiSink. Nil sinks are ignored.
func NewMultiSink(sinks ...Sink) *MultiSink {
	filtered := make([]Sink, 0, len(sinks))
	for _, s := range sinks {
		if s != nil {
			filtered = append(filtered, s)
		}
	}
	return &MultiSink{sinks: filtered}
}

// Record implements the Sink interface.
func (s *MultiSink) Record(ctx context.Context, event Event) {
	for _, sink := range s.sinks {
		sink.Record(ctx, event)
	}
}

// RingBufferSink buffers the most recent events in a ring.
type RingBufferSink struct {
	mu   sync.Mutex
	buf  []Event
	next int
	cap  int
}

// NewRingBufferSink creates a ring buffer with the specified capacity.
func NewRingBufferSink(capacity int) *RingBufferSink {
	if capacity <= 0 {
		capacity = 1
	}
	return &RingBufferSink{
		buf: make([]Event, 0, capacity),
		cap: capacity,
	}
}

// Record implements the Sink interface.
func (r *RingBufferSink) Record(_ context.Context, event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	eventCopy := event
	eventCopy.Fields = copyFields(event.Fields)

	if len(r.buf) < r.cap {
		r.buf = append(r.buf, eventCopy)
		if len(r.buf) == r.cap {
			r.next = 0
		}
		return
	}

	r.buf[r.next] = eventCopy
	r.next = (r.next + 1) % r.cap
}

// Reset clears all buffered events.
func (r *RingBufferSink) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = r.buf[:0]
	r.next = 0
}

// Snapshot returns buffered events ordered from oldest to newest.
func (r *RingBufferSink) Snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.buf) == 0 {
		return nil
	}

	result := make([]Event, len(r.buf))
	if len(r.buf) < r.cap {
		for i := range r.buf {
			result[i] = cloneEvent(r.buf[i])
		}
		return result
	}

	idx := 0
	for i := r.next; i < len(r.buf); i++ {
		result[idx] = cloneEvent(r.buf[i])
		idx++
	}
	for i := range r.next {
		result[idx] = cloneEvent(r.buf[i])
		idx++
	}
	return result
}

func cloneEvent(ev Event) Event {
	c := ev
	c.Fields = copyFields(ev.Fields)
	return c
}

// DumpFlightRecorderToLogger emits the buffered events to the background logger.
// Intended for crash diagnostics (e.g. panic handling).
func DumpFlightRecorderToLogger(reason string) {
	events := FlightRecorder().Snapshot()
	if len(events) == 0 {
		return
	}
	logger := logutil.BgLogger()
	logger.Info("dump flight recorder", zap.String("reason", reason), zap.Int("event_count", len(events)))
	for _, ev := range events {
		fields := make([]zap.Field, 0, len(ev.Fields)+4)
		fields = append(fields, zap.String("category", getCategoryName(ev.Category)))
		fields = append(fields, zap.Time("event_ts", ev.Timestamp))
		if ev.TraceID != "" {
			fields = append(fields, zap.String("trace_id", ev.TraceID))
		}
		fields = append(fields, ev.Fields...)
		logger.Info("[trace-event-flight]", fields...)
	}
}

// getCategoryName returns the string name for a category.
func getCategoryName(category TraceCategory) string {
	switch category {
	case CoprRegionCache:
		return "copr-region-cache"
	default:
		return "unknown(" + strconv.FormatUint(uint64(category), 10) + ")"
	}
}
