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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// ModeOff disables all trace event recording (no flight recorder, no logging).
	ModeOff = "off"
	// ModeBase enables flight recorder only (default mode).
	ModeBase = "base"
	// ModeFull enables both flight recorder and log emission.
	ModeFull = "full"
)

const (
	// TxnLifecycle traces transaction begin/commit/rollback events.
	TxnLifecycle = tracing.TxnLifecycle
	// Txn2PC traces two-phase commit prewrite and commit phases.
	Txn2PC = tracing.Txn2PC
	// TxnLockResolve traces lock resolution and conflict handling.
	TxnLockResolve = tracing.TxnLockResolve
	// StmtLifecycle traces statement start/finish events.
	StmtLifecycle = tracing.StmtLifecycle
	// StmtPlan traces statement plan digest and optimization.
	StmtPlan = tracing.StmtPlan
	// KvRequest traces client-go kv request and responses
	KvRequest = tracing.KvRequest
	// General is used by tracing API
	General = tracing.General
	// UnknownClient is the fallback category for unmapped client-go trace events.
	// Used when client-go emits events with categories not yet mapped in adapter.go.
	// This provides forward compatibility if client-go adds new categories.
	UnknownClient = tracing.UnknownClient
	// AllCategories can be used to enable every known trace category.
	AllCategories = tracing.AllCategories
)

// recorderEnabled controls whether the flight recorder is active.
// loggingEnabled controls whether the log sink emits logs.
// lastDumpTime stores the Unix timestamp of the last flight recorder dump.
var (
	recorderEnabled atomic.Bool
	loggingEnabled  atomic.Bool
	lastDumpTime    atomic.Int64
)

// DefaultFlightRecorderCapacity controls the number of events retained in the in-memory recorder.
// make this small so there won't be too many logs, until we have ability to filter out the ones we need.
const DefaultFlightRecorderCapacity = 1024

// FlightRecorderCoolingOffPeriod is the minimum time between full flight recorder dumps.
// During the cooling-off period, only a single summary log is emitted.
const FlightRecorderCoolingOffPeriod = 10 * time.Second

// eventSink stores the global sink used to record events.
type sinkHolder struct {
	sink Sink
}

var eventSink atomic.Value // of type sinkHolder

// flightRecorder keeps a rolling buffer with recent events for post-mortem analysis.
var flightRecorder = NewRingBufferSink(DefaultFlightRecorderCapacity)

// init sets up the default sink configuration.
// Default mode is "base" (flight recorder enabled, logging disabled).
func init() {
	defaultSink := &LogSink{}
	eventSink.Store(sinkHolder{sink: defaultSink})
	recorderEnabled.Store(true) // base mode: recorder enabled
	loggingEnabled.Store(false) // base mode: logging disabled

	// Register TiDB's trace event handlers with client-go
	RegisterWithClientGo()
}

// Enable enables trace events for the specified categories.
// Multiple categories can be combined with bitwise OR.
var Enable = tracing.Enable

// IsEnabled returns whether the specified category is enabled.
var IsEnabled = tracing.IsEnabled

// Disable disables trace events for the specified categories.
var Disable = tracing.Disable

// SetCategories sets the enabled categories to exactly the specified value.
var SetCategories = tracing.SetCategories

// GetEnabledCategories returns the currently enabled categories.
var GetEnabledCategories = tracing.GetEnabledCategories

// NormalizeMode converts a user-supplied tracing mode string into its canonical representation.
func NormalizeMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case ModeOff, "0", "false":
		return ModeOff, nil
	case ModeBase:
		return ModeBase, nil
	case ModeFull:
		return ModeFull, nil
	default:
		return "", fmt.Errorf("unsupported trace event mode %q, valid modes: off, base, full", mode)
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
		recorderEnabled.Store(false)
		loggingEnabled.Store(false)
	case ModeBase:
		recorderEnabled.Store(true)
		loggingEnabled.Store(false)
	case ModeFull:
		recorderEnabled.Store(true)
		loggingEnabled.Store(true)
	default:
		return "", fmt.Errorf("unknown trace event mode %q after normalization", normalized)
	}
	return normalized, nil
}

// CurrentMode reports the canonical tracing mode string.
func CurrentMode() string {
	recEnabled := recorderEnabled.Load()
	logEnabled := loggingEnabled.Load()

	if !recEnabled && !logEnabled {
		return ModeOff
	}
	if recEnabled && logEnabled {
		return ModeFull
	}
	if recEnabled && !logEnabled {
		return ModeBase
	}
	// Shouldn't happen (logging without recorder), but return full for consistency
	return ModeFull
}

// Event captures the raw information describing a trace event. This structure
// is intentionally generic so that it can later be transformed into the Trace
// Event Format (TEF) once the full design is finalized.
type Event = tracing.Event

// TraceCategory represents different trace event categories.
type TraceCategory = tracing.TraceCategory

// Sink records trace events.
type Sink = tracing.Sink

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

	// Defensive copy prevents corruption from caller's buffer reuse.
	// Reserve extra capacity for LogSink to append metadata without allocation.
	event := Event{
		Category:  category,
		Name:      name,
		Phase:     tracing.PhaseInstant,
		Timestamp: time.Now(),
		TraceID:   TraceIDFromContext(ctx),
		Fields:    copyFieldsWithCapacity(fields, 3),
	}

	// Record to flight recorder if enabled (base or full mode).
	if recorderEnabled.Load() {
		// TODO: clean up here
		if recorder := FlightRecorder(); recorder != nil {
			recorder.Record(ctx, event)
		}
		sink := tracing.GetSink(ctx)
		if sink != nil {
			sink.(Sink).Record(ctx, event)
		}
	}

	// Record to log sink if logging is enabled (full mode).
	if sink := CurrentSink(); sink != nil {
		sink.Record(ctx, event)
	}
}

// TraceIDFromContext returns the trace identifier from ctx if present.
// It delegates to client-go's TraceIDFromContext implementation.
func TraceIDFromContext(ctx context.Context) []byte {
	return trace.TraceIDFromContext(ctx)
}

// ContextWithTraceID returns a new context with the given trace identifier.
// It delegates to client-go's ContextWithTraceID implementation.
func ContextWithTraceID(ctx context.Context, traceID []byte) context.Context {
	return trace.ContextWithTraceID(ctx, traceID)
}

// GenerateTraceID creates a trace ID from transaction start timestamp and statement count.
// The trace ID is 20 bytes: [start_ts (8 bytes)][stmt_count (8 bytes)][random (4 bytes)] in big-endian format.
// The random suffix distinguishes different statement executions.
// This function should be called ONCE per statement execution, not per retry.
// If no transaction has started, start_ts will be 0.
func GenerateTraceID(ctx context.Context, startTS uint64, stmtCount uint64) []byte {
	traceID := make([]byte, 20)
	binary.BigEndian.PutUint64(traceID[0:8], startTS)
	binary.BigEndian.PutUint64(traceID[8:16], stmtCount)
	var rand32 uint32
	if sink := tracing.GetSink(ctx); sink != nil {
		if t, ok := sink.(*Trace); ok {
			t.mu.Lock()
			rand32 = t.rand32
			t.mu.Unlock()
		}
	}
	if rand32 == 0 {
		rand32 = rand.Uint32()
	}
	binary.BigEndian.PutUint32(traceID[16:20], rand32)
	return traceID
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

	logEvent(ctx, event)
}

func logEvent(ctx context.Context, event Event) {
	// Append to reserved capacity without allocation.
	// Field order: [event fields] [category] [timestamp] [trace_id?]
	fields := event.Fields
	fields = append(fields, zap.String("category", event.Category.String()))
	fields = append(fields, zap.Int64("event_ts", event.Timestamp.UnixMicro()))
	if len(event.TraceID) > 0 {
		fields = append(fields, zap.String("trace_id", hex.EncodeToString(event.TraceID)))
	}

	logutil.Logger(ctx).Info("[trace-event] "+event.Name, fields...)
}

// copyFieldsWithCapacity copies fields with extra capacity for appending without reallocation.
func copyFieldsWithCapacity(fields []zap.Field, extraCap int) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	out := make([]zap.Field, len(fields), len(fields)+extraCap)
	copy(out, fields)
	return out
}

// copyFields copies fields without extra capacity.
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
// Assumes event.Fields is already an independent copy.
func (r *RingBufferSink) Record(_ context.Context, event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.buf) < r.cap {
		r.buf = append(r.buf, event)
		if len(r.buf) == r.cap {
			r.next = 0
		}
		return
	}

	r.buf[r.next] = event
	r.next = (r.next + 1) % r.cap
}

// DiscardOrFlush clears all buffered events.
func (r *RingBufferSink) DiscardOrFlush() {
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
		for i := range len(r.buf) {
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
// If called within the cooling-off period, only a summary log is emitted.
func DumpFlightRecorderToLogger(reason string) {
	events := FlightRecorder().Snapshot()
	if len(events) == 0 {
		return
	}

	logger := logutil.BgLogger()
	now := time.Now().Unix()
	last := lastDumpTime.Load()
	elapsed := time.Duration(now-last) * time.Second

	// Check if we're in the cooling-off period
	if last > 0 && elapsed < FlightRecorderCoolingOffPeriod {
		logger.Info("flight recorder dump suppressed (cooling off)",
			zap.String("reason", reason),
			zap.Int("event_count", len(events)),
			zap.Duration("elapsed_since_last_dump", elapsed))
		return
	}

	// Update timestamp for next cooling-off check
	lastDumpTime.Store(now)

	// Perform full dump
	logger.Info("dump flight recorder", zap.String("reason", reason), zap.Int("event_count", len(events)))
	for _, ev := range events {
		fields := make([]zap.Field, 0, len(ev.Fields)+4)
		fields = append(fields, zap.String("category", ev.Category.String()))
		fields = append(fields, zap.Int64("event_ts", ev.Timestamp.UnixMicro()))
		if len(ev.TraceID) > 0 {
			fields = append(fields, zap.String("trace_id", hex.EncodeToString(ev.TraceID)))
		}
		fields = append(fields, ev.Fields...)
		logger.Info("[trace-event-flight]", fields...)
	}
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
	default:
		return "unknown(" + strconv.FormatUint(uint64(category), 10) + ")"
	}
}

// RenderEvent defines the event structure for trace event rendering on http://ui.perfetto.dev
// See https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview?tab=t.0
type RenderEvent struct {
	Name     string          `json:"name"`
	Phase    tracing.Phase   `json:"ph"`
	Ts       int64           `json:"ts"` // microsecond
	PID      uint32          `json:"pid"`
	TID      uint32          `json:"tid"`
	ID       uint64          `json:"id,omitempty"` // used by async / flow
	Category string          `json:"cat,omitempty"`
	Args     json.RawMessage `json:"args,omitempty"`
}

func extractRandFromTraceID(traceID []byte) uint32 {
	if len(traceID) != 20 {
		return 0
	}
	return *((*uint32)(unsafe.Pointer(&traceID[16])))
}

// ConvertEventsForRendering converts []Event to []RenderEvent for trace event rendering.
func ConvertEventsForRendering(events []Event) []RenderEvent {
	var tid uint32
	res := make([]RenderEvent, 0, len(events))
	for _, event := range events {
		e := RenderEvent{
			Name:     event.Name,
			Phase:    event.Phase,
			Ts:       event.Timestamp.UnixMicro(),
			PID:      0,
			Category: event.Category.String(),
		}
		if tid == 0 {
			tid = extractRandFromTraceID(event.TraceID)
		} else {
			val := extractRandFromTraceID(event.TraceID)
			if tid != val {
				logutil.BgLogger().Info("wrong traceid",
					zap.Uint32("expect", tid),
					zap.Uint32("get", val))
			}
		}
		if len(event.TraceID) > 0 && len(event.TraceID) != 20 {
			logutil.BgLogger().Info("wrong traceid format",
				zap.String("trace_id", string(event.TraceID)))
		}

		if len(event.Fields) > 0 {
			cfg := zap.NewProductionEncoderConfig()
			cfg.LevelKey = ""
			cfg.MessageKey = ""
			enc := zapcore.NewJSONEncoder(cfg)
			fields := event.Fields
			if len(event.TraceID) > 0 {
				fields = append(event.Fields, zap.String("trace_id", hex.EncodeToString(event.TraceID)))
			}
			buf, _ := enc.EncodeEntry(
				zapcore.Entry{},
				fields,
			)
			e.Args = buf.Bytes()
		}
		res = append(res, e)
	}
	if tid == 0 {
		logutil.BgLogger().Info("wrong traceid")
	}
	for i := range res {
		res[i].TID = tid
	}
	return res
}
