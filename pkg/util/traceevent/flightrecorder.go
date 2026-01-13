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
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

// TraceBuf is the buffer for trace events.
// The buffer might be discard or dump.
type TraceBuf struct {
	mu      sync.RWMutex
	events  []Event
	bits    uint64
	TraceID []byte
}

var _ tracing.TraceBuf = (*TraceBuf)(nil)

// WithTraceBuf creates a context with the given TraceBuf.
func WithTraceBuf(ctx context.Context, trace *TraceBuf) context.Context {
	return tracing.WithTraceBuf(ctx, trace)
}

func getTraceBuf(ctx context.Context) *TraceBuf {
	val := tracing.GetTraceBuf(ctx)
	if val == nil {
		// For background job and internal session, it might be missing?
		return nil
	}
	traceBuf, ok := val.(*TraceBuf)
	if !ok {
		logutil.BgLogger().Warn("getTraceBuf assertion fails, traceBuf object type mismatch")
		return nil
	}
	return traceBuf
}

var globalHTTPFlightRecorder atomic.Pointer[HTTPFlightRecorder]

// HTTPFlightRecorder implements Sink interface.
// TODO: rename HTTPFlightRecorder to FlightRecorder as it may sink to log now instead of just HTTP
// TODO: remove the old global flight recorder, clean up code.
type HTTPFlightRecorder struct {
	ch                chan<- []Event
	enabledCategories TraceCategory
	counter           atomic.Int64 // used when dump trigger config is sampling
	Config            *FlightRecorderConfig
	compiledDumpTriggerConfig
}

// UserCommandConfig is the configuration for DumpTriggerConfig of user command type.
type UserCommandConfig struct {
	Type       string `json:"type"`
	SQLRegexp  string `json:"sql_regexp"`
	SQLDigest  string `json:"sql_digest"`
	PlanDigest string `json:"plan_digest"`
	StmtLabel  string `json:"stmt_label"`
	ByUser     string `json:"by_user"`
	Table      string `json:"table"`
}

// compile compiles the UserCommandConfig.
func (c *UserCommandConfig) compile(b *strings.Builder, mapping *compiledDumpTriggerConfig, conf *DumpTriggerConfig) (uint64, error) {
	if c == nil {
		return 0, fmt.Errorf("dump_trigger.user_command missing")
	}
	b.WriteString(".user_command")
	switch c.Type {
	case "sql_regexp":
		if c.SQLRegexp == "" {
			return 0, fmt.Errorf("dump_trigger.user_command.sql_regexp should not be empty")
		}
		b.WriteString(".sql_regexp")
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "sql_digest":
		if c.SQLDigest == "" {
			return 0, fmt.Errorf("dump_trigger.user_command.sql_digest should not be empty")
		}
		b.WriteString(".sql_digest")
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "plan_digest":
		if c.PlanDigest == "" {
			return 0, fmt.Errorf("dump_trigger.user_command.plan_digest should not be empty")
		}
		b.WriteString(".plan_digest")
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "stmt_label":
		if c.StmtLabel == "" {
			return 0, fmt.Errorf("dump_trigger.user_command.stmt_label should not be empty, should be something in https://github.com/pingcap/tidb/blob/adf08267939416d1b989e56dba6a6544bf34a8dd/pkg/parser/ast/ast.go#L160")
		}
		b.WriteString(".stmt_label")
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "by_user":
		if c.ByUser == "" {
			return 0, fmt.Errorf("dump_trigger.user_command.by_user should not be empty")
		}
		b.WriteString(".by_user")
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "table":
		if c.Table == "" {
			return 0, fmt.Errorf("dump_trigger.user_command.table should not be empty")
		}
		b.WriteString(".table")
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	}
	return 0, fmt.Errorf("wrong dump_trigger.user_command.type")
}

// SuspiciousEventConfig is the configuration for suspicious event.
type SuspiciousEventConfig struct {
	Type string `json:"type"`
	// SlowQuery
	// QueryFail error code?
	// ResolveLock?
	// RegionError
	IsInternal bool            `json:"is_internal,omitempty"`
	DevDebug   *DevDebugConfig `json:"dev_debug,omitempty"`
}

// DevDebugConfig is the configuration for development debugging.
type DevDebugConfig struct {
	Type string
}

const (
	// DevDebugTypeExecuteInternalTraceMissing is the type for execute internal trace missing.
	DevDebugTypeExecuteInternalTraceMissing = "execute_internal_trace_missing"
	// DevDebugTypeSendRequestTraceIDMissing is the type for send request trace id missing.
	DevDebugTypeSendRequestTraceIDMissing = "send_request_trace_id_missing"
)

// compile validates the development debugging configuration.
func (c *DevDebugConfig) compile(b *strings.Builder, mapping *compiledDumpTriggerConfig, conf *DumpTriggerConfig) (uint64, error) {
	if c == nil {
		return 0, fmt.Errorf("dump_trigger.suspicious_event.dev_debug missing")
	}
	b.WriteString(".dev_debug")
	switch c.Type {
	case DevDebugTypeExecuteInternalTraceMissing, DevDebugTypeSendRequestTraceIDMissing:
		return mapping.addTrigger(b.String(), conf)
	}
	return 0, fmt.Errorf("wrong dump_trigger.suspicious_event.dev_debug.type")
}

// compile compiles the suspicious event configuration.
func (c *SuspiciousEventConfig) compile(b *strings.Builder, mapping *compiledDumpTriggerConfig, conf *DumpTriggerConfig) (uint64, error) {
	if c == nil {
		return 0, fmt.Errorf("dump_trigger.suspicious_event missing")
	}
	b.WriteString(".suspicious_event")
	switch c.Type {
	case "slow_query":
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "query_fail":
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "resolve_lock":
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "region_error":
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "is_internal":
		b.WriteString(".is_internal")
		canonicalName := b.String()
		return mapping.addTrigger(canonicalName, conf)
	case "dev_debug":
		return c.DevDebug.compile(b, mapping, conf)
	}
	return 0, fmt.Errorf("wrong dump_trigger.suspicious_event.type")
}

// DumpTriggerConfig is the configuration for dump trigger.
type DumpTriggerConfig struct {
	Type string `json:"type"`
	// sampling = n means every n events will be sampled.
	// For example, sampling = 1000 means 1/1000 sampling rate.
	Sampling    int64                  `json:"sampling,omitempty"`
	Event       *SuspiciousEventConfig `json:"suspicious_event,omitempty"`
	UserCommand *UserCommandConfig     `json:"user_command,omitempty"`
	And         []DumpTriggerConfig    `json:"and,omitempty"`
	Or          []DumpTriggerConfig    `json:"or,omitempty"`
}

// Compile compiles the DumpTriggerConfig.
// When compile successfully, it returns nil, strings.Builder will contain the canonical name of the trigger.
func (c *DumpTriggerConfig) Compile(b *strings.Builder, mapping *compiledDumpTriggerConfig) ([]uint64, error) {
	if c == nil {
		return nil, fmt.Errorf("dump_trigger missing")
	}
	b.WriteString("dump_trigger")
	switch c.Type {
	case "sampling":
		if c.Sampling <= 0 {
			return nil, fmt.Errorf("wrong dump_trigger.sampling")
		}
		b.WriteString(".sampling")
		res, err := mapping.addTrigger(b.String(), c)
		if err != nil {
			return nil, err
		}
		return []uint64{res}, nil
	case "suspicious_event":
		ret, err := c.Event.compile(b, mapping, c)
		if err != nil {
			return nil, err
		}
		return []uint64{ret}, nil
	case "user_command":
		ret, err := c.UserCommand.compile(b, mapping, c)
		if err != nil {
			return nil, err
		}
		return []uint64{ret}, nil
	case "and":
		if len(c.And) == 0 {
			return nil, fmt.Errorf("dump_trigger.and missing")
		}
		var ret []uint64
		for _, and := range c.And {
			var buf strings.Builder
			tmp, err := and.Compile(&buf, mapping)
			if err != nil {
				return nil, err
			}
			ret = truthTableForAnd(ret, tmp)
		}
		return ret, nil
	case "or":
		if len(c.Or) == 0 {
			return nil, fmt.Errorf("dump_trigger.or missing")
		}
		var ret []uint64
		for _, or := range c.Or {
			var buf strings.Builder
			tmp, err := or.Compile(&buf, mapping)
			if err != nil {
				return nil, err
			}
			ret = truthTableForOr(ret, tmp)
		}
		return ret, nil
	}
	return nil, fmt.Errorf("wrong dump_trigger.type")
}

// How it works?
// Imagine we need to implement support any combination of AND and OR operations for flight recorder dump trigger conditions.
// Like dump_trigger.user_command.sql_digest = xxx && dump_trigger.suspicious_event.resolve_lock ...
// Each trigger condition can be write as A, B etc for short, so this is A && B
//
// We use 1 bit for each condition.
// A: 1...
// B: 01...
// C: 001...
// D: 0001...
//
// Use bit | to represent AND
// A && B => 11...
// A && C => 101...
//
// Use array to represent OR
// A || B => [1..., 01...]
// A || C => [1..., 001...]
//
// Now we can combine any AND and OR operations.
// A && [B || C] => [A && B, A && C] => [11..., 101...]
// [A || B] && [C || D] => [A && C, A && D, B && C, B && D] => [110..., 1001.., 011..., 0101..]
//
// How to check if a condition is satisfied?
// For example, we have a condition A && [B || C] && D => [A && B, A && C] => [1101., 1011...]
// And the sequence of events is A, D, C, we calculate A && D && C => 1011...
// We can use bit & to check if a condition is satisfied. 1011 & 1101 => 1001, the first check fail;
// 1011 & 1011 => 1011, the second check pass, it is an OR condition
// So this sequence satisfies the condition.
type compiledDumpTriggerConfig struct {
	// nameMapping maps a dump trigger canonical name to a bit representation
	nameMapping map[string]int
	configRef   []*DumpTriggerConfig
	// short cut for checking combinations of AND and OR conditions
	truthTable []uint64
}

func (c *compiledDumpTriggerConfig) addTrigger(canonicalName string, config *DumpTriggerConfig) (uint64, error) {
	_, ok := c.nameMapping[canonicalName]
	if ok {
		return 0, fmt.Errorf("duplicate trigger name: %s", canonicalName)
	}
	idx := len(c.nameMapping)
	if idx >= 64 {
		return 0, fmt.Errorf("too many triggers")
	}
	c.nameMapping[canonicalName] = idx
	c.configRef = append(c.configRef, config)
	return 1 << idx, nil
}

func truthTableForAnd(x, y []uint64) []uint64 {
	if len(x) == 0 {
		return y
	}
	if len(x) == 1 {
		// A && [B, C, D] => [A && B, A && C, A && D]
		return truthTableForAnd1(x[0], y)
	}
	// [A || B || C] && D => [A && D || B && D || C && D]
	ret := make([]uint64, 0, len(x)*len(y))
	for _, v := range x {
		pos := len(ret)
		ret = append(ret, y...)
		truthTableForAnd1(v, ret[pos:])
	}
	return ret
}

func truthTableForAnd1(x uint64, xs []uint64) []uint64 {
	for i := 0; i < len(xs); i++ {
		xs[i] = xs[i] | x
	}
	return xs
}

func truthTableForOr(x, y []uint64) []uint64 {
	// not doing any deduplication because duplicate trigger condition is not allowed by compile
	return append(x, y...)
}

func checkTruthTable(bits uint64, table []uint64) bool {
	for _, v := range table {
		if bits&v == v {
			return true
		}
	}
	return false
}

// CheckFlightRecorderDumpTrigger checks if the flight recorder should dump based on the trigger configuration.
func CheckFlightRecorderDumpTrigger(ctx context.Context, triggerName string, check func(*DumpTriggerConfig) bool) {
	flightRecorder := globalHTTPFlightRecorder.Load()
	if flightRecorder == nil {
		return
	}
	traceBuf := getTraceBuf(ctx)
	if traceBuf == nil {
		return
	}
	idx, ok := flightRecorder.compiledDumpTriggerConfig.nameMapping[triggerName]
	if !ok {
		return
	}
	conf := flightRecorder.compiledDumpTriggerConfig.configRef[idx]
	if check(conf) {
		traceBuf.markBits(idx)
	}
}

// FlightRecorderConfig represents the configuration for the flight recorder.
// A example of flight recorder configuration in json:
//
//	{
//		"enabled_categories": ["general"],
//		"dump_trigger": {
//			"type": "sampling"
//			"sampling": 100
//			"suspicious_event":
//			{
//				"type": "long_txn",
//				"long_txn": ...,
//				"resolve_lock": ...,
//				"slow query": ...,
//				"error": ...,
//			},
//			"user_command" :  {
//				"type": "sql_regexp",
//				"sql_regexp": "select * from xx",
//				"plan_digest": "42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772",
//				"table": "test"
//				"by_user": "root",
//			}
//		}
//	}
type FlightRecorderConfig struct {
	EnabledCategories []string          `json:"enabled_categories"`
	DumpTrigger       DumpTriggerConfig `json:"dump_trigger"`
}

// Initialize initializes the default flight recorder configuration.
// It will dump all the events, but excludes TiKV write/read details and developer debug by default
// to avoid excessive overhead.
func (c *FlightRecorderConfig) Initialize() {
	c.EnabledCategories = []string{"-", "tikv_write_details", "tikv_read_details", "dev_debug"}
	c.DumpTrigger.Type = "sampling"
	c.DumpTrigger.Sampling = 1
}

// Compile compiles the flight recorder configuration.
func (c *FlightRecorderConfig) Compile() (compiledDumpTriggerConfig, error) {
	var b strings.Builder
	result := compiledDumpTriggerConfig{
		nameMapping: make(map[string]int),
	}
	truthTable, err := c.DumpTrigger.Compile(&b, &result)
	if err != nil {
		return result, err
	}
	result.truthTable = truthTable
	return result, nil
}

func parseCategories(categories []string) TraceCategory {
	var result TraceCategory
	sub := false
	for _, str := range categories {
		if str == "*" {
			result = tracing.AllCategories
			break
		}
		if str == "-" {
			result = tracing.AllCategories
			sub = true
			continue
		}

		if sub {
			result &= ^tracing.ParseTraceCategory(str)
		} else {
			result |= tracing.ParseTraceCategory(str)
		}
	}
	return result
}

func newHTTPFlightRecorder(config *FlightRecorderConfig) (*HTTPFlightRecorder, error) {
	compiled, err := config.Compile()
	if err != nil {
		return nil, err
	}

	categories := parseCategories(config.EnabledCategories)
	ret := &HTTPFlightRecorder{
		enabledCategories:         categories,
		Config:                    config,
		compiledDumpTriggerConfig: compiled,
	}
	logutil.BgLogger().Info("start http flight recorder",
		zap.Stringer("category", categories),
		zap.Any("mapping", compiled.nameMapping),
		zap.Uint64s("truthTable", ret.truthTable))

	globalHTTPFlightRecorder.Store(ret)
	return ret, nil
}

// StartHTTPFlightRecorder starts the HTTP flight recorder.
func StartHTTPFlightRecorder(ch chan<- []Event, config *FlightRecorderConfig) (*HTTPFlightRecorder, error) {
	ret, err := newHTTPFlightRecorder(config)
	if err != nil {
		return nil, err
	}
	ret.ch = ch
	return ret, nil
}

// StartLogFlightRecorder starts the flight recorder that sink to log.
func StartLogFlightRecorder(config *FlightRecorderConfig) error {
	_, err := newHTTPFlightRecorder(config)
	return err
}

// GetFlightRecorder returns the flight recorder.
func GetFlightRecorder() *HTTPFlightRecorder {
	return globalHTTPFlightRecorder.Load()
}

// Close closes the HTTP flight recorder.
func (*HTTPFlightRecorder) Close() {
	globalHTTPFlightRecorder.Store(nil)
}

func (r *HTTPFlightRecorder) shouldKeep(bits uint64) bool {
	return checkTruthTable(bits, r.truthTable)
}

// collect sends events to the HTTP flight recorder channel.
// The caller must pass a cloned slice to avoid data races; this function
// does not clone the slice to avoid redundant allocations.
func (r *HTTPFlightRecorder) collect(ctx context.Context, events []Event) {
	if r.ch == nil {
		// Used by log flight recorder
		for _, event := range events {
			logEvent(ctx, event)
		}
		return
	}

	// Used by http flight recorder
	select {
	case r.ch <- events:
	default:
	}
}

// NewTrace creates a new Trace.
func NewTraceBuf() *TraceBuf {
	return &TraceBuf{}
}

// Record implements the FlightRecorder interface.
func (r *TraceBuf) Record(_ context.Context, event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *TraceBuf) markBits(idx int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bits |= 1 << idx
}

const maxEvents = 4096

// CheckSampling checks whether the trace should be sampled.
func (r *HTTPFlightRecorder) CheckSampling(conf *DumpTriggerConfig) bool {
	v := r.counter.Add(1)
	if v >= conf.Sampling {
		r.counter.Store(0)
		return true
	}
	return false
}

// DiscardOrFlush will flush or discard the trace.
func (r *TraceBuf) DiscardOrFlush(ctx context.Context) {
	sink := globalHTTPFlightRecorder.Load()
	if sink != nil {
		var shouldFlush bool
		var eventsToFlush []Event
		// Read phase: use RLock to safely read keep flag and clone events.
		// We must clone while holding the lock to avoid data races where
		// concurrent Record() or DiscardOrFlush() calls might modify the
		// backing array after we release RLock.
		r.mu.RLock()
		if sink.shouldKeep(r.bits) {
			shouldFlush = true
			eventsToFlush = slices.Clone(r.events) // Deep copy to avoid data race
		}
		r.mu.RUnlock()

		// Process without holding any lock
		if shouldFlush {
			sink.collect(ctx, eventsToFlush)
		}
	}
	// Write phase: use Lock for cleanup
	r.mu.Lock()
	r.bits = 0
	if len(r.events) > maxEvents {
		// avoid using too much memory for each session.
		r.events = nil
	} else {
		r.events = r.events[:0]
	}
	r.TraceID = nil
	r.mu.Unlock()
}
