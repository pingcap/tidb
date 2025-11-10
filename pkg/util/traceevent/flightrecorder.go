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
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

// Trace implements Sink interface
type Trace struct {
	mu     sync.Mutex
	events []Event
	keep   bool
	rand32 uint32
}

var globalHTTPFlightRecorder atomic.Pointer[HTTPFlightRecorder]

// HTTPFlightRecorder implements Sink interface.
// TODO: rename HTTPFlightRecorder to FlightRecorder as it may sink to log now instead of just HTTP
// TODO: remove the old global flight recorder, clean up code.
type HTTPFlightRecorder struct {
	ch                   chan<- []Event
	oldEnabledCategories TraceCategory
	counter              int // used when dump trigger config is sampling
	Config               *FlightRecorderConfig
	// Or should it be a Set if we support trigger condition combination?
	triggerCanonicalName string
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

// Validate validates the UserCommandConfig.
func (c *UserCommandConfig) Validate(b *strings.Builder) error {
	if c == nil {
		return fmt.Errorf("dump_trigger.user_command missing")
	}
	b.WriteString(".user_command")
	switch c.Type {
	case "sql_regexp":
		if c.SQLRegexp == "" {
			return fmt.Errorf("dump_trigger.user_command.sql_regexp should not be empty")
		}
		b.WriteString(".sql_regexp")
	case "sql_digest":
		if c.SQLDigest == "" {
			return fmt.Errorf("dump_trigger.user_command.sql_digest should not be empty")
		}
		b.WriteString(".sql_digest")
	case "plan_digest":
		if c.PlanDigest == "" {
			return fmt.Errorf("dump_trigger.user_command.plan_digest should not be empty")
		}
		b.WriteString(".plan_digest")
	case "stmt_label":
		if c.StmtLabel == "" {
			return fmt.Errorf("dump_trigger.user_command.stmt_label should not be empty, should be something in https://github.com/pingcap/tidb/blob/adf08267939416d1b989e56dba6a6544bf34a8dd/pkg/parser/ast/ast.go#L160")
		}
		b.WriteString(".stmt_label")
	case "by_user":
		if c.ByUser == "" {
			return fmt.Errorf("dump_trigger.user_command.by_user should not be empty")
		}
		b.WriteString(".by_user")
	case "table":
		if c.Table == "" {
			return fmt.Errorf("dump_trigger.user_command.table should not be empty")
		}
		b.WriteString(".table")
	default:
		return fmt.Errorf("wrong dump_trigger.user_command.type")
	}
	return nil
}

// SuspiciousEventConfig is the configuration for suspicious event.
type SuspiciousEventConfig struct {
	Type string `json:"type"`
	// SlowQuery
	// QueryFail error code?
	// ResolveLock?
	// RegionError
}

// Validate validates the suspicious event configuration.
func (c *SuspiciousEventConfig) Validate(b *strings.Builder) error {
	if c == nil {
		return fmt.Errorf("dump_trigger.suspicious_event missing")
	}
	b.WriteString(".suspicious_event")
	switch c.Type {
	case "slow_query":
	case "query_fail":
	case "resolve_lock":
	case "region_error":
	default:
		return fmt.Errorf("wrong dump_trigger.suspicious_event.type")
	}
	return nil
}

// DumpTriggerConfig is the configuration for dump trigger.
type DumpTriggerConfig struct {
	Type string `json:"type"`
	// sampling = n means every n events will be sampled.
	// For example, sampling = 1000 means 1/1000 sampling rate.
	Sampling    int                    `json:"sampling,omitempty"`
	Event       *SuspiciousEventConfig `json:"suspicious_event,omitempty"`
	UserCommand *UserCommandConfig     `json:"user_command,omitempty"`
}

// Validate validates the DumpTriggerConfig.
// When validate successfully, it returns nil, strings.Builder will contain the canonical name of the trigger.
func (c *DumpTriggerConfig) Validate(b *strings.Builder) error {
	if c == nil {
		return fmt.Errorf("dump_trigger missing")
	}
	b.WriteString("dump_trigger")
	switch c.Type {
	case "sampling":
		if c.Sampling <= 0 {
			return fmt.Errorf("wrong dump_trigger.sampling")
		}
		b.WriteString(".sampling")
	case "suspicious_event":
		return c.Event.Validate(b)
	case "user_command":
		return c.UserCommand.Validate(b)
	default:
		return fmt.Errorf("wrong dump_trigger.type")
	}
	return nil
}

// CheckFlightRecorderDumpTrigger checks if the flight recorder should dump based on the trigger name and configuration.
func CheckFlightRecorderDumpTrigger(ctx context.Context, triggerName string, check func(*DumpTriggerConfig) bool) {
	flightRecorder := globalHTTPFlightRecorder.Load()
	if flightRecorder == nil {
		return
	}
	// Sink should always be set, it should be a Trace object which implements MarkDump()
	sink := tracing.GetSink(ctx)
	if sink == nil {
		return
	}
	raw, ok := sink.(tracing.FlightRecorder)
	if !ok {
		return
	}
	if flightRecorder.triggerCanonicalName == triggerName {
		if check(&flightRecorder.Config.DumpTrigger) {
			raw.MarkDump()
		}
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
// It will dump all the events.
func (c *FlightRecorderConfig) Initialize() {
	c.EnabledCategories = []string{"*"}
	c.DumpTrigger.Type = "sampling"
	c.DumpTrigger.Sampling = 1
}

// Validate validates the flight recorder configuration.
func (c *FlightRecorderConfig) Validate(b *strings.Builder) error {
	return c.DumpTrigger.Validate(b)
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
	var b strings.Builder
	if err := config.Validate(&b); err != nil {
		return nil, err
	}

	categories := parseCategories(config.EnabledCategories)
	ret := &HTTPFlightRecorder{
		oldEnabledCategories: tracing.GetEnabledCategories(),
		Config:               config,
		triggerCanonicalName: b.String(),
	}
	logutil.BgLogger().Info("start http flight recorder",
		zap.Stringer("category", categories),
		zap.String("triggerCanonicalName", ret.triggerCanonicalName))
	SetCategories(categories)
	globalHTTPFlightRecorder.Store(ret)
	return ret, nil
}

// StartHTTPFlightRecorder starts the HTTP flight recorder.
func StartHTTPFlightRecorder(ch chan<- []Event, config *FlightRecorderConfig) (*HTTPFlightRecorder, error) {
	ret, err := newHTTPFlightRecorder(config)
	ret.ch = ch
	return ret, err
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
func (r *HTTPFlightRecorder) Close() {
	tracing.SetCategories(r.oldEnabledCategories)
	globalHTTPFlightRecorder.Store(nil)
}

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
	case r.ch <- slices.Clone(events):
	default:
	}
}

// Trace implement the FlightRecorder interface.
var _ tracing.FlightRecorder = &Trace{}

// NewTrace creates a new Trace.
func NewTrace() *Trace {
	return &Trace{
		rand32: rand.Uint32(),
	}
}

// Record implements the FlightRecorder interface.
func (r *Trace) Record(_ context.Context, event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

// MarkDump implements the FlightRecorder interface.
func (r *Trace) MarkDump() {
	r.mu.Lock()
	r.keep = true
	r.mu.Unlock()
}

const maxEvents = 4096

// DiscardOrFlush will flush or discard the trace, depending on the whether MarkDump has been called.
func (r *Trace) DiscardOrFlush(ctx context.Context) {
	sink := globalHTTPFlightRecorder.Load()
	if sink != nil {
		if r.keep {
			sink.collect(ctx, r.events)
		}
		if sink.Config.DumpTrigger.Type == "sampling" {
			sink.counter++
			if sink.counter >= sink.Config.DumpTrigger.Sampling {
				sink.collect(ctx, r.events)
				sink.counter = 0
			}
		}
	}
	newRand := rand.Uint32()
	r.mu.Lock()
	r.keep = false
	if len(r.events) > maxEvents {
		// avoid using too much memory for each session.
		r.events = nil
	} else {
		r.events = r.events[:0]
	}
	r.rand32 = newRand
	r.mu.Unlock()
}
