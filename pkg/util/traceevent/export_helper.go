// Copyright 2026 PingCAP, Inc.
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
	"sync/atomic"

	"github.com/tikv/client-go/v2/trace"
	"go.uber.org/zap"
)

// Exported types for testing

// ExportedHTTPFlightRecorder exports HTTPFlightRecorder for testing
type ExportedHTTPFlightRecorder = HTTPFlightRecorder

// ExportedTraceCategory exports TraceCategory for testing
type ExportedTraceCategory = TraceCategory

// ExportedEvent exports Event for testing
type ExportedEvent = Event

// ExportedRingBufferSink exports RingBufferSink for testing
type ExportedRingBufferSink = RingBufferSink

// ExportedFlightRecorderConfig exports FlightRecorderConfig for testing
type ExportedFlightRecorderConfig = FlightRecorderConfig

// ExportedNewRingBufferSink exports NewRingBufferSink for testing
func ExportedNewRingBufferSink(capacity int) *RingBufferSink {
	return NewRingBufferSink(capacity)
}

// ExportedStartLogFlightRecorder exports StartLogFlightRecorder for testing
func ExportedStartLogFlightRecorder(config *FlightRecorderConfig) error {
	return StartLogFlightRecorder(config)
}

// ExportedSetCategories sets the enabledCategories for HTTPFlightRecorder for testing
func ExportedSetCategories(r *HTTPFlightRecorder, categories TraceCategory) {
	r.enabledCategories = categories
}

// ExportedDisable disables trace events for the specified categories for testing
func ExportedDisable(r *HTTPFlightRecorder, categories TraceCategory) {
	current := r.enabledCategories
	next := current &^ categories
	r.enabledCategories = next
}

// ExportedEnable enables trace events for the specified categories for testing
func ExportedEnable(r *HTTPFlightRecorder, categories TraceCategory) {
	current := r.enabledCategories
	next := current | categories
	r.enabledCategories = next
}

// ExportedGetFlightRecorder exports GetFlightRecorder for testing
func ExportedGetFlightRecorder() *HTTPFlightRecorder {
	return GetFlightRecorder()
}

// ExportedNewTrace exports NewTrace for testing
func ExportedNewTrace() *Trace {
	return NewTrace()
}

// ExportedHandleTraceControlExtractor exports handleTraceControlExtractor for testing
func ExportedHandleTraceControlExtractor(ctx context.Context) trace.TraceControlFlags {
	return handleTraceControlExtractor(ctx)
}

// ExportedGetTraceBits returns the bits field from Trace for testing
func ExportedGetTraceBits(tr *Trace) uint64 {
	return tr.bits
}

// ExportedSetTraceBits sets the bits field in Trace for testing
func ExportedSetTraceBits(tr *Trace, bits uint64) {
	tr.bits = bits
}

// ExportedTraceMarkBits calls the markBits method on Trace for testing
func ExportedTraceMarkBits(tr *Trace, idx int) {
	tr.markBits(idx)
}

// ExportedParseCategories exports parseCategories for testing
func ExportedParseCategories(categories []string) TraceCategory {
	return parseCategories(categories)
}

// ExportedGetTruthTable returns the truthTable field from HTTPFlightRecorder for testing
func ExportedGetTruthTable(fr *HTTPFlightRecorder) []uint64 {
	return fr.truthTable
}

// ExportedGetNameMapping returns the nameMapping field from compiledDumpTriggerConfig for testing
func ExportedGetNameMapping(c *compiledDumpTriggerConfig) map[string]int {
	return c.nameMapping
}

// ExportedCompiledDumpTriggerConfig exports compiledDumpTriggerConfig for testing
type ExportedCompiledDumpTriggerConfig = compiledDumpTriggerConfig

// ExportedTruthTableForAnd exports truthTableForAnd for testing
func ExportedTruthTableForAnd(x, y []uint64) []uint64 {
	return truthTableForAnd(x, y)
}

// ExportedTruthTableForOr exports truthTableForOr for testing
func ExportedTruthTableForOr(x, y []uint64) []uint64 {
	return truthTableForOr(x, y)
}

// ExportedCheckTruthTableBool exports checkTruthTable with different signature for testing
func ExportedCheckTruthTableBool(bits uint64, table []uint64) bool {
	return checkTruthTable(bits, table)
}

// ExportedAddTrigger calls addTrigger on compiledDumpTriggerConfig for testing
func ExportedAddTrigger(c *compiledDumpTriggerConfig, name string, conf *DumpTriggerConfig) (uint64, error) {
	return c.addTrigger(name, conf)
}

// ExportedCurrentSink returns the current sink for testing
func ExportedCurrentSink() Sink {
	return CurrentSink()
}

// ExportedSetSink sets the current sink for testing
func ExportedSetSink(sink Sink) {
	SetSink(sink)
}

// ExportedIsEnabled checks if a category is enabled for testing
func ExportedIsEnabled(category TraceCategory) bool {
	return IsEnabled(category)
}

// ExportedNewCompiledDumpTriggerConfig creates a new compiledDumpTriggerConfig for testing
func ExportedNewCompiledDumpTriggerConfig() *compiledDumpTriggerConfig {
	return &compiledDumpTriggerConfig{
		nameMapping: make(map[string]int),
	}
}

// ExportedSetCompiledDumpTriggerConfigNameMapping sets the nameMapping field for testing
func ExportedSetCompiledDumpTriggerConfigNameMapping(c *compiledDumpTriggerConfig, m map[string]int) {
	c.nameMapping = m
}

// ExportedMode constants for testing
const (
	ExportedModeOff  = ModeOff
	ExportedModeBase = ModeBase
	ExportedModeFull = ModeFull
)

// ExportedCategory constants for testing
const (
	ExportedTxnLifecycle   = TxnLifecycle
	ExportedTxn2PC         = Txn2PC
	ExportedTxnLockResolve = TxnLockResolve
	ExportedStmtLifecycle  = StmtLifecycle
	ExportedStmtPlan       = StmtPlan
	ExportedKvRequest      = KvRequest
	ExportedGeneral        = General
	ExportedUnknownClient  = UnknownClient
	ExportedAllCategories  = AllCategories
)

// ExportedSetMode exports SetMode for testing
func ExportedSetMode(mode string) (string, error) {
	return SetMode(mode)
}

// ExportedCurrentMode exports CurrentMode for testing
func ExportedCurrentMode() string {
	return CurrentMode()
}

// ExportedNormalizeMode exports NormalizeMode for testing
func ExportedNormalizeMode(mode string) (string, error) {
	return NormalizeMode(mode)
}

// ExportedFlightRecorder exports FlightRecorder for testing
func ExportedFlightRecorder() *RingBufferSink {
	return FlightRecorder()
}

// ExportedTraceEvent exports TraceEvent for testing
func ExportedTraceEvent(ctx context.Context, category TraceCategory, name string, fields ...zap.Field) {
	TraceEvent(ctx, category, name, fields...)
}

// ExportedNewRingBufferSinkUnexported exports NewRingBufferSink for testing (alias for consistency)
func ExportedNewRingBufferSinkUnexported(capacity int) *RingBufferSink {
	return NewRingBufferSink(capacity)
}

// ExportedFlightRecorderConfigUnexported exports FlightRecorderConfig for testing
type ExportedFlightRecorderConfigUnexported = FlightRecorderConfig

// ExportedStartLogFlightRecorderUnexported exports StartLogFlightRecorder for testing
func ExportedStartLogFlightRecorderUnexported(config *FlightRecorderConfig) error {
	return StartLogFlightRecorder(config)
}

// ExportedGetFlightRecorderUnexported exports GetFlightRecorder for testing
func ExportedGetFlightRecorderUnexported() *HTTPFlightRecorder {
	return GetFlightRecorder()
}

// ExportedDumpFlightRecorderToLogger exports DumpFlightRecorderToLogger for testing
func ExportedDumpFlightRecorderToLogger(reason string) {
	DumpFlightRecorderToLogger(reason)
}

// ExportedEnableUnexported exports Enable for testing (wrapper function)
func ExportedEnableUnexported(r *HTTPFlightRecorder, categories TraceCategory) {
	ExportedEnable(r, categories)
}

// ExportedDisableUnexported exports Disable for testing (wrapper function)
func ExportedDisableUnexported(r *HTTPFlightRecorder, categories TraceCategory) {
	ExportedDisable(r, categories)
}

// ExportedSetCategoriesUnexported exports SetCategories for testing (wrapper function)
func ExportedSetCategoriesUnexported(r *HTTPFlightRecorder, categories TraceCategory) {
	ExportedSetCategories(r, categories)
}

// ExportedLastDumpTime exports lastDumpTime for testing
func ExportedLastDumpTime() *atomic.Int64 {
	return &lastDumpTime
}
