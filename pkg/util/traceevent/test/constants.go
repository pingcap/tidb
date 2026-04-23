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

package traceevent_test

import (
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// Constants re-exported from the main package for testing
const (
	ModeOff        = traceevent.ExportedModeOff
	ModeBase       = traceevent.ExportedModeBase
	ModeFull       = traceevent.ExportedModeFull
	TxnLifecycle   = traceevent.ExportedTxnLifecycle
	Txn2PC         = traceevent.ExportedTxn2PC
	TxnLockResolve = traceevent.ExportedTxnLockResolve
	StmtLifecycle  = traceevent.ExportedStmtLifecycle
	StmtPlan       = traceevent.ExportedStmtPlan
	KvRequest      = traceevent.ExportedKvRequest
	General        = traceevent.ExportedGeneral
	UnknownClient  = traceevent.ExportedUnknownClient
	AllCategories  = traceevent.ExportedAllCategories
)

// Type aliases for testing
type (
	Event                = traceevent.ExportedEvent
	TraceCategory        = traceevent.ExportedTraceCategory
	RingBufferSink       = traceevent.ExportedRingBufferSink
	HTTPFlightRecorder   = traceevent.ExportedHTTPFlightRecorder
	FlightRecorderConfig = traceevent.ExportedFlightRecorderConfig
)

// CurrentSink returns the current sink.
func CurrentSink() traceevent.Sink {
	return traceevent.ExportedCurrentSink()
}

// SetSink sets the current sink.
func SetSink(sink traceevent.Sink) {
	traceevent.ExportedSetSink(sink)
}

// IsEnabled checks if a category is enabled.
func IsEnabled(category tracing.TraceCategory) bool {
	return traceevent.ExportedIsEnabled(category)
}

// lastDumpTime is exported for testing
var lastDumpTime = traceevent.ExportedLastDumpTime()
