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

package stmtctx

// DMLResult counts input-row outcomes independently of affected rows. It is
// valid only for a successful statement; executor retries reset it completely.
// Executor hooks run on the statement goroutine, like RecordRows/CopiedRows.
type DMLResult struct {
	// Version identifies the optional DML telemetry contract.
	Version int `json:"version"`
	// Records counts input records presented to the executor.
	Records uint64 `json:"records"`
	// Imported counts successful ordinary source insertions.
	Imported uint64 `json:"imported"`
	// Replaced counts replacement inputs, including unchanged replacements.
	Replaced uint64 `json:"replaced"`
	// Skipped counts source inputs omitted by duplicate-key policy.
	Skipped uint64 `json:"skipped"`
	// Error counts inputs rejected by an ignored constraint error.
	Error uint64 `json:"error"`
	// Unsupported suppresses telemetry for unsupported statement side effects.
	Unsupported bool `json:"-"`
}

// InitDMLResult registers an input batch without resetting earlier batches.
// Triggers/cascades and ON DUPLICATE UPDATE do not expose source outcomes yet.
func (sc *StatementContext) InitDMLResult(records uint64, supported bool) {
	if sc.DMLResult == nil {
		sc.DMLResult = &DMLResult{Version: 1}
	}
	sc.DMLResult.Records += records
	// Trigger-body inputs belong to an internal statement, including when the
	// outer UPDATE/DELETE has no source-result hooks of its own.
	sc.DMLResult.Unsupported = sc.DMLResult.Unsupported || !supported || sc.TriggerCtx.InTrigger
}

// AddDMLImported records one successful ordinary source insertion.
func (sc *StatementContext) AddDMLImported() {
	if sc.DMLResult != nil {
		sc.DMLResult.Imported++
	}
}

// AddDMLSkipped records one source input skipped by a duplicate-key policy.
func (sc *StatementContext) AddDMLSkipped() {
	if sc.DMLResult != nil {
		sc.DMLResult.Skipped++
	}
}

// AddDMLError records one input rejected by an ignored constraint error.
func (sc *StatementContext) AddDMLError() {
	if sc.DMLResult != nil {
		sc.DMLResult.Error++
	}
}

// AddDMLReplaced records one replacement input, regardless of deleted row count.
func (sc *StatementContext) AddDMLReplaced() {
	if sc.DMLResult != nil {
		sc.DMLResult.Replaced++
	}
}

// Valid reports whether all inputs have a supported, disjoint outcome.
func (r *DMLResult) Valid() bool {
	return r != nil && r.Version == 1 && !r.Unsupported && r.Records == r.Imported+r.Replaced+r.Skipped+r.Error
}
