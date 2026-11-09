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

// Package pkdbsummary tracks mutually exclusive outcomes of source records.
package pkdbsummary

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Outcomes counts confirmed source outcomes, never affected rows or index keys.
type Outcomes struct {
	// Imported counts confirmed ordinary insertions.
	Imported uint64 `json:"imported"`
	// Replaced counts replacement inputs once, independently of deleted rows.
	Replaced uint64 `json:"replaced"`
	// Skipped counts inputs deliberately skipped by conflict policy.
	Skipped uint64 `json:"skipped"`
	// Error counts inputs definitively rejected by conversion or writing.
	Error uint64 `json:"error"`
}

// Count returns the number of classified source records.
func (o Outcomes) Count() uint64 { return o.Imported + o.Replaced + o.Skipped + o.Error }

// Add combines disjoint groups of source records.
func (o *Outcomes) Add(other Outcomes) {
	o.Imported += other.Imported
	o.Replaced += other.Replaced
	o.Skipped += other.Skipped
	o.Error += other.Error
}

// Result is an immutable summary. Nil totals mean historical counts are missing.
type Result struct {
	// Version identifies the JSON statistics contract.
	Version int `json:"version"`
	// Total counts distinct source records read by the logical task.
	Total *uint64 `json:"total"`
	Outcomes
	// Unresolved counts processed inputs without a confirmed final category.
	Unresolved *uint64 `json:"unresolved"`
	// Complete is true when Total is known and every input is classified.
	Complete bool `json:"complete"`
}

// NewResult derives Unresolved from one coherent snapshot of confirmed outcomes.
func NewResult(total uint64, known bool, outcomes Outcomes) Result {
	r := Result{Version: 1, Outcomes: outcomes}
	if known && outcomes.Count() <= total {
		unresolved := total - outcomes.Count()
		r.Total, r.Unresolved = &total, &unresolved
		r.Complete = unresolved == 0
	}
	return r
}

// Store persists a versioned ledger independently of import control checkpoints.
// A failed statistics write must never change whether a source write is retried.
type Store interface {
	// Load returns the stored ledger, or an empty slice for a new task.
	Load(context.Context) ([]byte, error)
	// Save persists a complete ledger snapshot.
	Save(context.Context, []byte) error
}

type unit struct {
	Table          string         `json:"table"`
	High           int64          `json:"high"`
	Ack            int64          `json:"ack"`
	Fence          int64          `json:"fence"`
	Total          uint64         `json:"total"`
	Known          bool           `json:"known"`
	Outcomes       Outcomes       `json:"outcomes"`
	Staged         uint64         `json:"staged"`
	StagedReplaced uint64         `json:"staged-replaced,omitempty"`
	RetryErrors    map[int64]bool `json:"retry-errors,omitempty"`
	// Replay is process-local: a persisted write fence is ambiguous after a crash.
	Replay int64 `json:"-"`
}

type state struct {
	Scope   string           `json:"scope,omitempty"`
	Version int              `json:"version"`
	Running bool             `json:"running"`
	Known   bool             `json:"known"`
	Units   map[string]*unit `json:"units"`
}

// IncrementalStore avoids rewriting every chunk on each checkpoint update.
// Metadata and changed entries must be committed together in SaveEntries.
type IncrementalStore interface {
	Store
	// SaveEntries atomically replaces metadata and changed entries; reset starts a new task.
	SaveEntries(ctx context.Context, metadata []byte, entries map[string][]byte, reset bool) error
}

// Ledger collects whole-task counts. Source offsets and absolute snapshots make
// parser replay idempotent even when statistics are ahead of import checkpoints.
type Ledger struct {
	recoveredIncomplete bool
	mu                  sync.Mutex
	storeMu             sync.Mutex
	closed              bool
	dirty               map[string]bool
	reset               bool
	state               state
	store               Store
	err                 error
}

// Open restores statistics. An unclean exit may have lost a read-ahead tail, so
// its historical total is unknown; already confirmed outcomes remain available.
func Open(ctx context.Context, store Store, resumed bool) *Ledger {
	l := &Ledger{dirty: make(map[string]bool), reset: !resumed, store: store, state: state{Version: 1, Known: !resumed, Units: make(map[string]*unit)}}
	if store != nil {
		data, err := store.Load(ctx)
		if err != nil {
			l.err = err
			l.state.Known = false
		}
		if resumed && len(data) > 0 && err == nil {
			var saved state
			if err = json.Unmarshal(data, &saved); err == nil && validState(saved) {
				l.state = saved
				if saved.Running {
					l.state.Known = false
				}
				for _, u := range l.state.Units {
					u.Replay = max(u.Ack, u.Fence)
				}
			} else {
				l.err = fmt.Errorf("unsupported or invalid import statistics")
				l.reset = true
				l.state.Known = false
			}
		}
	}
	// A failed pre-write statistics save may itself have been lost in a crash.
	// Beyond the persisted fence, replay may still see the previous attempt's
	// writes. Keep those write roles unknown when the process history is incomplete.
	l.recoveredIncomplete = resumed && (!l.state.Known || l.state.Running)
	l.state.Running = true
	l.save(ctx)
	return l
}

// Ensure registers a chunk. Missing statistics for an already advanced chunk
// must not be mistaken for a new chunk with zero historical records.
func (l *Ledger) Ensure(key, table string, start, checkpoint int64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	if _, ok := l.state.Units[key]; !ok {
		l.dirty[key] = true
		l.state.Units[key] = &unit{Table: table, High: start - 1, Ack: start - 1, Fence: start - 1, Replay: checkpoint - 1, Known: checkpoint == start}
		if checkpoint > start {
			l.state.Known = false
		}
	}
}

// Observe counts a source record once. kind may classify a conversion failure or
// a policy skip immediately; ordinary rows wait for target acknowledgement.
func (l *Ledger) Observe(key string, offset int64, kind Outcomes) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	u := l.state.Units[key]
	if u == nil {
		return false
	}
	if offset <= u.High {
		// Fatal known failures may be retried from an older control checkpoint.
		// Move that input back to unresolved before observing its new outcome.
		if u.RetryErrors[offset] && offset > max(u.Ack, u.Replay) {
			delete(u.RetryErrors, offset)
			l.dirty[key] = true
			u.Outcomes.Error--
			return true
		}
		return false
	}
	l.dirty[key] = true
	u.High = offset
	u.Total++
	u.Outcomes.Add(kind)
	return true
}

// ClassifyRead classifies a freshly observed conversion failure or policy skip.
func (l *Ledger) ClassifyRead(key string, offset int64, fresh bool, o Outcomes) {
	if l == nil || !fresh {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	u := l.state.Units[key]
	l.dirty[key] = true
	u.Outcomes.Add(o)
	if o.Error > 0 {
		if u.RetryErrors == nil {
			u.RetryErrors = make(map[int64]bool)
		}
		u.RetryErrors[offset] = true
	}
}

// Eligible reports whether an outcome belongs to an unclassified write range.
func (l *Ledger) Eligible(key string, offset int64) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.recoveredIncomplete {
		return false
	}
	u := l.state.Units[key]
	return u != nil && offset > max(u.Ack, u.Replay)
}

// BeginWrite durably fences the input range before target writes. A restart
// keeps an unacknowledged fenced range unresolved instead of reclassifying it
// from a replayed REPLACE that may see the previous attempt's own inserted rows.
func (l *Ledger) BeginWrite(ctx context.Context, key string, end int64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	if u := l.state.Units[key]; u != nil {
		l.dirty[key] = true
		u.Fence = max(u.Fence, end)
	}
	l.mu.Unlock()
	l.save(ctx)
}

// MarkAmbiguous keeps a write range unresolved across subsequent retries.
func (l *Ledger) MarkAmbiguous(ctx context.Context, key string, end int64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	if u := l.state.Units[key]; u != nil {
		l.dirty[key] = true
		u.Replay = max(u.Replay, end)
		u.Fence = max(u.Fence, end)
	}
	l.mu.Unlock()
	l.save(ctx)
}

// RollbackWrite clears a fence when the server proves the statement rolled back.
func (l *Ledger) RollbackWrite(ctx context.Context, key string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	if u := l.state.Units[key]; u != nil {
		l.dirty[key] = true
		u.Fence = max(u.Ack, u.Replay)
	}
	l.mu.Unlock()
	l.save(ctx)
}

// Reject records definitive row failures without advancing the acknowledged
// write frontier, allowing a later resumed attempt to reclassify these inputs.
func (l *Ledger) Reject(ctx context.Context, key string, offsets []int64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed || l.recoveredIncomplete {
		l.mu.Unlock()
		return
	}
	u := l.state.Units[key]
	if u == nil {
		l.mu.Unlock()
		return
	}
	l.dirty[key] = true
	if u.RetryErrors == nil {
		u.RetryErrors = make(map[int64]bool)
	}
	for _, offset := range offsets {
		if offset > max(u.Ack, u.Replay) && !u.RetryErrors[offset] {
			u.RetryErrors[offset] = true
			u.Outcomes.Error++
		}
	}
	u.Fence = max(u.Ack, u.Replay)
	l.mu.Unlock()
	l.save(ctx)
}

// FinishWrite records confirmed outcomes. SQL batches must not straddle the
// Eligible boundary; local writes stage candidates until data and indexes land.
func (l *Ledger) FinishWrite(ctx context.Context, key string, end int64, o Outcomes, staged, stagedReplaced uint64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	u := l.state.Units[key]
	if !l.recoveredIncomplete && u != nil && end > max(u.Ack, u.Replay) {
		l.dirty[key] = true
		u.Outcomes.Add(o)
		u.Staged += staged
		u.StagedReplaced += stagedReplaced
		u.Ack = end
		for offset := range u.RetryErrors {
			if offset <= end {
				delete(u.RetryErrors, offset)
			}
		}
	}
	l.mu.Unlock()
	l.save(ctx)
}

// ConfirmTable acknowledges staged local rows only when conflict resolution
// proves an ordinary insertion. A local file write alone is not an import.
func (l *Ledger) ConfirmTable(ctx context.Context, table string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	for key, u := range l.state.Units {
		if u.Table == table {
			l.dirty[key] = true
			u.Outcomes.Imported += u.Staged
			u.Outcomes.Replaced += u.StagedReplaced
			u.Staged = 0
			u.StagedReplaced = 0
		}
	}
	l.mu.Unlock()
	l.save(ctx)
}

// Snapshot returns table and task summaries from the same locked state.
func (l *Ledger) Snapshot() (map[string]Result, Result) {
	l.mu.Lock()
	defer l.mu.Unlock()
	type aggregate struct {
		total    uint64
		outcomes Outcomes
		known    bool
	}
	tables := make(map[string]*aggregate)
	all := aggregate{known: l.state.Known}
	for _, u := range l.state.Units {
		a := tables[u.Table]
		if a == nil {
			a = &aggregate{known: l.state.Known}
			tables[u.Table] = a
		}
		a.total += u.Total
		a.outcomes.Add(u.Outcomes)
		a.known = a.known && u.Known
		all.total += u.Total
		all.outcomes.Add(u.Outcomes)
		all.known = all.known && u.Known
	}
	results := make(map[string]Result, len(tables))
	for name, a := range tables {
		results[name] = NewResult(a.total, a.known, a.outcomes)
	}
	return results, NewResult(all.total, all.known, all.outcomes)
}

// Close flushes the read-ahead tail before checkpoints are cleaned or renamed.
func (l *Ledger) Close(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	l.closed = true
	l.state.Running = false
	l.mu.Unlock()
	l.saveState(ctx, true)
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *Ledger) save(ctx context.Context) { l.saveState(ctx, false) }

func (l *Ledger) saveState(ctx context.Context, final bool) {
	if l.store == nil {
		return
	}
	// Serialize store writes, then take the latest state. Capturing a snapshot
	// before storeMu would allow a stale acknowledgement to overwrite a newer one.
	l.storeMu.Lock()
	defer l.storeMu.Unlock()
	l.mu.Lock()
	if l.closed && !final {
		l.mu.Unlock()
		return
	}
	var err error
	var attemptedEntries map[string][]byte
	var attemptedReset bool
	if store, ok := l.store.(IncrementalStore); ok {
		entries := make(map[string][]byte, len(l.dirty))
		for key := range l.dirty {
			entries[key], err = json.Marshal(l.state.Units[key])
			if err != nil {
				break
			}
		}
		metadata := l.state
		metadata.Units = nil
		var data []byte
		if err == nil {
			data, err = json.Marshal(metadata)
		}
		reset := l.reset
		attemptedEntries = entries
		attemptedReset = reset
		l.dirty = make(map[string]bool)
		l.reset = false
		l.mu.Unlock()
		if err == nil {
			err = store.SaveEntries(ctx, data, entries, reset)
		}
	} else {
		var data []byte
		data, err = json.Marshal(l.state)
		l.mu.Unlock()
		if err == nil {
			err = l.store.Save(ctx, data)
		}
	}
	if err != nil {
		l.mu.Lock()
		l.err = err
		l.reset = l.reset || attemptedReset
		for key := range attemptedEntries {
			l.dirty[key] = true
		}
		l.state.Known = false
		l.mu.Unlock()
	}
}

type progressKey struct{}

// Progress associates a target writer with one sequential source chunk.
type Progress struct {
	// Ledger receives outcomes for this writer.
	Ledger *Ledger
	// Key identifies its sequential source chunk.
	Key string
}

// WithProgress carries statistics through a writer without changing its API.
func WithProgress(ctx context.Context, ledger *Ledger, key string) context.Context {
	if ledger == nil {
		return ctx
	}
	return context.WithValue(ctx, progressKey{}, &Progress{Ledger: ledger, Key: key})
}

// FromContext returns the source progress attached by Lightning, if present.
func FromContext(ctx context.Context) *Progress {
	p, _ := ctx.Value(progressKey{}).(*Progress)
	return p
}

// OpenSnapshots restores absolute job snapshots, whose totals do not depend on
// recovering a parser read-ahead tail. A new group never inherits a prior task.
func OpenSnapshots(ctx context.Context, store Store, scope string) *Ledger {
	l := Open(ctx, store, true)
	l.recoveredIncomplete = false
	if l.state.Scope != scope {
		l.state = state{Version: 1, Scope: scope, Units: make(map[string]*unit)}
		l.dirty = make(map[string]bool)
		l.reset = true
	}
	l.state.Known = true
	l.save(ctx)
	return l
}

// RegisterResult makes missing historical table statistics visible as unknown.
func (l *Ledger) RegisterResult(key, table string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	if l.state.Units[key] == nil {
		l.state.Units[key] = &unit{Table: table}
		l.dirty[key] = true
	}
}

// ReplaceResult replaces an absolute table/job generation snapshot. Polling and
// retry jobs never add an attempt's rows to the previous attempt's total.
func (l *Ledger) ReplaceResult(ctx context.Context, key, table string, result Result) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	u := &unit{Table: table, Outcomes: result.Outcomes}
	if result.Version == 1 && result.Total != nil {
		u.Total = *result.Total
		u.Known = true
	}
	l.dirty[key] = true
	l.state.Units[key] = u
	l.mu.Unlock()
	l.save(ctx)
}

func validState(saved state) bool {
	if saved.Version != 1 || saved.Units == nil {
		return false
	}
	for _, u := range saved.Units {
		if u == nil {
			return false
		}
		if u.Known {
			remaining := u.Total
			for _, count := range []uint64{u.Outcomes.Imported, u.Outcomes.Replaced, u.Outcomes.Skipped, u.Outcomes.Error, u.Staged, u.StagedReplaced} {
				if count > remaining {
					return false
				}
				remaining -= count
			}
		}
	}
	return true
}
