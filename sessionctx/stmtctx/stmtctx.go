// Copyright 2017 PingCAP, Inc.
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

package stmtctx

import (
	"math"
	"sync"
	"time"

	"github.com/pingcap/tidb/mysql"
)

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {
	// Set the following variables before execution

	InInsertStmt           bool
	InUpdateOrDeleteStmt   bool
	InSelectStmt           bool
	IgnoreTruncate         bool
	IgnoreZeroInDate       bool
	DividedByZeroAsWarning bool
	TruncateAsWarning      bool
	OverflowAsWarning      bool
	InShowWarning          bool
	UseCache               bool
	PadCharToFullLength    bool
	BatchCheck             bool

	// mu struct holds variables that change during execution.
	mu struct {
		sync.Mutex
		affectedRows      uint64
		foundRows         uint64
		warnings          []error
		histogramsNotLoad bool
	}

	// Copied from SessionVars.TimeZone.
	TimeZone     *time.Location
	Priority     mysql.PriorityEnum
	NotFillCache bool
}

// AddAffectedRows adds affected rows.
func (sc *StatementContext) AddAffectedRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.affectedRows += rows
	sc.mu.Unlock()
}

// AffectedRows gets affected rows.
func (sc *StatementContext) AffectedRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.affectedRows
	sc.mu.Unlock()
	return rows
}

// FoundRows gets found rows.
func (sc *StatementContext) FoundRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.foundRows
	sc.mu.Unlock()
	return rows
}

// AddFoundRows adds found rows.
func (sc *StatementContext) AddFoundRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.foundRows += rows
	sc.mu.Unlock()
}

// GetWarnings gets warnings.
func (sc *StatementContext) GetWarnings() []error {
	sc.mu.Lock()
	warns := make([]error, len(sc.mu.warnings))
	copy(warns, sc.mu.warnings)
	sc.mu.Unlock()
	return warns
}

// WarningCount gets warning count.
func (sc *StatementContext) WarningCount() uint16 {
	if sc.InShowWarning {
		return 0
	}
	sc.mu.Lock()
	wc := uint16(len(sc.mu.warnings))
	sc.mu.Unlock()
	return wc
}

// SetWarnings sets warnings.
func (sc *StatementContext) SetWarnings(warns []error) {
	sc.mu.Lock()
	sc.mu.warnings = warns
	sc.mu.Unlock()
}

// AppendWarning appends a warning.
func (sc *StatementContext) AppendWarning(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, warn)
	}
	sc.mu.Unlock()
}

// SetHistogramsNotLoad sets histogramsNotLoad.
func (sc *StatementContext) SetHistogramsNotLoad() {
	sc.mu.Lock()
	sc.mu.histogramsNotLoad = true
	sc.mu.Unlock()
}

// HistogramsNotLoad gets histogramsNotLoad.
func (sc *StatementContext) HistogramsNotLoad() bool {
	sc.mu.Lock()
	notLoad := sc.mu.histogramsNotLoad
	sc.mu.Unlock()
	return notLoad
}

// HandleTruncate ignores or returns the error based on the StatementContext state.
func (sc *StatementContext) HandleTruncate(err error) error {
	// TODO: At present we have not checked whether the error can be ignored or treated as warning.
	// We will do that later, and then append WarnDataTruncated instead of the error itself.
	if err == nil {
		return nil
	}
	if sc.IgnoreTruncate {
		return nil
	}
	if sc.TruncateAsWarning {
		sc.AppendWarning(err)
		return nil
	}
	return err
}

// HandleOverflow treats ErrOverflow as warnings or returns the error based on the StmtCtx.OverflowAsWarning state.
func (sc *StatementContext) HandleOverflow(err error, warnErr error) error {
	if err == nil {
		return nil
	}

	if sc.OverflowAsWarning {
		sc.AppendWarning(warnErr)
		return nil
	}
	return err
}

// ResetForRetry resets the changed states during execution.
func (sc *StatementContext) ResetForRetry() {
	sc.mu.Lock()
	sc.mu.affectedRows = 0
	sc.mu.foundRows = 0
	sc.mu.warnings = nil
	sc.mu.Unlock()
}
