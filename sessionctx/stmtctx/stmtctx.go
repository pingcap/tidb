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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
)

const (
	// WarnLevelError represents level "Error" for 'SHOW WARNINGS' syntax.
	WarnLevelError = "Error"
	// WarnLevelWarning represents level "Warning" for 'SHOW WARNINGS' syntax.
	WarnLevelWarning = "Warning"
	// WarnLevelNote represents level "Note" for 'SHOW WARNINGS' syntax.
	WarnLevelNote = "Note"
)

// SQLWarn relates a sql warning and it's level.
type SQLWarn struct {
	Level string
	Err   error
}

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {
	// Set the following variables before execution

	// IsDDLJobInQueue is used to mark whether the DDL job is put into the queue.
	// If IsDDLJobInQueue is true, it means the DDL job is in the queue of storage, and it can be handled by the DDL worker.
	IsDDLJobInQueue        bool
	InInsertStmt           bool
	InUpdateStmt           bool
	InDeleteStmt           bool
	InSelectStmt           bool
	InLoadDataStmt         bool
	IgnoreTruncate         bool
	IgnoreZeroInDate       bool
	DupKeyAsWarning        bool
	BadNullAsWarning       bool
	DividedByZeroAsWarning bool
	TruncateAsWarning      bool
	OverflowAsWarning      bool
	InShowWarning          bool
	UseCache               bool
	PadCharToFullLength    bool
	BatchCheck             bool
	InNullRejectCheck      bool
	AllowInvalidDate       bool
	// CastStrToIntStrict is used to control the way we cast float format string to int.
	// If ConvertStrToIntStrict is false, we convert it to a valid float string first,
	// then cast the float string to int string. Otherwise, we cast string to integer
	// prefix in a strict way, only extract 0-9 and (+ or - in first bit).
	CastStrToIntStrict bool

	// StartTime is the query start time.
	StartTime time.Time
	// DurationParse is the duration of pasing SQL string to AST.
	DurationParse time.Duration
	// DurationCompile is the duration of compiling AST to execution plan.
	DurationCompile time.Duration

	// mu struct holds variables that change during execution.
	mu struct {
		sync.Mutex
		affectedRows      uint64
		foundRows         uint64
		warnings          []SQLWarn
		histogramsNotLoad bool
		execDetails       execdetails.ExecDetails
		allExecDetails    []*execdetails.ExecDetails
	}
	// PrevAffectedRows is the affected-rows value(DDL is 0, DML is the number of affected rows).
	PrevAffectedRows int64
	// PrevLastInsertID is the last insert ID of previous statement.
	PrevLastInsertID uint64
	// LastInsertID is the auto-generated ID in the current statement.
	LastInsertID uint64
	// InsertID is the given insert ID of an auto_increment column.
	InsertID uint64

	// Copied from SessionVars.TimeZone.
	TimeZone         *time.Location
	Priority         mysql.PriorityEnum
	NotFillCache     bool
	MemTracker       *memory.Tracker
	RuntimeStatsColl *execdetails.RuntimeStatsColl
	TableIDs         []int64
	IndexNames       []string
	nowTs            time.Time // use this variable for now/current_timestamp calculation/cache for one stmt
	stmtTimeCached   bool
	StmtType         string
	Tables           []TableEntry
	OriginalSQL      string
	digestMemo       struct {
		sync.Once
		normalized string
		digest     string
	}
}

// GetNowTsCached getter for nowTs, if not set get now time and cache it
func (sc *StatementContext) GetNowTsCached() time.Time {
	if !sc.stmtTimeCached {
		now := time.Now()
		sc.nowTs = now
		sc.stmtTimeCached = true
	}
	return sc.nowTs
}

// ResetNowTs resetter for nowTs, clear cached time flag
func (sc *StatementContext) ResetNowTs() {
	sc.stmtTimeCached = false
}

// SQLDigest gets normalized and digest for provided sql.
// it will cache result after first calling.
func (sc *StatementContext) SQLDigest() (normalized, sqlDigest string) {
	sc.digestMemo.Do(func() {
		sc.digestMemo.normalized, sc.digestMemo.digest = parser.NormalizeDigest(sc.OriginalSQL)
	})
	return sc.digestMemo.normalized, sc.digestMemo.digest
}

// TableEntry presents table in db.
type TableEntry struct {
	DB    string
	Table string
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
func (sc *StatementContext) GetWarnings() []SQLWarn {
	sc.mu.Lock()
	warns := make([]SQLWarn, len(sc.mu.warnings))
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

// NumWarnings gets warning count. It's different from `WarningCount` in that
// `WarningCount` return the warning count of the last executed command, so if
// the last command is a SHOW statement, `WarningCount` return 0. On the other
// hand, `NumWarnings` always return number of warnings(or errors if `errOnly`
// is set).
func (sc *StatementContext) NumWarnings(errOnly bool) uint16 {
	var wc uint16
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if errOnly {
		for _, warn := range sc.mu.warnings {
			if warn.Level == WarnLevelError {
				wc++
			}
		}
	} else {
		wc = uint16(len(sc.mu.warnings))
	}
	return wc
}

// SetWarnings sets warnings.
func (sc *StatementContext) SetWarnings(warns []SQLWarn) {
	sc.mu.Lock()
	sc.mu.warnings = warns
	sc.mu.Unlock()
}

// AppendWarning appends a warning with level 'Warning'.
func (sc *StatementContext) AppendWarning(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelWarning, warn})
	}
	sc.mu.Unlock()
}

// AppendNote appends a warning with level 'Note'.
func (sc *StatementContext) AppendNote(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelNote, warn})
	}
	sc.mu.Unlock()
}

// AppendError appends a warning with level 'Error'.
func (sc *StatementContext) AppendError(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelError, warn})
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
	sc.mu.execDetails = execdetails.ExecDetails{}
	sc.mu.allExecDetails = make([]*execdetails.ExecDetails, 0, 4)
	sc.mu.Unlock()
	sc.TableIDs = sc.TableIDs[:0]
	sc.IndexNames = sc.IndexNames[:0]
	sc.StartTime = time.Now()
	sc.DurationCompile = time.Duration(0)
	sc.DurationParse = time.Duration(0)
}

// MergeExecDetails merges a single region execution details into self, used to print
// the information in slow query log.
func (sc *StatementContext) MergeExecDetails(details *execdetails.ExecDetails, commitDetails *execdetails.CommitDetails) {
	sc.mu.Lock()
	if details != nil {
		sc.mu.execDetails.ProcessTime += details.ProcessTime
		sc.mu.execDetails.WaitTime += details.WaitTime
		sc.mu.execDetails.BackoffTime += details.BackoffTime
		sc.mu.execDetails.RequestCount++
		sc.mu.execDetails.TotalKeys += details.TotalKeys
		sc.mu.execDetails.ProcessedKeys += details.ProcessedKeys
		sc.mu.allExecDetails = append(sc.mu.allExecDetails, details)
	}
	sc.mu.execDetails.CommitDetail = commitDetails
	sc.mu.Unlock()
}

// GetExecDetails gets the execution details for the statement.
func (sc *StatementContext) GetExecDetails() execdetails.ExecDetails {
	var details execdetails.ExecDetails
	sc.mu.Lock()
	details = sc.mu.execDetails
	sc.mu.Unlock()
	return details
}

// ShouldClipToZero indicates whether values less than 0 should be clipped to 0 for unsigned integer types.
// This is the case for `insert`, `update`, `alter table` and `load data infile` statements, when not in strict SQL mode.
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
func (sc *StatementContext) ShouldClipToZero() bool {
	// TODO: Currently altering column of integer to unsigned integer is not supported.
	// If it is supported one day, that case should be added here.
	return sc.InInsertStmt || sc.InLoadDataStmt
}

// ShouldIgnoreOverflowError indicates whether we should ignore the error when type conversion overflows,
// so we can leave it for further processing like clipping values less than 0 to 0 for unsigned integer types.
func (sc *StatementContext) ShouldIgnoreOverflowError() bool {
	if (sc.InInsertStmt && sc.TruncateAsWarning) || sc.InLoadDataStmt {
		return true
	}
	return false
}

// CopTasksDetails returns some useful information of cop-tasks during execution.
func (sc *StatementContext) CopTasksDetails() *CopTasksDetails {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	n := len(sc.mu.allExecDetails)
	d := &CopTasksDetails{NumCopTasks: n}
	if n == 0 {
		return d
	}
	d.AvgProcessTime = sc.mu.execDetails.ProcessTime / time.Duration(n)
	d.AvgWaitTime = sc.mu.execDetails.WaitTime / time.Duration(n)

	sort.Slice(sc.mu.allExecDetails, func(i, j int) bool {
		return sc.mu.allExecDetails[i].ProcessTime < sc.mu.allExecDetails[j].ProcessTime
	})
	d.P90ProcessTime = sc.mu.allExecDetails[n*9/10].ProcessTime
	d.MaxProcessTime = sc.mu.allExecDetails[n-1].ProcessTime

	sort.Slice(sc.mu.allExecDetails, func(i, j int) bool {
		return sc.mu.allExecDetails[i].WaitTime < sc.mu.allExecDetails[j].WaitTime
	})
	d.P90WaitTime = sc.mu.allExecDetails[n*9/10].WaitTime
	d.MaxWaitTime = sc.mu.allExecDetails[n-1].WaitTime
	return d
}

//CopTasksDetails collects some useful information of cop-tasks during execution.
type CopTasksDetails struct {
	NumCopTasks int

	AvgProcessTime time.Duration
	P90ProcessTime time.Duration
	MaxProcessTime time.Duration

	AvgWaitTime time.Duration
	P90WaitTime time.Duration
	MaxWaitTime time.Duration
}
