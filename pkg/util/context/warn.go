// Copyright 2023 PingCAP, Inc.
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

package context

import (
	"encoding/json"
	"math"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
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

type jsonSQLWarn struct {
	Level  string        `json:"level"`
	SQLErr *terror.Error `json:"err,omitempty"`
	Msg    string        `json:"msg,omitempty"`
}

// MarshalJSON implements the Marshaler.MarshalJSON interface.
func (warn *SQLWarn) MarshalJSON() ([]byte, error) {
	w := &jsonSQLWarn{
		Level: warn.Level,
	}
	e := errors.Cause(warn.Err)
	switch x := e.(type) {
	case *terror.Error:
		// Omit outter errors because only the most inner error matters.
		w.SQLErr = x
	default:
		w.Msg = e.Error()
	}
	return json.Marshal(w)
}

// UnmarshalJSON implements the Unmarshaler.UnmarshalJSON interface.
func (warn *SQLWarn) UnmarshalJSON(data []byte) error {
	var w jsonSQLWarn
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	warn.Level = w.Level
	if w.SQLErr != nil {
		warn.Err = w.SQLErr
	} else {
		warn.Err = errors.New(w.Msg)
	}
	return nil
}

// WarnAppender provides a function to add a warning.
// Using interface rather than a simple function/closure can avoid memory allocation in some cases.
// See https://github.com/pingcap/tidb/issues/49277
type WarnAppender interface {
	// AppendWarning appends a warning
	AppendWarning(err error)
	// AppendNote appends a warning with level 'Note'.
	AppendNote(msg error)
}

// WarnHandler provides a handler to append and get warnings.
type WarnHandler interface {
	WarnAppender
	// WarningCount gets warning count.
	WarningCount() int
	// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
	//
	// Deprecated: This method is deprecated. Because it's unsafe to read the warnings returned by `GetWarnings`
	// after truncate and append the warnings.
	//
	// Currently it's used in two cases and they all have better alternatives:
	// 1. Read warnings count, do some operation and truncate warnings to read new warnings. In this case, we
	//   can use a new temporary WarnHandler to do the operation and get the warnings without touching the
	//   global `WarnHandler` in the statement context.
	// 2. Read warnings count, do some operation and truncate warnings to see whether new warnings are appended.
	//   In this case, we can use a specially designed `WarnHandler` which doesn't actually record warnings, but
	//   just counts whether new warnings are appended.
	// It's understandable to use `TruncateWarnings` as it's not always easy to assign a new `WarnHandler` to the
	// context now.
	// TODO: Make it easier to assign a new `WarnHandler` to the context (of `table` and other packages) and remove
	// this method.
	TruncateWarnings(start int) []SQLWarn
	// CopyWarnings copies warnings to another slice.
	// The input argument provides target warnings that copy to.
	// If the dist capacity is not enough, it will allocate a new slice.
	CopyWarnings(dst []SQLWarn) []SQLWarn
}

// WarnHandlerExt includes more methods for WarnHandler. It allows more detailed control over warnings.
// TODO: it's a standalone interface, because it's not necessary for all WarnHandler to implement these methods.
// However, it's still needed for many executors, so we'll see whether it's good to merge it with `WarnAppender`
// and `WarnHandler` in the future.
type WarnHandlerExt interface {
	WarnHandler

	// AppendWarnings appends multiple warnings
	AppendWarnings(warns []SQLWarn)
	// AppendNote appends a warning with level 'Note'.
	AppendNote(warn error)
	// AppendError appends a warning with level 'Error'.
	AppendError(warn error)

	// GetWarnings gets all warnings. The slice is not copied, so it should not be modified.
	GetWarnings() []SQLWarn
	// SetWarnings resets all warnings in the handler directly. The handler may ignore the given warnings.
	SetWarnings(warns []SQLWarn)
	// SetWarningsWithCount resets the available warning details and their logical total.
	SetWarningsWithCount(warns []SQLWarn, total uint16)
	// AddOmittedWarnings adds warnings whose details are unavailable.
	AddOmittedWarnings(count uint64)
	// CopyWarningsWithCount copies the available warning details and their logical total.
	CopyWarningsWithCount(dst []SQLWarn) ([]SQLWarn, uint16)
	// NumErrorWarnings returns the number of warnings with level 'Error' and the total number of warnings.
	NumErrorWarnings() (uint16, int)
}

var _ WarnHandlerExt = &StaticWarnHandler{}

// StaticWarnHandler implements the WarnHandler interface.
type StaticWarnHandler struct {
	sync.Mutex
	warnings     []SQLWarn
	warningCount int
}

// NewStaticWarnHandler creates a new StaticWarnHandler.
func NewStaticWarnHandler(sliceCap int) *StaticWarnHandler {
	var warnings []SQLWarn
	if sliceCap > 0 {
		warnings = make([]SQLWarn, 0, sliceCap)
	}
	return &StaticWarnHandler{warnings: warnings}
}

// NewStaticWarnHandlerWithHandler creates a new StaticWarnHandler with copying the warnings from the given WarnHandler.
func NewStaticWarnHandlerWithHandler(h WarnHandler) *StaticWarnHandler {
	if h == nil {
		return NewStaticWarnHandler(0)
	}

	newHandler := NewStaticWarnHandler(0)
	if ext, ok := h.(WarnHandlerExt); ok {
		var count uint16
		newHandler.warnings, count = ext.CopyWarningsWithCount(nil)
		newHandler.warningCount = int(count)
		return newHandler
	}
	newHandler.warningCount = h.WarningCount()
	if newHandler.warningCount > 0 {
		newHandler.warnings = h.CopyWarnings(nil)
	}
	return newHandler
}

// Reset resets the warnings of this handler.
func (h *StaticWarnHandler) Reset() {
	h.Lock()
	defer h.Unlock()
	if h.warnings != nil {
		h.warnings = h.warnings[:0]
	}
	h.warningCount = 0
}

// AppendWarning implements the StaticWarnHandler.AppendWarning.
func (h *StaticWarnHandler) AppendWarning(warn error) {
	h.Lock()
	defer h.Unlock()

	h.appendWarningWithLevel(WarnLevelWarning, warn)
}

// AppendWarnings appends multiple warnings
func (h *StaticWarnHandler) AppendWarnings(warns []SQLWarn) {
	h.Lock()
	defer h.Unlock()
	if h.warningCount >= math.MaxUint16 || len(warns) == 0 {
		return
	}
	appendCount := min(len(warns), math.MaxUint16-h.warningCount)
	if h.warningCount == len(h.warnings) {
		h.warnings = append(h.warnings, warns[:appendCount]...)
	}
	h.warningCount += appendCount
}

// AppendNote appends a warning with level 'Note'.
func (h *StaticWarnHandler) AppendNote(warn error) {
	h.Lock()
	defer h.Unlock()

	h.appendWarningWithLevel(WarnLevelNote, warn)
}

// AppendError appends a warning with level 'Error'.
func (h *StaticWarnHandler) AppendError(warn error) {
	h.Lock()
	defer h.Unlock()

	h.appendWarningWithLevel(WarnLevelError, warn)
}

// WarningCount implements the StaticWarnHandler.WarningCount.
func (h *StaticWarnHandler) WarningCount() int {
	h.Lock()
	defer h.Unlock()
	return h.warningCount
}

// TruncateWarnings implements the StaticWarnHandler.TruncateWarnings.
func (h *StaticWarnHandler) TruncateWarnings(start int) []SQLWarn {
	h.Lock()
	defer h.Unlock()
	if start >= h.warningCount {
		return nil
	}
	if start < 0 {
		start = 0
	}
	detailStart := min(len(h.warnings), start)
	sz := len(h.warnings) - detailStart
	if sz <= 0 {
		h.warningCount = start
		return nil
	}
	ret := make([]SQLWarn, sz)
	copy(ret, h.warnings[detailStart:])
	h.warnings = h.warnings[:detailStart]
	h.warningCount = start
	return ret
}

// CopyWarnings implements the StaticWarnHandler.CopyWarnings.
func (h *StaticWarnHandler) CopyWarnings(dst []SQLWarn) []SQLWarn {
	h.Lock()
	defer h.Unlock()
	if cnt := len(h.warnings); cap(dst) < cnt {
		dst = make([]SQLWarn, cnt)
	} else {
		dst = dst[:cnt]
	}
	copy(dst, h.warnings)
	return dst
}

// GetWarnings returns all warnings in the handler. It's not safe to modify the returned slice.
func (h *StaticWarnHandler) GetWarnings() []SQLWarn {
	h.Mutex.Lock()
	defer h.Mutex.Unlock()

	return h.warnings
}

func (h *StaticWarnHandler) appendWarningWithLevel(level string, warn error) {
	if h.warningCount >= math.MaxUint16 {
		return
	}
	if h.warningCount == len(h.warnings) {
		h.warnings = append(h.warnings, SQLWarn{level, warn})
	}
	h.warningCount++
}

// SetWarnings sets the internal warnings directly.
func (h *StaticWarnHandler) SetWarnings(warns []SQLWarn) {
	h.Lock()
	defer h.Unlock()

	h.warningCount = min(len(warns), math.MaxUint16)
	h.warnings = warns[:h.warningCount]
}

// SetWarningsWithCount sets the available warning details and their logical total.
func (h *StaticWarnHandler) SetWarningsWithCount(warns []SQLWarn, total uint16) {
	h.Lock()
	defer h.Unlock()

	detailCount := min(len(warns), math.MaxUint16)
	h.warnings = warns[:detailCount]
	h.warningCount = max(detailCount, int(total))
}

// AddOmittedWarnings adds warnings without materializing unavailable details.
func (h *StaticWarnHandler) AddOmittedWarnings(count uint64) {
	h.Lock()
	defer h.Unlock()

	remaining := uint64(max(0, math.MaxUint16-h.warningCount))
	omitted := int(min(count, remaining))
	h.warningCount += omitted
}

// CopyWarningsWithCount copies warning details and their logical total under one lock.
func (h *StaticWarnHandler) CopyWarningsWithCount(dst []SQLWarn) ([]SQLWarn, uint16) {
	h.Lock()
	defer h.Unlock()
	if cnt := len(h.warnings); cap(dst) < cnt {
		dst = make([]SQLWarn, cnt)
	} else {
		dst = dst[:cnt]
	}
	copy(dst, h.warnings)
	return dst, uint16(h.warningCount)
}

// NumErrorWarnings returns the number of warnings with level 'Error' and the total number of warnings.
func (h *StaticWarnHandler) NumErrorWarnings() (uint16, int) {
	h.Lock()
	defer h.Unlock()

	var numError uint16
	for _, w := range h.warnings {
		if w.Level == WarnLevelError {
			numError++
		}
	}
	return numError, h.warningCount
}

type ignoreWarn struct{}

func (*ignoreWarn) AppendWarning(_ error) {}

func (*ignoreWarn) AppendNote(_ error) {}

func (*ignoreWarn) WarningCount() int { return 0 }

func (*ignoreWarn) TruncateWarnings(_ int) []SQLWarn { return nil }

func (*ignoreWarn) CopyWarnings(_ []SQLWarn) []SQLWarn { return nil }

// IgnoreWarn is WarnHandler which does nothing
var IgnoreWarn WarnHandler = &ignoreWarn{}

type funcWarnAppender struct {
	fn func(level string, err error)
}

func (r *funcWarnAppender) AppendWarning(err error) {
	r.fn(WarnLevelWarning, err)
}

func (r *funcWarnAppender) AppendNote(err error) {
	r.fn(WarnLevelNote, err)
}

// NewFuncWarnAppenderForTest creates a `WarnHandler` which will use the function to handle warn
// To have a better performance, it's not suggested to use this function in production.
func NewFuncWarnAppenderForTest(fn func(level string, err error)) WarnAppender {
	return &funcWarnAppender{fn}
}
