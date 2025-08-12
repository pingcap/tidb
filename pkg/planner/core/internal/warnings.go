package internal

import (
	"math"

	"github.com/pingcap/tidb/pkg/util/context"
)

type SimpleWarnings struct {
	warnings []*context.SQLWarn
}

// WarningCount returns the number of warnings.
func (s *SimpleWarnings) WarningCount() int {
	return len(s.warnings)
}

// Copy implemented the simple warnings copy to avoid use the same warnings slice for different task instance.
func (s *SimpleWarnings) Copy(src *SimpleWarnings) {
	warnings := make([]*context.SQLWarn, 0, len(src.warnings))
	warnings = append(warnings, src.warnings...)
	s.warnings = warnings
}

// CopyFrom copy the warnings from src to s.
func (s *SimpleWarnings) CopyFrom(src ...*SimpleWarnings) {
	if src == nil {
		return
	}
	length := 0
	for _, one := range src {
		if one == nil {
			continue
		}
		length += one.WarningCount()
	}
	s.warnings = make([]*context.SQLWarn, 0, length)
	for _, one := range src {
		if one == nil {
			continue
		}
		s.warnings = append(s.warnings, one.warnings...)
	}
}

// AppendWarning appends a warning to the warnings slice.
func (s *SimpleWarnings) AppendWarning(warn error) {
	if len(s.warnings) < math.MaxUint16 {
		s.warnings = append(s.warnings, &context.SQLWarn{Level: context.WarnLevelWarning, Err: warn})
	}
}

// AppendNote appends a note to the warnings slice.
func (s *SimpleWarnings) AppendNote(note error) {
	if len(s.warnings) < math.MaxUint16 {
		s.warnings = append(s.warnings, &context.SQLWarn{Level: context.WarnLevelNote, Err: note})
	}
}

// GetWarnings returns the internal all stored warnings.
func (s *SimpleWarnings) GetWarnings() []context.SQLWarn {
	// we just reuse and reorganize pointer of warning elem across different level's
	// task warnings slice to avoid copy them totally leading mem cost.
	// when best task is finished and final warnings is determined, we should convert
	// pointer to struct to append it to session context.
	warnings := make([]context.SQLWarn, 0, len(s.warnings))
	for _, w := range s.warnings {
		warnings = append(warnings, *w)
	}
	return warnings
}
