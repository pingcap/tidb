// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package log

import (
	"strings"

	"go.uber.org/zap/zapcore"
)

var _ zapcore.Core = (*FilterCore)(nil)

// FilterCore is a zapcore.Core implementation, it filters log by path-qualified
// package name.
type FilterCore struct {
	zapcore.Core
	filters []string
}

// NewFilterCore returns a FilterCore.
//
// Example, filter TiDB's log, `NewFilterCore(core, "github.com/pingcap/tidb/")`.
// Note, must set AddCaller() to the logger.
func NewFilterCore(core zapcore.Core, filteredPackages ...string) *FilterCore {
	return &FilterCore{
		Core:    core,
		filters: filteredPackages,
	}
}

// With adds structured context to the Core.
func (f *FilterCore) With(fields []zapcore.Field) zapcore.Core {
	return &FilterCore{
		Core:    f.Core.With(fields),
		filters: f.filters,
	}
}

// Check overrides wrapper core.Check and adds itself to zapcore.CheckedEntry.
func (f *FilterCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if f.Enabled(entry.Level) {
		return ce.AddCore(entry, f)
	}
	return ce
}

// Write filters entry by checking if entry's Caller.Function matches filtered
// package path.
func (f *FilterCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	for i := range f.filters {
		// Caller.Function is a package path-qualified function name.
		if strings.Contains(entry.Caller.Function, f.filters[i]) {
			return nil
		}
	}
	return f.Core.Write(entry, fields)
}
