// Copyright 2024 PingCAP, Inc.
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
	"io"
	"reflect"
	"testing"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestSQLWarn(t *testing.T) {
	warns := []SQLWarn{
		{
			Level: WarnLevelError,
			Err:   errors.New("any error"),
		},
		{
			Level: WarnLevelError,
			Err:   errors.Trace(errors.New("any error")),
		},
		{
			Level: WarnLevelWarning,
			Err:   terror.ErrResultUndetermined.GenWithStackByArgs("unknown"),
		},
		{
			Level: WarnLevelWarning,
			Err:   errors.Trace(terror.ErrResultUndetermined.GenWithStackByArgs("unknown")),
		},
		{
			Level: WarnLevelNote,
			Err:   io.EOF,
		},
	}

	d, err := json.Marshal(warns)
	require.NoError(t, err)

	var newWarns []SQLWarn
	require.NoError(t, json.Unmarshal(d, &newWarns))

	for i, warn := range warns {
		require.Equal(t, warn.Level, newWarns[i].Level, i)
		require.EqualError(t, newWarns[i].Err, warn.Err.Error(), i)
	}
}

func TestIgnoreWarn(t *testing.T) {
	require.Equal(t, 0, IgnoreWarn.WarningCount())

	IgnoreWarn.AppendWarning(errors.New("warn0"))
	require.Equal(t, 0, IgnoreWarn.WarningCount())

	require.Empty(t, IgnoreWarn.CopyWarnings(nil))
	require.Empty(t, IgnoreWarn.CopyWarnings(make([]SQLWarn, 0, 8)))
	require.Equal(t, 0, IgnoreWarn.WarningCount())

	IgnoreWarn.AppendWarning(errors.New("warn1"))
	require.Empty(t, IgnoreWarn.TruncateWarnings(0))
	require.Equal(t, 0, IgnoreWarn.WarningCount())
}

func TestStaticWarnHandler(t *testing.T) {
	h := NewStaticWarnHandler(0)
	require.Equal(t, 0, h.WarningCount())
	h.AppendWarning(errors.NewNoStackError("warn0"))
	h.AppendWarning(errors.NewNoStackError("warn1"))
	h.AppendWarning(errors.NewNoStackError("warn2"))
	h.AppendWarning(errors.NewNoStackError("warn3"))
	require.Equal(t, 4, h.WarningCount())

	expected := []SQLWarn{
		{Level: WarnLevelWarning, Err: errors.NewNoStackError("warn0")},
		{Level: WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
		{Level: WarnLevelWarning, Err: errors.NewNoStackError("warn2")},
		{Level: WarnLevelWarning, Err: errors.NewNoStackError("warn3")},
	}

	// Copy warnings with nil dst.
	got := h.CopyWarnings(nil)
	require.Equal(t, expected, got)
	require.Equal(t, expected, h.warnings)
	require.Equal(t, 4, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))

	// Copy warnings to dst with enough capacity.
	dst := make([]SQLWarn, 8)
	got = h.CopyWarnings(dst)
	require.Equal(t, expected, got)
	require.Equal(t, expected, h.warnings)
	require.Equal(t, 4, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))
	require.True(t, warnSliceInnerArrayEqual(dst, got))

	// Copy warnings to dst without enough capacity.
	dst = make([]SQLWarn, 1)
	got = h.CopyWarnings(dst)
	require.Equal(t, expected, got)
	require.Equal(t, expected, h.warnings)
	require.Equal(t, 4, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))
	require.False(t, warnSliceInnerArrayEqual(dst, got))

	// Copy warnings to dst with enough capacity but len(dst) < len(h.warnings).
	dst = make([]SQLWarn, 1, 8)
	got = h.CopyWarnings(dst)
	require.Equal(t, expected, got)
	require.Equal(t, expected, h.warnings)
	require.Equal(t, 4, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))
	require.True(t, warnSliceInnerArrayEqual(dst, got))

	// Truncate warnings with start that is out of index
	got = h.TruncateWarnings(len(h.warnings))
	require.Empty(t, got)
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))
	got = h.TruncateWarnings(len(h.warnings) + 1)
	require.Empty(t, got)
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))

	// Truncate warnings with start that is in index
	got = h.TruncateWarnings(2)
	require.Equal(t, expected[2:], got)
	require.Equal(t, expected[:2], h.warnings)
	require.Equal(t, 2, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))

	// Truncate warnings with 0
	got = h.TruncateWarnings(0)
	require.Equal(t, expected[:2], got)
	require.Empty(t, h.warnings)
	require.Equal(t, 0, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))

	// Copy when warnings are empty
	got = h.CopyWarnings(nil)
	require.Empty(t, got)
	require.Empty(t, h.warnings)
	require.False(t, warnSliceInnerArrayEqual(h.warnings, got))
}

func TestCopyWarnHandler(t *testing.T) {
	h1 := NewStaticWarnHandler(0)
	h1.AppendWarning(errors.NewNoStackError("warn0"))
	h1.AppendWarning(errors.NewNoStackError("warn1"))
	h1.AppendWarning(errors.NewNoStackError("warn2"))

	h2 := NewStaticWarnHandlerWithHandler(h1)
	require.Equal(t, 3, h2.WarningCount())
	require.Equal(t, h2.warnings, []SQLWarn{
		{Level: WarnLevelWarning, Err: errors.NewNoStackError("warn0")},
		{Level: WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
		{Level: WarnLevelWarning, Err: errors.NewNoStackError("warn2")},
	})
	require.False(t, warnSliceInnerArrayEqual(h1.warnings, h2.warnings))

	h2 = NewStaticWarnHandlerWithHandler(nil)
	require.Equal(t, 0, h2.WarningCount())
}

func warnSliceInnerArrayEqual(a []SQLWarn, b []SQLWarn) bool {
	if a == nil || b == nil {
		return false
	}
	header1 := (*reflect.SliceHeader)(unsafe.Pointer(&a))
	header2 := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return header1.Data == header2.Data
}
