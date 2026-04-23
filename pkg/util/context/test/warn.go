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

package context_test

import (
	"encoding/json"
	"io"
	"testing"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func RunSQLWarn(t *testing.T) {
	warns := []context.ExportedSQLWarn{
		{
			Level: context.ExportedWarnLevelError(),
			Err:   errors.New("any error"),
		},
		{
			Level: context.ExportedWarnLevelError(),
			Err:   errors.Trace(errors.New("any error")),
		},
		{
			Level: "Warning",
			Err:   terror.ErrResultUndetermined.GenWithStackByArgs("unknown"),
		},
		{
			Level: "Warning",
			Err:   errors.Trace(terror.ErrResultUndetermined.GenWithStackByArgs("unknown")),
		},
		{
			Level: context.ExportedWarnLevelNote(),
			Err:   io.EOF,
		},
	}

	d, err := json.Marshal(warns)
	require.NoError(t, err)

	var newWarns []context.ExportedSQLWarn
	require.NoError(t, json.Unmarshal(d, &newWarns))

	for i, warn := range warns {
		require.Equal(t, warn.Level, newWarns[i].Level, i)
		require.EqualError(t, newWarns[i].Err, warn.Err.Error(), i)
	}
}

func RunIgnoreWarn(t *testing.T) {
	require.Equal(t, 0, context.ExportedIgnoreWarn().WarningCount())

	context.ExportedIgnoreWarn().AppendWarning(errors.New("warn0"))
	require.Equal(t, 0, context.ExportedIgnoreWarn().WarningCount())

	require.Empty(t, context.ExportedIgnoreWarn().CopyWarnings(nil))
	require.Empty(t, context.ExportedIgnoreWarn().CopyWarnings(make([]context.ExportedSQLWarn, 0, 8)))
	require.Equal(t, 0, context.ExportedIgnoreWarn().WarningCount())

	context.ExportedIgnoreWarn().AppendWarning(errors.New("warn1"))
	require.Empty(t, context.ExportedIgnoreWarn().TruncateWarnings(0))
	require.Equal(t, 0, context.ExportedIgnoreWarn().WarningCount())
}

func RunStaticWarnHandler(t *testing.T) {
	h := context.ExportedNewStaticWarnHandler(0)
	require.Equal(t, 0, h.WarningCount())
	h.AppendWarning(errors.NewNoStackError("warn0"))
	h.AppendWarning(errors.NewNoStackError("warn1"))
	h.AppendWarning(errors.NewNoStackError("warn2"))
	h.AppendWarning(errors.NewNoStackError("warn3"))
	require.Equal(t, 4, h.WarningCount())

	expected := []context.ExportedSQLWarn{
		{Level: "Warning", Err: errors.NewNoStackError("warn0")},
		{Level: "Warning", Err: errors.NewNoStackError("warn1")},
		{Level: "Warning", Err: errors.NewNoStackError("warn2")},
		{Level: "Warning", Err: errors.NewNoStackError("warn3")},
	}

	// Copy warnings with nil dst.
	got := h.CopyWarnings(nil)
	require.Equal(t, expected, got)
	require.Equal(t, expected, context.ExportedGetStaticWarnHandlerWarnings(h))
	require.Equal(t, 4, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))

	// Copy warnings to dst with enough capacity.
	dst := make([]context.ExportedSQLWarn, 8)
	got = h.CopyWarnings(dst)
	require.Equal(t, expected, got)
	require.Equal(t, expected, context.ExportedGetStaticWarnHandlerWarnings(h))
	require.Equal(t, 4, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))
	require.True(t, warnSliceInnerArrayEqual(dst, got))

	// Copy warnings to dst without enough capacity.
	dst = make([]context.ExportedSQLWarn, 1)
	got = h.CopyWarnings(dst)
	require.Equal(t, expected, got)
	require.Equal(t, expected, context.ExportedGetStaticWarnHandlerWarnings(h))
	require.Equal(t, 4, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))
	require.False(t, warnSliceInnerArrayEqual(dst, got))

	// Copy warnings to dst with enough capacity but len(dst) < len(context.ExportedGetStaticWarnHandlerWarnings(h)).
	dst = make([]context.ExportedSQLWarn, 1, 8)
	got = h.CopyWarnings(dst)
	require.Equal(t, expected, got)
	require.Equal(t, expected, context.ExportedGetStaticWarnHandlerWarnings(h))
	require.Equal(t, 4, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))
	require.True(t, warnSliceInnerArrayEqual(dst, got))

	// Truncate warnings with start that is out of index
	got = h.TruncateWarnings(len(context.ExportedGetStaticWarnHandlerWarnings(h)))
	require.Empty(t, got)
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))
	got = h.TruncateWarnings(len(context.ExportedGetStaticWarnHandlerWarnings(h)) + 1)
	require.Empty(t, got)
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))

	// Truncate warnings with start that is in index
	got = h.TruncateWarnings(2)
	require.Equal(t, expected[2:], got)
	require.Equal(t, expected[:2], context.ExportedGetStaticWarnHandlerWarnings(h))
	require.Equal(t, 2, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))

	// Truncate warnings with 0
	got = h.TruncateWarnings(0)
	require.Equal(t, expected[:2], got)
	require.Empty(t, context.ExportedGetStaticWarnHandlerWarnings(h))
	require.Equal(t, 0, h.WarningCount())
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))

	// Copy when warnings are empty
	got = h.CopyWarnings(nil)
	require.Empty(t, got)
	require.Empty(t, context.ExportedGetStaticWarnHandlerWarnings(h))
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h), got))
}

func RunCopyWarnHandler(t *testing.T) {
	h1 := context.ExportedNewStaticWarnHandler(0)
	h1.AppendWarning(errors.NewNoStackError("warn0"))
	h1.AppendWarning(errors.NewNoStackError("warn1"))
	h1.AppendWarning(errors.NewNoStackError("warn2"))

	h2 := context.ExportedNewStaticWarnHandlerWithHandler(h1)
	require.Equal(t, 3, h2.WarningCount())
	require.Equal(t, context.ExportedGetStaticWarnHandlerWarnings(h2), []context.ExportedSQLWarn{
		{Level: "Warning", Err: errors.NewNoStackError("warn0")},
		{Level: "Warning", Err: errors.NewNoStackError("warn1")},
		{Level: "Warning", Err: errors.NewNoStackError("warn2")},
	})
	require.False(t, warnSliceInnerArrayEqual(context.ExportedGetStaticWarnHandlerWarnings(h1), context.ExportedGetStaticWarnHandlerWarnings(h2)))

	h2 = context.ExportedNewStaticWarnHandlerWithHandler(nil)
	require.Equal(t, 0, h2.WarningCount())
}

func warnSliceInnerArrayEqual(a []context.ExportedSQLWarn, b []context.ExportedSQLWarn) bool {
	if a == nil || b == nil {
		return false
	}
	return unsafe.SliceData(a) == unsafe.SliceData(b)
}
