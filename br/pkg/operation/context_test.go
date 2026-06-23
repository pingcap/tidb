// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operation

import (
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewContextGeneratesOperationID(t *testing.T) {
	ctx, err := NewContext("log-restore")

	require.NoError(t, err)
	require.NotEmpty(t, ctx.OperationID)
	require.False(t, ctx.StartedAt.IsZero())
}

func TestSetHintFieldBehavior(t *testing.T) {
	t.Run("initialized context records hint field each time", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)

		ctx.SetHintField("lineage_id", "123")
		ctx.SetHintField("lineage_id", "123")

		require.Equal(t, []HintField{{Key: "lineage_id", Value: "123"}}, ctx.HintFields())
		require.Equal(t, 2, logs.FilterMessage("BR operation hint field resolved").Len())
	})

	t.Run("zero context ignores hint field", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		var ctx Context

		ctx.SetHintField("lineage_id", "123")

		require.Empty(t, ctx.HintFields())
		require.Equal(t, 0, logs.FilterMessage("BR operation hint field resolved").Len())
	})

	t.Run("ignored hint field before context creation can be recorded later", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		var ctx Context

		ctx.SetHintField("lineage_id", "123")
		require.Empty(t, ctx.HintFields())

		var err error
		ctx, err = NewContext("log-restore")
		require.NoError(t, err)
		ctx.SetHintField("lineage_id", "123")

		require.Equal(t, []HintField{{Key: "lineage_id", Value: "123"}}, ctx.HintFields())
		require.Equal(t, 1, logs.FilterMessage("BR operation hint field resolved").Len())
	})

	t.Run("copied initialized context records hint field independently", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)

		ctx.SetHintField("lineage_id", "123")
		copiedCtx := ctx
		copiedCtx.SetHintField("lineage_id", "456")

		require.Equal(t, []HintField{{Key: "lineage_id", Value: "123"}}, ctx.HintFields())
		require.Equal(t, []HintField{{Key: "lineage_id", Value: "456"}}, copiedCtx.HintFields())
		require.Equal(t, 2, logs.FilterMessage("BR operation hint field resolved").Len())
	})

	t.Run("returned hint fields cannot mutate context", func(t *testing.T) {
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)

		ctx.SetHintField("lineage_id", "123")
		fields := ctx.HintFields()
		fields[0].Value = "456"

		require.Equal(t, []HintField{{Key: "lineage_id", Value: "123"}}, ctx.HintFields())
	})

	t.Run("changed hint field logs warning and updates value", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)

		ctx.SetHintField("lineage_id", "123")
		ctx.SetHintField("lineage_id", "456")

		require.Equal(t, []HintField{{Key: "lineage_id", Value: "456"}}, ctx.HintFields())
		require.Equal(t, 2, logs.FilterMessage("BR operation hint field resolved").Len())
		warnLogs := logs.FilterMessage("BR operation hint field changed")
		require.Equal(t, 1, warnLogs.Len())
		require.Equal(t, "lineage_id", loggedStringField(t, warnLogs.All()[0], "hint_key"))
		require.Equal(t, "123", loggedStringField(t, warnLogs.All()[0], "old_value"))
		require.Equal(t, "456", loggedStringField(t, warnLogs.All()[0], "new_value"))
	})

	t.Run("empty value removes hint field", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)

		ctx.SetHintField("lineage_id", "123")
		ctx.SetHintField("lineage_id", "")

		require.Empty(t, ctx.HintFields())
		require.Equal(t, 1, logs.FilterMessage("BR operation hint field resolved").Len())
	})
}

func TestLockMeta(t *testing.T) {
	startedAt := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	ctx := Context{
		OperationID: "operation-id",
		StartedAt:   startedAt,
	}
	ctx.SetHintField("lineage_id", "123")

	meta, err := ctx.LockMeta(LockResourceMigrationRead, "test hint")

	require.NoError(t, err)
	require.Equal(t, "operation-id", meta.OwnerID)
	require.Equal(t, string(LockResourceMigrationRead), meta.LockType)
	require.Contains(t, meta.Hint, "operation_started_at=2026-06-15T12:00:00Z")
	require.Contains(t, meta.Hint, "lineage_id=123")
	require.Contains(t, meta.Hint, `detail="test hint"`)
}

func TestLockMetaValidation(t *testing.T) {
	cases := []struct {
		name        string
		ctx         Context
		resource    LockResourceType
		expectedErr string
	}{
		{
			name:        "missing operation ID",
			ctx:         Context{},
			resource:    LockResourceMigrationRead,
			expectedErr: "operation ID",
		},
		{
			name: "missing started time",
			ctx: Context{
				OperationID: "operation-id",
			},
			resource:    LockResourceMigrationRead,
			expectedErr: "operation started time",
		},
		{
			name: "missing resource type",
			ctx: Context{
				OperationID: "operation-id",
				StartedAt:   time.Now(),
			},
			expectedErr: "resource type",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.ctx.LockMeta(c.resource, "test hint")
			require.Error(t, err)
			require.ErrorContains(t, err, c.expectedErr)
		})
	}
}

func withObservedLogs(t *testing.T) (*zap.Logger, *observer.ObservedLogs) {
	t.Helper()

	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core)
	restore := log.ReplaceGlobals(logger, &log.ZapProperties{
		Core:  core,
		Level: zap.NewAtomicLevelAt(zap.InfoLevel),
	})
	t.Cleanup(restore)
	return logger, logs
}

func loggedStringField(t *testing.T, entry observer.LoggedEntry, key string) string {
	t.Helper()

	for _, field := range entry.Context {
		if field.Key == key {
			return field.String
		}
	}
	require.Failf(t, "missing log field", "field %s not found", key)
	return ""
}
