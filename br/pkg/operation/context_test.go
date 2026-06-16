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

func TestEnsureGeneratesStableOperationID(t *testing.T) {
	var ctx Context

	require.NoError(t, ctx.Ensure("log-restore"))
	require.NotEmpty(t, ctx.OperationID)
	require.False(t, ctx.StartedAt.IsZero())

	operationID := ctx.OperationID
	startedAt := ctx.StartedAt

	require.NoError(t, ctx.Ensure("log-restore"))
	require.Equal(t, operationID, ctx.OperationID)
	require.Equal(t, startedAt, ctx.StartedAt)
}

func TestEnsureRejectsIncompleteInitializedState(t *testing.T) {
	ctx := Context{OperationID: "operation-id"}

	err := ctx.Ensure("log-restore")

	require.Error(t, err)
	require.ErrorContains(t, err, "operation started time")
}

func TestSetRestoreIDBehavior(t *testing.T) {
	t.Run("initialized context records restore ID idempotently", func(t *testing.T) {
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)

		ctx.SetRestoreID(123)
		ctx.SetRestoreID(123)

		require.Equal(t, uint64(123), ctx.RestoreID)
	})

	t.Run("zero context ignores restore ID", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		var ctx Context

		ctx.SetRestoreID(123)

		require.Equal(t, uint64(0), ctx.RestoreID)
		require.Equal(t, 0, logs.FilterMessage("BR operation restore ID resolved").Len())
	})

	t.Run("ignored restore ID before Ensure can be recorded later", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		var ctx Context

		ctx.SetRestoreID(123)
		require.Equal(t, uint64(0), ctx.RestoreID)

		require.NoError(t, ctx.Ensure("log-restore"))
		ctx.SetRestoreID(123)

		require.Equal(t, uint64(123), ctx.RestoreID)
		require.Equal(t, 1, logs.FilterMessage("BR operation restore ID resolved").Len())
	})

	t.Run("copied initialized context shares restore ID log dedupe state", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)
		copiedCtx := ctx

		ctx.SetRestoreID(123)
		copiedCtx.SetRestoreID(123)

		require.Equal(t, uint64(123), ctx.RestoreID)
		require.Equal(t, uint64(123), copiedCtx.RestoreID)
		require.Equal(t, 1, logs.FilterMessage("BR operation restore ID resolved").Len())
	})
}

func TestLockMeta(t *testing.T) {
	startedAt := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	ctx := Context{
		OperationID: "operation-id",
		StartedAt:   startedAt,
		RestoreID:   123,
	}

	meta, err := ctx.LockMeta(LockResourceMigrationRead, "test hint")

	require.NoError(t, err)
	require.Equal(t, "operation-id", meta.OperationID)
	require.NotNil(t, meta.OperationStartedAt)
	require.Equal(t, startedAt, *meta.OperationStartedAt)
	require.Equal(t, uint64(123), meta.RestoreID)
	require.Equal(t, string(LockResourceMigrationRead), meta.ResourceType)
	require.Equal(t, "test hint", meta.Hint)
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
