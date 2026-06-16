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
	"context"
	"encoding/json"
	stderrors "errors"
	"testing"
	"time"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
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

		require.NotNil(t, ctx.Extra)
		require.Equal(t, uint64(123), ctx.Extra.RestoreID)
	})

	t.Run("zero context ignores restore ID", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		var ctx Context

		ctx.SetRestoreID(123)

		require.Nil(t, ctx.Extra)
		require.Equal(t, 0, logs.FilterMessage("BR operation restore ID resolved").Len())
	})

	t.Run("ignored restore ID before Ensure can be recorded later", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		var ctx Context

		ctx.SetRestoreID(123)
		require.Nil(t, ctx.Extra)

		require.NoError(t, ctx.Ensure("log-restore"))
		ctx.SetRestoreID(123)

		require.NotNil(t, ctx.Extra)
		require.Equal(t, uint64(123), ctx.Extra.RestoreID)
		require.Equal(t, 1, logs.FilterMessage("BR operation restore ID resolved").Len())
	})

	t.Run("copied initialized context shares restore ID log dedupe state", func(t *testing.T) {
		_, logs := withObservedLogs(t)
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)
		copiedCtx := ctx

		ctx.SetRestoreID(123)
		copiedCtx.SetRestoreID(123)

		require.NotNil(t, ctx.Extra)
		require.NotNil(t, copiedCtx.Extra)
		require.Equal(t, uint64(123), ctx.Extra.RestoreID)
		require.Equal(t, uint64(123), copiedCtx.Extra.RestoreID)
		require.Equal(t, 1, logs.FilterMessage("BR operation restore ID resolved").Len())
	})

	t.Run("copied extra fields are value snapshots", func(t *testing.T) {
		ctx, err := NewContext("log-restore")
		require.NoError(t, err)
		ctx.SetRestoreID(123)
		copiedCtx := ctx

		ctx.SetRestoreID(456)

		require.NotNil(t, ctx.Extra)
		require.NotNil(t, copiedCtx.Extra)
		require.Equal(t, uint64(456), ctx.Extra.RestoreID)
		require.Equal(t, uint64(123), copiedCtx.Extra.RestoreID)
	})
}

func TestLockMeta(t *testing.T) {
	startedAt := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	ctx := Context{
		OperationID: "operation-id",
		StartedAt:   startedAt,
		Extra:       &Extra{RestoreID: 123},
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

func TestLockWithRetryWriteBuildsOperationMetadata(t *testing.T) {
	ctx := context.Background()
	storage := createOperationTestStorage(t)
	operationContext, err := NewContext("log-truncate")
	require.NoError(t, err)
	operationContext.SetRestoreID(123)

	lock, err := LockWithRetryWrite(
		ctx,
		storage,
		"test.lock",
		operationContext,
		LockResourceMigrationWrite,
		"writer",
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, lock.Unlock(ctx)) }()

	data, err := storage.ReadFile(ctx, "test.lock.WRIT")
	require.NoError(t, err)
	var meta objstore.LockMeta
	require.NoError(t, json.Unmarshal(data, &meta))
	require.Equal(t, operationContext.OperationID, meta.OperationID)
	require.NotNil(t, meta.OperationStartedAt)
	require.True(t, meta.OperationStartedAt.Equal(operationContext.StartedAt))
	require.Equal(t, uint64(123), meta.RestoreID)
	require.Equal(t, string(LockResourceMigrationWrite), meta.ResourceType)
	require.Equal(t, "writer", meta.Hint)
}

func TestLockWithRetryRequiresOperationContext(t *testing.T) {
	_, err := LockWithRetryRead(
		context.Background(),
		createOperationTestStorage(t),
		"test.lock",
		Context{},
		LockResourceMigrationRead,
		"reader",
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "operation ID")
}

func TestLockMetadataError(t *testing.T) {
	baseErr := stderrors.New("missing metadata")

	require.True(t, IsLockMetadataError(LockMetadataError{Err: baseErr}))
	require.True(t, IsLockMetadataError(&LockMetadataError{Err: baseErr}))
	require.False(t, IsLockMetadataError(baseErr))
	require.Equal(t, "lock metadata error", (LockMetadataError{}).Error())
}

func createOperationTestStorage(t *testing.T) storeapi.Storage {
	t.Helper()

	storage, err := objstore.New(context.Background(), &backup.StorageBackend{
		Backend: &backup.StorageBackend_Local{
			Local: &backup.Local{Path: t.TempDir()},
		},
	}, nil)
	require.NoError(t, err)
	return storage
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
