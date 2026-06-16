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

func TestSetRestoreID(t *testing.T) {
	ctx, err := NewContext("log-restore")
	require.NoError(t, err)

	ctx.SetRestoreID(123)

	require.Equal(t, uint64(123), ctx.RestoreID)
}

func TestSetRestoreIDIsIdempotent(t *testing.T) {
	ctx, err := NewContext("log-restore")
	require.NoError(t, err)

	ctx.SetRestoreID(123)
	ctx.SetRestoreID(123)

	require.Equal(t, uint64(123), ctx.RestoreID)
}

func TestSetRestoreIDOnZeroContextDoesNotLog(t *testing.T) {
	_, logs := withObservedLogs(t)
	var ctx Context

	ctx.SetRestoreID(123)

	require.Equal(t, uint64(0), ctx.RestoreID)
	require.Equal(t, 0, logs.FilterMessage("BR operation restore ID resolved").Len())
}

func TestSetRestoreIDBeforeEnsureDoesNotSuppressInitializedTransition(t *testing.T) {
	_, logs := withObservedLogs(t)
	var ctx Context

	ctx.SetRestoreID(123)
	require.Equal(t, uint64(0), ctx.RestoreID)

	require.NoError(t, ctx.Ensure("log-restore"))
	ctx.SetRestoreID(123)

	require.Equal(t, uint64(123), ctx.RestoreID)
	require.Equal(t, 1, logs.FilterMessage("BR operation restore ID resolved").Len())
}

func TestSetRestoreIDCopiedInitializedContextSharesLogDedupeState(t *testing.T) {
	_, logs := withObservedLogs(t)
	ctx, err := NewContext("log-restore")
	require.NoError(t, err)
	copiedCtx := ctx

	ctx.SetRestoreID(123)
	copiedCtx.SetRestoreID(123)

	require.Equal(t, uint64(123), ctx.RestoreID)
	require.Equal(t, uint64(123), copiedCtx.RestoreID)
	require.Equal(t, 1, logs.FilterMessage("BR operation restore ID resolved").Len())
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

func TestLockMetaRequiresOperationID(t *testing.T) {
	_, err := (Context{}).LockMeta(LockResourceMigrationRead, "test hint")

	require.Error(t, err)
	require.ErrorContains(t, err, "operation ID")
}

func TestLockMetaRequiresResourceType(t *testing.T) {
	ctx := Context{
		OperationID: "operation-id",
		StartedAt:   time.Now(),
	}

	_, err := ctx.LockMeta("", "test hint")

	require.Error(t, err)
	require.ErrorContains(t, err, "resource type")
}

func TestTryLockRemoteBuildsOperationMetadata(t *testing.T) {
	ctx := context.Background()
	storage := createOperationTestStorage(t)
	operationContext, err := NewContext("log-truncate")
	require.NoError(t, err)
	operationContext.SetRestoreID(123)

	lock, err := TryLockRemote(
		ctx,
		storage,
		"test.lock",
		operationContext,
		LockResourceLogTruncateExclusive,
		"truncate",
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, lock.Unlock(ctx)) }()

	data, err := storage.ReadFile(ctx, "test.lock")
	require.NoError(t, err)
	var meta objstore.LockMeta
	require.NoError(t, json.Unmarshal(data, &meta))
	require.Equal(t, operationContext.OperationID, meta.OperationID)
	require.NotNil(t, meta.OperationStartedAt)
	require.True(t, meta.OperationStartedAt.Equal(operationContext.StartedAt))
	require.Equal(t, uint64(123), meta.RestoreID)
	require.Equal(t, string(LockResourceLogTruncateExclusive), meta.ResourceType)
	require.Equal(t, "truncate", meta.Hint)
}

func TestLockWithRetryRequiresOperationContext(t *testing.T) {
	_, err := LockWithRetry(
		context.Background(),
		objstore.TryLockRemoteRead,
		createOperationTestStorage(t),
		"test.lock",
		Context{},
		LockResourceMigrationRead,
		"reader",
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "operation ID")
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
