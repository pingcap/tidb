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
	stderrors "errors"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

// LockMetadataError marks local failures while constructing operation-aware lock metadata.
type LockMetadataError struct {
	Err error
}

func (e LockMetadataError) Error() string {
	if e.Err == nil {
		return "lock metadata error"
	}
	return e.Err.Error()
}

func (e LockMetadataError) Unwrap() error {
	return e.Err
}

// IsLockMetadataError returns whether err was caused by local lock metadata construction.
func IsLockMetadataError(err error) bool {
	var metadataErr LockMetadataError
	if stderrors.As(err, &metadataErr) {
		return true
	}
	var metadataErrPtr *LockMetadataError
	return stderrors.As(err, &metadataErrPtr)
}

func lockMetaInput(operationContext Context, resource LockResourceType, hint string) (objstore.LockMetaInput, error) {
	input, err := operationContext.LockMeta(resource, hint)
	if err != nil {
		return objstore.LockMetaInput{}, LockMetadataError{
			Err: errors.Annotate(err, "failed to build operation lock metadata"),
		}
	}
	return input, nil
}

// TryLockRemote acquires an object-storage lock with operation metadata.
func TryLockRemote(
	ctx context.Context,
	storage storeapi.Storage,
	path string,
	operationContext Context,
	resource LockResourceType,
	hint string,
) (objstore.RemoteLock, error) {
	input, err := lockMetaInput(operationContext, resource, hint)
	if err != nil {
		return objstore.RemoteLock{}, err
	}
	return objstore.TryLockRemote(ctx, storage, path, input)
}

// TryLockRemoteRead acquires an object-storage read lock with operation metadata.
func TryLockRemoteRead(
	ctx context.Context,
	storage storeapi.Storage,
	path string,
	operationContext Context,
	resource LockResourceType,
	hint string,
) (objstore.RemoteLock, error) {
	input, err := lockMetaInput(operationContext, resource, hint)
	if err != nil {
		return objstore.RemoteLock{}, err
	}
	return objstore.TryLockRemoteRead(ctx, storage, path, input)
}

// TryLockRemoteWrite acquires an object-storage write lock with operation metadata.
func TryLockRemoteWrite(
	ctx context.Context,
	storage storeapi.Storage,
	path string,
	operationContext Context,
	resource LockResourceType,
	hint string,
) (objstore.RemoteLock, error) {
	input, err := lockMetaInput(operationContext, resource, hint)
	if err != nil {
		return objstore.RemoteLock{}, err
	}
	return objstore.TryLockRemoteWrite(ctx, storage, path, input)
}

// LockWithRetry acquires an object-storage lock with retry and operation metadata.
func LockWithRetry(
	ctx context.Context,
	locker objstore.Locker,
	storage storeapi.Storage,
	path string,
	operationContext Context,
	resource LockResourceType,
	hint string,
) (objstore.RemoteLock, error) {
	input, err := lockMetaInput(operationContext, resource, hint)
	if err != nil {
		return objstore.RemoteLock{}, err
	}
	return objstore.LockWithRetry(ctx, locker, storage, path, input)
}

// LockWithRetryRead acquires an object-storage read lock with retry and operation metadata.
func LockWithRetryRead(
	ctx context.Context,
	storage storeapi.Storage,
	path string,
	operationContext Context,
	resource LockResourceType,
	hint string,
) (objstore.RemoteLock, error) {
	return LockWithRetry(ctx, objstore.TryLockRemoteRead, storage, path, operationContext, resource, hint)
}

// LockWithRetryWrite acquires an object-storage write lock with retry and operation metadata.
func LockWithRetryWrite(
	ctx context.Context,
	storage storeapi.Storage,
	path string,
	operationContext Context,
	resource LockResourceType,
	hint string,
) (objstore.RemoteLock, error) {
	return LockWithRetry(ctx, objstore.TryLockRemoteWrite, storage, path, operationContext, resource, hint)
}

// LockConflictLogFields returns structured fields for an operation-aware failed lock attempt.
func LockConflictLogFields(
	path string,
	operationContext Context,
	resource LockResourceType,
	hint string,
	err error,
) []zap.Field {
	input, metaErr := operationContext.LockMeta(resource, hint)
	if metaErr != nil {
		return []zap.Field{
			logutil.ShortError(err),
			zap.String("path", path),
			logutil.AShortError("lock_meta_error", metaErr),
		}
	}
	return objstore.LockConflictLogFields(path, input, err)
}
