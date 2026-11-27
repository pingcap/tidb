// Copyright 2026 PingCAP, Inc.
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

package errors_test

import (
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassify(t *testing.T) {
	memoryMessage := "resource is not enough Memory usage too high, used: 512, limit: 256"
	memoryErr := errors.Annotate(berrors.ErrKVDownloadFailed, memoryMessage)
	permissionMessage := `Io(Os { code: 13, kind: PermissionDenied, message: "Permission denied" })`
	tests := []struct {
		name     string
		err      error
		category berrors.Category
		code     string
	}{
		{name: "nil", category: berrors.CategoryUnknown},
		{name: "unknown", err: errors.New("unexpected failure"), category: berrors.CategoryUnknown},
		{name: "memory guard", err: memoryErr, category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "wrapped memory guard", err: fmt.Errorf("restore failed: %w", errors.Trace(memoryErr)), category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "legacy cause memory guard", err: &causeOnlyError{cause: memoryErr}, category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "ingest memory guard", err: errors.Annotate(berrors.ErrKVIngestFailed, memoryMessage), category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVIngestFailed"},
		{name: "backup memory guard", err: errors.Annotate(berrors.ErrKVStorage, memoryMessage), category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVStorage"},
		{name: "generic memory text", err: errors.Annotate(berrors.ErrKVDownloadFailed, "out of memory"), category: berrors.CategoryUnknown, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "unguarded memory message", err: errors.New(memoryMessage), category: berrors.CategoryUnknown},
		{name: "unrelated normalized memory message", err: errors.Annotate(berrors.ErrUnknown, memoryMessage), category: berrors.CategoryUnknown, code: "BR:Common:ErrUnknown"},
		{name: "resource exhausted", err: status.Error(codes.ResourceExhausted, "quota exhausted"), category: berrors.CategoryResourceExhausted},
		{name: "grpc memory text", err: status.Error(codes.ResourceExhausted, memoryMessage), category: berrors.CategoryResourceExhausted},
		{name: "unavailable", err: fmt.Errorf("download: %w", status.Error(codes.Unavailable, "connection reset")), category: berrors.CategoryUnavailable},
		{name: "unavailable memory text", err: status.Error(codes.Unavailable, memoryMessage), category: berrors.CategoryUnavailable},
		{name: "connection failed", err: berrors.ErrFailedToConnect, category: berrors.CategoryUnavailable, code: "BR:Common:ErrFailedToConnect"},
		{name: "disk full code", err: berrors.ErrKVDiskFull, category: berrors.CategoryDiskFull, code: "BR:KV:ErrKVDiskFull"},
		{name: "disk full response", err: errors.Annotate(berrors.ErrKVDownloadFailed, "I/O error: No space left on device"), category: berrors.CategoryDiskFull, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "disk I/O failure", err: errors.Annotate(berrors.ErrKVStorage, "Input/output error"), category: berrors.CategoryUnknown, code: "BR:KV:ErrKVStorage"},
		{name: "disk resource exhausted", err: errors.Annotate(berrors.ErrKVDownloadFailed, "resource is not enough Disk space usage too high"), category: berrors.CategoryUnknown, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "filesystem full", err: &os.PathError{Op: "write", Path: "backup.sst", Err: syscall.ENOSPC}, category: berrors.CategoryDiskFull},
		{name: "filesystem permission", err: &os.PathError{Op: "open", Path: "backup.sst", Err: syscall.EACCES}, category: berrors.CategoryStoragePermission},
		{name: "storage permission", err: errors.Trace(berrors.ErrStorageInvalidPermission), category: berrors.CategoryStoragePermission, code: "BR:ExternalStorage:ErrStorageInvalidPermission"},
		{name: "download filesystem permission", err: errors.Annotate(berrors.ErrKVDownloadFailed, permissionMessage), category: berrors.CategoryStoragePermission, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "backup filesystem permission", err: errors.Annotate(berrors.ErrKVStorage, permissionMessage), category: berrors.CategoryStoragePermission, code: "BR:KV:ErrKVStorage"},
		{name: "unguarded filesystem permission", err: errors.New(permissionMessage), category: berrors.CategoryUnknown},
		{name: "generic permission text", err: errors.Annotate(berrors.ErrKVStorage, "permission denied"), category: berrors.CategoryUnknown, code: "BR:KV:ErrKVStorage"},
		{name: "separate code and permission", err: stderrors.Join(berrors.ErrKVStorage, errors.New(permissionMessage)), category: berrors.CategoryUnknown, code: "BR:KV:ErrKVStorage"},
		{name: "storage configuration", err: berrors.ErrStorageInvalidConfig, category: berrors.CategoryConfiguration, code: "BR:ExternalStorage:ErrStorageInvalidConfig"},
		{name: "backup checksum", err: fmt.Errorf("verify: %w", berrors.ErrBackupChecksumMismatch), category: berrors.CategoryChecksumMismatch, code: "BR:Backup:ErrBackupChecksumMismatch"},
		{name: "restore checksum", err: berrors.ErrRestoreChecksumMismatch, category: berrors.CategoryChecksumMismatch, code: "BR:Restore:ErrRestoreChecksumMismatch"},
		{name: "gc safepoint", err: berrors.ErrBackupGCSafepointExceeded, category: berrors.CategoryGCSafepoint, code: "BR:Backup:ErrBackupGCSafepointExceeded"},
		{name: "canceled", err: errors.Trace(fmt.Errorf("request: %w", context.Canceled)), category: berrors.CategoryCanceled},
		{name: "deadline", err: context.DeadlineExceeded, category: berrors.CategoryDeadlineExceeded},
		{name: "grpc canceled", err: status.Error(codes.Canceled, "request canceled"), category: berrors.CategoryCanceled},
		{name: "grpc deadline", err: status.Error(codes.DeadlineExceeded, "request timed out"), category: berrors.CategoryDeadlineExceeded},
		{name: "memory before cancellation", err: stderrors.Join(memoryErr, context.Canceled), category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "memory after cancellation", err: errors.Trace(stderrors.Join(context.Canceled, memoryErr)), category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "multierr memory", err: multierr.Combine(context.DeadlineExceeded, memoryErr), category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "memory after normalized cancellation", err: stderrors.Join(berrors.ErrUnknown.Wrap(context.Canceled), memoryErr), category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "memory after disk failure", err: stderrors.Join(berrors.ErrKVDiskFull, memoryErr), category: berrors.CategoryTiKVMemoryPressure, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "unknown before cancellation", err: stderrors.Join(context.Canceled, errors.New("unexpected failure")), category: berrors.CategoryUnknown},
		{name: "separate code and message", err: fmt.Errorf("restore: %w", stderrors.Join(berrors.ErrKVDownloadFailed, errors.New(memoryMessage))), category: berrors.CategoryUnknown, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "separate message and code", err: stderrors.Join(errors.New(memoryMessage), berrors.ErrKVDownloadFailed), category: berrors.CategoryUnknown, code: "BR:KV:ErrKVDownloadFailed"},
		{name: "outer code preserved", err: berrors.ErrUnknown.Wrap(memoryErr), category: berrors.CategoryTiKVMemoryPressure, code: "BR:Common:ErrUnknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, berrors.Classification{Category: tt.category, Code: tt.code}, berrors.Classify(tt.err))
		})
	}
}

type causeOnlyError struct {
	cause error
}

func (e *causeOnlyError) Error() string { return "legacy wrapper: " + e.cause.Error() }
func (e *causeOnlyError) Cause() error  { return e.cause }
