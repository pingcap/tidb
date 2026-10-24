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

package errors

import (
	"context"
	"os"
	"strings"
	"syscall"

	"github.com/pingcap/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Category is a stable, bounded classification for reporting BR failures.
type Category string

// Categories used in BR failure reports.
const (
	CategoryTiKVMemoryPressure Category = "tikv_memory_pressure"
	CategoryUnavailable        Category = "unavailable"
	CategoryDiskFull           Category = "disk_full"
	CategoryStoragePermission  Category = "storage_permission"
	CategoryConfiguration      Category = "configuration"
	CategoryChecksumMismatch   Category = "checksum_mismatch"
	CategoryCanceled           Category = "canceled"
	CategoryDeadlineExceeded   Category = "deadline_exceeded"
	CategoryResourceExhausted  Category = "resource_exhausted"
	CategoryGCSafepoint        Category = "gc_safepoint"
	CategoryUnknown            Category = "unknown"
)

// Classification describes an error without replacing its original identity.
// Code is the existing normalized RFC error code, when available.
type Classification struct {
	Category Category
	Code     string
}

// Classify categorizes an error for reporting only; it must not control retries.
// Explicit failures take precedence over cancellation in joined errors. A TiKV
// memory guard response is evidence of memory pressure, not of an OOM kill.
func Classify(err error) Classification {
	result := Classification{Category: CategoryUnknown}
	if err == nil {
		return result
	}
	original := err
	hasCategory, tikvError := false, false
	for err != nil {
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			outerCode := result.Code
			for _, cause := range joined.Unwrap() {
				if cause == nil {
					continue
				}
				candidate := Classify(cause)
				if !hasCategory || categoryPriority(candidate.Category) > categoryPriority(result.Category) ||
					(candidate.Category == result.Category && result.Code == "" && candidate.Code != "") {
					result.Category = candidate.Category
					if outerCode == "" {
						result.Code = candidate.Code
					}
					hasCategory = true
				}
			}
			// Inspect each branch separately. An unrelated TiKV code must not
			// turn a sibling's memory-related text into a TiKV memory failure.
			return result
		}
		category := CategoryUnknown
		if normalized, ok := err.(*errors.Error); ok {
			if result.Code == "" {
				result.Code = string(normalized.RFCCode())
			}
			category = categoryForCode(normalized)
			switch normalized.ID() {
			case ErrKVDownloadFailed.ID(), ErrKVIngestFailed.ID(), ErrKVStorage.ID():
				tikvError = true
			}
		}
		if category == CategoryUnknown {
			category = categoryForType(err)
		}
		if category != CategoryUnknown && (!hasCategory || categoryPriority(category) > categoryPriority(result.Category)) {
			result.Category = category
			hasCategory = true
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() error }:
			err = wrapped.Unwrap()
		case interface{ Cause() error }:
			err = wrapped.Cause()
		default:
			err = nil
		}
	}
	if tikvError {
		message := original.Error()
		switch {
		case strings.Contains(message, "resource is not enough Memory usage too high"):
			result.Category = CategoryTiKVMemoryPressure
		case strings.Contains(message, "No space left on device"), strings.Contains(message, "no space left on device"):
			result.Category = CategoryDiskFull
		case strings.Contains(message, "kind: PermissionDenied"):
			result.Category = CategoryStoragePermission
		}
	}
	return result
}

// categoryForCode maps known BR error identities without relying on their text.
// Unmapped codes remain unknown so broad codes do not imply a specific cause.
func categoryForCode(err *errors.Error) Category {
	switch err.ID() {
	case ErrKVDiskFull.ID():
		return CategoryDiskFull
	case ErrStorageInvalidPermission.ID():
		return CategoryStoragePermission
	case ErrBackupChecksumMismatch.ID(), ErrRestoreChecksumMismatch.ID():
		return CategoryChecksumMismatch
	case ErrBackupGCSafepointExceeded.ID():
		return CategoryGCSafepoint
	case ErrFailedToConnect.ID(), ErrPDLeaderNotFound.ID(), ErrBackupNoLeader.ID(), ErrKVNotLeader.ID():
		return CategoryUnavailable
	case ErrInvalidArgument.ID(), ErrUndefinedRestoreDbOrTable.ID(), ErrVersionMismatch.ID(),
		ErrInvalidMetaFile.ID(), ErrEnvNotSpecified.ID(), ErrUnsupportedOperation.ID(), ErrInvalidRange.ID(),
		ErrMigrationVersionNotSupported.ID(), ErrStorageInvalidConfig.ID(), ErrRestoreModeMismatch.ID(),
		ErrRestoreRangeMismatch.ID(), ErrRestoreCheckpointMismatch.ID(), ErrRestoreTableIDMismatch.ID(),
		ErrRestoreInvalidRewrite.ID(), ErrRestoreInvalidBackup.ID(), ErrRestoreNotFreshCluster.ID(),
		ErrRestoreIncompatibleSys.ID(), ErrUnsupportedSystemTable.ID(), ErrKVClusterIDMismatch.ID(), ErrKVNotTiKV.ID():
		return CategoryConfiguration
	default:
		return CategoryUnknown
	}
}

// categoryForType recognizes standard and gRPC errors without inspecting text.
// Generic resource exhaustion does not establish TiKV memory pressure.
func categoryForType(err error) Category {
	switch err {
	case context.Canceled:
		return CategoryCanceled
	case context.DeadlineExceeded:
		return CategoryDeadlineExceeded
	case syscall.ENOSPC:
		return CategoryDiskFull
	case os.ErrPermission, syscall.EACCES, syscall.EPERM:
		return CategoryStoragePermission
	}
	if grpcError, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		switch grpcError.GRPCStatus().Code() {
		case codes.Canceled:
			return CategoryCanceled
		case codes.DeadlineExceeded:
			return CategoryDeadlineExceeded
		case codes.Unavailable:
			return CategoryUnavailable
		case codes.ResourceExhausted:
			return CategoryResourceExhausted
		case codes.InvalidArgument:
			return CategoryConfiguration
		}
	}
	return CategoryUnknown
}

// categoryPriority favors explicit failures over cancellation and deadlines,
// while preserving unknown failures when no more specific cause is available.
func categoryPriority(category Category) int {
	switch category {
	case CategoryTiKVMemoryPressure:
		return 3
	case CategoryCanceled, CategoryDeadlineExceeded:
		return 0
	case CategoryUnknown:
		return 1
	default:
		return 2
	}
}
