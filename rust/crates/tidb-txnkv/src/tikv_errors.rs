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

//! Error prototypes defined by the Go package(s) named per entry.
//!
//! GENERATED mechanically from the Go sources by scanning every
//! package-level `Class*.NewStd` / `NewStdErr` definition; the (class,
//! code) set of every module is cross-checked against the real Go
//! registry (`terror.ErrClassToMySQLCodes`) by
//! `tests/catalog_source.rs`. Do not hand-edit entries.

use std::sync::LazyLock;

use tidb_dbterror::catalog_support::{std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `store/driver/error.ErrTokenLimit` (`pkg/store/driver/error/error.go:34`).
pub static ERR_TOKEN_LIMIT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrTiKVStoreLimit));
/// Source `store/driver/error.ErrTiKVServerTimeout` (`pkg/store/driver/error/error.go:36`).
pub static ERR_TI_KV_SERVER_TIMEOUT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrTiKVServerTimeout));
/// Source `store/driver/error.ErrTiFlashServerTimeout` (`pkg/store/driver/error/error.go:37`).
pub static ERR_TI_FLASH_SERVER_TIMEOUT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrTiFlashServerTimeout));
/// Source `store/driver/error.ErrTxnAbortedByGC` (`pkg/store/driver/error/error.go:39`).
pub static ERR_TXN_ABORTED_BY_GC: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrTxnAbortedByGC));
/// Source `store/driver/error.ErrTiKVStaleCommand` (`pkg/store/driver/error/error.go:41`).
pub static ERR_TI_KV_STALE_COMMAND: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrTiKVStaleCommand));
/// Source `store/driver/error.ErrQueryInterrupted` (`pkg/store/driver/error/error.go:43`).
pub static ERR_QUERY_INTERRUPTED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrQueryInterrupted));
/// Source `store/driver/error.ErrTiKVMaxTimestampNotSynced` (`pkg/store/driver/error/error.go:45`).
pub static ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrTiKVMaxTimestampNotSynced));
/// Source `store/driver/error.ErrLockAcquireFailAndNoWaitSet` (`pkg/store/driver/error/error.go:47`).
pub static ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrLockAcquireFailAndNoWaitSet));
/// Source `store/driver/error.ErrResolveLockTimeout` (`pkg/store/driver/error/error.go:48`).
pub static ERR_RESOLVE_LOCK_TIMEOUT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrResolveLockTimeout));
/// Source `store/driver/error.ErrLockWaitTimeout` (`pkg/store/driver/error/error.go:50`).
pub static ERR_LOCK_WAIT_TIMEOUT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrLockWaitTimeout));
/// Source `store/driver/error.ErrTiKVServerBusy` (`pkg/store/driver/error/error.go:52`).
pub static ERR_TI_KV_SERVER_BUSY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrTiKVServerBusy));
/// Source `store/driver/error.ErrTiFlashServerBusy` (`pkg/store/driver/error/error.go:54`).
pub static ERR_TI_FLASH_SERVER_BUSY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrTiFlashServerBusy));
/// Source `store/driver/error.ErrPDServerTimeout` (`pkg/store/driver/error/error.go:56`).
pub static ERR_PD_SERVER_TIMEOUT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrPDServerTimeout));
/// Source `store/driver/error.ErrRegionUnavailable` (`pkg/store/driver/error/error.go:58`).
pub static ERR_REGION_UNAVAILABLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrRegionUnavailable));
/// Source `store/driver/error.ErrResourceGroupNotExists` (`pkg/store/driver/error/error.go:60`).
pub static ERR_RESOURCE_GROUP_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrResourceGroupNotExists));
/// Source `store/driver/error.ErrResourceGroupConfigUnavailable` (`pkg/store/driver/error/error.go:62`).
pub static ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrResourceGroupConfigUnavailable));
/// Source `store/driver/error.ErrResourceGroupThrottled` (`pkg/store/driver/error/error.go:64`).
pub static ERR_RESOURCE_GROUP_THROTTLED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrResourceGroupThrottled));
/// Source `store/driver/error.ErrUnknown` (`pkg/store/driver/error/error.go:66`).
pub static ERR_UNKNOWN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrUnknown));

#[cfg(test)]
mod go_class_code_tests {
    use tidb_errno::errcode;
    use tidb_terror::TerrorClass;

    /// Each prototype carries the class and code of the Go definition it
    /// transcribes (`Class.NewStd(code)`), so `Error()` prints Go's
    /// `[class:code]`.
    #[test]
    fn prototypes_carry_their_go_class_and_code() {
        assert_eq!(
            super::ERR_TOKEN_LIMIT.class(),
            TerrorClass::TiKv,
            "tikv::ERR_TOKEN_LIMIT"
        );
        assert_eq!(
            super::ERR_TOKEN_LIMIT.code().value(),
            errcode::ErrTiKVStoreLimit as isize,
            "tikv::ERR_TOKEN_LIMIT"
        );
        assert_eq!(
            super::ERR_TI_KV_SERVER_TIMEOUT.class(),
            TerrorClass::TiKv,
            "tikv::ERR_TI_KV_SERVER_TIMEOUT"
        );
        assert_eq!(
            super::ERR_TI_KV_SERVER_TIMEOUT.code().value(),
            errcode::ErrTiKVServerTimeout as isize,
            "tikv::ERR_TI_KV_SERVER_TIMEOUT"
        );
        assert_eq!(
            super::ERR_TI_FLASH_SERVER_TIMEOUT.class(),
            TerrorClass::TiKv,
            "tikv::ERR_TI_FLASH_SERVER_TIMEOUT"
        );
        assert_eq!(
            super::ERR_TI_FLASH_SERVER_TIMEOUT.code().value(),
            errcode::ErrTiFlashServerTimeout as isize,
            "tikv::ERR_TI_FLASH_SERVER_TIMEOUT"
        );
        assert_eq!(
            super::ERR_TXN_ABORTED_BY_GC.class(),
            TerrorClass::TiKv,
            "tikv::ERR_TXN_ABORTED_BY_GC"
        );
        assert_eq!(
            super::ERR_TXN_ABORTED_BY_GC.code().value(),
            errcode::ErrTxnAbortedByGC as isize,
            "tikv::ERR_TXN_ABORTED_BY_GC"
        );
        assert_eq!(
            super::ERR_TI_KV_STALE_COMMAND.class(),
            TerrorClass::TiKv,
            "tikv::ERR_TI_KV_STALE_COMMAND"
        );
        assert_eq!(
            super::ERR_TI_KV_STALE_COMMAND.code().value(),
            errcode::ErrTiKVStaleCommand as isize,
            "tikv::ERR_TI_KV_STALE_COMMAND"
        );
        assert_eq!(
            super::ERR_QUERY_INTERRUPTED.class(),
            TerrorClass::TiKv,
            "tikv::ERR_QUERY_INTERRUPTED"
        );
        assert_eq!(
            super::ERR_QUERY_INTERRUPTED.code().value(),
            errcode::ErrQueryInterrupted as isize,
            "tikv::ERR_QUERY_INTERRUPTED"
        );
        assert_eq!(
            super::ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED.class(),
            TerrorClass::TiKv,
            "tikv::ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED"
        );
        assert_eq!(
            super::ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED
                .code()
                .value(),
            errcode::ErrTiKVMaxTimestampNotSynced as isize,
            "tikv::ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED"
        );
        assert_eq!(
            super::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET.class(),
            TerrorClass::TiKv,
            "tikv::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET"
        );
        assert_eq!(
            super::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET
                .code()
                .value(),
            errcode::ErrLockAcquireFailAndNoWaitSet as isize,
            "tikv::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET"
        );
        assert_eq!(
            super::ERR_RESOLVE_LOCK_TIMEOUT.class(),
            TerrorClass::TiKv,
            "tikv::ERR_RESOLVE_LOCK_TIMEOUT"
        );
        assert_eq!(
            super::ERR_RESOLVE_LOCK_TIMEOUT.code().value(),
            errcode::ErrResolveLockTimeout as isize,
            "tikv::ERR_RESOLVE_LOCK_TIMEOUT"
        );
        assert_eq!(
            super::ERR_LOCK_WAIT_TIMEOUT.class(),
            TerrorClass::TiKv,
            "tikv::ERR_LOCK_WAIT_TIMEOUT"
        );
        assert_eq!(
            super::ERR_LOCK_WAIT_TIMEOUT.code().value(),
            errcode::ErrLockWaitTimeout as isize,
            "tikv::ERR_LOCK_WAIT_TIMEOUT"
        );
        assert_eq!(
            super::ERR_TI_KV_SERVER_BUSY.class(),
            TerrorClass::TiKv,
            "tikv::ERR_TI_KV_SERVER_BUSY"
        );
        assert_eq!(
            super::ERR_TI_KV_SERVER_BUSY.code().value(),
            errcode::ErrTiKVServerBusy as isize,
            "tikv::ERR_TI_KV_SERVER_BUSY"
        );
        assert_eq!(
            super::ERR_TI_FLASH_SERVER_BUSY.class(),
            TerrorClass::TiKv,
            "tikv::ERR_TI_FLASH_SERVER_BUSY"
        );
        assert_eq!(
            super::ERR_TI_FLASH_SERVER_BUSY.code().value(),
            errcode::ErrTiFlashServerBusy as isize,
            "tikv::ERR_TI_FLASH_SERVER_BUSY"
        );
        assert_eq!(
            super::ERR_PD_SERVER_TIMEOUT.class(),
            TerrorClass::TiKv,
            "tikv::ERR_PD_SERVER_TIMEOUT"
        );
        assert_eq!(
            super::ERR_PD_SERVER_TIMEOUT.code().value(),
            errcode::ErrPDServerTimeout as isize,
            "tikv::ERR_PD_SERVER_TIMEOUT"
        );
        assert_eq!(
            super::ERR_REGION_UNAVAILABLE.class(),
            TerrorClass::TiKv,
            "tikv::ERR_REGION_UNAVAILABLE"
        );
        assert_eq!(
            super::ERR_REGION_UNAVAILABLE.code().value(),
            errcode::ErrRegionUnavailable as isize,
            "tikv::ERR_REGION_UNAVAILABLE"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_NOT_EXISTS.class(),
            TerrorClass::TiKv,
            "tikv::ERR_RESOURCE_GROUP_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_NOT_EXISTS.code().value(),
            errcode::ErrResourceGroupNotExists as isize,
            "tikv::ERR_RESOURCE_GROUP_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE.class(),
            TerrorClass::TiKv,
            "tikv::ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE
                .code()
                .value(),
            errcode::ErrResourceGroupConfigUnavailable as isize,
            "tikv::ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_THROTTLED.class(),
            TerrorClass::TiKv,
            "tikv::ERR_RESOURCE_GROUP_THROTTLED"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_THROTTLED.code().value(),
            errcode::ErrResourceGroupThrottled as isize,
            "tikv::ERR_RESOURCE_GROUP_THROTTLED"
        );
        assert_eq!(
            super::ERR_UNKNOWN.class(),
            TerrorClass::TiKv,
            "tikv::ERR_UNKNOWN"
        );
        assert_eq!(
            super::ERR_UNKNOWN.code().value(),
            errcode::ErrUnknown as isize,
            "tikv::ERR_UNKNOWN"
        );
    }
}
