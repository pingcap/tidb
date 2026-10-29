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

use tidb_terror::catalog_support::{std_err, Class};
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
