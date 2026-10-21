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

use super::{std_err, std_err_msg, Class};
use crate::terror::TerrorError;
use crate::tidb::errcode;

/// Source `kv.ErrNotExist` (`pkg/kv/error.go:31`).
pub static ERR_NOT_EXIST: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Kv, errcode::ErrNotExist));
/// Source `kv.ErrTxnRetryable` (`pkg/kv/error.go:35`).
pub static ERR_TXN_RETRYABLE: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err_msg(
        Class::Kv,
        errcode::ErrTxnRetryable,
        errcode::ErrTxnRetryable,
        "[try again later]",
    )
});
/// Source `kv.ErrCannotSetNilValue` (`pkg/kv/error.go:43`).
pub static ERR_CANNOT_SET_NIL_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Kv, errcode::ErrCannotSetNilValue));
/// Source `kv.ErrInvalidTxn` (`pkg/kv/error.go:45`).
pub static ERR_INVALID_TXN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Kv, errcode::ErrInvalidTxn));
/// Source `kv.ErrTxnTooLarge` (`pkg/kv/error.go:47`).
pub static ERR_TXN_TOO_LARGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Kv, errcode::ErrTxnTooLarge));
/// Source `kv.ErrEntryTooLarge` (`pkg/kv/error.go:49`).
pub static ERR_ENTRY_TOO_LARGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Kv, errcode::ErrEntryTooLarge));
/// Source `kv.ErrKeyTooLarge` (`pkg/kv/error.go:51`).
pub static ERR_KEY_TOO_LARGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Kv, errcode::ErrKeyTooLarge));
/// Source `kv.ErrKeyExists` (`pkg/kv/error.go:54`).
pub static ERR_KEY_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Kv, errcode::ErrDupEntry));
/// Source `kv.ErrNotImplemented` (`pkg/kv/error.go:56`).
pub static ERR_NOT_IMPLEMENTED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Kv, errcode::ErrNotImplemented));
/// Source `kv.ErrWriteConflict` (`pkg/kv/error.go:58`).
pub static ERR_WRITE_CONFLICT: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err_msg(
        Class::Kv,
        errcode::ErrWriteConflict,
        errcode::ErrWriteConflict,
        " [try again later]",
    )
});
/// Source `kv.ErrWriteConflictInTiDB` (`pkg/kv/error.go:66`).
pub static ERR_WRITE_CONFLICT_IN_TI_DB: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err_msg(
        Class::Kv,
        errcode::ErrWriteConflictInTiDB,
        errcode::ErrWriteConflictInTiDB,
        " [try again later]",
    )
});
/// Source `kv.ErrSharedLockLost` (`pkg/kv/error.go:74`).
pub static ERR_SHARED_LOCK_LOST: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrSharedLockLost));
/// Source `kv.ErrLockExpire` (`pkg/kv/error.go:76`).
pub static ERR_LOCK_EXPIRE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrLockExpire));
/// Source `kv.ErrAssertionFailed` (`pkg/kv/error.go:78`).
pub static ERR_ASSERTION_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::TiKv, errcode::ErrAssertionFailed));
