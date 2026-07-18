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

//! Typed translation of the complete `pkg/kv/error.go` contract.
//!
//! The Go source constructs these identities through `dbterror`, but their
//! public identity is only the registered TiDB error class, MySQL error code,
//! message template, and redaction positions. Keeping those four fields here
//! makes equality and retry classification source-exact without inventing a
//! retryable flag or pulling the whole server error registry into `txnkv`.

use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use tidb_error::{mysql, tidb, ErrMessage};

/// Backward-compatible marker appended to retryable transaction errors.
///
/// This is `pkg/kv.TxnRetryableMark`; changing it would change TiDB's external
/// error-message contract.
pub const TXN_RETRYABLE_MARK: &str = "[try again later]";

/// The two registered TiDB error classes used by `pkg/kv/error.go`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ErrorClass {
    /// The `kv` error class (`terror.ClassKV`).
    Kv = 8,
    /// The `tikv` error class (`terror.ClassTiKV`).
    TiKv = 24,
}

impl ErrorClass {
    /// Returns the registered class name used in the RFC error identity.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Kv => "kv",
            Self::TiKv => "tikv",
        }
    }

    /// Returns the numeric `terror.ErrClass` value.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// MySQL protocol error codes referenced by `pkg/kv/error.go`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum MysqlErrorCode {
    /// `mysql.ErrDupEntry`.
    DupEntry = mysql::errcode::ErrDupEntry,
    /// `mysql.ErrTxnTooLarge`.
    TxnTooLarge = tidb::errcode::ErrTxnTooLarge,
    /// `mysql.ErrWriteConflictInTiDB`.
    WriteConflictInTiDb = tidb::errcode::ErrWriteConflictInTiDB,
    /// `mysql.ErrNotExist`.
    NotExist = tidb::errcode::ErrNotExist,
    /// `mysql.ErrTxnRetryable`.
    TxnRetryable = tidb::errcode::ErrTxnRetryable,
    /// `mysql.ErrCannotSetNilValue`.
    CannotSetNilValue = tidb::errcode::ErrCannotSetNilValue,
    /// `mysql.ErrInvalidTxn`.
    InvalidTxn = tidb::errcode::ErrInvalidTxn,
    /// `mysql.ErrEntryTooLarge`.
    EntryTooLarge = tidb::errcode::ErrEntryTooLarge,
    /// `mysql.ErrNotImplemented`.
    NotImplemented = tidb::errcode::ErrNotImplemented,
    /// `mysql.ErrAssertionFailed`.
    AssertionFailed = tidb::errcode::ErrAssertionFailed,
    /// `mysql.ErrKeyTooLarge`.
    KeyTooLarge = tidb::errcode::ErrKeyTooLarge,
    /// `mysql.ErrLockExpire`.
    LockExpire = tidb::errcode::ErrLockExpire,
    /// `mysql.ErrWriteConflict`.
    WriteConflict = tidb::errcode::ErrWriteConflict,
}

impl MysqlErrorCode {
    /// Returns the protocol error number.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ErrorIdentity {
    class: ErrorClass,
    code: MysqlErrorCode,
    template: &'static str,
    redact_arg_positions: &'static [usize],
}

impl ErrorIdentity {
    const fn new(
        class: ErrorClass,
        code: MysqlErrorCode,
        template: &'static str,
        redact_arg_positions: &'static [usize],
    ) -> Self {
        Self {
            class,
            code,
            template,
            redact_arg_positions,
        }
    }

    const fn from_message(class: ErrorClass, code: MysqlErrorCode, message: ErrMessage) -> Self {
        Self::new(class, code, message.raw, message.redact_arg_pos)
    }

    const fn same_registered_error(self, other: Self) -> bool {
        self.class.as_u8() == other.class.as_u8() && self.code.as_u16() == other.code.as_u16()
    }
}

/// A source-backed TiDB KV error prototype or generated error.
///
/// As in `errors.Error.Equal`, equality is based on the registered RFC
/// identity (`class:code`), not on the generated message arguments.
#[derive(Clone, Debug)]
pub struct KvError {
    identity: ErrorIdentity,
    message: Cow<'static, str>,
}

impl KvError {
    const fn prototype(identity: ErrorIdentity) -> Self {
        Self {
            identity,
            message: Cow::Borrowed(identity.template),
        }
    }

    fn generated(identity: ErrorIdentity, message: String) -> Self {
        Self {
            identity,
            message: Cow::Owned(message),
        }
    }

    /// Returns the registered TiDB error class.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        self.identity.class
    }

    /// Returns the MySQL protocol error code.
    #[must_use]
    pub const fn mysql_code(&self) -> MysqlErrorCode {
        self.identity.code
    }

    /// Returns the registered `class:code` RFC identity.
    #[must_use]
    pub fn rfc_code(&self) -> String {
        format!(
            "{}:{}",
            self.identity.class.name(),
            self.identity.code.as_u16()
        )
    }

    /// Returns the source MySQL message template before argument generation.
    #[must_use]
    pub const fn message_template(&self) -> &'static str {
        self.identity.template
    }

    /// Returns zero-based argument positions redacted by TiDB's error layer.
    #[must_use]
    pub const fn redact_arg_positions(&self) -> &'static [usize] {
        self.identity.redact_arg_positions
    }

    /// Tests source-style error equality after following the error source chain
    /// to its root cause.
    #[must_use]
    pub fn equal(&self, error: &(dyn Error + 'static)) -> bool {
        root_cause(error)
            .downcast_ref::<Self>()
            .is_some_and(|other| self.identity.same_registered_error(other.identity))
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for KvError {}

impl PartialEq for KvError {
    fn eq(&self, other: &Self) -> bool {
        self.identity.same_registered_error(other.identity)
    }
}

impl Eq for KvError {}

fn root_cause<'a>(mut error: &'a (dyn Error + 'static)) -> &'a (dyn Error + 'static) {
    while let Some(source) = error.source() {
        error = source;
    }
    error
}

const NOT_EXIST_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::Kv,
    MysqlErrorCode::NotExist,
    tidb::errname::ErrNotExist,
);
const TXN_RETRYABLE_IDENTITY: ErrorIdentity = ErrorIdentity::new(
    ErrorClass::Kv,
    MysqlErrorCode::TxnRetryable,
    "Error: KV error safe to retry %s [try again later]",
    tidb::errname::ErrTxnRetryable.redact_arg_pos,
);
const CANNOT_SET_NIL_VALUE_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::Kv,
    MysqlErrorCode::CannotSetNilValue,
    tidb::errname::ErrCannotSetNilValue,
);
const INVALID_TXN_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::Kv,
    MysqlErrorCode::InvalidTxn,
    tidb::errname::ErrInvalidTxn,
);
const TXN_TOO_LARGE_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::Kv,
    MysqlErrorCode::TxnTooLarge,
    tidb::errname::ErrTxnTooLarge,
);
const ENTRY_TOO_LARGE_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::Kv,
    MysqlErrorCode::EntryTooLarge,
    tidb::errname::ErrEntryTooLarge,
);
const KEY_TOO_LARGE_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::Kv,
    MysqlErrorCode::KeyTooLarge,
    tidb::errname::ErrKeyTooLarge,
);
const KEY_EXISTS_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::Kv,
    MysqlErrorCode::DupEntry,
    tidb::errname::ErrDupEntry,
);
const NOT_IMPLEMENTED_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::Kv,
    MysqlErrorCode::NotImplemented,
    tidb::errname::ErrNotImplemented,
);
const WRITE_CONFLICT_IDENTITY: ErrorIdentity = ErrorIdentity::new(
    ErrorClass::Kv,
    MysqlErrorCode::WriteConflict,
    "Write conflict, txnStartTS=%d, conflictStartTS=%d, conflictCommitTS=%d, key=%s%s%s%s, reason=%s [try again later]",
    tidb::errname::ErrWriteConflict.redact_arg_pos,
);
const WRITE_CONFLICT_IN_TIDB_IDENTITY: ErrorIdentity = ErrorIdentity::new(
    ErrorClass::Kv,
    MysqlErrorCode::WriteConflictInTiDb,
    "Write conflict, txnStartTS %d is stale [try again later]",
    tidb::errname::ErrWriteConflictInTiDB.redact_arg_pos,
);
const LOCK_EXPIRE_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::TiKv,
    MysqlErrorCode::LockExpire,
    tidb::errname::ErrLockExpire,
);
const ASSERTION_FAILED_IDENTITY: ErrorIdentity = ErrorIdentity::from_message(
    ErrorClass::TiKv,
    MysqlErrorCode::AssertionFailed,
    tidb::errname::ErrAssertionFailed,
);

/// `pkg/kv.ErrNotExist`.
pub static ERR_NOT_EXIST: KvError = KvError::prototype(NOT_EXIST_IDENTITY);
/// `pkg/kv.ErrTxnRetryable`.
pub static ERR_TXN_RETRYABLE: KvError = KvError::prototype(TXN_RETRYABLE_IDENTITY);
/// `pkg/kv.ErrCannotSetNilValue`.
pub static ERR_CANNOT_SET_NIL_VALUE: KvError = KvError::prototype(CANNOT_SET_NIL_VALUE_IDENTITY);
/// `pkg/kv.ErrInvalidTxn`.
pub static ERR_INVALID_TXN: KvError = KvError::prototype(INVALID_TXN_IDENTITY);
/// `pkg/kv.ErrTxnTooLarge`.
pub static ERR_TXN_TOO_LARGE: KvError = KvError::prototype(TXN_TOO_LARGE_IDENTITY);
/// `pkg/kv.ErrEntryTooLarge`.
pub static ERR_ENTRY_TOO_LARGE: KvError = KvError::prototype(ENTRY_TOO_LARGE_IDENTITY);
/// `pkg/kv.ErrKeyTooLarge`.
pub static ERR_KEY_TOO_LARGE: KvError = KvError::prototype(KEY_TOO_LARGE_IDENTITY);
/// `pkg/kv.ErrKeyExists`.
pub static ERR_KEY_EXISTS: KvError = KvError::prototype(KEY_EXISTS_IDENTITY);
/// `pkg/kv.ErrNotImplemented`.
pub static ERR_NOT_IMPLEMENTED: KvError = KvError::prototype(NOT_IMPLEMENTED_IDENTITY);
/// `pkg/kv.ErrWriteConflict`.
pub static ERR_WRITE_CONFLICT: KvError = KvError::prototype(WRITE_CONFLICT_IDENTITY);
/// `pkg/kv.ErrWriteConflictInTiDB`.
pub static ERR_WRITE_CONFLICT_IN_TIDB: KvError =
    KvError::prototype(WRITE_CONFLICT_IN_TIDB_IDENTITY);
/// `pkg/kv.ErrLockExpire`.
pub static ERR_LOCK_EXPIRE: KvError = KvError::prototype(LOCK_EXPIRE_IDENTITY);
/// `pkg/kv.ErrAssertionFailed`.
pub static ERR_ASSERTION_FAILED: KvError = KvError::prototype(ASSERTION_FAILED_IDENTITY);

/// Returns whether the root cause has one of the three source retryable KV
/// identities.
#[must_use]
pub fn is_txn_retryable_error(error: Option<&(dyn Error + 'static)>) -> bool {
    let Some(error) = error else {
        return false;
    };

    ERR_TXN_RETRYABLE.equal(error)
        || ERR_WRITE_CONFLICT.equal(error)
        || ERR_WRITE_CONFLICT_IN_TIDB.equal(error)
}

/// Returns whether the root cause is `ERR_NOT_EXIST`.
#[must_use]
pub fn is_err_not_found(error: Option<&(dyn Error + 'static)>) -> bool {
    error.is_some_and(|error| ERR_NOT_EXIST.equal(error))
}

/// Generates `ERR_KEY_EXISTS` by joining handle-column values with `-`.
///
/// The source MySQL template limits the entry to 64 Unicode characters and
/// the key name to 192 Unicode characters. Go's `fmt` precision for `%s` is in
/// runes, so this truncates by Rust `char`, not by UTF-8 byte.
#[must_use]
pub fn gen_key_exists_err<S: AsRef<str>>(key_columns: &[S], key_name: &str) -> KvError {
    let mut joined = String::new();
    for (index, column) in key_columns.iter().enumerate() {
        if index != 0 {
            joined.push('-');
        }
        joined.push_str(column.as_ref());
    }

    let entry = truncate_chars(&joined, 64);
    let key = truncate_chars(key_name, 192);
    KvError::generated(
        KEY_EXISTS_IDENTITY,
        format!("Duplicate entry '{entry}' for key '{key}'"),
    )
}

/// Generates `pkg/kv.ErrTxnTooLarge` with the source size argument.
#[must_use]
pub fn gen_txn_too_large_err(size: i64) -> KvError {
    KvError::generated(
        TXN_TOO_LARGE_IDENTITY,
        format!("Transaction is too large, size: {size}"),
    )
}

/// Generates `pkg/kv.ErrEntryTooLarge` with source limit and actual sizes.
#[must_use]
pub fn gen_entry_too_large_err(limit: u64, size: u64) -> KvError {
    KvError::generated(
        ENTRY_TOO_LARGE_IDENTITY,
        format!("entry too large, the max entry size is {limit}, the size of data is {size}"),
    )
}

/// Generates `pkg/kv.ErrKeyTooLarge` with the source key size.
#[must_use]
pub fn gen_key_too_large_err(key_size: i64) -> KvError {
    KvError::generated(
        KEY_TOO_LARGE_IDENTITY,
        format!("key is too large, the size of given key is {key_size}"),
    )
}

/// Generates `pkg/kv.ErrWriteConflictInTiDB` for a latch conflict.
#[must_use]
pub fn gen_write_conflict_in_tidb_err(start_ts: u64) -> KvError {
    KvError::generated(
        WRITE_CONFLICT_IN_TIDB_IDENTITY,
        format!("Write conflict, txnStartTS {start_ts} is stale [try again later]"),
    )
}

fn truncate_chars(value: &str, maximum: usize) -> Cow<'_, str> {
    let mut characters = value.char_indices();
    let Some((end, _)) = characters.nth(maximum) else {
        return Cow::Borrowed(value);
    };
    Cow::Owned(value[..end].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ContextError {
        source: KvError,
    }

    impl fmt::Display for ContextError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("context")
        }
    }

    impl Error for ContextError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.source)
        }
    }

    #[test]
    fn generated_errors_keep_identity() {
        let generated = gen_key_exists_err(&["one", "two"], "primary");
        assert_eq!(generated, ERR_KEY_EXISTS);
        assert!(ERR_KEY_EXISTS.equal(&generated));
        assert!(!ERR_NOT_EXIST.equal(&generated));

        let same_registered_error_with_different_metadata = KvError::prototype(ErrorIdentity::new(
            ErrorClass::Kv,
            MysqlErrorCode::DupEntry,
            "different template",
            &[],
        ));
        assert_eq!(
            same_registered_error_with_different_metadata,
            ERR_KEY_EXISTS
        );
        assert!(ERR_KEY_EXISTS.equal(&same_registered_error_with_different_metadata));
    }

    #[test]
    fn character_precision_does_not_split_utf8() {
        assert_eq!(truncate_chars("数据库", 2), "数据");
        assert_eq!(truncate_chars("数据库", 3), "数据库");
        assert_eq!(truncate_chars("database", 64), "database");
    }

    #[test]
    fn classifiers_follow_wrapped_root_identity() {
        let retryable = ContextError {
            source: ERR_TXN_RETRYABLE.clone(),
        };
        let missing = ContextError {
            source: ERR_NOT_EXIST.clone(),
        };

        assert!(is_txn_retryable_error(Some(&retryable)));
        assert!(is_err_not_found(Some(&missing)));
        assert!(!is_txn_retryable_error(Some(&missing)));
    }
}
