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

//! Go `pkg/ddl/executor.go`'s charset / collation rules for ALTER TABLE:
//! `checkModifyCharsetAndCollation` and
//! `isCharsetAllowingDirectCollationChange`. A MODIFY / CHANGE COLUMN
//! conversion is classified by [`crate::modify_column_change`].

use tidb_terror::MysqlError;

/// Go `checkModifyCharsetAndCollation`'s refusals.
pub enum CharsetChangeRefusal {
    /// `ErrUnknownCharacterSet`: the target pair is not a valid one.
    UnknownCharset(String),
    /// `ErrUnsupportedModifyCollation`: an indexed column would need its
    /// entries rewritten for an incompatible collation. Every Go caller
    /// rewords it, so the caller supplies the message.
    Collation,
    /// `ErrUnsupportedModifyCharset`, with Go's `modify %s` argument.
    Charset(String),
}

impl CharsetChangeRefusal {
    /// The error, with the indexed-collation message worded by the caller.
    pub fn into_error(self, collation_message: impl FnOnce() -> String) -> MysqlError {
        match self {
            Self::UnknownCharset(message) => MysqlError::from_terror(
                &tidb_dbterror::ERR_UNKNOWN_CHARACTER_SET.gen_with_stack(message),
            ),
            Self::Collation => MysqlError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_MODIFY_COLLATION
                    .gen_with_stack(collation_message()),
            ),
            Self::Charset(reason) => MysqlError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_MODIFY_CHARSET
                    .gen_with_stack_by_args(&[reason.into()]),
            ),
        }
    }
}

/// Go `isCharsetAllowingDirectCollationChange`: TiDB only allows
/// utf8/latin1 to utf8mb4, or a collation change within utf8/utf8mb4.
#[must_use]
pub fn is_charset_allowing_direct_collation_change(orig_charset: &str, to_charset: &str) -> bool {
    matches!(
        (orig_charset, to_charset),
        ("utf8", "utf8mb4") | ("utf8", "utf8") | ("utf8mb4", "utf8mb4") | ("latin1", "utf8mb4")
    )
}

/// Go `checkModifyCharsetAndCollation`: a valid target pair; an indexed
/// column may not move to an incompatible collation under the new collation
/// framework; and only utf8/latin1 to utf8mb4, or a collation change within
/// utf8/utf8mb4, is metadata-only.
///
/// # Errors
///
/// The refusal Go returns.
pub fn check_modify_charset_and_collation(
    to_charset: &str,
    to_collation: &str,
    from_charset: &str,
    from_collation: &str,
    need_rewrite_collation_data: bool,
) -> Result<(), CharsetChangeRefusal> {
    if !tidb_charset::valid_charset_and_collation(to_charset, to_collation) {
        return Err(CharsetChangeRefusal::UnknownCharset(format!(
            "Unknown character set: '{to_charset}', collation: '{to_collation}'"
        )));
    }
    if need_rewrite_collation_data
        && tidb_datatype::new_collation_enabled()
        && !tidb_datatype::compatible_collate(from_collation, to_collation)
    {
        return Err(CharsetChangeRefusal::Collation);
    }
    if is_charset_allowing_direct_collation_change(from_charset, to_charset) {
        return Ok(());
    }
    if to_charset != from_charset {
        return Err(CharsetChangeRefusal::Charset(format!(
            "charset from {from_charset} to {to_charset}"
        )));
    }
    if to_collation != from_collation {
        return Err(CharsetChangeRefusal::Charset(format!(
            "change collate from {from_collation} to {to_collation}"
        )));
    }
    Ok(())
}
