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

//! Recognizing a session's transaction-control statements.

use tidb_ast::{SessionStmt, Stmt, TransactionMode};

/// One recognized transaction-control statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionControl {
    /// `BEGIN` / `START TRANSACTION` opening a transaction.
    ///
    /// `mode` is the statement's own `OPTIMISTIC`/`PESSIMISTIC` keyword, which
    /// outranks `@@tidb_txn_mode`; `TransactionMode::Default` means the
    /// variable decides. Resolve it with
    /// [`crate::txn_mode::txn_mode_for_begin`].
    Begin {
        /// The mode named by the statement itself.
        mode: TransactionMode,
    },
    /// `COMMIT`: publish the transaction's buffered writes.
    Commit,
    /// `ROLLBACK`: discard them, releasing anything the transaction holds.
    ///
    /// Kept apart from [`Self::Commit`] because a transaction that buffered
    /// writes does opposite things for the two, and a read-only transaction
    /// only *looks* like it does the same thing for both.
    Rollback,
    /// A transaction-control statement the bounded node cannot honor yet (named
    /// for its diagnostic), e.g. `START TRANSACTION ... AS OF TIMESTAMP`.
    Unsupported(&'static str),
}

/// Classifies whether `sql` is a transaction-control statement.
///
/// Returns `None` when `sql` is not `BEGIN`/`START TRANSACTION`, `COMMIT`, or
/// `ROLLBACK` — including when it fails to parse, so the ordinary query path
/// reports the exact parse error instead of this classifier swallowing it.
/// Mirrors Go `pkg/session`'s dispatch of `ast.BeginStmt`/`ast.CommitStmt`/
/// `ast.RollbackStmt` to transaction control rather than statement execution.
#[must_use]
pub fn classify_transaction_control(sql: &str) -> Option<TransactionControl> {
    let Ok(statement) = tidb_parser::parse(sql) else {
        return None;
    };
    let Stmt::Session(session) = statement else {
        return None;
    };
    match session.into_inner() {
        SessionStmt::Begin(begin) => {
            if begin.as_of.is_some() {
                // `START TRANSACTION READ ONLY AS OF TIMESTAMP <expr>` pins a
                // specific historical snapshot, a distinct feature this slice
                // does not own.
                Some(TransactionControl::Unsupported(
                    "START TRANSACTION ... AS OF TIMESTAMP",
                ))
            } else {
                Some(TransactionControl::Begin { mode: begin.mode })
            }
        }
        SessionStmt::Commit(tidb_ast::CompletionType::Default) => Some(TransactionControl::Commit),
        SessionStmt::Rollback {
            savepoint: None,
            completion: tidb_ast::CompletionType::Default,
        } => Some(TransactionControl::Rollback),
        SessionStmt::Commit(completion) if completion != tidb_ast::CompletionType::Default => Some(
            TransactionControl::Unsupported("transaction completion mode"),
        ),
        SessionStmt::Rollback { completion, .. }
            if completion != tidb_ast::CompletionType::Default =>
        {
            Some(TransactionControl::Unsupported(
                "transaction completion mode",
            ))
        }
        // Savepoints and every other session statement are not this node's
        // transaction control; the query path admits or rejects them.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_transaction_control, TransactionControl};
    use tidb_ast::TransactionMode;

    #[test]
    fn begin_and_start_transaction_open_a_transaction() {
        assert_eq!(
            classify_transaction_control("BEGIN"),
            Some(TransactionControl::Begin {
                mode: TransactionMode::Default
            })
        );
        assert_eq!(
            classify_transaction_control("start transaction"),
            Some(TransactionControl::Begin {
                mode: TransactionMode::Default
            })
        );
        assert_eq!(
            classify_transaction_control("START TRANSACTION READ ONLY"),
            Some(TransactionControl::Begin {
                mode: TransactionMode::Default
            })
        );
        // The BEGIN spelling carries the mode the transaction opens in.
        assert_eq!(
            classify_transaction_control("BEGIN PESSIMISTIC"),
            Some(TransactionControl::Begin {
                mode: TransactionMode::Pessimistic
            })
        );
        assert_eq!(
            classify_transaction_control("begin optimistic"),
            Some(TransactionControl::Begin {
                mode: TransactionMode::Optimistic
            })
        );
    }

    #[test]
    fn commit_and_rollback_end_a_transaction() {
        assert_eq!(
            classify_transaction_control("COMMIT"),
            Some(TransactionControl::Commit)
        );
        assert_eq!(
            classify_transaction_control("rollback"),
            Some(TransactionControl::Rollback)
        );
    }

    #[test]
    fn ordinary_statements_are_not_transaction_control() {
        assert_eq!(classify_transaction_control("SELECT 1"), None);
        assert_eq!(classify_transaction_control("SET autocommit = 1"), None);
        assert_eq!(classify_transaction_control("USE test"), None);
        // A savepoint is transaction-related but not BEGIN/COMMIT/ROLLBACK.
        assert_eq!(classify_transaction_control("SAVEPOINT s1"), None);
    }

    #[test]
    fn unparseable_sql_is_left_for_the_query_path() {
        assert_eq!(classify_transaction_control("NOT VALID SQL @@"), None);
    }

    #[test]
    fn a_time_travel_transaction_is_recognized_but_unsupported() {
        assert!(matches!(
            classify_transaction_control(
                "START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-01-01 00:00:00'"
            ),
            Some(TransactionControl::Unsupported(_))
        ));
    }
}
