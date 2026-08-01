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

//! The statement warning buffer, which Go keeps in `StmtCtx.warnings` and
//! `SHOW WARNINGS` / `SHOW ERRORS` report.
//!
//! The buffer belongs to the statement before the one running, which is why the
//! statements that REPORT it must be handed the previous statement's entries --
//! see [`reports_warnings`].

use tidb_ast::{AdminStmt, ShowInspectionKind, Stmt};
use tidb_datatype::{Datum, FieldType};

use crate::{Session, StmtOutput};

/// Go `ddl.errCheckConstraintIsOff` is built with `errors.NewNoStackError`, so
/// it carries no MySQL code of its own and `AppendWarning` files it under
/// `ER_UNKNOWN_ERROR`. Captured through testkit's `SHOW WARNINGS`:
/// `Warning | 1105 | tidb_enable_check_constraint is off`.
pub(crate) const CHECK_CONSTRAINT_IS_OFF_CODE: u16 = 1105;
/// See [`CHECK_CONSTRAINT_IS_OFF_CODE`]; the text is the variable name Go
/// interpolates, not a sentence, so it is reproduced verbatim.
pub(crate) const CHECK_CONSTRAINT_IS_OFF_MESSAGE: &str = "tidb_enable_check_constraint is off";

/// Go `dbterror.ErrUnsupportedCreatePartition` (8200), the code a `LINEAR
/// HASH`/`LINEAR KEY` clause is warned under while the table is built as a
/// plain non-linear one. Captured through testkit's `SHOW WARNINGS`:
/// `Warning | 8200 | LINEAR HASH is not supported, using non-linear HASH
/// instead`.
pub(crate) const UNSUPPORTED_CREATE_PARTITION_CODE: u16 = 8200;

/// A statement warning, which Go keeps in `StmtCtx` and `SHOW WARNINGS`
/// reports as `Level | Code | Message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlWarning {
    /// Whether the statement survived it.
    pub level: WarningLevel,
    /// The MySQL error code the warning carries.
    pub code: u16,
    /// The message text.
    pub message: String,
}

/// A warning's `Level` column, which Go fills from
/// `StmtCtx.warnings[i].Level`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningLevel {
    /// The statement continued.
    Warning,
    /// The statement failed; Go records its error in the same buffer.
    Error,
}

impl WarningLevel {
    /// The text the `Level` column shows.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            WarningLevel::Warning => "Warning",
            WarningLevel::Error => "Error",
        }
    }
}

/// Whether the statement reports the warning buffer, and so must be handed the
/// previous statement's entries instead of an empty buffer.
///
/// Go decides this on the parsed node: `ResetContextOfStmt` takes an
/// `ast.StmtNode`, builds a fresh `StatementContext`, and for exactly
/// `ShowWarnings`, `ShowErrors`, and `ShowSessionStates` copies the outgoing
/// context's entries into it (`pkg/executor/select.go`, `case *ast.ShowStmt`).
/// This mirrors that set. `SHOW COUNT(*) WARNINGS` parses to `ShowWarnings`
/// with `count_only`, so it needs no separate spelling.
pub(crate) fn reports_warnings(stmt: &Stmt) -> bool {
    let Stmt::Admin(admin) = stmt else {
        return false;
    };
    match &**admin {
        AdminStmt::ShowWarnings(_) | AdminStmt::ShowErrors(_) => true,
        // Go lists `ShowSessionStates` beside the other two. `SHOW
        // SESSION_STATES` is refused by `show.rs` today, so this arm is
        // source fidelity rather than reachable behaviour; it stays here so
        // admitting the statement later does not silently drop the buffer.
        AdminStmt::ShowInspection(show) => show.kind == ShowInspectionKind::SessionStates,
        _ => false,
    }
}

impl Session {
    pub(crate) fn warning_output(&self, count_only: bool, errors_only: bool) -> StmtOutput {
        let reported = self
            .warnings
            .iter()
            .filter(|warning| !errors_only || warning.level == WarningLevel::Error);
        if count_only {
            let count = reported.count() as i64;
            let name = if errors_only {
                "@@session.error_count"
            } else {
                "@@session.warning_count"
            };
            return StmtOutput::Rows {
                columns: vec![(
                    name.to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )],
                rows: vec![vec![Datum::Int(count)]],
            };
        }
        let text = || FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let rows = reported
            .map(|warning| {
                vec![
                    Datum::Bytes(warning.level.as_str().as_bytes().to_vec()),
                    Datum::Int(i64::from(warning.code)),
                    Datum::Bytes(warning.message.clone().into_bytes()),
                ]
            })
            .collect();
        StmtOutput::Rows {
            columns: vec![
                ("Level".to_owned(), text()),
                (
                    "Code".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ),
                ("Message".to_owned(), text()),
            ],
            rows,
        }
    }

    /// Moves what evaluation recorded into the statement's warning buffer.
    pub(crate) fn drain_eval_warnings(&mut self, ctx: &tidb_executor::StmtContext) {
        for (code, message) in ctx.take_warnings() {
            self.warnings.push(SqlWarning {
                level: WarningLevel::Warning,
                code,
                message,
            });
        }
    }

    /// The warnings the last statement produced.
    #[must_use]
    pub fn warnings(&self) -> &[SqlWarning] {
        &self.warnings
    }
}
