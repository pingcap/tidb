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
//! two statements that REPORT it must not clear it -- see
//! [`reports_warnings`].

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

/// Whether the statement reports the warning buffer, and so must not clear it
/// before running. Go decides this on the parsed node; parsing here would mean
/// parsing the statement twice, so this reads the leading keywords the same
/// way the dispatcher's own fast paths do.
pub(crate) fn reports_warnings(sql: &str) -> bool {
    let mut words = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .filter(|word| !word.is_empty());
    if !words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("SHOW"))
    {
        return false;
    }
    // `SHOW WARNINGS`, `SHOW ERRORS`, and the `SHOW COUNT(*) WARNINGS` form.
    words.any(|word| {
        let word = word.trim_end_matches(';');
        word.eq_ignore_ascii_case("WARNINGS") || word.eq_ignore_ascii_case("ERRORS")
    })
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
