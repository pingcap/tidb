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

//! `LOAD DATA` payloads translated from `pkg/parser/ast/dml.go`.

use crate::util::{back_quote, escape_string_literal, push_name_path};
use crate::{Assignment, Expr};

/// TiDB's duplicate-key action in `LOAD DATA`.
///
/// This is deliberately not an `INSERT` option: local-file loads gain an
/// AST-visible implicit `IGNORE` action in Go's parser, whereas ordinary
/// server-side loads retain the error action when neither modifier is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadDataOnDuplicate {
    /// No modifier was written for a server-side load.
    Error,
    /// `REPLACE`.
    Replace,
    /// Explicit `IGNORE`, or Go's implicit `LOCAL` default.
    Ignore,
}

/// A target-column or source user-variable in a file-load mapping.
///
/// Go uses one `ColumnNameOrUserVar` node for both `LOAD DATA` and `IMPORT
/// INTO`; keeping that shared shape avoids two subtly divergent parsers and
/// restores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnOrUserVar {
    /// A destination table column.
    Column(String),
    /// A source user-variable name, without its leading `@`.
    UserVar(String),
}

impl ColumnOrUserVar {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Column(name) => out.push_str(&back_quote(name)),
            Self::UserVar(name) => {
                out.push('@');
                out.push_str(&back_quote(name));
            }
        }
    }
}

/// One parser-level `WITH` option shared by `LOAD DATA` and `IMPORT INTO`.
///
/// TiDB intentionally accepts a raw lowercased option name and an optional
/// signed literal here. Option validation belongs to its import/load job
/// pipeline rather than the grammar production.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadDataOption {
    /// Lowercase option spelling as retained by Go's `LoadDataOpt.Name`.
    pub name: String,
    /// The optional literal value after `=` / `:=`.
    pub value: Option<Expr>,
}

impl LoadDataOption {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(&self.name);
        if let Some(value) = &self.value {
            out.push('=');
            value.restore_into(out);
        }
    }
}

/// The optional `FIELDS`/`COLUMNS` clause on `LOAD DATA`.
///
/// The fixed restore order is Go's `FieldsClause.Restore` order, not source
/// order. `ENCLOSED BY` and `ESCAPED BY` are validated by the parser because
/// TiDB requires a one-byte separator (or a single backslash).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadDataFields {
    /// `TERMINATED BY` value.
    pub terminated: Option<String>,
    /// `ENCLOSED BY` value.
    pub enclosed: Option<String>,
    /// Whether the enclosed form was `OPTIONALLY ENCLOSED BY`.
    pub optionally_enclosed: bool,
    /// `ESCAPED BY` value.
    pub escaped: Option<String>,
    /// `DEFINED NULL BY` value.
    pub defined_null_by: Option<String>,
    /// `OPTIONALLY ENCLOSED` after `DEFINED NULL BY`.
    pub null_optionally_enclosed: bool,
}

impl LoadDataFields {
    fn restore_into(&self, out: &mut String) {
        if self.terminated.is_none()
            && self.enclosed.is_none()
            && self.escaped.is_none()
            && self.defined_null_by.is_none()
        {
            return;
        }
        out.push_str(" FIELDS");
        if let Some(value) = &self.terminated {
            out.push_str(" TERMINATED BY ");
            restore_load_string(out, value);
        }
        if let Some(value) = &self.enclosed {
            if self.optionally_enclosed {
                out.push_str(" OPTIONALLY");
            }
            out.push_str(" ENCLOSED BY ");
            restore_load_string(out, value);
        }
        if let Some(value) = &self.escaped {
            out.push_str(" ESCAPED BY ");
            restore_load_string(out, value);
        }
        if let Some(value) = &self.defined_null_by {
            out.push_str(" DEFINED NULL BY ");
            restore_load_string(out, value);
            if self.null_optionally_enclosed {
                out.push_str(" OPTIONALLY ENCLOSED");
            }
        }
    }
}

/// The optional `LINES` clause on `LOAD DATA`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadDataLines {
    /// `STARTING BY` value.
    pub starting: Option<String>,
    /// `TERMINATED BY` value.
    pub terminated: Option<String>,
}

impl LoadDataLines {
    fn restore_into(&self, out: &mut String) {
        if self.starting.is_none() && self.terminated.is_none() {
            return;
        }
        out.push_str(" LINES");
        if let Some(value) = &self.starting {
            out.push_str(" STARTING BY ");
            restore_load_string(out, value);
        }
        if let Some(value) = &self.terminated {
            out.push_str(" TERMINATED BY ");
            restore_load_string(out, value);
        }
    }
}

/// TiDB's `LOAD DATA` parser/restore envelope.
///
/// Reading client/server/external files, applying duplicate handling, and
/// coordinating TiDB's distributed import pipeline are executor concerns; the
/// seed executor rejects this statement before it opens an implicit
/// transaction rather than pretending a file load succeeded.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadDataStmt {
    /// `LOW_PRIORITY` modifier.
    pub low_priority: bool,
    /// `LOCAL` file location; false is Go's server-or-remote location.
    pub local: bool,
    /// Decoded `INFILE` path.
    pub path: String,
    /// Optional decoded `FORMAT` string.
    pub format: Option<String>,
    /// Duplicate-key action, including LOCAL's parser default.
    pub on_duplicate: LoadDataOnDuplicate,
    /// Target table name path.
    pub table: Vec<String>,
    /// Optional canonical charset name.
    pub charset: Option<String>,
    /// Optional field delimiters.
    pub fields: LoadDataFields,
    /// Optional line delimiters.
    pub lines: LoadDataLines,
    /// Optional `IGNORE n LINES` prefix.
    pub ignore_lines: Option<u64>,
    /// Optional column/user-variable mapping.
    pub columns_and_user_vars: Vec<ColumnOrUserVar>,
    /// Optional `SET` assignments.
    pub column_assignments: Vec<Assignment>,
    /// Optional load-job options.
    pub options: Vec<LoadDataOption>,
}

impl LoadDataStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("LOAD DATA ");
        if self.low_priority {
            out.push_str("LOW_PRIORITY ");
        }
        if self.local {
            out.push_str("LOCAL ");
        }
        out.push_str("INFILE ");
        restore_load_string(out, &self.path);
        if let Some(format) = &self.format {
            out.push_str(" FORMAT ");
            restore_load_string(out, format);
        }
        match self.on_duplicate {
            LoadDataOnDuplicate::Error => {}
            LoadDataOnDuplicate::Replace => out.push_str(" REPLACE"),
            LoadDataOnDuplicate::Ignore => out.push_str(" IGNORE"),
        }
        out.push_str(" INTO TABLE ");
        push_name_path(out, &self.table);
        if let Some(charset) = &self.charset {
            out.push_str(" CHARACTER SET ");
            out.push_str(charset);
        }
        self.fields.restore_into(out);
        self.lines.restore_into(out);
        if let Some(ignore_lines) = self.ignore_lines {
            out.push_str(" IGNORE ");
            out.push_str(&ignore_lines.to_string());
            out.push_str(" LINES");
        }
        if !self.columns_and_user_vars.is_empty() {
            out.push_str(" (");
            for (index, column) in self.columns_and_user_vars.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                column.restore_into(out);
            }
            out.push(')');
        }
        if !self.column_assignments.is_empty() {
            out.push_str(" SET");
            for (index, assignment) in self.column_assignments.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push(' ');
                assignment.restore_into(out);
            }
        }
        if !self.options.is_empty() {
            out.push_str(" WITH");
            for (index, option) in self.options.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push(' ');
                option.restore_into(out);
            }
        }
    }
}

fn restore_load_string(out: &mut String, value: &str) {
    out.push('\'');
    out.push_str(&escape_string_literal(value));
    out.push('\'');
}
