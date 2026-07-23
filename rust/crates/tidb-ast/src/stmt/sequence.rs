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

//! Typed statements and canonical restore owned by Go's
//! `pkg/parser/ddl_sequence_parser.go` source domain.
//!
//! That Go file also owns `ALTER INSTANCE RELOAD TLS` and `ALTER RANGE`;
//! keeping those leaves here preserves source ownership even though only the
//! three sequence statements mutate sequence metadata.

use crate::util::{back_quote, push_name_path};
use crate::{PlacementOption, PlacementRestoreMode, TableOption};

/// A `CREATE SEQUENCE [IF NOT EXISTS] name [options...]` statement.
///
/// Go lets sequence options and ordinary table options interleave, then
/// restores every sequence option before every table option while preserving
/// order within each group. The parser therefore keeps the two groups
/// separately instead of trying to reconstruct source order later.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSequenceStmt {
    /// Whether `IF NOT EXISTS` was written.
    pub if_not_exists: bool,
    /// The sequence name path.
    pub name: Vec<String>,
    /// Sequence options, in source order.
    pub options: Vec<SequenceOption>,
    /// All Go `parseTableOption` payloads, in source order.
    pub table_options: Vec<TableOption>,
}

impl CreateSequenceStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE SEQUENCE ");
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        push_name_path(out, &self.name);
        for option in &self.options {
            out.push(' ');
            option.restore_into(out);
        }
        for option in &self.table_options {
            out.push(' ');
            option.restore_into(out);
        }
    }
}

/// An `ALTER SEQUENCE [IF EXISTS] name option [option ...]` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterSequenceStmt {
    /// Whether `IF EXISTS` was written.
    pub if_exists: bool,
    /// The sequence name path.
    pub name: Vec<String>,
    /// The non-empty sequence-option list, in source order.
    pub options: Vec<SequenceOption>,
}

impl AlterSequenceStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER SEQUENCE ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        push_name_path(out, &self.name);
        for option in &self.options {
            out.push(' ');
            option.restore_into(out);
        }
    }
}

/// A `DROP SEQUENCE [IF EXISTS] name [, name2 ...]` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSequenceStmt {
    /// Whether `IF EXISTS` was written.
    pub if_exists: bool,
    /// Sequence name paths, in source order.
    pub names: Vec<Vec<String>>,
}

impl DropSequenceStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP SEQUENCE ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        for (index, name) in self.names.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, name);
        }
    }
}

/// One sequence option after Go's canonical spelling normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceOption {
    /// `INCREMENT [BY | =] n`.
    IncrementBy(i64),
    /// `START [WITH | =] n`.
    StartWith(i64),
    /// `MINVALUE [=] n`.
    MinValue(i64),
    /// `NOMINVALUE` or `NO MINVALUE`.
    NoMinValue,
    /// `MAXVALUE [=] n`.
    MaxValue(i64),
    /// `NOMAXVALUE` or `NO MAXVALUE`.
    NoMaxValue,
    /// `CACHE [=] n`.
    Cache(i64),
    /// `NOCACHE` or `NO CACHE`.
    NoCache,
    /// `CYCLE`.
    Cycle,
    /// `NOCYCLE` or `NO CYCLE`.
    NoCycle,
    /// Bare `RESTART`, valid only in `ALTER SEQUENCE`.
    Restart,
    /// `RESTART [WITH | =] n`, valid only in `ALTER SEQUENCE`.
    RestartWith(i64),
}

impl SequenceOption {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::IncrementBy(value) => push_integer_option(out, "INCREMENT BY", *value),
            Self::StartWith(value) => push_integer_option(out, "START WITH", *value),
            Self::MinValue(value) => push_integer_option(out, "MINVALUE", *value),
            Self::NoMinValue => out.push_str("NO MINVALUE"),
            Self::MaxValue(value) => push_integer_option(out, "MAXVALUE", *value),
            Self::NoMaxValue => out.push_str("NO MAXVALUE"),
            Self::Cache(value) => push_integer_option(out, "CACHE", *value),
            Self::NoCache => out.push_str("NOCACHE"),
            Self::Cycle => out.push_str("CYCLE"),
            Self::NoCycle => out.push_str("NOCYCLE"),
            Self::Restart => out.push_str("RESTART"),
            Self::RestartWith(value) => push_integer_option(out, "RESTART WITH", *value),
        }
    }
}

fn push_integer_option(out: &mut String, name: &str, value: i64) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
}

/// `ALTER INSTANCE RELOAD TLS [NO ROLLBACK ON ERROR]`.
///
/// `ReloadTLS` is always true for every state the Go parser can construct, so
/// it is encoded by the statement variant rather than retained as a boolean
/// that would admit the invalid `ALTER INSTANCE` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlterInstanceStmt {
    /// Whether `NO ROLLBACK ON ERROR` was written.
    pub no_rollback_on_error: bool,
}

impl AlterInstanceStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER INSTANCE RELOAD TLS");
        if self.no_rollback_on_error {
            out.push_str(" NO ROLLBACK ON ERROR");
        }
    }
}

/// `ALTER RANGE name placement_option`.
///
/// Go admits a bare `ALTER RANGE name`, leaving the placement option nil.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRangeStmt {
    /// The range name.
    pub range_name: String,
    /// The optional placement payload.
    pub placement: Option<PlacementOption>,
}

impl AlterRangeStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER RANGE ");
        out.push_str(&back_quote(&self.range_name));
        if let Some(placement) = &self.placement {
            out.push(' ');
            placement.restore_into(out, PlacementRestoreMode::Default);
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for CreateSequenceStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            if_not_exists,
            name,
            options,
            table_options,
        } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in table_options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = if_not_exists;
        let _ = name;
        let _ = options;
        let _ = table_options;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterSequenceStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            if_exists,
            name,
            options,
        } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = if_exists;
        let _ = name;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropSequenceStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { if_exists, names } = self;
        let _ = if_exists;
        let _ = names;
        visitor.leave(self)
    }
}

impl crate::Visitable for SequenceOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::IncrementBy(field_0) => {
                let _ = field_0;
            }
            Self::StartWith(field_0) => {
                let _ = field_0;
            }
            Self::MinValue(field_0) => {
                let _ = field_0;
            }
            Self::NoMinValue => {}
            Self::MaxValue(field_0) => {
                let _ = field_0;
            }
            Self::NoMaxValue => {}
            Self::Cache(field_0) => {
                let _ = field_0;
            }
            Self::NoCache => {}
            Self::Cycle => {}
            Self::NoCycle => {}
            Self::Restart => {}
            Self::RestartWith(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterInstanceStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            no_rollback_on_error,
        } = self;
        let _ = no_rollback_on_error;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterRangeStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            range_name,
            placement,
        } = self;
        if let Some(value) = placement.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = range_name;
        let _ = placement;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
