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

//! Transaction-control statement payloads.

use crate::{Expr, TransactionMode};

/// Completion mode carried by `COMMIT` and `ROLLBACK`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompletionType {
    /// Default / `NO RELEASE` / `AND NO CHAIN`; restores nothing.
    #[default]
    Default,
    /// `AND CHAIN`.
    Chain,
    /// `RELEASE`.
    Release,
}

impl CompletionType {
    pub(crate) const fn sql(self) -> &'static str {
        match self {
            Self::Default => "",
            Self::Chain => " AND CHAIN",
            Self::Release => " RELEASE",
        }
    }
}

/// The complete payload carried by Go's `ast.BeginStmt`.
///
/// `READ WRITE` and `WITH CONSISTENT SNAPSHOT` deliberately have no dedicated
/// flag: Go's own parser treats both as the default transaction and restores
/// them as bare `START TRANSACTION`. In contrast, read-only, AS OF, and causal
/// consistency are AST-visible and must remain visible even when an executor
/// cannot implement their distributed semantics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BeginStmt {
    /// The `BEGIN` mode, if this statement used the `BEGIN` spelling.
    pub mode: TransactionMode,
    /// `START TRANSACTION READ ONLY`.
    pub read_only: bool,
    /// `AS OF TIMESTAMP <expr>`, valid only together with [`Self::read_only`]
    /// in the grammar.
    pub as_of: Option<Expr>,
    /// `START TRANSACTION WITH CAUSAL CONSISTENCY ONLY`.
    pub causal_consistency_only: bool,
}

impl BeginStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self.mode {
            TransactionMode::Default if self.read_only => {
                out.push_str("START TRANSACTION READ ONLY");
                if let Some(as_of) = &self.as_of {
                    out.push_str(" AS OF TIMESTAMP ");
                    as_of.restore_into(out);
                }
            }
            TransactionMode::Default if self.causal_consistency_only => {
                out.push_str("START TRANSACTION WITH CAUSAL CONSISTENCY ONLY");
            }
            TransactionMode::Default => out.push_str("START TRANSACTION"),
            TransactionMode::Optimistic => out.push_str("BEGIN OPTIMISTIC"),
            TransactionMode::Pessimistic => out.push_str("BEGIN PESSIMISTIC"),
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for BeginStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            mode,
            read_only,
            as_of,
            causal_consistency_only,
        } = self;
        if !crate::Visitable::accept(mode, visitor) {
            return false;
        }
        if let Some(value) = as_of.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = mode;
        let _ = read_only;
        let _ = as_of;
        let _ = causal_consistency_only;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for CompletionType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visitor.leave(self)
    }
}
