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
