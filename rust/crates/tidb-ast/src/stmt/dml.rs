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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Data-manipulation statements and their shared restore boundary.

use crate::util::push_name_path;
use crate::{
    DeleteStmt, DistributeTableStmt, Expr, ImportIntoStmt, InsertStmt, LoadDataStmt,
    RestoreContext, UpdateStmt, WithClause,
};

/// A statement that mutates table rows.
#[derive(Debug, Clone, PartialEq)]
pub enum DmlStmt {
    /// A top-level `WITH ... <DML>` statement. The CTE prefix belongs to the
    /// mutation statement, not to a synthetic SELECT.
    With {
        /// The source-ordered CTE definitions.
        with: WithClause,
        /// The DML statement governed by those CTE definitions.
        statement: Box<DmlStmt>,
    },
    /// An `INSERT` or `REPLACE` statement.
    Insert(Box<InsertStmt>),
    /// An `UPDATE` statement.
    Update(Box<UpdateStmt>),
    /// A `DELETE` statement.
    Delete(Box<DeleteStmt>),
    /// An `IMPORT INTO` statement.
    ImportInto(Box<ImportIntoStmt>),
    /// A `LOAD DATA` file-load statement.
    LoadData(Box<LoadDataStmt>),
    /// A TiDB non-transactional `BATCH ... <DML>` wrapper.
    Batch(Box<BatchDmlStmt>),
    /// A physical `DISTRIBUTE TABLE` request.
    DistributeTable(Box<DistributeTableStmt>),
    /// A stored-procedure invocation.
    Call(Box<CallStmt>),
}

impl DmlStmt {
    /// Appends the DML statement's canonical SQL to `out`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, &RestoreContext::default());
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        match self {
            Self::With { with, statement } => {
                let scoped = with.restore_into_with_context(out, context);
                out.push(' ');
                statement.restore_into_with_context(out, &scoped);
            }
            Self::Insert(insert) => insert.restore_into_with_context(out, context),
            Self::Update(update) => update.restore_into_with_context(out, context),
            Self::Delete(delete) => delete.restore_into_with_context(out, context),
            Self::ImportInto(import) => import.restore_into(out),
            Self::LoadData(load) => load.restore_into(out),
            Self::Batch(batch) => batch.restore_into(out),
            Self::DistributeTable(distribute) => distribute.restore_into(out),
            Self::Call(call) => call.restore_into(out),
        }
    }
}

/// `CALL procedure[(arg, ...)]`.
#[derive(Debug, Clone, PartialEq)]
pub struct CallStmt {
    /// Qualified procedure name.
    pub name: Vec<String>,
    /// Call arguments.
    pub args: Vec<Expr>,
}

impl CallStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("CALL ");
        push_name_path(out, &self.name);
        out.push('(');
        for (index, arg) in self.args.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            arg.restore_into(out);
        }
        out.push(')');
    }
}

/// How a [`BatchDmlStmt`] should be dry-run by TiDB.
///
/// This maps Go's `NoDryRun`, `DryRunQuery`, and `DryRunSplitDml` constants
/// without conflating their observably different restore spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchDmlDryRun {
    /// Execute the command normally.
    None,
    /// `DRY RUN QUERY`.
    Query,
    /// `DRY RUN`.
    SplitDml,
}

/// The inner DML family permitted by TiDB's non-transactional DML wrapper.
///
/// This intentionally does not use [`DmlStmt`]: Go's
/// `NonTransactionalDMLStmt.DMLStmt` is a `ShardableDMLStmt`, which excludes
/// another `BATCH` wrapper as well as every non-DML statement.
#[derive(Debug, Clone, PartialEq)]
pub enum BatchDml {
    /// An `INSERT` or `REPLACE` statement.
    Insert(Box<InsertStmt>),
    /// An `UPDATE` statement.
    Update(Box<UpdateStmt>),
    /// A `DELETE` statement.
    Delete(Box<DeleteStmt>),
}

impl BatchDml {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Insert(insert) => insert.restore_into(out),
            Self::Update(update) => update.restore_into(out),
            Self::Delete(delete) => delete.restore_into(out),
        }
    }
}

/// TiDB's `BATCH [ON column] LIMIT N [DRY RUN [QUERY]] <DML>` statement.
///
/// It is an AST/restore boundary only. The seed executor deliberately rejects
/// this before opening an implicit transaction because implementing it needs
/// TiDB's shard selection and repeated statement execution protocol.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchDmlStmt {
    /// Optional shard column chosen by `ON`; absent means TiDB chooses it.
    pub shard_column: Option<Vec<String>>,
    /// Required positive/zero unsigned batch limit as parsed by TiDB's
    /// `intLit` grammar.
    pub limit: u64,
    /// Requested dry-run mode.
    pub dry_run: BatchDmlDryRun,
    /// The shardable inner DML statement.
    pub dml: BatchDml,
}

impl BatchDmlStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("BATCH ");
        if let Some(column) = &self.shard_column {
            out.push_str("ON ");
            push_name_path(out, column);
            out.push(' ');
        }
        out.push_str("LIMIT ");
        out.push_str(&self.limit.to_string());
        out.push(' ');
        match self.dry_run {
            BatchDmlDryRun::None => {}
            BatchDmlDryRun::Query => out.push_str("DRY RUN QUERY "),
            BatchDmlDryRun::SplitDml => out.push_str("DRY RUN "),
        }
        self.dml.restore_into(out);
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for DmlStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::With { with, statement } => {
                if !crate::Visitable::accept(with, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(statement.as_mut(), visitor) {
                    return false;
                }
                let _ = with;
                let _ = statement;
            }
            Self::Insert(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Update(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Delete(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ImportInto(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::LoadData(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Batch(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DistributeTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Call(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CallStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        for arg in &mut self.args {
            if !crate::Visitable::accept(arg, visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BatchDmlDryRun {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Query => {}
            Self::SplitDml => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BatchDml {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Insert(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Update(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Delete(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BatchDmlStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            shard_column,
            limit,
            dry_run,
            dml,
        } = self;
        if !crate::Visitable::accept(dry_run, visitor) {
            return false;
        }
        if !crate::Visitable::accept(dml, visitor) {
            return false;
        }
        let _ = shard_column;
        let _ = limit;
        let _ = dry_run;
        let _ = dml;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
