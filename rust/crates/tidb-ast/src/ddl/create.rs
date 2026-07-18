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

//! `CREATE TABLE` statement envelope and canonical restore.
//!
//! This leaf owns the statement-level data flow: temporary-table mode, table
//! name, LIKE, the ordered body, trailing options, partitioning, creation-side
//! SPLIT, CTAS, and the global-temporary `ON COMMIT` tail. The element grammar
//! and shared payloads deliberately remain in their owning leaves.

use crate::util::{back_quote, push_name_path};
use crate::{QueryStmt, RestoreContext};

use super::{ColumnDef, CreateTableSplit, TableConstraint, TableOption, TablePartitioning};

/// The temporary-table modifier of a `CREATE TABLE` declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTableTemporary {
    /// An ordinary persistent table.
    None,
    /// `CREATE TEMPORARY TABLE`, whose session-local catalog is not modelled.
    Local,
    /// `CREATE GLOBAL TEMPORARY TABLE`, which always carries an `ON COMMIT`
    /// policy in the Go grammar.
    Global,
}

/// Go's `OnDuplicateKeyHandlingType` as attached to a `CREATE TABLE ...
/// [IGNORE|REPLACE] AS <result-set>` statement.
///
/// The policy belongs to CTAS rather than to the base table declaration: Go
/// only restores it when `CreateTableStmt.Select` is non-nil. Keeping it in
/// the same payload as the result source prevents a dangling `IGNORE` from
/// being restored for the source-accepted `CREATE TABLE t IGNORE` form.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CreateTableOnDuplicate {
    /// The source's ordinary error-on-duplicate policy; restores as `AS`.
    #[default]
    Error,
    /// `IGNORE AS`.
    Ignore,
    /// `REPLACE AS`.
    Replace,
}

/// The result-set payload of `CREATE TABLE ... AS <query>`.
///
/// This intentionally owns one typed [`QueryStmt`] instead of lowering CTAS
/// into a synthetic `CREATE TABLE` followed by `INSERT`. TiDB's AST keeps a
/// result-set node directly, which makes `SELECT`, `TABLE`, `VALUES`, and
/// `WITH` sources observable through one restore path. The outer braces are
/// also syntactic data: they survive Go's canonical restore even when the
/// contained query is a plain `SELECT`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableAsQuery {
    /// Go's duplicate-key policy for rows materialized by this CTAS.
    pub on_duplicate: CreateTableOnDuplicate,
    /// The CTAS result source.
    pub query: Box<QueryStmt>,
    /// Whether the source result set was wrapped in outer `(...)`.
    pub parenthesized: bool,
}

/// A `CREATE TABLE` statement: the table name and its column definitions.
/// Table-level `PRIMARY KEY (...)`, `UNIQUE [KEY] (...)`, basic non-unique
/// `KEY|INDEX (...)`, `CHECK (...)`, and `FOREIGN KEY (...)` constraints are
/// captured. `CREATE [GLOBAL] TEMPORARY TABLE target LIKE source` has no
/// column list; its source table is represented separately so the executor
/// cannot mistake it for an empty ordinary table.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    /// The declaration's temporary-table kind.
    pub temporary: CreateTableTemporary,
    /// The global temporary table's required `ON COMMIT` policy. `true` is
    /// `DELETE ROWS`; `false` is `PRESERVE ROWS`. It is meaningful only for
    /// [`CreateTableTemporary::Global`], mirroring Go's `OnCommitDelete`
    /// field.
    pub on_commit_delete: bool,
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// The table name path.
    pub name: Vec<String>,
    /// The source table for `CREATE [TEMPORARY] TABLE target LIKE source`.
    /// A present value means this statement has no column list, constraints,
    /// or table options.
    pub like_table: Option<Vec<String>>,
    /// The declared columns, in order.
    pub columns: Vec<ColumnDef>,
    /// The declared table-level constraints (`PRIMARY KEY`/`UNIQUE`/basic
    /// `INDEX`/`CHECK`/`FOREIGN KEY`), in the order they were written — like
    /// [`TableOption`] below, real TiDB restores these in WRITTEN order
    /// (confirmed via `godump restore`, not assumed: e.g. `UNIQUE(b), PRIMARY
    /// KEY(a)` restores in that exact order, not `PRIMARY KEY` first). A
    /// single-column primary/unique key is more commonly declared inline via
    /// a column's own `ColumnOption` instead.
    pub table_constraints: Vec<TableConstraint>,
    /// Trailing table options (`ENGINE=...`, `COMMENT='...'`, ...), in the
    /// order they were written — unlike most other lists in this AST, real
    /// TiDB restores these in WRITTEN order rather than a fixed canonical
    /// order (confirmed via `godump restore` on several reorderings, not
    /// assumed). Options this parser does not recognize (`ROW_FORMAT=...`,
    /// `KEY_BLOCK_SIZE=...`, ...) are parsed but discarded, not retained
    /// here.
    pub table_options: Vec<TableOption>,
    /// Creation-side `PARTITION BY` payload. It is deliberately not an ALTER
    /// action: it owns the method, submethod, definitions and index-locality
    /// attributes that exist only while constructing a table.
    pub partitioning: Option<TablePartitioning>,
    /// Creation-side `SPLIT [REGION] {TABLE|PRIMARY KEY|INDEX name}` options,
    /// in source order. This is intentionally distinct from ALTER TABLE's
    /// `SplitTarget`: CREATE owns its table name in this statement and Go
    /// represents the payload as `CreateTableStmt.SplitIndex`.
    pub splits: Vec<CreateTableSplit>,
    /// Optional `CREATE TABLE ... [IGNORE|REPLACE] AS <result-set>` payload.
    /// A bare `CREATE TABLE name` remains source-valid with this absent; the
    /// executor owns the separate capability rejection for that no-column
    /// declaration.
    pub ctas: Option<CreateTableAsQuery>,
}

impl CreateTableStmt {
    /// Appends the ordinary canonical SQL used by default restoration.
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, RestoreContext::default());
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        out.push_str(match self.temporary {
            CreateTableTemporary::None => "CREATE TABLE ",
            CreateTableTemporary::Local => "CREATE TEMPORARY TABLE ",
            CreateTableTemporary::Global => "CREATE GLOBAL TEMPORARY TABLE ",
        });
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        push_name_path(out, &self.name);
        if let Some(source) = &self.like_table {
            out.push_str(" LIKE ");
            push_name_path(out, source);
            if self.temporary == CreateTableTemporary::Global {
                out.push_str(if self.on_commit_delete {
                    " ON COMMIT DELETE ROWS"
                } else {
                    " ON COMMIT PRESERVE ROWS"
                });
            }
            return;
        }
        if !self.columns.is_empty() || !self.table_constraints.is_empty() {
            out.push_str(" (");
            let mut first = true;
            for c in &self.columns {
                if !first {
                    out.push(',');
                }
                first = false;
                c.restore_into_with_context(out, context);
            }
            // Table-level constraints restore in WRITTEN order (like table
            // options below) — confirmed via `godump restore` on several
            // reorderings, not assumed.
            for tc in &self.table_constraints {
                if !first {
                    out.push(',');
                }
                first = false;
                match tc {
                    TableConstraint::Index(constraint) => {
                        constraint.restore_into_with_context(out, context)
                    }
                    TableConstraint::Check(check) => check.restore_into_with_context(out, context),
                    TableConstraint::ForeignKey(fk) => fk.restore_into_with_context(out, context),
                }
            }
            out.push(')');
        }
        // Table options restore in WRITTEN order (unlike most other lists
        // here, which restore in a fixed canonical order) — confirmed via
        // `godump restore` on several reorderings.
        for opt in &self.table_options {
            out.push(' ');
            opt.restore_into_with_context(out, context);
        }
        if let Some(partitioning) = &self.partitioning {
            partitioning.restore_into(out);
        }
        // Go restores creation-side SPLIT after partitioning, but before a
        // CTAS result source and GLOBAL TEMPORARY's ON COMMIT clause.
        for split in &self.splits {
            out.push(' ');
            split.restore_into(out);
        }
        if let Some(ctas) = &self.ctas {
            out.push_str(match ctas.on_duplicate {
                CreateTableOnDuplicate::Error => " AS ",
                CreateTableOnDuplicate::Ignore => " IGNORE AS ",
                CreateTableOnDuplicate::Replace => " REPLACE AS ",
            });
            if ctas.parenthesized {
                out.push('(');
            }
            ctas.query.restore_into(out);
            if ctas.parenthesized {
                out.push(')');
            }
        }
        if self.temporary == CreateTableTemporary::Global {
            out.push_str(if self.on_commit_delete {
                " ON COMMIT DELETE ROWS"
            } else {
                " ON COMMIT PRESERVE ROWS"
            });
        }
    }

    /// Restores a CREATE TABLE statement without requiring arbitrary ENUM/SET
    /// members to be valid UTF-8. Ordinary statements continue to use the
    /// existing String sink; this path is selected by the parser differential
    /// ring when it needs the Go AST's raw byte contract.
    pub(crate) fn restore_into_bytes(&self, out: &mut Vec<u8>, context: RestoreContext) {
        out.extend_from_slice(match self.temporary {
            CreateTableTemporary::None => b"CREATE TABLE ".as_slice(),
            CreateTableTemporary::Local => b"CREATE TEMPORARY TABLE ".as_slice(),
            CreateTableTemporary::Global => b"CREATE GLOBAL TEMPORARY TABLE ".as_slice(),
        });
        if self.if_not_exists {
            out.extend_from_slice(b"IF NOT EXISTS ");
        }
        push_name_path_bytes(out, &self.name);
        if let Some(source) = &self.like_table {
            out.extend_from_slice(b" LIKE ");
            push_name_path_bytes(out, source);
            if self.temporary == CreateTableTemporary::Global {
                out.extend_from_slice(if self.on_commit_delete {
                    b" ON COMMIT DELETE ROWS"
                } else {
                    b" ON COMMIT PRESERVE ROWS"
                });
            }
            return;
        }
        if !self.columns.is_empty() || !self.table_constraints.is_empty() {
            out.extend_from_slice(b" (");
            let mut first = true;
            for column in &self.columns {
                if !first {
                    out.push(b',');
                }
                first = false;
                column.restore_into_bytes(out, context);
            }
            for constraint in &self.table_constraints {
                if !first {
                    out.push(b',');
                }
                first = false;
                let mut text = String::new();
                match constraint {
                    TableConstraint::Index(value) => {
                        value.restore_into_with_context(&mut text, context)
                    }
                    TableConstraint::Check(value) => {
                        value.restore_into_with_context(&mut text, context)
                    }
                    TableConstraint::ForeignKey(value) => {
                        value.restore_into_with_context(&mut text, context)
                    }
                }
                out.extend_from_slice(text.as_bytes());
            }
            out.push(b')');
        }
        for option in &self.table_options {
            out.push(b' ');
            let mut text = String::new();
            option.restore_into_with_context(&mut text, context);
            out.extend_from_slice(text.as_bytes());
        }
        if let Some(partitioning) = &self.partitioning {
            let mut text = String::new();
            partitioning.restore_into(&mut text);
            out.extend_from_slice(text.as_bytes());
        }
        for split in &self.splits {
            out.push(b' ');
            let mut text = String::new();
            split.restore_into(&mut text);
            out.extend_from_slice(text.as_bytes());
        }
        if let Some(ctas) = &self.ctas {
            out.extend_from_slice(match ctas.on_duplicate {
                CreateTableOnDuplicate::Error => b" AS".as_slice(),
                CreateTableOnDuplicate::Ignore => b" IGNORE AS".as_slice(),
                CreateTableOnDuplicate::Replace => b" REPLACE AS".as_slice(),
            });
            if ctas.parenthesized {
                out.push(b'(');
            }
            let mut text = String::new();
            ctas.query.restore_into(&mut text);
            out.extend_from_slice(text.as_bytes());
            if ctas.parenthesized {
                out.push(b')');
            }
        }
        if self.temporary == CreateTableTemporary::Global {
            out.extend_from_slice(if self.on_commit_delete {
                b" ON COMMIT DELETE ROWS"
            } else {
                b" ON COMMIT PRESERVE ROWS"
            });
        }
    }
}

fn push_name_path_bytes(out: &mut Vec<u8>, path: &[String]) {
    for (index, part) in path.iter().enumerate() {
        if index > 0 {
            out.push(b'.');
        }
        out.extend_from_slice(back_quote(part).as_bytes());
    }
}
