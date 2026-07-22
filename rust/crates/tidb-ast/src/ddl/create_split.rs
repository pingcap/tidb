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

//! Creation-side `CREATE TABLE ... SPLIT` payload and restore.
//!
//! Go's `ast.CreateTableStmt.SplitIndex` uses `ast.SplitIndexOption`, which is
//! also used by ALTER TABLE. The statement envelopes are not interchangeable:
//! CREATE already owns the table name and can carry multiple split options.
//! This leaf preserves that creation-side contract without leaking ALTER's
//! target vocabulary into it.

use crate::util::back_quote;

use super::SplitOption;

/// The keyspace selected by a creation-side split option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTableSplitTarget {
    /// The new table's record keyspace. Go's canonical restore emits no
    /// additional target words for this implicit/default form.
    Table,
    /// The new table's primary-index keyspace.
    PrimaryKey,
    /// A named secondary-index keyspace.
    Index(String),
}

/// One `SPLIT [REGION] [TABLE|PRIMARY KEY|INDEX name] <split-option>` on a
/// `CREATE TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableSplit {
    /// The table, primary-key, or secondary-index keyspace to split.
    pub target: CreateTableSplitTarget,
    /// The shared point/range boundary payload. Only this value grammar is
    /// common with standalone and ALTER split statements.
    pub option: SplitOption,
}

impl CreateTableSplit {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SPLIT ");
        match &self.target {
            CreateTableSplitTarget::Table => {}
            CreateTableSplitTarget::PrimaryKey => out.push_str("PRIMARY KEY "),
            CreateTableSplitTarget::Index(name) => {
                out.push_str("INDEX ");
                out.push_str(&back_quote(name));
                out.push(' ');
            }
        }
        self.option.restore_into(out);
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for CreateTableSplitTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table => {}
            Self::PrimaryKey => {}
            Self::Index(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateTableSplit {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { target, option } = self;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        if !crate::Visitable::accept(option, visitor) {
            return false;
        }
        let _ = target;
        let _ = option;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS

#[cfg(test)]
mod tests {
    use crate::{
        CreateTableSplit, CreateTableSplitTarget, CreateTableStmt, CreateTableTemporary, Expr,
        SplitOption,
    };

    #[test]
    fn create_table_split_restores_before_on_commit() {
        let mut out = String::new();
        CreateTableStmt {
            temporary: CreateTableTemporary::Global,
            on_commit_delete: true,
            if_not_exists: false,
            name: vec!["t".to_owned()],
            like_table: None,
            columns: Vec::new(),
            table_constraints: Vec::new(),
            table_options: Vec::new(),
            partitioning: None,
            splits: vec![
                CreateTableSplit {
                    target: CreateTableSplitTarget::Table,
                    option: SplitOption::By(vec![vec![Expr::Int("1".to_owned())]]),
                },
                CreateTableSplit {
                    target: CreateTableSplitTarget::Index("idx".to_owned()),
                    option: SplitOption::Between {
                        lower: vec![Expr::Int("2".to_owned())],
                        upper: vec![Expr::Int("3".to_owned())],
                        regions: 4,
                    },
                },
            ],
            ctas: None,
        }
        .restore_into(&mut out);
        assert_eq!(
            out,
            "CREATE GLOBAL TEMPORARY TABLE `t` SPLIT BY (1) SPLIT INDEX `idx` BETWEEN (2) AND (3) REGIONS 4 ON COMMIT DELETE ROWS"
        );
    }
}
