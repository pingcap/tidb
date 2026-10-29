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

//! Go `checkConstraintNames` (`pkg/ddl/create_table.go`): the one place a
//! CREATE TABLE settles its constraint names, shared by every table builder.
//!
//! Go runs it right after `buildColumnsAndConstraints`, over the statement's
//! constraints in that function's order: every table-level constraint as
//! written, then each column's inline `PRIMARY KEY`, `UNIQUE` and (with
//! `tidb_enable_check_constraint` on) `CHECK`, column by column. Two passes:
//!
//! 1. every explicit name is checked for a case-insensitive duplicate --
//!    foreign keys in a pool of their own (1826), every other constraint in a
//!    shared pool (1061, or 3822 when the duplicate is a CHECK);
//! 2. `setEmptyConstraintName` names each anonymous key constraint after its
//!    first column (`expression_index` / `vector_index` for an expression
//!    part), suffixing `_2`, `_3`, ... until the name is free. An anonymous
//!    PRIMARY KEY is named too: the index is later called `PRIMARY`, but the
//!    column's name is spent, so `primary key (k(4)), key (k)` names the key
//!    `k_2`.
//!
//! Pass 2 looks names up exactly as spelled while pass 1 stores them
//! lowercased, as Go's maps do; an anonymous CHECK is named afterwards by
//! [`crate::check_constraint::build_constraint_infos`] from the names
//! settled here.

use tidb_ast::{
    ColumnOption, CreateTableStmt, IndexConstraintDefinition, IndexPart, IndexType, InlineKeyKind,
    TableConstraint,
};
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

/// The names `checkConstraintNames` settles for a CREATE TABLE's key
/// constraints.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateTableConstraintNames {
    /// The name of `table_constraints[i]` when it is an index constraint (a
    /// PRIMARY KEY included); `None` for a CHECK or FOREIGN KEY.
    pub table: Vec<Option<String>>,
    /// For `columns[c].options[o]`, the name of that inline `PRIMARY KEY` or
    /// `UNIQUE`; `None` for any other option and for a repeated inline
    /// `PRIMARY KEY`, which Go does not turn into a second constraint.
    pub inline: Vec<Vec<Option<String>>>,
}

impl CreateTableConstraintNames {
    /// The settled name of the table-level constraint at `index`.
    #[must_use]
    pub fn table_constraint(&self, index: usize) -> Option<&str> {
        self.table.get(index).and_then(Option::as_deref)
    }

    /// The settled name of the inline key option `option` of column `column`.
    #[must_use]
    pub fn inline_key(&self, column: usize, option: usize) -> Option<&str> {
        self.inline
            .get(column)
            .and_then(|options| options.get(option))
            .and_then(Option::as_deref)
    }
}

/// A duplicate explicit constraint name, with the code Go raises for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintNameError {
    /// `dbterror.ErrDupKeyName` (1061), `ErrFkDupName` (1826) or
    /// `ErrCheckConstraintDupName` (3822).
    pub proto: &'static tidb_error::terror::TerrorError,
    /// The duplicate name as it is spelled.
    pub name: String,
    /// Go's message, naming the duplicate as it is spelled.
    pub message: String,
}

/// Go `ast.ConstraintType`, as far as naming tells the kinds apart.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Key,
    Check,
    ForeignKey,
}

/// Where a constraint sits in the statement, so its settled name can be
/// handed back to the builder that owns it.
#[derive(Clone, Copy)]
enum Slot {
    Table(usize),
    Inline { column: usize, option: usize },
}

struct Constraint<'a> {
    kind: Kind,
    name: String,
    /// The name an anonymous key is called after; `None` when the constraint
    /// has no key parts (Go `len(constr.Keys) == 0`).
    prefix: Option<&'a str>,
    slot: Slot,
}

/// Go `checkConstraintNames` over a CREATE TABLE.
///
/// `enable_check_constraint` is `tidb_enable_check_constraint`: with it off Go
/// warns about a column-level CHECK and never builds the constraint, so its
/// name takes no part; a table-level CHECK always does.
pub fn check_constraint_names(
    create: &CreateTableStmt,
    enable_check_constraint: bool,
) -> Result<CreateTableConstraintNames, ConstraintNameError> {
    let mut constraints = Vec::new();
    for (index, constraint) in create.table_constraints.iter().enumerate() {
        let slot = Slot::Table(index);
        constraints.push(match constraint {
            TableConstraint::Index(index) => Constraint {
                kind: Kind::Key,
                name: index.name.clone().unwrap_or_default(),
                prefix: anonymous_index_prefix(index),
                slot,
            },
            TableConstraint::Check(check) => Constraint {
                kind: Kind::Check,
                name: check.name.clone().unwrap_or_default(),
                prefix: None,
                slot,
            },
            TableConstraint::ForeignKey(foreign_key) => Constraint {
                kind: Kind::ForeignKey,
                name: foreign_key.name.clone().unwrap_or_default(),
                prefix: None,
                slot,
            },
        });
    }
    for (column, definition) in create.columns.iter().enumerate() {
        // Go `columnDefToCol`: "Check PriKeyFlag first to avoid extra
        // duplicate constraints." Its UNIQUE arm tests `UniqueFlag` but sets
        // `UniqueKeyFlag`, so a repeated inline UNIQUE is a second constraint.
        let mut primary = false;
        for (option, value) in definition.options.iter().enumerate() {
            let slot = Slot::Inline { column, option };
            match value {
                ColumnOption::InlineKey(key) => {
                    if matches!(key.kind, InlineKeyKind::Primary { .. }) {
                        if primary {
                            continue;
                        }
                        primary = true;
                    }
                    constraints.push(Constraint {
                        kind: Kind::Key,
                        name: String::new(),
                        prefix: Some(definition.name.as_str()),
                        slot,
                    });
                }
                ColumnOption::Check(check) if enable_check_constraint => {
                    constraints.push(Constraint {
                        kind: Kind::Check,
                        name: check.name.clone().unwrap_or_default(),
                        prefix: None,
                        slot,
                    });
                }
                _ => {}
            }
        }
    }

    // Go `checkDuplicateConstraint`, which keys its maps by the lowercased
    // name.
    let mut names = std::collections::HashSet::new();
    let mut foreign_key_names = std::collections::HashSet::new();
    for constraint in &constraints {
        if constraint.name.is_empty() {
            continue;
        }
        let lower = constraint.name.go_to_lower();
        let duplicate = match constraint.kind {
            Kind::ForeignKey => !foreign_key_names.insert(lower),
            Kind::Key | Kind::Check => !names.insert(lower),
        };
        if duplicate {
            let name = &constraint.name;
            return Err(match constraint.kind {
                Kind::ForeignKey => ConstraintNameError {
                    proto: &tidb_dbterror::ERR_FK_DUP_NAME,
                    name: name.clone(),
                    message: format!("Duplicate foreign key constraint name '{name}'"),
                },
                Kind::Check => ConstraintNameError {
                    proto: &tidb_dbterror::ERR_CHECK_CONSTRAINT_DUP_NAME,
                    name: name.clone(),
                    message: format!("Duplicate check constraint name '{name}'."),
                },
                Kind::Key => ConstraintNameError {
                    proto: &tidb_dbterror::ERR_DUP_KEY_NAME,
                    name: name.clone(),
                    message: format!("Duplicate key name '{name}'"),
                },
            });
        }
    }

    // Go `setEmptyConstraintName`, which reads and writes the same map with
    // the name as spelled.
    for constraint in &mut constraints {
        if constraint.kind == Kind::ForeignKey || !constraint.name.is_empty() {
            continue;
        }
        let Some(prefix) = constraint.prefix else {
            continue;
        };
        let mut name = prefix.to_owned();
        let mut suffix = 2;
        if name.go_equal_fold("PRIMARY") {
            name = format!("{prefix}_2");
            suffix = 3;
        }
        while names.contains(&name) {
            name = format!("{prefix}_{suffix}");
            suffix += 1;
        }
        names.insert(name.clone());
        constraint.name = name;
    }

    let mut settled = CreateTableConstraintNames {
        table: vec![None; create.table_constraints.len()],
        inline: create
            .columns
            .iter()
            .map(|definition| vec![None; definition.options.len()])
            .collect(),
    };
    for constraint in constraints {
        if constraint.kind != Kind::Key {
            continue;
        }
        match constraint.slot {
            Slot::Table(index) => settled.table[index] = Some(constraint.name),
            Slot::Inline { column, option } => {
                settled.inline[column][option] = Some(constraint.name);
            }
        }
    }
    Ok(settled)
}

/// The name an anonymous index constraint is called after: Go
/// `setEmptyConstraintName` uses `getAnonymousIndexPrefix` when any key part
/// is an expression, and the first column otherwise.
fn anonymous_index_prefix(index: &IndexConstraintDefinition) -> Option<&str> {
    let first = index.parts.first()?;
    if index
        .parts
        .iter()
        .any(|part| matches!(part, IndexPart::Expr { .. }))
    {
        // Go reads `IndexTypeVector`; the preprocessor has already rewritten
        // an `HNSW` spelling to it.
        let vector = matches!(
            index.options.index_type,
            Some(IndexType::VECTOR | IndexType::HNSW)
        );
        return Some(if vector {
            "vector_index"
        } else {
            "expression_index"
        });
    }
    match first {
        IndexPart::Column { name, .. } => Some(name.as_str()),
        IndexPart::Expr { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(sql: &str) -> Result<CreateTableConstraintNames, ConstraintNameError> {
        let stmt = tidb_parser::parse(sql).expect("parses");
        let tidb_ast::Stmt::Ddl(ddl) = &stmt else {
            panic!("not DDL: {sql}");
        };
        let tidb_ast::DdlStmt::CreateTable(create) = &**ddl else {
            panic!("not CREATE TABLE: {sql}");
        };
        check_constraint_names(create, true)
    }

    fn table_names(sql: &str) -> Vec<Option<String>> {
        names(sql).unwrap().table
    }

    /// Go names an anonymous PRIMARY KEY too, so the key after it on the same
    /// column is `k_2` (`expression/charset_and_collation`,
    /// `TestCollationPrefixClusteredIndex`).
    #[test]
    fn an_anonymous_primary_key_spends_its_columns_name() {
        assert_eq!(
            table_names(
                "create table t (k char(20), v int, primary key (k(4)) clustered, key (k))"
            ),
            [Some("k".to_owned()), Some("k_2".to_owned())]
        );
    }

    /// Explicit names are reserved before any anonymous one is chosen.
    #[test]
    fn explicit_names_are_reserved_first() {
        assert_eq!(
            table_names("create table n4 (a int, b int, key a_2(b), unique(a), key(a))"),
            [
                Some("a_2".to_owned()),
                Some("a".to_owned()),
                Some("a_3".to_owned())
            ]
        );
        // `KEY b (...), PRIMARY KEY (b(...))`: the PRIMARY KEY takes `b_2`, so
        // both indexes are built.
        assert_eq!(
            table_names("create table d9 (b varchar(10), key b (b), primary key (b(3)))"),
            [Some("b".to_owned()), Some("b_2".to_owned())]
        );
    }

    /// An explicit CHECK name is in the same pool as the key names.
    #[test]
    fn a_check_name_is_reserved_against_keys() {
        assert_eq!(
            table_names("create table t (a int, constraint a check (a > 0), key (a))"),
            [None, Some("a_2".to_owned())]
        );
        assert_eq!(
            names("create table t (a int, key c (a), constraint c check (a > 0))").unwrap_err(),
            ConstraintNameError {
                proto: &tidb_dbterror::ERR_CHECK_CONSTRAINT_DUP_NAME,
                name: "c".to_owned(),
                message: "Duplicate check constraint name 'c'.".to_owned()
            }
        );
        assert_eq!(
            names("create table t (a int, constraint c check (a > 0), key c (a))").unwrap_err(),
            ConstraintNameError {
                proto: &tidb_dbterror::ERR_DUP_KEY_NAME,
                name: "c".to_owned(),
                message: "Duplicate key name 'c'".to_owned()
            }
        );
    }

    /// Inline keys follow every table-level constraint; a repeated inline
    /// UNIQUE is a second constraint, a repeated inline PRIMARY KEY is not.
    #[test]
    fn inline_keys_are_named_after_table_constraints() {
        let settled = names("create table p1 (a int primary key unique unique, key (a))").unwrap();
        assert_eq!(settled.table, [Some("a".to_owned())]);
        assert_eq!(
            settled.inline[0],
            [
                Some("a_2".to_owned()),
                Some("a_3".to_owned()),
                Some("a_4".to_owned())
            ]
        );
        let settled = names("create table p2 (a int primary key primary key)").unwrap();
        assert_eq!(settled.inline[0], [Some("a".to_owned()), None]);
    }

    /// Pass 1 stores names lowercased and pass 2 looks them up as spelled, so
    /// Go names an anonymous key on `A` `A` beside an explicit `a` (oracle:
    /// `KEY \`a\` (\`A\`), KEY \`A\` (\`A\`)`).
    #[test]
    fn anonymous_names_are_looked_up_as_spelled() {
        assert_eq!(
            table_names("create table t7 (A int, key a (A), key (A))"),
            [Some("a".to_owned()), Some("A".to_owned())]
        );
    }

    /// Expression parts name the index `expression_index`, or `vector_index`
    /// for a vector index; a column called `primary` starts at `primary_2`.
    #[test]
    fn expression_and_primary_prefixes() {
        assert_eq!(
            table_names("create table e (a int, key ((a + 1)), key ((a + 2)))"),
            [
                Some("expression_index".to_owned()),
                Some("expression_index_2".to_owned())
            ]
        );
        assert_eq!(
            table_names("create table e (`primary` int, key (`primary`))"),
            [Some("primary_2".to_owned())]
        );
    }

    /// Foreign keys keep a pool of their own and are never named here.
    #[test]
    fn foreign_keys_have_their_own_pool() {
        assert_eq!(
            table_names(
                "create table f (a int, key fk (a), constraint fk foreign key (a) references p (a))"
            ),
            [Some("fk".to_owned()), None]
        );
        assert_eq!(
            names(
                "create table f (a int, constraint fk foreign key (a) references p (a), \
                 constraint fk foreign key (a) references q (a))"
            )
            .unwrap_err()
            .proto
            .code()
            .value(),
            1826
        );
    }
}
