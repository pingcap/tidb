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

//! Shared Go `pkg/ddl` CHECK-constraint metadata construction.
//!
//! Both runnable local DDL and cluster DDL build the same `ConstraintInfo`.
//! Keeping validation, naming, expression restoration, dependency discovery,
//! and ID allocation here prevents the two catalog paths from accepting
//! different SQL or persisting different metadata.

use std::collections::HashSet;

use tidb_ast::{CheckConstraintDefinition, CiString, ColumnOption, Expr, RestoreFlags};
use tidb_datatype::FieldTypeFlags;
use tidb_model::table::ConstraintInfo;
use tidb_model::{ColumnInfo, SchemaState};

use crate::StmtContext;

const MAX_CONSTRAINT_IDENTIFIER_LEN: usize = 64;

/// One CHECK declaration plus whether it was written inline on a column.
#[derive(Clone, Debug)]
pub struct CheckConstraintInput {
    /// The parsed CHECK payload.
    pub definition: CheckConstraintDefinition,
    /// The declaring column for an inline CHECK; `None` for a table CHECK.
    pub in_column: Option<String>,
}

/// The part of an existing foreign key relevant to CHECK validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckConstraintForeignKey {
    /// Referencing column names.
    pub columns: Vec<String>,
    /// Whether ON DELETE or ON UPDATE names any referential action.
    pub has_referential_action: bool,
}

/// A Go-coded CHECK DDL error shared by both catalog paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckConstraintError {
    /// MySQL/TiDB error number.
    pub code: u16,
    /// Client-visible message.
    pub message: String,
}

impl CheckConstraintError {
    fn new(code: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Whether one persisted CHECK constraint depends on `column_name`.
#[must_use]
pub fn uses_column(info: &ConstraintInfo, column_name: &str) -> bool {
    info.constraint_cols
        .iter()
        .any(|column| column.original().eq_ignore_ascii_case(column_name))
}

/// Go `ErrCantDropColWithCheckConstraint` (3959).
#[must_use]
pub fn column_dependency_error(constraint_name: &str, column_name: &str) -> CheckConstraintError {
    CheckConstraintError::new(
        tidb_error::tidb::errcode::ErrDependentByCheckConstraint,
        format!(
            "Check constraint '{}' uses column '{}', hence column cannot be dropped or renamed.",
            constraint_name, column_name
        ),
    )
}

/// Collects CHECK declarations in Go `buildColumnsAndConstraints` order:
/// table constraints first, then inline column constraints.
#[must_use]
pub fn create_inputs(create: &tidb_ast::CreateTableStmt) -> Vec<CheckConstraintInput> {
    let mut checks = create
        .table_constraints
        .iter()
        .filter_map(|constraint| {
            let tidb_ast::TableConstraint::Check(definition) = constraint else {
                return None;
            };
            Some(CheckConstraintInput {
                definition: definition.clone(),
                in_column: None,
            })
        })
        .collect::<Vec<_>>();
    for column in &create.columns {
        checks.extend(column.options.iter().filter_map(|option| {
            let ColumnOption::Check(definition) = option else {
                return None;
            };
            Some(CheckConstraintInput {
                definition: definition.clone(),
                in_column: Some(column.name.clone()),
            })
        }));
    }
    checks
}

/// Builds Go `ConstraintInfo` values and advances `max_constraint_id` once
/// for each result.
#[allow(clippy::too_many_arguments)]
pub fn build_constraint_infos(
    table_name: &CiString,
    columns: &[ColumnInfo],
    non_fk_constraint_names: impl IntoIterator<Item = String>,
    foreign_keys: &[CheckConstraintForeignKey],
    checks: &[CheckConstraintInput],
    max_constraint_id: &mut i64,
    state: SchemaState,
    context: &StmtContext,
) -> Result<Vec<ConstraintInfo>, CheckConstraintError> {
    if checks.is_empty() {
        return Ok(Vec::new());
    }

    let mut used_names = non_fk_constraint_names
        .into_iter()
        .map(|name| name.to_lowercase())
        .collect::<HashSet<_>>();

    // Go validates every explicit name before assigning generated names. An
    // unnamed first CHECK therefore skips a later explicit `t_chk_1`.
    for check in checks {
        let Some(name) = check
            .definition
            .name
            .as_deref()
            .filter(|name| !name.is_empty())
        else {
            continue;
        };
        if !used_names.insert(name.to_lowercase()) {
            return Err(CheckConstraintError::new(
                tidb_error::tidb::errcode::ErrCheckConstraintDupName,
                format!("Duplicate check constraint name '{name}'."),
            ));
        }
    }

    let column_names = columns
        .iter()
        .map(|column| column.name.original().to_owned())
        .collect::<Vec<_>>();
    let column_types = columns
        .iter()
        .map(|column| column.field_type.clone())
        .collect::<Vec<_>>();
    let mut generated = 1usize;
    let mut infos = Vec::with_capacity(checks.len());

    for check in checks {
        let name = match check
            .definition
            .name
            .as_deref()
            .filter(|name| !name.is_empty())
        {
            Some(name) => name.to_owned(),
            None => loop {
                let candidate = format!("{}_chk_{generated}", table_name.lowercase());
                generated += 1;
                if used_names.insert(candidate.clone()) {
                    break candidate;
                }
            },
        };
        if name.len() > MAX_CONSTRAINT_IDENTIFIER_LEN {
            return Err(CheckConstraintError::new(
                tidb_error::tidb::errcode::ErrTooLongIdent,
                format!("Identifier name '{name}' is too long"),
            ));
        }
        validate_expression_ast(&name, &check.definition.expression)?;

        let resolver = crate::generated_column::TableColumnResolver::with_like_default_escape(
            &column_names,
            &column_types,
            context.session_zone(),
            context.like_default_escape(),
        );
        let built = match tidb_expr::rewriter::rewrite_expr_resolved(
            &check.definition.expression,
            &resolver,
        ) {
            Ok(built) => built,
            Err(error) => {
                if let Some(missing) = resolver.missing_name() {
                    return Err(unknown_column(&name, &missing));
                }
                return Err(CheckConstraintError::new(1105, format!("{error:?}")));
            }
        };
        if let Some(missing) = resolver.missing_name() {
            return Err(unknown_column(&name, &missing));
        }
        let dependencies = resolver.dependency_names();
        if let Some(column_name) = &check.in_column {
            if dependencies.len() > 1
                || dependencies
                    .first()
                    .is_some_and(|dependency| !dependency.eq_ignore_ascii_case(column_name))
            {
                return Err(CheckConstraintError::new(
                    tidb_error::tidb::errcode::ErrColumnCheckConstraintReferencesOtherColumn,
                    format!("Column check constraint '{name}' references other column."),
                ));
            }
        }
        if dependencies.iter().any(|dependency| {
            columns.iter().any(|column| {
                column.name.original().eq_ignore_ascii_case(dependency)
                    && column.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT)
            })
        }) {
            return Err(CheckConstraintError::new(
                tidb_error::tidb::errcode::ErrCheckConstraintRefersAutoIncrementColumn,
                format!("Check constraint '{name}' cannot refer to an auto-increment column."),
            ));
        }
        for foreign_key in foreign_keys {
            if foreign_key.has_referential_action {
                if let Some(dependency) = dependencies.iter().find(|dependency| {
                    foreign_key
                        .columns
                        .iter()
                        .any(|column| column.eq_ignore_ascii_case(dependency))
                }) {
                    return Err(CheckConstraintError::new(
                        tidb_error::tidb::errcode::ErrCheckConstraintClauseUsingFKReferActionColumn,
                        format!(
                            "Column '{dependency}' cannot be used in a check constraint '{name}': needed in a foreign key constraint referential action."
                        ),
                    ));
                }
            }
        }
        if !built
            .static_type()
            .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::IS_BOOLEAN))
        {
            return Err(CheckConstraintError::new(
                tidb_error::tidb::errcode::ErrNonBooleanExprForCheckConstraint,
                format!(
                    "An expression of non-boolean type specified to a check constraint '{name}'."
                ),
            ));
        }

        let flags = RestoreFlags::STRING_SINGLE_QUOTES
            | RestoreFlags::KEYWORD_LOWERCASE
            | RestoreFlags::NAME_BACK_QUOTES
            | RestoreFlags::SPACES_AROUND_BINARY_OPERATION
            | RestoreFlags::WITHOUT_SCHEMA_NAME
            | RestoreFlags::WITHOUT_TABLE_NAME;
        *max_constraint_id += 1;
        infos.push(ConstraintInfo {
            id: *max_constraint_id,
            name: CiString::new(name),
            table: table_name.clone(),
            constraint_cols: dependencies
                .into_iter()
                .map(CiString::new)
                .collect::<Vec<_>>()
                .into(),
            enforced: check.definition.enforced,
            in_column: check.in_column.is_some(),
            expr_string: check.definition.expression.restore_with_flags(flags),
            state,
        });
    }
    Ok(infos)
}

fn unknown_column(name: &str, missing: &str) -> CheckConstraintError {
    CheckConstraintError::new(
        tidb_error::tidb::errcode::ErrTableCheckConstraintReferUnknown,
        format!("Check constraint '{name}' refers to non-existing column '{missing}'."),
    )
}

fn validate_expression_ast(name: &str, expression: &Expr) -> Result<(), CheckConstraintError> {
    struct Checker<'a> {
        name: &'a str,
        error: Option<CheckConstraintError>,
    }

    impl tidb_ast::Visitor for Checker<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if self.error.is_some() {
                return true;
            }
            let Some(expression) = node.downcast_ref::<Expr>() else {
                return false;
            };
            let error = match expression {
                Expr::UserVar(_) | Expr::SysVar { .. } | Expr::Assign { .. } => {
                    Some(CheckConstraintError::new(
                        tidb_error::tidb::errcode::ErrCheckConstraintVariables,
                        format!(
                            "An expression of a check constraint '{}' cannot refer to a user or system variable.",
                            self.name
                        ),
                    ))
                }
                Expr::Subquery(_)
                | Expr::Exists { .. }
                | Expr::InSubquery { .. }
                | Expr::CompareSubquery { .. } => Some(CheckConstraintError::new(
                    tidb_error::tidb::errcode::ErrCheckConstraintFunctionIsNotAllowed,
                    format!(
                        "An expression of a check constraint '{}' contains disallowed function.",
                        self.name
                    ),
                )),
                Expr::Default(_) => Some(disallowed_named_function(self.name, "default")),
                Expr::Func { name, .. } if is_disallowed_function(name) => {
                    Some(disallowed_named_function(self.name, &name.to_lowercase()))
                }
                _ => None,
            };
            if let Some(error) = error {
                self.error = Some(error);
                true
            } else {
                false
            }
        }

        fn leave(&mut self, _: &mut dyn std::any::Any) -> bool {
            self.error.is_none()
        }
    }

    let mut expression = expression.clone();
    let mut checker = Checker { name, error: None };
    let _ = tidb_ast::Visitable::accept(&mut expression, &mut checker);
    checker.error.map_or(Ok(()), Err)
}

fn disallowed_named_function(name: &str, function: &str) -> CheckConstraintError {
    CheckConstraintError::new(
        tidb_error::tidb::errcode::ErrCheckConstraintNamedFunctionIsNotAllowed,
        format!(
            "An expression of a check constraint '{name}' contains disallowed function: {function}."
        ),
    )
}

fn is_disallowed_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "now"
            | "current_timestamp"
            | "curdate"
            | "current_date"
            | "curtime"
            | "current_time"
            | "localtime"
            | "localtimestamp"
            | "unix_timestamp"
            | "utc_date"
            | "utc_timestamp"
            | "utc_time"
            | "connection_id"
            | "current_user"
            | "session_user"
            | "version"
            | "found_rows"
            | "last_insert_id"
            | "system_user"
            | "user"
            | "rand"
            | "row_count"
            | "get_lock"
            | "is_free_lock"
            | "is_used_lock"
            | "release_lock"
            | "release_all_locks"
            | "load_file"
            | "uuid"
            | "uuid_v4"
            | "uuid_v7"
            | "uuid_short"
            | "sleep"
    )
}
