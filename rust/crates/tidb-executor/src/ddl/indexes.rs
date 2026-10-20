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

//! Adding and removing a key on an existing table: `CREATE INDEX` and
//! `DROP INDEX`, plus the `ALTER TABLE ... ADD/DROP INDEX` actions that reach
//! the same code.
//!
//! Inside: [`run_create_index_in`] and [`run_drop_index_in`], the statement
//! entry points; [`add_index_to_table`], [`prepare_drop_index`] and
//! [`drop_prepared_index`], the actions the `ALTER TABLE` dispatcher shares
//! with them; and the two AST
//! readers `is_visible` (Go `!IndexInfo.Invisible`) and `index_part_names`,
//! which reject the index shapes this tier does not model rather than
//! silently creating a plain index.
//!
//! Mirrors Go `pkg/ddl/index.go` (`CreateIndex`, `dropIndex`): creating a
//! unique index backfills and re-checks the existing rows, so a collision
//! leaves the table WITHOUT the index, which is the behaviour the doc
//! comments record as captured from TiDB.

use super::{Catalog, DdlStmt, DriverError, KvColumn, KvIndex, Stmt};
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

/// Go `GetName4AnonymousIndex` over an in-process table's indexes; see
/// [`get_name_for_anonymous_index`].
pub(crate) fn anonymous_index_name(existing: &[KvIndex], first_column: &str) -> String {
    let names = existing
        .iter()
        .map(|index| index.name.as_str())
        .collect::<Vec<_>>();
    get_name_for_anonymous_index(&names, first_column, "")
}

/// Go `GetName4AnonymousIndex` (`pkg/ddl/executor.go`): the name an `ADD INDEX`
/// or `CREATE INDEX` written without one takes, given the names of the
/// table's existing indexes.
///
/// The base is `column_name` (the first key column, or `expression_index`),
/// or `index_name` when the caller passes a truncated retry. `PRIMARY` is
/// reserved, so a column of that name starts at `primary_2`. A collision
/// (compared lowercased) tries `<column>_2`, `<column>_3`, ... from the first
/// index again; a candidate longer than 64 characters restarts from the
/// column's first 30 bytes, exactly as Go recurses.
pub fn get_name_for_anonymous_index(
    existing: &[&str],
    column_name: &str,
    index_name: &str,
) -> String {
    let mut id = 2;
    let mut name = column_name.to_owned();
    if !index_name.is_empty() {
        name = index_name.to_owned();
        id = 3;
    }
    if name.go_to_lower().go_equal_fold("PRIMARY") {
        name = format!("{column_name}_{id}");
        id = 3;
    }
    let mut position = 0;
    while position < existing.len() {
        if existing[position].go_to_lower() == name.go_to_lower() {
            name = format!("{column_name}_{id}");
            if super::check_too_long_identifier(&name).is_err() {
                let truncated = go_byte_prefix(column_name, 30);
                name = get_name_for_anonymous_index(existing, truncated, &format!("{truncated}_2"));
            }
            position = 0;
            id += 1;
            continue;
        }
        position += 1;
    }
    name
}

/// Go `s[:n]` on a string: the first `n` bytes, here stopped at a character
/// boundary so the result stays a `str`.
fn go_byte_prefix(text: &str, bytes: usize) -> &str {
    let mut end = bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Runs a `CREATE INDEX`, backfilling the existing rows.
///
/// Captured from TiDB: a duplicate index name is 1061, and a unique index
/// whose existing rows already collide is 1062 naming `table.index`, leaving
/// the table without the index.
pub fn run_create_index_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let stmt = ctx.parse(sql)?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::unsupported(
            "only CREATE INDEX is supported here",
        ));
    };
    let DdlStmt::CreateIndex(create) = &**ddl else {
        return Err(DriverError::unsupported(
            "only CREATE INDEX is supported here",
        ));
    };
    let (database, table_name) = crate::driver::split_table_path_pub(&create.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    super::refuse_local_temporary_table_ddl(catalog, &database, &table_name, "CREATE INDEX")?;
    let unique = match create.kind {
        tidb_ast::IndexKind::Ordinary => false,
        tidb_ast::IndexKind::Unique => true,
        _ => {
            return Err(DriverError::unsupported(
                "SPATIAL, FULLTEXT, VECTOR and COLUMNAR indexes are not supported yet",
            ))
        }
    };
    if matches!(
        catalog.table_in(&database, &table_name),
        Some(crate::TableEntry::Kv(table)) if table.is_cache_table()
    ) {
        return Err(DriverError::OperationOnCachedTable("Create Index"));
    }
    if create.options.condition.is_some()
        && matches!(
            catalog.table_in(&database, &table_name),
            Some(crate::TableEntry::Kv(table)) if table.partition().is_some()
        )
    {
        return Err(unsupported_partial_index(
            "partial index on partitioned table is not supported",
        ));
    }
    let max_index_length = catalog.max_index_length();
    let bdr = super::bdr::Admission::new(catalog, ctx.ddl_cdc_write_source(), &database);
    add_index_to_table(
        catalog,
        &database,
        &table_name,
        IndexSpec {
            name: &create.name,
            comment: create.options.comment.as_deref().unwrap_or(""),
            unique,
            parts: &create.parts,
            visible: is_visible(&create.options),
            global: create.options.global,
            if_not_exists: create.if_not_exists,
            condition: create.options.condition.as_ref(),
        },
        ctx,
        max_index_length,
        Some(&bdr),
    )
}

/// Go `IndexInfo.Invisible`, read off the statement that declares the index.
///
/// The visibility a statement writes and the `visible` flag the planner reads
/// are the same fact in two places, so it is resolved here, at the single
/// point where an `IndexOptions` becomes a `KvIndex` -- a `CREATE INDEX`, an
/// `ALTER TABLE ... ADD INDEX` and a `CREATE TABLE` key all pass through it.
/// A key with no visibility clause is visible, which is Go's default.
pub(crate) fn is_visible(options: &tidb_ast::IndexOptions) -> bool {
    options.visibility != Some(tidb_ast::IndexVisibility::Invisible)
}

/// Go `dbterror.ErrUnsupportedAddPartialIndex` (8200), including its
/// user-visible message prefix. Partial-index validation has a dedicated
/// errno in TiDB; using the generic unsupported (1105) would make otherwise
/// correct accept/refuse decisions observably different.
pub(crate) fn unsupported_partial_index(reason: impl Into<String>) -> DriverError {
    DriverError::DdlCoded {
        errno: tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
        message: format!("Unsupported add partial index: {}", reason.into()),
    }
}

/// Go `dbterror.ErrModifyColumnReferencedByPartialCondition` (8272), emitted
/// before DROP/MODIFY/CHANGE can leave an index predicate naming a column that
/// no longer exists.
pub(crate) fn partial_index_column_dependency(column: &str, index: &str) -> DriverError {
    DriverError::DdlCoded {
        errno: tidb_error::tidb::errcode::ErrModifyColumnReferencedByPartialCondition,
        message: format!(
            "Cannot drop, change or modify column '{column}': it is referenced in partial index '{index}'"
        ),
    }
}

/// What Go's `checkIndexCondition` reads of the column a condition names.
pub struct IndexConditionColumn {
    /// The column's type.
    pub field_type: tidb_datatype::FieldType,
    /// Whether the column is generated.
    pub generated: bool,
}

/// The type Go's parser driver gives an `ast.ValueExpr` literal
/// (`types.DefaultTypeForValue`, under the connection's charset and
/// collation); `None` for any other expression.
fn index_condition_literal_type(
    expression: &tidb_ast::Expr,
    (charset, collation): (&str, &str),
) -> Option<tidb_datatype::FieldType> {
    use tidb_datatype::{default_field_type_for_value, FieldTypeValue};
    Some(match expression {
        tidb_ast::Expr::Int(text) => match text.parse::<i64>() {
            Ok(value) => {
                default_field_type_for_value(FieldTypeValue::Signed(value), charset, collation)
            }
            Err(_) => default_field_type_for_value(
                FieldTypeValue::Unsigned(text.parse().ok()?),
                charset,
                collation,
            ),
        },
        tidb_ast::Expr::Decimal(text) => {
            let (decimal, _) = tidb_datatype::MyDecimal::from_string(text.as_bytes());
            default_field_type_for_value(
                FieldTypeValue::Decimal {
                    display_len: decimal.to_string_bytes().len() as i64,
                    fraction_digits: i64::from(decimal.digits_frac()),
                },
                charset,
                collation,
            )
        }
        tidb_ast::Expr::Float(value) => {
            default_field_type_for_value(FieldTypeValue::Float64(*value), charset, collation)
        }
        tidb_ast::Expr::Hex(digits) => {
            let literal = tidb_datatype::parse_hex_str(&format!("0x{digits}")).ok()?;
            default_field_type_for_value(
                FieldTypeValue::HexLiteral(literal.as_bytes()),
                charset,
                collation,
            )
        }
        tidb_ast::Expr::Bit(value) => default_field_type_for_value(
            FieldTypeValue::BitLiteral(value.as_bytes()),
            charset,
            collation,
        ),
        tidb_ast::Expr::String(value) | tidb_ast::Expr::RawString(value) => {
            default_field_type_for_value(FieldTypeValue::String(value), charset, collation)
        }
        tidb_ast::Expr::CharsetString {
            charset: introducer,
            value,
        } => {
            let introducer = introducer.to_ascii_lowercase();
            let collation = tidb_datatype::get_default_collation(&introducer).ok()?;
            default_field_type_for_value(FieldTypeValue::String(value), &introducer, &collation)
        }
        tidb_ast::Expr::CharsetBinary {
            charset: introducer,
            value,
        } => {
            let introducer = introducer.to_ascii_lowercase();
            let collation = tidb_datatype::get_default_collation(&introducer).ok()?;
            index_condition_literal_type(value, (charset, collation.as_str()))?
                .with_charset_name(&introducer)
                .with_collation_name(&collation)
        }
        tidb_ast::Expr::Null => {
            default_field_type_for_value(FieldTypeValue::Null, charset, collation)
        }
        tidb_ast::Expr::Bool(value) => {
            default_field_type_for_value(FieldTypeValue::Bool(*value), charset, collation)
        }
        _ => return None,
    })
}

/// Go `checkIndexCondition` (`pkg/ddl/index.go`): a partial index's
/// condition is `column IS [NOT] NULL`, or `column <op> literal` with `op`
/// one of `= != > < >= <=`, over a column that is not generated and a
/// literal whose type fits the column's. `column` finds a table column by its
/// lowercase name; `connection` is the charset and collation a literal takes.
pub fn check_index_condition(
    column: &dyn Fn(&str) -> Option<IndexConditionColumn>,
    condition: &tidb_ast::Expr,
    connection: (&str, &str),
) -> Result<(), DriverError> {
    use tidb_datatype::FieldTypeCode as Tp;
    let name_of = |expression: &tidb_ast::Expr| match expression {
        tidb_ast::Expr::Column(path) => path.last().map(|name| tidb_hack::go_to_lower(name)),
        _ => None,
    };
    let generated = |name: &str| {
        unsupported_partial_index(format!(
            "generated column {name} cannot be used in partial index condition"
        ))
    };
    match condition {
        tidb_ast::Expr::Is {
            expr,
            target: tidb_ast::IsTarget::Null,
            ..
        } => {
            let Some(name) = name_of(expr) else {
                return Err(unsupported_partial_index(
                    "partial index condition must include a column name in the IS NULL expression",
                ));
            };
            let Some(info) = column(&name) else {
                return Err(unsupported_partial_index(format!(
                    "column name {name} referenced in partial index condition is not found in table"
                )));
            };
            if info.generated {
                return Err(generated(&name));
            }
            Ok(())
        }
        tidb_ast::Expr::Binary(op, left, right) => {
            if !matches!(
                op,
                tidb_ast::BinaryOp::Eq
                    | tidb_ast::BinaryOp::Ne
                    | tidb_ast::BinaryOp::Gt
                    | tidb_ast::BinaryOp::Lt
                    | tidb_ast::BinaryOp::Ge
                    | tidb_ast::BinaryOp::Le
            ) {
                return Err(unsupported_partial_index(format!(
                    "binary operation {} is not supported",
                    op.opcode().name()
                )));
            }
            let (name, another_side) = match (name_of(left), name_of(right)) {
                (Some(name), _) => (name, right.as_ref()),
                (None, Some(name)) => (name, left.as_ref()),
                (None, None) => {
                    return Err(unsupported_partial_index(
                        "partial index condition must include a column name in the binary operation",
                    ));
                }
            };
            let Some(info) = column(&name) else {
                return Err(unsupported_partial_index(format!(
                    "column name `{name}` referenced in partial index condition is not found in table"
                )));
            };
            if info.generated {
                return Err(generated(&name));
            }
            let Some(literal) = index_condition_literal_type(another_side, connection) else {
                return Err(unsupported_partial_index(
                    "partial index condition must include a literal value on the other side of the binary operation",
                ));
            };
            let incompatible = || {
                unsupported_partial_index(format!(
                    "the type {} of the column `{name}` in partial index condition is not compatible with the literal value type {literal}",
                    info.field_type
                ))
            };
            let column_tp = info.field_type.code();
            match literal.code() {
                Tp::Tiny | Tp::Short | Tp::Long | Tp::LongLong | Tp::Int24 | Tp::Bit | Tp::Year => {
                    if !matches!(
                        column_tp,
                        Tp::Tiny
                            | Tp::Short
                            | Tp::Long
                            | Tp::LongLong
                            | Tp::Int24
                            | Tp::Bit
                            | Tp::Year
                            | Tp::Enum
                            | Tp::Set
                    ) {
                        return Err(incompatible());
                    }
                    Ok(())
                }
                Tp::Float | Tp::Double | Tp::NewDecimal => {
                    if !matches!(column_tp, Tp::Float | Tp::Double | Tp::NewDecimal) {
                        return Err(incompatible());
                    }
                    Ok(())
                }
                Tp::Varchar
                | Tp::VarString
                | Tp::String
                | Tp::TinyBlob
                | Tp::MediumBlob
                | Tp::LongBlob
                | Tp::Blob => {
                    // Go `types.IsString`.
                    if matches!(
                        column_tp,
                        Tp::String
                            | Tp::Varchar
                            | Tp::VarString
                            | Tp::TinyBlob
                            | Tp::MediumBlob
                            | Tp::LongBlob
                            | Tp::Blob
                            | Tp::Unspecified
                    ) {
                        if info.field_type.charset_name() != literal.charset_name() {
                            return Err(unsupported_partial_index(format!(
                                "the charset {} of the column `{name}` in partial index condition is not compatible with the literal value charset {}",
                                info.field_type.charset_name(),
                                literal.charset_name()
                            )));
                        }
                        return Ok(());
                    }
                    if matches!(
                        column_tp,
                        Tp::Timestamp | Tp::Date | Tp::Duration | Tp::NewDate | Tp::Datetime
                    ) || matches!(column_tp, Tp::Enum | Tp::Set)
                    {
                        return Ok(());
                    }
                    Err(incompatible())
                }
                Tp::Null => Err(unsupported_partial_index(
                    "= NULL is not supported in partial index condition because it is always false",
                )),
                _ => Err(unsupported_partial_index(format!(
                    "the type {literal} of the literal value in partial index condition is not supported"
                ))),
            }
        }
        _ => Err(unsupported_partial_index(
            "the kind of partial index condition is not supported",
        )),
    }
}

/// Go `CheckAndBuildIndexConditionString` (`pkg/ddl/index.go`): the checked
/// condition restored as `IndexInfo.ConditionExprString`, empty without one.
/// `partitioned` is whether the table already has partitions; CREATE TABLE
/// refuses a partitioned partial index later, in `buildTablePartitionInfo`.
pub fn check_and_build_index_condition_string(
    partitioned: bool,
    column: &dyn Fn(&str) -> Option<IndexConditionColumn>,
    condition: Option<&tidb_ast::Expr>,
    connection: (&str, &str),
) -> Result<String, DriverError> {
    let Some(condition) = condition else {
        return Ok(String::new());
    };
    if partitioned {
        return Err(unsupported_partial_index(
            "partial index on partitioned table is not supported",
        ));
    }
    check_index_condition(column, condition, connection)?;
    Ok(condition.restore_with_flags(crate::generated_column::generated_restore_flags()))
}

/// The in-process table's `checkIndexCondition`.
pub(crate) fn validate_partial_index_condition(
    columns: &[KvColumn],
    expression: &tidb_ast::Expr,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let column = |name: &str| {
        columns
            .iter()
            .find(|column| column.name.go_lower_eq(name))
            .map(|column| IndexConditionColumn {
                field_type: column.field_type.clone(),
                generated: column.generated.is_some(),
            })
    };
    check_index_condition(&column, expression, ctx.connection_charset_info())
}

/// [`index_part_names`] for a FOREIGN KEY's child key. Go names an expression
/// part's hidden column `_V$_<key name>_<position>` and then fails to find it
/// among the table's columns: `ErrKeyColumnDoesNotExits` (1072).
pub(crate) fn foreign_key_part_names(
    parts: &[tidb_ast::IndexPart],
    key_name: &str,
) -> Result<Vec<String>, DriverError> {
    if let Some(position) = parts
        .iter()
        .position(|part| matches!(part, tidb_ast::IndexPart::Expr { .. }))
    {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::tidb::errcode::ErrKeyColumnDoesNotExits,
            message: format!("Key column '_V$_{key_name}_{position}' doesn't exist in table"),
        });
    }
    index_part_names(parts)
}

/// The column names an index's key parts name, rejecting the forms this tier
/// does not maintain rather than creating an index that ignores them.
pub(crate) fn index_part_names(parts: &[tidb_ast::IndexPart]) -> Result<Vec<String>, DriverError> {
    let mut names = Vec::with_capacity(parts.len());
    for part in parts {
        let tidb_ast::IndexPart::Column {
            name, prefix_len, ..
        } = part
        else {
            return Err(DriverError::unsupported(
                "an expression index is not supported yet",
            ));
        };
        if prefix_len.is_some() {
            return Err(DriverError::unsupported(
                "a prefix-length index is not supported yet",
            ));
        }
        names.push(name.clone());
    }
    Ok(names)
}

/// Go planner `checkDuplicateColumnName`: a named column may occur only once
/// in one index. Expression parts are independent hidden columns and do not
/// participate in this name set.
pub(crate) fn reject_duplicate_index_columns(
    parts: &[tidb_ast::IndexPart],
) -> Result<(), DriverError> {
    let mut seen = std::collections::HashSet::with_capacity(parts.len());
    for part in parts {
        if let tidb_ast::IndexPart::Column { name, .. } = part {
            if !seen.insert(name.go_to_lower()) {
                return Err(DriverError::DuplicateColumnName(name.clone()));
            }
        }
    }
    Ok(())
}

/// The index a statement asks for, read off either spelling of the request:
/// `CREATE INDEX` and `ALTER TABLE ... ADD INDEX` name the same four facts in
/// different AST nodes.
#[derive(Clone, Copy)]
pub(crate) struct IndexSpec<'a> {
    /// The index's name.
    pub name: &'a str,
    /// Go `IndexInfo.Comment`.
    pub comment: &'a str,
    /// Go `IndexInfo.Unique`.
    pub unique: bool,
    /// The key parts, each a column or an expression.
    pub parts: &'a [tidb_ast::IndexPart],
    /// Go `!IndexInfo.Invisible`.
    pub visible: bool,
    /// Go `IndexInfo.Global`, read off the statement's `GLOBAL` keyword.
    pub global: bool,
    /// Optional `WHERE` predicate for a partial index. The table compiles it
    /// into a sidecar keyed by the assigned index id before backfill.
    pub condition: Option<&'a tidb_ast::Expr>,
    /// Go's `CREATE INDEX`/`ADD INDEX IF NOT EXISTS` guard.
    pub if_not_exists: bool,
}

/// Conditional no-ops retain their note until this action is reached. They
/// never become a multi-schema job or a conflict-check input.
pub(super) enum IndexAdmission<T> {
    Change(T),
    Note(DriverError),
}

pub(super) struct BuiltIndex {
    pub(super) index: KvIndex,
    pub(super) hidden: Vec<crate::expression_index::HiddenIndexColumn>,
}

/// Build metadata without reading rows or writing the catalog. Go performs
/// index admission before collecting multi-schema conflicts and rebuilds the
/// definition in the worker before backfill.
pub(super) fn build_index_definition(
    table: &crate::KvTable,
    index: IndexSpec<'_>,
    ctx: &crate::StmtContext,
    max_index_length: i64,
) -> Result<IndexAdmission<BuiltIndex>, DriverError> {
    let IndexSpec {
        name: index_name,
        comment,
        unique,
        parts,
        visible,
        global,
        condition,
        if_not_exists,
    } = index;
    reject_duplicate_index_columns(parts)?;
    if let Some(condition) = condition {
        validate_partial_index_condition(table.columns(), condition, ctx)?;
    }
    if table
        .indexes()
        .iter()
        .any(|existing| existing.name.go_lower_eq(index_name))
    {
        // Go's `checkIndexNameAndColumns` turns a duplicate index name into
        // ErrDupKeyName (1061) and, for IF NOT EXISTS, appends that error as a
        // Note before returning without building hidden columns or touching
        // the existing index.
        let duplicate = DriverError::DuplicateKeyName(index_name.to_owned());
        if if_not_exists {
            return Ok(IndexAdmission::Note(duplicate));
        }
        return Err(duplicate);
    }
    // Go `createIndex` checks the name's length once it is known to be new
    // (`checkTooLongIndex`), and the job refuses a table that would pass the
    // index limit (`checkTooManyIndexes`).
    super::check_too_long_identifier(index_name)?;
    super::check_too_many_indexes(table.indexes().len() + 1)?;
    // A hidden column is built against the VISIBLE columns, so an expression
    // can never name an earlier index's hidden column.
    let names: Vec<String> = table
        .visible_columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let types: Vec<tidb_datatype::FieldType> = table
        .visible_columns()
        .iter()
        .map(|c| c.field_type.clone())
        .collect();
    let hidden = crate::expression_index::build_hidden_columns_with_like_default_escape(
        index_name,
        parts,
        &names,
        &types,
        &ctx.session_zone(),
        ctx.like_default_escape(),
    )?;
    check_expression_index_auto_increment(
        index_name,
        &hidden,
        table
            .auto_increment_offset()
            .map(|auto| table.columns[auto].name.as_str()),
        ctx,
    )?;
    let mut offsets = Vec::with_capacity(parts.len());
    let mut resolved = Vec::with_capacity(parts.len());
    let mut built = hidden.into_iter();
    let mut pending = Vec::new();
    for part in parts {
        match part {
            tidb_ast::IndexPart::Column {
                name, prefix_len, ..
            } => {
                let offset = table
                    .columns
                    .iter()
                    .position(|candidate| candidate.name.go_lower_eq(name))
                    .ok_or_else(|| DriverError::DdlCoded {
                        errno: 1072,
                        message: format!("column does not exist: {name}"),
                    })?;
                offsets.push(offset);
                resolved.push(ResolvedKeyPart {
                    offset,
                    declared: *prefix_len,
                    expression: false,
                });
            }
            tidb_ast::IndexPart::Expr { .. } => {
                let (_, column) = built.next().expect("one hidden column per expression part");
                let offset = table.columns.len() + pending.len();
                offsets.push(offset);
                resolved.push(ResolvedKeyPart {
                    offset,
                    declared: None,
                    expression: true,
                });
                pending.push(column);
            }
        }
    }
    // The table's columns with the hidden ones this index adds after them.
    let indexed_columns = table
        .columns
        .iter()
        .map(|column| IndexedTableColumn {
            name: &column.name,
            field_type: &column.field_type,
            virtual_generated: column
                .generated
                .as_ref()
                .is_some_and(|generated| !generated.stored),
        })
        .chain(pending.iter().map(|column| IndexedTableColumn {
            name: &column.name,
            field_type: &column.field_type,
            virtual_generated: true,
        }))
        .collect::<Vec<_>>();
    let (prefix_lengths, part_types) =
        build_index_columns(&indexed_columns, &resolved, unique, max_index_length)?;
    let partition_offsets = match table.partition() {
        // A name the partition expression reads that no column has is a
        // catalog bug, not a statement error -- DDL refuses to drop or rename
        // such a column (3855). It must not fall back to an empty list,
        // because an empty list covers every key and the 8264 refusal would
        // silently turn into an ACCEPT.
        Some(partition) => Some(
            crate::generated_column::dependency_offsets(&table.columns, &partition.dependencies)
                .map_err(|missing| {
                    DriverError::Parse(format!(
                        "partition expression of `{index_name}`'s table reads column \
                         `{missing}`, which the table does not define"
                    ))
                })?,
        ),
        None => None,
    };
    let expression_parts = resolved
        .iter()
        .map(|part| part.expression)
        .collect::<Vec<_>>();
    check_create_global_index(
        index_name,
        unique,
        global,
        partition_offsets.as_deref(),
        &indexed_columns,
        &offsets,
        &prefix_lengths,
        &expression_parts,
        ctx,
    )?;
    let comment =
        super::validate_comment_length(comment, index_name, super::CommentOwner::Index, ctx)?;
    Ok(IndexAdmission::Change(BuiltIndex {
        index: KvIndex {
            id: 0,
            name: index_name.to_owned(),
            comment,
            unique,
            column_offsets: offsets,
            prefix_lengths,
            visible,
            global,
            global_index_version: global_index_version(
                global,
                table.pk_handle_offset().is_some() || !table.common_handle_offsets().is_empty(),
                unique,
                &part_types,
            ),
            clustered_primary: false,
        },
        hidden: pending,
    }))
}

/// One column of the table an index is built over, as Go `buildIndexColumns`
/// and `checkCreateGlobalIndex` read it.
pub struct IndexedTableColumn<'a> {
    /// The column's name.
    pub name: &'a str,
    /// The column's type.
    pub field_type: &'a tidb_datatype::FieldType,
    /// Whether it is a VIRTUAL generated column (an expression part's hidden
    /// column is one).
    pub virtual_generated: bool,
}

/// One key part resolved to its column: Go's `IndexPartSpecification` after
/// `BuildHiddenColumnInfo` has pointed an expression part at its hidden column.
pub struct ResolvedKeyPart {
    /// The column's offset in the [`IndexedTableColumn`] list.
    pub offset: usize,
    /// The declared prefix length, if any.
    pub declared: Option<i64>,
    /// Whether the part is an expression, whose hidden column is indexed
    /// whole.
    pub expression: bool,
}

/// Go `buildIndexColumns` for an ADD INDEX: each column part's stored length
/// (`checkIndexColumn`, run strict because the job worker re-runs it with a
/// nil context), the multi-valued rule, and the key's running byte total.
/// Returns each part's stored prefix length and type.
///
/// # Errors
///
/// Go's key-part errors (1089, 1167, 1170, 1391, 3152, 1071, ...).
pub fn build_index_columns(
    columns: &[IndexedTableColumn<'_>],
    parts: &[ResolvedKeyPart],
    unique: bool,
    max_index_length: i64,
) -> Result<(Vec<i64>, Vec<tidb_datatype::FieldType>), DriverError> {
    let mut prefix_lengths = Vec::with_capacity(parts.len());
    let mut part_types: Vec<tidb_datatype::FieldType> = Vec::with_capacity(parts.len());
    let mut mv_index = false;
    for part in parts {
        let column = &columns[part.offset];
        if part.expression {
            prefix_lengths.push(crate::ddl::index_prefix::UNSPECIFIED_LENGTH);
        } else {
            prefix_lengths.push(crate::ddl::index_prefix::key_part_length_with_max(
                column.field_type,
                crate::ddl::index_prefix::IndexedColumn::Named(column.name),
                part.declared,
                true,
                max_index_length,
            )?);
        }
        part_types.push(column.field_type.clone());
        crate::ddl::index_prefix::note_multi_valued_part(
            &mut mv_index,
            part_types.last().expect("this part's type was just pushed"),
        )
        .map_err(|feature| DriverError::NotSupportedYet(feature.into()))?;
    }
    // Go runs the same running sum for ADD INDEX as for CREATE TABLE: each
    // part may be legal and their total still refused.
    crate::ddl::index_prefix::check_index_key_length_with_max(
        part_types.iter().zip(prefix_lengths.iter().copied()),
        parts.len(),
        unique,
        true,
        max_index_length,
    )
    .map_err(crate::ddl::index_prefix::driver_error)?;
    Ok((prefix_lengths, part_types))
}

/// Go `checkCreateGlobalIndex`: GLOBAL needs a partitioned table; a unique
/// index on one must cover every column the partition expression reads
/// unless it is GLOBAL (`checkPartitionKeysConstraint`, 8264); and a global
/// index over a prefix or virtual part warns that auto analyze skips it.
/// `partition_offsets` is `None` for an unpartitioned table.
///
/// # Errors
///
/// 8200 for GLOBAL on an unpartitioned table, 8264 for an uncovered unique
/// key.
#[allow(clippy::too_many_arguments)]
pub fn check_create_global_index(
    index_name: &str,
    unique: bool,
    global: bool,
    partition_offsets: Option<&[usize]>,
    columns: &[IndexedTableColumn<'_>],
    offsets: &[usize],
    prefix_lengths: &[i64],
    expression_parts: &[bool],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if global && partition_offsets.is_none() {
        return Err(global_index_on_non_partitioned_table());
    }
    if global {
        warn_global_index_needs_manual_analyze(
            ctx,
            index_name,
            offsets
                .iter()
                .zip(prefix_lengths)
                .zip(expression_parts.iter().chain(std::iter::repeat(&false)))
                .map(|((offset, prefix), expression)| {
                    // Go reads `tblInfo.Columns[col.Offset]`; an expression
                    // part's hidden column is not in the table yet and still
                    // carries `Offset` 0, so Go judges the first column.
                    let column = if *expression {
                        columns.first()
                    } else {
                        columns.get(*offset)
                    };
                    column.is_none_or(|column| column.virtual_generated)
                        || *prefix != crate::ddl::index_prefix::UNSPECIFIED_LENGTH
                }),
        );
    }
    if let Some(partition_offsets) = partition_offsets.filter(|_| unique && !global) {
        if !partition_offsets
            .iter()
            .all(|offset| super::table_partition::key_part_covers(offsets, prefix_lengths, *offset))
        {
            return Err(DriverError::PartitionGlobalIndexNeeded(
                index_name.to_owned(),
            ));
        }
    }
    Ok(())
}

/// Go `checkExpressionIndexAutoIncrement` (3754): an expression index may not
/// read the AUTO_INCREMENT column unless
/// `tidb_enable_auto_increment_in_generated` allows it.
///
/// # Errors
///
/// 3754 naming the index.
pub fn check_expression_index_auto_increment(
    index_name: &str,
    hidden: &[(usize, crate::expression_index::HiddenIndexColumn)],
    auto_increment_column: Option<&str>,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let Some(auto) = auto_increment_column.filter(|_| !ctx.auto_increment_in_generated()) else {
        return Ok(());
    };
    if hidden.iter().any(|(_, column)| {
        column
            .generated
            .dependencies
            .iter()
            .any(|dependency| dependency.go_lower_eq(auto))
    }) {
        return Err(DriverError::ExpressionIndexCanNotRefer(
            index_name.to_owned(),
        ));
    }
    Ok(())
}

/// Go `checkInvisibleIndexOnPK` over `indexes`, the table's indexes as the
/// DDL leaves them: the primary key Go's `GetPrimaryKey` finds -- the
/// explicit one, else the first unique key over visible NOT NULL columns --
/// cannot be invisible. A PK-is-handle table has none to check.
pub(crate) fn check_invisible_index_on_pk(
    table: &crate::KvTable,
    indexes: &[&KvIndex],
) -> Result<(), DriverError> {
    if table.pk_handle_offset().is_some() {
        return Ok(());
    }
    let explicit = indexes
        .iter()
        .find(|index| index.clustered_primary || index.name.eq_ignore_ascii_case("PRIMARY"));
    let primary = explicit.or_else(|| {
        indexes.iter().find(|index| {
            index.unique
                && !index.column_offsets.is_empty()
                && index.column_offsets.iter().all(|offset| {
                    !table.is_hidden(*offset)
                        && table.columns()[*offset]
                            .field_type
                            .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
                })
        })
    });
    if primary.is_some_and(|index| !index.visible) {
        return Err(pk_index_cant_be_invisible());
    }
    Ok(())
}

/// Go `ErrPKIndexCantBeInvisible` (3522).
pub(crate) fn pk_index_cant_be_invisible() -> DriverError {
    DriverError::PrimaryKeyCantBeInvisible
}

/// Go `checkGlobalIndex` / `checkCreateGlobalIndex`: a GLOBAL index on a
/// table that is not partitioned.
pub(crate) fn global_index_on_non_partitioned_table() -> DriverError {
    DriverError::DdlCoded {
        errno: 8200,
        message: "Unsupported Global Index on non-partitioned table".to_owned(),
    }
}

/// Go `validateGlobalIndexWithGeneratedColumns`: auto analyze cannot
/// sample a global index with a prefix or virtual generated part, so Go
/// warns once (`ErrWarnGlobalIndexNeedManuallyAnalyze`).
pub(crate) fn warn_global_index_needs_manual_analyze(
    ctx: &crate::StmtContext,
    index_name: &str,
    mut part_is_prefix_or_virtual: impl Iterator<Item = bool>,
) {
    if part_is_prefix_or_virtual.any(|flag| flag) {
        ctx.append_warning_parts(
            tidb_error::tidb::errcode::ErrWarnGlobalIndexNeedManuallyAnalyze,
            &format!(
                "Auto analyze is not effective for index '{index_name}', need analyze manually"
            ),
        );
    }
}

/// Go `setGlobalIndexVersion`: a global index on a table without a clustered
/// index keys its entries by partition too (V1) when it is not unique or
/// covers a nullable column, because two partitions may hold the same
/// `_tidb_rowid`.
pub(crate) fn global_index_version(
    global: bool,
    clustered: bool,
    unique: bool,
    part_types: &[tidb_datatype::FieldType],
) -> u8 {
    let nullable = part_types
        .iter()
        .any(|field_type| !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL));
    u8::from(global && !clustered && (!unique || nullable))
}

pub(super) fn prepare_add_index(
    table: &crate::KvTable,
    index: IndexSpec<'_>,
    ctx: &crate::StmtContext,
    max_index_length: i64,
) -> Result<IndexAdmission<()>, DriverError> {
    // A dry run: `add_index_to_table` builds the index again and raises
    // its warnings (a truncated comment), so Go raises each once.
    let bookmark = ctx.warning_count();
    let built = build_index_definition(table, index, ctx, max_index_length);
    tidb_expr::Columns::truncate_warnings(ctx, bookmark);
    Ok(match built? {
        IndexAdmission::Change(_) => IndexAdmission::Change(()),
        IndexAdmission::Note(note) => IndexAdmission::Note(note),
    })
}

/// Adds one index to a table, shared by `CREATE INDEX` and
/// `ALTER TABLE ... ADD INDEX`.
///
/// An EXPRESSION key part is rewritten into a hidden generated column
/// appended to the table, exactly as `CREATE TABLE` does it -- see
/// [`crate::expression_index`]. Nothing about index maintenance changes:
/// entries are written from the materialized row, so the hidden column's
/// value is indexed by the generated-column path that already exists.
pub(crate) fn add_index_to_table(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index: IndexSpec<'_>,
    ctx: &crate::StmtContext,
    max_index_length: i64,
    bdr: Option<&super::bdr::Admission>,
) -> Result<(), DriverError> {
    let schema_id = catalog.database_id(database);
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let table = std::sync::Arc::make_mut(table);
    let BuiltIndex {
        index: mut built,
        hidden: pending,
    } = match build_index_definition(table, index, ctx, max_index_length)? {
        IndexAdmission::Change(built) => built,
        IndexAdmission::Note(note) => {
            ctx.append_suppressed(&note);
            return Ok(());
        }
    };
    // Go `onCreateIndex` checks the table with the new index appended.
    check_invisible_index_on_pk(
        table,
        &table.indexes().iter().chain([&built]).collect::<Vec<_>>(),
    )?;
    // Go `createIndex` submits its job once the index is built.
    if let Some(bdr) = bdr {
        bdr.admit(&[super::bdr::SubmittedJob::add_index(
            tidb_model::ActionType::ACTION_ADD_INDEX,
            index.unique,
        )])?;
        ctx.submit_ddl_job(
            super::job_history::SubmittedDdlJob::new(
                tidb_model::ActionType::ACTION_ADD_INDEX,
                database,
                table_name,
            )
            .with_ids(schema_id, Some(table.table_id)),
        );
    }
    // Go `createIndex`: `checkAndWarnMissingRegionSplitPolicy` before the job.
    super::region_split::warn_missing_region_split_policy(table, &built.name, ctx);
    let index_name = index.name;
    let condition = index.condition;
    let added = pending.len();
    for column in pending {
        table.add_hidden_column(KvColumn {
            name: column.name,
            id: table.next_column_id(),
            field_type: column.field_type,
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            // A hidden expression-index column carries no user comment.
            comment: String::new(),
            generated: Some(column.generated),
            default_value: None,
            origin_default: None,
        });
    }
    let id = table.next_index_id();
    built.id = id;
    if let Some(condition) = condition {
        if let Err(error) = table.add_partial_index_condition(
            id,
            index_name,
            condition,
            &ctx.session_zone(),
            ctx.like_default_escape(),
        ) {
            for _ in 0..added {
                table.drop_column(table.columns.len() - 1);
            }
            return Err(DriverError::Parse(format!(
                "partial index condition failed: {error:?}"
            )));
        }
    }
    let result = table
        .create_index_counting_rows(built, ctx)
        .map(|added| ctx.add_ddl_job_row_count(i64::try_from(added).unwrap_or(i64::MAX)))
        .map_err(index_backfill_error);
    // A failed `CREATE INDEX` must leave no trace. The hidden columns have to
    // be in place BEFORE the index is built (its entries are computed from
    // the materialized row), so a failure takes them back off rather than
    // leaving a column no statement can name and no index uses.
    if result.is_err() {
        table.remove_partial_index_condition(id);
        for _ in 0..added {
            table.drop_column(table.columns.len() - 1);
        }
    }
    result
}

/// Preserve the source SQL diagnostic for local and cluster index backfill.
pub fn index_backfill_error(error: crate::kv_table::KvTableError) -> DriverError {
    match error {
        crate::kv_table::KvTableError::Storage(error) => error.into(),
        crate::kv_table::KvTableError::ColumnCast(error) => {
            DriverError::Exec(crate::ExecError::Mysql(error))
        }
        crate::kv_table::KvTableError::DuplicateKeyName(name) => {
            DriverError::DuplicateKeyName(name)
        }
        // The backfill computes each entry's value from the row, so a
        // generated column that cannot be evaluated fails the DDL with
        // its own code -- Go answers 1365 for
        // `ALTER TABLE t ADD INDEX ((100/a))` over a row with `a = 0`.
        crate::kv_table::KvTableError::Generation {
            eval: Some(eval), ..
        } => DriverError::Exec(crate::ExecError::Eval(eval)),
        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
            DriverError::DuplicateEntry { value, key }
        }
        other => DriverError::Parse(format!("index creation failed: {other:?}")),
    }
}

/// Runs a `DROP INDEX`.
///
/// Captured from TiDB: dropping one that does not exist is 1091 with its own
/// message, "index nosuch doesn't exist".
pub fn run_drop_index_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    // The session's own statement context: this entry RE-PARSES text the
    // session already parsed, so without it a double-quoted name would mean
    // one thing to the session and another here. Key-range deletion itself
    // does not evaluate rows or depend on the session time zone.
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let stmt = ctx.parse(sql)?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::unsupported(
            "only DROP INDEX is supported here",
        ));
    };
    let DdlStmt::DropIndex(drop) = &**ddl else {
        return Err(DriverError::unsupported(
            "only DROP INDEX is supported here",
        ));
    };
    let (database, table_name) = crate::driver::split_table_path_pub(&drop.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    super::refuse_local_temporary_table_ddl(catalog, &database, &table_name, "DROP INDEX")?;
    if matches!(
        catalog.table_in(&database, &table_name),
        Some(crate::TableEntry::Kv(table)) if table.is_cache_table()
    ) {
        return Err(DriverError::OperationOnCachedTable("Drop Index"));
    }
    match prepare_drop_index(catalog, &database, &table_name, &drop.name, drop.if_exists)? {
        IndexAdmission::Change(id) => {
            // Go `dropIndex` submits ActionDropPrimaryKey for PRIMARY.
            let action = if drop.name.go_lower_eq("primary") {
                tidb_model::ActionType::ACTION_DROP_PRIMARY_KEY
            } else {
                tidb_model::ActionType::ACTION_DROP_INDEX
            };
            super::bdr::submit(
                catalog,
                ctx,
                &database,
                &table_name,
                None,
                &[super::bdr::SubmittedJob::new(action)],
            )?;
            drop_prepared_index(catalog, &database, &table_name, id)
        }
        IndexAdmission::Note(note) => {
            ctx.append_suppressed(&note);
            Ok(())
        }
    }
}

pub(super) fn prepare_drop_index(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    index_name: &str,
    if_exists: bool,
) -> Result<IndexAdmission<i64>, DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let found = table
        .indexes()
        .iter()
        .find(|index| index.name.go_lower_eq(index_name));
    // Go `CheckIsDropPrimaryKey`: dropping PRIMARY is dropping the primary
    // key -- 1091 naming PRIMARY when the table has none, and refused while
    // the table is clustered on it, whatever IF EXISTS says.
    if index_name.go_lower_eq("primary") {
        let clustered = table.pk_handle_offset().is_some()
            || found.is_some_and(|index| index.clustered_primary);
        if found.is_none() && !clustered {
            return Err(DriverError::UnknownIndex("PRIMARY".to_owned()));
        }
        if clustered {
            return Err(unsupported_modify_primary_key(
                "Unsupported drop primary key when the table is using clustered index",
            ));
        }
    }
    let Some(index) = found else {
        let missing = DriverError::UnknownIndex(index_name.to_owned());
        if !if_exists {
            return Err(missing);
        }
        return Ok(IndexAdmission::Note(missing));
    };
    crate::foreign_key::check_index_needed(catalog, database, table_name, index_name)?;
    // Go `checkDropIndex` -> `checkInvisibleIndexesOnPK`: the remaining
    // indexes may promote an invisible unique key to the primary key.
    check_invisible_index_on_pk(
        table,
        &table
            .indexes()
            .iter()
            .filter(|other| other.id != index.id)
            .collect::<Vec<_>>(),
    )?;
    Ok(IndexAdmission::Change(index.id))
}

pub(super) fn drop_prepared_index(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index_id: i64,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    // A covering index can disappear with a sibling column drop. Admission
    // already proved its original identity; do not turn its retirement into
    // a new missing-name error or target a different index that reused a name.
    let Some(index) = table.indexes().iter().find(|index| index.id == index_id) else {
        return Ok(());
    };
    let index_name = index.name.clone();
    crate::foreign_key::check_index_needed(catalog, database, table_name, &index_name)?;
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        unreachable!("table resolved above");
    };
    let table = std::sync::Arc::make_mut(table);
    // The hidden columns this index owns go with it. Captured from Go:
    // `alter table te drop index idxe` leaves `SHOW CREATE TABLE` printing
    // only the declared columns, and an `INSERT` then takes their arity.
    // Collected before the drop, while the index still names its offsets.
    let hidden: Vec<usize> = table
        .indexes()
        .iter()
        .find(|index| index.name.go_lower_eq(&index_name))
        .map(|index| {
            let mut offsets: Vec<usize> = index
                .column_offsets
                .iter()
                .copied()
                .filter(|offset| table.is_hidden(*offset))
                .collect();
            // Highest first, so each removal leaves the rest addressable.
            offsets.sort_unstable_by(|a, b| b.cmp(a));
            offsets
        })
        .unwrap_or_default();
    // Go `DropIndexColumnFlag`: the primary key's columns lose PRI (their
    // NOT NULL stays).
    let primary_columns: Vec<usize> = if index_name.go_lower_eq("primary") {
        table
            .indexes()
            .iter()
            .find(|index| index.name.go_lower_eq(&index_name))
            .map(|index| index.column_offsets.clone())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let dropped = table
        .drop_index(&index_name)
        .map_err(|e| DriverError::Parse(format!("index drop failed: {e:?}")))?;
    debug_assert!(dropped, "prepared index identity was resolved above");
    for offset in primary_columns {
        if let Some(column) = table.columns_mut().get_mut(offset) {
            column
                .field_type
                .del_flags(tidb_datatype::FieldTypeFlags::PRI_KEY);
        }
    }
    for offset in hidden {
        table.drop_column(offset);
    }
    Ok(())
}

/// Go `dbterror.ErrUnsupportedModifyPrimaryKey` raised `GenWithStack`, which
/// replaces the template with `message` (8200).
fn unsupported_modify_primary_key(message: &str) -> DriverError {
    DriverError::DdlCoded {
        errno: 8200,
        message: message.to_owned(),
    }
}

/// Go `CreatePrimaryKey` (`ddl/executor.go`): `ALTER TABLE ... ADD PRIMARY
/// KEY` admits only a NONCLUSTERED key named PRIMARY on a table that has no
/// primary key, over plain or stored columns.
pub(super) fn prepare_add_primary_key(
    table: &crate::KvTable,
    definition: &tidb_ast::IndexConstraintDefinition,
    index: IndexSpec<'_>,
    ctx: &crate::StmtContext,
    max_index_length: i64,
) -> Result<IndexAdmission<()>, DriverError> {
    if matches!(
        definition.options.primary_key_storage,
        Some(tidb_ast::PrimaryKeyStorage::Clustered)
    ) {
        return Err(unsupported_modify_primary_key(
            "Adding clustered primary key is not supported. Please consider adding NONCLUSTERED primary key instead",
        ));
    }
    if definition
        .name
        .as_ref()
        .is_some_and(|name| name.chars().count() > 64)
    {
        return Err(DriverError::TooLongIdent("PRIMARY".to_owned()));
    }
    // "If the table's PKIsHandle is true, it also means that this table has a
    // primary key."
    if table.pk_handle_offset().is_some()
        || table
            .indexes()
            .iter()
            .any(|existing| existing.name.go_lower_eq("primary"))
    {
        return Err(DriverError::MultiplePrimaryKey);
    }
    // Primary keys cannot include expression index parts, which are virtual
    // generated columns.
    if definition
        .parts
        .iter()
        .any(|part| matches!(part, tidb_ast::IndexPart::Expr { .. }))
    {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::tidb::errcode::ErrFunctionalIndexPrimaryKey,
            message: "The primary key cannot be an expression index".to_owned(),
        });
    }
    // Go `CheckPKOnGeneratedColumn`.
    for part in &definition.parts {
        let tidb_ast::IndexPart::Column { name, .. } = part else {
            continue;
        };
        let Some(offset) = table
            .columns()
            .iter()
            .position(|column| column.name.go_lower_eq(name))
            .filter(|offset| !table.is_hidden(*offset))
        else {
            // Go `ErrKeyColumnDoesNotExits` (1072), the variant foreign keys
            // raise too.
            return Err(DriverError::ForeignKeyChildColumnMissing(name.clone()));
        };
        if table.columns()[offset]
            .generated
            .as_ref()
            .is_some_and(|generated| !generated.stored)
        {
            return Err(DriverError::DdlCoded {
                errno: 3106,
                message: "'Defining a virtual generated column as primary key' is not supported for generated columns.".to_owned(),
            });
        }
    }
    prepare_add_index(table, index, ctx, max_index_length)
}

/// Go `onCreateIndex` with `isPK`: `checkPrimaryKeyNotNull` makes the key's
/// nullable columns NOT NULL -- after `modifyColsFromNull2NotNull` proves no
/// row holds NULL there (1138) -- the index is built, and `AddIndexColumnFlag`
/// marks every key column PRI.
pub(super) fn add_primary_key_to_table(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index: IndexSpec<'_>,
    ctx: &crate::StmtContext,
    max_index_length: i64,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let table = std::sync::Arc::make_mut(table);
    let offsets: Vec<usize> = index
        .parts
        .iter()
        .filter_map(|part| match part {
            tidb_ast::IndexPart::Column { name, .. } => table
                .columns()
                .iter()
                .position(|column| column.name.go_lower_eq(name)),
            tidb_ast::IndexPart::Expr { .. } => None,
        })
        .collect();
    let nullable: Vec<usize> = offsets
        .iter()
        .copied()
        .filter(|&offset| {
            !table.columns()[offset]
                .field_type
                .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
        })
        .collect();
    if !nullable.is_empty()
        && table
            .any_null_in_columns(&nullable, &crate::kv_table::RowDecodeContext::for_ddl(ctx))
            .map_err(index_backfill_error)?
    {
        return Err(DriverError::InvalidUseOfNull);
    }
    add_index_to_table(
        catalog,
        database,
        table_name,
        index,
        ctx,
        max_index_length,
        None,
    )?;
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        unreachable!("the table was resolved above");
    };
    let table = std::sync::Arc::make_mut(table);
    for offset in offsets {
        if let Some(column) = table.columns_mut().get_mut(offset) {
            column.field_type.add_flags(
                tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::PRI_KEY,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::get_name_for_anonymous_index;

    /// Go `GetName4AnonymousIndex`: the first column names the index, a
    /// collision (compared lowercased) restarts the scan with `_2`, `_3`, ...,
    /// and `PRIMARY` is never taken.
    #[test]
    fn an_anonymous_index_is_named_as_go_names_it() {
        assert_eq!(get_name_for_anonymous_index(&[], "a", ""), "a");
        assert_eq!(get_name_for_anonymous_index(&["a_2", "A"], "a", ""), "a_3");
        assert_eq!(
            get_name_for_anonymous_index(&["x"], "primary", ""),
            "primary_2"
        );
        assert_eq!(
            get_name_for_anonymous_index(&["primary_2"], "Primary", ""),
            "Primary_3"
        );
        assert_eq!(
            get_name_for_anonymous_index(&["expression_index"], "expression_index", ""),
            "expression_index_2"
        );
    }

    /// A candidate past 64 characters restarts from the column's first 30
    /// bytes, with `<prefix>_2` as the first try.
    #[test]
    fn a_too_long_candidate_restarts_from_a_thirty_byte_prefix() {
        let column = "c".repeat(64);
        let prefix = "c".repeat(30);
        assert_eq!(
            get_name_for_anonymous_index(&[column.as_str()], &column, ""),
            format!("{prefix}_2")
        );
        let taken = format!("{prefix}_2");
        assert_eq!(
            get_name_for_anonymous_index(&[column.as_str(), taken.as_str()], &column, ""),
            format!("{prefix}_3")
        );
    }
}
