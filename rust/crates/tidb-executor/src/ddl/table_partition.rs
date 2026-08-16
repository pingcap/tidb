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

//! `CREATE TABLE ... PARTITION BY`: building the partitioning, and refusing
//! the methods this tier cannot route.
//!
//! Mirrors Go `pkg/ddl/partition.go`'s `buildTablePartitionInfo` together
//! with the validation `checkTableInfoValidWithStmt` runs after it
//! (`checkPartitionDefinitionConstraints`, `checkPartitionFuncType`,
//! `checkPartitioningKeysConstraints`). The result is the
//! [`crate::partition_routing::PartitionSpec`] the table stores; a table with
//! no clause gets `None`.
//!
//! # What is accepted
//!
//! HASH, KEY, scalar RANGE, RANGE COLUMNS, scalar LIST, and LIST COLUMNS. They are
//! routed by [`crate::partition_routing`], stored as one physical key prefix
//! per partition, pruned by [`crate::partition_pruning`], and printed back by
//! `SHOW CREATE TABLE`.
//!
//! KEY and RANGE COLUMNS are routed as typed tuples, alongside scalar RANGE
//! and LIST COLUMNS.
//!
//! # Why "accept and ignore" is never the answer
//!
//! There IS a shape of `CREATE TABLE ... PARTITION BY` that real TiDB
//! accepts while building an ordinary table: `buildTablePartitionInfo`
//! leaves `enable == false`, warns `Unsupported partition type %v, treat as
//! normal table`, and returns with `tbInfo.Partition` unset. That branch is
//! unreachable for every method the grammar admits: RANGE and LIST set
//! `enable = true` unconditionally, and HASH/KEY set it in both arms of
//! their own `if`. So for every statement a user can write, Go builds a REAL
//! partitioned table, and a node that discards the clause is simply wrong.
//!
//! # The validation is not optional
//!
//! Go rejects a great deal at `CREATE`, and a node that accepts the clause
//! without those rules starts SUCCEEDING on statements TiDB refuses. Every
//! rule below was captured from real TiDB; the ones that belong to a method
//! still refused stay unported on purpose -- the refusal is what keeps them
//! honest until the method lands.
//!
//! # LINEAR
//!
//! `LINEAR HASH` is ACCEPTED by Go as plain HASH, with warning 8200
//! `LINEAR HASH is not supported, using non-linear HASH instead` (captured).
//! It is accepted here the same way, and `SHOW CREATE TABLE` prints it back
//! without the keyword, exactly as Go does.

use tidb_ast::{CreateTableStmt, Expr, PartitionDefinitionClause, PartitionType};
use tidb_datatype::{FieldType, FieldTypeCode};

use crate::generated_column::TableColumnResolver;
use crate::kv_table::KvIndex;
use crate::partition_routing::{PartitionDef, PartitionKind, PartitionSpec};
use crate::DriverError;

/// Go `checkAddPartitionTooManyPartitions`: the hard cap on partitions.
pub(super) const MAX_PARTITIONS: u64 = 8192;

/// Builds a table's partitioning from its `PARTITION BY` clause.
///
/// `names`/`types` are the table's own columns (the partition expression is
/// resolved against them and evaluated by their offsets); `indexes` are the
/// indexes already built for the table, which the unique-key rule reads;
/// `handle_offsets` are the clustered primary key's column offsets, which
/// carry the same rule without being an entry in `indexes`.
/// `allocate_id` yields one physical table id per partition, in definition
/// order, so the partitions occupy a contiguous ascending block.
///
/// `Ok(None)` for a statement with no partitioning clause. The one WARNING
/// this clause can produce is reported by [`linear_partitioning_warning`],
/// which the session reads from the statement itself.
///
/// # Errors
///
/// The captured `CREATE` rejections -- 1054, 1486, 1500, 1503, 1504, 1517,
/// 1564, 1659, 8264 -- plus [`DriverError::Unsupported`] naming the method,
/// for one this tier does not route.
pub fn build_table_partitioning(
    create: &CreateTableStmt,
    names: &[String],
    types: &[FieldType],
    indexes: &[KvIndex],
    handle_offsets: &[usize],
    allocate_id: &mut dyn FnMut() -> i64,
    // The SESSION's context: a RANGE bound is FOLDED here, and its value can
    // depend on the session `time_zone`. Go threads its `BuildContext` down
    // the same path (`buildTablePartitionInfo` -> `checkPartitionValuesIsInt`).
    ctx: &crate::StmtContext,
) -> Result<Option<PartitionSpec>, DriverError> {
    let Some(partitioning) = &create.partitioning else {
        return Ok(None);
    };
    let method = &partitioning.method;
    if !matches!(
        method.kind,
        PartitionType::HASH | PartitionType::KEY | PartitionType::RANGE | PartitionType::LIST
    ) {
        // The method name is Go's own spelling, so the refusal reads like the
        // clause the user wrote rather than like a Rust variant.
        let name = method.kind.sql();
        return Err(DriverError::unsupported(format!(
            "CREATE TABLE ... PARTITION BY {name} is not supported by this node: \
             it can neither route a row to one of those partitions nor prune \
             them, so accepting the clause would build an ordinary \
             unpartitioned table"
        )));
    }
    // Go `buildTablePartitionInfo`: subpartitioning is only legal under
    // RANGE/LIST, and Go WARNS rather than errors there. This tier routes no
    // subpartition either way, so it refuses both: 1500 under HASH, which is
    // Go's own error, and a loud refusal under RANGE, where accepting the
    // clause and ignoring the subpartitioning is the discard this module
    // exists to prevent.
    if partitioning.subpartition.is_some() {
        if matches!(method.kind, PartitionType::HASH | PartitionType::KEY) {
            return Err(DriverError::PartitionSubpartition);
        }
        return Err(DriverError::unsupported(format!(
            "CREATE TABLE ... PARTITION BY {} ... SUBPARTITION BY is not supported by this \
                 node",
            method.kind.sql()
        )));
    }

    if method.kind == PartitionType::KEY {
        let dependencies =
            build_key_partition_columns(&method.columns, names, types, handle_offsets)?;
        let definitions = build_hash_partition_definitions(create, method.count, allocate_id)?;
        check_partition_name_unique(&definitions)?;
        if definitions.len() as u64 > MAX_PARTITIONS {
            return Err(DriverError::PartitionTooMany);
        }
        if definitions.is_empty() {
            return Err(DriverError::PartitionNoParts("partitions"));
        }
        let dependency_offsets = dependencies
            .iter()
            .map(|name| {
                names
                    .iter()
                    .position(|candidate| candidate.eq_ignore_ascii_case(name))
                    .expect("KEY names resolved above")
            })
            .collect::<Vec<_>>();
        check_unique_keys_include_partition_columns(indexes, handle_offsets, &dependency_offsets)?;
        return Ok(Some(PartitionSpec {
            kind: PartitionKind::Key,
            expr_text: dependencies
                .iter()
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(","),
            expr: tidb_expr::expression::Expression::Constant(
                tidb_expr::expression::Constant::new(
                    tidb_datatype::Datum::Null,
                    FieldType::new(FieldTypeCode::LongLong),
                ),
            ),
            dependencies,
            definitions,
        }));
    }

    if method.kind == PartitionType::LIST && method.expr.is_none() {
        let (dependencies, kind) = super::table_partition_list::build_list_columns_values(
            &method.columns,
            &partitioning.definitions,
            names,
            types,
            ctx,
        )?;
        let definitions = build_named_partition_definitions(create, allocate_id);
        check_partition_name_unique(&definitions)?;
        if definitions.len() as u64 > MAX_PARTITIONS {
            return Err(DriverError::PartitionTooMany);
        }
        let dependency_offsets = dependencies
            .iter()
            .map(|name| {
                names
                    .iter()
                    .position(|candidate| candidate.eq_ignore_ascii_case(name))
                    .expect("LIST COLUMNS names resolved above")
            })
            .collect::<Vec<_>>();
        check_unique_keys_include_partition_columns(indexes, handle_offsets, &dependency_offsets)?;
        return Ok(Some(PartitionSpec {
            kind,
            expr_text: method
                .columns
                .iter()
                .filter_map(|path| path.last())
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(","),
            expr: tidb_expr::expression::Expression::Constant(
                tidb_expr::expression::Constant::new(
                    tidb_datatype::Datum::Null,
                    FieldType::new(FieldTypeCode::LongLong),
                ),
            ),
            dependencies,
            definitions,
        }));
    }

    if method.kind == PartitionType::RANGE && method.expr.is_none() {
        let (dependencies, field_types, less_than) =
            super::table_partition_range::build_range_columns_bounds(
                &method.columns,
                &partitioning.definitions,
                names,
                types,
                ctx,
            )?;
        let definitions = build_named_partition_definitions(create, allocate_id);
        check_partition_name_unique(&definitions)?;
        if definitions.len() as u64 > MAX_PARTITIONS {
            return Err(DriverError::PartitionTooMany);
        }
        let dependency_offsets = dependencies
            .iter()
            .map(|name| {
                names
                    .iter()
                    .position(|candidate| candidate.eq_ignore_ascii_case(name))
                    .expect("RANGE COLUMNS names resolved above")
            })
            .collect::<Vec<_>>();
        check_unique_keys_include_partition_columns(indexes, handle_offsets, &dependency_offsets)?;
        return Ok(Some(PartitionSpec {
            kind: PartitionKind::RangeColumns {
                less_than,
                field_types,
            },
            expr_text: method
                .columns
                .iter()
                .filter_map(|path| path.last())
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(","),
            expr: tidb_expr::expression::Expression::Constant(
                tidb_expr::expression::Constant::new(
                    tidb_datatype::Datum::Null,
                    FieldType::new(FieldTypeCode::LongLong),
                ),
            ),
            dependencies,
            definitions,
        }));
    }

    let Some(expr) = &method.expr else {
        // A column list rather than an expression is Go's KEY-shaped path
        // (`HASH COLUMNS`), which this tier does not route.
        let name = method.kind.sql();
        return Err(DriverError::unsupported(format!(
            "CREATE TABLE ... PARTITION BY {name} COLUMNS is not supported by this node"
        )));
    };
    // `RANGE ... INTERVAL (...)` GENERATES definitions from a step, which
    // this tier does not expand; accepting it would build a table with the
    // wrong partitions rather than none.
    if method.interval.is_some() {
        return Err(DriverError::unsupported(
            "CREATE TABLE ... PARTITION BY RANGE ... INTERVAL is not supported by this node"
                .to_owned(),
        ));
    }

    let (expr_text, built, dependencies, dependency_offsets) = build_partition_expression(
        expr,
        names,
        types,
        &ctx.session_zone(),
        ctx.like_default_escape(),
    )?;
    // Go `checkPartitionFuncType`: the partition expression must evaluate to
    // an integer.
    check_partition_expression_type(expr, names, types)?;

    let (kind, definitions) = match method.kind {
        PartitionType::RANGE => {
            let (less_than, unsigned) = super::table_partition_range::build_range_bounds(
                expr,
                &partitioning.definitions,
                names,
                types,
                &dependency_offsets,
                ctx,
            )?;
            let definitions = build_named_partition_definitions(create, allocate_id);
            (
                PartitionKind::Range {
                    less_than,
                    unsigned,
                },
                definitions,
            )
        }
        PartitionType::LIST => {
            let kind = super::table_partition_list::build_list_values(
                expr,
                &partitioning.definitions,
                names,
                types,
                &dependency_offsets,
                ctx,
            )?;
            let definitions = build_named_partition_definitions(create, allocate_id);
            (kind, definitions)
        }
        _ => (
            PartitionKind::Hash,
            build_hash_partition_definitions(create, method.count, allocate_id)?,
        ),
    };
    check_partition_name_unique(&definitions)?;
    if definitions.len() as u64 > MAX_PARTITIONS {
        return Err(DriverError::PartitionTooMany);
    }
    // Go `checkNoHashPartitions`. The parser now rejects a written
    // `PARTITIONS 0` itself (as Go's does), so this check is defensive:
    // no SQL text reaches it with zero definitions.
    if definitions.is_empty() {
        return Err(DriverError::PartitionNoParts("partitions"));
    }
    check_unique_keys_include_partition_columns(indexes, handle_offsets, &dependency_offsets)?;

    Ok(Some(PartitionSpec {
        kind,
        expr_text,
        expr: built,
        dependencies,
        definitions,
    }))
}

/// Resolve Go `PARTITION BY KEY` columns.  An empty list means the table's
/// primary key; a heap table therefore hashes the empty byte stream, exactly
/// as Go's `ForKeyPruning` does when `PartitionInfo.Columns` remains empty.
fn build_key_partition_columns(
    columns: &[Vec<String>],
    names: &[String],
    types: &[FieldType],
    handle_offsets: &[usize],
) -> Result<Vec<String>, DriverError> {
    let selected: Vec<String> = if columns.is_empty() {
        handle_offsets
            .iter()
            .filter_map(|offset| names.get(*offset).cloned())
            .collect()
    } else {
        columns
            .iter()
            .map(|path| {
                path.last()
                    .cloned()
                    .ok_or(DriverError::PartitionColumnValueWrongType)
            })
            .collect::<Result<_, _>>()?
    };
    let mut dependencies = Vec::with_capacity(selected.len());
    for name in selected {
        if dependencies
            .iter()
            .any(|candidate: &String| candidate.eq_ignore_ascii_case(&name))
        {
            return Err(DriverError::PartitionDuplicateField(name));
        }
        let offset = names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(&name))
            .ok_or_else(|| DriverError::UnknownColumnInClause {
                column: name.clone(),
                clause: "partition function".to_owned(),
            })?;
        if !key_partition_type_allowed(&types[offset]) {
            return Err(DriverError::PartitionFieldTypeNotAllowed(name));
        }
        dependencies.push(name);
    }
    Ok(dependencies)
}

/// Go `isValidKeyPartitionColType`: only LOB/JSON/geometry/vector columns are
/// rejected.  Character and temporal values hash through their datum keys.
fn key_partition_type_allowed(field_type: &FieldType) -> bool {
    !matches!(
        field_type.code(),
        FieldTypeCode::TinyBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Json
            | FieldTypeCode::Geometry
            | FieldTypeCode::VectorFloat32
    )
}

/// The partitions of a method whose definitions are all WRITTEN, which is
/// RANGE (and would be LIST): one per source definition, in source order.
fn build_named_partition_definitions(
    create: &CreateTableStmt,
    allocate_id: &mut dyn FnMut() -> i64,
) -> Vec<PartitionDef> {
    create.partitioning.as_ref().map_or_else(Vec::new, |p| {
        p.definitions
            .iter()
            .map(|definition| PartitionDef {
                id: allocate_id(),
                name: definition.name.clone(),
            })
            .collect()
    })
}

/// The warning a `LINEAR HASH`/`LINEAR KEY` clause earns, or `None`.
///
/// Go accepts `LINEAR` and builds a plain non-linear table, appending
/// `ErrUnsupportedCreatePartition` (8200). Captured verbatim:
/// `LINEAR HASH is not supported, using non-linear HASH instead`, and
/// `SHOW CREATE TABLE` then prints the clause WITHOUT the keyword.
///
/// It is computed from the statement rather than returned by
/// [`build_table_partitioning`] because the session already reads a
/// `CREATE TABLE`'s warnings off the AST that way (the discarded-`CHECK`
/// warnings take the same route), so the build path needs no warning sink.
#[must_use]
pub fn linear_partitioning_warning(create: &CreateTableStmt) -> Option<String> {
    let partitioning = create.partitioning.as_ref()?;
    if !partitioning.method.linear {
        return None;
    }
    let name = partitioning.method.kind.sql();
    Some(format!(
        "LINEAR {name} is not supported, using non-linear {name} instead"
    ))
}

/// Go's restore flags for `PartitionInfo.Expr`
/// (`format.DefaultRestoreFlags | RestoreBracketAroundBinaryOperation |
/// RestoreWithoutSchemaName | RestoreWithoutTableName`).
///
/// The bracket flag is why `hash(a+b)` is stored -- and printed -- as
/// ``(`a`+`b`)``, with no spaces around the operator, unlike a generated
/// column's ``(`a` + 1)``. Captured: ``PARTITION BY HASH ((`a`+`b`))``.
fn partition_restore_flags() -> tidb_ast::RestoreFlags {
    tidb_ast::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_ast::RestoreFlags::KEYWORD_UPPERCASE
        | tidb_ast::RestoreFlags::NAME_BACK_QUOTES
        | tidb_ast::RestoreFlags::BRACKET_AROUND_BINARY_OPERATION
        | tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME
        | tidb_ast::RestoreFlags::WITHOUT_TABLE_NAME
}

/// Go `checkPartitionFuncValid` plus the expression build: the restored text
/// Go stores, the evaluable form, and the column offsets it reads.
fn build_partition_expression(
    expr: &Expr,
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
) -> Result<
    (
        String,
        tidb_expr::expression::Expression,
        Vec<String>,
        Vec<usize>,
    ),
    DriverError,
> {
    check_partition_expression_allowed(expr)?;
    let resolver = TableColumnResolver::with_like_default_escape(
        names,
        types,
        zone.clone(),
        like_default_escape,
    );
    let built =
        tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver).map_err(|_| match resolver
            .missing_name()
        {
            Some(name) => DriverError::UnknownColumnInClause {
                column: name,
                clause: "partition function".to_owned(),
            },
            None => DriverError::unsupported(
                "this partition expression is not supported yet".to_owned(),
            ),
        })?;
    // A rewrite can succeed while a branch failed to resolve only if the
    // resolver was never consulted, so the missing name is still the
    // authority on 1054.
    if let Some(name) = resolver.missing_name() {
        return Err(DriverError::UnknownColumnInClause {
            column: name,
            clause: "partition function".to_owned(),
        });
    }
    let dependency_offsets = resolver.dependencies();
    let dependencies = resolver.dependency_names();
    // Go `checkPartitionFuncValid`: an expression naming no column at all is
    // 1486 -- `PARTITION BY HASH(1)` has nothing to partition ON.
    if dependencies.is_empty() {
        return Err(DriverError::PartitionWrongExprInFunc);
    }
    Ok((
        expr.restore_with_flags(partition_restore_flags()),
        built,
        dependencies,
        dependency_offsets,
    ))
}

/// Go `AllowedPartitionFuncMap`, by lowercase function name.
const ALLOWED_PARTITION_FUNCTIONS: &[&str] = &[
    "to_days",
    "to_seconds",
    "dayofmonth",
    "month",
    "dayofyear",
    "quarter",
    "yearweek",
    "year",
    "weekday",
    "dayofweek",
    "day",
    "hour",
    "minute",
    "second",
    "time_to_sec",
    "microsecond",
    "unix_timestamp",
    "from_days",
    "extract",
    "abs",
    "ceiling",
    "datediff",
    "floor",
    "mod",
];

/// Go `checkPartitionExprAllowed`: every node of a partition expression must
/// be in the whitelist, or 1564.
///
/// Go's list is [`ALLOWED_PARTITION_FUNCTIONS`],
/// `AllowedPartition4BinaryOpMap` (`+ - * DIV %`),
/// `AllowedPartition4UnaryOpMap` (unary `+ -`), and the leaf forms: a column
/// reference, parentheses, a literal, `MAXVALUE`, `DEFAULT`, a time unit.
/// Everything else -- `rand()` being the captured case -- is refused.
fn check_partition_expression_allowed(expr: &Expr) -> Result<(), DriverError> {
    match expr {
        Expr::Column(_)
        | Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::Bool(_)
        | Expr::Default(_) => Ok(()),
        Expr::Paren(inner) => check_partition_expression_allowed(inner),
        Expr::Extract { value, .. } => check_partition_expression_allowed(value),
        // Go `AllowedPartition4UnaryOpMap`.
        Expr::Unary(tidb_ast::UnaryOp::Plus | tidb_ast::UnaryOp::Minus, inner) => {
            check_partition_expression_allowed(inner)
        }
        // Go `AllowedPartition4BinaryOpMap`.
        Expr::Binary(
            tidb_ast::BinaryOp::Plus
            | tidb_ast::BinaryOp::Minus
            | tidb_ast::BinaryOp::Mul
            | tidb_ast::BinaryOp::IntDiv
            | tidb_ast::BinaryOp::Mod,
            left,
            right,
        ) => {
            check_partition_expression_allowed(left)?;
            check_partition_expression_allowed(right)
        }
        Expr::Func { name, args, .. }
            if ALLOWED_PARTITION_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str()) =>
        {
            for arg in args {
                check_partition_expression_allowed(arg)?;
            }
            Ok(())
        }
        _ => Err(DriverError::PartitionFunctionNotAllowed),
    }
}

/// Go `checkPartitionFuncType`: the expression must evaluate to an integer.
///
/// Go builds the expression and asks its `EvalType()`, and WHICH error it
/// then reports depends on the shape of the expression, not on which column
/// is at fault:
///
/// * a bare COLUMN of a non-integer type is `ErrNotAllowedTypeInPartition`
///   (1659), naming the column -- Go reaches that arm only when the built
///   expression IS an `expression.Column`. Captured:
///   `partition by hash(a)` over a `double a` is
///   ``Field 'a' is of a not allowed type for this type of partitioning``;
/// * anything else whose result type is not integral is
///   `ErrPartitionFuncNotAllowed` (1491), naming the CLAUSE rather than a
///   column: `The PARTITION function returns the wrong type`.
///
/// So the question a whitelisted expression has to answer is its own result
/// type, not its columns' -- `hash(year(d))` over a DATE column is ACCEPTED,
/// because `YEAR` returns an integer. Reading the columns instead would
/// reject the single most common date-partitioning form there is.
fn check_partition_expression_type(
    expr: &Expr,
    names: &[String],
    types: &[FieldType],
) -> Result<(), DriverError> {
    // A bare column reference reports the name AS WRITTEN, which a qualified
    // `t.a` shortens to `a` exactly as Go's `col2.Name.Name.L` does.
    if let Expr::Column(path) = unwrap_parentheses(expr) {
        if partition_expr_is_integral(expr, names, types) {
            return Ok(());
        }
        let name = path.last().cloned().unwrap_or_else(|| "?".to_owned());
        return Err(DriverError::PartitionFieldTypeNotAllowed(name));
    }
    if partition_expr_is_integral(expr, names, types) {
        return Ok(());
    }
    Err(DriverError::PartitionFuncWrongType)
}

/// Whether a whitelisted partition expression evaluates to an INTEGER, which
/// is Go's `e.GetType().EvalType() == types.ETInt`.
///
/// Only the forms [`check_partition_expression_allowed`] admits reach here.
/// Every function on Go's `AllowedPartitionFuncMap` returns an integer, `DIV`
/// is integer division whatever its operands are, and the remaining
/// arithmetic is integral exactly when both operands are.
fn partition_expr_is_integral(expr: &Expr, names: &[String], types: &[FieldType]) -> bool {
    match expr {
        Expr::Column(path) => path
            .last()
            .and_then(|name| {
                names
                    .iter()
                    .position(|candidate| candidate.eq_ignore_ascii_case(name))
            })
            .is_some_and(|offset| is_integer_type(&types[offset])),
        Expr::Paren(inner) => partition_expr_is_integral(inner, names, types),
        Expr::Unary(_, inner) => partition_expr_is_integral(inner, names, types),
        Expr::Binary(tidb_ast::BinaryOp::IntDiv, _, _) => true,
        Expr::Binary(_, left, right) => {
            partition_expr_is_integral(left, names, types)
                && partition_expr_is_integral(right, names, types)
        }
        // Reachable only for a whitelisted name, all of which return an
        // integer.
        Expr::Func { .. } => true,
        Expr::Extract { .. } => true,
        Expr::Int(_) | Expr::Bool(_) | Expr::Hex(_) | Expr::Bit(_) => true,
        _ => false,
    }
}

/// The parenthesised expression's subject, since `(a)` partitions on `a`.
fn unwrap_parentheses(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) => unwrap_parentheses(inner),
        other => other,
    }
}

/// Whether a column's type is one a HASH partition expression may read: Go's
/// `EvalType() == types.ETInt`.
fn is_integer_type(field_type: &FieldType) -> bool {
    matches!(
        field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
            | FieldTypeCode::Bit
    )
}

/// Go `buildHashPartitionDefinitions`: `n` partitions, named `p0..pn-1`
/// unless the statement named them itself.
fn build_hash_partition_definitions(
    create: &CreateTableStmt,
    count: u64,
    allocate_id: &mut dyn FnMut() -> i64,
) -> Result<Vec<PartitionDef>, DriverError> {
    // A HASH partition definition carries no values -- `tidb_parser`'s
    // `validate_definition` already rejects `VALUES` on one -- so the written
    // definitions contribute nothing but their names.
    let written = create
        .partitioning
        .as_ref()
        .map_or(&[][..], |partitioning| &partitioning.definitions);
    debug_assert!(written
        .iter()
        .all(|definition| matches!(definition.clause, PartitionDefinitionClause::None)));
    let mut definitions = Vec::with_capacity(count as usize);
    for index in 0..count {
        let name = written
            .get(index as usize)
            .map_or_else(|| format!("p{index}"), |written| written.name.clone());
        definitions.push(PartitionDef {
            id: allocate_id(),
            name,
        });
    }
    Ok(definitions)
}

/// Go `checkPartitionNameUnique` (1517), matched case-insensitively as
/// partition names are.
fn check_partition_name_unique(definitions: &[PartitionDef]) -> Result<(), DriverError> {
    for (index, definition) in definitions.iter().enumerate() {
        if definitions[..index]
            .iter()
            .any(|earlier| earlier.name.eq_ignore_ascii_case(&definition.name))
        {
            return Err(DriverError::PartitionSameName(definition.name.clone()));
        }
    }
    Ok(())
}

/// Go `checkPartitionKeysConstraint` and `ErrGlobalIndexNotExplicitlySet`
/// (8264): every UNIQUE key must contain every column the partition
/// expression reads, unless the index was declared GLOBAL.
///
/// This rule is also what makes a LOCAL unique index and a GLOBAL one
/// indistinguishable in this tier: because a unique key covers the partition
/// columns, two rows sharing that key route to the SAME partition, so
/// per-partition uniqueness and table-wide uniqueness are the same
/// constraint. That equivalence is why index entries here stay keyed by the
/// TABLE id rather than by the partition id -- and it holds only while this
/// check does, which is why the two live in one file.
fn check_unique_keys_include_partition_columns(
    indexes: &[KvIndex],
    handle_offsets: &[usize],
    dependencies: &[usize],
) -> Result<(), DriverError> {
    for index in indexes {
        if !index.unique {
            continue;
        }
        if !dependencies
            .iter()
            .all(|offset| index.column_offsets.contains(offset))
        {
            return Err(DriverError::PartitionGlobalIndexNeeded(index.name.clone()));
        }
    }
    // A clustered primary key is no entry in `indexes` -- its encoding IS the
    // row key -- so Go checks it separately, and reports it as
    // `CLUSTERED INDEX` rather than by name.
    if !handle_offsets.is_empty()
        && !dependencies
            .iter()
            .all(|offset| handle_offsets.contains(offset))
    {
        return Err(DriverError::PartitionUniqueKeyNeedAllFields(
            "CLUSTERED INDEX".to_owned(),
        ));
    }
    Ok(())
}
