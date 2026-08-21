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

use tidb_ast::{
    CreateTableStmt, Expr, PartitionDefinition, PartitionDefinitionClause, PartitionType,
    PartitionValue,
};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::generated_column::TableColumnResolver;
use crate::kv_table::KvIndex;
use crate::partition_routing::{
    PartitionDef, PartitionKind, PartitionSpec, RangeBound, RangeColumnBound,
};
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
        if dependencies
            .iter()
            .all(|offset| index.column_offsets.contains(offset))
        {
            continue;
        }
        // Go `ddl/partition.go:700` reports the CLUSTERED primary key as
        // `CLUSTERED INDEX` (1503) rather than by name, and does so BEFORE
        // the global exemption below -- a clustered primary key is the row
        // key itself and can never be a global index.
        //
        // Only a COMMON handle reaches here: a `pk_is_handle` table encodes
        // its primary key as the handle and builds no index for it, so there
        // is no PRIMARY entry in this list to test.
        if !handle_offsets.is_empty() && index.name.eq_ignore_ascii_case("PRIMARY") {
            return Err(DriverError::PartitionUniqueKeyNeedAllFields(
                "CLUSTERED INDEX".to_owned(),
            ));
        }
        // Go `ddl/partition.go:703` guards the 8264 refusal with
        // `if !index.Global`: a GLOBAL index is EXEMPT from the covering
        // rule, because its entries span every partition and so can enforce
        // a constraint across them.
        //
        // This node writes only per-partition LOCAL entries. Accepting the
        // exemption would enforce the unique constraint within each
        // partition and let the same value repeat across them, so the index
        // is refused -- but for the reason it is actually refused. Raising
        // 8264 here told the user GLOBAL was not given when it was.
        if index.global {
            let name = &index.name;
            return Err(DriverError::unsupported(format!(
                "a GLOBAL index ({name}) is not supported by this node: it maintains only \
                 per-partition index entries, so a unique constraint spanning the \
                 partitions would not be enforced"
            )));
        }
        return Err(DriverError::PartitionGlobalIndexNeeded(index.name.clone()));
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

/// Builds a [`PartitionSpec`] from the STORED metadata a Go cluster wrote,
/// rather than from a `CREATE TABLE` statement.
///
/// The cluster catalog loader has only `model.PartitionInfo`: the expression
/// as Go's restored TEXT, the columns, and the definitions with their
/// physical ids. `build_table_partitioning` above cannot serve it, because
/// that one starts from the statement's AST.
///
/// The expression is parsed with `tidb_model::generated_expr::parse_expression`,
/// which is Go's own `pkg/util/generatedexpr.ParseExpression` -- the function
/// Go uses for exactly this, reading an expression back out of metadata. It is
/// then bound and type-checked through the same two helpers the AST path uses,
/// so a loaded table routes rows by the identical `Expression` a created one
/// does.
///
/// HASH only, for now, and the refusal is explicit rather than silent: RANGE
/// and LIST keep their bounds in FOLDED form (`PartitionKind::Range`'s
/// `less_than`, `PartitionKind::List`'s `values`), so loading those means
/// parsing and folding every stored bound text per kind -- the work
/// `table_partition_range`/`table_partition_list` do from the AST. HASH has no
/// One stored partition definition, as Go's `model.PartitionDefinition`
/// carries it across a restart: the physical id, the name, and the bounds as
/// SQL TEXT rather than as folded values.
///
/// Go re-parses that text every time it loads the table (`newPartitionExpr`
/// builds a fresh `parser.New()` for exactly this), because the stored form
/// is the user's own `VALUES LESS THAN (...)` / `VALUES IN (...)` and the
/// folding depends on the table's current column types.
#[derive(Clone, Debug, Default)]
pub struct StoredPartitionDefinition {
    /// Go `PartitionDefinition.ID`: the physical table id.
    pub id: i64,
    /// Go `PartitionDefinition.Name`.
    pub name: String,
    /// Go `PartitionDefinition.LessThan`: one bound per partition column,
    /// with the literal `MAXVALUE` kept as text.
    pub less_than: Vec<String>,
    /// Go `PartitionDefinition.InValues`: one tuple per `VALUES IN` entry,
    /// with the literal `DEFAULT` kept as text in the first component.
    pub in_values: Vec<Vec<String>>,
}

/// Rebuild the AST value clause a stored definition was written from, so the
/// DDL-side bound builders can fold it exactly as they folded the original
/// `CREATE TABLE`.
///
/// Go keeps `MAXVALUE` and `DEFAULT` as those literal words in the stored
/// text and matches them case-insensitively (`strings.EqualFold`) before
/// parsing, because neither is an expression.
fn stored_clause(
    definition: &StoredPartitionDefinition,
) -> Result<PartitionDefinitionClause, DriverError> {
    let parse = |text: &String| {
        tidb_model::generated_expr::parse_expression(text)
            .map_err(|error| DriverError::Parse(error.message))
    };
    if !definition.less_than.is_empty() {
        let mut values = Vec::with_capacity(definition.less_than.len());
        for bound in &definition.less_than {
            values.push(if bound.eq_ignore_ascii_case("MAXVALUE") {
                PartitionValue::MaxValue
            } else {
                PartitionValue::Expr(parse(bound)?)
            });
        }
        return Ok(PartitionDefinitionClause::LessThan(values));
    }
    if !definition.in_values.is_empty() {
        let mut values = Vec::with_capacity(definition.in_values.len());
        for tuple in &definition.in_values {
            // Go `buildListPartitionValueMap`: `DEFAULT` is recognised on the
            // FIRST component only, and the rest of the tuple is not read.
            if tuple.first().is_some_and(|first| first.eq_ignore_ascii_case("DEFAULT")) {
                values.push(PartitionValue::Default);
                continue;
            }
            values.push(match tuple.as_slice() {
                [single] => PartitionValue::Expr(parse(single)?),
                many => PartitionValue::Tuple(
                    many.iter().map(parse).collect::<Result<Vec<_>, _>>()?,
                ),
            });
        }
        return Ok(PartitionDefinitionClause::In(values));
    }
    Ok(PartitionDefinitionClause::None)
}

/// The stored definitions as the AST nodes the bound builders read, which is
/// the shape Go reconstructs when it re-parses `LessThan`/`InValues`.
fn stored_definitions_as_ast(
    definitions: &[StoredPartitionDefinition],
) -> Result<Vec<PartitionDefinition>, DriverError> {
    definitions
        .iter()
        .map(|definition| {
            Ok(PartitionDefinition {
                name: definition.name.clone(),
                clause: stored_clause(definition)?,
                options: Vec::new(),
                sub_partitions: Vec::new(),
            })
        })
        .collect()
}

/// Rebuild a table's routing from the partition metadata a Go cluster
/// STORED, which is Go `newPartitionedTable` -> `newPartitionExpr`
/// (`table/tables/partition.go`).
///
/// This is the LOAD side of partitioning, not the DDL side, and the two
/// validate different things. DDL decides whether a `PARTITION BY` clause is
/// legal -- the expression type-checks, the count is under the ceiling, every
/// unique key covers the partition columns. By the time metadata exists those
/// questions are settled, and Go asks none of them again here: it refuses
/// only a definition list that is empty (`table.ErrUnknownPartition`) and
/// otherwise rebuilds. Re-running the DDL checks at load would let this node
/// refuse a table a Go cluster is serving, which is the one outcome a loader
/// must not produce.
///
/// `columns` is Go's `PartitionInfo.Columns`, the explicit column list that
/// KEY and the `COLUMNS` variants carry instead of an expression; Go
/// dispatches on it the same way (`len(partCols) < 1` chooses RANGE over
/// RANGE COLUMNS in `generateRangePartitionExpr`).
///
/// # Errors
///
/// [`DriverError::PartitionNoParts`] for an empty definition list, and a
/// parse or fold error when the stored expression or a stored bound no longer
/// resolves against the table's current columns.
pub fn partition_spec_from_metadata(
    kind: PartitionType,
    expr_text: &str,
    columns: &[String],
    definitions: &[StoredPartitionDefinition],
    names: &[String],
    types: &[FieldType],
    ctx: &crate::StmtContext,
) -> Result<PartitionSpec, DriverError> {
    // Go `newPartitionedTable`: an empty definition list is a table that
    // cannot route at all, so it is refused before the expression is touched.
    if definitions.is_empty() {
        return Err(DriverError::PartitionNoParts("partitions"));
    }
    let physical = definitions
        .iter()
        .map(|definition| PartitionDef {
            id: definition.id,
            name: definition.name.clone(),
        })
        .collect::<Vec<_>>();
    // The COLUMNS forms name their inputs directly. The builders take the
    // parser's qualified paths, whose last component is the column name.
    let column_paths = columns
        .iter()
        .map(|name| vec![name.clone()])
        .collect::<Vec<_>>();
    let column_expr_text = || {
        columns
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(",")
    };
    // The COLUMNS forms route off the stored column list rather than a built
    // expression, so the expression slot holds the same placeholder the DDL
    // path puts there.
    let placeholder = || {
        tidb_expr::expression::Expression::Constant(tidb_expr::expression::Constant::new(
            tidb_datatype::Datum::Null,
            FieldType::new(FieldTypeCode::LongLong),
        ))
    };
    let ast_definitions = || stored_definitions_as_ast(definitions);
    match kind {
        // Go `generateKeyPartitionExpr`: KEY builds no expression at all. The
        // stored column list IS the routing input, hashed by the same
        // `key_partition_index` the DDL-built spec routes through, so the
        // rebuild only has to resolve those names back to the table.
        PartitionType::KEY => {
            for column in columns {
                if !names
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(column))
                {
                    return Err(DriverError::UnknownColumnInClause {
                        column: column.clone(),
                        clause: "partition function".to_owned(),
                    });
                }
            }
            Ok(PartitionSpec {
                kind: PartitionKind::Key,
                expr_text: column_expr_text(),
                expr: placeholder(),
                dependencies: columns.to_vec(),
                definitions: physical,
            })
        }
        // Go `generateRangePartitionExpr` / `generateListPartitionExpr` with
        // `partCols` non-empty: the bounds are folded through the named
        // columns' own types.
        PartitionType::RANGE if !columns.is_empty() => {
            let (dependencies, field_types, less_than) =
                super::table_partition_range::build_range_columns_bounds(
                    &column_paths,
                    &ast_definitions()?,
                    names,
                    types,
                    ctx,
                )?;
            Ok(PartitionSpec {
                kind: PartitionKind::RangeColumns {
                    less_than,
                    field_types,
                },
                expr_text: column_expr_text(),
                expr: placeholder(),
                dependencies,
                definitions: physical,
            })
        }
        PartitionType::LIST if !columns.is_empty() => {
            let (dependencies, kind) = super::table_partition_list::build_list_columns_values(
                &column_paths,
                &ast_definitions()?,
                names,
                types,
                ctx,
            )?;
            Ok(PartitionSpec {
                kind,
                expr_text: column_expr_text(),
                expr: placeholder(),
                dependencies,
                definitions: physical,
            })
        }
        // The expression forms: Go parses the stored `PartitionInfo.Expr` and
        // builds it against the table's own columns
        // (`generateHashPartitionExpr` and the `len(partCols) < 1` arms).
        PartitionType::HASH | PartitionType::RANGE | PartitionType::LIST => {
            let expr = tidb_model::generated_expr::parse_expression(expr_text)
                .map_err(|error| DriverError::Parse(error.message))?;
            let (expr_text, built, dependencies, dependency_offsets) = build_partition_expression(
                &expr,
                names,
                types,
                &ctx.session_zone(),
                ctx.like_default_escape(),
            )?;
            let kind = match kind {
                PartitionType::RANGE => {
                    let (less_than, unsigned) = super::table_partition_range::build_range_bounds(
                        &expr,
                        &ast_definitions()?,
                        names,
                        types,
                        &dependency_offsets,
                        ctx,
                    )?;
                    PartitionKind::Range {
                        less_than,
                        unsigned,
                    }
                }
                PartitionType::LIST => super::table_partition_list::build_list_values(
                    &expr,
                    &ast_definitions()?,
                    names,
                    types,
                    &dependency_offsets,
                    ctx,
                )?,
                _ => PartitionKind::Hash,
            };
            Ok(PartitionSpec {
                kind,
                expr_text,
                expr: built,
                dependencies,
                definitions: physical,
            })
        }
        other => {
            let name = other.sql();
            Err(DriverError::unsupported(format!(
                "loading a PARTITION BY {name} table is not supported by this node"
            )))
        }
    }
}

/// A `PARTITION BY` clause in the STORED form Go persists, which is Go
/// `buildTablePartitionInfo` + `buildPartitionDefinitionsInfo`
/// (`ddl/partition.go`) reduced to the fields `model.PartitionInfo` carries.
///
/// Physical ids are NOT here. Go allocates them when the DDL job is
/// submitted, one per definition after the table's own
/// (`ddl/jobsubmit/submit.go` `assignIDsForTable`), so the builder that
/// persists this metadata supplies them.
#[derive(Clone, Debug)]
pub struct StoredPartitionMetadata {
    /// Go `PartitionInfo.Type`.
    pub kind: PartitionType,
    /// Go `PartitionInfo.Enable`. Metadata written with this false is not
    /// partitioning -- `GetPartitionInfo` returns nil for it -- so a method
    /// Go treats as unsupported is stored disabled rather than refused.
    pub enable: bool,
    /// Go `PartitionInfo.Num`: the written `PARTITIONS n`.
    pub num: u64,
    /// Go `PartitionInfo.Expr`, restored under Go's own flags. Empty for the
    /// COLUMNS and KEY forms, which name their inputs instead.
    pub expr: String,
    /// Go `PartitionInfo.Columns`.
    pub columns: Vec<String>,
    /// Go `PartitionInfo.IsEmptyColumns`: set when `PARTITION BY KEY` named
    /// no columns and Go filled them in from the primary key.
    pub is_empty_columns: bool,
    /// One entry per partition, in written order, with `id` left at zero for
    /// the id allocator to fill.
    pub definitions: Vec<StoredPartitionDefinition>,
}

/// Builds the STORED partition metadata for a `CREATE TABLE`, alongside the
/// routing spec the same clause produces.
///
/// Both come from one walk on purpose. Go derives them from one another --
/// the stored `LessThan` text under RANGE COLUMNS is the FOLDED datum
/// rendered back out (`generatePartValuesWithTp`), not the source text -- so
/// splitting the walk would let the persisted bounds and the bounds this node
/// routes by drift apart while both looked right in isolation.
///
/// `allocate_id` fills the routing spec's physical ids; the stored form
/// leaves them zero, because the two are filled by different tiers.
///
/// # Errors
///
/// Whatever [`build_table_partitioning`] raises for the clause.
pub fn build_partition_metadata(
    create: &CreateTableStmt,
    names: &[String],
    types: &[FieldType],
    indexes: &[KvIndex],
    handle_offsets: &[usize],
    allocate_id: &mut dyn FnMut() -> i64,
    ctx: &crate::StmtContext,
) -> Result<Option<(StoredPartitionMetadata, PartitionSpec)>, DriverError> {
    let Some(spec) = build_table_partitioning(
        create,
        names,
        types,
        indexes,
        handle_offsets,
        allocate_id,
        ctx,
    )?
    else {
        return Ok(None);
    };
    let partitioning = create
        .partitioning
        .as_ref()
        .expect("a spec was built, so the clause is there");
    let method = &partitioning.method;
    // Go dispatches the stored shape on whether an EXPRESSION was written:
    // `pi.Expr` for one, `pi.Columns` for a written column list.
    let (expr, columns, is_empty_columns) = if method.expr.is_some() {
        (spec.expr_text.clone(), Vec::new(), false)
    } else {
        let written = method
            .columns
            .iter()
            .filter_map(|path| path.last().cloned())
            .collect::<Vec<_>>();
        // Go `buildTablePartitionInfo`: `PARTITION BY KEY` with no column
        // list stores the PRIMARY KEY's columns and records that it filled
        // them in, so `SHOW CREATE TABLE` can print the clause as written.
        // The routing spec resolved the same columns, which is why they are
        // taken from it rather than resolved a second time.
        if written.is_empty() {
            (String::new(), spec.dependencies.clone(), true)
        } else {
            (String::new(), written, false)
        }
    };
    let definitions = stored_definitions_for(partitioning, &spec, ctx)?;
    Ok(Some((
        StoredPartitionMetadata {
            kind: method.kind,
            // Every method this tier builds a spec for is one Go enables;
            // the ones Go stores disabled it refuses outright, above.
            enable: true,
            num: method.count,
            expr,
            columns,
            is_empty_columns,
            definitions,
        },
        spec,
    )))
}

/// The stored `LessThan`/`InValues` TEXT for each definition.
///
/// Go renders these from the FOLDED values under the COLUMNS forms
/// (`generatePartValuesWithTp`) and from the written expression under the
/// expression forms (`expr.Format`). Both are reproduced from the routing
/// spec, which holds the folded values, and the clause, which holds the
/// written ones.
fn stored_definitions_for(
    partitioning: &tidb_ast::TablePartitioning,
    spec: &PartitionSpec,
    ctx: &crate::StmtContext,
) -> Result<Vec<StoredPartitionDefinition>, DriverError> {
    let mut stored = Vec::with_capacity(spec.definitions.len());
    for (ordinal, definition) in spec.definitions.iter().enumerate() {
        let written = partitioning.definitions.get(ordinal);
        let mut entry = StoredPartitionDefinition {
            // Filled by the tier that publishes this metadata; Go allocates
            // it at job-submission time.
            id: 0,
            name: definition.name.clone(),
            less_than: Vec::new(),
            in_values: Vec::new(),
        };
        match &spec.kind {
            // HASH and KEY definitions carry a name and nothing else.
            PartitionKind::Hash | PartitionKind::Key => {}
            PartitionKind::Range { less_than, unsigned } => {
                entry.less_than = vec![match less_than.get(ordinal) {
                    Some(RangeBound::MaxValue) | None => PARTITION_MAX_VALUE.to_owned(),
                    Some(RangeBound::Value(value)) if *unsigned => {
                        format!("{}", *value as u64)
                    }
                    Some(RangeBound::Value(value)) => format!("{value}"),
                }];
            }
            PartitionKind::RangeColumns {
                less_than,
                field_types,
            } => {
                let bounds = less_than.get(ordinal).map_or(&[][..], Vec::as_slice);
                for (position, bound) in bounds.iter().enumerate() {
                    entry.less_than.push(match bound {
                        RangeColumnBound::MaxValue => PARTITION_MAX_VALUE.to_owned(),
                        RangeColumnBound::Value(datum) => {
                            stored_value_text(datum, field_types.get(position))?
                        }
                    });
                }
            }
            PartitionKind::List { .. } | PartitionKind::ListColumns { .. } => {
                entry.in_values = stored_in_values(written, spec, ctx)?;
            }
        }
        stored.push(entry);
    }
    Ok(stored)
}

/// Go's `partitionMaxValue`, stored as that literal word and matched back
/// with `strings.EqualFold`.
const PARTITION_MAX_VALUE: &str = "MAXVALUE";

/// One folded value in the text form Go stores it as, which is Go
/// `generatePartValuesWithTp`.
///
/// The dispatch is on the COLUMN's type, not the datum's, exactly as Go's
/// is: a value has already been converted to its column's type when it was
/// folded, and the one case that cannot be read off the datum -- a BINARY
/// column, whose bytes need not be valid UTF-8 -- is a property of the
/// column alone.
fn stored_value_text(datum: &Datum, field_type: Option<&FieldType>) -> Result<String, DriverError> {
    if matches!(datum, Datum::Null) {
        return Ok("NULL".to_owned());
    }
    // Go stores a BINARY value as a hex literal rather than as quoted text,
    // because the content would otherwise be lost when the metadata is
    // marshalled: `fmt.Sprintf("_binary 0x%x", s)`, two digits per byte.
    if field_type.is_some_and(|field_type| field_type.charset() == tidb_datatype::Charset::Binary) {
        let bytes = match datum {
            Datum::Bytes(value) => value.as_slice(),
            Datum::String(value) => value.bytes(),
            _ => return Err(DriverError::PartitionColumnValueWrongType),
        };
        if !bytes.is_empty() {
            let mut hex = String::with_capacity(bytes.len() * 2 + 10);
            hex.push_str("_binary 0x");
            for byte in bytes {
                hex.push_str(&format!("{byte:02x}"));
            }
            return Ok(hex);
        }
    }
    // Every other arm of Go's switch -- bare digits for an integer, single
    // quotes for a string or a temporal -- is what a restored value
    // expression already is, including `WrapInSingleQuotes`' ordered
    // backslash-then-quote escaping.
    let rendered = datum
        .restore_value_expr()
        .map_err(|_| DriverError::PartitionColumnValueWrongType)?;
    Ok(String::from_utf8_lossy(&rendered).into_owned())
}

/// The stored `InValues` tuples for one LIST definition.
fn stored_in_values(
    written: Option<&PartitionDefinition>,
    spec: &PartitionSpec,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<String>>, DriverError> {
    let Some(written) = written else {
        return Ok(Vec::new());
    };
    let values = match &written.clause {
        PartitionDefinitionClause::In(values) => values.as_slice(),
        // This parser NORMALISES `VALUES IN (DEFAULT)` into the bare
        // `Default` clause, where Go keeps it as a one-element `ClauseIn`
        // holding a `DefaultExpr`. Go's stored form is what has to match, and
        // that is `InValues = [["DEFAULT"]]` -- the word its own list entry,
        // which `buildListPartitionValueMap` finds by `EqualFold` on the
        // first component. Reading only the `In` spelling stored the
        // catch-all partition with NO values at all, and a reload then saw a
        // LIST partition that listed nothing.
        PartitionDefinitionClause::Default => {
            return Ok(vec![vec!["DEFAULT".to_owned()]]);
        }
        _ => return Ok(Vec::new()),
    };
    let field_types = match &spec.kind {
        PartitionKind::ListColumns { field_types, .. } => field_types.as_slice(),
        _ => &[],
    };
    let mut tuples = Vec::with_capacity(values.len());
    for value in values {
        match value {
            // Go stores the catch-all as the literal word `DEFAULT`, which
            // `buildListPartitionValueMap` matches with `strings.EqualFold`.
            PartitionValue::Default => tuples.push(vec!["DEFAULT".to_owned()]),
            PartitionValue::MaxValue => {
                return Err(DriverError::PartitionColumnValueWrongType)
            }
            PartitionValue::Expr(expr) => {
                tuples.push(vec![stored_list_component(expr, field_types.first(), ctx)?]);
            }
            PartitionValue::Tuple(components) => {
                let mut tuple = Vec::with_capacity(components.len());
                for (position, component) in components.iter().enumerate() {
                    tuple.push(stored_list_component(component, field_types.get(position), ctx)?);
                }
                tuples.push(tuple);
            }
        }
    }
    Ok(tuples)
}

/// One written LIST value as stored text.
///
/// Under LIST COLUMNS Go stores the value FOLDED through the column's type;
/// under plain LIST it stores the written expression. Both are re-parsed on
/// load, so the difference is only in which normalization the text has
/// already had.
fn stored_list_component(
    expr: &Expr,
    field_type: Option<&FieldType>,
    ctx: &crate::StmtContext,
) -> Result<String, DriverError> {
    match field_type {
        Some(field_type) => {
            let datum = super::table_partition_list::fold_column_value(expr, field_type, ctx)?;
            stored_value_text(&datum, Some(field_type))
        }
        None => Ok(expr.restore_with_flags(partition_restore_flags())),
    }
}

#[cfg(test)]
mod round_trip_tests {
    use super::*;

    /// The two halves of partitioning must agree. `build_partition_metadata`
    /// renders a clause into the TEXT Go stores; `partition_spec_from_metadata`
    /// reads that text back. A rendering that loses or reshapes a bound would
    /// leave both halves looking right on their own while the table routed
    /// rows to one partition and read them from another.
    ///
    /// So every case here is run down BOTH paths and the results compared,
    /// rather than each being checked against a written-out expectation.
    /// A routing method rendered so that EQUAL specs render equally.
    ///
    /// `PartitionKind::ListColumns` carries a `HashMap` of encoded keys, and
    /// a `HashMap`'s `Debug` order is not stable between two maps holding the
    /// same entries. Comparing the derived `Debug` text directly made this
    /// check pass or fail on hash order rather than on the bounds, which is
    /// exactly the sort of test that reports green while the thing it guards
    /// is broken.
    fn comparable(kind: &PartitionKind) -> String {
        match kind {
            PartitionKind::ListColumns {
                values,
                keys,
                default_partition,
                field_types,
            } => {
                let mut keys = keys
                    .iter()
                    .map(|(key, ordinal)| format!("{key:?}=>{ordinal}"))
                    .collect::<Vec<_>>();
                keys.sort_unstable();
                format!("ListColumns {values:?} {keys:?} {default_partition:?} {field_types:?}")
            }
            other => format!("{other:?}"),
        }
    }

    fn routes_the_same_way(sql: &str) {
        let statement = tidb_parser::parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let tidb_ast::Stmt::Ddl(ddl) = statement else {
            panic!("{sql} is not DDL");
        };
        let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
            panic!("{sql} is not CREATE TABLE");
        };
        let names = create
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let types = create
            .columns
            .iter()
            .map(|column| {
                crate::ddl::column_field_type::build_field_type(
                    &column.name,
                    &column.ty,
                    "utf8mb4",
                    "utf8mb4_bin",
                )
                .unwrap_or_else(|error| panic!("{}: {error}", column.name))
            })
            .collect::<Vec<_>>();
        let ctx = crate::StmtContext::for_query();
        let mut next = 100;
        let built = build_partition_metadata(
            create,
            &names,
            &types,
            &[],
            &[],
            &mut || {
                next += 1;
                next
            },
            &ctx,
        )
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .unwrap_or_else(|| panic!("{sql} declares partitioning"));
        let (stored, direct) = built;
        let mut definitions = stored.definitions.clone();
        for (ordinal, definition) in definitions.iter_mut().enumerate() {
            definition.id = direct.definitions[ordinal].id;
        }
        let reloaded = partition_spec_from_metadata(
            stored.kind,
            &stored.expr,
            &stored.columns,
            &definitions,
            &names,
            &types,
            &ctx,
        )
        .unwrap_or_else(|error| {
            panic!(
                "{sql} reloads: {error:?}\n  kind={:?} expr={:?} columns={:?}\n  definitions={:?}",
                stored.kind, stored.expr, stored.columns, definitions
            )
        });
        assert_eq!(
            comparable(&direct.kind),
            comparable(&reloaded.kind),
            "{sql}: the stored text did not fold back to the bounds it came from"
        );
        assert_eq!(
            direct.expr_text, reloaded.expr_text,
            "{sql}: the reloaded spec prints a different clause"
        );
        assert_eq!(
            direct.dependencies, reloaded.dependencies,
            "{sql}: the reloaded spec reads different columns"
        );
        assert_eq!(
            direct.physical_ids(),
            reloaded.physical_ids(),
            "{sql}: the reloaded spec routes to different physical tables"
        );
    }

    /// Go EXEMPTS a GLOBAL unique index from the covering rule: at
    /// `ddl/partition.go:703` the 8264 refusal is guarded by `if
    /// !index.Global`, because a global index spans every partition and so
    /// can enforce uniqueness across them.
    ///
    /// This node maintains only per-partition local index entries, so it must
    /// not accept one -- accepting it would enforce the unique constraint
    /// WITHIN each partition and let the same value repeat across them. But
    /// the refusal has to name that, rather than telling the user GLOBAL was
    /// not given when it was.
    #[test]
    fn a_global_unique_index_is_refused_for_the_reason_it_is_refused() {
        let sql = "CREATE TABLE t (id BIGINT, v BIGINT, UNIQUE KEY uv (v) GLOBAL) \
                   PARTITION BY HASH (id) PARTITIONS 2";
        let statement = tidb_parser::parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let tidb_ast::Stmt::Ddl(ddl) = statement else {
            panic!("not DDL");
        };
        let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
            panic!("not CREATE TABLE");
        };
        let names = vec!["id".to_owned(), "v".to_owned()];
        let types = vec![
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
        ];
        let indexes = vec![crate::kv_table::KvIndex {
            id: 1,
            name: "uv".to_owned(),
            comment: String::new(),
            unique: true,
            // Only `v`: the partition column `id` is NOT covered, which is
            // what brings the rule into play at all.
            column_offsets: vec![1],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
            visible: true,
            global: true,
        }];
        let error = build_table_partitioning(
            create,
            &names,
            &types,
            &indexes,
            &[],
            &mut || 1,
            &crate::StmtContext::for_query(),
        )
        .expect_err("this node cannot serve a GLOBAL index");
        let message = error.to_mysql_error().message;
        assert!(
            !message.contains("GLOBAL is not given"),
            "GLOBAL WAS given; the refusal must not claim otherwise: {message}"
        );
        assert!(
            message.contains("GLOBAL"),
            "the refusal must name the global index as the reason: {message}"
        );
    }

    #[test]
    fn every_method_survives_the_store_and_load_round_trip() {
        for sql in [
            "CREATE TABLE t (id BIGINT, v BIGINT) PARTITION BY HASH (id) PARTITIONS 4",
            "CREATE TABLE t (id BIGINT, v BIGINT) PARTITION BY KEY (id) PARTITIONS 2",
            "CREATE TABLE t (id BIGINT, v BIGINT) PARTITION BY RANGE (id) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
            "CREATE TABLE t (id BIGINT, v BIGINT) PARTITION BY LIST (id) \
             (PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3, 4))",
            "CREATE TABLE t (id BIGINT, v VARCHAR(16)) PARTITION BY RANGE COLUMNS (v) \
             (PARTITION p0 VALUES LESS THAN ('m'), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
            "CREATE TABLE t (id BIGINT, v VARCHAR(16)) PARTITION BY LIST COLUMNS (v) \
             (PARTITION p0 VALUES IN ('a', 'b'), PARTITION pd VALUES IN (DEFAULT))",
        ] {
            routes_the_same_way(sql);
        }
    }

    /// A value whose stored text needs ESCAPING is the case a naive renderer
    /// gets wrong: Go doubles backslashes and then quotes
    /// (`driver.WrapInSingleQuotes`), and the text is re-parsed on every load.
    #[test]
    fn a_bound_needing_escaping_survives_the_round_trip() {
        routes_the_same_way(
            "CREATE TABLE t (id BIGINT, v VARCHAR(16)) PARTITION BY LIST COLUMNS (v) \
             (PARTITION p0 VALUES IN ('it''s'), PARTITION p1 VALUES IN ('a\\\\b'))",
        );
    }
}
