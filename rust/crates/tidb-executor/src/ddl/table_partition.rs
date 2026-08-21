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
/// [`build_table_partitioning_inner`], with the STORED bound text mirrored
/// onto every definition.
///
/// Go keeps `LessThan`/`InValues` on the definition it routes by, and
/// `AppendPartitionDefs` prints from those strings. Both of this tier's paths
/// therefore need them: the loader is handed them, and a CREATE renders them
/// from the folded values here -- otherwise `SHOW CREATE TABLE` on a freshly
/// created table printed a LIST partition as a bare `DEFAULT`, having no
/// values to show.
pub fn build_table_partitioning(
    create: &CreateTableStmt,
    names: &[String],
    types: &[FieldType],
    indexes: &[KvIndex],
    handle_offsets: &[usize],
    allocate_id: &mut dyn FnMut() -> i64,
    ctx: &crate::StmtContext,
) -> Result<Option<PartitionSpec>, DriverError> {
    let Some(mut spec) = build_table_partitioning_inner(
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
    if let Some(partitioning) = create.partitioning.as_ref() {
        let stored = stored_definitions_for(partitioning, &spec, ctx)?;
        for (routing, stored) in spec.definitions.iter_mut().zip(&stored) {
            routing.less_than.clone_from(&stored.less_than);
            routing.in_values.clone_from(&stored.in_values);
        }
    }
    Ok(Some(spec))
}

fn build_table_partitioning_inner(
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
            build_key_partition_columns(&method.columns, names, types, handle_offsets, indexes)?;
        let definitions = build_hash_partition_definitions(create, method.count, allocate_id, ctx)?;
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
            is_empty_columns: method.columns.is_empty(),
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
        let definitions = build_named_partition_definitions(create, allocate_id, ctx)?;
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
            is_empty_columns: false,
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
                PartitionBuildMode::Create,
            )?;
        let definitions = build_named_partition_definitions(create, allocate_id, ctx)?;
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
            is_empty_columns: false,
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
                PartitionBuildMode::Create,
            )?;
            let definitions = build_named_partition_definitions(create, allocate_id, ctx)?;
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
            let definitions = build_named_partition_definitions(create, allocate_id, ctx)?;
            (kind, definitions)
        }
        _ => (
            PartitionKind::Hash,
            build_hash_partition_definitions(create, method.count, allocate_id, ctx)?,
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
        is_empty_columns: false,
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
    indexes: &[KvIndex],
) -> Result<Vec<String>, DriverError> {
    let selected: Vec<String> = if columns.is_empty() {
        if handle_offsets.is_empty() {
            // Go `buildTablePartitionInfo` (`ddl/partition.go:631`): with no
            // written column list, `PARTITION BY KEY` takes the PRIMARY KEY
            // when it is the handle, and OTHERWISE what
            // `TableInfo.GetPrimaryKey` returns -- the explicit PRIMARY
            // index, clustered or NOT, and failing that the first unique key
            // whose columns are all NOT NULL.
            //
            // Reading only the clustered handle left `dependencies` EMPTY for
            // a table with a nonclustered primary key or a NOT NULL unique
            // key, and `key_partition_index` then hashes the empty byte
            // stream: crc32 of nothing is 0, so every row routed to partition
            // 0 and `PARTITION (p1)` came back empty.
            let resolved = primary_or_implicit_key_columns(indexes, names, types);
            // Go `getPartitionColSlices` (`ddl/partition.go:781`) ends with a
            // bare `errors.Errorf`, which reaches the client as 1105.
            //
            // It is reached only when the table HAS keys but none can serve:
            // `checkPartitioningKeysConstraints` returns early for
            // `len(Indices) == 0 && !PKIsHandle` (partition.go:4710), so a
            // bare heap table keeps its empty list and hashes the empty
            // stream. That is why the emptiness alone is not the error.
            if resolved.is_empty() && !indexes.is_empty() {
                return Err(DriverError::PartitionMetadataIncomplete);
            }
            resolved
        } else {
            handle_offsets
                .iter()
                .filter_map(|offset| names.get(*offset).cloned())
                .collect()
        }
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

/// Go `TableInfo.GetPrimaryKey` (`meta/model/table.go:504`): the columns
/// `PARTITION BY KEY ()` routes by when the table has no clustered handle.
///
/// The EXPLICIT primary key wins as soon as it is met, whether or not it is
/// clustered. Otherwise the first UNIQUE key whose every column carries NOT
/// NULL becomes the implicit primary key. A table with neither keeps an empty
/// list, which is a legal heap table: Go hashes the empty stream for it too.
fn primary_or_implicit_key_columns(
    indexes: &[KvIndex],
    names: &[String],
    types: &[FieldType],
) -> Vec<String> {
    let columns_of = |index: &KvIndex| {
        index
            .column_offsets
            .iter()
            .filter_map(|offset| names.get(*offset).cloned())
            .collect::<Vec<_>>()
    };
    let mut implicit: Option<&KvIndex> = None;
    for index in indexes {
        if index.name.eq_ignore_ascii_case("PRIMARY") {
            return columns_of(index);
        }
        // Go guards this: an index with no columns is never a primary key.
        if index.column_offsets.is_empty() {
            continue;
        }
        if implicit.is_none() && index.unique {
            let all_not_null = index.column_offsets.iter().all(|offset| {
                types.get(*offset).is_some_and(|field_type| {
                    field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
                })
            });
            if all_not_null {
                implicit = Some(index);
            }
        }
    }
    implicit.map(columns_of).unwrap_or_default()
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
    ctx: &crate::StmtContext,
) -> Result<Vec<PartitionDef>, DriverError> {
    let Some(partitioning) = create.partitioning.as_ref() else {
        return Ok(Vec::new());
    };
    partitioning
        .definitions
        .iter()
        .map(|definition| {
            check_too_long_partition_name(&definition.name)?;
            Ok(PartitionDef {
                id: allocate_id(),
                name: definition.name.clone(),
                // Filled by `build_partition_metadata`, which renders the
                // stored text from the folded values exactly as Go does.
                less_than: Vec::new(),
                in_values: Vec::new(),
                comment: partition_definition_comment(definition, ctx)?,
            })
        })
        .collect()
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
    check_partition_expression_allowed(expr, names, types)?;
    // Go `checkPartitionFuncValid` (`ddl/partition.go:1855`): after the walk,
    // an expression that extracted NO column is 1486 however well-formed it
    // is. `PARTITION BY HASH (1+1)` is constant, so every row would route to
    // the same partition -- which is what the error's wording describes.
    if !partition_expr_reads_a_column(expr) {
        return Err(DriverError::PartitionWrongExprInFunc);
    }
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
fn check_partition_expression_allowed(
    expr: &Expr,
    names: &[String],
    types: &[FieldType],
) -> Result<(), DriverError> {
    // Go runs `checkPartitionExprArgs` as the FIRST processor on every node
    // of a PRE-ORDER walk (`partition.go:1849`), so the parent's argument
    // rule fires before its children are looked at: `unix_timestamp(date(dt))`
    // is 1486 from the outer call and never reaches `date` to report 1564
    // (`ddl/tests/partition/db_partition_test.go:315`).
    check_partition_expr_args(expr, names, types)?;
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
        Expr::Paren(inner) => check_partition_expression_allowed(inner, names, types),
        Expr::Extract { value, .. } => check_partition_expression_allowed(value, names, types),
        // Go `AllowedPartition4UnaryOpMap`.
        Expr::Unary(tidb_ast::UnaryOp::Plus | tidb_ast::UnaryOp::Minus, inner) => {
            check_partition_expression_allowed(inner, names, types)
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
            check_partition_expression_allowed(left, names, types)?;
            check_partition_expression_allowed(right, names, types)
        }
        Expr::Func { name, args, .. }
            if ALLOWED_PARTITION_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str()) =>
        {
            for arg in args {
                check_partition_expression_allowed(arg, names, types)?;
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
    // ONE rule, not two: the inference below is Go's, and asking it is Go's
    // `e.GetType(ctx).EvalType() == types.ETInt` (`ddl/partition.go:1895`).
    //
    // This used to carry its own arms for columns and arithmetic, and they
    // disagreed with the inference: the binary arm required BOTH operands to
    // be integer COLUMNS, so `MOD(dt, 2)` over an fsp-0 `datetime` was
    // refused where Go reads both operands in numeric context and calls it
    // integer arithmetic.
    partition_expr_result_type(expr, names, types)
        .is_some_and(|field_type| field_type.eval_type() == tidb_datatype::EvalType::Int)
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
    ctx: &crate::StmtContext,
) -> Result<Vec<PartitionDef>, DriverError> {
    // Go `buildHashPartitionDefinitions` checks the cap FIRST
    // (`partition.go:1514`), before a single definition is materialised. The
    // order is observable: a HASH statement that is BOTH over-cap and has a
    // duplicate name gets 1499, where the equivalent RANGE statement gets
    // 1517 -- because RANGE's cap check runs after its name check
    // (`partition.go:5276` then `:5279`).
    //
    // The comparison is STRICTLY greater, so exactly 8192 partitions are
    // legal and 8193 are not.
    if count > MAX_PARTITIONS {
        return Err(DriverError::PartitionTooMany);
    }
    // A HASH partition definition carries no VALUES -- `tidb_parser`'s
    // `validate_definition` already rejects them -- so the written
    // definitions contribute their names and their comments.
    let written = create
        .partitioning
        .as_ref()
        .map_or(&[][..], |partitioning| &partitioning.definitions);
    debug_assert!(written
        .iter()
        .all(|definition| matches!(definition.clause, PartitionDefinitionClause::None)));
    let mut definitions = Vec::with_capacity(count as usize);
    for index in 0..count {
        let written_definition = written.get(index as usize);
        let name = written_definition
            .map_or_else(|| format!("p{index}"), |written| written.name.clone());
        let comment = match written_definition {
            Some(definition) => partition_definition_comment(definition, ctx)?,
            None => String::new(),
        };
        definitions.push(PartitionDef {
            id: allocate_id(),
            name,
            // HASH and KEY definitions carry no VALUES clause at all.
            less_than: Vec::new(),
            in_values: Vec::new(),
            comment,
        });
    }
    Ok(definitions)
}

/// Go `MaxCommentLength` for a partition comment (`ddl/index.go:103`); the
/// table-comment limit doubles it, a partition's does not
/// (`ddl/executor.go:5612-5619`).
const MAX_PARTITION_COMMENT_LENGTH: usize = 1024;

/// Go `def.Comment()` plus `validateCommentLength(... ErrTooLongTablePartitionComment)`
/// (`ddl/partition.go:1663`).
///
/// Over the limit, STRICT mode is an error and permissive mode truncates and
/// warns (`ddl/executor.go:5622-5630`). Dropping the comment entirely, as
/// this did, loses it from `SHOW CREATE TABLE` AND changes the shape of the
/// clause: `AppendPartitionInfo` prints the definition list rather than
/// `PARTITIONS n` as soon as one partition carries a comment.
fn partition_definition_comment(
    definition: &PartitionDefinition,
    ctx: &crate::StmtContext,
) -> Result<String, DriverError> {
    let Some(comment) = definition.options.iter().find_map(|option| match option {
        tidb_ast::TableOption::Comment(comment) => Some(comment.clone()),
        _ => None,
    }) else {
        return Ok(String::new());
    };
    if comment.len() > MAX_PARTITION_COMMENT_LENGTH {
        if ctx.strict() {
            return Err(DriverError::PartitionCommentTooLong {
                name: definition.name.clone(),
                limit: MAX_PARTITION_COMMENT_LENGTH,
            });
        }
        // Go truncates by BYTES, having already appended the warning.
        return Ok(comment[..MAX_PARTITION_COMMENT_LENGTH].to_owned());
    }
    Ok(comment)
}

/// Go `hexIfNonPrint` (`ddl/partition.go:5079`): how a stored bound or
/// `VALUES IN` value is spelled back out by `SHOW CREATE TABLE`.
///
/// Printable text is returned untouched. Otherwise the six escapes MySQL
/// interprets are substituted (`\0 \b \t \n \r \Z`), and if anything
/// unprintable survives that, the whole value becomes a `0x...` hex literal
/// -- which is the only form that round-trips bytes MySQL cannot quote.
///
/// Go's inner `break` leaves the SWITCH rather than the loop, so a rune that
/// is not printable and not one of the six sets the flag and is DROPPED from
/// the interpreted attempt while iteration continues. That attempt is then
/// discarded in favour of the hex form, so the dropped runes never reach the
/// output -- but the control flow is reproduced here so the two agree if that
/// ever stops being true.
///
/// `strconv.IsPrint` is Go's own Unicode table. This uses the ASCII-exact
/// rule -- control characters and non-ASCII whitespace are not printable --
/// which agrees with Go on every byte a bound can hold in practice; a fully
/// faithful port needs Go's table and is noted rather than pretended.
#[must_use]
pub fn hex_if_non_print(value: &str) -> String {
    fn go_is_print(character: char) -> bool {
        character == ' ' || !(character.is_control() || character.is_whitespace())
    }
    if value.chars().all(go_is_print) {
        return value.to_owned();
    }
    let mut interpreted = String::with_capacity(value.len());
    let mut printable = true;
    for character in value.chars() {
        match character {
            '\0' => interpreted.push_str("\\0"),
            '\u{7}' => interpreted.push_str("\\b"),
            '\t' => interpreted.push_str("\\t"),
            '\n' => interpreted.push_str("\\n"),
            '\r' => interpreted.push_str("\\r"),
            '\u{1a}' => interpreted.push_str("\\Z"),
            other => {
                if go_is_print(other) {
                    interpreted.push(other);
                } else {
                    printable = false;
                }
            }
        }
    }
    if printable {
        return interpreted;
    }
    // Go unwraps the single quotes first, so the hex covers the VALUE and not
    // the quoting around it.
    let unwrapped = value
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
        .unwrap_or(value);
    // Go `hex.EncodeToString` emits LOWERCASE digits, so the literal reads
    // `0x7f` and not `0x7F`. Captured against a recorded TiDB catalog read.
    let mut hex = String::from("0x");
    for byte in unwrapped.as_bytes() {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// Go's ` COMMENT '<c>'` tail on a partition definition
/// (`ddl/partition.go:5237`), or nothing when the partition carries none.
#[must_use]
pub fn partition_comment_text(comment: &str) -> String {
    if comment.is_empty() {
        return String::new();
    }
    // Go `format.OutputFormat`: a single quote doubles, a backslash escapes.
    let escaped = comment.replace('\\', "\\\\").replace('\'', "''");
    format!(" COMMENT '{escaped}'")
}

/// Go `newReturnFieldTypeForBaseBuiltinFunc` (`expression/builtin.go:149`):
/// the skeleton result type every builtin starts from, chosen by its declared
/// return `EvalType` alone.
fn base_return_field_type(eval: tidb_datatype::EvalType) -> FieldType {
    use tidb_datatype::EvalType;
    let (code, flen, decimal) = match eval {
        EvalType::Int => (FieldTypeCode::LongLong, 20, 0),
        EvalType::Real => (FieldTypeCode::Double, 23, UNSPECIFIED_DECIMAL),
        EvalType::Decimal => (FieldTypeCode::NewDecimal, 11, 0),
        EvalType::Datetime | EvalType::Timestamp => (FieldTypeCode::Datetime, 26, 6),
        EvalType::Duration => (FieldTypeCode::Duration, 17, 6),
        _ => (FieldTypeCode::VarString, UNSPECIFIED_DECIMAL, UNSPECIFIED_DECIMAL),
    };
    let mut field_type = FieldType::new(code);
    field_type.set_flen(flen);
    field_type.set_decimal(decimal);
    field_type
}

/// Go `types.UnspecifiedLength`.
const UNSPECIFIED_DECIMAL: i64 = -1;

/// Go `numericContextResultType` (`expression/builtin_arithmetic.go:80`),
/// which is how `MOD` and the arithmetic operators read an argument.
///
/// A TEMPORAL argument is an INTEGER in numeric context when it carries no
/// fractional seconds -- `MOD(dt, 2)` over a `datetime` is integer arithmetic
/// -- and a decimal when it does. That is why `MOD` cannot share a predicate
/// with `ABS` and `FLOOR`, which reject the same column.
fn numeric_context_result_type(field_type: &FieldType) -> tidb_datatype::EvalType {
    use tidb_datatype::EvalType;
    if matches!(
        field_type.code(),
        FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::Timestamp
            | FieldTypeCode::Duration
    ) {
        return if field_type.decimal() > 0 {
            EvalType::Decimal
        } else {
            EvalType::Int
        };
    }
    // Go also folds a constant binary literal to Int here; a BIT column takes
    // the same branch by type.
    if field_type.code() == FieldTypeCode::Bit {
        return EvalType::Int;
    }
    match field_type.eval_type() {
        EvalType::Decimal => EvalType::Decimal,
        EvalType::Int => EvalType::Int,
        _ => EvalType::Real,
    }
}

/// Go's per-builtin `getFunction` result-type inference, for the names on
/// `AllowedPartitionFuncMap` (`expression/function_traits.go:174`) -- the only
/// ones a partition expression can hold.
///
/// This is inference, not a table of verdicts: three of the four inferring
/// arms read their argument's `flen`/`decimal`, and tabulating their answers
/// got `FLOOR` over a DECIMAL wrong (Go accepts it whenever the integer part
/// fits in 18 digits) and `MOD` over an fsp-0 temporal wrong (Go treats it as
/// integer arithmetic).
///
/// It belongs in `tidb-expr` beside the builtins, so that
/// `Expression::static_type` can answer for a scalar function and this check
/// collapses to Go's one-liner. It is seeded here because that inference does
/// not exist yet.
fn partition_function_result_type(name: &str, args: &[FieldType]) -> FieldType {
    use tidb_datatype::EvalType;
    let arg = args.first();
    match name.to_ascii_lowercase().as_str() {
        // Go `fromDaysFunctionClass` (`builtin_time.go:894`) declares ETDatetime
        // and then `setDecimalAndFlenForDate` makes it a DATE. Its ARGUMENT is
        // cast to an integer, which is what makes it look integral; the RESULT
        // never is.
        "from_days" => {
            let mut field_type = FieldType::new(FieldTypeCode::Date);
            field_type.set_flen(10);
            field_type.set_decimal(0);
            field_type
        }
        // Go `unixTimestampFunctionClass` (`builtin_time.go:4363`): the scale
        // comes from the ARGUMENT, `UnspecifiedLength` clamps to 6 (NOT to 0),
        // and only an exact zero stays an integer.
        "unix_timestamp" => {
            let Some(arg) = arg else {
                let mut field_type = base_return_field_type(EvalType::Int);
                field_type.set_flen(11);
                return field_type;
            };
            let mut decimal = if arg.eval_type() == EvalType::String {
                UNSPECIFIED_DECIMAL
            } else {
                arg.decimal()
            };
            if decimal > 6 || decimal == UNSPECIFIED_DECIMAL {
                decimal = 6;
            }
            if decimal == 0 {
                let mut field_type = base_return_field_type(EvalType::Int);
                field_type.set_flen(11);
                field_type
            } else {
                let mut field_type = base_return_field_type(EvalType::Decimal);
                field_type.set_flen(12 + decimal);
                field_type.set_decimal(decimal);
                field_type
            }
        }
        // Go `absFunctionClass` (`builtin_math.go:119`): the result type IS
        // the argument type, collapsed to Int/Decimal/Real.
        "abs" => {
            let eval = arg.map_or(EvalType::Real, |arg| match arg.eval_type() {
                EvalType::Int => EvalType::Int,
                EvalType::Decimal => EvalType::Decimal,
                _ => EvalType::Real,
            });
            let mut field_type = base_return_field_type(eval);
            if let (Some(arg), true) = (arg, eval != EvalType::Real) {
                field_type.set_flen(arg.flen());
                field_type.set_decimal(arg.decimal());
            }
            field_type
        }
        // Go `getEvalTp4FloorAndCeil` (`builtin_math.go:693`): an INTEGER
        // argument stays an integer, and a DECIMAL one does too as long as its
        // INTEGER PART fits in `MaxIntWidth - 2` digits.
        "ceiling" | "floor" => {
            let eval = match arg.map(|arg| (arg.eval_type(), arg.flen(), arg.decimal())) {
                Some((EvalType::Int, _, _)) => EvalType::Int,
                Some((EvalType::Decimal, flen, decimal)) => {
                    if flen - decimal > 18 {
                        EvalType::Decimal
                    } else {
                        EvalType::Int
                    }
                }
                _ => EvalType::Real,
            };
            base_return_field_type(eval)
        }
        // Go `arithmeticModFunctionClass`: the numeric-context type of BOTH
        // arguments decides, so a temporal with no fractional seconds is
        // integer arithmetic.
        "mod" => {
            let left = args.first().map(numeric_context_result_type);
            let right = args.get(1).map(numeric_context_result_type);
            let eval = match (left, right) {
                (Some(EvalType::Real), _) | (_, Some(EvalType::Real)) => EvalType::Real,
                (Some(EvalType::Decimal), _) | (_, Some(EvalType::Decimal)) => EvalType::Decimal,
                (Some(EvalType::Int), Some(EvalType::Int)) => EvalType::Int,
                _ => EvalType::Real,
            };
            base_return_field_type(eval)
        }
        // The remaining nineteen declare `types.ETInt` outright and read
        // nothing off their arguments.
        _ => base_return_field_type(EvalType::Int),
    }
}

/// The result `FieldType` of a partition expression, inferred bottom-up the
/// way Go infers it while BUILDING the expression -- which is what
/// `checkPartitionFuncType` then reads a single `EvalType()` off
/// (`ddl/partition.go:1895`).
///
/// `None` for a shape this tier cannot type; the caller treats that as
/// non-integral rather than guessing.
fn partition_expr_result_type(
    expr: &Expr,
    names: &[String],
    types: &[FieldType],
) -> Option<FieldType> {
    match expr {
        Expr::Paren(inner) => partition_expr_result_type(inner, names, types),
        Expr::Column(path) => path
            .last()
            .and_then(|name| {
                names
                    .iter()
                    .position(|candidate| candidate.eq_ignore_ascii_case(name))
            })
            .and_then(|offset| types.get(offset).cloned()),
        Expr::Int(_) | Expr::Bool(_) | Expr::Hex(_) | Expr::Bit(_) => {
            Some(base_return_field_type(tidb_datatype::EvalType::Int))
        }
        Expr::Func { name, args, .. } => {
            let arg_types = args
                .iter()
                .map(|arg| {
                    partition_expr_result_type(arg, names, types)
                        .unwrap_or_else(|| base_return_field_type(tidb_datatype::EvalType::Real))
                })
                .collect::<Vec<_>>();
            Some(partition_function_result_type(name, &arg_types))
        }
        // Go `EXTRACT` declares `types.ETInt` like the other nineteen.
        Expr::Extract { .. } => Some(base_return_field_type(tidb_datatype::EvalType::Int)),
        // Go `AllowedPartition4BinaryOpMap` is `+ - * DIV %`. `DIV` is
        // integer division whatever its operands are
        // (`builtin_arithmetic.go:837`); the rest read BOTH operands in
        // numeric context, which is the same rule `MOD` uses -- and is why a
        // `datetime` with no fractional seconds is integer arithmetic here.
        Expr::Binary(tidb_ast::BinaryOp::IntDiv, _, _) => {
            Some(base_return_field_type(tidb_datatype::EvalType::Int))
        }
        Expr::Binary(
            tidb_ast::BinaryOp::Plus
            | tidb_ast::BinaryOp::Minus
            | tidb_ast::BinaryOp::Mul
            | tidb_ast::BinaryOp::Mod,
            left,
            right,
        ) => {
            use tidb_datatype::EvalType;
            let of = |side| {
                partition_expr_result_type(side, names, types)
                    .as_ref()
                    .map_or(EvalType::Real, numeric_context_result_type)
            };
            let eval = match (of(left), of(right)) {
                (EvalType::Real, _) | (_, EvalType::Real) => EvalType::Real,
                (EvalType::Decimal, _) | (_, EvalType::Decimal) => EvalType::Decimal,
                _ => EvalType::Int,
            };
            Some(base_return_field_type(eval))
        }
        // Go `AllowedPartition4UnaryOpMap` is unary `+ -`, which keep their
        // operand's numeric-context type.
        Expr::Unary(tidb_ast::UnaryOp::Plus | tidb_ast::UnaryOp::Minus, inner) => {
            partition_expr_result_type(inner, names, types)
                .as_ref()
                .map(|field_type| base_return_field_type(numeric_context_result_type(field_type)))
        }
        _ => None,
    }
}

/// Go `checkPartitionExprArgs` (`ddl/partition.go:4981`): the ARGUMENT types
/// a whitelisted partition function will accept.
///
/// Go runs this as the FIRST of three processors on every node of a pre-order
/// walk (`checkPartitionFuncValid`, `partition.go:1849`), so it fires before
/// the whitelist check on the same node and the first error aborts the walk.
///
/// `collectArgsType` silently SKIPS any argument that is not a plain column
/// (`partition.go:5029`), and every predicate is an ANY over the collected
/// bytes -- so a call with no column arguments collects nothing, every
/// predicate is false, and `checkResultOK` raises 1486. That is how
/// `YEAR(1)` is refused.
fn check_partition_expr_args(
    expr: &Expr,
    names: &[String],
    types: &[FieldType],
) -> Result<(), DriverError> {
    let Expr::Func { name, args, .. } = expr else {
        return Ok(());
    };
    // Go `collectArgsType`: the declared type of each COLUMN argument, in
    // order; a non-column argument contributes nothing.
    let mut arg_types = Vec::new();
    for arg in args {
        // Go type-asserts `*ast.ColumnNameExpr` DIRECTLY and skips anything
        // else (`partition.go:5029`). It does NOT look through parentheses,
        // so `YEAR((d))` collects no argument type at all -- every positive
        // rule then fails and the call is 1486.
        let Expr::Column(path) = arg else {
            continue;
        };
        let Some(column) = path.last() else { continue };
        let Some(offset) = names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(column))
        else {
            // Go `findColumnByName` returning nil is 1054 naming the clause.
            return Err(DriverError::UnknownColumnInClause {
                column: column.clone(),
                clause: "partition function".to_owned(),
            });
        };
        arg_types.push(types[offset].code());
    }
    let has_date = arg_types
        .iter()
        .any(|code| matches!(code, FieldTypeCode::Date | FieldTypeCode::Datetime));
    let has_time = arg_types
        .iter()
        .any(|code| matches!(code, FieldTypeCode::Duration | FieldTypeCode::Datetime));
    let has_timestamp = arg_types
        .iter()
        .any(|code| matches!(code, FieldTypeCode::Timestamp));
    let ok = |accepted: bool| {
        if accepted {
            Ok(())
        } else {
            Err(DriverError::PartitionWrongExprInFunc)
        }
    };
    match name.to_ascii_lowercase().as_str() {
        "to_days" | "to_seconds" | "dayofmonth" | "month" | "dayofyear" | "quarter"
        | "yearweek" | "year" | "weekday" | "dayofweek" | "day" => ok(has_date),
        "hour" | "minute" | "second" | "time_to_sec" | "microsecond" => ok(has_time),
        "unix_timestamp" => ok(has_timestamp),
        "from_days" => ok(has_date || has_time),
        // Go `slice.AllOf` over an EMPTY slice is TRUE, so `DATEDIFF` with
        // no column arguments passes this check (and is caught later, by the
        // no-columns rule).
        "datediff" => ok(arg_types
            .iter()
            .all(|code| matches!(code, FieldTypeCode::Date | FieldTypeCode::Datetime))),
        // Go raises 1486 for a TIMESTAMP argument to these, because their
        // value depends on the session time zone.
        "abs" | "ceiling" | "floor" | "mod" => {
            if has_timestamp {
                Err(DriverError::PartitionWrongExprInFunc)
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

/// Go's `if len(checker.columns) == 0` (`ddl/partition.go:1855`): a partition
/// expression that reads NO column is 1486, however well-formed it is.
///
/// `PARTITION BY HASH (1+1)` is constant, so every row would route to the
/// same partition -- which is what the error's own wording is about.
fn partition_expr_reads_a_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Paren(inner) | Expr::Unary(_, inner) => partition_expr_reads_a_column(inner),
        Expr::Extract { value, .. } => partition_expr_reads_a_column(value),
        Expr::Binary(_, left, right) => {
            partition_expr_reads_a_column(left) || partition_expr_reads_a_column(right)
        }
        Expr::Func { args, .. } => args.iter().any(partition_expr_reads_a_column),
        _ => false,
    }
}

/// Go `tables.NewPartitionExprBuildCtx` (`table/tables/partition.go:284`):
/// the FIXED context a metadata load folds bounds under.
///
/// `newPartitionExpr` discards the caller's context and builds this one
/// (`partition.go:306`), which is the whole point: routing must not depend on
/// who loaded the table. Threading the SESSION context through instead meant
/// the same stored table could fold to different bounds under
/// `SET time_zone = '+08:00'` than under UTC -- two nodes, or the same node
/// on two connections, routing the same row to different partitions.
///
/// It is deliberately NON-STRICT: truncation is ignored, zero and invalid
/// dates are accepted. A stored bound was already validated when it was
/// written, so a load that re-imposed CREATE-time strictness could refuse a
/// table the cluster is serving.
fn partition_metadata_context() -> crate::StmtContext {
    // `for_query` is the nearest public constructor; what matters for a
    // metadata load is that the zone is UTC and the folding is not strict,
    // both of which are set explicitly here rather than inherited.
    crate::StmtContext::for_query()
        .with_strict(false)
        .with_time_zone(tidb_datatype::SessionTimeZone::utc())
        .with_date_modes(crate::zero_date::DateModes {
            allow_invalid_dates: true,
            ..crate::zero_date::DateModes::TIDB_DEFAULT_SQL_MODE
        })
}

/// Which of Go's TWO partition-metadata paths a build is running as.
///
/// Go does not share this code: `ddl/partition.go` validates a written
/// `PARTITION BY` clause, and `table/tables/partition.go` rebuilds routing
/// from stored metadata. They agree on how a bound FOLDS and disagree on
/// what is checked -- the loader repeats almost nothing, because a stored
/// bound was already validated when it was written.
///
/// The folding stays in one place here on purpose: two copies could drift,
/// and a loader that folded a bound differently from the DDL that wrote it is
/// the exact failure this module exists to prevent. Only the CHECKS are
/// gated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionBuildMode {
    /// Go `ddl/partition.go`: a written clause, fully validated.
    Create,
    /// Go `tables/partition.go`: stored metadata, rebuilt and not re-judged.
    Load,
}

impl PartitionBuildMode {
    /// Whether a CREATE-time check should run.
    #[must_use]
    pub const fn validates(self) -> bool {
        matches!(self, Self::Create)
    }
}

/// Go `checkPartitionNameUnique` (1517), matched case-insensitively as
/// partition names are.
fn check_partition_name_unique(definitions: &[PartitionDef]) -> Result<(), DriverError> {
    for (index, definition) in definitions.iter().enumerate() {
        let folded = go_to_lower(&definition.name);
        if definitions[..index]
            .iter()
            .any(|earlier| go_to_lower(&earlier.name) == folded)
        {
            // Go reports the LATER occurrence, original-cased
            // (`partition.go:1761`).
            return Err(DriverError::PartitionSameName(definition.name.clone()));
        }
    }
    Ok(())
}

/// Go `strings.ToLower`, which is what `ast.NewCIStr` stores as `.L` and what
/// every partition-name comparison is made on (`ast/model.go:300`).
///
/// This is Unicode SIMPLE lowercase -- one rune in, one rune out. It is
/// neither ASCII-only folding nor Rust's `char::to_lowercase`, and all three
/// disagree: comparing with `eq_ignore_ascii_case` let `PARTITION Á` and
/// `PARTITION á` coexist where Go calls them the same name (1517), while
/// Rust's FULL mapping turns `İ` into two runes where Go's simple mapping
/// gives a single `i`.
///
/// `ß` stays `ß` under the simple map -- it does NOT expand to `ss` -- so `ß`
/// and `SS` remain distinct partition names.
fn go_to_lower(name: &str) -> String {
    name.chars()
        .map(|source| {
            // The one rune where Rust's full mapping differs from Go's simple
            // one: U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE lowercases to
            // `i` + U+0307 in full mapping, and to a bare `i` in Go.
            if source == '\u{130}' {
                return 'i';
            }
            let mut mapped = source.to_lowercase();
            match (mapped.next(), mapped.next()) {
                (Some(single), None) => single,
                // A multi-rune expansion has no simple-mapping equivalent, so
                // Go would have left the rune alone.
                _ => source,
            }
        })
        .collect()
}

/// Go `checkTooLongTable` (`ddl/executor.go:864`), applied to a partition
/// name because a partition IS a physical table and reuses
/// `mysql.MaxTableNameLength`.
///
/// Counts RUNES, not bytes, so 64 CJK characters are legal where 65 ASCII
/// ones are not. Go runs it INSIDE the per-definition loop for RANGE and
/// LIST only -- HASH and KEY partition names are never length-checked at all,
/// which is a quirk of where the call sits rather than a rule.
fn check_too_long_partition_name(name: &str) -> Result<(), DriverError> {
    const MAX_TABLE_NAME_LENGTH: usize = 64;
    if name.chars().count() > MAX_TABLE_NAME_LENGTH {
        return Err(DriverError::TooLongIdent(name.to_owned()));
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
    /// Go `PartitionDefinition.Comment`.
    pub comment: String,
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
    is_empty_columns: bool,
    definitions: &[StoredPartitionDefinition],
    names: &[String],
    types: &[FieldType],
) -> Result<PartitionSpec, DriverError> {
    // Go builds its own; the caller's context is deliberately not used here.
    let ctx = &partition_metadata_context();
    // Go `newPartitionedTable` (`tables/partition.go:114`): an empty
    // definition list is refused before the expression is touched -- and with
    // `ErrUnknownPartition` (1735), not the 1504 a CREATE would give for
    // `PARTITIONS 0`. The two paths raise different errors for the same
    // shape because they are different code.
    if definitions.is_empty() {
        return Err(DriverError::PartitionMetadataUnknown);
    }
    let physical = definitions
        .iter()
        .map(|definition| PartitionDef {
            id: definition.id,
            name: definition.name.clone(),
            less_than: definition.less_than.clone(),
            in_values: definition.in_values.clone(),
            comment: definition.comment.clone(),
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
                is_empty_columns,
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
                    PartitionBuildMode::Load,
                )?;
            Ok(PartitionSpec {
                is_empty_columns,
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
                is_empty_columns,
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
                        PartitionBuildMode::Load,
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
                is_empty_columns,
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
            comment: definition.comment.clone(),
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
                entry.in_values = stored_in_values(
                    written,
                    match &spec.kind {
                        PartitionKind::ListColumns { field_types, .. } => field_types.as_slice(),
                        _ => &[],
                    },
                    ctx,
                )?;
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
    use tidb_datatype::EvalType;
    if matches!(datum, Datum::Null) {
        return Ok("NULL".to_owned());
    }
    let rendered = || {
        datum
            .restore_value_expr()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .map_err(|_| DriverError::PartitionColumnValueWrongType)
    };
    // Go switches on the COLUMN's EvalType first, and the `_binary 0x..`
    // form lives INSIDE the ETString arm (`ddl/partition.go:5260-5266`).
    //
    // Hoisting the charset test above the switch broke every numeric and
    // temporal column: those carry charset `binary` in MySQL, so an INT
    // bound took the hex path, found no bytes to render, and failed the
    // whole CREATE. The nesting is the rule, not decoration.
    match field_type.map(FieldType::eval_type) {
        Some(EvalType::Int) => rendered(),
        Some(EvalType::String) => {
            let binary = field_type
                .is_some_and(|ft| ft.charset() == tidb_datatype::Charset::Binary);
            let bytes = match datum {
                Datum::Bytes(value) => Some(value.as_slice()),
                Datum::String(value) => Some(value.bytes()),
                _ => None,
            };
            match (binary, bytes) {
                // Go emits the hex only for a NON-EMPTY binary value.
                (true, Some(bytes)) if !bytes.is_empty() => {
                    let mut hex = String::with_capacity(bytes.len() * 2 + 10);
                    hex.push_str("_binary 0x");
                    for byte in bytes {
                        hex.push_str(&format!("{byte:02x}"));
                    }
                    Ok(hex)
                }
                _ => rendered(),
            }
        }
        Some(EvalType::Datetime | EvalType::Timestamp | EvalType::Duration) => rendered(),
        // Go's switch has no other arm and returns
        // `ErrWrongTypeColumnValue` for anything else.
        _ => Err(DriverError::PartitionColumnValueWrongType),
    }
}

/// The stored `InValues` tuples for one LIST definition.
pub(super) fn stored_in_values(
    written: Option<&PartitionDefinition>,
    field_types: &[FieldType],
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
            stored.is_empty_columns,
            &definitions,
            &names,
            &types,
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

    /// Go's own golden matrix for `PARTITION BY KEY ()`
    /// (`tests/integrationtest/r/ddl/partition.result`): which key the empty
    /// column list resolves to, and when the statement is refused.
    ///
    /// Reading only the clustered handle gave k2 and k3 an EMPTY column list,
    /// and `key_partition_index` hashes the empty byte stream -- crc32 of
    /// nothing is 0 -- so every row routed to partition 0 while `SHOW CREATE`
    /// still printed `PARTITION BY KEY ()` and nothing surfaced the loss.
    #[test]
    fn an_empty_key_clause_resolves_the_key_go_resolves() {
        // (label, columns, not_null, handle_offsets, indexes, expected)
        let long = || FieldType::new(FieldTypeCode::LongLong);
        let not_null = || {
            let mut t = FieldType::new(FieldTypeCode::LongLong);
            t.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            t
        };
        let index = |name: &str, unique: bool, offsets: Vec<usize>| crate::kv_table::KvIndex {
            id: 1,
            name: name.to_owned(),
            comment: String::new(),
            unique,
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; offsets.len()],
            column_offsets: offsets,
            visible: true,
            global: false,
        };

        // k1: the primary key IS the handle.
        assert_eq!(
            key_clause_dependencies(&[not_null(), long()], &[0], &[]).expect("k1 is legal"),
            vec!["id".to_owned()]
        );
        // k2: no handle; the implicit primary key is the first UNIQUE key
        // whose columns are all NOT NULL.
        assert_eq!(
            key_clause_dependencies(
                &[not_null(), long()],
                &[],
                &[index("id", true, vec![0])]
            )
            .expect("k2 is legal"),
            vec!["id".to_owned()]
        );
        // k3: an explicit PRIMARY index wins even when it is NONCLUSTERED,
        // and even though a unique key precedes it.
        assert_eq!(
            key_clause_dependencies(
                &[not_null(), long()],
                &[],
                &[index("id", true, vec![0]), index("PRIMARY", true, vec![0])]
            )
            .expect("k3 is legal"),
            vec!["id".to_owned()]
        );
        // k4: the only unique key covers a NULLABLE column, so it cannot be
        // the implicit primary key and Go refuses the statement with 1105.
        let error = key_clause_dependencies(
            &[not_null(), long()],
            &[],
            &[index("id_id1", true, vec![0, 1])],
        )
        .expect_err("k4 has no key that can serve");
        assert_eq!(error.to_mysql_error().code, 1105);
        // A table with NO keys at all keeps the empty list: Go returns early
        // from `checkPartitioningKeysConstraints` and hashes the empty
        // stream, so this shape must stay legal.
        assert_eq!(
            key_clause_dependencies(&[long(), long()], &[], &[]).expect("a heap table is legal"),
            Vec::<String>::new()
        );
    }

    /// Builds `PARTITION BY KEY () PARTITIONS 2` over a two-column table and
    /// returns the columns it routes by.
    fn key_clause_dependencies(
        types: &[FieldType],
        handle_offsets: &[usize],
        indexes: &[crate::kv_table::KvIndex],
    ) -> Result<Vec<String>, DriverError> {
        let sql = "CREATE TABLE t (id BIGINT, id1 BIGINT) PARTITION BY KEY () PARTITIONS 2";
        let statement = tidb_parser::parse(sql).expect("the fixture parses");
        let tidb_ast::Stmt::Ddl(ddl) = statement else {
            panic!("not DDL");
        };
        let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
            panic!("not CREATE TABLE");
        };
        let names = vec!["id".to_owned(), "id1".to_owned()];
        build_table_partitioning(
            create,
            &names,
            types,
            indexes,
            handle_offsets,
            &mut || 1,
            &crate::StmtContext::for_query(),
        )
        .map(|spec| spec.expect("the clause is there").dependencies)
    }

    /// Go `checkPartitionFuncType` accepts a whitelisted partition function
    /// only when its RESULT type is an integer (`ddl/partition.go:1895`), and
    /// the whitelist is not uniform about that.
    ///
    /// Answering "every whitelisted name returns an integer" admitted these
    /// at CREATE, and routing then read a Real/Decimal/Time datum and put
    /// every row in partition 0 -- the table was created and answered
    /// wrongly rather than being refused.
    #[test]
    fn a_partition_function_is_admitted_on_its_result_type_not_its_name() {
        // Go asserts both halves of the FLOOR split itself, at
        // `ddl/tests/partition/db_partition_test.go:241-242`.
        assert_eq!(partition_clause_error("FLOOR(c2)"), Some(1491), "FLOOR over a float is a REAL");
        assert_eq!(partition_clause_error("FLOOR(c1)"), None, "FLOOR over an int is an int");
        assert_eq!(partition_clause_error("ABS(c2)"), Some(1491));
        assert_eq!(partition_clause_error("CEILING(c2)"), Some(1491));
        // FROM_DAYS returns a DATE whatever it is given -- but WHICH error
        // it gets depends on the order Go runs its checks in.
        // `checkPartitionExprArgs` runs at `buildTablePartitionInfo:615` and
        // wants a date-or-time argument, long before `checkPartitionFuncType`
        // (`create_table.go:530`) ever looks at the result type. So a BIGINT
        // argument is refused by the ARGUMENT rule with 1486 ...
        assert_eq!(partition_clause_error("FROM_DAYS(c1)"), Some(1486));
        // ... and only an argument that SATISFIES the argument rule survives
        // to be refused for its DATE result type with 1491.
        assert_eq!(partition_clause_error("FROM_DAYS(d)"), Some(1491));
        // The argument rules themselves: a function wanting a DATE will not
        // take an integer column, and the TIMESTAMP ban on ABS/FLOOR/MOD is
        // how Go keeps a partition expression timezone-independent.
        assert_eq!(partition_clause_error("YEAR(c1)"), Some(1486));
        assert_eq!(partition_clause_error("FLOOR(ts0)"), Some(1486));
        // A partition expression that reads no column at all is 1486
        // (`partition.go:1855`): it is constant, so every row would route to
        // the same partition.
        assert_eq!(partition_clause_error("1 + 1"), Some(1486));
        // UNIX_TIMESTAMP keeps ETInt only at fsp 0; a fractional-second
        // argument makes it a DECIMAL (`builtin_time.go:4398`).
        assert_eq!(partition_clause_error("UNIX_TIMESTAMP(ts0)"), None);
        assert_eq!(partition_clause_error("UNIX_TIMESTAMP(ts3)"), Some(1491));
        // Go INFERS these from the argument's flen/decimal, which a table of
        // verdicts cannot reproduce. Each is asserted by Go's own tests.
        //
        // `FLOOR` over a DECIMAL is an integer whenever the integer part fits
        // in `MaxIntWidth - 2` digits (`builtin_math.go:701`).
        assert_eq!(partition_clause_error("FLOOR(d2)"), None);
        // And the nested case Go accepts at `db_partition_test.go:311`:
        // `unix_timestamp(ts3)` is `decimal(15,3)`, and 15 - 3 = 12 <= 18, so
        // FLOOR of it is an integer even though the inner call is not.
        assert_eq!(partition_clause_error("FLOOR(UNIX_TIMESTAMP(ts3))"), None);
        // `MOD` reads its arguments in NUMERIC context, where a temporal with
        // no fractional seconds is integer arithmetic
        // (`builtin_arithmetic.go:80`) -- so it cannot share a predicate with
        // ABS and FLOOR, which reject the very same column.
        assert_eq!(partition_clause_error("MOD(dt0, 2)"), None);
        assert_eq!(partition_clause_error("ABS(dt0)"), Some(1491));
        assert_eq!(partition_clause_error("FLOOR(dt0)"), Some(1491));
        // The single most common date-partitioning form stays legal: YEAR
        // returns an integer even though its argument is a DATE.
        assert_eq!(partition_clause_error("YEAR(d)"), None);
        assert_eq!(partition_clause_error("TO_DAYS(d)"), None);
    }

    /// The MySQL error a `PARTITION BY RANGE (<expr>)` clause is refused
    /// with, or `None` when it is accepted.
    fn partition_clause_error(expr: &str) -> Option<u16> {
        let sql = format!(
            "CREATE TABLE t (c1 BIGINT, c2 FLOAT, d DATE, ts0 TIMESTAMP, ts3 TIMESTAMP(3), \
             d2 DECIMAL(10,2), dt0 DATETIME) \
             PARTITION BY RANGE ({expr}) (PARTITION p0 VALUES LESS THAN (100))"
        );
        let statement = tidb_parser::parse(&sql).expect("the fixture parses");
        let tidb_ast::Stmt::Ddl(ddl) = statement else { panic!("not DDL") };
        let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else { panic!("not CREATE TABLE") };
        // Derive the column types the way the DDL path does, so the fixture
        // cannot drift from what a real CREATE TABLE produces -- a plain
        // TIMESTAMP is fsp 0, and hand-building the FieldType left it
        // unspecified, which clamps to 6 and reads as a DECIMAL.
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
        build_table_partitioning(
            create,
            &names,
            &types,
            &[],
            &[],
            &mut || 1,
            &crate::StmtContext::for_query(),
        )
        .err()
        .map(|error| error.to_mysql_error().code)
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

