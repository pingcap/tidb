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
//! [`tidb_table::partition_routing::PartitionSpec`] the table stores; a table with
//! no clause gets `None`.
//!
//! # What is accepted
//!
//! HASH, KEY, scalar RANGE, RANGE COLUMNS, scalar LIST, and LIST COLUMNS. They are
//! routed by [`tidb_table::partition_routing`], stored as one physical key prefix
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
use tidb_model::index::IndexInfo;
use tidb_table::partition_routing::{
    PartitionDef, PartitionKind, PartitionSpec, RangeBound, RangeColumnBound,
};
use tidb_error::mysql::MysqlError;

pub use tidb_table::partition_metadata::{
    partition_spec_from_metadata, stored_definitions_as_ast, StoredPartitionDefinition,
    PARTITION_MAX_VALUE,
};
use tidb_hack::GoCaseCompare;

/// Go `checkAddPartitionTooManyPartitions`: the hard cap on partitions.
pub const MAX_PARTITIONS: u64 = 8192;

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
/// 1564, 1659, 8264 -- plus a 1105 refusal naming the method,
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
    indexes: &[IndexInfo],
    handle_offsets: &[usize],
    allocate_id: &mut dyn FnMut() -> i64,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Option<PartitionSpec>, MysqlError> {
    create
        .partitioning
        .as_ref()
        .map(|partitioning| {
            build_partitioning(
                partitioning,
                names,
                types,
                indexes,
                handle_offsets,
                allocate_id,
                ctx,
            )
        })
        .transpose()
}

/// Go `buildTablePartitionInfo` for one written partition clause, the
/// CREATE TABLE battery that `ALTER TABLE ... PARTITION BY` and REORGANIZE
/// PARTITION's `checkReorgPartitionDefs` run as well.
pub fn build_partitioning(
    partitioning: &tidb_ast::TablePartitioning,
    names: &[String],
    types: &[FieldType],
    indexes: &[IndexInfo],
    handle_offsets: &[usize],
    allocate_id: &mut dyn FnMut() -> i64,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<PartitionSpec, MysqlError> {
    let mut spec = build_table_partitioning_inner(
        partitioning,
        names,
        types,
        indexes,
        handle_offsets,
        allocate_id,
        ctx,
    )?;
    let stored = stored_definitions_for(partitioning, &spec, ctx)?;
    for (routing, stored) in spec.definitions.iter_mut().zip(&stored) {
        routing.less_than.clone_from(&stored.less_than);
        routing.in_values.clone_from(&stored.in_values);
    }
    Ok(spec)
}

fn build_table_partitioning_inner(
    partitioning: &tidb_ast::TablePartitioning,
    names: &[String],
    types: &[FieldType],
    indexes: &[IndexInfo],
    handle_offsets: &[usize],
    allocate_id: &mut dyn FnMut() -> i64,
    // The SESSION's context: a RANGE bound is FOLDED here, and its value can
    // depend on the session `time_zone`. Go threads its `BuildContext` down
    // the same path (`buildTablePartitionInfo` -> `checkPartitionValuesIsInt`).
    ctx: &tidb_expr::metabuild::Context,
) -> Result<PartitionSpec, MysqlError> {
    let method = &partitioning.method;
    if !matches!(
        method.kind,
        PartitionType::HASH | PartitionType::KEY | PartitionType::RANGE | PartitionType::LIST
    ) {
        // The method name is Go's own spelling, so the refusal reads like the
        // clause the user wrote rather than like a Rust variant.
        let name = method.kind.sql();
        return Err(tidb_error::mysql::MysqlError::unknown(format!(
            "CREATE TABLE ... PARTITION BY {name} is not supported by this node: \
             it can neither route a row to one of those partitions nor prune \
             them, so accepting the clause would build an ordinary \
             unpartitioned table"
        )));
    }
    // Go `buildTablePartitionInfo`: subpartitioning is only legal under
    // RANGE/LIST (1500 under HASH/KEY), and there Go WARNS (8200) and builds
    // the table with the first-level partitioning alone.
    if partitioning.subpartition.is_some() {
        if matches!(method.kind, PartitionType::HASH | PartitionType::KEY) {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::ast::ERR_SUBPARTITION.generate_with_stack_by_args(&[]),
            ));
        }
        ctx.append_warning(tidb_util::context::WarnErr::Terror(
            tidb_util::dbterror::ERR_UNSUPPORTED_CREATE_PARTITION.generate(format!(
                "Unsupported subpartitioning, only using {} partitioning",
                method.kind.sql()
            )),
        ));
    }

    // Go `generatePartitionDefinitionsFromInterval`: INTERVAL expands into
    // ordinary RANGE definitions right after `checkColumnsPartitionType` and
    // the partition expression's own checks, and the expansion is then built
    // like a written clause.
    if method.interval.is_some() {
        let mut column = None;
        let mut unsigned = false;
        if let Some(expr) = &method.expr {
            let (_, built, _, _) = build_partition_expression(
                expr,
                names,
                types,
                &ctx.get_expr_ctx().get_eval_ctx().time_zone(),
                ctx.get_expr_ctx().get_eval_ctx().like_default_escape(),
            )?;
            unsigned = super::table_partition_range::partition_expression_is_unsigned(&built);
        } else if let [path] = method.columns.as_slice() {
            let name = path.last().ok_or_else(|| {
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE
                        .generate_with_stack_by_args(&[]),
                )
            })?;
            let offset = names
                .iter()
                .position(|candidate| candidate.go_lower_eq(name))
                .ok_or_else(|| {
                    tidb_error::mysql::MysqlError::from_terror(
                        &tidb_util::dbterror::ERR_FIELD_NOT_FOUND_PART
                            .generate_with_stack_by_args(&[]),
                    )
                })?;
            if !super::table_partition_list::list_columns_type_allowed(&types[offset]) {
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION
                        .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
                ));
            }
            column = Some(&types[offset]);
        }
        let definitions =
            super::partition_interval::expand_interval(partitioning, column, unsigned, ctx)?;
        let mut expanded = partitioning.clone();
        expanded.definitions = definitions;
        expanded.method.interval = None;
        expanded.subpartition = None;
        return build_table_partitioning_inner(
            &expanded,
            names,
            types,
            indexes,
            handle_offsets,
            allocate_id,
            ctx,
        );
    }

    if method.kind == PartitionType::KEY {
        // Go's order, which four separate errors depend on:
        //   phase 1 `buildTablePartitionInfo` -- resolve the column list,
        //     `checkColumnsPartitionType` (1488/1659), then
        //     `buildHashPartitionDefinitions`, whose FIRST act is the
        //     partition-count cap (1499, `partition.go:1455`);
        //   phase 2 `checkPartitionDefinitionConstraints` -- 1517, 1499,
        //     1652, then `checkPartitionByHash`'s 1504;
        //   last, `checkPartitioningKeysConstraints` -- 1105 and 1503.
        let (dependencies, unresolved_with_keys) =
            build_key_partition_columns(&method.columns, names, types, handle_offsets, indexes)?;
        let definitions =
            build_hash_partition_definitions(partitioning, method.count, allocate_id, ctx)?;
        check_partition_name_unique(&definitions)?;
        if definitions.len() as u64 > MAX_PARTITIONS {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS.generate_with_stack_by_args(&[]),
            ));
        }
        check_partition_columns_unique(&dependencies)?;
        if definitions.is_empty() {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::ast::ERR_NO_PARTS
                    .generate_with_stack_by_args(&["partitions".into()]),
            ));
        }
        if unresolved_with_keys {
            return Err(tidb_error::mysql::MysqlError::unknown("Table partition metadata not correct, neither partition expression or list of partition columns"));
        }
        let dependency_offsets = dependencies
            .iter()
            .map(|name| {
                names
                    .iter()
                    .position(|candidate| candidate.go_lower_eq(name))
                    .expect("KEY names resolved above")
            })
            .collect::<Vec<_>>();
        let indexes = apply_update_indexes(partitioning, indexes, handle_offsets)?;
        check_unique_keys_include_partition_columns(&indexes, names, handle_offsets, &dependency_offsets)?;
        return Ok(PartitionSpec {
            overlapping_dropping_partition_indices: Vec::new(),
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
        });
    }

    if method.kind == PartitionType::LIST && method.expr.is_none() {
        let mut definitions = Vec::with_capacity(partitioning.definitions.len());
        let (dependencies, kind) = super::table_partition_list::build_list_columns_values(
            &method.columns,
            &partitioning.definitions,
            names,
            types,
            ctx,
            &mut definition_tail(partitioning, &mut definitions, allocate_id, ctx, false),
        )?;
        check_partition_name_unique(&definitions)?;
        if definitions.len() as u64 > MAX_PARTITIONS {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS.generate_with_stack_by_args(&[]),
            ));
        }
        check_partition_columns_unique(&dependencies)?;
        let dependency_offsets = dependencies
            .iter()
            .map(|name| {
                names
                    .iter()
                    .position(|candidate| candidate.go_lower_eq(name))
                    .expect("LIST COLUMNS names resolved above")
            })
            .collect::<Vec<_>>();
        let indexes = apply_update_indexes(partitioning, indexes, handle_offsets)?;
        check_unique_keys_include_partition_columns(&indexes, names, handle_offsets, &dependency_offsets)?;
        return Ok(PartitionSpec {
            overlapping_dropping_partition_indices: Vec::new(),
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
        });
    }

    if method.kind == PartitionType::RANGE && method.expr.is_none() {
        let mut definitions = Vec::with_capacity(partitioning.definitions.len());
        let (dependencies, field_types, less_than) =
            super::table_partition_range::build_range_columns_bounds(
                &method.columns,
                &partitioning.definitions,
                names,
                types,
                ctx,
                &mut definition_tail(partitioning, &mut definitions, allocate_id, ctx, true),
            )?;
        // Go `checkPartitionDefinitionConstraints` order: 1517, 1499, 1652,
        // and only then `checkPartitionByRange`'s 1493.
        check_partition_name_unique(&definitions)?;
        if definitions.len() as u64 > MAX_PARTITIONS {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS.generate_with_stack_by_args(&[]),
            ));
        }
        check_partition_columns_unique(&dependencies)?;
        super::table_partition_range::check_range_columns_strictly_increasing(
            &less_than,
            &field_types,
        )?;
        let dependency_offsets = dependencies
            .iter()
            .map(|name| {
                names
                    .iter()
                    .position(|candidate| candidate.go_lower_eq(name))
                    .expect("RANGE COLUMNS names resolved above")
            })
            .collect::<Vec<_>>();
        let indexes = apply_update_indexes(partitioning, indexes, handle_offsets)?;
        check_unique_keys_include_partition_columns(&indexes, names, handle_offsets, &dependency_offsets)?;
        return Ok(PartitionSpec {
            overlapping_dropping_partition_indices: Vec::new(),
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
        });
    }

    let Some(expr) = &method.expr else {
        // A column list rather than an expression is Go's KEY-shaped path
        // (`HASH COLUMNS`), which this tier does not route.
        let name = method.kind.sql();
        return Err(tidb_error::mysql::MysqlError::unknown(format!(
            "CREATE TABLE ... PARTITION BY {name} COLUMNS is not supported by this node"
        )));
    };
    let (expr_text, built, dependencies, dependency_offsets) = build_partition_expression(
        expr,
        names,
        types,
        &ctx.get_expr_ctx().get_eval_ctx().time_zone(),
        ctx.get_expr_ctx().get_eval_ctx().like_default_escape(),
    )?;
    // `deferred` carries the LIST duplicate-constant refusal (1495) that Go
    // raises from `checkPartitionByList`, i.e. after 1517 and 1499.
    let mut written = Vec::with_capacity(partitioning.definitions.len());
    let (kind, definitions, deferred) = match method.kind {
        PartitionType::RANGE => {
            let (less_than, unsigned) = super::table_partition_range::build_range_bounds(
                &built,
                &partitioning.definitions,
                ctx,
                &mut definition_tail(partitioning, &mut written, allocate_id, ctx, true),
            )?;
            (
                PartitionKind::Range {
                    less_than,
                    unsigned,
                },
                written,
                None,
            )
        }
        PartitionType::LIST => {
            let (kind, duplicate) = super::table_partition_list::build_list_values(
                &built,
                &partitioning.definitions,
                ctx,
                &mut definition_tail(partitioning, &mut written, allocate_id, ctx, false),
            )?;
            (kind, written, duplicate)
        }
        _ => (
            PartitionKind::Hash,
            build_hash_partition_definitions(partitioning, method.count, allocate_id, ctx)?,
            None,
        ),
    };
    check_partition_name_unique(&definitions)?;
    if definitions.len() as u64 > MAX_PARTITIONS {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS.generate_with_stack_by_args(&[]),
        ));
    }
    // Go checks the partition definition constraints before
    // `checkPartitionFuncType` (`create_table.go:517-533`).  Keep the same
    // order: malformed VALUES (including an early MAXVALUE or a non-integer
    // LIST bound) must win over a non-integral partition expression.
    check_partition_expression_type(expr, &built)?;
    // Go's type-specific branch of `checkPartitionDefinitionConstraints`,
    // which runs AFTER 1517 and 1499: `checkPartitionByRange` is where 1493
    // lives, so a duplicate partition name beside a non-increasing bound
    // reports the duplicate, as Go does.
    if let PartitionKind::Range {
        less_than,
        unsigned,
    } = &kind
    {
        super::table_partition_range::check_strictly_increasing(less_than, *unsigned)?;
    }
    if let Some(duplicate) = deferred {
        return Err(duplicate);
    }
    // Go `checkNoHashPartitions`. The parser now rejects a written
    // `PARTITIONS 0` itself (as Go's does), so this check is defensive:
    // no SQL text reaches it with zero definitions.
    if definitions.is_empty() {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_error::catalog::ast::ERR_NO_PARTS
                .generate_with_stack_by_args(&["partitions".into()]),
        ));
    }
    let indexes = apply_update_indexes(partitioning, indexes, handle_offsets)?;
    check_unique_keys_include_partition_columns(&indexes, names, handle_offsets, &dependency_offsets)?;

    Ok(PartitionSpec {
        overlapping_dropping_partition_indices: Vec::new(),
        is_empty_columns: false,
        kind,
        expr_text,
        expr: built,
        dependencies,
        definitions,
    })
}
/// Go `buildTablePartitionInfo`'s `UPDATE INDEXES (name GLOBAL|LOCAL, ...)`:
/// the table's indexes with each named one's locality replaced, which the
/// unique-key rule then reads. An unknown or repeated name is 1280; naming
/// `PRIMARY` on a table whose primary key is the integer handle is 1503,
/// since that key has no index to move.
pub fn apply_update_indexes(
    partitioning: &tidb_ast::TablePartitioning,
    indexes: &[IndexInfo],
    handle_offsets: &[usize],
) -> Result<Vec<IndexInfo>, MysqlError> {
    let mut updated = indexes.to_vec();
    let mut seen = std::collections::HashSet::new();
    for update in &partitioning.update_indexes {
        let Some(index) = updated
            .iter_mut()
            .find(|index| index.name.original().go_equal_fold(&update.name))
        else {
            if update.name.go_equal_fold("primary") && !handle_offsets.is_empty() {
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF
                        .generate_with_stack_by_args(&[("CLUSTERED INDEX".to_owned())
                            .to_string()
                            .into()]),
                ));
            }
            return Err(MysqlError::from_terror(
                &tidb_util::dbterror::ERR_WRONG_NAME_FOR_INDEX
                    .generate_with_stack_by_args(&[update.name.clone().into()]),
            ));
        };
        if !seen.insert(go_to_lower(&update.name)) {
            return Err(MysqlError::from_terror(
                &tidb_util::dbterror::ERR_WRONG_NAME_FOR_INDEX
                    .generate_with_stack_by_args(&[update.name.clone().into()]),
            ));
        }
        index.global = update.global;
    }
    Ok(updated)
}

/// Resolve Go `PARTITION BY KEY` columns.  An empty list means the table's
/// primary key; a heap table therefore hashes the empty byte stream, exactly
/// as Go's `ForKeyPruning` does when `PartitionInfo.Columns` remains empty.
///
/// This is Go's PHASE ONE only -- the column resolution at
/// `buildTablePartitionInfo` (`ddl/partition.go:627-643`) and the type gate
/// `checkColumnsPartitionType` that applies there. The duplicate-column rule
/// (1652) and the "has keys but none can serve" refusal (1105) belong to
/// LATER phases and are raised by the caller, because Go reports four other
/// errors ahead of them.
///
/// The returned flag is Go's `getPartitionColSlices` failure, deferred:
/// `checkPartitioningKeysConstraints` returns early for a table with no
/// indexes and no handle (`partition.go:4710`), so a bare heap table keeps
/// its empty list and legally hashes the empty stream.
fn build_key_partition_columns(
    columns: &[Vec<String>],
    names: &[String],
    types: &[FieldType],
    handle_offsets: &[usize],
    indexes: &[IndexInfo],
) -> Result<(Vec<String>, bool), MysqlError> {
    let mut unresolved_with_keys = false;
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
            // bare `errors.Errorf`, which reaches the client as 1105 -- but
            // only from `checkPartitioningKeysConstraints`, the LAST check
            // `checkTableInfoValidWithStmt` runs (`create_table.go:533`).
            // Raising it here made it win over 1499, 1517, 1652 and 1504,
            // every one of which Go reports first.
            unresolved_with_keys = resolved.is_empty() && !indexes.is_empty();
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
                path.last().cloned().ok_or_else(|| {
                    tidb_error::mysql::MysqlError::from_terror(
                        &tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE
                            .generate_with_stack_by_args(&[]),
                    )
                })
            })
            .collect::<Result<_, _>>()?
    };
    let mut dependencies = Vec::with_capacity(selected.len());
    for name in selected {
        let offset = names
            .iter()
            .position(|candidate| candidate.go_lower_eq(&name))
            // Go `checkColumnsPartitionType` (`partition.go:787`) reports a
            // column list's missing name as `ErrFieldNotFoundPart` (1488),
            // a DIFFERENT error from the 1054 the expression path raises.
            .ok_or_else(|| {
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_FIELD_NOT_FOUND_PART.generate_with_stack_by_args(&[]),
                )
            })?;
        if !key_partition_type_allowed(&types[offset]) {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION
                    .generate_with_stack_by_args(&[name.to_string().into()]),
            ));
        }
        dependencies.push(name);
    }
    Ok((dependencies, unresolved_with_keys))
}

/// An index's key parts as offsets into `names` (resolved by name, as Go's
/// partition checks compare column names) and their prefix lengths.
fn index_key_parts(index: &IndexInfo, names: &[String]) -> (Vec<usize>, Vec<i64>) {
    index
        .columns
        .iter_deref()
        .filter_map(|column| {
            let column = column.read();
            names
                .iter()
                .position(|name| name.go_lower_eq(column.name.original()))
                .map(|offset| (offset, column.length))
        })
        .unzip()
}

/// Go `TableInfo.GetPrimaryKey` (`meta/model/table.go:504`): the columns
/// `PARTITION BY KEY ()` routes by when the table has no clustered handle.
///
/// The EXPLICIT primary key wins as soon as it is met, whether or not it is
/// clustered. Otherwise the first UNIQUE key whose every column carries NOT
/// NULL becomes the implicit primary key. A table with neither keeps an empty
/// list, which is a legal heap table: Go hashes the empty stream for it too.
fn primary_or_implicit_key_columns(
    indexes: &[IndexInfo],
    names: &[String],
    types: &[FieldType],
) -> Vec<String> {
    let columns_of = |index: &IndexInfo| {
        index_key_parts(index, names)
            .0
            .iter()
            .filter_map(|offset| names.get(*offset).cloned())
            .collect::<Vec<_>>()
    };
    let mut implicit: Option<&IndexInfo> = None;
    for index in indexes {
        if index.primary {
            return columns_of(index);
        }
        // Go guards this: an index with no columns is never a primary key.
        if index.columns.is_empty() {
            continue;
        }
        if implicit.is_none() && index.unique {
            let all_not_null = index_key_parts(index, names).0.iter().all(|offset| {
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

/// Go `isValidKeyPartitionColType` (`ddl/partition.go:798`): only LOB/JSON/
/// geometry/vector columns are rejected. Character and temporal values hash
/// through their datum keys.
///
/// Go's list is `TypeBlob, TypeMediumBlob, TypeLongBlob, TypeJSON,
/// TypeGeometry, TypeTiDBVectorFloat32` -- `mysql.TypeTinyBlob` is NOT in it,
/// so TINYBLOB and TINYTEXT fall to `default: return true` and are legal KEY
/// partitioning columns. Adding TinyBlob here "corrected" a quirk Go does not
/// have and rejected `PARTITION BY KEY (a)` over a TINYTEXT that Go accepts.
fn key_partition_type_allowed(field_type: &FieldType) -> bool {
    !matches!(
        field_type.code(),
        FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Json
            | FieldTypeCode::Geometry
            | FieldTypeCode::VectorFloat32
    )
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
pub fn partition_restore_flags() -> tidb_ast::RestoreFlags {
    tidb_ast::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_ast::RestoreFlags::KEYWORD_UPPERCASE
        | tidb_ast::RestoreFlags::NAME_BACK_QUOTES
        | tidb_ast::RestoreFlags::BRACKET_AROUND_BINARY_OPERATION
        | tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME
        | tidb_ast::RestoreFlags::WITHOUT_TABLE_NAME
        // go's partition-bound restore prints string/DATE bounds WITHOUT the
        // charset introducer (`SHOW CREATE TABLE pf`: '2021-06-01', not
        // _UTF8MB4'2021-06-01').
        | tidb_ast::RestoreFlags::STRING_WITHOUT_CHARSET
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
    MysqlError,
> {
    // The whitelist (1564), the argument rules (1486/1054) and the no-column
    // rule (1486) all belong to `checkPartitionFuncValid`, which Go calls
    // from `buildTablePartitionInfo` (`partition.go:615`). The loader
    // (`tidb_table::partition_metadata`) only parses and builds.
    check_partition_expression_allowed(expr, names, types)?;
    // Go `checkPartitionFuncValid` (`ddl/partition.go:1855`): after the
    // walk, an expression that extracted NO column is 1486 however
    // well-formed it is. `PARTITION BY HASH (1+1)` is constant, so every
    // row would route to the same partition -- which is what the error's
    // wording describes.
    if !partition_expr_reads_a_column(expr) {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC.generate_with_stack_by_args(&[]),
        ));
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
            // Go's `extractColumns` quotes `columnNameExpr.Name.Name.L`,
            // the folded name, so a mixed-case `NoSuch` is reported as
            // `Unknown column 'nosuch'`.
            Some(name) => tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                    tidb_hack::go_to_lower(go_to_lower(&name)).into(),
                    ("partition function".to_owned()).to_string().into(),
                ]),
            ),
            None => tidb_error::mysql::MysqlError::unknown(
                "this partition expression is not supported yet".to_owned(),
            ),
        })?;
    // A rewrite can succeed while a branch failed to resolve only if the
    // resolver was never consulted, so the missing name is still the
    // authority on 1054.
    if let Some(name) = resolver.missing_name() {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_error::plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                tidb_hack::go_to_lower(go_to_lower(&name)).into(),
                ("partition function".to_owned()).to_string().into(),
            ]),
        ));
    }
    let dependency_offsets = resolver.dependencies();
    let dependencies = resolver.dependency_names();
    // Go `checkPartitionFuncValid`: an expression naming no column at all is
    // 1486 -- `PARTITION BY HASH(1)` has nothing to partition ON.
    if dependencies.is_empty() {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC.generate_with_stack_by_args(&[]),
        ));
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
) -> Result<(), MysqlError> {
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
        // Go's allowed-leaf list is `*driver.ValueExpr`, which a NULL literal
        // IS -- so `hash(-null)` reaches the no-column rule and is 1486, not
        // the whitelist's 1564.
        | Expr::Null
        | Expr::Default(_) => Ok(()),
        Expr::Paren(inner) => check_partition_expression_allowed(inner, names, types),
        Expr::Extract { value, .. } => check_partition_expression_allowed(value, names, types),
        // Go `AllowedPartition4UnaryOpMap`.
        Expr::Unary(tidb_ast::UnaryOp::Plus | tidb_ast::UnaryOp::Minus, inner) => {
            check_no_timestamp_args(&[inner.as_ref()], names, types)?;
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
            // Go returns `checkNoTimestampArgs(tb, v.L, v.R)` from the
            // operator node itself and only then descends, so a TIMESTAMP
            // operand is 1486 here rather than something the children raise.
            check_no_timestamp_args(&[left.as_ref(), right.as_ref()], names, types)?;
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
        _ => Err(tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED.generate_with_stack_by_args(&[]))),
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
    built: &tidb_expr::expression::Expression,
) -> Result<(), MysqlError> {
    let integral = built
        .static_type()
        .is_some_and(|field_type| field_type.eval_type() == tidb_datatype::EvalType::Int);
    if integral {
        return Ok(());
    }
    // Go type-asserts the ORIGINAL node as `*ast.ColumnNameExpr` and quotes
    // `col2.Name.Name.L` -- the folded name -- so a `DOUBLE A` reports
    // `Field 'a'`. The assertion does not look through parentheses; that is
    // why `unwrap_parentheses` has no place here.
    if let Expr::Column(path) = expr {
        let name = path.last().cloned().unwrap_or_else(|| "?".to_owned());
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION
                .generate_with_stack_by_args(&[(go_to_lower(&name)).to_string().into()]),
        ));
    }
    // Go's remaining `expression.Column` arm quotes `col.OrigName`, the name
    // as the source spelled it -- which is what a parenthesised column
    // reaches.
    if let Expr::Column(path) = unwrap_parentheses(expr) {
        let name = path.last().cloned().unwrap_or_else(|| "?".to_owned());
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION
                .generate_with_stack_by_args(&[name.to_string().into()]),
        ));
    }
    Err(tidb_error::mysql::MysqlError::from_terror(
        &tidb_util::dbterror::ERR_PARTITION_FUNC_NOT_ALLOWED
            .generate_with_stack_by_args(&["PARTITION".into()]),
    ))
}

/// The parenthesised expression's subject, since `(a)` partitions on `a`.
fn unwrap_parentheses(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) => unwrap_parentheses(inner),
        other => other,
    }
}

/// Go `buildHashPartitionDefinitions`: `n` partitions, named `p0..pn-1`
/// unless the statement named them itself.
fn build_hash_partition_definitions(
    partitioning: &tidb_ast::TablePartitioning,
    count: u64,
    allocate_id: &mut dyn FnMut() -> i64,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Vec<PartitionDef>, MysqlError> {
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
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS.generate_with_stack_by_args(&[]),
        ));
    }
    // A HASH partition definition carries no VALUES -- `tidb_parser`'s
    // `validate_definition` already rejects them -- so the written
    // definitions contribute their names and their comments.
    let written = &partitioning.definitions[..];
    debug_assert!(written
        .iter()
        .all(|definition| matches!(definition.clause, PartitionDefinitionClause::None)));
    let mut definitions = Vec::with_capacity(count as usize);
    for index in 0..count {
        let written_definition = written.get(index as usize);
        let name =
            written_definition.map_or_else(|| format!("p{index}"), |written| written.name.clone());
        let comment = match written_definition {
            Some(definition) => partition_definition_comment(definition, ctx, false)?,
            None => String::new(),
        };
        let placement_policy = written_definition.and_then(partition_definition_placement);
        definitions.push(PartitionDef {
            id: allocate_id(),
            name,
            // HASH and KEY definitions carry no VALUES clause at all.
            less_than: Vec::new(),
            in_values: Vec::new(),
            comment,
            placement_policy,
            // Resolved from the table's ENGINE_ATTRIBUTE once the table is
            // built (`rebuildStorageClassForPartitions`).
            storage_class: Default::default(),
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
/// Go `setPartitionPlacementFromOptions` (`ddl/partition.go`): the policy a
/// partition definition names for ITSELF.
///
/// Go's comment there is the important part: a partition does NOT inherit a
/// copy of the table's policy. The table's rules cover the partition's key
/// range already, so copying would freeze a snapshot and break the cascade
/// when the policy is altered. Only a policy written on this definition is
/// recorded.
pub fn partition_definition_placement(
    definition: &PartitionDefinition,
) -> Option<tidb_model::PolicyRefInfo> {
    definition.options.iter().find_map(|option| match option {
        // The ID is left at zero here and stamped where the catalog is in
        // reach (`run_create_table_in`, the ALTER partition builders). Go resolves a table's and its partitions' references in ONE
        // place for the same reason (`CreateTableWithInfo`): the policy has
        // to exist, and the reference is BY id once it does.
        tidb_ast::TableOption::PlacementPolicy(name) => Some(tidb_model::PolicyRefInfo {
            id: 0,
            name: tidb_ast::CiString::new(name.clone()),
        }),
        _ => None,
    })
}

pub fn partition_definition_comment(
    definition: &PartitionDefinition,
    ctx: &tidb_expr::metabuild::Context,
    validate_length: bool,
) -> Result<String, MysqlError> {
    let Some(comment) = definition.options.iter().find_map(|option| match option {
        tidb_ast::TableOption::Comment(comment) => Some(comment.clone()),
        _ => None,
    }) else {
        return Ok(String::new());
    };
    // Go calls `validateCommentLength` from the RANGE definition loop ONLY
    // (`ddl/partition.go:1663`). The LIST loop (`:1571`) and
    // `buildHashPartitionDefinitions` (`:1454`) take the comment verbatim, so
    // a 1025-byte LIST or HASH partition comment is stored whole by Go and
    // was refused here with 1793.
    if !validate_length {
        return Ok(comment);
    }
    if comment.len() > MAX_PARTITION_COMMENT_LENGTH {
        let too_long = tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_TOO_LONG_TABLE_PARTITION_COMMENT.generate_with_stack_by_args(
                &[
                    (definition.name.clone()).to_string().into(),
                    MAX_PARTITION_COMMENT_LENGTH.into(),
                ],
            ),
        );
        if ctx.get_sql_mode().has_strict_mode() {
            return Err(too_long);
        }
        // Go `validateCommentLength` appends the very error it would have
        // returned as a WARNING and then truncates, so a non-strict session
        // still learns the comment was cut.
        let reported = too_long;
        ctx.append_warning(tidb_util::context::WarnErr::Terror(
            tidb_util::dbterror::ERR_TOO_LONG_TABLE_PARTITION_COMMENT.generate(reported.message),
        ));
        // Go truncates by BYTES.
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
#[must_use]
pub fn hex_if_non_print(value: &str) -> String {
    use tidb_hack::go_strconv::is_print as go_is_print;
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
    let unwrapped = unwrap_from_single_quotes(value);
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
    // Go `format.OutputFormat` (`parser/format/format.go:48`) replaces
    // exactly four runes: NUL -> `\0`, `'` -> `''`, LF -> `\n`, CR -> `\r`.
    // A BACKSLASH is NOT among them -- doubling it turned a stored `a\b`
    // into `a\\b`, and leaving a real newline unescaped broke the statement
    // across lines.
    let mut escaped = String::with_capacity(comment.len());
    for character in comment.chars() {
        match character {
            '\0' => escaped.push_str("\\0"),
            '\'' => escaped.push_str("''"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            other => escaped.push(other),
        }
    }
    format!(" COMMENT '{escaped}'")
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
) -> Result<(), MysqlError> {
    // Go's parser keeps `EXTRACT(unit FROM v)` as an `*ast.FuncCallExpr`
    // whose `Args[0]` is the time unit, so it arrives here through the same
    // door as every other call. THIS parser gives `EXTRACT` a dedicated
    // `Expr::Extract`, so the arm has to be entered on its own -- and the
    // unit Go reads out of `Args[0]` is the variant's own field. Matching
    // only `Expr::Func` skipped the entire `case ast.Extract:` rule set.
    match expr {
        Expr::Func { name, args, .. } => {
            let borrowed: Vec<&Expr> = args.iter().collect();
            check_partition_call_args(&name.to_ascii_lowercase(), None, &borrowed, names, types)
        }
        Expr::Extract { unit, value } => check_partition_call_args(
            "extract",
            Some(unit.as_str()),
            &[value.as_ref()],
            names,
            types,
        ),
        _ => Ok(()),
    }
}

/// Go `collectArgsType` (`ddl/partition.go:5024`): the declared type of each
/// COLUMN argument, in order; a non-column argument contributes nothing.
///
/// Go type-asserts `*ast.ColumnNameExpr` DIRECTLY and skips anything else, so
/// it does NOT look through parentheses -- `YEAR((d))` collects no argument
/// type at all, every positive rule then fails, and the call is 1486.
fn collect_args_type(
    args: &[&Expr],
    names: &[String],
    types: &[FieldType],
) -> Result<Vec<FieldTypeCode>, MysqlError> {
    let mut arg_types = Vec::new();
    for arg in args {
        let Expr::Column(path) = arg else {
            continue;
        };
        let Some(column) = path.last() else { continue };
        let Some(offset) = names
            .iter()
            .position(|candidate| candidate.go_lower_eq(column))
        else {
            // Go `findColumnByName` returning nil is 1054 naming the clause.
            // Go quotes `col.Name.Name.L`, the name the parser already folded,
            // so the message reads `Unknown column 'nosuch'` for a `NoSuch`
            // that was written in mixed case.
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                    tidb_hack::go_to_lower(go_to_lower(column)).into(),
                    ("partition function".to_owned()).to_string().into(),
                ]),
            ));
        };
        arg_types.push(types[offset].code());
    }
    Ok(arg_types)
}

/// Go `checkNoTimestampArgs` (`ddl/partition.go:5065`): the whitelisted
/// binary and unary operators refuse a TIMESTAMP operand, because a
/// TIMESTAMP's numeric value depends on the session time zone and rows would
/// route to different partitions for different sessions.
///
/// Go applies this INSIDE `checkPartitionExprAllowed`, on the operator node
/// itself, before the walk descends -- which is also why a missing column
/// under an operator reports 1054 from here rather than 1564 from the child.
fn check_no_timestamp_args(
    args: &[&Expr],
    names: &[String],
    types: &[FieldType],
) -> Result<(), MysqlError> {
    let arg_types = collect_args_type(args, names, types)?;
    if arg_types
        .iter()
        .any(|code| matches!(code, FieldTypeCode::Timestamp))
    {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC.generate_with_stack_by_args(&[]),
        ));
    }
    Ok(())
}

/// The `switch expr.FnName.L` of Go's `checkPartitionExprArgs`, shared by the
/// `Expr::Func` and `Expr::Extract` spellings of a call.
fn check_partition_call_args(
    name: &str,
    extract_unit: Option<&str>,
    args: &[&Expr],
    names: &[String],
    types: &[FieldType],
) -> Result<(), MysqlError> {
    let arg_types = collect_args_type(args, names, types)?;
    let has_date = arg_types
        .iter()
        .any(|code| matches!(code, FieldTypeCode::Date | FieldTypeCode::Datetime));
    let has_time = arg_types
        .iter()
        .any(|code| matches!(code, FieldTypeCode::Duration | FieldTypeCode::Datetime));
    let has_timestamp = arg_types
        .iter()
        .any(|code| matches!(code, FieldTypeCode::Timestamp));
    // Go `hasDatetimeArgs` is DATETIME only -- a DATE column does not satisfy
    // it, which is what separates the `DAY_HOUR` family from `DAY`.
    let has_datetime = arg_types
        .iter()
        .any(|code| matches!(code, FieldTypeCode::Datetime));
    let ok = |accepted: bool| {
        if accepted {
            Ok(())
        } else {
            Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC
                    .generate_with_stack_by_args(&[]),
            ))
        }
    };
    match name {
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
        // Go's `case ast.Extract:` switches on the UNIT, and its `default:`
        // is an unconditional 1486 -- so `EXTRACT(WEEK FROM d)` is refused
        // however good the argument is.
        "extract" => match extract_unit.unwrap_or_default() {
            "YEAR" | "YEAR_MONTH" | "QUARTER" | "MONTH" | "DAY" => ok(has_date),
            "DAY_MICROSECOND" | "DAY_HOUR" | "DAY_MINUTE" | "DAY_SECOND" => ok(has_datetime),
            "HOUR" | "HOUR_MINUTE" | "HOUR_SECOND" | "MINUTE" | "MINUTE_SECOND" | "SECOND"
            | "MICROSECOND" | "HOUR_MICROSECOND" | "MINUTE_MICROSECOND" | "SECOND_MICROSECOND" => {
                ok(has_time)
            }
            _ => Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC
                    .generate_with_stack_by_args(&[]),
            )),
        },
        // Go raises 1486 for a TIMESTAMP argument to these, because their
        // value depends on the session time zone.
        "abs" | "ceiling" | "floor" | "mod" => {
            if has_timestamp {
                Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC
                        .generate_with_stack_by_args(&[]),
                ))
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

/// The stored `LessThan` TEXT for one folded RANGE bound, which is what
/// `SHOW CREATE TABLE` prints (Go `AppendPartitionDefs`,
/// `ddl/partition.go:5204`).
///
/// Shared so that `ALTER TABLE ... ADD PARTITION` records the same text a
/// `CREATE` would: the renderer reads these strings, and a definition that
/// carries none has nothing to print.
#[must_use]
pub fn stored_range_bound_text(bound: RangeBound, unsigned: bool) -> String {
    match bound {
        RangeBound::MaxValue => PARTITION_MAX_VALUE.to_owned(),
        RangeBound::Value(value) if unsigned => format!("{}", value as u64),
        RangeBound::Value(value) => format!("{value}"),
    }
}

/// Go `checkPartitionNameUnique` (1517), matched case-insensitively as
/// partition names are.
/// Go `driver.UnwrapFromSingleQuotes` (`types/parser_driver/value_expr.go:196`).
///
/// It is not a quote strip: inside the quotes it also collapses the doubled
/// backslash and the doubled single quote that the stored spelling escaped
/// with. Removing only the outer quotes hexed the escape characters as well
/// as the value.
///
/// The guard is Go's own: a string shorter than two characters, or one that
/// is not quoted at BOTH ends, is returned untouched.
pub fn unwrap_from_single_quotes(value: &str) -> String {
    let bytes = value.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'\'' || bytes[bytes.len() - 1] != b'\'' {
        return value.to_owned();
    }
    value[1..value.len() - 1]
        .replace("\\\\", "\\")
        .replace("''", "'")
}

/// Go `stringutil.Escape(def.Name.O, sqlMode)` (`ddl/partition.go:5201`):
/// the partition name as an escaped identifier.
///
/// The doubling is what makes the output re-parseable -- a partition named
/// ``p`0`` printed as ``` `p`0` ``` reads as three tokens, and reloading the
/// dump fails. Go doubles the quote character it chose: a double quote under
/// `ANSI_QUOTES`, a backtick otherwise.
#[must_use]
pub fn escape_identifier(name: &str, ansi_quotes: bool) -> String {
    let quote = if ansi_quotes { '"' } else { '`' };
    let doubled: String = [quote, quote].iter().collect();
    format!("{quote}{}{quote}", name.replace(quote, &doubled))
}

/// Go `AppendPartitionDefs` (`ddl/partition.go:5196`): the whole definition
/// list, for every partitioning method.
///
/// Go has ONE of these. It prints each definition's name, then switches on
/// the partition TYPE for the values clause -- `VALUES LESS THAN` for RANGE,
/// `VALUES IN`/`DEFAULT` for LIST, and nothing at all for HASH, KEY and NONE
/// -- and every value it prints comes from the STORED text
/// (`def.LessThan`, `def.InValues`) run through `hexIfNonPrint`.
///
/// Five separate renderers stood here, four of which re-derived the values
/// from the folded routing structures. That is a second source of truth for
/// something Go reads straight out of the metadata, and it drifted in ways
/// only a comparison against Go could find: a LIST COLUMNS partition carrying
/// both real values and `DEFAULT` printed as a bare `DEFAULT`, losing the
/// values, and RANGE COLUMNS lost Go's `_binary` spelling.
#[must_use]
pub fn append_partition_defs(
    definitions: &[PartitionDef],
    kind: &PartitionKind,
    ansi_quotes: bool,
) -> String {
    let mut out = String::from("\n(");
    for (ordinal, definition) in definitions.iter().enumerate() {
        if ordinal > 0 {
            out.push_str(",\n ");
        }
        out.push_str("PARTITION ");
        out.push_str(&escape_identifier(&definition.name, ansi_quotes));
        match kind {
            PartitionKind::Range { .. } | PartitionKind::RangeColumns { .. } => {
                out.push_str(" VALUES LESS THAN (");
                for (position, bound) in definition.less_than.iter().enumerate() {
                    if position > 0 {
                        out.push(',');
                    }
                    out.push_str(&hex_if_non_print(bound));
                }
                out.push(')');
            }
            PartitionKind::List { .. } | PartitionKind::ListColumns { .. } => {
                // Go's two spellings of a DEFAULT partition: no stored values
                // at all, or the single literal word.
                let bare_default = definition.in_values.is_empty()
                    || (definition.in_values.len() == 1
                        && definition.in_values[0].len() == 1
                        && definition.in_values[0][0].eq_ignore_ascii_case("DEFAULT"));
                if bare_default {
                    out.push_str(" DEFAULT");
                } else {
                    out.push_str(" VALUES IN (");
                    for (index, tuple) in definition.in_values.iter().enumerate() {
                        if index > 0 {
                            out.push(',');
                        }
                        match tuple.as_slice() {
                            [single] => out.push_str(&hex_if_non_print(single)),
                            many => {
                                out.push('(');
                                for (position, value) in many.iter().enumerate() {
                                    if position > 0 {
                                        out.push(',');
                                    }
                                    out.push_str(&hex_if_non_print(value));
                                }
                                out.push(')');
                            }
                        }
                    }
                    out.push(')');
                }
            }
            // HASH, KEY and NONE have no values clause.
            PartitionKind::Hash | PartitionKind::Key | PartitionKind::None => {}
        }
        out.push_str(&partition_comment_text(&definition.comment));
        out.push_str(&partition_placement_text(
            definition.placement_policy.as_ref(),
            ansi_quotes,
        ));
    }
    out.push(')');
    out
}

/// Go `AppendPartitionDefs` (`ddl/partition.go:5241`): the
/// ` /*T![placement] PLACEMENT POLICY=<name> */` tail a definition carrying
/// a policy earns, after its comment.
///
/// The `/*T![placement] ... */` wrapper is TiDB's feature-gated comment
/// syntax: another MySQL-compatible parser skips it, while TiDB reads it
/// back. Printing the clause bare would make the dump unloadable elsewhere.
#[must_use]
pub fn partition_placement_text(
    reference: Option<&tidb_model::PolicyRefInfo>,
    ansi_quotes: bool,
) -> String {
    let Some(reference) = reference else {
        return String::new();
    };
    // Go escapes the policy NAME as an identifier, so one needing quoting
    // round-trips.
    format!(
        " /*T![placement] PLACEMENT POLICY={} */",
        escape_identifier(reference.name.original(), ansi_quotes)
    )
}

/// Whether two partition names denote the same partition.
///
/// Go compares `ast.CIStr.L` against `ast.CIStr.L` -- the `strings.ToLower`
/// fold the parser already applied -- so the comparison is Unicode-simple,
/// not ASCII-only. `eq_ignore_ascii_case` leaves every non-ASCII letter
/// case-sensitive, which made `DROP PARTITION` miss a partition named with a
/// non-ASCII capital that Go drops for the lowercase spelling.
#[must_use]
pub fn partition_names_equal(left: &str, right: &str) -> bool {
    go_to_lower(left) == go_to_lower(right)
}

/// Go's per-definition remainder of `buildPartitionDefinitionsInfo`: the
/// comment and the name, checked for THIS definition before the loop reads
/// the NEXT one's values (`ddl/partition.go:1565-1578` for LIST,
/// `:1661-1679` for RANGE).
///
/// Running the value folding and this as two separate passes reported the
/// wrong error whenever a statement was wrong in both ways -- a 65-character
/// name on definition 0 beside a bad bound on definition 1 is 1059 in Go, and
/// was the bound's error here.
///
/// Go validates the comment LENGTH only in its RANGE loop; the LIST loop and
/// `buildHashPartitionDefinitions` store whatever was written.
fn definition_tail<'a>(
    partitioning: &'a tidb_ast::TablePartitioning,
    built: &'a mut Vec<PartitionDef>,
    allocate_id: &'a mut dyn FnMut() -> i64,
    ctx: &'a tidb_expr::metabuild::Context,
    validate_comment_length: bool,
) -> impl FnMut(usize) -> Result<(), MysqlError> + 'a {
    move |ordinal: usize| {
        let Some(definition) = partitioning.definitions.get(ordinal) else {
            return Ok(());
        };
        let comment = partition_definition_comment(definition, ctx, validate_comment_length)?;
        check_too_long_partition_name(&definition.name)?;
        built.push(PartitionDef {
            id: allocate_id(),
            name: definition.name.clone(),
            // Filled by `build_table_partitioning`, which mirrors the stored
            // text back onto every definition once the spec is complete.
            less_than: Vec::new(),
            in_values: Vec::new(),
            comment,
            placement_policy: partition_definition_placement(definition),
            // Resolved from the table's ENGINE_ATTRIBUTE once the table is
            // built (`rebuildStorageClassForPartitions`).
            storage_class: Default::default(),
        });
        Ok(())
    }
}

/// Go `checkPartitionColumnsUnique` (`ddl/partition.go:4664`), which
/// `checkPartitionDefinitionConstraints` runs AFTER the name-uniqueness and
/// partition-count checks (`:5285`).
///
/// Go returns early for a list of one, and quotes `col.L` -- the folded name.
fn check_partition_columns_unique(columns: &[String]) -> Result<(), MysqlError> {
    if columns.len() <= 1 {
        return Ok(());
    }
    let mut seen: Vec<String> = Vec::with_capacity(columns.len());
    for column in columns {
        let folded = go_to_lower(column);
        if seen.contains(&folded) {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_SAME_NAME_PARTITION_FIELD
                    .generate_with_stack_by_args(&[folded.to_string().into()]),
            ));
        }
        seen.push(folded);
    }
    Ok(())
}

fn check_partition_name_unique(definitions: &[PartitionDef]) -> Result<(), MysqlError> {
    // Go keeps a map of the lowered names (`checkPartitionNameUnique`), one
    // pass; an INTERVAL clause can generate thousands of definitions.
    let mut seen = std::collections::HashSet::with_capacity(definitions.len());
    for definition in definitions {
        if !seen.insert(go_to_lower(&definition.name)) {
            // Go reports the LATER occurrence, original-cased
            // (`partition.go:1761`).
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_SAME_NAME_PARTITION
                    .generate_with_stack_by_args(&[(definition.name.clone()).to_string().into()]),
            ));
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
pub fn go_to_lower(name: &str) -> String {
    tidb_mysql::to_lowercase(name)
}

/// Go `checkTooLongTable` (`ddl/executor.go:864`), applied to a partition
/// name because a partition IS a physical table and reuses
/// `mysql.MaxTableNameLength`.
///
/// Counts RUNES, not bytes, so 64 CJK characters are legal where 65 ASCII
/// ones are not. Go runs it INSIDE the per-definition loop for RANGE and
/// LIST only -- HASH and KEY partition names are never length-checked at all,
/// which is a quirk of where the call sits rather than a rule.
pub fn check_too_long_partition_name(name: &str) -> Result<(), MysqlError> {
    const MAX_TABLE_NAME_LENGTH: usize = 64;
    if name.chars().count() > MAX_TABLE_NAME_LENGTH {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_TOO_LONG_IDENT
                .generate_with_stack_by_args(&[name.to_owned().into()]),
        ));
    }
    Ok(())
}

/// Go `checkUniqueKeyIncludePartKey`: a partition column is covered only by
/// a key part over the WHOLE column; a prefix part (`Length > 0`) is not.
pub fn key_part_covers(offsets: &[usize], prefix_lengths: &[i64], column: usize) -> bool {
    offsets
        .iter()
        .position(|offset| *offset == column)
        .is_some_and(|position| {
            prefix_lengths
                .get(position)
                .is_none_or(|length| *length <= 0)
        })
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
    indexes: &[IndexInfo],
    names: &[String],
    handle_offsets: &[usize],
    dependencies: &[usize],
) -> Result<(), MysqlError> {
    for index in indexes {
        if !index.unique {
            continue;
        }
        let (offsets, lengths) = index_key_parts(index, names);
        if dependencies
            .iter()
            .all(|offset| key_part_covers(&offsets, &lengths, *offset))
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
        if !handle_offsets.is_empty() && index.primary {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF
                    .generate_with_stack_by_args(&[("CLUSTERED INDEX".to_owned())
                        .to_string()
                        .into()]),
            ));
        }
        // Go `ddl/partition.go:703`: a GLOBAL index is exempt because its
        // entries share the logical table's index keyspace.
        if index.global {
            continue;
        }
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_GLOBAL_INDEX_NOT_EXPLICITLY_SET.generate_with_stack_by_args(
                &[(index.name.original().to_owned()).to_string().into()],
            ),
        ));
    }
    // A clustered primary key is no entry in `indexes` -- its encoding IS the
    // row key -- so Go checks it separately, and reports it as
    // `CLUSTERED INDEX` rather than by name.
    if !handle_offsets.is_empty()
        && !dependencies
            .iter()
            .all(|offset| handle_offsets.contains(offset))
    {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF
                .generate_with_stack_by_args(&[("CLUSTERED INDEX".to_owned()).to_string().into()]),
        ));
    }
    Ok(())
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
    indexes: &[IndexInfo],
    handle_offsets: &[usize],
    allocate_id: &mut dyn FnMut() -> i64,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Option<(StoredPartitionMetadata, PartitionSpec)>, MysqlError> {
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
    Ok(Some((
        stored_partition_metadata(partitioning, &spec, ctx)?,
        spec,
    )))
}

/// Go `TableFromMeta` -> `newPartitionedTable` over a `PartitionInfo` read
/// back from meta JSON: every stored string of the partitioning -- the
/// expression, the column list and each definition's `LessThan` / `InValues`
/// bound text -- has each byte that is not UTF-8 replaced by U+FFFD, and the
/// routing is rebuilt from those strings, so a latin1 client's bound `'x\xE9'`
/// routes as `'x\u{FFFD}'` (the stored text this tier renders from the
/// folded bound already reads that way; the routing built from the statement
/// still held the raw byte). `None` for a table that is not partitioned.
/// `names` / `types` are the table's visible columns as reloaded.
///
/// # Errors
///
/// What [`partition_spec_from_metadata`] raises for the reloaded strings.
pub fn reload_partition_spec_from_meta_json(
    spec: &PartitionSpec,
    names: &[String],
    types: &[FieldType],
) -> Option<Result<PartitionSpec, MysqlError>> {
    use tidb_table::kv_table::table_meta::go_json_text_round_trip;
    let reloaded = |text: &str| {
        let mut text = text.to_owned();
        go_json_text_round_trip(&mut text);
        text
    };
    let kind = match spec.kind {
        PartitionKind::Range { .. } | PartitionKind::RangeColumns { .. } => PartitionType::RANGE,
        PartitionKind::List { .. } | PartitionKind::ListColumns { .. } => PartitionType::LIST,
        PartitionKind::Hash => PartitionType::HASH,
        PartitionKind::Key => PartitionType::KEY,
        PartitionKind::None => return None,
    };
    // Go stores `pi.Expr` for a written expression and `pi.Columns` for a
    // written column list (`stored_partition_metadata`).
    let columns = match spec.kind {
        PartitionKind::RangeColumns { .. }
        | PartitionKind::ListColumns { .. }
        | PartitionKind::Key => spec
            .dependencies
            .iter()
            .map(|name| reloaded(name))
            .collect(),
        _ => Vec::new(),
    };
    let expr_text = if columns.is_empty() {
        reloaded(&spec.expr_text)
    } else {
        String::new()
    };
    let definitions = spec
        .definitions
        .iter()
        .map(|definition| StoredPartitionDefinition {
            id: definition.id,
            name: reloaded(&definition.name),
            less_than: definition
                .less_than
                .iter()
                .map(|bound| reloaded(bound))
                .collect(),
            in_values: definition
                .in_values
                .iter()
                .map(|tuple| tuple.iter().map(|value| reloaded(value)).collect())
                .collect(),
            comment: reloaded(&definition.comment),
            placement_policy: definition.placement_policy.clone(),
            storage_class: definition.storage_class.clone(),
        })
        .collect::<Vec<_>>();
    Some(partition_spec_from_metadata(
        kind,
        &expr_text,
        &columns,
        spec.is_empty_columns,
        &definitions,
        &spec.overlapping_dropping_partition_indices,
        names,
        types,
    ))
}

/// The STORED partition metadata a partitioning clause and the routing spec
/// it built describe (Go `buildTablePartitionInfo` and
/// `buildPartitionDefinitionsInfo`), the definition ids left at zero.
///
/// # Errors
///
/// A bound that cannot be rendered back.
pub fn stored_partition_metadata(
    partitioning: &tidb_ast::TablePartitioning,
    spec: &PartitionSpec,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<StoredPartitionMetadata, MysqlError> {
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
    let definitions = stored_definitions_for(partitioning, spec, ctx)?;
    Ok(StoredPartitionMetadata {
        kind: method.kind,
        // Every method this tier builds a spec for is one Go enables; the
        // ones Go stores disabled it refuses outright, above.
        enable: true,
        num: method.count,
        expr,
        columns,
        is_empty_columns,
        definitions,
    })
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
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Vec<StoredPartitionDefinition>, MysqlError> {
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
            placement_policy: definition.placement_policy.clone(),
            storage_class: definition.storage_class.clone(),
        };
        match &spec.kind {
            // HASH, KEY and NONE definitions carry a name and nothing else.
            PartitionKind::Hash | PartitionKind::Key | PartitionKind::None => {}
            PartitionKind::Range {
                less_than,
                unsigned,
            } => {
                entry.less_than = vec![stored_range_bound_text(
                    less_than
                        .get(ordinal)
                        .copied()
                        .unwrap_or(RangeBound::MaxValue),
                    *unsigned,
                )];
            }
            PartitionKind::RangeColumns {
                less_than,
                field_types,
            } => {
                let bounds = less_than.get(ordinal).map_or(&[][..], Vec::as_slice);
                entry.less_than = stored_range_columns_bound_text(bounds, field_types)?;
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

/// One RANGE COLUMNS bound tuple in the text Go stores
/// (`generatePartValuesWithTp`), `MAXVALUE` as that word.
pub fn stored_range_columns_bound_text(
    bounds: &[RangeColumnBound],
    field_types: &[FieldType],
) -> Result<Vec<String>, MysqlError> {
    bounds
        .iter()
        .enumerate()
        .map(|(position, bound)| match bound {
            RangeColumnBound::MaxValue => Ok(PARTITION_MAX_VALUE.to_owned()),
            RangeColumnBound::Value(datum) => stored_value_text(datum, field_types.get(position)),
        })
        .collect()
}

/// One folded value in the text form Go stores it as, which is Go
/// `generatePartValuesWithTp`.
///
/// The dispatch is on the COLUMN's type, not the datum's, exactly as Go's
/// is: a value has already been converted to its column's type when it was
/// folded, and the one case that cannot be read off the datum -- a BINARY
/// column, whose bytes need not be valid UTF-8 -- is a property of the
/// column alone.
fn stored_value_text(datum: &Datum, field_type: Option<&FieldType>) -> Result<String, MysqlError> {
    use tidb_datatype::EvalType;
    if matches!(datum, Datum::Null) {
        return Ok("NULL".to_owned());
    }
    let rendered = || {
        datum
            .restore_value_expr()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .map_err(|_| {
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE
                        .generate_with_stack_by_args(&[]),
                )
            })
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
            let binary =
                field_type.is_some_and(|ft| ft.charset() == tidb_datatype::Charset::Binary);
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
        // Go's arm is `case types.ETDatetime, types.ETDuration:` -- there is
        // no `ETTimestamp` in it. A TIMESTAMP partitioning column is refused
        // long before this by `isColTypeAllowedAsPartitioningCol` (1659), so
        // the extra arm was unreachable rather than wrong; it is dropped so
        // the two switches read alike and cannot diverge if that changes.
        Some(EvalType::Datetime | EvalType::Duration) => rendered(),
        // Go's switch has no other arm and returns
        // `ErrWrongTypeColumnValue` for anything else.
        _ => Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[]),
        )),
    }
}

/// The stored `InValues` tuples for one LIST definition.
pub fn stored_in_values(
    written: Option<&PartitionDefinition>,
    field_types: &[FieldType],
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Vec<Vec<String>>, MysqlError> {
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
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE
                        .generate_with_stack_by_args(&[]),
                ))
            }
            PartitionValue::Expr(expr) => {
                tuples.push(vec![stored_list_component(expr, field_types.first(), ctx)?]);
            }
            PartitionValue::Tuple(components) => {
                let mut tuple = Vec::with_capacity(components.len());
                for (position, component) in components.iter().enumerate() {
                    tuple.push(stored_list_component(
                        component,
                        field_types.get(position),
                        ctx,
                    )?);
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
    ctx: &tidb_expr::metabuild::Context,
) -> Result<String, MysqlError> {
    match field_type {
        Some(field_type) => {
            let datum = super::table_partition_list::fold_column_value(expr, field_type, ctx)?;
            stored_value_text(&datum, Some(field_type))
        }
        None => Ok(expr.restore_with_flags(partition_restore_flags())),
    }
}


#[cfg(test)]
mod load_permissiveness_tests {
    use super::*;

    /// Go's LOADER re-judges nothing. `newPartitionExpr`
    /// (`tables/partition.go:306`) rebuilds what a DDL already validated, and
    /// the CREATE-time rules live on the CREATE path alone.
    ///
    /// That asymmetry is the whole point: a table a Go cluster is serving
    /// must open here even when this node's CREATE would refuse the same
    /// shape. Round-trip tests cannot cover it, because they start from a
    /// CREATE and so only ever produce metadata CREATE accepts.
    fn definition(
        id: i64,
        name: &str,
        less_than: &[&str],
        in_values: &[&[&str]],
    ) -> StoredPartitionDefinition {
        StoredPartitionDefinition {
            id,
            name: name.to_owned(),
            less_than: less_than.iter().map(|bound| (*bound).to_owned()).collect(),
            in_values: in_values
                .iter()
                .map(|tuple| tuple.iter().map(|value| (*value).to_owned()).collect())
                .collect(),
            comment: String::new(),
            placement_policy: None,
            storage_class: Default::default(),
        }
    }

    fn int_column() -> (Vec<String>, Vec<FieldType>) {
        (
            vec!["a".to_owned()],
            vec![FieldType::new(FieldTypeCode::Long)],
        )
    }

    #[test]
    fn a_list_table_with_a_repeated_value_loads() {
        // `checkListPartitionValue`'s 1495 is CREATE-only. Go's loader fills a
        // map, so a repeated value is simply owned by the LAST definition that
        // claims it -- refusing here would make a table a Go cluster serves
        // unopenable.
        let (names, types) = int_column();
        let spec = partition_spec_from_metadata(
            PartitionType::LIST,
            "`a`",
            &[],
            false,
            &[
                definition(1, "p0", &[], &[&["1"], &["2"]]),
                definition(2, "p1", &[], &[&["2"], &["3"]]),
            ],
            &[],
            &names,
            &types,
        )
        .expect("a Go cluster's LIST metadata loads even with a repeated value");
        let PartitionKind::List { values, .. } = &spec.kind else {
            panic!("a scalar LIST spec");
        };
        // The repeated value belongs to the LAST claimant, as Go's map does.
        let owner = values
            .iter()
            .find(|(value, _)| *value == 2)
            .map(|(_, ordinal)| *ordinal);
        assert_eq!(owner, Some(1), "the later definition owns the repeat");
    }

    #[test]
    fn a_range_table_whose_bounds_do_not_increase_loads() {
        // `checkPartitionByRange`'s 1493 is CREATE-only for the same reason.
        let (names, types) = int_column();
        partition_spec_from_metadata(
            PartitionType::RANGE,
            "`a`",
            &[],
            false,
            &[
                definition(1, "p0", &["10"], &[]),
                definition(2, "p1", &["5"], &[]),
            ],
            &[],
            &names,
            &types,
        )
        .expect("the loader does not re-judge bounds it did not write");
    }

    #[test]
    fn a_table_mid_repartition_loads_and_routes_to_partition_zero() {
        // `PartitionTypeNone` is what a table WEARS during
        // `ALTER TABLE ... PARTITION BY`. Go returns a nil expression for it
        // (`tables/partition.go:313`) and routes every row to index 0
        // (`:1453`). Refusing it made an ordinary table unreadable for the
        // length of the reorg.
        let (names, types) = int_column();
        let spec = partition_spec_from_metadata(
            PartitionType::NONE,
            "",
            &[],
            false,
            &[definition(1, "p0", &[], &[])],
            &[],
            &names,
            &types,
        )
        .expect("a table mid-repartition opens");
        assert!(matches!(spec.kind, PartitionKind::None));
    }

    #[test]
    fn online_drop_overlap_mapping_survives_metadata_rebuild() {
        let (names, types) = int_column();
        let spec = partition_spec_from_metadata(
            PartitionType::RANGE,
            "`a`",
            &[],
            false,
            &[
                definition(1, "p0", &["10"], &[]),
                definition(2, "p1", &["MAXVALUE"], &[]),
            ],
            &[Some(1), Some(1)],
            &names,
            &types,
        )
        .expect("online DROP PARTITION metadata rebuilds");

        assert_eq!(spec.overlapping_dropping_partition_index(0), Some(1));
        assert_eq!(spec.overlapping_dropping_partition_index(1), Some(1));
    }
}
