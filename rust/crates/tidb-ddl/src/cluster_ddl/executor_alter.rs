// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/ddl/executor.go`: `AlterTable` calls each specification's method.

use super::add_index::AddIndexSubmission;
use super::*;

/// Go `executor.AlterTable`: one specification builds its typed Go job.
#[allow(clippy::too_many_arguments)]
fn build_alter_table_spec<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    alter: &AlterTableStmt,
    action: &tidb_ast::AlterTableAction,
    default_schema: &str,
    context: &DdlContext,
    store_count: usize,
    multi_schema: bool,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
    has_null: &mut dyn FnMut(&str, &TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    match action {
        tidb_ast::AlterTableAction::AddIndexConstraint(index) => create_index_from_constraint(
            snapshot,
            catalog,
            alter,
            index,
            default_schema,
            context,
            store_count,
            has_null,
        ),
        tidb_ast::AlterTableAction::AddForeignKey(definition) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            create_foreign_key_job(
                snapshot,
                catalog,
                &schema,
                &table,
                definition,
                context,
                store_count,
            )
        }
        tidb_ast::AlterTableAction::Cache(mode) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_table_cache(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(*mode == tidb_ast::AlterTableCacheMode::Cache),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::DropForeignKey(drop) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::drop_foreign_key(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(drop.name.clone()),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::DropIndex { if_exists, name } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            drop_index::drop_index_by_name(
                snapshot,
                catalog,
                &schema,
                &table,
                name,
                *if_exists,
                false,
                context.as_ref(),
            )
        }
        tidb_ast::AlterTableAction::RenameTable { new_name } => {
            let (from_schema, from_table) =
                split_name(&alter.name, default_schema, "renamed table")?;
            let (to_schema, to_table) = split_name(new_name, default_schema, "new table name")?;
            // Retain identity ALTER renames so planning resolves the source before
            // returning a no-op, as Go does.
            rename_table::rename_tables_job(
                catalog,
                &[RenameTablePair {
                    from_schema: from_schema,
                    from_table: from_table,
                    to_schema: to_schema,
                    to_table: to_table,
                }],
                true,
                context.as_ref(),
            )
        }
        // Go `AlterTableSetRegionSplitPolicy`.
        tidb_ast::AlterTableAction::SplitRegion { target, option } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_table_set_region_split_policy(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(target.clone()),
                &(option.clone()),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::SetTiFlashReplica {
            hypo,
            count,
            labels,
        } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_table_set_ti_flash_replica(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(*count),
                &(labels.clone()),
                &(false),
                &(*hypo),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        // Go `AlterTable` handles a spec's TTL, TTL_ENABLE and
        // TTL_JOB_INTERVAL once, as one `AlterTableTTLInfoOrEnable`.
        tidb_ast::AlterTableAction::SetTableOptions { options }
            if !options.is_empty() && options.iter().all(is_ttl_table_option) =>
        {
            let ttl_info = tidb_ddl_build::ttl_info_from_options(options)
                .map_err(DdlAdmissionError::from_mysql_error)?;
            let enable = options.iter().rev().find_map(|option| match option {
                tidb_ast::TableOption::TtlEnable(enabled) => Some(*enabled),
                _ => None,
            });
            let job_interval = options.iter().rev().find_map(|option| match option {
                tidb_ast::TableOption::TtlJobInterval(interval) => Some(interval.clone()),
                _ => None,
            });
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_table_ttl_info(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(ttl_info.map(Box::new)),
                &(enable),
                &(job_interval),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::RemoveTtl(_) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_table_remove_ttl(
                snapshot,
                catalog,
                &(schema),
                &(table),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::SetTableOptions { options }
            if !options.is_empty()
                && options.iter().all(|option| {
                    matches!(
                        option,
                        tidb_ast::TableOption::CharacterSet(_) | tidb_ast::TableOption::Collate(_)
                    )
                }) =>
        {
            // Go's `AlterTableOption` loop handles every charset/collate
            // option at once: `GetCharsetAndCollateInTableOption` from the
            // first, then one `AlterTableCharsetAndCollate` job.
            let (charset, collate) =
                tidb_ddl_build::column_types::get_charset_and_collate_in_table_option(
                    0,
                    options,
                    context.default_collation_for_utf8mb4(),
                )
                .map_err(DdlAdmissionError::from_mysql_error)?;
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_table_charset_and_collate(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(charset),
                &(collate),
                &(false),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::SetTableOptions { options } => {
            // Go folds every ENGINE_ATTRIBUTE / STORAGE_CLASS written in one
            // specification into a single job, validating each (8270 / 8271).
            if options.len() > 1
                && options.iter().all(|option| {
                    matches!(
                        option,
                        tidb_ast::TableOption::EngineAttribute(_)
                            | tidb_ast::TableOption::StorageClass(_)
                    )
                })
            {
                let engine_attribute =
                    tidb_ddl_build::storage_class::get_engine_attribute_from_storage_class_table_options(options)
                        .map_err(DdlAdmissionError::from_mysql_error)?
                        .expect("the options are all storage-class options");
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return alter_table::alter_table_engine_attribute(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(engine_attribute),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                );
            }
            let [option] = options.as_slice() else {
                return Ok(None);
            };
            if let tidb_ast::TableOption::AutoIdCache(value) = option {
                let new_cache = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain("AUTO_ID_CACHE needs an integer value")
                })?;
                if new_cache > i64::MAX as u64 {
                    return Err(DdlAdmissionError::plain(
                        "table option auto_id_cache overflows int64",
                    )
                    .into());
                }
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return alter_table::alter_table_auto_id_cache(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(new_cache as i64),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                );
            }
            if let tidb_ast::TableOption::EngineAttribute(_)
            | tidb_ast::TableOption::StorageClass(_) = option
            {
                let engine_attribute =
                    tidb_ddl_build::storage_class::get_engine_attribute_from_storage_class_table_options(options)
                        .map_err(DdlAdmissionError::from_mysql_error)?
                        .expect("the one option is a storage-class option");
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return alter_table::alter_table_engine_attribute(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(engine_attribute),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                );
            }
            if let tidb_ast::TableOption::Affinity(level) = option {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return alter_table::alter_table_affinity(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(level.clone()),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                );
            }
            if let tidb_ast::TableOption::PlacementPolicy(policy) = option {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return alter_table::alter_table_placement(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(policy.clone()),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                );
            }
            let ignored = match option {
                tidb_ast::TableOption::Engine(_) => Some("ENGINE"),
                tidb_ast::TableOption::RowFormat(_) => Some("ROW_FORMAT"),
                _ => None,
            };
            if let Some(option) = ignored {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return alter_table_no_job(catalog, &schema, &table, None);
            }
            if let tidb_ast::TableOption::AutoIncrement(value)
            | tidb_ast::TableOption::ForceAutoIncrement(value) = option
            {
                // Go parses the option into `opt.UintValue`, so the written
                // value is an unsigned literal that is then handed to
                // `RebaseAutoID` as an `int64`.
                let new_base = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain("AUTO_INCREMENT needs an integer value")
                })? as i64;
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return alter_table::rebase_auto_id(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(new_base),
                    &(matches!(option, tidb_ast::TableOption::ForceAutoIncrement(_))),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                );
            }
            if let tidb_ast::TableOption::Comment(comment) = option {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                // Go `AlterTableComment` validates the length against the
                // statement's SQL mode BEFORE it publishes the job: strict
                // refuses, non-strict warns and truncates.
                let comment = tidb_ddl_build::validate_comment_length(
                    comment,
                    &tidb_hack::go_to_lower(&table),
                    tidb_ddl_build::CommentOwner::Table,
                    &context.metabuild_context(),
                )
                .map_err(DdlAdmissionError::from_mysql_error)?;
                return alter_table::alter_table_comment(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(comment),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                );
            }
            // Go `AlterTable` clamps the value to `MaxShardRowIDBits` before
            // `ShardRowID`.
            if let tidb_ast::TableOption::ShardRowIdBits(value) = option {
                let bits = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain("SHARD_ROW_ID_BITS needs an integer value")
                })?;
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return alter_table::shard_row_id(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(bits.min(tidb_vardef::tidb_vars::MAX_SHARD_ROW_ID_BITS)),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                );
            }
            let (value, force) = match option {
                tidb_ast::TableOption::AutoRandomBase(value) => (value, false),
                tidb_ast::TableOption::ForceAutoRandomBase(value) => (value, true),
                // Options Go's `AlterTable` switch runs that are planned
                // elsewhere (charset/collate, TTL).
                tidb_ast::TableOption::CharacterSet(_)
                | tidb_ast::TableOption::Collate(_)
                | tidb_ast::TableOption::Ttl { .. }
                | tidb_ast::TableOption::TtlEnable(_)
                | tidb_ast::TableOption::TtlJobInterval(_) => return Ok(None),
                // Go `AlterTable`'s option switch default:
                // `dbterror.ErrUnsupportedAlterTableOption`.
                _ => {
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_dbterror::ERR_UNSUPPORTED_ALTER_TABLE_OPTION
                            .gen_with_stack_by_args(&[]),
                    )
                    .into())
                }
            };
            let next = value
                .parse::<u64>()
                .map_err(|_| DdlAdmissionError::plain("AUTO_RANDOM_BASE needs an integer value"))?
                as i64;
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::rebase_auto_random_id(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(next),
                &(force),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::ModifyColumn {
            if_exists,
            column,
            position,
        } => {
            // Go `GetModifiableColumnJob` handles an AUTO_RANDOM change too
            // (`checkAutoRandom`, then the owner's
            // `checkAndApplyAutoRandomBits`).
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            modify_column::modify_column_with_info(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(Box::new(column.clone())),
                &(position.clone()),
                &(DdlStatementContext(context.clone())),
                &(None),
                &(*if_exists),
                context.as_ref(),
                store_count,
                has_null,
            )
        }
        tidb_ast::AlterTableAction::ChangeColumn {
            if_exists,
            old_name,
            column,
            position,
        } => {
            let [old] = old_name.as_slice() else {
                return Err(DdlAdmissionError::plain(
                    "CHANGE COLUMN takes an unqualified source column name here",
                )
                .into());
            };
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            return modify_column::modify_column_with_info(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(Box::new(column.clone())),
                &(position.clone()),
                &(DdlStatementContext(context.clone())),
                &(Some(old.clone())),
                &(*if_exists),
                context.as_ref(),
                store_count,
                has_null,
            );
        }
        tidb_ast::AlterTableAction::RenameColumn(rename) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            return modify_column::rename_column(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(rename.from.clone()),
                &(rename.to.clone()),
                context.as_ref(),
                store_count,
                has_null,
            );
        }
        tidb_ast::AlterTableAction::DropColumn { if_exists, name } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            drop_column::drop_column_by_name(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(*if_exists),
                &(name.clone()),
                &(context.allow_remove_auto_inc()),
                context.as_ref(),
            )
        }
        tidb_ast::AlterTableAction::ConvertCharacterSet { charset, collation } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            if let Some(name) = charset {
                // go's CONVERT TO accepts any identifier at the grammar and
                // answers `ErrUnknownCharacterSet` (1115) when the name is
                // not a charset — a collation name, say.
                if tidb_charset::get_charset_info(name).is_err() {
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_dbterror::ERR_UNKNOWN_CHARACTER_SET
                            .gen_with_stack_by_args(&[name.into()]),
                    )
                    .into());
                }
            }
            alter_table::alter_table_charset_and_collate(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(charset.clone().unwrap_or_default()),
                &(collation.clone().unwrap_or_default()),
                &(true),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::DropPrimaryKey(_) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            drop_index::drop_index_by_name(
                snapshot,
                catalog,
                &schema,
                &table,
                &"PRIMARY".to_owned(),
                false,
                false,
                context.as_ref(),
            )
        }
        tidb_ast::AlterTableAction::RenameIndex(action) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::rename_index(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(action.from.clone()),
                &(action.to.clone()),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::AlterColumnDefault(action) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            let Some(column) = action.name.last() else {
                return Err(DdlAdmissionError::plain("ALTER COLUMN needs a column name").into());
            };
            alter_table::alter_column(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(column.clone()),
                &(action.default_value.clone().map(Box::new)),
                &(DdlStatementContext(context.clone())),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::OrderByColumns { .. } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            order_by_columns(catalog, &schema, &table)
        }
        tidb_ast::AlterTableAction::AlterIndexVisibility(action) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_index_visibility(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(action.name.clone()),
                &(action.visibility == tidb_ast::IndexVisibility::Invisible),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::AddColumn {
            if_not_exists,
            column,
            position,
        } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            add_column::add_column_with_info(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(*if_not_exists),
                &(Box::new(column.clone())),
                &(position.clone()),
                &(DdlStatementContext(context.clone())),
                context.as_ref(),
            )
        }
        tidb_ast::AlterTableAction::AddCheck(definition) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            if !context.enable_check_constraint() {
                context.append_warning_parts(1105, "tidb_enable_check_constraint is off");
                return alter_table_no_job(catalog, &schema, &table, None);
            }
            create_check_constraint_job(
                catalog,
                &(schema),
                &(table),
                &(Box::new(definition.clone())),
                &(DdlStatementContext(context.clone())),
            )
            .map(|job| Some(AddIndexSubmission::Job(Box::new(job), Vec::new())))
        }
        tidb_ast::AlterTableAction::DropCheck(drop) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            drop_check_constraint_job(
                catalog,
                &(schema),
                &(table),
                &(drop.name.clone()),
                &(DdlStatementContext(context.clone())),
            )
            .map(|job| Some(AddIndexSubmission::Job(Box::new(job), Vec::new())))
        }
        tidb_ast::AlterTableAction::AlterCheck(action) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            if !context.enable_check_constraint() {
                context.append_warning_parts(1105, "tidb_enable_check_constraint is off");
                return alter_table_no_job(catalog, &schema, &table, None);
            }
            alter_check_constraint_job(
                catalog,
                &(schema),
                &(table),
                &(action.name.clone()),
                &(action.enforced),
                &(DdlStatementContext(context.clone())),
            )
            .map(|job| Some(AddIndexSubmission::Job(Box::new(job), Vec::new())))
        }
        tidb_ast::AlterTableAction::Partition(
            action @ tidb_ast::AlterPartitionAction::Add { .. },
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            match partition::add_table_partitions_job(
                snapshot,
                catalog,
                &schema,
                &table,
                &Box::new(action.clone()),
                context.as_ref(),
            )? {
                Some(built) => Ok(Some(built)),
                None => reorganize_partition::reorganize_partitions_job(
                    snapshot,
                    catalog,
                    &schema,
                    &table,
                    action,
                    context,
                    context.as_ref(),
                    store_count,
                ),
            }
        }
        // Go `AlterTablePartition` for INTERVAL partitioning: LAST PARTITION
        // LESS THAN is an ADD PARTITION job of the generated definitions ...
        tidb_ast::AlterTableAction::Partition(
            action @ tidb_ast::AlterPartitionAction::LastPartitionLessThan { .. },
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            match partition::add_table_partitions_job(
                snapshot,
                catalog,
                &schema,
                &table,
                &Box::new(action.clone()),
                context.as_ref(),
            )? {
                Some(built) => Ok(Some(built)),
                None => reorganize_partition::reorganize_partitions_job(
                    snapshot,
                    catalog,
                    &schema,
                    &table,
                    action,
                    context,
                    context.as_ref(),
                    store_count,
                ),
            }
        }
        // ... and FIRST PARTITION LESS THAN a DROP PARTITION job of the
        // definitions below the bound.
        tidb_ast::AlterTableAction::Partition(
            action @ (tidb_ast::AlterPartitionAction::Drop { .. }
            | tidb_ast::AlterPartitionAction::FirstPartitionLessThan { .. }),
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            partition::drop_table_partition_job(
                snapshot,
                catalog,
                &schema,
                &table,
                &Box::new(action.clone()),
                context.as_ref(),
            )
        }
        tidb_ast::AlterTableAction::Partition(
            action @ (tidb_ast::AlterPartitionAction::Reorganize { .. }
            | tidb_ast::AlterPartitionAction::Repartition(_)
            | tidb_ast::AlterPartitionAction::RemovePartitioning
            | tidb_ast::AlterPartitionAction::Coalesce { .. }),
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            reorganize_partition::reorganize_partitions_job(
                snapshot,
                catalog,
                &schema,
                &table,
                action,
                context,
                context.as_ref(),
                store_count,
            )
        }
        tidb_ast::AlterTableAction::Partition(
            action @ tidb_ast::AlterPartitionAction::Truncate { .. },
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            partition::truncate_table_partition_job(
                snapshot,
                catalog,
                &schema,
                &table,
                &Box::new(action.clone()),
                context.as_ref(),
            )
        }
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Exchange {
            partition,
            table: standalone,
            with_validation,
        }) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            let (standalone_schema, standalone_table) =
                split_name(standalone, default_schema, "exchange table")?;
            exchange_partition::exchange_table_partition_job(
                catalog,
                &(schema),
                &(table),
                &(partition.clone()),
                &(standalone_schema),
                &(standalone_table),
                &(*with_validation),
                context.as_ref(),
            )
        }
        // Go `AlterTablePartitionOptions`: PLACEMENT POLICY is the one
        // partition option; the last one written wins.
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::SetOptions {
            partition,
            options,
        }) => {
            let mut policy = None;
            for option in options {
                match option {
                    tidb_ast::TableOption::PlacementPolicy(name) => policy = Some(name.clone()),
                    _ => {
                        return Err(
                            DdlAdmissionError::plain("unknown partition option".to_owned()).into(),
                        )
                    }
                }
            }
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            match policy {
                Some(policy) => alter_table::alter_table_partition_placement(
                    snapshot,
                    catalog,
                    &(schema),
                    &(table),
                    &(partition.clone()),
                    &(policy),
                    context.as_ref(),
                    multi_schema,
                    tiflash_stores,
                ),
                None => alter_table_no_job(catalog, &schema, &table, None),
            }
        }
        tidb_ast::AlterTableAction::SetAttributes(attributes) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_table_attributes(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(attributes.clone()),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::SetAttributes {
            partition,
            attributes,
        }) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table::alter_table_partition_attributes(
                snapshot,
                catalog,
                &(schema),
                &(table),
                &(partition.clone()),
                &(attributes.clone()),
                context.as_ref(),
                multi_schema,
                tiflash_stores,
            )
        }
        tidb_ast::AlterTableAction::WithValidation => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table_no_job(
                catalog,
                &schema,
                &table,
                Some((
                    8200,
                    "ALTER TABLE WITH VALIDATION is currently unsupported".to_owned(),
                )),
            )
        }
        // go raises 8200 for the WITHOUT VALIDATION option-only form too,
        // naming its own spelling (oracle g-alter: `ALTER TABLE ab3 WITHOUT
        // VALIDATION` answers `ALTER TABLE WITHOUT VALIDATION is currently
        // unsupported`); ALGORITHM/LOCK stay silent.
        tidb_ast::AlterTableAction::WithoutValidation => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table_no_job(
                catalog,
                &schema,
                &table,
                Some((
                    8200,
                    "ALTER TABLE WITHOUT VALIDATION is currently unsupported".to_owned(),
                )),
            )
        }
        // Go `AlterTable`: ENABLE / DISABLE KEYS are MyISAM-only; nothing
        // to do.
        tidb_ast::AlterTableAction::SetKeysEnabled(_) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            alter_table_no_job(catalog, &schema, &table, None)
        }
        tidb_ast::AlterTableAction::SetStatsOptions(_) => Err(DdlAdmissionError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ALTER_TABLE_OPTION.gen_with_stack_by_args(&[]),
        )
        .into()),
        // Go `AlterTable`'s default arm: the specifications it has no job for.
        tidb_ast::AlterTableAction::Force
        | tidb_ast::AlterTableAction::TablespaceImport(_)
        | tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::ImportTablespace { .. }
            | tidb_ast::AlterPartitionAction::DiscardTablespace { .. },
        ) => Err(DdlAdmissionError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ALTER_TABLE_SPEC.gen_with_stack_by_args(&[]),
        )
        .into()),
        _ => Ok(None),
    }
}

/// Go `executor.CreatePrimaryKey` / `createIndex` for an ADD CONSTRAINT spec.
fn create_index_from_constraint<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    alter: &AlterTableStmt,
    index: &IndexConstraintDefinition,
    default_schema: &str,
    context: &DdlContext,
    store_count: usize,
    has_null: &mut dyn FnMut(&str, &TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    // go's `ADD PRIMARY KEY` is a unique index named PRIMARY whose columns
    // gain NOT NULL+PRI — the same shape a CREATE TABLE PK carries
    // (`ALTER TABLE pf ADD PRIMARY KEY(b(2))` → `PRIMARY KEY (b(2))
    // /*T![clustered_index] NONCLUSTERED */` with the columns stamped
    // NOT NULL), so it lowers through the same CreateIndex statement the
    // other index kinds use.
    let primary = index.kind == IndexConstraintKind::PrimaryKey;
    // Go `CreatePrimaryKey` refuses a clustered key before it looks at the
    // table.
    if primary
        && matches!(
            index.options.primary_key_storage,
            Some(tidb_ast::PrimaryKeyStorage::Clustered)
        )
    {
        return Err(DdlAdmissionError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_MODIFY_PRIMARY_KEY.gen_with_stack(
                "Adding clustered primary key is not supported. Please consider adding \
             NONCLUSTERED primary key instead",
            ),
        )
        .into());
    }
    let kind = match index.kind {
        IndexConstraintKind::Key | IndexConstraintKind::Index => tidb_ast::IndexKind::Ordinary,
        IndexConstraintKind::Unique
        | IndexConstraintKind::UniqueKey
        | IndexConstraintKind::UniqueIndex => tidb_ast::IndexKind::Unique,
        IndexConstraintKind::PrimaryKey => tidb_ast::IndexKind::Unique,
        IndexConstraintKind::Fulltext => tidb_ast::IndexKind::Fulltext,
        IndexConstraintKind::Vector => tidb_ast::IndexKind::Vector,
        IndexConstraintKind::Columnar => tidb_ast::IndexKind::Columnar,
    };
    // go's `ADD PRIMARY KEY` has a FIXED reserved name (`PRIMARY`); any
    // other anonymous index stays unnamed here, and the plan names it with
    // `GetName4AnonymousIndex` against the loaded table, as Go's
    // `checkIndexNameAndColumns` does.
    let name = match index.name.as_ref().filter(|name| !name.is_empty()) {
        Some(name) => name.clone(),
        None if primary => "PRIMARY".to_owned(),
        None => String::new(),
    };
    create_index_job(
        snapshot,
        catalog,
        &CreateIndexStmt {
            kind,
            if_not_exists: index.if_not_exists,
            name: name.clone(),
            table: alter.name.clone(),
            parts: index.parts.clone(),
            options: index.options.clone(),
            online: Default::default(),
        },
        default_schema,
        primary,
        context,
        store_count,
        has_null,
    )
}

/// Go `executor.AlterTable`: accepted specifications without a job still resolve the table.
fn alter_table_no_job(
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
    warning: Option<(u16, String)>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    locate_table(catalog, schema, table)?;
    Ok(Some(AddIndexSubmission::AlreadySatisfied {
        detail: format!("ALTER TABLE `{schema}`.`{table}` changed nothing"),
        warnings: warning
            .into_iter()
            .map(|(code, message)| {
                tidb_context::SqlWarn::with_code(WarningLevel::Warning, code, message)
            })
            .collect(),
    }))
}

/// Go `executor.OrderByColumns`.
fn order_by_columns(
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (_, stored) = locate_table(catalog, schema, table)?;
    let has_primary_key = stored.read().columns.iter_deref().any(|column| {
        column
            .read()
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
    });
    alter_table_no_job(catalog, schema, table, has_primary_key.then(|| (1105, format!("ORDER BY ignored as there is a user-defined clustered index in the table '{table}'"))))
}

/// Go `executor.CreateForeignKey`: its optional implicit index shares one parent job.
fn create_foreign_key_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
    definition: &tidb_ast::ForeignKeyConstraintDefinition,
    context: &DdlContext,
    store_count: usize,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (_, stored) = locate_table(catalog, schema, table)?;
    let name = foreign_key::foreign_key_name(definition, &stored.read(), 0);
    let mut jobs = foreign_key::build_add_foreign_key_jobs(
        snapshot,
        catalog,
        schema,
        table,
        &name,
        definition,
        &DdlStatementContext(context.clone()),
        context.as_ref(),
        store_count,
    )?;
    let job = if jobs.len() == 1 {
        jobs.pop().unwrap()
    } else {
        multi_schema::build_multi_schema_job(
            catalog,
            schema,
            table,
            jobs,
            context.as_ref(),
            store_count,
        )?
    };
    Ok(Some(AddIndexSubmission::Job(Box::new(job), Vec::new())))
}

/// Go `executor.AlterTable` and `multiSchemaChange`: append each job in specification order.
#[allow(clippy::too_many_arguments)]
pub(crate) fn alter_table_job<S: MetaSnapshot>(
    snapshot: &mut S,
    alter: &AlterTableStmt,
    default_schema: &str,
    context: &DdlContext,
    start_ts: u64,
    upgrading: bool,
    store_count: usize,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
    has_null: &mut dyn FnMut(&str, &TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;
    let (schema, table) = split_name(&alter.name, default_schema, "table")?;
    let (_, stored) = locate_table(&catalog, &schema, &table)?;
    refuse_non_base_table(&stored.read(), &schema, &table)?;
    check_alter_table_spec_refusals(alter)?;
    tidb_ddl_build::storage_class::check_storage_class_conflict_in_alter_table_specs(
        alter.actions.iter().flat_map(|action| match action {
            tidb_ast::AlterTableAction::SetTableOptions { options } => options.as_slice(),
            _ => &[],
        }),
    )
    .map_err(DdlPlanError::from_mysql_error)?;
    if alter.actions.len() > 1
        && (context.enable_row_level_checksum()
            || tidb_vardef::ENABLE_ROW_LEVEL_CHECKSUM.load(std::sync::atomic::Ordering::SeqCst))
    {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES.gen_with_stack(
                "Unsupported multi schema change when row level checksum is enabled",
            ),
        ));
    }
    if alter.actions.is_empty() {
        return alter_table_no_job(&catalog, &schema, &table, None);
    }
    if let [action] = alter.actions.as_slice() {
        let built = build_alter_table_spec(
            snapshot,
            &catalog,
            alter,
            action,
            default_schema,
            context,
            store_count,
            false,
            tiflash_stores,
            has_null,
        )?;
        return submit_built_job(snapshot, built, start_ts, upgrading);
    }
    let mut specs = Vec::new();
    let mut anonymous_foreign_keys = 0;
    for action in &alter.actions {
        if let tidb_ast::AlterTableAction::MaskingPolicy(_) = action {
            return Err(go_unsupported_multi_schema_change("masking policy").into());
        }
        if let tidb_ast::AlterTableAction::AddForeignKey(definition) = action {
            let name =
                foreign_key::foreign_key_name(definition, &stored.read(), anonymous_foreign_keys);
            if definition.name.as_deref().is_none_or(str::is_empty) {
                anonymous_foreign_keys += 1;
            }
            for spec in foreign_key::build_add_foreign_key_jobs(
                snapshot,
                &catalog,
                &schema,
                &table,
                &name,
                definition,
                &DdlStatementContext(context.clone()),
                context.as_ref(),
                store_count,
            )? {
                multi_schema::append_to_sub_jobs(&mut specs, spec)?;
            }
            continue;
        }
        let single = AlterTableStmt {
            name: alter.name.clone(),
            actions: vec![action.clone()],
        };
        let parts = split_table_option_alter(&single).unwrap_or_else(|| vec![single]);
        for part in parts {
            let built = build_alter_table_spec(
                snapshot,
                &catalog,
                &part,
                &part.actions[0],
                default_schema,
                context,
                store_count,
                true,
                tiflash_stores,
                has_null,
            )?;
            match built {
                Some(AddIndexSubmission::Job(spec, warnings)) => {
                    for warning in warnings {
                        context.append_sql_warn(warning);
                    }
                    multi_schema::append_to_sub_jobs(&mut specs, *spec)?;
                }
                Some(AddIndexSubmission::AlreadySatisfied { warnings, .. }) => {
                    for warning in warnings {
                        context.append_sql_warn(warning);
                    }
                }
                None => {
                    return Err(DdlPlanError::Admission(DdlAdmissionError::plain(
                        "the ALTER TABLE specification has no executor",
                    )))
                }
            }
        }
    }
    if specs.is_empty() {
        return alter_table_no_job(&catalog, &schema, &table, None);
    }
    let job = multi_schema::build_multi_schema_job(
        &catalog,
        &schema,
        &table,
        specs,
        context.as_ref(),
        store_count,
    )?;
    submit_built_job(
        snapshot,
        Some(AddIndexSubmission::Job(Box::new(job), Vec::new())),
        start_ts,
        upgrading,
    )
}
