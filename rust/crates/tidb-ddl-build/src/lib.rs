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

//! The metadata-building half of Go `pkg/ddl`: a statement's column, index,
//! constraint, partition and table-option definitions checked and built into
//! metadata (`BuildTableInfoWithStmt` and the builders under it), below the
//! executor. The job workers that publish the metadata are `tidb-ddl`'s.

use tidb_terror::MysqlError;

pub mod plan_error;
pub mod column_defaults;
pub mod column_field_type;
pub mod foreign_key;
pub mod index;
pub mod check_constraint;
pub mod constraint_names;
pub mod split_region;
pub mod storage_class;
pub mod modify_column_change;
pub mod modify_column_type;
pub mod partition_interval;
pub mod table_partition_range;
pub mod table_partition_list;
pub mod table_info_build;
pub mod materialized_view;
pub mod table_partition;
pub mod table_lock;
pub mod column_changes;
pub mod table_lifecycle;
pub mod label_rules;
pub mod partition_ddl;
pub mod multi_schema_change;
pub mod cluster;
pub mod ddl_executor;
pub mod sequence;
pub mod metabuild;
pub mod session_context;
pub mod session_pool;
pub mod backfilling;
pub mod partition_reorg;
pub mod reorg_errors;
pub mod table_backfill;
pub mod column_types;
pub mod auto_random;
pub mod expression_index;
pub mod generated_column;

/// What a comment is attached to, for Go `validateCommentLength`'s error.
#[derive(Clone, Copy)]
pub enum CommentOwner {
    /// A table: `ErrTooLongTableComment` (1628), at most 2048 bytes.
    Table,
    /// A column: `ErrTooLongFieldComment` (1629).
    Field,
    /// An index: `ErrTooLongIndexComment` (1688).
    Index,
}

/// Go `validateCommentLength`: a table comment holds at most 2048 bytes and
/// any other at most 1024; strict mode refuses a longer one, otherwise it
/// warns and keeps the bytes within the limit.
pub fn validate_comment_length(
    comment: &str,
    name: &str,
    owner: CommentOwner,
    ctx: &tidb_metabuild::Context,
) -> Result<String, MysqlError> {
    // Go `MaxCommentLength`, doubled for a table as MySQL 5.7 has it.
    const MAX_COMMENT_LENGTH: usize = 1024;
    let max_len = match owner {
        CommentOwner::Table => MAX_COMMENT_LENGTH * 2,
        CommentOwner::Field | CommentOwner::Index => MAX_COMMENT_LENGTH,
    };
    if comment.len() <= max_len {
        return Ok(comment.to_owned());
    }
    // `%-.64s`: the name as Go prints it.
    let shown: String = name.chars().take(64).collect();
    let (proto, owner) = match owner {
        CommentOwner::Table => (&*tidb_dbterror::ERR_TOO_LONG_TABLE_COMMENT, "table"),
        CommentOwner::Field => (&*tidb_dbterror::ERR_TOO_LONG_FIELD_COMMENT, "field"),
        CommentOwner::Index => (&*tidb_dbterror::ERR_TOO_LONG_INDEX_COMMENT, "index"),
    };
    let message = format!("Comment for {owner} '{shown}' is too long (max = {max_len})");
    if ctx.get_sql_mode().has_strict_mode() {
        return Err(MysqlError::from_terror(&proto.gen_with_stack(message)));
    }
    ctx.append_warning(tidb_context::WarnErr::Terror(proto.generate(message)));
    let mut end = max_len;
    while !comment.is_char_boundary(end) {
        end -= 1;
    }
    Ok(comment[..end].to_owned())
}

/// Go `model.NewTableAffinityInfoWithLevel` with `ErrInvalidTableAffinity`
/// (8266) for a level it does not know.
pub fn table_affinity(level: &str) -> Result<Option<tidb_model::TableAffinityInfo>, MysqlError> {
    tidb_model::new_table_affinity_info_with_level(level).map_err(|_| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_CANNOT_SET_AFFINITY_ON_TABLE
                .gen_with_stack(format!("Invalid AFFINITY '{level}'")),
        )
    })
}

/// Go `validateTableAffinity`: no affinity on a temporary table, `table`
/// level only on an unpartitioned one and `partition` level only on a
/// partitioned one (`ErrCannotSetAffinityOnTable`, 8266).
pub fn validate_table_affinity(
    temporary: bool,
    partitioned: bool,
    affinity: Option<&tidb_model::TableAffinityInfo>,
) -> Result<(), MysqlError> {
    let Some(affinity) = affinity else {
        return Ok(());
    };
    let refuse = |what: String, kind: &str| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_CANNOT_SET_AFFINITY_ON_TABLE
                .gen_with_stack_by_args(&[what.into(), kind.into()]),
        )
    };
    if temporary {
        return Err(refuse("AFFINITY".to_owned(), "temporary table"));
    }
    match affinity.level.as_str() {
        "table" if partitioned => Err(refuse(
            format!("AFFINITY='{}'", affinity.level),
            "partition table",
        )),
        "partition" if !partitioned => Err(refuse(
            format!("AFFINITY='{}'", affinity.level),
            "non-partition table",
        )),
        _ => Ok(()),
    }
}

/// Go `handleTableOptions`' TTL arm for CREATE TABLE: the `TTL=` definition
/// the options describe, and TTL_ENABLE / TTL_JOB_INTERVAL refused without
/// one (8150).
pub fn create_table_ttl_info(
    options: &[tidb_ast::TableOption],
) -> Result<Option<tidb_model::TTLInfo>, MysqlError> {
    let ttl_info = ttl_info_from_options(options)?;
    if ttl_info.is_none() {
        if options
            .iter()
            .any(|option| matches!(option, tidb_ast::TableOption::TtlEnable(_)))
        {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_SET_TTL_OPTION_FOR_NON_TTL_TABLE
                    .gen_with_stack_by_args(&[("TTL_ENABLE".to_owned()).to_string().into()]),
            ));
        }
        if options
            .iter()
            .any(|option| matches!(option, tidb_ast::TableOption::TtlJobInterval(_)))
        {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_SET_TTL_OPTION_FOR_NON_TTL_TABLE
                    .gen_with_stack_by_args(&[("TTL_JOB_INTERVAL".to_owned())
                        .to_string()
                        .into()]),
            ));
        }
    }
    Ok(ttl_info)
}

/// Go `setTemporaryType` (`pkg/ddl/create_table.go`): the table's
/// `TempTableType` from the statement's `TEMPORARY` keyword, refusing a
/// GLOBAL temporary table that asked to preserve its rows on commit.
pub fn set_temporary_type(
    temporary: tidb_ast::CreateTableTemporary,
    on_commit_delete: bool,
) -> Result<tidb_model::TempTableType, MysqlError> {
    match temporary {
        tidb_ast::CreateTableTemporary::None => Ok(tidb_model::TempTableType::NONE),
        tidb_ast::CreateTableTemporary::Global => {
            if on_commit_delete {
                Ok(tidb_model::TempTableType::GLOBAL)
            } else {
                Err(tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_UNSUPPORTED_ON_COMMIT_PRESERVE
                        .gen_with_stack_by_args(&[]),
                ))
            }
        }
        tidb_ast::CreateTableTemporary::Local => Ok(tidb_model::TempTableType::LOCAL),
    }
}

/// `mysql.MaxTableNameLength`, `MaxColumnNameLength`, `MaxDatabaseNameLength`
/// and `MaxIndexIdentifierLen`: identifiers are at most 64 characters.
pub const MAX_IDENTIFIER_LENGTH: usize = 64;

/// Go `checkTooLongTable` / `checkTooLongColumn` / `checkTooLongSchema` /
/// `checkTooLongIndex`: `ErrTooLongIdent` (1059) for a name longer than 64
/// characters.
pub fn check_too_long_identifier(name: &str) -> Result<(), MysqlError> {
    if name.chars().count() > MAX_IDENTIFIER_LENGTH {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_TOO_LONG_IDENT
                .gen_with_stack_by_args(&[name.to_owned().into()]),
        ));
    }
    Ok(())
}

/// Go `handleTableOptions`' TTL block: the `TTLInfo` a table's options
/// describe, or `None` when none of them is a `TTL` definition.
///
/// `TTL_ENABLE` and `TTL_JOB_INTERVAL` alone do not make one -- Go keeps them
/// on an existing `TTLInfo` -- so the `TTL=` clause is what decides. Their
/// defaults are Go's: `TTL_ENABLE` is ON and the job interval is
/// `DefaultTTLJobInterval`.
pub fn ttl_info_from_options(
    options: &[tidb_ast::TableOption],
) -> Result<Option<tidb_model::TTLInfo>, MysqlError> {
    let mut info: Option<tidb_model::TTLInfo> = None;
    for option in options {
        if let tidb_ast::TableOption::Ttl {
            column,
            value,
            unit,
        } = option
        {
            let interval_time_unit =
                tidb_model::time_unit_type_from_keyword(unit).ok_or_else(|| {
                    tidb_terror::MysqlError::unknown(format!(
                        "`{unit}` is not a TTL interval unit"
                    ))
                })?;
            info = Some(tidb_model::TTLInfo {
                column_name: tidb_ast::CiString::new(column.clone()),
                // Go `getTTLInfoInOptions` restores the magnitude with
                // `RestoreStringSingleQuotes | RestoreNameBackQuotes` and
                // NOTHING ELSE -- in particular without
                // `RestoreKeyWordUppercase`, which the default set carries.
                // The difference shows on a string magnitude, whose CHARSET
                // INTRODUCER is written as the parser produced it: TiDB
                // records `INTERVAL _utf8mb4'15:20' HOUR_MINUTE`, and the
                // default flags would print `_UTF8MB4`.
                interval_expr_str: value.restore_with_flags(
                    tidb_parser_format::RestoreFlags::STRING_SINGLE_QUOTES
                        | tidb_parser_format::RestoreFlags::NAME_BACK_QUOTES,
                ),
                interval_time_unit,
                enable: true,
                job_interval: tidb_model::DEFAULT_TTL_JOB_INTERVAL.to_owned(),
            });
        }
    }
    let Some(built) = info.as_mut() else {
        return Ok(None);
    };
    for option in options {
        match option {
            tidb_ast::TableOption::TtlEnable(enabled) => built.enable = *enabled,
            tidb_ast::TableOption::TtlJobInterval(interval) => {
                built.job_interval = interval.clone();
            }
            _ => {}
        }
    }
    Ok(info)
}

// Go's test binary links `planner/core`, whose `init` sets `expression.BuildSimpleExpr`.
#[cfg(test)]
tidb_planner_init::link!();
