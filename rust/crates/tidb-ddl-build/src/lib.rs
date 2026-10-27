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

use tidb_error::mysql::MysqlError;

pub mod column_defaults;
pub mod errors;
pub mod column_field_type;
pub mod foreign_key;
pub mod index;
pub mod check_constraint;
pub mod constraint_names;
pub mod split_region;
pub mod storage_class;
pub mod modify_column_type;
pub mod partition_interval;
pub mod table_partition_range;
pub mod table_partition_list;
pub use tidb_table::table_from_meta;
pub mod table_info_build;
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
    ctx: &tidb_expr::metabuild::Context,
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
        CommentOwner::Table => (&*tidb_util::dbterror::ERR_TOO_LONG_TABLE_COMMENT, "table"),
        CommentOwner::Field => (&*tidb_util::dbterror::ERR_TOO_LONG_FIELD_COMMENT, "field"),
        CommentOwner::Index => (&*tidb_util::dbterror::ERR_TOO_LONG_INDEX_COMMENT, "index"),
    };
    let message = format!("Comment for {owner} '{shown}' is too long (max = {max_len})");
    if ctx.get_sql_mode().has_strict_mode() {
        return Err(MysqlError::terror(proto, message));
    }
    ctx.append_warning(tidb_util::context::WarnErr::Terror(proto.generate(message)));
    let mut end = max_len;
    while !comment.is_char_boundary(end) {
        end -= 1;
    }
    Ok(comment[..end].to_owned())
}

/// Go `model.NewTableAffinityInfoWithLevel` with `ErrInvalidTableAffinity`
/// (8266) for a level it does not know.
pub fn table_affinity(level: &str) -> Result<Option<tidb_model::TableAffinityInfo>, MysqlError> {
    tidb_model::new_table_affinity_info_with_level(level).map_err(|_| MysqlError::terror(&tidb_util::dbterror::ERR_CANNOT_SET_AFFINITY_ON_TABLE, format!("Invalid AFFINITY '{level}'")))
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
    let refuse = |what: String, kind: &str| MysqlError::terror(&tidb_util::dbterror::ERR_CANNOT_SET_AFFINITY_ON_TABLE, format!("Can not set {what} on a {kind}."));
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
            return Err(errors::set_ttl_option_for_non_ttl_table(
                "TTL_ENABLE".to_owned(),
            ));
        }
        if options
            .iter()
            .any(|option| matches!(option, tidb_ast::TableOption::TtlJobInterval(_)))
        {
            return Err(errors::set_ttl_option_for_non_ttl_table(
                "TTL_JOB_INTERVAL".to_owned(),
            ));
        }
    }
    Ok(ttl_info)
}

/// Go `setTemporaryType` (`pkg/ddl/create_table.go:1026`): the ONE place a
/// written `TEMPORARY` keyword becomes a `TableInfo.TempTableType`.
///
/// `ON COMMIT PRESERVE ROWS` is the interesting arm. TiDB parses it -- so
/// `OnCommitDelete` is simply false -- and then refuses the statement,
/// because a global temporary table's rows are discarded by the commit-time
/// KV filter (`temporaryTableKVFilter`) whatever the clause said. Accepting
/// the spelling and preserving nothing would be a wrong answer rather than a
/// missing feature, which is why Go raises `ErrUnsupportedOnCommitPreserve`
/// (8200) instead.
///
/// Nothing records `on_commit_delete` past this point, and Go does not
/// either: `TableInfo` has no such field, so `SHOW CREATE TABLE` prints
/// ` ON COMMIT DELETE ROWS` for every global temporary table unconditionally.
pub fn validate_temporary_table_create(
    create: &tidb_ast::CreateTableStmt,
) -> Result<tidb_model::TempTableType, MysqlError> {
    // Go `preprocessor.checkCreateTableGrammar` (`preprocess.go:946`): two
    // table options are refused on a temporary table before anything else is
    // built, because neither has a meaning for a relation that never reaches
    // TiKV. `AUTO_RANDOM` is rejected by the column-option pass at the same
    // preprocessing boundary.
    if create.temporary != tidb_ast::CreateTableTemporary::None {
        for option in &create.table_options {
            match option {
                tidb_ast::TableOption::ShardRowIdBits(_) => {
                    return Err(errors::opt_on_temporary_table("shard_row_id_bits"));
                }
                tidb_ast::TableOption::PlacementPolicy(_) => {
                    return Err(errors::opt_on_temporary_table("PLACEMENT"));
                }
                _ => {}
            }
        }
        if create.columns.iter().any(|column| {
            column
                .options
                .iter()
                .any(|option| matches!(option, tidb_ast::ColumnOption::AutoRandom(_)))
        }) {
            return Err(errors::opt_on_temporary_table("auto_random"));
        }
    }

    set_temporary_type(create.temporary, create.on_commit_delete)
}

/// Go `preprocessor.checkCreateTableGrammar`'s `CREATE TABLE ... LIKE` arm
/// (`preprocess.go:933`): a temporary source is refused, and a temporary
/// copy may not inherit what a temporary table cannot carry
/// (`checkReferInfoForTemporaryTable`).
pub fn check_create_table_like_source(
    source: &tidb_model::TableInfo,
    temporary: tidb_ast::CreateTableTemporary,
) -> Result<(), MysqlError> {
    if source.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(opt_on_temporary_table("create table like"));
    }
    if temporary == tidb_ast::CreateTableTemporary::None {
        return Ok(());
    }
    if source.auto_random_bits != 0 {
        return Err(opt_on_temporary_table("auto_random"));
    }
    if source.pre_split_regions != 0 {
        return Err(opt_on_temporary_table("pre split regions"));
    }
    if source.partition.is_some() {
        return Err(MysqlError::terror(
            &tidb_error::plannererrors::ERR_PARTITION_NO_TEMPORARY,
            tidb_error::tidb::errname::ErrPartitionNoTemporary.raw,
        ));
    }
    if source.shard_row_id_bits != 0 {
        return Err(opt_on_temporary_table("shard_row_id_bits"));
    }
    if source.placement_policy_ref.is_some() {
        return Err(opt_on_temporary_table("placement"));
    }
    Ok(())
}

/// The preprocessor's `plannererrors.ErrOptOnTemporaryTable`.
fn opt_on_temporary_table(operation: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE,
        format!("`{operation}` is unsupported on temporary tables."),
    )
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
                Err(errors::unsupported_on_commit_preserve())
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
        return Err(errors::too_long_ident(name.to_owned()));
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
                    errors::unsupported(format!("`{unit}` is not a TTL interval unit"))
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
                    tidb_ast::RestoreFlags::STRING_SINGLE_QUOTES
                        | tidb_ast::RestoreFlags::NAME_BACK_QUOTES,
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
