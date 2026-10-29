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

//! `CREATE` / `ALTER` / `DROP SEQUENCE`: Go `pkg/ddl/sequence.go`.
//!
//! A sequence occupies the TABLE namespace (Go stores it as a
//! `model.TableInfo` with `Sequence` set), so `CREATE SEQUENCE` over an
//! existing table name is 1050 and `SHOW TABLES` lists it -- both captured.

use tidb_table::sequence::SequenceInfo;
use tidb_error::mysql::MysqlError;

use tidb_ast::SequenceOption;

/// Go `model.Default*Sequence*Value`: the defaults depend on the SIGN of the
/// increment, and only apply to the bounds the statement left unwritten.
const DEFAULT_CACHE_VALUE: i64 = 1000;
const POSITIVE_START: i64 = 1;
const POSITIVE_MIN: i64 = 1;
const POSITIVE_MAX: i64 = 9_223_372_036_854_775_806;
const NEGATIVE_START: i64 = -1;
const NEGATIVE_MAX: i64 = -1;
const NEGATIVE_MIN: i64 = -9_223_372_036_854_775_807;

/// Go `handleSequenceOptions`: apply the written options, then fill each
/// unwritten bound from the sign-dependent defaults.
///
/// `base` is the starting point -- Go's fresh `SequenceInfo` for `CREATE`, and
/// the EXISTING options for `ALTER`, which is why an `ALTER` that names only
/// `INCREMENT BY` keeps every other bound.
fn apply_sequence_options(
    base: SequenceInfo,
    options: &[SequenceOption],
    is_create: bool,
) -> SequenceInfo {
    let mut info = base;
    let (mut min_set, mut max_set, mut start_set) = (false, false, false);
    for option in options {
        match option {
            SequenceOption::IncrementBy(value) => info.increment = *value,
            SequenceOption::StartWith(value) => {
                info.start = *value;
                start_set = true;
            }
            SequenceOption::MinValue(value) => {
                info.min_value = *value;
                min_set = true;
            }
            SequenceOption::MaxValue(value) => {
                info.max_value = *value;
                max_set = true;
            }
            SequenceOption::Cache(value) => {
                info.cache_value = *value;
                info.cache = true;
            }
            SequenceOption::NoCache => {
                info.cache_value = 0;
                info.cache = false;
            }
            SequenceOption::Cycle => info.cycle = true,
            SequenceOption::NoCycle => info.cycle = false,
            // Go's parser rejects `NOMINVALUE`/`NOMAXVALUE` nowhere, but its
            // option handler ignores them, leaving the default to fill in --
            // which is what an unset flag already does here.
            SequenceOption::NoMinValue | SequenceOption::NoMaxValue => {}
            // Handled by the caller: RESTART moves the counter, it is not an
            // option stored on the sequence.
            SequenceOption::Restart | SequenceOption::RestartWith(_) => {}
        }
    }
    // Go only fills defaults when at least one of the three is unwritten, and
    // ALTER never refills (it starts from the stored options, which are
    // already complete).
    if is_create && !(min_set && max_set && start_set) {
        if info.increment >= 0 {
            if !min_set {
                info.min_value = POSITIVE_MIN;
            }
            if !start_set {
                info.start = info.min_value.max(POSITIVE_START);
            }
            if !max_set {
                info.max_value = POSITIVE_MAX;
            }
        } else {
            if !max_set {
                info.max_value = NEGATIVE_MAX;
            }
            if !start_set {
                info.start = info.max_value.min(NEGATIVE_START);
            }
            if !min_set {
                info.min_value = NEGATIVE_MIN;
            }
        }
    }
    info
}

/// Go `validateSequenceOptions`. Every clause is reproduced, including the two
/// that look redundant: `MaxValue != i64::MAX` and `MinValue != i64::MIN` are
/// what make the DEFAULT maximum `i64::MAX - 1`, and the cache bound is what
/// keeps `cache * increment` from overflowing.
fn sequence_options_are_valid(info: &SequenceInfo) -> bool {
    if info.increment == 0 {
        return false;
    }
    if info.cache && info.cache_value <= 0 {
        return false;
    }
    let max_increment = info.increment.unsigned_abs() as i64;
    info.max_value >= info.start
        && info.max_value > info.min_value
        && info.start >= info.min_value
        && info.max_value != i64::MAX
        && info.min_value != i64::MIN
        && info.cache_value < (i64::MAX - max_increment) / max_increment
}

/// The options a `CREATE SEQUENCE` describes, or 4136 when they conflict.
fn build_sequence_info(
    options: &[SequenceOption],
    database: &str,
    name: &str,
) -> Result<SequenceInfo, MysqlError> {
    // Go starts from a struct whose bounds are ZERO, so an unwritten bound is
    // filled by `apply_sequence_options` rather than inherited.
    let base = SequenceInfo {
        start: 0,
        increment: 1,
        min_value: 0,
        max_value: 0,
        cache_value: DEFAULT_CACHE_VALUE,
        cache: true,
        cycle: false,
    };
    let info = apply_sequence_options(base, options, true);
    if !sequence_options_are_valid(&info) {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_SEQUENCE_INVALID_DATA.generate_with_stack_by_args(&[
                tidb_hack::go_to_lower(database).into(),
                tidb_hack::go_to_lower(name).into(),
            ]),
        ));
    }
    Ok(info)
}

/// Go passes `TableOption.StrValue` to
/// `ErrSequenceUnsupportedTableOption`; table-option variants backed by
/// another AST field therefore contribute the empty string.
fn sequence_table_option_string_value(option: &tidb_ast::TableOption) -> String {
    match option {
        tidb_ast::TableOption::CharacterSet(value)
        | tidb_ast::TableOption::Collate(value)
        | tidb_ast::TableOption::Engine(value)
        | tidb_ast::TableOption::AutoIncrement(value)
        | tidb_ast::TableOption::ForceAutoIncrement(value)
        | tidb_ast::TableOption::Comment(value)
        | tidb_ast::TableOption::RowFormat(value)
        | tidb_ast::TableOption::KeyBlockSize(value)
        | tidb_ast::TableOption::Compression(value)
        | tidb_ast::TableOption::Tablespace(value)
        | tidb_ast::TableOption::StorageMedia(value)
        | tidb_ast::TableOption::StorageClass(value)
        | tidb_ast::TableOption::ShardRowIdBits(value)
        | tidb_ast::TableOption::PreSplitRegions(value)
        | tidb_ast::TableOption::AutoIdCache(value)
        | tidb_ast::TableOption::MaxRows(value)
        | tidb_ast::TableOption::MinRows(value)
        | tidb_ast::TableOption::AvgRowLength(value)
        | tidb_ast::TableOption::Checksum(value)
        | tidb_ast::TableOption::DelayKeyWrite(value)
        | tidb_ast::TableOption::AutoRandomBase(value)
        | tidb_ast::TableOption::ForceAutoRandomBase(value)
        | tidb_ast::TableOption::Connection(value)
        | tidb_ast::TableOption::Password(value)
        | tidb_ast::TableOption::StatsAutoRecalc(value)
        | tidb_ast::TableOption::StatsSamplePages(value)
        | tidb_ast::TableOption::Nodegroup(value)
        | tidb_ast::TableOption::DataDirectory(value)
        | tidb_ast::TableOption::IndexDirectory(value)
        | tidb_ast::TableOption::AutoextendSize(value)
        | tidb_ast::TableOption::PageChecksum(value)
        | tidb_ast::TableOption::PageCompressed(value)
        | tidb_ast::TableOption::PageCompressionLevel(value)
        | tidb_ast::TableOption::Transactional(value)
        | tidb_ast::TableOption::IetfQuotes(value)
        | tidb_ast::TableOption::Sequence(value)
        | tidb_ast::TableOption::InsertMethod(value)
        | tidb_ast::TableOption::Encryption(value)
        | tidb_ast::TableOption::SecondaryEngine(value)
        | tidb_ast::TableOption::SecondaryEngineAttribute(value)
        | tidb_ast::TableOption::EngineAttribute(value)
        | tidb_ast::TableOption::TableChecksum(value)
        | tidb_ast::TableOption::PlacementPolicy(value)
        | tidb_ast::TableOption::StatsBuckets(value)
        | tidb_ast::TableOption::StatsTopN(value)
        | tidb_ast::TableOption::StatsSampleRate(value)
        | tidb_ast::TableOption::StatsColChoice(value)
        | tidb_ast::TableOption::StatsColList(value)
        | tidb_ast::TableOption::TtlJobInterval(value)
        | tidb_ast::TableOption::Affinity(value) => value.clone(),
        tidb_ast::TableOption::StatsPersistent
        | tidb_ast::TableOption::PackKeys
        | tidb_ast::TableOption::SecondaryEngineNull
        | tidb_ast::TableOption::StatsColChoiceDefault
        | tidb_ast::TableOption::StatsColListDefault
        | tidb_ast::TableOption::Union(_)
        | tidb_ast::TableOption::Ttl { .. }
        | tidb_ast::TableOption::TtlEnable(_) => String::new(),
    }
}

/// Go `buildSequenceInfo`, after the preprocessor's name check: the options a
/// `CREATE SEQUENCE` describes and its comment. Every table option but
/// COMMENT and ENGINE is refused (ENGINE is the parser's implicit InnoDB
/// spelling and does not become sequence metadata), then the options are
/// filled and validated (4136).
pub fn build_create_sequence(
    create: &tidb_ast::CreateSequenceStmt,
    database: &str,
    name: &str,
) -> Result<(SequenceInfo, String), MysqlError> {
    // Go's CREATE SEQUENCE preprocessor applies IsInCorrectIdentifierName
    // before DDL execution: empty identifiers and names whose final byte is a
    // space are rejected with ErrWrongTableName (1103).
    if name.is_empty() || name.as_bytes().last() == Some(&b' ') {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_TABLE_NAME
                .generate_with_stack_by_args(&[name.to_string().into()]),
        ));
    }
    let mut comment = String::new();
    for option in &create.table_options {
        match option {
            tidb_ast::TableOption::Comment(value) => comment = value.clone(),
            tidb_ast::TableOption::Engine(_) => {}
            _ => {
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_dbterror::ERR_SEQUENCE_UNSUPPORTED_TABLE_OPTION
                        .generate_with_stack_by_args(&[(sequence_table_option_string_value(
                            option,
                        ))
                        .to_string()
                        .into()]),
                ));
            }
        }
    }
    // RESTART is an ALTER-only option, and the PARSER already refuses it on a
    // CREATE -- as real TiDB does (captured: `create sequence s restart with 5`
    // is an error there too). No check is needed here.
    let info = build_sequence_info(&create.options, database, name)?;
    Ok((info, comment))
}

/// Go `alterSequenceOptions` over the stored options: the new options, or
/// 4136 when they conflict, and the value a `RESTART [WITH]` moves the
/// counter to (a bare `RESTART` restarts at the possibly just-changed START).
pub fn alter_sequence_options(
    existing: SequenceInfo,
    options: &[SequenceOption],
    database: &str,
    name: &str,
) -> Result<(SequenceInfo, Option<i64>), MysqlError> {
    let info = apply_sequence_options(existing, options, false);
    if !sequence_options_are_valid(&info) {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_SEQUENCE_INVALID_DATA.generate_with_stack_by_args(&[
                tidb_hack::go_to_lower(database).into(),
                tidb_hack::go_to_lower(name).into(),
            ]),
        ));
    }
    let restart = options.iter().find_map(|option| match option {
        SequenceOption::RestartWith(value) => Some(*value),
        SequenceOption::Restart => Some(info.start),
        _ => None,
    });
    Ok((info, restart))
}


