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

use crate::driver::{split_table_path_pub, SequenceDef};
use crate::sequence::{SequenceAllocator, SequenceInfo};
use crate::{Catalog, DriverError, SchemaErrorKind};
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
    qualified: &str,
) -> Result<SequenceInfo, DriverError> {
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
        return Err(DriverError::Schema(
            SchemaErrorKind::SequenceValuesConflicting(qualified.to_owned()),
        ));
    }
    Ok(info)
}

/// Go's qualified name in a sequence error, which is LOWERCASED
/// (`ident.Schema.L`, `ident.Name.L`) -- captured as `'test.nosuch'`.
fn qualified(database: &str, name: &str) -> String {
    format!("{}.{}", database.to_lowercase(), name.to_lowercase())
}

/// `CREATE SEQUENCE`. Returns whether a sequence was created (`false` only for
/// `IF NOT EXISTS` over an existing name).
///
/// Captured: `create sequence s1` twice reports
/// `[schema:1050] Table 'test.s1' already exists` -- the TABLE error, because
/// the two kinds share one namespace.
pub fn run_create_sequence_in(
    create: &tidb_ast::CreateSequenceStmt,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<bool, DriverError> {
    let (database, name) = split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    if !catalog.has_database(&database) {
        return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
            database,
        )));
    }
    if catalog.contains_in(&database, &name) {
        if create.if_not_exists {
            return Ok(false);
        }
        return Err(DriverError::Schema(SchemaErrorKind::TableExists(
            qualified(&database, &name),
        )));
    }
    // Go rejects every table option but COMMENT and ENGINE; neither reaches a
    // value, and this tier stores neither, so any option at all is refused
    // rather than silently dropped.
    if !create.table_options.is_empty() {
        return Err(DriverError::Unsupported(
            "a table option on CREATE SEQUENCE is not supported yet",
        ));
    }
    // RESTART is an ALTER-only option, and the PARSER already refuses it on a
    // CREATE -- as real TiDB does (captured: `create sequence s restart with 5`
    // is an error there too). No check is needed here.
    let info = build_sequence_info(&create.options, &qualified(&database, &name))?;
    catalog.register_sequence_in(
        &database,
        &name,
        SequenceDef {
            name: name.clone(),
            allocator: SequenceAllocator::new(info),
        },
    )?;
    Ok(true)
}

/// `ALTER SEQUENCE`. The new options replace the old and the cache is
/// discarded; `RESTART` additionally moves the counter (Go
/// `alterSequenceOptions`, whose bare `RESTART` restarts at `START`).
///
/// Captured: `alter sequence nosuch increment by 2` reports
/// `[schema:1146] Table 'test.nosuch' doesn't exist` -- 1146, NOT the 4139
/// `DROP SEQUENCE` uses.
pub fn run_alter_sequence_in(
    alter: &tidb_ast::AlterSequenceStmt,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let (database, name) = split_table_path_pub(&alter.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    let existing = match catalog.sequence_in(&database, &name) {
        Some(sequence) => sequence.allocator.info(),
        None => {
            if alter.if_exists {
                return Ok(());
            }
            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(
                qualified(&database, &name),
            )));
        }
    };
    let info = apply_sequence_options(existing, &alter.options, false);
    if !sequence_options_are_valid(&info) {
        return Err(DriverError::Schema(
            SchemaErrorKind::SequenceValuesConflicting(qualified(&database, &name)),
        ));
    }
    let restart = alter.options.iter().find_map(|option| match option {
        SequenceOption::RestartWith(value) => Some(*value),
        // Go's bare `RESTART` restarts at the (possibly just-changed) START.
        SequenceOption::Restart => Some(info.start),
        _ => None,
    });
    let sequence = catalog
        .sequence_mut_in(&database, &name)
        .expect("checked above");
    sequence.allocator.alter(info);
    if let Some(value) = restart {
        sequence.allocator.restart(value);
    }
    Ok(())
}

/// `DROP SEQUENCE [IF EXISTS] a, b`. Go drops the names it finds and reports
/// 4139 naming the FIRST it does not -- captured: after
/// `drop sequence s1, nosuch` fails, `s1` is gone.
pub fn run_drop_sequence_in(
    drop: &tidb_ast::DropSequenceStmt,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let mut missing = None;
    for path in &drop.names {
        let (database, name) = split_table_path_pub(path, current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        if catalog.sequence_in(&database, &name).is_none() {
            if missing.is_none() {
                missing = Some(qualified(&database, &name));
            }
            continue;
        }
        catalog.drop_table_in(&database, &name);
    }
    match missing {
        // `IF EXISTS` turns the whole report into a no-op, as Go's does.
        Some(name) if !drop.if_exists => {
            Err(DriverError::Schema(SchemaErrorKind::UnknownSequence(name)))
        }
        _ => Ok(()),
    }
}

/// The `SHOW CREATE SEQUENCE` / `SHOW CREATE TABLE` text for a sequence.
///
/// Captured verbatim from real TiDB for an option-free sequence:
/// ``CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806
/// increment by 1 cache 1000 nocycle ENGINE=InnoDB`` -- note the fixed clause
/// ORDER (start, minvalue, maxvalue, increment, cache, cycle), the lowercase
/// keywords, and that `SHOW CREATE TABLE` over a sequence prints this same
/// text rather than a `CREATE TABLE`.
#[must_use]
pub fn show_create_sequence(sequence: &SequenceDef) -> String {
    let info = sequence.allocator.info();
    let cache = if info.cache {
        format!("cache {}", info.cache_value)
    } else {
        "nocache".to_owned()
    };
    let cycle = if info.cycle { "cycle" } else { "nocycle" };
    format!(
        "CREATE SEQUENCE `{}` start with {} minvalue {} maxvalue {} increment by {} {} {} ENGINE=InnoDB",
        sequence.name, info.start, info.min_value, info.max_value, info.increment, cache, cycle
    )
}

#[cfg(test)]
mod tests;
