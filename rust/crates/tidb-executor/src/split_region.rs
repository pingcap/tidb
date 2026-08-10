// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Split-key planning for `SPLIT TABLE` and `SPLIT TABLE ... INDEX`.
//!
//! Go boundary: `pkg/util/regionsplit/split_handle.go` and
//! `pkg/util/split.go`. This first executable slice intentionally admits the
//! two exact forms used by Sysbench prepare: an integer clustered handle and
//! a single-column, non-unique integer secondary index, both with `BETWEEN`.
//! Every other shape fails closed instead of claiming a region layout it did
//! not create.

use tidb_ast::{Expr, SplitOption, SplitRegionStmt};
use tidb_codec::table_key::{
    encode_non_unique_index_key, encode_record_key, encode_table_index_prefix,
    gen_table_record_prefix, RecordHandle,
};
use tidb_datatype::Datum;

use crate::{Catalog, DriverError, SchemaErrorKind, TableEntry};

const MIN_REGION_STEP_VALUE: i64 = 1000;
const MAX_SPLIT_REGION_NUM: i64 = 1000;

/// Physical split request resolved against the session's current catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitRegionPlan {
    /// Go `TableInfo.ID`, also used as PD's scatter group.
    pub table_id: i64,
    /// Raw TiKV keys at which regions must be split.
    pub split_keys: Vec<Vec<u8>>,
}

/// Resolves one parsed split statement and generates Go-compatible raw keys.
pub fn prepare_split_region(
    statement: &SplitRegionStmt,
    current_database: &str,
    catalog: &Catalog,
) -> Result<SplitRegionPlan, DriverError> {
    if statement.partition_syntax || !statement.partitions.is_empty() {
        return Err(DriverError::unsupported(
            "partitioned SPLIT TABLE is not supported yet",
        ));
    }
    let (database, table_name) = resolve_table_name(&statement.table, current_database)?;
    let table = match catalog.table_in(database, table_name) {
        Some(TableEntry::Kv(table)) => table,
        Some(_) => {
            return Err(DriverError::unsupported(
                "SPLIT TABLE needs a storage-backed base table",
            ))
        }
        None => {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                "{database}.{table_name}"
            ))))
        }
    };
    if table.partition().is_some() {
        return Err(DriverError::unsupported(
            "partitioned SPLIT TABLE is not supported yet",
        ));
    }

    let (lower, upper, regions) = between_integer_bounds(&statement.option)?;
    let split_keys = match statement.index.as_deref() {
        Some(index_name) => {
            let index = table
                .indexes()
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(index_name))
                .ok_or_else(|| DriverError::UnknownIndex(index_name.to_owned()))?;
            if index.unique || index.column_offsets.len() != 1 || index.has_prefix() {
                return Err(DriverError::unsupported(
                    "SPLIT INDEX currently requires one non-unique, non-prefix integer column",
                ));
            }
            split_index_keys(
                table.table_id,
                table.indexes().first().map(|first| first.id),
                index.id,
                lower,
                upper,
                regions,
            )?
        }
        None => {
            if table.pk_handle_offset().is_none() {
                return Err(DriverError::unsupported(
                    "SPLIT TABLE currently requires an integer clustered primary key",
                ));
            }
            split_table_keys(
                table.table_id,
                !table.indexes().is_empty(),
                lower,
                upper,
                regions,
            )?
        }
    };
    Ok(SplitRegionPlan {
        table_id: table.table_id,
        split_keys,
    })
}

fn resolve_table_name<'a>(
    path: &'a [String],
    current_database: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    match path {
        [table] => {
            if current_database.is_empty() {
                Err(DriverError::unsupported("no database selected"))
            } else {
                Ok((current_database, table))
            }
        }
        [database, table] => Ok((database, table)),
        _ => Err(DriverError::unsupported("invalid SPLIT TABLE name")),
    }
}

fn between_integer_bounds(option: &SplitOption) -> Result<(i64, i64, usize), DriverError> {
    let SplitOption::Between {
        lower,
        upper,
        regions,
    } = option
    else {
        return Err(DriverError::unsupported(
            "SPLIT TABLE BY is not supported yet",
        ));
    };
    if !(1..=MAX_SPLIT_REGION_NUM).contains(regions) {
        return Err(DriverError::unsupported(format!(
            "split region count must be between 1 and {MAX_SPLIT_REGION_NUM}"
        )));
    }
    let [lower] = lower.as_slice() else {
        return Err(DriverError::unsupported(
            "SPLIT TABLE currently requires one integer lower bound",
        ));
    };
    let [upper] = upper.as_slice() else {
        return Err(DriverError::unsupported(
            "SPLIT TABLE currently requires one integer upper bound",
        ));
    };
    let lower = integer_literal(lower)?;
    let upper = integer_literal(upper)?;
    if upper <= lower {
        return Err(DriverError::unsupported(format!(
            "lower value {lower} should be less than upper value {upper}"
        )));
    }
    Ok((lower, upper, *regions as usize))
}

fn integer_literal(expression: &Expr) -> Result<i64, DriverError> {
    match expression {
        Expr::Int(value) => value.parse::<i64>().map_err(|_| {
            DriverError::unsupported(format!("split bound {value} is outside signed BIGINT"))
        }),
        _ => Err(DriverError::unsupported(
            "SPLIT TABLE currently requires literal integer bounds",
        )),
    }
}

fn split_table_keys(
    table_id: i64,
    contains_index: bool,
    lower: i64,
    upper: i64,
    regions: usize,
) -> Result<Vec<Vec<u8>>, DriverError> {
    let step =
        (upper.wrapping_sub(lower) as u64) / u64::try_from(regions).expect("positive region count");
    if step < MIN_REGION_STEP_VALUE as u64 {
        return Err(DriverError::unsupported(format!(
            "the region size is too small, expected at least {MIN_REGION_STEP_VALUE}, but got {step}"
        )));
    }
    let record_prefix = gen_table_record_prefix(table_id);
    let mut keys = Vec::with_capacity(regions);
    if contains_index {
        // Go splits the index keyspace away from the record keyspace first.
        keys.push(record_prefix.clone());
    }
    let mut handle = lower;
    for _ in 1..regions {
        handle = handle.wrapping_add(step as i64);
        keys.push(encode_record_key(
            &record_prefix,
            &RecordHandle::Int(handle),
        ));
    }
    Ok(keys)
}

fn split_index_keys(
    table_id: i64,
    first_index_id: Option<i64>,
    index_id: i64,
    lower: i64,
    upper: i64,
    regions: usize,
) -> Result<Vec<Vec<u8>>, DriverError> {
    let mut keys = Vec::with_capacity(regions + 1);
    if first_index_id.is_some_and(|first| first != index_id) {
        keys.push(encode_table_index_prefix(table_id, index_id));
    }
    // Go always isolates the index's end boundary.
    keys.push(encode_table_index_prefix(table_id, index_id + 1));
    let lower_key = encode_non_unique_index_key(table_id, index_id, &[Datum::Int(lower)], i64::MIN)
        .map_err(|error| DriverError::unsupported(error.to_string()))?;
    let upper_key = encode_non_unique_index_key(table_id, index_id, &[Datum::Int(upper)], i64::MIN)
        .map_err(|error| DriverError::unsupported(error.to_string()))?;
    if lower_key >= upper_key {
        return Err(DriverError::unsupported(format!(
            "lower value {lower} should be less than upper value {upper}"
        )));
    }
    get_values_list(&lower_key, &upper_key, regions, &mut keys);
    Ok(keys)
}

/// Port of Go `util.GetValuesList` over memcomparable byte strings.
fn get_values_list(lower: &[u8], upper: &[u8], regions: usize, output: &mut Vec<Vec<u8>>) {
    let common = lower
        .iter()
        .zip(upper)
        .take_while(|(left, right)| left == right)
        .count();
    let step = (uint64_prefix(&upper[common..], 0xff) - uint64_prefix(&lower[common..], 0))
        / regions as u64;
    let mut value = uint64_prefix(&lower[common..], 0);
    for _ in 1..regions {
        value = value.wrapping_add(step);
        let mut key = Vec::with_capacity(common + 8);
        key.extend_from_slice(&lower[..common]);
        key.extend_from_slice(&value.to_be_bytes());
        output.push(key);
    }
}

fn uint64_prefix(bytes: &[u8], pad: u8) -> u64 {
    let mut buffer = [pad; 8];
    let copied = bytes.len().min(8);
    buffer[..copied].copy_from_slice(&bytes[..copied]);
    u64::from_be_bytes(buffer)
}

#[cfg(test)]
mod tests {
    use super::{get_values_list, split_table_keys};
    use tidb_codec::table_key::{encode_record_key, gen_table_record_prefix, RecordHandle};

    #[test]
    fn byte_interpolation_matches_go_get_values_list() {
        let mut lower = vec![1, 2, 1];
        lower.extend_from_slice(&[0; 7]);
        let mut upper = vec![1, 2, 101];
        upper.extend_from_slice(&[0; 7]);
        let mut values = Vec::new();
        get_values_list(&lower, &upper, 10, &mut values);
        assert_eq!(values.len(), 9);
        assert_eq!(values[0], vec![1, 2, 11, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(values[8], vec![1, 2, 91, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn integer_table_split_includes_index_boundary_and_seven_handles() {
        let table_id = 42;
        let keys = split_table_keys(table_id, true, 0, 10_000_001, 8).unwrap();
        assert_eq!(keys.len(), 8);
        assert_eq!(keys[0], gen_table_record_prefix(table_id));
        assert_eq!(
            keys[1],
            encode_record_key(
                &gen_table_record_prefix(table_id),
                &RecordHandle::Int(1_250_000),
            )
        );
        assert_eq!(
            keys[7],
            encode_record_key(
                &gen_table_record_prefix(table_id),
                &RecordHandle::Int(8_750_000),
            )
        );
    }
}
