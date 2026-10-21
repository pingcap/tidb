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

//! `SHOW TABLE ... [PARTITION (...)] [INDEX ...] REGIONS | DISTRIBUTIONS`.
//!
//! Go builds the answer in `ShowExec.fetchShowTableRegions`
//! (`pkg/executor/show.go`) from `getTableRegions` / `getTableIndexRegions`
//! (`pkg/executor/split.go`): for every physical table, the regions covering
//! its record range and then each index range, de-duplicated by region id,
//! each key rendered by `regionKeyDecoder`. The regions come from the store's
//! region cache and PD ([`crate::region_map::RegionStore`]).

use std::collections::HashSet;

use crate::kv_table::KvTable;
use crate::region_map::RegionMeta;
use crate::{Catalog, DriverError, MysqlError, SchemaErrorKind, TableEntry};
use tidb_codec::decode_int;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_error::tidb::errcode;
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;
use tidb_model::PlacementSettings;

/// What `fetchSchedulingInfo` reports for a table whose placement policy
/// applies: the in-process store runs no placement rules, so the schedule
/// never completes.
const PENDING: &str = "PENDING";

/// Go `buildTableRegionsSchema`.
#[must_use]
pub fn table_region_columns() -> Vec<(String, FieldType)> {
    let number = |flen: i64| {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        field_type.set_flen(flen);
        field_type.add_flags(FieldTypeFlags::UNSIGNED);
        field_type.set_charset_name("binary");
        field_type.set_collation_name("binary");
        field_type
    };
    let text = |flen: i64| {
        let mut field_type = FieldType::new(FieldTypeCode::Varchar);
        field_type.set_flen(flen);
        field_type.set_charset_name("utf8mb4");
        field_type.set_collation_name("utf8mb4_bin");
        field_type
    };
    let mut scattering = FieldType::new(FieldTypeCode::Tiny);
    scattering.set_flen(1);
    scattering.add_flags(FieldTypeFlags::UNSIGNED);
    scattering.set_charset_name("binary");
    scattering.set_collation_name("binary");
    vec![
        ("REGION_ID".to_owned(), number(4)),
        ("START_KEY".to_owned(), text(64)),
        ("END_KEY".to_owned(), text(64)),
        ("LEADER_ID".to_owned(), number(4)),
        ("LEADER_STORE_ID".to_owned(), number(4)),
        ("PEERS".to_owned(), text(64)),
        ("SCATTERING".to_owned(), scattering),
        ("WRITTEN_BYTES".to_owned(), number(4)),
        ("READ_BYTES".to_owned(), number(4)),
        ("APPROXIMATE_SIZE(MB)".to_owned(), number(4)),
        ("APPROXIMATE_KEYS".to_owned(), number(4)),
        ("SCHEDULING_CONSTRAINTS".to_owned(), text(256)),
        ("SCHEDULING_STATE".to_owned(), text(16)),
    ]
}

/// The table a `SHOW TABLE` statement names, reduced to what Go's region
/// listing reads of its `TableInfo`.
struct Target<'a> {
    name: String,
    id: i64,
    /// `None` for a view, a sequence or a memory table, which have an id and
    /// nothing else.
    table: Option<&'a KvTable>,
}

fn resolve<'a>(
    catalog: &'a Catalog,
    database: &str,
    table_name: &str,
    what: &'static str,
) -> Result<Target<'a>, DriverError> {
    let Some(entry) = catalog.table_in(database, table_name) else {
        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
            "{database}.{table_name}"
        ))));
    };
    let target = match entry {
        TableEntry::Kv(table) => Target {
            name: match table.name() {
                "" => table_name.to_owned(),
                name => name.to_owned(),
            },
            id: table.table_id,
            table: Some(table),
        },
        TableEntry::View(view) => Target {
            name: table_name.to_owned(),
            id: view.id,
            table: None,
        },
        TableEntry::Sequence(sequence) => Target {
            name: table_name.to_owned(),
            id: sequence.id,
            table: None,
        },
        TableEntry::Mem(_) => Target {
            name: table_name.to_owned(),
            id: if database.eq_ignore_ascii_case("information_schema") {
                crate::infoschema_meta::memory_table_id(table_name).unwrap_or(0)
            } else {
                0
            },
            table: None,
        },
    };
    if target.table.is_some_and(KvTable::is_temporary) {
        return Err(DriverError::OptOnTemporaryTable(what));
    }
    Ok(target)
}

/// The physical ids the statement covers, and whether a global index makes the
/// logical table's own id part of them. Go `fetchShowTableRegions` and
/// `fetchShowDistributions` share this resolution of `PARTITION (...)`.
fn physical_ids(
    target: &Target<'_>,
    partitions: &[String],
) -> Result<(Vec<i64>, bool), DriverError> {
    let Some(partitioning) = target.table.and_then(KvTable::partition) else {
        if !partitions.is_empty() {
            return Err(DriverError::PartitionClauseOnNonpartitioned);
        }
        return Ok((vec![target.id], false));
    };
    let mut ids = Vec::new();
    for name in partitions {
        let definition = partitioning
            .definitions
            .iter()
            .find(|definition| {
                definition
                    .name
                    .go_to_lower()
                    .go_equal_fold(&name.go_to_lower())
            })
            .ok_or_else(|| DriverError::UnknownPartition {
                partition: name.go_to_lower(),
                table: target.name.clone(),
            })?;
        ids.push(definition.id);
    }
    if ids.is_empty() {
        ids = partitioning
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect();
    }
    let has_global_index = target
        .table
        .is_some_and(|table| table.indexes().iter().any(|index| index.global));
    Ok((ids, has_global_index))
}

/// One region of the listing, with the physical table that first reached it.
struct Row {
    region: RegionMeta,
    start: String,
    end: String,
    physical_id: i64,
}

/// Go `regionKeyDecoder`.
struct KeyDecoder {
    physical_id: i64,
    table_prefix: Vec<u8>,
    record_prefix: Vec<u8>,
    index_prefix: Vec<u8>,
    index_id: i64,
    unsigned_int_handle: bool,
}

impl KeyDecoder {
    fn decode(&self, key: &[u8]) -> String {
        let id = self.physical_id;
        if !self.index_prefix.is_empty() && key.starts_with(&self.index_prefix) {
            return format!(
                "t_{id}_i_{}_{}",
                self.index_id,
                hex(&key[self.index_prefix.len()..])
            );
        }
        if !self.record_prefix.is_empty() && key.starts_with(&self.record_prefix) {
            if self.record_prefix.len() == key.len() {
                return format!("t_{id}_r");
            }
            let rest = &key[self.record_prefix.len()..];
            if rest.len() == 8 {
                if let Ok((_, handle)) = decode_int(rest) {
                    return if self.unsigned_int_handle {
                        format!("t_{id}_r_{}", handle as u64)
                    } else {
                        format!("t_{id}_r_{handle}")
                    };
                }
            }
            return format!("t_{id}_r_{}", hex(rest));
        }
        if !self.table_prefix.is_empty() && key.starts_with(&self.table_prefix) {
            let rest = &key[self.table_prefix.len()..];
            let Some(rest) = rest.strip_prefix(b"_i") else {
                return format!("t_{id}_{}", hex(rest));
            };
            if let Ok((tail, index_id)) = decode_int(rest) {
                return format!("t_{id}_i_{index_id}_{}", hex(tail));
            }
            return format!("t_{id}_i__{}", hex(rest));
        }
        if let Some(rest) = key.strip_prefix(b"t") {
            if let Ok((tail, table_id)) = decode_int(rest) {
                return format!("t_{table_id}_{}", hex(tail));
            }
            return format!("t_{}", hex(rest));
        }
        hex(key)
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// Go `getRegionMeta`: the regions not seen yet, with their keys decoded.
fn collect(
    catalog: &Catalog,
    range: (Vec<u8>, Vec<u8>),
    decoder: &KeyDecoder,
    seen: &mut HashSet<u64>,
    rows: &mut Vec<Row>,
) -> Result<(), DriverError> {
    let regions = catalog
        .region_metas_in(&range.0, &range.1)
        .map_err(|error| DriverError::Mysql(MysqlError::unknown(error)))?;
    for region in regions {
        if !seen.insert(region.id) {
            continue;
        }
        rows.push(Row {
            start: decoder.decode(&region.start_key),
            end: decoder.decode(&region.end_key),
            region,
            physical_id: decoder.physical_id,
        });
    }
    Ok(())
}

/// Go `getPhysicalTableRegions`: the record range, then every index range.
fn table_regions(
    catalog: &Catalog,
    target: &Target<'_>,
    physical_id: i64,
    seen: &mut HashSet<u64>,
    rows: &mut Vec<Row>,
) -> Result<(), DriverError> {
    let decoder = |index_prefix: Vec<u8>, index_id: i64| KeyDecoder {
        physical_id,
        table_prefix: tidb_codec::table_key::gen_table_prefix(physical_id),
        record_prefix: tidb_codec::gen_table_record_prefix(physical_id),
        index_prefix,
        index_id,
        unsigned_int_handle: target.table.is_some_and(KvTable::unsigned_pk_handle),
    };
    collect(
        catalog,
        tidb_codec::table_key::get_table_handle_key_range(physical_id),
        &decoder(Vec::new(), 0),
        seen,
        rows,
    )?;
    for index in target.table.map_or(&[][..], KvTable::indexes) {
        collect(
            catalog,
            tidb_tablecodec::get_table_index_key_range(physical_id, index.id),
            &decoder(
                tidb_codec::table_key::encode_table_index_prefix(physical_id, index.id),
                index.id,
            ),
            seen,
            rows,
        )?;
    }
    Ok(())
}

/// Go `getPhysicalIndexRegions`.
fn index_regions(
    catalog: &Catalog,
    index_id: i64,
    physical_id: i64,
    seen: &mut HashSet<u64>,
    rows: &mut Vec<Row>,
) -> Result<(), DriverError> {
    let index_prefix = tidb_codec::table_key::encode_table_index_prefix(physical_id, index_id);
    let decoder = KeyDecoder {
        physical_id,
        table_prefix: tidb_codec::table_key::gen_table_prefix(physical_id),
        record_prefix: tidb_codec::gen_table_record_prefix(physical_id),
        index_prefix,
        index_id,
        unsigned_int_handle: false,
    };
    collect(
        catalog,
        tidb_tablecodec::get_table_index_key_range(physical_id, index_id),
        &decoder,
        seen,
        rows,
    )
}

/// Go `getPolicyPlacement`.
pub fn policy_placement(
    catalog: &Catalog,
    policy: Option<&tidb_model::PolicyRefInfo>,
) -> Result<Option<PlacementSettings>, DriverError> {
    let Some(policy) = policy else {
        return Ok(None);
    };
    let Some(found) = catalog.policy(policy.name.original()) else {
        return Err(DriverError::Mysql(MysqlError::unknown(&format!(
            "Policy with name '{}' not found",
            policy.name.original()
        ))));
    };
    Ok(found
        .placement_settings
        .as_ref()
        .map(|settings| settings.read().clone()))
}

/// Go `fetchSchedulingInfo`: each physical id's placement, as the listing
/// prints it. A partition without a policy of its own inherits the table's.
fn scheduling(
    catalog: &Catalog,
    target: &Target<'_>,
) -> Result<std::collections::HashMap<i64, PlacementSettings>, DriverError> {
    let table_placement =
        policy_placement(catalog, target.table.and_then(KvTable::placement_policy))?;
    let mut by_id = std::collections::HashMap::new();
    match target.table.and_then(KvTable::partition) {
        Some(partitioning) => {
            for definition in &partitioning.definitions {
                let placement = policy_placement(catalog, definition.placement_policy.as_ref())?
                    .or_else(|| table_placement.clone());
                if let Some(placement) = placement {
                    by_id.insert(definition.id, placement);
                }
            }
        }
        None => {
            if let Some(placement) = table_placement {
                by_id.insert(target.id, placement);
            }
        }
    }
    Ok(by_id)
}

/// The rows of `SHOW TABLE <table> [PARTITION (...)] [INDEX <index>] REGIONS`.
///
/// `table_path` is the statement's name, `[table]` or `[database, table]`,
/// resolved against `current_database` as Go's planner does.
pub fn show_table_regions(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    partitions: &[String],
    index: Option<&str>,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let target = resolve(catalog, database, table_name, "show table regions")?;
    let (mut physical_ids, has_global_index) = physical_ids(&target, partitions)?;
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    match index.filter(|name| !name.is_empty()) {
        Some(name) => {
            let found = target.table.and_then(|table| {
                table
                    .indexes()
                    .iter()
                    .find(|index| index.name.go_lower_eq(name))
            });
            let Some(found) = found else {
                return Err(DriverError::KeyNotExists {
                    key: name.to_owned(),
                    table: target.name.clone(),
                });
            };
            if found.global {
                physical_ids = vec![target.id];
            }
            for physical_id in physical_ids {
                index_regions(catalog, found.id, physical_id, &mut seen, &mut rows)?;
            }
        }
        None => {
            if has_global_index {
                physical_ids.insert(0, target.id);
            }
            for physical_id in physical_ids {
                table_regions(catalog, &target, physical_id, &mut seen, &mut rows)?;
            }
        }
    }
    // Go `checkRegionsStatus`: whether PD is scattering each region.
    let store = catalog.region_store();
    for row in &mut rows {
        row.region.scattering = store
            .check_region_in_scattering(row.region.id)
            .map_err(|error| DriverError::Mysql(MysqlError::unknown(error)))?;
    }
    let placements = scheduling(catalog, &target)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let (constraints, state) = match placements.get(&row.physical_id) {
                Some(placement) => (placement.to_string(), PENDING.to_owned()),
                None => (String::new(), String::new()),
            };
            vec![
                Datum::UInt(row.region.id),
                Datum::Bytes(row.start.into_bytes()),
                Datum::Bytes(row.end.into_bytes()),
                Datum::UInt(row.region.peer_id),
                Datum::UInt(row.region.store_id),
                Datum::Bytes(
                    row.region
                        .peers
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                        .into_bytes(),
                ),
                Datum::Int(i64::from(row.region.scattering)),
                Datum::UInt(row.region.written_bytes),
                Datum::UInt(row.region.read_bytes),
                Datum::Int(row.region.approximate_size),
                Datum::Int(row.region.approximate_keys),
                Datum::Bytes(constraints.into_bytes()),
                Datum::Bytes(state.into_bytes()),
            ]
        })
        .collect())
}

/// `SHOW TABLE <table> [PARTITION (...)] DISTRIBUTIONS`: Go asks PD's HTTP API
/// (`infosync.GetRegionDistributionByKeyRange`), which a store without PD does
/// not have. The statement still validates the table and partitions first.
pub fn show_table_distributions(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    partitions: &[String],
) -> Result<(), DriverError> {
    let target = resolve(catalog, database, table_name, "show table distributions")?;
    physical_ids(&target, partitions)?;
    Err(DriverError::Mysql(MysqlError::unknown(
        "[PD:client:ErrClientGetLeader]get leader failed, pd http cli is nil",
    )))
}
