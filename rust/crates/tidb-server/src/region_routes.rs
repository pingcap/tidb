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

//! Go `pkg/server/handler/tikvhandler`'s region handlers over the store's
//! region cache and PD: `RegionHandler` (`/regions/meta`,
//! `/regions/{regionID}`) and `TableHandler`'s `regions`, `ranges` and
//! `disk-usage` operations, with `handler.RegionMeta` /
//! `TikvHandlerTool.GetRegionsMeta`. Go registers them only for a TiKV
//! store (`s.cfg.Store == config.StoreTypeTiKV`).

use base64::Engine;
use serde::Serialize;
use tidb_kv::region_map::{PdPeer, PdRegion, RegionStore};

/// The catalog the handlers read (Go `TikvHandlerTool.Schema()`).
pub type Schema = dyn Fn() -> tidb_infoschema::SessionExtendedInfoSchema + Send + Sync;

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Go `metapb.Peer` through `encoding/json` (gogoproto `omitempty` tags).
#[derive(Serialize)]
struct PeerJson {
    #[serde(skip_serializing_if = "is_zero_u64")]
    id: u64,
    #[serde(skip_serializing_if = "is_zero_u64")]
    store_id: u64,
    #[serde(skip_serializing_if = "is_zero_i32")]
    role: i32,
    #[serde(skip_serializing_if = "is_false")]
    is_witness: bool,
}

impl From<&PdPeer> for PeerJson {
    fn from(peer: &PdPeer) -> Self {
        Self {
            id: peer.id,
            store_id: peer.store_id,
            role: peer.role,
            is_witness: peer.is_witness,
        }
    }
}

/// Go `metapb.RegionEpoch` through `encoding/json`.
#[derive(Serialize)]
struct RegionEpochJson {
    #[serde(skip_serializing_if = "is_zero_u64")]
    conf_ver: u64,
    #[serde(skip_serializing_if = "is_zero_u64")]
    version: u64,
}

/// Go `handler.RegionMeta`.
#[derive(Serialize)]
struct RegionMeta {
    region_id: u64,
    leader: Option<PeerJson>,
    peers: Option<Vec<PeerJson>>,
    region_epoch: Option<RegionEpochJson>,
}

impl RegionMeta {
    fn of(region_id: u64, region: &PdRegion) -> Self {
        Self {
            region_id,
            leader: region.leader.as_ref().map(PeerJson::from),
            // Go's nil `Meta.Peers` marshals as `null`.
            peers: (!region.peers.is_empty()).then(|| region.peers.iter().map(PeerJson::from).collect()),
            region_epoch: region
                .region_epoch
                .map(|(conf_ver, version)| RegionEpochJson { conf_ver, version }),
        }
    }
}

/// Go `TikvHandlerTool.GetRegionsMeta`.
fn get_regions_meta(store: &dyn RegionStore, region_ids: &[u64]) -> Result<Vec<RegionMeta>, String> {
    region_ids
        .iter()
        .map(|&region_id| {
            let region = store.pd_region_by_id(region_id)?;
            Ok(RegionMeta::of(region_id, &region))
        })
        .collect()
}

/// Go's `[]byte` through `encoding/json`: base64, a nil slice `null`.
fn bytes_json(bytes: &[u8]) -> Option<String> {
    (!bytes.is_empty()).then(|| base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// Go `RangeDetail`.
#[derive(Serialize)]
struct RangeDetail {
    start_key: Option<String>,
    end_key: Option<String>,
    start_key_hex: String,
    end_key_hex: String,
}

/// Go `createRangeDetail`.
fn create_range_detail(start: &[u8], end: &[u8]) -> RangeDetail {
    RangeDetail {
        start_key: bytes_json(start),
        end_key: bytes_json(end),
        start_key_hex: hex(start),
        end_key_hex: hex(end),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Go `RegionDetail`: its `RangeDetail` is embedded, so its fields come
/// first.
#[derive(Serialize)]
struct RegionDetail {
    #[serde(flatten)]
    range_detail: RangeDetail,
    region_id: u64,
    frames: Option<Vec<tidb_store_helper::FrameItem>>,
}

impl RegionDetail {
    /// Go `addTableInRange`.
    fn add_table_in_range(
        &mut self,
        db_name: &str,
        cur_table: &tidb_model::TableInfo,
        r: &mut tidb_store_helper::RegionFrameRange,
    ) {
        let t_name = cur_table.name.original().to_owned();
        let t_id = cur_table.id;
        let pi = cur_table.get_partition_info().map(|pi| pi.read().definitions.snapshot());
        let is_common_handle = cur_table.is_common_handle;
        let mut frames = Vec::new();
        for index in cur_table.indices.iter_deref() {
            let index = index.read();
            if index.primary && is_common_handle {
                continue;
            }
            match &pi {
                Some(definitions) => {
                    for def in definitions {
                        let name = format!("{t_name}({})", def.name.original());
                        if let Some(f) = r.get_index_frame(def.id, index.id, db_name, &name, index.name.original()) {
                            frames.push(f);
                        }
                    }
                }
                None => {
                    if let Some(f) = r.get_index_frame(t_id, index.id, db_name, &t_name, index.name.original()) {
                        frames.push(f);
                    }
                }
            }
        }

        match &pi {
            Some(definitions) => {
                for def in definitions {
                    let name = format!("{t_name}({})", def.name.original());
                    if let Some(f) = r.get_record_frame(def.id, db_name, &name, is_common_handle) {
                        frames.push(f);
                    }
                }
            }
            None => {
                if let Some(f) = r.get_record_frame(t_id, db_name, &t_name, is_common_handle) {
                    frames.push(f);
                }
            }
        }
        if !frames.is_empty() {
            self.frames.get_or_insert_with(Vec::new).extend(frames);
        }
    }
}

/// Go `IndexRegions`.
#[derive(Serialize)]
struct IndexRegions {
    name: String,
    id: i64,
    regions: Vec<RegionMeta>,
}

/// Go `TableRegions`.
#[derive(Serialize)]
struct TableRegions {
    name: String,
    id: i64,
    record_regions: Vec<RegionMeta>,
    indices: Vec<IndexRegions>,
}

/// Go `TableRanges`.
#[derive(Serialize)]
struct TableRanges {
    name: String,
    id: i64,
    table: RangeDetail,
    record: RangeDetail,
    index: RangeDetail,
    #[serde(skip_serializing_if = "Option::is_none")]
    indices: Option<std::collections::BTreeMap<String, RangeDetail>>,
}

fn to_json(value: &impl Serialize) -> Result<String, String> {
    serde_json::to_string(value).map_err(|error| error.to_string())
}

/// Go `RegionHandler.ServeHTTP` and `TableHandler.ServeHTTP`'s `regions`,
/// `ranges` and `disk-usage` operations: the JSON `WriteData` writes, or the
/// error `WriteError` writes.
pub fn region_response(path: &str, store: &dyn RegionStore, schema: &Schema) -> Option<Result<String, String>> {
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    if let Some(rest) = route.strip_prefix("/regions/") {
        if rest.contains('/') {
            return None;
        }
        return match rest {
            "meta" => Some(regions_meta(store)),
            // Go `/regions/hot` scrapes PD's hot statistics, not ported here.
            "hot" => None,
            region_id => Some(region_detail(store, schema, region_id)),
        };
    }
    let rest = route.strip_prefix("/tables/")?;
    let parts: Vec<&str> = rest.split('/').collect();
    let [db_name, table_name, op] = parts.as_slice() else {
        return None;
    };
    if !matches!(*op, "regions" | "ranges" | "disk-usage") {
        return None;
    }
    Some(table_response(store, schema, db_name, table_name, op))
}

/// Go `RegionHandler` for `RegionsMeta`: the meta regions (`[m, n)`).
fn regions_meta(store: &dyn RegionStore) -> Result<String, String> {
    let record_region_ids: Vec<u64> = store.pd_scan_regions(b"m", b"n")?.iter().map(|region| region.id).collect();
    to_json(&get_regions_meta(store, &record_region_ids)?)
}

/// Go `RegionHandler` for `/regions/{regionID}`.
fn region_detail(store: &dyn RegionStore, schema: &Schema, region_id: &str) -> Result<String, String> {
    let region_id_int = tidb_go_strconv::parse_int(region_id, 0, 64).map_err(|(_, error)| error.to_string())?;
    let region_id = region_id_int as u64;

    // locate region
    let (start_key, end_key) = store.locate_region_by_id(region_id)?;
    let mut frame_range = tidb_store_helper::new_region_frame_range(&start_key, &end_key)?;

    // create RegionDetail from RegionFrameRange
    let mut region_detail = RegionDetail {
        range_detail: create_range_detail(&start_key, &end_key),
        region_id,
        frames: None,
    };
    let catalog = schema();
    // Since we need a database's name for each frame, and a table's database
    // name can not get from table's ID directly, walk every table.
    for database in catalog.stored_schemas() {
        let database = database.read();
        if tidb_metadef::db::is_mem_db(database.name.lowercase()) {
            continue;
        }
        for table in database.deprecated_tables.iter_deref() {
            region_detail.add_table_in_range(database.name.original(), &table.read(), &mut frame_range);
        }
    }
    to_json(&region_detail)
}

/// Go `TableHandler.getRegionsByID`.
fn get_regions_by_id(
    store: &dyn RegionStore,
    table: &tidb_model::TableInfo,
    id: i64,
    name: &str,
) -> Result<TableRegions, String> {
    let metas = |start: &[u8], end: &[u8]| -> Result<Vec<RegionMeta>, String> {
        Ok(store
            .pd_scan_regions(start, end)?
            .iter()
            .map(|region| RegionMeta::of(region.id, region))
            .collect())
    };
    // for record
    let (start_key, end_key) = tidb_tablecodec::table_key::get_table_handle_key_range(id);
    let record_regions = metas(&start_key, &end_key)?;

    // for indices
    let mut indices = Vec::new();
    for index in table.indices.iter_deref() {
        let index = index.read();
        let (start_key, end_key) = tidb_tablecodec::get_table_index_key_range(id, index.id);
        indices.push(IndexRegions {
            name: index.name.original().to_owned(),
            id: index.id,
            regions: metas(&start_key, &end_key)?,
        });
    }

    Ok(TableRegions {
        name: name.to_owned(),
        id,
        record_regions,
        indices,
    })
}

/// Go `createTableRanges`.
fn create_table_ranges(tbl_id: i64, tbl_name: &str, table: &tidb_model::TableInfo) -> TableRanges {
    use tidb_tablecodec::table_key::{encode_table_index_prefix, encode_table_prefix, gen_table_index_prefix, gen_table_record_prefix};
    let index_prefix = gen_table_index_prefix(tbl_id);
    let record_prefix = gen_table_record_prefix(tbl_id);
    let table_end = encode_table_prefix(tbl_id + 1);
    let mut ranges = TableRanges {
        name: tbl_name.to_owned(),
        id: tbl_id,
        table: create_range_detail(&encode_table_prefix(tbl_id), &table_end),
        record: create_range_detail(&record_prefix, &table_end),
        index: create_range_detail(&index_prefix, &record_prefix),
        indices: None,
    };
    let indexes: Vec<_> = table.indices.iter_deref().collect();
    if !indexes.is_empty() {
        let mut index_ranges = std::collections::BTreeMap::new();
        for index in indexes {
            let index = index.read();
            let start = encode_table_index_prefix(tbl_id, index.id);
            let end = encode_table_index_prefix(tbl_id, index.id + 1);
            index_ranges.insert(index.name.original().to_owned(), create_range_detail(&start, &end));
        }
        ranges.indices = Some(index_ranges);
    }
    ranges
}

/// Go `TableHandler.ServeHTTP` for one table operation.
fn table_response(
    store: &dyn RegionStore,
    schema: &Schema,
    db_name: &str,
    table_name: &str,
    op: &str,
) -> Result<String, String> {
    let catalog = schema();
    let Some((_, table)) = catalog.table_info_by_name(db_name, table_name) else {
        return Err(format!("[schema:1146]Table '{db_name}.{table_name}' doesn't exist"));
    };
    let meta = table.read();
    let partitions = meta.get_partition_info().map(|pi| pi.read().definitions.snapshot());
    match op {
        "regions" => match partitions {
            // Partitioned table.
            Some(definitions) => {
                let data = definitions
                    .iter()
                    .map(|def| get_regions_by_id(store, &meta, def.id, def.name.original()))
                    .collect::<Result<Vec<_>, _>>()?;
                to_json(&(!data.is_empty()).then_some(data))
            }
            None => to_json(&get_regions_by_id(store, &meta, meta.id, meta.name.original())?),
        },
        "ranges" => match partitions {
            // Partitioned table.
            Some(definitions) => {
                let data: Vec<_> = definitions
                    .iter()
                    .map(|def| create_table_ranges(def.id, def.name.original(), &meta))
                    .collect();
                to_json(&(!data.is_empty()).then_some(data))
            }
            None => to_json(&create_table_ranges(meta.id, meta.name.original(), &meta)),
        },
        _ => {
            // Go `handleDiskUsageRequest`: `GetPDRegionStats(ctx, id, false).StorageSize`.
            let client = tidb_store_helper::try_get_pdhttp_client(store)
                .ok_or_else(|| tidb_store_helper::PD_HTTP_CLIENT_UNAVAILABLE.to_owned())?;
            let stats = tidb_store_helper::get_pd_region_stats(&client, meta.id)?;
            to_json(&stats.storage_size)
        }
    }
}
