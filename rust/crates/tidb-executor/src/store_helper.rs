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

//! The part of Go `pkg/store/helper` (`helper.go`) the PD-backed memory
//! tables read: `TryGetPDHTTPClient`, `GetRegions` and `GetPDRegionStats`
//! over the store's PD HTTP client; region-to-table attribution by key range
//! (`GetTablesInfoWithKeyRange`, `ParseRegionsTableInfos`,
//! `GetRegionsTableInfo`); and the hot-region scrape (`FetchHotRegion`,
//! `FetchRegionTableIndex`) with its region frames (`RegionFrameRange`,
//! `NewFrameItemFromRegionKey`, `GetRecordFrame`, `GetIndexFrame`).
//!
//! Go walks an `infoschema.SchemaAndTable`; here that is [`all_schemas`],
//! built from the session's catalog in Go's infoschema v2 order.

use std::collections::{BTreeMap, HashMap};

use tidb_codec::table_key::{
    decode_key_head, decode_record_key, encode_index_seek_key, encode_table_prefix,
    get_table_handle_key_range, KeyHead, RecordHandle, TABLE_PREFIX, TABLE_SPLIT_KEY_LEN,
};
use tidb_kv::region_map::RegionStore;
use crate::{Catalog, TableEntry};
use tidb_hack::GoToLower;
use tikv_client::pd_http;

/// Go `helper.TryGetPDHTTPClient`'s failure for a store without PD.
pub const PD_HTTP_CLIENT_UNAVAILABLE: &str = "pd http client unavailable";

/// Go `helper.TryGetPDHTTPClient`: the store's PD HTTP client tagged with
/// the helper's caller ID, `None` for a store without PD.
pub fn try_get_pd_http_client(store: &dyn RegionStore) -> Option<pd_http::Client> {
    store
        .pd_http_client()
        .map(|client| client.with_caller_id("tidb-store-helper"))
}

/// Runs one PD HTTP request from synchronous code on the store runtime the
/// client's connections live on; a failure is Go's plain error text.
pub fn block_on_pd<T, F>(request: F) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, tikv_client::Error>> + Send,
    T: Send,
{
    tidb_txnkv::driver::tikv_store::block_on_store_runtime(request)?
        .map_err(|error| error.to_string())
}

/// Go `Store.GetCodec()` for this node's store: client-go's
/// `NewCodecV1(ModeTxn)`, as the store opens no keyspace.
fn region_key_codec() -> tikv_client::request::ApiV1Codec {
    tikv_client::request::ApiV1Codec::new(tikv_client::request::KeyMode::Txn)
}

/// Go `kv.Key.PrefixNext`.
fn prefix_next(key: &[u8]) -> Vec<u8> {
    tidb_txnkv::Key::from(key.to_vec())
        .prefix_next()
        .as_bytes()
        .to_vec()
}

/// Go `Helper.GetRegions`: every region of the store's key range, from PD.
pub fn get_regions(client: &pd_http::Client) -> Result<pd_http::RegionsInfo, String> {
    let (start_key, end_key) = region_key_codec().encode_region_range(&[], &[]);
    let key_range = pd_http::KeyRange::new(start_key, end_key);
    block_on_pd(client.get_regions_by_key_range(&key_range, -1))
}

/// The regions of one physical table's whole `t{id}` prefix, record and
/// index keys alike -- Go `memtableRetriever.getRegionsInfoForSingleTable`.
pub fn get_regions_by_table_prefix(
    client: &pd_http::Client,
    table_id: i64,
) -> Result<pd_http::RegionsInfo, String> {
    let start = encode_table_prefix(table_id);
    let end = prefix_next(&start);
    let (start_key, end_key) = region_key_codec().encode_region_range(&start, &end);
    let key_range = pd_http::KeyRange::new(start_key, end_key);
    block_on_pd(client.get_regions_by_key_range(&key_range, -1))
}

/// Go `Helper.GetPDRegionStats` with `noIndexStats` false: PD's region
/// statistics for the table's whole `t{id}` prefix.
pub fn get_pd_region_stats(
    client: &pd_http::Client,
    table_id: i64,
) -> Result<pd_http::RegionStats, String> {
    let start = encode_table_prefix(table_id);
    let end = prefix_next(&start);
    let (start_key, end_key) = region_key_codec().encode_region_range(&start, &end);
    let key_range = pd_http::KeyRange::new(start_key, end_key);
    block_on_pd(client.get_region_status_by_key_range(&key_range, false))
}

/// One partition of a [`HelperTable`]: Go `model.PartitionDefinition`'s ID
/// and `Name.O`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelperPartition {
    pub(crate) id: i64,
    pub(crate) name: String,
}

/// One index of a [`HelperTable`]: Go `model.IndexInfo`'s ID, `Name.O` and
/// `Global`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelperIndex {
    pub(crate) id: i64,
    pub(crate) name: String,
    pub(crate) global: bool,
}

/// The facts of a Go `model.TableInfo` the helper reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelperTable {
    pub(crate) id: i64,
    /// Go `TableInfo.Name.O`.
    pub(crate) name: String,
    /// Go `TableInfo.Partition.Definitions`; empty for a table without
    /// partitioning.
    pub(crate) partitions: Vec<HelperPartition>,
    /// Go `TableInfo.Indices`.
    pub(crate) indices: Vec<HelperIndex>,
    /// Go `TableInfo.IsCommonHandle`.
    pub(crate) is_common_handle: bool,
}

/// One Go `model.DBInfo` with its `SchemaTableInfos`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelperSchema {
    /// Go `DBInfo.Name.O`.
    pub(crate) name: String,
    pub(crate) tables: Vec<HelperTable>,
}

impl HelperSchema {
    /// Go `metadef.IsMemDB(db.Name.L)`.
    pub(crate) fn is_mem_db(&self) -> bool {
        tidb_metadef::db::is_mem_db(&self.name.go_to_lower())
    }
}

/// Go infoschema v2's `AllSchemas` with each schema's `SchemaTableInfos`:
/// the stored schemas in descending lower-case name order, each listing its
/// tables, views and sequences in the meta KV's key order (`Table:<id>`
/// sorts by the ID's decimal text), then the memory schemas.
///
/// Go keeps the memory schemas in a `sync.Map` and their tables in a map, so
/// both orders are unspecified there; this lists `INFORMATION_SCHEMA` then
/// `METRICS_SCHEMA`, tables by name. `PERFORMANCE_SCHEMA` is left out: its
/// Go table IDs (`perfschema.tableIDMap`) have no port here. Its IDs lie
/// between the other two memory schemas' and hold no keys, so no region
/// boundary falls among them.
pub fn all_schemas(catalog: &Catalog) -> Vec<HelperSchema> {
    let mut stored = catalog
        .database_names()
        .into_iter()
        .filter(|name| !tidb_metadef::db::is_mem_db(&name.go_to_lower()))
        .collect::<Vec<_>>();
    stored.sort_by_key(|name| std::cmp::Reverse(name.go_to_lower()));
    let mut schemas = Vec::with_capacity(stored.len() + 2);
    for schema in stored {
        let mut tables = Vec::new();
        for table_name in catalog.table_names(&schema).unwrap_or_default() {
            let Some(entry) = catalog.table_in(&schema, &table_name) else {
                continue;
            };
            let table = match entry {
                TableEntry::Kv(table) => HelperTable {
                    id: table.table_id(),
                    name: table.name(),
                    partitions: table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| HelperPartition {
                                    id: definition.id,
                                    name: definition.name.clone(),
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                    indices: table
                        .indexes()
                        .iter()
                        .map(|index| HelperIndex {
                            id: index.id,
                            name: index.name.clone(),
                            global: index.global,
                        })
                        .collect(),
                    is_common_handle: !table.common_handle_offsets().is_empty(),
                },
                TableEntry::View(view) => HelperTable {
                    id: view.id,
                    name: view.name.clone(),
                    partitions: Vec::new(),
                    indices: Vec::new(),
                    is_common_handle: false,
                },
                TableEntry::Sequence(sequence) => HelperTable {
                    id: sequence.id,
                    name: sequence.name.clone(),
                    partitions: Vec::new(),
                    indices: Vec::new(),
                    is_common_handle: false,
                },
                TableEntry::Mem(_) => continue,
            };
            tables.push(table);
        }
        tables.sort_by_key(|table| table.id.to_string());
        schemas.push(HelperSchema {
            name: schema,
            tables,
        });
    }
    let memory_table = |id: i64, name: &str| HelperTable {
        id,
        name: name.to_owned(),
        partitions: Vec::new(),
        indices: Vec::new(),
        is_common_handle: false,
    };
    let information_schema = catalog
        .table_names(tidb_infoschema::infoschema_meta::INFORMATION_SCHEMA)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|name| {
            crate::infoschema_reader::memory_table_id(&name).map(|id| memory_table(id, &name))
        })
        .collect::<Vec<_>>();
    if !information_schema.is_empty() {
        schemas.push(HelperSchema {
            name: "INFORMATION_SCHEMA".to_owned(),
            tables: information_schema,
        });
    }
    if catalog.has_database("metrics_schema") {
        schemas.push(HelperSchema {
            name: "METRICS_SCHEMA".to_owned(),
            tables: crate::infoschema_reader::METRICS_SCHEMA_TABLES
                .iter()
                .map(|(name, id)| memory_table(*id, name))
                .collect(),
        });
    }
    schemas
}

/// Go `helper.TableInfo`: the table, partition or index a key range belongs
/// to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableInfo {
    /// Go `DB.Name.O`.
    pub(crate) db_name: String,
    /// Go `Table.ID`, the logical table's.
    pub(crate) table_id: i64,
    /// Go `Table.Name.O`.
    pub(crate) table_name: String,
    /// Go `Partition` when `IsPartition`.
    pub(crate) partition: Option<HelperPartition>,
    /// Go `Index` when `IsIndex`.
    pub(crate) index: Option<HelperIndex>,
}

impl TableInfo {
    /// Go `metadef.IsMemDB(DB.Name.L)`.
    pub(crate) fn is_mem_db(&self) -> bool {
        tidb_metadef::db::is_mem_db(&self.db_name.go_to_lower())
    }
}

/// Go `helper.TableInfoWithKeyRange`: a table, partition or index with its
/// region key range as upper-case hex.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableInfoWithKeyRange {
    pub(crate) table_info: TableInfo,
    pub(crate) start_key: String,
    pub(crate) end_key: String,
}

/// Go `bytesKeyToHex`.
fn bytes_key_to_hex(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02X}")).collect()
}

/// Go `newTableInfoWithKeyRange`.
fn new_table_info_with_key_range(
    db: &HelperSchema,
    table: &HelperTable,
    partition: Option<&HelperPartition>,
    index: Option<&HelperIndex>,
) -> TableInfoWithKeyRange {
    let physical_id = partition.map_or(table.id, |partition| partition.id);
    let (start, end) = match index {
        None => get_table_handle_key_range(physical_id),
        // Go `tablecodec.GetTableIndexKeyRange`.
        Some(index) => (
            encode_index_seek_key(physical_id, index.id, &[]),
            encode_index_seek_key(physical_id, index.id, &[255]),
        ),
    };
    let (start, end) = region_key_codec().encode_region_range(&start, &end);
    TableInfoWithKeyRange {
        table_info: TableInfo {
            db_name: db.name.clone(),
            table_id: table.id,
            table_name: table.name.clone(),
            partition: partition.cloned(),
            index: index.cloned(),
        },
        start_key: bytes_key_to_hex(&start),
        end_key: bytes_key_to_hex(&end),
    }
}

/// Go `Helper.GetTablesInfoWithKeyRange`: every table (or partition) record
/// range and index range of `schemas`, the memory schemas left out when
/// `filter_mem_dbs` (Go's `FilterMemDBs` filter), sorted by start key.
pub fn get_tables_info_with_key_range(
    schemas: &[HelperSchema],
    filter_mem_dbs: bool,
) -> Vec<TableInfoWithKeyRange> {
    let mut tables = Vec::new();
    for db in schemas {
        if filter_mem_dbs && db.is_mem_db() {
            continue;
        }
        for table in &db.tables {
            if table.partitions.is_empty() {
                tables.push(new_table_info_with_key_range(db, table, None, None));
            } else {
                for partition in &table.partitions {
                    tables.push(new_table_info_with_key_range(
                        db,
                        table,
                        Some(partition),
                        None,
                    ));
                }
            }
            for index in &table.indices {
                if table.partitions.is_empty() || index.global {
                    tables.push(new_table_info_with_key_range(db, table, None, Some(index)));
                    continue;
                }
                for partition in &table.partitions {
                    tables.push(new_table_info_with_key_range(
                        db,
                        table,
                        Some(partition),
                        Some(index),
                    ));
                }
            }
        }
    }
    tables.sort_by(|left, right| left.start_key.cmp(&right.start_key));
    tables
}

/// Go `isBeforeKeyRange` (`x` ends at or before `start_key`).
fn is_before_key_range(x_end_key: &str, start_key: &str) -> bool {
    !x_end_key.is_empty() && x_end_key <= start_key
}

/// Go `isBehindKeyRange` (`x` starts at or after `end_key`).
fn is_behind_key_range(x_start_key: &str, end_key: &str) -> bool {
    !end_key.is_empty() && x_start_key >= end_key
}

/// Go `isIntersecting`.
fn is_intersecting(x_start_key: &str, x_end_key: &str, y_start_key: &str, y_end_key: &str) -> bool {
    !is_before_key_range(x_end_key, y_start_key) && !is_behind_key_range(x_start_key, y_end_key)
}

/// Go `Helper.ParseRegionsTableInfos`: for each region, the tables,
/// partitions and indices its key range intersects. The first region past
/// the last table range reads an empty entry and ends the walk, so the
/// regions after it have no entry.
pub fn parse_regions_table_infos(
    regions: &[&pd_http::RegionInfo],
    tables: &[TableInfoWithKeyRange],
) -> HashMap<i64, Vec<TableInfo>> {
    let mut table_infos = HashMap::with_capacity(regions.len());
    if tables.is_empty() || regions.is_empty() {
        return table_infos;
    }
    // `tables` is sorted by `get_tables_info_with_key_range`.
    let mut regions = regions.to_vec();
    regions.sort_by(|left, right| left.start_key.cmp(&right.start_key));
    let mut idx = 0;
    'out: for region in regions {
        let infos = table_infos.entry(region.id).or_insert_with(Vec::new);
        infos.clear();
        while is_behind_key_range(&region.start_key, &tables[idx].end_key) {
            idx += 1;
            if idx >= tables.len() {
                break 'out;
            }
        }
        let mut i = idx;
        while i < tables.len()
            && is_intersecting(
                &region.start_key,
                &region.end_key,
                &tables[i].start_key,
                &tables[i].end_key,
            )
        {
            infos.push(tables[i].table_info.clone());
            i += 1;
        }
    }
    table_infos
}

/// Go `Helper.GetRegionsTableInfo`.
pub fn get_regions_table_info(
    regions_info: &pd_http::RegionsInfo,
    schemas: &[HelperSchema],
    filter_mem_dbs: bool,
) -> HashMap<i64, Vec<TableInfo>> {
    let tables = get_tables_info_with_key_range(schemas, filter_mem_dbs);
    let regions = regions_info.regions.iter().collect::<Vec<_>>();
    parse_regions_table_infos(&regions, &tables)
}

/// Go `helper.RegionMetric`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RegionMetric {
    pub(crate) flow_bytes: u64,
    pub(crate) max_hot_degree: i64,
    pub(crate) count: i64,
}

/// Go `helper.HotRead` / `helper.HotWrite`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotType {
    Read,
    Write,
}

/// Go `Helper.FetchHotRegion`: each hot region PD reports as a leader, by
/// region ID. Go fills a map from PD's per-store map, a later store's entry
/// replacing an earlier one's; both are unordered there, and here the
/// stores are walked by ID and the regions listed by ID.
pub fn fetch_hot_region(
    client: &pd_http::Client,
    hot_type: HotType,
) -> Result<BTreeMap<u64, RegionMetric>, String> {
    let region_resp = match hot_type {
        HotType::Read => block_on_pd(client.get_hot_read_regions())?,
        HotType::Write => block_on_pd(client.get_hot_write_regions())?,
    };
    Ok(region_metrics(&region_resp))
}

/// The metric map Go `Helper.FetchHotRegion` builds from PD's answer.
pub fn region_metrics(
    region_resp: &pd_http::StoreHotPeersInfos,
) -> BTreeMap<u64, RegionMetric> {
    let mut stores = region_resp.as_leader.iter().collect::<Vec<_>>();
    stores.sort_by_key(|(store_id, _)| **store_id);
    let mut metric = BTreeMap::new();
    for (_, hot_regions) in stores {
        for region in &hot_regions.stats {
            metric.insert(
                region.region_id,
                RegionMetric {
                    flow_bytes: region.byte_rate as u64,
                    max_hot_degree: region.hot_degree,
                    count: 0,
                },
            );
        }
    }
    metric
}

/// Go `helper.FrameItem`, without the record ID and index values no reader
/// here prints.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FrameItem {
    pub(crate) db_name: String,
    pub(crate) table_name: String,
    pub(crate) table_id: i64,
    pub(crate) is_record: bool,
    pub(crate) index_name: String,
    pub(crate) index_id: i64,
}

/// Go `helper.RegionFrameRange`: the frames at a region's two ends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionFrameRange {
    pub(crate) first: FrameItem,
    pub(crate) last: FrameItem,
}

/// Go `helper.NewRegionFrameRange` over a region's decoded key bounds.
pub fn new_region_frame_range(
    start_key: &[u8],
    end_key: &[u8],
) -> Result<RegionFrameRange, String> {
    let first = if start_key.is_empty() {
        // An empty start key starts at -infinity.
        FrameItem {
            index_id: i64::MIN,
            is_record: false,
            table_id: i64::MIN,
            ..FrameItem::default()
        }
    } else {
        new_frame_item_from_region_key(start_key)?
    };
    let last = if end_key.is_empty() {
        // An empty end key ends at +infinity.
        FrameItem {
            table_id: i64::MAX,
            index_id: i64::MAX,
            is_record: true,
            ..FrameItem::default()
        }
    } else {
        new_frame_item_from_region_key(end_key)?
    };
    Ok(RegionFrameRange { first, last })
}

/// Go `helper.NewFrameItemFromRegionKey`. A record key with a common handle
/// names the `PRIMARY` index; Go decodes the handle's values and index
/// values too, which no reader here prints, and ignores decode errors past
/// the key head.
pub fn new_frame_item_from_region_key(key: &[u8]) -> Result<FrameItem, String> {
    let mut frame = FrameItem::default();
    match decode_key_head(key) {
        Ok(KeyHead::Record { table_id }) => {
            frame.table_id = table_id;
            frame.is_record = true;
            if let Ok((_, RecordHandle::Common(_))) = decode_record_key(key) {
                frame.index_name = "PRIMARY".to_owned();
            }
            return Ok(frame);
        }
        Ok(KeyHead::Index { table_id, index_id }) => {
            frame.table_id = table_id;
            frame.index_id = index_id;
            return Ok(frame);
        }
        Err(_) => {}
    }
    if key.starts_with(TABLE_PREFIX) {
        // If SplitTable is enabled, the key may be `t{id}`.
        if key.len() == TABLE_SPLIT_KEY_LEN {
            frame.table_id = tidb_codec::decode_int(&key[TABLE_PREFIX.len()..])
                .map_or(0, |(_, table_id)| table_id);
            return Ok(frame);
        }
        // Go `tablecodec.DecodeKeyHead`'s error.
        if key.len() < TABLE_SPLIT_KEY_LEN {
            return Err("insufficient bytes to decode value".to_owned());
        }
        return Err(format!(
            "invalid key - {}",
            tidb_error::mysql::go_quote_bytes(key)
        ));
    }
    // A key before the table prefix is before every table; one after it is
    // after every table.
    if key < TABLE_PREFIX {
        frame.table_id = i64::MIN;
        frame.index_id = i64::MIN;
        frame.is_record = false;
    } else {
        frame.table_id = i64::MAX;
        frame.is_record = true;
    }
    Ok(frame)
}

impl RegionFrameRange {
    /// Go `RegionFrameRange.GetRecordFrame`.
    pub(crate) fn get_record_frame(
        &mut self,
        table_id: i64,
        db_name: &str,
        table_name: &str,
        is_common_handle: bool,
    ) -> Option<FrameItem> {
        let mut frame = if table_id == self.first.table_id && self.first.is_record {
            self.first.db_name = db_name.to_owned();
            self.first.table_name = table_name.to_owned();
            Some(self.first.clone())
        } else if table_id == self.last.table_id && self.last.is_record {
            self.last.db_name = db_name.to_owned();
            self.last.table_name = table_name.to_owned();
            Some(self.last.clone())
        } else if table_id >= self.first.table_id && table_id < self.last.table_id {
            Some(FrameItem {
                db_name: db_name.to_owned(),
                table_name: table_name.to_owned(),
                table_id,
                is_record: true,
                ..FrameItem::default()
            })
        } else {
            None
        };
        if let Some(frame) = frame.as_mut() {
            if frame.is_record && is_common_handle {
                frame.index_name = "PRIMARY".to_owned();
            }
        }
        frame
    }

    /// Go `RegionFrameRange.GetIndexFrame`.
    pub(crate) fn get_index_frame(
        &mut self,
        table_id: i64,
        index_id: i64,
        db_name: &str,
        table_name: &str,
        index_name: &str,
    ) -> Option<FrameItem> {
        if table_id == self.first.table_id
            && !self.first.is_record
            && index_id == self.first.index_id
        {
            self.first.db_name = db_name.to_owned();
            self.first.table_name = table_name.to_owned();
            self.first.index_name = index_name.to_owned();
            return Some(self.first.clone());
        }
        if table_id == self.last.table_id && index_id == self.last.index_id {
            self.last.db_name = db_name.to_owned();
            self.last.table_name = table_name.to_owned();
            self.last.index_name = index_name.to_owned();
            return Some(self.last.clone());
        }
        let greater_than_first = table_id > self.first.table_id
            || (table_id == self.first.table_id
                && !self.first.is_record
                && index_id > self.first.index_id);
        let less_than_last = table_id < self.last.table_id
            || (table_id == self.last.table_id
                && (self.last.is_record || index_id < self.last.index_id));
        if greater_than_first && less_than_last {
            return Some(FrameItem {
                db_name: db_name.to_owned(),
                table_name: table_name.to_owned(),
                table_id,
                is_record: false,
                index_name: index_name.to_owned(),
                index_id,
            });
        }
        None
    }
}

/// Go `findRangeInPhysicalTable`.
fn find_range_in_physical_table(
    hot_range: &mut RegionFrameRange,
    physical_id: i64,
    db_name: &str,
    table_name: &str,
    indices: &[HelperIndex],
    is_common_handle: bool,
) -> Option<FrameItem> {
    if let Some(frame) =
        hot_range.get_record_frame(physical_id, db_name, table_name, is_common_handle)
    {
        return Some(frame);
    }
    indices.iter().find_map(|index| {
        hot_range.get_index_frame(physical_id, index.id, db_name, table_name, &index.name)
    })
}

/// Go `findRangeInTable`: a partition is named `table(partition)`.
fn find_range_in_table(
    hot_range: &mut RegionFrameRange,
    db: &HelperSchema,
    table: &HelperTable,
) -> Option<FrameItem> {
    if table.partitions.is_empty() {
        return find_range_in_physical_table(
            hot_range,
            table.id,
            &db.name,
            &table.name,
            &table.indices,
            table.is_common_handle,
        );
    }
    table.partitions.iter().find_map(|partition| {
        let table_partition = format!("{}({})", table.name, partition.name);
        find_range_in_physical_table(
            hot_range,
            partition.id,
            &db.name,
            &table_partition,
            &table.indices,
            table.is_common_handle,
        )
    })
}

/// Go `Helper.FindTableIndexOfRegion`: the first table, in schema order,
/// whose record or index range the region covers.
pub fn find_table_index_of_region(
    schemas: &[HelperSchema],
    hot_range: &mut RegionFrameRange,
) -> Option<FrameItem> {
    for db in schemas {
        for table in &db.tables {
            if let Some(frame) = find_range_in_table(hot_range, db, table) {
                return Some(frame);
            }
        }
    }
    None
}

/// Go `helper.HotTableIndex`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HotTableIndex {
    pub(crate) region_id: u64,
    pub(crate) region_metric: RegionMetric,
    pub(crate) db_name: String,
    pub(crate) table_name: String,
    pub(crate) table_id: i64,
    pub(crate) index_name: String,
    pub(crate) index_id: i64,
}

/// Go `Helper.FetchRegionTableIndex`: each hot region with the table or
/// index its range covers. A region the region cache cannot locate is
/// skipped (Go logs it). Go's `filter` argument is unused there, so every
/// schema is searched.
pub fn fetch_region_table_index(
    store: &dyn RegionStore,
    metrics: &BTreeMap<u64, RegionMetric>,
    schemas: &[HelperSchema],
) -> Result<Vec<HotTableIndex>, String> {
    let mut hot_tables = Vec::with_capacity(metrics.len());
    for (region_id, region_metric) in metrics {
        let mut hot_table = HotTableIndex {
            region_id: *region_id,
            region_metric: *region_metric,
            ..HotTableIndex::default()
        };
        let (start_key, end_key) = match store.locate_region_by_id(*region_id) {
            Ok(bounds) => bounds,
            Err(error) => {
                tidb_util::logutil::bg_logger().error(
                    "locate region failed",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
                );
                continue;
            }
        };
        let mut hot_range = new_region_frame_range(&start_key, &end_key)?;
        if let Some(frame) = find_table_index_of_region(schemas, &mut hot_range) {
            hot_table.db_name = frame.db_name;
            hot_table.table_name = frame.table_name;
            hot_table.table_id = frame.table_id;
            hot_table.index_name = frame.index_name;
            hot_table.index_id = frame.index_id;
        }
        hot_tables.push(hot_table);
    }
    Ok(hot_tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region(id: i64, start_key: &str, end_key: &str) -> pd_http::RegionInfo {
        pd_http::RegionInfo {
            id,
            start_key: start_key.to_owned(),
            end_key: end_key.to_owned(),
            ..pd_http::RegionInfo::default()
        }
    }

    fn schema(name: &str, tables: Vec<HelperTable>) -> HelperSchema {
        HelperSchema {
            name: name.to_owned(),
            tables,
        }
    }

    fn table(id: i64, name: &str, indices: &[(i64, &str)]) -> HelperTable {
        HelperTable {
            id,
            name: name.to_owned(),
            partitions: Vec::new(),
            indices: indices
                .iter()
                .map(|(id, name)| HelperIndex {
                    id: *id,
                    name: (*name).to_owned(),
                    global: false,
                })
                .collect(),
            is_common_handle: false,
        }
    }

    fn hex_region_key(raw: &[u8]) -> String {
        bytes_key_to_hex(&region_key_codec().encode_region_key(raw))
    }

    /// Go `TestParseRegionsTableInfos`-style attribution: a region covering
    /// a table's record and index ranges lists both, in key order; the first
    /// region past every range reads an empty entry and the next none; the
    /// first region (empty start key) sees the first table.
    #[test]
    fn parse_regions_table_infos_attributes_ranges() {
        let schemas = vec![schema(
            "test",
            vec![table(100, "t1", &[(1, "idx")]), table(102, "t2", &[])],
        )];
        let tables = get_tables_info_with_key_range(&schemas, true);
        // t100 index range sorts before its record range ('_i' < '_r').
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].table_info.index.as_ref().unwrap().name, "idx");
        assert!(tables[1].table_info.index.is_none());
        assert_eq!(tables[2].table_info.table_id, 102);

        let split_101 = hex_region_key(&encode_table_prefix(101));
        let split_200 = hex_region_key(&encode_table_prefix(200));
        let split_300 = hex_region_key(&encode_table_prefix(300));
        let regions = [
            region(1, "", &split_101),
            region(2, &split_101, &split_200),
            region(3, &split_200, &split_300),
            region(4, &split_300, ""),
        ];
        let refs = regions.iter().collect::<Vec<_>>();
        let infos = parse_regions_table_infos(&refs, &tables);
        let names = |id: i64| {
            infos.get(&id).map(|list| {
                list.iter()
                    .map(|info| {
                        (
                            info.table_name.clone(),
                            info.index.as_ref().map(|index| index.name.clone()),
                        )
                    })
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(
            names(1),
            Some(vec![
                ("t1".to_owned(), Some("idx".to_owned())),
                ("t1".to_owned(), None)
            ])
        );
        assert_eq!(names(2), Some(vec![("t2".to_owned(), None)]));
        assert_eq!(names(3), Some(Vec::new()));
        assert_eq!(names(4), None);
    }

    /// Go `GetTablesInfoWithKeyRange`: a partitioned table lists each
    /// partition's record and (local) index range, and a global index once
    /// under the logical table.
    #[test]
    fn partitioned_table_ranges() {
        let mut partitioned = table(110, "p", &[(1, "local_idx"), (2, "global_idx")]);
        partitioned.indices[1].global = true;
        partitioned.partitions = vec![
            HelperPartition {
                id: 111,
                name: "p0".to_owned(),
            },
            HelperPartition {
                id: 112,
                name: "p1".to_owned(),
            },
        ];
        let tables = get_tables_info_with_key_range(&[schema("test", vec![partitioned])], false);
        let described = tables
            .iter()
            .map(|range| {
                (
                    range.table_info.partition.as_ref().map(|p| p.id),
                    range.table_info.index.as_ref().map(|i| i.id),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            described,
            vec![
                (None, Some(2)),
                (Some(111), Some(1)),
                (Some(111), None),
                (Some(112), Some(1)),
                (Some(112), None),
            ]
        );
    }

    /// Go `NewFrameItemFromRegionKey` and `FindTableIndexOfRegion`: a region
    /// from inside one table's index to another table's records belongs to
    /// the first table whose range it covers.
    #[test]
    fn hot_region_frames_find_table_index() {
        let start = encode_index_seek_key(100, 1, &[]);
        let end = encode_table_prefix(102);
        let mut hot_range = new_region_frame_range(&start, &end).unwrap();
        assert_eq!(hot_range.first.table_id, 100);
        assert_eq!(hot_range.first.index_id, 1);
        assert!(!hot_range.first.is_record);
        assert_eq!(hot_range.last.table_id, 102);
        let schemas = vec![schema(
            "test",
            vec![table(100, "t1", &[(1, "idx")]), table(102, "t2", &[])],
        )];
        let frame = find_table_index_of_region(&schemas, &mut hot_range).unwrap();
        // t1's record range follows its index range inside the region.
        assert_eq!(frame.table_name, "t1");
        assert!(frame.is_record);
        assert_eq!(frame.index_name, "");

        let mut open = new_region_frame_range(&[], &[]).unwrap();
        let frame = find_table_index_of_region(&schemas, &mut open).unwrap();
        assert_eq!((frame.table_id, frame.db_name.as_str()), (100, "test"));

        assert_eq!(
            new_frame_item_from_region_key(b"m").unwrap().table_id,
            i64::MIN
        );
        assert_eq!(
            new_frame_item_from_region_key(b"u").unwrap().table_id,
            i64::MAX
        );
        assert_eq!(
            new_frame_item_from_region_key(b"t\x80").unwrap_err(),
            "insufficient bytes to decode value"
        );
    }

    /// Go `FetchHotRegion`: the leader statistics by region, a later store's
    /// entry for the same region replacing an earlier one's.
    #[test]
    fn region_metrics_from_hot_peers() {
        let stat = |region_id: u64, byte_rate: f64, hot_degree: i64| pd_http::HotPeerStatShow {
            region_id,
            byte_rate,
            hot_degree,
            ..pd_http::HotPeerStatShow::default()
        };
        let mut infos = pd_http::StoreHotPeersInfos::default();
        infos.as_leader.insert(
            1,
            pd_http::HotPeersStat {
                stats: vec![stat(7, 10.9, 3)],
                ..pd_http::HotPeersStat::default()
            },
        );
        infos.as_leader.insert(
            2,
            pd_http::HotPeersStat {
                stats: vec![stat(7, 20.0, 4), stat(5, 1.0, 1)],
                ..pd_http::HotPeersStat::default()
            },
        );
        infos.as_peer.insert(
            3,
            pd_http::HotPeersStat {
                stats: vec![stat(9, 1.0, 1)],
                ..pd_http::HotPeersStat::default()
            },
        );
        let metrics = region_metrics(&infos);
        assert_eq!(metrics.keys().copied().collect::<Vec<_>>(), vec![5, 7]);
        assert_eq!(
            metrics[&7],
            RegionMetric {
                flow_bytes: 20,
                max_hot_degree: 4,
                count: 0
            }
        );
    }
}
