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
pub fn region_response(
    path: &str,
    store: &dyn RegionStore,
    schema: &Schema,
    security: &tidb_pd_client::ClusterSecurity,
) -> Option<Result<String, String>> {
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    if let Some(rest) = route.strip_prefix("/regions/") {
        if rest.contains('/') {
            return None;
        }
        return match rest {
            "meta" => Some(regions_meta(store)),
            "hot" => Some(regions_hot(store, schema)),
            region_id => Some(region_detail(store, schema, region_id)),
        };
    }
    let rest = route.strip_prefix("/tables/")?;
    let parts: Vec<&str> = rest.split('/').collect();
    let [db_name, table_name, op] = parts.as_slice() else {
        return None;
    };
    match *op {
        "regions" | "ranges" | "disk-usage" => Some(table_response(store, schema, db_name, table_name, op)),
        // supports partition table, only get one physical table, prevent too many scatter schedulers.
        "scatter" | "stop-scatter" => Some((|| {
            let (table, physical_id) = crate::mvcc_routes::get_table(schema, db_name, table_name)?;
            let tbl = table.read();
            if *op == "scatter" {
                handle_scatter_table_request(store, security, &tbl, physical_id)
            } else {
                handle_stop_scatter_table_request(store, security, &tbl, physical_id)
            }
        })()),
        _ => None,
    }
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

/// Go `RegionHandler` for `RegionHot`: `ScrapeHotInfo` of the hot writes and
/// reads, each hot region with the table or index its range covers.
fn regions_hot(store: &dyn RegionStore, schema: &Schema) -> Result<String, String> {
    let catalog = schema();
    let schemas = tidb_executor::memtable_retrievers::all_schemas(&catalog);
    let scrape_hot_info = |hot_type| -> Result<Vec<HotTableIndex>, String> {
        let client = tidb_store_helper::try_get_pdhttp_client(store)
            .ok_or_else(|| tidb_store_helper::PD_HTTP_CLIENT_UNAVAILABLE.to_owned())?;
        let region_metrics = tidb_store_helper::fetch_hot_region(&client, hot_type)?;
        Ok(tidb_store_helper::fetch_region_table_index(store, &region_metrics, &schemas)?
            .iter()
            .map(HotTableIndex::from)
            .collect())
    };
    let hot_read = scrape_hot_info(tidb_store_helper::HotType::Read)?;
    let hot_write = scrape_hot_info(tidb_store_helper::HotType::Write)?;
    to_json(&std::collections::BTreeMap::from([("read", hot_read), ("write", hot_write)]))
}

/// Go `helper.RegionMetric` as `WriteData` marshals it.
#[derive(Serialize)]
struct RegionMetric {
    flow_bytes: u64,
    max_hot_degree: i64,
    #[serde(rename = "region_count")]
    count: i64,
}

/// Go `helper.HotTableIndex` as `WriteData` marshals it.
#[derive(Serialize)]
struct HotTableIndex {
    region_id: u64,
    region_metric: RegionMetric,
    db_name: String,
    table_name: String,
    table_id: i64,
    index_name: String,
    index_id: i64,
}

impl From<&tidb_store_helper::HotTableIndex> for HotTableIndex {
    fn from(index: &tidb_store_helper::HotTableIndex) -> Self {
        Self {
            region_id: index.region_id,
            region_metric: RegionMetric {
                flow_bytes: index.region_metric.flow_bytes,
                max_hot_degree: index.region_metric.max_hot_degree,
                count: index.region_metric.count,
            },
            db_name: index.db_name.clone(),
            table_name: index.table_name.clone(),
            table_id: index.table_id,
            index_name: index.index_name.clone(),
            index_id: index.index_id,
        }
    }
}

/// Go `net/url.QueryEscape` over a key's bytes.
fn query_escape(bytes: &[u8]) -> String {
    let mut escaped = String::with_capacity(bytes.len());
    for byte in bytes {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(char::from(*byte));
            }
            b' ' => escaped.push('+'),
            _ => escaped.push_str(&format!("%{byte:02X}")),
        }
    }
    escaped
}

/// Go `TableHandler.getPDAddr`.
fn get_pd_addr(store: &dyn RegionStore) -> Result<Vec<String>, String> {
    let pd_addrs = store.get_pd_addrs()?;
    if pd_addrs.is_empty() {
        return Err("pd unavailable".to_owned());
    }
    Ok(pd_addrs)
}

/// Go `TableHandler.addScatterSchedule`: a PD `scatter-range` scheduler over
/// `[start_key, end_key)`.
fn add_scatter_schedule(
    store: &dyn RegionStore,
    security: &tidb_pd_client::ClusterSecurity,
    start_key: &[u8],
    end_key: &[u8],
    name: &str,
) -> Result<(), String> {
    let pd_addrs = get_pd_addr(store)?;
    let input = std::collections::BTreeMap::from([
        ("end_key", query_escape(end_key)),
        ("name", "scatter-range".to_owned()),
        ("range_name", name.to_owned()),
        ("start_key", query_escape(start_key)),
    ]);
    // Go `json.Marshal` escapes HTML characters.
    let v = tidb_stringutil::go_json_html_escape(
        &serde_json::to_string(&input).map_err(|error| error.to_string())?,
    );
    let schedule_url = format!(
        "{}://{}{}",
        internal_http_schema(security),
        pd_addrs[0],
        tikv_client::pd_http::SCHEDULERS
    );
    tidb_store_driver_txn::cluster_http::internal_http_client(security)?
        .post(schedule_url)
        .header("Content-Type", "application/json")
        .body(v)
        .send()
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Go `TableHandler.deleteScatterSchedule`.
fn delete_scatter_schedule(
    store: &dyn RegionStore,
    security: &tidb_pd_client::ClusterSecurity,
    name: &str,
) -> Result<(), String> {
    let pd_addrs = get_pd_addr(store)?;
    let schedule_url = format!(
        "{}://{}{}",
        internal_http_schema(security),
        pd_addrs[0],
        tikv_client::pd_http::scatter_range_scheduler_with_name(name)
    );
    tidb_store_driver_txn::cluster_http::internal_http_client(security)?
        .delete(schedule_url)
        .send()
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Go `util.InternalHTTPSchema`.
fn internal_http_schema(security: &tidb_pd_client::ClusterSecurity) -> &'static str {
    if security.is_tls_enabled() {
        "https"
    } else {
        "http"
    }
}

/// Go `TableHandler.handleScatterTableRequest`.
fn handle_scatter_table_request(
    store: &dyn RegionStore,
    security: &tidb_pd_client::ClusterSecurity,
    tbl: &tidb_model::TableInfo,
    table_id: i64,
) -> Result<String, String> {
    // for record
    let (start_key, end_key) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
    let start_key = encode_bytes(&start_key);
    let end_key = encode_bytes(&end_key);
    let table_name = format!("{}-{table_id}", tbl.name.original());
    add_scatter_schedule(store, security, &start_key, &end_key, &table_name)
        .map_err(|error| format!("scatter record error: {error}"))?;
    // for indices
    for index in tbl.indices.iter_deref() {
        let index = index.read();
        let (start_key, end_key) = tidb_tablecodec::get_table_index_key_range(table_id, index.id);
        let start_key = encode_bytes(&start_key);
        let end_key = encode_bytes(&end_key);
        let name = format!("{table_name}-{}", index.name.original());
        add_scatter_schedule(store, security, &start_key, &end_key, &name)
            .map_err(|error| format!("scatter index({name}) error: {error}"))?;
    }
    to_json(&"success!")
}

/// Go `TableHandler.handleStopScatterTableRequest`.
fn handle_stop_scatter_table_request(
    store: &dyn RegionStore,
    security: &tidb_pd_client::ClusterSecurity,
    tbl: &tidb_model::TableInfo,
    table_id: i64,
) -> Result<String, String> {
    // for record
    let table_name = format!("{}-{table_id}", tbl.name.original());
    delete_scatter_schedule(store, security, &table_name)
        .map_err(|error| format!("stop scatter record error: {error}"))?;
    // for indices
    for index in tbl.indices.iter_deref() {
        let name = format!("{table_name}-{}", index.read().name.original());
        delete_scatter_schedule(store, security, &name)
            .map_err(|error| format!("delete scatter index({name}) error: {error}"))?;
    }
    to_json(&"success!")
}

/// Go `codec.EncodeBytes([]byte{}, key)`.
fn encode_bytes(key: &[u8]) -> Vec<u8> {
    let mut buffer = Vec::new();
    tidb_codec::encode_bytes(&mut buffer, key);
    buffer
}

#[cfg(test)]
pub(crate) mod hot_scatter_tests {
    use super::*;
    use std::io::{BufRead, Read, Write};
    use std::sync::{Arc, Mutex};

    /// A PD HTTP server answering each request with `reply(method, path)`,
    /// recording `(method, path, body)`.
    pub(crate) fn pd_fixture(
        reply: impl Fn(&str, &str) -> String + Send + 'static,
    ) -> (String, Arc<Mutex<Vec<(String, String, String)>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = std::io::BufReader::new(&mut stream);
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let mut parts = request.split_whitespace();
                let method = parts.next().unwrap().to_owned();
                let path = parts.next().unwrap().to_owned();
                let answer = reply(&method, &path);
                recorded.lock().unwrap().push((method, path, String::from_utf8(body).unwrap()));
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                    answer.len()
                )
                .unwrap();
            }
        });
        (address, requests)
    }

    pub(crate) struct Store {
        pub(crate) pd_http: Option<tikv_client::pd_http::Client>,
        pub(crate) pd_addrs: Vec<String>,
        pub(crate) regions: Vec<(u64, Vec<u8>, Vec<u8>)>,
    }

    impl std::fmt::Debug for Store {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("Store")
        }
    }

    impl RegionStore for Store {
        fn split_regions(&self, _: Vec<Vec<u8>>, _: bool, _: Option<i64>) -> (Vec<u64>, Option<String>) {
            unreachable!()
        }
        fn wait_scatter_region_finish(&self, _: u64, _: i64) -> Result<(), String> {
            unreachable!()
        }
        fn check_region_in_scattering(&self, _: u64) -> Result<bool, String> {
            unreachable!()
        }
        fn load_regions_in_key_range(&self, _: &[u8], _: &[u8]) -> Result<Vec<tidb_kv::region_map::RegionMeta>, String> {
            unreachable!()
        }
        fn pd_http_client(&self) -> Option<tikv_client::pd_http::Client> {
            self.pd_http.clone()
        }
        fn locate_region_by_id(&self, region_id: u64) -> Result<(Vec<u8>, Vec<u8>), String> {
            self.regions
                .iter()
                .find(|(id, _, _)| *id == region_id)
                .map(|(_, start, end)| (start.clone(), end.clone()))
                .ok_or_else(|| format!("region {region_id} not found"))
        }
        fn get_pd_addrs(&self) -> Result<Vec<String>, String> {
            Ok(self.pd_addrs.clone())
        }
    }

    fn table(id: i64, name: &str, indexes: &[(i64, &str)]) -> tidb_model::TableInfo {
        tidb_model::TableInfo {
            id,
            name: tidb_ast::CiString::new(name),
            state: tidb_model::SchemaState::PUBLIC,
            columns: vec![tidb_model::ColumnInfo {
                id: 1,
                name: tidb_ast::CiString::new("a"),
                offset: 0,
                state: tidb_model::SchemaState::PUBLIC,
                field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Long),
                ..tidb_model::ColumnInfo::default()
            }]
            .into(),
            indices: indexes
                .iter()
                .map(|(id, name)| tidb_model::IndexInfo {
                    id: *id,
                    name: tidb_ast::CiString::new(*name),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::IndexInfo::default()
                })
                .collect::<Vec<_>>()
                .into(),
            ..tidb_model::TableInfo::default()
        }
    }

    fn schema_with(table: tidb_model::TableInfo) -> impl Fn() -> tidb_infoschema::SessionExtendedInfoSchema {
        move || {
            tidb_infoschema::SessionExtendedInfoSchema::init_with_db_infos(
                vec![tidb_model::GoShared::new(tidb_model::DBInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("test"),
                    state: tidb_model::SchemaState::PUBLIC,
                    deprecated_tables: vec![table.clone_like_go()].into(),
                    ..tidb_model::DBInfo::default()
                })],
                Vec::new(),
                Vec::new(),
                1,
            )
        }
    }

    /// Go `TableHandler.handleScatterTableRequest` and
    /// `handleStopScatterTableRequest`: one PD `scatter-range` scheduler for
    /// the records and one per index, named `<table>-<id>[-<index>]`.
    #[test]
    fn table_scatter_adds_and_deletes_gos_schedulers() {
        let (address, requests) = pd_fixture(|_, _| "\"ok\"".to_owned());
        let store = Store { pd_http: None, pd_addrs: vec![address], regions: Vec::new() };
        let schema = schema_with(table(41, "t", &[(1, "idx")]));
        let security = tidb_pd_client::ClusterSecurity::plaintext();
        assert_eq!(
            region_response("/tables/test/t/scatter", &store, &schema, &security).unwrap().unwrap(),
            "\"success!\""
        );
        assert_eq!(
            region_response("/tables/test/t/stop-scatter", &store, &schema, &security).unwrap().unwrap(),
            "\"success!\""
        );
        let requests = requests.lock().unwrap().clone();
        let (start, end) = tidb_tablecodec::table_key::get_table_handle_key_range(41);
        let record: serde_json::Value = serde_json::from_str(&requests[0].2).unwrap();
        assert_eq!(
            (requests[0].0.as_str(), requests[0].1.as_str()),
            ("POST", "/pd/api/v1/schedulers")
        );
        assert_eq!(record["name"], "scatter-range");
        assert_eq!(record["range_name"], "t-41");
        assert_eq!(record["start_key"], query_escape(&encode_bytes(&start)));
        assert_eq!(record["end_key"], query_escape(&encode_bytes(&end)));
        let index: serde_json::Value = serde_json::from_str(&requests[1].2).unwrap();
        assert_eq!(index["range_name"], "t-41-idx");
        assert_eq!(
            requests[2..].iter().map(|(method, path, _)| (method.as_str(), path.as_str())).collect::<Vec<_>>(),
            [
                ("DELETE", "/pd/api/v1/schedulers/scatter-range-scheduler-t-41"),
                ("DELETE", "/pd/api/v1/schedulers/scatter-range-scheduler-t-41-idx"),
            ]
        );
    }
}
