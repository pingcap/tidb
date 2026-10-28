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

//! Go's memory-table retrievers that request by their scan's extractor:
//! `MetricsSummaryRetriever` and `MetricsSummaryByLabelRetriever`
//! (`pkg/executor/metrics_reader.go`), the PD-backed tables
//! (`dataForTiKVStoreStatus`, `setDataForTiKVRegionStatus`,
//! `setDataForTiDBHotRegions`, `tableStorageStatsRetriever` in
//! `infoschema_reader.go`; `tikvRegionPeersRetriever` and
//! `hotRegionsHistoryRetriver` in `memtable_reader.go`) and `SHOW PLACEMENT
//! LABELS` (`show_placement.go`). The PD-backed ones ask PD through the
//! store's PD HTTP client (Go `helper.Storage.GetPDHTTPClient()`); a store
//! without PD answers Go's no-PD errors.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use tidb_datatype::Datum;
use tidb_kv::region_map::RegionStore;
use tidb_executor::{Catalog, DriverError};
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_planner::logical::mem_table::QueryTimeRange;
use tidb_planner::memtable_predicate_extractor::{
    HotRegionsHistoryTableExtractor, MetricSummaryTableExtractor, TableStorageStatsExtractor,
    TiKVRegionStatusExtractor, TikvRegionPeersExtractor,
};
use tikv_client::pd_http;

use crate::infoschema::{PrivMask, SchemaVisibility};
use crate::store_helper::{self, PD_HTTP_CLIENT_UNAVAILABLE};

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

fn plain_error(message: &str) -> DriverError {
    DriverError::unsupported(message.to_owned())
}

/// Go `MetricsSummaryRetriever.retrieve` (`by_label` false) and
/// `MetricsSummaryByLabelRetriever.retrieve` (`by_label` true) after the
/// PROCESS check: every metrics table the extractor admits, in name order,
/// summarised by the restricted SQL `query` runs over `metrics_schema`.
pub(crate) fn metrics_summary_rows(
    extractor: &MetricSummaryTableExtractor,
    time_range: &QueryTimeRange,
    by_label: bool,
    mut query: impl FnMut(&str) -> Result<Vec<Vec<Datum>>, DriverError>,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    if extractor.skip_request {
        return Ok(Vec::new());
    }
    // Go `QueryTimeRange.Condition`.
    let condition = format!(
        "where time>='{}' and time<='{}'",
        time_range.from, time_range.to
    );
    let quantiles = || {
        if extractor.quantiles.is_empty() {
            None
        } else {
            Some(
                extractor
                    .quantiles
                    .iter()
                    .map(|quantile| format!("{quantile:.6}"))
                    .collect::<Vec<_>>()
                    .join(","),
            )
        }
    };
    let mut total_rows = Vec::new();
    // `METRIC_TABLE_MAP` is sorted by name, Go's `slices.Sort(tables)`.
    for (name, def) in tidb_metadef::metric_table_def::METRIC_TABLE_MAP {
        // Go `inspectionFilter.enable`.
        if !extractor.metrics_names.is_empty() && !extractor.metrics_names.contains(*name) {
            continue;
        }
        let sql = if by_label {
            let mut cols = def.labels.to_vec();
            let mut cond = condition.clone();
            if def.quantile > 0.0 {
                cols.push("quantile");
                match quantiles() {
                    Some(quantiles) => cond.push_str(&format!(" and quantile in ({quantiles})")),
                    None => cond.push_str(" and quantile=0.99"),
                }
            }
            if cols.is_empty() {
                format!(
                    "select sum(value),avg(value),min(value),max(value) from `metrics_schema`.`{name}` {cond}"
                )
            } else {
                let cols = cols.join("`,`");
                format!(
                    "select sum(value),avg(value),min(value),max(value),`{cols}` from `metrics_schema`.`{name}` {cond} group by `{cols}` order by `{cols}`"
                )
            }
        } else if def.quantile > 0.0 {
            let quantiles = quantiles().unwrap_or_else(|| "0.99".to_owned());
            format!(
                "select sum(value),avg(value),min(value),max(value),quantile from `metrics_schema`.`{name}` {condition} and quantile in ({quantiles}) group by quantile order by quantile"
            )
        } else {
            format!(
                "select sum(value),avg(value),min(value),max(value) from `metrics_schema`.`{name}` {condition}"
            )
        };
        let rows = query(&sql).map_err(|error| {
            plain_error(&format!(
                "execute '{sql}' failed: {}",
                error.to_mysql_error().message
            ))
        })?;
        let non_instance_label_index =
            usize::from(def.labels.first().is_some_and(|label| *label == "instance"));
        // Skip sum/avg/min/max.
        const SKIP_COLS: usize = 4;
        for row in rows {
            // Go `Row.GetString`, and `Row.GetFloat64`, which reads a NULL
            // cell as zero.
            let string_at = |index: usize| match row.get(index) {
                Some(Datum::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
                Some(Datum::String(text)) => String::from_utf8_lossy(text.bytes()).into_owned(),
                _ => String::new(),
            };
            let float_at = |index: usize| match row.get(index) {
                Some(Datum::Real(value)) => Datum::Real(*value),
                _ => Datum::Real(0.0),
            };
            let quantile = if def.quantile > 0.0 {
                float_at(row.len() - 1)
            } else {
                Datum::Null
            };
            let values = [float_at(0), float_at(1), float_at(2), float_at(3)];
            if !by_label {
                let mut out = vec![text(name), quantile];
                out.extend(values);
                out.push(text(def.comment));
                total_rows.push(out);
                continue;
            }
            let instance = if non_instance_label_index > 0 {
                string_at(SKIP_COLS)
            } else {
                String::new()
            };
            let labels = def.labels[non_instance_label_index..]
                .iter()
                .enumerate()
                .map(|(i, label)| {
                    let value = string_at(SKIP_COLS + non_instance_label_index + i);
                    if *label == "store" || *label == "store_id" {
                        format!("store_id:{value}")
                    } else {
                        value
                    }
                })
                .collect::<Vec<_>>();
            let mut out = vec![
                text(&instance),
                text(name),
                text(&labels.join(", ")),
                quantile,
            ];
            out.extend(values);
            out.push(text(def.comment));
            total_rows.push(out);
        }
    }
    Ok(total_rows)
}

/// Go `memtableRetriever.dataForTiKVStoreStatus`: one row per store PD
/// lists. Without PD, Go's `TryGetPDHTTPClient` fails the statement with
/// 1105 'pd http client unavailable' (oracle-captured); the Eval wrap marks
/// that error evaluation-origin, which the statement completion door treats
/// as buffer-invisible.
pub(crate) fn tikv_store_status_rows(
    store: &dyn RegionStore,
    sem_redacts: bool,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let Some(client) = store_helper::try_get_pd_http_client(store) else {
        return Err(DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Unsupported(PD_HTTP_CLIENT_UNAVAILABLE),
        )));
    };
    let stores_stat =
        store_helper::block_on_pd(client.get_stores()).map_err(|error| plain_error(&error))?;
    tikv_store_status_rows_from(&stores_stat, sem_redacts).map_err(|error| plain_error(&error))
}

/// A `DATETIME` cell for a PD time: Go `types.NewTime(types.FromGoTime(t),
/// mysql.TypeDatetime, types.DefaultFsp)`, in the wall clock of the offset
/// PD wrote.
fn pd_time_datum(time: &pd_http::Time) -> Datum {
    let core = tidb_datatype::core_time_from_datetime(time.0);
    tidb_datatype::Time::new(core, tidb_datatype::TimeType::DateTime, 0)
        .map_or(Datum::Null, Datum::Time)
}

/// The rows Go `dataForTiKVStoreStatus` builds from PD's `GetStores`
/// answer. `sem_redacts` is Go's SEM check without `RESTRICTED_TABLES_ADMIN`:
/// the address, capacity, available, start time and uptime read NULL.
///
/// Go marshals `Store.Labels` with `encoding/json`; PD omits an empty label
/// list, so Go's nil slice marshals to JSON `null`.
pub(crate) fn tikv_store_status_rows_from(
    stores_stat: &pd_http::StoresInfo,
    sem_redacts: bool,
) -> Result<Vec<Vec<Datum>>, String> {
    let mut rows = Vec::with_capacity(stores_stat.stores.len());
    for store_stat in &stores_stat.stores {
        let (store, status) = (&store_stat.store, &store_stat.status);
        let data = if store.labels.is_empty() {
            "null".to_owned()
        } else {
            serde_json::to_string(&store.labels).map_err(|error| error.to_string())?
        };
        let label = tidb_datatype::BinaryJSON::parse(&data).map_err(|error| error.to_string())?;
        let mut row = vec![
            Datum::Int(store.id),
            text(&store.address),
            Datum::Int(store.state),
            text(&store.state_name),
            Datum::Json(label),
            text(&store.version),
            text(&status.capacity),
            text(&status.available),
            Datum::Int(status.leader_count),
            Datum::Real(status.leader_weight),
            Datum::Real(status.leader_score),
            Datum::Int(status.leader_size),
            Datum::Int(status.region_count),
            Datum::Real(status.region_weight),
            Datum::Real(status.region_score),
            Datum::Int(status.region_size),
            pd_time_datum(&status.start_ts),
            pd_time_datum(&status.last_heartbeat_ts),
            text(&status.uptime),
        ];
        if sem_redacts {
            // Patch out IP addresses etc. for a user without
            // RESTRICTED_TABLES_ADMIN.
            for index in [1, 6, 7, 16, 18] {
                row[index] = Datum::Null;
            }
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Go `showPlacementLabelsResultBuilder`: each label key with the set of
/// values the stores give it.
#[derive(Debug, Default)]
pub(crate) struct ShowPlacementLabelsResultBuilder {
    label_key2values: BTreeMap<String, BTreeSet<String>>,
}

impl ShowPlacementLabelsResultBuilder {
    /// Go `AppendStoreLabels`: one store's `LABEL` cell, a JSON array of
    /// `{"key", "value"}` objects or `null`.
    pub(crate) fn append_store_labels(
        &mut self,
        bj: &tidb_datatype::BinaryJSON,
    ) -> Result<(), String> {
        let value = bj.to_value().map_err(|error| error.to_string())?;
        if value.is_null() {
            return Ok(());
        }
        if !value.is_array() {
            return Err("only array or null type is allowed".to_owned());
        }
        let labels = serde_json::from_value::<Vec<pd_http::StoreLabel>>(value)
            .map_err(|error| error.to_string())?;
        for label in labels {
            self.label_key2values
                .entry(label.key)
                .or_default()
                .insert(label.value);
        }
        Ok(())
    }

    /// Go `BuildRows`: one `(Key, Values)` row per key in key order, the
    /// values a sorted JSON array.
    pub(crate) fn build_rows(&self) -> Result<Vec<Vec<Datum>>, String> {
        let mut rows = Vec::with_capacity(self.label_key2values.len());
        for (key, values) in &self.label_key2values {
            let data = serde_json::to_string(&values.iter().collect::<Vec<_>>())
                .map_err(|error| error.to_string())?;
            let values_json =
                tidb_datatype::BinaryJSON::parse(&data).map_err(|error| error.to_string())?;
            rows.push(vec![text(key), Datum::Json(values_json)]);
        }
        Ok(rows)
    }
}

/// Go `ShowExec.fetchShowPlacementLabels`: the labels of every store. Go
/// reads them with the restricted SQL `SELECT DISTINCT LABEL FROM
/// information_schema.TIKV_STORE_STATUS`; this reads the same retriever's
/// `LABEL` cells directly (the internal session is never SEM-redacted, and
/// the builder's sets make `DISTINCT` moot). A failure is the retriever's
/// error text.
pub(crate) fn show_placement_labels_rows(
    store: &dyn RegionStore,
) -> Result<Vec<Vec<Datum>>, String> {
    let client = store_helper::try_get_pd_http_client(store)
        .ok_or_else(|| PD_HTTP_CLIENT_UNAVAILABLE.to_owned())?;
    let stores_stat = store_helper::block_on_pd(client.get_stores())?;
    let mut builder = ShowPlacementLabelsResultBuilder::default();
    for row in tikv_store_status_rows_from(&stores_stat, false)? {
        if let Some(Datum::Json(bj)) = row.get(4) {
            builder.append_store_labels(bj)?;
        }
    }
    builder.build_rows()
}

/// Go `tikvRegionPeersRetriever.retrieve`: the peers of every region, or of
/// the regions on the extractor's stores (only those stores' peers) and/or
/// with the extractor's region IDs.
pub(crate) fn tikv_region_peers_rows(
    extractor: &TikvRegionPeersExtractor,
    store: &dyn RegionStore,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    if extractor.skip_request {
        return Ok(Vec::new());
    }
    let client = store_helper::try_get_pd_http_client(store)
        .ok_or_else(|| plain_error(PD_HTTP_CLIENT_UNAVAILABLE))?;
    let mut store_map = HashSet::new();
    if extractor.store_ids.is_empty() && extractor.region_ids.is_empty() {
        let regions_info =
            store_helper::get_regions(&client).map_err(|error| plain_error(&error))?;
        return Ok(pack_tikv_region_peers_rows(
            &regions_info.regions,
            extractor,
            &store_map,
        ));
    }
    let mut regions_info_by_store_id = Vec::new();
    let mut region_map = HashMap::new();
    for store_id in &extractor.store_ids {
        // A region with peers on stores 1, 4 and 7 is listed for each of
        // them; `store_map` filters out the peers on other stores.
        store_map.insert(*store_id as i64);
        let store_regions_info =
            store_helper::block_on_pd(client.get_regions_by_store_id(*store_id))
                .map_err(|error| plain_error(&error))?;
        for region_info in store_regions_info.regions {
            // `region_map` removes duplicate regions.
            if !region_map.contains_key(&region_info.id) {
                regions_info_by_store_id.push(region_info.clone());
                region_map.insert(region_info.id, region_info);
            }
        }
    }
    if extractor.region_ids.is_empty() {
        return Ok(pack_tikv_region_peers_rows(
            &regions_info_by_store_id,
            extractor,
            &store_map,
        ));
    }
    let mut regions_info = Vec::new();
    for region_id in &extractor.region_ids {
        match region_map.get(&(*region_id as i64)) {
            Some(region_info) => regions_info.push(region_info.clone()),
            // With store IDs the target region came from them; otherwise it
            // is fetched from PD.
            None if extractor.store_ids.is_empty() => regions_info.push(
                store_helper::block_on_pd(client.get_region_by_id(*region_id))
                    .map_err(|error| plain_error(&error))?,
            ),
            None => {}
        }
    }
    Ok(pack_tikv_region_peers_rows(
        &regions_info,
        extractor,
        &store_map,
    ))
}

/// Go `tikvRegionPeersRetriever.packTiKVRegionPeersRows`.
pub(crate) fn pack_tikv_region_peers_rows(
    regions_info: &[pd_http::RegionInfo],
    extractor: &TikvRegionPeersExtractor,
    store_map: &HashSet<i64>,
) -> Vec<Vec<Datum>> {
    // Go `isUnexpectedStoreID`.
    let is_unexpected_store_id =
        |store_id: i64| !extractor.store_ids.is_empty() && !store_map.contains(&store_id);
    let mut rows = Vec::new();
    for region in regions_info {
        let pending_peer_id_set = region
            .pending_peers
            .iter()
            .map(|peer| peer.id)
            .collect::<HashSet<_>>();
        let down_peer_map = region
            .down_peers
            .iter()
            .map(|peer_stat| (peer_stat.peer.id, peer_stat.down_sec))
            .collect::<HashMap<_, _>>();
        for peer in &region.peers {
            if is_unexpected_store_id(peer.store_id) {
                continue;
            }
            let (status, down_seconds) = if let Some(down_sec) = down_peer_map.get(&peer.id) {
                ("DOWN", Datum::Int(*down_sec))
            } else if pending_peer_id_set.contains(&peer.id) {
                ("PENDING", Datum::Null)
            } else {
                ("NORMAL", Datum::Null)
            };
            rows.push(vec![
                Datum::Int(region.id),
                Datum::Int(peer.id),
                Datum::Int(peer.store_id),
                Datum::Int(i64::from(peer.is_learner)),
                Datum::Int(i64::from(peer.id == region.leader.id)),
                text(status),
                down_seconds,
            ]);
        }
    }
    rows
}

/// Go `memtableRetriever.getRegionsInfoForTable` for a table the infoschema
/// has: its own prefix's regions merged with each partition's.
fn regions_info_for_table(
    store: &dyn RegionStore,
    table: &store_helper::HelperTable,
) -> Result<pd_http::RegionsInfo, String> {
    // Go `getRegionsInfoForSingleTable`.
    let single_table = |table_id: i64| {
        let client = store_helper::try_get_pd_http_client(store)
            .ok_or_else(|| PD_HTTP_CLIENT_UNAVAILABLE.to_owned())?;
        store_helper::get_regions_by_table_prefix(&client, table_id)
    };
    let mut all_regions_info = single_table(table.id)?;
    for partition in &table.partitions {
        let regions_info = single_table(partition.id)?;
        all_regions_info = all_regions_info.merge(Some(&regions_info));
    }
    Ok(all_regions_info)
}

/// Go `memtableRetriever.setDataForTiKVRegionStatus`: the regions of the
/// tables the extractor names (an ID naming no table is skipped) or of the
/// whole store, each read once per table, partition or index its key range
/// covers that is not in a memory schema and that the session may see. A
/// region covering nothing reads one row without table cells.
pub(crate) fn tikv_region_status_rows(
    extractor: Option<&TiKVRegionStatusExtractor>,
    catalog: &Catalog,
    store: &dyn RegionStore,
    visibility: &SchemaVisibility,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let schemas = store_helper::all_schemas(catalog);
    let extractor_table_ids = extractor.map_or(&[][..], |extractor| extractor.tables_id.as_slice());
    let all_regions_info = if extractor_table_ids.is_empty() {
        let client = store_helper::try_get_pd_http_client(store)
            .ok_or_else(|| plain_error(PD_HTTP_CLIENT_UNAVAILABLE))?;
        Some(store_helper::get_regions(&client).map_err(|error| plain_error(&error))?)
    } else {
        let mut all_regions_info: Option<pd_http::RegionsInfo> = None;
        for table_id in extractor_table_ids {
            // Go `is.TableByID`; `ErrTableNotExists` skips the ID.
            let Some(table) = schemas
                .iter()
                .flat_map(|schema| schema.tables.iter())
                .find(|table| table.id == *table_id)
            else {
                continue;
            };
            let regions_info =
                regions_info_for_table(store, table).map_err(|error| plain_error(&error))?;
            all_regions_info = Some(
                all_regions_info
                    .unwrap_or_default()
                    .merge(Some(&regions_info)),
            );
        }
        all_regions_info
    };
    let Some(all_regions_info) = all_regions_info else {
        return Ok(Vec::new());
    };
    let table_infos = store_helper::get_regions_table_info(&all_regions_info, &schemas, false);
    let mut rows = Vec::new();
    for region in &all_regions_info.regions {
        let region_table_list = table_infos.get(&region.id).map_or(&[][..], Vec::as_slice);
        if region_table_list.is_empty() {
            rows.push(new_tikv_region_status_row(region, None));
        }
        for region_table in region_table_list {
            // Exclude virtual schemas.
            if region_table.is_mem_db() {
                continue;
            }
            if !visibility.allows(
                &region_table.db_name.go_to_lower(),
                &region_table.table_name.go_to_lower(),
                PrivMask::Any,
            ) {
                continue;
            }
            if extractor_table_ids.is_empty()
                || extractor_table_ids.contains(&region_table.table_id)
            {
                rows.push(new_tikv_region_status_row(region, Some(region_table)));
            }
        }
    }
    Ok(rows)
}

/// Go `memtableRetriever.setNewTiKVRegionStatusCol`.
pub(crate) fn new_tikv_region_status_row(
    region: &pd_http::RegionInfo,
    table: Option<&store_helper::TableInfo>,
) -> Vec<Datum> {
    let mut row = vec![Datum::Null; 20];
    row[0] = Datum::Int(region.id);
    row[1] = text(&region.start_key);
    row[2] = text(&region.end_key);
    row[6] = Datum::Int(0);
    row[9] = Datum::Int(0);
    if let Some(table) = table {
        row[3] = Datum::Int(table.table_id);
        row[4] = text(&table.db_name);
        row[5] = text(&table.table_name);
        if let Some(index) = &table.index {
            row[6] = Datum::Int(1);
            row[7] = Datum::Int(index.id);
            row[8] = text(&index.name);
        }
        if let Some(partition) = &table.partition {
            row[9] = Datum::Int(1);
            row[10] = Datum::Int(partition.id);
            row[11] = text(&partition.name);
        }
    }
    row[12] = Datum::Int(region.epoch.conf_ver);
    row[13] = Datum::Int(region.epoch.version);
    row[14] = Datum::UInt(region.written_bytes);
    row[15] = Datum::UInt(region.read_bytes);
    row[16] = Datum::Int(region.approximate_size);
    row[17] = Datum::Int(region.approximate_keys);
    if let Some(replication_status) = &region.replication_status {
        row[18] = text(&replication_status.state);
        row[19] = Datum::Int(replication_status.state_id);
    }
    row
}

/// Go `memtableRetriever.setDataForTiDBHotRegions`: the hot read regions,
/// then the hot write regions (Go `Helper.ScrapeHotInfo`), each with the
/// table or index its range covers.
pub(crate) fn tidb_hot_regions_rows(
    catalog: &Catalog,
    store: &dyn RegionStore,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let client = store_helper::try_get_pd_http_client(store)
        .ok_or_else(|| plain_error(PD_HTTP_CLIENT_UNAVAILABLE))?;
    let schemas = store_helper::all_schemas(catalog);
    let mut rows = Vec::new();
    for (hot_type, tp) in [
        (store_helper::HotType::Read, "read"),
        (store_helper::HotType::Write, "write"),
    ] {
        let metrics = store_helper::fetch_hot_region(&client, hot_type)
            .map_err(|error| plain_error(&error))?;
        let hot_tables = store_helper::fetch_region_table_index(store, &metrics, &schemas)
            .map_err(|error| plain_error(&error))?;
        rows.extend(hot_region_rows_by_metrics(&hot_tables, tp));
    }
    Ok(rows)
}

/// Go `memtableRetriever.setDataForHotRegionByMetrics`.
pub(crate) fn hot_region_rows_by_metrics(
    metrics: &[store_helper::HotTableIndex],
    tp: &str,
) -> Vec<Vec<Datum>> {
    metrics
        .iter()
        .map(|tbl_index| {
            let (index_id, index_name) = if tbl_index.index_name.is_empty() {
                (Datum::Null, Datum::Null)
            } else {
                (Datum::Int(tbl_index.index_id), text(&tbl_index.index_name))
            };
            vec![
                Datum::Int(tbl_index.table_id),
                index_id,
                text(&tbl_index.db_name),
                text(&tbl_index.table_name),
                index_name,
                Datum::UInt(tbl_index.region_id),
                text(tp),
                Datum::Int(tbl_index.region_metric.max_hot_degree),
                Datum::Int(tbl_index.region_metric.count),
                Datum::UInt(tbl_index.region_metric.flow_bytes),
            ]
        })
        .collect()
}

/// Go `util.InternalHTTPSchema`: `https` when the cluster TLS CA is set.
fn internal_http_schema() -> &'static str {
    if tidb_config::config_tree::config::get_global_config()
        .security
        .cluster_ssl_ca
        .is_empty()
    {
        "http"
    } else {
        "https"
    }
}

/// Go `hotRegionsHistoryRetriver.retrieve` after the PROCESS check and Go
/// `infoschema.GetPDServerInfo` (`pd_servers`, their status addresses): the
/// time range must be bounded on both sides; each PD server is asked for
/// each hot region type's history, a failed request a warning; the answers
/// merge by update time, then hot degree (Go's `hotRegionsResponseHeap`),
/// and each region reads once per table or index of a stored schema its
/// range covers (a region covering none reads nothing).
///
/// Go sends each request with `util.InternalHTTPClient()` to
/// `/pd/api/v1/hotspot/regions/history` on the server; this sends it with the
/// store's PD HTTP client pinned to that server (`WithTargetURL`), the same
/// endpoint with the same follower-handle header. Go's request errors read
/// differently from the client's.
pub(crate) fn tidb_hot_regions_history_rows(
    extractor: &HotRegionsHistoryTableExtractor,
    pd_servers: &[String],
    catalog: &Catalog,
    store: &dyn RegionStore,
    mut append_warning: impl FnMut(&str),
) -> Result<Vec<Vec<Datum>>, DriverError> {
    if extractor.skip_request {
        return Ok(Vec::new());
    }
    // To avoid overloading the hot regions interface, the time range must be
    // bounded.
    if extractor.start_time == 0 {
        return Err(plain_error(
            "denied to scan hot regions, please specified the start time, \
             such as `update_time > '2020-01-01 00:00:00'`",
        ));
    }
    if extractor.end_time == 0 {
        return Err(plain_error(
            "denied to scan hot regions, please specified the end time, \
             such as `update_time < '2020-01-01 00:00:00'`",
        ));
    }
    let request = pd_http::HistoryHotRegionsRequest {
        start_time: extractor.start_time,
        end_time: extractor.end_time,
        region_ids: extractor.region_ids.clone(),
        store_ids: extractor.store_ids.clone(),
        peer_ids: extractor.peer_ids.clone(),
        is_learners: extractor.is_learners.clone(),
        is_leaders: extractor.is_leaders.clone(),
        hot_region_types: Vec::new(),
    };
    let client = store.pd_http_client();
    let scheme = internal_http_schema();
    let mut results = Vec::new();
    for address in pd_servers {
        for typ in &extractor.hot_region_types {
            let mut request = request.clone();
            request.hot_region_types = vec![typ.clone()];
            let result = match &client {
                Some(client) => store_helper::block_on_pd(
                    client
                        .with_target_url(format!("{scheme}://{address}"))
                        .get_history_hot_regions(&request),
                ),
                None => Err(PD_HTTP_CLIENT_UNAVAILABLE.to_owned()),
            };
            match result {
                Err(error) => append_warning(&error),
                Ok(history) if history.history_hot_region.is_empty() => {}
                Ok(history) => results.push(VecDeque::from(history.history_hot_region)),
            }
        }
    }
    let schemas = store_helper::all_schemas(catalog);
    let tables = store_helper::get_tables_info_with_key_range(&schemas, true);
    let mut final_rows = Vec::new();
    loop {
        let Some(min_index) = (0..results.len()).min_by(|left, right| {
            let (lhs, rhs) = (&results[*left][0], &results[*right][0]);
            (lhs.update_time, lhs.hot_degree).cmp(&(rhs.update_time, rhs.hot_degree))
        }) else {
            break;
        };
        if let Some(his_hot_region) = results[min_index].pop_front() {
            final_rows.extend(hot_region_row_with_schema_info(&his_hot_region, &tables));
        }
        if results[min_index].is_empty() {
            results.remove(min_index);
        }
    }
    Ok(final_rows)
}

/// Go `hotRegionsHistoryRetriver.getHotRegionRowWithSchemaInfo`. Go builds
/// the update time with `time.UnixMilli` in the process's local zone (its
/// `In(tz)` result is discarded), as a `TIMESTAMP` of fsp 0.
pub(crate) fn hot_region_row_with_schema_info(
    his_hot_region: &pd_http::HistoryHotRegion,
    tables: &[store_helper::TableInfoWithKeyRange],
) -> Vec<Vec<Datum>> {
    use chrono::TimeZone;
    use tidb_util::stringutil::go_to_upper;
    let region_id = his_hot_region.region_id as i64;
    let region = pd_http::RegionInfo {
        id: region_id,
        start_key: his_hot_region.start_key.clone(),
        end_key: his_hot_region.end_key.clone(),
        ..pd_http::RegionInfo::default()
    };
    let regions_table_infos = store_helper::parse_regions_table_infos(&[&region], tables);
    let mut rows = Vec::new();
    // Ignore a row without a corresponding schema.
    let Some(table_infos) = regions_table_infos.get(&region_id) else {
        return rows;
    };
    let update_time = chrono::Local
        .timestamp_millis_opt(his_hot_region.update_time)
        .single()
        .and_then(|update_timestamp| {
            let core = tidb_datatype::core_time_from_datetime(update_timestamp);
            tidb_datatype::Time::new(core, tidb_datatype::TimeType::Timestamp, 0).ok()
        })
        .map_or(Datum::Null, Datum::Time);
    for table_info in table_infos {
        let (index_name, index_id) = match &table_info.index {
            Some(index) => (text(&go_to_upper(&index.name)), Datum::Int(index.id)),
            None => (Datum::Null, Datum::Null),
        };
        rows.push(vec![
            update_time.clone(),
            text(&go_to_upper(&table_info.db_name)),
            text(&go_to_upper(&table_info.table_name)),
            Datum::Int(table_info.table_id),
            index_name,
            index_id,
            Datum::Int(region_id),
            Datum::Int(his_hot_region.store_id as i64),
            Datum::Int(his_hot_region.peer_id as i64),
            Datum::Int(i64::from(his_hot_region.is_learner)),
            Datum::Int(i64::from(his_hot_region.is_leader)),
            text(&go_to_upper(&his_hot_region.hot_region_type)),
            Datum::Int(his_hot_region.hot_degree),
            Datum::Real(his_hot_region.flow_bytes),
            Datum::Real(his_hot_region.key_rate),
            Datum::Real(his_hot_region.query_rate),
        ]);
    }
    rows
}

/// Go `tableStorageStatsRetriever`: a scan that names no `TABLE_SCHEMA` is
/// refused; otherwise each named stored schema's tables (or the named ones),
/// the session may see, read one row per physical table with PD's region
/// statistics for its key prefix. A store without PD fails as Go's
/// `helper.GetPDAddr` does (`pd unavailable`), after the tables are listed.
/// A scan whose predicates contradict each other (`SkipRequest`) reads
/// nothing.
///
/// Go walks the extractor's schema and table sets in map order; these are
/// sorted sets.
pub(crate) fn table_storage_stats_rows(
    extractor: &TableStorageStatsExtractor,
    catalog: &Catalog,
    store: &dyn RegionStore,
    visibility: &SchemaVisibility,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    if extractor.skip_request {
        return Ok(Vec::new());
    }
    // Without TABLE_SCHEMA, refuse to traverse every schema and table.
    if extractor.table_schema.is_empty() {
        return Err(plain_error(
            "Please add where clause to filter the column TABLE_SCHEMA. \
             For example, where TABLE_SCHEMA = 'xxx' or where TABLE_SCHEMA in ('xxx', 'yyy')",
        ));
    }
    let schemas = store_helper::all_schemas(catalog);
    let mut initial_tables = Vec::new();
    // Filter the memory schemas.
    for db in extractor
        .table_schema
        .iter()
        .filter(|schema| !tidb_metadef::db::is_mem_db(schema))
    {
        // Go `SchemaTableInfos` / `TableByName` find no table in a missing
        // schema.
        let Some(schema) = schemas
            .iter()
            .find(|schema| schema.name.go_to_lower() == *db)
        else {
            continue;
        };
        if extractor.table_name.is_empty() {
            for table in &schema.tables {
                if visibility.allows(db, &table.name.go_to_lower(), PrivMask::Any) {
                    initial_tables.push((db.clone(), table));
                }
            }
        } else {
            for tb in &extractor.table_name {
                let Some(table) = schema
                    .tables
                    .iter()
                    .find(|table| table.name.go_to_lower() == *tb)
                else {
                    continue;
                };
                if visibility.allows(db, &table.name.go_to_lower(), PrivMask::Any) {
                    initial_tables.push((db.clone(), table));
                }
            }
        }
    }
    // Go `helper.GetPDAddr`: a store without PD addresses fails.
    let Some(client) = store_helper::try_get_pd_http_client(store) else {
        return Err(plain_error("pd unavailable"));
    };
    let mut rows = Vec::new();
    for (db, tbl) in initial_tables {
        let tbl_ids =
            std::iter::once(tbl.id).chain(tbl.partitions.iter().map(|partition| partition.id));
        for table_id in tbl_ids {
            let stats = store_helper::get_pd_region_stats(&client, table_id)
                .map_err(|error| plain_error(&error))?;
            let peer_count = stats.store_peer_count.values().sum::<i64>();
            rows.push(vec![
                text(&db),
                text(&tbl.name),
                Datum::Int(table_id),
                Datum::Int(peer_count),
                Datum::Int(stats.count),
                Datum::Int(stats.empty_count),
                Datum::Int(stats.storage_size),
                Datum::Int(stats.storage_keys),
            ]);
        }
    }
    Ok(rows)
}

/// Go `setDataForPseudoProfiling`: one all-zero row while the session's
/// `profiling` is on, none otherwise.
pub(crate) fn pseudo_profiling_rows(profiling: bool) -> Vec<Vec<Datum>> {
    if !profiling {
        return Vec::new();
    }
    let zero_decimal = || Datum::Decimal(tidb_datatype::Decimal::from_int(0));
    vec![vec![
        Datum::Int(0),
        Datum::Int(0),
        text(""),
        zero_decimal(),
        zero_decimal(),
        zero_decimal(),
        Datum::Int(0),
        Datum::Int(0),
        Datum::Int(0),
        Datum::Int(0),
        Datum::Int(0),
        Datum::Int(0),
        Datum::Int(0),
        Datum::Int(0),
        Datum::Int(0),
        text(""),
        text(""),
        Datum::Int(0),
    ]]
}

/// Go `setDataForKeyspaceMeta` for a store without keyspace meta: one row of
/// an empty name and ID and Go's zero `BinaryJSON` config.
pub(crate) fn keyspace_meta_rows() -> Vec<Vec<Datum>> {
    vec![vec![
        text(""),
        text(""),
        Datum::Json(tidb_datatype::BinaryJSON::zero()),
    ]]
}

/// Go `setDataFromPlacementPolicies`: every placement policy, a `FOLLOWERS`
/// of zero (unset) shown as the default two. Go walks its policy map in
/// random order; this lists the policies by name.
pub(crate) fn placement_policies_rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    let mut policies = catalog.policies().collect::<Vec<_>>();
    policies.sort_by(|left, right| left.name.lowercase().cmp(right.name.lowercase()));
    policies
        .into_iter()
        .map(|policy| {
            let settings = policy
                .placement_settings
                .as_ref()
                .map(|settings| settings.read().clone())
                .unwrap_or_default();
            let followers = if settings.followers == 0 {
                2
            } else {
                settings.followers
            };
            vec![
                Datum::Int(policy.id),
                text("def"),
                text(policy.name.original()),
                text(&settings.primary_region),
                text(&settings.regions),
                text(&settings.constraints),
                text(&settings.leader_constraints),
                text(&settings.follower_constraints),
                text(&settings.learner_constraints),
                text(&settings.schedule),
                Datum::UInt(followers),
                Datum::UInt(settings.learners),
            ]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pd_time(text: &str) -> pd_http::Time {
        serde_json::from_value(serde_json::Value::String(text.to_owned())).unwrap()
    }

    fn stores_info() -> pd_http::StoresInfo {
        let mut labeled = pd_http::StoreInfo::default();
        labeled.store.id = 1;
        labeled.store.address = "127.0.0.1:20160".to_owned();
        labeled.store.state_name = "Up".to_owned();
        labeled.store.version = "8.5.0".to_owned();
        labeled.store.labels = vec![
            pd_http::StoreLabel {
                key: "zone".to_owned(),
                value: "z2".to_owned(),
            },
            pd_http::StoreLabel {
                key: "host".to_owned(),
                value: "h1".to_owned(),
            },
        ];
        labeled.status.capacity = "100GiB".to_owned();
        labeled.status.available = "50GiB".to_owned();
        labeled.status.leader_count = 3;
        labeled.status.leader_weight = 1.0;
        labeled.status.leader_score = 3.5;
        labeled.status.region_count = 7;
        labeled.status.start_ts = pd_time("2026-10-20T10:11:12.987654321+08:00");
        labeled.status.last_heartbeat_ts = pd_time("2026-10-20T02:11:22Z");
        labeled.status.uptime = "1h0m0s".to_owned();
        let mut unlabeled = pd_http::StoreInfo::default();
        unlabeled.store.id = 4;
        unlabeled.store.labels = Vec::new();
        let mut second_zone = pd_http::StoreInfo::default();
        second_zone.store.id = 5;
        second_zone.store.labels = vec![pd_http::StoreLabel {
            key: "zone".to_owned(),
            value: "z1".to_owned(),
        }];
        pd_http::StoresInfo {
            count: 3,
            stores: vec![labeled, unlabeled, second_zone],
        }
    }

    fn cell(datum: &Datum) -> String {
        match datum {
            Datum::Null => "NULL".to_owned(),
            Datum::Int(value) => value.to_string(),
            Datum::UInt(value) => value.to_string(),
            Datum::Real(value) => value.to_string(),
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Datum::Json(json) => json.to_string(),
            Datum::Time(time) => time.to_string(),
            other => format!("{other:?}"),
        }
    }

    /// Go `dataForTiKVStoreStatus`: each store's PD fields in column order,
    /// its labels as JSON (`null` without labels) and its PD times in the
    /// wall clock PD wrote, at second precision.
    #[test]
    fn tikv_store_status_rows_follow_pd_stores() {
        let rows = tikv_store_status_rows_from(&stores_info(), false).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].len(), 19);
        let first = rows[0].iter().map(cell).collect::<Vec<_>>();
        assert_eq!(first[0], "1");
        assert_eq!(first[1], "127.0.0.1:20160");
        assert_eq!(first[3], "Up");
        assert_eq!(
            first[4],
            r#"[{"key": "zone", "value": "z2"}, {"key": "host", "value": "h1"}]"#
        );
        assert_eq!(first[6], "100GiB");
        assert_eq!(first[8], "3");
        assert_eq!(first[10], "3.5");
        assert_eq!(first[12], "7");
        assert_eq!(first[16], "2026-10-20 10:11:12");
        assert_eq!(first[17], "2026-10-20 02:11:22");
        assert_eq!(first[18], "1h0m0s");
        assert_eq!(cell(&rows[1][4]), "null");
        // Go's zero `time.Time`.
        assert_eq!(cell(&rows[1][16]), "0001-01-01 00:00:00");
    }

    /// Go's SEM patch without RESTRICTED_TABLES_ADMIN: the address,
    /// capacity, available, start time and uptime read NULL.
    #[test]
    fn tikv_store_status_rows_sem_redaction() {
        let rows = tikv_store_status_rows_from(&stores_info(), true).unwrap();
        let first = rows[0].iter().map(cell).collect::<Vec<_>>();
        for index in [1, 6, 7, 16, 18] {
            assert_eq!(first[index], "NULL", "column {index}");
        }
        assert_eq!(first[0], "1");
        assert_eq!(first[17], "2026-10-20 02:11:22");
    }

    /// Go `showPlacementLabelsResultBuilder` over the stores' `LABEL`
    /// cells: keys in order, each with its sorted distinct values.
    #[test]
    fn placement_labels_rows_from_store_labels() {
        let mut builder = ShowPlacementLabelsResultBuilder::default();
        for row in tikv_store_status_rows_from(&stores_info(), false).unwrap() {
            let Datum::Json(bj) = &row[4] else {
                panic!("LABEL is not JSON: {:?}", row[4]);
            };
            builder.append_store_labels(bj).unwrap();
        }
        let rows = builder
            .build_rows()
            .unwrap()
            .iter()
            .map(|row| row.iter().map(cell).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                vec!["host".to_owned(), r#"["h1"]"#.to_owned()],
                vec!["zone".to_owned(), r#"["z1", "z2"]"#.to_owned()],
            ]
        );
        let object = tidb_datatype::BinaryJSON::parse(r#"{"a": 1}"#).unwrap();
        assert_eq!(
            builder.append_store_labels(&object).unwrap_err(),
            "only array or null type is allowed"
        );
    }

    fn peer(id: i64, store_id: i64) -> pd_http::RegionPeer {
        pd_http::RegionPeer {
            id,
            store_id,
            is_learner: false,
        }
    }

    /// Go `packTiKVRegionPeersRows`: leader, down (with its seconds),
    /// pending and normal peers; with store IDs, only those stores' peers.
    #[test]
    fn region_peers_rows_mark_peer_status() {
        let mut learner = peer(13, 3);
        learner.is_learner = true;
        let region = pd_http::RegionInfo {
            id: 10,
            peers: vec![peer(11, 1), peer(12, 2), learner, peer(14, 4)],
            leader: peer(11, 1),
            down_peers: vec![pd_http::RegionPeerStat {
                peer: peer(12, 2),
                down_sec: 42,
            }],
            pending_peers: vec![peer(13, 3)],
            ..pd_http::RegionInfo::default()
        };
        let extractor = TikvRegionPeersExtractor::default();
        let rows =
            pack_tikv_region_peers_rows(std::slice::from_ref(&region), &extractor, &HashSet::new())
                .iter()
                .map(|row| row.iter().map(cell).collect::<Vec<_>>().join(" "))
                .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                "10 11 1 0 1 NORMAL NULL",
                "10 12 2 0 0 DOWN 42",
                "10 13 3 1 0 PENDING NULL",
                "10 14 4 0 0 NORMAL NULL",
            ]
        );
        let mut by_store = TikvRegionPeersExtractor::default();
        by_store.store_ids = vec![2];
        let rows = pack_tikv_region_peers_rows(&[region], &by_store, &HashSet::from([2_i64]));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Datum::Int(12));
    }

    /// Go `setNewTiKVRegionStatusCol`: a region without a table reads its
    /// table cells NULL and IS_INDEX / IS_PARTITION 0; an index of a
    /// partition reads both flags 1.
    #[test]
    fn tikv_region_status_row_cells() {
        let region = pd_http::RegionInfo {
            id: 8,
            start_key: "7480".to_owned(),
            end_key: String::new(),
            epoch: pd_http::RegionEpoch {
                conf_ver: 5,
                version: 9,
            },
            written_bytes: 100,
            approximate_size: 1,
            approximate_keys: 2,
            ..pd_http::RegionInfo::default()
        };
        let bare = new_tikv_region_status_row(&region, None)
            .iter()
            .map(cell)
            .collect::<Vec<_>>();
        assert_eq!(
            bare,
            [
                "8", "7480", "", "NULL", "NULL", "NULL", "0", "NULL", "NULL", "0", "NULL", "NULL",
                "5", "9", "100", "0", "1", "2", "NULL", "NULL"
            ]
        );
        let table = store_helper::TableInfo {
            db_name: "test".to_owned(),
            table_id: 100,
            table_name: "t".to_owned(),
            partition: Some(store_helper::HelperPartition {
                id: 101,
                name: "p0".to_owned(),
            }),
            index: Some(store_helper::HelperIndex {
                id: 1,
                name: "idx".to_owned(),
                global: false,
            }),
        };
        let row = new_tikv_region_status_row(&region, Some(&table))
            .iter()
            .map(cell)
            .collect::<Vec<_>>();
        assert_eq!(
            &row[3..12],
            ["100", "test", "t", "1", "1", "idx", "1", "101", "p0"]
        );
    }

    /// Go `getHotRegionRowWithSchemaInfo`: one row per table or index the
    /// region covers, the names upper-cased; a region covering no stored
    /// table reads nothing.
    #[test]
    fn hot_region_history_rows_attribute_schema() {
        let schemas = vec![store_helper::HelperSchema {
            name: "test".to_owned(),
            tables: vec![store_helper::HelperTable {
                id: 100,
                name: "t".to_owned(),
                partitions: Vec::new(),
                indices: vec![store_helper::HelperIndex {
                    id: 1,
                    name: "idx".to_owned(),
                    global: false,
                }],
                is_common_handle: false,
            }],
        }];
        let tables = store_helper::get_tables_info_with_key_range(&schemas, true);
        let hot = pd_http::HistoryHotRegion {
            update_time: 1_700_000_000_000,
            region_id: 3,
            peer_id: 4,
            store_id: 5,
            is_leader: true,
            hot_region_type: "read".to_owned(),
            hot_degree: 6,
            flow_bytes: 1.5,
            start_key: String::new(),
            end_key: String::new(),
            ..pd_http::HistoryHotRegion::default()
        };
        let rows = hot_region_row_with_schema_info(&hot, &tables)
            .iter()
            .map(|row| row[1..].iter().map(cell).collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                "TEST T 100 IDX 1 3 5 4 0 1 READ 6 1.5 0 0",
                "TEST T 100 NULL NULL 3 5 4 0 1 READ 6 1.5 0 0",
            ]
        );
        let elsewhere = pd_http::HistoryHotRegion {
            start_key: "7A".to_owned(),
            ..hot
        };
        assert!(hot_region_row_with_schema_info(&elsewhere, &tables).is_empty());
    }
}

pub(crate) fn table_storage_statistics_catalog_of(
    mut scratch: tidb_executor::Catalog,
    table_storage_stats: Option<&dyn crate::TableStorageStatsProvider>,
    resource_group: &str,
    needs_column_lengths: bool,
) -> Result<tidb_executor::Catalog, DriverError> {
    let estimates = match table_storage_stats {
        Some(provider) => {
            let physical_ids = tidb_executor::table_size_stats::physical_table_ids(&scratch);
            match provider.load_table_row_stats(
                resource_group,
                needs_column_lengths,
                &physical_ids,
            ) {
                Ok(row_stats) => {
                    tidb_executor::table_size_stats::table_storage_estimates_from_row_stats(
                        &scratch, &row_stats,
                    )
                }
                // The production provider already mirrors Go's
                // warn-and-serve-cache refresh, so it never returns Err;
                // a seam provider that does is served with zero size
                // columns for this statement rather than failing the
                // query, matching Go's reader never failing on a stats
                // refresh error. Go has no line for it: its refresh
                // warning is the provider's.
                Err(_) => {
                    scratch.clear_table_storage_statistics();
                    return Ok(scratch);
                }
            }
        }
        // The in-process store is its own statistics handle: Go's
        // `TableRowStatsCache` reads `stats_meta` and the column
        // histograms, which here are the catalog's table statistics.
        None => tidb_executor::table_size_stats::table_storage_estimates(&scratch),
    };
    for estimate in estimates {
        scratch.set_table_storage_statistics(
            estimate.table_id,
            estimate.table,
            &estimate.partitions,
        );
    }
    Ok(scratch)
}

/// Go's `memtableRetriever` for the `information_schema` tables whose rows
/// are a function of the statement's schema and the session's identity: the
/// executor builds them when the scan is first read, over the catalog the
/// statement was built with and a snapshot of what the viewer may see.
pub(crate) struct InformationSchemaRetriever {
    visibility: SchemaVisibility,
    table_storage_stats: Option<std::sync::Arc<dyn crate::TableStorageStatsProvider>>,
    resource_group: String,
}

impl InformationSchemaRetriever {
    pub(crate) fn new(
        visibility: SchemaVisibility,
        table_storage_stats: Option<std::sync::Arc<dyn crate::TableStorageStatsProvider>>,
        resource_group: String,
    ) -> Self {
        Self {
            visibility,
            table_storage_stats,
            resource_group,
        }
    }
}

impl tidb_executor::mem_table_retrieval::MemTableRetriever for InformationSchemaRetriever {
    fn serves(&self, read: &tidb_executor::mem_table_retrieval::MemTableRead) -> bool {
        read.db_name
            .go_lower_eq(tidb_executor::driver::infoschema_meta::INFORMATION_SCHEMA)
            && crate::infoschema::serves_table(&read.table_name)
    }

    fn retrieve(
        &self,
        read: &tidb_executor::mem_table_retrieval::MemTableRead,
        catalog: &Catalog,
        ctx: &tidb_executor::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        // Go `updateStatsCacheIfNeed`: the size columns of TABLES and
        // PARTITIONS are read from the table-size cache, refreshed first.
        let sized;
        let catalog = if read.needs_table_storage_statistics() {
            sized = table_storage_statistics_catalog_of(
                catalog.clone(),
                self.table_storage_stats.as_deref(),
                &self.resource_group,
                read.needs_table_storage_column_lengths(),
            )?;
            &sized
        } else {
            catalog
        };
        Ok(crate::infoschema::table_rows(&read.table_name, catalog, &self.visibility, ctx)
            .unwrap_or_default())
    }
}

/// Go's `memTableRetriever`s for the tables read from the statement's schema
/// image and the store's region map: the PD-backed region and store tables,
/// `ATTRIBUTES`, `PLACEMENT_POLICIES` and `KEYSPACE_META`. A scan builds them
/// at its first `Next` from the catalog it was built with.
pub(crate) struct CatalogTablesRetriever {
    visibility: SchemaVisibility,
    /// Whether SEM hides the cluster metadata from the viewer
    /// (`sem_hides_cluster_metadata`).
    hides_cluster_metadata: bool,
}

impl CatalogTablesRetriever {
    pub(crate) fn new(visibility: SchemaVisibility, hides_cluster_metadata: bool) -> Self {
        Self {
            visibility,
            hides_cluster_metadata,
        }
    }
}

impl tidb_executor::mem_table_retrieval::MemTableRetriever for CatalogTablesRetriever {
    fn serves(&self, read: &tidb_executor::mem_table_retrieval::MemTableRead) -> bool {
        !read.is_cluster_scan
            && read
                .db_name
                .go_lower_eq(tidb_executor::driver::infoschema_meta::INFORMATION_SCHEMA)
            && [
                "TIKV_STORE_STATUS",
                "TIKV_REGION_STATUS",
                "TIKV_REGION_PEERS",
                "TABLE_STORAGE_STATS",
                "TIDB_HOT_REGIONS",
                "ATTRIBUTES",
                "PLACEMENT_POLICIES",
                "KEYSPACE_META",
            ]
            .iter()
            .any(|name| read.table_name.go_lower_eq(name))
    }

    fn retrieve(
        &self,
        read: &tidb_executor::mem_table_retrieval::MemTableRead,
        catalog: &Catalog,
        ctx: &tidb_executor::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_planner::memtable_predicate_extractor::MemTablePredicateExtractor as Extractor;
        let table = read.table_name.as_str();
        let store = ctx.region_store();
        if table.go_lower_eq("TIKV_STORE_STATUS") {
            return tikv_store_status_rows(store.as_ref(), self.hides_cluster_metadata);
        }
        if table.go_lower_eq("ATTRIBUTES") {
            return crate::infoschema::attributes_rows(catalog, &self.visibility);
        }
        if table.go_lower_eq("PLACEMENT_POLICIES") {
            return Ok(placement_policies_rows(catalog));
        }
        if table.go_lower_eq("KEYSPACE_META") {
            return Ok(keyspace_meta_rows());
        }
        if table.go_lower_eq("TIDB_HOT_REGIONS") {
            return tidb_hot_regions_rows(catalog, store.as_ref());
        }
        if table.go_lower_eq("TABLE_STORAGE_STATS") {
            return match &read.extractor {
                Some(Extractor::TableStorageStats(extractor)) => {
                    table_storage_stats_rows(extractor, catalog, store.as_ref(), &self.visibility)
                }
                _ => Ok(Vec::new()),
            };
        }
        if table.go_lower_eq("TIKV_REGION_PEERS") {
            return match &read.extractor {
                Some(Extractor::TikvRegionPeers(extractor)) => {
                    tikv_region_peers_rows(extractor, store.as_ref())
                }
                _ => Ok(Vec::new()),
            };
        }
        let extractor = match &read.extractor {
            Some(Extractor::TiKVRegionStatus(extractor)) => Some(extractor),
            _ => None,
        };
        tikv_region_status_rows(extractor, catalog, store.as_ref(), &self.visibility)
    }
}
