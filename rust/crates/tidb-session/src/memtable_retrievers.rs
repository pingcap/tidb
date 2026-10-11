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

//! Go's memory-table retrievers that request by their scan's extractor, for
//! a store without PD: `MetricsSummaryRetriever` and
//! `MetricsSummaryByLabelRetriever` (`pkg/executor/metrics_reader.go`), the
//! PD-backed region tables (`tikvRegionPeersRetriever`,
//! `hotRegionsHistoryRetriver`, `setDataForTiKVRegionStatus`,
//! `setDataForTiDBHotRegions`) and `tableStorageStatsRetriever.initialize`.

use tidb_datatype::Datum;
use tidb_executor::{Catalog, DriverError};
use tidb_planner::logical::mem_table::QueryTimeRange;
use tidb_planner::memtable_predicate_extractor::{
    HotRegionsHistoryTableExtractor, MetricSummaryTableExtractor, TableStorageStatsExtractor,
    TiKVRegionStatusExtractor, TikvRegionPeersExtractor,
};

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

fn plain_error(message: &str) -> DriverError {
    DriverError::DdlCoded {
        errno: 1105,
        message: message.to_owned(),
    }
}

/// client-go `helper.TryGetPDHTTPClient`'s failure without PD.
const PD_HTTP_CLIENT_UNAVAILABLE: &str = "pd http client unavailable";

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

/// Go `tikvRegionPeersRetriever.retrieve`: a scan that requests anything
/// needs PD's HTTP client.
pub(crate) fn tikv_region_peers_rows(
    extractor: &TikvRegionPeersExtractor,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    if extractor.skip_request {
        return Ok(Vec::new());
    }
    Err(plain_error(PD_HTTP_CLIENT_UNAVAILABLE))
}

/// Go `setDataForTiKVRegionStatus`: the regions of the named tables (a table
/// ID that names no table is skipped) or of every table, from PD.
pub(crate) fn tikv_region_status_rows(
    extractor: Option<&TiKVRegionStatusExtractor>,
    catalog: &Catalog,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let table_ids = extractor.map_or(&[][..], |extractor| extractor.tables_id.as_slice());
    if !table_ids.is_empty()
        && table_ids
            .iter()
            .all(|table_id| catalog.table_names_by_id(*table_id).is_none())
    {
        return Ok(Vec::new());
    }
    Err(plain_error(PD_HTTP_CLIENT_UNAVAILABLE))
}

/// Go `setDataForTiDBHotRegions`: the hot regions PD reports.
pub(crate) fn tidb_hot_regions_rows() -> Result<Vec<Vec<Datum>>, DriverError> {
    Err(plain_error(PD_HTTP_CLIENT_UNAVAILABLE))
}

/// Go `hotRegionsHistoryRetriver.retrieve` after the PROCESS check: the time
/// range must be bounded on both sides, and this store has no PD server to
/// ask (Go's `GetPDServerInfo` lists none), so a bounded scan reads nothing.
pub(crate) fn tidb_hot_regions_history_rows(
    extractor: &HotRegionsHistoryTableExtractor,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    if extractor.skip_request {
        return Ok(Vec::new());
    }
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
    Ok(Vec::new())
}

/// Go `tableStorageStatsRetriever.initialize`: a scan that names no
/// `TABLE_SCHEMA` is refused, and any other needs PD's region statistics,
/// which a store without PD cannot answer (Go's `helper.GetPDAddr` fails with
/// `pd unavailable`). A scan whose predicates contradict each other
/// (`SkipRequest`) reads nothing.
pub(crate) fn table_storage_stats_rows(
    extractor: &TableStorageStatsExtractor,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    if extractor.skip_request {
        return Ok(Vec::new());
    }
    if extractor.table_schema.is_empty() {
        return Err(plain_error(
            "Please add where clause to filter the column TABLE_SCHEMA. \
             For example, where TABLE_SCHEMA = 'xxx' or where TABLE_SCHEMA in ('xxx', 'yyy')",
        ));
    }
    Err(plain_error("pd unavailable"))
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
