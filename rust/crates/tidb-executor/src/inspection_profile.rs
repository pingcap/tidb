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

//! Go `pkg/executor/inspection_profile.go`: the metric profile `/metrics/profile`
//! serves, a graphviz `dot` graph of where the query time went, read from
//! `metrics_schema`.
//!
//! Go's metric tree shares the `tidb_kv_request` node between two parents
//! through one pointer; the nodes live in an arena here, so the shared node
//! is one index with one memoized value. Go ranges over a node's label
//! values in map order; they are visited in label order here.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;

use chrono::{DateTime, FixedOffset};
use tidb_datatype::Datum;

/// Go `dateTimeFormat` (`time.DateTime`).
const DATE_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// Go `metricValueType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricValueType {
    /// Go `metricValueSum`.
    Sum,
    /// Go `metricValueAvg`.
    Avg,
    /// Go `metricValueCnt`.
    Cnt,
}

impl MetricValueType {
    /// Go `metricValueType.String`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Cnt => "count",
            Self::Sum => "sum",
        }
    }
}

/// Go `metricValue`.
#[derive(Clone, Debug, Default)]
struct MetricValue {
    sum: f64,
    count: i64,
    avg_p99: f64,
    avg_p90: f64,
    avg_p80: f64,
    comment: String,
}

/// Go `time.Duration(int64(seconds * float64(time.Second))).String()`.
fn seconds_string(seconds: f64) -> String {
    tidb_model::go_duration::format_go_duration((seconds * 1e9) as i64)
}

impl MetricValue {
    /// Go `metricValue.getValue`.
    fn get_value(&self, tp: MetricValueType) -> f64 {
        let time_value = match tp {
            MetricValueType::Cnt => return self.count as f64,
            MetricValueType::Sum => self.sum,
            MetricValueType::Avg => {
                if self.count == 0 {
                    return 0.0;
                }
                self.sum / self.count as f64
            }
        };
        if time_value.is_nan() {
            return 0.0;
        }
        time_value
    }

    /// Go `metricValue.getComment`.
    fn get_comment(&self) -> String {
        if self.count == 0 {
            return String::new();
        }
        format!(
            "{}\n\ntotal_time: {}\ntotal_count: {}\navg_time: {}\navgP99: {}\navgP90: {}\navgP80: {}",
            self.comment,
            seconds_string(self.sum),
            self.count,
            seconds_string(self.sum / self.count as f64),
            seconds_string(self.avg_p99),
            seconds_string(self.avg_p90),
            seconds_string(self.avg_p80),
        )
    }
}

/// Go `metricNode`.
#[derive(Default)]
struct MetricNode {
    table: &'static str,
    name: &'static str,
    label: &'static [&'static str],
    condition: &'static str,
    label_value: BTreeMap<String, MetricValue>,
    value: MetricValue,
    unit: i64,
    children: Vec<usize>,
    /// Go `isPartOfParent` indicates the parent of this node not fully
    /// contain this node.
    is_part_of_parent: bool,
    initialized: bool,
}

impl MetricNode {
    /// Go `metricNode.getName`.
    fn get_name(&self, label: &str) -> String {
        let name = if self.name.is_empty() { self.table } else { self.name };
        if label.is_empty() {
            name.to_owned()
        } else {
            format!("{name}.{label}")
        }
    }
}

/// The time of a profile bound: Go `time.Time` in its location.
#[derive(Clone, Debug)]
pub struct ProfileTime {
    /// The instant in its zone's offset.
    pub time: DateTime<FixedOffset>,
    /// The zone's abbreviation (Go `MST` in a layout); `None` prints the
    /// offset, as a Go fixed zone without a name does.
    pub zone_abbreviation: Option<String>,
}

impl ProfileTime {
    /// Go `Time.String`: `2006-01-02 15:04:05.999999999 -0700 MST`.
    fn go_string(&self) -> String {
        let mut text = self.time.format("%Y-%m-%d %H:%M:%S").to_string();
        let nanos = self.time.timestamp_subsec_nanos();
        if nanos != 0 {
            let fraction = format!("{nanos:09}");
            text.push('.');
            text.push_str(fraction.trim_end_matches('0'));
        }
        let offset = self.time.format("%z").to_string();
        let zone = self.zone_abbreviation.clone().unwrap_or_else(|| offset.clone());
        format!("{text} {offset} {zone}")
    }
}

/// Go `profileBuilder`.
pub struct ProfileBuilder<'a> {
    /// Go `sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL`.
    exec: &'a dyn Fn(&str) -> Result<Vec<Vec<Datum>>, String>,
    id_map: HashMap<String, u64>,
    id_allocator: u64,
    total_value: f64,
    unique_map: HashSet<String>,
    buf: String,
    start: ProfileTime,
    end: ProfileTime,
    value_tp: MetricValueType,
    nodes: Vec<MetricNode>,
}

impl<'a> ProfileBuilder<'a> {
    /// Go `NewProfileBuilder` returns a new profileBuilder.
    ///
    /// # Errors
    /// Go's unknown-type error.
    pub fn new(
        exec: &'a dyn Fn(&str) -> Result<Vec<Vec<Datum>>, String>,
        start: ProfileTime,
        end: ProfileTime,
        tp: &str,
    ) -> Result<Self, String> {
        let value_tp = match tp.to_lowercase().as_str() {
            "sum" => MetricValueType::Sum,
            "avg" => MetricValueType::Avg,
            "count" => MetricValueType::Cnt,
            // Use type sum when doesn't specified the type, this is used to
            // compatible with old behaviour.
            "" => MetricValueType::Sum,
            _ => {
                return Err(format!(
                    "unknown metric profile type: {tp}, expect value should be one of 'sum', 'avg' or 'count'"
                ))
            }
        };
        Ok(Self {
            exec,
            id_map: HashMap::new(),
            id_allocator: 1,
            total_value: 0.0,
            unique_map: HashSet::new(),
            buf: String::with_capacity(1024),
            start,
            end,
            value_tp,
            nodes: Vec::new(),
        })
    }

    /// Go `Collect` uses to collect the related metric information.
    ///
    /// # Errors
    /// A metric query that failed.
    pub fn collect(&mut self) -> Result<(), String> {
        let tidb_query = self.gen_tidb_query_tree();
        self.init(tidb_query, "tidb_query")?;
        self.traversal(tidb_query)?;
        let gc = self.gen_tidb_gc_tree();
        self.traversal(gc)
    }

    /// Go `Build` returns the metric profile dot.
    #[must_use]
    pub fn build(mut self) -> Vec<u8> {
        self.buf.push('}');
        self.buf.into_bytes()
    }

    fn node(&mut self, node: MetricNode) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    /// Go `getNameID`.
    fn get_name_id(&mut self, name: &str) -> u64 {
        if let Some(id) = self.id_map.get(name) {
            return *id;
        }
        let id = self.id_allocator;
        self.id_allocator += 1;
        self.id_map.insert(name.to_owned(), id);
        id
    }

    /// Go `init`.
    fn init(&mut self, total: usize, name: &str) -> Result<(), String> {
        let tp = match self.value_tp {
            MetricValueType::Avg => "avg_time",
            MetricValueType::Cnt => "total_count",
            MetricValueType::Sum => "total_time",
        };
        let duration = (self.end.time - self.start.time).num_nanoseconds().unwrap_or(i64::MAX);
        let _ = writeln!(self.buf, r#"digraph "tidb_profile" {{"#);
        let _ = writeln!(self.buf, r##"node [style=filled fillcolor="#f8f8f8"]"##);
        let _ = writeln!(
            self.buf,
            r#"subgraph {0} {{ "{0}" [shape=box fontsize=16 label="Type: {0}\lTime: {1}\lDuration: {2}\l"] }}"#,
            format!("{name}_{tp}"),
            self.start.go_string(),
            tidb_model::go_duration::format_go_duration(duration),
        );
        let v = self.get_total_value(total)?;
        self.total_value = if v != 0.0 { v } else { 1.0 };
        Ok(())
    }

    /// Go `GetTotalValue`.
    fn get_total_value(&mut self, root: usize) -> Result<f64, String> {
        match self.value_tp {
            MetricValueType::Sum | MetricValueType::Cnt => {
                Ok(self.get_value(root)?.get_value(self.value_tp))
            }
            MetricValueType::Avg => self.get_max_node_value(root),
        }
    }

    /// Go `GetMaxNodeValue`.
    fn get_max_node_value(&mut self, n: usize) -> Result<f64, String> {
        let value = self.get_value(n)?;
        let tp = self.value_tp;
        let label_max = |nodes: &[MetricNode], maxv: f64| {
            nodes[n]
                .label_value
                .values()
                .map(|v| v.get_value(tp))
                .fold(maxv, |maxv, v| if v > maxv { v } else { maxv })
        };
        let mut maxv = label_max(&self.nodes, value.get_value(tp));
        for child in self.nodes[n].children.clone() {
            let child_max = self.get_max_node_value(child)?;
            if child_max > maxv {
                maxv = child_max;
            }
            maxv = label_max(&self.nodes, maxv);
        }
        Ok(maxv)
    }

    /// Go `metricNode.getValue`.
    fn get_value(&mut self, n: usize) -> Result<MetricValue, String> {
        if !self.nodes[n].initialized {
            self.nodes[n].initialized = true;
            self.initialize_metric_value(n)?;
        }
        Ok(self.nodes[n].value.clone())
    }

    /// Go `metricNode.queryRowsByLabel`.
    fn query_rows_by_label(
        &mut self,
        n: usize,
        query: &str,
        mut handle_row: impl FnMut(&mut MetricNode, String, f64),
    ) -> Result<(), String> {
        let rows = (self.exec)(query)?;
        if rows.first().is_none_or(Vec::is_empty) {
            return Ok(());
        }
        let node = &mut self.nodes[n];
        for row in rows {
            // Go `row.GetFloat64(0)`: a NULL reads as zero.
            let mut v = row[0].to_f64().map_or(0.0, |converted| converted.value);
            if node.unit != 0 {
                v /= node.unit as f64;
            }
            let label = row[1..]
                .iter()
                .map(|datum| datum.sql_string().unwrap_or_default())
                .collect::<Vec<_>>()
                .join(",");
            if label.is_empty() && !node.label.is_empty() {
                continue;
            }
            handle_row(node, label, v);
        }
        Ok(())
    }

    /// Go `metricNode.initializeMetricValue`.
    fn initialize_metric_value(&mut self, n: usize) -> Result<(), String> {
        let node = &mut self.nodes[n];
        node.label_value = BTreeMap::new();
        node.value = MetricValue::default();
        let mut query_condition = format!(
            "where time >= '{}' and time <= '{}' and value is not null and value>0",
            self.start.time.format(DATE_TIME_FORMAT),
            self.end.time.format(DATE_TIME_FORMAT)
        );
        if !node.condition.is_empty() {
            query_condition.push_str(" and ");
            query_condition.push_str(node.condition);
        }
        let (table, labels) = (node.table, node.label.join("`,`"));
        let grouped = !node.label.is_empty();
        let query_for = |suffix: &str, aggregate: &str, condition: &str| {
            if grouped {
                format!(
                    "select {aggregate}(value), `{labels}` from `metrics_schema`.`{table}_{suffix}` {condition} group by `{labels}` having sum(value) > 0"
                )
            } else {
                format!("select {aggregate}(value), '' from `metrics_schema`.`{table}_{suffix}` {condition}")
            }
        };

        // 1. Get total count value.
        let mut total_count = 0.0;
        self.query_rows_by_label(n, &query_for("total_count", "sum", &query_condition), |node, label, v| {
            total_count += v;
            node.label_value.entry(label).or_default().count = v as i64;
        })?;
        // Return early if the metric doesn't has value
        if total_count as i64 == 0 {
            return Ok(());
        }
        self.nodes[n].value.count = total_count as i64;

        // 2. Get total sum time.
        let mut total_sum = 0.0;
        self.query_rows_by_label(n, &query_for("total_time", "sum", &query_condition), |node, label, mut v| {
            if node.unit != 0 {
                v /= node.unit as f64;
            }
            total_sum += v;
            node.label_value.entry(label).or_default().sum = v;
        })?;
        self.nodes[n].value.sum = total_sum;

        // 3. Get quantile value.
        let set_quantile_value = |value: &mut MetricValue, quantile: f64, v: f64| {
            if quantile == 0.99 {
                value.avg_p99 = v;
            } else if quantile == 0.90 {
                value.avg_p90 = v;
            } else if quantile == 0.80 {
                value.avg_p80 = v;
            }
        };
        for (quantile, text) in [(0.99, "0.99"), (0.90, "0.9"), (0.80, "0.8")] {
            let condition = format!("{query_condition} and quantile={text}");
            let mut total_value = 0.0;
            let mut cnt = 0;
            self.query_rows_by_label(n, &query_for("duration", "avg", &condition), |node, label, mut v| {
                if node.unit != 0 {
                    v /= node.unit as f64;
                }
                total_value += v;
                cnt += 1;
                set_quantile_value(node.label_value.entry(label).or_default(), quantile, v);
            })?;
            set_quantile_value(&mut self.nodes[n].value, quantile, total_value / f64::from(cnt));
        }

        // 4. Add metric comment.
        let node = &mut self.nodes[n];
        if let Some(def) = tidb_infoschema::metric_table_def::get_metric_table_def(&format!("{}_total_time", node.table)) {
            node.value.comment = def.comment.to_owned();
            let labels = node.label.join(",");
            for (label, value) in &mut node.label_value {
                value.comment = format!("{}, the label of [{labels}] is [{label}]", def.comment);
            }
        }
        Ok(())
    }

    /// Go `traversal`.
    fn traversal(&mut self, n: usize) -> Result<(), String> {
        let node_name = self.nodes[n].get_name("");
        if !self.unique_map.insert(node_name) {
            return Ok(());
        }
        let node_value = self.get_value(n)?;
        if self.ignore_fraction(&node_value, self.total_value) {
            return Ok(());
        }
        let mut total_children_value = 0.0;
        for child in self.nodes[n].children.clone() {
            let child_value = self.get_value(child)?;
            self.add_node_edge(n, child, &child_value);
            if !self.nodes[child].is_part_of_parent {
                total_children_value += child_value.get_value(self.value_tp);
            }
        }
        let self_value = node_value.get_value(self.value_tp);
        let self_cost = self_value - total_children_value;
        self.add_node(n, self_cost, self_value);
        for child in self.nodes[n].children.clone() {
            self.traversal(child)?;
        }
        Ok(())
    }

    /// Go `addNodeEdge`.
    fn add_node_edge(&mut self, parent: usize, child: usize, child_value: &MetricValue) {
        if self.ignore_fraction(child_value, self.total_value) {
            return;
        }
        let style = if self.nodes[child].is_part_of_parent { "dotted" } else { "" };
        let value = child_value.get_value(self.value_tp);
        let child_name = self.nodes[child].get_name("");
        if self.nodes[parent].label.is_empty() {
            let label = if self.nodes[child].is_part_of_parent {
                String::new()
            } else {
                self.format_value_by_tp(value)
            };
            let parent_name = self.nodes[parent].get_name("");
            self.add_edge(&parent_name, &child_name, &label, style, value);
        } else {
            let labels: Vec<(String, MetricValue)> = self.nodes[parent]
                .label_value
                .iter()
                .map(|(label, value)| (label.clone(), value.clone()))
                .collect();
            for (label, label_value) in labels {
                if self.ignore_fraction(&label_value, self.total_value) {
                    continue;
                }
                let parent_name = self.nodes[parent].get_name(&label);
                self.add_edge(&parent_name, &child_name, "", style, value);
            }
        }
    }

    /// Go `addNode`.
    fn add_node(&mut self, n: usize, mut self_cost: f64, node_total: f64) {
        let name = self.nodes[n].get_name("");
        let mut weight = self_cost;
        if !self.nodes[n].label.is_empty() {
            let labels: Vec<(String, MetricValue)> = self.nodes[n]
                .label_value
                .iter()
                .map(|(label, value)| (label.clone(), value.clone()))
                .collect();
            for (label, value) in labels {
                if self.ignore_fraction(&value, self.total_value) {
                    continue;
                }
                let v = value.get_value(self.value_tp);
                let v_str = self.format_value_by_tp(v);
                let label_name = self.nodes[n].get_name(&label);
                self.add_edge(&name, &label_name, &format!(" {v_str}"), "", v);
                let label_value = format!("{label_name}\n {v_str} ({:.2}%)", v * 100.0 / self.total_value);
                self.add_node_def(&label_name, &label_value, &value.get_comment(), v, v);
            }
            weight = self_cost / 2.0;
            // Since this node has labels, all cost was consume on the
            // children, so the selfCost is 0.
            self_cost = 0.0;
        }
        let label = format!(
            "{name}\n {} ({:.2}%)\nof {} ({:.2}%)",
            self.format_value_by_tp(self_cost),
            self_cost * 100.0 / self.total_value,
            self.format_value_by_tp(node_total),
            node_total * 100.0 / self.total_value
        );
        let comment = self.nodes[n].value.get_comment();
        self.add_node_def(&name, &label, &comment, weight, self_cost);
    }

    /// Go `addNodeDef`.
    fn add_node_def(&mut self, name: &str, label_value: &str, comment: &str, font_weight: f64, color_weight: f64) {
        let (base_font_size, max_font_size, max_font_growth) = (5_i64, 64_i64, 18.0_f64);
        let font_size = (base_font_size
            + (max_font_growth * (font_weight.abs() / self.total_value).sqrt()).ceil() as i64)
            .min(max_font_size);
        let id = self.get_name_id(name);
        let _ = writeln!(
            self.buf,
            r#"N{id} [label="{label_value}" tooltip="{comment}" fontsize={font_size} shape=box color="{}" fillcolor="{}"]"#,
            dot_color(color_weight / self.total_value, false),
            dot_color(color_weight / self.total_value, true),
        );
    }

    /// Go `addEdge`.
    fn add_edge(&mut self, from: &str, to: &str, label: &str, style: &str, value: f64) {
        let weight = 1 + (value * 100.0 / self.total_value).min(100.0) as i64;
        let color = dot_color(value / self.total_value, false);
        let (from, to) = (self.get_name_id(from), self.get_name_id(to));
        let _ = write!(self.buf, "N{from} -> N{to} [");
        if !label.is_empty() {
            let _ = write!(self.buf, r#" label="{label}" "#);
        }
        if !style.is_empty() {
            let _ = write!(self.buf, r#" style="{style}" "#);
        }
        let _ = writeln!(self.buf, r#" weight={weight} color="{color}"]"#);
    }

    /// Go `ignoreFraction`.
    fn ignore_fraction(&self, v: &MetricValue, total: f64) -> bool {
        v.get_value(self.value_tp) * 100.0 / total < 0.01
    }

    /// Go `formatValueByTp`.
    fn format_value_by_tp(&self, value: f64) -> String {
        match self.value_tp {
            MetricValueType::Cnt => (value as i64).to_string(),
            MetricValueType::Sum | MetricValueType::Avg => {
                if value.is_nan() {
                    String::new()
                } else if value.abs() > 1.0 {
                    // second unit
                    format!("{value:.2}s")
                } else if (value * 1000.0).abs() > 1.0 {
                    // millisecond unit
                    format!("{:.2} ms", value * 1000.0)
                } else if (value * 1000.0 * 1000.0).abs() > 1.0 {
                    // microsecond unit (Go labels it ms)
                    format!("{:.2} ms", value * 1000.0 * 1000.0)
                } else {
                    seconds_string(value)
                }
            }
        }
    }

    /// Go `genTiDBGCTree`.
    fn gen_tidb_gc_tree(&mut self) -> usize {
        let kv_request = self.node(MetricNode { table: "tidb_kv_request", is_part_of_parent: true, ..MetricNode::default() });
        self.node(MetricNode {
            table: "tidb_gc",
            is_part_of_parent: true,
            label: &["stage"],
            children: vec![kv_request],
            ..MetricNode::default()
        })
    }

    /// Go `genTiDBQueryTree`.
    fn gen_tidb_query_tree(&mut self) -> usize {
        let leaf = |table: &'static str| MetricNode { table, ..MetricNode::default() };
        let batch_client_wait = self.node(MetricNode { table: "tidb_batch_client_wait", unit: 1_000_000_000, ..MetricNode::default() });
        let batch_client_wait_conn = self.node(leaf("tidb_batch_client_wait_conn"));
        let batch_client_unavailable = self.node(leaf("tidb_batch_client_unavailable"));
        let pd_client_cmd = self.node(MetricNode {
            table: "pd_client_cmd",
            condition: "type not in ('tso','wait','tso_async_wait')",
            ..MetricNode::default()
        });
        let cop_wait = self.node(MetricNode {
            table: "tikv_cop_wait",
            label: &["type"],
            condition: "type != 'all'",
            ..MetricNode::default()
        });
        let cop_handle = self.node(leaf("tikv_cop_handle"));
        let cop_request = self.node(MetricNode { table: "tikv_cop_request", children: vec![cop_wait, cop_handle], ..MetricNode::default() });
        let latch_wait = self.node(leaf("tikv_scheduler_latch_wait"));
        let processing_read = self.node(leaf("tikv_scheduler_processing_read"));
        let snapshot = self.node(MetricNode {
            table: "tikv_storage_async_request",
            name: "tikv_storage_async_request.snapshot",
            condition: "type='snapshot'",
            ..MetricNode::default()
        });
        let propose_wait = self.node(leaf("tikv_raftstore_propose_wait"));
        let append_log = self.node(leaf("tikv_raftstore_append_log"));
        let raft_process = self.node(MetricNode { table: "tikv_raftstore_process", children: vec![append_log], ..MetricNode::default() });
        let commit_log = self.node(leaf("tikv_raftstore_commit_log"));
        let apply_wait = self.node(leaf("tikv_raftstore_apply_wait"));
        let apply_log = self.node(leaf("tikv_raftstore_apply_log"));
        let write = self.node(MetricNode {
            table: "tikv_storage_async_request",
            name: "tikv_storage_async_request.write",
            condition: "type='write'",
            children: vec![propose_wait, raft_process, commit_log, apply_wait, apply_log],
            ..MetricNode::default()
        });
        let async_request = self.node(MetricNode { table: "tikv_storage_async_request", children: vec![snapshot, write], ..MetricNode::default() });
        let scheduler_command = self.node(MetricNode {
            table: "tikv_scheduler_command",
            children: vec![latch_wait, processing_read, async_request],
            ..MetricNode::default()
        });
        let gc_tasks = self.node(MetricNode { table: "tikv_gc_tasks", label: &["task"], ..MetricNode::default() });
        let grpc_message = self.node(MetricNode {
            table: "tikv_grpc_message",
            children: vec![cop_request, scheduler_command, gc_tasks],
            ..MetricNode::default()
        });
        let tidb_kv_request = self.node(MetricNode {
            table: "tidb_kv_request",
            is_part_of_parent: true,
            label: &["type"],
            children: vec![batch_client_wait, batch_client_wait_conn, batch_client_unavailable, pd_client_cmd, grpc_message],
            ..MetricNode::default()
        });
        let ddl_children = [
            "tidb_ddl_batch_add_index",
            "tidb_load_schema",
            "tidb_ddl_update_self_version",
            "tidb_owner_handle_syncer",
            "tidb_meta_operation",
        ]
        .map(|table| self.node(leaf(table)))
        .to_vec();
        let ddl_worker = self.node(MetricNode { table: "tidb_ddl_worker", label: &["type"], children: ddl_children, ..MetricNode::default() });
        let ddl_time = self.node(MetricNode { table: "tidb_ddl", label: &["type"], children: vec![ddl_worker], ..MetricNode::default() });
        let start_tso_wait = self.node(leaf("pd_start_tso_wait"));
        let auto_id_request = self.node(leaf("tidb_auto_id_request"));
        let cop_backoff = self.node(MetricNode { table: "tidb_kv_backoff", label: &["type"], is_part_of_parent: true, ..MetricNode::default() });
        let cop = self.node(MetricNode {
            table: "tidb_cop",
            is_part_of_parent: true,
            children: vec![cop_backoff, tidb_kv_request],
            ..MetricNode::default()
        });
        let txn_backoff = self.node(MetricNode { table: "tidb_kv_backoff", label: &["type"], is_part_of_parent: true, ..MetricNode::default() });
        let txn_cmd = self.node(MetricNode {
            table: "tidb_txn_cmd",
            label: &["type"],
            children: vec![txn_backoff, tidb_kv_request],
            ..MetricNode::default()
        });
        let tidb_execute = self.node(MetricNode {
            table: "tidb_execute",
            children: vec![start_tso_wait, auto_id_request, cop, txn_cmd, ddl_time],
            ..MetricNode::default()
        });
        let get_token = self.node(MetricNode { table: "tidb_get_token", unit: 1_000_000, ..MetricNode::default() });
        let parse = self.node(leaf("tidb_parse"));
        let compile = self.node(leaf("tidb_compile"));
        self.node(MetricNode {
            table: "tidb_query",
            label: &["sql_type"],
            children: vec![get_token, parse, compile, tidb_execute],
            ..MetricNode::default()
        })
    }
}

/// Go `dotColor` (copied by Go from github.com/google/pprof).
fn dot_color(score: f64, is_background: bool) -> String {
    // A float between 0.0 and 1.0, indicating the extent to which colors
    // should be shifted away from grey.
    const SHIFT: f64 = 0.7;
    // Saturation and value (in hsv colorspace) for background colors.
    const BG_SATURATION: f64 = 0.1;
    const BG_VALUE: f64 = 0.93;
    // Saturation and value (in hsv colorspace) for foreground colors.
    const FG_SATURATION: f64 = 1.0;
    const FG_VALUE: f64 = 0.7;
    let (mut saturation, value) = if is_background {
        (BG_SATURATION, BG_VALUE)
    } else {
        (FG_SATURATION, FG_VALUE)
    };
    // Limit the score values to the range [-1.0, 1.0].
    let mut score = score.clamp(-1.0, 1.0);
    // Reduce saturation near score=0 (so it is colored grey, rather than
    // yellow).
    if score.abs() < 0.2 {
        saturation *= score.abs() / 0.2;
    }
    // Apply 'shift' to move scores away from 0.0 (grey).
    if score > 0.0 {
        score = score.powf(1.0 - SHIFT);
    }
    if score < 0.0 {
        score = -(-score).powf(1.0 - SHIFT);
    }
    let (r, g) = if score < 0.0 {
        (value * (1.0 + saturation * score), value)
    } else {
        (value, value * (1.0 - saturation * score))
    };
    let b = value * (1.0 - saturation);
    // Go's `uint8(f)` truncates toward zero.
    format!("#{:02x}{:02x}{:02x}", (r * 255.0) as u8, (g * 255.0) as u8, (b * 255.0) as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> ProfileTime {
        ProfileTime {
            time: DateTime::parse_from_rfc3339(text).unwrap(),
            zone_abbreviation: Some("UTC".to_owned()),
        }
    }

    /// Go `dotColor` on pprof's scale: grey near zero, saturated at the ends.
    #[test]
    fn dot_color_matches_pprof() {
        assert_eq!(dot_color(0.0, false), "#b2b2b2");
        assert_eq!(dot_color(1.0, false), "#b20000");
        assert_eq!(dot_color(1.0, true), "#edd5d5");
        assert_eq!(dot_color(-1.0, false), "#00b200");
    }

    /// Go `NewProfileBuilder`'s type check and an empty `metrics_schema`:
    /// the graph is the header and the closing brace, and every query asks
    /// the window Go formats.
    #[test]
    fn a_profile_with_no_metric_value_is_its_header() {
        let queries = std::cell::RefCell::new(Vec::new());
        let exec = |sql: &str| -> Result<Vec<Vec<Datum>>, String> {
            queries.borrow_mut().push(sql.to_owned());
            Ok(Vec::new())
        };
        assert_eq!(
            ProfileBuilder::new(&exec, at("2026-11-05T00:00:00Z"), at("2026-11-05T00:10:00Z"), "max").err(),
            Some("unknown metric profile type: max, expect value should be one of 'sum', 'avg' or 'count'".to_owned())
        );
        let mut pb = ProfileBuilder::new(&exec, at("2026-11-05T00:00:00Z"), at("2026-11-05T00:10:00Z"), "").unwrap();
        pb.collect().unwrap();
        let dot = String::from_utf8(pb.build()).unwrap();
        assert!(dot.starts_with("digraph \"tidb_profile\" {\n"), "{dot}");
        assert!(dot.contains(r#"subgraph tidb_query_total_time { "tidb_query_total_time" [shape=box fontsize=16 label="Type: tidb_query_total_time\lTime: 2026-11-05 00:00:00 +0000 UTC\lDuration: 10m0s\l"] }"#), "{dot}");
        assert!(dot.ends_with('}'));
        assert_eq!(
            queries.borrow()[0],
            "select sum(value), `sql_type` from `metrics_schema`.`tidb_query_total_count` where time >= '2026-11-05 00:00:00' and time <= '2026-11-05 00:10:00' and value is not null and value>0 group by `sql_type` having sum(value) > 0"
        );
    }

    /// Go's traversal: a node's value, its labels and its children become
    /// `dot` nodes and edges, the self cost being the value its non-partial
    /// children do not cover.
    #[test]
    fn a_profile_draws_the_query_tree() {
        let exec = |sql: &str| -> Result<Vec<Vec<Datum>>, String> {
            let row = |v: f64, label: &str| vec![Datum::Real(v), Datum::new_string_datum(label)];
            Ok(if sql.contains("`tidb_query_total_count`") {
                vec![row(10.0, "Select")]
            } else if sql.contains("`tidb_query_total_time`") {
                vec![row(4.0, "Select")]
            } else if sql.contains("`tidb_parse_total_count`") {
                vec![row(10.0, "")]
            } else if sql.contains("`tidb_parse_total_time`") {
                vec![row(1.0, "")]
            } else {
                Vec::new()
            })
        };
        let mut pb = ProfileBuilder::new(&exec, at("2026-11-05T00:00:00Z"), at("2026-11-05T00:10:00Z"), "sum").unwrap();
        pb.collect().unwrap();
        let dot = String::from_utf8(pb.build()).unwrap();
        // The label node tidb_query.Select (N1) -> tidb_parse (N2), then the
        // node tidb_query (N3) -> its label.
        assert!(dot.contains(r##"N1 -> N2 [ weight=26 color="#b23c00"]"##), "{dot}");
        assert!(dot.contains(r##"N3 -> N1 [ label=" 4.00s"  weight=101 color="#b20000"]"##), "{dot}");
        assert!(dot.contains("tidb_query.Select\n 4.00s (100.00%)"), "{dot}");
        // Go formats one second as `1000.00 ms` (its seconds branch is `> 1`).
        assert!(dot.contains("tidb_parse\n 1000.00 ms (25.00%)\nof 1000.00 ms (25.00%)"), "{dot}");
    }
}
