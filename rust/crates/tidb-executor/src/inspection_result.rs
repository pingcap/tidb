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

//! The `INSPECTION_RESULT` memory table: Go `inspectionResultRetriever`
//! (`pkg/executor/inspection_result.go`).

use std::collections::{BTreeMap, HashMap};

use tidb_datatype::Datum;
use tidb_planner::memtable_predicate_extractor::{InspectionResultTableExtractor, StringSet};
use tidb_planner_core_operator_logicalop::mem_table::QueryTimeRange;
use tidb_sqlescape::SqlArg;

use crate::memtable_reader::{InspectionTableCache, TableSnapshot};
use crate::DriverError;

/// What Go's `mockMergeMockInspectionTables` failpoint reads from the
/// statement's context.
#[cfg(any(test, feature = "test-util"))]
pub mod mock {
    use super::{HashMap, TableSnapshot};
    use std::sync::{Mutex, PoisonError};

    static INSPECTION_TABLES: Mutex<Option<HashMap<String, TableSnapshot>>> = Mutex::new(None);

    /// Go `context.WithValue(ctx, "__mockInspectionTables", tables)` with the
    /// `mockMergeMockInspectionTables` failpoint on.
    pub fn set_inspection_tables(tables: Option<HashMap<String, TableSnapshot>>) {
        *INSPECTION_TABLES.lock().unwrap_or_else(PoisonError::into_inner) = tables;
    }

    pub(super) fn inspection_tables() -> Option<HashMap<String, TableSnapshot>> {
        INSPECTION_TABLES.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

/// Go `inspectionResult`.
#[derive(Default)]
struct InspectionResult {
    tp: String,
    instance: String,
    status_address: String,
    // represents the diagnostics item, e.g: `ddl.lease` `raftstore.cpuusage`
    item: String,
    // diagnosis result value base on current cluster status
    actual: String,
    expected: String,
    severity: String,
    detail: String,
    // degree only used for sort.
    degree: f64,
}

/// Go `inspectionFilter`.
struct InspectionFilter<'a> {
    set: &'a StringSet,
    time_range: &'a QueryTimeRange,
}

impl InspectionFilter<'_> {
    /// Go `inspectionFilter.enable`.
    fn enable(&self, name: &str) -> bool {
        self.set.is_empty() || self.set.contains(name)
    }
}

/// Go `QueryTimeRange.Condition`.
fn condition(time_range: &QueryTimeRange) -> String {
    format!("where time>='{}' and time<='{}'", time_range.from, time_range.to)
}

/// Go `Row.GetString`.
fn get_string(row: &[Datum], index: usize) -> String {
    match row.get(index) {
        Some(Datum::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        Some(Datum::String(text)) => String::from_utf8_lossy(text.bytes()).into_owned(),
        _ => String::new(),
    }
}

/// Go `Row.GetFloat64`: the cell's first eight bytes as a float64, whatever
/// the column's type. A NULL cell's bytes are zero; a DECIMAL cell's are the
/// head of its `MyDecimal` struct, which is how Go reads the literal
/// threshold of a rule without a configuration key.
fn get_float64(row: &[Datum], index: usize) -> f64 {
    match row.get(index) {
        Some(Datum::Real(value)) => *value,
        Some(Datum::Decimal(value)) => {
            let bytes = value.to_chunk_my_decimal_lossy().to_raw_bytes();
            f64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        }
        _ => 0.0,
    }
}

/// Go `Row.GetUint64`.
fn get_uint64(row: &[Datum], index: usize) -> u64 {
    match row.get(index) {
        Some(Datum::Int(value)) => *value as u64,
        Some(Datum::UInt(value)) => *value,
        _ => 0,
    }
}

/// Go `Row.GetTime(index).String()`.
fn get_time(row: &[Datum], index: usize) -> String {
    match row.get(index) {
        Some(Datum::Time(time)) => time.to_string(),
        _ => String::new(),
    }
}

/// Go `fmt`'s `%v` of a float64.
fn float_v(value: f64) -> String {
    tidb_goruntime::go_strconv::format_float(value, b'g', -1, 64)
}

/// Go `cmp.Compare` of two float64s: a NaN is less than any non-NaN and
/// equal to a NaN.
fn compare_float64(left: f64, right: f64) -> std::cmp::Ordering {
    left.partial_cmp(&right)
        .unwrap_or_else(|| right.is_nan().cmp(&left.is_nan()))
}

type Rows = Vec<Vec<Datum>>;

/// The session an inspection runs on: its restricted SQL executor and its
/// statement's warnings.
struct Inspection<'a> {
    query: &'a mut dyn FnMut(&str, &[SqlArg<'_>]) -> Result<Rows, DriverError>,
    warn: &'a mut dyn FnMut(String),
}

impl Inspection<'_> {
    /// Go `exec.ExecRestrictedSQL(ctx, nil, sql, args...)`, the error by its
    /// text.
    fn exec(&mut self, sql: &str, arguments: &[SqlArg<'_>]) -> Result<Rows, String> {
        (self.query)(sql, arguments).map_err(|error| error.to_mysql_error().message)
    }

    /// The rows of `sql`, or Go's `execute '%s' failed: %v` warning.
    fn exec_or_warn(&mut self, sql: &str) -> Option<Rows> {
        match self.exec(sql, &[]) {
            Ok(rows) => Some(rows),
            Err(err) => {
                (self.warn)(format!("execute '{sql}' failed: {err}"));
                None
            }
        }
    }
}

/// Go `inspectionResultRetriever.retrieve` for one scan: the results of each
/// inspection rule the extractor admits, most severe first within a rule.
pub fn inspection_result_rows(
    extractor: &InspectionResultTableExtractor,
    time_range: &QueryTimeRange,
    cache: &InspectionTableCache,
    mut query: impl FnMut(&str, &[SqlArg<'_>]) -> Result<Rows, DriverError>,
    mut warn: impl FnMut(String),
) -> Rows {
    if extractor.skip_inspection {
        return Vec::new();
    }

    // Some data of cluster-level memory tables will be retrieved many times in different inspection rules,
    // and the cost of retrieving some data is expensive. We use the `TableSnapshot` to cache those data
    // and obtain them lazily, and provide a consistent view of inspection tables for each inspection rules.
    // All cached snapshots should be released at the end of retrieving.
    struct Release<'a>(&'a InspectionTableCache);
    impl Drop for Release<'_> {
        fn drop(&mut self) {
            self.0.set(None);
        }
    }
    #[allow(unused_mut)]
    let mut tables: HashMap<String, TableSnapshot> = HashMap::new();
    // Merge mock snapshots injected from failpoint for test purpose
    #[cfg(any(test, feature = "test-util"))]
    for (name, snap) in mock::inspection_tables().unwrap_or_default() {
        tables.insert(name.to_lowercase(), snap);
    }
    cache.set(Some(tables));
    let _release = Release(cache);

    let mut inspection = Inspection {
        query: &mut query,
        warn: &mut warn,
    };

    // Get cluster info.
    let mut instance_to_status_address = HashMap::new();
    let mut status_to_instance_address = HashMap::new();
    let rows = inspection
        .exec("select instance,status_address from information_schema.cluster_info;", &[])
        .unwrap_or_else(|err| {
            (inspection.warn)(format!("get cluster info failed: {err}"));
            Vec::new()
        });
    for row in &rows {
        if row.len() < 2 {
            continue;
        }
        instance_to_status_address.insert(get_string(row, 0), get_string(row, 1));
        status_to_instance_address.insert(get_string(row, 1), get_string(row, 0));
    }

    let items = InspectionFilter {
        set: &extractor.items,
        time_range,
    };
    // Go `inspectionRules`, in order.
    let inspection_rules: [(&str, fn(&mut Inspection<'_>, &InspectionFilter<'_>) -> Vec<InspectionResult>); 5] = [
        ("config", |inspection, filter| inspection.config_inspection(filter)),
        ("version", |inspection, filter| inspection.version_inspection(filter)),
        ("node-load", |inspection, filter| inspection.node_load_inspection(filter)),
        ("critical-error", |inspection, filter| inspection.critical_error_inspection(filter)),
        ("threshold-check", |inspection, filter| inspection.threshold_check_inspection(filter)),
    ];
    let mut final_rows = Vec::new();
    for (name, inspect) in inspection_rules {
        if !extractor.rules.is_empty() && !extractor.rules.contains(name) {
            continue;
        }
        let mut results = inspect(&mut inspection, &items);
        // make result stable
        results.sort_by(|i, j| {
            compare_float64(i.degree, j.degree)
                .reverse()
                .then_with(|| i.item.cmp(&j.item))
                .then_with(|| i.actual.cmp(&j.actual))
                .then_with(|| i.tp.cmp(&j.tp))
                .then_with(|| i.instance.cmp(&j.instance))
        });
        for mut result in results {
            if result.instance.is_empty() {
                result.instance = status_to_instance_address
                    .get(&result.status_address)
                    .cloned()
                    .unwrap_or_default();
            }
            if result.status_address.is_empty() {
                result.status_address = instance_to_status_address
                    .get(&result.instance)
                    .cloned()
                    .unwrap_or_default();
            }
            final_rows.push(
                [
                    name,
                    &result.item,
                    &result.tp,
                    &result.instance,
                    &result.status_address,
                    &result.actual,
                    &result.expected,
                    &result.severity,
                    &result.detail,
                ]
                .map(|value| Datum::Bytes(value.as_bytes().to_vec()))
                .to_vec(),
            );
        }
    }
    final_rows
}

/// Go `inspectDiffConfig`'s `ignoreConfigKey`.
const IGNORE_CONFIG_KEY: &[&str] = &[
    // TiDB
    "port",
    "status.status-port",
    "host",
    "path",
    "advertise-address",
    "status.status-port",
    "log.file.filename",
    "log.slow-query-file",
    "tmp-storage-path",
    // PD
    "advertise-client-urls",
    "advertise-peer-urls",
    "client-urls",
    "data-dir",
    "log-file",
    "log.file.filename",
    "metric.job",
    "name",
    "peer-urls",
    "initial-cluster",
    "initial-cluster-state",
    "join",
    // TiKV
    "server.addr",
    "server.advertise-addr",
    "server.advertise-status-addr",
    "server.status-addr",
    "log-file",
    "raftstore.raftdb-path",
    "storage.data-dir",
    "storage.block-cache.capacity",
    // TiProxy
    "proxy.advertise-addr",
];

/// Go `ruleChecker`.
trait RuleChecker {
    fn gen_sql(&self, time_range: &QueryTimeRange) -> String;
    fn gen_result(&self, row: &[Datum]) -> InspectionResult;
    fn get_item(&self) -> String;
}

impl Inspection<'_> {
    /// Go `configInspection.inspect`: checks whether a same configuration
    /// item has a different value between different instance in the cluster.
    fn config_inspection(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        let mut results = self.inspect_diff_config(filter);
        results.extend(self.inspect_check_config(filter));
        results
    }

    /// Go `configInspection.inspectDiffConfig`.
    fn inspect_diff_config(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        // check the configuration consistent
        let ignore_config_key = IGNORE_CONFIG_KEY.iter().map(|key| key.as_bytes()).collect::<Vec<_>>();
        let rows = self
            .exec(
                "select type, `key`, count(distinct value) as c from information_schema.cluster_config where `key` not in (%?) group by type, `key` having c > 1",
                &[SqlArg::Strings(&ignore_config_key)],
            )
            .unwrap_or_else(|err| {
                (self.warn)(format!("check configuration consistency failed: {err}"));
                Vec::new()
            });

        let mut results = Vec::new();
        for row in &rows {
            if filter.enable(&get_string(row, 1)) {
                let detail = self.generate_detail(&get_string(row, 0), &get_string(row, 1));
                results.push(InspectionResult {
                    tp: get_string(row, 0),
                    item: get_string(row, 1), // key
                    actual: "inconsistent".to_owned(),
                    expected: "consistent".to_owned(),
                    severity: "warning".to_owned(),
                    detail,
                    ..InspectionResult::default()
                });
            }
        }
        results
    }

    /// Go `inspectDiffConfig`'s `generateDetail`.
    fn generate_detail(&mut self, tp: &str, item: &str) -> String {
        let rows = match self.exec(
            "select value, instance from information_schema.cluster_config where type=%? and `key`=%?;",
            &[SqlArg::String(tp.as_bytes()), SqlArg::String(item.as_bytes())],
        ) {
            Ok(rows) => rows,
            Err(err) => {
                (self.warn)(format!("check configuration consistency failed: {err}"));
                return format!(
                    "the cluster has different config value of {item}, execute the sql to see more detail: select * from information_schema.cluster_config where type='{tp}' and `key`='{item}'"
                );
            }
        };
        let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for row in &rows {
            let value = get_string(row, 0);
            let instance = get_string(row, 1);
            m.entry(value).or_default().push(instance);
        }
        let mut groups = Vec::with_capacity(m.len());
        for (k, mut v) in m {
            v.sort();
            groups.push(format!("{} config value is {k}", v.join(",")));
        }
        groups.sort();
        groups.join("\n")
    }

    /// Go `configInspection.inspectCheckConfig`.
    fn inspect_check_config(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        // check the configuration in reason.
        // table, key, expect, cond, detail
        let cases = [
            (
                "cluster_config",
                "log.slow-threshold",
                "> 0",
                "type = 'tidb' and `key` = 'log.slow-threshold' and value = '0'",
                "slow-threshold = 0 will record every query to slow log, it may affect performance",
            ),
            (
                "cluster_config",
                "raftstore.sync-log",
                "true",
                "type = 'tikv' and `key` = 'raftstore.sync-log' and value = 'false'",
                "sync-log should be true to avoid recover region when the machine breaks down",
            ),
            (
                "cluster_systeminfo",
                "transparent_hugepage_enabled",
                "always madvise [never]",
                "system_name = 'kernel' and name = 'transparent_hugepage_enabled' and value not like '%[never]%'",
                "Transparent HugePages can cause memory allocation delays during runtime, TiDB recommends that you disable Transparent HugePages on all TiDB servers",
            ),
        ];

        let mut results = Vec::new();
        // Go declares `rows` outside the loop: a failed case reads the rows
        // of the case before it.
        let mut rows = Vec::new();
        for (table, key, expect, cond, detail) in cases {
            if !filter.enable(key) {
                continue;
            }
            let sql = format!("select type,instance,value from information_schema.{table} where {cond}");
            match self.exec(&sql, &[]) {
                Ok(case_rows) => rows = case_rows,
                Err(err) => (self.warn)(format!("check configuration in reason failed: {err}")),
            }

            for row in &rows {
                results.push(InspectionResult {
                    tp: get_string(row, 0),
                    instance: get_string(row, 1),
                    item: key.to_owned(),
                    actual: get_string(row, 2),
                    expected: expect.to_owned(),
                    severity: "warning".to_owned(),
                    detail: detail.to_owned(),
                    ..InspectionResult::default()
                });
            }
        }
        results.extend(self.check_tikv_block_cache_size_config(filter));
        results
    }

    /// Go `configInspection.checkTiKVBlockCacheSizeConfig`.
    fn check_tikv_block_cache_size_config(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        let item = "storage.block-cache.capacity";
        if !filter.enable(item) {
            return Vec::new();
        }
        let rows = self
            .exec(
                "select instance,value from information_schema.cluster_config where type='tikv' and `key` = 'storage.block-cache.capacity'",
                &[],
            )
            .unwrap_or_else(|err| {
                (self.warn)(format!("check configuration in reason failed: {err}"));
                Vec::new()
            });
        let extract_ip = |addr: String| match addr.find(':') {
            Some(idx) => addr[..idx].to_owned(),
            None => addr,
        };

        let mut ip_to_block_size: BTreeMap<String, u64> = BTreeMap::new();
        let mut ip_to_count: BTreeMap<String, i64> = BTreeMap::new();
        for row in &rows {
            let ip = extract_ip(get_string(row, 0));
            let size = match convert_readable_size_to_byte_size(&get_string(row, 1)) {
                Ok(size) => size,
                Err(err) => {
                    (self.warn)(format!("check TiKV block-cache configuration in reason failed: {err}"));
                    return Vec::new();
                }
            };
            let block_size = ip_to_block_size.entry(ip.clone()).or_default();
            *block_size = block_size.wrapping_add(size);
            *ip_to_count.entry(ip).or_default() += 1;
        }

        let rows = self
            .exec("select instance, value from metrics_schema.node_total_memory where time=now()", &[])
            .unwrap_or_else(|err| {
                (self.warn)(format!("check configuration in reason failed: {err}"));
                Vec::new()
            });
        let mut ip_to_memory_size: BTreeMap<String, f64> = BTreeMap::new();
        for row in &rows {
            let ip = extract_ip(get_string(row, 0));
            let size = get_float64(row, 1);
            *ip_to_memory_size.entry(ip).or_default() += size;
        }

        let mut results = Vec::new();
        for (ip, block_size) in ip_to_block_size {
            if let Some(memory_size) = ip_to_memory_size.get(&ip) {
                if block_size as f64 > memory_size * 0.45 {
                    let detail = format!(
                        "There are {} TiKV server in {ip} node, the total 'storage.block-cache.capacity' of TiKV is more than (0.45 * total node memory)",
                        ip_to_count[&ip]
                    );
                    results.push(InspectionResult {
                        tp: "tikv".to_owned(),
                        instance: ip,
                        item: item.to_owned(),
                        actual: block_size.to_string(),
                        expected: format!("< {:.0}", memory_size * 0.45),
                        severity: "warning".to_owned(),
                        detail,
                        ..InspectionResult::default()
                    });
                }
            }
        }
        results
    }

    /// Go `versionInspection.inspect`: checks whether the same component has
    /// different versions in the cluster.
    fn version_inspection(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        // check the configuration consistent
        let rows = self
            .exec(
                "select type, count(distinct git_hash) as c from information_schema.cluster_info group by type having c > 1;",
                &[],
            )
            .unwrap_or_else(|err| {
                (self.warn)(format!("check version consistency failed: {err}"));
                Vec::new()
            });

        const NAME: &str = "git_hash";
        let mut results = Vec::new();
        for row in &rows {
            if filter.enable(NAME) {
                let tp = get_string(row, 0);
                results.push(InspectionResult {
                    item: NAME.to_owned(),
                    actual: "inconsistent".to_owned(),
                    expected: "consistent".to_owned(),
                    severity: "critical".to_owned(),
                    detail: format!(
                        "the cluster has {} different {tp} versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='{tp}'",
                        get_uint64(row, 1)
                    ),
                    tp,
                    ..InspectionResult::default()
                });
            }
        }
        results
    }

    /// Go `nodeLoadInspection.inspect`: checks the node load of memory/disk/cpu
    /// have reached a high-level threshold.
    fn node_load_inspection(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        let rules: [&dyn RuleChecker; 6] = [
            &InspectCpuLoad { item: "load1", tbl: "node_load1" },
            &InspectCpuLoad { item: "load5", tbl: "node_load5" },
            &InspectCpuLoad { item: "load15", tbl: "node_load15" },
            &InspectVirtualMemUsage,
            &InspectSwapMemoryUsed,
            &InspectDiskUsage,
        ];
        self.check_rules(filter, &rules)
    }

    /// Go `checkRules`.
    fn check_rules(&mut self, filter: &InspectionFilter<'_>, rules: &[&dyn RuleChecker]) -> Vec<InspectionResult> {
        let mut results = Vec::new();
        for rule in rules {
            if !filter.enable(&rule.get_item()) {
                continue;
            }
            let sql = rule.gen_sql(filter.time_range);
            let Some(rows) = self.exec_or_warn(&sql) else {
                continue;
            };
            for row in &rows {
                results.push(rule.gen_result(row));
            }
        }
        results
    }

    /// Go `criticalErrorInspection.inspect`: checks are there some critical
    /// errors occurred in the past.
    fn critical_error_inspection(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        let mut results = self.inspect_error(filter);
        results.extend(self.inspect_for_server_down(filter));
        results
    }

    /// Go `criticalErrorInspection.inspectError`.
    fn inspect_error(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        // tp, item, tbl
        let rules = [
            ("tikv", "critical-error", "tikv_critical_error_total_count"),
            ("tidb", "panic-count", "tidb_panic_count_total_count"),
            ("tidb", "binlog-error", "tidb_binlog_error_total_count"),
            ("tikv", "scheduler-is-busy", "tikv_scheduler_is_busy_total_count"),
            ("tikv", "coprocessor-is-busy", "tikv_coprocessor_is_busy_total_count"),
            ("tikv", "channel-is-full", "tikv_channel_full_total_count"),
            ("tikv", "tikv_engine_write_stall", "tikv_engine_write_stall"),
        ];

        let condition = condition(filter.time_range);
        let mut results = Vec::new();
        for (tp, item, tbl) in rules {
            if filter.enable(item) {
                let Some(def) = tidb_infoschema::metric_table_def::get_metric_table_def(tbl) else {
                    (self.warn)(format!("metrics table: {tbl} not found"));
                    continue;
                };
                let labels = def.labels.join("`,`");
                let sql = format!(
                    "select `{labels}`,sum(value) as total from `metrics_schema`.`{tbl}` {condition} group by `{labels}` having total>=1.0"
                );
                let Some(rows) = self.exec_or_warn(&sql) else {
                    continue;
                };
                for row in &rows {
                    let rest = &def.labels[1..];
                    let (actual, degree) = if rest.is_empty() {
                        (format!("{:.2}", get_float64(row, 1)), get_float64(row, 1))
                    } else {
                        // `i+1` and `1+len(rest)` means skip the first field `instance`
                        let values = (0..rest.len()).map(|i| get_string(row, i + 1)).collect::<Vec<_>>();
                        // TODO: find a better way to construct the `actual` field
                        (
                            format!("{:.2}({})", get_float64(row, 1 + rest.len()), values.join(", ")),
                            get_float64(row, 1 + rest.len()),
                        )
                    };
                    results.push(InspectionResult {
                        tp: tp.to_owned(),
                        // NOTE: all tables which can be inspected here whose first label must be `instance`
                        status_address: get_string(row, 0),
                        item: item.to_owned(),
                        actual,
                        expected: "0".to_owned(),
                        severity: "critical".to_owned(),
                        detail: format!("the total number of errors about '{item}' is too many"),
                        degree,
                        ..InspectionResult::default()
                    });
                }
            }
        }
        results
    }

    /// Go `criticalErrorInspection.inspectForServerDown`.
    fn inspect_for_server_down(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        let item = "server-down";
        if !filter.enable(item) {
            return Vec::new();
        }
        let condition = condition(filter.time_range);
        let sql = format!(
            "select t1.job,t1.instance, t2.min_time from\n\t\t(select instance,job from metrics_schema.up {condition} group by instance,job having max(value)-min(value)>0) as t1 join\n\t\t(select instance,min(time) as min_time from metrics_schema.up {condition} and value=0 group by instance,job) as t2 on t1.instance=t2.instance order by job"
        );
        let rows = self.exec_or_warn(&sql).unwrap_or_default();
        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            if row.len() < 3 {
                continue;
            }
            let detail = format!(
                "{} {} disconnect with prometheus around time '{}'",
                get_string(row, 0),
                get_string(row, 1),
                get_time(row, 2)
            );
            results.push(InspectionResult {
                tp: get_string(row, 0),
                status_address: get_string(row, 1),
                item: item.to_owned(),
                severity: "critical".to_owned(),
                detail,
                degree: 10000.0 + results.len() as f64,
                ..InspectionResult::default()
            });
        }
        // Check from log.
        let sql = format!(
            "select type,instance,time from information_schema.cluster_log {condition} and level = 'info' and message like '%Welcome to'"
        );
        let rows = self.exec_or_warn(&sql).unwrap_or_default();
        for row in &rows {
            if row.len() < 3 {
                continue;
            }
            let detail = format!(
                "{} {} restarted at time '{}'",
                get_string(row, 0),
                get_string(row, 1),
                get_string(row, 2)
            );
            results.push(InspectionResult {
                tp: get_string(row, 0),
                instance: get_string(row, 1),
                item: item.to_owned(),
                severity: "critical".to_owned(),
                detail,
                degree: 10000.0 + results.len() as f64,
                ..InspectionResult::default()
            });
        }
        results
    }

    /// Go `thresholdCheckInspection.inspect`: checks threshold of
    /// performance-related metrics have exceed the expected value.
    fn threshold_check_inspection(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        let mut results = self.inspect_threshold1(filter);
        results.extend(self.inspect_threshold2(filter));
        results.extend(self.inspect_threshold3(filter));
        results.extend(self.inspect_for_leader_drop(filter));
        results
    }

    /// Go `thresholdCheckInspection.inspectThreshold1`.
    fn inspect_threshold1(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        // item, component, configKey, threshold
        let rules = [
            ("coprocessor-normal-cpu", "cop_normal%", "readpool.coprocessor.normal-concurrency", 0.9),
            ("coprocessor-high-cpu", "cop_high%", "readpool.coprocessor.high-concurrency", 0.9),
            ("coprocessor-low-cpu", "cop_low%", "readpool.coprocessor.low-concurrency", 0.9),
            ("grpc-cpu", "grpc%", "server.grpc-concurrency", 0.9),
            ("raftstore-cpu", "raftstore_%", "raftstore.store-pool-size", 0.8),
            ("apply-cpu", "apply_%", "raftstore.apply-pool-size", 0.8),
            ("storage-readpool-normal-cpu", "store_read_norm%", "readpool.storage.normal-concurrency", 0.9),
            ("storage-readpool-high-cpu", "store_read_high%", "readpool.storage.high-concurrency", 0.9),
            ("storage-readpool-low-cpu", "store_read_low%", "readpool.storage.low-concurrency", 0.9),
            ("scheduler-worker-cpu", "sched_%", "storage.scheduler-worker-pool-size", 0.85),
            ("split-check-cpu", "split_check", "", 0.9),
        ];

        let condition = condition(filter.time_range);
        let mut results = Vec::new();
        for (item, component, config_key, threshold) in rules {
            if !filter.enable(item) {
                continue;
            }

            let sql = if config_key.is_empty() {
                format!(
                    "select t1.instance, t1.cpu, {threshold:.6} from\n\t\t\t\t(select instance, max(value) as cpu from metrics_schema.tikv_thread_cpu {condition} and name like '{component}' group by instance) as t1\n\t\t\t\twhere t1.cpu > {threshold:.6};"
                )
            } else {
                format!(
                    "select t1.status_address, t1.cpu, (t2.value * {threshold:.6}) as threshold, t2.value from\n\t\t\t\t(select status_address, max(sum_value) as cpu from (select instance as status_address, sum(value) as sum_value from metrics_schema.tikv_thread_cpu {condition} and name like '{component}' group by instance, time) as tmp group by tmp.status_address) as t1 join\n\t\t\t\t(select instance, value from information_schema.cluster_config where type='tikv' and `key` = '{config_key}') as t2 join\n\t\t\t\t(select instance,status_address from information_schema.cluster_info where type='tikv') as t3\n\t\t\t\ton t1.status_address=t3.status_address and t2.instance=t3.instance where t1.cpu > (t2.value * {threshold:.6})"
                )
            };
            let Some(rows) = self.exec_or_warn(&sql) else {
                continue;
            };
            for row in &rows {
                let actual = format!("{:.2}", get_float64(row, 1));
                let degree = (get_float64(row, 1) - get_float64(row, 2)).abs()
                    / get_float64(row, 1).max(get_float64(row, 2));
                let expected = if config_key.is_empty() {
                    format!("< {:.2}", get_float64(row, 2))
                } else {
                    format!("< {:.2}, config: {config_key}={}", get_float64(row, 2), get_string(row, 3))
                };
                let detail = format!("the '{item}' max cpu-usage of {} tikv is too high", get_string(row, 0));
                results.push(InspectionResult {
                    tp: "tikv".to_owned(),
                    status_address: get_string(row, 0),
                    item: item.to_owned(),
                    actual,
                    expected,
                    severity: "warning".to_owned(),
                    detail,
                    degree,
                    ..InspectionResult::default()
                });
            }
        }
        results
    }

    /// Go `thresholdCheckInspection.inspectThreshold2`.
    fn inspect_threshold2(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        // tp, item, tbl, condition, threshold, factor, isMin, detail; a zero
        // factor is one, and 10e5 is for a metric whose unit is microsecond.
        let rules = [
            ("tidb", "tso-duration", "pd_tso_wait_duration", "quantile=0.999", 0.05, 1.0, false, ""),
            ("tidb", "get-token-duration", "tidb_get_token_duration", "quantile=0.999", 0.001, 10e5, false, ""),
            ("tidb", "load-schema-duration", "tidb_load_schema_duration", "quantile=0.99", 1.0, 1.0, false, ""),
            ("tikv", "scheduler-cmd-duration", "tikv_scheduler_command_duration", "quantile=0.99", 0.1, 1.0, false, ""),
            ("tikv", "handle-snapshot-duration", "tikv_handle_snapshot_duration", "", 30.0, 1.0, false, ""),
            ("tikv", "storage-write-duration", "tikv_storage_async_request_duration", "type='write'", 0.1, 1.0, false, ""),
            ("tikv", "storage-snapshot-duration", "tikv_storage_async_request_duration", "type='snapshot'", 0.05, 1.0, false, ""),
            ("tikv", "rocksdb-write-duration", "tikv_engine_write_duration", "type='write_max'", 0.1, 10e5, false, ""),
            ("tikv", "rocksdb-get-duration", "tikv_engine_max_get_duration", "type='get_max'", 0.05, 10e5, false, ""),
            ("tikv", "rocksdb-seek-duration", "tikv_engine_max_seek_duration", "type='seek_max'", 0.05, 10e5, false, ""),
            (
                "tikv",
                "scheduler-pending-cmd-count",
                "tikv_scheduler_pending_commands",
                "",
                1000.0,
                1.0,
                false,
                " %s tikv scheduler has too many pending commands",
            ),
            ("tikv", "index-block-cache-hit", "tikv_block_index_cache_hit", "value > 0", 0.95, 1.0, true, ""),
            ("tikv", "filter-block-cache-hit", "tikv_block_filter_cache_hit", "value > 0", 0.95, 1.0, true, ""),
            ("tikv", "data-block-cache-hit", "tikv_block_data_cache_hit", "value > 0", 0.80, 1.0, true, ""),
        ];

        let condition = condition(filter.time_range);
        let mut results = Vec::new();
        for (tp, item, tbl, rule_condition, threshold, factor, is_min, rule_detail) in rules {
            if !filter.enable(item) {
                continue;
            }
            let cond = if rule_condition.is_empty() {
                condition.clone()
            } else {
                format!("{condition} and {rule_condition}")
            };
            let sql = if is_min {
                format!(
                    "select instance, min(value)/{factor:.0} as min_value from metrics_schema.{tbl} {cond} group by instance having min_value < {threshold:.6};"
                )
            } else {
                format!(
                    "select instance, max(value)/{factor:.0} as max_value from metrics_schema.{tbl} {cond} group by instance having max_value > {threshold:.6};"
                )
            };
            let Some(rows) = self.exec_or_warn(&sql) else {
                continue;
            };
            for row in &rows {
                let actual = format!("{:.3}", get_float64(row, 1));
                let degree = (get_float64(row, 1) - threshold).abs() / get_float64(row, 1).max(threshold);
                let expected = if is_min {
                    format!("> {threshold:.3}")
                } else {
                    format!("< {threshold:.3}")
                };
                let detail = if !rule_detail.is_empty() {
                    rule_detail.replacen("%s", &get_string(row, 0), 1)
                } else if item.ends_with("duration") {
                    format!("max duration of {} {tp} {item} is too slow", get_string(row, 0))
                } else if item.ends_with("hit") {
                    format!("min {item} rate of {} {tp} is too low", get_string(row, 0))
                } else {
                    String::new()
                };
                results.push(InspectionResult {
                    tp: tp.to_owned(),
                    status_address: get_string(row, 0),
                    item: item.to_owned(),
                    actual,
                    expected,
                    severity: "warning".to_owned(),
                    detail,
                    degree,
                    ..InspectionResult::default()
                });
            }
        }
        results
    }

    /// Go `thresholdCheckInspection.inspectThreshold3`.
    fn inspect_threshold3(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        let rules: [&dyn RuleChecker; 5] = [
            &CompareStoreStatus { item: "leader-score-balance", tp: "leader_score", threshold: 0.05 },
            &CompareStoreStatus { item: "region-score-balance", tp: "region_score", threshold: 0.05 },
            &CompareStoreStatus { item: "store-available-balance", tp: "store_available", threshold: 0.2 },
            &CheckRegionHealth,
            &CheckStoreRegionTooMuch,
        ];
        self.check_rules(filter, &rules)
    }

    /// Go `thresholdCheckInspection.inspectForLeaderDrop`.
    fn inspect_for_leader_drop(&mut self, filter: &InspectionFilter<'_>) -> Vec<InspectionResult> {
        let condition = condition(filter.time_range);
        let threshold = 50.0;
        let sql = format!(
            "select address,min(value) as mi,max(value) as mx from metrics_schema.pd_scheduler_store_status {condition} and type='leader_count' group by address having mx-mi>{}",
            float_v(threshold)
        );

        let Some(rows) = self.exec_or_warn(&sql) else {
            return Vec::new();
        };
        let mut results = Vec::new();
        for row in &rows {
            let address = get_string(row, 0);
            let sql = format!(
                "select time, value from metrics_schema.pd_scheduler_store_status {condition} and type='leader_count' and address = '{address}' order by time"
            );
            let Some(sub_rows) = self.exec_or_warn(&sql) else {
                continue;
            };

            let mut last_value = 0.0;
            for (i, sub_row) in sub_rows.iter().enumerate() {
                let v = get_float64(sub_row, 1);
                if i == 0 {
                    last_value = v;
                    continue;
                }
                if last_value - v > threshold {
                    let level = if v == 0.0 { "critical" } else { "warning" };
                    results.push(InspectionResult {
                        tp: "tikv".to_owned(),
                        instance: address.clone(),
                        item: "leader-drop".to_owned(),
                        actual: format!("{:.0}", last_value - v),
                        expected: format!("<= {threshold:.0}"),
                        severity: level.to_owned(),
                        detail: format!(
                            "{address} tikv has too many leader-drop around time {}, leader count from {last_value:.0} drop to {v:.0}",
                            get_time(sub_row, 0)
                        ),
                        degree: last_value - v,
                        ..InspectionResult::default()
                    });
                    break;
                }
                last_value = v;
            }
        }
        results
    }
}

/// Go `configInspection.convertReadableSizeToByteSize`.
fn convert_readable_size_to_byte_size(size_str: &str) -> Result<u64, String> {
    const KB: u64 = 1 << 10;
    let rate = [("KiB", KB), ("MiB", KB << 10), ("GiB", KB << 20), ("TiB", KB << 30), ("PiB", KB << 40)]
        .into_iter()
        .find(|(suffix, _)| size_str.ends_with(suffix))
        .map_or(1, |(_, rate)| rate);
    let mut size_str = size_str;
    if rate != 1 && size_str.len() > 3 {
        size_str = &size_str[..size_str.len() - 3];
    }
    let size_str = size_str.strip_suffix('B').unwrap_or(size_str);
    // Go `strconv.Atoi`.
    let size = tidb_go_strconv::parse_int(size_str, 10, 0).map_err(|(_, mut err)| {
        err.func = "Atoi";
        err.to_string()
    })?;
    Ok((size as u64).wrapping_mul(rate))
}

/// Go `inspectVirtualMemUsage`.
struct InspectVirtualMemUsage;

impl RuleChecker for InspectVirtualMemUsage {
    fn gen_sql(&self, time_range: &QueryTimeRange) -> String {
        format!(
            "select instance, max(value) as max_usage from metrics_schema.node_memory_usage {} group by instance having max_usage >= 70",
            condition(time_range)
        )
    }

    fn gen_result(&self, row: &[Datum]) -> InspectionResult {
        InspectionResult {
            tp: "node".to_owned(),
            instance: get_string(row, 0),
            item: self.get_item(),
            actual: format!("{:.1}%", get_float64(row, 1)),
            expected: "< 70%".to_owned(),
            severity: "warning".to_owned(),
            detail: "the memory-usage is too high".to_owned(),
            ..InspectionResult::default()
        }
    }

    fn get_item(&self) -> String {
        "virtual-memory-usage".to_owned()
    }
}

/// Go `inspectSwapMemoryUsed`.
struct InspectSwapMemoryUsed;

impl RuleChecker for InspectSwapMemoryUsed {
    fn gen_sql(&self, time_range: &QueryTimeRange) -> String {
        format!(
            "select instance, max(value) as max_used from metrics_schema.node_memory_swap_used {} group by instance having max_used > 0",
            condition(time_range)
        )
    }

    fn gen_result(&self, row: &[Datum]) -> InspectionResult {
        InspectionResult {
            tp: "node".to_owned(),
            instance: get_string(row, 0),
            item: self.get_item(),
            actual: format!("{:.1}", get_float64(row, 1)),
            expected: "0".to_owned(),
            severity: "warning".to_owned(),
            ..InspectionResult::default()
        }
    }

    fn get_item(&self) -> String {
        "swap-memory-used".to_owned()
    }
}

/// Go `inspectDiskUsage`.
struct InspectDiskUsage;

impl RuleChecker for InspectDiskUsage {
    fn gen_sql(&self, time_range: &QueryTimeRange) -> String {
        format!(
            "select instance, device, max(value) as max_usage from metrics_schema.node_disk_usage {} and device like '/%' group by instance, device having max_usage >= 70",
            condition(time_range)
        )
    }

    fn gen_result(&self, row: &[Datum]) -> InspectionResult {
        InspectionResult {
            tp: "node".to_owned(),
            instance: get_string(row, 0),
            item: self.get_item(),
            actual: format!("{:.1}%", get_float64(row, 2)),
            expected: "< 70%".to_owned(),
            severity: "warning".to_owned(),
            detail: format!("the disk-usage of {} is too high", get_string(row, 1)),
            ..InspectionResult::default()
        }
    }

    fn get_item(&self) -> String {
        "disk-usage".to_owned()
    }
}

/// Go `inspectCPULoad`.
struct InspectCpuLoad {
    item: &'static str,
    tbl: &'static str,
}

impl RuleChecker for InspectCpuLoad {
    fn gen_sql(&self, time_range: &QueryTimeRange) -> String {
        let condition = condition(time_range);
        format!(
            "select t1.instance, t1.max_load , 0.7*t2.cpu_count from\n\t\t\t(select instance,max(value) as max_load  from metrics_schema.{} {condition} group by instance) as t1 join\n\t\t\t(select instance,max(value) as cpu_count from metrics_schema.node_virtual_cpus {condition} group by instance) as t2\n\t\t\ton t1.instance=t2.instance where t1.max_load>(0.7*t2.cpu_count);",
            self.tbl
        )
    }

    fn gen_result(&self, row: &[Datum]) -> InspectionResult {
        InspectionResult {
            tp: "node".to_owned(),
            instance: get_string(row, 0),
            item: self.get_item(),
            actual: format!("{:.1}", get_float64(row, 1)),
            expected: format!("< {:.1}", get_float64(row, 2)),
            severity: "warning".to_owned(),
            detail: format!("{} should less than (cpu_logical_cores * 0.7)", self.get_item()),
            ..InspectionResult::default()
        }
    }

    fn get_item(&self) -> String {
        format!("cpu-{}", self.item)
    }
}

/// Go `compareStoreStatus`.
struct CompareStoreStatus {
    item: &'static str,
    tp: &'static str,
    threshold: f64,
}

impl RuleChecker for CompareStoreStatus {
    fn gen_sql(&self, time_range: &QueryTimeRange) -> String {
        let (from, to) = (&time_range.from, &time_range.to);
        let condition = format!(
            "where t1.time>='{from}' and t1.time<='{to}' and\n\t\t t2.time>='{from}' and t2.time<='{to}'"
        );
        format!(
            "\n\t\tSELECT t1.address,\n        \tmax(t1.value),\n        \tt2.address,\n        \tmin(t2.value),\n         \tmax((t1.value-t2.value)/t1.value) AS ratio\n\t\tFROM metrics_schema.pd_scheduler_store_status t1\n\t\tJOIN metrics_schema.pd_scheduler_store_status t2 {condition}\n        \tAND t1.type='{}'\n        \tAND t1.time = t2.time\n        \tAND t1.type=t2.type\n        \tAND t1.address != t2.address\n        \tAND (t1.value-t2.value)/t1.value>{}\n        \tAND t1.value > 0\n\t\tGROUP BY  t1.address,t2.address\n\t\tORDER BY  ratio desc",
            self.tp,
            float_v(self.threshold)
        )
    }

    fn gen_result(&self, row: &[Datum]) -> InspectionResult {
        let addr1 = get_string(row, 0);
        let value1 = get_float64(row, 1);
        let addr2 = get_string(row, 2);
        let value2 = get_float64(row, 3);
        let ratio = get_float64(row, 4);
        let tp = self.tp;
        let detail = format!("{addr1} max {tp} is {value1:.2}, much more than {addr2} min {tp} {value2:.2}");
        InspectionResult {
            tp: "tikv".to_owned(),
            instance: addr2,
            item: self.item.to_owned(),
            actual: format!("{:.2}%", ratio * 100.0),
            expected: format!("< {:.2}%", self.threshold * 100.0),
            severity: "warning".to_owned(),
            detail,
            degree: ratio,
            ..InspectionResult::default()
        }
    }

    fn get_item(&self) -> String {
        self.item.to_owned()
    }
}

/// Go `checkRegionHealth`.
struct CheckRegionHealth;

impl RuleChecker for CheckRegionHealth {
    fn gen_sql(&self, time_range: &QueryTimeRange) -> String {
        format!(
            "select instance, sum(value) as sum_value from metrics_schema.pd_region_health {} and\n\t\ttype in ('extra-peer-region-count','learner-peer-region-count','pending-peer-region-count') having sum_value>100",
            condition(time_range)
        )
    }

    fn gen_result(&self, row: &[Datum]) -> InspectionResult {
        let value = get_float64(row, 1);
        InspectionResult {
            tp: "pd".to_owned(),
            instance: get_string(row, 0),
            item: self.get_item(),
            actual: format!("{value:.2}"),
            expected: "< 100".to_owned(),
            severity: "warning".to_owned(),
            detail: format!(
                "the count of extra-perr and learner-peer and pending-peer are {}, it means the scheduling is too frequent or too slow",
                float_v(value)
            ),
            degree: (value - 100.0).abs() / value.max(100.0),
            ..InspectionResult::default()
        }
    }

    fn get_item(&self) -> String {
        "region-health".to_owned()
    }
}

/// Go `checkStoreRegionTooMuch`.
struct CheckStoreRegionTooMuch;

impl RuleChecker for CheckStoreRegionTooMuch {
    fn gen_sql(&self, time_range: &QueryTimeRange) -> String {
        format!(
            "select address, max(value) from metrics_schema.pd_scheduler_store_status {} and type='region_count' and value > 20000 group by address",
            condition(time_range)
        )
    }

    fn gen_result(&self, row: &[Datum]) -> InspectionResult {
        let value = get_float64(row, 1);
        InspectionResult {
            tp: "tikv".to_owned(),
            instance: get_string(row, 0),
            item: self.get_item(),
            actual: format!("{value:.2}"),
            expected: "<= 20000".to_owned(),
            severity: "warning".to_owned(),
            detail: format!("{} tikv has too many regions", get_string(row, 0)),
            degree: (value - 20000.0).abs() / value.max(20000.0),
            ..InspectionResult::default()
        }
    }

    fn get_item(&self) -> String {
        "region-count".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestConfigCheckOfStorageBlockCacheSize`'s sizes, and
    /// `strconv.Atoi`'s error for a value that is no size.
    #[test]
    fn a_readable_size_is_its_byte_size_like_go() {
        assert_eq!(convert_readable_size_to_byte_size("10GiB"), Ok(10 << 30));
        assert_eq!(convert_readable_size_to_byte_size("512MiB"), Ok(512 << 20));
        assert_eq!(convert_readable_size_to_byte_size("1024B"), Ok(1024));
        assert_eq!(convert_readable_size_to_byte_size("7"), Ok(7));
        assert_eq!(
            convert_readable_size_to_byte_size("1.5GiB"),
            Err("strconv.Atoi: parsing \"1.5\": invalid syntax".to_owned())
        );
    }

    #[test]
    fn a_nan_degree_sorts_as_gos_cmp_compare() {
        use std::cmp::Ordering;
        assert_eq!(compare_float64(f64::NAN, 1.0), Ordering::Less);
        assert_eq!(compare_float64(1.0, f64::NAN), Ordering::Greater);
        assert_eq!(compare_float64(f64::NAN, f64::NAN), Ordering::Equal);
        assert_eq!(compare_float64(2.0, 1.0), Ordering::Greater);
    }
}
