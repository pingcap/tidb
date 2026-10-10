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

//! Go `pkg/infoschema/metric_table_def.go` (`MetricTableMap`) and the
//! `MetricTableDef` half of `pkg/infoschema/metrics_schema.go`.
//!
//! The definitions live in this crate rather than beside the executor's other
//! `infoschema` tables because the planner reads them too
//! (`MetricTableExtractor.GetMetricTablePromQL`), and this is the lowest crate
//! both depend on. [`METRIC_TABLE_MAP`] is generated from Go's map literal and
//! sorted by name, the order Go's `setDataForMetricTables` and table-ID
//! assignment both sort into.

use std::collections::{BTreeMap, BTreeSet};

const PROM_QL_QUANTILE_KEY: &str = "$QUANTILE";
const PROM_QL_LABEL_CONDITION_KEY: &str = "$LABEL_CONDITIONS";
const PROM_QL_RANGE_DURATION_KEY: &str = "$RANGE_DURATION";

/// Go `infoschema.MetricTableDef`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricTableDef {
    /// Go `PromQL`, with the `$QUANTILE`, `$LABEL_CONDITIONS` and
    /// `$RANGE_DURATION` placeholders [`Self::gen_prom_ql`] fills.
    pub prom_ql: &'static str,
    /// Go `Labels`: the label columns, in table-column order.
    pub labels: &'static [&'static str],
    /// Go `Quantile`; zero when the table has no `quantile` column.
    pub quantile: f64,
    /// Go `Comment`.
    pub comment: &'static str,
}

/// Go `infoschema.IsMetricTable(lowerTableName)`.
#[must_use]
pub fn is_metric_table(lower_table_name: &str) -> bool {
    get_metric_table_def(lower_table_name).is_some()
}

/// Go `infoschema.GetMetricTableDef(lowerTableName)`; `None` is Go's
/// "can not find metric table" error.
#[must_use]
pub fn get_metric_table_def(lower_table_name: &str) -> Option<&'static MetricTableDef> {
    METRIC_TABLE_MAP
        .binary_search_by(|(name, _)| (*name).cmp(lower_table_name))
        .ok()
        .map(|index| &METRIC_TABLE_MAP[index].1)
}

impl MetricTableDef {
    /// Go `MetricTableDef.GenPromQL(metricsSchemaRangeDuration, labels,
    /// quantile)`.
    #[must_use]
    pub fn gen_prom_ql(
        &self,
        metrics_schema_range_duration: i64,
        labels: &BTreeMap<String, BTreeSet<String>>,
        quantile: f64,
    ) -> String {
        self.prom_ql
            .replace(PROM_QL_QUANTILE_KEY, &format_float(quantile))
            .replace(
                PROM_QL_LABEL_CONDITION_KEY,
                &self.gen_label_condition(labels),
            )
            .replace(
                PROM_QL_RANGE_DURATION_KEY,
                &format!("{metrics_schema_range_duration}s"),
            )
    }

    /// Go `MetricTableDef.genLabelCondition(labels)`.
    fn gen_label_condition(&self, labels: &BTreeMap<String, BTreeSet<String>>) -> String {
        let mut conditions = Vec::new();
        for label in self.labels {
            let Some(values) = labels.get(*label).filter(|values| !values.is_empty()) else {
                continue;
            };
            let operator = if values.len() == 1 { "=" } else { "=~" };
            conditions.push(format!(
                "{label}{operator}\"{}\"",
                gen_label_condition_values(values)
            ));
        }
        conditions.join(",")
    }

    /// Go `strconv.FormatFloat(def.Quantile, 'f', -1, 64)`, the `quantile`
    /// column's default in `genColumnInfos`.
    #[must_use]
    pub fn quantile_default(&self) -> String {
        format_float(self.quantile)
    }
}

/// Go `infoschema.GenLabelConditionValues(values)`: sorted, joined by `|`.
#[must_use]
pub fn gen_label_condition_values(values: &BTreeSet<String>) -> String {
    values
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("|")
}

/// Go `strconv.FormatFloat(v, 'f', -1, 64)`: the shortest round-tripping
/// decimal, never in exponent form, which is also Rust's `f64` display.
fn format_float(value: f64) -> String {
    format!("{value}")
}

/// Go `infoschema.MetricTableMap`, sorted by table name.
pub static METRIC_TABLE_MAP: &[(&str, MetricTableDef)] = &[
    (
        "abnormal_stores",
        MetricTableDef {
            prom_ql: "sum(pd_cluster_status{ type=~\"store_disconnected_count|store_unhealth_count|store_low_space_count|store_down_count|store_offline_count|store_tombstone_count\"})",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "etcd_disk_wal_fsync_rate",
        MetricTableDef {
            prom_ql: "delta(etcd_disk_wal_fsync_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The rate of writing WAL into the persistent storage",
        },
    ),
    (
        "etcd_wal_fsync_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(etcd_disk_wal_fsync_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile time consumed of writing WAL into the persistent storage",
        },
    ),
    (
        "etcd_wal_fsync_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(etcd_disk_wal_fsync_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of writing WAL into the persistent storage",
        },
    ),
    (
        "etcd_wal_fsync_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(etcd_disk_wal_fsync_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of writing WAL into the persistent storage",
        },
    ),
    (
        "go_gc_count",
        MetricTableDef {
            prom_ql: " rate(go_gc_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "The Go garbage collection counts per second",
        },
    ),
    (
        "go_gc_cpu_usage",
        MetricTableDef {
            prom_ql: "go_memstats_gc_cpu_fraction{$LABEL_CONDITIONS}",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "The fraction of TiDB/PD available CPU time used by the GC since the program started.",
        },
    ),
    (
        "go_gc_duration",
        MetricTableDef {
            prom_ql: "rate(go_gc_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "Go garbage collection STW pause duration(second)",
        },
    ),
    (
        "go_heap_mem_usage",
        MetricTableDef {
            prom_ql: "go_memstats_heap_alloc_bytes{$LABEL_CONDITIONS}",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "TiDB heap memory size in use",
        },
    ),
    (
        "go_threads",
        MetricTableDef {
            prom_ql: "go_threads{$LABEL_CONDITIONS}",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "Total threads TiDB/PD process created currently",
        },
    ),
    (
        "goroutines_count",
        MetricTableDef {
            prom_ql: " go_goroutines{$LABEL_CONDITIONS}",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "Process current goroutines count)",
        },
    ),
    (
        "node_cpu_usage",
        MetricTableDef {
            prom_ql: "sum(rate(node_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (mode,instance) * 100 / count(node_cpu_seconds_total{$LABEL_CONDITIONS}) by (mode,instance) or sum(irate(node_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (mode,instance) * 100 / count(node_cpu_seconds_total{$LABEL_CONDITIONS}) by (mode,instance)",
            labels: &["instance", "mode"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_disk_available_size",
        MetricTableDef {
            prom_ql: "node_filesystem_avail_bytes{$LABEL_CONDITIONS}",
            labels: &["instance", "device", "fstype", "mountpoint"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_disk_io_util",
        MetricTableDef {
            prom_ql: "rate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_disk_iops",
        MetricTableDef {
            prom_ql: "sum(rate(node_disk_reads_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) + rate(node_disk_writes_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_disk_read_latency",
        MetricTableDef {
            prom_ql: "(rate(node_disk_read_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])/ rate(node_disk_reads_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "node disk read latency",
        },
    ),
    (
        "node_disk_size",
        MetricTableDef {
            prom_ql: "node_filesystem_size_bytes{$LABEL_CONDITIONS}",
            labels: &["instance", "device", "fstype", "mountpoint"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_disk_state",
        MetricTableDef {
            prom_ql: "node_filesystem_readonly{$LABEL_CONDITIONS}",
            labels: &["instance", "device", "fstype", "mountpoint"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_disk_throughput",
        MetricTableDef {
            prom_ql: "irate(node_disk_read_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) + irate(node_disk_written_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "Units is byte",
        },
    ),
    (
        "node_disk_usage",
        MetricTableDef {
            prom_ql: "((node_filesystem_size_bytes{$LABEL_CONDITIONS} - node_filesystem_avail_bytes{$LABEL_CONDITIONS}) / node_filesystem_size_bytes{$LABEL_CONDITIONS}) * 100",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "Filesystem used space. If is > 80% then is Critical.",
        },
    ),
    (
        "node_disk_write_latency",
        MetricTableDef {
            prom_ql: "(rate(node_disk_write_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])/ rate(node_disk_writes_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "node disk write latency",
        },
    ),
    (
        "node_file_descriptor_allocated",
        MetricTableDef {
            prom_ql: "node_filefd_allocated{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_kernel_context_switches",
        MetricTableDef {
            prom_ql: "rate(node_context_switches_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_context_switches_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_kernel_forks",
        MetricTableDef {
            prom_ql: "rate(node_forks_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_forks_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_kernel_interrupts",
        MetricTableDef {
            prom_ql: "rate(node_intr_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_intr_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_load1",
        MetricTableDef {
            prom_ql: "node_load1{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "1 minute load averages in node",
        },
    ),
    (
        "node_load15",
        MetricTableDef {
            prom_ql: "node_load15{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "15 minutes load averages in node",
        },
    ),
    (
        "node_load5",
        MetricTableDef {
            prom_ql: "node_load5{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "5 minutes load averages in node",
        },
    ),
    (
        "node_memory_active",
        MetricTableDef {
            prom_ql: "node_memory_Active_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_available",
        MetricTableDef {
            prom_ql: "node_memory_MemAvailable_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_buffers",
        MetricTableDef {
            prom_ql: "node_memory_Buffers_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_cached",
        MetricTableDef {
            prom_ql: "node_memory_Cached_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_dirty",
        MetricTableDef {
            prom_ql: "node_memory_Dirty_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_free",
        MetricTableDef {
            prom_ql: "node_memory_MemFree_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_inactive",
        MetricTableDef {
            prom_ql: "node_memory_Inactive_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_mapped",
        MetricTableDef {
            prom_ql: "node_memory_Mapped_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_shared",
        MetricTableDef {
            prom_ql: "node_memory_Shmem_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_swap_used",
        MetricTableDef {
            prom_ql: "node_memory_SwapTotal_bytes{$LABEL_CONDITIONS} - node_memory_SwapFree_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "bytes used of node swap memory",
        },
    ),
    (
        "node_memory_usage",
        MetricTableDef {
            prom_ql: "100* (1-(node_memory_MemAvailable_bytes{$LABEL_CONDITIONS}/node_memory_MemTotal_bytes{$LABEL_CONDITIONS}))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_writeback",
        MetricTableDef {
            prom_ql: "node_memory_Writeback_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_memory_writeback_tmp",
        MetricTableDef {
            prom_ql: "node_memory_WritebackTmp_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_in_drops",
        MetricTableDef {
            prom_ql: "rate(node_network_receive_drop_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) ",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_in_errors",
        MetricTableDef {
            prom_ql: "rate(node_network_receive_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_in_errors_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(node_network_receive_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by(instance, device)",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_in_packets",
        MetricTableDef {
            prom_ql: "rate(node_network_receive_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_receive_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_in_traffic",
        MetricTableDef {
            prom_ql: "rate(node_network_receive_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_receive_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_interface_speed",
        MetricTableDef {
            prom_ql: "node_network_transmit_queue_length{$LABEL_CONDITIONS}",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "node_network_transmit_queue_length = transmit_queue_length value of /sys/class/net/<iface>.",
        },
    ),
    (
        "node_network_out_drops",
        MetricTableDef {
            prom_ql: "rate(node_network_transmit_drop_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_out_errors",
        MetricTableDef {
            prom_ql: "rate(node_network_transmit_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_out_errors_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(node_network_transmit_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, device)",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_out_packets",
        MetricTableDef {
            prom_ql: "rate(node_network_transmit_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_transmit_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_out_traffic",
        MetricTableDef {
            prom_ql: "rate(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_utilization_in_hourly",
        MetricTableDef {
            prom_ql: "sum(increase(node_network_receive_bytes_total{$LABEL_CONDITIONS}[1h]))",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_network_utilization_out_hourly",
        MetricTableDef {
            prom_ql: "sum(increase(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[1h]))",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_process_open_fd_count",
        MetricTableDef {
            prom_ql: "process_open_fds{$LABEL_CONDITIONS}",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "Process opened file descriptors count",
        },
    ),
    (
        "node_processes_blocked",
        MetricTableDef {
            prom_ql: "node_procs_blocked{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_processes_running",
        MetricTableDef {
            prom_ql: "node_procs_running{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_tcp_connections",
        MetricTableDef {
            prom_ql: "node_netstat_Tcp_CurrEstab{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_tcp_in_use",
        MetricTableDef {
            prom_ql: "node_sockstat_TCP_inuse{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_tcp_segments_retransmitted",
        MetricTableDef {
            prom_ql: "rate(node_netstat_Tcp_RetransSegs{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_netstat_Tcp_RetransSegs{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "node_total_memory",
        MetricTableDef {
            prom_ql: "node_memory_MemTotal_bytes{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "total memory in node",
        },
    ),
    (
        "node_uptime",
        MetricTableDef {
            prom_ql: "node_time_seconds{$LABEL_CONDITIONS} - node_boot_time_seconds{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "node uptime, units are seconds",
        },
    ),
    (
        "node_virtual_cpus",
        MetricTableDef {
            prom_ql: "count(node_cpu_seconds_total{mode=\"user\"}) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "node virtual cpu count",
        },
    ),
    (
        "normal_stores",
        MetricTableDef {
            prom_ql: "sum(pd_cluster_status{type=\"store_up_count\"}) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The count of healthy stores",
        },
    ),
    (
        "pd_balance_scheduler_status",
        MetricTableDef {
            prom_ql: "sum(delta(pd_scheduler_event_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,name)",
            labels: &["instance", "name", "type"],
            quantile: 0.0,
            comment: "The inner status of balance leader scheduler",
        },
    ),
    (
        "pd_checker_event_count",
        MetricTableDef {
            prom_ql: "sum(delta(pd_checker_event_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance,type)",
            labels: &["instance", "name", "type"],
            quantile: 0.0,
            comment: "The replica/region checker's status",
        },
    ),
    (
        "pd_client_cmd_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
            labels: &["instance", "type"],
            quantile: 0.95,
            comment: "The quantile of pd client command durations",
        },
    ),
    (
        "pd_client_cmd_ops",
        MetricTableDef {
            prom_ql: "sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "pd client command ops",
        },
    ),
    (
        "pd_client_cmd_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_cmd_handle_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of pd client command durations",
        },
    ),
    (
        "pd_client_cmd_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_cmd_handle_cmds_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of pd client command durations",
        },
    ),
    (
        "pd_cluster_metadata",
        MetricTableDef {
            prom_ql: "pd_cluster_metadata{$LABEL_CONDITIONS}",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_cluster_status",
        MetricTableDef {
            prom_ql: "sum(pd_cluster_status{$LABEL_CONDITIONS}) by (instance, type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_cmd_fail_ops",
        MetricTableDef {
            prom_ql: "sum(rate(pd_client_cmd_handle_failed_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "pd client command fail count",
        },
    ),
    (
        "pd_cmd_fail_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_cmd_handle_failed_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of pd client command fail",
        },
    ),
    (
        "pd_grpc_completed_commands_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(grpc_server_handling_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,grpc_method,instance))",
            labels: &["instance", "grpc_method"],
            quantile: 0.99,
            comment: "The quantile time consumed of completing each kind of gRPC commands",
        },
    ),
    (
        "pd_grpc_completed_commands_rate",
        MetricTableDef {
            prom_ql: "sum(rate(grpc_server_handling_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (grpc_method,instance)",
            labels: &["instance", "grpc_method"],
            quantile: 0.0,
            comment: "The rate of completing each kind of gRPC commands",
        },
    ),
    (
        "pd_grpc_completed_commands_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(grpc_server_handling_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,grpc_method)",
            labels: &["instance", "grpc_method"],
            quantile: 0.0,
            comment: "The total count of completing each kind of gRPC commands",
        },
    ),
    (
        "pd_grpc_completed_commands_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(grpc_server_handling_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,grpc_method)",
            labels: &["instance", "grpc_method"],
            quantile: 0.0,
            comment: "The total time of completing each kind of gRPC commands",
        },
    ),
    (
        "pd_handle_transactions_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(pd_txn_handle_txns_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance, result))",
            labels: &["instance", "result"],
            quantile: 0.99,
            comment: "The quantile time consumed of handling etcd transactions",
        },
    ),
    (
        "pd_handle_transactions_rate",
        MetricTableDef {
            prom_ql: "sum(rate(pd_txn_handle_txns_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, result)",
            labels: &["instance", "result"],
            quantile: 0.0,
            comment: "The rate of handling etcd transactions",
        },
    ),
    (
        "pd_handle_transactions_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_txn_handle_txns_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
            labels: &["instance", "result"],
            quantile: 0.0,
            comment: "The total count of handling etcd transactions",
        },
    ),
    (
        "pd_handle_transactions_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(pd_txn_handle_txns_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
            labels: &["instance", "result"],
            quantile: 0.0,
            comment: "The total time of handling etcd transactions",
        },
    ),
    (
        "pd_hotspot_status",
        MetricTableDef {
            prom_ql: "pd_hotspot_status{$LABEL_CONDITIONS}",
            labels: &["instance", "address", "store", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_label_distribution",
        MetricTableDef {
            prom_ql: "pd_cluster_placement_status{$LABEL_CONDITIONS}",
            labels: &["name"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_operator_finish_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(pd_schedule_finish_operators_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type))",
            labels: &["type"],
            quantile: 0.99,
            comment: "The quantile time consumed when the operator is finished",
        },
    ),
    (
        "pd_operator_finish_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_schedule_finish_operators_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
            labels: &["type"],
            quantile: 0.0,
            comment: "The total count of the operator is finished",
        },
    ),
    (
        "pd_operator_finish_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(pd_schedule_finish_operators_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
            labels: &["type"],
            quantile: 0.0,
            comment: "The total time consumed when the operator is finished",
        },
    ),
    (
        "pd_operator_step_finish_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(pd_schedule_finish_operator_steps_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type))",
            labels: &["type"],
            quantile: 0.99,
            comment: "The quantile time consumed when the operator step is finished",
        },
    ),
    (
        "pd_operator_step_finish_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_schedule_finish_operator_steps_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
            labels: &["type"],
            quantile: 0.0,
            comment: "The total count of the operator step is finished",
        },
    ),
    (
        "pd_operator_step_finish_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(pd_schedule_finish_operator_steps_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
            labels: &["type"],
            quantile: 0.0,
            comment: "The total time consumed when the operator step is finished",
        },
    ),
    (
        "pd_peer_round_trip_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(etcd_network_peer_round_trip_time_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,To))",
            labels: &["instance", "To"],
            quantile: 0.99,
            comment: "The quantile latency of the network in .99",
        },
    ),
    (
        "pd_peer_round_trip_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(etcd_network_peer_round_trip_time_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,To)",
            labels: &["instance", "To"],
            quantile: 0.0,
            comment: "The total count of the network in .99",
        },
    ),
    (
        "pd_peer_round_trip_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(etcd_network_peer_round_trip_time_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,To)",
            labels: &["instance", "To"],
            quantile: 0.0,
            comment: "The total time of latency of the network in .99",
        },
    ),
    (
        "pd_region_health",
        MetricTableDef {
            prom_ql: "sum(pd_regions_status{$LABEL_CONDITIONS}) by (instance, type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "It records the unusual Regions' count which may include pending peers, down peers, extra peers, offline peers, missing peers or learner peers",
        },
    ),
    (
        "pd_region_heartbeat_duration",
        MetricTableDef {
            prom_ql: "round(histogram_quantile($QUANTILE, sum(rate(pd_scheduler_region_heartbeat_latency_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,address, store)), 1000)",
            labels: &["address", "store"],
            quantile: 0.99,
            comment: "The quantile of heartbeat latency of each TiKV instance in",
        },
    ),
    (
        "pd_region_heartbeat_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_scheduler_region_heartbeat_latency_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store)",
            labels: &["address", "store"],
            quantile: 0.0,
            comment: "The total count of heartbeat latency of each TiKV instance in",
        },
    ),
    (
        "pd_region_heartbeat_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(pd_scheduler_region_heartbeat_latency_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store)",
            labels: &["address", "store"],
            quantile: 0.0,
            comment: "The total time of heartbeat latency of each TiKV instance in",
        },
    ),
    (
        "pd_region_label_isolation_level",
        MetricTableDef {
            prom_ql: "pd_regions_label_level{$LABEL_CONDITIONS}",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_region_syncer_status",
        MetricTableDef {
            prom_ql: "pd_region_syncer_status{$LABEL_CONDITIONS}",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_request_rpc_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.999,
            comment: "The quantile of pd client handle request duration(second)",
        },
    ),
    (
        "pd_request_rpc_duration_avg",
        MetricTableDef {
            prom_ql: "avg(rate(pd_client_request_handle_requests_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type) / avg(rate(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
            labels: &["type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_request_rpc_ops",
        MetricTableDef {
            prom_ql: "sum(rate(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "pd client handle request operation per second",
        },
    ),
    (
        "pd_request_rpc_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of pd client handle request duration(second)",
        },
    ),
    (
        "pd_request_rpc_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_request_handle_requests_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of pd client handle request duration(second)",
        },
    ),
    (
        "pd_role",
        MetricTableDef {
            prom_ql: "delta(pd_tso_events{type=\"save\"}[$RANGE_DURATION]) > bool 0",
            labels: &["instance"],
            quantile: 0.0,
            comment: "It indicates whether the current PD is the leader or a follower.",
        },
    ),
    (
        "pd_schedule_filter",
        MetricTableDef {
            prom_ql: "sum(delta(pd_schedule_filter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (store, type, scope, instance)",
            labels: &["instance", "scope", "store", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_schedule_operator",
        MetricTableDef {
            prom_ql: "sum(delta(pd_schedule_operators_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,event,instance)",
            labels: &["instance", "type", "event"],
            quantile: 0.0,
            comment: "The number of different operators",
        },
    ),
    (
        "pd_schedule_operator_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(pd_schedule_operators_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,event,instance)",
            labels: &["instance", "type", "event"],
            quantile: 0.0,
            comment: "The total number of different operators",
        },
    ),
    (
        "pd_schedule_store_limit",
        MetricTableDef {
            prom_ql: "pd_schedule_store_limit{$LABEL_CONDITIONS}",
            labels: &["instance", "store", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_scheduler_balance_direction",
        MetricTableDef {
            prom_ql: "sum(delta(pd_scheduler_balance_direction{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,source,target,instance)",
            labels: &["instance", "source", "target", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_scheduler_balance_leader",
        MetricTableDef {
            prom_ql: "sum(delta(pd_scheduler_balance_leader{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store,instance,type)",
            labels: &["instance", "address", "store", "type"],
            quantile: 0.0,
            comment: "The leader movement details among TiKV instances",
        },
    ),
    (
        "pd_scheduler_balance_region",
        MetricTableDef {
            prom_ql: "sum(delta(pd_scheduler_balance_region{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store,instance,type)",
            labels: &["instance", "address", "store", "type"],
            quantile: 0.0,
            comment: "The Region movement details among TiKV instances",
        },
    ),
    (
        "pd_scheduler_config",
        MetricTableDef {
            prom_ql: "pd_config_status{$LABEL_CONDITIONS}",
            labels: &["type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_scheduler_op_influence",
        MetricTableDef {
            prom_ql: "pd_scheduler_op_influence{$LABEL_CONDITIONS}",
            labels: &["instance", "scheduler", "store", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_scheduler_region_heartbeat",
        MetricTableDef {
            prom_ql: "sum(rate(pd_scheduler_region_heartbeat{$LABEL_CONDITIONS}[$RANGE_DURATION])*60) by (address,instance, store, status,type)",
            labels: &["instance", "address", "status", "store", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_scheduler_status",
        MetricTableDef {
            prom_ql: "pd_scheduler_status{$LABEL_CONDITIONS}",
            labels: &["instance", "kind", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_scheduler_store_status",
        MetricTableDef {
            prom_ql: "pd_scheduler_store_status{$LABEL_CONDITIONS}",
            labels: &["instance", "address", "store", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_scheduler_tolerant_resource",
        MetricTableDef {
            prom_ql: "pd_scheduler_tolerant_resource{$LABEL_CONDITIONS}",
            labels: &["instance", "scheduler", "source", "target"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "pd_server_etcd_state",
        MetricTableDef {
            prom_ql: "pd_server_etcd_state{$LABEL_CONDITIONS}",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The current term of Raft",
        },
    ),
    (
        "pd_start_tso_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_pdclient_ts_future_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.999,
            comment: "The quantile duration of the waiting time for getting the start timestamp oracle",
        },
    ),
    (
        "pd_start_tso_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_pdclient_ts_future_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of the waiting for getting the start timestamp oracle",
        },
    ),
    (
        "pd_start_tso_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_pdclient_ts_future_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of duration of the waiting time for getting the start timestamp oracle",
        },
    ),
    (
        "pd_tso_rpc_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{type=\"tso\"}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.999,
            comment: "The quantile duration of a client sending TSO request until received the response.",
        },
    ),
    (
        "pd_tso_rpc_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_request_handle_requests_duration_seconds_count{type=\"tso\"}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of a client sending TSO request until received the response.",
        },
    ),
    (
        "pd_tso_rpc_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_request_handle_requests_duration_seconds_sum{type=\"tso\"}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of a client sending TSO request until received the response.",
        },
    ),
    (
        "pd_tso_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{type=\"wait\"}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.999,
            comment: "The quantile duration of a client starting to wait for the TS until received the TS result.",
        },
    ),
    (
        "pd_tso_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_cmd_handle_cmds_duration_seconds_count{type=\"wait\"}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of a client starting to wait for the TS until received the TS result.",
        },
    ),
    (
        "pd_tso_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(pd_client_cmd_handle_cmds_duration_seconds_sum{type=\"wait\"}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of a client starting to wait for the TS until received the TS result.",
        },
    ),
    (
        "process_cpu_usage",
        MetricTableDef {
            prom_ql: "rate(process_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "resource_manager_resource_unit",
        MetricTableDef {
            prom_ql: "sum(rate(resource_manager_resource_unit_read_request_unit_sum{type=~\"|tp\"}[$RANGE_DURATION])) + sum(rate(resource_manager_resource_unit_write_request_unit_sum{type=~\"|tp\"}[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "The Total RU consumption per second",
        },
    ),
    (
        "store_available_ratio",
        MetricTableDef {
            prom_ql: "sum(pd_scheduler_store_status{type=\"store_available\"}) by (address, store) / sum(pd_scheduler_store_status{type=\"store_capacity\"}) by (address, store)",
            labels: &["address", "store"],
            quantile: 0.0,
            comment: "It is equal to Store available capacity size over Store capacity size for each TiKV instance",
        },
    ),
    (
        "store_size_amplification",
        MetricTableDef {
            prom_ql: "sum(pd_scheduler_store_status{type=\"region_size\"}) by (address, store) / sum(pd_scheduler_store_status{type=\"store_used\"}) by (address, store) * 2^20",
            labels: &["address", "store"],
            quantile: 0.0,
            comment: "The size amplification, which is equal to Store Region size over Store used capacity size, of each TiKV instance",
        },
    ),
    (
        "tidb_auto_id_qps",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_autoid_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB auto id requests per second including single table/global auto id processing and single table auto id rebase processing",
        },
    ),
    (
        "tidb_auto_id_request_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_autoid_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
            labels: &["instance", "type"],
            quantile: 0.95,
            comment: "The quantile of TiDB auto id requests durations",
        },
    ),
    (
        "tidb_auto_id_request_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_autoid_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of TiDB auto id requests durations",
        },
    ),
    (
        "tidb_auto_id_request_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_autoid_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of TiDB auto id requests durations",
        },
    ),
    (
        "tidb_batch_client_pending_req_count",
        MetricTableDef {
            prom_ql: "sum(tidb_tikvclient_pending_batch_requests{$LABEL_CONDITIONS}) by (store,instance)",
            labels: &["instance", "store"],
            quantile: 0.0,
            comment: "kv storage batch requests in queue",
        },
    ),
    (
        "tidb_batch_client_unavailable_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_batch_client_unavailable_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of kv storage batch processing unvailable durations",
        },
    ),
    (
        "tidb_batch_client_unavailable_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_batch_client_unavailable_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of kv storage batch processing unvailable durations",
        },
    ),
    (
        "tidb_batch_client_unavailable_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_batch_client_unavailable_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of kv storage batch processing unvailable durations",
        },
    ),
    (
        "tidb_batch_client_wait_conn_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_batch_client_wait_connection_establish_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of batch client wait new connection establish durations",
        },
    ),
    (
        "tidb_batch_client_wait_conn_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_batch_client_wait_connection_establish_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of batch client wait new connection establish",
        },
    ),
    (
        "tidb_batch_client_wait_conn_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_batch_client_wait_connection_establish_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of batch client wait new connection establish",
        },
    ),
    (
        "tidb_batch_client_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_batch_wait_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of kv storage batch processing durations, the unit is nanosecond",
        },
    ),
    (
        "tidb_batch_client_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_batch_wait_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of kv storage batch processing durations",
        },
    ),
    (
        "tidb_batch_client_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_batch_wait_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of kv storage batch processing durations, the unit is nanosecond",
        },
    ),
    (
        "tidb_binlog_error_count",
        MetricTableDef {
            prom_ql: "tidb_server_critical_error_total{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB write binlog error, skip binlog count",
        },
    ),
    (
        "tidb_binlog_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB write binlog error and skip binlog",
        },
    ),
    (
        "tidb_compile_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_session_compile_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, sql_type,instance))",
            labels: &["instance", "sql_type"],
            quantile: 0.95,
            comment: "The quantile time cost of building the query plan(second)",
        },
    ),
    (
        "tidb_compile_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_compile_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total count of building the query plan(second)",
        },
    ),
    (
        "tidb_compile_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_compile_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total time of cost of building the query plan(second)",
        },
    ),
    (
        "tidb_connection_count",
        MetricTableDef {
            prom_ql: "tidb_server_connections{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB current connection counts",
        },
    ),
    (
        "tidb_connection_idle_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_server_conn_idle_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,in_txn,instance))",
            labels: &["instance", "in_txn"],
            quantile: 0.90,
            comment: "The quantile of TiDB connection idle durations(second)",
        },
    ),
    (
        "tidb_connection_idle_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_conn_idle_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (in_txn,instance)",
            labels: &["instance", "in_txn"],
            quantile: 0.0,
            comment: "The total count of TiDB connection idle",
        },
    ),
    (
        "tidb_connection_idle_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_conn_idle_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (in_txn,instance)",
            labels: &["instance", "in_txn"],
            quantile: 0.0,
            comment: "The total time of TiDB connection idle",
        },
    ),
    (
        "tidb_cop_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_request_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of kv storage coprocessor processing durations",
        },
    ),
    (
        "tidb_cop_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_cop_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of kv storage coprocessor processing durations",
        },
    ),
    (
        "tidb_cop_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_cop_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of kv storage coprocessor processing durations",
        },
    ),
    (
        "tidb_ddl_add_index_speed",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_ddl_add_index_total[$RANGE_DURATION])) by (type)",
            labels: &[],
            quantile: 0.0,
            comment: "TiDB add index speed",
        },
    ),
    (
        "tidb_ddl_batch_add_index_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_batch_add_idx_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, instance))",
            labels: &["instance", "type"],
            quantile: 0.95,
            comment: "The quantile of TiDB batch add index durations by histogram buckets",
        },
    ),
    (
        "tidb_ddl_batch_add_index_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_batch_add_idx_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of TiDB batch add index durations by histogram buckets",
        },
    ),
    (
        "tidb_ddl_batch_add_index_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_batch_add_idx_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of TiDB batch add index durations by histogram buckets",
        },
    ),
    (
        "tidb_ddl_deploy_syncer_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_deploy_syncer_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, result,instance))",
            labels: &["instance", "type", "result"],
            quantile: 0.95,
            comment: "The quantile of TiDB ddl schema syncer statistics, including init, start, watch, clear function call time cost",
        },
    ),
    (
        "tidb_ddl_deploy_syncer_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_deploy_syncer_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "The total count of TiDB ddl schema syncer statistics, including init, start, watch, clear function call",
        },
    ),
    (
        "tidb_ddl_deploy_syncer_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_deploy_syncer_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "The total time of TiDB ddl schema syncer statistics, including init, start, watch, clear function call time cost",
        },
    ),
    (
        "tidb_ddl_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_handle_job_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
            labels: &["instance", "type"],
            quantile: 0.95,
            comment: "The quantile of TiDB DDL duration statistics",
        },
    ),
    (
        "tidb_ddl_meta_opm",
        MetricTableDef {
            prom_ql: "increase(tidb_ddl_worker_operation_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB different ddl worker numbers",
        },
    ),
    (
        "tidb_ddl_opm",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_ddl_handle_job_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The quantile of executed DDL jobs per minute",
        },
    ),
    (
        "tidb_ddl_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_handle_job_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of TiDB DDL duration statistics",
        },
    ),
    (
        "tidb_ddl_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_handle_job_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of TiDB DDL duration statistics",
        },
    ),
    (
        "tidb_ddl_update_self_version_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_update_self_ver_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, result,instance))",
            labels: &["instance", "result"],
            quantile: 0.95,
            comment: "The quantile of TiDB schema syncer version update time duration",
        },
    ),
    (
        "tidb_ddl_update_self_version_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_update_self_ver_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
            labels: &["instance", "result"],
            quantile: 0.0,
            comment: "The total count of TiDB schema syncer version update",
        },
    ),
    (
        "tidb_ddl_update_self_version_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_update_self_ver_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
            labels: &["instance", "result"],
            quantile: 0.0,
            comment: "The total time of TiDB schema syncer version update time duration",
        },
    ),
    (
        "tidb_ddl_waiting_jobs_num",
        MetricTableDef {
            prom_ql: "tidb_ddl_waiting_jobs{$LABEL_CONDITIONS}",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB ddl request in queue",
        },
    ),
    (
        "tidb_ddl_worker_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(increase(tidb_ddl_worker_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, action, result,instance))",
            labels: &["instance", "type", "result", "action"],
            quantile: 0.95,
            comment: "The quantile of TiDB ddl worker duration",
        },
    ),
    (
        "tidb_ddl_worker_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_worker_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result,action)",
            labels: &["instance", "type", "result", "action"],
            quantile: 0.0,
            comment: "The total count of TiDB ddl worker duration",
        },
    ),
    (
        "tidb_ddl_worker_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_worker_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result,action)",
            labels: &["instance", "type", "result", "action"],
            quantile: 0.0,
            comment: "The total time of TiDB ddl worker duration",
        },
    ),
    (
        "tidb_distsql_copr_cache",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_distsql_copr_cache{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of TiDB distsql coprocessor cache",
        },
    ),
    (
        "tidb_distsql_execution_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, instance))",
            labels: &["instance", "type"],
            quantile: 0.95,
            comment: "The quantile durations of distsql execution(second)",
        },
    ),
    (
        "tidb_distsql_execution_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_distsql_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of distsql execution(second)",
        },
    ),
    (
        "tidb_distsql_execution_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_distsql_handle_query_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of distsql execution(second)",
        },
    ),
    (
        "tidb_distsql_partial_num",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_partial_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of distsql partial numbers per query",
        },
    ),
    (
        "tidb_distsql_partial_num_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_distsql_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of distsql partial numbers per query",
        },
    ),
    (
        "tidb_distsql_partial_qps",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_distsql_scan_keys_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "the numebr of distsql partial scan numbers",
        },
    ),
    (
        "tidb_distsql_partial_scan_key_num",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_scan_keys_partial_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile numebr of distsql partial scan key numbers",
        },
    ),
    (
        "tidb_distsql_partial_scan_key_num_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_distsql_scan_keys_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of distsql partial scan key numbers",
        },
    ),
    (
        "tidb_distsql_partial_scan_key_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_distsql_scan_keys_partial_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total num of distsql partial scan key numbers",
        },
    ),
    (
        "tidb_distsql_partial_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_distsql_partial_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total num of distsql partial numbers per query",
        },
    ),
    (
        "tidb_distsql_qps",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_distsql_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "distsql query handling durations per second",
        },
    ),
    (
        "tidb_distsql_scan_key_num",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_scan_keys_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile numebr of distsql scan numbers",
        },
    ),
    (
        "tidb_distsql_scan_key_num_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_distsql_scan_keys_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of distsql scan numbers",
        },
    ),
    (
        "tidb_distsql_scan_key_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_distsql_scan_keys_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total num of distsql scan numbers",
        },
    ),
    (
        "tidb_event_opm",
        MetricTableDef {
            prom_ql: "increase(tidb_server_event_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB Server critical events total, including start/close/shutdown/hang etc",
        },
    ),
    (
        "tidb_execute_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_session_execute_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, sql_type, instance))",
            labels: &["instance", "sql_type"],
            quantile: 0.95,
            comment: "The quantile time cost of executing the SQL which does not include the time to get the results of the query(second)",
        },
    ),
    (
        "tidb_execute_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_execute_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total count of of TiDB executing the SQL",
        },
    ),
    (
        "tidb_execute_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_execute_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total time cost of executing the SQL which does not include the time to get the results of the query(second)",
        },
    ),
    (
        "tidb_expensive_executors_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_executor_expensive_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB executors using more cpu and memory resources",
        },
    ),
    (
        "tidb_failed_query_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_execute_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB failed query opm",
        },
    ),
    (
        "tidb_gc_action_result_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_gc_action_result{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "kv storage garbage collection results including failed and successful ones",
        },
    ),
    (
        "tidb_gc_config",
        MetricTableDef {
            prom_ql: "tidb_tikvclient_gc_config{$LABEL_CONDITIONS}",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "kv storage garbage collection config including gc_life_time and gc_run_interval",
        },
    ),
    (
        "tidb_gc_delete_range_fail_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_gc_unsafe_destroy_range_failures{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "kv storage unsafe destroy range failed counts",
        },
    ),
    (
        "tidb_gc_delete_range_task_status",
        MetricTableDef {
            prom_ql: "sum(tidb_tikvclient_range_task_stats{$LABEL_CONDITIONS}) by (type, result,instance)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "kv storage delete range task execution status by type",
        },
    ),
    (
        "tidb_gc_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_gc_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,stage))",
            labels: &["instance", "stage"],
            quantile: 0.95,
            comment: "The quantile of kv storage garbage collection time durations",
        },
    ),
    (
        "tidb_gc_fail_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_gc_failure{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "kv storage garbage collection failing counts",
        },
    ),
    (
        "tidb_gc_push_task_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_range_task_push_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.95,
            comment: "The quantile of kv storage range worker processing one task duration",
        },
    ),
    (
        "tidb_gc_push_task_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_range_task_push_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of kv storage range worker processing one task duration",
        },
    ),
    (
        "tidb_gc_push_task_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_range_task_push_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of kv storage range worker processing one task duration",
        },
    ),
    (
        "tidb_gc_too_many_locks_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_gc_region_too_many_locks[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "kv storage region garbage collection clean too many locks count",
        },
    ),
    (
        "tidb_gc_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_gc_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,stage)",
            labels: &["instance", "stage"],
            quantile: 0.0,
            comment: "The total count of kv storage garbage collection",
        },
    ),
    (
        "tidb_gc_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_gc_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,stage)",
            labels: &["instance", "stage"],
            quantile: 0.0,
            comment: "The total time of kv storage garbage collection time durations",
        },
    ),
    (
        "tidb_gc_worker_action_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_gc_worker_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "kv storage garbage collection counts by type",
        },
    ),
    (
        "tidb_get_token_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_server_get_token_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: " The quantile of Duration (us) for getting token, it should be small until concurrency limit is reached(microsecond)",
        },
    ),
    (
        "tidb_get_token_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_get_token_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of Duration (us) for getting token, it should be small until concurrency limit is reached",
        },
    ),
    (
        "tidb_get_token_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_get_token_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of Duration (us) for getting token, it should be small until concurrency limit is reached(microsecond)",
        },
    ),
    (
        "tidb_handshake_error_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_handshake_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The OPM of TiDB processing handshake error",
        },
    ),
    (
        "tidb_handshake_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_handshake_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB processing handshake error",
        },
    ),
    (
        "tidb_ia_remote_read_segment_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_ia_remote_read_segment_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of IA remote read segments observed by each TiDB instance",
        },
    ),
    (
        "tidb_ia_remote_read_segment_size",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_ia_remote_read_segment_size_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total bytes of IA remote read segments observed by each TiDB instance",
        },
    ),
    (
        "tidb_ia_remote_read_segment_wait_time_histogram",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_server_ia_remote_read_segment_wait_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,le)",
            labels: &["instance", "le"],
            quantile: 0.0,
            comment: "The histogram of IA remote read segment wait time observed by each TiDB instance",
        },
    ),
    (
        "tidb_keep_alive_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_monitor_keep_alive_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB instance monitor average keep alive times",
        },
    ),
    (
        "tidb_kv_backoff_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_backoff_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.95,
            comment: "The quantile of kv backoff time durations(second)",
        },
    ),
    (
        "tidb_kv_backoff_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_backoff_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "kv storage backoff times",
        },
    ),
    (
        "tidb_kv_backoff_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_backoff_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of kv backoff",
        },
    ),
    (
        "tidb_kv_backoff_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_backoff_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of kv backoff time durations(second)",
        },
    ),
    (
        "tidb_kv_region_error_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_region_err_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "kv region error times",
        },
    ),
    (
        "tidb_kv_region_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_region_err_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of kv region error",
        },
    ),
    (
        "tidb_kv_request_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_request_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,store,instance))",
            labels: &["instance", "type", "store"],
            quantile: 0.95,
            comment: "The quantile of kv requests durations by store",
        },
    ),
    (
        "tidb_kv_request_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_request_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "kv request total by instance and command type",
        },
    ),
    (
        "tidb_kv_request_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_request_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,store)",
            labels: &["instance", "type", "store"],
            quantile: 0.0,
            comment: "The total count of kv requests durations by store",
        },
    ),
    (
        "tidb_kv_request_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_request_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,store)",
            labels: &["instance", "type", "store"],
            quantile: 0.0,
            comment: "The total time of kv requests durations by store",
        },
    ),
    (
        "tidb_kv_snapshot_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_snapshot_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "using snapshots total",
        },
    ),
    (
        "tidb_kv_txn_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_txn_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB total kv transaction counts",
        },
    ),
    (
        "tidb_kv_write_num",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_write_kv_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 1.0,
            comment: "The quantile of kv write count per transaction execution",
        },
    ),
    (
        "tidb_kv_write_num_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_txn_write_kv_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of kv write in transaction execution",
        },
    ),
    (
        "tidb_kv_write_size",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_write_size_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 1.0,
            comment: "The quantile of kv write size per transaction execution",
        },
    ),
    (
        "tidb_kv_write_size_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_txn_write_size_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of kv write size per transaction execution",
        },
    ),
    (
        "tidb_kv_write_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_txn_write_kv_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total num of kv write in transaction execution",
        },
    ),
    (
        "tidb_kv_write_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_txn_write_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total kv write size in transaction execution",
        },
    ),
    (
        "tidb_load_privilege_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_domain_load_privilege_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB load privilege counts",
        },
    ),
    (
        "tidb_load_safepoint_fail_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "safe point update ops",
        },
    ),
    (
        "tidb_load_safepoint_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The OPS of load safe point loading",
        },
    ),
    (
        "tidb_load_safepoint_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of safe point loading",
        },
    ),
    (
        "tidb_load_schema_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_domain_load_schema_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile of TiDB loading schema time durations by instance",
        },
    ),
    (
        "tidb_load_schema_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_domain_load_schema_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB loading schema times including both failed and successful ones",
        },
    ),
    (
        "tidb_load_schema_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_domain_load_schema_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB loading schema by instance",
        },
    ),
    (
        "tidb_load_schema_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_domain_load_schema_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of TiDB loading schema time durations by instance",
        },
    ),
    (
        "tidb_lock_cleanup_fail_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_lock_cleanup_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "lock cleanup failed ops",
        },
    ),
    (
        "tidb_lock_resolver_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_lock_resolver_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "lock resolve times",
        },
    ),
    (
        "tidb_lock_resolver_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_lock_resolver_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total number of lock resolve",
        },
    ),
    (
        "tidb_meta_operation_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_meta_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,result,instance))",
            labels: &["instance", "type", "result"],
            quantile: 0.95,
            comment: "The quantile of TiDB meta operation durations including get/set schema and ddl jobs",
        },
    ),
    (
        "tidb_meta_operation_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_meta_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "The total count of TiDB meta operation durations including get/set schema and ddl jobs",
        },
    ),
    (
        "tidb_meta_operation_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_meta_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "The total time of TiDB meta operation durations including get/set schema and ddl jobs",
        },
    ),
    (
        "tidb_new_etcd_session_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_owner_new_session_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,result, instance))",
            labels: &["instance", "type", "result"],
            quantile: 0.95,
            comment: "The quantile of TiDB new session durations for new etcd sessions",
        },
    ),
    (
        "tidb_new_etcd_session_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_owner_new_session_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "The total count of TiDB new session durations for new etcd sessions",
        },
    ),
    (
        "tidb_new_etcd_session_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_owner_new_session_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "The total time of TiDB new session durations for new etcd sessions",
        },
    ),
    (
        "tidb_ops_internal",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_session_restricted_sql_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB internal SQL is used by TiDB itself.",
        },
    ),
    (
        "tidb_ops_statement",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_executor_statement_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB statement statistics",
        },
    ),
    (
        "tidb_owner_handle_syncer_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_owner_handle_syncer_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, result,instance))",
            labels: &["instance", "type", "result"],
            quantile: 0.95,
            comment: "The quantile of TiDB ddl owner time operations on etcd duration statistics ",
        },
    ),
    (
        "tidb_owner_handle_syncer_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_owner_handle_syncer_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "The total count of TiDB ddl owner operations on etcd ",
        },
    ),
    (
        "tidb_owner_handle_syncer_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_ddl_owner_handle_syncer_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "The total time of TiDB ddl owner time operations on etcd duration statistics ",
        },
    ),
    (
        "tidb_owner_watcher_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_owner_watch_owner_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, result, instance)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "TiDB owner watcher counts",
        },
    ),
    (
        "tidb_panic_count",
        MetricTableDef {
            prom_ql: "increase(tidb_server_panic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB instance panic count",
        },
    ),
    (
        "tidb_panic_count_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_panic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB instance panic",
        },
    ),
    (
        "tidb_parse_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_session_parse_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,sql_type,instance))",
            labels: &["instance", "sql_type"],
            quantile: 0.95,
            comment: "The quantile time cost of parsing SQL to AST(second)",
        },
    ),
    (
        "tidb_parse_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_parse_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total count of parsing SQL to AST(second)",
        },
    ),
    (
        "tidb_parse_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_parse_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total time cost of parsing SQL to AST(second)",
        },
    ),
    (
        "tidb_prepared_statement_count",
        MetricTableDef {
            prom_ql: "tidb_server_prepared_stmts{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB prepare statements count",
        },
    ),
    (
        "tidb_process_mem_usage",
        MetricTableDef {
            prom_ql: "process_resident_memory_bytes{$LABEL_CONDITIONS}",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "process rss memory usage",
        },
    ),
    (
        "tidb_qps",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_server_query_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (result,type,instance)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "TiDB query processing numbers per second",
        },
    ),
    (
        "tidb_qps_ideal",
        MetricTableDef {
            prom_ql: "sum(tidb_server_connections) * sum(rate(tidb_server_handle_query_duration_seconds_count[$RANGE_DURATION])) / sum(rate(tidb_server_handle_query_duration_seconds_sum[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tidb_query_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_server_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,sql_type,instance))",
            labels: &["instance", "sql_type"],
            quantile: 0.90,
            comment: "The quantile of TiDB query durations(second)",
        },
    ),
    (
        "tidb_query_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total count of TiDB query durations(second)",
        },
    ),
    (
        "tidb_query_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_handle_query_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total time of TiDB query durations(second)",
        },
    ),
    (
        "tidb_query_using_plan_cache_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_server_plan_cache_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB plan cache hit ops",
        },
    ),
    (
        "tidb_region_cache_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_tikvclient_region_cache_operations_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,result,instance)",
            labels: &["instance", "type", "result"],
            quantile: 0.0,
            comment: "TiDB region cache operations count",
        },
    ),
    (
        "tidb_schema_lease_error_opm",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_schema_lease_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB schema lease error counts",
        },
    ),
    (
        "tidb_schema_lease_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_schema_lease_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB schema lease error",
        },
    ),
    (
        "tidb_server_maxprocs",
        MetricTableDef {
            prom_ql: "tidb_server_maxprocs{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The Total CPU quota of each TiDB instance",
        },
    ),
    (
        "tidb_slow_query_cop_process_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_cop_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.90,
            comment: "The quantile of TiDB slow query statistics with slow query total cop process time(second)",
        },
    ),
    (
        "tidb_slow_query_cop_process_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_slow_query_cop_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB slow query cop process",
        },
    ),
    (
        "tidb_slow_query_cop_process_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_slow_query_cop_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of TiDB slow query statistics with slow query total cop process time(second)",
        },
    ),
    (
        "tidb_slow_query_cop_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_wait_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.90,
            comment: "The quantile of TiDB slow query statistics with slow query total cop wait time(second)",
        },
    ),
    (
        "tidb_slow_query_cop_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_slow_query_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB slow query cop wait",
        },
    ),
    (
        "tidb_slow_query_cop_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_slow_query_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of TiDB slow query statistics with slow query total cop wait time(second)",
        },
    ),
    (
        "tidb_slow_query_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_process_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.90,
            comment: "The quantile of TiDB slow query statistics with slow query time(second)",
        },
    ),
    (
        "tidb_slow_query_qps",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_server_slow_query_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "TiDB slow query processing numbers per second",
        },
    ),
    (
        "tidb_slow_query_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_slow_query_process_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB slow query",
        },
    ),
    (
        "tidb_slow_query_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_server_slow_query_process_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of TiDB slow query statistics with slow query time(second)",
        },
    ),
    (
        "tidb_statistics_auto_analyze_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_statistics_auto_analyze_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of TiDB auto analyze time durations within 95 percent histogram buckets",
        },
    ),
    (
        "tidb_statistics_auto_analyze_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_statistics_auto_analyze_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB auto analyze query per second",
        },
    ),
    (
        "tidb_statistics_auto_analyze_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_statistics_auto_analyze_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB auto analyze",
        },
    ),
    (
        "tidb_statistics_auto_analyze_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_statistics_auto_analyze_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of TiDB auto analyze time durations within 95 percent histogram buckets",
        },
    ),
    (
        "tidb_statistics_manual_analyze_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_statistics_manual_analyze_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB manual analyze query per second",
        },
    ),
    (
        "tidb_statistics_pseudo_estimation_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_statistics_pseudo_estimation_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB optimizer using pseudo estimation counts",
        },
    ),
    (
        "tidb_statistics_pseudo_estimation_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_statistics_pseudo_estimation_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB optimizer using pseudo estimation",
        },
    ),
    (
        "tidb_statistics_stats_inaccuracy_rate",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_statistics_stats_inaccuracy_rate_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of TiDB statistics inaccurate rate",
        },
    ),
    (
        "tidb_statistics_stats_inaccuracy_rate_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_statistics_stats_inaccuracy_rate_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB statistics inaccurate rate",
        },
    ),
    (
        "tidb_statistics_stats_inaccuracy_total_rate",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_statistics_stats_inaccuracy_rate_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of TiDB statistics inaccurate rate",
        },
    ),
    (
        "tidb_statistics_update_stats_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_statistics_update_stats_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "TiDB updating statistics using feed back counts",
        },
    ),
    (
        "tidb_statistics_update_stats_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_statistics_update_stats_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of TiDB updating statistics using feed back",
        },
    ),
    (
        "tidb_time_jump_back_ops",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_monitor_time_jump_back_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "TiDB monitor time jump back count",
        },
    ),
    (
        "tidb_transaction_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_session_transaction_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,sql_type,instance))",
            labels: &["instance", "type", "sql_type"],
            quantile: 0.95,
            comment: "The quantile of transaction execution durations, including retry(second)",
        },
    ),
    (
        "tidb_transaction_local_latch_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_local_latch_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of TiDB transaction latch wait time on key value storage(second)",
        },
    ),
    (
        "tidb_transaction_local_latch_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_local_latch_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB transaction latch wait on key value storage(second)",
        },
    ),
    (
        "tidb_transaction_local_latch_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_local_latch_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of TiDB transaction latch wait time on key value storage(second)",
        },
    ),
    (
        "tidb_transaction_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_session_transaction_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
            labels: &["instance", "type", "sql_type"],
            quantile: 0.0,
            comment: "TiDB transaction processing counts by type and source. Internal means TiDB inner transaction calls",
        },
    ),
    (
        "tidb_transaction_retry_error_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tidb_session_retry_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
            labels: &["instance", "type", "sql_type"],
            quantile: 0.0,
            comment: "Error numbers of transaction retry",
        },
    ),
    (
        "tidb_transaction_retry_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_retry_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
            labels: &["instance", "type", "sql_type"],
            quantile: 0.0,
            comment: "The total count of transaction retry",
        },
    ),
    (
        "tidb_transaction_retry_num",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_session_retry_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of TiDB transaction retry num",
        },
    ),
    (
        "tidb_transaction_retry_num_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_retry_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of TiDB transaction retry num",
        },
    ),
    (
        "tidb_transaction_retry_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_retry_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total num of TiDB transaction retry num",
        },
    ),
    (
        "tidb_transaction_statement_num",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_session_transaction_statement_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,sql_type))",
            labels: &["instance", "sql_type"],
            quantile: 0.95,
            comment: "The quantile of TiDB statements numbers within one transaction. Internal means TiDB inner transaction",
        },
    ),
    (
        "tidb_transaction_statement_num_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_transaction_statement_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total count of TiDB statements numbers within one transaction. Internal means TiDB inner transaction",
        },
    ),
    (
        "tidb_transaction_statement_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_transaction_statement_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
            labels: &["instance", "sql_type"],
            quantile: 0.0,
            comment: "The total num of TiDB statements numbers within one transaction. Internal means TiDB inner transaction",
        },
    ),
    (
        "tidb_transaction_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_transaction_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,sql_type)",
            labels: &["instance", "type", "sql_type"],
            quantile: 0.0,
            comment: "The total count of transaction execution durations, including retry(second)",
        },
    ),
    (
        "tidb_transaction_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_session_transaction_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,sql_type)",
            labels: &["instance", "type", "sql_type"],
            quantile: 0.0,
            comment: "The total time of transaction execution durations, including retry(second)",
        },
    ),
    (
        "tidb_txn_cmd_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_cmd_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.90,
            comment: "The quantile of TiDB transaction command durations(second)",
        },
    ),
    (
        "tidb_txn_cmd_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_txn_cmd_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of TiDB transaction command",
        },
    ),
    (
        "tidb_txn_cmd_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_txn_cmd_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of TiDB transaction command",
        },
    ),
    (
        "tidb_txn_region_num",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_regions_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 0.95,
            comment: "The quantile of regions transaction operates on count",
        },
    ),
    (
        "tidb_txn_region_num_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_txn_regions_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of regions transaction operates on count",
        },
    ),
    (
        "tidb_txn_region_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tidb_tikvclient_txn_regions_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total num of regions transaction operates on count",
        },
    ),
    (
        "tiflash_cpu_quota",
        MetricTableDef {
            prom_ql: "tiflash_system_current_metric_LogicalCPUCores{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tiflash_process_cpu_usage",
        MetricTableDef {
            prom_ql: "rate(tiflash_proxy_process_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tiflash_resource_manager_resource_unit",
        MetricTableDef {
            prom_ql: "sum(rate(tiflash_compute_request_unit[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_active_written_leaders",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_region_written_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The number of leaders being written on each TiKV instance",
        },
    ),
    (
        "tikv_admin_apply",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_admin_cmd_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,status,instance)",
            labels: &["instance", "type", "status"],
            quantile: 0.0,
            comment: "The number of the processed apply command",
        },
    ),
    (
        "tikv_allocator_stats",
        MetricTableDef {
            prom_ql: "tikv_allocator_stats{$LABEL_CONDITIONS}",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_apply_avg_wait_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_apply_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_apply_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_approximate_avg_region_size",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_region_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_region_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) ",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The avg approximate Region size",
        },
    ),
    (
        "tikv_approximate_region_size",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_region_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile of approximate Region size, the default value is P99",
        },
    ),
    (
        "tikv_approximate_region_size_histogram",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_region_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_approximate_region_size_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_region_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of approximate Region size, the default value is P99",
        },
    ),
    (
        "tikv_approximate_region_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_region_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total size of approximate Region size",
        },
    ),
    (
        "tikv_auto_gc_progress",
        MetricTableDef {
            prom_ql: "sum(tikv_gcworker_autogc_processed_regions{type=\"scan\"}) by (instance,type) / sum(tikv_raftstore_region_count{type=\"region\"}) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "Progress of TiKV's GC",
        },
    ),
    (
        "tikv_auto_gc_safepoint",
        MetricTableDef {
            prom_ql: "max(tikv_gcworker_autogc_safe_point{$LABEL_CONDITIONS}) by (instance) / (2^18)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "SafePoint used for TiKV's Auto GC",
        },
    ),
    (
        "tikv_auto_gc_working",
        MetricTableDef {
            prom_ql: "sum(max_over_time(tikv_gcworker_autogc_status{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,state)",
            labels: &["instance", "state"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_average_grpc_messge_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_grpc_msg_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(tikv_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_backup_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_backup_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_backup_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_backup_errors",
        MetricTableDef {
            prom_ql: "rate(tikv_backup_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "error"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_errors_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_backup_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,error)",
            labels: &["instance", "error"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_flow",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_backup_range_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_range_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_backup_range_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(tikv_backup_range_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_range_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_backup_range_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_backup_range_size",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_backup_range_size_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,cf,instance))",
            labels: &["instance", "cf"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_backup_range_size_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_backup_range_size_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,cf)",
            labels: &["instance", "cf"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_range_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_backup_range_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_range_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_backup_range_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,cf)",
            labels: &["instance", "cf"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_range_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_backup_range_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_backup_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_backup_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_backup_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_block_all_cache_hit",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_hit\"}[$RANGE_DURATION])) by (db,instance) / (sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_hit\"}[$RANGE_DURATION])) by (db,instance) + sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_miss\"}[$RANGE_DURATION])) by (db,instance))",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The hit rate of all block cache",
        },
    ),
    (
        "tikv_block_bloom_prefix_cache_hit",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_bloom_efficiency{type=\"bloom_prefix_useful\"}[$RANGE_DURATION])) by (db,instance) / sum(rate(tikv_engine_bloom_efficiency{type=\"bloom_prefix_checked\"}[$RANGE_DURATION])) by (db,instance)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The hit rate of data block cache",
        },
    ),
    (
        "tikv_block_cache_size",
        MetricTableDef {
            prom_ql: "topk(20, avg(tikv_engine_block_cache_size_bytes{$LABEL_CONDITIONS}) by(cf, instance, db))",
            labels: &["instance", "cf", "db"],
            quantile: 0.0,
            comment: "The block cache size. Broken down by column family if shared block cache is disabled.",
        },
    ),
    (
        "tikv_block_data_cache_hit",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_data_hit\"}[$RANGE_DURATION])) by (db,instance) / (sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_data_hit\"}[$RANGE_DURATION])) by (db,instance) + sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_data_miss\"}[$RANGE_DURATION])) by (db,instance))",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The hit rate of data block cache",
        },
    ),
    (
        "tikv_block_filter_cache_hit",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_filter_hit\"}[$RANGE_DURATION])) by (db,instance) / (sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_filter_hit\"}[$RANGE_DURATION])) by (db,instance) + sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_filter_miss\"}[$RANGE_DURATION])) by (db,instance))",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The hit rate of data block cache",
        },
    ),
    (
        "tikv_block_index_cache_hit",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_index_hit\"}[$RANGE_DURATION])) by (db,instance) / (sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_index_hit\"}[$RANGE_DURATION])) by (db,instance) + sum(rate(tikv_engine_cache_efficiency{type=\"block_cache_index_miss\"}[$RANGE_DURATION])) by (db,instance))",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The hit rate of data block cache",
        },
    ),
    (
        "tikv_channel_full",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_channel_full_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,db)",
            labels: &["instance", "db", "type"],
            quantile: 0.0,
            comment: "The ops of channel full errors on each TiKV instance, it will make the TiKV instance unavailable temporarily",
        },
    ),
    (
        "tikv_channel_full_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_channel_full_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,db)",
            labels: &["instance", "db", "type"],
            quantile: 0.0,
            comment: "The total number of channel full errors on each TiKV instance, it will make the TiKV instance unavailable temporarily",
        },
    ),
    (
        "tikv_check_split",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_check_split_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The number of raftstore split checks",
        },
    ),
    (
        "tikv_check_split_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_check_split_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 0.9999,
            comment: "The quantile of time consumed when running split check in .9999",
        },
    ),
    (
        "tikv_check_split_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_check_split_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of running split check",
        },
    ),
    (
        "tikv_check_split_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_check_split_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of time consumed when running split check in .9999",
        },
    ),
    (
        "tikv_client_task_progress",
        MetricTableDef {
            prom_ql: "max(tidb_tikvclient_range_task_stats{$LABEL_CONDITIONS}) by (result,type)",
            labels: &["result", "type"],
            quantile: 0.0,
            comment: "The progress of tikv client task",
        },
    ),
    (
        "tikv_compaction_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_compaction_time{$LABEL_CONDITIONS}) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time consumed when executing the compaction and flush operations",
        },
    ),
    (
        "tikv_compaction_max_duration",
        MetricTableDef {
            prom_ql: "max(tikv_engine_compaction_time{$LABEL_CONDITIONS}) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time consumed when executing the compaction and flush operations",
        },
    ),
    (
        "tikv_compaction_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_event_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The count of compaction and flush operations",
        },
    ),
    (
        "tikv_compaction_pending_bytes",
        MetricTableDef {
            prom_ql: "tikv_engine_pending_compaction_bytes{$LABEL_CONDITIONS}",
            labels: &["instance", "cf", "db"],
            quantile: 0.0,
            comment: "The pending bytes to be compacted",
        },
    ),
    (
        "tikv_compaction_reason",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_compaction_reason{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,cf,reason,instance)",
            labels: &["instance", "cf", "reason", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_compression_ratio",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_compression_ratio{$LABEL_CONDITIONS}) by (level,instance,db)",
            labels: &["instance", "level", "db"],
            quantile: 0.0,
            comment: "The compression ratio of each level",
        },
    ),
    (
        "tikv_config_raftstore",
        MetricTableDef {
            prom_ql: "tikv_config_raftstore{$LABEL_CONDITIONS}",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "TiKV rocksdb config value",
        },
    ),
    (
        "tikv_config_rocksdb",
        MetricTableDef {
            prom_ql: "tikv_config_rocksdb{$LABEL_CONDITIONS}",
            labels: &["instance", "cf", "name"],
            quantile: 0.0,
            comment: "TiKV rocksdb config value",
        },
    ),
    (
        "tikv_cop_dag_executors_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_executor_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The number of DAG executors per seconds",
        },
    ),
    (
        "tikv_cop_dag_requests_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_dag_request_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (vec_type,instance)",
            labels: &["instance", "vec_type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_handle_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_coprocessor_request_handle_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance))",
            labels: &["instance", "req"],
            quantile: 1.0,
            comment: "The quantile of time consumed when handling coprocessor requests",
        },
    ),
    (
        "tikv_cop_handle_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_request_handle_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "The total count of tikv coprocessor handling coprocessor requests",
        },
    ),
    (
        "tikv_cop_handle_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_request_handle_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "The total time of time consumed when handling coprocessor requests",
        },
    ),
    (
        "tikv_cop_kv_cursor_operations",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, avg(rate(tikv_coprocessor_scan_keys_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance)) ",
            labels: &["instance", "req"],
            quantile: 1.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_kv_cursor_operations_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_scan_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_request_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_coprocessor_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance))",
            labels: &["instance", "req"],
            quantile: 1.0,
            comment: "The quantile of time consumed to handle coprocessor read requests",
        },
    ),
    (
        "tikv_cop_request_durations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,req)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "The time consumed to handle coprocessor read requests",
        },
    ),
    (
        "tikv_cop_request_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "The total count of tikv handle coprocessor read requests",
        },
    ),
    (
        "tikv_cop_request_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "The total time of time consumed to handle coprocessor read requests",
        },
    ),
    (
        "tikv_cop_requests_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,instance)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_scan_details",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,req,cf,instance)",
            labels: &["instance", "tag", "req", "cf"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_scan_details_total",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,req,cf,instance)",
            labels: &["instance", "tag", "req", "cf"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_scan_keys_num",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_scan_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,instance)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_scan_keys_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_scan_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
            labels: &["instance", "req"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_total_response_size_per_seconds",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_response_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_total_response_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_response_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_total_rocksdb_perf_statistics",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_rocksdb_perf{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,metric,instance)",
            labels: &["instance", "req", "metric"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_cop_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_coprocessor_request_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,type,instance))",
            labels: &["instance", "req", "type"],
            quantile: 1.0,
            comment: "The quantile of time consumed when coprocessor requests are wait for being handled",
        },
    ),
    (
        "tikv_cop_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_request_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req,type)",
            labels: &["instance", "req", "type"],
            quantile: 0.0,
            comment: "The total count of coprocessor requests that wait for being handled",
        },
    ),
    (
        "tikv_cop_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_request_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req,type)",
            labels: &["instance", "req", "type"],
            quantile: 0.0,
            comment: "The total time of time consumed when coprocessor requests are wait for being handled",
        },
    ),
    (
        "tikv_coprocessor_is_busy",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_request_error{type='full'}[$RANGE_DURATION])) by (instance,db,type)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The ops of Coprocessor Full events that make the TiKV instance unavailable temporarily",
        },
    ),
    (
        "tikv_coprocessor_is_busy_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_request_error{type='full'}[$RANGE_DURATION])) by (instance,db,type)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The total count of Coprocessor Full events that make the TiKV instance unavailable temporarily",
        },
    ),
    (
        "tikv_coprocessor_request_error",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_coprocessor_request_error{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)",
            labels: &["instance", "reason"],
            quantile: 0.0,
            comment: "The number of different coprocessor errors on each TiKV instance",
        },
    ),
    (
        "tikv_coprocessor_request_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_coprocessor_request_error{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)",
            labels: &["instance", "reason"],
            quantile: 0.0,
            comment: "The total number of different coprocessor errors on each TiKV instance",
        },
    ),
    (
        "tikv_corrrput_keys_flow",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_compaction_num_corrupt_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,cf,instance)",
            labels: &["instance", "db", "cf"],
            quantile: 0.0,
            comment: "The flow of corrupt operations on keys",
        },
    ),
    (
        "tikv_cpu_quota",
        MetricTableDef {
            prom_ql: "tikv_server_cpu_cores_quota{$LABEL_CONDITIONS}",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The Total CPU quota of each TiKV instance",
        },
    ),
    (
        "tikv_critical_error",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The OPS of the TiKV critical error",
        },
    ),
    (
        "tikv_critical_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total number of the TiKV critical error",
        },
    ),
    (
        "tikv_disk_read_bytes",
        MetricTableDef {
            prom_ql: "sum(irate(node_disk_read_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_disk_write_bytes",
        MetricTableDef {
            prom_ql: "sum(irate(node_disk_written_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_avg_get_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_get_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The average time consumed when executing get operations, the unit is microsecond",
        },
    ),
    (
        "tikv_engine_avg_seek_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_seek_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time consumed when executing seek operation, the unit is microsecond",
        },
    ),
    (
        "tikv_engine_blob_bytes_flow",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_blob_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_file_count",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_titandb_num_obsolete_blob_file{$LABEL_CONDITIONS}) by (instance,db)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_file_read_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_blob_file_read_micros_seconds{$LABEL_CONDITIONS}) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "the unit is microsecond",
        },
    ),
    (
        "tikv_engine_blob_file_size",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_titandb_obsolete_blob_file_size{$LABEL_CONDITIONS}) by (instance,db)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_file_sync_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_blob_file_sync_micros_seconds{$LABEL_CONDITIONS}) by (instance,type,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "the unit is microsecond",
        },
    ),
    (
        "tikv_engine_blob_file_sync_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_blob_file_synced{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_file_write_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_blob_file_write_micros_seconds{$LABEL_CONDITIONS}) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "the unit is microsecond",
        },
    ),
    (
        "tikv_engine_blob_gc_bytes_flow",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_blob_gc_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_gc_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_blob_gc_micros_seconds{$LABEL_CONDITIONS}) by (db,instance,type)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_gc_file",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_blob_gc_file_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_gc_keys_flow",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_blob_gc_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_get_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_blob_get_micros_seconds{$LABEL_CONDITIONS}) by (type,db,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "the unit is microsecond",
        },
    ),
    (
        "tikv_engine_blob_key_avg_size",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_blob_key_size{$LABEL_CONDITIONS}) by (db,instance,type)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_key_max_size",
        MetricTableDef {
            prom_ql: "max(tikv_engine_blob_key_size{$LABEL_CONDITIONS}) by (db,instance,type)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_seek_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_blob_seek_micros_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "the unit is microsecond",
        },
    ),
    (
        "tikv_engine_blob_seek_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_blob_locate{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_value_avg_size",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_blob_value_size{$LABEL_CONDITIONS}) by (db,instance,type)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_blob_value_max_size",
        MetricTableDef {
            prom_ql: "max(tikv_engine_blob_value_size{$LABEL_CONDITIONS}) by (db,instance,type)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_compaction_flow_bytes",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_compaction_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The flow rate of compaction operations per type",
        },
    ),
    (
        "tikv_engine_get_block_cache_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_cache_efficiency{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The count of get memtable operations",
        },
    ),
    (
        "tikv_engine_get_cpu_cache_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_get_served{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The count of get l0,l1,l2 operations",
        },
    ),
    (
        "tikv_engine_get_memtable_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_memtable_efficiency{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The count of get memtable operations",
        },
    ),
    (
        "tikv_engine_live_blob_size",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_titandb_live_blob_size{$LABEL_CONDITIONS}) by (instance,db)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_engine_max_get_duration",
        MetricTableDef {
            prom_ql: "max(tikv_engine_get_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The max time consumed when executing get operations, the unit is microsecond",
        },
    ),
    (
        "tikv_engine_max_seek_duration",
        MetricTableDef {
            prom_ql: "max(tikv_engine_seek_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time consumed when executing seek operation, the unit is microsecond",
        },
    ),
    (
        "tikv_engine_seek_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_locate{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The count of seek operations",
        },
    ),
    (
        "tikv_engine_size",
        MetricTableDef {
            prom_ql: "sum(tikv_engine_size_bytes{$LABEL_CONDITIONS}) by (instance, type, db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The storage size per TiKV instance",
        },
    ),
    (
        "tikv_engine_wal_sync_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_wal_file_synced{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,instance)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The count of WAL sync operations",
        },
    ),
    (
        "tikv_engine_write_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_write_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time consumed when executing write operation, the unit is microsecond",
        },
    ),
    (
        "tikv_engine_write_max_duration",
        MetricTableDef {
            prom_ql: "max(tikv_engine_write_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time consumed when executing write operation, the unit is microsecond",
        },
    ),
    (
        "tikv_engine_write_operations",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_write_served{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The count of write operations",
        },
    ),
    (
        "tikv_engine_write_stall",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_write_stall{type=\"write_stall_percentile99\"}) by (instance, db)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "Indicates occurrences of Write Stall events that make the TiKV instance unavailable temporarily",
        },
    ),
    (
        "tikv_flow_mbps",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The total bytes of read and write in each TiKV instance",
        },
    ),
    (
        "tikv_flush_messages",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_server_raft_message_flush_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The number of Raft messages flushed by each TiKV instance",
        },
    ),
    (
        "tikv_flush_messages_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_raft_message_flush_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total number of Raft messages flushed by each TiKV instance",
        },
    ),
    (
        "tikv_futurepool_handled_tasks",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_futurepool_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "The number of tasks handled by future_pool",
        },
    ),
    (
        "tikv_futurepool_handled_tasks_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_futurepool_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "Total number of tasks handled by future_pool",
        },
    ),
    (
        "tikv_futurepool_pending_tasks",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_futurepool_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "Current pending and running tasks of future_pool",
        },
    ),
    (
        "tikv_futurepool_pending_tasks_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_futurepool_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "Total pending and running tasks of future_pool",
        },
    ),
    (
        "tikv_gc_fail_tasks",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_gcworker_gc_task_fail_vec{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)",
            labels: &["instance", "task"],
            quantile: 0.0,
            comment: "The count of GC tasks processed fail by gc_worker",
        },
    ),
    (
        "tikv_gc_keys",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_gcworker_gc_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,cf,instance)",
            labels: &["instance", "tag", "cf"],
            quantile: 0.0,
            comment: "The count of keys in write CF affected during GC",
        },
    ),
    (
        "tikv_gc_keys_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_gcworker_gc_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,cf,instance)",
            labels: &["instance", "tag", "cf"],
            quantile: 0.0,
            comment: "The total number of keys in write CF affected during GC",
        },
    ),
    (
        "tikv_gc_skipped_tasks",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_storage_gc_skipped_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)",
            labels: &["instance", "task"],
            quantile: 0.0,
            comment: "The count of GC skipped tasks processed by gc_worker",
        },
    ),
    (
        "tikv_gc_speed",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_storage_mvcc_gc_delete_versions_sum[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "The GC keys per second",
        },
    ),
    (
        "tikv_gc_tasks_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_gcworker_gc_task_duration_vec_sum{}[$RANGE_DURATION])) by (task,instance) / sum(rate(tikv_gcworker_gc_task_duration_vec_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)",
            labels: &["instance", "task"],
            quantile: 0.0,
            comment: "The time consumed when executing GC tasks",
        },
    ),
    (
        "tikv_gc_tasks_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_gcworker_gc_task_duration_vec_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,task,instance))",
            labels: &["instance", "task"],
            quantile: 1.0,
            comment: "The quantile of time consumed when executing GC tasks",
        },
    ),
    (
        "tikv_gc_tasks_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_gcworker_gc_tasks_vec{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)",
            labels: &["instance", "task"],
            quantile: 0.0,
            comment: "The count of GC total tasks processed by gc_worker per second",
        },
    ),
    (
        "tikv_gc_tasks_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_gcworker_gc_task_duration_vec_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,task)",
            labels: &["instance", "task"],
            quantile: 0.0,
            comment: "The total count of executing GC tasks",
        },
    ),
    (
        "tikv_gc_tasks_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_gcworker_gc_task_duration_vec_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,task)",
            labels: &["instance", "task"],
            quantile: 0.0,
            comment: "The total time of time consumed when executing GC tasks",
        },
    ),
    (
        "tikv_gc_too_busy",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_gc_worker_too_busy{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The count of GC worker too busy",
        },
    ),
    (
        "tikv_grpc_avg_req_batch_size",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_server_grpc_req_batch_size_sum[$RANGE_DURATION])) / sum(rate(tikv_server_grpc_req_batch_size_count[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_grpc_avg_resp_batch_size",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_server_grpc_resp_batch_size_sum[$RANGE_DURATION])) / sum(rate(tikv_server_grpc_resp_batch_size_count[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_grpc_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_grpc_msg_fail_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of the gRPC message failures",
        },
    ),
    (
        "tikv_grpc_errors",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_grpc_msg_fail_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The OPS of the gRPC message failures",
        },
    ),
    (
        "tikv_grpc_message_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_grpc_msg_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile execution time of gRPC message",
        },
    ),
    (
        "tikv_grpc_message_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of tikv execution gRPC message",
        },
    ),
    (
        "tikv_grpc_message_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_grpc_msg_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of execution time of gRPC message",
        },
    ),
    (
        "tikv_grpc_qps",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The QPS per command in each TiKV instance",
        },
    ),
    (
        "tikv_grpc_req_batch_size",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_server_grpc_req_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_grpc_req_batch_size_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_grpc_req_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_grpc_req_batch_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_grpc_req_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_grpc_resp_batch_size",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_server_grpc_resp_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_grpc_resp_batch_size_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_grpc_resp_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_grpc_resp_batch_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_grpc_resp_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_handle_snapshot_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_snapshot_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile of time consumed when handling snapshots",
        },
    ),
    (
        "tikv_handle_snapshot_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_snapshot_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of tikv handling snapshots",
        },
    ),
    (
        "tikv_handle_snapshot_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_snapshot_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of time consumed when handling snapshots",
        },
    ),
    (
        "tikv_ingest_sst_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_snapshot_ingest_sst_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_snapshot_ingest_sst_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The average time consumed when ingesting SST files",
        },
    ),
    (
        "tikv_ingest_sst_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_snapshot_ingest_sst_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,db))",
            labels: &["instance", "db"],
            quantile: 0.99,
            comment: "The quantile of time consumed when ingesting SST files",
        },
    ),
    (
        "tikv_ingest_sst_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_snapshot_ingest_sst_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,db)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The total count of ingesting SST files",
        },
    ),
    (
        "tikv_ingest_sst_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_snapshot_ingest_sst_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,db)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The total time of time consumed when ingesting SST files",
        },
    ),
    (
        "tikv_io_utilization",
        MetricTableDef {
            prom_ql: "rate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
            labels: &["instance", "device"],
            quantile: 0.0,
            comment: "The I/O utilization per TiKV instance",
        },
    ),
    (
        "tikv_leader_missing",
        MetricTableDef {
            prom_ql: "sum(tikv_raftstore_leader_missing{$LABEL_CONDITIONS}) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The count of missing leaders per TiKV instance",
        },
    ),
    (
        "tikv_local_reader_execute_requests",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_local_read_executed_requests{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The number of total requests from the local read thread",
        },
    ),
    (
        "tikv_local_reader_reject_requests",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_local_read_reject_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)",
            labels: &["instance", "reason"],
            quantile: 0.0,
            comment: "The number of rejections from the local read thread",
        },
    ),
    (
        "tikv_lock_manager_deadlock_detect_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_lock_manager_detect_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_lock_manager_detect_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_deadlock_detect_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_lock_manager_detect_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_deadlock_detect_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_lock_manager_detect_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_deadlock_detect_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_lock_manager_detect_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_deadlock_detector_leader",
        MetricTableDef {
            prom_ql: "sum(max_over_time(tikv_lock_manager_detector_leader_heartbeat{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_detect_error",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_lock_manager_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_detect_error_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_lock_manager_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_handled_tasks",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_lock_manager_task_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_wait_table",
        MetricTableDef {
            prom_ql: "sum(max_over_time(tikv_lock_manager_wait_table_status{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_waiter_lifetime_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_lock_manager_waiter_lifetime_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_lock_manager_waiter_lifetime_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_waiter_lifetime_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_lock_manager_waiter_lifetime_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_waiter_lifetime_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_lock_manager_waiter_lifetime_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_lock_manager_waiter_lifetime_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_lock_manager_waiter_lifetime_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_memory",
        MetricTableDef {
            prom_ql: "avg(process_resident_memory_bytes{$LABEL_CONDITIONS}) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The memory usage per TiKV instance",
        },
    ),
    (
        "tikv_memtable_hit",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_memtable_efficiency{type=\"memtable_hit\"}[$RANGE_DURATION])) by (instance,db) / (sum(rate(tikv_engine_memtable_efficiency{}[$RANGE_DURATION])) by (instance,db) + sum(rate(tikv_engine_memtable_efficiency{}[$RANGE_DURATION])) by (instance,db))",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The hit rate of memtable",
        },
    ),
    (
        "tikv_memtable_size",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_memory_bytes{$LABEL_CONDITIONS}) by (type,instance,db,cf)",
            labels: &["instance", "cf", "type", "db"],
            quantile: 0.0,
            comment: "The memtable size of each column family",
        },
    ),
    (
        "tikv_mvcc_delete_versions",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_storage_mvcc_gc_delete_versions_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The number of versions deleted by GC for each key",
        },
    ),
    (
        "tikv_mvcc_versions",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_storage_mvcc_versions_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The number of versions for each key",
        },
    ),
    (
        "tikv_number_files_at_each_level",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_num_files_at_level{$LABEL_CONDITIONS}) by (cf, level,db,instance)",
            labels: &["instance", "cf", "level", "db"],
            quantile: 0.0,
            comment: "The number of SST files for different column families in each level",
        },
    ),
    (
        "tikv_number_of_snapshots",
        MetricTableDef {
            prom_ql: "tikv_engine_num_snapshots{$LABEL_CONDITIONS}",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The number of snapshot of each TiKV instance",
        },
    ),
    (
        "tikv_oldest_snapshots_duration",
        MetricTableDef {
            prom_ql: "tikv_engine_oldest_snapshot_duration{$LABEL_CONDITIONS}",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The time that the oldest unreleased snapshot survivals",
        },
    ),
    (
        "tikv_pd_heartbeat",
        MetricTableDef {
            prom_ql: "sum(delta(tikv_pd_heartbeat_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total number of the gRPC message failures",
        },
    ),
    (
        "tikv_pd_heartbeats",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_pd_heartbeat_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: " The total number of PD heartbeat messages",
        },
    ),
    (
        "tikv_pd_request_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_pd_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance) / sum(rate(tikv_pd_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The time consumed by requests that TiKV sends to PD",
        },
    ),
    (
        "tikv_pd_request_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_pd_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_pd_request_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_pd_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The OPS of requests that TiKV sends to PD",
        },
    ),
    (
        "tikv_pd_request_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_pd_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The count of requests that TiKV sends to PD",
        },
    ),
    (
        "tikv_pd_request_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_pd_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The count of requests that TiKV sends to PD",
        },
    ),
    (
        "tikv_pd_validate_peers",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_pd_validate_peer_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total number of peers validated by the PD worker",
        },
    ),
    (
        "tikv_per_read_avg_bytes",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_bytes_per_read{$LABEL_CONDITIONS}) by (type,db,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The avg bytes per read",
        },
    ),
    (
        "tikv_per_read_max_bytes",
        MetricTableDef {
            prom_ql: "max(tikv_engine_bytes_per_read{$LABEL_CONDITIONS}) by (type,db,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The max bytes per read",
        },
    ),
    (
        "tikv_per_write_avg_bytes",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_bytes_per_write{$LABEL_CONDITIONS}) by (type,db,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The avg bytes per write",
        },
    ),
    (
        "tikv_per_write_max_bytes",
        MetricTableDef {
            prom_ql: "max(tikv_engine_bytes_per_write{$LABEL_CONDITIONS}) by (type,db,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The max bytes per write",
        },
    ),
    (
        "tikv_propose_avg_wait_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_request_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_request_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The average wait time of each proposal",
        },
    ),
    (
        "tikv_raft_dropped_messages",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_raft_dropped_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The number of dropped Raft messages per type",
        },
    ),
    (
        "tikv_raft_dropped_messages_total",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_raft_dropped_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total number of dropped Raft messages per type",
        },
    ),
    (
        "tikv_raft_log_speed",
        MetricTableDef {
            prom_ql: "avg(rate(tikv_raftstore_propose_log_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The rate at which peers propose logs",
        },
    ),
    (
        "tikv_raft_message_avg_batch_size",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_server_raft_message_batch_size_sum[$RANGE_DURATION])) / sum(rate(tikv_server_raft_message_batch_size_count[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_raft_message_batch_size",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_server_raft_message_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_raft_message_batch_size_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_raft_message_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_raft_message_batch_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_raft_message_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_raft_proposals",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_proposal_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The number of proposals per type in raft",
        },
    ),
    (
        "tikv_raft_proposals_per_ready",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_apply_proposal_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile proposal count of all Regions in a mio tick",
        },
    ),
    (
        "tikv_raft_proposals_per_ready_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_apply_proposal_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of proposal count of all Regions in a mio tick",
        },
    ),
    (
        "tikv_raft_proposals_per_total_ready",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_apply_proposal_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total proposal count of all Regions in a mio tick",
        },
    ),
    (
        "tikv_raft_proposals_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_proposal_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total number of proposals per type in raft",
        },
    ),
    (
        "tikv_raft_sent_messages",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_raft_sent_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The number of Raft messages sent by each TiKV instance",
        },
    ),
    (
        "tikv_raft_sent_messages_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_raft_sent_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total number of Raft messages sent by each TiKV instance",
        },
    ),
    (
        "tikv_raft_store_events_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_event_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile time consumed by raftstore events (P99).99",
        },
    ),
    (
        "tikv_raft_store_events_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_event_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of raftstore events (P99).99",
        },
    ),
    (
        "tikv_raft_store_events_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_event_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of raftstore events (P99).99",
        },
    ),
    (
        "tikv_raftstore_append_log_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_append_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_append_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The avg time consumed when Raft appends log",
        },
    ),
    (
        "tikv_raftstore_append_log_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_append_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile time consumed when Raft appends log",
        },
    ),
    (
        "tikv_raftstore_append_log_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_append_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of Raft appends log",
        },
    ),
    (
        "tikv_raftstore_append_log_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_append_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of Raft appends log",
        },
    ),
    (
        "tikv_raftstore_apply_log_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_apply_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_apply_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) ",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The average time consumed when Raft applies log",
        },
    ),
    (
        "tikv_raftstore_apply_log_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_apply_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile time consumed when Raft applies log",
        },
    ),
    (
        "tikv_raftstore_apply_log_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_apply_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of Raft applies log",
        },
    ),
    (
        "tikv_raftstore_apply_log_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_apply_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of Raft applies log",
        },
    ),
    (
        "tikv_raftstore_apply_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_apply_wait_time_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "",
        },
    ),
    (
        "tikv_raftstore_apply_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_apply_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_raftstore_apply_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_apply_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_raftstore_commit_log_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_commit_log_duration_seconds_sum[$RANGE_DURATION])) / sum(rate(tikv_raftstore_commit_log_duration_seconds_count[$RANGE_DURATION]))",
            labels: &[],
            quantile: 0.0,
            comment: "The time consumed when Raft commits log",
        },
    ),
    (
        "tikv_raftstore_commit_log_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_commit_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile time consumed when Raft commits log",
        },
    ),
    (
        "tikv_raftstore_commit_log_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_commit_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of Raft commits log",
        },
    ),
    (
        "tikv_raftstore_commit_log_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_commit_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of Raft commits log",
        },
    ),
    (
        "tikv_raftstore_process_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_raft_process_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile time consumed for peer processes in Raft",
        },
    ),
    (
        "tikv_raftstore_process_handled",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_raft_process_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The count of different process type of Raft",
        },
    ),
    (
        "tikv_raftstore_process_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_raft_process_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of peer processes in Raft",
        },
    ),
    (
        "tikv_raftstore_process_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_raft_process_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of peer processes in Raft",
        },
    ),
    (
        "tikv_raftstore_propose_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_request_wait_time_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile wait time of each proposal",
        },
    ),
    (
        "tikv_raftstore_propose_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_request_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of each proposal",
        },
    ),
    (
        "tikv_raftstore_propose_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_raftstore_request_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of wait time of each proposal",
        },
    ),
    (
        "tikv_read_amplication",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_engine_read_amp_flow_bytes{type=\"read_amp_total_read_bytes\"}[$RANGE_DURATION])) by (instance,db) / sum(rate(tikv_engine_read_amp_flow_bytes{type=\"read_amp_estimate_useful_bytes\"}[$RANGE_DURATION])) by (instance,db)",
            labels: &["instance", "db"],
            quantile: 0.0,
            comment: "The read amplification per TiKV instance",
        },
    ),
    (
        "tikv_ready_handled",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_raftstore_raft_ready_handled_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The count of ready handled of Raft",
        },
    ),
    (
        "tikv_receive_messages",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_server_raft_message_recv_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The number of Raft messages received by each TiKV instance",
        },
    ),
    (
        "tikv_receive_messages_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_raft_message_recv_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total number of Raft messages received by each TiKV instance",
        },
    ),
    (
        "tikv_region_average_written_bytes",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_region_written_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance) / sum(rate(tikv_region_written_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The average rate of writing bytes to Regions per TiKV instance",
        },
    ),
    (
        "tikv_region_average_written_keys",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_region_written_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance) / sum(rate(tikv_region_written_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The average rate of written keys to Regions per TiKV instance",
        },
    ),
    (
        "tikv_region_change",
        MetricTableDef {
            prom_ql: "sum(delta(tikv_raftstore_region_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The count of region change per TiKV instance",
        },
    ),
    (
        "tikv_region_count",
        MetricTableDef {
            prom_ql: "sum(tikv_raftstore_region_count{$LABEL_CONDITIONS}) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The number of regions on each TiKV instance",
        },
    ),
    (
        "tikv_region_written_bytes",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_region_written_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_region_written_keys",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_region_written_keys_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_request_batch_avg",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_server_request_batch_ratio_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(tikv_server_request_batch_ratio_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The ratio of request batch output to input per TiKV instance",
        },
    ),
    (
        "tikv_request_batch_ratio",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_server_request_batch_ratio_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile ratio of request batch output to input per TiKV instance",
        },
    ),
    (
        "tikv_request_batch_ratio_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_request_batch_ratio_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of request batch output to input per TiKV instance",
        },
    ),
    (
        "tikv_request_batch_size",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_server_request_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile size of requests into request batch per TiKV instance",
        },
    ),
    (
        "tikv_request_batch_size_avg",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_server_request_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(tikv_server_request_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The avg size of requests into request batch per TiKV instance",
        },
    ),
    (
        "tikv_request_batch_size_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_request_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of request batch per TiKV instance",
        },
    ),
    (
        "tikv_request_batch_total_ratio",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_request_batch_ratio_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total ratio of request batch output to input per TiKV instance",
        },
    ),
    (
        "tikv_request_batch_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_request_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total size of requests into request batch per TiKV instance",
        },
    ),
    (
        "tikv_scheduler_command_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_scheduler_command_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_scheduler_command_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) ",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The average time consumed when executing command",
        },
    ),
    (
        "tikv_scheduler_command_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_command_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile of time consumed when executing command",
        },
    ),
    (
        "tikv_scheduler_command_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_command_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of tikv scheduler executing command",
        },
    ),
    (
        "tikv_scheduler_command_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_command_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of time consumed when executing command",
        },
    ),
    (
        "tikv_scheduler_is_busy",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_scheduler_too_busy_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,db,type,stage)",
            labels: &["instance", "db", "type", "stage"],
            quantile: 0.0,
            comment: "Indicates occurrences of Scheduler Busy events that make the TiKV instance unavailable temporarily",
        },
    ),
    (
        "tikv_scheduler_is_busy_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_too_busy_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,db,type,stage)",
            labels: &["instance", "db", "type", "stage"],
            quantile: 0.0,
            comment: "The total count of Scheduler Busy events that make the TiKV instance unavailable temporarily",
        },
    ),
    (
        "tikv_scheduler_keys_read",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_kv_command_key_read_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile count of keys read by command",
        },
    ),
    (
        "tikv_scheduler_keys_read_avg",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_scheduler_kv_command_key_read_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_scheduler_kv_command_key_read_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) ",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The average count of keys read by command",
        },
    ),
    (
        "tikv_scheduler_keys_read_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_kv_command_key_read_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of keys read by a command",
        },
    ),
    (
        "tikv_scheduler_keys_total_read",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_kv_command_key_read_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of keys read by command",
        },
    ),
    (
        "tikv_scheduler_keys_total_written",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_kv_command_key_write_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of keys written by a command",
        },
    ),
    (
        "tikv_scheduler_keys_written",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_kv_command_key_write_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile of count of keys written by a command",
        },
    ),
    (
        "tikv_scheduler_keys_written_avg",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_scheduler_kv_command_key_write_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_scheduler_kv_command_key_write_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) ",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The average count of keys written by a command",
        },
    ),
    (
        "tikv_scheduler_keys_written_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_kv_command_key_write_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of keys written by a command",
        },
    ),
    (
        "tikv_scheduler_latch_wait_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_scheduler_latch_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_scheduler_latch_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) ",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The average time which is caused by latch wait in command",
        },
    ),
    (
        "tikv_scheduler_latch_wait_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_latch_wait_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile time which is caused by latch wait in command",
        },
    ),
    (
        "tikv_scheduler_latch_wait_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_latch_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count which is caused by latch wait in command",
        },
    ),
    (
        "tikv_scheduler_latch_wait_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_latch_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time which is caused by latch wait in command",
        },
    ),
    (
        "tikv_scheduler_pending_commands",
        MetricTableDef {
            prom_ql: "sum(tikv_scheduler_contex_total{$LABEL_CONDITIONS}) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The count of pending commands per TiKV instance",
        },
    ),
    (
        "tikv_scheduler_priority_commands",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_scheduler_commands_pri_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (priority,instance)",
            labels: &["instance", "priority"],
            quantile: 0.0,
            comment: "The count of different priority commands",
        },
    ),
    (
        "tikv_scheduler_processing_read_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_processing_read_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile time of scheduler processing read in command",
        },
    ),
    (
        "tikv_scheduler_processing_read_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_processing_read_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of scheduler processing read in command",
        },
    ),
    (
        "tikv_scheduler_processing_read_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_processing_read_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of scheduler processing read in command",
        },
    ),
    (
        "tikv_scheduler_scan_details",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_scheduler_kv_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,instance,req,cf)",
            labels: &["instance", "tag", "req", "cf"],
            quantile: 0.0,
            comment: "The keys scan details of each CF when executing command",
        },
    ),
    (
        "tikv_scheduler_scan_details_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_kv_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,instance,req,cf)",
            labels: &["instance", "tag", "req", "cf"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_scheduler_stage",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_scheduler_stage_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, stage,type)",
            labels: &["instance", "stage", "type"],
            quantile: 0.0,
            comment: "The number of scheduler state on each TiKV instance",
        },
    ),
    (
        "tikv_scheduler_stage_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_scheduler_stage_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, stage,type)",
            labels: &["instance", "stage", "type"],
            quantile: 0.0,
            comment: "The total number of scheduler state on each TiKV instance",
        },
    ),
    (
        "tikv_scheduler_writing_bytes",
        MetricTableDef {
            prom_ql: "sum(tikv_scheduler_writing_bytes{$LABEL_CONDITIONS}) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total writing bytes of commands on each stage",
        },
    ),
    (
        "tikv_send_snapshot_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_server_send_snapshot_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.99,
            comment: "The quantile of time consumed when sending snapshots",
        },
    ),
    (
        "tikv_send_snapshot_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_send_snapshot_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of sending snapshots",
        },
    ),
    (
        "tikv_send_snapshot_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_send_snapshot_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total time of time consumed when sending snapshots",
        },
    ),
    (
        "tikv_server_report_failures",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_server_report_failure_msg_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,store_id)",
            labels: &["instance", "store_id", "type"],
            quantile: 0.0,
            comment: "The total number of reported failure messages",
        },
    ),
    (
        "tikv_server_report_failures_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_server_report_failure_msg_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,store_id)",
            labels: &["instance", "store_id", "type"],
            quantile: 0.0,
            comment: "The total number of reported failure messages",
        },
    ),
    (
        "tikv_snapshot_kv_count",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_snapshot_kv_count_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.9999,
            comment: "The quantile of number of KV within a snapshot",
        },
    ),
    (
        "tikv_snapshot_kv_count_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_snapshot_kv_count_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of number of KV within a snapshot",
        },
    ),
    (
        "tikv_snapshot_kv_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_snapshot_kv_count_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total number of KV within a snapshot",
        },
    ),
    (
        "tikv_snapshot_size",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_snapshot_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
            labels: &["instance"],
            quantile: 0.9999,
            comment: "The quantile of snapshot size",
        },
    ),
    (
        "tikv_snapshot_size_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_snapshot_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total count of snapshot size",
        },
    ),
    (
        "tikv_snapshot_state_count",
        MetricTableDef {
            prom_ql: "sum(tikv_raftstore_snapshot_traffic_total{$LABEL_CONDITIONS}) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The number of snapshots in different states",
        },
    ),
    (
        "tikv_snapshot_state_total_count",
        MetricTableDef {
            prom_ql: "sum(delta(tikv_raftstore_snapshot_traffic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total number of snapshots in different states",
        },
    ),
    (
        "tikv_snapshot_total_size",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_snapshot_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
            labels: &["instance"],
            quantile: 0.0,
            comment: "The total size of snapshot size",
        },
    ),
    (
        "tikv_sst_read_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_sst_read_micros{$LABEL_CONDITIONS}) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time consumed when reading SST files",
        },
    ),
    (
        "tikv_sst_read_max_duration",
        MetricTableDef {
            prom_ql: "max(tikv_engine_sst_read_micros{$LABEL_CONDITIONS}) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The max time consumed when reading SST files",
        },
    ),
    (
        "tikv_stall_conditions_changed_of_each_cf",
        MetricTableDef {
            prom_ql: "tikv_engine_stall_conditions_changed{$LABEL_CONDITIONS}",
            labels: &["instance", "cf", "type", "db"],
            quantile: 0.0,
            comment: "Stall conditions changed of each column family",
        },
    ),
    (
        "tikv_storage_async_request_avg_duration",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_storage_engine_async_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_storage_engine_async_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The time consumed by processing asynchronous snapshot requests",
        },
    ),
    (
        "tikv_storage_async_request_duration",
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(tikv_storage_engine_async_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))",
            labels: &["instance", "type"],
            quantile: 0.99,
            comment: "The quantile of time consumed by processing asynchronous snapshot requests",
        },
    ),
    (
        "tikv_storage_async_request_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_storage_engine_async_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of processing asynchronous snapshot requests",
        },
    ),
    (
        "tikv_storage_async_request_total_time",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_storage_engine_async_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total time of time consumed by processing asynchronous snapshot requests",
        },
    ),
    (
        "tikv_storage_async_requests",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_storage_engine_async_request_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, status, type)",
            labels: &["instance", "status", "type"],
            quantile: 0.0,
            comment: "The number of different raftstore errors on each TiKV instance",
        },
    ),
    (
        "tikv_storage_async_requests_total_count",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_storage_engine_async_request_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, status, type)",
            labels: &["instance", "status", "type"],
            quantile: 0.0,
            comment: "The total number of different raftstore errors on each TiKV instance",
        },
    ),
    (
        "tikv_storage_command_ops",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_storage_command_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The total count of different kinds of commands received per seconds",
        },
    ),
    (
        "tikv_store_size",
        MetricTableDef {
            prom_ql: "sum(tikv_store_size_bytes{$LABEL_CONDITIONS}) by (instance,type)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The available or capacity size of each TiKV instance",
        },
    ),
    (
        "tikv_thread_cpu",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_thread_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,name)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "The CPU usage of each TiKV instance",
        },
    ),
    (
        "tikv_thread_nonvoluntary_context_switches",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_thread_nonvoluntary_context_switches{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, name)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_thread_voluntary_context_switches",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_thread_voluntary_context_switches{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, name)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_threads_io",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_threads_io_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,io,instance)",
            labels: &["instance", "io", "name"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_threads_state",
        MetricTableDef {
            prom_ql: "sum(tikv_threads_state{$LABEL_CONDITIONS}) by (instance,state)",
            labels: &["instance", "state"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "tikv_total_keys",
        MetricTableDef {
            prom_ql: "sum(tikv_engine_estimate_num_keys{$LABEL_CONDITIONS}) by (db,cf,instance)",
            labels: &["instance", "db", "cf"],
            quantile: 0.0,
            comment: "The count of keys in each column family",
        },
    ),
    (
        "tikv_wal_sync_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_wal_file_sync_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type"],
            quantile: 0.0,
            comment: "The time consumed when executing WAL sync operation, the unit is microsecond",
        },
    ),
    (
        "tikv_wal_sync_max_duration",
        MetricTableDef {
            prom_ql: "max(tikv_engine_wal_file_sync_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The max time consumed when executing WAL sync operation, the unit is microsecond",
        },
    ),
    (
        "tikv_worker_handled_tasks",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_worker_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "The number of tasks handled by worker",
        },
    ),
    (
        "tikv_worker_handled_tasks_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_worker_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "Total number of tasks handled by worker",
        },
    ),
    (
        "tikv_worker_pending_tasks",
        MetricTableDef {
            prom_ql: "sum(rate(tikv_worker_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "Current pending and running tasks of worker",
        },
    ),
    (
        "tikv_worker_pending_tasks_total_num",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_worker_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)",
            labels: &["instance", "name"],
            quantile: 0.0,
            comment: "Total pending and running tasks of worker",
        },
    ),
    (
        "tikv_write_stall_avg_duration",
        MetricTableDef {
            prom_ql: "avg(tikv_engine_write_stall{$LABEL_CONDITIONS}) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time which is caused by write stall",
        },
    ),
    (
        "tikv_write_stall_max_duration",
        MetricTableDef {
            prom_ql: "max(tikv_engine_write_stall{$LABEL_CONDITIONS}) by (type,instance,db)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "The time which is caused by write stall",
        },
    ),
    (
        "tikv_write_stall_reason",
        MetricTableDef {
            prom_ql: "sum(increase(tikv_engine_write_stall_reason{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)",
            labels: &["instance", "type", "db"],
            quantile: 0.0,
            comment: "",
        },
    ),
    (
        "up",
        MetricTableDef {
            prom_ql: "up{$LABEL_CONDITIONS}",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "whether the instance is up. 1 is up, 0 is down(off-line)",
        },
    ),
    (
        "uptime",
        MetricTableDef {
            prom_ql: "(time() - process_start_time_seconds{$LABEL_CONDITIONS})",
            labels: &["instance", "job"],
            quantile: 0.0,
            comment: "TiDB uptime since last restart(second)",
        },
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_map_is_sorted_for_lookup() {
        assert!(METRIC_TABLE_MAP
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0));
        assert_eq!(METRIC_TABLE_MAP.len(), 642);
    }

    #[test]
    fn gen_prom_ql_fills_every_placeholder_as_go_does() {
        let def = get_metric_table_def("tidb_query_duration").unwrap();
        let mut labels = BTreeMap::new();
        labels.insert(
            "instance".to_owned(),
            ["b:1".to_owned(), "a:1".to_owned()].into_iter().collect(),
        );
        labels.insert(
            "sql_type".to_owned(),
            std::iter::once("Select".to_owned()).collect(),
        );
        assert_eq!(
            def.gen_prom_ql(60, &labels, 0.9),
            "histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance=~\"a:1|b:1\",sql_type=\"Select\"}[60s])) by (le,sql_type,instance))"
        );
        assert_eq!(def.quantile_default(), "0.9");
        assert!(get_metric_table_def("TIDB_QUERY_DURATION").is_none());
    }
}
