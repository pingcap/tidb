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

//! Ports of Go `pkg/executor/memtable_reader_test.go` at the tier's
//! transport boundary: the CLUSTER CONFIG row production, the CLUSTER LOG
//! k-way merge, and their warning behavior, driven through mock
//! [`crate::memtable_reader::ClusterConfigSource`] / `LogStream`
//! implementations instead of Go's httptest/gRPC mock servers.
//!
//! Go's SQL surface (`select ... from information_schema.cluster_config /
//! cluster_log` with `MustQuery`) plus its time/level WHERE filtering are
//! extractor + SQL-layer behavior; the retriever/merger here is the layer
//! Go's tests actually exercise through the failpoint-injected servers.

use crate::memtable_reader::{
    format_log_time, ClusterConfigRetriever, CLUSTER_LOG_BATCH_SIZE, LogMessage, LogRowMerger,
    LogStream, MemTableRetriever, ServerInfo,
};
use tidb_datatype::Datum;

/// The three PD/TiKV/TiDB-like servers Go starts (`testServerCount = 3`).
fn three_servers(node_type: &str) -> Vec<ServerInfo> {
    (0..3)
        .map(|index| ServerInfo {
            server_type: node_type.to_owned(),
            address: format!("{node_type}-{index}:2379"),
            status_addr: format!("{node_type}-{index}:2379"),
        })
        .collect()
}

/// Go `TestTiDBClusterConfig`'s mock config, ALREADY flattened the way
/// `config.FlattenConfigItems` would deliver it: `key1`, `key2.nest1`,
/// `key2.nest2`, plus the performance/prepared-plan-cache subtrees Go hides.
fn flattened_mock_config() -> std::collections::BTreeMap<String, serde_json::Value> {
    let mut config = std::collections::BTreeMap::new();
    config.insert("key1".to_owned(), serde_json::json!("value1"));
    config.insert("key2.nest1".to_owned(), serde_json::json!("n-value1"));
    config.insert("key2.nest2".to_owned(), serde_json::json!("n-value2"));
    // "We need hide the follow config" (Go's comment): the hidden items the
    // Go mock carries and the assertions require to be ABSENT from rows.
    config.insert(
        "performance.index-usage-sync-lease".to_owned(),
        serde_json::json!("0s"),
    );
    config.insert(
        "performance.INDEX-USAGE-SYNC-LEASE".to_owned(),
        serde_json::json!("0s"),
    );
    config.insert("enable-batch-dml".to_owned(), serde_json::json!("false"));
    config.insert(
        "prepared-plan-cache.enabled".to_owned(),
        serde_json::json!("true"),
    );
    config
}

struct MockConfigSource;

impl crate::memtable_reader::ClusterConfigSource for MockConfigSource {
    fn flattened_config(
        &self,
        _server: &ServerInfo,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>, String> {
        Ok(flattened_mock_config())
    }

    fn is_hidden_config(&self, key: &str) -> bool {
        // Go `config.ContainHiddenConfig`: the case-insensitive
        // index-usage-sync-lease, deprecated enable-batch-dml and
        // prepared-plan-cache subtrees.
        let lower = key.to_lowercase();
        lower.contains("index-usage-sync-lease")
            || lower == "enable-batch-dml"
            || lower.starts_with("prepared-plan-cache.")
    }
}

/// Go `pkg/executor/memtable_reader_test.go:101::TestTiDBClusterConfig`,
/// at the [`ClusterConfigRetriever`] boundary: 7 node types x 3 servers (Go's
/// `servers` failpoint list), each answering the mock config, must produce
/// exactly Go's expected rows -- `(type, address, key, value)` for the three
/// visible keys per server, in server order, hidden keys absent, and NO
/// warnings (Go asserts `require.Len(t, warnings, 0)` and `requestCounter ==
/// 21`; here one pull per server is the 21 config fetches).
#[test]
fn cluster_config_retriever_rows_source() {
    // Go's type order in the failpoint expression.
    let node_types = ["tidb", "tikv", "tiflash", "tiproxy", "pd", "tso", "scheduling"];
    let mut servers = Vec::new();
    for node_type in node_types {
        servers.extend(three_servers(node_type));
    }

    let mut retriever = ClusterConfigRetriever::new(MockConfigSource, servers.clone(), false);
    let rows = retriever.retrieve().unwrap();
    assert!(retriever.warnings().is_empty(), "unexpected warnings {:?}", retriever.warnings());

    let mut expected = Vec::new();
    for server in &servers {
        for (key, value) in [("key1", "value1"), ("key2.nest1", "n-value1"), ("key2.nest2", "n-value2")] {
            expected.push(vec![
                Datum::String(tidb_datatype::StringDatum::new(
                    server.server_type.as_str(),
                    tidb_datatype::Collation::Utf8Mb4Bin,
                )),
                Datum::String(tidb_datatype::StringDatum::new(
                    server.address.as_str(),
                    tidb_datatype::Collation::Utf8Mb4Bin,
                )),
                Datum::String(tidb_datatype::StringDatum::new(
                    key,
                    tidb_datatype::Collation::Utf8Mb4Bin,
                )),
                Datum::String(tidb_datatype::StringDatum::new(
                    value,
                    tidb_datatype::Collation::Utf8Mb4Bin,
                )),
            ]);
        }
    }
    assert_eq!(rows, expected, "cluster_config rows must match Go's ordered expectation");

    // Retrieval is once-per-statement (Go's `retrieved` flag): a second pull
    // is empty and adds no warnings.
    assert!(retriever.retrieve().unwrap().is_empty());
    assert!(retriever.warnings().is_empty());
}

/// Go `pkg/executor/memtable_reader_test.go:497::TestTiDBClusterLog`, at the
/// [`LogRowMerger`] boundary: five node streams logging Go's messages must
/// merge ordered by (time, node type) -- Go's `fullLogs` matrix, including
/// the same-millisecond ties ordered `pd < ticdc < tidb < tikv < tiproxy` --
/// with the level UPPERCASED (Go's `strings.ToUpper` at
/// pkg/executor/memtable_reader.go:590) and the time rendered
/// `2006/01/02 15:04:05.000`.
#[test]
fn cluster_log_merge_order_source() {
    // (node type, messages) in Go's per-file order; timestamps are UTC
    // millis for 2019/08/26 06:xx:xx.011.
    let minute = 60i64 * 1000;
    // 2019-08-26 06:18:00 UTC in millis, so t(mins, secs) renders as
    // 06:{18+mins}:{secs}.011 exactly like Go's log timestamps.
    let base: i64 = 1_566_800_280_000;
    let t = |mins: i64, secs: i64| base + mins * minute + secs * 1000 + 11;
    let msg = |node: &str, time: i64, level: &str, text: &str| LogMessage {
        time_millis: time,
        level: level.to_owned(),
        message: format!("[test log message {node} {text}]"),
    };

    let mut streams: Vec<Box<dyn LogStream>> = Vec::new();
    // Each node: one stream, one batch (Go's log files are read in full).
    let mut tidb_msgs = vec![
        msg("tidb", t(1, 13), "INFO", "1, foo"),
        msg("tidb", t(1, 14), "DEBUG", "2, foo"),
        msg("tidb", t(1, 15), "error", "3, foo"),
        msg("tidb", t(1, 16), "trace", "4, foo"),
        msg("tidb", t(1, 17), "CRITICAL", "5, foo"),
        msg("tidb", t(7, 13), "info", "10, bar"),
        msg("tidb", t(7, 14), "debug", "11, bar"),
        msg("tidb", t(7, 15), "ERROR", "12, bar"),
        msg("tidb", t(7, 16), "TRACE", "13, bar"),
        msg("tidb", t(7, 17), "critical", "14, bar"),
    ];
    let mut tikv_msgs = vec![
        msg("tikv", t(1, 13), "INFO", "1, foo"),
        msg("tikv", t(2, 14), "DEBUG", "2, foo"),
        msg("tikv", t(3, 15), "error", "3, foo"),
        msg("tikv", t(4, 16), "trace", "4, foo"),
        msg("tikv", t(5, 17), "CRITICAL", "5, foo"),
        msg("tikv", t(6, 15), "info", "10, bar"),
        msg("tikv", t(7, 16), "debug", "11, bar"),
        msg("tikv", t(8, 17), "ERROR", "12, bar"),
        msg("tikv", t(9, 18), "TRACE", "13, bar"),
        msg("tikv", t(10, 19), "critical", "14, bar"),
    ];
    let mut tiproxy_msgs = vec![
        msg("tiproxy", t(1, 13), "INFO", "1, foo"),
        msg("tiproxy", t(2, 14), "DEBUG", "2, foo"),
        msg("tiproxy", t(3, 15), "error", "3, foo"),
        msg("tiproxy", t(4, 16), "trace", "4, foo"),
        msg("tiproxy", t(5, 17), "CRITICAL", "5, foo"),
        msg("tiproxy", t(6, 15), "info", "10, bar"),
        msg("tiproxy", t(7, 16), "debug", "11, bar"),
        msg("tiproxy", t(8, 17), "ERROR", "12, bar"),
        msg("tiproxy", t(9, 18), "TRACE", "13, bar"),
        msg("tiproxy", t(10, 19), "critical", "14, bar"),
    ];
    let mut ticdc_msgs = vec![
        msg("ticdc", t(1, 13), "INFO", "1, foo"),
        msg("ticdc", t(2, 14), "DEBUG", "2, foo"),
        msg("ticdc", t(3, 15), "error", "3, foo"),
        msg("ticdc", t(4, 16), "trace", "4, foo"),
        msg("ticdc", t(5, 17), "CRITICAL", "5, foo"),
        msg("ticdc", t(6, 15), "info", "10, bar"),
        msg("ticdc", t(7, 16), "debug", "11, bar"),
        msg("ticdc", t(8, 17), "ERROR", "12, bar"),
        msg("ticdc", t(9, 18), "TRACE", "13, bar"),
        msg("ticdc", t(10, 19), "critical", "14, bar"),
    ];
    let mut pd_msgs = vec![
        msg("pd", t(0, 13), "INFO", "1, foo"),
        msg("pd", t(1, 14), "DEBUG", "2, foo"),
        msg("pd", t(2, 15), "error", "3, foo"),
        msg("pd", t(3, 16), "trace", "4, foo"),
        msg("pd", t(4, 17), "CRITICAL", "5, foo"),
        msg("pd", t(5, 13), "info", "10, bar"),
        msg("pd", t(6, 14), "debug", "11, bar"),
        msg("pd", t(7, 15), "ERROR", "12, bar"),
        msg("pd", t(8, 16), "TRACE", "13, bar"),
        msg("pd", t(9, 17), "critical", "14, bar"),
    ];
    streams.push(Box::new(VecStream::new("tidb", "tidb:4000", std::mem::take(&mut tidb_msgs))));
    streams.push(Box::new(VecStream::new("tikv", "tikv:20160", std::mem::take(&mut tikv_msgs))));
    streams.push(Box::new(VecStream::new("tiproxy", "tiproxy:6000", std::mem::take(&mut tiproxy_msgs))));
    streams.push(Box::new(VecStream::new("ticdc", "ticdc:8300", std::mem::take(&mut ticdc_msgs))));
    streams.push(Box::new(VecStream::new("pd", "pd:2379", std::mem::take(&mut pd_msgs))));

    let mut merger = LogRowMerger::new(streams, false);
    let mut rows = merger.next_batch();
    rows.extend(merger.next_batch());
    assert!(merger.warnings().is_empty(), "unexpected warnings {:?}", merger.warnings());

    // Go's fullLogs: (time, type, LEVEL-uppercased, message) plus the address
    // column; sorted by (time, type) with node-type tiebreaks.
    let expected_pairs: Vec<(&str, &str, &str, i64)> = vec![
        ("06:18:13.011", "pd", "INFO", 0),
        ("06:19:13.011", "ticdc", "INFO", 0),
        ("06:19:13.011", "tidb", "INFO", 0),
        ("06:19:13.011", "tikv", "INFO", 0),
        ("06:19:13.011", "tiproxy", "INFO", 0),
        ("06:19:14.011", "pd", "DEBUG", 0),
        ("06:19:14.011", "tidb", "DEBUG", 0),
        ("06:19:15.011", "tidb", "ERROR", 0),
        ("06:19:16.011", "tidb", "TRACE", 0),
        ("06:19:17.011", "tidb", "CRITICAL", 0),
        ("06:20:14.011", "ticdc", "DEBUG", 0),
        ("06:20:14.011", "tikv", "DEBUG", 0),
        ("06:20:14.011", "tiproxy", "DEBUG", 0),
        ("06:20:15.011", "pd", "ERROR", 0),
        ("06:21:15.011", "ticdc", "ERROR", 0),
        ("06:21:15.011", "tikv", "ERROR", 0),
        ("06:21:15.011", "tiproxy", "ERROR", 0),
        ("06:21:16.011", "pd", "TRACE", 0),
        ("06:22:16.011", "ticdc", "TRACE", 0),
        ("06:22:16.011", "tikv", "TRACE", 0),
        ("06:22:16.011", "tiproxy", "TRACE", 0),
        ("06:22:17.011", "pd", "CRITICAL", 0),
        ("06:23:13.011", "pd", "INFO", 0),
        ("06:23:17.011", "ticdc", "CRITICAL", 0),
        ("06:23:17.011", "tikv", "CRITICAL", 0),
        ("06:23:17.011", "tiproxy", "CRITICAL", 0),
        ("06:24:14.011", "pd", "DEBUG", 0),
        ("06:24:15.011", "ticdc", "INFO", 0),
        ("06:24:15.011", "tikv", "INFO", 0),
        ("06:24:15.011", "tiproxy", "INFO", 0),
        ("06:25:13.011", "tidb", "INFO", 0),
        ("06:25:14.011", "tidb", "DEBUG", 0),
        ("06:25:15.011", "pd", "ERROR", 0),
        ("06:25:15.011", "tidb", "ERROR", 0),
        ("06:25:16.011", "ticdc", "DEBUG", 0),
        ("06:25:16.011", "tidb", "TRACE", 0),
        ("06:25:16.011", "tikv", "DEBUG", 0),
        ("06:25:16.011", "tiproxy", "DEBUG", 0),
        ("06:25:17.011", "tidb", "CRITICAL", 0),
        ("06:26:16.011", "pd", "TRACE", 0),
        ("06:26:17.011", "ticdc", "ERROR", 0),
        ("06:26:17.011", "tikv", "ERROR", 0),
        ("06:26:17.011", "tiproxy", "ERROR", 0),
        ("06:27:17.011", "pd", "CRITICAL", 0),
        ("06:27:18.011", "ticdc", "TRACE", 0),
        ("06:27:18.011", "tikv", "TRACE", 0),
        ("06:27:18.011", "tiproxy", "TRACE", 0),
        ("06:28:19.011", "ticdc", "CRITICAL", 0),
        ("06:28:19.011", "tikv", "CRITICAL", 0),
        ("06:28:19.011", "tiproxy", "CRITICAL", 0),
    ];
    // The merger emits (time, type, address, LEVEL, message); check the
    // Go-observable (time, type, LEVEL, message) tuple in order.
    assert_eq!(rows.len(), expected_pairs.len(), "50 messages expected");
    let datum_text = |datum: &Datum| match datum {
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        other => format!("{other:?}"),
    };
    let observed: Vec<(String, String, String, String)> = rows
        .iter()
        .map(|row| {
            (
                datum_text(&row[0]),
                datum_text(&row[1]),
                datum_text(&row[3]),
                datum_text(&row[4]),
            )
        })
        .collect();
    let expected: Vec<(String, String, String)> = expected_pairs
        .iter()
        .map(|(time, node, level, _)| (format!("2019/08/26 {time}"), (*node).to_owned(), (*level).to_owned()))
        .collect();
    for (index, (observed, expected)) in observed.iter().zip(expected).enumerate() {
        assert_eq!(
            (observed.0.as_str(), observed.1.as_str(), observed.2.as_str()),
            (expected.0.as_str(), expected.1.as_str(), expected.2.as_str()),
            "row {index}",
        );
    }
    // The first and last messages carry the fixture texts Go writes.
    assert!(observed[0].3.contains("pd 1, foo"), "first merged message {:?}", observed[0]);
    assert!(observed[49].3.contains("tiproxy 14, bar"), "last merged message {:?}", observed[49]);

    // Batching: with all rows drained, further pulls are empty.
    assert!(merger.next_batch().is_empty());
}

/// Go `pkg/executor/memtable_reader_test.go:497` (TestTiDBClusterLog)'s
/// batching dimensions at [`CLUSTER_LOG_BATCH_SIZE`]: the merger never
/// returns more than the batch size in one pull.
#[test]
fn cluster_log_merge_respects_batch_size() {
    let messages: Vec<LogMessage> = (0..(CLUSTER_LOG_BATCH_SIZE + 10))
        .map(|index| LogMessage {
            time_millis: index as i64,
            level: "info".to_owned(),
            message: format!("m{index}"),
        })
        .collect();
    let mut merger = LogRowMerger::new(
        vec![Box::new(VecStream::new("tidb", "tidb:4000", messages))],
        false,
    );
    let first = merger.next_batch();
    assert_eq!(first.len(), CLUSTER_LOG_BATCH_SIZE);
    let second = merger.next_batch();
    assert_eq!(second.len(), 10);
    assert!(merger.next_batch().is_empty());
}

/// Go `pkg/executor/memtable_reader_test.go:996::TestTiDBClusterLogError`,
/// recoverable half: a stream that ERRORS on refill must be DROPPED with an
/// appended warning while the rest of the merge completes (Go's
/// `clusterLogRetriever.retrieve` warning-append at
/// pkg/executor/memtable_reader.go:573-581 and the refill drop at :597-606).
/// The Go test's other half -- the three `denied to scan` guards -- is the
/// `cluster_log_scan_guard_gaps` test below.
#[test]
fn cluster_log_stream_error_warns_and_drops() {
    // tidb logs two batches; tikv errors after the first batch.
    let tidb: Vec<LogMessage> = (0..4)
        .map(|index| LogMessage {
            time_millis: index * 10,
            level: "info".to_owned(),
            message: format!("tidb m{index}"),
        })
        .collect();
    let tikv: Vec<LogMessage> = (0..2)
        .map(|index| LogMessage {
            time_millis: index * 10 + 5,
            level: "info".to_owned(),
            message: format!("tikv m{index}"),
        })
        .collect();
    let mut merger = LogRowMerger::new(
        vec![
            Box::new(VecStream::new("tidb", "tidb:4000", tidb)),
            Box::new(ErroringStream::new("tikv", "tikv:20160", tikv)),
        ],
        false,
    );
    let rows = merger.next_batch();
    // Both streams' buffered messages merge (4 + 2); the tikv stream's REFILL
    // error is recorded as exactly one warning and the stream is dropped, so
    // nothing after its buffered batch appears.
    assert_eq!(rows.len(), 6, "all buffered rows must survive the failed stream");
    assert_eq!(merger.warnings().len(), 1, "the stream error must append exactly one warning");
    assert!(merger.next_batch().is_empty());
}

/// A [`LogStream`] over a fixed message list.
struct VecStream {
    node_type: String,
    address: String,
    batches: std::collections::VecDeque<Vec<LogMessage>>,
}

impl VecStream {
    fn new(node_type: &str, address: &str, messages: Vec<LogMessage>) -> Self {
        VecStream {
            node_type: node_type.to_owned(),
            address: address.to_owned(),
            batches: std::collections::VecDeque::from(vec![messages]),
        }
    }
}

impl LogStream for VecStream {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn next_batch(&mut self) -> Result<Option<Vec<LogMessage>>, String> {
        Ok(self.batches.pop_front())
    }
}

/// A [`LogStream`] whose FIRST batch succeeds and whose refill ERRORS -- the
/// mock-server failure mode behind Go's warning-append path.
struct ErroringStream {
    node_type: String,
    address: String,
    first: Option<Vec<LogMessage>>,
}

impl ErroringStream {
    fn new(node_type: &str, address: &str, messages: Vec<LogMessage>) -> Self {
        ErroringStream {
            node_type: node_type.to_owned(),
            address: address.to_owned(),
            first: Some(messages),
        }
    }
}

impl LogStream for ErroringStream {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn next_batch(&mut self) -> Result<Option<Vec<LogMessage>>, String> {
        match self.first.take() {
            Some(messages) => Ok(Some(messages)),
            None => Err("mock stream failure".to_owned()),
        }
    }
}

/// The Go test's guard half (`pkg/executor/memtable_reader_test.go:1005-1013`
/// against pkg/executor/memtable_reader.go:450-457): `select * from
/// information_schema.cluster_log` must fail with
/// `denied to scan logs, please specified the start time...`, a start time
/// without an end time with `denied to scan logs, please specified the end
/// time...`, and both bounds without a message predicate with
/// `denied to scan full logs (use ...)` -- plus a first pull returning no
/// servers at all must fail the query (the `return("")` failpoint arm).
#[test]
#[ignore = "go-parity-gap: the denied-to-scan guards inspect the SQL predicate extractor (start/end time bounds and an explicit message LIKE) at clusterLogRetriever.initialize; the SQL predicate-extraction surface is not transcreated, so the guards have no hook"]
fn cluster_log_scan_guard_gaps() {}

/// Go `pkg/executor/memtable_reader_test.go:40::TestMetricTableData`: the
/// `metrics_schema` tables (`tidb_query_duration` quantile buckets) fed from
/// a Prometheus matrix through the `mockMetricsPromData` failpoint.
#[test]
#[ignore = "go-parity-gap: the metrics-schema tables and their Prometheus quantile reader (pkg/executor/metrics_reader.go / pkg/bindinfo-free metric table defs) are not transcreated; the failpoint-fed prom matrix has no tier hook"]
fn metric_table_prom_data_gap() {}

/// Go's `restime` helper (`pkg/executor/memtable_reader_test.go:515-518`):
/// the merged TIME column renders `2006/01/02 15:04:05.000`. This tier
/// renders in UTC and says so ([`format_log_time`]).
#[test]
fn cluster_log_time_rendering_matches_go_format() {
    // 2019-08-26 06:19:13.011 UTC in millis since the epoch.
    let millis = 1_566_800_353_011;
    assert_eq!(format_log_time(millis), "2019/08/26 06:19:13.011");
}
