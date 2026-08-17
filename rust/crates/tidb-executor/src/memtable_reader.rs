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

//! `pkg/executor/memtable_reader.go`: `MemTableReaderExec` (:74) and the
//! CLUSTER-scoped retrievers behind it -- `clusterConfigRetriever` (:164),
//! `clusterServerInfoRetriever` (:315), `clusterLogRetriever` (:375),
//! `hotRegionsHistoryRetriver` (:662) and `tikvRegionPeersRetriever` (:905).
//!
//! # The hard boundary, stated first
//!
//! Every retriever in this Go file answers a query by TALKING TO OTHER NODES.
//! `fetchClusterConfig` (:179) issues an HTTP `GET` per PD/TiKV/TiDB/TiFlash/
//! TiProxy/TiCDC/TSO/scheduling instance; `clusterServerInfoRetriever` calls
//! `infoschema.FetchClusterServerInfoWithoutPrivilegeCheck`, which is the
//! `diagnosticspb` gRPC `ServerInfo` RPC; `clusterLogRetriever` opens a
//! `diagnosticspb.Diagnostics/search_log` STREAM per node;
//! `hotRegionsHistoryRetriver` and `tikvRegionPeersRetriever` go to the PD
//! HTTP API through `pkg/store/helper`.
//!
//! None of that transport exists in this tier, and inventing it would be
//! inventing behavior. So each fan-out is NARROWED TO A TRAIT -- one per kind
//! of remote answer -- and everything the Go file computes ON TOP of those
//! answers is ported in full. Those traits are the honest edge of this file:
//!
//! * [`ClusterConfigSource`] -- boundary: the per-node config `GET` inside
//!   `fetchClusterConfig` (:216-:253).
//! * [`LogStream`] -- boundary: `diagnosticspb.Diagnostics/search_log`, opened
//!   by `clusterLogRetriever.startRetrieving` (:470).
//! * [`HotRegionStream`] -- boundary: the PD `hotregion/history` request in
//!   `hotRegionsHistoryRetriver.startRetrieving` (:736).
//! * `clusterServerInfoRetriever.retrieve` (:323) is NOT ported: after the
//!   privilege check its whole body is
//!   `infoschema.FetchClusterServerInfoWithoutPrivilegeCheck` plus a flatten
//!   of `info.Rows`, so there is no local computation left once the RPC is
//!   removed. Its server filtering is [`filter_cluster_server_info`].
//!
//! # What IS ported, in full
//!
//! * [`MemTableReaderExec`] -- Go `Open`/`Next`/`Close` (:97/:113/:157),
//!   including the inspection-cache rule, which is the only non-obvious part
//!   of the operator: in inspection mode a cacheable table is retrieved ONCE
//!   per statement and the cached `(rows, err)` pair is replayed, and the
//!   SECOND `Next` returns nothing at all rather than the cached rows again.
//! * [`is_inspection_cacheable_table`] -- Go :83.
//! * [`parse_failpoint_server_info`] -- Go :361.
//! * [`config_url`] -- Go's per-node-type URL switch (:216).
//! * [`cluster_config_rows`] -- Go :253-:288: hidden keys dropped, string
//!   values used verbatim while every other value is JSON-marshalled, and the
//!   rows sorted BY KEY. The sort is what makes `SELECT * FROM
//!   information_schema.cluster_config` stable across runs.
//! * [`LogRowMerger`] -- Go `logResponseHeap` (:393) plus the merge loop of
//!   `clusterLogRetriever.retrieve` (:552): a k-way merge of per-node log
//!   streams ordered by `(time, node type)`, in batches of
//!   [`CLUSTER_LOG_BATCH_SIZE`], refilling a stream when its current batch
//!   runs out and dropping it on error with a warning.
//! * [`HotRegionMerger`] -- Go `hotRegionsResponseHeap` (:632) plus the merge
//!   loop of :786, ordered by `(update time, hot degree)` in batches of
//!   [`HOT_REGIONS_HISTORY_BATCH_SIZE`].
//! * [`pack_tikv_region_peers_rows`] / [`is_unexpected_store_id`] -- Go :992
//!   and :982, the peer row shape including the down/pending/normal status
//!   precedence.
//!
//! # Sequential here, concurrent there
//!
//! Go runs both fan-outs concurrently and this tier runs them one node at a
//! time. What that changes, precisely:
//!
//! * **`fetchClusterConfig`** starts one goroutine per instance, collects
//!   into a buffered channel, and then SORTS the results back into server
//!   order by `idx` (:301) -- the comment there says so outright: "Keep the
//!   original order to make the result more stable". Asking the nodes in
//!   order and keeping the answers in order produces the identical row
//!   sequence, so nothing observable is lost. What IS lost is latency: a
//!   cluster of N nodes costs N round trips in series. Warnings are also
//!   emitted in server order here, where Go emits them in completion order;
//!   the SET of warnings is the same.
//! * **`clusterLogRetriever`** is subtler. Go's per-node streams are pulled
//!   by goroutines that stay ahead of the merge, so a slow node only stalls
//!   the merge when the merge actually needs its next batch. [`LogRowMerger`]
//!   pulls the same batches at the same POINTS in the merge -- initialization
//!   pulls one batch per stream, and a stream is refilled exactly when its
//!   current batch is exhausted (Go :596) -- so the sequence of batches
//!   requested and the row order produced are identical. Only the overlap is
//!   gone.
//! * A genuine difference: Go's `startRetrieving` bounds the streams with a
//!   `context.CancelFunc` that `close` (:615) invokes, so abandoning a query
//!   tears the RPCs down. [`LogRowMerger`] has no cancellation to hand a
//!   [`LogStream`]; an implementation that needs it must carry its own, and
//!   [`MemTableRetriever::close`] is where it belongs.
//!
//! # Other narrowings, each named
//!
//! * `hasPriv(sctx, mysql.ConfigPriv)` / `ProcessPriv` (:185, :326) is the
//!   privilege manager on the session; not reachable here, so the caller
//!   performs the check before constructing the retriever.
//! * `config.FlattenConfigItems` and `config.ContainHiddenConfig` live in
//!   `pkg/config`, so [`cluster_config_rows`] takes an ALREADY flattened map
//!   and a hidden-key predicate.
//! * `getHotRegionRowWithSchemaInfo` (:845) resolves a region's key range to
//!   a table/index through `helper.ParseRegionsTableInfos`
//!   (`pkg/store/helper`); [`HotRegionMerger`] therefore yields the ORDERED
//!   `HistoryHotRegion` records and leaves the schema join to the caller.
//! * `tikvRegionPeersRetriever.retrieve` (:911) is PD region lookup;
//!   [`pack_tikv_region_peers_rows`] is the part that turns the answer into
//!   rows.
//! * `execdetails.RuntimeStats`, `dummyCloser` (:61), `util.WithRecovery`,
//!   and `failpoint.Inject` have no counterpart in this tier.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;

use crate::executor::{ExecError, Executor, ExecutorMeta};

/// Go `clusterLogBatchSize` (:58).
pub const CLUSTER_LOG_BATCH_SIZE: usize = 256;

/// Go `hotRegionsHistoryBatchSize` (:59).
pub const HOT_REGIONS_HISTORY_BATCH_SIZE: usize = 256;

/// Go `downPeer`/`pendingPeer`/`normalPeer` (`pkg/executor/infoschema_reader.go`),
/// the three values column `down_seconds`' sibling reports.
const DOWN_PEER: &str = "DOWN";
const PENDING_PEER: &str = "PENDING";
const NORMAL_PEER: &str = "NORMAL";

/// Builds Go's `types.NewStringDatum(value)`.
fn string_datum(value: impl Into<String>) -> Datum {
    Datum::String(tidb_datatype::StringDatum::new(
        value.into().into_bytes(),
        tidb_datatype::Collation::Utf8Mb4Bin,
    ))
}

/// Go `memTableRetriever` (:67): produces the rows of one memory table.
///
/// Go's `retrieve` is called REPEATEDLY and signals exhaustion by returning no
/// rows -- `clusterLogRetriever` and `hotRegionsHistoryRetriver` return one
/// batch per call and keep their own `isDrained` flag. That contract is kept.
pub trait MemTableRetriever {
    /// Go `retrieve`. An empty result means the retriever is drained.
    fn retrieve(&mut self) -> Result<Vec<Vec<Datum>>, ExecError>;

    /// Go `close`. `dummyCloser` (:63) is this default.
    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }
}

/// Go `variable.TableSnapshot`: the rows a cacheable inspection table
/// produced, or the error it failed with. Both are replayed.
#[derive(Clone, Debug)]
pub enum TableSnapshot {
    /// The rows the retriever returned.
    Rows(Vec<Vec<Datum>>),
    /// The error it returned, replayed verbatim to later readers.
    Failed(String),
}

/// Go `SessionVars.InspectionTableCache`: present only while an inspection
/// statement is running, `None` otherwise.
pub type InspectionTableCache = BTreeMap<String, TableSnapshot>;

/// Go `MemTableReaderExec.isInspectionCacheableTable` (:83).
///
/// Only the five CLUSTER-wide tables an inspection rule reads repeatedly are
/// cacheable; caching anything else would make a memory table stale within
/// one statement.
#[must_use]
pub fn is_inspection_cacheable_table(table_name: &str) -> bool {
    matches!(
        table_name,
        "cluster_config"
            | "cluster_info"
            | "cluster_systeminfo"
            | "cluster_load"
            | "cluster_hardware"
    )
}

/// Go `MemTableReaderExec` (:74).
///
/// `Open`'s only body (:97) is activating the transaction so that a long
/// `SELECT ... FROM information_schema.*` blocks the GC worker; there is no
/// GC worker at this tier, and no transaction to activate, so `open` is the
/// base behavior with that fact recorded here rather than silently dropped.
pub struct MemTableReaderExec {
    meta: ExecutorMeta,
    /// Go `table.Name.L`.
    table_name: String,
    retriever: Box<dyn MemTableRetriever>,
    /// Go `cacheRetrieved`.
    cache_retrieved: bool,
}

impl MemTableReaderExec {
    /// Builds the operator over `retriever`.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        table_name: String,
        retriever: Box<dyn MemTableRetriever>,
    ) -> Self {
        MemTableReaderExec {
            meta,
            table_name,
            retriever,
            cache_retrieved: false,
        }
    }

    /// Go's body of `Next` (:113) up to the chunk fill.
    ///
    /// `cache` is Go's `SessionVars.InspectionTableCache`: `None` is
    /// non-inspection mode. Note the shape Go's own TODO calls out -- once a
    /// cacheable table has been served from the cache, EVERY later `Next`
    /// returns nothing, because `cacheRetrieved` gates the whole branch, not
    /// just the cache lookup.
    pub fn rows(
        &mut self,
        cache: Option<&mut InspectionTableCache>,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        let Some(cache) = cache else {
            return self.retriever.retrieve();
        };
        if !is_inspection_cacheable_table(&self.table_name) {
            return self.retriever.retrieve();
        }
        if self.cache_retrieved {
            return Ok(Vec::new());
        }
        if !cache.contains_key(&self.table_name) {
            let snapshot = match self.retriever.retrieve() {
                Ok(rows) => TableSnapshot::Rows(rows),
                Err(error) => TableSnapshot::Failed(format!("{error:?}")),
            };
            cache.insert(self.table_name.clone(), snapshot);
        }
        self.cache_retrieved = true;
        match &cache[&self.table_name] {
            TableSnapshot::Rows(rows) => Ok(rows.clone()),
            TableSnapshot::Failed(error) => Err(ExecError::Internal(error.clone().into())),
        }
    }
}

impl Executor for MemTableReaderExec {
    /// Go `Open` (:97). See the type doc for the transaction activation that
    /// has no counterpart here.
    fn open(&mut self) -> Result<(), ExecError> {
        self.cache_retrieved = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        // boundary: `SessionVars.InspectionTableCache`. The operator has no
        // session here, so a driver that runs inspection statements calls
        // `rows` with the cache instead.
        let rows = self.rows(None)?;
        if rows.is_empty() {
            return Ok(());
        }
        req.grow_and_reset(rows.len());
        for row in rows {
            for (column, value) in row.iter().enumerate() {
                req.append_datum(column, value);
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.retriever.close()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Go `infoschema.ServerInfo`, in the three fields this file reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerInfo {
    /// Go `ServerType`: `pd`, `tikv`, `tidb`, `tiflash`, ...
    pub server_type: String,
    /// Go `Address`.
    pub address: String,
    /// Go `StatusAddr`, the host:port the config/diagnostics endpoint listens
    /// on. An EMPTY status address is why a node is skipped with a warning
    /// (:209).
    pub status_addr: String,
}

/// Go `parseFailpointServerInfo` (:361): the `type,address,status-address`
/// triples the `mockClusterConfigServerInfo` failpoint injects, separated by
/// `;`.
///
/// Go indexes `parts[2]` unconditionally and panics on a malformed entry;
/// a short entry is skipped here instead, which cannot change the behavior of
/// a well-formed failpoint value.
#[must_use]
pub fn parse_failpoint_server_info(value: &str) -> Vec<ServerInfo> {
    let mut servers = Vec::new();
    for server in value.split(';') {
        let parts: Vec<&str> = server.split(',').collect();
        if parts.len() < 3 {
            continue;
        }
        servers.push(ServerInfo {
            server_type: parts[0].to_owned(),
            address: parts[1].to_owned(),
            status_addr: parts[2].to_owned(),
        });
    }
    servers
}

/// Go `infoschema.FilterClusterServerInfo(serversInfo, nodeTypes, instances)`.
///
/// An EMPTY set means "no restriction on this dimension", which is what makes
/// an unqualified `SELECT * FROM cluster_config` read every node.
#[must_use]
pub fn filter_cluster_server_info(
    servers: &[ServerInfo],
    node_types: &[String],
    instances: &[String],
) -> Vec<ServerInfo> {
    servers
        .iter()
        .filter(|server| {
            (node_types.is_empty() || node_types.contains(&server.server_type))
                && (instances.is_empty() || instances.contains(&server.address))
        })
        .cloned()
        .collect()
}

/// Go's per-node-type config URL switch inside `fetchClusterConfig` (:216).
///
/// `None` is Go's default arm: "currently we do not support get config from
/// node type", which becomes a warning and skips the node.
#[must_use]
pub fn config_url(scheme: &str, node_type: &str, status_addr: &str) -> Option<String> {
    let path = match node_type {
        // Go writes `pd.Config`, the PD HTTP client's `/pd/api/v1/config`.
        "pd" => "/pd/api/v1/config",
        "tikv" | "tidb" | "tiflash" => "/config",
        "tiproxy" => "/api/admin/config?format=json",
        "ticdc" => "/config",
        "tso" => "/tso/api/v1/config",
        "scheduling" => "/scheduling/api/v1/config",
        _ => return None,
    };
    Some(format!("{scheme}://{status_addr}{path}"))
}

/// boundary: the per-node config `GET` inside Go `fetchClusterConfig`
/// (:233-:252), including its `PD-Allow-follower-handle` header and the
/// non-200 check.
pub trait ClusterConfigSource {
    /// The node's config, ALREADY flattened by `config.FlattenConfigItems`
    /// the way Go flattens the decoded JSON body (:253).
    fn flattened_config(
        &self,
        server: &ServerInfo,
    ) -> Result<BTreeMap<String, serde_json::Value>, String>;

    /// Go `config.ContainHiddenConfig(key)`.
    fn is_hidden_config(&self, _key: &str) -> bool {
        false
    }
}

/// Go :253-:288: one node's flattened config as `(type, instance, key, value)`
/// rows.
///
/// Two rules matter and are kept exactly: a hidden key contributes NO row, and
/// a STRING value is used verbatim while any other value is JSON-marshalled --
/// which is what stops `"3s"` from being reported as `"\"3s\""`. The rows are
/// sorted by key so the output is stable.
pub fn cluster_config_rows(
    node_type: &str,
    address: &str,
    flattened: &BTreeMap<String, serde_json::Value>,
    is_hidden: &dyn Fn(&str) -> bool,
) -> Result<Vec<Vec<Datum>>, String> {
    let mut items: Vec<(String, String)> = Vec::new();
    for (key, value) in flattened {
        if is_hidden(key) {
            continue;
        }
        let rendered = match value {
            // Go: `case string: // remove quotes`.
            serde_json::Value::String(text) => text.clone(),
            other => serde_json::to_string(other).map_err(|error| error.to_string())?,
        };
        items.push((key.clone(), rendered));
    }
    items.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(items
        .into_iter()
        .map(|(key, value)| {
            vec![
                string_datum(node_type),
                string_datum(address),
                string_datum(key),
                string_datum(value),
            ]
        })
        .collect())
}

/// Go `clusterConfigRetriever` (:164) + `fetchClusterConfig` (:179), with the
/// HTTP fan-out behind [`ClusterConfigSource`].
///
/// Servers are asked IN ORDER, which is the order Go restores by sorting on
/// `idx` before flattening the rows (:301).
pub struct ClusterConfigRetriever<S: ClusterConfigSource> {
    source: S,
    servers: Vec<ServerInfo>,
    /// Go `extractor.SkipRequest`: the extractor proved no node can match.
    skip_request: bool,
    /// Go `retrieved`.
    retrieved: bool,
    /// Go's `sctx.GetSessionVars().StmtCtx.AppendWarning` calls, collected
    /// because this tier has no statement context to append to.
    warnings: Vec<String>,
}

impl<S: ClusterConfigSource> ClusterConfigRetriever<S> {
    /// Builds the retriever over the already filtered server list.
    pub fn new(source: S, servers: Vec<ServerInfo>, skip_request: bool) -> Self {
        ClusterConfigRetriever {
            source,
            servers,
            skip_request,
            retrieved: false,
            warnings: Vec::new(),
        }
    }

    /// The warnings this retrieval produced, in server order.
    #[must_use]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }
}

impl<S: ClusterConfigSource> MemTableRetriever for ClusterConfigRetriever<S> {
    fn retrieve(&mut self) -> Result<Vec<Vec<Datum>>, ExecError> {
        if self.skip_request || self.retrieved {
            return Ok(Vec::new());
        }
        self.retrieved = true;
        let mut rows = Vec::new();
        for server in &self.servers {
            if server.status_addr.is_empty() {
                self.warnings.push(format!(
                    "{} node {} does not contain status address",
                    server.server_type, server.address
                ));
                continue;
            }
            match self.source.flattened_config(server) {
                Err(error) => self.warnings.push(error),
                Ok(flattened) => {
                    match cluster_config_rows(
                        &server.server_type,
                        &server.address,
                        &flattened,
                        &|key| self.source.is_hidden_config(key),
                    ) {
                        Err(error) => self.warnings.push(error),
                        Ok(node_rows) => rows.extend(node_rows),
                    }
                }
            }
        }
        Ok(rows)
    }
}

/// Go `diagnosticspb.LogMessage`, in the three fields the merge reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogMessage {
    /// Go `Time`, Unix milliseconds.
    pub time_millis: i64,
    /// Go `Level.String()`; the row uppercases it.
    pub level: String,
    /// Go `Message`.
    pub message: String,
}

/// boundary: one node's `diagnosticspb.Diagnostics/search_log` stream, opened
/// by Go `clusterLogRetriever.startRetrieving` (:470).
pub trait LogStream {
    /// Go's node type (`tidb`, `tikv`, `pd`), the merge's tiebreaker.
    fn node_type(&self) -> &str;
    /// Go's node address, reported in the row.
    fn address(&self) -> &str;
    /// The next batch of messages, in ascending time order. `Ok(None)` is a
    /// drained stream; Go models both "no more" and an empty batch the same
    /// way (:597).
    fn next_batch(&mut self) -> Result<Option<Vec<LogMessage>>, String>;
}

/// One live stream in the merge: its remaining messages and its identity.
struct LogStreamState {
    stream: Box<dyn LogStream>,
    messages: std::collections::VecDeque<LogMessage>,
}

/// Go `logResponseHeap` (:393) plus the merge loop of
/// `clusterLogRetriever.retrieve` (:552).
///
/// Go's `Less` (:399) orders by the HEAD message's time and breaks ties by
/// NODE TYPE, so two nodes logging in the same millisecond always interleave
/// the same way. A `BinaryHeap` is a max-heap, so the ordering below is
/// deliberately reversed to pop the minimum, which is what `heap.Pop` gives.
pub struct LogRowMerger {
    streams: Vec<LogStreamState>,
    /// Go `retrieving`.
    initialized: bool,
    /// Go `isDrained`.
    drained: bool,
    /// Go `extractor.SkipRequest`.
    skip_request: bool,
    warnings: Vec<String>,
}

impl LogRowMerger {
    /// Builds the merger over one stream per node.
    #[must_use]
    pub fn new(streams: Vec<Box<dyn LogStream>>, skip_request: bool) -> Self {
        LogRowMerger {
            streams: streams
                .into_iter()
                .map(|stream| LogStreamState {
                    stream,
                    messages: std::collections::VecDeque::new(),
                })
                .collect(),
            initialized: false,
            drained: false,
            skip_request,
            warnings: Vec::new(),
        }
    }

    /// The warnings a failing stream produced, as Go's `AppendWarning` calls.
    #[must_use]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Go's heap initialization (:566): pull one batch per stream, dropping a
    /// stream that errored (with a warning) or came back empty.
    fn initialize(&mut self) {
        let mut live = Vec::new();
        for mut state in std::mem::take(&mut self.streams) {
            match state.stream.next_batch() {
                Err(error) => self.warnings.push(error),
                Ok(None) => {}
                Ok(Some(messages)) => {
                    if messages.is_empty() {
                        continue;
                    }
                    state.messages = messages.into();
                    live.push(state);
                }
            }
        }
        self.streams = live;
        self.initialized = true;
    }

    /// Go `Less` (:399), as the index of the stream whose head sorts first.
    fn min_stream(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (index, state) in self.streams.iter().enumerate() {
            let Some(head) = state.messages.front() else {
                continue;
            };
            let Some(current) = best else {
                best = Some(index);
                continue;
            };
            let other = &self.streams[current];
            let other_head = other
                .messages
                .front()
                .expect("a live stream always has a head message");
            let ordering = head
                .time_millis
                .cmp(&other_head.time_millis)
                .then_with(|| state.stream.node_type().cmp(other.stream.node_type()));
            if ordering == Ordering::Less {
                best = Some(index);
            }
        }
        best
    }

    /// Go `clusterLogRetriever.retrieve` (:552): the next batch of at most
    /// [`CLUSTER_LOG_BATCH_SIZE`] merged rows.
    pub fn next_batch(&mut self) -> Vec<Vec<Datum>> {
        if self.skip_request || self.drained {
            return Vec::new();
        }
        if !self.initialized {
            self.initialize();
        }
        let mut rows = Vec::new();
        while rows.len() < CLUSTER_LOG_BATCH_SIZE {
            let Some(index) = self.min_stream() else {
                break;
            };
            let head = self.streams[index]
                .messages
                .pop_front()
                .expect("the selected stream has a head message");
            rows.push(vec![
                string_datum(format_log_time(head.time_millis)),
                string_datum(self.streams[index].stream.node_type()),
                string_datum(self.streams[index].stream.address()),
                string_datum(head.level.to_uppercase()),
                string_datum(head.message),
            ]);
            if self.streams[index].messages.is_empty() {
                // Go: the current streaming result is drained, read the next
                // to supply. An error drops the stream with a warning; an
                // empty next batch drops it silently.
                match self.streams[index].stream.next_batch() {
                    Err(error) => {
                        self.warnings.push(error);
                        self.streams.remove(index);
                    }
                    Ok(None) => {
                        self.streams.remove(index);
                    }
                    Ok(Some(messages)) => {
                        if messages.is_empty() {
                            self.streams.remove(index);
                        } else {
                            self.streams[index].messages = messages.into();
                        }
                    }
                }
            }
        }
        self.drained = self.streams.iter().all(|state| state.messages.is_empty());
        rows
    }
}

impl MemTableRetriever for LogRowMerger {
    fn retrieve(&mut self) -> Result<Vec<Vec<Datum>>, ExecError> {
        Ok(self.next_batch())
    }
}

/// Go's log timestamp rendering (:588): `time.UnixMilli(...).Format(
/// "2006/01/02 15:04:05.000")`, in UTC.
///
/// Go formats in the process's local zone; the zone this tier would use is a
/// session fact it does not have, so UTC is used and named here rather than
/// guessed.
#[must_use]
pub fn format_log_time(time_millis: i64) -> String {
    let seconds = time_millis.div_euclid(1000);
    let millis = time_millis.rem_euclid(1000);
    match chrono::DateTime::from_timestamp(seconds, 0) {
        Some(stamp) => format!("{}.{:03}", stamp.format("%Y/%m/%d %H:%M:%S"), millis),
        None => String::new(),
    }
}

/// Go `HistoryHotRegion` (:690), in the fields the merge orders on and the
/// row builder reads.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryHotRegion {
    /// Go `UpdateTime`, Unix milliseconds; the primary sort key.
    pub update_time: i64,
    /// Go `HotDegree`; the tiebreaker.
    pub hot_degree: i64,
    /// Go `RegionID`.
    pub region_id: u64,
    /// Go `StoreID`.
    pub store_id: u64,
    /// Go `PeerID`.
    pub peer_id: u64,
    /// Go `IsLearner`.
    pub is_learner: bool,
    /// Go `IsLeader`.
    pub is_leader: bool,
    /// Go `HotRegionType`; the row uppercases it.
    pub hot_region_type: String,
    /// Go `FlowBytes`.
    pub flow_bytes: f64,
    /// Go `KeyRate`.
    pub key_rate: f64,
    /// Go `QueryRate`.
    pub query_rate: f64,
    /// Go `StartKey`.
    pub start_key: Vec<u8>,
    /// Go `EndKey`.
    pub end_key: Vec<u8>,
}

/// boundary: one PD's `hotregion/history` response stream, requested by Go
/// `hotRegionsHistoryRetriver.startRetrieving` (:736).
pub trait HotRegionStream {
    /// The next batch of records, ascending by update time. `Ok(None)` ends
    /// the stream.
    fn next_batch(&mut self) -> Result<Option<Vec<HistoryHotRegion>>, String>;
}

/// Go `hotRegionsResponseHeap` (:632) plus the merge loop of :786.
///
/// Ordering is `(UpdateTime, HotDegree)` (:638). Unlike the log merge, Go
/// NEVER refills a drained stream here (:832 pushes back only when messages
/// remain), so a stream contributes exactly the batch it was initialized
/// with; that is reproduced rather than "fixed".
pub struct HotRegionMerger {
    batches: Vec<Vec<HistoryHotRegion>>,
    initialized: bool,
    drained: bool,
    skip_request: bool,
    streams: Vec<Box<dyn HotRegionStream>>,
    warnings: Vec<String>,
}

impl HotRegionMerger {
    /// Builds the merger over one stream per PD.
    #[must_use]
    pub fn new(streams: Vec<Box<dyn HotRegionStream>>, skip_request: bool) -> Self {
        HotRegionMerger {
            batches: Vec::new(),
            initialized: false,
            drained: false,
            skip_request,
            streams,
            warnings: Vec::new(),
        }
    }

    /// The warnings a failing stream produced.
    #[must_use]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn initialize(&mut self) {
        for stream in &mut self.streams {
            match stream.next_batch() {
                Err(error) => self.warnings.push(error),
                Ok(None) => {}
                Ok(Some(records)) => {
                    if !records.is_empty() {
                        self.batches.push(records);
                    }
                }
            }
        }
        self.initialized = true;
    }

    /// The ordered records of the next batch, at most
    /// [`HOT_REGIONS_HISTORY_BATCH_SIZE`] of them.
    ///
    /// Go emits ROWS here, but one record can expand into several rows (one
    /// per table or index the region covers) and that expansion needs the
    /// schema join named in the module doc; the batch cap is applied to
    /// records, as Go applies it to rows before the expansion of the NEXT
    /// record.
    pub fn next_batch(&mut self) -> Vec<HistoryHotRegion> {
        if self.skip_request || self.drained {
            return Vec::new();
        }
        if !self.initialized {
            self.initialize();
        }
        let mut selected = Vec::new();
        while selected.len() < HOT_REGIONS_HISTORY_BATCH_SIZE {
            let Some(index) = self.min_batch() else {
                break;
            };
            selected.push(self.batches[index].remove(0));
            if self.batches[index].is_empty() {
                self.batches.remove(index);
            }
        }
        self.drained = self.batches.is_empty();
        selected
    }

    /// Go `Less` (:638).
    fn min_batch(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (index, batch) in self.batches.iter().enumerate() {
            let Some(head) = batch.first() else {
                continue;
            };
            let Some(current) = best else {
                best = Some(index);
                continue;
            };
            let other = &self.batches[current][0];
            let ordering = head
                .update_time
                .cmp(&other.update_time)
                .then_with(|| head.hot_degree.cmp(&other.hot_degree));
            if ordering == Ordering::Less {
                best = Some(index);
            }
        }
        best
    }
}

/// Go `pd.RegionPeer`, in the fields `packTiKVRegionPeersRows` reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionPeer {
    /// Go `Peer.ID`.
    pub id: i64,
    /// Go `Peer.StoreID`.
    pub store_id: i64,
    /// Go `Peer.IsLearner`.
    pub is_learner: bool,
}

/// Go `pd.RegionInfo` (`pkg/store/helper`'s PD HTTP model), in the fields
/// `packTiKVRegionPeersRows` reads.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RegionInfo {
    /// Go `RegionInfo.ID`.
    pub id: i64,
    /// Go `RegionInfo.Peers`.
    pub peers: Vec<RegionPeer>,
    /// Go `RegionInfo.Leader.ID`.
    pub leader_peer_id: i64,
    /// Go `RegionInfo.PendingPeers`, by peer id.
    pub pending_peer_ids: Vec<i64>,
    /// Go `RegionInfo.DownPeers`, as `(peer id, down seconds)`.
    pub down_peers: Vec<(i64, i64)>,
}

/// Go `tikvRegionPeersRetriever.isUnexpectedStoreID` (:982).
///
/// A region located on stores 1, 4 and 7 is returned in full when ANY of them
/// is asked for, so the peers on stores the query did not name must be
/// dropped. With no store filter at all nothing is dropped.
#[must_use]
pub fn is_unexpected_store_id(store_id: i64, requested_stores: &[i64]) -> bool {
    if requested_stores.is_empty() {
        return false;
    }
    !requested_stores.contains(&store_id)
}

/// Go `tikvRegionPeersRetriever.packTiKVRegionPeersRows` (:992).
///
/// The status column's precedence is Go's and is the only real rule here:
/// DOWN wins over PENDING, which wins over NORMAL, and only a DOWN peer
/// carries a `down_seconds` value.
#[must_use]
pub fn pack_tikv_region_peers_rows(
    regions: &[RegionInfo],
    requested_stores: &[i64],
) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for region in regions {
        for peer in &region.peers {
            if is_unexpected_store_id(peer.store_id, requested_stores) {
                continue;
            }
            let down_seconds = region
                .down_peers
                .iter()
                .find(|(id, _)| *id == peer.id)
                .map(|(_, seconds)| *seconds);
            let (status, down) = match down_seconds {
                Some(seconds) => (DOWN_PEER, Some(seconds)),
                None if region.pending_peer_ids.contains(&peer.id) => (PENDING_PEER, None),
                None => (NORMAL_PEER, None),
            };
            rows.push(vec![
                Datum::Int(region.id),
                Datum::Int(peer.id),
                Datum::Int(peer.store_id),
                Datum::Int(i64::from(peer.is_learner)),
                Datum::Int(i64::from(peer.id == region.leader_peer_id)),
                string_datum(status),
                match down {
                    Some(seconds) => Datum::Int(seconds),
                    None => Datum::Null,
                },
            ]);
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_meta() -> ExecutorMeta {
        ExecutorMeta::new(Schema::default(), 0, 1, 1)
    }

    fn text(datum: &Datum) -> String {
        match datum {
            Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
            other => format!("{other:?}"),
        }
    }

    #[test]
    fn only_the_five_cluster_tables_are_inspection_cacheable() {
        for name in [
            "cluster_config",
            "cluster_info",
            "cluster_systeminfo",
            "cluster_load",
            "cluster_hardware",
        ] {
            assert!(is_inspection_cacheable_table(name), "{name}");
        }
        assert!(!is_inspection_cacheable_table("cluster_log"));
        assert!(!is_inspection_cacheable_table("tikv_region_peers"));
    }

    #[test]
    fn the_failpoint_server_list_is_parsed_by_position() {
        assert_eq!(
            parse_failpoint_server_info("tidb,127.0.0.1:4000,127.0.0.1:10080;pd,a:1,b:2"),
            vec![
                ServerInfo {
                    server_type: "tidb".to_owned(),
                    address: "127.0.0.1:4000".to_owned(),
                    status_addr: "127.0.0.1:10080".to_owned(),
                },
                ServerInfo {
                    server_type: "pd".to_owned(),
                    address: "a:1".to_owned(),
                    status_addr: "b:2".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn each_node_type_has_its_own_config_endpoint() {
        assert_eq!(
            config_url("http", "pd", "h:1").unwrap(),
            "http://h:1/pd/api/v1/config"
        );
        assert_eq!(
            config_url("https", "tikv", "h:2").unwrap(),
            "https://h:2/config"
        );
        assert_eq!(
            config_url("http", "tiproxy", "h:3").unwrap(),
            "http://h:3/api/admin/config?format=json"
        );
        assert_eq!(
            config_url("http", "tso", "h:4").unwrap(),
            "http://h:4/tso/api/v1/config"
        );
        assert!(
            config_url("http", "grafana", "h:5").is_none(),
            "an unsupported node type warns and is skipped"
        );
    }

    fn flattened(pairs: &[(&str, serde_json::Value)]) -> BTreeMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect()
    }

    #[test]
    fn config_rows_are_sorted_by_key_and_strings_keep_their_quotes_off() {
        let config = flattened(&[
            ("zeta", serde_json::json!("3s")),
            ("alpha", serde_json::json!(7)),
            ("beta", serde_json::json!({ "on": true })),
        ]);
        let rows = cluster_config_rows("tikv", "h:1", &config, &|_| false).unwrap();
        let rendered: Vec<(String, String)> = rows
            .iter()
            .map(|row| (text(&row[2]), text(&row[3])))
            .collect();
        assert_eq!(
            rendered,
            vec![
                ("alpha".to_owned(), "7".to_owned()),
                ("beta".to_owned(), "{\"on\":true}".to_owned()),
                ("zeta".to_owned(), "3s".to_owned()),
            ],
            "a string value is reported verbatim, everything else as JSON"
        );
        assert_eq!(text(&rows[0][0]), "tikv");
        assert_eq!(text(&rows[0][1]), "h:1");
    }

    #[test]
    fn hidden_config_keys_produce_no_row() {
        let config = flattened(&[
            ("security.ssl-key", serde_json::json!("secret")),
            ("visible", serde_json::json!(1)),
        ]);
        let rows =
            cluster_config_rows("pd", "h:1", &config, &|key| key.starts_with("security.")).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0][2]), "visible");
    }

    struct StubConfigSource;

    impl ClusterConfigSource for StubConfigSource {
        fn flattened_config(
            &self,
            server: &ServerInfo,
        ) -> Result<BTreeMap<String, serde_json::Value>, String> {
            if server.server_type == "broken" {
                return Err(format!("request to {} failed", server.address));
            }
            Ok(flattened(&[("k", serde_json::json!("v"))]))
        }
    }

    fn server(kind: &str, address: &str, status: &str) -> ServerInfo {
        ServerInfo {
            server_type: kind.to_owned(),
            address: address.to_owned(),
            status_addr: status.to_owned(),
        }
    }

    #[test]
    fn a_node_without_a_status_address_warns_and_is_skipped() {
        let servers = vec![
            server("tikv", "a:1", ""),
            server("tikv", "b:1", "b:2"),
            server("broken", "c:1", "c:2"),
        ];
        let mut retriever = ClusterConfigRetriever::new(StubConfigSource, servers, false);
        let rows = retriever.retrieve().unwrap();
        assert_eq!(rows.len(), 1, "only the healthy node contributed a row");
        assert_eq!(text(&rows[0][1]), "b:1");
        assert_eq!(
            retriever.warnings(),
            [
                "tikv node a:1 does not contain status address".to_owned(),
                "request to c:1 failed".to_owned(),
            ]
        );
        assert!(
            retriever.retrieve().unwrap().is_empty(),
            "a second retrieve is drained"
        );
    }

    #[test]
    fn a_skipped_request_reads_nothing() {
        let mut retriever =
            ClusterConfigRetriever::new(StubConfigSource, vec![server("tikv", "a:1", "a:2")], true);
        assert!(retriever.retrieve().unwrap().is_empty());
    }

    struct StubLogStream {
        node_type: String,
        address: String,
        batches: std::collections::VecDeque<Result<Vec<LogMessage>, String>>,
    }

    impl LogStream for StubLogStream {
        fn node_type(&self) -> &str {
            &self.node_type
        }

        fn address(&self) -> &str {
            &self.address
        }

        fn next_batch(&mut self) -> Result<Option<Vec<LogMessage>>, String> {
            match self.batches.pop_front() {
                None => Ok(None),
                Some(Err(error)) => Err(error),
                Some(Ok(messages)) => Ok(Some(messages)),
            }
        }
    }

    fn message(time_millis: i64, text: &str) -> LogMessage {
        LogMessage {
            time_millis,
            level: "info".to_owned(),
            message: text.to_owned(),
        }
    }

    fn stream(
        node_type: &str,
        address: &str,
        batches: Vec<Result<Vec<LogMessage>, String>>,
    ) -> Box<dyn LogStream> {
        Box::new(StubLogStream {
            node_type: node_type.to_owned(),
            address: address.to_owned(),
            batches: batches.into(),
        })
    }

    #[test]
    fn log_streams_merge_by_time_and_break_ties_by_node_type() {
        let streams = vec![
            stream(
                "tikv",
                "kv:1",
                vec![Ok(vec![message(10, "kv-a"), message(30, "kv-b")])],
            ),
            stream(
                "pd",
                "pd:1",
                vec![Ok(vec![message(10, "pd-a"), message(20, "pd-b")])],
            ),
        ];
        let mut merger = LogRowMerger::new(streams, false);
        let rows = merger.next_batch();
        let messages: Vec<String> = rows.iter().map(|row| text(&row[4])).collect();
        assert_eq!(
            messages,
            vec![
                "pd-a".to_owned(),
                "kv-a".to_owned(),
                "pd-b".to_owned(),
                "kv-b".to_owned()
            ],
            "the 10ms tie goes to `pd` because it sorts before `tikv`"
        );
        assert_eq!(text(&rows[0][1]), "pd");
        assert_eq!(text(&rows[0][2]), "pd:1");
        assert_eq!(text(&rows[0][3]), "INFO", "the level is uppercased");
        assert!(merger.next_batch().is_empty(), "the merge is drained");
    }

    #[test]
    fn a_stream_is_refilled_only_when_its_current_batch_runs_out() {
        let streams = vec![
            stream(
                "tikv",
                "kv:1",
                vec![
                    Ok(vec![message(10, "first")]),
                    Ok(vec![message(40, "third")]),
                ],
            ),
            stream("pd", "pd:1", vec![Ok(vec![message(20, "second")])]),
        ];
        let mut merger = LogRowMerger::new(streams, false);
        let messages: Vec<String> = merger
            .next_batch()
            .iter()
            .map(|row| text(&row[4]))
            .collect();
        assert_eq!(
            messages,
            vec!["first".to_owned(), "second".to_owned(), "third".to_owned()]
        );
    }

    #[test]
    fn a_failing_log_stream_warns_and_leaves_the_others_merging() {
        let streams = vec![
            stream("tikv", "kv:1", vec![Err("kv unreachable".to_owned())]),
            stream("pd", "pd:1", vec![Ok(vec![message(5, "only")])]),
        ];
        let mut merger = LogRowMerger::new(streams, false);
        let rows = merger.next_batch();
        assert_eq!(rows.len(), 1);
        assert_eq!(merger.warnings(), ["kv unreachable".to_owned()]);
    }

    #[test]
    fn the_log_batch_is_capped() {
        let messages: Vec<LogMessage> = (0..CLUSTER_LOG_BATCH_SIZE + 10)
            .map(|index| message(index as i64, "m"))
            .collect();
        let mut merger = LogRowMerger::new(vec![stream("tidb", "db:1", vec![Ok(messages)])], false);
        assert_eq!(merger.next_batch().len(), CLUSTER_LOG_BATCH_SIZE);
        assert_eq!(merger.next_batch().len(), 10);
    }

    #[test]
    fn a_log_timestamp_renders_with_milliseconds() {
        assert_eq!(format_log_time(0), "1970/01/01 00:00:00.000");
        assert_eq!(format_log_time(1_500), "1970/01/01 00:00:01.500");
    }

    struct StubHotStream {
        batches: std::collections::VecDeque<Result<Vec<HistoryHotRegion>, String>>,
    }

    impl HotRegionStream for StubHotStream {
        fn next_batch(&mut self) -> Result<Option<Vec<HistoryHotRegion>>, String> {
            match self.batches.pop_front() {
                None => Ok(None),
                Some(Err(error)) => Err(error),
                Some(Ok(records)) => Ok(Some(records)),
            }
        }
    }

    fn hot(update_time: i64, hot_degree: i64, region_id: u64) -> HistoryHotRegion {
        HistoryHotRegion {
            update_time,
            hot_degree,
            region_id,
            store_id: 1,
            peer_id: 1,
            is_learner: false,
            is_leader: true,
            hot_region_type: "read".to_owned(),
            flow_bytes: 0.0,
            key_rate: 0.0,
            query_rate: 0.0,
            start_key: Vec::new(),
            end_key: Vec::new(),
        }
    }

    fn hot_stream(batch: Vec<HistoryHotRegion>) -> Box<dyn HotRegionStream> {
        Box::new(StubHotStream {
            batches: vec![Ok(batch)].into(),
        })
    }

    #[test]
    fn hot_regions_merge_by_update_time_then_hot_degree() {
        let mut merger = HotRegionMerger::new(
            vec![
                hot_stream(vec![hot(10, 5, 1), hot(30, 1, 3)]),
                hot_stream(vec![hot(10, 2, 2), hot(20, 9, 4)]),
            ],
            false,
        );
        let ids: Vec<u64> = merger
            .next_batch()
            .iter()
            .map(|record| record.region_id)
            .collect();
        assert_eq!(
            ids,
            vec![2, 1, 4, 3],
            "the 10ms tie goes to the lower hot degree"
        );
        assert!(merger.next_batch().is_empty());
    }

    fn region() -> RegionInfo {
        RegionInfo {
            id: 7,
            peers: vec![
                RegionPeer {
                    id: 1,
                    store_id: 10,
                    is_learner: false,
                },
                RegionPeer {
                    id: 2,
                    store_id: 20,
                    is_learner: true,
                },
                RegionPeer {
                    id: 3,
                    store_id: 30,
                    is_learner: false,
                },
            ],
            leader_peer_id: 1,
            pending_peer_ids: vec![2],
            down_peers: vec![(3, 42)],
        }
    }

    #[test]
    fn region_peer_rows_carry_status_learner_and_leader_flags() {
        let rows = pack_tikv_region_peers_rows(&[region()], &[]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Datum::Int(7));
        assert_eq!(rows[0][3], Datum::Int(0), "peer 1 is not a learner");
        assert_eq!(rows[0][4], Datum::Int(1), "peer 1 is the leader");
        assert_eq!(text(&rows[0][5]), "NORMAL");
        assert_eq!(rows[0][6], Datum::Null);
        assert_eq!(rows[1][3], Datum::Int(1), "peer 2 is a learner");
        assert_eq!(text(&rows[1][5]), "PENDING");
        assert_eq!(text(&rows[2][5]), "DOWN");
        assert_eq!(rows[2][6], Datum::Int(42));
    }

    #[test]
    fn a_store_filter_drops_the_peers_on_stores_the_query_did_not_name() {
        let rows = pack_tikv_region_peers_rows(&[region()], &[20]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][2], Datum::Int(20));
        assert!(!is_unexpected_store_id(999, &[]));
        assert!(is_unexpected_store_id(999, &[1]));
    }

    struct StubRetriever {
        batches: std::collections::VecDeque<Vec<Vec<Datum>>>,
        calls: usize,
    }

    impl MemTableRetriever for StubRetriever {
        fn retrieve(&mut self) -> Result<Vec<Vec<Datum>>, ExecError> {
            self.calls += 1;
            Ok(self.batches.pop_front().unwrap_or_default())
        }
    }

    #[test]
    fn an_inspection_cacheable_table_is_retrieved_once_and_then_returns_nothing() {
        let rows = vec![vec![Datum::Int(1)]];
        let mut exec = MemTableReaderExec::new(
            test_meta(),
            "cluster_config".to_owned(),
            Box::new(StubRetriever {
                batches: vec![rows.clone(), rows.clone()].into(),
                calls: 0,
            }),
        );
        let mut cache = InspectionTableCache::new();
        assert_eq!(exec.rows(Some(&mut cache)).unwrap(), rows);
        assert_eq!(
            exec.rows(Some(&mut cache)).unwrap(),
            Vec::<Vec<Datum>>::new(),
            "Go's `cacheRetrieved` gates the whole branch, not just the lookup"
        );
        assert!(cache.contains_key("cluster_config"));
    }

    #[test]
    fn a_second_reader_of_the_same_statement_replays_the_cached_rows() {
        let rows = vec![vec![Datum::Int(1)]];
        let mut cache = InspectionTableCache::new();
        cache.insert(
            "cluster_config".to_owned(),
            TableSnapshot::Rows(rows.clone()),
        );
        let mut exec = MemTableReaderExec::new(
            test_meta(),
            "cluster_config".to_owned(),
            Box::new(StubRetriever {
                batches: std::collections::VecDeque::new(),
                calls: 0,
            }),
        );
        assert_eq!(
            exec.rows(Some(&mut cache)).unwrap(),
            rows,
            "the retriever is never asked"
        );
    }

    #[test]
    fn a_non_cacheable_table_ignores_the_cache_entirely() {
        let first = vec![vec![Datum::Int(1)]];
        let second = vec![vec![Datum::Int(2)]];
        let mut exec = MemTableReaderExec::new(
            test_meta(),
            "cluster_log".to_owned(),
            Box::new(StubRetriever {
                batches: vec![first.clone(), second.clone()].into(),
                calls: 0,
            }),
        );
        let mut cache = InspectionTableCache::new();
        assert_eq!(exec.rows(Some(&mut cache)).unwrap(), first);
        assert_eq!(exec.rows(Some(&mut cache)).unwrap(), second);
        assert!(cache.is_empty());
    }
}
