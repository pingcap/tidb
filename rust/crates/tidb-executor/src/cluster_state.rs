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

//! The diagnostic and cluster `information_schema` tables, read through the
//! executor-owned retrievers: the transaction, deadlock and lock-wait tables,
//! the `CLUSTER_*` tables and their peer fan-out, and the tables that ask the
//! cluster's servers (`CLUSTER_INFO`, `CLUSTER_CONFIG`, `CLUSTER_LOG`, the
//! server-info tables, `TIDB_SERVERS_INFO`, `TIDB_HOT_REGIONS_HISTORY`).
//!
//! What they read of the viewing session is the [`ViewerState`] snapshot; the
//! warnings a read raises go to the statement context, which the session
//! drains into its own warnings.

use tidb_datatype::{Datum, FieldType};
use crate::Catalog;
use crate::DriverError;

use crate::memtable_reader::ViewerState;
use std::sync::Arc;
use crate::infoschema_reader::datetime_datum;
use crate::memtable_reader::go_uptime_string;
use crate::memtable_reader::peer_cluster_table_rows;

impl ViewerState {
    /// Go `SQLDigestTextRetriever` over this node and, with `global`, its
    /// peers.
    fn sql_digest_texts(
        &self,
        ctx: &crate::StmtContext,
        digests: impl IntoIterator<Item = String>,
        global: bool,
    ) -> Result<std::collections::HashMap<String, String>, DriverError> {
        self.digest_retriever.retrieve(ctx, digests, global)
    }

    /// Go fetchClusterConfig checks CONFIG before discovery and reports per-node
    /// failures as warnings while retaining successful nodes.
    pub fn cluster_config_rows(
        &self,
        ctx: &crate::StmtContext,
        filters: &[tidb_planner::memtable_predicate_extractor::ClusterTableFilter],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        if filters.iter().all(|filter| filter.skip_request()) {
            return Ok(Vec::new());
        }
        if !self.config_scoped {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["CONFIG".into()]),
            ));
        }
        let client = self.cluster_config.clone().ok_or_else(|| {
            DriverError::unsupported("CLUSTER_CONFIG live retrieval is not installed")
        })?;
        let topology = self.cluster_topology.clone().ok_or_else(|| {
            DriverError::unsupported("CLUSTER_CONFIG cluster discovery is not installed")
        })?;
        let mut warnings: Vec<String> = Vec::new();
        let servers = topology.servers(&mut warnings);
        for warning in warnings.drain(..) {
            ctx.append_warning_parts(1105, &warning);
        }
        let servers: Vec<_> = servers
            .map_err(DriverError::unsupported)?
            .into_iter()
            .filter(|server| {
                filters
                    .iter()
                    .any(|filter| filter.matches(&server.server_type, &server.address))
            })
            .collect();
        let rows = client.fetch(&servers, &mut warnings);
        for warning in warnings {
            ctx.append_warning_parts(1105, &warning);
        }
        Ok(rows)
    }


    /// Go `infoschema.GetClusterServerInfo` filtered by the scans'
    /// `FilterClusterServerInfo` needs, or None when this session has no
    /// cluster discovery or diagnostics client (an embedded session, which
    /// reads these tables as empty).
    #[allow(clippy::type_complexity)]
    fn cluster_diagnostics_servers(
        &self,
        ctx: &crate::StmtContext,
        matches: impl Fn(&str, &str) -> bool,
    ) -> Result<
        Option<(
            Arc<crate::cluster_diagnostics::ClusterDiagnosticsClient>,
            Vec<tidb_domain::cluster_topology::ClusterServer>,
        )>,
        DriverError,
    > {
        let (Some(client), Some(topology)) = (
            self.cluster_diagnostics.clone(),
            self.cluster_topology.clone(),
        ) else {
            return Ok(None);
        };
        let mut warnings: Vec<String> = Vec::new();
        let servers = topology.servers(&mut warnings);
        for warning in warnings {
            ctx.append_warning_parts(1105, &warning);
        }
        let servers = servers
            .map_err(DriverError::unsupported)?
            .into_iter()
            .filter(|server| matches(&server.server_type, &server.address))
            .collect();
        Ok(Some((client, servers)))
    }


    /// Go `clusterServerInfoRetriever.retrieve` after its privilege check:
    /// ask every matching server's diagnostics service; a server that fails
    /// is a warning.
    pub fn cluster_server_info_rows(
        &self,
        ctx: &crate::StmtContext,
        tp: tidb_proto::diagnosticspb::ServerInfoType,
        extractors: &[tidb_planner::memtable_predicate_extractor::ClusterTableExtractor],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let extractors: Vec<_> = extractors
            .iter()
            .filter(|extractor| !extractor.skip_request)
            .collect();
        if extractors.is_empty() {
            return Ok(Vec::new());
        }
        let Some((client, servers)) = self.cluster_diagnostics_servers(ctx, |tp, addr| {
            extractors.iter().any(|extractor| {
                (extractor.node_types.is_empty() || extractor.node_types.contains(tp))
                    && (extractor.instances.is_empty() || extractor.instances.contains(addr))
            })
        })?
        else {
            return Ok(Vec::new());
        };
        let mut warnings: Vec<String> = Vec::new();
        let rows = client.fetch_server_info(&servers, tp, &mut warnings);
        for warning in warnings {
            ctx.append_warning_parts(1105, &warning);
        }
        Ok(rows)
    }


    /// Go `clusterLogRetriever.retrieve` and `initialize`: a contradiction
    /// reads nothing; otherwise PROCESS is required, and the search needs a
    /// start time, an end time, and at least one pattern, level, instance or
    /// node type. Each matching server's diagnostics `SearchLog` stream is
    /// merged by time; a failing server is a warning.
    pub fn cluster_log_rows(
        &self,
        ctx: &crate::StmtContext,
        extractors: &[tidb_planner::memtable_predicate_extractor::ClusterLogTableExtractor],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let Some(extractor) = extractors.iter().find(|extractor| !extractor.skip_request) else {
            return Ok(Vec::new());
        };
        if !self.process_scoped {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["PROCESS".into()]),
            ));
        }
        let refusal = if extractor.start_time == 0 {
            Some("denied to scan logs, please specified the start time, such as `time > '2020-01-01 00:00:00'`")
        } else if extractor.end_time == 0 {
            Some("denied to scan logs, please specified the end time, such as `time < '2020-01-01 00:00:00'`")
        } else if extractor.patterns.is_empty()
            && extractor.log_levels.is_empty()
            && extractor.instances.is_empty()
            && extractor.node_types.is_empty()
        {
            Some("denied to scan full logs (use `SELECT * FROM cluster_log WHERE message LIKE '%'` explicitly if intentionally)")
        } else {
            None
        };
        if let Some(refusal) = refusal {
            return Err(DriverError::Exec(crate::ExecError::Eval(
                crate::EvalError::Unsupported(refusal),
            )));
        }
        let extractor = extractor.clone();
        let Some((client, servers)) = self.cluster_diagnostics_servers(ctx, |tp, addr| {
            (extractor.node_types.is_empty() || extractor.node_types.contains(tp))
                && (extractor.instances.is_empty() || extractor.instances.contains(addr))
        })?
        else {
            return Ok(Vec::new());
        };
        let request = tidb_proto::diagnosticspb::SearchLogRequest {
            start_time: extractor.start_time,
            end_time: extractor.end_time,
            levels: extractor
                .log_levels
                .iter()
                .map(|level| tidb_sysutil::parse_log_level(level) as i32)
                .collect(),
            patterns: extractor.patterns.clone(),
            target: 0,
        };
        let mut warnings: Vec<String> = Vec::new();
        let rows = client.search_log(&servers, &request, &mut warnings);
        for warning in warnings {
            ctx.append_warning_parts(1105, &warning);
        }
        Ok(rows)
    }


    /// Go's seven-source GetClusterServerInfo in dataForTiDBClusterInfo column order.
    pub fn cluster_info_rows(
        &self,
        ctx: &crate::StmtContext,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use tidb_datatype::Datum;
        let Some(topology) = self.cluster_topology.as_ref() else {
            return Ok(Vec::new());
        };
        let mut warnings: Vec<String> = Vec::new();
        let servers = topology.servers(&mut warnings);
        for warning in warnings {
            ctx.append_warning_parts(1105, &warning);
        }
        let servers = servers.map_err(DriverError::unsupported)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs() as i64);
        let redact = self.hides_cluster_metadata;
        Ok(servers
            .into_iter()
            .map(|info| {
                let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
                let (start, uptime) = if info.start_timestamp > 0 {
                    (
                        info.start_timestamp,
                        go_uptime_string(now - info.start_timestamp),
                    )
                } else {
                    (now, String::new())
                };
                vec![
                    text(&info.server_type),
                    if redact {
                        text(&info.server_id.to_string())
                    } else {
                        text(&info.address)
                    },
                    if redact {
                        Datum::Null
                    } else {
                        text(&info.status_address)
                    },
                    text(&info.version),
                    text(&info.git_hash),
                    if redact {
                        Datum::Null
                    } else {
                        datetime_datum(start)
                    },
                    if redact { Datum::Null } else { text(&uptime) },
                    Datum::UInt(info.server_id),
                ]
            })
            .collect())
    }


    /// Go infoschema.GetInstanceAddr uses local status identity. This does not
    /// discover peers: loss of etcd cannot relabel or hide local process rows.
    pub fn cluster_instance_address(&self) -> String {
        let Some(syncer) = self.server_info_syncer.as_ref() else {
            return String::new();
        };
        let info = syncer.local_server_info();
        if self.hides_cluster_metadata {
            info.static_info.id.clone()
        } else {
            tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
                &info.static_info.ip,
                info.static_info.status_port,
            )
        }
    }


    /// Go `setDataForServersInfo` (`infoschema_reader.go:2730`): one row per
    /// server `GetAllServerInfo` reports, in Go's column order.
    ///
    /// Without a syncer the table is EMPTY rather than invented: a tier with
    /// no node identity has no server to report. With one and no etcd
    /// client, the syncer answers this node alone -- Go's `etcdCli == nil`
    /// path -- and the same call picks up peers once a client is present.
    ///
    /// The rows are ordered by id so the table reads deterministically;
    /// Go's map iteration leaves the order unspecified.
    pub fn tidb_servers_info_rows(
        &self,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use tidb_datatype::Datum;
        let Some(syncer) = self.server_info_syncer.as_ref() else {
            return Ok(Vec::new());
        };
        let all = syncer.all_server_info().map_err(DriverError::unsupported)?;
        let mut ids: Vec<&String> = all.keys().collect();
        ids.sort();
        let redact = self.hides_cluster_metadata;
        Ok(ids
            .into_iter()
            .map(|id| {
                let info = &all[id];
                let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
                vec![
                    text(&info.static_info.id),
                    if redact {
                        Datum::Null
                    } else {
                        text(&info.static_info.ip)
                    },
                    Datum::Int(info.static_info.port as i64),
                    Datum::Int(info.static_info.status_port as i64),
                    text(&info.static_info.lease),
                    text(&info.static_info.version_info.version),
                    text(&info.static_info.version_info.git_hash),
                    text(&tidb_domain_serverinfo::serverinfo_syncer::build_string_from_labels(
                        &info.dynamic_info.labels,
                    )),
                ]
            })
            .collect())
    }


    /// Receiving scan requirements use original column names, including predicates.
    /// None retains full-row callers; a present empty set needs no expensive fields.
    pub fn local_cluster_table_rows_for(
        &self,
        ctx: &crate::StmtContext,
        catalog: &Catalog,
        table_name: &str,
        registry: Option<&tidb_sessmgr::ProcessRegistry>,
        zone: &tidb_datatype::SessionTimeZone,
        required: Option<&std::collections::HashSet<String>>,
        digests: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let source = tidb_infoschema::infoschema_meta::cluster_table_source(table_name)
            .ok_or_else(|| DriverError::unsupported("unsupported TiDB cluster table"))?;
        let mut rows = match source {
            "PROCESSLIST" => {
                if let Some(registry) = registry {
                    crate::memtable_reader::format_process_rows(
                        self.filter_process_rows(registry.snapshot()),
                        zone,
                    )
                } else {
                    crate::memtable_reader::format_process_rows(self.visible_process_rows(true), zone)
                }
            }
            "TRX_SUMMARY" => self.transaction_summary_rows(registry),
            "STATEMENTS_SUMMARY"
            | "STATEMENTS_SUMMARY_HISTORY"
            | "STATEMENTS_SUMMARY_EVICTED"
            | "TIDB_STATEMENTS_STATS" => {
                let columns = tidb_infoschema::infoschema_meta::table_schema(source).expect("registered summary source");
                let offsets: Vec<_> = columns
                    .iter()
                    .enumerate()
                    .filter(|(_, (name, _))| required.is_none_or(|set| set.contains(name)))
                    .map(|(index, _)| index)
                    .collect();
                let selected: Vec<_> = offsets
                    .iter()
                    .map(|index| columns[*index].clone())
                    .collect();
                self.statement_summary_rows(source, &selected, zone, false, digests)?
                    .into_iter()
                    .map(|row| {
                        let mut full = vec![tidb_datatype::Datum::Null; columns.len()];
                        for (index, value) in offsets.iter().zip(row) {
                            full[*index] = value;
                        }
                        full
                    })
                    .collect()
            }
            _ => self.diagnostic_rows(ctx, catalog, source, registry, required)?,
        };
        let instance = self.cluster_instance_address();
        for row in &mut rows {
            row.insert(0, tidb_datatype::Datum::new_string(instance.as_str()));
        }
        Ok(rows)
    }


    /// Local and received diagnostics borrow the same data and admission owners.
    fn diagnostic_rows(
        &self,
        ctx: &crate::StmtContext,
        catalog: &Catalog,
        source: &str,
        registry: Option<&tidb_sessmgr::ProcessRegistry>,
        required: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let needs = |name: &str| required.is_none_or(|set| set.contains(name));
        Ok(match source {
            "TIDB_TRX" => self.tidb_trx_rows(ctx, registry, needs("CURRENT_SQL_DIGEST_TEXT"))?,
            "DEADLOCKS" => {
                if !self.has_process {
                    return Err(DriverError::from(
                        tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                            .generate_with_stack_by_args(&["PROCESS".into()]),
                    ));
                }
                self.deadlock_history_rows(ctx, catalog, 
                    needs("CURRENT_SQL_DIGEST_TEXT"),
                    needs("KEY_INFO"),
                )?
            }
            "MEMORY_USAGE" => crate::memtable_reader::memory_usage_table_rows(),
            "MEMORY_USAGE_OPS_HISTORY" => {
                tidb_servermemorylimit::GLOBAL_MEMORY_OPS_HISTORY_MANAGER.get_rows()
            }
            "TIDB_INDEX_USAGE" => crate::infoschema_reader::tidb_index_usage_rows(
                catalog,
                &self.visibility,
                self.index_usage.as_ref(),
            ),
            // A cluster scan carries no slow-query extractor here: Go's
            // receiving reader then reads every file with no time filter.
            "SLOW_QUERY" => {
                let (rows, warnings) = self.slow_query_rows(&[], &ctx.session_zone())?;
                for warning in warnings {
                    ctx.append_warning_parts(1105, &warning);
                }
                rows
            }
            // Go `setDataFromPlanCache` lists the instance plan cache, which
            // is off by default (`tidb_enable_instance_plan_cache`) and not
            // modelled here.
            "TIDB_PLAN_CACHE" => Vec::new(),
            _ => {
                return Err(DriverError::unsupported(
                    "unsupported local diagnostic table",
                ))
            }
        })
    }


    /// Pinned Go `tidbTrxTableRetriever.retrieve` for this node.
    fn tidb_trx_rows(
        &self,
        ctx: &crate::StmtContext,
        registry: Option<&tidb_sessmgr::ProcessRegistry>,
        needs_text: bool,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use chrono::{DateTime, Local};
        use tidb_datatype::{core_time_from_datetime, Collation, Datum, MysqlEnum, Time, TimeType};

        let Some(registry) =
            registry.or_else(|| self.registry.as_ref())
        else {
            return Ok(Vec::new());
        };
        let login_username = self
            .login_user
            .as_deref()
            .and_then(|identity| identity.split_once('@').map(|(user, _)| user));
        let has_process = self.has_process;
        let own_connection = self.own_process_id;
        let transactions: Vec<_> = registry
            .transaction_snapshot()
            .into_iter()
            .map(|mut transaction| {
                // Go `ExecuteStmt` returns the record set before any row is
                // fetched, and its deferred `LazyTxn.onStmtEnd` has by then
                // made the transaction Idle with no current statement: the
                // statement reading `TIDB_TRX` sees its own row that way.
                // The rows here are built while the statement still runs.
                if Some(transaction.session_id) == own_connection
                    && transaction.state == "Running"
                {
                    transaction.state = "Idle";
                    transaction.current_sql_digest = None;
                }
                transaction
            })
            .filter(|transaction| {
                has_process
                    || login_username.is_none_or(|username| username == transaction.user.as_str())
            })
            .collect();
        let texts = if needs_text {
            self.sql_digest_texts(ctx, 
                transactions
                    .iter()
                    .filter_map(|transaction| transaction.current_sql_digest.clone()),
                false,
            )?
        } else {
            Default::default()
        };
        Ok(transactions
            .into_iter()
            .map(|transaction| {
                let current_sql_digest_text = transaction
                    .current_sql_digest
                    .as_ref()
                    .and_then(|digest| texts.get(digest))
                    .filter(|text| !text.is_empty())
                    .cloned();
                let start = DateTime::from_timestamp_millis((transaction.start_ts >> 18) as i64)
                    .map(DateTime::<Local>::from)
                    .map(|value| {
                        Datum::new_time(
                            Time::new(core_time_from_datetime(value), TimeType::Timestamp, 6)
                                .expect("fsp 6 is valid"),
                        )
                    })
                    .unwrap_or(Datum::Null);
                let waiting_start = transaction
                    .waiting_start
                    .map(DateTime::<Local>::from)
                    .map(|value| {
                        Datum::new_time(
                            Time::new(core_time_from_datetime(value), TimeType::Timestamp, 6)
                                .expect("fsp 6 is valid"),
                        )
                    })
                    .unwrap_or(Datum::Null);
                let waiting_time = transaction
                    .waiting_start
                    .map(|started| {
                        Datum::Real(
                            chrono::Utc::now()
                                .signed_duration_since(started)
                                .num_microseconds()
                                .unwrap_or(0) as f64
                                / 1_000_000.0,
                        )
                    })
                    .unwrap_or(Datum::Null);
                let state_index = match transaction.state {
                    "Idle" => 1,
                    "Running" => 2,
                    "LockWaiting" => 3,
                    "Committing" => 4,
                    "RollingBack" => 5,
                    _ => 1,
                };
                vec![
                    Datum::UInt(transaction.start_ts),
                    start,
                    transaction
                        .current_sql_digest
                        .clone()
                        .map(Datum::new_string)
                        .unwrap_or(Datum::Null),
                    current_sql_digest_text
                        .map(Datum::new_string)
                        .unwrap_or(Datum::Null),
                    Datum::new_enum(
                        MysqlEnum::new(transaction.state, state_index),
                        Collation::Utf8Mb4Bin,
                    ),
                    waiting_start,
                    Datum::UInt(transaction.mem_buffer_keys),
                    Datum::Int(transaction.mem_buffer_bytes),
                    Datum::UInt(transaction.session_id),
                    Datum::new_string(transaction.user),
                    Datum::new_string(transaction.db),
                    Datum::new_string(
                        serde_json::to_string(&transaction.all_sql_digests)
                            .expect("SQL digest strings serialize"),
                    ),
                    Datum::new_string(
                        transaction
                            .related_table_ids
                            .iter()
                            .map(i64::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                    ),
                    waiting_time,
                ]
            })
            .collect())
    }


    /// Pinned Go `dataLockWaitsTableRetriever.retrieve` for pessimistic waits.
    fn data_lock_waits_rows(
        &self,
        ctx: &crate::StmtContext,
        catalog: &Catalog,
        required: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use std::fmt::Write as _;
        use tidb_datatype::Datum;

        if !self.has_process {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["PROCESS".into()]),
            ));
        }
        let Some(provider) = self.data_lock_waits.as_ref().map(std::sync::Arc::clone) else {
            return Ok(Vec::new());
        };
        let waits = provider.lock_waits().map_err(DriverError::unsupported)?;
        let digests: Vec<_> = waits
            .iter()
            .map(|wait| {
                tidb_resourcegrouptag::decode_resource_group_tag(&wait.resource_group_tag)
                    .ok()
                    .flatten()
                    .map(|bytes| {
                        let mut hex = String::with_capacity(bytes.len() * 2);
                        for byte in bytes {
                            write!(&mut hex, "{byte:02x}")
                                .expect("writing hexadecimal to String cannot fail");
                        }
                        hex
                    })
            })
            .collect();
        let texts = if required.is_none_or(|set| set.contains("SQL_DIGEST_TEXT")) {
            self.sql_digest_texts(ctx, digests.iter().flatten().cloned(), true)?
        } else {
            Default::default()
        };
        let key_info = if required.is_none_or(|set| set.contains("KEY_INFO")) {
            waits
                .iter()
                .map(|wait| {
                    tidb_keydecoder::decode_key(&wait.key, catalog)
                        .ok()
                        .and_then(|decoded| serde_json::to_vec(&decoded).ok())
                        .map(Datum::Bytes)
                        .unwrap_or(Datum::Null)
                })
                .collect::<Vec<_>>()
        } else {
            vec![Datum::Null; waits.len()]
        };

        Ok(waits
            .into_iter()
            .zip(key_info)
            .zip(digests)
            .map(|((wait, key_info), digest)| {
                let mut key_hex = String::with_capacity(wait.key.len() * 2);
                for byte in &wait.key {
                    write!(&mut key_hex, "{byte:02X}")
                        .expect("writing hexadecimal to String cannot fail");
                }
                let digest_text = digest
                    .as_ref()
                    .and_then(|digest| texts.get(digest))
                    .filter(|text| !text.is_empty())
                    .cloned()
                    .map(Datum::new_string)
                    .unwrap_or(Datum::Null);
                vec![
                    Datum::new_string(key_hex),
                    key_info,
                    Datum::UInt(wait.txn),
                    Datum::UInt(wait.wait_for_txn),
                    digest.map(Datum::new_string).unwrap_or(Datum::Null),
                    digest_text,
                ]
            })
            .collect())
    }


    /// Each discovered peer consumes the same projected local table owner.
    /// The ordinary SQL executor retains residual predicates after fanout.
    pub fn cluster_table_rows(
        &self,
        ctx: &crate::StmtContext,
        catalog: &Catalog,
        table_name: &str,
        columns: &[(String, FieldType)],
        required: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let rows = self.local_cluster_table_rows_for(ctx, catalog, 
            table_name,
            None,
            &ctx.session_zone(),
            required,
            None,
        )?;
        // The statement's requirements are upper-cased column names; a
        // mixed-case table (CLUSTER_SLOW_QUERY's `Query`) matches them
        // case-insensitively, as Go's column names do.
        let mut offsets: Vec<_> = columns
            .iter()
            .enumerate()
            .filter(|(_, (name, _))| {
                required.is_none_or(|set| {
                    set.contains(name) || set.contains(&name.to_ascii_uppercase())
                })
            })
            .map(|(index, _)| index)
            .collect();
        // A transport column preserves row cardinality for COUNT(*) scans.
        if offsets.is_empty() && !columns.is_empty() {
            offsets.push(0);
        }
        let projected = rows
            .into_iter()
            .map(|row| offsets.iter().map(|index| row[*index].clone()).collect())
            .collect();
        self.cluster_table_rows_with_rows(ctx, table_name, columns, &offsets, projected, false, None)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        let mut full = vec![Datum::Null; columns.len()];
                        for (index, value) in offsets.iter().zip(row) {
                            full[*index] = value;
                        }
                        full
                    })
                    .collect()
            })
    }


    /// Shared discovery, cancellation and warnings for ordinary and restricted readers.
    pub fn cluster_table_rows_with_rows(
        &self,
        ctx: &crate::StmtContext,
        table_name: &str,
        columns: &[(String, FieldType)],
        offsets: &[usize],
        rows: Vec<Vec<Datum>>,
        internal: bool,
        digests: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        // Go carries Username/Hostname (the presented login), not AuthHostname.
        let user = if internal {
            None
        } else {
            self.login_user
                .as_deref()
                .and_then(|identity| identity.split_once('@'))
        };
        let (rows, warnings) = peer_cluster_table_rows(
            self.server_info_syncer.as_deref(),
            self.cluster_peer.as_deref(),
            ctx,
            user,
            &ctx.session_zone(),
            table_name,
            columns,
            offsets,
            rows,
            digests,
        )?;
        for (code, message) in warnings {
            ctx.append_warning_parts(code, &message);
        }
        Ok(rows)
    }


    /// Executor-owned rows of `INFORMATION_SCHEMA.DEADLOCKS`.
    fn deadlock_history_rows(
        &self,
        ctx: &crate::StmtContext,
        catalog: &Catalog,
        needs_text: bool,
        needs_key_info: bool,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_datatype::{Collation, StringDatum};
        use tidb_deadlockhistory::{
            COL_CURRENT_SQL_DIGEST, COL_DEADLOCK_ID, COL_KEY, COL_OCCUR_TIME, COL_RETRYABLE,
            COL_TRX_HOLDING_LOCK, COL_TRY_LOCK_TRX_ID, GLOBAL_DEADLOCK_HISTORY,
        };
        let records = GLOBAL_DEADLOCK_HISTORY.get_all();
        let texts = if needs_text {
            self.sql_digest_texts(ctx, 
                records.iter().flat_map(|record| {
                    record.wait_chain.iter().map(|item| item.sql_digest.clone())
                }),
                true,
            )?
        } else {
            Default::default()
        };
        let render = |catalog: Option<&Catalog>| {
            records
                .into_iter()
                .flat_map(|record| {
                    (0..record.wait_chain.len())
                        .map(|idx| {
                            let item = &record.wait_chain[idx];
                            let digest_text = texts
                                .get(&item.sql_digest)
                                .filter(|text| !text.is_empty())
                                .cloned()
                                .map(Datum::new_string)
                                .unwrap_or(Datum::Null);
                            let key_info = if item.key.is_empty() || !needs_key_info {
                                Datum::Null
                            } else {
                                tidb_keydecoder::decode_key(
                                    &item.key,
                                    catalog.expect("requested key metadata"),
                                )
                                .ok()
                                .and_then(|decoded| serde_json::to_vec(&decoded).ok())
                                .map(|json| {
                                    Datum::String(StringDatum::new(json, Collation::DEFAULT))
                                })
                                .unwrap_or(Datum::Null)
                            };
                            vec![
                                record.to_datum(idx, COL_DEADLOCK_ID),
                                record.to_datum(idx, COL_OCCUR_TIME),
                                record.to_datum(idx, COL_RETRYABLE),
                                record.to_datum(idx, COL_TRY_LOCK_TRX_ID),
                                record.to_datum(idx, COL_CURRENT_SQL_DIGEST),
                                digest_text,
                                record.to_datum(idx, COL_KEY),
                                key_info,
                                record.to_datum(idx, COL_TRX_HOLDING_LOCK),
                            ]
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        if needs_key_info {
            Ok(render(Some(catalog)))
        } else {
            Ok(render(None))
        }
    }

    /// Go `hotRegionsHistoryRetriver.retrieve`: PD's hot-region history for
    /// the scan's extractor, after the PROCESS check.
    fn hot_regions_history_rows(
        &self,
        ctx: &crate::StmtContext,
        catalog: &Catalog,
        extractor: Option<&tidb_planner::memtable_predicate_extractor::HotRegionsHistoryTableExtractor>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let Some(extractor) = extractor else {
            return Ok(Vec::new());
        };
        if extractor.skip_request {
            return Ok(Vec::new());
        }
        if !self.process_scoped {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["PROCESS".into()]),
            ));
        }
        // Go `infoschema.GetPDServerInfo`, whose per-member failures are
        // warnings.
        let mut warnings: Vec<String> = Vec::new();
        let pd_servers = match &self.cluster_topology {
            Some(topology) => topology.pd_servers(&mut warnings),
            None => Ok(Vec::new()),
        };
        for warning in &warnings {
            ctx.append_warning_parts(1105, warning);
        }
        let pd_servers = pd_servers
            .map_err(DriverError::unsupported)?
            .into_iter()
            .map(|server| server.status_address)
            .collect::<Vec<_>>();
        crate::memtable_retrievers::tidb_hot_regions_history_rows(
            extractor,
            &pd_servers,
            catalog,
            ctx.region_store().as_ref(),
            |warning| ctx.append_warning_parts(1105, warning),
        )
    }

    /// Go `clusterServerInfoRetriever.retrieve`: hardware needs CONFIG, load
    /// and system info PROCESS, checked before the extractor's SkipRequest.
    fn cluster_server_info_dispatch(
        &self,
        ctx: &crate::StmtContext,
        table_name: &str,
        extractor: Option<&tidb_planner::memtable_predicate_extractor::MemTablePredicateExtractor>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_hack::GoCaseCompare;
        use tidb_proto::diagnosticspb::ServerInfoType;
        let (allowed, name, tp) = if table_name.go_lower_eq("CLUSTER_HARDWARE") {
            (self.config_scoped, "CONFIG", ServerInfoType::HardwareInfo)
        } else if table_name.go_lower_eq("CLUSTER_LOAD") {
            (self.process_scoped, "PROCESS", ServerInfoType::LoadInfo)
        } else {
            (self.process_scoped, "PROCESS", ServerInfoType::SystemInfo)
        };
        if !allowed {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&[name.to_owned().into()]),
            ));
        }
        let extractors = [extractor
            .and_then(tidb_planner::memtable_predicate_extractor::MemTablePredicateExtractor::as_cluster_table)
            .cloned()
            .unwrap_or_default()];
        self.cluster_server_info_rows(ctx, tp, &extractors)
    }
}

/// The columns a scan reads, upper-cased, which retrievers use to skip
/// expensive fields; a cluster table's scan names them by id (`offset + 1`).
fn required_columns(
    read: &crate::mem_table_retrieval::MemTableRead,
    columns: &[(String, FieldType)],
) -> std::collections::HashSet<String> {
    let mut required: std::collections::HashSet<String> = read
        .columns
        .iter()
        .map(|name| name.to_ascii_uppercase())
        .collect();
    required.extend(read.column_ids.iter().filter_map(|id| {
        let offset = usize::try_from(id - 1).ok()?;
        columns
            .get(offset)
            .map(|(name, _)| name.to_ascii_uppercase())
    }));
    required
}

/// The retriever for the diagnostic and cluster tables.
pub struct ClusterTablesRetriever {
    viewer: ViewerState,
}

impl ClusterTablesRetriever {
    pub fn new(viewer: ViewerState) -> Self {
        Self { viewer }
    }
}

impl crate::mem_table_retrieval::MemTableRetriever for ClusterTablesRetriever {
    fn serves(&self, read: &crate::mem_table_retrieval::MemTableRead) -> bool {
        use tidb_hack::GoCaseCompare;
        if !read
            .db_name
            .go_lower_eq(tidb_infoschema::infoschema_meta::INFORMATION_SCHEMA)
        {
            return false;
        }
        tidb_infoschema::infoschema_meta::cluster_table_source(&read.table_name).is_some()
            || (!read.is_cluster_scan
                && [
                    "TIDB_TRX",
                    "DEADLOCKS",
                    "DATA_LOCK_WAITS",
                    "CLUSTER_LOG",
                    "TIDB_HOT_REGIONS_HISTORY",
                    "CLUSTER_HARDWARE",
                    "CLUSTER_LOAD",
                    "CLUSTER_SYSTEMINFO",
                    "CLUSTER_CONFIG",
                    "TIDB_SERVERS_INFO",
                    "CLUSTER_INFO",
                ]
                .iter()
                .any(|name| read.table_name.go_lower_eq(name)))
    }

    fn retrieve(
        &self,
        read: &crate::mem_table_retrieval::MemTableRead,
        catalog: &Catalog,
        ctx: &crate::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_hack::GoCaseCompare;
        use tidb_planner::memtable_predicate_extractor::{
            ClusterTableFilter, MemTablePredicateExtractor as Extractor,
        };
        let table = read.table_name.as_str();
        let columns = tidb_infoschema::infoschema_meta::table_schema(table);
        let required = columns
            .as_deref()
            .map(|columns| required_columns(read, columns));
        if tidb_infoschema::infoschema_meta::cluster_table_source(table).is_some() {
            let columns = columns.ok_or_else(|| {
                DriverError::unsupported("a cluster table has no declared schema")
            })?;
            return self.viewer.cluster_table_rows(
                ctx,
                catalog,
                table,
                &columns,
                required.as_ref(),
            );
        }
        if table.go_lower_eq("TIDB_TRX") || table.go_lower_eq("DEADLOCKS") {
            return self.viewer.diagnostic_rows(
                ctx,
                catalog,
                &table.to_ascii_uppercase(),
                None,
                required.as_ref(),
            );
        }
        if table.go_lower_eq("DATA_LOCK_WAITS") {
            return self
                .viewer
                .data_lock_waits_rows(ctx, catalog, required.as_ref());
        }
        if table.go_lower_eq("CLUSTER_LOG") {
            let extractors = [read
                .extractor
                .as_ref()
                .and_then(Extractor::as_cluster_log_table)
                .cloned()
                .unwrap_or_default()];
            return self.viewer.cluster_log_rows(ctx, &extractors);
        }
        if table.go_lower_eq("TIDB_HOT_REGIONS_HISTORY") {
            let extractor = match &read.extractor {
                Some(Extractor::HotRegionsHistoryTable(extractor)) => Some(extractor),
                _ => None,
            };
            return self.viewer.hot_regions_history_rows(ctx, catalog, extractor);
        }
        if ["CLUSTER_HARDWARE", "CLUSTER_LOAD", "CLUSTER_SYSTEMINFO"]
            .iter()
            .any(|name| table.go_lower_eq(name))
        {
            return self
                .viewer
                .cluster_server_info_dispatch(ctx, table, read.extractor.as_ref());
        }
        if table.go_lower_eq("CLUSTER_CONFIG") {
            let filters = [ClusterTableFilter::of(read.extractor.as_ref())];
            return self.viewer.cluster_config_rows(ctx, &filters);
        }
        if table.go_lower_eq("TIDB_SERVERS_INFO") {
            return self.viewer.tidb_servers_info_rows();
        }
        // CLUSTER_INFO: Go `memtableRetriever.retrieve` needs PROCESS.
        if !self.viewer.process_scoped {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["PROCESS".into()]),
            ));
        }
        self.viewer.cluster_info_rows(ctx)
    }
}
