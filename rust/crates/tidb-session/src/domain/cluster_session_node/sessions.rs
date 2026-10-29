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

//! The sessions this node's domain runs on, which Go's `session` package
//! creates for it: `CreateSession` over the store (`open_session`,
//! `open_storage_session`), the system and statistics sessions its pools hand
//! out (Go `sysSessionPool`'s factory), the workload repository's sessions
//! and the internal binding writer.

use super::*;

/// Go `session.CreateSession` and `createSessionWithOpt` over the domain: the sessions the session package opens on it.
pub trait DomainSessions {
    /// What every connection's session gets once it is opened, whoever opens
    /// it (Go `createSessionWithOpt`): its connection id, its process-list
    /// entry, the privilege cache, the domain's services and the
    /// global-binding writer.
    fn attach_connection(
        &self,
        session: &mut Session,
        connection_id: u64,
        process: crate::process::ProcessGuard,
    );

    /// Go `Domain.SysSessionPool().Get()` for one internal statement: a
    /// session over this node with the internal identity, whose SQL an
    /// account executor runs in its own transaction.
    fn open_system_session(&self) -> Result<Session, SqlQueryError>;

    fn open_session(&self, context: SessionContext) -> Result<Session, SqlQueryError>;

    /// A session over this node's storage for internal work: no
    /// authentication identity, process-list entry or user transaction.
    fn open_storage_session(&self, connection_id: u64) -> Result<Session, SqlQueryError>;
}

impl DomainSessions for Domain {
    fn attach_connection(
        &self,
        session: &mut Session,
        connection_id: u64,
        process: crate::process::ProcessGuard,
    ) {
        session.set_connection_id(connection_id);
        process.transaction_observer().use_external_owner();
        session.attach_process(connection_id, process);
        session.attach_privileges(self.privileges.clone());
        session.install_domain_services(crate::domain_services::DomainServices {
            ddl: Arc::new(ClusterDdlService {
                ddl: Arc::clone(&self.ddl),
                catalog: Arc::clone(&self.catalog),
            }),
            accounts: Arc::new(ClusterAccountService {
                accounts: Arc::clone(&self.accounts),
                factory: self.binding_session_factory.clone(),
            }),
            sysvars: Arc::new(ClusterSysvarService(Arc::clone(&self.sysvars))),
            analyze: super::statistics::analyze_service(self),
            stats: super::statistics::stats_statement_service(self),
        });
        if let Some(bindings) = &self.bindings {
            session.set_global_binding_cache(bindings.cache());
            session.set_global_binding_writer(Arc::new(InternalBindingWriter {
                factory: self.binding_session_factory.clone(),
                bindings: Arc::clone(bindings),
                connection_id,
            }));
        }
    }

    fn open_system_session(&self) -> Result<Session, SqlQueryError> {
        use std::sync::atomic::{AtomicU64, Ordering};
        // Apart from client connections and the advanced pool's range.
        static NEXT_SYSTEM_CONNECTION_ID: AtomicU64 = AtomicU64::new(1_u64 << 62);
        let connection_id = NEXT_SYSTEM_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let mut session = self.open_session(SessionContext {
            connection_id,
            peer_addr: crate::domain::peer_address::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
            identity: crate::domain::configured_user_store::AuthenticatedIdentity::internal(),
            client_found_rows: false,
            tls_state: None,
            compression_algorithm: tidb_mysql::consts::CompressionNone,
            compression_level: 0,
            cancellation: crate::domain::session_api::ConnectionCancellation::default(),
            close: crate::domain::session_api::ConnectionClose::default(),
        })?;
        session.set_restricted_sql(true);
        Ok(session)
    }

    fn open_session(&self, context: SessionContext) -> Result<Session, SqlQueryError> {
        let mut opened = self.open_storage_session(context.connection_id)?;
        let session = &mut opened;
        let identity = &context.identity;
        session.set_restricted_sql(identity.is_internal());
        session.set_user(
            format!("{}@{}", identity.username(), identity.host()),
            format!("{}@{}", identity.username(), context.peer_addr.host()),
        );
        session.set_client_found_rows(context.client_found_rows);
        session.set_tls_state(context.tls_state);
        session.set_compression_level(context.compression_level);
        session.set_compression_algorithm(context.compression_algorithm);
        if identity.in_sandbox_mode() {
            session.enable_sandbox_mode();
        }
        if identity.privilege_bypassed() {
            session.enable_privilege_bypass();
        }
        // Go's Domain-internal sessions never enter the server's client
        // list (`Server.clients` holds connections, the sys-session pool is
        // separate), so they never appear in SHOW PROCESSLIST. Registering
        // them here leaked every pooled internal session as a permanent
        // `Sleep` row.
        let guard = if identity.is_internal() {
            self.processes
                .register_internal(context.connection_id, session.current_database().to_owned())
        } else {
            self.processes.register(
                context.connection_id,
                identity.username().to_owned(),
                context.peer_addr.process_host(),
                session.current_database().to_owned(),
                Some(Arc::new(ConnectionKillTarget::new(
                    context.cancellation.clone(),
                    context.close.clone(),
                ))),
            )
        };
        self.attach_connection(session, context.connection_id, guard);
        Ok(opened)
    }

    // No authentication identity, process-list entry, or user transaction is
    // copied into internal storage work. Its MDL holds nest in the caller's
    // connection scope, while its storage and transaction are independent.
    fn open_storage_session(&self, connection_id: u64) -> Result<Session, SqlQueryError> {
        // The connection's own snapshot slot and staged writes: one session,
        // one transaction, exactly as Go's session owns one `kv.Transaction`.
        let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
        let buffer = MutationBuffer::new();
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&slot) as _;
        let mut storage = ClusterTableStorage::new(buffer.clone(), handle);
        if let Some(mpp) = self.mpp_client.as_ref() {
            storage = storage.with_mpp_client(Arc::clone(mpp));
        }
        if let Some(client) = self.kv_client.as_ref() {
            storage = storage.with_client(Arc::clone(client));
        }
        let loaded = self.catalog.load();
        // One fully built table set per schema version, shared the same way:
        // each session clones its tables (columns/indexes are Arc-shared) and
        // swaps in only its own storage seam below.
        let template_storage = detached_storage();
        let mut kv_templates = self
            .session_kv_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        kv_templates.reuse(&loaded);
        let mut built = cluster_session_catalog_with_templates(
            &loaded,
            &storage,
            None,
            self.auto_ids.as_ref(),
            &template_storage,
            Some(&mut kv_templates),
        );
        let statistics_loading = ClusterStatisticsLoading {
            view: Arc::clone(&self.statistics_view),
            loader: Arc::new(ClusterStatisticsItemLoader {
                transactions: Arc::clone(&self.transactions),
                catalog: Arc::clone(&self.catalog),
                stats: Arc::clone(&self.stats),
                global_vars: self.global_vars.clone(),
            }),
            workers: Arc::clone(
                self.stats_load_workers
                    .get_or_init(tidb_executor::driver::StatisticsLoadWorkers::new),
            ),
        };
        let stats_handle = statistics_loading.handle();
        // Go `session` owns its store half from birth.
        let node = Box::new(NodeConn {
            domain: self
                .binding_session_factory
                .upgrade()
                .expect("a domain opens sessions from behind its shared Arc"),
            txn: TxnManager::new(
                Arc::clone(&self.transactions),
                Arc::new(NodeSchemaPins(Arc::clone(&self.schema_pins))),
                connection_id,
                buffer.clone(),
            ),
            slot: Arc::clone(&slot),
            follower: catalog_follower::CatalogFollower::new(
                Arc::clone(&self.catalog),
                storage.clone(),
                Arc::clone(&self.auto_ids),
                loaded.schema_version,
                built.skipped,
            ),
            commit_hooks: Box::new({
                let bindings = self.bindings.clone();
                let schema_validator = self.schema_validator.clone();
                move |session, schema_version| {
                    Box::new(NodeCommitHooks {
                        bindings: bindings.clone(),
                        schema_validator: schema_validator.clone(),
                        schema_version,
                        catalog: session.shared_catalog(),
                    })
                }
            }),
            activate_statement_txn: false,
            buffer: buffer.clone(),
            connection_id: connection_id,
            ok_packet: WriteOutcome::default(),
            parked_local_infile: None,
            store: None,
        });
        let mut session = Session::over_catalog(crate::SharedCatalog::new(built.catalog), node);
        session.set_stats_handle(stats_handle);
        session.set_global_id_generator(Arc::new(ClusterGlobalIds {
            transactions: Arc::clone(&self.transactions),
            global_vars: self.global_vars.clone(),
        }));
        if let Some(regions) = &self.region_store {
            session.set_region_store(Arc::clone(regions));
        }
        session.set_restricted_sql(true);
        {
            // Go `Txn(true)`: asking the bound statement snapshot for its
            // timestamp is its activation, which publishes the statement's
            // start ts (`StatementReadTs`).
            let activation_slot = Arc::clone(&slot);
            session.set_statement_txn_activator(Arc::new(move || {
                activation_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .start_ts();
            }));
        }
        {
            let transactions = Arc::clone(&self.transactions);
            let historical_catalog = Arc::clone(&self.catalog);
            let historical_storage = storage.snapshot_only();
            let historical_slot = Arc::clone(&slot);
            let auto_ids = Arc::clone(&self.auto_ids);
            let globals = self.global_vars.clone();
            // Go plans a stale read from the domain's current statistics.
            let statistics_view = Arc::clone(&self.statistics_view);
            session.set_historical_read_provider(Arc::new(move |ts, resource_group, schema| {
                let hold = Arc::new(tidb_txnkv::ACTIVE_START_TS.hold(ts));
                let snapshot = transactions
                    .open_snapshot_at(ts, resource_group)
                    .map_err(tidb_executor::DriverError::unsupported)?;
                let (catalog, snapshot) = if let Some(schema) = schema {
                    (schema.clone(), snapshot)
                } else {
                    let mut metadata = SnapshotMetaSnapshot::new(snapshot);
                    let capacity = globals
                        .get("tidb_schema_version_cache_limit")
                        .ok()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(16);
                    let loaded = historical_catalog
                        .historical_at(&mut metadata, capacity)
                        .map_err(historical_catalog_error)?;
                    let built = cluster_session_catalog_with_templates(
                        &loaded,
                        &historical_storage,
                        Some(Arc::clone(&statistics_view)),
                        auto_ids.as_ref(),
                        &historical_storage,
                        None,
                    );
                    (built.catalog, metadata.into_snapshot())
                };
                // SET retains schema only; each read binds its own native snapshot.
                drop(
                    historical_slot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .bind(snapshot),
                );
                Ok(crate::HistoricalRead {
                    catalog,
                    timestamp_hold: hold,
                })
            }));
        }
        {
            let transactions = Arc::clone(&self.transactions);
            let catalogs = Arc::clone(&self.catalog);
            let connection_storage = storage.snapshot_only();
            let auto_ids = Arc::clone(&self.auto_ids);
            let globals = self.global_vars.clone();
            let statistics_view = Arc::clone(&self.statistics_view);
            let restricted_sql = Arc::new(NodeRestrictedSql {
                factory: self.binding_session_factory.clone(),
            });
            session.set_restricted_sql_executor(Arc::clone(&restricted_sql) as _);
            session.set_snapshot_schema_provider(Arc::new(move |ts, resource_group, validate| {
                let hold = tidb_txnkv::ACTIVE_START_TS.hold(ts);
                let snapshot = transactions
                    .open_snapshot_at(ts, resource_group)
                    .map_err(tidb_executor::DriverError::unsupported)?;
                if validate {
                    crate::gcutil::validate_snapshot(restricted_sql.as_ref(), ts)?;
                }
                let mut metadata = SnapshotMetaSnapshot::new(snapshot);
                let capacity = globals
                    .get("tidb_schema_version_cache_limit")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(16);
                let loaded = catalogs
                    .historical_at(&mut metadata, capacity)
                    .map_err(historical_catalog_error)?;
                let built = cluster_session_catalog_with_templates(
                    &loaded,
                    &connection_storage,
                    Some(Arc::clone(&statistics_view)),
                    auto_ids.as_ref(),
                    &connection_storage,
                    None,
                );
                drop(metadata);
                drop(hold);
                Ok(built.catalog)
            }));
        }
        session.set_plan_cache_invalidation(Arc::clone(&self.plan_cache_invalidation));
        session.set_plan_cache_schema_version(loaded.schema_version as u64);
        session.set_info_schema(Arc::clone(&loaded));
        let accounts = Arc::clone(&self.accounts);
        session.set_privilege_change_notifier(Arc::new(move || {
            accounts.notify_all_privileges_changed();
        }));
        if let Some(syncer) = &self.global_config_syncer {
            session.set_global_config_syncer(Arc::clone(syncer));
        }
        session.set_index_usage_collector(self.stats_usage.index_usage_collector());
        if self
            .stats_usage_workers
            .get()
            .is_some_and(|workers| workers.flush_on_drop)
            && tidb_config::config_tree::config::get_global_config().enable_collect_execution_info
        {
            session.set_session_index_usage_collector(
                self.stats_usage.new_session_index_usage_collector(),
            );
        }
        session.set_stats_collector(self.stats_usage.new_session_stats_item());
        session.set_data_lock_waits_provider(self.data_lock_waits.clone());
        session.set_column_stats_usage_provider(Arc::new(ClusterColumnStatsUsageProvider {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
        }));
        session.set_approximate_table_count_provider(
            Arc::clone(&self.approximate_table_counts) as Arc<_>,
        );
        session.set_analyze_status_provider(Arc::new(ClusterAnalyzeStatusProvider {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            approximate_counts: Arc::clone(&self.approximate_table_counts),
        }));
        session.set_table_storage_stats_provider(Arc::new(ClusterTableStorageStatsProvider {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            row_stats_cache: StatsTableRowCache::new(),
        }));
        if let Some(store) = self.bdr_role_store.get() {
            session.set_bdr_role_store(Arc::clone(store));
        }
        if let Some(store) = self.ddl_job_history_store.get() {
            session.set_ddl_job_history_store(Arc::clone(store));
        }
        session.set_advisory_lock_service(Arc::new(transactions::ClusterAdvisoryLockService::new(
            Arc::clone(&self.transactions),
        )));
        if let Some(syncer) = self.server_info.as_ref() {
            session.set_server_info_syncer(Arc::clone(syncer));
        }
        if let Some(client) = &self.cluster_peer {
            session.set_cluster_peer_client(Arc::clone(client));
        }
        if let Some(client) = &self.cluster_config {
            session.set_cluster_config_client(Arc::clone(client));
        }
        if let Some(client) = &self.cluster_diagnostics {
            session.set_cluster_diagnostics_client(Arc::clone(client));
        }
        if let Some(topology) = &self.cluster_topology {
            session.set_cluster_topology(Arc::clone(topology));
        }
        // `ADMIN SHOW DDL` reports the version this node currently follows,
        // which moves as the reloader picks up peers' changes -- so it is
        // read at statement time rather than captured when the session opens.
        let catalog_versions = Arc::clone(&self.catalog);
        session.set_cluster_schema_version_source(Arc::new(move || {
            catalog_versions.load().schema_version
        }));
        // Go `preprocess.go:2270` writes each bound table into the
        // transaction's `GetRelatedTableForMDL` map; here the driver session
        // reports it into the node's pin registry, which is what the
        // schema-sync acknowledger's per-table gate reads
        // (`RemoveLockDDLJobs`'s check, in `schema_sync.rs`).
        session.set_mdl_related_table_sink(Arc::new(schema_sync::ConnectionMdlSink::new(
            Arc::clone(&self.schema_pins),
            connection_id,
        )));
        if let Some(take_snapshot) = self.take_snapshot.get() {
            session.set_take_snapshot(Arc::clone(take_snapshot));
        }
        if let Some(spill_storage) = self.spill_storage.as_ref() {
            session.set_spill_storage(Arc::clone(spill_storage));
        }
        if let Some(arbitrator) = self.mem_arbitrator.as_ref() {
            session.set_mem_arbitrator(Arc::clone(arbitrator));
        }

        session
            .seed_globals(Arc::new(self.global_vars.clone()))
            .map_err(map_error)?;
        Ok(session)
    }
}


/// Go `GetSnapshotInfoSchema`'s failure as the statement sees it: a meta
/// read's storage error with a SQL identity (`ErrTxnAbortedByGC` for a
/// timestamp behind the txn safe point) is returned unwrapped.
pub(crate) fn historical_catalog_error(
    error: tidb_infoschema::cluster_catalog::ClusterCatalogError,
) -> tidb_executor::DriverError {
    match error {
        tidb_infoschema::cluster_catalog::ClusterCatalogError::Sql(error) => {
            tidb_executor::DriverError::Mysql(error)
        }
        other => tidb_executor::DriverError::unsupported(other.to_string()),
    }
}

pub(crate) struct ClusterStatsSessionState {
    pub(crate) session: Mutex<Option<Session>>,
    global_vars: GlobalSysvars,
    registered: std::sync::atomic::AtomicBool,
}


pub(crate) struct SystemSessionExecution<'a, 's> {
    session: &'a mut ClusterServerSession<'s>,
    connection_id: u64,
}

impl Drop for SystemSessionExecution<'_, '_> {
    fn drop(&mut self) {
        self.session.node.connection_id = self.connection_id;
        self.session.session.set_connection_id(self.connection_id);
    }
}

pub(crate) struct SysProcessTrackGuard {
    process_id: u64,
    untrack: tidb_sqlexec::UntrackSysProc,
}

impl Drop for SysProcessTrackGuard {
    fn drop(&mut self) {
        (self.untrack)(self.process_id);
    }
}

impl ClusterStatsSessionState {
    pub(crate) fn with_session<T>(
        &self,
        callback: impl FnOnce(&mut ClusterServerSession<'_>) -> Result<T, tidb_sqlexec::SqlExecError>,
    ) -> Result<T, tidb_sqlexec::SqlExecError> {
        let mut slot = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = slot
            .as_mut()
            .ok_or_else(|| stats_session_error("system session is closed"))?;
        let mut conn = session.node_conn();
        callback(&mut conn)
    }

    pub(crate) fn materialize(
        &self,
        sql: &str,
    ) -> Result<
        (
            Vec<Vec<tidb_datatype::Datum>>,
            Vec<tidb_resolve::ResultFieldRef>,
        ),
        tidb_sqlexec::SqlExecError,
    > {
        self.with_session(|session| {
            let mut result = session
                .reborrow()
                .execute(sql)
                .map_err(|error| stats_session_error(error.message))?;
            let source = result.source();
            let fields = source.fields()?;
            let mut rows = Vec::new();
            loop {
                let batch = source.next_batch(1024)?;
                if batch.is_empty() {
                    break;
                }
                rows.extend(batch);
            }
            source.finish()?;
            source.close()?;
            Ok((rows, stats_result_fields(fields)))
        })
    }

    fn execute_auto_analyze(
        &self,
        sql: &str,
        options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> Result<
        (
            Vec<Vec<tidb_datatype::Datum>>,
            Vec<tidb_resolve::ResultFieldRef>,
        ),
        tidb_sqlexec::SqlExecError,
    > {
        let option = tidb_sqlexec::exec_option(options);
        if option.analyze_ver != 2 || !option.use_cur_session {
            return Err(stats_session_error(
                "auto analyze requires statistics version 2 in the current session",
            ));
        }
        let track_guard = if let Some(track) = option.track_sys_proc.as_ref() {
            let process = self.with_session(|session| {
                session
                    .session
                    .system_process()
                    .ok_or_else(|| stats_session_error("system session has no process owner"))
            })?;
            track(option.track_sys_proc_id, Arc::new(process))?;
            option
                .untrack_sys_proc
                .clone()
                .map(|untrack| SysProcessTrackGuard {
                    process_id: option.track_sys_proc_id,
                    untrack,
                })
        } else {
            None
        };
        let result = self.with_session(|session| {
            let execution = SystemSessionExecution {
                connection_id: session.node.connection_id,
                session,
            };
            if option.track_sys_proc.is_some() {
                execution.session.node.connection_id = option.track_sys_proc_id;
                execution
                    .session
                    .session
                    .set_connection_id(option.track_sys_proc_id);
            }
            execution
                .session
                .run_auto_analyze_sql(sql, option.analyze_snapshot, &option.partition_prune_mode)
                .map_err(|error| stats_session_error(error.message))?;
            Ok((Vec::new(), Vec::new()))
        });
        drop(track_guard);
        result
    }
}

pub(crate) fn stats_result_fields(
    fields: Vec<tidb_executor::adapter::ResultFieldMetadata>,
) -> Vec<tidb_resolve::ResultFieldRef> {
    fields
        .into_iter()
        .map(|field| {
            let model_column = tidb_model::GoShared::new(tidb_model::ColumnInfo {
                name: tidb_ast::CiString::new(field.org_name.clone()),
                field_type: crate::domain::pipeline_session::field_type_of(&field.field_type),
                ..tidb_model::ColumnInfo::default()
            });
            tidb_model::GoShared::new(tidb_resolve::ResultField {
                column: Some(model_column),
                column_as_name: tidb_ast::CiString::new(field.name),
                empty_org_name: field.empty_org_name,
                table_as_name: tidb_ast::CiString::new(field.table),
                db_name: tidb_ast::CiString::new(field.schema),
                ..tidb_resolve::ResultField::default()
            })
        })
        .collect()
}

pub(crate) struct ClusterStatsSqlExecutor {
    state: Arc<ClusterStatsSessionState>,
}

impl ClusterStatsSqlExecutor {
    fn materialized_record_set(
        &self,
        sql: &str,
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        let (rows, fields) = self.state.materialize(sql)?;
        Ok((!fields.is_empty()).then(|| {
            Box::new(tidb_sqlexec::SimpleRecordSet::new(fields, rows, 1024))
                as Box<dyn tidb_sqlexec::RecordSet>
        }))
    }
}

impl tidb_sqlexec::SqlExecutor for ClusterStatsSqlExecutor {
    fn execute(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> Result<Vec<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        Ok(self.materialized_record_set(sql)?.into_iter().collect())
    }

    fn execute_internal(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_util::sqlescape::SqlArg<'_>],
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        let escaped = tidb_util::sqlescape::escape_sql(sql, arguments)?;
        let escaped = String::from_utf8(escaped)?;
        self.materialized_record_set(&escaped)
    }

    fn execute_stmt(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        let sql = String::from_utf8(statement.original_text().to_vec())?;
        self.materialized_record_set(&sql)
    }
}

impl tidb_sqlexec::RestrictedSqlExecutor for ClusterStatsSqlExecutor {
    fn parse_with_params(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_util::sqlescape::SqlArg<'_>],
    ) -> Result<tidb_ast::Stmt, tidb_sqlexec::SqlExecError> {
        let escaped = tidb_util::sqlescape::escape_sql(sql, arguments)?;
        let escaped = String::from_utf8(escaped)?;
        tidb_parser::parse(&escaped).map_err(|error| stats_session_error(format!("{error:?}")))
    }

    fn exec_restricted_stmt(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
        _options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> Result<
        (
            Vec<Vec<tidb_datatype::Datum>>,
            Vec<tidb_resolve::ResultFieldRef>,
        ),
        tidb_sqlexec::SqlExecError,
    > {
        let sql = String::from_utf8(statement.original_text().to_vec())?;
        self.state.materialize(&sql)
    }

    fn exec_restricted_sql(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        options: &[tidb_sqlexec::OptionFuncAlias],
        sql: &str,
        arguments: &[tidb_util::sqlescape::SqlArg<'_>],
    ) -> Result<
        (
            Vec<Vec<tidb_datatype::Datum>>,
            Vec<tidb_resolve::ResultFieldRef>,
        ),
        tidb_sqlexec::SqlExecError,
    > {
        let escaped = tidb_util::sqlescape::escape_sql(sql, arguments)?;
        let escaped = String::from_utf8(escaped)?;
        if is_analyze_table_sql(&escaped) && !options.is_empty() {
            self.state.execute_auto_analyze(&escaped, options)
        } else {
            self.state.materialize(&escaped)
        }
    }
}

pub(crate) struct ClusterStatsSessionContext {
    pub(crate) state: Arc<ClusterStatsSessionState>,
}

impl ClusterStatsSessionContext {
    pub(crate) fn new(session: Session, global_vars: GlobalSysvars) -> Self {
        Self {
            state: Arc::new(ClusterStatsSessionState {
                session: Mutex::new(Some(session)),
                global_vars,
                registered: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn with_session<T>(
        &self,
        callback: impl FnOnce(&mut ClusterServerSession<'_>) -> Result<T, tidb_sqlexec::SqlExecError>,
    ) -> Result<T, tidb_sqlexec::SqlExecError> {
        self.state.with_session(callback)
    }

    fn set_var(&self, name: &str, value: impl Into<String>) {
        let value = value.into();
        self.with_session(
            |session| match session.session.set_internal_system_var(name, value) {
                Ok(()) => Ok(()),
                Err(error) if error.equal(&tidb_error::catalog::variable::ERR_GLOBAL_VARIABLE) => {
                    Ok(())
                }
                Err(error) => Err(stats_session_error(format!("{error:?}"))),
            },
        )
        .expect("validated global variable must be valid in an internal session");
    }

    pub(crate) fn scalar(&self, sql: &str) -> Result<Option<tidb_datatype::Datum>, String> {
        let (rows, _) = self
            .state
            .materialize(sql)
            .map_err(|error| error.to_string())?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|mut row| (!row.is_empty()).then(|| row.remove(0))))
    }

}

impl tidb_stats_handle_autoanalyze_exec::AutoAnalyzeSessionContext for ClusterStatsSessionContext {
    fn partition_prune_mode(&self) -> String {
        <Self as tidb_stats_handle_util::StatsSessionContext>::partition_prune_mode(self)
    }

    fn enable_analyze_snapshot(&self) -> bool {
        self.with_session(|session| {
            session
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT)
                .map(|value| tidb_vardef::option_values::tidb_opt_on(&value))
                .map_err(|error| stats_session_error(format!("{error:?}")))
        })
        .unwrap_or(false)
    }
}

impl tidb_syssession::SessionContext for ClusterStatsSessionContext {
    fn close(&self) {
        self.state
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    fn rollback_txn(&self, _context: &dyn tidb_sqlexec::ExecutionContext) {
        let _ = self.with_session(|session| {
            session
                .execute_internal("ROLLBACK")
                .map_err(|error| stats_session_error(error.message))?;
            Ok(())
        });
    }

    fn has_prepared_txn_future(&self) -> bool {
        false
    }

    fn txn_valid(&self) -> Result<bool, tidb_sqlexec::SqlExecError> {
        self.with_session(|session| Ok(session.node.txn.explicit.is_some()))
    }

    fn sql_executor(&self) -> Arc<dyn tidb_sqlexec::SqlExecutor> {
        Arc::new(ClusterStatsSqlExecutor {
            state: Arc::clone(&self.state),
        })
    }

    fn restricted_sql_executor(&self) -> Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
        Arc::new(ClusterStatsSqlExecutor {
            state: Arc::clone(&self.state),
        })
    }

    fn register_internal_session(&self) {
        self.state
            .registered
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn unregister_internal_session(&self) {
        self.state
            .registered
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn contains_internal_session(&self) -> bool {
        self.state
            .registered
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn store_internal_session(&self) -> bool {
        self.register_internal_session();
        true
    }
}

impl tidb_stats_handle_util::StatsSessionContext for ClusterStatsSessionContext {
    fn global_system_var(&self, name: &str) -> Result<String, tidb_sqlexec::SqlExecError> {
        self.state
            .global_vars
            .get(name)
            .map_err(|error| stats_session_error(format!("{error:?}")))
    }

    fn set_enable_async_merge_global_stats(&self, enabled: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS,
            if enabled { "ON" } else { "OFF" },
        );
    }

    fn set_analyze_partition_concurrency(&self, concurrency: i64) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY,
            concurrency.to_string(),
        );
    }

    fn set_analyze_version(&self, version: i64) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION,
            version.to_string(),
        );
    }

    fn set_analyze_store_batch_size(&self, value: &str) -> Result<(), tidb_sqlexec::SqlExecError> {
        self.with_session(|session| {
            match session.session.set_internal_system_var(
                tidb_vardef::tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE,
                value,
            ) {
                Ok(()) => Ok(()),
                Err(error) if error.equal(&tidb_error::catalog::variable::ERR_GLOBAL_VARIABLE) => {
                    Ok(())
                }
                Err(error) => Err(stats_session_error(format!("{error:?}"))),
            }
        })
    }

    fn set_enable_historical_stats(&self, enabled: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS,
            if enabled { "ON" } else { "OFF" },
        );
    }

    fn set_partition_prune_mode(&self, mode: &str) {
        self.set_var(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE, mode);
    }

    fn partition_prune_mode(&self) -> String {
        self.with_session(|session| {
            session
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
                .map_err(|error| stats_session_error(format!("{error:?}")))
        })
        .unwrap_or_default()
    }

    fn set_enable_analyze_snapshot(&self, enabled: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT,
            if enabled { "ON" } else { "OFF" },
        );
    }

    fn set_analyze_skip_column_types(&self, value: std::collections::BTreeSet<String>) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES,
            value.into_iter().collect::<Vec<_>>().join(","),
        );
    }

    fn set_skip_missing_partition_stats(&self, enabled: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS,
            if enabled { "ON" } else { "OFF" },
        );
    }

    fn set_lock_wait_timeout(&self, milliseconds: i64) {
        self.set_var(
            "innodb_lock_wait_timeout",
            (milliseconds / 1_000).to_string(),
        );
    }

    fn set_time_zone(&self, value: &str) -> Result<(), tidb_sqlexec::SqlExecError> {
        self.with_session(|session| {
            session
                .session
                .set_internal_system_var("time_zone", value)
                .map_err(|error| stats_session_error(format!("{error:?}")))
        })
    }

    fn location(&self) -> String {
        self.with_session(|session| {
            session
                .session
                .vars()
                .get_system("time_zone")
                .map_err(|error| stats_session_error(format!("{error:?}")))
        })
        .unwrap_or_else(|_| "SYSTEM".to_owned())
    }

    fn set_statement_time_zone(&self, _value: &str) {
        // Rust constructs its statement context from SessionVars at every
        // statement boundary, so set_time_zone above updates both authorities.
    }

    fn transaction_start_ts(&self, active: bool) -> Result<u64, tidb_sqlexec::SqlExecError> {
        self.with_session(|session| {
            if let Some(transaction) = session.node.txn.explicit.as_ref() {
                return Ok(transaction.start_ts());
            }
            if active {
                return Err(stats_session_error(
                    "system-session transaction has not been activated",
                ));
            }
            Ok(0)
        })
    }
}


pub(crate) struct InternalBindingWriter {
    pub(crate) factory: Weak<Domain>,
    pub(crate) bindings: Arc<dyn crate::domain::cluster_binding_seam::ClusterBindings>,
    pub(crate) connection_id: u64,
}

impl crate::binding_arm::GlobalBindingWriter for InternalBindingWriter {
    fn execute(
        &self,
        operation: &mut dyn FnMut(&mut Session) -> Result<u64, tidb_executor::DriverError>,
    ) -> Result<u64, tidb_executor::DriverError> {
        let to_driver = |error: SqlQueryError| {
            tidb_executor::DriverError::Mysql(
                tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
                    .with_identity_opt(error.identity),
            )
        };
        let factory = self.factory.upgrade().ok_or_else(|| {
            tidb_executor::DriverError::unsupported("global binding session factory is stopped")
        })?;
        let mut internal_session = factory
            .open_storage_session(self.connection_id)
            .map_err(to_driver)?;
        let mut internal = internal_session.node_conn();
        internal
            .session
            .set_global_binding_cache(self.bindings.cache());
        internal.session.enable_privilege_bypass();
        internal
            .execute_internal("BEGIN PESSIMISTIC")
            .map_err(to_driver)?;
        let result = (|| {
            // Use Go's shared record-lock statement so binding writers on
            // different nodes serialize through the same storage transaction.
            internal.execute_internal(crate::binding_utils::LOCK_BIND_INFO_SQL)?;
            let resource_group = internal.session.current_resource_group().to_owned();
            let changed = internal.with_prelocked_statement(
                StatementReadShape::LockingRead,
                &resource_group,
                true,
                |session| operation(session).map_err(map_error),
            )?;
            internal.execute_internal("COMMIT")?;
            Ok(changed)
        })();
        match result {
            Ok(changed) => {
                // Do not replay a successful commit if cache refresh fails.
                if let Err(error) = self.bindings.reload() {
                    eprintln!("committed binding cache reload failed: {error}");
                }
                Ok(changed)
            }
            Err(error) => {
                let _ = internal.execute_internal("ROLLBACK");
                Err(to_driver(error))
            }
        }
    }
}

/// Go `createSessionFunc`'s session as the domain's workers use it.
impl tidb_domain_core::sys_session::DomainSysSession for ClusterStatsSessionContext {
    fn scalar(&self, sql: &str) -> Result<Option<tidb_datatype::Datum>, String> {
        ClusterStatsSessionContext::scalar(self, sql)
    }

    fn execute_internal_write(&self, sql: &str) -> Result<u64, String> {
        self.with_session(|session| {
            session
                .execute_internal(sql)
                .map(|_| session.node.ok_packet.affected_rows)
                .map_err(|error| stats_session_error(error.message))
        })
        .map_err(|error| error.to_string())
    }

    fn query_internal(&self, sql: &str) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.state
            .materialize(sql)
            .map(|(rows, _)| rows)
            .map_err(|error| error.to_string())
    }

    fn handle_stats_ddl_event(
        &self,
        event: &tidb_ddl_notifier::SchemaChangeEvent,
    ) -> Result<(), String> {
        self.with_session(|session| {
            session
                .stage_stats_notifier_event(event)
                .map_err(|error| stats_session_error(error.message))
        })
        .map_err(|error| error.to_string())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// The session package's concrete session behind a pooled
/// [`tidb_domain_core::sys_session::DomainSysSession`].
pub(crate) fn cluster_stats_session(
    context: &dyn tidb_domain_core::sys_session::DomainSysSession,
) -> &ClusterStatsSessionContext {
    context
        .as_any()
        .downcast_ref::<ClusterStatsSessionContext>()
        .expect("the domain's pool holds the session package's sessions")
}

/// Go `createSessionFunc` for the domain's system-session pool: one internal
/// session over the store, which [`Domain::new`] receives as its
/// `sys_session_opener`.
pub fn open_pool_session(
    domain: &Arc<Domain>,
    connection_id: u64,
) -> Result<Arc<dyn tidb_domain_core::sys_session::DomainSysSession>, String> {
    let global_vars = domain.global_vars.clone();
    let session = domain
        .open_session(SessionContext {
            connection_id,
            peer_addr: crate::domain::peer_address::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
            identity: crate::domain::configured_user_store::AuthenticatedIdentity::internal(),
            client_found_rows: false,
            tls_state: None,
            compression_algorithm: tidb_mysql::consts::CompressionNone,
            compression_level: 0,
            cancellation: crate::domain::session_api::ConnectionCancellation::default(),
            close: crate::domain::session_api::ConnectionClose::default(),
        })
        .map_err(|error| error.message)?;
    Ok(Arc::new(ClusterStatsSessionContext::new(session, global_vars)))
}
