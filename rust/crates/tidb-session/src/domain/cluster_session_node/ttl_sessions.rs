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

//! The domain's pooled internal session as Go's `pkg/timer/tablestore` and
//! `pkg/ttl` reach it through `sessionctx.Context`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tidb_datatype::SessionTimeZone;
use tidb_timeutil::TimeZone;
use tidb_ttl_ttlworker::{InfoSchemaRef, Row, SessionVars, Store, TtlSessionContext};

use super::sessions::{ClusterStatsSessionContext, ClusterStatsSessionState, ClusterStatsSqlExecutor};
use super::Domain;

fn time_zone_of(zone: SessionTimeZone) -> TimeZone {
    match zone {
        SessionTimeZone::Local => TimeZone::Local,
        SessionTimeZone::Named(zone) => TimeZone::Named(zone),
        SessionTimeZone::Fixed { name, offset_secs } => TimeZone::Fixed { name, offset_secs },
    }
}

fn read_system(state: &ClusterStatsSessionState, name: &str) -> Result<String, String> {
    state
        .with_session(|session| {
            session
                .session
                .vars()
                .get_system(name)
                .map_err(|error| tidb_domain_core::sys_session::stats_session_error(format!("{error:?}")))
        })
        .map_err(|error| error.to_string())
}

fn read_global(state: &ClusterStatsSessionState, name: &str) -> Result<String, String> {
    state
        .with_session(|session| {
            session
                .session
                .vars()
                .get_global(name)
                .map_err(|error| tidb_domain_core::sys_session::stats_session_error(format!("{error:?}")))
        })
        .map_err(|error| error.to_string())
}

fn location_of(state: &ClusterStatsSessionState) -> TimeZone {
    state
        .with_session(|session| Ok(time_zone_of(session.session.vars().session_time_zone())))
        .unwrap_or(TimeZone::Named(chrono_tz::Tz::UTC))
}

impl tidb_timer_tablestore::store::SessionContext for ClusterStatsSessionContext {
    fn get_enable_index_merge(&self) -> bool {
        read_system(&self.state, tidb_vardef::tidb_vars::TIDB_ENABLE_INDEX_MERGE)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

    fn set_enable_index_merge(&self, enable: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ENABLE_INDEX_MERGE,
            if enable { "ON" } else { "OFF" },
        );
    }

    fn location(&self) -> TimeZone {
        location_of(&self.state)
    }

    fn get_global_system_var(&self, name: &str) -> tidb_timer::error::Result<String> {
        read_global(&self.state, name).map_err(tidb_timer::error::TimerError::message)
    }
}

impl tidb_ttl_ttlworker::SysSessionContext for ClusterStatsSessionContext {
    fn ttl_session_context(&self) -> Arc<dyn TtlSessionContext> {
        let (killer, domain) = self
            .with_session(|session| Ok((session.session.sql_killer(), Arc::clone(&session.node.domain))))
            .expect("a pooled system session is open");
        Arc::new(TtlClusterSessionContext {
            state: Arc::clone(&self.state),
            killer,
            domain,
            sql_executor: ClusterStatsSqlExecutor::new(Arc::clone(&self.state)),
            internal_sql_scan_user_table: AtomicBool::new(false),
        })
    }
}

/// The pooled session's `sessionctx.Context` as `pkg/ttl/session` drives it.
struct TtlClusterSessionContext {
    state: Arc<ClusterStatsSessionState>,
    /// The session's killer, reachable while a statement holds the session.
    killer: Arc<tidb_sqlkiller::SqlKiller>,
    domain: Arc<Domain>,
    /// Go `sctx.GetSQLExecutor()`.
    sql_executor: ClusterStatsSqlExecutor,
    /// Go `SessionVars.InternalSQLScanUserTable`.
    ///
    /// `// boundary:` Go's only consumers gate the predicate-column and
    /// stats-load rules for restricted SQL; the Rust optimizer runs both
    /// for every statement, so the flag is kept and read by nothing.
    internal_sql_scan_user_table: AtomicBool,
}

/// Go `infoschema.InfoSchema` over one catalog image.
struct CatalogInfoSchema(Arc<tidb_infoschema::SessionExtendedInfoSchema>);

impl tidb_ttl_ttlworker::InfoSchema for CatalogInfoSchema {
    fn schema_meta_version(&self) -> i64 {
        self.0.schema_meta_version()
    }

    fn list_ttl_tables(&self) -> Vec<(tidb_ast::CiString, Vec<tidb_model::GoShared<tidb_model::TableInfo>>)> {
        self.0
            .stored_schemas()
            .iter()
            .filter_map(|schema| {
                let schema = schema.read();
                let tables: Vec<_> = schema
                    .deprecated_tables
                    .iter_handles()
                    .flatten()
                    .filter(|table| table.read().ttl_info.is_some())
                    .collect();
                (!tables.is_empty()).then(|| (schema.name.clone(), tables))
            })
            .collect()
    }

    fn table_info_by_name(
        &self,
        schema: &tidb_ast::CiString,
        table: &tidb_ast::CiString,
    ) -> Result<tidb_model::GoShared<tidb_model::TableInfo>, String> {
        self.0
            .table_info_by_name(schema.original(), table.original())
            .map(|(_, table)| table)
            .ok_or_else(|| {
                // Go `infoschema.ErrTableNotExists.FastGenByArgs(schema, table)`.
                format!(
                    "[schema:1146]Table '{}.{}' doesn't exist",
                    schema.original(),
                    table.original()
                )
            })
    }

    fn table_by_id(&self, id: i64) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        self.0.table_by_id(id).map(|(_, table)| table.meta())
    }
}

/// Go `%?` arguments as `ExecuteInternal` binds them.
impl tidb_ttl::session::SessionContext for TtlClusterSessionContext {
    type Row = Row;
    type Store = Store;
    type InfoSchema = InfoSchemaRef;
    type SessionVars = dyn SessionVars;

    fn get_store(&self) -> Store {
        self.domain.region_store.clone()
    }

    fn get_latest_info_schema(&self) -> InfoSchemaRef {
        InfoSchemaRef(Arc::new(CatalogInfoSchema(self.domain.catalog.get_latest())))
    }

    /// Go `SessionInfoSchema()`: the schema the session's statements run
    /// against.
    fn get_txn_info_schema(&self) -> InfoSchemaRef {
        let catalog = self
            .state
            .with_session(|session| {
                Ok(session
                    .session
                    .shared_catalog()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone())
            })
            .map(Arc::new)
            .unwrap_or_else(|_| self.domain.catalog.get_latest());
        InfoSchemaRef(Arc::new(CatalogInfoSchema(catalog)))
    }

    fn get_sql_executor(&self) -> &dyn tidb_sqlexec::SqlExecutor {
        &self.sql_executor
    }

    fn get_session_vars(&self) -> &(dyn SessionVars + 'static) {
        self
    }

    fn get_global_system_var(&self, name: &str) -> tidb_ttl::session::Result<String> {
        read_global(&self.state, name).map_err(tidb_ttl::session::SessionError)
    }

    fn get_session_or_global_system_var(&self, name: &str) -> tidb_ttl::session::Result<String> {
        read_system(&self.state, name).map_err(tidb_ttl::session::SessionError)
    }

    fn time_zone(&self) -> Option<TimeZone> {
        Some(location_of(&self.state))
    }

    fn location(&self) -> TimeZone {
        location_of(&self.state)
    }

    fn sql_killer(&self) -> &tidb_sqlkiller::SqlKiller {
        &self.killer
    }
}

impl SessionVars for TtlClusterSessionContext {
    /// Go `StmtCtx.AffectedRows()` of the session's last statement.
    fn affected_rows(&self) -> u64 {
        self.state
            .with_session(|session| Ok(session.node.ok_packet.affected_rows))
            .unwrap_or_default()
    }

    fn location(&self) -> TimeZone {
        location_of(&self.state)
    }

    fn retry_limit(&self) -> i64 {
        read_system(&self.state, tidb_vardef::tidb_vars::TIDB_RETRY_LIMIT)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_RETRY_LIMIT)
    }

    fn enable_1pc(&self) -> bool {
        read_system(&self.state, tidb_vardef::tidb_vars::TIDB_ENABLE1_PC)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

    fn enable_async_commit(&self) -> bool {
        read_system(&self.state, tidb_vardef::tidb_vars::TIDB_ENABLE_ASYNC_COMMIT)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

    fn isolation_read_engines_are_all(&self) -> bool {
        read_system(&self.state, tidb_vardef::tidb_vars::TIDB_ISOLATION_READ_ENGINES).is_ok_and(|value| {
            let engines: Vec<&str> = value.split(',').map(str::trim).collect();
            ["tidb", "tikv", "tiflash"]
                .iter()
                .all(|engine| engines.iter().any(|candidate| candidate.eq_ignore_ascii_case(engine)))
        })
    }

    fn distsql_scan_concurrency(&self) -> i64 {
        read_system(&self.state, tidb_vardef::tidb_vars::TIDB_DIST_SQL_SCAN_CONCURRENCY)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_DIST_SQL_SCAN_CONCURRENCY)
    }

    fn enable_paging(&self) -> bool {
        read_system(&self.state, tidb_vardef::tidb_vars::TIDB_ENABLE_PAGING)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

    fn set_internal_sql_scan_user_table(&self, value: bool) {
        self.internal_sql_scan_user_table.store(value, Ordering::SeqCst);
    }
}
