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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Administrative-statement execution boundaries.

use tidb_ast::AdminStmt;

use crate::{Database, ExecError, Outcome};

impl Database {
    pub(super) fn run_admin(&mut self, stmt: &AdminStmt) -> Result<Outcome, ExecError> {
        match stmt {
            // DO evaluates expressions solely for their side effects and
            // diagnostics. The seed has neither a discard-result protocol
            // nor a warning surface, so reject before evaluation or state
            // mutation rather than fabricate successful execution.
            AdminStmt::Do(_) => Err(ExecError::Unsupported("DO")),
            // GRANT requires authenticated principals, a persistent grant
            // graph, privilege checks, and invalidation of privilege caches.
            // This seed has none of that state, so reject before any catalog
            // or transaction mutation rather than pretending a grant applied.
            AdminStmt::Grant(_) => Err(ExecError::Unsupported("GRANT")),
            // Role membership mutates the same durable account/role graph as
            // privilege grants, but Go gives it a distinct AST and executor
            // path. Keep that distinction at the unsupported boundary too.
            AdminStmt::GrantRole(_) => Err(ExecError::Unsupported("GRANT ROLE")),
            // REVOKE needs the same durable privilege graph and cache
            // invalidation machinery as GRANT. Reject before any transaction
            // or catalog mutation until that subsystem is real.
            AdminStmt::Revoke(_) => Err(ExecError::Unsupported("REVOKE")),
            AdminStmt::RevokeRole(_) => Err(ExecError::Unsupported("REVOKE ROLE")),
            // SHOW GRANTS requires current-account resolution, active-role
            // expansion, and a durable privilege graph. This seed has none
            // of that state, so never fabricate result rows or mutate state.
            AdminStmt::ShowGrants(_) => Err(ExecError::Unsupported("SHOW GRANTS")),
            // SHOW MASTER STATUS reads the server's binlog position and GTID
            // state. The seed has no binlog subsystem or result schema, so
            // reject before transaction mutation rather than fabricate rows.
            AdminStmt::ShowMasterStatus => Err(ExecError::Unsupported("SHOW MASTER STATUS")),
            // SHOW PRIVILEGES reads TiDB's privilege registry and
            // compatibility metadata. The seed owns neither, so reject
            // before transaction mutation rather than return an incomplete
            // privilege list.
            AdminStmt::ShowPrivileges => Err(ExecError::Unsupported("SHOW PRIVILEGES")),
            // SHOW BUILTINS reads TiDB's builtin-function registry and result
            // schema. The seed owns neither, so reject before transaction
            // mutation rather than fabricate an incomplete list.
            AdminStmt::ShowBuiltins => Err(ExecError::Unsupported("SHOW BUILTINS")),
            // FLUSH STATUS resets session status counters, while FLUSH TABLES
            // coordinates table-handle invalidation and optional global read
            // locks. This seed owns none of that state, so reject before any
            // transaction mutation rather than make either command a no-op.
            AdminStmt::Flush(flush) => match flush.as_ref() {
                tidb_ast::FlushStmt::Status => Err(ExecError::Unsupported("FLUSH STATUS")),
                tidb_ast::FlushStmt::Tables { .. } => Err(ExecError::Unsupported("FLUSH TABLES")),
                tidb_ast::FlushStmt::Privileges => Err(ExecError::Unsupported("FLUSH PRIVILEGES")),
            },
            // Scoped plan-cache eviction requires TiDB's prepared-plan cache,
            // session registry, and cross-instance invalidation protocol.
            // Reject before transaction mutation until those subsystems exist.
            AdminStmt::FlushPlanCache(scope) => Err(ExecError::Unsupported(match scope {
                tidb_ast::AdminPlanCacheScope::Session => "ADMIN FLUSH SESSION PLAN_CACHE",
                tidb_ast::AdminPlanCacheScope::Global => "ADMIN FLUSH GLOBAL PLAN_CACHE",
            })),
            // Reloading optimizer blacklists, bindings, or statistics mutates
            // process-wide caches outside this seed's catalog model. Reject
            // before transaction/catalog mutation rather than fake a reload.
            AdminStmt::Reload(_) => Err(ExecError::Unsupported("ADMIN RELOAD")),
            AdminStmt::LoadStats(_) => Err(ExecError::Unsupported("LOAD STATS")),
            AdminStmt::DropStats(_) => Err(ExecError::Unsupported("DROP STATS")),
            // Splitting needs TiKV key encoding, region placement, scatter,
            // and wait semantics. Reject before transaction/catalog mutation
            // rather than treating a physical region operation as a no-op.
            AdminStmt::SplitRegion(_) => Err(ExecError::Unsupported("SPLIT TABLE")),
            // BDR controls alter cluster-wide replication metadata and gate
            // later DDL. This seed has neither distributed metadata nor its
            // DDL restriction policy, so reject before transaction mutation.
            AdminStmt::SetBdrRole(_) => Err(ExecError::Unsupported("ADMIN SET BDR ROLE")),
            AdminStmt::UnsetBdrRole => Err(ExecError::Unsupported("ADMIN UNSET BDR ROLE")),
            // Showing the BDR role reads cluster-wide replication metadata,
            // which this single-node seed does not own. Reject before any
            // transaction/catalog mutation rather than invent a role row.
            AdminStmt::ShowBdrRole => Err(ExecError::Unsupported("ADMIN SHOW BDR ROLE")),
            // Showing slow statements needs TiDB's statement-summary history,
            // aggregation policy, privilege filtering, and result layout.
            // Reject before transaction mutation rather than fabricate rows.
            AdminStmt::ShowSlow(_) => Err(ExecError::Unsupported("ADMIN SHOW SLOW")),
            // Bare DDL inspection reads TiDB's live owner and job metadata.
            // The seed has none of that distributed state, so reject before
            // transaction mutation rather than return partial owner data.
            AdminStmt::ShowDdl => Err(ExecError::Unsupported("ADMIN SHOW DDL")),
            // DDL job inspection requires TiDB's persistent DDL job queue,
            // job-history retention, and metadata result schema. Reject before
            // transaction mutation rather than inventing queue rows.
            AdminStmt::ShowDdlJobs(_) => Err(ExecError::Unsupported("ADMIN SHOW DDL JOBS")),
            // DDL job-query history likewise requires TiDB's persistent DDL
            // queue and result schema, including range semantics. Reject
            // before transaction mutation rather than fabricate query text.
            AdminStmt::ShowDdlJobQueries(_) => {
                Err(ExecError::Unsupported("ADMIN SHOW DDL JOB QUERIES"))
            }
            // Queue controls need TiDB's persistent DDL queue, owner leases,
            // job state transitions, and history semantics. Reject before
            // transaction/catalog mutation rather than pretend a job moved.
            AdminStmt::DdlJobControl(control) => Err(ExecError::Unsupported(match control.kind {
                tidb_ast::AdminDdlJobControlKind::Cancel => "ADMIN CANCEL DDL JOBS",
                tidb_ast::AdminDdlJobControlKind::Pause => "ADMIN PAUSE DDL JOBS",
                tidb_ast::AdminDdlJobControlKind::Resume => "ADMIN RESUME DDL JOBS",
            })),
            // Altering a queued DDL job needs the persistent queue, owner
            // lease, and job-option validation machinery. Reject before any
            // transaction/catalog mutation rather than pretending options
            // were applied.
            AdminStmt::AlterDdlJobs(_) => Err(ExecError::Unsupported("ADMIN ALTER DDL JOBS")),
            // Next-row-ID inspection needs TiDB auto-ID allocators, table
            // metadata, and privilege-aware result formatting. This seed has
            // none, so reject before transaction mutation rather than return
            // a synthetic counter.
            AdminStmt::ShowNextRowId(_) => Err(ExecError::Unsupported("ADMIN SHOW NEXT_ROW_ID")),
            AdminStmt::ShowCreate { .. } | AdminStmt::ShowCreateUser(_) => {
                Err(ExecError::Unsupported("SHOW CREATE"))
            }
            AdminStmt::ShowVariables { .. } => Err(ExecError::Unsupported("SHOW VARIABLES")),
            // Status variables come from TiDB's session/global instrumentation
            // registry. The seed owns neither registry nor result layout, so
            // reject before transaction mutation rather than synthesize rows.
            AdminStmt::ShowStatus(_) => Err(ExecError::Unsupported("SHOW STATUS")),
            AdminStmt::ShowWarnings(_) => Err(ExecError::Unsupported("SHOW WARNINGS")),
            // SHOW ERRORS reads the session diagnostics area. The seed does
            // not preserve statement warnings/errors or the count-only result
            // shape, so reject before transaction mutation.
            AdminStmt::ShowErrors(_) => Err(ExecError::Unsupported("SHOW ERRORS")),
            // SHOW COLLATION requires TiDB's server collation catalog,
            // compatibility-version rules, and its information-schema result
            // layout. The seed has none of those, so reject instead of
            // fabricating incomplete metadata rows.
            AdminStmt::ShowCollation(_) => Err(ExecError::Unsupported("SHOW COLLATION")),
            // SHOW ENGINES reads the server's storage-engine registry and
            // capability metadata. The seed owns neither, so reject before
            // transaction mutation rather than fabricate engine rows.
            AdminStmt::ShowEngines(_) => Err(ExecError::Unsupported("SHOW ENGINES")),
            // SHOW CHARSET reads TiDB's charset catalog and compatibility
            // aliases. The seed has no catalog/result schema, so reject
            // before transaction mutation rather than fabricate rows.
            AdminStmt::ShowCharset(_) => Err(ExecError::Unsupported("SHOW CHARSET")),
            // Histogram rows come from TiDB's statistics metadata and depend
            // on analyze state, schema visibility, and result formatting.
            // This seed owns none of those, so reject before transaction
            // mutation rather than manufacture a partial statistics view.
            AdminStmt::ShowStatsHistograms(_) => {
                Err(ExecError::Unsupported("SHOW STATS_HISTOGRAMS"))
            }
            // Histogram-bucket rows require TiDB's persisted statistics
            // metadata and result-schema semantics, so reject before state
            // mutation rather than manufacture partial data.
            AdminStmt::ShowStatsBuckets(_) => Err(ExecError::Unsupported("SHOW STATS_BUCKETS")),
            // This reads TiDB's durable stats lock metadata and needs its
            // privilege-filtered virtual-table result schema. The seed owns
            // neither, so reject before transaction/catalog mutation.
            AdminStmt::ShowStatsLocked(_) => Err(ExecError::Unsupported("SHOW STATS_LOCKED")),
            // TopN rows require TiDB's statistics metadata, encoded values,
            // schema visibility, and result formatting. The seed owns none of
            // that, so reject before transaction mutation rather than return a
            // partial or synthetic statistics view.
            AdminStmt::ShowStatsTopN(_) => Err(ExecError::Unsupported("SHOW STATS_TOPN")),
            // SHOW DATABASES is information-schema and privilege dependent;
            // this seed has neither a database namespace nor schema
            // visibility state, so reject before transaction mutation.
            AdminStmt::ShowDatabases(_) => Err(ExecError::Unsupported("SHOW DATABASES")),
            // SHOW TABLES needs TiDB's schema visibility rules, temporary and
            // system-table handling, and its information-schema result layout.
            // Reject before transaction mutation rather than fabricate rows.
            AdminStmt::ShowTables(_) => Err(ExecError::Unsupported("SHOW TABLES")),
            // SHOW OPEN TABLES reports the server's open-table cache and
            // metadata locks, neither of which exists in this seed.
            AdminStmt::ShowOpenTables(_) => Err(ExecError::Unsupported("SHOW OPEN TABLES")),
            // SHOW TABLE STATUS additionally exposes engine/table statistics
            // from information schema. The seed owns neither that metadata nor
            // TiDB's visibility rules, so reject before transaction mutation.
            AdminStmt::ShowTableStatus(_) => Err(ExecError::Unsupported("SHOW TABLE STATUS")),
            AdminStmt::ShowTableNextRowId(_) => {
                Err(ExecError::Unsupported("SHOW TABLE NEXT_ROW_ID"))
            }
            // SHOW COLUMNS needs TiDB's information-schema metadata,
            // privileges, generated-column details, and SHOW result layout.
            // Reject before transaction mutation rather than fabricate rows.
            AdminStmt::ShowColumns(_) => Err(ExecError::Unsupported("SHOW COLUMNS")),
            // SHOW INDEX needs named secondary-index descriptors, generated
            // expressions, visibility/global/clustered flags, statistics,
            // and privilege-aware schema lookup. This executor has none of
            // those, so reject before touching transaction state rather than
            // inventing empty or partial metadata rows.
            AdminStmt::ShowIndex(_) => Err(ExecError::Unsupported("SHOW INDEX")),
            AdminStmt::CreateBinding(_) => Err(ExecError::Unsupported("CREATE BINDING")),
            AdminStmt::DropBinding(_) => Err(ExecError::Unsupported("DROP BINDING")),
            AdminStmt::SetBinding(_) => Err(ExecError::Unsupported("SET BINDING")),
            AdminStmt::ShowBindings(_) => Err(ExecError::Unsupported("SHOW BINDINGS")),
            AdminStmt::AnalyzeTable(_) => Err(ExecError::Unsupported("ANALYZE TABLE")),
            AdminStmt::AnalyzeIncremental(_) => Err(ExecError::Unsupported("ANALYZE INCREMENTAL")),
            AdminStmt::Traffic(traffic) => Err(ExecError::Unsupported(match traffic.as_ref() {
                tidb_ast::TrafficStmt::Capture { .. } => "TRAFFIC CAPTURE",
                tidb_ast::TrafficStmt::Replay { .. } => "TRAFFIC REPLAY",
                tidb_ast::TrafficStmt::ShowJobs => "SHOW TRAFFIC JOBS",
                tidb_ast::TrafficStmt::CancelJobs => "CANCEL TRAFFIC JOBS",
            })),
            AdminStmt::RefreshStats(_) => Err(ExecError::Unsupported("REFRESH STATS")),
            AdminStmt::AdminCheck(check) => match check.as_ref() {
                tidb_ast::AdminCheckStmt::Table { .. } => {
                    Err(ExecError::Unsupported("ADMIN CHECK TABLE"))
                }
                tidb_ast::AdminCheckStmt::Index { .. } => {
                    Err(ExecError::Unsupported("ADMIN CHECK INDEX"))
                }
            },
            // Checksums scan encoded TiKV table/index key ranges and merge
            // distributed CRC/KV/byte responses. The seed only has decoded
            // in-memory rows, so reject before any transaction mutation.
            AdminStmt::AdminChecksum(_) => Err(ExecError::Unsupported("ADMIN CHECKSUM TABLE")),
            // Index recovery scans physical table records, backfills encoded
            // index keys, and reports repair counts. This seed has neither
            // durable secondary indexes nor TiKV key encodings, so reject
            // before touching an active transaction.
            AdminStmt::AdminRecoverIndex(_) => Err(ExecError::Unsupported("ADMIN RECOVER INDEX")),
            // Cleanup releases stale metadata locks owned by named tables.
            // The seed has no lock-manager state, so reject before touching
            // the catalog or active transaction rather than silently making
            // this destructive administrative command a no-op.
            AdminStmt::CleanupTableLock(_) => {
                Err(ExecError::Unsupported("ADMIN CLEANUP TABLE LOCK"))
            }
            // Statistics locks are durable optimizer metadata. The seed has
            // neither analyze statistics nor their lock state, so reject
            // before altering transaction or catalog state.
            AdminStmt::LockStats(_) => Err(ExecError::Unsupported("LOCK STATS")),
            AdminStmt::UnlockStats(_) => Err(ExecError::Unsupported("UNLOCK STATS")),
            // A real EXPLAIN path requires PlanBuilder, optimization, physical
            // plan flattening, and ExplainExec. Do not synthesize those plans
            // from the parsed AST inside this seed executor.
            AdminStmt::Explain(_) => Err(ExecError::Unsupported("EXPLAIN")),
            // Plan Replayer needs planner traces, statistics snapshots, and
            // durable artifact storage. The seed executor has none of those,
            // so reject before any transaction or catalog mutation.
            AdminStmt::PlanReplayerDumpExplain(_) => {
                Err(ExecError::Unsupported("PLAN REPLAYER DUMP EXPLAIN"))
            }
            AdminStmt::DescribeTable(_) => Err(ExecError::Unsupported("DESC")),
        }
    }
}
