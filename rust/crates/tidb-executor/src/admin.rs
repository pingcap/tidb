//! Go's ADMIN executors and the `SimpleExec` arms that touch no user table.
//!
//! `ADMIN CHECK TABLE` / `ADMIN CHECK INDEX`: the `AdminStmt::AdminCheck` arm
//! of the session's ADMIN dispatch.
//!
//! The check itself lives in `crate::admin_check` (which mirrors Go
//! `pkg/executor/check_table_index.go`); this file is only the statement
//! seam -- name resolution, the output SHAPE, and the refusals.
//!
//! # The shapes, from TiDB's own recording
//!
//! From `tests/integrationtest/r/util/admin.result` and
//! `tests/integrationtest/r/executor/admin.result`, which are TiDB's oracle
//! and are only ever read here:
//!
//! - `admin check table t;` -- NO output at all: Go's `CheckTable` plan has
//!   an EMPTY schema, so the server sends an OK packet, not a zero-column
//!   result set (see [`admin_check_passed`]). Producing nothing is the whole
//!   success contract, which is exactly why the check underneath must be
//!   real: an `Ok` returned without reading a byte is indistinguishable from
//!   a passing check, and would be a success return that is not one.
//! - `admin check index t idx;` -- also no output; the same consistency
//!   check, narrowed to one index.
//! - `admin check index t idx (2, 4);` -- a RESULT SET: the indexed columns
//!   followed by `extra_handle`, in index order, for the rows whose handle
//!   falls in a half-open interval.
//!
//! # What this refuses, by name
//!
//! A table whose rows are not stored as index-bearing bytes has no index
//! entries to check against, so `ADMIN CHECK` over one is REFUSED rather than
//! answered OK. Same for the multi-table `ADMIN CHECK TABLE t1, t2` form,
//! which Go's own planner rejects.

use tidb_meta::BdrRoleStore;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::KvTableDdl;
use crate::DriverError;
use crate::StmtOutput;

/// Go `SimpleExec.executeFlush`.
///
/// Most targets are accepted and do nothing observable here: the counters and
/// caches they reset are per-instance, and this node re-reads accounts from
/// the cluster rather than holding the privilege cache Go notifies. The two
/// that are NOT no-ops keep Go's exact answers.
pub fn flush_stmt(flush: &tidb_ast::FlushStmt) -> Result<StmtOutput, DriverError> {
    match &flush.target {
        tidb_ast::FlushTarget::Tables { read_lock, .. } => {
            if *read_lock {
                // Go returns this as a plain error, double space and all.
                return Err(DriverError::unsupported(
                    "FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot",
                ));
            }
        }
        // Go `plugin.NotifyFlush` fails for a name no loaded plugin answers
        // to, and no plugin framework runs here, so every name fails.
        tidb_ast::FlushTarget::TiDbPlugins(plugins) => {
            if let Some(name) = plugins.first() {
                return Err(DriverError::unsupported(format!(
                    "plugin '{name}' not found"
                )));
            }
        }
        // Go `executeFlushStatsDeltaOnCurrentInstance` dumps the domain's
        // buffered deltas through the statistics handle: the session's node
        // routes the statement to its statistics service.
        tidb_ast::FlushTarget::StatsDelta { .. } => {
            return Err(DriverError::unsupported(
                "statistics statements run on the session's node",
            ));
        }
        tidb_ast::FlushTarget::ClientErrorsSummary => {
            tidb_errno::infoschema::flush_stats();
        }
        tidb_ast::FlushTarget::Status
        | tidb_ast::FlushTarget::Privileges
        | tidb_ast::FlushTarget::Hosts
        | tidb_ast::FlushTarget::Logs(_) => {}
    }
    Ok(StmtOutput::Done(true))
}

/// Go `ShowDDLExec`'s six columns. The rows come from the session, which owns
/// the node identity and the followed schema version.
pub fn show_ddl_output(rows: &[Vec<Datum>]) -> StmtOutput {
    let varchar = |size: i64| FieldType::new(FieldTypeCode::Varchar).with_flen(size);
    StmtOutput::Rows {
        columns: vec![
            (
                "SCHEMA_VER".to_owned(),
                FieldType::new(FieldTypeCode::LongLong).with_flen(4),
            ),
            ("OWNER_ID".to_owned(), varchar(64)),
            ("OWNER_ADDRESS".to_owned(), varchar(32)),
            ("RUNNING_JOBS".to_owned(), varchar(256)),
            ("SELF_ID".to_owned(), varchar(64)),
            ("QUERY".to_owned(), varchar(256)),
        ],
        rows: rows.to_vec(),
    }
}

/// Go `ShowImports`, which `buildShowSchema` has no case for: this tier
/// answers the empty `Job_ID | State` listing.
pub fn show_imports_output() -> StmtOutput {
    StmtOutput::Rows {
        columns: vec![
            ("Job_ID".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ("State".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
        ],
        rows: Vec::new(),
    }
}

/// Go's ADMIN executors (`ShowDDLJobsExec`, `ShowDDLJobQueriesExec`,
/// `ShowSlowExec`, `ChecksumTableExec`, `SplitTableRegionExec`,
/// `CancelDDLJobsExec` and its siblings) over the session primitives.
/// Go `executor.TakeSnapshot`: the workload repository's manual snapshot,
/// installed by the repository when it is set up (`SetupRepository`).
pub type TakeSnapshot = std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

pub trait AdminExec: crate::index_advisor::IndexAdvisorExec {
    /// Go `DDLJobRetriever.initial`: the running jobs, then the meta
    /// history newest first, read on a fresh system-session transaction.
    fn all_ddl_jobs(&mut self) -> Result<(Vec<tidb_model::Job>, Vec<tidb_model::Job>), DriverError>;
    /// Runs `attempt` over the statement's catalog, answering the memory
    /// tables it asks for (Go's `MemTableReaderExec` retrievers) and
    /// retrying.
    fn with_catalog_retrieving<T>(
        &mut self,
        ctx: &crate::StmtContext,
        attempt: impl FnMut(&mut crate::Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError>;

    /// Resolves a written table path against the current database.
    fn split_table_path(
        &self,
        path: &[String],
    ) -> Result<(String, String), DriverError> {
        match path {
            [name] => Ok((self.require_current_database()?.to_owned(), name.clone())),
            [database, name] => Ok((database.clone(), name.clone())),
            _ => Err(DriverError::unsupported("empty table name")),
        }
    }

    /// go `SplitTableRegionExec` / `SplitIndexRegionExec`
    /// (`pkg/executor/split.go`): one `(TOTAL_SPLIT_REGION,
    /// SCATTER_FINISH_RATIO)` row.
    fn split_region_stmt(
        &mut self,
        split: &tidb_ast::SplitRegionStmt,
    ) -> Result<StmtOutput, DriverError> {
        let (database, name) = self.split_table_path(&split.table)?;
        let ctx = self.statement_context(false);
        let wait_finish = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_WAIT_SPLIT_REGION_FINISH)
            .is_ok_and(|value| value.eq_ignore_ascii_case("ON") || value == "1");
        // Go `GetSplitRegionTimeout`: `tidb_wait_split_region_timeout` seconds.
        let timeout = std::time::Duration::from_secs(
            self.session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_WAIT_SPLIT_REGION_TIMEOUT)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(300),
        );
        let row = self.with_catalog_mut(|catalog| {
            let catalog = &*catalog;
            match catalog.table_in(&database, &name) {
                Some(crate::TableEntry::Kv(table)) => {
                    crate::split_region::split_region(
                        split,
                        table,
                        &ctx,
                        wait_finish,
                        timeout,
                    )
                }
                Some(_) => Err(DriverError::unsupported(
                    "SPLIT names a table without regions",
                )),
                None => Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
                )),
            }
        })?;
        Ok(StmtOutput::Rows {
            columns: crate::split_region::split_region_columns(),
            rows: vec![row],
        })
    }

    /// go `ChecksumTableExec` (`pkg/executor/checksum.go`), built by
    /// `buildChecksumTable`: one coprocessor checksum request per physical
    /// table (the table's own ID plus every partition) and per public index
    /// on each, all at the statement's snapshot, folded per table by
    /// `updateChecksumResponse` (see [`crate::checksum`]). One
    /// `(Db_name, Table_name, Checksum_crc64_xor, Total_kvs, Total_bytes)`
    /// row per distinct table, the schema of `buildChecksumTableSchema`.
    fn admin_checksum_stmt(
        &mut self,
        tables: &[Vec<String>],
    ) -> Result<StmtOutput, DriverError> {
        let mut paths = Vec::with_capacity(tables.len());
        for path in tables {
            paths.push(self.split_table_path(path)?);
        }
        let (contexts, client) = self.with_catalog_mut(|catalog| {
            let mut contexts = Vec::with_capacity(paths.len());
            let mut client = None;
            for (database, name) in &paths {
                match catalog.table_in(database, name) {
                    // Go `checkAdminCheckTableGrammar` (`preprocess.go:908`).
                    Some(crate::TableEntry::Kv(table)) if table.is_temporary() => {
                        return Err(DriverError::from(
                            tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                                .generate_with_stack_by_args(&["admin checksum table".into()]),
                        ));
                    }
                    Some(crate::TableEntry::Kv(table)) => {
                        // Every table of a session shares one storage, so
                        // one coprocessor client and one snapshot: Go
                        // `GetClient()` and `getSnapshotTS()`.
                        if client.is_none() {
                            client = table
                                .store()
                                .client()
                                .map(|client| (client, table.store().start_ts()));
                        }
                        contexts.push(crate::checksum::ChecksumContext::new(
                            database, table,
                        ));
                    }
                    Some(_) => {
                        return Err(DriverError::unsupported(
                            "checksum reads stored tables only",
                        ))
                    }
                    None => {
                        return Err(DriverError::from(
                            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                                .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
                        ))
                    }
                }
            }
            Ok((contexts, client))
        })?;
        // Go `getChecksumTableConcurrency`.
        let concurrency = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_CHECKSUM_TABLE_CONCURRENCY)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(4);
        let explicit_request_source_type = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_EXPLICIT_REQUEST_SOURCE_TYPE)
            .unwrap_or_default();
        let ctx = self.statement_context(false);
        let (client, start_ts) = match client {
            Some((client, start_ts)) => (Some(client), start_ts),
            None => (None, 0),
        };
        let tables = crate::checksum::ChecksumTableExec::new(
            contexts,
            client,
            start_ts,
            explicit_request_source_type,
            &ctx,
        )
        .run(usize::try_from(concurrency).unwrap_or(1))?;
        let rows = tables
            .into_iter()
            .map(|table| {
                vec![
                    Datum::Bytes(table.db_name.into_bytes()),
                    Datum::Bytes(table.table_name.into_bytes()),
                    Datum::UInt(table.response.checksum()),
                    Datum::UInt(table.response.total_kvs()),
                    Datum::UInt(table.response.total_bytes()),
                ]
            })
            .collect();
        // Go `buildColumnWithName` flags every non-string column unsigned.
        let ulonglong = || {
            FieldType::new(FieldTypeCode::LongLong)
                .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED)
        };
        Ok(StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                ("Table_name".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
                ("Checksum_crc64_xor".to_owned(), ulonglong()),
                ("Total_kvs".to_owned(), ulonglong()),
                ("Total_bytes".to_owned(), ulonglong()),
            ],
            rows,
        })
    }

    /// go `execCommandOnDDLJobs` (`pkg/executor/admin.go`): one
    /// `(JOB_ID, RESULT)` row per requested id. Every id this node holds no
    /// active job for answers the not-found error string go's
    /// `GetDDLJobById` path produces — `error: [ddl:8224]DDL Job:%d not
    /// found` — which is the only reachable shape on a node whose DDL runs
    /// to completion synchronously (no job is ever mid-flight at query
    /// time).
    fn ddl_job_control_stmt(
        &mut self,
        control: &tidb_ast::AdminDdlJobControlStmt,
    ) -> Result<StmtOutput, DriverError> {
        let varchar = || FieldType::new(FieldTypeCode::Varchar);
        let rows = control
            .job_ids
            .iter()
            .map(|job_id| {
                vec![
                    Datum::Bytes(job_id.to_string().into_bytes()),
                    Datum::Bytes(
                        format!("error: [ddl:8224]DDL Job:{job_id} not found").into_bytes(),
                    ),
                ]
            })
            .collect();
        Ok(StmtOutput::Rows {
            columns: vec![
                ("JOB_ID".to_owned(), varchar()),
                ("RESULT".to_owned(), varchar()),
            ],
            rows,
        })
    }

    /// go `buildShowSlowSchema` (`pkg/planner/core/planbuilder.go:3340`)
    /// rendered over the recorded slow statements.
    fn admin_show_slow_stmt(
        &mut self,
        show: &tidb_ast::AdminShowSlowStmt,
    ) -> Result<StmtOutput, DriverError> {
        use tidb_datatype::{core_time_from_datetime, Time, TimeType};
        let records = tidb_domain::topn_slow_query::show_slow_query(&show.mode, show.count);
        let varchar = || FieldType::new(FieldTypeCode::Varchar);
        let rows = records
            .into_iter()
            .map(|record| {
                let start: chrono::DateTime<chrono::Local> = record
                    .start
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                    .into();
                let nanos = record.duration.subsec_nanos() as i64
                    + i64::try_from(record.duration.as_secs()).unwrap_or(0) * 1_000_000_000;
                vec![
                    // The text carries a byte that is not UTF-8 as its
                    // `raw_bytes` escape; Go's string holds the byte.
                    Datum::Bytes(tidb_hack::raw_bytes::str_to_bytes(&record.sql).into_owned()),
                    Datum::new_time(
                        Time::new(
                            core_time_from_datetime(start),
                            TimeType::Timestamp,
                            6,
                        )
                        .unwrap_or_else(|_| {
                            Time::new(
                                core_time_from_datetime(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH.with_timezone(&chrono::Local)),
                                TimeType::Timestamp,
                                0,
                            )
                            .expect("fsp 0 is valid")
                        }),
                    ),
                    Datum::new_duration(
                        tidb_datatype::MySqlDuration::from_nanoseconds(nanos, 6)
                            .unwrap_or_else(|_| {
                                tidb_datatype::MySqlDuration::from_nanoseconds(0, 0)
                                    .expect("fsp 0 is valid")
                            }),
                    ),
                    Datum::Bytes(record.detail.into_bytes()),
                    Datum::Int(i64::from(record.succ)),
                    Datum::UInt(record.conn_id),
                    Datum::UInt(record.txn_ts),
                    Datum::Bytes(record.user.into_bytes()),
                    Datum::Bytes(record.db.into_bytes()),
                    Datum::Bytes(record.table_ids.into_bytes()),
                    Datum::Bytes(record.index_names.into_bytes()),
                    Datum::Int(i64::from(record.internal)),
                    Datum::Bytes(record.digest.into_bytes()),
                    Datum::Bytes(record.sess_alias.into_bytes()),
                ]
            })
            .collect();
        Ok(StmtOutput::Rows {
            columns: vec![
                ("SQL".to_owned(), varchar()),
                (
                    "START".to_owned(),
                    FieldType::new(FieldTypeCode::Timestamp),
                ),
                ("DURATION".to_owned(), FieldType::new(FieldTypeCode::Duration)),
                ("DETAILS".to_owned(), varchar()),
                ("SUCC".to_owned(), FieldType::new(FieldTypeCode::Tiny)),
                ("CONN_ID".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                (
                    "TRANSACTION_TS".to_owned(),
                    FieldType::new(FieldTypeCode::LongLong),
                ),
                ("USER".to_owned(), varchar()),
                ("DB".to_owned(), varchar()),
                ("TABLE_IDS".to_owned(), varchar()),
                ("INDEX_IDS".to_owned(), varchar()),
                ("INTERNAL".to_owned(), FieldType::new(FieldTypeCode::Tiny)),
                ("DIGEST".to_owned(), varchar()),
                ("SESSION_ALIAS".to_owned(), varchar()),
            ],
            rows,
        })
    }

    /// Go `ShowDDLJobQueriesExec` / `ShowDDLJobQueriesWithRangeExec`: the
    /// queries of the named jobs, searched in the running jobs and the newest
    /// ten history jobs (the range form: the newest `offset + count`).
    fn admin_show_ddl_job_queries_stmt(
        &mut self,
        show: &tidb_ast::AdminShowDdlJobQueriesStmt,
    ) -> Result<StmtOutput, DriverError> {
        /// Go `ddl.DefNumHistoryJobs`.
        const DEF_NUM_HISTORY_JOBS: usize = 10;
        let history_limit = match show {
            tidb_ast::AdminShowDdlJobQueriesStmt::JobIds(_) => DEF_NUM_HISTORY_JOBS,
            tidb_ast::AdminShowDdlJobQueriesStmt::Limit { offset, count } => {
                usize::try_from(offset.saturating_add(*count)).unwrap_or(usize::MAX)
            }
        };
        let (running, history) = self.all_ddl_jobs()?;
        // Deduplicated by job id, running jobs first.
        let mut seen = std::collections::BTreeSet::new();
        let jobs = running
            .iter()
            .chain(history.iter().take(history_limit))
            .filter(|job| seen.insert(job.id))
            .collect::<Vec<_>>();
        let varchar = || FieldType::new(FieldTypeCode::Varchar);
        let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
        match show {
            tidb_ast::AdminShowDdlJobQueriesStmt::JobIds(ids) => {
                // Go's executor answers nothing once the named ids are at
                // least as many as the jobs it found.
                let rows = if ids.len() >= jobs.len() {
                    Vec::new()
                } else {
                    ids.iter()
                        .flat_map(|id| {
                            jobs.iter()
                                .filter(move |job| job.id == *id)
                                .map(|job| vec![text(&job.query.to_utf8_lossy_go())])
                        })
                        .collect()
                };
                Ok(StmtOutput::Rows {
                    columns: vec![("QUERY".to_owned(), varchar())],
                    rows,
                })
            }
            tidb_ast::AdminShowDdlJobQueriesStmt::Limit { offset, count } => {
                let offset = usize::try_from(*offset).unwrap_or(usize::MAX);
                let end = offset.saturating_add(usize::try_from(*count).unwrap_or(usize::MAX));
                let rows = if offset > jobs.len() {
                    Vec::new()
                } else {
                    jobs.iter()
                        .enumerate()
                        .skip(offset)
                        .take_while(|(index, _)| *index < end)
                        .map(|(_, job)| vec![text(&job.id.to_string()), text(&job.query.to_utf8_lossy_go())])
                        .collect()
                };
                Ok(StmtOutput::Rows {
                    columns: vec![
                        ("JOB_ID".to_owned(), varchar()),
                        ("QUERY".to_owned(), varchar()),
                    ],
                    rows,
                })
            }
        }
    }

    /// Go `ShowDDLJobsExec`: the newest `job_number` jobs of the DDL history
    /// (Go's `DefNumHistoryJobs`, ten, when the statement names none), each
    /// rendered by `appendJobToChunk` with its COMMENTS, a multi-schema
    /// change followed by its sub-jobs. The statement checks no table
    /// privilege.
    fn admin_show_ddl_jobs_stmt(
        &mut self,
        show: &tidb_ast::AdminShowDdlJobsStmt,
    ) -> Result<StmtOutput, DriverError> {
        /// Go `ddl.DefNumHistoryJobs`.
        const DEF_NUM_HISTORY_JOBS: usize = 10;
        // Go `buildAdmin`: `LogicalShowDDLJobs` under the WHERE's selection.
        let ctx = self.statement_context(false);
        let current_db = self.session_vars().current_db.clone();
        let mut physical = self.with_catalog_mut(|catalog| {
            crate::driver::plan_admin_show_ddl_jobs(
                show.job_number,
                show.where_clause.as_ref(),
                catalog,
                &current_db,
                &ctx,
            )
        })?;
        if let Some(leaf) = crate::driver::physical_show_ddl_jobs_of(&physical) {
            let job_number = if leaf.job_number > 0 {
                usize::try_from(leaf.job_number).unwrap_or(usize::MAX)
            } else {
                DEF_NUM_HISTORY_JOBS
            };
            let plan_id = leaf.base.base.id();
            // Go `ShowDDLJobsExec.Next`: every running job, then at most
            // `job_number` history jobs.
            let (running, history) = self.all_ddl_jobs()?;
            // Go `ShowDDLJobsExec.Open`: names resolve through the
            // statement's InfoSchema (`DDLJobRetriever.is = e.is`).
            let rows = self.with_catalog_mut(|catalog| {
                Ok(running
                    .iter()
                    .chain(history.iter().take(job_number))
                    .flat_map(|job| crate::ddl_jobs::ddl_job_rows(job, true, catalog))
                    .collect::<Vec<_>>())
            })?;
            ctx.show_retrieval().supply(plan_id, rows);
        }
        let (columns, rows) = self.with_catalog_retrieving(&ctx, |catalog| {
            crate::driver::open_show_plan(&mut physical, catalog, &ctx)?.collect()
        })?;
        Ok(StmtOutput::Rows { columns, rows })
    }

    /// Go `CheckTableExec` / `CheckIndexRangeExec`.
    fn admin_check_stmt(
        &mut self,
        check: &tidb_ast::AdminCheckStmt,
    ) -> Result<StmtOutput, DriverError> {
        match check {
            tidb_ast::AdminCheckStmt::Table { tables } => {
                let [path] = tables.as_slice() else {
                    // Go's parser accepts the list and its planner then
                    // refuses it; refusing here keeps the same outcome.
                    return Err(DriverError::unsupported(
                        "ADMIN CHECK TABLE checks one table at a time",
                    ));
                };
                let (database, name) = self.split_table_path(path)?;
                self.run_admin_check(&database, &name, None, "admin check table")?;
                Ok(admin_check_passed())
            }
            tidb_ast::AdminCheckStmt::Index {
                table,
                index,
                handle_ranges,
            } => {
                let (database, name) = self.split_table_path(table)?;
                if handle_ranges.is_empty() {
                    self.run_admin_check(&database, &name, Some(index), "admin check index")?;
                    return Ok(admin_check_passed());
                }
                let index = index.clone();
                let ranges = handle_ranges.clone();
                let ctx = self.statement_context(false);
                let decode_context = crate::RowDecodeContext::for_query(&ctx);
                let (columns, rows) = self.with_catalog_mut(|catalog| {
                    let table =
                        admin_check_table_mut(catalog, &database, &name, "admin check index")?;
                    crate::admin_check::check_index_ranges(
                        table,
                        &index,
                        &ranges,
                        &decode_context,
                    )
                    .map_err(admin_check_error)
                })?;
                Ok(StmtOutput::Rows { columns, rows })
            }
        }
    }

    /// Go `RecoverIndexExec`: `ADDED_COUNT` and `SCAN_COUNT` for the rows
    /// whose missing entries of the named index were written back.
    fn admin_recover_index_stmt(
        &mut self,
        recover: &tidb_ast::AdminRecoverIndexStmt,
    ) -> Result<StmtOutput, DriverError> {
        let (database, name) = self.split_table_path(&recover.table)?;
        let index = recover.index.clone();
        let ctx = self.statement_context(true);
        let decode_context = crate::RowDecodeContext::for_query(&ctx);
        let (added, scanned) = self.with_catalog_mut(|catalog| {
            let table = admin_index_table_mut(catalog, &database, &name)?;
            let table_name = table.name().to_owned();
            table
                .recover_index(&index, &ctx, &decode_context)
                .map_err(crate::ddl::index_backfill_error)?
                .ok_or_else(|| secondary_index_not_found(&index, &table_name))
        })?;
        let count = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        Ok(StmtOutput::Rows {
            columns: vec![
                ("ADDED_COUNT".to_owned(), count.clone()),
                ("SCAN_COUNT".to_owned(), count),
            ],
            rows: vec![vec![Datum::Int(added), Datum::Int(scanned)]],
        })
    }

    /// Go `CleanupIndexExec`: `REMOVED_COUNT` for the entries of the named
    /// index whose row is gone.
    fn admin_cleanup_index_stmt(
        &mut self,
        cleanup: &tidb_ast::AdminRecoverIndexStmt,
    ) -> Result<StmtOutput, DriverError> {
        let (database, name) = self.split_table_path(&cleanup.table)?;
        let index = cleanup.index.clone();
        let removed = self.with_catalog_mut(|catalog| {
            let table = admin_index_table_mut(catalog, &database, &name)?;
            let table_name = table.name().to_owned();
            table
                .cleanup_index(&index)
                .map_err(crate::ddl::index_backfill_error)?
                .ok_or_else(|| secondary_index_not_found(&index, &table_name))
        })?;
        Ok(StmtOutput::Rows {
            columns: vec![(
                "REMOVED_COUNT".to_owned(),
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            )],
            rows: vec![vec![Datum::UInt(removed)]],
        })
    }

    /// Go `CheckTableExec.Next`'s consistency check over one table, or one
    /// of its indexes.
    fn run_admin_check(
        &mut self,
        database: &str,
        name: &str,
        only_index: Option<&str>,
        statement: &'static str,
    ) -> Result<usize, DriverError> {
        let (database, name) = (database.to_owned(), name.to_owned());
        let only_index = only_index.map(str::to_owned);
        let ctx = self.statement_context(false);
        let decode_context = crate::RowDecodeContext::for_query(&ctx);
        let fast_check = self.session_vars().system_bool("tidb_enable_fast_table_check", true);
        self.with_catalog_mut(|catalog| {
            let table = admin_check_table_mut(catalog, &database, &name, statement)?;
            crate::admin_check::check_table(
                table,
                only_index.as_deref(),
                &decode_context,
                fast_check,
            )
            .map_err(admin_check_error)
        })
    }

    /// Go `ExplainExec` over the planned statement.
    fn explain_stmt(
        &mut self,
        explain: &tidb_ast::ExplainStmt,
    ) -> Result<Option<StmtOutput>, DriverError>;
    /// Go `session.ExecuteStmt` for the statement `TraceExec` runs.
    fn execute_statement_owned(
        &mut self,
        stmt: tidb_ast::Stmt,
        sql: &str,
    ) -> Result<StmtOutput, DriverError>;
    /// Go `meta.Mutator`'s BDR role, read and written in a new transaction.
    fn bdr_role_store(&self) -> Result<std::sync::Arc<dyn BdrRoleStore>, DriverError>;
    /// Go `SimpleExec.executeAdminFlushPlanCache`.
    fn flush_session_plan_cache(
        &mut self,
        scope: tidb_ast::AdminPlanCacheScope,
    ) -> Result<(), DriverError>;
    /// Go `executor.TakeSnapshot`, which the workload repository installs.
    fn take_snapshot(&self) -> Option<TakeSnapshot>;
    /// Go `SimpleExec.executeFlush`'s `FlushPrivileges` arm: reload the
    /// privilege cache and notify the domain.
    fn flush_privileges(&mut self) -> Result<(), DriverError>;
    /// Go `ShowDDLExec`'s rows: the node identity and the followed schema
    /// version.
    fn show_ddl_rows(&self) -> Vec<Vec<Datum>>;
    /// Go `Compile` + `runStmt` over `buildShow`'s plan.
    fn run_planned_show(&mut self, show: &tidb_ast::ShowStmt) -> Result<StmtOutput, DriverError>;
    /// Go `globalconn`'s server id of this node.
    fn server_id(&self) -> u64;
    /// Go's TiDB peer RPC client (`KILL` of another node's connection).
    fn cluster_peer(&self) -> Option<&std::sync::Arc<crate::cluster_peer::ClusterPeerClient>>;
    /// Go `infosync`'s server discovery.
    fn server_info_syncer(&self)
        -> Option<&std::sync::Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>>;

    /// The `AdminStmt` dispatch for the whole admin/inspection surface: the `SHOW` family here, plus
    /// `EXPLAIN`, `GRANT`/`REVOKE`/`SHOW GRANTS`, and `KILL` delegated to
    /// their own modules.
    /// Go `executorBuilder.build` over the ADMIN plans and the executors
    /// `Next` runs: one arm per statement.
    fn admin_next(
        &mut self,
        admin: &tidb_ast::AdminStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        match admin {
            // `EXPLAIN <select>`: plan the statement and report the plan,
            // running nothing. Go's EXPLAIN plans without executing (an
            // `EXPLAIN INSERT` inserts no row, captured), and so does
            // this: `crate::explain_select_stmt` re-runs the
            // driver's own read-path decisions without touching storage.
            //
            // See `crate::explain`'s module doc for every place
            // this tier's plan text diverges from Go's and why.
            tidb_ast::AdminStmt::Explain(explain) => self.explain_stmt(explain),
            // Go `executeAdminSetBDRRole` / `executeAdminUnsetBDRRole` write
            // the meta key `BDRRole`; `AdminShowBDRRoleExec` reads it.
            tidb_ast::AdminStmt::SetBdrRole(_)
            | tidb_ast::AdminStmt::UnsetBdrRole
            | tidb_ast::AdminStmt::ShowBdrRole => {
                let role = match admin {
                    tidb_ast::AdminStmt::SetBdrRole(tidb_ast::BdrRole::Primary) => "primary",
                    tidb_ast::AdminStmt::SetBdrRole(tidb_ast::BdrRole::Secondary) => "secondary",
                    tidb_ast::AdminStmt::UnsetBdrRole => "",
                    _ => {
                        let role = self
                            .bdr_role_store()?
                            .get()
                            .map_err(DriverError::unsupported)?;
                        let mut field_type =
                            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::String);
                        field_type.set_flen(1);
                        return Ok(Some(StmtOutput::Rows {
                            columns: vec![("BDR_ROLE".to_owned(), field_type)],
                            rows: vec![vec![Datum::Bytes(role.into_bytes())]],
                        }));
                    }
                };
                self.bdr_role_store()?
                    .set(role)
                    .map_err(DriverError::unsupported)?;
                Ok(Some(StmtOutput::Done(true)))
            }
            tidb_ast::AdminStmt::FlushPlanCache(scope) => {
                self.flush_session_plan_cache(*scope)?;
                Ok(Some(StmtOutput::Done(true)))
            }
            // SQL bindings. See `crate::binding_arm`, and `crate::binding`
            // for the normalization and hint-transfer they are built on.
            tidb_ast::AdminStmt::CreateBinding(create) => {
                Ok(Some(self.create_binding_stmt(create)?))
            }
            tidb_ast::AdminStmt::DropBinding(drop) => Ok(Some(self.drop_binding_stmt(drop)?)),
            tidb_ast::AdminStmt::SetBinding(set) => Ok(Some(self.set_binding_stmt(set)?)),
            // ANALYZE, LOAD STATS and DROP STATS write the statistics storage
            // and the domain's cache: the session's node runs them
            // (`StatementRoute::Analyze` / `LoadStats` / `DropStats`).
            tidb_ast::AdminStmt::AnalyzeTable(_)
            | tidb_ast::AdminStmt::AnalyzeIncremental(_)
            | tidb_ast::AdminStmt::LoadStats(_)
            | tidb_ast::AdminStmt::DropStats(_)
            | tidb_ast::AdminStmt::LockStats(_)
            | tidb_ast::AdminStmt::UnlockStats(_) => Err(DriverError::unsupported(
                "statistics statements run on the session's node",
            )),
            tidb_ast::AdminStmt::CreateWorkloadSnapshot => {
                let take_snapshot = self.take_snapshot().ok_or_else(|| {
                    DriverError::from(
                        tidb_plannererrors::ERR_NOT_SUPPORTED_YET
                            .generate_with_stack_by_args(&[
                                "Workload repository is not enabled".into()
                            ]),
                    )
                })?;
                take_snapshot().map_err(|error| {
                    if error == "Workload repository is not enabled" {
                        DriverError::from(
                            tidb_plannererrors::ERR_NOT_SUPPORTED_YET
                                .generate_with_stack_by_args(&[error.into()]),
                        )
                    } else {
                        DriverError::unsupported(error)
                    }
                })?;
                Ok(Some(StmtOutput::Done(true)))
            }
            // `ADMIN RELOAD <blacklist>`: Go's `ReloadExprPushdownBlacklist`
            // and `ReloadOptRuleBlacklist` executors, each of which reads its
            // `mysql.*` table and publishes what the optimizer consults. Both
            // answer no rows, as every ADMIN maintenance statement does.
            tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::ExprPushdownBlacklist) => {
                self.reload_expr_pushdown_blacklist()?;
                Ok(Some(StmtOutput::Done(true)))
            }
            tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::OptRuleBlacklist) => {
                self.reload_opt_rule_blacklist()?;
                Ok(Some(StmtOutput::Done(true)))
            }
            // go `SimpleExec.executeReloadBindings` (`pkg/executor/simple.go`):
            // the reload re-syncs the binding cache from storage and answers
            // no rows, like every maintenance statement.
            tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::Bindings) => {
                Ok(Some(StmtOutput::Done(true)))
            }
            // Go `executeAdminReloadStatistics`.
            tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::Statistics) => {
                Err(DriverError::Mysql(crate::MysqlError::unknown(
                    "Extended statistics feature has been removed",
                )))
            }
            // Go `plugin.ChangeDisableFlagAndFlush`: no plugin is loaded
            // here, so the first named plugin is unknown.
            tidb_ast::AdminStmt::Plugins { plugins, .. } => match plugins.first() {
                Some(name) => Err(DriverError::Mysql(crate::MysqlError::unknown(format!(
                    "plugin '{name}' not found"
                )))),
                None => Ok(Some(StmtOutput::Done(true))),
            },
            // go `SimpleExec.executeAdminBindingControl` (`pkg/executor/simple.go`):
            // `ADMIN FLUSH BINDINGS` publishes the cache and answers OK;
            // `CAPTURE`/`EVOLVE` refuse with the two messages Go's own
            // feature gates produce (`Auto Capture is not supported` when
            // `tidb_capture_plan_baselines` is off, and the baseline-evolution
            // GA refusal).
            tidb_ast::AdminStmt::BindingControl(kind) => match kind {
                tidb_ast::AdminBindingControlKind::Flush => Ok(Some(StmtOutput::Done(true))),
                tidb_ast::AdminBindingControlKind::Capture => {
                    Err(DriverError::unsupported("Auto Capture is not supported"))
                }
                tidb_ast::AdminBindingControlKind::Evolve => Err(DriverError::unsupported(
                    "Cannot enable baseline evolution feature, it is not generally available now",
                )),
            },
            // go `execCommandOnDDLJobs` (`pkg/executor/admin.go`): one
            // `(JOB_ID, RESULT)` row per requested id, not an error.
            tidb_ast::AdminStmt::DdlJobControl(control) => {
                Ok(Some(self.ddl_job_control_stmt(control)?))
            }
            tidb_ast::AdminStmt::SplitRegion(split) => Ok(Some(self.split_region_stmt(split)?)),
            // go `ChecksumTableExec` (`pkg/executor/checksum.go`).
            tidb_ast::AdminStmt::AdminChecksum(checksum) => {
                Ok(Some(self.admin_checksum_stmt(&checksum.tables)?))
            }
            // go `adminShowSlowExec` over the domain's slow-query memory.
            tidb_ast::AdminStmt::ShowSlow(show) => Ok(Some(self.admin_show_slow_stmt(show)?)),
            // go `fetchShowDDLJobs` over the finished-job history.
            tidb_ast::AdminStmt::ShowDdlJobs(show) => {
                Ok(Some(self.admin_show_ddl_jobs_stmt(show)?))
            }
            // go `ShowDDLJobQueriesExec` / `...WithRangeExec`.
            tidb_ast::AdminStmt::ShowDdlJobQueries(show) => {
                Ok(Some(self.admin_show_ddl_job_queries_stmt(show)?))
            }
            // go `SimpleExec.executeExplainFor` (`pkg/executor/simple.go`).
            tidb_ast::AdminStmt::ExplainFor(explain_for) => self.explain_for_stmt(explain_for),
            // Go `TraceExec`: runs the statement, then reports the spans.
            tidb_ast::AdminStmt::Trace(trace) => self.trace_stmt(trace),
            // Go `getTiProxyAddrs`: every TRAFFIC statement talks to a
            // TiProxy, and this deployment has none.
            tidb_ast::AdminStmt::Traffic(_) => Err(DriverError::Mysql(crate::MysqlError::unknown(
                "no tiproxy server found",
            ))),
            // Go `RecommendIndexExec` (`executor/recommend_index.go`).
            tidb_ast::AdminStmt::RecommendIndex(recommend) => {
                self.recommend_index_stmt(recommend).map(Some)
            }
            tidb_ast::AdminStmt::Grant(grant) => {
                self.grant_stmt(grant)?;
                Ok(Some(StmtOutput::Affected(0)))
            }
            tidb_ast::AdminStmt::Revoke(revoke) => {
                self.revoke_stmt(revoke)?;
                Ok(Some(StmtOutput::Affected(0)))
            }
            tidb_ast::AdminStmt::GrantRole(grant) => {
                self.grant_role_stmt(grant)?;
                Ok(Some(StmtOutput::Affected(0)))
            }
            tidb_ast::AdminStmt::RevokeRole(revoke) => {
                self.revoke_role_stmt(revoke)?;
                Ok(Some(StmtOutput::Affected(0)))
            }
            // Go `SimpleExec.executeFlush`.
            tidb_ast::AdminStmt::Flush(flush) => {
                if matches!(flush.target, tidb_ast::FlushTarget::Status) {
                    self.session_vars_mut().keys_examined = 0;
                }
                if matches!(flush.target, tidb_ast::FlushTarget::Privileges) {
                    self.flush_privileges()?;
                }
                return flush_stmt(flush).map(Some);
            }
            tidb_ast::AdminStmt::ShowDdl => {
                return Ok(Some(show_ddl_output(&self.show_ddl_rows())));
            }
            // Go `Compile` + `runStmt` over `buildShow`'s plan.
            tidb_ast::AdminStmt::Show(show) if crate::show::is_planned_show(show.tp) => {
                self.run_planned_show(show).map(Some)
            }
            // Go has no `ShowImports` fetcher or schema (`buildShowSchema`
            // names no columns for it); this tier answers the empty
            // `Job_ID | State` listing.
            tidb_ast::AdminStmt::Show(show) if show.tp == tidb_ast::ShowStmtType::Imports => {
                Ok(Some(show_imports_output()))
            }
            // Go `SimpleExec.executeKillStmt`.
            tidb_ast::AdminStmt::Kill(kill) => self.kill_stmt(kill),
            // Go `CheckTableExec` / `CheckIndexRangeExec`.
            tidb_ast::AdminStmt::AdminCheck(check) => {
                self.admin_check_stmt(check.as_ref()).map(Some)
            }
            // Go `RecoverIndexExec` / `CleanupIndexExec`.
            tidb_ast::AdminStmt::AdminRecoverIndex(recover) => {
                self.admin_recover_index_stmt(recover.as_ref()).map(Some)
            }
            tidb_ast::AdminStmt::AdminCleanupIndex(cleanup) => {
                self.admin_cleanup_index_stmt(cleanup.as_ref()).map(Some)
            }
            // Go `fetchShowCreateProcedure`'s object is not stored here: the
            // statement answers nothing.
            tidb_ast::AdminStmt::Show(show)
                if show.tp == tidb_ast::ShowStmtType::CreateProcedure =>
            {
                Ok(None)
            }
            // Go `SimpleExec.executeShutdown` + `asyncDelayShutdown`
            // (pkg/executor/simple.go:3584-3611): the statement answers
            // success immediately, and a 1-second-delayed task SIGTERMs this
            // very process, whose installed handler performs the graceful
            // stop. `RESTART`/`HELP` keep their unsupported report.
            tidb_ast::AdminStmt::ServerControl(control) => match &**control {
                tidb_ast::ServerControlStmt::Shutdown => {
                    std::thread::spawn(|| {
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        signal_hook::low_level::raise(signal_hook::consts::SIGTERM);
                    });
                    Ok(Some(StmtOutput::Affected(0)))
                }
                // Go's executor builder names the AST node of the statements
                // it has no executor for (`Unsupported type *ast.HelpStmt`,
                // 8108).
                tidb_ast::ServerControlStmt::Restart => Err(DriverError::from(
                    (&tidb_plannererrors::ERR_UNSUPPORTED_TYPE)
                        .generate_with_stack("Unsupported type *ast.RestartStmt".to_owned()),
                )),
                tidb_ast::ServerControlStmt::Help(_) => Err(DriverError::from(
                    (&tidb_plannererrors::ERR_UNSUPPORTED_TYPE)
                        .generate_with_stack("Unsupported type *ast.HelpStmt".to_owned()),
                )),
            },
            _ => Ok(None),
        }
    }

    fn kill_stmt(&mut self, kill: &tidb_ast::KillStmt) -> Result<Option<StmtOutput>, DriverError> {
        const INVALID_OPERATION: &str = "Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead";
        let registry = self.process_registry();
        let target = match &kill.target {
            tidb_ast::KillTarget::ConnectionId(id) => *id,
            tidb_ast::KillTarget::Expr(tidb_ast::Expr::Func { name, args, .. })
                if name.eq_ignore_ascii_case("connection_id") && args.is_empty() =>
            {
                // Go executes this local expression before numeric ID/config gates.
                if let Some(registry) = registry {
                    registry.kill(self.session_vars().connection_id.unwrap_or(0), kill.query);
                }
                return Ok(Some(StmtOutput::Affected(0)));
            }
            tidb_ast::KillTarget::Expr(_) => {
                return Err(DriverError::unsupported(INVALID_OPERATION));
            }
        };

        let config = tidb_config::config_tree::config::get_global_config();
        if !config.enable_global_kill {
            if kill.tidb_extension || config.compatible_kill_query {
                if let Some(registry) = registry {
                    registry.kill(target, kill.query);
                }
            } else {
                self.append_sql_warning(tidb_context::SqlWarn::with_code(
                    tidb_context::WarningLevel::Warning,
                    1105,
                    INVALID_OPERATION.to_owned(),
                ));
            }
            return Ok(Some(StmtOutput::Affected(0)));
        }
        let Some(registry) = registry else {
            return Ok(Some(StmtOutput::Affected(0)));
        };
        match tidb_globalconn::parse_conn_id(target) {
            Err(error) => {
                self.append_sql_warning(tidb_context::SqlWarn::with_code(
                    tidb_context::WarningLevel::Warning,
                    1105,
                    format!("Parse ConnectionID failed: {error}"),
                ));
            }
            Ok((_, true)) => {
                self.append_sql_warning(tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Warning, 1105, "Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID.".to_owned()));
            }
            Ok((id, false)) => {
                if id.server_id == self.server_id() {
                    registry.kill(target, kill.query);
                } else if let Err(error) = self.kill_remote_connection(id, kill.query) {
                    self.append_sql_warning(tidb_context::SqlWarn::with_code(
                        tidb_context::WarningLevel::Warning,
                        1105,
                        format!("KILL remote connection failed: {error}"),
                    ));
                }
            }
        }
        Ok(Some(StmtOutput::Affected(0)))
    }

    fn kill_remote_connection(
        &self,
        id: tidb_globalconn::Gcid,
        query: bool,
    ) -> Result<(), String> {
        if id.server_id == 0 {
            return Err("Unexpected ZERO ServerID. Please file a bug to the TiDB Team".into());
        }
        let client = self
            .cluster_peer()
            .ok_or("TiDB peer RPC client is not installed")?;
        let syncer = self
            .server_info_syncer()
            .ok_or("TiDB peer discovery is not installed")?;
        let servers: Vec<_> = syncer.all_server_info()?.into_values().collect();
        client.kill(
            &servers,
            id,
            query,
            &self.statement_context(false),
            &self.session_time_zone(),
        )
    }

    /// go `SimpleExec.executeExplainFor` (`pkg/executor/simple.go`):
    /// the target connection must exist on this server
    /// (`ErrNoSuchThread` 1094, `Unknown thread id: %d`); its running
    /// statement then explains through the ordinary machinery.
    fn explain_for_stmt(
        &mut self,
        explain_for: &tidb_ast::ExplainForStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let target = explain_for.connection_id;
        let Some(registry) = self.process_registry() else {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NO_SUCH_THREAD
                    .generate_with_stack_by_args(&[target.into()]),
            ));
        };
        let Some(target_row) = registry.snapshot().into_iter().find(|row| row.id == target) else {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NO_SUCH_THREAD
                    .generate_with_stack_by_args(&[target.into()]),
            ));
        };
        let plan = registry.plan_of(target);
        // Go `buildExplainFor`: another user's connection needs SUPER,
        // refused with `ErrAccessDenied` over the two names alone.
        if let Some((_, user, host)) = self.privilege_context() {
            if user != target_row.user && !self.has_scoped_privilege("", "", tidb_mysql::SuperPriv)
            {
                return Err(DriverError::Mysql(crate::MysqlError::from_terror(
                    &tidb_privilege::privileges_errors::ERR_ACCESS_DENIED.generate_with_stack(
                        format!(
                        "Access denied for user '{user}'@'{host}' (using password: %!s(MISSING))"
                    ),
                    ),
                )));
            }
        }
        let format = explain_for.format.to_lowercase();
        let text = |name: &str| {
            (
                name.to_owned(),
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString),
            )
        };
        // Go `buildExplainFor`: no plan at all is the statement's own
        // empty result; any other plan needs one of the three formats.
        if matches!(plan, tidb_sessmgr::ProcessPlan::None) {
            return Ok(Some(StmtOutput::Affected(0)));
        }
        let explain_format = match format.as_str() {
            "" | "row" => crate::ExplainFormat::Row,
            "brief" => crate::ExplainFormat::Brief,
            "verbose" => crate::ExplainFormat::Verbose,
            other => {
                return Err(DriverError::Mysql(crate::MysqlError::unknown(format!(
                    "explain format '{other}' for connection is not supported now"
                ))));
            }
        };
        let plan = match plan {
            tidb_sessmgr::ProcessPlan::Plan(plan) => plan,
            // An `Explain` plan renders nothing, under the analyze
            // header of the format.
            _ => {
                let names: &[&str] = if explain_format == crate::ExplainFormat::Verbose {
                    &[
                        "id",
                        "estRows",
                        "estCost",
                        "actRows",
                        "task",
                        "access object",
                        "execution info",
                        "operator info",
                        "memory",
                        "disk",
                    ]
                } else {
                    &[
                        "id",
                        "estRows",
                        "actRows",
                        "task",
                        "access object",
                        "execution info",
                        "operator info",
                        "memory",
                        "disk",
                    ]
                };
                return Ok(Some(StmtOutput::Rows {
                    columns: names.iter().map(|name| text(name)).collect(),
                    rows: Vec::new(),
                }));
            }
        };
        let ctx = self.statement_context(false);
        let (columns, rows) = self.with_catalog_mut(|catalog| {
            crate::explain_connection_plan(
                &plan
                    .get::<tidb_planner::physical::PhysicalPlan>()
                    .expect("a stored plan is the planner's"),
                catalog,
                &ctx,
                explain_format,
            )
        })?;
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }

    /// Go `TraceExec`: runs the statement, then reports the spans.
    fn trace_stmt(
        &mut self,
        trace: &tidb_ast::TraceStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        crate::trace::check_options(trace)
            .map_err(|message| DriverError::Mysql(crate::MysqlError::unknown(message)))?;
        let inner_sql = crate::trace::statement_text(&trace.statement);
        let inner = (*trace.statement).clone();
        let recorder = crate::trace::StatementTrace::start();
        let entered = recorder.enter();
        // Go `session.ExecuteStmt`; this path drains the record set
        // inside it.
        let execute_stmt = tidb_tracing::begin_thread_region_scope("session.ExecuteStmt");
        let outcome = self.execute_statement_owned(inner, &inner_sql);
        drop(execute_stmt);
        drop(entered);
        // Go `executeChild` and `drainRecordSet`'s events.
        let affected = match outcome {
            Ok(StmtOutput::Rows { rows, .. }) => {
                recorder.event(&format!(
                    "execute done, ReturnRow: {}, ModifyRow: 0",
                    rows.len()
                ));
                0
            }
            Ok(StmtOutput::Affected(count)) => count,
            Ok(_) => 0,
            Err(error) => {
                let mysql = error.to_mysql_error();
                recorder.event(&format!(
                    "execute with error({}): {}",
                    mysql.code, mysql.message
                ));
                0
            }
        };
        recorder.event(&format!("execute done, modify row: {affected}"));
        let (spans, started) = recorder.finish();
        let (columns, rows) = crate::trace::render(&trace.format, &spans, started)
            .map_err(|message| DriverError::Mysql(crate::MysqlError::unknown(message)))?;
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }

    /// Go `LoadExprPushdownBlacklist`, and the `ADMIN RELOAD
    /// EXPR_PUSHDOWN_BLACKLIST` that calls it.
    fn reload_expr_pushdown_blacklist(&mut self) -> Result<(), DriverError> {
        let rows =
            self.exec_restricted_sql("SELECT name, store_type FROM mysql.expr_pushdown_blacklist")?;
        let mut loaded = crate::ExprPushDownBlacklist::new();
        for row in rows {
            let Some(name) = row
                .first()
                .and_then(tidb_datatype::datum_text::sysvar_datum_text)
            else {
                continue;
            };
            let name = crate::blacklist_name(&name);
            let store_types = row
                .get(1)
                .and_then(tidb_datatype::datum_text::sysvar_datum_text)
                .unwrap_or_default();
            let value =
                loaded.get(&name).copied().unwrap_or(0) | crate::blacklist_store_mask(&store_types);
            loaded.insert(name, value);
        }
        self.pushdown_blacklists().publish_expressions(loaded);
        Ok(())
    }

    /// Go `LoadOptRuleBlacklist`, and the `ADMIN RELOAD OPT_RULE_BLACKLIST`
    /// that calls it. The names are the rules' own `Name()` strings and are
    /// NOT case-folded, which is Go's behaviour: `row.GetString(0)` goes into
    /// the set as written.
    fn reload_opt_rule_blacklist(&mut self) -> Result<(), DriverError> {
        let rows = self.exec_restricted_sql("SELECT name FROM mysql.opt_rule_blacklist")?;
        let mut loaded = std::collections::HashSet::new();
        for row in rows {
            if let Some(name) = row
                .first()
                .and_then(tidb_datatype::datum_text::sysvar_datum_text)
            {
                loaded.insert(name);
            }
        }
        self.pushdown_blacklists().publish_rules(loaded);
        Ok(())
    }
}

/// What a passing `ADMIN CHECK TABLE`/`CHECK INDEX` answers.
///
/// NOT an empty result set: Go's `CheckTable` plan is a
/// `SimpleSchemaProducer` that never calls `SetSchema`, so its schema is
/// EMPTY and the server replies with an OK packet rather than a zero-column
/// result set. `r/util/admin.result` records the statement with nothing
/// under it, which a zero-column result set would render as a blank header
/// line -- one line where TiDB wrote none.
pub(crate) fn admin_check_passed() -> StmtOutput {
    StmtOutput::Affected(0)
}

/// The storage-backed table `ADMIN CHECK` needs, or the refusal that names
/// why it is not checkable.
pub(crate) fn admin_check_table_mut<'a>(
    catalog: &'a mut crate::Catalog,
    database: &str,
    name: &str,
    // The statement name Go's 8006 carries: `admin check table` or
    // `admin check index`, spelled as Go spells it.
    statement: &'static str,
) -> Result<&'a mut tidb_table::kv_table::KvTable, DriverError> {
    let Some(entry) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
        ));
    };
    match entry {
        // Go `checkAdminCheckTableGrammar` (`preprocess.go:908`): a temporary
        // table's rows are in a session membuffer, never in TiKV, so there is
        // no stored index for the check to compare them against and TiDB
        // refuses the statement instead of answering OK about nothing. Both
        // scopes are refused; the argument names the statement, which is why
        // the caller passes it.
        crate::TableEntry::Kv(table) if table.is_temporary() => Err(DriverError::from(
            tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                .generate_with_stack_by_args(&[statement.into()]),
        )),
        crate::TableEntry::Kv(table) => Ok(std::sync::Arc::make_mut(table)),
        // A view has no rows of its own and a sequence has no index; a
        // `MemTable` has rows but stores no index entries, so "consistent"
        // would be a statement about nothing.
        _ => Err(DriverError::unsupported(
            "ADMIN CHECK needs a storage-backed table: this table stores no index entries to \
             check its rows against",
        )),
    }
}

/// The stored table `ADMIN RECOVER INDEX` / `ADMIN CLEANUP INDEX` works on.
pub(crate) fn admin_index_table_mut<'a>(
    catalog: &'a mut crate::Catalog,
    database: &str,
    name: &str,
) -> Result<&'a mut tidb_table::kv_table::KvTable, DriverError> {
    match catalog.table_mut_in(database, name) {
        Some(crate::TableEntry::Kv(table)) => Ok(std::sync::Arc::make_mut(table)),
        Some(_) => Err(DriverError::unsupported(
            "ADMIN RECOVER/CLEANUP INDEX needs a storage-backed table",
        )),
        None => Err(DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
        )),
    }
}

/// Go `executorBuilder.buildRecoverIndex`/`buildCleanupIndex`'s
/// `errors.Errorf("secondary index `%v` is not found in table `%v`")`, which
/// names the index as written and the table by its stored name.
pub(crate) fn secondary_index_not_found(index: &str, table: &str) -> DriverError {
    DriverError::Mysql(crate::driver::MysqlError::unknown(
        format!("secondary index `{index}` is not found in table `{table}`"),
    ))
}

/// Maps the check's own outcome onto the errors TiDB reports.
pub(crate) fn admin_check_error(error: crate::admin_check::AdminCheckError) -> DriverError {
    use crate::admin_check::AdminCheckError;
    match error {
        AdminCheckError::CountMismatch {
            table_count,
            index,
            index_count,
        } => DriverError::from(
            crate::admin_errors::ERR_ADMIN_CHECK_TABLE.generate_with_stack(format!(
                "table count {table_count} != index({index}) count {index_count}"
            )),
        ),
        AdminCheckError::Inconsistent {
            table,
            index,
            handle,
            index_values,
            record_values,
        } => DriverError::from(
            crate::consistency_errors::ERR_ADMIN_CHECK_INCONSISTENT.generate_with_stack(
                format!(
                    "data inconsistency in table: {table}, index: {index}, handle: {handle}, \
             index-values:{index_values} != record-values:{record_values}"
                ),
            ),
        ),
        AdminCheckError::ValueMismatch(mismatch) => {
            let crate::admin_check::AdminValueMismatch {
                table,
                index,
                column,
                handle,
                index_value,
                record_value,
                compare_error,
            } = *mismatch;
            DriverError::from(
                crate::consistency_errors::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO
                    .generate_with_stack(format!(
                "data inconsistency in table: {table}, index: {index}, col: {column}, handle: \
                 {handle:?}, index-values:{index_value:?} != record-values:{record_value:?}, \
                 compare err:{compare_error}"
            )),
            )
        }
        AdminCheckError::UnknownIndex { index, .. } => DriverError::from(
            crate::ExecError::internal(format!("secondary index {index} does not exist")),
        ),
        AdminCheckError::NotStored(detail) | AdminCheckError::Decode(detail) => {
            DriverError::unsupported(detail)
        }
        AdminCheckError::PartialIndexWithoutFastCheck => DriverError::from(
            crate::executor_errors::ERR_CHECK_PARTIAL_INDEX_WITHOUT_FAST_CHECK
                .generate_with_stack_by_args(&[]),
        ),
    }
}

