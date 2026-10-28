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

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::KvTableDdl;
use crate::{DriverError, StmtOutput};

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
            tidb_error::tidb::infoschema::flush_stats();
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
pub trait AdminExec: crate::show::exec::ShowExec {
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
                    tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
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
                            tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
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
                            tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
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
) -> Result<&'a mut crate::kv_table::KvTable, DriverError> {
    let Some(entry) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
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
            tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
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
) -> Result<&'a mut crate::kv_table::KvTable, DriverError> {
    match catalog.table_mut_in(database, name) {
        Some(crate::TableEntry::Kv(table)) => Ok(std::sync::Arc::make_mut(table)),
        Some(_) => Err(DriverError::unsupported(
            "ADMIN RECOVER/CLEANUP INDEX needs a storage-backed table",
        )),
        None => Err(DriverError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
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
            tidb_error::catalog::admin::ERR_ADMIN_CHECK_TABLE.generate_with_stack(format!(
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
            tidb_error::catalog::consistency::ERR_ADMIN_CHECK_INCONSISTENT.generate_with_stack(
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
                tidb_error::catalog::consistency::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO
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
            tidb_error::catalog::executor::ERR_CHECK_PARTIAL_INDEX_WITHOUT_FAST_CHECK
                .generate_with_stack_by_args(&[]),
        ),
    }
}
