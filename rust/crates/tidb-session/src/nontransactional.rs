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

//! Go `pkg/session/nontransactional.go`: `BATCH [ON column] LIMIT n
//! [DRY RUN [QUERY]] <DML>`.
//!
//! The statement reads the shard column's values (`SELECT col FROM t WHERE
//! <the DML's condition> ORDER BY IF(ISNULL(col),0,1), col`), cuts them into
//! jobs of about `n` rows whose bounds never split one value, and runs the
//! DML once per job with its condition narrowed to that job's range, each in
//! its own auto-committed transaction. Like Go, the statement is not one
//! statement of the session: each SELECT and job DML is.

use std::any::Any;

use tidb_ast::{
    Assignment, BatchDml, BatchDmlDryRun, BatchDmlStmt, BinaryOp, DeleteKind, Expr, Join, JoinNode,
    QueryStmt, RestoreFlags, TableRef, UpdateKind, Visitable, Visitor,
};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::{DriverError, TableEntry};

use crate::{Session, StmtOutput};

/// Go `model.ExtraHandleName`.
const EXTRA_HANDLE_NAME: &str = "_tidb_rowid";

/// Go `ErrNonTransactionalJobFailure`.
const ERR_NON_TRANSACTIONAL_JOB_FAILURE: u16 = 8143;

/// The flags Go restores both the condition of the shard SELECT and every
/// split statement with.
fn restore_flags() -> RestoreFlags {
    RestoreFlags::DEFAULT
        | RestoreFlags::NAME_BACK_QUOTES
        | RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | RestoreFlags::BRACKET_AROUND_BINARY_OPERATION
        | RestoreFlags::STRING_WITHOUT_CHARSET
}

/// Go `job`: the handle keys in `[start, end]`.
struct Job {
    start: Datum,
    end: Datum,
    err: Option<DriverError>,
    job_id: usize,
    /// It can be inaccurate if there are concurrent writes.
    job_size: usize,
    sql: String,
}

impl Job {
    /// Go `job.String`.
    fn describe(&self, redact: &str) -> String {
        format!(
            "job id: {}, estimated size: {}, sql: {}",
            self.job_id,
            self.job_size,
            tidb_util::redact::string(redact, &self.sql)
        )
    }
}

/// Go `*ast.ColumnName`: `[schema.][table.]name` as Go splits it.
#[derive(Clone, Debug, Default)]
struct ColumnName {
    schema: String,
    table: String,
    name: String,
}

impl ColumnName {
    fn from_path(path: &[String]) -> Self {
        match path {
            [name] => Self {
                name: name.clone(),
                ..Self::default()
            },
            [table, name] => Self {
                table: table.clone(),
                name: name.clone(),
                ..Self::default()
            },
            [schema, table, name, ..] => Self {
                schema: schema.clone(),
                table: table.clone(),
                name: name.clone(),
            },
            [] => Self::default(),
        }
    }

    fn to_path(&self) -> Vec<String> {
        [&self.schema, &self.table, &self.name]
            .into_iter()
            .skip_while(|part| part.is_empty())
            .cloned()
            .collect()
    }

    fn expr(&self) -> Expr {
        Expr::Column(self.to_path())
    }
}

/// The shard column Go keeps as `*model.ColumnInfo`; `None` is
/// `_tidb_rowid`.
#[derive(Clone)]
struct ShardColumnInfo {
    name: String,
    field_type: FieldType,
}

/// Go `*ast.TableName` with its `TableSource.AsName`.
#[derive(Clone)]
struct TableSource {
    schema: String,
    name: String,
    as_name: String,
}

impl TableSource {
    fn from_ref(table: &TableRef) -> Option<Self> {
        let (schema, name) = match table.name.as_slice() {
            [schema, name] => (schema.clone(), name.clone()),
            [name] => (String::new(), name.clone()),
            _ => return None,
        };
        Some(Self {
            schema,
            name,
            as_name: table.alias.clone().unwrap_or_default(),
        })
    }
}

fn plain_error(message: impl Into<String>) -> DriverError {
    DriverError::unsupported(message.into())
}

fn lower(value: &str) -> String {
    tidb_util::stringutil::go_to_lower(value)
}

impl Session {
    /// Go `HandleNonTransactionalDML`, the entry point of a non-transactional
    /// DML statement.
    pub(crate) fn run_non_transactional_dml(
        &mut self,
        stmt: &BatchDmlStmt,
    ) -> Result<StmtOutput, DriverError> {
        // NT-DML is a write operation and should not be affected by
        // read_staleness, which is supposed to affect only SELECT.
        let read_staleness = self.override_system_var("tidb_read_staleness", "0");
        let mut stmt = stmt.clone();
        let result = self.handle_non_transactional_dml(&mut stmt);
        self.restore_system_var("tidb_read_staleness", read_staleness);
        result
    }

    fn handle_non_transactional_dml(
        &mut self,
        stmt: &mut BatchDmlStmt,
    ) -> Result<StmtOutput, DriverError> {
        self.preprocess_non_transactional_dml(stmt)?;
        self.check_non_transactional_constraint(stmt)?;

        let (table_name, select_sql, shard_column_info, table_sources) =
            self.build_non_transactional_select_sql(stmt)?;
        self.check_constraint_with_shard_column(
            stmt,
            &table_name,
            shard_column_info.as_ref(),
            &table_sources,
        )?;

        let max_chunk_size = self.max_chunk_size_for_non_transactional_dml();
        if stmt.dry_run == BatchDmlDryRun::Query {
            return Ok(dry_run_results(stmt.dry_run, vec![select_sql]));
        }

        let mut jobs = self.build_shard_jobs(
            stmt,
            &select_sql,
            shard_column_info.as_ref(),
            max_chunk_size,
        )?;
        let original_condition = where_expr(&stmt.dml).cloned();
        let split_statements =
            self.run_non_transactional_jobs(&mut jobs, stmt, &table_name, original_condition)?;
        if stmt.dry_run == BatchDmlDryRun::SplitDml {
            return Ok(dry_run_results(stmt.dry_run, split_statements));
        }
        self.build_execute_results(&jobs)
    }

    /// The part of Go `core.Preprocess` the split statements show: every
    /// table name of the DML is qualified by the current database (CTE names
    /// excepted), so the restored statements name `db`.`t`. A multi-table
    /// DELETE's target list is skipped, as Go's preprocessor skips
    /// `DeleteTableList`: its names may be aliases.
    fn preprocess_non_transactional_dml(&self, stmt: &mut BatchDmlStmt) -> Result<(), DriverError> {
        struct CteNames(Vec<String>);
        impl Visitor for CteNames {
            fn enter(&mut self, node: &mut dyn Any) -> bool {
                if let Some(with) = node.downcast_mut::<tidb_ast::WithClause>() {
                    self.0.extend(with.ctes.iter().map(|cte| lower(&cte.name)));
                }
                false
            }
            fn leave(&mut self, _node: &mut dyn Any) -> bool {
                true
            }
        }
        struct Qualify<'a> {
            current_db: &'a str,
            ctes: &'a [String],
        }
        impl Visitor for Qualify<'_> {
            fn enter(&mut self, node: &mut dyn Any) -> bool {
                if let Some(table) = node.downcast_mut::<TableRef>() {
                    if let [name] = table.name.as_slice() {
                        if !self.ctes.contains(&lower(name)) {
                            table.name.insert(0, self.current_db.to_owned());
                        }
                    }
                }
                false
            }
            fn leave(&mut self, _node: &mut dyn Any) -> bool {
                true
            }
        }

        let current_db = self.current_database().to_owned();
        let mut ctes = CteNames(Vec::new());
        accept_dml(&mut stmt.dml, &mut ctes);
        let needs_db = |path: &[String]| path.len() == 1 && !ctes.0.contains(&lower(&path[0]));
        let mut unqualified = false;
        {
            let mut finder = FindUnqualified {
                ctes: &ctes.0,
                found: false,
            };
            accept_dml(&mut stmt.dml, &mut finder);
            unqualified |= finder.found;
        }
        if let BatchDml::Insert(insert) = &stmt.dml {
            unqualified |= needs_db(&insert.table);
        }
        if unqualified && current_db.is_empty() {
            // Go `ErrNoDB`.
            return Err(DriverError::Schema(
                tidb_executor::SchemaErrorKind::NoDatabaseSelected,
            ));
        }
        let mut qualify = Qualify {
            current_db: &current_db,
            ctes: &ctes.0,
        };
        accept_dml(&mut stmt.dml, &mut qualify);
        if let BatchDml::Insert(insert) = &mut stmt.dml {
            if needs_db(&insert.table) {
                insert.table.insert(0, current_db.clone());
            }
        }
        Ok(())
    }

    /// Go `checkConstraint`.
    fn check_non_transactional_constraint(&self, stmt: &BatchDmlStmt) -> Result<(), DriverError> {
        let autocommit = self.is_autocommit();
        let in_txn = self.in_transaction();
        if !(autocommit && !in_txn) {
            return Err(plain_error(format!(
                "non-transactional DML can only run in auto-commit mode. auto-commit:{autocommit}, inTxn:{in_txn}"
            )));
        }
        let dml_batch_size = self
            .vars
            .system_value("tidb_dml_batch_size")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        if self.session_bool("tidb_enable_batch_dml", false)
            && dml_batch_size > 0
            && (self.session_bool("tidb_batch_delete", false)
                || self.session_bool("tidb_batch_insert", false))
        {
            return Err(plain_error(
                "can't run non-transactional DML with batch-dml",
            ));
        }
        if self
            .vars
            .system_value("tidb_read_consistency")
            .is_ok_and(|value| value.eq_ignore_ascii_case("weak"))
        {
            return Err(plain_error(
                "can't run non-transactional under weak read consistency",
            ));
        }
        if self
            .vars
            .system_value("tidb_snapshot")
            .is_ok_and(|value| !value.is_empty())
        {
            return Err(plain_error(
                "can't do non-transactional DML when tidb_snapshot is set",
            ));
        }

        match &stmt.dml {
            BatchDml::Delete(delete) => {
                check_read_clauses(delete.limit.is_some(), !delete.order_by.is_empty())
            }
            BatchDml::Update(update) => {
                check_read_clauses(update.limit.is_some(), !update.order_by.is_empty())
            }
            BatchDml::Insert(insert) => {
                let Some(source) = &insert.source else {
                    return Err(plain_error(
                        "Non-transactional insert supports insert select stmt only",
                    ));
                };
                let QueryStmt::Select(select) = &**source else {
                    return Err(plain_error(
                        "Non-transactional insert doesn't support non-select source",
                    ));
                };
                if select.from.is_none() {
                    return Err(plain_error("table reference is nil"));
                }
                check_read_clauses(select.limit.is_some(), !select.order_by.is_empty())
            }
        }
    }

    /// Go `checkConstraintWithShardColumn`: in an UPDATE (or an INSERT's ON
    /// DUPLICATE KEY UPDATE) the shard column cannot be updated.
    fn check_constraint_with_shard_column(
        &self,
        stmt: &BatchDmlStmt,
        table_name: &TableSource,
        shard_column_info: Option<&ShardColumnInfo>,
        table_sources: &[TableSource],
    ) -> Result<(), DriverError> {
        match &stmt.dml {
            BatchDml::Update(update) => self.check_update_shard_column(
                &update.assignments,
                shard_column_info,
                table_name,
                table_sources,
                true,
            ),
            BatchDml::Insert(insert) => self.check_update_shard_column(
                &insert.on_duplicate,
                shard_column_info,
                table_name,
                table_sources,
                false,
            ),
            BatchDml::Delete(_) => Ok(()),
        }
    }

    /// Go `checkUpdateShardColumn`.
    fn check_update_shard_column(
        &self,
        assignments: &[Assignment],
        shard_column_info: Option<&ShardColumnInfo>,
        table_name: &TableSource,
        table_sources: &[TableSource],
        is_update: bool,
    ) -> Result<(), DriverError> {
        // If the table has an alias, the assignments use it.
        let mut aliased_table_name = lower(&table_name.name);
        for source in table_sources {
            if lower(&source.name) == aliased_table_name && !source.as_name.is_empty() {
                aliased_table_name = lower(&source.as_name);
            }
        }
        let Some(shard_column_info) = shard_column_info else {
            return Ok(());
        };
        let table_schema = lower(&table_name.schema);
        for assignment in assignments {
            let column = ColumnName::from_path(&assignment.col);
            let column_schema = lower(&column.schema);
            let same_db = column_schema == table_schema
                || (column_schema.is_empty() && table_schema == self.current_database());
            if !same_db {
                continue;
            }
            let same_table = lower(&column.table) == aliased_table_name
                || (is_update && table_sources.len() == 1);
            if !same_table {
                continue;
            }
            if lower(&column.name) == lower(&shard_column_info.name) {
                return Err(plain_error(
                    "Non-transactional DML, shard column cannot be updated",
                ));
            }
        }
        Ok(())
    }

    /// Go `buildSelectSQL`: the leftmost table, the SELECT reading its shard
    /// column (NULL values first), and the shard column itself.
    fn build_non_transactional_select_sql(
        &mut self,
        stmt: &mut BatchDmlStmt,
    ) -> Result<
        (
            TableSource,
            String,
            Option<ShardColumnInfo>,
            Vec<TableSource>,
        ),
        DriverError,
    > {
        let Some(join) = table_refs_join(&stmt.dml) else {
            return Err(plain_error("Non-transactional DML, table source not found"));
        };
        let mut table_sources = Vec::new();
        collect_table_sources_in_join(join, &mut table_sources)?;
        let Some(left_most) = table_sources.first().cloned() else {
            return Err(plain_error(
                "Non-transactional DML, no tables found in table refs",
            ));
        };

        let (shard_column_info, table_name) =
            self.select_shard_column(stmt, &table_sources, &left_most)?;

        let condition = match where_expr(&stmt.dml) {
            Some(condition) => condition.restore_with_flags(restore_flags()),
            None => "TRUE".to_owned(),
        };
        let shard_column_name = stmt
            .shard_column
            .as_deref()
            .map(ColumnName::from_path)
            .unwrap_or_default()
            .name;
        let db_name = self.database_original_name(&table_name.schema);
        // Assure NULL values are placed first.
        let select_sql = format!(
            "SELECT `{shard_column_name}` FROM `{db_name}`.`{}` WHERE {condition} ORDER BY IF(ISNULL(`{shard_column_name}`),0,1),`{shard_column_name}`",
            table_name.name
        );
        Ok((table_name, select_sql, shard_column_info, table_sources))
    }

    /// Go `selectShardColumn`.
    fn select_shard_column(
        &mut self,
        stmt: &mut BatchDmlStmt,
        table_sources: &[TableSource],
        left_most: &TableSource,
    ) -> Result<(Option<ShardColumnInfo>, TableSource), DriverError> {
        let (indexed, shard_column_info, selected) = if table_sources.len() == 1 {
            let table = self.non_transactional_table(&left_most.schema, &left_most.name)?;
            let (indexed, info) = match &stmt.shard_column {
                None => select_shard_column_automatically(stmt, &table, left_most)?,
                Some(path) => {
                    let name = lower(&ColumnName::from_path(path).name);
                    select_shard_column_by_given_name(&name, &table)?
                }
            };
            (indexed, info, left_most.clone())
        } else {
            let specified = stmt.shard_column.as_deref().map(ColumnName::from_path);
            match specified {
                None => {
                    let table = self.non_transactional_table(&left_most.schema, &left_most.name)?;
                    let (indexed, info) =
                        select_shard_column_automatically(stmt, &table, left_most)?;
                    (indexed, info, left_most.clone())
                }
                Some(column)
                    if !column.schema.is_empty()
                        && !column.table.is_empty()
                        && !column.name.is_empty() =>
                {
                    let (db, table, name) = (
                        lower(&column.schema),
                        lower(&column.table),
                        lower(&column.name),
                    );
                    // The specified table must be in the join.
                    let chosen = table_sources.iter().find(|source| {
                        let final_name = if source.as_name.is_empty() {
                            &source.name
                        } else {
                            &source.as_name
                        };
                        lower(&source.schema) == db && lower(final_name) == table
                    });
                    let Some(chosen) = chosen.cloned() else {
                        return Err(plain_error(format!(
                            "Non-transactional DML, shard column {db}.{table}.{name} is not in the tables involved in the join"
                        )));
                    };
                    let kv = self.non_transactional_table(&column.schema, &chosen.name)?;
                    let (indexed, info) = select_shard_column_by_given_name(&name, &kv)?;
                    (indexed, info, chosen)
                }
                Some(_) => {
                    return Err(plain_error(
                        "Non-transactional DML, shard column must be fully specified (i.e. `BATCH ON dbname.tablename.colname`) when multiple tables are involved",
                    ));
                }
            }
        };
        if !indexed {
            let name = stmt
                .shard_column
                .as_deref()
                .map(ColumnName::from_path)
                .unwrap_or_default()
                .name;
            return Err(plain_error(format!(
                "Non-transactional DML, shard column {} is not indexed",
                lower(&name)
            )));
        }
        Ok((shard_column_info, selected))
    }

    /// Go `InfoSchema().TableByName`.
    fn non_transactional_table(
        &mut self,
        schema: &str,
        name: &str,
    ) -> Result<std::sync::Arc<tidb_executor::kv_table::KvTable>, DriverError> {
        self.with_catalog_mut(|catalog| match catalog.table_in(schema, name) {
            Some(TableEntry::Kv(kv)) => Ok(std::sync::Arc::clone(kv)),
            _ => Err(DriverError::Schema(
                tidb_executor::SchemaErrorKind::UnknownTable(format!("{schema}.{name}")),
            )),
        })
    }

    /// `tnW.DBInfo.Name.O`: the database's name as it was created.
    fn database_original_name(&mut self, schema: &str) -> String {
        self.with_catalog_mut(|catalog| {
            Ok(catalog
                .database_names()
                .into_iter()
                .find(|name| name.eq_ignore_ascii_case(schema)))
        })
        .ok()
        .flatten()
        .unwrap_or_else(|| schema.to_owned())
    }

    /// Go `buildShardJobs`: reads the shard values in `max_chunk_size`
    /// chunks, as Go's record set returns them, and cuts a job whenever it
    /// holds `LIMIT` rows and the next value differs from the job's end.
    fn build_shard_jobs(
        &mut self,
        stmt: &BatchDmlStmt,
        select_sql: &str,
        shard_column_info: Option<&ShardColumnInfo>,
        max_chunk_size: usize,
    ) -> Result<Vec<Job>, DriverError> {
        let collation = shard_column_info
            .map(|info| info.field_type.collation())
            // Go `collate.GetCollator("")` for `_tidb_rowid`.
            .unwrap_or(tidb_datatype::Collation::Binary);
        // A NT-DML is not a SELECT: the SelectLimit must not cut the read,
        // and as a write it takes no max execution time.
        let select_limit = self.override_system_var("sql_select_limit", &u64::MAX.to_string());
        let max_execution_time = self.override_system_var("max_execution_time", "0");
        let result = self.run_with_columns(select_sql);
        self.restore_system_var("sql_select_limit", select_limit);
        self.restore_system_var("max_execution_time", max_execution_time);
        let rows = match result? {
            StmtOutput::Rows { rows, .. } => rows,
            _ => {
                return Err(plain_error(
                    "Non-transactional DML, expecting 1 record set, but got 0",
                ));
            }
        };

        let batch_size = usize::try_from(stmt.limit).unwrap_or(usize::MAX);
        if batch_size == 0 {
            return Err(plain_error(
                "Non-transactional DML, batch size should be positive",
            ));
        }
        let mut job_count = 0;
        let mut jobs: Vec<Job> = Vec::new();
        let mut current_size = 0;
        let mut current_start = Datum::Null;
        let mut current_end = Datum::Null;
        let append_job = |jobs: &mut Vec<Job>, id, start, end, size| {
            jobs.push(Job {
                start,
                end,
                err: None,
                job_id: id,
                job_size: size,
                sql: String::new(),
            });
        };
        for chunk in rows.chunks(max_chunk_size.max(1)) {
            if !jobs.is_empty() && chunk.len() + current_size < batch_size {
                // Not enough data for a batch.
                current_size += chunk.len();
                current_end = chunk.last().map(|row| row[0].clone()).unwrap_or_default();
                continue;
            }
            for row in chunk {
                let value = row[0].clone();
                if current_size == 0 {
                    current_start = value.clone();
                }
                if current_size >= batch_size {
                    let ordering = value
                        .compare(&current_end, collation)
                        .map_err(|error| plain_error(error.to_string()))?;
                    if ordering != std::cmp::Ordering::Equal {
                        job_count += 1;
                        append_job(
                            &mut jobs,
                            job_count,
                            current_start.clone(),
                            current_end.clone(),
                            current_size,
                        );
                        current_size = 0;
                        current_start = value.clone();
                    }
                }
                current_end = value;
                current_size += 1;
            }
        }
        // There's remaining work.
        if current_size > 0 {
            append_job(
                &mut jobs,
                job_count + 1,
                current_start,
                current_end,
                current_size,
            );
        }
        Ok(jobs)
    }

    /// Go `runJobs`: the single-threaded worker over the jobs' key ranges.
    fn run_non_transactional_jobs(
        &mut self,
        jobs: &mut [Job],
        stmt: &mut BatchDmlStmt,
        table_name: &TableSource,
        original_condition: Option<Expr>,
    ) -> Result<Vec<String>, DriverError> {
        let shard_column = stmt
            .shard_column
            .as_deref()
            .map(ColumnName::from_path)
            .unwrap_or_default();
        let table = self.non_transactional_table(&table_name.schema, &table_name.name)?;
        let shard_column_type = match table
            .columns()
            .iter()
            .find(|column| lower(&column.name) == lower(&shard_column.name))
        {
            Some(column) => column.field_type.clone(),
            None if lower(&shard_column.name) == EXTRA_HANDLE_NAME => {
                FieldType::new(FieldTypeCode::LongLong)
            }
            None => {
                return Err(plain_error("Non-transactional DML, shard column not found"));
            }
        };
        let redact = self
            .vars
            .system_value("tidb_redact_log")
            .map(|value| value.into_owned())
            .unwrap_or_else(|_| "OFF".to_owned());
        let ignore_error = self.session_bool("tidb_nontransactional_ignore_error", false);

        let total = jobs.len();
        let mut split_statements = Vec::with_capacity(total);
        for index in 0..total {
            if stmt.dry_run == BatchDmlDryRun::SplitDml {
                if index > 0 && index + 1 < total {
                    continue;
                }
                let split = do_one_job(
                    &mut jobs[index],
                    stmt,
                    &shard_column,
                    &shard_column_type,
                    original_condition.as_ref(),
                );
                split_statements.extend(split);
            } else if let Some(sql) = do_one_job(
                &mut jobs[index],
                stmt,
                &shard_column,
                &shard_column_type,
                original_condition.as_ref(),
            ) {
                let job = &mut jobs[index];
                job.sql = sql;
                let text = format!("/* job {}/{total} */ {}", job.job_id, job.sql);
                if let Err(error) = self.run_with_columns(&text) {
                    job.err = Some(error);
                }
            }

            let job = &jobs[index];
            let Some(error) = &job.err else {
                continue;
            };
            // If the first job failed, there is a large chance that all jobs
            // will fail, so return early. Go annotates the cause, which its
            // server strips before the client sees it.
            if index == 0 {
                return Err(error.clone());
            }
            if !ignore_error {
                return Err(DriverError::DdlCoded {
                    errno: ERR_NON_TRANSACTIONAL_JOB_FAILURE,
                    message: format!(
                        "non-transactional job failed, job id: {}, total jobs: {total}. job range: [{}, {}], job sql: {}, err: {error}",
                        job.job_id,
                        go_datum_string(&job.start),
                        go_datum_string(&job.end),
                        job.describe(&redact),
                    ),
                });
            }
        }
        Ok(split_statements)
    }

    /// Go `buildExecuteResults`.
    fn build_execute_results(&self, jobs: &[Job]) -> Result<StmtOutput, DriverError> {
        let failed = jobs
            .iter()
            .filter(|job| job.err.is_some())
            .collect::<Vec<_>>();
        if failed.is_empty() {
            return Ok(StmtOutput::Rows {
                columns: vec![
                    (
                        "number of jobs".to_owned(),
                        FieldType::new(FieldTypeCode::Long),
                    ),
                    (
                        "job status".to_owned(),
                        FieldType::new(FieldTypeCode::String),
                    ),
                ],
                rows: vec![vec![
                    Datum::Int(i64::try_from(jobs.len()).unwrap_or(i64::MAX)),
                    Datum::new_string("all succeeded"),
                ]],
            });
        }
        // tidb_nontransactional_ignore_error must be set.
        let redact = self
            .vars
            .system_value("tidb_redact_log")
            .map(|value| value.into_owned())
            .unwrap_or_else(|_| "OFF".to_owned());
        let mut errors = String::new();
        for job in &failed {
            let error = job.err.as_ref().expect("a failed job has an error");
            errors.push_str(&format!("{}, {error};\n", job.describe(&redact)));
        }
        let shown = &errors[..floor_char_boundary(&errors, 500.min(errors.len() - 1))];
        Err(plain_error(format!(
            "{}/{} jobs failed in the non-transactional DML: {shown}, ...(more in logs)",
            failed.len(),
            jobs.len(),
        )))
    }

    /// The chunk size Go's shard SELECT record set returns its rows in.
    fn max_chunk_size_for_non_transactional_dml(&self) -> usize {
        self.vars
            .system_value("tidb_max_chunk_size")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1024)
    }

    /// Sets a session variable for the statement, returning its value to
    /// restore.
    fn override_system_var(&mut self, name: &str, value: &str) -> Option<String> {
        let previous = self.vars.system_value(name).ok()?.into_owned();
        self.vars
            .set_system_var_without_validation(name, value.to_owned())
            .ok()?;
        Some(previous)
    }

    fn restore_system_var(&mut self, name: &str, previous: Option<String>) {
        if let Some(previous) = previous {
            let _ = self.vars.set_system_var_without_validation(name, previous);
        }
    }
}

/// Go `checkReadClauses`.
fn check_read_clauses(has_limit: bool, has_order: bool) -> Result<(), DriverError> {
    if has_limit {
        return Err(plain_error(
            "Non-transactional statements don't support limit",
        ));
    }
    if has_order {
        return Err(plain_error(
            "Non-transactional statements don't support order by",
        ));
    }
    Ok(())
}

/// Go `selectShardColumnByGivenName`.
fn select_shard_column_by_given_name(
    shard_column_name: &str,
    table: &tidb_executor::kv_table::KvTable,
) -> Result<(bool, Option<ShardColumnInfo>), DriverError> {
    let has_clustered_index =
        table.pk_handle_offset().is_some() || !table.common_handle_offsets().is_empty();
    if shard_column_name == EXTRA_HANDLE_NAME && !has_clustered_index {
        return Ok((true, None));
    }
    let Some((offset, column)) = table
        .columns()
        .iter()
        .enumerate()
        .find(|(_, column)| lower(&column.name) == shard_column_name)
    else {
        return Err(plain_error(format!(
            "shard column {shard_column_name} not found"
        )));
    };
    let info = ShardColumnInfo {
        name: column.name.clone(),
        field_type: column.field_type.clone(),
    };
    // An integer handle.
    if table.is_clustered_handle_column(offset) {
        return Ok((true, Some(info)));
    }
    // Only the first column of a visible index is checked.
    let indexed = table
        .indexes()
        .iter()
        .any(|index| index.visible && index.column_offsets.first() == Some(&offset));
    Ok((indexed, Some(info)))
}

/// Go `selectShardColumnAutomatically`: the integer handle, a one-column
/// clustered primary key, or `_tidb_rowid`, written back into the statement
/// (qualified by the table's alias, so an alias works).
fn select_shard_column_automatically(
    stmt: &mut BatchDmlStmt,
    table: &tidb_executor::kv_table::KvTable,
    table_name: &TableSource,
) -> Result<(bool, Option<ShardColumnInfo>), DriverError> {
    let column_info = |offset: usize| {
        table.columns().get(offset).map(|column| ShardColumnInfo {
            name: column.name.clone(),
            field_type: column.field_type.clone(),
        })
    };
    let shard_column_info = if let Some(offset) = table.pk_handle_offset() {
        column_info(offset)
    } else if !table.common_handle_offsets().is_empty() {
        let Some(primary) = table.indexes().iter().find(|index| index.clustered_primary) else {
            return Err(plain_error(
                "Non-transactional DML, the clustered index is not found",
            ));
        };
        // A clustered index of several columns cannot be a shard column.
        let [offset] = primary.column_offsets.as_slice() else {
            return Err(plain_error(
                "Non-transactional DML, the clustered index contains multiple columns. Please specify a shard column",
            ));
        };
        column_info(*offset)
    } else {
        None
    };
    let shard_column_name = shard_column_info
        .as_ref()
        .map_or_else(|| EXTRA_HANDLE_NAME.to_owned(), |info| lower(&info.name));
    let output_table_name = if table_name.as_name.is_empty() {
        table_name.name.clone()
    } else {
        table_name.as_name.clone()
    };
    stmt.shard_column = Some(
        ColumnName {
            schema: table_name.schema.clone(),
            table: output_table_name,
            name: shard_column_name,
        }
        .to_path(),
    );
    Ok((true, shard_column_info))
}

/// Go `doOneJob`: the job's statement, the DML's condition narrowed to the
/// job's range. Returns the SQL to run (or the dry-run example), or `None`
/// with the job's error set when the range cannot be restored.
fn do_one_job(
    job: &mut Job,
    stmt: &mut BatchDmlStmt,
    shard_column: &ColumnName,
    shard_column_type: &FieldType,
    original_condition: Option<&Expr>,
) -> Option<String> {
    let column = || Box::new(shard_column.expr());
    let value = |datum: &Datum| value_expr(datum, shard_column_type);
    let restored = (|| {
        let condition = if job.start.is_null() {
            let is_null = Expr::Is {
                expr: column(),
                not: false,
                target: tidb_ast::IsTarget::Null,
            };
            if job.end.is_null() {
                // `where x is null`
                is_null
            } else {
                // `where (x <= job.end) || (x is null)`
                Expr::Binary(
                    BinaryOp::LogicOr,
                    Box::new(Expr::Binary(
                        BinaryOp::Le,
                        column(),
                        Box::new(value(&job.end)?),
                    )),
                    Box::new(is_null),
                )
            }
        } else {
            // A normal `where x between start and end`.
            Expr::Between {
                expr: column(),
                low: Box::new(value(&job.start)?),
                high: Box::new(value(&job.end)?),
                not: false,
            }
        };
        let condition = match original_condition {
            None => condition,
            Some(original) => Expr::Binary(
                BinaryOp::LogicAnd,
                Box::new(condition),
                Box::new(original.clone()),
            ),
        };
        set_where_expr(&mut stmt.dml, condition);
        Some(restore_dml(&stmt.dml))
    })();
    if restored.is_none() {
        job.err = Some(plain_error(
            "Failed to restore the DML statement, probably because of unsupported type of the shard column",
        ));
    }
    restored
}

/// Go `driver.ValueExpr.Restore` of a job bound typed as the shard column.
/// `None` is Go's "Not implemented" for the kinds it cannot restore.
fn value_expr(datum: &Datum, field_type: &FieldType) -> Option<Expr> {
    Some(match datum {
        Datum::Null => Expr::Null,
        Datum::Int(value) => {
            if field_type.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN) {
                Expr::Bool(*value > 0)
            } else {
                Expr::Int(value.to_string())
            }
        }
        Datum::UInt(value) => Expr::Int(value.to_string()),
        Datum::Real(value) | Datum::Float32(value) => Expr::Float(*value),
        Datum::String(value) => {
            Expr::RawString(String::from_utf8_lossy(value.bytes()).into_owned())
        }
        Datum::Bytes(value) => Expr::RawString(String::from_utf8_lossy(value).into_owned()),
        Datum::Decimal(value) => Expr::Decimal(value.to_string()),
        Datum::BinaryLiteral(value) => {
            if field_type.is_unsigned() {
                Expr::Hex(
                    value
                        .as_bytes()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect(),
                )
            } else {
                Expr::Decimal(value.to_bit_literal_string(true))
            }
        }
        Datum::Duration(value) => Expr::RawString(value.to_string()),
        Datum::Time(value) => Expr::RawString(value.to_string()),
        Datum::Enum(..)
        | Datum::Bit(_)
        | Datum::Set(..)
        | Datum::Json(_)
        | Datum::Raw(_)
        | Datum::MinNotNull
        | Datum::MaxValue
        | Datum::VectorFloat32(_) => return None,
    })
}

/// Go `Datum.String`: `(Kind, value)`.
fn go_datum_string(datum: &Datum) -> String {
    let (kind, value) = match datum {
        Datum::Null => ("KindNull", "<nil>".to_owned()),
        Datum::Int(value) => ("KindInt64", value.to_string()),
        Datum::UInt(value) => ("KindUint64", value.to_string()),
        Datum::Real(value) => ("KindFloat64", value.to_string()),
        Datum::Float32(value) => ("KindFloat32", value.to_string()),
        Datum::String(value) => (
            "KindString",
            String::from_utf8_lossy(value.bytes()).into_owned(),
        ),
        Datum::Bytes(value) => (
            "KindBytes",
            format!(
                "[{}]",
                value
                    .iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        ),
        Datum::Decimal(value) => ("KindMysqlDecimal", value.to_string()),
        Datum::Duration(value) => ("KindMysqlDuration", value.to_string()),
        Datum::Time(value) => ("KindMysqlTime", value.to_string()),
        other => ("KindInterface", format!("{other:?}")),
    };
    format!("({kind}, {value})")
}

/// Go `buildDryRunResults`.
fn dry_run_results(dry_run: BatchDmlDryRun, results: Vec<String>) -> StmtOutput {
    let field_name = if dry_run == BatchDmlDryRun::SplitDml {
        "split statement examples"
    } else {
        "query statement"
    };
    StmtOutput::Rows {
        columns: vec![(field_name.to_owned(), FieldType::new(FieldTypeCode::String))],
        rows: results
            .into_iter()
            .map(|result| vec![Datum::new_string(result)])
            .collect(),
    }
}

/// Go `ShardableDMLStmt.TableRefsJoin`.
fn table_refs_join(dml: &BatchDml) -> Option<JoinView<'_>> {
    match dml {
        BatchDml::Update(update) => Some(match &update.kind {
            UpdateKind::Single(table) => JoinView::Table(table),
            UpdateKind::Multi { from, .. } => JoinView::Join(from),
        }),
        BatchDml::Delete(delete) => Some(match &delete.kind {
            DeleteKind::Single(table) => JoinView::Table(table),
            DeleteKind::Multi { from, .. } => JoinView::Join(from),
        }),
        BatchDml::Insert(insert) => match insert.source.as_deref() {
            Some(QueryStmt::Select(select)) => select.from.as_ref().map(JoinView::Join),
            _ => None,
        },
    }
}

/// A DML's table refs: one table, or a join.
#[derive(Clone, Copy)]
enum JoinView<'a> {
    Table(&'a TableRef),
    Join(&'a Join),
}

/// Go `collectTableSourcesInJoin`.
fn collect_table_sources_in_join(
    node: JoinView<'_>,
    table_sources: &mut Vec<TableSource>,
) -> Result<(), DriverError> {
    match node {
        JoinView::Table(table) => {
            let Some(source) = TableSource::from_ref(table) else {
                return Err(plain_error(
                    "Non-transactional DML, table name not found in join",
                ));
            };
            table_sources.push(source);
        }
        JoinView::Join(join) => {
            collect_join_node(&join.left, table_sources)?;
            if let Some(right) = &join.right {
                collect_join_node(right, table_sources)?;
            }
        }
    }
    Ok(())
}

fn collect_join_node(
    node: &JoinNode,
    table_sources: &mut Vec<TableSource>,
) -> Result<(), DriverError> {
    match node {
        JoinNode::Table(table) => {
            collect_table_sources_in_join(JoinView::Table(table), table_sources)
        }
        JoinNode::Join(join) => collect_table_sources_in_join(JoinView::Join(join), table_sources),
        _ => Err(plain_error(
            "Non-transactional DML, table name not found in join",
        )),
    }
}

/// Go `ShardableDMLStmt.WhereExpr`.
fn where_expr(dml: &BatchDml) -> Option<&Expr> {
    match dml {
        BatchDml::Update(update) => update.where_clause.as_ref(),
        BatchDml::Delete(delete) => delete.where_clause.as_ref(),
        BatchDml::Insert(insert) => match insert.source.as_deref() {
            Some(QueryStmt::Select(select)) => select.where_clause.as_ref(),
            _ => None,
        },
    }
}

/// Go `ShardableDMLStmt.SetWhereExpr`.
fn set_where_expr(dml: &mut BatchDml, condition: Expr) {
    match dml {
        BatchDml::Update(update) => update.where_clause = Some(condition),
        BatchDml::Delete(delete) => delete.where_clause = Some(condition),
        BatchDml::Insert(insert) => {
            if let Some(source) = insert.source.as_mut() {
                if let QueryStmt::Select(select) = &mut **source {
                    select.where_clause = Some(condition);
                }
            }
        }
    }
}

/// The inner DML restored with Go's split-statement flags.
fn restore_dml(dml: &BatchDml) -> String {
    let stmt = match dml.clone() {
        BatchDml::Insert(insert) => tidb_ast::DmlStmt::Insert(insert),
        BatchDml::Update(update) => tidb_ast::DmlStmt::Update(update),
        BatchDml::Delete(delete) => tidb_ast::DmlStmt::Delete(delete),
    };
    tidb_ast::Stmt::Dml(tidb_ast::NodeBox::new(stmt)).restore_with_flags(restore_flags())
}

fn accept_dml<V: Visitor>(dml: &mut BatchDml, visitor: &mut V) {
    match dml {
        BatchDml::Insert(insert) => {
            Visitable::accept(&mut **insert, visitor);
        }
        BatchDml::Update(update) => {
            Visitable::accept(&mut **update, visitor);
        }
        BatchDml::Delete(delete) => {
            Visitable::accept(&mut **delete, visitor);
        }
    }
}

/// Whether the DML names a table without a database.
struct FindUnqualified<'a> {
    ctes: &'a [String],
    found: bool,
}

impl Visitor for FindUnqualified<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(table) = node.downcast_mut::<TableRef>() {
            if let [name] = table.name.as_slice() {
                self.found |= !self.ctes.contains(&lower(name));
            }
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        true
    }
}

/// The largest char boundary at or below `index`.
fn floor_char_boundary(text: &str, mut index: usize) -> usize {
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}
