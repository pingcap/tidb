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

//! Go `pkg/ddl/job_submitter.go`: the jobs a node's sessions submit go
//! through one channel; one loop takes whatever is queued as a batch, merges
//! CREATE TABLE jobs under `tidb_enable_fast_create_table`, and inserts the
//! batch into the job table in one transaction.

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

use tidb_ddl_jobsubmit::JobSpec;
use tidb_model::{ActionType, GoField, GoShared, InvolvingSchemaInfo, JobArgsValue};
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

use crate::real_tikv_ddl::{ClusterDdlError, DdlJobDoneChMap};

/// Go `jobSubmitResult`: the submitted job's id, and the table id a merged
/// CREATE TABLE gave this session's table.
#[derive(Clone, Debug)]
pub(crate) struct JobSubmitResult {
    pub(crate) job_id: i64,
    pub(crate) action: ActionType,
    /// The id of the table this submission created, when its CREATE TABLE
    /// was merged into a CREATE TABLES job.
    pub(crate) merged_table_id: Option<i64>,
}

type ResultCh = Sender<Result<JobSubmitResult, ClusterDdlError>>;

/// Go `JobWrapper`: one job and the result channels of every submission it
/// carries (several once merged).
struct JobWrapper {
    spec: JobSpec,
    result_ch: Vec<ResultCh>,
}

impl JobWrapper {
    /// Go `JobWrapper.NotifyResult`.
    fn notify_result(&self, result: &Result<(), ClusterDdlError>) {
        let merged = self.result_ch.len() > 1;
        for (position, result_ch) in self.result_ch.iter().enumerate() {
            let answer = match result {
                Ok(()) => Ok(JobSubmitResult {
                    job_id: self.spec.job.id,
                    action: self.spec.job.type_,
                    merged_table_id: merged.then(|| merged_table_id(&self.spec, position)).flatten(),
                }),
                Err(error) => Err(error.clone()),
            };
            let _ = result_ch.send(answer);
        }
    }
}

/// The table id the `position`-th submission of a merged job got.
fn merged_table_id(spec: &JobSpec, position: usize) -> Option<i64> {
    let JobArgsValue::BatchCreateTable(Some(args)) = &spec.args else {
        return None;
    };
    let tables = args.read().tables.get().handles();
    let table = tables.get(position)?.as_ref()?;
    let info = table.read().table_info.get()?;
    let id = info.read().id;
    Some(id)
}

/// Go `JobSubmitter`: the node's channel into its submit loop.
pub(crate) struct JobSubmitter {
    limit_job_ch: Sender<JobWrapper>,
}

impl JobSubmitter {
    /// Go `JobSubmitter.submitLoop`, started with the DDL.
    pub(crate) fn start<S>(
        opener: Arc<RealOptimisticTransactionOpener<S>>,
        sess_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
        timeout: Duration,
        ddl_job_done_ch_map: Arc<DdlJobDoneChMap>,
    ) -> Self
    where
        S: StoreClient + Send + Sync + 'static,
    {
        let (limit_job_ch, receiver) = channel();
        std::thread::Builder::new()
            .name("ddl-job-submitter".to_owned())
            .spawn(move || {
                submit_loop(
                    &receiver,
                    &opener,
                    sess_pool.as_ref(),
                    timeout,
                    &ddl_job_done_ch_map,
                )
            })
            .expect("the DDL job submitter thread starts");
        Self { limit_job_ch }
    }

    /// Go `executor.deliveryJob` and the wait on its result channel.
    pub(crate) fn delivery_job(&self, spec: JobSpec) -> Result<JobSubmitResult, ClusterDdlError> {
        let (result_ch, result) = channel();
        self.limit_job_ch
            .send(JobWrapper {
                spec,
                result_ch: vec![result_ch],
            })
            .map_err(|_| ClusterDdlError::NotCommitted("the DDL job submitter is stopped".to_owned()))?;
        result
            .recv()
            .map_err(|_| ClusterDdlError::NotCommitted("the DDL job submitter is stopped".to_owned()))?
    }
}

/// Go `submitLoop`: a job, and every job queued behind it, are one batch.
fn submit_loop<S: StoreClient>(
    receiver: &Receiver<JobWrapper>,
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    timeout: Duration,
    ddl_job_done_ch_map: &DdlJobDoneChMap,
) {
    while let Ok(job_w) = receiver.recv() {
        let mut job_ws = vec![job_w];
        job_ws.extend(receiver.try_iter());
        add_batch_ddl_jobs(job_ws, opener, sess_pool, timeout, ddl_job_done_ch_map);
    }
}

/// Go `addBatchDDLJobs`.
fn add_batch_ddl_jobs<S: StoreClient>(
    mut job_ws: Vec<JobWrapper>,
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    timeout: Duration,
    ddl_job_done_ch_map: &DdlJobDoneChMap,
) {
    let fast_create = tidb_vardef::ENABLE_FAST_CREATE_TABLE.load(std::sync::atomic::Ordering::SeqCst);
    if fast_create {
        match merge_create_table_jobs(job_ws) {
            Ok(merged) => job_ws = merged,
            Err((error, unmerged)) => {
                tidb_ddl_logutil::ddl_logger().warn(
                    "failed to merge create table jobs",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(format!("{error:?}")))],
                );
                job_ws = unmerged;
            }
        }
    }
    let mut specs: Vec<JobSpec> = job_ws
        .iter_mut()
        .map(|job_w| job_w.spec.clone())
        .collect();
    let result = crate::real_tikv_ddl::gen_gid_and_insert_jobs_with_retry(
        Arc::clone(opener),
        Some(sess_pool),
        &mut specs,
        timeout,
        |specs| Some(ddl_job_done_ch_map.register_job_done_channels(specs)),
    );
    for (job_w, spec) in job_ws.iter_mut().zip(specs) {
        job_w.spec = spec;
        job_w.notify_result(&result);
    }
}

/// Go `mergeCreateTableJobs`: the CREATE TABLE jobs of one schema, without
/// pre-allocated ids or foreign keys, go as CREATE TABLES jobs of up to eight
/// tables. On error the batch is answered as submitted.
fn merge_create_table_jobs(
    job_ws: Vec<JobWrapper>,
) -> Result<Vec<JobWrapper>, (ClusterDdlError, Vec<JobWrapper>)> {
    if job_ws.len() <= 1 {
        return Ok(job_ws);
    }
    let mut res_job_ws = Vec::with_capacity(job_ws.len());
    let mut mergeable: Vec<(String, Vec<JobWrapper>)> = Vec::new();
    for job_w in job_ws {
        // Jobs with pre-allocated ids are not merged.
        if job_w.spec.job.type_ != ActionType::ACTION_CREATE_TABLE || job_w.spec.id_allocated {
            res_job_ws.push(job_w);
            continue;
        }
        // ActionCreateTables does not support foreign keys.
        if create_table_has_foreign_keys(&job_w.spec) {
            res_job_ws.push(job_w);
            continue;
        }
        // CreateTables only takes tables of one schema.
        let schema = job_w.spec.job.schema_name.to_string();
        match mergeable.iter_mut().find(|(name, _)| *name == schema) {
            Some((_, jobs)) => jobs.push(job_w),
            None => mergeable.push((schema, vec![job_w])),
        }
    }
    const MAX_BATCH_SIZE: usize = 8;
    let mut failure = None;
    for (schema, jobs) in mergeable {
        let total = jobs.len();
        if total <= 1 {
            res_job_ws.extend(jobs);
            continue;
        }
        let mut jobs = jobs.into_iter().peekable();
        while jobs.peek().is_some() {
            let batch: Vec<JobWrapper> = jobs.by_ref().take(MAX_BATCH_SIZE).collect();
            let batch_size = batch.len();
            match merge_create_table_jobs_of_same_schema(batch) {
                Ok(merged) => {
                    tidb_ddl_logutil::ddl_logger().info(
                        "merge create table jobs",
                        &[
                            tidb_log::Field::new("schema", tidb_log::Value::Str(schema.clone())),
                            tidb_log::Field::new("total", tidb_log::Value::I64(total as i64)),
                            tidb_log::Field::new("batch_size", tidb_log::Value::I64(batch_size as i64)),
                        ],
                    );
                    res_job_ws.push(merged);
                }
                Err((error, batch)) => {
                    failure.get_or_insert(error);
                    res_job_ws.extend(batch);
                }
            }
        }
    }
    match failure {
        Some(error) => Err((error, res_job_ws)),
        None => Ok(res_job_ws),
    }
}

fn create_table_has_foreign_keys(spec: &JobSpec) -> bool {
    let JobArgsValue::CreateTable(Some(args)) = &spec.args else {
        return false;
    };
    args.read()
        .table_info
        .get()
        .is_some_and(|info| !info.read().foreign_keys.is_empty())
}

/// Go `buildQueryStringFromJobs`: each query trimmed and ended by `;`,
/// joined by a space.
fn build_query_string_from_jobs(jobs: &[JobWrapper]) -> String {
    jobs.iter()
        .map(|job| {
            let query = job.spec.job.query.to_string();
            let query = query.trim();
            if query.ends_with(';') {
                query.to_owned()
            } else {
                format!("{query};")
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Go `mergeCreateTableJobsOfSameSchema`.
fn merge_create_table_jobs_of_same_schema(
    mut job_ws: Vec<JobWrapper>,
) -> Result<JobWrapper, (ClusterDdlError, Vec<JobWrapper>)> {
    let Some(mut combined_job) = job_ws.first_mut().and_then(|first| first.spec.job.deep_clone()) else {
        return Err((
            ClusterDdlError::NotCommitted("expect non-empty jobs".to_owned()),
            job_ws,
        ));
    };
    combined_job.type_ = ActionType::ACTION_CREATE_TABLES;
    let mut tables = Vec::with_capacity(job_ws.len());
    let mut involving_schema_info = Vec::with_capacity(job_ws.len());
    let mut duplication = std::collections::HashSet::new();
    for job in &job_ws {
        let JobArgsValue::CreateTable(Some(args)) = &job.spec.args else {
            return Err((
                ClusterDdlError::NotCommitted("a CREATE TABLE job without its arguments".to_owned()),
                job_ws,
            ));
        };
        let args = args.read().clone();
        let name = args
            .table_info
            .get()
            .map(|info| info.read().name.lowercase().to_owned())
            .unwrap_or_default();
        if !duplication.insert(name.clone()) {
            // An error even under CREATE TABLE IF NOT EXISTS.
            return Err((
                ClusterDdlError::Plan(tidb_ddl_build::plan_error::DdlPlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS
                        .fast_gen_by_args(&["can not batch create tables with same name".into()]),
                )),
                job_ws,
            ));
        }
        involving_schema_info.push(InvolvingSchemaInfo {
            database: job.spec.job.schema_name.clone(),
            table: name.into(),
            ..InvolvingSchemaInfo::default()
        });
        tables.push(args);
    }
    combined_job.involving_schema_info = involving_schema_info.into();
    combined_job.query = build_query_string_from_jobs(&job_ws).into();
    let result_ch = job_ws
        .iter_mut()
        .flat_map(|job| std::mem::take(&mut job.result_ch))
        .collect();
    Ok(JobWrapper {
        spec: JobSpec {
            job: combined_job,
            args: JobArgsValue::BatchCreateTable(Some(GoShared::new(
                tidb_model::BatchCreateTableArgs {
                    tables: GoField::new(tables.into()),
                },
            ))),
            id_allocated: false,
        },
        result_ch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_model::{CreateTableArgs, Job, TableInfo};

    fn create_table(schema: &str, table: &str, query: &str, foreign_key: bool) -> (JobWrapper, Receiver<Result<JobSubmitResult, ClusterDdlError>>) {
        let mut info = TableInfo {
            name: tidb_ast::CiString::new(table),
            ..TableInfo::default()
        };
        if foreign_key {
            info.foreign_keys = vec![tidb_model::table::FKInfo::default()].into();
        }
        let mut job = Job::default();
        job.type_ = ActionType::ACTION_CREATE_TABLE;
        job.schema_name = schema.into();
        job.query = query.into();
        let (result_ch, result) = channel();
        (
            JobWrapper {
                spec: JobSpec {
                    job,
                    args: JobArgsValue::CreateTable(Some(GoShared::new(CreateTableArgs {
                        table_info: GoField::new(Some(GoShared::new(info))),
                        ..CreateTableArgs::default()
                    }))),
                    id_allocated: false,
                },
                result_ch: vec![result_ch],
            },
            result,
        )
    }

    fn summary(job_ws: &[JobWrapper]) -> Vec<(String, String, usize)> {
        job_ws
            .iter()
            .map(|job_w| {
                (
                    job_w.spec.job.type_.to_string(),
                    job_w.spec.job.query.to_string(),
                    job_w.result_ch.len(),
                )
            })
            .collect()
    }

    /// Go `TestMergeCreateTableJobs`'s rules: CREATE TABLE jobs of one schema
    /// merge (queries joined, every result channel kept); a table with a
    /// foreign key, a pre-allocated job, a lone job of its schema and any
    /// other action stay as they are.
    #[test]
    fn create_table_jobs_of_one_schema_merge_as_go() {
        let mut job_ws = Vec::new();
        let mut results = Vec::new();
        for (schema, table, query, fk) in [
            ("db", "t1", "create table t1(a int)", false),
            ("db", "t2", " create table t2(a int); ", false),
            ("db", "fk", "create table fk(a int)", true),
            ("other", "t3", "create table t3(a int)", false),
        ] {
            let (job_w, result) = create_table(schema, table, query, fk);
            job_ws.push(job_w);
            results.push(result);
        }
        let (mut allocated, _allocated_result) = create_table("db", "t4", "create table t4(a int)", false);
        allocated.spec.id_allocated = true;
        job_ws.push(allocated);
        let merged = merge_create_table_jobs(job_ws).map_err(|(error, _)| error).unwrap();
        let mut summary = summary(&merged);
        summary.sort();
        assert_eq!(
            summary,
            [
                ("create table".to_owned(), "create table fk(a int)".to_owned(), 1),
                ("create table".to_owned(), "create table t3(a int)".to_owned(), 1),
                ("create table".to_owned(), "create table t4(a int)".to_owned(), 1),
                (
                    "create tables".to_owned(),
                    "create table t1(a int); create table t2(a int);".to_owned(),
                    2
                ),
            ]
        );
        let batch = merged
            .iter()
            .find(|job_w| job_w.spec.job.type_ == ActionType::ACTION_CREATE_TABLES)
            .unwrap();
        assert_eq!(
            batch
                .spec
                .job
                .involving_schema_info
                .snapshot()
                .iter()
                .map(|info| (info.database.to_string(), info.table.to_string()))
                .collect::<Vec<_>>(),
            [("db".to_owned(), "t1".to_owned()), ("db".to_owned(), "t2".to_owned())]
        );
    }

    /// Go `mergeCreateTableJobsOfSameSchema`: two tables of one name refuse
    /// the merge, and the batch is submitted as it came.
    #[test]
    fn a_duplicated_table_name_leaves_the_batch_unmerged() {
        let (first, _first_result) = create_table("db", "t", "create table t(a int)", false);
        let (second, _second_result) = create_table("db", "T", "create table T(a int)", false);
        let Err((error, unmerged)) = merge_create_table_jobs(vec![first, second]) else {
            panic!("the merge is refused");
        };
        assert!(format!("{error:?}").contains("can not batch create tables with same name"), "{error:?}");
        assert_eq!(summary(&unmerged).len(), 2);
    }

    /// Go `NotifyResult`: every submission a merged job carries gets the
    /// job's id, and here the id its own table was given.
    #[test]
    fn a_merged_job_answers_each_submission_with_its_table() {
        let (first, first_result) = create_table("db", "t1", "create table t1(a int)", false);
        let (second, second_result) = create_table("db", "t2", "create table t2(a int)", false);
        let mut merged = merge_create_table_jobs(vec![first, second]).map_err(|(error, _)| error).unwrap();
        let batch = merged.pop().unwrap();
        let JobArgsValue::BatchCreateTable(Some(args)) = &batch.spec.args else {
            panic!("a CREATE TABLES job");
        };
        for (position, table) in args.read().tables.get().handles().into_iter().enumerate() {
            table.unwrap().read().table_info.get().unwrap().write().id = 100 + position as i64;
        }
        let mut batch = batch;
        batch.spec.job.id = 7;
        batch.notify_result(&Ok(()));
        let first = first_result.recv().unwrap().unwrap();
        let second = second_result.recv().unwrap().unwrap();
        assert_eq!((first.job_id, first.merged_table_id), (7, Some(100)));
        assert_eq!((second.job_id, second.merged_table_id), (7, Some(101)));
        assert_eq!(first.action, ActionType::ACTION_CREATE_TABLES);
    }
}
