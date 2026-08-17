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

//! SEED transcreation of two narrow, dependency-closed slices of Go `pkg/ddl`.
//!
//! This module is explicitly NOT a complete package transcreation. It carries
//! exactly two slices, each taken from a much larger Go file:
//!
//! 1. `pkg/ddl/job_submitter.go` lines 124-245 only: `mergeCreateTableJobs`,
//!    `buildQueryStringFromJobs`, and `mergeCreateTableJobsOfSameSchema`. The
//!    rest of that file is the `JobSubmitter` type, which is wired to etcd, the
//!    owner manager, the session pool, and the DDL job table; none of that is
//!    transcreated here.
//! 2. `pkg/ddl/executor.go` (7685 lines) lines 4211-4231 only: the
//!    `systemTables` set and the standalone `isUndroppableTable` function. The
//!    surrounding `executor` type and its ~7600 other lines are out of scope.
//!
//! Supporting Go definitions pulled in so the two slices close:
//!
//! - `pkg/ddl/ddl.go` lines 215-249: `JobWrapper`, `NewJobWrapper`,
//!   `NewJobWrapperWithArgs` (see [`JobWrapper`]).
//! - `pkg/util/mathutil/math.go` lines 96-113: `Divide2Batches` (see
//!   [`divide2_batches`]); it is not yet transcreated in `tidb-util`, so it is
//!   carried here with attribution rather than duplicated as a new crate API.
//!
//! Narrowings, each named at its definition site:
//!
//! - `jobSubmitResult` / `ResultCh`: the Go channel payload belongs to the
//!   `JobSubmitter` machinery that is out of scope. The channel slice is kept
//!   because merging concatenates it and the Go test asserts its length.
//! - `logutil.DDLLogger()` and `metrics` calls inside `mergeCreateTableJobs`
//!   are dropped; they have no observable effect on the merge result.
//! - `kerneltype.IsNextGen()` and the `skipCheckReservedSchemaObjInNextGen`
//!   failpoint are process-global build-mode facts, so the resolved boolean is
//!   a parameter of [`is_undroppable_table`].
//! - `mysql.SystemDB` / `mysql.WorkloadSchema`: `tidb-mysql` is not a
//!   dependency of this crate and dependency manifests are not edited here, so
//!   the two string constants are restated locally with their Go home named.
//! - Go's `errors.Trace` / `infoschema.ErrTableExists.FastGenByArgs` wrapping
//!   is represented by the typed [`MergeCreateTableJobsError`].
//! - Go map iteration order over the per-schema groups is unspecified; a
//!   `BTreeMap` is used so the grouped output order is deterministic. Both the
//!   Go tests and callers sort or group the result, so this is not observable.

use std::collections::BTreeMap;
use std::fmt;

use tidb_model::job::{InvolvingSchemaInfo, Job};
use tidb_model::job_args::{
    BatchCreateTableArgs, CreateTableArgs, EmptyArgs, GoField, JobArgsValue,
};
use tidb_model::{ActionType, GoShared, GoSharedPointerSlice, GoSharedSlice};

/// Go `mysql.SystemDB` from `pkg/parser/mysql/const.go`.
///
/// boundary: restated here because `tidb-mysql` is not a dependency of this
/// crate and dependency manifests are out of scope for this module.
pub const SYSTEM_DB: &str = "mysql";

/// Go `mysql.WorkloadSchema` from `pkg/parser/mysql/const.go`.
///
/// boundary: see [`SYSTEM_DB`].
pub const WORKLOAD_SCHEMA: &str = "workload_schema";

/// Go `pkg/ddl/executor.go` line 4211: `systemTables`, the `mysql`-schema
/// tables that may never be dropped.
pub const SYSTEM_TABLES: [&str; 3] = ["tidb", "gc_delete_range", "gc_delete_range_done"];

/// The maximum number of create-table jobs merged into one `CreateTables` job.
///
/// Go `pkg/ddl/job_submitter.go` line 155: `const maxBatchSize = 8`.
const MAX_BATCH_SIZE: usize = 8;

/// boundary: Go `pkg/ddl/job_submitter.go` `jobSubmitResult`.
///
/// The producing side of this value is the `JobSubmitter` loop, which is not
/// transcreated here. The type is retained as an opaque payload so that
/// [`JobWrapper::result_ch`] keeps its Go shape and length.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JobSubmitResult {
    /// Job identifier assigned by the submitter.
    pub job_id: i64,
    /// Whether the submitted job was merged into a batch job.
    pub merged: bool,
}

/// One result channel handle held by a [`JobWrapper`].
///
/// boundary: Go's `chan jobSubmitResult`. Merging only ever concatenates these
/// handles, so an identity token is enough to reproduce the Go behavior that
/// the tests observe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobSubmitResultChannel(usize);

impl JobSubmitResultChannel {
    /// Creates a fresh, distinct channel handle, as Go's `make(chan
    /// jobSubmitResult)` does.
    #[must_use]
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// The channel's identity token.
    #[must_use]
    pub fn id(&self) -> usize {
        self.0
    }
}

impl Default for JobSubmitResultChannel {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `pkg/ddl/ddl.go` line 215: `JobWrapper`, the `pkg/ddl`-package-local
/// wrapper around `*model.Job` used while a DDL job is being submitted.
///
/// boundary: `JobWrapper` lives in Go's `ddl` package, not in `model`, so it is
/// defined here rather than reused from `tidb-model`. Go embeds `*model.Job`,
/// which lets call sites write `jobW.Type`; the Rust form names the field.
#[derive(Clone, Debug)]
pub struct JobWrapper {
    /// Go's embedded `*model.Job`.
    pub job: Job,
    /// Go `IDAllocated`: see the field of the same name in
    /// `CreateTableConfig`. Jobs with pre-allocated IDs are never merged.
    pub id_allocated: bool,
    /// Go `JobArgs model.JobArgs`.
    pub job_args: JobArgsValue,
    /// Go `ResultCh []chan jobSubmitResult`. Job submission is asynchronous;
    /// when fast-create-table merges jobs, the merged wrapper carries every
    /// source job's channel.
    pub result_ch: Vec<JobSubmitResultChannel>,
}

impl JobWrapper {
    /// Go `NewJobWrapper` (`pkg/ddl/ddl.go` line 229).
    #[must_use]
    pub fn new(job: Job, id_allocated: bool) -> Self {
        Self {
            job,
            id_allocated,
            job_args: JobArgsValue::Empty(Some(GoShared::new(EmptyArgs::default()))),
            result_ch: vec![JobSubmitResultChannel::new()],
        }
    }

    /// Go `NewJobWrapperWithArgs` (`pkg/ddl/ddl.go` line 240).
    #[must_use]
    pub fn new_with_args(job: Job, args: JobArgsValue, id_allocated: bool) -> Self {
        Self {
            job,
            id_allocated,
            job_args: args,
            result_ch: vec![JobSubmitResultChannel::new()],
        }
    }

    /// The wrapped job's action type; Go reaches this through the embedded
    /// `*model.Job` as `jobW.Type`.
    #[must_use]
    pub fn action_type(&self) -> ActionType {
        self.job.type_
    }

    /// The wrapped job's schema name; Go's `jobW.SchemaName`.
    #[must_use]
    pub fn schema_name(&self) -> &str {
        self.job.schema_name.as_utf8().unwrap_or_default()
    }

    fn create_table_args(&self) -> Option<GoShared<CreateTableArgs>> {
        match &self.job_args {
            JobArgsValue::CreateTable(args) => args.clone(),
            _ => None,
        }
    }
}

/// Failures returned by the create-table job merge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeCreateTableJobsError {
    /// Go `errors.Trace(fmt.Errorf("expect non-empty jobs"))`.
    EmptyJobs,
    /// Go `infoschema.ErrTableExists.FastGenByArgs("can not batch create
    /// tables with same name")`, raised even for `CREATE TABLE IF NOT EXISTS`.
    TableExists,
    /// The wrapper claims `ActionCreateTable` but carries no
    /// `*model.CreateTableArgs`. Go type-asserts and panics instead.
    MissingCreateTableArgs,
}

impl fmt::Display for MergeCreateTableJobsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyJobs => f.write_str("expect non-empty jobs"),
            Self::TableExists => f.write_str("can not batch create tables with same name"),
            Self::MissingCreateTableArgs => f.write_str("expect *model.CreateTableArgs"),
        }
    }
}

impl std::error::Error for MergeCreateTableJobsError {}

/// Go `pkg/util/mathutil/math.go` line 98: `Divide2Batches`.
///
/// boundary: `mathutil.Divide2Batches` is not transcreated in `tidb-util`, so
/// the few lines are carried here with attribution. Go's generic form is
/// narrowed to `usize`, the only instantiation the merge slice uses.
#[must_use]
pub fn divide2_batches(total: usize, batches: usize) -> Vec<usize> {
    let mut result = Vec::with_capacity(batches);
    let mut total = total;
    let quotient = total / batches;
    let mut remainder = total % batches;
    while total > 0 {
        let mut size = quotient;
        if remainder > 0 {
            size += 1;
            remainder -= 1;
        }
        debug_assert!(size > 0, "size should be positive");
        result.push(size);
        total -= size;
    }
    result
}

/// Go `pkg/ddl/job_submitter.go` line 125: `mergeCreateTableJobs` merges
/// `CreateTable` jobs into `CreateTables` jobs.
///
/// Jobs are left unmerged when they are not `ActionCreateTable`, when their IDs
/// are pre-allocated, or when the table has foreign keys (`ActionCreateTables`
/// does not support foreign keys). Mergeable jobs are grouped per schema
/// because `CreateTables` only supports tables of one schema, then split into
/// batches of at most [`MAX_BATCH_SIZE`] tables.
pub fn merge_create_table_jobs(
    job_ws: Vec<JobWrapper>,
) -> Result<Vec<JobWrapper>, MergeCreateTableJobsError> {
    if job_ws.len() <= 1 {
        return Ok(job_ws);
    }
    let mut res_job_ws: Vec<JobWrapper> = Vec::with_capacity(job_ws.len());
    let mut mergeable_job_ws: BTreeMap<String, Vec<JobWrapper>> = BTreeMap::new();
    for job_w in job_ws {
        // we don't merge jobs with ID pre-allocated.
        if job_w.action_type() != ActionType::ACTION_CREATE_TABLE || job_w.id_allocated {
            res_job_ws.push(job_w);
            continue;
        }
        // ActionCreateTables doesn't support foreign key now.
        let args = job_w
            .create_table_args()
            .ok_or(MergeCreateTableJobsError::MissingCreateTableArgs)?;
        let has_foreign_keys = args
            .read()
            .table_info
            .get()
            .is_some_and(|info| !info.read().foreign_keys.handles().is_empty());
        if has_foreign_keys {
            res_job_ws.push(job_w);
            continue;
        }
        // CreateTables only support tables of same schema now.
        mergeable_job_ws
            .entry(job_w.schema_name().to_owned())
            .or_default()
            .push(job_w);
    }

    for (_schema, jobs) in mergeable_job_ws {
        let total = jobs.len();
        if total <= 1 {
            res_job_ws.extend(jobs);
            continue;
        }
        let batch_count = total.div_ceil(MAX_BATCH_SIZE);
        let mut start = 0usize;
        for batch_size in divide2_batches(total, batch_count) {
            let batch = &jobs[start..start + batch_size];
            let new_job_w = merge_create_table_jobs_of_same_schema(batch)?;
            start += batch_size;
            // logutil.DDLLogger().Info("merge create table jobs", ...): dropped.
            res_job_ws.push(new_job_w);
        }
    }
    Ok(res_job_ws)
}

/// Go `pkg/ddl/job_submitter.go` line 187: `buildQueryStringFromJobs`.
///
/// Concatenates the jobs' queries into a single query string separated by a
/// semicolon and a space. Trailing spaces are trimmed from each query, and a
/// semicolon is appended when one is not already present.
#[must_use]
pub fn build_query_string_from_jobs(jobs: &[JobWrapper]) -> String {
    let mut query_builder = String::new();
    for (i, job) in jobs.iter().enumerate() {
        let q = job.job.query.as_utf8().unwrap_or_default().trim();
        query_builder.push_str(q);
        if !q.ends_with(';') {
            query_builder.push(';');
        }

        if i < jobs.len() - 1 {
            query_builder.push(' ');
        }
    }
    query_builder
}

/// Go `pkg/ddl/job_submitter.go` line 203: `mergeCreateTableJobsOfSameSchema`
/// combines `CreateTable` jobs into one `BatchCreateTable` job.
pub fn merge_create_table_jobs_of_same_schema(
    job_ws: &[JobWrapper],
) -> Result<JobWrapper, MergeCreateTableJobsError> {
    if job_ws.is_empty() {
        return Err(MergeCreateTableJobsError::EmptyJobs);
    }

    let mut combined_job: Option<Job> = None;
    let mut tables: Vec<Option<GoShared<CreateTableArgs>>> = Vec::with_capacity(job_ws.len());
    let mut involving_schema_info: Vec<InvolvingSchemaInfo> = Vec::with_capacity(job_ws.len());

    // if there is any duplicated table name
    let mut duplication: std::collections::HashSet<String> = std::collections::HashSet::new();
    for job in job_ws {
        if combined_job.is_none() {
            let mut cloned = job.job.clone();
            cloned.type_ = ActionType::ACTION_CREATE_TABLES;
            combined_job = Some(cloned);
        }
        let job_args = job
            .create_table_args()
            .ok_or(MergeCreateTableJobsError::MissingCreateTableArgs)?;
        tables.push(Some(job_args.clone()));

        let info = job_args
            .read()
            .table_info
            .get()
            .ok_or(MergeCreateTableJobsError::MissingCreateTableArgs)?;
        let lower_name = info.read().name.lowercase().to_owned();
        if !duplication.insert(lower_name.clone()) {
            // return err even if create table if not exists
            return Err(MergeCreateTableJobsError::TableExists);
        }

        involving_schema_info.push(InvolvingSchemaInfo {
            database: job.job.schema_name.clone(),
            table: lower_name.into(),
            ..Default::default()
        });
    }

    let mut combined_job = combined_job.expect("non-empty jobs always set the combined job");
    combined_job.involving_schema_info = GoSharedSlice::from_vec(involving_schema_info);
    combined_job.query = build_query_string_from_jobs(job_ws).into();

    let args = BatchCreateTableArgs {
        tables: GoField::new(GoSharedPointerSlice::from_handles(tables)),
    };
    let mut new_job_w = JobWrapper {
        job: combined_job,
        id_allocated: false,
        job_args: JobArgsValue::BatchCreateTable(Some(GoShared::new(args))),
        result_ch: Vec::with_capacity(job_ws.len()),
    };
    // merge the result channels.
    for j in job_ws {
        new_job_w.result_ch.extend(j.result_ch.iter().cloned());
    }

    Ok(new_job_w)
}

/// Go `pkg/ddl/executor.go` line 7626: `isReservedSchemaObjInNextGen`.
///
/// boundary: Go consults the process-global `kerneltype.IsNextGen()` build mode
/// and the `skipCheckReservedSchemaObjInNextGen` failpoint. Neither is
/// transcreated here, so the resolved decision arrives as `next_gen`.
#[must_use]
pub fn is_reserved_schema_obj_in_next_gen(id: i64, next_gen: bool) -> bool {
    next_gen && tidb_metadef::is_reserved_id(id)
}

/// Go `pkg/ddl/executor.go` line 4217: `isUndroppableTable`.
///
/// Reports whether the named table is protected from `DROP`. Reserved
/// next-generation schema objects and everything in the workload schema are
/// undroppable; inside `mysql`, only [`SYSTEM_TABLES`] are.
#[must_use]
pub fn is_undroppable_table(
    schema: &str,
    table: &str,
    table_info: &tidb_model::table_info::TableInfo,
    next_gen: bool,
) -> bool {
    if is_reserved_schema_obj_in_next_gen(table_info.id, next_gen) {
        return true;
    }
    if schema == WORKLOAD_SCHEMA {
        return true;
    }
    if schema != SYSTEM_DB {
        return false;
    }
    if SYSTEM_TABLES.contains(&table) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    //! Go `pkg/ddl/executor_nokit_test.go`, all four tests:
    //! `TestBuildQueryStringFromJobs`, `TestMergeCreateTableJobsOfSameSchema`,
    //! `TestMergeCreateTableJobs`, and `TestIsUndroppableTable`.

    use super::*;
    use tidb_model::history::HistoryInfo;
    use tidb_model::job_enums::get_job_ver_in_use;
    use tidb_model::table::FKInfo;
    use tidb_model::table_info::TableInfo;

    /// Builds a `CiString` the way Go's `ast.NewCIStr` does.
    fn ci_str(name: &str) -> tidb_ast::CiString {
        tidb_ast::CiString::new(name)
    }

    /// `model.Job` keeps `mu` and `args` private, so fixtures start from
    /// `Job::default()` and assign the public fields instead of using a struct
    /// literal with `..Default::default()`.
    fn job_with(fields: impl FnOnce(&mut Job)) -> Job {
        let mut job = Job::default();
        fields(&mut job);
        job
    }

    fn query_job(query: &str) -> JobWrapper {
        JobWrapper {
            job: job_with(|j| j.query = query.into()),
            id_allocated: false,
            job_args: JobArgsValue::Empty(None),
            result_ch: Vec::new(),
        }
    }

    fn create_table_args(table_info: TableInfo, fk_check: bool) -> JobArgsValue {
        JobArgsValue::CreateTable(Some(GoShared::new(CreateTableArgs {
            table_info: GoField::new(Some(GoShared::new(table_info))),
            fk_check: GoField::new(fk_check),
            ..Default::default()
        })))
    }

    fn named_table(name: &str) -> TableInfo {
        TableInfo {
            name: ci_str(name),
            ..Default::default()
        }
    }

    fn fk_table() -> TableInfo {
        TableInfo {
            foreign_keys: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(
                FKInfo::default(),
            ))]),
            ..Default::default()
        }
    }

    fn create_table_job(schema: &str) -> Job {
        job_with(|j| {
            j.version = get_job_ver_in_use();
            j.schema_name = schema.into();
            j.type_ = ActionType::ACTION_CREATE_TABLE;
        })
    }

    /// Go's `require.EqualValues` on `[]*JobWrapper` compares the wrappers
    /// deeply. `model.Job` has no `PartialEq` in `tidb-model`, so the Rust
    /// tests compare the observable fields the merge can touch.
    fn fingerprint(w: &JobWrapper) -> (String, u8, String, bool, Option<String>, usize) {
        (
            w.schema_name().to_owned(),
            w.action_type().0,
            w.job.query.as_utf8().unwrap_or_default().to_owned(),
            w.id_allocated,
            w.create_table_args().map(|a| {
                a.read()
                    .table_info
                    .get()
                    .map(|i| i.read().name.lowercase().to_owned())
                    .unwrap_or_default()
            }),
            w.result_ch.len(),
        )
    }

    fn fingerprints(ws: &[JobWrapper]) -> Vec<(String, u8, String, bool, Option<String>, usize)> {
        ws.iter().map(fingerprint).collect()
    }

    fn created_table_name(w: &JobWrapper) -> String {
        w.create_table_args()
            .and_then(|a| {
                a.read()
                    .table_info
                    .get()
                    .map(|i| i.read().name.lowercase().to_owned())
            })
            .unwrap_or_default()
    }

    #[test]
    fn test_build_query_string_from_jobs() {
        let cases: Vec<(&str, Vec<JobWrapper>, &str)> = vec![
            ("Empty jobs", vec![], ""),
            (
                "Single create table job",
                vec![query_job(
                    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));",
                )],
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));",
            ),
            (
                "Multiple create table jobs with trailing semicolons",
                vec![
                    query_job("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));"),
                    query_job("CREATE TABLE products (id INT PRIMARY KEY, description TEXT);"),
                ],
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id INT PRIMARY KEY, description TEXT);",
            ),
            (
                "Multiple create table jobs with and without trailing semicolons",
                vec![
                    query_job("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))"),
                    query_job("CREATE TABLE products (id INT PRIMARY KEY, description TEXT);"),
                    query_job(
                        "   CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product_id INT) ",
                    ),
                ],
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id INT PRIMARY KEY, description TEXT); CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product_id INT);",
            ),
        ];

        for (name, jobs, expected) in cases {
            assert_eq!(
                build_query_string_from_jobs(&jobs),
                expected,
                "Query strings do not match: {name}"
            );
        }
    }

    #[test]
    fn test_merge_create_table_jobs_of_same_schema() {
        let job1 = JobWrapper::new_with_args(
            job_with(|j| {
                j.version = get_job_ver_in_use();
                j.schema_id = 1;
                j.type_ = ActionType::ACTION_CREATE_TABLE;
                j.binlog_info = Some(GoShared::new(HistoryInfo::default()));
                j.query = "create table db1.t1 (c1 int, c2 int)".into();
            }),
            create_table_args(named_table("t1"), false),
            false,
        );
        let job2 = JobWrapper::new_with_args(
            job_with(|j| {
                j.version = get_job_ver_in_use();
                j.schema_id = 1;
                j.type_ = ActionType::ACTION_CREATE_TABLE;
                j.binlog_info = Some(GoShared::new(HistoryInfo::default()));
                j.query = "create table db1.t2 (c1 int, c2 int);".into();
            }),
            create_table_args(named_table("t2"), true),
            false,
        );
        let job = merge_create_table_jobs_of_same_schema(&[job1, job2]).unwrap();
        assert_eq!(
            job.job.query.as_utf8().unwrap_or_default(),
            "create table db1.t1 (c1 int, c2 int); create table db1.t2 (c1 int, c2 int);"
        );
    }

    #[test]
    fn test_merge_create_table_jobs_zero_or_one_job() {
        let new_ws = merge_create_table_jobs(vec![]).unwrap();
        assert!(new_ws.is_empty());

        let job_ws = vec![JobWrapper {
            job: Job::default(),
            id_allocated: false,
            job_args: JobArgsValue::Empty(None),
            result_ch: Vec::new(),
        }];
        let expected = fingerprints(&job_ws);
        let new_ws = merge_create_table_jobs(job_ws).unwrap();
        assert_eq!(fingerprints(&new_ws), expected);
    }

    #[test]
    fn test_merge_create_table_jobs_non_create_table_are_not_merged() {
        let job_ws = vec![
            JobWrapper {
                job: create_table_job("db"),
                id_allocated: false,
                job_args: create_table_args(named_table("t1"), false),
                result_ch: Vec::new(),
            },
            JobWrapper {
                job: job_with(|j| {
                    j.schema_name = "db".into();
                    j.type_ = ActionType::ACTION_ADD_COLUMN;
                }),
                id_allocated: false,
                job_args: JobArgsValue::Empty(None),
                result_ch: Vec::new(),
            },
            JobWrapper {
                job: create_table_job("db"),
                id_allocated: false,
                job_args: create_table_args(named_table("t2"), false),
                result_ch: Vec::new(),
            },
        ];
        let mut new_ws = merge_create_table_jobs(job_ws).unwrap();
        assert_eq!(new_ws.len(), 2);
        new_ws.sort_by_key(|w| w.action_type().0);
        assert_eq!(new_ws[0].action_type(), ActionType::ACTION_ADD_COLUMN);
        assert_eq!(new_ws[1].action_type(), ActionType::ACTION_CREATE_TABLES);
    }

    #[test]
    fn test_merge_create_table_jobs_pre_allocated_ids_are_not_merged() {
        let job_ws = vec![
            JobWrapper {
                job: create_table_job("db"),
                id_allocated: true,
                job_args: create_table_args(named_table("t1"), false),
                result_ch: Vec::new(),
            },
            JobWrapper {
                job: create_table_job("db"),
                id_allocated: false,
                job_args: create_table_args(named_table("t2"), false),
                result_ch: Vec::new(),
            },
        ];
        let expected = fingerprints(&job_ws);
        let mut new_ws = merge_create_table_jobs(job_ws).unwrap();
        new_ws.sort_by_key(created_table_name);
        assert_eq!(fingerprints(&new_ws), expected);
    }

    #[test]
    fn test_merge_create_table_jobs_foreign_keys_are_not_merged() {
        let job_ws = vec![
            JobWrapper {
                job: create_table_job("db"),
                id_allocated: false,
                job_args: create_table_args(fk_table(), false),
                result_ch: Vec::new(),
            },
            JobWrapper {
                job: create_table_job("db"),
                id_allocated: false,
                job_args: create_table_args(named_table("t2"), false),
                result_ch: Vec::new(),
            },
        ];
        let expected = fingerprints(&job_ws);
        let mut new_ws = merge_create_table_jobs(job_ws).unwrap();
        new_ws.sort_by_key(created_table_name);
        assert_eq!(fingerprints(&new_ws), expected);
    }

    #[test]
    fn test_merge_create_table_jobs_different_schema_are_not_merged() {
        let job_ws = vec![
            JobWrapper {
                job: create_table_job("db1"),
                id_allocated: false,
                job_args: create_table_args(named_table("t1"), false),
                result_ch: Vec::new(),
            },
            JobWrapper {
                job: create_table_job("db2"),
                id_allocated: false,
                job_args: create_table_args(named_table("t2"), false),
                result_ch: Vec::new(),
            },
        ];
        let expected = fingerprints(&job_ws);
        let mut new_ws = merge_create_table_jobs(job_ws).unwrap();
        new_ws.sort_by_key(|w| w.schema_name().to_owned());
        assert_eq!(fingerprints(&new_ws), expected);
    }

    #[test]
    fn test_merge_create_table_jobs_max_batch_size_8() {
        let mut job_ws: Vec<JobWrapper> = Vec::with_capacity(100);
        job_ws.push(JobWrapper::new(
            job_with(|j| {
                j.schema_name = "db0".into();
                j.type_ = ActionType::ACTION_ADD_COLUMN;
            }),
            false,
        ));
        job_ws.push(JobWrapper::new_with_args(
            create_table_job("db1"),
            create_table_args(named_table("t1"), false),
            true,
        ));
        job_ws.push(JobWrapper::new_with_args(
            create_table_job("db2"),
            create_table_args(fk_table(), false),
            false,
        ));
        for (db, cnt) in [("db3", 9), ("db4", 7), ("db5", 22)] {
            for i in 0..cnt {
                let tbl_name = format!("t{i}");
                job_ws.push(JobWrapper::new_with_args(
                    create_table_job(db),
                    create_table_args(named_table(&tbl_name), false),
                    false,
                ));
            }
        }
        let mut new_ws = merge_create_table_jobs(job_ws).unwrap();
        new_ws.sort_by_key(|w| w.schema_name().to_owned());
        // 3 non-mergeable + 2 + 1 + 3
        assert_eq!(new_ws.len(), 9);
        assert_eq!(new_ws[0].action_type(), ActionType::ACTION_ADD_COLUMN);
        assert_eq!(new_ws[1].action_type(), ActionType::ACTION_CREATE_TABLE);
        assert_eq!(new_ws[1].schema_name(), "db1");
        assert_eq!(new_ws[2].action_type(), ActionType::ACTION_CREATE_TABLE);
        assert_eq!(new_ws[2].schema_name(), "db2");

        let mut schema_cnts: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for w in &new_ws[3..9] {
            assert_eq!(w.action_type(), ActionType::ACTION_CREATE_TABLES);
            let JobArgsValue::BatchCreateTable(Some(args)) = &w.job_args else {
                panic!("expect *model.BatchCreateTableArgs");
            };
            let table_count = args.read().tables.get().handles().len();
            schema_cnts
                .entry(w.schema_name().to_owned())
                .or_default()
                .push(table_count);
            assert_eq!(table_count, w.result_ch.len());
        }
        for counts in schema_cnts.values_mut() {
            counts.sort_unstable();
        }
        let expected: BTreeMap<String, Vec<usize>> = [
            ("db3".to_owned(), vec![4, 5]),
            ("db4".to_owned(), vec![7]),
            ("db5".to_owned(), vec![7, 7, 8]),
        ]
        .into_iter()
        .collect();
        assert_eq!(schema_cnts, expected);
    }

    #[test]
    fn test_is_undroppable_table() {
        // `skip_classic` / `skip_next_gen` in Go select the kernel build mode;
        // the Rust port passes the resolved mode instead of skipping.
        let tests: Vec<(&str, &str, &str, i64, bool, bool)> = vec![
            (
                "reserved ID upper bound in next gen",
                "test",
                "test_table",
                tidb_metadef::RESERVED_GLOBAL_ID_UPPER_BOUND,
                true,
                true,
            ),
            (
                "reserved ID lower bound in next gen",
                "test",
                "test_table",
                tidb_metadef::RESERVED_GLOBAL_ID_LOWER_BOUND + 1,
                true,
                true,
            ),
            (
                "non-reserved ID in next gen",
                "test",
                "test_table",
                100,
                true,
                false,
            ),
            (
                "reserved ID in classic",
                "test",
                "test_table",
                tidb_metadef::RESERVED_GLOBAL_ID_UPPER_BOUND,
                false,
                false,
            ),
            (
                "table in workload_schema",
                WORKLOAD_SCHEMA,
                "any_table",
                100,
                false,
                true,
            ),
            (
                "tidb table in mysql schema",
                SYSTEM_DB,
                "tidb",
                100,
                false,
                true,
            ),
            (
                "gc_delete_range table in mysql schema",
                SYSTEM_DB,
                "gc_delete_range",
                100,
                false,
                true,
            ),
            (
                "gc_delete_range_done table in mysql schema",
                SYSTEM_DB,
                "gc_delete_range_done",
                100,
                false,
                true,
            ),
            (
                "non-system table in mysql schema",
                SYSTEM_DB,
                "user",
                100,
                false,
                false,
            ),
            (
                "table in test schema",
                "test",
                "test_table",
                100,
                false,
                false,
            ),
            (
                "table in information_schema",
                "information_schema",
                "tables",
                100,
                false,
                false,
            ),
        ];

        for (name, schema, table, table_id, next_gen, want) in tests {
            let table_info = TableInfo {
                id: table_id,
                name: ci_str(table),
                ..Default::default()
            };
            assert_eq!(
                is_undroppable_table(schema, table, &table_info, next_gen),
                want,
                "{name}: schema={schema}, table={table}, tableID={table_id}"
            );
        }
    }
}
