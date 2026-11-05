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

//! Go `ddl.go`'s per-job context map (`ddlCtx.jobCtx.jobCtxMap`) and
//! `job_worker.go`'s `ReorgContext`: the analyze state a reorganizing job
//! keeps between steps, and the Top SQL attachment its workers label their
//! threads with.

use std::collections::HashMap;
use std::sync::{mpsc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tidb_cpuprofile::labels::LabelSet;
use tidb_model::ActionType;
use tidb_parser::Digest;

/// Go `ReorgContext`.
#[derive(Default)]
pub(crate) struct ReorgContext {
    /// Go `ddlJobCtx`: the labels the job's SQL attached.
    ddl_job_ctx: Option<LabelSet>,
    /// Go `cacheSQL`.
    cache_sql: String,
    /// Go `cacheNormalizedSQL`.
    cache_normalized_sql: String,
    /// Go `cacheDigest`.
    cache_digest: Option<Digest>,
    /// Go `tp`: the job's request source (`setDDLLabelForDiagnosis`).
    tp: String,
    /// Go `resourceGroupName`.
    resource_group_name: String,
    /// Go `analyzeDone`: the running ANALYZE's report, an error or none.
    pub(crate) analyze_done: Option<mpsc::Receiver<Option<String>>>,
    /// Go `analyzeStartTime`.
    pub(crate) analyze_start_time: Option<Instant>,
    /// Go `analyzeCumulativeTimeout`.
    pub(crate) analyze_cumulative_timeout: Option<Duration>,
}

impl ReorgContext {
    /// Go `ReorgContext.attachTopProfilingInfo`: labels the calling thread
    /// with the job's SQL digest and registers its normalized SQL, keeping
    /// both for the job's next attach.
    fn attach_top_profiling_info(&mut self, job_query: &str) {
        if !tidb_topsql_state::top_profiling_enabled() || job_query.is_empty() {
            return;
        }
        if job_query != self.cache_sql || self.cache_digest.is_none() {
            let (normalized_sql, digest) = tidb_parser::normalize_digest(job_query);
            self.cache_normalized_sql = normalized_sql;
            self.cache_digest = Some(digest);
            self.cache_sql = job_query.to_owned();
            self.ddl_job_ctx = tidb_topsql::attach_and_register_sql_info(
                None,
                &self.cache_normalized_sql,
                self.cache_digest.as_ref(),
                false,
            );
        } else {
            tidb_topsql::attach_and_register_sql_info(
                self.ddl_job_ctx.clone(),
                &self.cache_normalized_sql,
                self.cache_digest.as_ref(),
                false,
            );
        }
    }
}

/// Go `DDLBackfillers`: the jobs whose requests carry a backfill source.
fn ddl_backfiller(job_type: ActionType) -> Option<&'static str> {
    match job_type {
        ActionType::ACTION_ADD_INDEX => Some("add_index"),
        ActionType::ACTION_MODIFY_COLUMN => Some("modify_column"),
        ActionType::ACTION_DROP_INDEX => Some("drop_index"),
        ActionType::ACTION_REORGANIZE_PARTITION => Some("reorganize_partition"),
        _ => None,
    }
}

/// Go `getDDLRequestSource`.
fn get_ddl_request_source(job_type: ActionType) -> String {
    match ddl_backfiller(job_type) {
        Some(tp) => format!("{}{tp}", tidb_txnkv::INTERNAL_TXN_BACKFILL_DDL_PREFIX),
        None => tidb_txnkv::INTERNAL_TXN_DDL.to_owned(),
    }
}

impl ReorgContext {
    /// Go `ReorgContext.setDDLLabelForDiagnosis`: the job's request source,
    /// fixed by its first step.
    fn set_ddl_label_for_diagnosis(&mut self, job_type: ActionType) {
        if self.tp.is_empty() {
            self.tp = get_ddl_request_source(job_type);
        }
    }

    /// Go `ReorgContext.getResourceGroupTaggerForTopSQL`: the tag the job's
    /// requests carry, its SQL digest, while Top SQL is on.
    fn get_resource_group_tagger_for_top_sql(&self) -> Option<tidb_txnkv::StatementResourceGroupTagger> {
        if !tidb_topsql_state::top_sql_enabled() {
            return None;
        }
        let digest = self.cache_digest.as_ref()?.as_bytes().to_vec();
        Some(std::sync::Arc::new(move || {
            let mut tagger = tidb_txnkv::ResourceGroupTagBuilder::new(
                tidb_keyspace::get_keyspace_name_bytes_by_settings(),
            );
            tagger.set_sql_digest(&digest);
            tagger
        }))
    }
}

/// Go `ddlCtx.jobCtx.jobCtxMap`.
static JOB_CTX_MAP: LazyLock<Mutex<HashMap<i64, ReorgContext>>> = LazyLock::new(Mutex::default);

/// The job's context, created on first use as Go's setters do
/// (`NewReorgContext`).
pub(crate) fn with_job_ctx<T>(job_id: i64, f: impl FnOnce(&mut ReorgContext) -> T) -> T {
    let mut map = JOB_CTX_MAP.lock().unwrap_or_else(PoisonError::into_inner);
    f(map.entry(job_id).or_default())
}

/// Go `ddlCtx.attachTopProfilingInfo`.
pub(crate) fn attach_top_profiling_info(job_id: i64, job_query: &str) {
    with_job_ctx(job_id, |ctx| ctx.attach_top_profiling_info(job_query));
}

/// Go `ddlCtx.setDDLSourceForDiagnosis`, returning Go `ddlJobSourceType`:
/// the request source the job's step transaction carries.
pub(crate) fn set_ddl_source_for_diagnosis(job_id: i64, job_type: ActionType) -> String {
    with_job_ctx(job_id, |ctx| {
        ctx.set_ddl_label_for_diagnosis(job_type);
        ctx.tp.clone()
    })
}

/// Go's DDL scheduler context, `kv.WithInternalSourceType(ctx,
/// kv.InternalTxnDDL)`: the source of a DDL transaction that reads or
/// writes the job's metadata.
pub(crate) fn internal_ddl_source() -> tidb_txnkv::RequestSource {
    tidb_txnkv::RequestSource {
        internal: true,
        source_type: tidb_txnkv::INTERNAL_TXN_DDL.to_owned(),
        explicit_source_type: String::new(),
    }
}

/// Go `kv.WithInternalSourceAndTaskType(ctx, jobContext.ddlJobSourceType(),
/// kvutil.ExplicitTypeDDL)`: the source of the job's backfill reads and
/// writes (`BackfillData`, `GetRangeEndKey`, `getTableRange`).
pub(crate) fn backfill_request_source(job_id: i64) -> tidb_txnkv::RequestSource {
    tidb_txnkv::RequestSource {
        internal: true,
        // Go `ddlCtx.jobContext(jobID).ddlJobSourceType()`.
        source_type: JOB_CTX_MAP
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&job_id)
            .map(|ctx| ctx.tp.clone())
            .unwrap_or_default(),
        explicit_source_type: tikv_client::util::EXPLICIT_TYPE_DDL.to_owned(),
    }
}

/// Go `ddlCtx.jobContext(jobID, reorgMeta).resourceGroupName`: the job's
/// context's group, which a context without one takes from the job's reorg
/// meta.
pub(crate) fn job_resource_group_name(job_id: i64, reorg_meta_group: Option<&str>) -> String {
    let mut map = JOB_CTX_MAP.lock().unwrap_or_else(PoisonError::into_inner);
    match map.get_mut(&job_id) {
        Some(ctx) => {
            if let (Some(group), true) = (reorg_meta_group, ctx.resource_group_name.is_empty()) {
                group.clone_into(&mut ctx.resource_group_name);
            }
            ctx.resource_group_name.clone()
        }
        None => reorg_meta_group.unwrap_or_default().to_owned(),
    }
}

/// Go `ddlCtx.getResourceGroupTaggerForTopSQL`: none for a job with no
/// context.
pub(crate) fn get_resource_group_tagger_for_top_sql(
    job_id: i64,
) -> Option<tidb_txnkv::StatementResourceGroupTagger> {
    JOB_CTX_MAP
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&job_id)
        .and_then(ReorgContext::get_resource_group_tagger_for_top_sql)
}

/// Go `removeJobCtx`: the job is done with its context.
pub(crate) fn remove_job_ctx(job_id: i64) {
    JOB_CTX_MAP
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&job_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `getDDLRequestSource` / `setDDLLabelForDiagnosis`: a backfilling
    /// job's requests carry `ddl_<backfiller>`, any other `ddl`, and the
    /// first step fixes it.
    #[test]
    fn a_jobs_request_source_names_its_backfill() {
        assert_eq!(set_ddl_source_for_diagnosis(9_100, ActionType::ACTION_ADD_INDEX), "ddl_add_index");
        assert_eq!(set_ddl_source_for_diagnosis(9_100, ActionType::ACTION_CREATE_TABLE), "ddl_add_index");
        assert_eq!(
            set_ddl_source_for_diagnosis(9_101, ActionType::ACTION_REORGANIZE_PARTITION),
            "ddl_reorganize_partition"
        );
        assert_eq!(set_ddl_source_for_diagnosis(9_102, ActionType::ACTION_CREATE_TABLE), "ddl");
        // Go `WithInternalSourceAndTaskType(ctx, ddlJobSourceType(),
        // ExplicitTypeDDL)` for the job's backfill.
        assert_eq!(
            backfill_request_source(9_100),
            tidb_txnkv::RequestSource {
                internal: true,
                source_type: "ddl_add_index".to_owned(),
                explicit_source_type: "ddl".to_owned(),
            }
        );
        // Go `jobContext(jobID, reorgMeta)`: a context without a group takes
        // the reorg meta's, and keeps it.
        assert_eq!(job_resource_group_name(9_100, Some("rg1")), "rg1");
        assert_eq!(job_resource_group_name(9_100, Some("rg2")), "rg1");
        assert_eq!(job_resource_group_name(9_103, Some("rg3")), "rg3");
        assert_eq!(job_resource_group_name(9_103, None), "");
        for job_id in [9_100, 9_101, 9_102] {
            remove_job_ctx(job_id);
        }
    }

    fn labelled_digest() -> Option<String> {
        tidb_cpuprofile::labels::goroutine_labels()
            .and_then(|labels| labels.values().find(|value| value.len() == 64).cloned())
    }

    /// Go `attachTopProfilingInfo` labels the calling thread with the job's
    /// SQL digest only while Top SQL profiles, recomputing it when the job's
    /// query changes and reusing the cached one otherwise.
    #[test]
    fn attach_labels_the_thread_with_the_jobs_sql_digest() {
        let job_id = 9_001;
        std::thread::spawn(move || {
            tidb_topsql_state::disable_top_sql();
            attach_top_profiling_info(job_id, "ALTER TABLE t ADD INDEX i (a)");
            assert_eq!(labelled_digest(), None);

            tidb_topsql_state::enable_top_sql();
            assert!(get_resource_group_tagger_for_top_sql(job_id).is_none(), "no digest yet");
            attach_top_profiling_info(job_id, "ALTER TABLE t ADD INDEX i (a)");
            let (_, add_index) = tidb_parser::normalize_digest("ALTER TABLE t ADD INDEX i (a)");
            assert_eq!(labelled_digest().as_deref(), Some(add_index.as_str()));
            assert!(get_resource_group_tagger_for_top_sql(job_id).is_some());
            // Go `getResourceGroupTaggerForTopSQL`: no tagger while Top SQL
            // is off, though the digest is cached.
            tidb_topsql_state::disable_top_sql();
            assert!(get_resource_group_tagger_for_top_sql(job_id).is_none());
            tidb_topsql_state::enable_top_sql();

            // Another thread of the same job reuses the cached digest.
            std::thread::spawn(move || {
                attach_top_profiling_info(job_id, "ALTER TABLE t ADD INDEX i (a)");
                assert_eq!(labelled_digest().as_deref(), Some(add_index.as_str()));
            })
            .join()
            .unwrap();

            attach_top_profiling_info(job_id, "ALTER TABLE t ADD INDEX j (b)");
            let (_, other) = tidb_parser::normalize_digest("ALTER TABLE t ADD INDEX j (b)");
            assert_eq!(labelled_digest().as_deref(), Some(other.as_str()));
            tidb_topsql_state::disable_top_sql();
            remove_job_ctx(job_id);
        })
        .join()
        .unwrap();
    }
}
