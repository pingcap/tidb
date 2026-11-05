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
            attach_top_profiling_info(job_id, "ALTER TABLE t ADD INDEX i (a)");
            let (_, add_index) = tidb_parser::normalize_digest("ALTER TABLE t ADD INDEX i (a)");
            assert_eq!(labelled_digest().as_deref(), Some(add_index.as_str()));

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
