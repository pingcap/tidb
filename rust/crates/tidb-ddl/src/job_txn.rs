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

//! The DDL job step's transaction, as Go's `runOneJobStep` writes through
//! `meta.NewMutator(txn)`: reads see the step's own writes over the job's
//! snapshot, and the writes are what the worker commits.

use std::collections::BTreeMap;

use tidb_infoschema::cluster_catalog::{ClusterCatalogError, MetaPairs, MetaSnapshot};
use tidb_structure::transaction::{RawRangeVisitor, RawTransaction};
use tidb_txnkv::transaction::{BufferMutation, BufferMutationOp};

/// Go's job transaction: the snapshot's keys with this step's writes over
/// them (Go's `MemBuffer`). It is the snapshot every handler reads (a
/// multi-schema change's sub-jobs read the writes of the ones before them)
/// and the transaction `meta()` writes through.
pub struct JobTxn<'a> {
    inner: &'a mut dyn MetaSnapshot,
    start_ts: u64,
    /// Go `w.sess`: the session whose transaction the step is, through which
    /// the step's system-table statements run. `None` for a run without a
    /// session pool (bootstrap, planner tests).
    session: Option<StepSession>,
    /// Each written key's value; `None` deletes it.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// The snapshot's own error for a read the meta reader made that failed,
    /// so a storage error keeps its SQL identity.
    failed: Option<ClusterCatalogError>,
}

/// Go `meta.Mutator` over a job step's transaction (`jobCtx.metaMut`).
pub type JobMeta<'t, 'a> = tidb_meta::transaction::Mutator<&'t mut JobTxn<'a>>;

impl<'a> JobTxn<'a> {
    /// The transaction of one step over `inner` (the job's snapshot, or a
    /// multi-schema change's transaction for one of its sub-jobs).
    pub fn new(inner: &'a mut dyn MetaSnapshot, start_ts: u64) -> Self {
        Self {
            inner,
            start_ts,
            session: None,
            writes: BTreeMap::new(),
            failed: None,
        }
    }

    /// The step's session (see [`StepSession`]).
    #[must_use]
    pub fn with_session(mut self, session: Option<StepSession>) -> Self {
        self.session = session;
        self
    }

    /// The step's session, for a transaction nested in this one.
    #[must_use]
    pub fn session(&self) -> Option<StepSession> {
        self.session.clone()
    }

    /// Go `w.sess.Execute(ctx, sql, label)` in the step's transaction;
    /// `None` when the step runs without a session.
    ///
    /// # Errors
    ///
    /// The statement's own error.
    pub fn execute(
        &self,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Option<Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError>> {
        let session = self.session.as_ref()?;
        Some(session.execute(self.start_ts, sql, args))
    }

    /// Go `meta.NewMutator(txn)`.
    pub fn meta(&mut self) -> JobMeta<'_, 'a> {
        tidb_meta::transaction::Mutator::new(self)
    }

    /// Writes a handler planned as raw mutations, so the reads after them see
    /// them.
    pub fn stage(&mut self, mutations: &[BufferMutation]) {
        for mutation in mutations {
            match mutation.kind() {
                BufferMutationOp::Set => {
                    self.writes
                        .insert(mutation.key().to_vec(), Some(mutation.value().to_vec()));
                }
                BufferMutationOp::Delete => {
                    self.writes.insert(mutation.key().to_vec(), None);
                }
                BufferMutationOp::Lock => {}
            }
        }
    }

    /// This step's writes in key order, as the worker commits them.
    pub fn into_mutations(self) -> Result<Vec<BufferMutation>, ClusterCatalogError> {
        self.writes
            .into_iter()
            .map(|(key, value)| match value {
                Some(value) => BufferMutation::set(key, value),
                None => BufferMutation::delete(key),
            })
            .collect::<Result<_, _>>()
            .map_err(|error| ClusterCatalogError::Decode(error.to_string()))
    }

    fn read<R>(&mut self, result: Result<R, ClusterCatalogError>) -> tidb_meta::Result<R> {
        result.map_err(|error| {
            let message = error.to_string();
            self.failed.get_or_insert(error);
            tidb_meta::MetaError::Storage(message)
        })
    }

    /// The pairs `inner` returned for `[start, end)` with this step's writes
    /// over them.
    fn overlay(&self, stored: MetaPairs, start: &[u8], end: Option<&[u8]>) -> MetaPairs {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = stored.into_iter().collect();
        let written = self
            .writes
            .range(start.to_vec()..)
            .take_while(|(key, _)| end.is_none_or(|end| key.as_slice() < end));
        for (key, value) in written {
            match value {
                Some(value) => merged.insert(key.clone(), value.clone()),
                None => merged.remove(key),
            };
        }
        merged.into_iter().collect()
    }
}

/// Go `w.sess` for one step: the worker's session pool and the step's
/// staged writes, which every statement of the step stages into and the
/// step commits with its meta writes.
#[derive(Clone)]
pub struct StepSession {
    /// Go `w.sessPool`.
    pub pool: std::sync::Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    /// Go `w.sess`'s membuffer.
    pub buffer: tidb_store_driver_txn::cluster_storage::MutationBuffer,
    /// Go `w.sess` itself: the step's one session, opened at its first
    /// statement and shared by every statement of the step.
    pub sql: StepSqlSlot,
}

/// The slot of a step's one session (see [`StepSession::sql`]).
pub type StepSqlSlot = std::sync::Arc<
    std::sync::Mutex<Option<Box<dyn tidb_ddl_build::session_pool::StepSqlSession>>>,
>;

impl StepSession {
    /// Go `w.sess.Execute(ctx, sql, label)` in the step's transaction.
    ///
    /// # Errors
    ///
    /// The session's failure to open, or the statement's error.
    pub fn execute(
        &self,
        start_ts: u64,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> {
        let mut slot = self
            .sql
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(self.pool.open_step_session(start_ts, &self.buffer)?);
        }
        slot.as_mut().expect("just opened").execute(sql, args)
    }
}

/// A meta error of a step: the snapshot's own when a read failed.
pub fn job_meta_error(meta: &JobMeta<'_, '_>, error: tidb_meta::MetaError) -> ClusterCatalogError {
    meta.inspect(|txn| txn.failed.clone())
        .ok()
        .flatten()
        .unwrap_or_else(|| ClusterCatalogError::Decode(error.to_string()))
}

impl MetaSnapshot for JobTxn<'_> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        match self.writes.get(key) {
            Some(written) => Ok(written.clone()),
            None => self.inner.get(key),
        }
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let stored = self.inner.scan_prefix(prefix)?;
        let end = tidb_kv::key::prefix_scan_end(prefix);
        Ok(self.overlay(stored, prefix, end.as_deref()))
    }

    fn scan_range(&mut self, start: &[u8], end: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let stored = self.inner.scan_range(start, end)?;
        Ok(self.overlay(stored, start, Some(end)))
    }

    fn auto_id_client(&self) -> Option<&std::sync::Arc<dyn tidb_autoid::service_rpc::AutoIdServiceRpc>> {
        self.inner.auto_id_client()
    }
}

impl RawTransaction for JobTxn<'_> {
    type Error = tidb_meta::MetaError;

    fn get(&mut self, key: &[u8]) -> tidb_meta::Result<Option<Vec<u8>>> {
        let result = MetaSnapshot::get(self, key);
        self.read(result)
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> tidb_meta::Result<()> {
        self.writes.insert(key, Some(value));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> tidb_meta::Result<()> {
        self.writes.insert(key.to_vec(), None);
        Ok(())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> tidb_meta::Result<MetaPairs> {
        let result = MetaSnapshot::scan_prefix(self, prefix);
        self.read(result)
    }

    fn iterate_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut RawRangeVisitor<'_, tidb_meta::MetaError>,
    ) -> tidb_meta::Result<()> {
        let result = MetaSnapshot::scan_range(self, start, end);
        for (key, value) in self.read(result)? {
            visit(&key, &value)?;
        }
        Ok(())
    }
}

impl tidb_meta::transaction::MetaTransaction for JobTxn<'_> {
    fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// Go `NewMutator`'s priority and disk-full options are the worker
    /// transaction's own here.
    fn configure_meta_mutator(&mut self) {}
}
