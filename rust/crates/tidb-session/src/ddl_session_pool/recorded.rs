//! Go `testkit.CreateMockStore` / `CreateSession`: SQL sessions over recorded
//! catalog fixtures. The fixture supplies KV bytes, never SQL result rows.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tidb_ddl::cluster_ddl::{DdlJobSchemaState, DdlStepWorker, DdlWrite};
use tidb_ddl_build::plan_error::DdlPlanError;
use tidb_ddl_build::session_pool::DdlSessionPool;
use tidb_infoschema::cluster_catalog::{MetaPairs, MetaSnapshot};
use tidb_store_driver_txn::cluster_storage::MutationBuffer;
use tidb_txnkv::transaction::BufferMutation;

/// Go `testkit.CreateMockStore`: a recorded planner fixture's store and real
/// DDL sessions. The caller still controls when to apply the returned writes.
pub struct RecordedDdlSession {
    opener: tidb_ddl::embedded_store::InProcessOpener,
    pool: Arc<super::NodeDdlSessionPool>,
    before: BTreeMap<Vec<u8>, Vec<u8>>,
    buffer: MutationBuffer,
}

fn error(error: impl std::fmt::Display) -> DdlPlanError {
    DdlPlanError::Encode(error.to_string())
}

fn pairs(snapshot: &mut impl MetaSnapshot) -> Result<MetaPairs, DdlPlanError> {
    let mut pairs = snapshot.scan_prefix(b"m")?;
    pairs.extend(snapshot.scan_prefix(b"t")?);
    Ok(pairs)
}

impl RecordedDdlSession {
    /// Go `testkit.CreateMockStore` / `CreateSession`: import the fixture
    /// before delivery, then bind the ordinary SQL pool to that store.
    pub fn new(
        snapshot: &mut impl MetaSnapshot,
        row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
    ) -> Result<Self, DdlPlanError> {
        let before: BTreeMap<_, _> = pairs(snapshot)?.into_iter().collect();
        let stack = tidb_ddl::embedded_store::in_process_stack(
            &tidb_pd_client::ClusterSecurity::plaintext(),
        )
        .map_err(error)?;
        // Recorded timestamps may predate today's TSO. Seed a committed
        // version before every fixture step instead of moving its timestamps.
        if !before.is_empty() {
            let mut handler = stack.handler.lock().unwrap();
            handler
                .store
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: before
                        .iter()
                        .map(|(key, value)| tidb_proto::KvrpcMutation {
                            op: tidb_proto::KvrpcOp::Put as i32,
                            key: key.clone(),
                            value: value.clone(),
                            ..Default::default()
                        })
                        .collect(),
                    primary_lock: before.keys().next().unwrap().clone(),
                    start_version: 1,
                    ..Default::default()
                })
                .map_err(error)?;
            handler
                .store
                .commit(&before.keys().cloned().collect::<Vec<_>>(), 1, 2)
                .map_err(error)?;
        }
        let pool = super::NodeDdlSessionPool::for_store(
            // Go `testkit.CreateMockStore`: SetSchemaLease(500 * time.Millisecond).
            stack.opener.clone(),
            Duration::from_secs(20),
            row_ids,
            true,
            Duration::from_millis(500),
        )
        .map_err(error)?;
        Ok(Self {
            opener: stack.opener,
            pool,
            before,
            buffer: MutationBuffer::new(),
        })
    }

    /// Go `systable.Manager.GetJobByID`: deliver outside the step transaction.
    pub fn schema_state(
        &self,
        job_id: i64,
        mdl_enabled: bool,
    ) -> Result<DdlJobSchemaState, DdlPlanError> {
        use tidb_ddl_systable::Manager;
        let manager =
            tidb_ddl_systable::SystemTableManager::new(self.pool.sess_pool().expect("bound pool"));
        let job = manager.get_job_by_id(job_id).map_err(error)?;
        Ok(DdlJobSchemaState::new(
            mdl_enabled,
            job,
            DdlStepWorker {
                sess_pool: self.pool.clone(),
                step_buffer: self.buffer.clone(),
                step_sql: Default::default(),
                store: None,
                region_store: None,
                start_mode: tidb_ddl::StartMode::Normal,
            },
        ))
    }

    /// Go `insertDDLJobs2Table`: use the submitting session even when
    /// the caller records the transaction instead of committing it.
    pub fn insert_jobs(
        &self,
        start_ts: u64,
        specs: &mut [tidb_ddl_jobsubmit::JobSpec],
    ) -> Result<(), DdlPlanError> {
        let mut session = self
            .pool
            .open_step_session(start_ts, &self.buffer)
            .map_err(DdlPlanError::from_mysql_error)?;
        tidb_ddl_jobsubmit::insert_ddl_jobs_2_table(
            &mut |sql| session.execute(sql, &[]).map(drop),
            specs,
        )
        .map_err(error)
    }

    /// Go `worker.transitOneJobStep`: collect the step session's SQL writes
    /// and separately committed GC records alongside the handler's meta writes.
    pub fn append_writes(&self, write: &mut DdlWrite) -> Result<(), DdlPlanError> {
        self.append_mutations(&mut write.mutations)
    }

    /// Go `sess.Commit`: collect the SQL transaction's writes for replay.
    pub fn append_mutations(
        &self,
        mutations: &mut Vec<BufferMutation>,
    ) -> Result<(), DdlPlanError> {
        use tidb_domain_core::cluster_transactions::{
            ClusterTransactions, RealClusterTransactions,
        };
        use tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot;
        let transactions =
            RealClusterTransactions::new(self.opener.clone(), Duration::from_secs(20));
        let at = transactions.fresh_timestamp().map_err(error)?;
        let snapshot = transactions.open_snapshot_at(at, "").map_err(error)?;
        let after: BTreeMap<_, _> = pairs(&mut SnapshotMetaSnapshot::new(snapshot))?
            .into_iter()
            .collect();
        for (key, value) in &after {
            if self.before.get(key) != Some(value) {
                mutations.push(BufferMutation::set(key.clone(), value.clone())?);
            }
        }
        for key in self.before.keys().filter(|key| !after.contains_key(*key)) {
            mutations.push(BufferMutation::delete(key.clone())?);
        }
        for (key, value) in self.buffer.snapshot() {
            mutations.push(match value {
                Some(value) => BufferMutation::set(key.as_bytes().to_vec(), value)?,
                None => BufferMutation::delete(key.as_bytes().to_vec())?,
            });
        }
        Ok(())
    }
}
