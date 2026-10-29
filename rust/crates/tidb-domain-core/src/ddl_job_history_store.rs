// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! The cluster's DDL jobs for `ADMIN SHOW DDL JOBS` and
//! `information_schema.DDL_JOBS`: Go's `DDLJobRetriever.initial` reads the
//! running jobs from `mysql.tidb_ddl_job` (`GetAllDDLJobs`) and the history
//! from the meta `DDLJobHistory` hash (`GetLastHistoryDDLJobsIterator`), in
//! a new transaction of the system session.

use std::sync::Arc;
use std::time::Duration;

use tidb_store_driver_txn::meta_txn::MetaTxnStorage;
use tidb_meta::transaction::Mutator;
use tidb_model::Job;
use tidb_txnkv::transaction::{RealOptimisticTransactionOpener, StoreClient};
use tidb_txnkv::{NewTxnStorage, NewTxnTransaction};

/// Go `ddl.batchNumHistoryJobs`, the history read's page size.
const BATCH_NUM_HISTORY_JOBS: i32 = 128;

pub struct ClusterDdlJobHistoryStore<S: StoreClient> {
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
}

impl<S: StoreClient> ClusterDdlJobHistoryStore<S> {
    pub fn new(opener: Arc<RealOptimisticTransactionOpener<S>>, timeout: Duration) -> Self {
        Self { opener, timeout }
    }
}

impl<S> tidb_ddl::ddl_history::DdlJobHistoryStore for ClusterDdlJobHistoryStore<S>
where
    S: StoreClient,
{
    fn all_ddl_jobs(&self) -> Result<(Vec<Job>, Vec<Job>), String> {
        // Go `GetAllDDLJobs`: `select job_meta from mysql.tidb_ddl_job ...
        // order by job_id`.
        let running = tidb_ddl::real_tikv_ddl::load_active_persisted_ddl_jobs(
            Arc::clone(&self.opener),
            self.timeout,
            0,
        )
        .map_err(|error| error.to_string())?;
        let mut storage = MetaTxnStorage::new(&self.opener, self.timeout);
        let mut transaction = storage.begin().map_err(|error| error.to_string())?;
        let history = (|| {
            let mut iterator = Mutator::new(&mut transaction).last_history_ddl_jobs::<Job>()?;
            let mut history = Vec::new();
            loop {
                let mut page = Vec::new();
                iterator.get_last_jobs(BATCH_NUM_HISTORY_JOBS, &mut page)?;
                let last_page = page.len() < BATCH_NUM_HISTORY_JOBS as usize;
                history.extend(page);
                if last_page {
                    return Ok(history);
                }
            }
        })()
        .map_err(|error: tidb_meta::MetaError| error.to_string());
        let _ = NewTxnTransaction::rollback(&mut transaction);
        Ok((running, history?))
    }
}
