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

//! Go `Domain.StartTTLJobManager`, `Domain.TTLJobManager` and the TTL half
//! of `Domain.Close`.
//!
//! Go's job manager holds a pooled session for its loop's whole life, and a
//! pooled session holds its domain, so the domain's `Drop` cannot be what
//! stops it: as Go's `Domain.Close` stops it explicitly, the
//! [`TtlJobManagerGuard`] the caller keeps stops it when dropped.

use std::sync::Arc;
use std::time::Duration;

use tidb_ttl_ttlworker::JobManager;

use super::Domain;

/// The TTL half of Go's `Domain.Close`: stops the job manager and waits for
/// it (10s in test, 30s otherwise) when dropped.
pub struct TtlJobManagerGuard {
    manager: Arc<JobManager>,
}

impl Drop for TtlJobManagerGuard {
    fn drop(&mut self) {
        tidb_logutil::bg_logger().info("stopping ttlJobManager", &[]);
        self.manager.stop();
        let timeout = if tidb_intest::IN_TEST {
            Duration::from_secs(10)
        } else {
            Duration::from_secs(30)
        };
        match self
            .manager
            .wait_stopped(&tidb_ttl_client::Context::background(), timeout)
        {
            Ok(()) => tidb_logutil::bg_logger().info("ttlJobManager exited.", &[]),
            Err(error) => tidb_logutil::bg_logger().warn(
                "fail to wait until the ttl job manager stop",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
            ),
        }
    }
}

impl Domain {
    /// Go `StartTTLJobManager`: `ttlworker.NewJobManager(do.ddl.GetID(),
    /// do.advancedSysSessionPool, do.store, do.etcdClient,
    /// do.ddl.OwnerManager().IsOwner)`, started. A second call keeps the
    /// first manager and returns no guard.
    pub fn start_ttl_job_manager(self: &Arc<Self>) -> Option<TtlJobManagerGuard> {
        if self.ttl_job_manager.get().is_some() {
            return None;
        }
        // Go `ddl.GetID()`: the DDL uuid the server info publishes.
        let id = self
            .server_info()
            .map(|syncer| syncer.get_local_server_info().static_info.id.clone())
            .or_else(|| self.ddl.owner_manager().map(|owner| owner.id().to_owned()))
            .unwrap_or_default();
        let ddl = Arc::clone(&self.ddl);
        let pool: Arc<dyn tidb_syssession::Pool<dyn crate::sys_session::DomainSysSession>> =
            self.advanced_sys_session_pool();
        let manager = JobManager::new(
            &id,
            pool,
            self.region_store.clone(),
            self.etcd_client(),
            Box::new(move || ddl.is_owner()),
        );
        if self.ttl_job_manager.set(Arc::clone(&manager)).is_err() {
            return None;
        }
        manager.start();
        Some(TtlJobManagerGuard { manager })
    }

    /// Go `TTLJobManager()`, absent until started.
    pub fn ttl_job_manager(&self) -> Option<Arc<JobManager>> {
        self.ttl_job_manager.get().cloned()
    }
}
