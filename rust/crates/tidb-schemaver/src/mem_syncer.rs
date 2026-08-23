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

//! Go `pkg/ddl/schemaver/mem_syncer.go`: [`MemSyncer`], the in-memory syncer
//! used where there is exactly ONE TiDB instance (uni-store), which is
//! mainly for test -- ported whole, both MDL paths included.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::mdl_enabled;

use crate::{Context, DoneCh, GlobalVerRx, SharedRecv, Syncer, WatchEvent};

/// Go `checkVersionsInterval`.
const CHECK_VERSIONS_INTERVAL: Duration = Duration::from_millis(2);

/// Go `MemSyncer`.
pub struct MemSyncer {
    self_schema_version: AtomicI64,
    mdl_schema_versions: Mutex<HashMap<i64, i64>>,
    global_ver: Mutex<Option<(std::sync::mpsc::Sender<WatchEvent>, GlobalVerRx)>>,
    mock_session: Mutex<Option<(std::sync::mpsc::Sender<()>, DoneCh)>>,
}

impl Default for MemSyncer {
    fn default() -> Self {
        Self::new()
    }
}

impl MemSyncer {
    /// Go `NewMemSyncer`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            self_schema_version: AtomicI64::new(0),
            mdl_schema_versions: Mutex::new(HashMap::new()),
            global_ver: Mutex::new(None),
            mock_session: Mutex::new(None),
        }
    }

    /// Go `CloseSession`: closes the mock session channel so [`Self::done`]
    /// fires. Exported upstream FOR TESTING.
    pub fn close_session(&self) {
        let mut slot = self
            .mock_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Dropping the sender closes every receiver, as `close(ch)` would.
        drop(slot.take());
    }
}

fn fresh_channel<T>() -> (std::sync::mpsc::Sender<T>, SharedRecv<T>) {
    let (sender, receiver) = std::sync::mpsc::channel::<T>();
    (sender, SharedRecv::new(receiver))
}

impl Syncer for MemSyncer {
    /// Go `Init`.
    fn init(&self, _ctx: &Context) -> Result<(), String> {
        *self
            .mdl_schema_versions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = HashMap::new();
        *self
            .global_ver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(fresh_channel::<WatchEvent>());
        *self
            .mock_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(fresh_channel::<()>());
        Ok(())
    }

    /// Go `UpdateSelfVersion`.
    fn update_self_version(&self, _ctx: &Context, job_id: i64, version: i64) -> Result<(), String> {
        if mdl_enabled() {
            self.mdl_schema_versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(job_id, version);
        } else {
            self.self_schema_version.store(version, Ordering::Release);
        }
        Ok(())
    }

    /// Go `OwnerUpdateGlobalVersion`: non-blocking send onto a one-slot
    /// channel; a pending event already there is left untouched.
    fn owner_update_global_version(&self, _ctx: &Context, _version: i64) -> Result<(), String> {
        let slot = self
            .global_ver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((sender, _)) = slot.as_ref() {
            let _ = sender.send(WatchEvent::default());
        }
        Ok(())
    }

    /// Go `GlobalVersionCh`.
    fn global_version_ch(&self) -> GlobalVerRx {
        let slot = self
            .global_ver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match slot.as_ref() {
            Some((_, receiver)) => receiver.clone(),
            None => {
                let (_, receiver) = fresh_channel::<WatchEvent>();
                receiver
            }
        }
    }

    /// Go `WatchGlobalSchemaVer`.
    fn watch_global_schema_ver(&self) {}

    /// Go `Done`.
    fn done(&self) -> DoneCh {
        self.mock_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|(_, receiver)| receiver.clone())
            .unwrap_or_else(|| fresh_channel::<()>().1)
    }

    /// Go `Restart`.
    fn restart(&self, _ctx: &Context) -> Result<(), String> {
        *self
            .mock_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(fresh_channel::<()>());
        Ok(())
    }

    /// Go `WaitVersionSynced`.
    fn wait_version_synced(
        &self,
        ctx: &Context,
        job_id: i64,
        latest_ver: i64,
    ) -> Result<(), String> {
        loop {
            if let Err(error) = ctx.sleep(CHECK_VERSIONS_INTERVAL) {
                return Err(error.to_string());
            }
            if mdl_enabled() {
                let versions = self
                    .mdl_schema_versions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if versions.get(&job_id).is_some_and(|ver| *ver >= latest_ver) {
                    return Ok(());
                }
            } else if self.self_schema_version.load(Ordering::Acquire) >= latest_ver {
                return Ok(());
            }
        }
    }

    /// Go `SyncJobSchemaVerLoop`.
    fn sync_job_schema_ver_loop(&self, _ctx: &Context) {}

    /// Go `Close`.
    fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::{globals_test_lock, Context};

    #[test]
    fn test_mem_wait_version_synced_both_paths() {
        let guard = globals_test_lock();
        let syncer = MemSyncer::new();
        syncer.init(&Context::background()).unwrap();

        // MDL ON: the per-job map is consulted.
        tidb_vardef::set_enable_mdl(true);
        let done = Context::with_timeout(&Context::background(), Duration::from_millis(30));
        assert!(syncer.wait_version_synced(&done, 7, 100).is_err());
        syncer
            .update_self_version(&Context::background(), 7, 99)
            .unwrap();
        assert!(
            syncer.wait_version_synced(&done, 7, 100).is_err(),
            "99 < 100"
        );
        syncer
            .update_self_version(&Context::background(), 7, 100)
            .unwrap();
        syncer
            .wait_version_synced(&Context::background(), 7, 100)
            .unwrap();

        // MDL OFF: the single self version is consulted.
        tidb_vardef::set_enable_mdl(false);
        let done = Context::with_timeout(&Context::background(), Duration::from_millis(30));
        assert!(syncer.wait_version_synced(&done, 7, 5).is_err());
        syncer
            .update_self_version(&Context::background(), 0, 5)
            .unwrap();
        syncer
            .wait_version_synced(&Context::background(), 0, 5)
            .unwrap();

        tidb_vardef::set_enable_mdl(false);
        drop(guard);
    }

    #[test]
    fn test_mem_owner_update_global_version_notifies_once() {
        let syncer = MemSyncer::new();
        syncer.init(&Context::background()).unwrap();
        let rx = syncer.global_version_ch();
        syncer
            .owner_update_global_version(&Context::background(), 42)
            .unwrap();
        assert!(
            matches!(
                rx.recv_timeout(Duration::from_secs(1)),
                crate::Recv::Item(_)
            ),
            "the first publish must notify"
        );
        // A second publish while nothing drains must NOT block (Go's
        // non-blocking send onto the buffered channel), and the pending slot
        // stays occupied.
        syncer
            .owner_update_global_version(&Context::background(), 43)
            .unwrap();
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            crate::Recv::Item(_)
        ));
    }

    #[test]
    fn test_mem_done_and_restart() {
        let syncer = MemSyncer::new();
        syncer.init(&Context::background()).unwrap();
        let done = syncer.done();
        syncer.close_session();
        assert!(matches!(
            done.recv_timeout(Duration::from_secs(1)),
            crate::Recv::Closed
        ));
        // Restart re-arms the session.
        syncer.restart(&Context::background()).unwrap();
        let done = syncer.done();
        assert!(matches!(
            done.recv_timeout(Duration::from_millis(50)),
            crate::Recv::Timeout
        ));
    }
}
