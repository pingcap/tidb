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

use crate::{
    AllServerInfo, Context, DoneCh, GlobalVerRx, SharedRecv, SyncSummary, Syncer, WatchEvent,
};

/// Go `checkVersionsInterval`.
const CHECK_VERSIONS_INTERVAL: Duration = Duration::from_millis(2);

/// Go `MemSyncer`.
pub struct MemSyncer {
    self_schema_version: AtomicI64,
    mdl_schema_versions: Mutex<HashMap<i64, i64>>,
    global_ver: Mutex<Option<(std::sync::mpsc::SyncSender<WatchEvent>, GlobalVerRx)>>,
    mock_session: Mutex<Option<(std::sync::mpsc::SyncSender<()>, DoneCh)>>,
}

impl Default for MemSyncer {
    fn default() -> Self {
        Self::new()
    }
}

impl MemSyncer {
    /// Go `NewMemSyncer`.
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
        // Go `close(ch)` panics for a nil/already-closed channel.
        drop(slot.take().expect("close of nil or closed mock session"));
    }
}

fn fresh_channel<T>() -> (std::sync::mpsc::SyncSender<T>, SharedRecv<T>) {
    let (sender, receiver) = std::sync::mpsc::sync_channel::<T>(1);
    (sender, SharedRecv::new(receiver))
}

fn fresh_global_channel() -> (std::sync::mpsc::SyncSender<WatchEvent>, GlobalVerRx) {
    let (sender, receiver) = std::sync::mpsc::sync_channel::<WatchEvent>(1);
    (sender, SharedRecv::new(receiver))
}

fn never_channel<T>() -> SharedRecv<T> {
    let (sender, receiver) = std::sync::mpsc::channel::<T>();
    // Go's nil receive channel blocks forever. Keeping this sender alive for
    // the process lifetime is the native equivalent.
    std::mem::forget(sender);
    SharedRecv::new(receiver)
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
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(fresh_global_channel());
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
            let _ = sender.try_send(WatchEvent::default());
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
            None => never_channel(),
        }
    }

    /// Go `WatchGlobalSchemaVer`.
    fn watch_global_schema_ver(&self, _ctx: &Context) {}

    /// Go `Done`.
    fn done(&self) -> DoneCh {
        self.mock_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|(_, receiver)| receiver.clone())
            .unwrap_or_else(never_channel)
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
        _check_assumed_server: bool,
    ) -> Result<SyncSummary, String> {
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
                    return Ok(SyncSummary {
                        server_count: 1,
                        assumed_server_count: 0,
                    });
                }
            } else if self.self_schema_version.load(Ordering::Acquire) >= latest_ver {
                return Ok(SyncSummary {
                    server_count: 1,
                    assumed_server_count: 0,
                });
            }
        }
    }

    /// Go `SyncJobSchemaVerLoop`.
    fn sync_job_schema_ver_loop(&self, _ctx: &Context) {}

    /// Go `SetServerInfoSyncer` is intentionally unused by the memory
    /// implementation.
    fn set_server_info_syncer(&self, _all_server_info: AllServerInfo) {}

    /// Go `Close`.
    fn close(&self) {}
}
