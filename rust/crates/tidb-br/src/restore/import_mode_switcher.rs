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

//! Go `br/pkg/restore/import_mode_switcher.go`: TiKV's import mode during
//! an offline restore, and the schedulers paused around it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_gocontext::{CancelFunc, Context};
use tidb_log::{Field, Value};
use tidb_proto::import_sstpb::{self, import_sst_client::ImportSstClient};

use crate::conn::{get_all_tikv_stores, Mgr, StoreBehavior};
use crate::errors::{self, Result};
use crate::pdclient::PdClient;
use crate::pdutil::{nop, ClusterConfig, UndoFunc};
use crate::utils::worker_pool::{ErrGroup, WorkerPool};

/// Go `ImportModeSwitcher`.
pub struct ImportModeSwitcher {
    pd_client: Arc<dyn PdClient>,
    switch_mode_interval: Duration,
    security: tidb_pd_client::ClusterSecurity,
    runtime: Arc<tokio::runtime::Runtime>,
    state: Mutex<Option<(CancelFunc, std::thread::JoinHandle<()>)>>,
}

impl ImportModeSwitcher {
    /// Go `NewImportModeSwitcher`.
    #[must_use]
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        switch_mode_interval: Duration,
        security: tidb_pd_client::ClusterSecurity,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pd_client,
            switch_mode_interval,
            security,
            runtime,
            state: Mutex::new(None),
        })
    }

    /// Go `SwitchToNormalMode`: stops the import-mode loop and switches
    /// every store back.
    ///
    /// # Errors
    /// A store's error.
    pub fn switch_to_normal_mode(&self, ctx: &Context) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let Some((cancel, worker)) = state.take() else {
            tidb_log::info("TiKV is already in normal mode", &[]);
            return Ok(());
        };
        tidb_log::info("Stopping the import mode goroutine", &[]);
        cancel.cancel();
        let _ = worker.join();
        self.switch_tikv_mode(ctx, import_sstpb::SwitchMode::Normal)
    }

    /// Go `switchTiKVMode`: `SwitchMode` on every TiKV store.
    fn switch_tikv_mode(&self, ctx: &Context, mode: import_sstpb::SwitchMode) -> Result<()> {
        let stores = get_all_tikv_stores(ctx, self.pd_client.as_ref(), StoreBehavior::SkipTiFlash)?;
        let pool = WorkerPool::new(stores.len(), "switch import mode");
        let (eg, ectx) = ErrGroup::with_context(ctx);
        for store in stores {
            if let Some(err) = ectx.err() {
                return Err(Arc::new(err));
            }
            let (security, runtime) = (self.security.clone(), Arc::clone(&self.runtime));
            pool.apply_on_error_group(&eg, move || {
                let endpoint = tidb_pd_client::secure_endpoint(&store.address, &security)
                    .map_err(|e| errors::errorf(e.to_string()))?
                    .connect_timeout(Duration::from_secs(5));
                runtime.block_on(async move {
                    let channel = endpoint
                        .connect()
                        .await
                        .map_err(|e| errors::errorf(e.to_string()))?;
                    let mut client = ImportSstClient::new(channel);
                    client
                        .switch_mode(import_sstpb::SwitchModeRequest {
                            mode: mode as i32,
                            ..Default::default()
                        })
                        .await
                        .map_err(errors::grpc)?;
                    Ok(())
                })
            });
        }
        eg.wait()
    }

    /// Go `GoSwitchToImportMode`: switches every store to import mode now
    /// and again every interval until switched back.
    ///
    /// # Errors
    /// The first switch's error.
    pub fn go_switch_to_import_mode(self: &Arc<Self>, ctx: &Context) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.is_some() {
            tidb_log::info("TiKV is already in import mode", &[]);
            return Ok(());
        }
        let (ctx, cancel) = Context::with_cancel(ctx);
        tidb_log::info("switch to import mode at beginning", &[]);
        if let Err(err) = self.switch_tikv_mode(&ctx, import_sstpb::SwitchMode::Import) {
            tidb_log::warn(
                "switch to import mode failed",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
            // Go keeps the cancel func set after a failed first switch.
            *state = Some((cancel, std::thread::spawn(|| {})));
            return Err(err);
        }
        let this = Arc::clone(self);
        let loop_ctx = ctx.clone();
        let worker = std::thread::spawn(move || {
            let tick = crossbeam_channel::tick(this.switch_mode_interval);
            loop {
                crossbeam_channel::select! {
                    recv(loop_ctx.done()) -> _ => {
                        tidb_log::info("stop automatic switch to import mode when context done", &[]);
                        return;
                    }
                    recv(tick) -> _ => {
                        tidb_log::info("switch to import mode", &[]);
                        if let Err(err) = this.switch_tikv_mode(&loop_ctx, import_sstpb::SwitchMode::Import) {
                            tidb_log::warn("switch to import mode failed", &[Field::new("error", Value::Str(err.to_string()))]);
                        }
                    }
                }
            }
        });
        *state = Some((cancel, worker));
        Ok(())
    }
}

/// Go `RestorePreWork`: nothing for an online restore; otherwise TiKV's
/// import mode (when asked) and the schedulers paused with their config.
///
/// # Errors
/// The import mode switch or the scheduler pause.
pub fn restore_pre_work(
    ctx: &Context,
    mgr: &Mgr,
    switcher: &Arc<ImportModeSwitcher>,
    is_online: bool,
    switch_to_import: bool,
) -> (UndoFunc, Option<ClusterConfig>, Result<()>) {
    if is_online {
        return (nop(), None, Ok(()));
    }
    if switch_to_import {
        if let Err(err) = switcher.go_switch_to_import_mode(ctx) {
            return (nop(), None, Err(err));
        }
    }
    mgr.pd_controller.remove_schedulers_with_config(ctx)
}

/// Go `FineGrainedRestorePreWork`: the schedulers paused only on the
/// restored key ranges.
///
/// # Errors
/// The import mode switch, PD's config, or the region pause.
pub fn fine_grained_restore_pre_work(
    ctx: &Context,
    mgr: &Mgr,
    switcher: &Arc<ImportModeSwitcher>,
    key_ranges: &[(Vec<u8>, Vec<u8>)],
    switch_to_import: bool,
) -> (UndoFunc, Option<ClusterConfig>, Result<()>) {
    if switch_to_import {
        tidb_log::info("switch to import mode for offline restore", &[]);
        if let Err(err) = switcher.go_switch_to_import_mode(ctx) {
            return (nop(), None, Err(err));
        }
    }
    let origin_cfg = match mgr.pd_controller.get_origin_pd_config(ctx) {
        Ok(c) => c,
        Err(err) => return (nop(), None, Err(err)),
    };
    let (rule_id, wait_pause_scheduler_done, res) = mgr
        .pd_controller
        .remove_schedulers_on_region(ctx, key_ranges);
    if let Err(err) = res {
        return (nop(), None, Err(err));
    }
    let mut new_cfg = origin_cfg.clone();
    new_cfg.rule_id = rule_id;
    let undo = mgr
        .pd_controller
        .make_fine_grained_undo_function(new_cfg, wait_pause_scheduler_done);
    (undo, Some(origin_cfg), Ok(()))
}

/// Go `RestorePostWork`: TiKV back to normal mode (offline restore) and the
/// schedulers restored; failures are only logged.
pub fn restore_post_work(
    ctx: &Context,
    switcher: &ImportModeSwitcher,
    restore_schedulers: UndoFunc,
    is_online: bool,
) {
    let ctx = if ctx.err().is_some() {
        tidb_log::warn("context canceled, try shutdown", &[]);
        Context::background()
    } else {
        ctx.clone()
    };
    if !is_online {
        if let Err(err) = switcher.switch_to_normal_mode(&ctx) {
            tidb_log::warn(
                "fail to switch to normal mode",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        }
    }
    if let Err(err) = restore_schedulers(&ctx) {
        tidb_log::warn(
            "failed to restore PD schedulers",
            &[Field::new("error", Value::Str(err.to_string()))],
        );
    }
}
