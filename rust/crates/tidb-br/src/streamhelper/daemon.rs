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


//! Go `streamhelper/daemon`: a daemon that runs on every TiDB node but does
//! its work only on the etcd-elected owner.

use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::{CancelFunc, Context};
use tidb_log::{Field, Value};

use crate::errors::{self, Result};

/// Go `daemon.Interface`.
pub trait Interface: Send + Sync {
    /// Go `OnStart`: on every node, owner or not.
    fn on_start(&self, ctx: &Context);
    /// Go `OnBecomeOwner`: `ctx` is cancelled once this node stops owning.
    fn on_become_owner(&self, ctx: &Context);
    /// Go `OnTick`.
    ///
    /// # Errors
    /// The tick's error, which is logged.
    fn on_tick(&self, ctx: &Context) -> Result<()>;
    /// Go `Name`.
    fn name(&self) -> String;
}

/// Go `OwnerDaemon`.
pub struct OwnerDaemon {
    daemon: Arc<dyn Interface>,
    manager: Arc<dyn tidb_owner::Manager>,
    tick_interval: Duration,
    /// When set, this node is the owner and the daemon runs.
    cancel: Option<CancelFunc>,
}

impl OwnerDaemon {
    /// Go `New`.
    #[must_use]
    pub fn new(daemon: Arc<dyn Interface>, manager: Arc<dyn tidb_owner::Manager>, tick_interval: Duration) -> Self {
        Self { daemon, manager, tick_interval, cancel: None }
    }

    /// Go `Running`.
    #[must_use]
    pub fn running(&self) -> bool {
        self.cancel.is_some()
    }

    fn cancel_run(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            tidb_log::info("cancel running daemon", &[Field::new("daemon", Value::Str(self.daemon.name()))]);
            cancel.cancel();
        }
    }

    fn owner_tick(&mut self, ctx: &Context) {
        if !self.running() {
            let (cx, cancel) = Context::with_cancel(ctx);
            self.cancel = Some(cancel);
            tidb_log::info(
                "daemon became owner",
                &[
                    Field::new("id", Value::Str(self.manager.id().to_owned())),
                    Field::new("daemon-id", Value::Str(self.daemon.name())),
                ],
            );
            self.daemon.on_become_owner(&cx);
        }
        if let Err(err) = self.daemon.on_tick(ctx) {
            tidb_log::warn("failed on tick", &[Field::new("error", Value::Str(err.to_string()))]);
        }
    }

    /// Go `Begin`: campaigns, starts the service, and returns the tick loop,
    /// which runs until `ctx` is done.
    ///
    /// # Errors
    /// The campaign error.
    pub fn begin(mut self, ctx: &Context) -> Result<impl FnOnce() + Send> {
        tidb_log::info("begin advancer daemon", &[Field::new("daemon-id", Value::Str(self.daemon.name()))]);
        self.manager.campaign_owner(&[]).map_err(errors::errorf)?;
        self.daemon.on_start(ctx);
        let ctx = ctx.clone();
        Ok(move || {
            tidb_log::info(
                "begin running daemon",
                &[
                    Field::new("id", Value::Str(self.manager.id().to_owned())),
                    Field::new("daemon-id", Value::Str(self.daemon.name())),
                ],
            );
            let tick = crossbeam_channel::tick(self.tick_interval);
            loop {
                crossbeam_channel::select! {
                    recv(ctx.done()) -> _ => {
                        tidb_log::info(
                            "daemon loop exits",
                            &[
                                Field::new("id", Value::Str(self.manager.id().to_owned())),
                                Field::new("daemon-id", Value::Str(self.daemon.name())),
                            ],
                        );
                        self.cancel_run();
                        return;
                    }
                    recv(tick) -> _ => {
                        if self.manager.is_owner() {
                            self.owner_tick(&ctx);
                        } else {
                            self.cancel_run();
                        }
                    }
                }
            }
        })
    }
}
