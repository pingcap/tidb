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

//! Go `br/pkg/registry/heartbeat.go`: a running restore refreshes its
//! registration's heartbeat every minute.

use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_sqlescape::SqlArg;

use super::SharedSession;
use crate::errors::{self, Result};

/// Go `UpdateHeartbeatSQLTemplate`.
pub const UPDATE_HEARTBEAT_SQL: &str = "
		UPDATE mysql.tidb_restore_registry
		SET last_heartbeat_time = FROM_UNIXTIME(%?)
		WHERE id = %?";

/// Go `defaultHeartbeatIntervalSeconds`.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Go `UpdateHeartbeat` on the heartbeat session.
///
/// # Errors
/// The update fails.
pub fn update_heartbeat(ctx: &Context, se: &SharedSession, restore_id: u64) -> Result<()> {
    let now = super::now_unix();
    se.lock()
        .unwrap()
        .execute_internal(
            ctx,
            UPDATE_HEARTBEAT_SQL,
            &[SqlArg::Signed(now), SqlArg::Unsigned(restore_id)],
        )
        .map_err(|e| {
            errors::annotate(
                e,
                format!("failed to update heartbeat for task {restore_id}"),
            )
        })?;
    tidb_log::debug(
        "updated task heartbeat",
        &[
            Field::new("restore_id", Value::U64(restore_id)),
            Field::new("timestamp", Value::I64(now)),
        ],
    );
    Ok(())
}

/// Go `HeartbeatManager`: a heartbeat now, then every interval, until
/// stopped or the context ends.
pub struct HeartbeatManager {
    stop: crossbeam_channel::Sender<()>,
    done: std::thread::JoinHandle<()>,
}

impl HeartbeatManager {
    /// Go `NewHeartbeatManager` and `Start`.
    #[must_use]
    pub fn start(ctx: &Context, se: SharedSession, restore_id: u64) -> Self {
        Self::start_with_interval(ctx, se, restore_id, DEFAULT_HEARTBEAT_INTERVAL)
    }

    pub(crate) fn start_with_interval(
        ctx: &Context,
        se: SharedSession,
        restore_id: u64,
        interval: Duration,
    ) -> Self {
        let (stop, stop_rx) = crossbeam_channel::bounded::<()>(0);
        let ctx = ctx.clone();
        let done = std::thread::spawn(move || {
            let warn = |msg: &str, err: &errors::Error| {
                tidb_log::warn(
                    msg,
                    &[
                        Field::new("restore_id", Value::U64(restore_id)),
                        Field::new("error", Value::Str(err.to_string())),
                    ],
                );
            };
            if let Err(err) = update_heartbeat(&ctx, &se, restore_id) {
                warn("failed to send initial heartbeat", &err);
            }
            let tick = crossbeam_channel::tick(interval);
            loop {
                crossbeam_channel::select! {
                    recv(tick) -> _ => {
                        if let Err(err) = update_heartbeat(&ctx, &se, restore_id) {
                            warn("failed to update heartbeat", &err);
                        }
                    }
                    recv(stop_rx) -> _ => return,
                    recv(ctx.done()) -> _ => {
                        tidb_log::warn("heartbeat manager context done", &[Field::new("restore_id", Value::U64(restore_id))]);
                        return;
                    }
                }
            }
        });
        Self { stop, done }
    }

    /// Go `Stop`: waits for the thread.
    pub fn stop(self) {
        drop(self.stop);
        let _ = self.done.join();
        tidb_log::info("stopped heartbeat manager", &[]);
    }
}
