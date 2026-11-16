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

//! Go `br/pkg/gc`: the service safe point that keeps PD's GC behind a
//! backup's snapshot, over the global safe point API or, in a keyspace, the
//! keyspace's GC barriers.

use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};

use crate::errors::{self, Result, ERR_BACKUP_GC_SAFEPOINT_EXCEEDED, ERR_INVALID_ARGUMENT};
use crate::pdclient::PdClient;

/// Go `preUpdateServiceSafePointFactor`.
const PRE_UPDATE_SERVICE_SAFE_POINT_FACTOR: i64 = 3;
/// Go `checkGCSafePointGapTime`.
const CHECK_GC_SAFE_POINT_GAP_TIME: Duration = Duration::from_secs(5);

/// Go `DefaultBRGCSafePointTTL`, seconds.
pub const DEFAULT_BR_GC_SAFE_POINT_TTL: i64 = 5 * 60;
/// Go `DefaultCheckpointGCSafePointTTL`, seconds.
pub const DEFAULT_CHECKPOINT_GC_SAFE_POINT_TTL: i64 = 72 * 60;
/// Go `DefaultStreamStartSafePointTTL`, seconds.
pub const DEFAULT_STREAM_START_SAFE_POINT_TTL: i64 = 1800;
/// Go `DefaultStreamPauseSafePointTTL`, seconds.
pub const DEFAULT_STREAM_PAUSE_SAFE_POINT_TTL: i64 = 24 * 3600;

/// Go `tikv.NullspaceID`.
pub const NULLSPACE_ID: u32 = 0xFFFF_FFFF;

/// Go `BRServiceSafePoint`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrServiceSafePoint {
    /// Go `ID`.
    pub id: String,
    /// Go `TTL`, seconds.
    pub ttl: i64,
    /// Go `BackupTS`.
    pub backup_ts: u64,
}

impl BrServiceSafePoint {
    /// Go `MarshalLogObject`: the fields Go logs the safe point with.
    #[must_use]
    pub fn log_fields(&self) -> Vec<Field> {
        let physical = crate::pdclient::extract_physical(self.backup_ts);
        let backup_time = chrono::DateTime::from_timestamp_millis(physical)
            .map(|t| t.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S%.3f %z %Z").to_string())
            .unwrap_or_default();
        vec![
            Field::new("ID", Value::Str(self.id.clone())),
            Field::new("TTL", Value::Str(tidb_model::go_duration::format_go_duration(self.ttl * 1_000_000_000))),
            Field::new("BackupTime", Value::Str(backup_time)),
            Field::new("BackupTS", Value::U64(self.backup_ts)),
        ]
    }
}

impl std::fmt::Display for BrServiceSafePoint {
    /// Go `%v` of the struct.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{} {} {}}}", self.id, self.ttl, self.backup_ts)
    }
}

/// Go `MakeSafePointID`: `br-<uuid>`.
#[must_use]
pub fn make_safe_point_id() -> String {
    format!("br-{}", uuid::Uuid::new_v4())
}

/// Go `Manager`.
pub trait Manager: Send + Sync {
    /// Go `GetGCSafePoint`.
    ///
    /// # Errors
    /// PD's error.
    fn get_gc_safe_point(&self, ctx: &Context) -> Result<u64>;
    /// Go `SetServiceSafePoint`.
    ///
    /// # Errors
    /// PD's error.
    fn set_service_safe_point(&self, ctx: &Context, sp: &BrServiceSafePoint) -> Result<()>;
    /// Go `DeleteServiceSafePoint`.
    ///
    /// # Errors
    /// PD's error.
    fn delete_service_safe_point(&self, ctx: &Context, sp: &BrServiceSafePoint) -> Result<()>;
}

/// Go `NewManager`: the global manager without a keyspace.
#[must_use]
pub fn new_manager(pd_client: Arc<dyn PdClient>, keyspace_id: u32) -> Arc<dyn Manager> {
    if keyspace_id == NULLSPACE_ID {
        Arc::new(GlobalManager { pd_client })
    } else {
        Arc::new(KeyspaceManager { pd_client, keyspace_id })
    }
}

/// Go `globalManager`.
struct GlobalManager {
    pd_client: Arc<dyn PdClient>,
}

impl Manager for GlobalManager {
    fn get_gc_safe_point(&self, ctx: &Context) -> Result<u64> {
        self.pd_client.update_gc_safe_point(ctx, 0)
    }

    fn set_service_safe_point(&self, ctx: &Context, sp: &BrServiceSafePoint) -> Result<()> {
        tidb_log::debug("update PD safePoint limit with TTL", &sp.log_fields());
        let res = self.pd_client.update_service_gc_safe_point(ctx, &sp.id, sp.ttl, sp.backup_ts.wrapping_sub(1));
        let last = *res.as_ref().unwrap_or(&0);
        if last > sp.backup_ts.wrapping_sub(1) && sp.ttl > 0 {
            let mut fields = vec![Field::new("lastSafePoint", Value::U64(last))];
            fields.extend(sp.log_fields());
            tidb_log::warn("service GC safe point lost, we may fail to back up if GC lifetime isn't long enough", &fields);
        }
        res.map(|_| ())
    }

    fn delete_service_safe_point(&self, ctx: &Context, sp: &BrServiceSafePoint) -> Result<()> {
        self.pd_client.update_service_gc_safe_point(ctx, &sp.id, 0, 0).map(|_| ())
    }
}

/// Go `keyspaceManager`: the keyspace's GC barriers.
struct KeyspaceManager {
    pd_client: Arc<dyn PdClient>,
    keyspace_id: u32,
}

impl Manager for KeyspaceManager {
    fn get_gc_safe_point(&self, ctx: &Context) -> Result<u64> {
        self.pd_client.get_keyspace_gc_safe_point(ctx, self.keyspace_id)
    }

    fn set_service_safe_point(&self, ctx: &Context, sp: &BrServiceSafePoint) -> Result<()> {
        let mut fields = vec![Field::new("keyspaceID", Value::U64(u64::from(self.keyspace_id)))];
        fields.extend(sp.log_fields());
        tidb_log::debug("set keyspace GC barrier", &fields);
        if sp.ttl <= 0 {
            return self.delete_service_safe_point(ctx, sp);
        }
        let (barrier_id, barrier_ts, ttl) = self.pd_client.set_gc_barrier(
            ctx,
            self.keyspace_id,
            &sp.id,
            sp.backup_ts.wrapping_sub(1),
            Duration::from_secs(sp.ttl as u64),
        )?;
        tidb_log::debug(
            "set keyspace GC barrier succeeded",
            &[
                Field::new("keyspaceID", Value::U64(u64::from(self.keyspace_id))),
                Field::new("barrierID", Value::Str(barrier_id)),
                Field::new("barrierTS", Value::U64(barrier_ts)),
                Field::new("TTL", Value::Duration(ttl.as_nanos() as i64)),
            ],
        );
        Ok(())
    }

    fn delete_service_safe_point(&self, ctx: &Context, sp: &BrServiceSafePoint) -> Result<()> {
        self.pd_client.delete_gc_barrier(ctx, self.keyspace_id, &sp.id)?;
        tidb_log::debug(
            "deleted keyspace GC barrier",
            &[
                Field::new("keyspaceID", Value::U64(u64::from(self.keyspace_id))),
                Field::new("barrierID", Value::Str(sp.id.clone())),
            ],
        );
        Ok(())
    }
}

/// Go `CheckGCSafePoint`: an error when `ts` is at or behind PD's GC safe
/// point. Failing to read the safe point only logs.
///
/// # Errors
/// [`ERR_BACKUP_GC_SAFEPOINT_EXCEEDED`].
pub fn check_gc_safe_point(ctx: &Context, mgr: &dyn Manager, ts: u64) -> Result<()> {
    let safe_point = match mgr.get_gc_safe_point(ctx) {
        Ok(sp) => sp,
        Err(err) => {
            tidb_log::warn("fail to get GC safe point", &[Field::new("error", Value::Str(err.to_string()))]);
            return Ok(());
        }
    };
    if ts <= safe_point {
        return Err(errors::annotatef(
            &ERR_BACKUP_GC_SAFEPOINT_EXCEEDED,
            format!("GC safepoint {safe_point} exceed TS {ts}"),
        ));
    }
    Ok(())
}

/// Go `StartServiceSafePointKeeper`: sets the service safe point, then
/// refreshes it every third of its TTL and checks the GC safe point every
/// 5 seconds until `ctx` is done. Go panics the process when the GC safe
/// point passes the backup; here the keeper cancels `on_lost` instead, which
/// the caller wires to the task's context.
///
/// # Errors
/// An invalid safe point, a GC safe point past the backup, or PD's error.
pub fn start_service_safe_point_keeper(
    ctx: &Context,
    sp: BrServiceSafePoint,
    mgr: Arc<dyn Manager>,
) -> Result<std::thread::JoinHandle<Option<errors::Error>>> {
    if sp.id.is_empty() || sp.ttl <= 0 {
        return Err(errors::annotatef(&ERR_INVALID_ARGUMENT, format!("invalid service safe point {sp}")));
    }
    check_gc_safe_point(ctx, mgr.as_ref(), sp.backup_ts)?;
    mgr.set_service_safe_point(ctx, &sp)?;
    let update_gap = Duration::from_secs(sp.ttl as u64) / PRE_UPDATE_SERVICE_SAFE_POINT_FACTOR as u32;
    let ctx = ctx.clone();
    std::thread::Builder::new()
        .name("br-gc-keeper".into())
        .spawn(move || {
            let update_tick = crossbeam_channel::tick(update_gap);
            let check_tick = crossbeam_channel::tick(CHECK_GC_SAFE_POINT_GAP_TIME);
            loop {
                crossbeam_channel::select! {
                    recv(ctx.done()) -> _ => {
                        tidb_log::debug("service safe point keeper exited", &[]);
                        return None;
                    }
                    recv(update_tick) -> _ => {
                        if let Err(err) = mgr.set_service_safe_point(&ctx, &sp) {
                            tidb_log::warn(
                                "failed to update service safe point, backup may fail if gc triggered",
                                &[Field::new("error", Value::Str(err.to_string()))],
                            );
                        }
                    }
                    recv(check_tick) -> _ => {
                        if let Err(err) = check_gc_safe_point(&ctx, mgr.as_ref(), sp.backup_ts) {
                            let mut fields = vec![Field::new("error", Value::Str(err.to_string()))];
                            fields.extend(sp.log_fields());
                            tidb_log::error("cannot pass gc safe point check, aborting", &fields);
                            return Some(err);
                        }
                    }
                }
            }
        })
        .map_err(|e| errors::errorf(e.to_string()))
}

#[cfg(test)]
mod tests;
