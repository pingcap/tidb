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

//! Go `pkg/session/sync_upgrade.go`: the cluster's upgrading state, which an
//! operator enters and leaves through the status port's `/upgrade/*`.

use std::time::{Duration, Instant};

use tidb_ddl_serverstate::{StateInfo, Syncer, STATE_NORMAL_RUNNING, STATE_UPGRADING};
use tidb_log::{Field, Value};
use tidb_timeutil::SleepContext;

/// Go `util.DDLOwnerKey`.
const DDL_OWNER_KEY: &str = "/tidb/ddl/fg/owner";

fn upgrading_logger() -> tidb_logutil::Logger {
    tidb_logutil::bg_logger().with_fields(&[Field::new("category", Value::Str("upgrading".to_owned()))])
}

/// Go `SyncUpgradeState` syncs upgrade state to etcd.
///
/// # Errors
///
/// The state update's failure, or Go's `context deadline exceeded` once the
/// DDL owner has not acknowledged the state within `timeout`.
pub fn sync_upgrade_state(
    state_syncer: &dyn Syncer,
    etcd_cli: Option<&dyn tidb_owner::OwnerStore>,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let logger = upgrading_logger();
    if let Err(error) = state_syncer.update_global_state(
        &SleepContext::with_timeout(timeout),
        &StateInfo::new(STATE_UPGRADING),
    ) {
        logger.error(
            "update global state failed",
            &[
                Field::new("state", Value::Str(STATE_UPGRADING.to_owned())),
                tidb_logutil::zap_error(&error),
            ],
        );
        return Err(error.to_string());
    }

    let interval = Duration::from_millis(200);
    for i in 0.. {
        if Instant::now() >= deadline {
            logger.error(
                "get owner op failed",
                &[Field::new("timeout", Value::Duration(timeout.as_nanos() as i64))],
            );
            return Err("context deadline exceeded".to_owned());
        }

        let child = tidb_owner::Context::with_timeout(Duration::from_secs(3));
        let op = tidb_owner::get_owner_op_value(&child, etcd_cli, DDL_OWNER_KEY);
        if op.as_ref().is_ok_and(|op| op.is_synced_upgrading_state()) {
            break;
        }
        if i % 10 == 0 {
            let mut fields = Vec::new();
            match &op {
                Ok(op) => fields.push(Field::new("op", Value::Str(op.to_string()))),
                Err(error) => fields.push(tidb_logutil::zap_error(error)),
            }
            logger.warn("get owner op failed", &fields);
        }
        std::thread::sleep(interval);
    }

    logger.info(
        "update global state to upgrading",
        &[Field::new("state", Value::Str(STATE_UPGRADING.to_owned()))],
    );
    Ok(())
}

/// Go `SyncNormalRunning` syncs normal state to etcd.
///
/// # Errors
///
/// The state update's failure.
pub fn sync_normal_running(
    state_syncer: &dyn Syncer,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
) -> Result<(), String> {
    let logger = upgrading_logger();
    match sess_pool
        .open_sql_session()
        .map_err(|error| error.to_string())
        .and_then(|mut sess| {
            tidb_ddl::job_control::resume_all_jobs_by_system(sess.as_mut())
                .map_err(|error| error.to_string())
        }) {
        Err(error) => logger.warn("resume all paused jobs failed", &[tidb_logutil::zap_error(&error)]),
        Ok(job_errs) => {
            for error in job_errs.values() {
                logger.warn("resume the job failed", &[tidb_logutil::zap_error(error)]);
            }
        }
    }

    // Go `dist_store.GetTaskManager()`: this node runs no distributed task
    // framework (O10), so there is no task overflow concurrency to adjust.

    if let Err(error) = state_syncer.update_global_state(
        &SleepContext::with_timeout(Duration::from_secs(3)),
        &StateInfo::new(STATE_NORMAL_RUNNING),
    ) {
        logger.error("update global state to normal failed", &[tidb_logutil::zap_error(&error)]);
        return Err(error.to_string());
    }
    logger.info("update global state to normal running finished", &[]);
    Ok(())
}

/// Go `IsUpgradingClusterState` checks whether the global state is upgrading.
///
/// # Errors
///
/// The state read's failure.
pub fn is_upgrading_cluster_state(state_syncer: &dyn Syncer) -> Result<bool, String> {
    let state_info = state_syncer
        .get_global_state(&SleepContext::with_timeout(Duration::from_secs(3)))
        .map_err(|error| error.to_string())?;
    Ok(state_info.state == STATE_UPGRADING)
}
