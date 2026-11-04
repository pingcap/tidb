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

//! Go `pkg/session/upgrade_run.go`, with the start-mode decision of
//! `session.go` (`getStartMode`, `mustGetStoreBootstrapVersion`,
//! `finishBootstrap`) that chooses it.

use std::time::{Duration, Instant};

use tidb_ddl::StartMode;

use crate::bootstrap::{
    get_bootstrap_version, internal_sql_timeout, update_bootstrap_ver, UpgradeResult,
    UpgradeSession, UpgradeStore,
};
use crate::upgrade_def::{
    upgrade_to_ver99_after, upgrade_to_ver99_before, CURRENT_BOOTSTRAP_VERSION,
    UPGRADE_TO_VER_FUNCTIONS, VERSION174,
};

/// Go `notBootstrapped`.
pub const NOT_BOOTSTRAPPED: i64 = 0;

/// Go `SupportUpgradeHTTPOpVer`: the minimum version of the upgrade by paused
/// user DDL can be notified through the HTTP API.
const SUPPORT_UPGRADE_HTTP_OP_VER: i64 = VERSION174;

/// Go `getStartMode` gets the start mode according to the bootstrap version.
#[must_use]
pub fn get_start_mode(ver: i64) -> StartMode {
    if ver == NOT_BOOTSTRAPPED {
        StartMode::Bootstrap
    } else if ver < CURRENT_BOOTSTRAP_VERSION {
        StartMode::Upgrade
    } else {
        StartMode::Normal
    }
}

/// Go `mustGetStoreBootstrapVersion`: the version in meta, which
/// `finishBootstrap` writes.
pub fn must_get_store_bootstrap_version(store: &dyn UpgradeStore) -> Result<i64, String> {
    store
        .get_bootstrap_version()
        .map_err(|error| format!("get store bootstrap version failed: {error}"))
}

/// Go `finishBootstrap`.
pub fn finish_bootstrap(store: &dyn UpgradeStore) -> UpgradeResult {
    store
        .finish_bootstrap(CURRENT_BOOTSTRAP_VERSION)
        .map_err(|error| format!("finish bootstrap failed: {error}"))
}

/// Go `InitMDLVariableForBootstrap` initializes the metadata lock variable.
pub fn init_mdl_variable_for_bootstrap(store: &dyn UpgradeStore) -> UpgradeResult {
    store.set_metadata_lock(true)?;
    tidb_vardef::set_enable_mdl(true);
    Ok(())
}

/// Go `upgrade` function will do some upgrade works, when the system is
/// bootstrapped by low version TiDB server. For example, add new system
/// variables into mysql.global_variables table.
pub fn upgrade(s: &mut dyn UpgradeSession) -> UpgradeResult {
    // Do upgrade works then update bootstrap version.
    let is_null = init_mdl_variable_for_upgrade(s.get_store())
        .map_err(|error| format!("init metadata lock failed during upgrade: {error}"))?;

    let ver = get_bootstrap_version(s)?;
    if ver >= CURRENT_BOOTSTRAP_VERSION {
        // It is already bootstrapped/upgraded by a higher version TiDB server.
        return Ok(());
    }

    print_cluster_state(s, ver);

    // when upgrade from v6.4.0 or earlier, enables metadata lock automatically,
    // but during upgrade we disable it.
    if is_null {
        upgrade_to_ver99_before(s)?;
    }

    for ver_fn in UPGRADE_TO_VER_FUNCTIONS {
        if ver < ver_fn.version {
            (ver_fn.func)(s, ver)?;
            tidb_logutil::bg_logger().info(
                "upgrade to bootstrap version.",
                &[
                    tidb_log::Field::new("old-start-version", tidb_log::Value::I64(ver)),
                    tidb_log::Field::new("in-progress-version", tidb_log::Value::I64(ver_fn.version)),
                    tidb_log::Field::new("target-version", tidb_log::Value::I64(CURRENT_BOOTSTRAP_VERSION)),
                ],
            );
        }
    }
    if is_null {
        upgrade_to_ver99_after(s)?;
    }

    update_bootstrap_ver(s)?;
    if let Err(error) = s.execute_internal("COMMIT") {
        let sleep_time = Duration::from_secs(1);
        tidb_logutil::bg_logger().info(
            "update bootstrap ver failed",
            &[
                tidb_logutil::zap_error(error.error_string()),
                tidb_log::Field::new(
                    "sleeping time",
                    tidb_log::Value::Duration(sleep_time.as_nanos() as i64),
                ),
            ],
        );
        std::thread::sleep(sleep_time);
        // Check if TiDB is already upgraded.
        let v = get_bootstrap_version(s).map_err(|error| format!("upgrade failed: {error}"))?;
        if v >= CURRENT_BOOTSTRAP_VERSION {
            // It is already bootstrapped/upgraded by a higher version TiDB server.
            return Ok(());
        }
        return Err(format!(
            "[upgrade] upgrade failed from {ver} to {CURRENT_BOOTSTRAP_VERSION}: {}",
            error.error_string()
        ));
    }
    Ok(())
}

/// Go `InitMDLVariableForUpgrade` initializes the metadata lock variable;
/// answers whether the stored switch was missing.
pub fn init_mdl_variable_for_upgrade(store: &dyn UpgradeStore) -> Result<bool, String> {
    let stored = store.get_metadata_lock();
    let (enable, is_null) = match &stored {
        Ok(Some(enable)) => (*enable, false),
        Ok(None) => (false, true),
        Err(_) => (false, false),
    };
    tidb_vardef::set_enable_mdl(!is_null && enable);
    stored.map(|_| is_null)
}

/// Go `printClusterState`.
fn print_cluster_state(s: &mut dyn UpgradeSession, ver: i64) {
    // After SupportUpgradeHTTPOpVer version, the upgrade by paused user DDL can be notified through the HTTP API.
    // We check the global state see if we are upgrading by paused the user DDL.
    if ver >= SUPPORT_UPGRADE_HTTP_OP_VER {
        is_upgrading_cluster_state_with_retry(s, ver, CURRENT_BOOTSTRAP_VERSION, internal_sql_timeout());
    }
}

/// Go `IsUpgradingClusterState` checks whether the global state is upgrading.
pub fn is_upgrading_cluster_state(s: &dyn UpgradeSession) -> Result<bool, String> {
    let context = tidb_timeutil::SleepContext::with_timeout(Duration::from_secs(3));
    let state_info = s
        .state_syncer()
        .get_global_state(&context)
        .map_err(|error| error.to_string())?;
    Ok(state_info.state == tidb_ddl_serverstate::STATE_UPGRADING)
}

/// Go `isUpgradingClusterStateWithRetry`.
fn is_upgrading_cluster_state_with_retry(
    s: &dyn UpgradeSession,
    old_ver: i64,
    new_ver: i64,
    timeout: Duration,
) {
    let now = Instant::now();
    let interval = Duration::from_millis(200);
    let logger = tidb_logutil::bg_logger();
    let category = || tidb_log::Field::new("category", tidb_log::Value::Str("upgrading".to_owned()));
    let versions = || {
        [
            tidb_log::Field::new("old version", tidb_log::Value::I64(old_ver)),
            tidb_log::Field::new("latest version", tidb_log::Value::I64(new_ver)),
        ]
    };
    for i in 0.. {
        let error = match is_upgrading_cluster_state(s) {
            Ok(is_upgrading) => {
                let [old, latest] = versions();
                logger.info(
                    "get global state",
                    &[
                        category(),
                        old,
                        latest,
                        tidb_log::Field::new("is upgrading state", tidb_log::Value::Bool(is_upgrading)),
                    ],
                );
                return;
            }
            Err(error) => error,
        };

        if now.elapsed() >= timeout {
            let [old, latest] = versions();
            logger.error(
                "get global state failed",
                &[category(), old, latest, tidb_logutil::zap_error(&error)],
            );
            return;
        }
        if i % 25 == 0 {
            let [old, latest] = versions();
            logger.warn(
                "get global state failed",
                &[category(), old, latest, tidb_logutil::zap_error(&error)],
            );
        }
        std::thread::sleep(interval);
    }
}
