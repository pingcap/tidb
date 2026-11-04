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

//! Go's store bootstrap (`pkg/session/bootstrap.go`) runs on the node
//! (`crate::mysql_bootstrap`): a store is bootstrapped once, before any
//! session opens on it, and every session consumes the bootstrapped
//! infoschema as Go's `createSessionWithOpt` does. This module holds the
//! helpers of `bootstrap.go` the upgrade (`crate::upgrade_def`) runs on.

use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::Datum;
use tidb_sqlescape::SqlArg;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_store_driver_txn::meta_txn::{MetaTxnError, MetaTxnStorage};
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

use crate::mysql_bootstrap::TIDB_SERVER_VERSION_VAR;
use crate::upgrade_def::{get_string, is_null, CURRENT_BOOTSTRAP_VERSION};

/// Go's `logutil.BgLogger().Fatal` message, answered instead of ending the
/// process: the startup that ran the step fails with it.
pub type UpgradeResult = Result<(), String>;

/// Go `sessionapi.Session` as the bootstrap and upgrade functions reach it.
pub trait UpgradeSession {
    /// Go `ExecuteInternal` of one statement with no arguments: the rows of a
    /// result set, none otherwise.
    fn execute_internal(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, SqlQueryError>;
    /// Go `GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar`.
    fn set_global_sys_var(&mut self, name: &str, value: &str) -> Result<(), String>;
    /// Go `GetStore()`.
    fn get_store(&self) -> &dyn UpgradeStore;
    /// Go `domain.GetDomain(s).DDL().StateSyncer()`.
    fn state_syncer(&self) -> &dyn tidb_ddl_serverstate::Syncer;
}

/// Go `kv.Storage` as the bootstrap and upgrade reach it: each method is one
/// `kv.RunInNewTxn` over a meta mutator.
pub trait UpgradeStore {
    /// Go `Mutator.GetMetadataLock`; `None` is Go's `isNull`.
    fn get_metadata_lock(&self) -> Result<Option<bool>, String>;
    /// Go `Mutator.SetMetadataLock`.
    fn set_metadata_lock(&self, enable: bool) -> Result<(), String>;
    /// Go `Mutator.GetDDLTableVersion`.
    fn get_ddl_table_version(&self) -> Result<i64, String>;
    /// Go `Reader.GetBootstrapVersion`.
    fn get_bootstrap_version(&self) -> Result<i64, String>;
    /// Go `Mutator.FinishBootstrap`.
    fn finish_bootstrap(&self, version: i64) -> Result<(), String>;
    /// Go `Storage.GetClusterID`.
    fn get_cluster_id(&self) -> u64;
}

/// Go `kv.Storage` over this node's transaction opener.
pub struct TxnUpgradeStore<'opener, S: StoreClient> {
    opener: &'opener RealOptimisticTransactionOpener<S>,
    timeout: Duration,
}

impl<'opener, S: StoreClient> TxnUpgradeStore<'opener, S> {
    /// The store every meta transaction of the upgrade runs on.
    pub fn new(opener: &'opener RealOptimisticTransactionOpener<S>, timeout: Duration) -> Self {
        Self { opener, timeout }
    }

    fn run_in_new_txn(
        &self,
        source_type: &str,
        retryable: bool,
        mut body: impl FnMut(
            &tidb_meta::transaction::Mutator<&mut tidb_store_driver_txn::meta_txn::MetaTxn<S>>,
        ) -> Result<(), MetaTxnError>,
    ) -> Result<(), String> {
        let mut storage = MetaTxnStorage::new(self.opener, self.timeout);
        let context = tidb_txnkv::RunInNewTxnContext {
            request_source: Some(tidb_txnkv::RequestSource {
                internal: true,
                source_type: source_type.to_owned(),
                explicit_source_type: String::new(),
            }),
        };
        tidb_txnkv::run_in_new_txn(&context, &mut storage, retryable, |transaction| {
            let mutator = tidb_meta::transaction::Mutator::new(&mut *transaction);
            body(&mutator)
        })
        .map_err(|error| error.to_string())
    }
}

impl<S: StoreClient> UpgradeStore for TxnUpgradeStore<'_, S> {
    fn get_metadata_lock(&self) -> Result<Option<bool>, String> {
        let mut enable = None;
        self.run_in_new_txn(tidb_txnkv::INTERNAL_TXN_DDL, true, |mutator| {
            enable = mutator.get_metadata_lock().map_err(MetaTxnError::from)?;
            Ok(())
        })?;
        Ok(enable)
    }

    fn set_metadata_lock(&self, enable: bool) -> Result<(), String> {
        self.run_in_new_txn(tidb_txnkv::INTERNAL_TXN_DDL, true, |mutator| {
            mutator.set_metadata_lock(enable).map_err(MetaTxnError::from)
        })
    }

    fn get_ddl_table_version(&self) -> Result<i64, String> {
        let mut transaction = self.opener.begin().map_err(|error| error.to_string())?;
        let version = {
            let mut snapshot =
                tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot::new(
                    &mut transaction,
                    self.timeout,
                );
            crate::mysql_bootstrap::get_ddl_table_version(&mut snapshot)
                .map_err(|error| error.to_string())?
        };
        transaction
            .finish_without_writes()
            .map_err(|error| error.to_string())?;
        Ok(version)
    }

    fn get_bootstrap_version(&self) -> Result<i64, String> {
        let mut version = 0;
        self.run_in_new_txn(tidb_txnkv::INTERNAL_TXN_BOOTSTRAP, false, |mutator| {
            version = mutator.get_bootstrap_version().map_err(MetaTxnError::from)?;
            Ok(())
        })?;
        Ok(version)
    }

    fn finish_bootstrap(&self, version: i64) -> Result<(), String> {
        self.run_in_new_txn(tidb_txnkv::INTERNAL_TXN_BOOTSTRAP, true, |mutator| {
            mutator.finish_bootstrap(version).map_err(MetaTxnError::from)
        })
    }

    fn get_cluster_id(&self) -> u64 {
        self.opener.cluster_id()
    }
}

/// Go `tidbDefMemoryQuotaQuery`: the variable name in mysql.tidb table and it
/// records the default value of mem-quota-query when upgrade from v3.0.x to
/// v4.0.9+.
pub(crate) const TIDB_DEF_MEMORY_QUOTA_QUERY: &str = "default_memory_quota_query";
/// Go `tidbDefOOMAction`: the variable name in mysql.tidb table and it records
/// the default value of oom-action when upgrade from v3.0.x to v4.0.11+.
const TIDB_DEF_OOM_ACTION: &str = "default_oom_action";

/// Go `bootstrapOwnerKey` is the key used by ddl owner mutex during boostrap.
pub const BOOTSTRAP_OWNER_KEY: &str = "/tidb/distributeDDLOwnerLock/";

/// Go `internalSQLTimeout`: DDL owner key's expired time is
/// ManagerSessionTTL seconds, we should wait the time and give more time to
/// have a chance to finish it.
pub(crate) fn internal_sql_timeout() -> Duration {
    let ttl = tidb_owner::MANAGER_SESSION_TTL.load(std::sync::atomic::Ordering::Acquire);
    Duration::from_secs((ttl + 15).max(0) as u64)
}

/// Go `ExecuteInternal(ctx, sql, args...)`: with arguments the statement is
/// `sqlescape.EscapeSQL`'d first (`ParseWithParams`).
pub(crate) fn execute_internal(
    s: &mut dyn UpgradeSession,
    sql: &str,
    args: &[SqlArg<'_>],
) -> Result<Vec<Vec<Datum>>, SqlQueryError> {
    if args.is_empty() {
        return s.execute_internal(sql);
    }
    let sql = tidb_sqlescape::escape_sql_text(sql, args)
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    s.execute_internal(&sql)
}

/// Go `getTiDBVar` gets variable value from mysql.tidb table; `None` is Go's
/// `isNull`.
pub(crate) fn get_tidb_var(
    s: &mut dyn UpgradeSession,
    name: &str,
) -> Result<Option<String>, SqlQueryError> {
    let rows = execute_internal(
        s,
        "SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME= %?",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"tidb"),
            SqlArg::String(name.as_bytes()),
        ],
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    if is_null(row, 0) {
        return Ok(None);
    }
    Ok(Some(get_string(row, 0)))
}

/// Go `acquireLock`: the etcd mutex the upgrading node holds; without etcd
/// (uni-store) there is nothing to hold.
pub fn acquire_lock(
    etcd_cli: Option<Arc<dyn tidb_owner::OwnerStore>>,
) -> Result<Box<dyn FnOnce() + Send>, String> {
    let Some(etcd_cli) = etcd_cli else {
        // Special handling for test.
        tidb_logutil::bg_logger().warn("skip acquire ddl owner lock for uni-store", &[]);
        return Ok(Box::new(|| {
            // do nothing
        }));
    };
    tidb_owner::acquire_distributed_lock(
        &tidb_owner::Context::background(),
        etcd_cli,
        BOOTSTRAP_OWNER_KEY,
        10,
    )
}

/// Go `initGlobalVariableIfNotExists` initialize a global variable with
/// specific val if it does not exist.
pub(crate) fn init_global_variable_if_not_exists(
    s: &mut dyn UpgradeSession,
    name: &str,
    val: SqlArg<'_>,
) -> UpgradeResult {
    let rows = execute_internal(
        s,
        "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(name.as_bytes()),
        ],
    )
    .map_err(|error| error.error_string())?;
    if !rows.is_empty() {
        return Ok(());
    }

    must_execute(
        s,
        "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(name.as_bytes()),
            val,
        ],
    )
}

/// Go `writeOOMAction`.
pub(crate) fn write_oom_action(s: &mut dyn UpgradeSession) -> UpgradeResult {
    let comment = "oom-action is `log` by default in v3.0.x, `cancel` by default in v4.0.11+";
    let log = tidb_vardef::global_sysvar_initial::OOM_ACTION_LOG;
    must_execute(
        s,
        "INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"tidb"),
            SqlArg::String(TIDB_DEF_OOM_ACTION.as_bytes()),
            SqlArg::String(log.as_bytes()),
            SqlArg::String(comment.as_bytes()),
            SqlArg::String(log.as_bytes()),
        ],
    )
}

/// Go `updateBootstrapVer` updates bootstrap version variable in mysql.TiDB
/// table.
pub(crate) fn update_bootstrap_ver(s: &mut dyn UpgradeSession) -> UpgradeResult {
    // Update bootstrap version.
    must_execute(
        s,
        r#"INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?"#,
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"tidb"),
            SqlArg::String(TIDB_SERVER_VERSION_VAR.as_bytes()),
            SqlArg::Signed(CURRENT_BOOTSTRAP_VERSION),
            SqlArg::Signed(CURRENT_BOOTSTRAP_VERSION),
        ],
    )
}

/// Go `getBootstrapVersion` gets bootstrap version from mysql.tidb table.
pub(crate) fn get_bootstrap_version(s: &mut dyn UpgradeSession) -> Result<i64, String> {
    let Some(s_val) = get_tidb_var(s, TIDB_SERVER_VERSION_VAR).map_err(|error| error.error_string())?
    else {
        return Ok(0);
    };
    s_val
        .parse::<i64>()
        .map_err(|error| format!("strconv.ParseInt: parsing {s_val:?}: {error}"))
}

/// Go `mustExecute`.
pub(crate) fn must_execute(
    s: &mut dyn UpgradeSession,
    sql: &str,
    args: &[SqlArg<'_>],
) -> UpgradeResult {
    execute_internal(s, sql, args)
        .map(|_| ())
        .map_err(|error| format!("mustExecute error: {}", error.error_string()))
}

/// Go `oldPasswordUpgrade` upgrade password to MySQL compatible format.
pub(crate) fn old_password_upgrade(pass: &str) -> Result<String, String> {
    if pass.len() % 2 != 0 {
        return Err("encoding/hex: odd length hex string".to_owned());
    }
    let hash1 = (0..pass.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&pass[i..i + 2], 16)
                .map_err(|_| format!("encoding/hex: invalid byte: {:?}", &pass[i..i + 2]))
        })
        .collect::<Result<Vec<u8>, String>>()?;

    let hash2 = tidb_parser::auth::sha1_hash(&hash1);
    let newpass = format!(
        "*{}",
        hash2.iter().map(|byte| format!("{byte:02X}")).collect::<String>()
    );
    Ok(newpass)
}

#[cfg(test)]
mod tests {
    use crate::Session;

    #[test]
    fn old_password_upgrade_follows_go() {
        let pwd = "abc";
        let oldpwd = tidb_parser::auth::sha1_hash(pwd.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>();
        let newpwd = super::old_password_upgrade(&oldpwd).unwrap();
        assert_eq!(newpwd, "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E");
    }

    /// A classic-kernel store has Go's 58 bootstrap jobs in its DDL history
    /// (jobs 3 .. 116) and its next user table is 117 with job 118.
    #[test]
    fn a_classic_store_has_the_bootstrap_jobs_in_its_history() {
        if tidb_config_kerneltype::is_next_gen() {
            return;
        }
        let mut session = Session::new();
        let text = crate::tests_support::row_text;
        let jobs = text(session.run("ADMIN SHOW DDL JOBS 100"));
        assert_eq!(jobs.len(), 58);
        assert_eq!(
            (jobs[0][0].as_str(), jobs[0][3].as_str()),
            ("116", "alter resource group")
        );
        assert_eq!((jobs[57][0].as_str(), jobs[57][1].as_str()), ("3", "sys"));
        session.run("CREATE TABLE test.layout_t (a INT)").unwrap();
        let newest = text(session.run("ADMIN SHOW DDL JOBS 1"));
        assert_eq!(newest[0][0], "118");
        assert_eq!(newest[0][6], "117");
    }
}
