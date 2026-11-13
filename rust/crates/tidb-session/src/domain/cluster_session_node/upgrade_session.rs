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

//! Go `runInBootstrapSession`'s session: one internal session of the domain,
//! as `pkg/session/upgrade_def.go` reaches it through `sessionapi.Session`.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_domain_core::sys_session::{stats_session_error, DomainSysSession};
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use super::sessions::cluster_stats_session;
use super::Domain;
use crate::bootstrap::{UpgradeSession, UpgradeStore};

/// Go `createSession(store)` in `runInBootstrapSession`.
struct BootstrapSession<'a> {
    context: Arc<dyn DomainSysSession>,
    store: &'a dyn UpgradeStore,
    state_syncer: &'a dyn tidb_ddl_serverstate::Syncer,
}

impl UpgradeSession for BootstrapSession<'_> {
    fn execute_internal(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, SqlQueryError> {
        let mut result = None;
        cluster_stats_session(&*self.context)
            .with_session(|session| {
                result = Some(session.execute_internal(sql));
                Ok(())
            })
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        result.expect("the session ran the statement")
    }

    fn set_global_sys_var(&mut self, name: &str, value: &str) -> Result<(), String> {
        cluster_stats_session(&*self.context)
            .with_session(|session| {
                tidb_executor::sessionctx::SessionContext::set_global_sys_var(
                    &mut *session.session,
                    name,
                    value.to_owned(),
                )
                .map_err(|error| stats_session_error(format!("{error:?}")))
            })
            .map_err(|error| error.to_string())
    }

    fn get_store(&self) -> &dyn UpgradeStore {
        self.store
    }

    fn state_syncer(&self) -> &dyn tidb_ddl_serverstate::Syncer {
        self.state_syncer
    }
}

/// Go `runInBootstrapSession`'s `upgrade(s)` over a new internal session of
/// the domain.
pub fn run_upgrade(
    domain: &Arc<Domain>,
    store: &dyn UpgradeStore,
    state_syncer: &dyn tidb_ddl_serverstate::Syncer,
) -> Result<(), String> {
    let context = super::open_pool_session(domain, 0)?;
    // Go's bootstrap and upgrade run under `kv.InternalTxnBootstrap`.
    cluster_stats_session(&*context).state.adopt_request_source_type(tidb_txnkv::INTERNAL_TXN_BOOTSTRAP);
    // For the bootstrap SQLs, the following variables should be compatible with old TiDB versions.
    cluster_stats_session(&*context).set_var(
        tidb_vardef::tidb_vars::TIDB_ENABLE_CLUSTERED_INDEX,
        "INT_ONLY",
    );
    let mut session = BootstrapSession {
        context,
        store,
        state_syncer,
    };
    crate::upgrade_run::upgrade(&mut session)
}

/// A single-quoted SQL string literal of `value`.
fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Provisions the node's `--auth-file` accounts (`accounts`, the registry it
/// boots from) through Go's account statements on an internal session, as
/// Go's bootstrap provisions `root@%` through SQL: the file is the whole
/// account set of a node whose store starts empty each boot, so an account
/// it does not list is dropped (`DROP USER`), a listed one that exists takes
/// the file's credential when it holds another (`ALTER USER ... IDENTIFIED
/// WITH ... AS`), and a
/// new one is created, with Go's bootstrap `root@%` grant when the file
/// makes it the superuser.
/// Each statement's commit reloads the privilege cache from the tables.
pub fn seed_auth_file_accounts(
    domain: &Arc<Domain>,
    accounts: &tidb_privilege::PrivilegeRegistry,
) -> Result<(), String> {
    // A statement runs on a SQL worker's stack, never the caller's.
    std::thread::scope(|scope| {
        tidb_worker_stack::sql_worker_thread("tidb-auth-file-seed")
            .spawn_scoped(scope, || seed_auth_file_accounts_on_worker(domain, accounts))
            .map_err(|error| error.to_string())?
            .join()
            .map_err(|_| "the account seeding thread panicked".to_owned())?
    })
}

fn seed_auth_file_accounts_on_worker(
    domain: &Arc<Domain>,
    accounts: &tidb_privilege::PrivilegeRegistry,
) -> Result<(), String> {
    let desired = tidb_privilege::load::cluster_image_from_registry(accounts).users;
    let context = super::open_pool_session(domain, 0)?;
    let run = |sql: &str| -> Result<Vec<Vec<Datum>>, String> {
        let mut result = None;
        cluster_stats_session(&*context)
            .with_session(|session| {
                result = Some(session.execute_internal(sql));
                Ok(())
            })
            .map_err(|error| error.to_string())?;
        result
            .expect("the session ran the statement")
            .map_err(|error| error.message)
    };
    let text = |datum: &Datum| match datum {
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        other => format!("{other:?}"),
    };
    let existing: Vec<(String, String, String, String)> =
        run("SELECT user, host, authentication_string, plugin FROM mysql.user")?
            .iter()
            .map(|row| (text(&row[0]), text(&row[1]), text(&row[2]), text(&row[3])))
            .collect();
    let account = |user: &str, host: &str| format!("{}@{}", quoted(user), quoted(host));
    for (user, host, _, _) in &existing {
        if !desired.iter().any(|wanted| &wanted.user == user && &wanted.host == host) {
            run(&format!("DROP USER {}", account(user, host)))?;
        }
    }
    for wanted in &desired {
        let identified = format!(
            "IDENTIFIED WITH {} AS {}",
            quoted(&wanted.plugin),
            quoted(&wanted.authentication_string)
        );
        let target = account(&wanted.user, &wanted.host);
        let stored = existing.iter().find(|(user, host, _, _)| user == &wanted.user && host == &wanted.host);
        if let Some((_, _, auth_string, plugin)) = stored {
            // An account that already holds the file's credential is left as
            // it is, so a node whose file agrees with the bootstrap changes
            // nothing.
            if auth_string != &wanted.authentication_string || plugin != &wanted.plugin {
                run(&format!("ALTER USER {target} {identified}"))?;
            }
        } else {
            run(&format!("CREATE USER {target} {identified}"))?;
            if !wanted.privileges.is_empty() {
                run(&format!("GRANT ALL PRIVILEGES ON *.* TO {target} WITH GRANT OPTION"))?;
            }
        }
    }
    Ok(())
}
