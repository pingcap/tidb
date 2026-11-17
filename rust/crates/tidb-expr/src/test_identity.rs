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

//! A test session's `SessionVars` as Go `sessionexpr` hands them to
//! builtins: the current-user and session-vars providers.

use std::sync::{Arc, Mutex};

use tidb_parser::auth::{RoleIdentity, UserIdentity};

/// The session variables a test session's builtins read.
#[derive(Clone, Default)]
pub(crate) struct TestSession {
    /// The matched account, `user@host`.
    pub(crate) current_user: Option<String>,
    /// The login, `user@host`; defaults to the account.
    pub(crate) login_user: Option<String>,
    /// The active roles, `(user, host)`.
    pub(crate) roles: Vec<(String, String)>,
    /// Go `SessionVars.ConnectionID`.
    pub(crate) connection_id: u64,
    /// Go `SessionVars.LastFoundRows`.
    pub(crate) last_found_rows: u64,
    /// Go `StmtCtx.PrevAffectedRows`.
    pub(crate) prev_affected_rows: i64,
    /// Go `StmtCtx.PrevLastInsertID`.
    pub(crate) prev_last_insert_id: u64,
    /// What `SetLastInsertID` wrote last.
    pub(crate) last_insert_id: Arc<Mutex<Option<u64>>>,
    /// Every `SetLastInsertID` call, in order.
    pub(crate) last_insert_id_calls: Arc<Mutex<Vec<u64>>>,
    /// Go `getHintResourceGroupName`.
    pub(crate) resource_group_name: String,
}

#[derive(Clone)]
struct Vars {
    session: TestSession,
    user: Option<Arc<UserIdentity>>,
    roles: Vec<Arc<RoleIdentity>>,
}

impl tidb_expropt::SessionVars for Vars {
    fn user(&self) -> Option<Arc<UserIdentity>> {
        self.user.clone()
    }

    fn active_roles(&self) -> Vec<Arc<RoleIdentity>> {
        self.roles.clone()
    }

    fn connection_id(&self) -> u64 {
        self.session.connection_id
    }

    fn last_found_rows(&self) -> u64 {
        self.session.last_found_rows
    }

    fn prev_affected_rows(&self) -> i64 {
        self.session.prev_affected_rows
    }

    fn prev_last_insert_id(&self) -> u64 {
        self.session.prev_last_insert_id
    }

    fn set_last_insert_id(&self, id: u64) {
        *self.session.last_insert_id.lock().unwrap() = Some(id);
        self.session.last_insert_id_calls.lock().unwrap().push(id);
    }

    fn hint_resource_group_name(&self) -> String {
        self.session.resource_group_name.clone()
    }
}

impl tidb_expropt::SessionVarsProvider for Vars {
    fn get_session_vars(&self) -> Arc<dyn tidb_expropt::SessionVars> {
        Arc::new(self.clone())
    }
}

impl TestSession {
    /// The session's providers. An unmatched login has empty
    /// `AuthUsername` / `AuthHostname`; there is no user when neither
    /// identity is given.
    pub(crate) fn props(&self) -> tidb_expropt::OptionalEvalPropProviders {
        let split = |identity: &str| {
            let (user, host) = identity.rsplit_once('@').unwrap_or((identity, ""));
            (user.to_owned(), host.to_owned())
        };
        let current = self.current_user.as_deref();
        let user = self.login_user.as_deref().or(current).map(|login| {
            let (auth_username, auth_hostname) = current.map(split).unwrap_or_default();
            let (username, hostname) = split(login);
            Arc::new(UserIdentity { username, hostname, auth_username, auth_hostname, ..UserIdentity::default() })
        });
        let roles: Vec<Arc<RoleIdentity>> = self
            .roles
            .iter()
            .map(|(username, hostname)| Arc::new(RoleIdentity { username: username.clone(), hostname: hostname.clone() }))
            .collect();
        let vars = Vars { session: self.clone(), user: user.clone(), roles: roles.clone() };
        let mut props = tidb_expropt::OptionalEvalPropProviders::new();
        props.add(Arc::new(tidb_expropt::CurrentUserPropProvider::new(move || (user.clone(), roles.clone()))));
        props.add(Arc::new(tidb_expropt::new_session_vars_provider(Arc::new(vars))));
        props
    }
}

/// [`TestSession::props`] of a session that matched the account `current`
/// from the login `login` with `roles` active.
pub(crate) fn identity_props(
    current: Option<&str>,
    login: Option<&str>,
    roles: &[(&str, &str)],
) -> tidb_expropt::OptionalEvalPropProviders {
    TestSession {
        current_user: current.map(str::to_owned),
        login_user: login.map(str::to_owned),
        roles: roles.iter().map(|(user, host)| ((*user).to_owned(), (*host).to_owned())).collect(),
        ..TestSession::default()
    }
    .props()
}
