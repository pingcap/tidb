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

//! A test session's identity as Go `sessionexpr` installs it: the
//! current-user and session-vars providers over `SessionVars.User` and
//! `ActiveRoles`.

use std::sync::Arc;

use tidb_parser::auth::{RoleIdentity, UserIdentity};

#[derive(Clone)]
struct Vars {
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
}

impl tidb_expropt::SessionVarsProvider for Vars {
    fn get_session_vars(&self) -> Arc<dyn tidb_expropt::SessionVars> {
        Arc::new(self.clone())
    }
}

/// The providers of a session that matched the account `current` from the
/// login `login` (each `user@host`; the login defaults to the account, and
/// an unmatched login has empty `AuthUsername` / `AuthHostname`) with `roles`
/// active; no user when neither is given.
pub(crate) fn identity_props(
    current: Option<&str>,
    login: Option<&str>,
    roles: &[(&str, &str)],
) -> tidb_expropt::OptionalEvalPropProviders {
    let split = |identity: &str| {
        let (user, host) = identity.rsplit_once('@').unwrap_or((identity, ""));
        (user.to_owned(), host.to_owned())
    };
    let user = login.or(current).map(|login| {
        let (auth_username, auth_hostname) = current.map(split).unwrap_or_default();
        let (username, hostname) = split(login);
        Arc::new(UserIdentity { username, hostname, auth_username, auth_hostname, ..UserIdentity::default() })
    });
    let roles: Vec<Arc<RoleIdentity>> = roles
        .iter()
        .map(|(username, hostname)| Arc::new(RoleIdentity { username: (*username).to_owned(), hostname: (*hostname).to_owned() }))
        .collect();
    let vars = Vars { user: user.clone(), roles: roles.clone() };
    let mut props = tidb_expropt::OptionalEvalPropProviders::new();
    props.add(Arc::new(tidb_expropt::CurrentUserPropProvider::new(move || (user.clone(), roles.clone()))));
    props.add(Arc::new(tidb_expropt::new_session_vars_provider(Arc::new(vars))));
    props
}
