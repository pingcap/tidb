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

//! Go `simple.go`.

use std::sync::LazyLock;

use crate::ldap_common::LdapAuthImpl;

/// Go `ldapSimplAuthImpl`.
#[derive(Default)]
pub struct LdapSimpleAuthImpl {
    inner: LdapAuthImpl,
}

impl std::ops::Deref for LdapSimpleAuthImpl {
    type Target = LdapAuthImpl;

    fn deref(&self) -> &LdapAuthImpl {
        &self.inner
    }
}

impl LdapSimpleAuthImpl {
    /// Go `AuthLDAPSimple`: authenticates the user through an LDAP simple
    /// bind. `password` is expected to be a NUL-terminated string.
    ///
    /// # Errors
    ///
    /// The password, the user search, the connection or the bind failed.
    pub fn auth_ldap_simple(
        &self,
        user_name: &str,
        dn: &str,
        password: &[u8],
    ) -> Result<(), String> {
        let state = self.inner.read();
        let Some((0, password)) = password.split_last() else {
            return Err("invalid password".to_owned());
        };
        let password_str = String::from_utf8_lossy(password);

        let dn = if dn.is_empty() {
            state.search_user(user_name)?
        } else {
            state.canonicalize_dn(user_name, dn)
        };

        let mut ldap_conn = state
            .get_connection()
            .map_err(|error| format!("create LDAP connection: {error}"))?;
        let result = ldap_conn
            .bind(&dn, &password_str)
            .map_err(|error| format!("bind LDAP: {error}"));
        state.put_connection(ldap_conn);
        result
    }
}

/// Go `LDAPSimpleAuthImpl`: authentication with an LDAP clear-text password.
pub static LDAP_SIMPLE_AUTH_IMPL: LazyLock<LdapSimpleAuthImpl> =
    LazyLock::new(LdapSimpleAuthImpl::default);
