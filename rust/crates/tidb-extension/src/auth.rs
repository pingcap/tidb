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

//! Go `pkg/extension/auth.go`.

use std::collections::HashSet;
use std::sync::Arc;
use tidb_exprctx::EvalError;
use tidb_mysql::PrivilegeType;
use tidb_parser::auth::RoleIdentity;
use tidb_variable::vars::TlsConnectionState;

/// Go `privilege/conn.AuthConn`: the authentication exchange on this connection.
pub trait AuthConn {
    /// Go `AuthConn.WriteAuthMoreData`.
    fn write_auth_more_data(&mut self, data: &[u8]) -> Result<(), String>;
    /// Go `AuthConn.ReadPacket`.
    fn read_packet(&mut self) -> Result<Vec<u8>, String>;
    /// Go `AuthConn.Flush`.
    fn flush(&mut self) -> Result<(), String>;
}

/// Go `AuthenticateRequest`. Deliberately has no Debug implementation: credentials are secret.
pub struct AuthenticateRequest<'a, 'b> {
    /// Go `User`.
    pub user: &'a str,
    /// Go `StoredAuthString`.
    pub stored_auth_string: &'a str,
    /// Go `InputAuthString`.
    pub input_auth_string: &'a [u8],
    /// Go `Salt`.
    pub salt: &'a [u8],
    /// Go `ConnState`.
    pub conn_state: Option<&'a TlsConnectionState>,
    /// Go `AuthConn`.
    pub auth_conn: Option<&'b mut dyn AuthConn>,
}

/// Go `VerifyStaticPrivRequest`.
pub struct VerifyStaticPrivRequest<'a> {
    /// Go `User`.
    pub user: &'a str,
    /// Go `Host`.
    pub host: &'a str,
    /// Go `DB`.
    pub db: &'a str,
    /// Go `Table`.
    pub table: &'a str,
    /// Go `Column`.
    pub column: &'a str,
    /// Go `StaticPriv`.
    pub static_priv: PrivilegeType,
    /// Go `ConnState`.
    pub conn_state: Option<&'a TlsConnectionState>,
    /// Go `ActiveRoles`.
    pub active_roles: &'a [RoleIdentity],
}

/// Go `VerifyDynamicPrivRequest`.
pub struct VerifyDynamicPrivRequest<'a> {
    /// Go `User`.
    pub user: &'a str,
    /// Go `Host`.
    pub host: &'a str,
    /// Go `DynamicPriv`.
    pub dynamic_priv: &'a str,
    /// Go `ConnState`.
    pub conn_state: Option<&'a TlsConnectionState>,
    /// Go `ActiveRoles`.
    pub active_roles: &'a [RoleIdentity],
    /// Go `WithGrant`.
    pub with_grant: bool,
}

/// Go `AuthPlugin`.
#[derive(Clone, Default)]
pub struct AuthPlugin {
    /// Go `Name`.
    pub name: String,
    /// Go `RequiredClientSidePlugin`.
    pub required_client_side_plugin: String,
    /// Go `AuthenticateUser`.
    pub authenticate_user:
        Option<Arc<dyn Fn(AuthenticateRequest<'_, '_>) -> Result<(), String> + Send + Sync>>,
    /// Go `GenerateAuthString`.
    pub generate_auth_string: Option<Arc<dyn Fn(&str) -> (String, bool) + Send + Sync>>,
    /// Go `ValidateAuthString`.
    pub validate_auth_string: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
    /// Go `VerifyPrivilege`.
    pub verify_privilege: Option<Arc<dyn Fn(VerifyStaticPrivRequest<'_>) -> bool + Send + Sync>>,
    /// Go `VerifyDynamicPrivilege`.
    pub verify_dynamic_privilege:
        Option<Arc<dyn Fn(VerifyDynamicPrivRequest<'_>) -> bool + Send + Sync>>,
}

impl std::fmt::Debug for AuthPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthPlugin")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Go `validateAuthPlugin`.
pub(crate) fn validate_auth_plugin(plugins: &[AuthPlugin]) -> Result<(), EvalError> {
    let mut names = HashSet::new();
    for p in plugins {
        let name = &p.name;
        let error = if name.is_empty() {
            Some(format!("auth plugin name cannot be empty for {name}"))
        } else if !names.insert(name) {
            Some(format!(
                "auth plugin name {name} has already been registered"
            ))
        } else if tidb_mysql::consts::DEFAULT_AUTH_PLUGINS.contains(&name.as_str()) {
            Some(format!(
                "auth plugin name {name} is a reserved name for default auth plugins"
            ))
        } else if p.authenticate_user.is_none() {
            Some(format!(
                "auth plugin AuthenticateUser function cannot be nil for {name}"
            ))
        } else if p.generate_auth_string.is_none() {
            Some(format!(
                "auth plugin GenerateAuthString function cannot be nil for {name}"
            ))
        } else if p.validate_auth_string.is_none() {
            Some(format!(
                "auth plugin ValidateAuthString function cannot be nil for {name}"
            ))
        } else {
            None
        };
        if let Some(error) = error {
            return Err(EvalError::Raw(error));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `pkg/extension/registry_test.go:TestAuthPluginValidation`.
    #[test]
    fn test_auth_plugin_validation() {
        let complete = AuthPlugin {
            name: "plugin1".into(),
            authenticate_user: Some(Arc::new(|_| Ok(()))),
            generate_auth_string: Some(Arc::new(|s| (s.into(), true))),
            validate_auth_string: Some(Arc::new(|_| true)),
            ..Default::default()
        };
        let cases = [
            (vec![AuthPlugin::default()], "auth plugin name cannot be empty for "),
            (vec![AuthPlugin { authenticate_user: None, ..complete.clone() }], "auth plugin AuthenticateUser function cannot be nil for plugin1"),
            (vec![AuthPlugin { validate_auth_string: None, ..complete.clone() }], "auth plugin ValidateAuthString function cannot be nil for plugin1"),
            (vec![AuthPlugin { generate_auth_string: None, ..complete.clone() }], "auth plugin GenerateAuthString function cannot be nil for plugin1"),
            (vec![complete.clone(), complete.clone()], "auth plugin name plugin1 has already been registered"),
            (vec![AuthPlugin { name: "mysql_native_password".into(), ..complete.clone() }], "auth plugin name mysql_native_password is a reserved name for default auth plugins"),
        ];
        for (plugins, expected) in cases {
            let option = crate::with_custom_auth_plugins(plugins);
            let result =
                crate::manifest::new_manifest_with_setup("test", &|| Ok(vec![option.clone()]));
            assert_eq!(result.err(), Some(EvalError::Raw(expected.into())));
        }
        assert!(validate_auth_plugin(&[complete]).is_ok());
    }
}
