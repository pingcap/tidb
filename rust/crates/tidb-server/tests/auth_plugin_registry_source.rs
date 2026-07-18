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

#![allow(missing_docs)]

use tidb_server::{
    AuthPluginAdmission, AuthPluginDescriptor, AuthPluginRegistry, AuthPluginRegistryError,
    ClientPluginSelection, ClientPluginSelectionRequest, DEFAULT_AUTH_PLUGINS,
};

#[test]
fn validation_preserves_go_callback_order_and_errors() {
    // Source: pkg/extension/auth.go:117-142 and
    // pkg/extension/registry_test.go:360-415. Go checks name, duplicate,
    // reserved-name, AuthenticateUser, GenerateAuthString, then
    // ValidateAuthString in that order.
    assert_eq!(
        AuthPluginRegistry::validate([AuthPluginDescriptor::new("")]),
        Err(AuthPluginRegistryError::EmptyName)
    );
    let validated = AuthPluginRegistry::validate([AuthPluginDescriptor::complete("plugin1")])
        .expect("complete metadata validates");
    assert_eq!(
        validated.get("plugin1").map(AuthPluginDescriptor::name),
        Some("plugin1")
    );

    let missing_auth = AuthPluginDescriptor::new("plugin1")
        .with_generate_auth_string()
        .with_validate_auth_string();
    assert_eq!(
        AuthPluginRegistry::validate([missing_auth]),
        Err(AuthPluginRegistryError::MissingAuthenticateUser(
            "plugin1".to_owned()
        ))
    );

    let missing_generate = AuthPluginDescriptor::new("plugin1")
        .with_authenticate_user()
        .with_validate_auth_string();
    assert_eq!(
        AuthPluginRegistry::validate([missing_generate]),
        Err(AuthPluginRegistryError::MissingGenerateAuthString(
            "plugin1".to_owned()
        ))
    );

    let missing_validate = AuthPluginDescriptor::new("plugin1")
        .with_authenticate_user()
        .with_generate_auth_string();
    assert_eq!(
        AuthPluginRegistry::validate([missing_validate]),
        Err(AuthPluginRegistryError::MissingValidateAuthString(
            "plugin1".to_owned()
        ))
    );
}

#[test]
fn duplicate_and_reserved_names_are_rejected_before_callbacks() {
    // Source: pkg/extension/auth.go:125-140 and
    // pkg/extension/registry_test.go:417-465. A duplicate or built-in name
    // is rejected before callback presence is inspected.
    let duplicate = [
        AuthPluginDescriptor::complete("plugin1"),
        AuthPluginDescriptor::complete("plugin1"),
    ];
    assert_eq!(
        AuthPluginRegistry::validate(duplicate),
        Err(AuthPluginRegistryError::DuplicateName("plugin1".to_owned()))
    );

    let reserved = AuthPluginDescriptor::new(DEFAULT_AUTH_PLUGINS[0]);
    assert_eq!(
        AuthPluginRegistry::validate([reserved]),
        Err(AuthPluginRegistryError::ReservedName(
            DEFAULT_AUTH_PLUGINS[0].to_owned()
        ))
    );
}

#[test]
fn client_plugin_mapping_preserves_auth_switch_selection_without_authentication() {
    // Source: pkg/server/conn.go:276-288. LDAP has built-in client mappings;
    // custom plugins use RequiredClientSidePlugin or their own name. This
    // leaf only selects metadata and never verifies a password or performs
    // client I/O.
    let registry = AuthPluginRegistry::validate([
        AuthPluginDescriptor::complete("custom_auth")
            .with_required_client_side_plugin("custom_client"),
        AuthPluginDescriptor::complete("same_name"),
    ])
    .expect("valid custom plugins");

    assert_eq!(
        registry.client_plugin_name("authentication_ldap_sasl"),
        "authentication_ldap_sasl_client"
    );
    assert_eq!(
        registry.client_plugin_name("authentication_ldap_simple"),
        "mysql_clear_password"
    );
    assert_eq!(registry.client_plugin_name("custom_auth"), "custom_client");
    assert_eq!(registry.client_plugin_name("same_name"), "same_name");
    assert_eq!(registry.client_plugin_name("unknown_auth"), "unknown_auth");
}

#[test]
fn plugin_admission_classifies_builtin_custom_and_unknown_names() {
    let registry = AuthPluginRegistry::validate([AuthPluginDescriptor::complete("custom_auth")])
        .expect("valid custom plugin");

    let built_in = registry.admit(DEFAULT_AUTH_PLUGINS[0]);
    assert_eq!(
        built_in,
        AuthPluginAdmission::BuiltIn(DEFAULT_AUTH_PLUGINS[0].to_owned())
    );
    assert!(built_in.is_supported());

    let custom = registry.admit("custom_auth");
    assert!(
        matches!(&custom, AuthPluginAdmission::Custom(descriptor) if descriptor.name() == "custom_auth")
    );
    assert!(custom.is_supported());

    let unknown = registry.admit("future_auth");
    assert_eq!(
        unknown,
        AuthPluginAdmission::Unsupported("future_auth".to_owned())
    );
    assert!(!unknown.is_supported());
}

#[test]
fn client_selection_preserves_fallback_switch_and_legacy_outcomes() {
    // Source: pkg/server/conn.go:952-1047 and pkg/server/conn_test.go:1564-1771.
    // The Go connection path first permits tidb_session_token, then falls
    // back to native password for an empty user-plugin row, maps
    // tidb_auth_token to mysql_clear_password, switches when the advertised
    // names differ, and rejects non-native plugins for legacy clients.
    let registry = AuthPluginRegistry::validate([AuthPluginDescriptor::complete("custom_auth")
        .with_required_client_side_plugin("custom_client")])
    .expect("valid custom plugin");

    assert_eq!(
        registry.select_client_plugin(&ClientPluginSelectionRequest::new(
            "caching_sha2_password",
            "tidb_session_token",
            Some("custom_auth".to_owned()),
            true,
        )),
        ClientPluginSelection::SessionTokenPassthrough
    );
    assert_eq!(
        registry.select_client_plugin(&ClientPluginSelectionRequest::new(
            "caching_sha2_password",
            "caching_sha2_password",
            None,
            true,
        )),
        ClientPluginSelection::NativePasswordFallback {
            client_plugin: "mysql_native_password".to_owned()
        }
    );
    assert_eq!(
        registry.select_client_plugin(&ClientPluginSelectionRequest::new(
            "caching_sha2_password",
            "caching_sha2_password",
            None,
            false,
        )),
        ClientPluginSelection::Keep {
            plugin: "mysql_native_password".to_owned()
        }
    );
    assert_eq!(
        registry.select_client_plugin(&ClientPluginSelectionRequest::new(
            "mysql_native_password",
            "mysql_native_password",
            Some("custom_auth".to_owned()),
            true,
        )),
        ClientPluginSelection::Switch {
            server_plugin: "custom_auth".to_owned(),
            client_plugin: "custom_client".to_owned()
        }
    );
    assert_eq!(
        registry.select_client_plugin(&ClientPluginSelectionRequest::new(
            "mysql_native_password",
            "mysql_native_password",
            Some("tidb_auth_token".to_owned()),
            true,
        )),
        ClientPluginSelection::Switch {
            server_plugin: "mysql_clear_password".to_owned(),
            client_plugin: "mysql_clear_password".to_owned()
        }
    );
    assert_eq!(
        registry.select_client_plugin(&ClientPluginSelectionRequest::new(
            "mysql_native_password",
            "mysql_native_password",
            Some("custom_auth".to_owned()),
            false,
        )),
        ClientPluginSelection::RejectLegacyClient {
            required_plugin: "custom_auth".to_owned()
        }
    );
    assert_eq!(
        registry.select_client_plugin(&ClientPluginSelectionRequest::new(
            "mysql_native_password",
            "mysql_native_password",
            Some("future_auth".to_owned()),
            true,
        )),
        ClientPluginSelection::UnsupportedServerPlugin {
            plugin: "future_auth".to_owned()
        }
    );
}
