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

//! Validation and client-plugin selection for extension authentication plugins.
//!
//! This is the metadata half of TiDB's Go `AuthPlugin` extension boundary.  Go
//! validates every custom plugin before setup (`pkg/extension/auth.go:117-142`)
//! and later maps a server-side plugin to the client-side plugin used by the
//! auth-switch packet (`pkg/server/conn.go:276-288`).  Rust keeps those rules
//! explicit without pretending to execute an extension callback: password
//! hashing, password verification, TLS state, and client I/O remain owned by
//! later server/privilege layers.

use std::collections::HashSet;
use std::fmt;

/// Authentication plugins built into TiDB and therefore unavailable to an
/// extension plugin with the same name.
pub const DEFAULT_AUTH_PLUGINS: &[&str] = &[
    AUTH_NATIVE_PASSWORD,
    AUTH_CACHING_SHA2_PASSWORD,
    "tidb_sm3_password",
    "authentication_ldap_sasl",
    "authentication_ldap_simple",
    "auth_socket",
    "tidb_session_token",
    "tidb_auth_token",
    "mysql_clear_password",
];

/// Built-in plugin names used by the connection-selection boundary.
pub const AUTH_NATIVE_PASSWORD: &str = "mysql_native_password";
pub const AUTH_CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";
pub const AUTH_TIDB_AUTH_TOKEN: &str = "tidb_auth_token";
pub const AUTH_TIDB_SESSION_TOKEN: &str = "tidb_session_token";
pub const AUTH_MYSQL_CLEAR_PASSWORD: &str = "mysql_clear_password";

/// Metadata needed to validate one custom authentication plugin.
///
/// The three `has_*` fields represent the non-optional Go callbacks.  They are
/// deliberately booleans rather than Rust function pointers: registering a
/// callback would claim an executable authentication implementation before the
/// privilege and extension owners have been ported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthPluginDescriptor {
    name: String,
    required_client_side_plugin: Option<String>,
    has_authenticate_user: bool,
    has_generate_auth_string: bool,
    has_validate_auth_string: bool,
}

impl AuthPluginDescriptor {
    /// Creates a descriptor with no callbacks registered yet.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            required_client_side_plugin: None,
            has_authenticate_user: false,
            has_generate_auth_string: false,
            has_validate_auth_string: false,
        }
    }

    /// Creates a complete metadata descriptor for tests or a future adapter.
    ///
    /// This only records that the Go callback slots exist; it does not perform
    /// password hashing or verification.
    #[must_use]
    pub fn complete(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            required_client_side_plugin: None,
            has_authenticate_user: true,
            has_generate_auth_string: true,
            has_validate_auth_string: true,
        }
    }

    /// Sets the client-side plugin requested by the server-side plugin.
    #[must_use]
    pub fn with_required_client_side_plugin(mut self, plugin: impl Into<String>) -> Self {
        self.required_client_side_plugin = Some(plugin.into());
        self
    }

    /// Marks the `AuthenticateUser` callback as present.
    #[must_use]
    pub const fn with_authenticate_user(mut self) -> Self {
        self.has_authenticate_user = true;
        self
    }

    /// Marks the `GenerateAuthString` callback as present.
    #[must_use]
    pub const fn with_generate_auth_string(mut self) -> Self {
        self.has_generate_auth_string = true;
        self
    }

    /// Marks the `ValidateAuthString` callback as present.
    #[must_use]
    pub const fn with_validate_auth_string(mut self) -> Self {
        self.has_validate_auth_string = true;
        self
    }

    /// Returns the server-side plugin name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the optional client-side plugin override.
    #[must_use]
    pub fn required_client_side_plugin(&self) -> Option<&str> {
        self.required_client_side_plugin.as_deref()
    }
}

/// A validation failure corresponding to one branch of Go's
/// `validateAuthPlugin` function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthPluginRegistryError {
    /// The plugin name was empty.
    EmptyName,
    /// A plugin name occurred more than once.
    DuplicateName(String),
    /// A custom plugin attempted to claim a built-in name.
    ReservedName(String),
    /// The required authentication callback was not supplied.
    MissingAuthenticateUser(String),
    /// The required password-string generation callback was not supplied.
    MissingGenerateAuthString(String),
    /// The required password-string validation callback was not supplied.
    MissingValidateAuthString(String),
}

impl fmt::Display for AuthPluginRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName => formatter.write_str("auth plugin name cannot be empty for "),
            Self::DuplicateName(name) => {
                write!(
                    formatter,
                    "auth plugin name {name} has already been registered"
                )
            }
            Self::ReservedName(name) => write!(
                formatter,
                "auth plugin name {name} is a reserved name for default auth plugins"
            ),
            Self::MissingAuthenticateUser(name) => write!(
                formatter,
                "auth plugin AuthenticateUser function cannot be nil for {name}"
            ),
            Self::MissingGenerateAuthString(name) => write!(
                formatter,
                "auth plugin GenerateAuthString function cannot be nil for {name}"
            ),
            Self::MissingValidateAuthString(name) => write!(
                formatter,
                "auth plugin ValidateAuthString function cannot be nil for {name}"
            ),
        }
    }
}

impl std::error::Error for AuthPluginRegistryError {}

/// Validated custom authentication-plugin metadata.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthPluginRegistry {
    plugins: Vec<AuthPluginDescriptor>,
}

/// Metadata-only admission result for one server-side plugin name.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthPluginAdmission {
    /// A built-in TiDB plugin name.
    BuiltIn(String),
    /// A validated custom plugin descriptor.
    Custom(AuthPluginDescriptor),
    /// No built-in or validated custom plugin owns this name.
    Unsupported(String),
}

impl AuthPluginAdmission {
    /// Whether a built-in or validated custom owner was found.
    #[must_use]
    pub const fn is_supported(&self) -> bool {
        !matches!(self, Self::Unsupported(_))
    }
}

/// Inputs to server-plugin → client-plugin selection during the handshake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientPluginSelectionRequest {
    server_plugin: String,
    client_plugin: String,
    user_plugin: Option<String>,
    client_supports_plugins: bool,
}

impl ClientPluginSelectionRequest {
    /// Retains the three source plugin names and client capability bit.
    #[must_use]
    pub fn new(
        server_plugin: impl Into<String>,
        client_plugin: impl Into<String>,
        user_plugin: Option<String>,
        client_supports_plugins: bool,
    ) -> Self {
        Self {
            server_plugin: server_plugin.into(),
            client_plugin: client_plugin.into(),
            user_plugin,
            client_supports_plugins,
        }
    }
}

/// Source-shaped outcome of client authentication-plugin selection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientPluginSelection {
    /// `tidb_session_token` bypasses stored user-plugin selection.
    SessionTokenPassthrough,
    /// Existing server/client plugin names already agree.
    Keep {
        /// Plugin name retained for the handshake.
        plugin: String,
    },
    /// A client-side auth-switch is required and supported.
    Switch {
        /// Server-side plugin selected for the user.
        server_plugin: String,
        /// Client-side plugin advertised in the switch request.
        client_plugin: String,
    },
    /// No user plugin was stored; native password is the source fallback and
    /// the auth-switch client plugin is always `mysql_native_password`.
    NativePasswordFallback {
        /// Native plugin selected for the auth-switch request.
        client_plugin: String,
    },
    /// An old client cannot switch to the required non-native plugin.
    RejectLegacyClient {
        /// Plugin that the server requires but the client cannot negotiate.
        required_plugin: String,
    },
    /// The user row names no built-in or validated custom plugin.
    UnsupportedServerPlugin {
        /// Unsupported plugin name from the user row.
        plugin: String,
    },
}

impl AuthPluginRegistry {
    /// Validates and owns custom plugin metadata in source order.
    pub fn validate(
        plugins: impl IntoIterator<Item = AuthPluginDescriptor>,
    ) -> Result<Self, AuthPluginRegistryError> {
        let mut names = HashSet::new();
        let mut validated = Vec::new();
        for plugin in plugins {
            let name = plugin.name();
            if name.is_empty() {
                return Err(AuthPluginRegistryError::EmptyName);
            }
            if !names.insert(name.to_owned()) {
                return Err(AuthPluginRegistryError::DuplicateName(name.to_owned()));
            }
            if DEFAULT_AUTH_PLUGINS.contains(&name) {
                return Err(AuthPluginRegistryError::ReservedName(name.to_owned()));
            }
            if !plugin.has_authenticate_user {
                return Err(AuthPluginRegistryError::MissingAuthenticateUser(
                    name.to_owned(),
                ));
            }
            if !plugin.has_generate_auth_string {
                return Err(AuthPluginRegistryError::MissingGenerateAuthString(
                    name.to_owned(),
                ));
            }
            if !plugin.has_validate_auth_string {
                return Err(AuthPluginRegistryError::MissingValidateAuthString(
                    name.to_owned(),
                ));
            }
            validated.push(plugin);
        }
        Ok(Self { plugins: validated })
    }

    /// Returns a validated custom plugin by server-side name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&AuthPluginDescriptor> {
        self.plugins.iter().find(|plugin| plugin.name() == name)
    }

    /// Classifies a server-side plugin name without executing callbacks.
    ///
    /// Unknown names remain an explicit [`AuthPluginAdmission::Unsupported`]
    /// outcome for the privilege/connection owner; this method never turns an
    /// unknown plugin into an authenticated session.
    #[must_use]
    pub fn admit(&self, name: &str) -> AuthPluginAdmission {
        if DEFAULT_AUTH_PLUGINS.contains(&name) {
            AuthPluginAdmission::BuiltIn(name.to_owned())
        } else if let Some(plugin) = self.get(name) {
            AuthPluginAdmission::Custom(plugin.clone())
        } else {
            AuthPluginAdmission::Unsupported(name.to_owned())
        }
    }

    /// Selects the client-side plugin without executing auth callbacks.
    ///
    /// This ports the source `checkAuthPlugin` decision boundary: session-token
    /// passthrough, empty-user-plugin native fallback, TiDB auth-token clear
    /// password mapping, plugin-switch capability checks, and explicit unknown
    /// or legacy-client outcomes. It never writes packets or verifies auth.
    #[must_use]
    pub fn select_client_plugin(
        &self,
        request: &ClientPluginSelectionRequest,
    ) -> ClientPluginSelection {
        if request.client_plugin == AUTH_TIDB_SESSION_TOKEN {
            return ClientPluginSelection::SessionTokenPassthrough;
        }

        let Some(user_plugin) = request
            .user_plugin
            .as_deref()
            .filter(|plugin| !plugin.is_empty())
        else {
            if request.client_plugin == AUTH_NATIVE_PASSWORD {
                return ClientPluginSelection::Keep {
                    plugin: AUTH_NATIVE_PASSWORD.to_owned(),
                };
            }
            if !request.client_supports_plugins {
                // `handleAuthPlugin` forces the response plugin to native for
                // legacy clients after `checkAuthPlugin` returns.
                return ClientPluginSelection::Keep {
                    plugin: AUTH_NATIVE_PASSWORD.to_owned(),
                };
            }
            // Go rewrites resp.AuthPlugin to mysql_native_password and sends
            // the auth-switch using that plugin, regardless of the plugin the
            // client advertised in its initial response.
            return ClientPluginSelection::NativePasswordFallback {
                client_plugin: AUTH_NATIVE_PASSWORD.to_owned(),
            };
        };

        if !self.admit(user_plugin).is_supported() {
            return ClientPluginSelection::UnsupportedServerPlugin {
                plugin: user_plugin.to_owned(),
            };
        }

        let server_plugin = if user_plugin == AUTH_TIDB_AUTH_TOKEN {
            AUTH_MYSQL_CLEAR_PASSWORD
        } else {
            user_plugin
        };
        let client_plugin = self.client_plugin_name(server_plugin);
        // Go compares both connection-advertised names with the raw user-row
        // plugin before rewriting `tidb_auth_token`; a required client-side
        // mapping therefore still causes a switch when the initial response
        // names differ from that raw plugin.
        if request.server_plugin == user_plugin && request.client_plugin == user_plugin {
            return ClientPluginSelection::Keep {
                plugin: request.client_plugin.clone(),
            };
        }
        if request.client_supports_plugins {
            ClientPluginSelection::Switch {
                server_plugin: server_plugin.to_owned(),
                client_plugin,
            }
        } else if server_plugin == AUTH_NATIVE_PASSWORD {
            ClientPluginSelection::Keep {
                plugin: AUTH_NATIVE_PASSWORD.to_owned(),
            }
        } else {
            ClientPluginSelection::RejectLegacyClient {
                required_plugin: server_plugin.to_owned(),
            }
        }
    }

    /// Returns the client-side plugin name used by an auth-switch request.
    ///
    /// Built-in LDAP mappings are kept here because the Go connection layer
    /// applies them before consulting the extension registry.  An extension
    /// with no explicit override uses its own name.  Unknown names are passed
    /// through; the separate handshake/privilege owner decides whether they
    /// are acceptable, so this metadata leaf does not turn them into success.
    #[must_use]
    pub fn client_plugin_name(&self, server_plugin: &str) -> String {
        match server_plugin {
            "authentication_ldap_sasl" => "authentication_ldap_sasl_client".to_owned(),
            "authentication_ldap_simple" => "mysql_clear_password".to_owned(),
            _ => self
                .get(server_plugin)
                .and_then(AuthPluginDescriptor::required_client_side_plugin)
                .unwrap_or(server_plugin)
                .to_owned(),
        }
    }

    /// Returns all validated custom plugins in registration order.
    #[must_use]
    pub fn plugins(&self) -> &[AuthPluginDescriptor] {
        &self.plugins
    }
}
