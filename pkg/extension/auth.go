// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extension

import (
	"crypto/tls"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege/conn"
)

// AuthPlugin contains attributes needed for an authentication plugin.
type AuthPlugin struct {
	// Name is the name of the auth plugin. It will be registered as a system variable in TiDB which can be used inside the `CREATE USER ... IDENTIFIED WITH 'plugin_name'` statement.
	Name string

	// RequiredClientSidePlugin is the name of the client-side plugin required by the server-side plugin. It will be used to check if the client has the required plugin installed and require the client to use it if installed.
	// The user can require default MySQL plugins such as 'caching_sha2_password' or 'mysql_native_password'.
	// If this is empty then `AuthPlugin.Name` is used as the required client-side plugin.
	RequiredClientSidePlugin string

	// AuthenticateUser is called when a client connects to the server as a user and the server authenticates the user.
	// If an error is returned, the login attempt fails, otherwise it succeeds.
	// request: The request context for the authentication plugin to authenticate a user
	AuthenticateUser func(request AuthenticateRequest) error

	// GenerateAuthString is a function for user to implement customized ways to encode the password (e.g. hash/salt/clear-text). The returned string will be stored as the encoded password in the mysql.user table.
	// If the input password is considered as invalid, this should return an error.
	// pwd: User's input password in CREATE/ALTER USER statements in clear-text
	GenerateAuthString func(pwd string) (string, bool)

	// ValidateAuthString checks if the password hash stored in the mysql.user table or passed in from `IDENTIFIED AS` is valid.
	// This is called when retrieving an existing user to make sure the password stored is valid and not modified and make sure user is passing a valid password hash in `IDENTIFIED AS`.
	// pwdHash: hash of the password stored in the internal user table
	ValidateAuthString func(pwdHash string) bool

	// VerifyPrivilege is called for each user queries, and serves as an extra check for privileges for the user.
	// It will only be executed if the user has already been granted the privilege in SQL layer.
	// Returns true if user has the requested privilege.
	// request: The request context for the authorization plugin to authorize a user's static privilege
	VerifyPrivilege func(request VerifyStaticPrivRequest) bool

	// VerifyDynamicPrivilege is called for each user queries, and serves as an extra check for dynamic privileges for the user.
	// It will only be executed if the user has already been granted the dynamic privilege in SQL layer.
	// Returns true if user has the requested privilege.
	// request: The request context for the authorization plugin to authorize a user's dynamic privilege
	VerifyDynamicPrivilege func(request VerifyDynamicPrivRequest) bool
}

// AuthenticateRequest contains the context for the authentication plugin to authenticate a user.
type AuthenticateRequest struct {
	// User The username in the connect attempt
	User string
	// StoredAuthString The user's auth string stored in mysql.user table
	StoredAuthString string
	// InputAuthString The user's auth string passed in from the connection attempt in bytes
	InputAuthString []byte
	// Salt Randomly generated salt for the current connection
	Salt []byte
	// ConnState The TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
	ConnState *tls.ConnectionState
	// AuthConn Interface for the plugin to communicate with the client
	AuthConn conn.AuthConn
}

// VerifyStaticPrivRequest contains the context for the plugin to authorize a user's static privilege.
type VerifyStaticPrivRequest struct {
	// User The username in the connect attempt
	User string
	// Host The host that the user is connecting from
	Host string
	// DB The database to check for privilege
	DB string
	// Table The table to check for privilege
	Table string
	// Column The column to check for privilege (currently just a placeholder in TiDB as column-level privilege is not supported by TiDB yet)
	Column string
	// StaticPriv The privilege type of the SQL statement that will be executed
	StaticPriv mysql.PrivilegeType
	// ConnState The TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
	ConnState *tls.ConnectionState
	// ActiveRoles List of active MySQL roles for the current user
	ActiveRoles []*auth.RoleIdentity
}

// VerifyDynamicPrivRequest contains the context for the plugin to authorize a user's dynamic privilege.
type VerifyDynamicPrivRequest struct {
	// User The username in the connect attempt
	User string
	// Host The host that the user is connecting from
	Host string
	// DynamicPriv the dynamic privilege required by the user's SQL statement
	DynamicPriv string
	// ConnState The TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
	ConnState *tls.ConnectionState
	// ActiveRoles List of active MySQL roles for the current user
	ActiveRoles []*auth.RoleIdentity
	// WithGrant Whether the statement to be executed is granting the user privilege for executing GRANT statements
	WithGrant bool
}

// validateAuthPlugin validates the auth plugin functions and attributes.
func validateAuthPlugin(m *Manifest) error {
	pluginNames := make(map[string]bool)
	// Validate required functions for the auth plugins
	for _, p := range m.authPlugins {
		if p.Name == "" {
			return errors.Errorf("auth plugin name cannot be empty for %s", p.Name)
		}
		if pluginNames[p.Name] {
			return errors.Errorf("auth plugin name %s has already been registered", p.Name)
		}
		pluginNames[p.Name] = true
		if slices.Contains(mysql.DefaultAuthPlugins, p.Name) {
			return errors.Errorf("auth plugin name %s is a reserved name for default auth plugins", p.Name)
		}
		if p.AuthenticateUser == nil {
			return errors.Errorf("auth plugin AuthenticateUser function cannot be nil for %s", p.Name)
		}
		if p.GenerateAuthString == nil {
			return errors.Errorf("auth plugin GenerateAuthString function cannot be nil for %s", p.Name)
		}
		if p.ValidateAuthString == nil {
			return errors.Errorf("auth plugin ValidateAuthString function cannot be nil for %s", p.Name)
		}
	}
	return nil
}
