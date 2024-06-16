package extension

import (
	"crypto/tls"
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
	// authContext: The context for the authentication plugin to authenticate a user
	AuthenticateUser func(ctx *AuthenticateContext) error

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
	// ctx: The context for the authorization plugin to authorize a user's privilege
	VerifyPrivilege func(ctx *AuthorizeContext) bool

	// VerifyDynamicPrivilege is called for each user queries, and serves as an extra check for dynamic privileges for the user.
	// It will only be executed if the user has already been granted the dynamic privilege in SQL layer.
	// Returns true if user has the requested privilege.
	// ctx: The context for the authorization plugin to authorize a user's privilege
	VerifyDynamicPrivilege func(ctx *AuthorizeContext) bool
}

// AuthenticateContext contains the context for the authentication plugin to authenticate a user.
type AuthenticateContext struct {
	// User The username in the connect attempt
	User string
	// StoredPwd The user's password stored in mysql.user table
	StoredPwd string
	// InputPwd The user's password passed in from the connection attempt in bytes
	InputPwd []byte
	// Salt Randomly generated salt for the current connection
	Salt []byte
	// ConnState The TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
	ConnState *tls.ConnectionState
	// AuthConn Interface for the plugin to communicate with the client
	AuthConn conn.AuthConn
}

// AuthorizeContext contains the context for the authorization plugin to authorize a user's privilege.
type AuthorizeContext struct {
	// User The username in the connect attempt
	User string
	// Host The host that the user is connecting from
	Host string
	// DB The database to check for privilege (should be empty if DynamicPriv is set)
	DB string
	// Table The table to check for privilege (should be empty if DynamicPriv is set)
	Table string
	// Column The column to check for privilege (currently just a placeholder in TiDB as column-level privilege is not supported by TiDB yet)
	Column string
	// StaticPriv The privilege type of the SQL statement that will be executed. Mutual exclusive with DynamicPriv.
	StaticPriv mysql.PrivilegeType
	// DynamicPriv the dynamic privilege required by the user's SQL statement. Mutual exclusive with StaticPriv.
	DynamicPriv string
	// ConnState The TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
	ConnState *tls.ConnectionState
	// ActiveRoles List of active MySQL roles for the current user
	ActiveRoles []*auth.RoleIdentity
	// WithGrant Whether the statement to be executed is granting the user privilege for executing GRANT statements
	WithGrant bool
}
