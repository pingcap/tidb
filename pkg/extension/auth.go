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
	RequiredClientSidePlugin string

	// AuthenticateUser is called when a client connects to the server as a user and the server authenticates the user.
	// If an error is returned, the login attempt fails, otherwise it succeeds.
	// authUser: the username in the connect attempt
	// storedPwd: the user's password stored in mysql.user table
	// inputPwd (authentication): the user's password passed in from the connection attempt in bytes
	// salt: randomly generated salt for the current connection
	// connState: the TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
	// authConn: interface for the plugin to communicate with the client
	AuthenticateUser func(authUser string, storedPwd string, inputPwd []byte, salt []byte, connState *tls.ConnectionState, authConn conn.AuthConn) error

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
	// activeRoles: list of active MySQL roles for the current user
	// user: current user's name
	// host: the host that the user is connecting from
	// db: the database to check for privilege
	// table: the table to check for privilege
	// column: the column to check for privilege (currently just a placeholder in TiDB as column-level privilege is not supported by TiDB yet)
	// priv: the privilege type of the SQL statement that will be executed
	// connState: the TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
	VerifyPrivilege func(activeRoles []*auth.RoleIdentity, user, host, db, table, column string, priv mysql.PrivilegeType, connState *tls.ConnectionState) bool

	// VerifyDynamicPrivilege is called for each user queries, and serves as an extra check for dynamic privileges for the user.
	// It will only be executed if the user has already been granted the dynamic privilege in SQL layer.
	// Returns true if user has the requested privilege.
	// activeRoles: list of active MySQL roles for the current user
	// user: current user's name
	// host: current user's host
	// priv: the dynamic privilege required by the user's SQL statement
	// withGrant: whether the statement to be executed is granting the user privilege for executing GRANT statements
	// connState: the TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
	VerifyDynamicPrivilege func(activeRoles []*auth.RoleIdentity, user, host, privName string, withGrant bool, connState *tls.ConnectionState) bool
}
