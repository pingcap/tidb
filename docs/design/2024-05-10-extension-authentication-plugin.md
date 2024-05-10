# Extension Authentication Plugin

- Author: [Yaoming Zhan](http://github.com/yzhan1)
- Discussion PR: TBD
- Tracking Issue: https://github.com/pingcap/tidb/issues/53181

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This design aims to introduce the extension auth plugin feature to TiDB. The extension auth plugin is a plugin that allows users to implement their own authentication and authorization logic. The plugin is loaded by TiDB and is responsible for authenticating users and authorizing their operations.

[Auth plugin](https://dev.mysql.com/doc/extending-mysql/8.0/en/writing-authentication-plugins-server-side.html) is a feature supported in MySQL, so implementing this feature in TiDB will make it more compatible with MySQL.

## Motivation or Background

Currently, TiDB only supports the built-in authentication and authorization mechanism. The built-in mechanism is not flexible enough to meet the needs of some users. For example, some users may want to use their own authentication and authorization logic, or some users may want to integrate TiDB with their existing authentication and authorization system. Using the extension system to implement an auth plugin is a good way for users to plug in their own logic.

### Current Code Path

Here we only cover the critical paths involving authentication and authorization.

Authentication:
+ `server.onConn`
  + A new connection is created and handled using a separate goroutine
+ `conn.handshake`
  + Perform handshake when a new handshake event is received
+ `conn.readOptionalSSLRequestAndHandshakeResponse`
  + SSL handshake completed before running custom auth code
  + This function handles validating and aligning the plugin used for connection on both client and server, and reading the password from client
    + For plugins like `caching_sha2_password` and LDAP, at this stage, the server can communicate with the client by sending extra data back to the client (e.g. salt). Then the client can use this data to process the password before sending it back to the server
+ `conn.openSessionAndDoAuth`
  + If `readOptionalSSLRequestAndHandshakeResponse` does not return error, we move forward to the session creation and authentication stage using the password read from the client
+ `session.Auth` -> `privileges.ConnectionVerification`
  + Authenticate the user based on the plugin specified. For password based authentication such as `mysql_native_password`, TiDB will compare the password digest with the locally stored hash


Authorization is more straight-forward (more details available in the [official developer guide](https://pingcap.github.io/tidb-dev-guide/understand-tidb/dql.html)):
+ `server.onConn`
+ `server.HandleStmt`
+ `server.ExecuteStmt`
+ `executor.Compile`
+ `Planner.optimize`
+ `privileges.RequestVerification`
  + Note that this function will be called multiple times, one time for each privilege required by the SQL statements

## Detailed Design

We propose improving the extension system to support the following functionalities:
+ Allow user defined extra authentication check to be executed during connection establishment phase
  + Current `SessionHandler` [support](https://github.com/pingcap/tidb/tree/master/pkg/extension) in the extension system only allows running observer functions that do not affect the connection attempt result
  + Errors returned from this check will cause the connect attempt to fail
+ Allow user defined extra SQL privilege check to be executed before SQL statements are executed

Note that the above plugin SQL privilege checks are only additional to the existing checks done by TiDB. If the existing checks done by TiDB fails, the plugin checks won’t be invoked. This means that the user should already have the corresponding SQL privileges granted to itself before we can trigger the plugin privilege check, otherwise the SQL request should be rejected beforehand since the user is missing the privileges.

Users should be able to develop custom auth plugins which handle the above two checks. For example, if a user develops a plugin called `authentication_my_plugin`, then once the plugin is loaded, the user can run the following SQL statement:

```sql
CREATE USER 'my_user'@'%' IDENTIFIED WITH 'authentication_my_plugin' AS 'optional_authentication_pwd';
```

After this, `my_user`’s connection verification and privilege verification will be handled by the plugin in addition to the existing default TiDB checks. Users not identified with the plugin will not go through the plugin authorization and authentication checks.

### Interface
We propose a set of interfaces for users to implement an auth plugin that are similar to what is offered in MySQL’s authentication plugins. We will modify TiDB’s code to call these interfaces during the authentication/authorization flows. Most of these interfaces will receive parameters that are used in the existing auth flows in TiDB, so that the plugin developer can access as much information as possible.

Following is the interface for an auth plugin:
```go
// AuthPlugin is an interface for authentication plugins.
type AuthPlugin interface {
    // Name returns the name of the auth plugin. It will be registered as a system variable in TiDB which can be used inside the `CREATE USER ... IDENTIFIED WITH 'plugin_name'` statement.
    Name() string
    
    // PasswordReader is a function to communicate with the client during handshake phase to 
    // read/write more data to the client-side plugin using authConn. For example, sending extra salt to the client.
    // If `plugin.PasswordReader()` returns nil, the connection will default to mysql_clear_password plugin which means the password will be passed from the client-side in clear text format. Otherwise, the connection will use `plugin.Name()` as the current plugin
    // When `plugin.PasswordReader()(ctx, authConn, resp)` is called, it should return the final password received from the client in bytes format. This bytes password will be passed into `plugin.Authenticate` below.
    // ctx: context passed in from caller
    // authConn: interface for the plugin to communicate with the client
    PasswordReader() func(ctx context.Context, authConn conn.AuthConn) ([]byte, error)
    
    // Authenticate is called when a client connects to the server as a user and the server authenticates the user.
    // If an error is returned, the login attempt fails, otherwise it succeeds.
    // authUser: the username in the connect attempt
    // storedPwd: the user's password stored in mysql.user table
    // inputPwd (authentication): the user's password passed in from the connection attempt in bytes (return value of `plugin.PasswordReader()(ctx, authConn, resp)` if `plugin.PasswordReader() != nil`)
    // salt: randomly generated salt for the current connection
    // connState: the TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
    // authConn: interface for the plugin to communicate with the client
    Authenticate(authUser string, storedPwd string, inputPwd []byte, salt []byte, connState *tls.ConnectionState, authConn conn.AuthConn) error
    
    // EncodePassword is a function for user to implement customized ways to encode the password (e.g. hash/salt/clear-text). The returned string will be stored as the encoded password in the mysql.user table.
    // If the input password is considered as invalid, this should return an error.
    // pwd: User's input password in CREATE/ALTER USER statements in clear-text
    EncodePassword(pwd string) (string, bool)
    
    // IsValidPasswordHash checks if the password hash stored in the mysql.user table is valid.
    // This is called when retrieving an existing user to make sure the password stored is valid and not modified.
    // pwdHash: hash of the password stored in the internal user table
    IsValidPasswordHash(pwdHash string) bool
    
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
    VerifyPrivilege(activeRoles []*auth.RoleIdentity, user, host, db, table, column string, priv mysql.PrivilegeType, connState *tls.ConnectionState) bool
    
    // VerifyDynamicPrivilege is called for each user queries, and serves as an extra check for dynamic privileges for the user.
    // It will only be executed if the user has already been granted the dynamic privilege in SQL layer.
    // Returns true if user has the requested privilege.
    // activeRoles: list of active MySQL roles for the current user
    // user: current user's name
    // host: current user's host
    // priv: the dynamic privilege required by the user's SQL statement
    // withGrant: whether the statement to be executed is granting the user privilege for executing GRANT statements
    // connState: the TLS connection state (contains the TLS certificate) if client is using TLS. It will be nil if the client is not using TLS
    VerifyDynamicPrivilege(activeRoles []*auth.RoleIdentity, user, host, privName string, withGrant bool, connState *tls.ConnectionState) bool
}
```

Let’s discuss each interface and why they are needed:

`PasswordReader`: This function will be called multiple times:
1. When the client establishes connections, TiDB should check if `PasswordReader() == nil`, if so, it means the plugin does not require custom handling for passing client passwords, so it will tell the client to use `mysql_clear_password` plugin on the client-side to pass the password in clear text format. 
Otherwise, if the function does not return nil, it means the plugin has some special mechanisms for reading the password from the client, which could include sending more data back to the client. In this case, TiDB will return the Name of the plugin to the client, so the client can look for the [local implementation](https://dev.mysql.com/doc/extending-mysql/8.0/en/writing-authentication-plugins-client-side.html) of the plugin which understands the data sent from the server side.
It is important to keep this as an option, as a user-implemented customized plugin can require client-side implementation as well. This should happen here: https://github.com/pingcap/tidb/blob/8d9e67b37dea759db0980aeddf4da967bf93e83e/pkg/server/conn.go#L252
2. If `PasswordReader() != nil`,  then the returned function will be called with the current connection, which the plugin can use to communicate with the client and return the final password (can be processed) from the client in here: https://github.com/pingcap/tidb/blob/8d9e67b37dea759db0980aeddf4da967bf93e83e/pkg/server/conn.go#L605

`Authenticate`: this should include the authentication check performed during connection attempts after the plugin/SSL information has been exchanged between the client and server. It should be called inside https://github.com/pingcap/tidb/blob/8d9e67b37dea759db0980aeddf4da967bf93e83e/pkg/privilege/privileges/privileges.go#L514 where the privilege manager checks the authentication plugin, and receives all the information it needs.
For example, username, password, and the TLS connection state which includes the TLS certificates. If an error is returned, the connection attempt will fail.

`IsValidPasswordHash`: after the connection attempt is granted, validates whether the password stored locally in TiDB is a valid password hash. This should be called in https://github.com/pingcap/tidb/blob/8d9e67b37dea759db0980aeddf4da967bf93e83e/pkg/privilege/privileges/privileges.go#L213

`EncodePassword`: called when executing `CREATE`/`ALTER USER`/`SET PASSWORD` statements to encode the password. It should return the encoded password and whether the input password is legal. Should be called inside https://github.com/pingcap/tidb/blob/8d9e67b37dea759db0980aeddf4da967bf93e83e/pkg/parser/ast/misc.go#L1421. Password validation during creation/alter time should be done inside this function.

`VerifyPrivilege`: is called to authorize users for executing SQL statements. This will be an extra check on top of the existing check done with the `MySQLPrivilege.RequestVerification`, so it should be called after the statement here: https://github.com/pingcap/tidb/blob/8d9e67b37dea759db0980aeddf4da967bf93e83e/pkg/privilege/privileges/privileges.go#L189.
If the user does not have the privileges granted in MySQL, the overall approval will fail and the user will get a privilege error. The overall logic should be:
```go
func VerifyPrivilege(mysqlPriv *MySQLPrivilege, user string) bool {
	return mysqlPriv.RequestVerification(...) && getPluginForUser(user).RequestVerification(...)
}
```

`VerifyDynamicPrivilege`: similar to the above interface, but used for dynamic privileges. This will be an extra check on top of the existing check done with the `MySQLPrivilege.RequestDynamicVerification`, so it should be called after the statement here: https://github.com/pingcap/tidb/blob/8d9e67b37dea759db0980aeddf4da967bf93e83e/pkg/privilege/privileges/privileges.go#L129.
If the user does not have the dynamic privileges granted in MySQL, the overall approval will fail and the user will get a privilege error. The overall logic should be:
```go
func VerifyDynamicPrivilege(mysqlPriv *MySQLPrivilege, user string) bool {
	return mysqlPriv.RequestDynamicVerification(...) && getPluginForUser(user).RequestDynamicVerification(...)
}
```

Note that for both the privilege checking functions above, they only receive one privilege in the parameter. This means if the transactions include multiple statements requiring multiple privileges, these functions will be called multiple times using the different privileges.

The above interfaces and parameters should give the users all information they need to make a decision on the authentication and authorization. For authorizations before each request, the MySQL user should already be granted with the corresponding privileges in MySQL using `GRANT` statements.

### Compatibility
The feature needs to be compatible with existing user related statements and options, such as `CREATE USER`, `ALTER USER`, `SET PASSWORD`.
However, it will not work with existing functionalities such as password expiry and password validation. Users can implement their own password validation logic inside the EncodePassword implementation.

## Test Design

This feature requires tests focusing on functional and compatibility. Performance should not be a concern, since that depends on the actual auth plugin implementation and it is up to the users to keep the implementation performant if needed.

### Functional Tests

We will implement unit tests for all of the auth plugin extension interfaces, and ensure that they are executed properly in the code path that we inject them.

### Scenario Tests

We will implement client to server tests to test out scenarios where MySQL users are created with and without using an auth plugin from the extension system. For example:
+ Creating a user using the plugin
  + Connecting as the user and verify that the connection verification API is called and errors are correctly translated to connection rejections
    + Execute SQL statements and verify that the request (dynamic) verification APIs are called and errors are correctly translated to permission rejections
    + Alter user changes can be applied correctly
    + Passwords can be encoded with the API defined in the plugin
+ When the connection changes between users, the system should behave correctly
  + From non-plugin user to plugin user
  + From plugin user to non-plugin user
  + From plugin user to plugin user

### Compatibility Tests

This feature should integrate well with all the existing test cases since it is an additional functionality. Existing tests related to authentication and authorization should continue to pass since they are not using auth plugins.

### Benchmark Tests

This feature should not affect the performance of the TiDB as it simply provides interfaces for users to inject their own auth plugins via the extension system. The performance implication will come from the implementation of those plugins which depends on how the users of the framework want to use it.

## Impacts & Risks

If a user is using an auth plugin, the SQL privilege status in TiDB might not reflect the actual privileges, since there might be an outside system that decides whether the privilege check can actually pass. However, this is an intentional behavioral change when the user uses an auth plugin since we are providing users customizations on checks to run.

The design does not account for implementing the same level of proxy user support as in MySQL auth plugin, since currently TiDB does not support [proxy users](https://dev.mysql.com/doc/extending-mysql/8.0/en/writing-authentication-plugins-proxy-users.html) yet. Also existing support for password expiry and validation will not work, as mentioned in the earlier section.


## Investigation & Alternatives

### MySQL

MySQL supports customizing authentication methods with [authentication plugins](https://dev.mysql.com/doc/extending-mysql/8.0/en/writing-authentication-plugins-server-side.html). This design proposes a similar set of interfaces for users to define, compared to those in MySQL. The main differences are that we also allow defining authorization checks (SQL privileges).

### PostgreSQL

PostgreSQL supports [custom authentication mechanisms](https://www.google.com/url?q=https://www.postgresql.org/docs/current/auth-pam.html&sa=D&source=docs&ust=1715365735752899&usg=AOvVaw36GkfLatUo1zfhHAY65XLC) using Linux’s [Pluggable Authentication Module (PAM)](https://documentation.suse.com/sles/12-SP5/html/SLES-all/cha-pam.html) support. When a user connects to Postgres, the server will authenticate the user using the PAM service locally, which can trigger some customized authentication code. However, this does not allow plugging in customized authorization checks for SQL privileges.

Technically, we could implement PAM support in TiDB instead of what is being proposed in this design, but it will just be one standard authentication option which does not utilize the existing extension framework. Implementing auth plugin extension support is more flexible as it allows many different customizations for both authentication and authorization. With the auth plugin extension support in place, we could even consider implementing the PAM auth scheme as an auth plugin.


## Unresolved Questions

+ Is the `IsValidPasswordHash` function really needed? Why do we want to do a password validity check for passwords fetched from the internal user table?
