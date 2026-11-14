//! Go `pkg/server/user_connections.go`: the live connections of each account,
//! which `max_user_connections` (the account's own, else the global one)
//! limits.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use tidb_store_driver_error::sql_query_error::SqlQueryError;

use crate::configured_user_store::ConfiguredUserStore;

/// Go `Server.userResource` under `Server.userResLock`: each account's
/// (`UserIdentity.String()`) count of live connections (`userResourceLimits`).
#[derive(Debug, Default)]
pub(crate) struct UserResource {
    connections: Mutex<HashMap<String, usize>>,
}

/// Go `calculateConnectionLimit`: an account's own limit, else the global
/// one; 0 is no limit.
fn calculate_connection_limit(global_limit: u32, user_limit: u32) -> u32 {
    if user_limit > 0 {
        return user_limit;
    }
    global_limit
}

/// Go `UserIdentity.String()` / `LoginString()` of an authenticated account.
fn target_user(user: &str, host: &str) -> String {
    format!("{user}@{host}")
}

fn too_many_user_connections(target_user: &str) -> SqlQueryError {
    tidb_terror::MysqlError::from_terror(
        &tidb_server_err::ERR_TOO_MANY_USER_CONNECTIONS
            .gen_with_stack_by_args(&[tidb_mysql::errors::FormatArg::from(target_user)]),
    )
    .into()
}

impl UserResource {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, usize>> {
        self.connections.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `clientConn.increaseUserConnectionsCount`, once a connection has
    /// logged in as `auth_user` / `auth_host` (not on `COM_CHANGE_USER`).
    fn increase_user_connections_count(
        &self,
        users: &ConfiguredUserStore,
        auth_user: &str,
        auth_host: &str,
    ) -> Result<(), SqlQueryError> {
        let target_user = target_user(auth_user, auth_host);
        let global_limit = tidb_vardef::MAX_USER_CONNECTIONS_VALUE.load(Ordering::SeqCst);
        // Go: "check the number of connections again".
        let user_limit = users
            .get_user_resources(auth_user, auth_host)
            .map_err(SqlQueryError::unknown)?;
        let limit = calculate_connection_limit(global_limit, user_limit as u32);
        let mut connections = self.lock();
        let Some(count) = connections.get_mut(&target_user) else {
            connections.insert(target_user, 1);
            return Ok(());
        };
        if limit > 0 && *count >= limit as usize {
            return Err(too_many_user_connections(&target_user));
        }
        *count += 1;
        Ok(())
    }

    /// Go `onConn`'s `increaseUserConnectionsCount` with its deferred
    /// `decreaseUserConnectionCount`: the count the returned guard holds
    /// until the connection ends.
    pub(crate) fn enter<'a>(
        &'a self,
        users: &ConfiguredUserStore,
        auth_user: &str,
        auth_host: &str,
    ) -> Result<UserConnection<'a>, SqlQueryError> {
        self.increase_user_connections_count(users, auth_user, auth_host)?;
        Ok(UserConnection {
            resource: self,
            auth_user: auth_user.to_owned(),
            auth_host: auth_host.to_owned(),
        })
    }

    /// Go `clientConn.decreaseUserConnectionCount`, as the connection ends.
    fn decrease_user_connection_count(&self, auth_user: &str, auth_host: &str) {
        let target_user = target_user(auth_user, auth_host);
        let mut connections = self.lock();
        if let Some(count) = connections.get_mut(&target_user) {
            if *count > 0 {
                *count -= 1;
                if *count == 0 {
                    connections.remove(&target_user);
                }
            }
        }
    }

    /// Go `clientConn.getUserConnectionCount`.
    fn get_user_connection_count(&self, target_user: &str) -> usize {
        self.lock().get(target_user).copied().unwrap_or(0)
    }

    /// Go `clientConn.checkUserConnectionCount`: `openSessionAndDoAuth`
    /// refuses a login whose account already holds its limit.
    pub(crate) fn check_user_connection_count(
        &self,
        users: &ConfiguredUserStore,
        user: &str,
        host: &str,
    ) -> Result<(), SqlQueryError> {
        // Go `session.MatchIdentity`'s error: the identity it failed to
        // match is the empty one.
        let Some((auth_user, auth_host)) = users.match_identity_account(user, host) else {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_privilege::privileges_errors::ERR_ACCESS_DENIED.fast_gen_by_args(&[
                    tidb_mysql::errors::FormatArg::from(""),
                    tidb_mysql::errors::FormatArg::from(""),
                ]),
            )
            .into());
        };
        let user_limit = users
            .get_user_resources(&auth_user, &auth_host)
            .map_err(SqlQueryError::unknown)?;
        let limit = calculate_connection_limit(
            tidb_vardef::MAX_USER_CONNECTIONS_VALUE.load(Ordering::SeqCst),
            user_limit as u32,
        );
        if limit == 0 {
            return Ok(());
        }
        let login = target_user(&auth_user, &auth_host);
        if self.get_user_connection_count(&login) >= limit as usize {
            tidb_logutil::bg_logger().error(
                "the maximum allowed connections exceeded",
                &[
                    tidb_log::Field::new("user", tidb_log::Value::Str(login.clone())),
                    tidb_log::Field::new("max-user-connections", tidb_log::Value::U64(u64::from(limit))),
                    tidb_log::Field::new(
                        "error",
                        tidb_log::Value::Str(tidb_server_err::ERR_CON_COUNT.to_string()),
                    ),
                ],
            );
            return Err(too_many_user_connections(&login));
        }
        Ok(())
    }
}

/// One logged-in connection's count in its account's [`UserResource`],
/// released when the connection ends (Go's deferred
/// `decreaseUserConnectionCount`).
pub(crate) struct UserConnection<'a> {
    resource: &'a UserResource,
    auth_user: String,
    auth_host: String,
}

impl Drop for UserConnection<'_> {
    fn drop(&mut self) {
        self.resource.decrease_user_connection_count(&self.auth_user, &self.auth_host);
    }
}
