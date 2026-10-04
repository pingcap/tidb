// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go session.Auth's durable failed-login transactions over the shared SQL owner.

use super::*;
use crate::configured_user_store::{
    AuthenticationFailure, ConfiguredUserStore, LoginPolicyStorage,
};
use tidb_session::privilege::{LoginPolicyAction, PasswordLocking};
use tidb_util::sqlescape::{escape_sql, SqlArg};

struct LoginStorage(Weak<ClusterSessionFactory>);

impl ClusterSessionFactory {
    pub(crate) fn attach_login_storage(self: &Arc<Self>, users: &ConfiguredUserStore) {
        users.attach_login_storage(Arc::new(LoginStorage(Arc::downgrade(self))));
    }
}

impl LoginPolicyStorage for LoginStorage {
    fn apply(
        &self,
        user: &str,
        host: &str,
        action: LoginPolicyAction,
    ) -> Result<(), AuthenticationFailure> {
        let factory = self.0.upgrade().ok_or_else(|| {
            AuthenticationFailure::Storage(SqlQueryError::unknown(
                "login storage session factory is stopped",
            ))
        })?;
        factory.apply_login_policy(user, host, action)
    }
}

impl ClusterSessionFactory {
    fn apply_login_policy(
        self: &Arc<Self>,
        user: &str,
        host: &str,
        action: LoginPolicyAction,
    ) -> Result<(), AuthenticationFailure> {
        self.advanced_sys_session_pool()
            .with_session(|lease| {
                lease.with_session_context(|context| {
                    let mut slot = context
                        .state
                        .session
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let session = slot.as_mut().ok_or_else(|| {
                        tidb_syssession::SysSessionError::new("login session is closed")
                    })?;
                    let result = self.apply_login_policy_in_session(session, user, host, action);
                    if matches!(result, Err(AuthenticationFailure::Storage(_))) {
                        lease.avoid_reuse();
                    }
                    Ok(result)
                })
            })
            .map_err(|error| {
                AuthenticationFailure::Storage(SqlQueryError::unknown(error.to_string()))
            })?
    }

    fn apply_login_policy_in_session(
        &self,
        session: &mut ClusterServerSession,
        user: &str,
        host: &str,
        action: LoginPolicyAction,
    ) -> Result<(), AuthenticationFailure> {
        let storage_error = AuthenticationFailure::Storage;
        session
            .control_transaction("BEGIN PESSIMISTIC")
            .map_err(storage_error)?;
        let result = (|| -> Result<_, SqlQueryError> {
            let select = escaped(
                "SELECT user_attributes FROM mysql.user WHERE User=%? AND Host=%? FOR UPDATE",
                &[user.into(), host.into()],
            )?;
            let attributes = {
                let mut result = session.execute(&select)?;
                let source = result.source();
                let rows = source
                    .next_batch(2)
                    .map_err(|e| SqlQueryError::unknown(e.to_string()))?;
                source
                    .finish()
                    .map_err(|e| SqlQueryError::unknown(e.to_string()))?;
                source
                    .close()
                    .map_err(|e| SqlQueryError::unknown(e.to_string()))?;
                match rows.as_slice() {
                    [row] => match row.first() {
                        Some(tidb_datatype::Datum::Json(json)) => json.to_string(),
                        _ => {
                            return Err(SqlQueryError::unknown(
                                "login user_attributes is NULL or not JSON",
                            ))
                        }
                    },
                    _ => {
                        return Err(SqlQueryError::unknown(
                            "login user_attributes row not found",
                        ))
                    }
                }
            };
            let before = PasswordLocking::from_attributes(Some(&attributes))
                .map_err(SqlQueryError::unknown)?
                .unwrap_or_default();
            let mut after = before;
            let admission =
                action.apply(&mut after, user, host, self.privileges.clock().now_unix());
            if after != before {
                let patch = action.patch(&before, &after).to_string();
                let update = escaped("UPDATE mysql.user SET user_attributes=JSON_MERGE_PATCH(COALESCE(user_attributes, '{}'), %?) WHERE User=%? AND Host=%?", &[patch.as_str().into(), user.into(), host.into()])?;
                session.execute_write(&update)?;
            }
            session.control_transaction("COMMIT")?;
            Ok((before, after, admission))
        })();
        let (before, after, admission) = match result {
            Ok(result) => result,
            Err(error) => {
                // An uncertain commit retains its fatal identity even if cleanup fails.
                let rollback = session.control_transaction("ROLLBACK");
                if !error.is_result_undetermined() {
                    rollback.map_err(storage_error)?;
                }
                return Err(storage_error(error));
            }
        };
        if before.auto_account_locked != after.auto_account_locked {
            self.privileges.publish_password_locking(user, host, after);
            self.accounts.notify_login_lock_change(user, host);
        }
        admission.map_err(AuthenticationFailure::AutoLocked)
    }
}

fn escaped(sql: &str, arguments: &[SqlArg<'_>]) -> Result<String, SqlQueryError> {
    let bytes = escape_sql(sql, arguments).map_err(|e| SqlQueryError::unknown(e.to_string()))?;
    String::from_utf8(bytes).map_err(|e| SqlQueryError::unknown(e.to_string()))
}
