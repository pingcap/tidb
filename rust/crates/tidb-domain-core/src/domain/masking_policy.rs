// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `Domain`'s session-pool factory for infoschema masking-policy loads.

use super::Domain;
use std::sync::{Arc, OnceLock, Weak};
use tidb_infoschema::masking_policy::{MaskingPolicySession, MaskingPolicySessionFactory};
use tidb_sqlexec::RestrictedSqlExecutor;
use tidb_terror::MysqlError;

struct Session(tidb_syssession::Session<dyn crate::sys_session::DomainSysSession>);

impl Drop for Session {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl MaskingPolicySession for Session {
    /// Go `loadMaskingPoliciesWithTableIDs`'s ExecRestrictedSQL on its resource.
    fn exec_restricted_sql(
        &self,
        snapshot_ts: u64,
        sql: &str,
        ids: &[i64],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, MysqlError> {
        let context = tikv_client::util::with_internal_source_type(
            &tikv_client::trace::TraceContext::new(),
            "ddl",
        );
        let mut options: Vec<tidb_sqlexec::OptionFuncAlias> =
            vec![Arc::new(tidb_sqlexec::exec_option_use_cur_session)];
        if snapshot_ts > 0 {
            options.push(tidb_sqlexec::exec_option_with_snapshot(snapshot_ts));
        }
        let args: Vec<_> = ids
            .iter()
            .copied()
            .map(tidb_sqlescape::SqlArg::Signed)
            .collect();
        self.0
            .exec_restricted_sql(&context, &options, sql, &args)
            .map(|(rows, _)| rows)
            .map_err(|error| {
                let error = crate::sys_session::stats_session_sql_error(error);
                MysqlError::from_parts(error.code, error.state, error.message)
                    .with_identity_opt(error.identity)
            })
    }
}

/// Go `getSessionFactoryInternal`: acquire the domain's internal resource.
pub(super) fn session_factory(domain: Arc<OnceLock<Weak<Domain>>>) -> MaskingPolicySessionFactory {
    Arc::new(move || {
        let domain = domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| MysqlError::unknown("cluster session factory is stopped"))?;
        let session = domain
            .advanced_sys_session_pool()
            .get()
            .map_err(|err| MysqlError::unknown(err.to_string()))?;
        Ok(Box::new(Session(session)))
    })
}
