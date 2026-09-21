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

//! The session's commit-time schema lease checker.
//!
//! Go `SetOptionsBeforeCommit` (`pkg/sessiontxn/isolation/base.go:560-616`)
//! hands every committing transaction a `domain.SchemaChecker` over the
//! domain's schema validator: the schema version the transaction was planned
//! against, the physical tables it wrote, and whether the delta scan is
//! needed (`!TxnCtx.EnableMDL`). client-go asks it at commit
//! (`checkSchemaValid`), and a refusal is `ErrInfoSchemaChanged` (8028) or,
//! when the validator is stopped or its lease ran out, `ErrInfoSchemaExpired`
//! (8027) after `SchemaOutOfDateRetryTimes` retries.
//!
//! The node's validator is `tidb-exec`'s port; the checker's retry loop is
//! `tidb-domain`'s. This module is the adapter between the two and the
//! `tidb-txnkv` trait the coordinator calls.

use std::sync::Arc;

use tidb_domain::schema_checker::{
    CheckResult, RelatedSchemaChange as DomainRelatedSchemaChange, SchemaChecker,
    SchemaValidator as DomainSchemaValidator,
};
use tidb_exec::schema_validator::{Result as ValidatorResult, SchemaValidator, Validator};
use tidb_txnkv::transaction::{SchemaLeaseChecker, SchemaLeaseError};

/// One transaction's checker: Go `domain.NewSchemaChecker(validator,
/// schemaVer, physicalTableIDs, needCheckSchemaByDelta)`, with the table IDs
/// supplied at commit by the transaction's own mutations.
pub(crate) struct SessionSchemaLeaseChecker {
    validator: Arc<SchemaValidator>,
    /// Go `GetTxnInfoSchema().SchemaMetaVersion()`: the version the
    /// transaction was planned against.
    schema_ver: i64,
}

impl SessionSchemaLeaseChecker {
    pub(crate) fn new(validator: Arc<SchemaValidator>, schema_ver: i64) -> Self {
        Self {
            validator,
            schema_ver,
        }
    }
}

impl SchemaLeaseChecker for SessionSchemaLeaseChecker {
    fn check_by_schema_ver(
        &self,
        check_ts: u64,
        related_physical_table_ids: &[i64],
    ) -> Result<(), SchemaLeaseError> {
        // Go `needCheckSchemaByDelta = !sessVars.TxnCtx.EnableMDL`, and the
        // validator's own `vardef.IsMDLEnabled()` reading inside `Check` is
        // the same global switch; this node reads it once here for both.
        let mdl_enabled = tidb_vardef::is_mdl_enabled(false);
        self.validator.set_mdl_enabled(mdl_enabled);
        let adapter = ValidatorAdapter(&self.validator);
        let checker = SchemaChecker::new(
            &adapter,
            self.schema_ver,
            Some(related_physical_table_ids.to_vec()),
            !mdl_enabled,
        );
        match checker.check(check_ts) {
            Ok(_) => Ok(()),
            Err((_, error)) => Err(SchemaLeaseError {
                code: error.code(),
                // Go's `terror` rendering: `[class:code]message`.
                message: format!("[domain:{}]{}", error.code(), error.message()),
            }),
        }
    }
}

/// `tidb-domain`'s validator trait over `tidb-exec`'s validator: the two
/// answer shapes are the same fields under different names.
struct ValidatorAdapter<'a>(&'a SchemaValidator);

impl DomainSchemaValidator for ValidatorAdapter<'_> {
    fn check(
        &self,
        txn_ts: u64,
        schema_ver: i64,
        related_physical_table_ids: Option<&[i64]>,
        need_check_schema_by_delta: bool,
    ) -> (Option<DomainRelatedSchemaChange>, CheckResult) {
        let (change, result) = Validator::check(
            self.0,
            txn_ts,
            schema_ver,
            related_physical_table_ids,
            need_check_schema_by_delta,
        );
        let change = change.map(|change| DomainRelatedSchemaChange {
            phy_tbl_ids: change.phy_tbl_ids,
            action_types: change.action_types,
            amendable: change.amendable,
        });
        let result = match result {
            ValidatorResult::Succ => CheckResult::Succ,
            ValidatorResult::Fail => CheckResult::Fail,
            ValidatorResult::Unknown => CheckResult::Unknown,
        };
        (change, result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tidb_domain::schema_checker::{
        set_schema_out_of_date_retry_interval, set_schema_out_of_date_retry_times,
    };
    use tidb_exec::schema_validator::RelatedSchemaChange;

    /// A PD timestamp whose physical part is `unix_millis`.
    fn ts_at(unix_millis: u64) -> u64 {
        unix_millis << 18
    }

    fn now_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("fits")
    }

    /// Go's `Check` under MDL: a transaction planned at an older version
    /// than the latest commits as long as it names the tables it wrote --
    /// the metadata lock, not the delta scan, is what protects it.
    #[test]
    fn a_transaction_behind_the_latest_version_commits_under_mdl() {
        set_schema_out_of_date_retry_interval(Duration::from_millis(1));
        let validator = Arc::new(SchemaValidator::new(Duration::from_secs(45)));
        let now = now_ms();
        validator.update(ts_at(now), 0, 10, None);
        let change = RelatedSchemaChange {
            phy_tbl_ids: vec![77],
            action_types: vec![5],
            amendable: false,
        };
        validator.update(ts_at(now), 10, 11, Some(&change));
        tidb_vardef::set_enable_mdl(true);
        let checker = SessionSchemaLeaseChecker::new(Arc::clone(&validator), 10);
        assert_eq!(checker.check_by_schema_ver(ts_at(now + 1), &[77]), Ok(()));
    }

    /// With MDL off, the delta scan decides: a table the transaction wrote
    /// changed after its version, so the commit is `ErrInfoSchemaChanged`.
    #[test]
    fn a_changed_table_refuses_the_commit_without_mdl() {
        set_schema_out_of_date_retry_interval(Duration::from_millis(1));
        let validator = Arc::new(SchemaValidator::new(Duration::from_secs(45)));
        let now = now_ms();
        validator.update(ts_at(now), 0, 10, None);
        let change = RelatedSchemaChange {
            phy_tbl_ids: vec![77],
            action_types: vec![5],
            amendable: false,
        };
        validator.update(ts_at(now), 10, 11, Some(&change));
        tidb_vardef::set_enable_mdl(false);
        let checker = SessionSchemaLeaseChecker::new(Arc::clone(&validator), 10);
        let error = checker
            .check_by_schema_ver(ts_at(now + 1), &[77])
            .expect_err("changed table");
        assert_eq!(error.code, 8028);
        assert!(
            error
                .message
                .starts_with("[domain:8028]Information schema is changed"),
            "{}",
            error.message
        );
        assert!(
            error.message.ends_with("[try again later]"),
            "{}",
            error.message
        );
        // A table the change did not touch commits.
        assert_eq!(checker.check_by_schema_ver(ts_at(now + 1), &[78]), Ok(()));
        tidb_vardef::set_enable_mdl(true);
    }

    /// A stopped validator (the node lost its etcd session) answers
    /// `Unknown` until the retries run out: `ErrInfoSchemaExpired`.
    #[test]
    fn a_stopped_validator_expires_the_commit() {
        set_schema_out_of_date_retry_interval(Duration::from_millis(1));
        set_schema_out_of_date_retry_times(3);
        let validator = Arc::new(SchemaValidator::new(Duration::from_secs(45)));
        validator.update(ts_at(now_ms()), 0, 10, None);
        validator.stop();
        let checker = SessionSchemaLeaseChecker::new(Arc::clone(&validator), 10);
        let error = checker
            .check_by_schema_ver(ts_at(now_ms()), &[77])
            .expect_err("stopped");
        assert_eq!(error.code, 8027);
        assert!(
            error
                .message
                .starts_with("[domain:8027]Information schema is out of date"),
            "{}",
            error.message
        );
        set_schema_out_of_date_retry_times(10);
    }

    /// Go `Restart(ver)`: after a reconnect, a transaction whose version
    /// predates the restart floor is refused outright.
    #[test]
    fn a_version_below_the_restart_floor_is_refused() {
        set_schema_out_of_date_retry_interval(Duration::from_millis(1));
        let validator = Arc::new(SchemaValidator::new(Duration::from_secs(45)));
        validator.update(ts_at(now_ms()), 0, 10, None);
        validator.stop();
        validator.restart(12);
        validator.update(ts_at(now_ms()), 10, 12, None);
        let checker = SessionSchemaLeaseChecker::new(Arc::clone(&validator), 11);
        assert_eq!(
            checker
                .check_by_schema_ver(ts_at(now_ms()), &[77])
                .expect_err("below floor")
                .code,
            8028
        );
    }
}
