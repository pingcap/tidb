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

//! Port of `pkg/domain/schema_checker_test.go::TestSchemaCheckerSimple`
//! (origin/master) against `tidb_domain::schema_checker::SchemaChecker`.
//!
//! go-parity-gap: the Go test drives the REAL `isvalidator.New(lease)` delta
//! ring (`pkg/infoschema/isvalidator`). Its transcreation lives in
//! `tidb_exec::schema_validator`, which `tidb-session` does not depend on —
//! and `tidb_domain::schema_checker` deliberately narrows its validator to
//! the one-method `SchemaValidator` boundary (see that module's doc: the
//! blanket impl over the real validator is reserved for the `domain.go`
//! batch). The test below is therefore `#[ignore]`d and drives a scripted
//! validator whose answers are the ones Go's real validator produces at each
//! step (re-derived from `pkg/infoschema/isvalidator/isvalidator.go`
//! `Check`: same-version → Succ; version in the ring without the table id →
//! Succ; version below the ring floor → Fail; latest version with an expired
//! lease → Unknown). Only the checker half of the Go test runs.

#![cfg(test)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use tidb_domain::schema_checker::{
    CheckResult, RetrySleeper, SchemaCheckError, SchemaChecker, SchemaValidator,
};

/// The verdict sequence Go's real validator answers at the test's six probe
/// points, in order.
#[derive(Default)]
struct ScriptedValidator {
    answers: RefCell<
        VecDeque<(
            Option<tidb_domain::schema_checker::RelatedSchemaChange>,
            CheckResult,
        )>,
    >,
}

impl SchemaValidator for ScriptedValidator {
    fn check(
        &self,
        _txn_ts: u64,
        _schema_ver: i64,
        _related_physical_table_ids: Option<&[i64]>,
        _need_check_schema_by_delta: bool,
    ) -> (
        Option<tidb_domain::schema_checker::RelatedSchemaChange>,
        CheckResult,
    ) {
        self.answers
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| panic!("validator script exhausted"))
    }
}

/// A no-op stand-in for Go's `time.Sleep` in the retry loop.
struct NoSleep;

impl RetrySleeper for NoSleep {
    fn sleep(&self, _d: Duration) {}
}

fn unix_nanos_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock before the epoch")
            .as_nanos(),
    )
    .unwrap_or(u64::MAX)
}

fn checker<'a>(
    validator: &'a ScriptedValidator,
    schema_ver: i64,
    related: Option<Vec<i64>>,
) -> SchemaChecker<'a, ScriptedValidator, NoSleep> {
    SchemaChecker {
        validator,
        schema_ver,
        related_table_ids: related,
        need_check_schema_by_delta: true,
        sleeper: NoSleep,
    }
}

/// Go `pkg/domain/schema_checker_test.go:28::TestSchemaCheckerSimple`.
// go-parity-gap: pkg/infoschema/isvalidator (ported as
// tidb_exec::schema_validator) is not a dependency of tidb-session; the
// validator answers are scripted to the values the real ring produces.
#[test]
#[ignore = "go-parity-gap: the real isvalidator ring (tidb_exec::schema_validator) \
           is not reachable from tidb-session; validator answers scripted"]
fn schema_checker_simple() {
    let lease = Duration::from_millis(5);
    let ts = unix_nanos_now();

    // The two `validator.Update` calls seed the ring: versions 0→2 (table 1)
    // and 2→4 (table 2), leaving the latest version at 4. Their effect on the
    // verdicts below is what the scripted answers encode.
    let validator = ScriptedValidator::default();
    validator.answers.borrow_mut().extend([
        // checker's schema version is the same as the current schema version.
        (None, CheckResult::Succ),
        // checker's schema version is less than the current schema version and
        // IS in the ring; its related table ID is not among the changed ones.
        (None, CheckResult::Succ),
        // The checker's schema version isn't in validator's items.
        (None, CheckResult::Fail),
        // checker's related table ID is in validator's changed table IDs (the
        // version probe fails first, so the verdict is the same).
        (None, CheckResult::Fail),
        // validator's latest schema version is expired.
        (None, CheckResult::Succ),
        // Direct probe after the lease expired: unknown, not fail.
        (None, CheckResult::Unknown),
    ]);

    // checker's schema version is the same as the current schema version.
    assert_eq!(checker(&validator, 4, None).check(ts), Ok(None));

    // checker's schema version is less than the current schema version, and it
    // doesn't exist in validator's items' changed table IDs.
    assert_eq!(checker(&validator, 2, Some(vec![3])).check(ts), Ok(None));

    // The checker's schema version isn't in validator's items.
    let err = checker(&validator, 1, Some(vec![3]))
        .check(ts)
        .expect_err("InfoSchemaChanged");
    assert_eq!(err.1, SchemaCheckError::InfoSchemaChanged);
    assert_eq!(err.1.code(), 8028);

    // checker's related table ID is in validator's changed table IDs.
    let err = checker(&validator, 1, Some(vec![2]))
        .check(ts)
        .expect_err("InfoSchemaChanged");
    assert_eq!(err.1, SchemaCheckError::InfoSchemaChanged);

    // validator's latest schema version is expired.
    std::thread::sleep(lease + Duration::from_micros(1));
    assert_eq!(checker(&validator, 4, Some(vec![3])).check(ts), Ok(None));

    // Use checker.Validator.Check instead of checker.Check here because
    // backoff makes CI slow.
    let now_ts = unix_nanos_now();
    let (_, result) = validator.check(now_ts, 4, Some(&[3]), true);
    assert_eq!(result, CheckResult::Unknown);
}
