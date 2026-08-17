// Copyright 2025 PingCAP, Inc.
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

//! Go `pkg/domain/schema_checker.go` lands complete.
//!
//! Every production symbol of that file is here: `SchemaChecker`
//! ([`SchemaChecker`]), `intSchemaVer` ([`IntSchemaVer`]),
//! `SchemaOutOfDateRetryInterval` ([`schema_out_of_date_retry_interval`] /
//! [`set_schema_out_of_date_retry_interval`]), `SchemaOutOfDateRetryTimes`
//! ([`schema_out_of_date_retry_times`] /
//! [`set_schema_out_of_date_retry_times`]), `NewSchemaChecker`
//! ([`SchemaChecker::new`]), `Check` ([`SchemaChecker::check`]), and
//! `CheckBySchemaVer` ([`SchemaChecker::check_by_schema_ver`]).
//!
//! The whole behavior of the file is one retry loop. It asks the validator
//! at most `SchemaOutOfDateRetryTimes` times; `ResultSucc` returns
//! immediately with no change, `ResultFail` returns the validator's change
//! together with `ErrInfoSchemaChanged`, and `ResultUnknown` sleeps
//! `SchemaOutOfDateRetryInterval` and asks again. Running the loop out
//! yields `ErrInfoSchemaExpired`. Note the Go quirk this reproduces: on
//! `ResultFail` the *change* is returned alongside the error, while on
//! timeout the change is dropped — so a caller that only inspects the error
//! cannot tell a same-version retry from an exhausted one.
//!
//! Narrowings, all named:
//!
//! - `// boundary:` Go `pkg/infoschema/validatorapi.Validator` — Go embeds
//!   the whole interface into `SchemaChecker` so the struct *is* a
//!   validator. Here only the one method this file calls is required, as
//!   [`SchemaValidator`]. The `pkg/infoschema/isvalidator` implementation of
//!   that interface is already ported, as
//!   `tidb_exec::schema_validator::SchemaValidator`; it is deliberately not
//!   depended on, because `tidb-exec` pulls in `tidb-planner` and
//!   `tidb-executor`, and `pkg/domain` must stay below them. The `domain.go`
//!   batch should collapse the two by writing a blanket impl of this trait
//!   for `tidb_exec::schema_validator::Validator`.
//! - `// boundary:` Go
//!   `github.com/tikv/client-go/v2/txnkv/transaction.RelatedSchemaChange` —
//!   re-declared locally as [`RelatedSchemaChange`] for the same reason;
//!   field-for-field identical to
//!   `tidb_exec::schema_validator::RelatedSchemaChange`.
//! - `// boundary:` Go `pkg/infoschema/validatorapi.Result` — re-declared as
//!   [`CheckResult`], same three variants.
//! - `// boundary:` Go `github.com/tikv/client-go/v2/tikv.SchemaVer` — the
//!   one-method interface `CheckBySchemaVer` takes. Reproduced as
//!   [`SchemaVer`], with `intSchemaVer` as [`IntSchemaVer`].
//! - `// boundary:` Go `pkg/metrics.SchemaLeaseErrorCounter` — the
//!   `"changed"` and `"outdated"` increments are pure telemetry and change
//!   no result; dropped.
//! - `// boundary:` Go `pkg/domain/domain.go` `ErrInfoSchemaChanged` /
//!   `ErrInfoSchemaExpired` (`domain.go:3012-3016`). `domain.go` is a later
//!   batch, but this file cannot return anything else, so the two errors are
//!   declared here as [`SchemaCheckError`] and will move when `domain.go`
//!   lands. Codes and message text come from `tidb_error`, and
//!   `ErrInfoSchemaChanged` keeps Go's appended `". [try again later]"`
//!   (Go `kv.TxnRetryableMark`, `pkg/kv/error.go:27`).
//! - Ambient sleeping is injected. Go calls `time.Sleep` inline; here the
//!   caller supplies a [`RetrySleeper`], defaulting to [`ThreadSleeper`]
//!   which is exactly `time.Sleep`. Without this the retry loop could not be
//!   tested without burning wall-clock seconds.

use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::Duration;

use tidb_error::tidb::{errcode, errname};

/// Go `validatorapi.Result`: the verdict of one validator check.
///
/// boundary: Go `pkg/infoschema/validatorapi.Result`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CheckResult {
    /// Go `ResultSucc`: the check passes.
    #[default]
    Succ,
    /// Go `ResultFail`: the schema changed under the transaction.
    Fail,
    /// Go `ResultUnknown`: the validator cannot tell yet — retry.
    Unknown,
}

/// The schema change the validator reports alongside a failed check.
///
/// boundary: Go
/// `github.com/tikv/client-go/v2/txnkv/transaction.RelatedSchemaChange`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelatedSchemaChange {
    /// Go `PhyTblIDS`: physical table IDs the change touched.
    pub phy_tbl_ids: Vec<i64>,
    /// Go `ActionTypes`: the DDL action type per entry of `phy_tbl_ids`.
    pub action_types: Vec<u64>,
    /// Go `Amendable`. Carried for shape; this file never reads it.
    pub amendable: bool,
}

/// Go `tikv.SchemaVer`: anything that can name a schema version.
///
/// boundary: Go `github.com/tikv/client-go/v2/tikv.SchemaVer`.
pub trait SchemaVer {
    /// Go `SchemaMetaVersion`.
    fn schema_meta_version(&self) -> i64;
}

/// Go `intSchemaVer`: a bare `int64` used as a [`SchemaVer`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct IntSchemaVer(pub i64);

impl SchemaVer for IntSchemaVer {
    fn schema_meta_version(&self) -> i64 {
        self.0
    }
}

/// The single validator method `schema_checker.go` calls.
///
/// boundary: Go `pkg/infoschema/validatorapi.Validator` — narrowed to
/// `Check`, the only method this file uses. See the module doc for why the
/// already-ported `tidb_exec::schema_validator::Validator` is not reused
/// directly.
pub trait SchemaValidator {
    /// Go `Validator.Check`.
    ///
    /// `None` for `related_physical_table_ids` is Go's nil slice, which
    /// means "compare versions only"; `Some(&[])` is Go's empty slice.
    fn check(
        &self,
        txn_ts: u64,
        schema_ver: i64,
        related_physical_table_ids: Option<&[i64]>,
        need_check_schema_by_delta: bool,
    ) -> (Option<RelatedSchemaChange>, CheckResult);
}

/// Go `time.Sleep`, made injectable so the retry loop is testable.
///
/// boundary: Go `time.Sleep` as called by `CheckBySchemaVer`.
pub trait RetrySleeper {
    /// Block for `d`.
    fn sleep(&self, d: Duration);
}

/// The production [`RetrySleeper`]: exactly Go's `time.Sleep`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ThreadSleeper;

impl RetrySleeper for ThreadSleeper {
    fn sleep(&self, d: Duration) {
        std::thread::sleep(d);
    }
}

/// The two errors `schema_checker.go` can return.
///
/// boundary: Go `pkg/domain/domain.go:3012-3016` `ErrInfoSchemaExpired` and
/// `ErrInfoSchemaChanged`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaCheckError {
    /// Go `ErrInfoSchemaChanged` (8028). Go builds it with `NewStdErr` and a
    /// message that appends `". " + kv.TxnRetryableMark`.
    InfoSchemaChanged,
    /// Go `ErrInfoSchemaExpired` (8027), a plain `NewStd`.
    InfoSchemaExpired,
}

impl SchemaCheckError {
    /// The MySQL error code, as `dbterror.ClassDomain.NewStd*` assigns it.
    #[must_use]
    pub fn code(self) -> u16 {
        match self {
            Self::InfoSchemaChanged => errcode::ErrInfoSchemaChanged,
            Self::InfoSchemaExpired => errcode::ErrInfoSchemaExpired,
        }
    }

    /// The rendered message. Neither error takes arguments, so the raw
    /// catalog template is the final text.
    #[must_use]
    pub fn message(self) -> String {
        match self {
            // Go: `errno.MySQLErrName[ErrInfoSchemaChanged].Raw + ". " +
            // kv.TxnRetryableMark`.
            Self::InfoSchemaChanged => {
                format!(
                    "{}. {TXN_RETRYABLE_MARK}",
                    errname::ErrInfoSchemaChanged.raw
                )
            }
            Self::InfoSchemaExpired => errname::ErrInfoSchemaExpired.raw.to_owned(),
        }
    }
}

/// Go `kv.TxnRetryableMark` (`pkg/kv/error.go:27`).
///
/// boundary: Go `pkg/kv.TxnRetryableMark` — the one constant this file needs
/// out of `pkg/kv`, which is otherwise unrelated to `pkg/domain`.
pub const TXN_RETRYABLE_MARK: &str = "[try again later]";

impl std::fmt::Display for SchemaCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for SchemaCheckError {}

/// Go `SchemaOutOfDateRetryInterval`, in nanoseconds.
///
/// Go holds a `*atomic.Duration` package variable; the same process-global
/// mutability is reproduced, since callers (and tests) tune it at runtime.
static SCHEMA_OUT_OF_DATE_RETRY_INTERVAL_NANOS: AtomicU64 = AtomicU64::new(500_000_000);

/// Go `SchemaOutOfDateRetryTimes`.
static SCHEMA_OUT_OF_DATE_RETRY_TIMES: AtomicI32 = AtomicI32::new(10);

/// Go `SchemaOutOfDateRetryInterval.Load()`: the backoff before retrying.
#[must_use]
pub fn schema_out_of_date_retry_interval() -> Duration {
    Duration::from_nanos(SCHEMA_OUT_OF_DATE_RETRY_INTERVAL_NANOS.load(Ordering::SeqCst))
}

/// Go `SchemaOutOfDateRetryInterval.Store(..)`.
pub fn set_schema_out_of_date_retry_interval(d: Duration) {
    let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
    SCHEMA_OUT_OF_DATE_RETRY_INTERVAL_NANOS.store(nanos, Ordering::SeqCst);
}

/// Go `SchemaOutOfDateRetryTimes.Load()`: the max retry count when the
/// schema is out of date.
#[must_use]
pub fn schema_out_of_date_retry_times() -> i32 {
    SCHEMA_OUT_OF_DATE_RETRY_TIMES.load(Ordering::SeqCst)
}

/// Go `SchemaOutOfDateRetryTimes.Store(..)`.
pub fn set_schema_out_of_date_retry_times(times: i32) {
    SCHEMA_OUT_OF_DATE_RETRY_TIMES.store(times, Ordering::SeqCst);
}

/// Go `SchemaChecker`: checks schema-validity for one transaction.
///
/// Go embeds `validatorapi.Validator` so a `*SchemaChecker` is itself a
/// validator; here the validator is a borrowed field, since the embedded
/// interface's other methods are never called through the checker in
/// production code.
pub struct SchemaChecker<'a, V: SchemaValidator + ?Sized, S: RetrySleeper = ThreadSleeper> {
    /// Go's embedded `validatorapi.Validator`.
    pub validator: &'a V,
    /// Go `schemaVer`.
    pub schema_ver: i64,
    /// Go `relatedTableIDs`. `None` is Go's nil slice — see
    /// [`SchemaValidator::check`].
    pub related_table_ids: Option<Vec<i64>>,
    /// Go `needCheckSchemaByDelta`.
    pub need_check_schema_by_delta: bool,
    /// boundary: Go `time.Sleep`.
    pub sleeper: S,
}

impl<'a, V: SchemaValidator + ?Sized> SchemaChecker<'a, V, ThreadSleeper> {
    /// Go `NewSchemaChecker`.
    #[must_use]
    pub fn new(
        validator: &'a V,
        schema_ver: i64,
        related_table_ids: Option<Vec<i64>>,
        need_check_schema_by_delta: bool,
    ) -> Self {
        Self {
            validator,
            schema_ver,
            related_table_ids,
            need_check_schema_by_delta,
            sleeper: ThreadSleeper,
        }
    }
}

impl<V: SchemaValidator + ?Sized, S: RetrySleeper> SchemaChecker<'_, V, S> {
    /// Go `SchemaChecker.Check`: check at the checker's own schema version.
    ///
    /// # Errors
    /// [`SchemaCheckError::InfoSchemaChanged`] when the validator reports a
    /// conflicting change, [`SchemaCheckError::InfoSchemaExpired`] when the
    /// retries run out.
    pub fn check(
        &self,
        txn_ts: u64,
    ) -> Result<Option<RelatedSchemaChange>, (Option<RelatedSchemaChange>, SchemaCheckError)> {
        self.check_by_schema_ver(txn_ts, IntSchemaVer(self.schema_ver))
    }

    /// Go `SchemaChecker.CheckBySchemaVer`.
    ///
    /// Go returns `(change, err)` as a pair, so the change survives a
    /// `ResultFail`; that pairing is preserved in the `Err` variant rather
    /// than discarded.
    ///
    /// # Errors
    /// As [`SchemaChecker::check`].
    pub fn check_by_schema_ver<T: SchemaVer>(
        &self,
        txn_ts: u64,
        start_schema_ver: T,
    ) -> Result<Option<RelatedSchemaChange>, (Option<RelatedSchemaChange>, SchemaCheckError)> {
        let retry_interval = schema_out_of_date_retry_interval();
        let retry_times = schema_out_of_date_retry_times();
        // Go's `for range schemaOutOfDateRetryTimes` over an int: a
        // non-positive count runs the body zero times and falls straight
        // through to ErrInfoSchemaExpired.
        let mut remaining = retry_times;
        while remaining > 0 {
            remaining -= 1;
            let (related_change, check_result) = self.validator.check(
                txn_ts,
                start_schema_ver.schema_meta_version(),
                self.related_table_ids.as_deref(),
                self.need_check_schema_by_delta,
            );
            match check_result {
                CheckResult::Succ => return Ok(None),
                // boundary: Go `metrics.SchemaLeaseErrorCounter` "changed".
                CheckResult::Fail => {
                    return Err((related_change, SchemaCheckError::InfoSchemaChanged))
                }
                CheckResult::Unknown => self.sleeper.sleep(retry_interval),
            }
        }
        // boundary: Go `metrics.SchemaLeaseErrorCounter` "outdated".
        Err((None, SchemaCheckError::InfoSchemaExpired))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::{Mutex, MutexGuard};

    use super::*;

    /// `SchemaOutOfDateRetryInterval` and `SchemaOutOfDateRetryTimes` are
    /// process-global in Go and stay process-global here, so every test that
    /// reads or writes them serializes on this and restores the Go defaults
    /// on the way out.
    static KNOBS: Mutex<()> = Mutex::new(());

    struct Knobs(#[allow(dead_code)] MutexGuard<'static, ()>);

    impl Knobs {
        fn lock() -> Self {
            Self(
                KNOBS
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            )
        }
    }

    impl Drop for Knobs {
        fn drop(&mut self) {
            set_schema_out_of_date_retry_interval(Duration::from_millis(500));
            set_schema_out_of_date_retry_times(10);
        }
    }

    /// A validator that replays a scripted sequence of verdicts.
    ///
    /// `TestSchemaCheckerSimple` in `pkg/domain/schema_checker_test.go`
    /// drives a real `isvalidator.New(lease)`, whose delta-ring semantics
    /// are already covered by `tidb_exec::schema_validator`'s own source
    /// tests. What `schema_checker.go` itself owns is the retry loop over
    /// whatever the validator answers, so these tests script the answers
    /// instead of re-deriving them.
    /// One recorded `check` call: `(txn_ts, schema_ver, related_table_ids,
    /// need_check_schema_by_delta)`.
    type RecordedCall = (u64, i64, Option<Vec<i64>>, bool);

    #[derive(Default)]
    struct ScriptedValidator {
        script: RefCell<Vec<(Option<RelatedSchemaChange>, CheckResult)>>,
        calls: RefCell<Vec<RecordedCall>>,
    }

    impl ScriptedValidator {
        fn new(script: Vec<(Option<RelatedSchemaChange>, CheckResult)>) -> Self {
            Self {
                // Popped from the back, so store reversed.
                script: RefCell::new(script.into_iter().rev().collect()),
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl SchemaValidator for ScriptedValidator {
        fn check(
            &self,
            txn_ts: u64,
            schema_ver: i64,
            related_physical_table_ids: Option<&[i64]>,
            need_check_schema_by_delta: bool,
        ) -> (Option<RelatedSchemaChange>, CheckResult) {
            self.calls.borrow_mut().push((
                txn_ts,
                schema_ver,
                related_physical_table_ids.map(<[i64]>::to_vec),
                need_check_schema_by_delta,
            ));
            self.script
                .borrow_mut()
                .pop()
                // Running off the end of the script keeps answering
                // Unknown, which is what an unresponsive validator does.
                .unwrap_or((None, CheckResult::Unknown))
        }
    }

    #[derive(Default)]
    struct CountingSleeper {
        slept: RefCell<Vec<Duration>>,
    }

    impl RetrySleeper for CountingSleeper {
        fn sleep(&self, d: Duration) {
            self.slept.borrow_mut().push(d);
        }
    }

    fn checker<'a>(
        v: &'a ScriptedValidator,
        s: CountingSleeper,
        schema_ver: i64,
        related: Option<Vec<i64>>,
    ) -> SchemaChecker<'a, ScriptedValidator, CountingSleeper> {
        SchemaChecker {
            validator: v,
            schema_ver,
            related_table_ids: related,
            need_check_schema_by_delta: true,
            sleeper: s,
        }
    }

    #[test]
    fn succ_returns_no_change_on_first_try() {
        let _knobs = Knobs::lock();
        let v = ScriptedValidator::new(vec![(None, CheckResult::Succ)]);
        let c = checker(&v, CountingSleeper::default(), 4, Some(vec![3]));
        assert_eq!(c.check(1234), Ok(None));
        assert!(c.sleeper.slept.borrow().is_empty());
        assert_eq!(
            *c.validator.calls.borrow(),
            vec![(1234, 4, Some(vec![3]), true)]
        );
    }

    #[test]
    fn fail_returns_changed_error_with_the_change() {
        let _knobs = Knobs::lock();
        let change = RelatedSchemaChange {
            phy_tbl_ids: vec![2],
            action_types: vec![2],
            amendable: false,
        };
        let v = ScriptedValidator::new(vec![(Some(change.clone()), CheckResult::Fail)]);
        let c = checker(&v, CountingSleeper::default(), 1, Some(vec![2]));
        assert_eq!(
            c.check(9),
            Err((Some(change), SchemaCheckError::InfoSchemaChanged))
        );
    }

    #[test]
    fn unknown_backs_off_then_succeeds() {
        let _knobs = Knobs::lock();
        set_schema_out_of_date_retry_interval(Duration::from_millis(7));
        let v = ScriptedValidator::new(vec![
            (None, CheckResult::Unknown),
            (None, CheckResult::Unknown),
            (None, CheckResult::Succ),
        ]);
        let c = checker(&v, CountingSleeper::default(), 4, Some(vec![3]));
        assert_eq!(c.check(1), Ok(None));
        assert_eq!(
            *c.sleeper.slept.borrow(),
            vec![Duration::from_millis(7), Duration::from_millis(7)]
        );
    }

    #[test]
    fn exhausting_retries_yields_expired_and_drops_the_change() {
        let _knobs = Knobs::lock();
        set_schema_out_of_date_retry_interval(Duration::ZERO);
        set_schema_out_of_date_retry_times(3);
        // Every answer carries a change, and every one is Unknown, so the
        // loop runs out. Go returns `nil, ErrInfoSchemaExpired` here — the
        // change is dropped even though the validator produced one.
        let change = RelatedSchemaChange {
            phy_tbl_ids: vec![7],
            ..RelatedSchemaChange::default()
        };
        let v = ScriptedValidator::new(vec![(Some(change), CheckResult::Unknown)]);
        let c = checker(&v, CountingSleeper::default(), 4, None);
        assert_eq!(c.check(1), Err((None, SchemaCheckError::InfoSchemaExpired)));
        assert_eq!(c.validator.calls.borrow().len(), 3);
        assert_eq!(c.sleeper.slept.borrow().len(), 3);
    }

    #[test]
    fn zero_retry_times_never_asks_the_validator() {
        let _knobs = Knobs::lock();
        set_schema_out_of_date_retry_times(0);
        let v = ScriptedValidator::new(vec![(None, CheckResult::Succ)]);
        let c = checker(&v, CountingSleeper::default(), 4, None);
        assert_eq!(c.check(1), Err((None, SchemaCheckError::InfoSchemaExpired)));
        assert!(c.validator.calls.borrow().is_empty());
    }

    #[test]
    fn check_by_schema_ver_overrides_the_checkers_own_version() {
        let _knobs = Knobs::lock();
        let v = ScriptedValidator::new(vec![(None, CheckResult::Succ)]);
        let c = checker(&v, CountingSleeper::default(), 4, None);
        assert_eq!(c.check_by_schema_ver(5, IntSchemaVer(99)), Ok(None));
        assert_eq!(c.validator.calls.borrow()[0].1, 99);
    }

    #[test]
    fn nil_related_table_ids_reaches_the_validator_as_none() {
        let _knobs = Knobs::lock();
        let v = ScriptedValidator::new(vec![(None, CheckResult::Succ)]);
        let c = checker(&v, CountingSleeper::default(), 4, None);
        assert_eq!(c.check(1), Ok(None));
        assert_eq!(c.validator.calls.borrow()[0].2, None);
        // An empty slice is distinct from nil, and stays distinct.
        let v2 = ScriptedValidator::new(vec![(None, CheckResult::Succ)]);
        let c2 = checker(&v2, CountingSleeper::default(), 4, Some(vec![]));
        assert_eq!(c2.check(1), Ok(None));
        assert_eq!(c2.validator.calls.borrow()[0].2, Some(vec![]));
    }

    #[test]
    fn int_schema_ver_is_the_identity() {
        assert_eq!(IntSchemaVer(-3).schema_meta_version(), -3);
        assert_eq!(IntSchemaVer(i64::MAX).schema_meta_version(), i64::MAX);
    }

    #[test]
    fn error_codes_and_messages_match_the_catalog() {
        assert_eq!(SchemaCheckError::InfoSchemaExpired.code(), 8027);
        assert_eq!(SchemaCheckError::InfoSchemaChanged.code(), 8028);
        assert!(SchemaCheckError::InfoSchemaExpired
            .message()
            .starts_with("Information schema is out of date"));
        assert!(SchemaCheckError::InfoSchemaChanged
            .message()
            .ends_with(". [try again later]"));
    }

    #[test]
    fn retry_knobs_round_trip() {
        let _knobs = Knobs::lock();
        set_schema_out_of_date_retry_interval(Duration::from_millis(123));
        assert_eq!(
            schema_out_of_date_retry_interval(),
            Duration::from_millis(123)
        );
        set_schema_out_of_date_retry_interval(Duration::from_millis(500));
        assert_eq!(schema_out_of_date_retry_times(), 10);
    }
}
