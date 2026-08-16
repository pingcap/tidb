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

//! Go `pkg/infoschema/isvalidator` lands as a complete package: the schema
//! validator that decides whether a transaction may commit with the schema
//! version it read, together with the small `pkg/infoschema/validatorapi`
//! vocabulary it answers in.
//!
//! Every production symbol is here. From `isvalidator/validator.go`:
//! `deltaSchemaInfo` ([`DeltaSchemaInfo`]), `validator`
//! ([`SchemaValidator`]), `New`, `IsStarted`, `Stop`, `Restart`, `Reset`,
//! `Update`, `IsLeaseExpired`, `isRelatedTablesChanged`, `findNewerDeltas`,
//! `Check`, `enqueue`, and `containIn`. From `validatorapi/interface.go`:
//! `Result` with `ResultSucc`/`ResultFail`/`ResultUnknown` ([`Result`]) and
//! the `Validator` interface ([`Validator`]). `validatorapi` is 50 lines of
//! result enum plus the one interface this file implements, with no other
//! implementor in the tree, so it stays in this module rather than earning
//! one of its own.
//!
//! The whole point of the type is the delta ring: [`SchemaValidator::update`]
//! appends one [`DeltaSchemaInfo`] per version bump, keeping the queue
//! ordered by ascending schema version, merging a new delta over the last one
//! when the last is contained in it, and evicting from the front past
//! `tidb_max_delta_schema_count`. A version older than everything still in
//! the ring is indistinguishable from a version whose deltas were evicted, so
//! both answer "changed" — [`Result::Fail`] — rather than risk a stale read.
//!
//! Narrowings, all named:
//!
//! - `// boundary:` Go
//!   `github.com/tikv/client-go/v2/txnkv/transaction.RelatedSchemaChange` —
//!   the one external type, reproduced locally as
//!   [`RelatedSchemaChange`] with its three fields (`PhyTblIDS`,
//!   `ActionTypes`, `Amendable`). Only the first two are read here; Go's
//!   `Check` likewise never populates the returned pointer, so the returned
//!   `Option` is always `None`, exactly as Go always returns `nil`.
//! - `// boundary:` Go `pkg/metrics` — `LoadSchemaCounter` /
//!   `LeaseExpireTime` increments in `Stop`, `Restart`, `Reset`, `Update`,
//!   and `isRelatedTablesChanged` are dropped; they are pure telemetry and
//!   change no result. So is every `logutil.BgLogger()` call, and with it
//!   the `changedSchemaVers` slice `isRelatedTablesChanged` builds only to
//!   log.
//! - `// boundary:` Go `pkg/sessionctx/vardef` — `GetMaxDeltaSchemaCount()`
//!   and `IsMDLEnabled()` are process-global mutable state read mid-method.
//!   Both become explicit validator state
//!   ([`SchemaValidator::set_max_delta_schema_count`],
//!   [`SchemaValidator::set_mdl_enabled`]) seeded from
//!   `DEF_TIDB_MAX_DELTA_SCHEMA_COUNT` and Go's `enableMDL` zero value, so a
//!   validator's answers depend only on that validator.
//! - Ambient time is injected. Go reads `time.Now()` in `IsLeaseExpired`;
//!   here the caller passes the instant
//!   ([`SchemaValidator::is_lease_expired`]). Timestamps that Go derives
//!   with `oracle.GetTimeFromTS` are still derived the same way, from the
//!   TSO the caller supplies — see [`get_time_from_ts`].

use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::Duration;

use tidb_vardef::defaults::DEF_TIDB_MAX_DELTA_SCHEMA_COUNT;

/// One millisecond, in the nanosecond unit this module keeps instants in.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Bits `oracle.ComposeTS` shifts the physical millisecond by.
const PHYSICAL_SHIFT_BITS: u32 = 18;

/// Go `oracle.GetTimeFromTS`: the physical half of a TSO, as nanoseconds
/// since the Unix epoch.
///
/// boundary: Go `github.com/tikv/client-go/v2/oracle.GetTimeFromTS`. A TSO
/// carries physical milliseconds in its high bits and a logical counter in
/// the low 18; the logical half is discarded, so two TSOs from the same
/// millisecond compare equal here exactly as they do in Go.
#[must_use]
pub fn get_time_from_ts(ts: u64) -> i64 {
    let physical_ms = i64::try_from(ts >> PHYSICAL_SHIFT_BITS).unwrap_or(i64::MAX);
    physical_ms.saturating_mul(NANOS_PER_MILLI)
}

/// The schema change carried alongside a version bump.
///
/// boundary: Go
/// `github.com/tikv/client-go/v2/txnkv/transaction.RelatedSchemaChange`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelatedSchemaChange {
    /// Go `PhyTblIDS`: physical table IDs the change touched.
    pub phy_tbl_ids: Vec<i64>,
    /// Go `ActionTypes`: the DDL action type per entry of `phy_tbl_ids`.
    pub action_types: Vec<u64>,
    /// Go `Amendable`. Carried for shape; the validator never reads it.
    pub amendable: bool,
}

/// Go `validatorapi.Result`: the verdict of a schema check.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Result {
    /// Go `ResultSucc`: the check passes.
    #[default]
    Succ,
    /// Go `ResultFail`: the check fails.
    Fail,
    /// Go `ResultUnknown`: the validator cannot tell — it is stopped, or the
    /// lease covering the latest version has expired.
    Unknown,
}

/// Go `deltaSchemaInfo`: one schema version and the tables it changed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeltaSchemaInfo {
    /// Go `schemaVersion`.
    pub schema_version: i64,
    /// Go `relatedIDs`.
    pub related_ids: Vec<i64>,
    /// Go `relatedActions`, positionally paired with `related_ids`.
    pub related_actions: Vec<u64>,
}

/// Go `containIn`: whether `last_delta` is included in `cur_delta`,
/// considering table ID and action type together.
///
/// Go indexes `relatedActions` by the `relatedIDs` position, which panics if
/// the two ever differ in length; pairing them by iteration keeps the same
/// answer for the well-formed case without that edge.
fn contain_in(last_delta: &DeltaSchemaInfo, cur_delta: &DeltaSchemaInfo) -> bool {
    if last_delta.related_ids.len() > cur_delta.related_ids.len() {
        return false;
    }
    last_delta
        .related_ids
        .iter()
        .zip(&last_delta.related_actions)
        .all(|(last_tbl_id, last_action)| {
            cur_delta
                .related_ids
                .iter()
                .zip(&cur_delta.related_actions)
                .any(|(cur_tbl_id, cur_action)| {
                    last_tbl_id == cur_tbl_id && last_action == cur_action
                })
        })
}

/// Go's `1 << actionType` over a `uint64`, whose own comment records the
/// intent: "if actionType >= 64, the value of left shift equals 0".
fn action_bit(action_type: u64) -> u64 {
    u32::try_from(action_type)
        .ok()
        .and_then(|shift| 1u64.checked_shl(shift))
        .unwrap_or(0)
}

/// The mutable half of Go's `validator`, i.e. everything its `sync.RWMutex`
/// guards.
#[derive(Debug)]
struct ValidatorState {
    /// Go `isStarted`.
    is_started: bool,
    /// Go `lease`.
    lease: Duration,
    /// Go `latestSchemaVer`.
    latest_schema_ver: i64,
    /// Go `restartSchemaVer`.
    restart_schema_ver: i64,
    /// Go `latestSchemaExpire`, as nanoseconds since the Unix epoch.
    latest_schema_expire: i64,
    /// Go `deltaSchemaInfos`: the history of changes, ordered by ascending
    /// schema version.
    delta_schema_infos: Vec<DeltaSchemaInfo>,
    /// boundary: Go `vardef.GetMaxDeltaSchemaCount()`.
    max_delta_schema_count: i64,
    /// boundary: Go `vardef.IsMDLEnabled()`.
    mdl_enabled: bool,
}

impl ValidatorState {
    /// Go `findNewerDeltas`: the suffix of the queue strictly newer than
    /// `curr_ver`. The queue is version-ordered, so this is a scan back from
    /// the tail.
    fn find_newer_deltas(&self, curr_ver: i64) -> &[DeltaSchemaInfo] {
        let queue = &self.delta_schema_infos;
        let mut pos = queue.len();
        for index in (0..queue.len()).rev() {
            if queue[index].schema_version <= curr_ver {
                break;
            }
            pos = index;
        }
        &queue[pos..]
    }

    /// Go `isRelatedTablesChanged`: whether any of `table_ids` changed
    /// between `curr_ver` and the latest version. A table ID of `-1` matches
    /// every changed table.
    ///
    /// Two cases answer "changed" without inspecting a single table: an empty
    /// history, and a `curr_ver` older than every delta still retained — in
    /// the latter the deltas that would have decided it may have been
    /// evicted, so the safe answer is the conservative one.
    fn is_related_tables_changed(&self, curr_ver: i64, table_ids: Option<&[i64]>) -> bool {
        if self.delta_schema_infos.is_empty() {
            return true;
        }
        let newer_deltas = self.find_newer_deltas(curr_ver);
        if newer_deltas.len() == self.delta_schema_infos.len() {
            return true;
        }

        let table_ids = table_ids.unwrap_or(&[]);
        let mut changed_tbl_map: BTreeMap<i64, u64> = BTreeMap::new();
        for item in newer_deltas {
            for (tbl_id, action) in item.related_ids.iter().zip(&item.related_actions) {
                for related_tbl_id in table_ids {
                    if tbl_id == related_tbl_id || *related_tbl_id == -1 {
                        *changed_tbl_map.entry(*tbl_id).or_default() |= action_bit(*action);
                    }
                }
            }
        }
        !changed_tbl_map.is_empty()
    }

    /// Go `enqueue`: append `schema_version`'s delta, merging and evicting.
    ///
    /// The last entry is overwritten when it is contained in the new delta,
    /// which keeps the queue short without losing coverage. The first entry
    /// is deliberately never merged away, so the queue keeps reaching as far
    /// back as it can.
    fn enqueue(&mut self, schema_version: i64, change: Option<&RelatedSchemaChange>) {
        let max_cnt = self.max_delta_schema_count;
        if max_cnt <= 0 {
            return;
        }

        let delta = DeltaSchemaInfo {
            schema_version,
            related_ids: change.map(|c| c.phy_tbl_ids.clone()).unwrap_or_default(),
            related_actions: change.map(|c| c.action_types.clone()).unwrap_or_default(),
        };
        if self.delta_schema_infos.is_empty() {
            self.delta_schema_infos.push(delta);
            return;
        }

        let last_offset = self.delta_schema_infos.len() - 1;
        if last_offset != 0 && contain_in(&self.delta_schema_infos[last_offset], &delta) {
            self.delta_schema_infos[last_offset] = delta;
        } else {
            self.delta_schema_infos.push(delta);
        }

        if self.delta_schema_infos.len() > usize::try_from(max_cnt).unwrap_or(usize::MAX) {
            self.delta_schema_infos.remove(0);
        }
    }
}

/// Go `validator`: the schema validator behind `validatorapi.Validator`.
#[derive(Debug)]
pub struct SchemaValidator {
    state: RwLock<ValidatorState>,
}

impl SchemaValidator {
    /// Go `New`. The lease must be positive, as Go's `intest.Assert` says.
    #[must_use]
    pub fn new(lease: Duration) -> Self {
        debug_assert!(!lease.is_zero(), "lease should be greater than 0");
        Self {
            state: RwLock::new(ValidatorState {
                is_started: true,
                lease,
                latest_schema_ver: 0,
                restart_schema_ver: 0,
                latest_schema_expire: 0,
                delta_schema_infos: Vec::with_capacity(
                    usize::try_from(DEF_TIDB_MAX_DELTA_SCHEMA_COUNT).unwrap_or_default(),
                ),
                max_delta_schema_count: DEF_TIDB_MAX_DELTA_SCHEMA_COUNT,
                mdl_enabled: false,
            }),
        }
    }

    /// boundary: Go `vardef.SetMaxDeltaSchemaCount`. Sets how many deltas the
    /// ring retains; a non-positive count makes [`Validator::update`] retain
    /// none.
    pub fn set_max_delta_schema_count(&self, count: i64) {
        self.write().max_delta_schema_count = count;
    }

    /// boundary: Go `vardef.SetEnableMDL`. With metadata locking on, DDL
    /// waits for the transaction instead, so [`Validator::check`] skips the
    /// delta scan unless the caller asks for it.
    pub fn set_mdl_enabled(&self, enabled: bool) {
        self.write().mdl_enabled = enabled;
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, ValidatorState> {
        self.state.read().expect("schema validator state")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, ValidatorState> {
        self.state.write().expect("schema validator state")
    }
}

/// Go `validatorapi.Validator`: checking the validity of a schema version.
pub trait Validator {
    /// Go `Update`: renew the lease at `curr_ver` and, when the version
    /// moved, record `change` as a new delta.
    fn update(
        &self,
        lease_grant_ts: u64,
        old_ver: i64,
        curr_ver: i64,
        change: Option<&RelatedSchemaChange>,
    );

    /// Go `Check`: whether committing at `txn_ts` with `schema_ver` and
    /// `related_physical_table_ids` is safe.
    ///
    /// `None` table IDs is Go's nil slice, which means "only compare schema
    /// versions" and fails outright on any version change; `Some(&[])` is
    /// Go's empty slice, reached by transactions touching only temporary
    /// tables.
    fn check(
        &self,
        txn_ts: u64,
        schema_ver: i64,
        related_physical_table_ids: Option<&[i64]>,
        need_check_schema_by_delta: bool,
    ) -> (Option<RelatedSchemaChange>, Result);

    /// Go `Stop`: stop checking, and forget both the latest version and the
    /// delta history.
    fn stop(&self);

    /// Go `Restart`: start checking again, remembering `curr_schema_ver` as
    /// the floor below which a transaction's version is too old to trust.
    fn restart(&self, curr_schema_ver: i64);

    /// Go `Reset`: back to the initial state, including the restart floor.
    fn reset(&self);

    /// Go `IsStarted`.
    fn is_started(&self) -> bool;

    /// Go `IsLeaseExpired`, with Go's `time.Now()` supplied by the caller as
    /// nanoseconds since the Unix epoch.
    fn is_lease_expired(&self, now_unix_nanos: i64) -> bool;
}

impl Validator for SchemaValidator {
    fn update(
        &self,
        lease_grant_ts: u64,
        old_ver: i64,
        curr_ver: i64,
        change: Option<&RelatedSchemaChange>,
    ) {
        let mut state = self.write();
        if !state.is_started {
            return;
        }

        // Renew the lease. Go trims a millisecond so the local expiry stays
        // inside the grant the owner handed out.
        state.latest_schema_ver = curr_ver;
        let lease_nanos = i64::try_from(state.lease.as_nanos()).unwrap_or(i64::MAX);
        state.latest_schema_expire = get_time_from_ts(lease_grant_ts)
            .saturating_add(lease_nanos)
            .saturating_sub(NANOS_PER_MILLI);

        if curr_ver != old_ver {
            state.enqueue(curr_ver, change);
        }
    }

    fn check(
        &self,
        txn_ts: u64,
        schema_ver: i64,
        related_physical_table_ids: Option<&[i64]>,
        need_check_schema_by_delta: bool,
    ) -> (Option<RelatedSchemaChange>, Result) {
        let state = self.read();
        if !state.is_started {
            return (None, Result::Unknown);
        }

        if schema_ver < state.restart_schema_ver {
            // TiDB and PD may have been unhealthy since the transaction
            // started; the version predates the reconnect.
            return (None, Result::Fail);
        }

        if schema_ver < state.latest_schema_ver {
            // A nil slice means the caller only wants the version compared —
            // e.g. a transaction whose DDL failed, asking whether a schema
            // change caused it.
            if related_physical_table_ids.is_none() {
                return (None, Result::Fail);
            }

            // `need_check_schema_by_delta` is false exactly when MDL was on,
            // and the MDL read covers MDL being switched mid-transaction in
            // either direction.
            if (need_check_schema_by_delta || !state.mdl_enabled)
                && state.is_related_tables_changed(schema_ver, related_physical_table_ids)
            {
                return (None, Result::Fail);
            }
            return (None, Result::Succ);
        }

        // Schema unchanged: success, unless the lease that vouched for it has
        // run out, in which case the validator cannot vouch either.
        if get_time_from_ts(txn_ts) > state.latest_schema_expire {
            return (None, Result::Unknown);
        }
        (None, Result::Succ)
    }

    fn stop(&self) {
        let mut state = self.write();
        state.is_started = false;
        state.latest_schema_ver = 0;
        state.delta_schema_infos.clear();
    }

    fn restart(&self, curr_schema_ver: i64) {
        let mut state = self.write();
        state.is_started = true;
        // Recording the latest version after the reload aborts write
        // transactions still holding a stale one before they commit. Read-only
        // transactions keep the problem.
        state.restart_schema_ver = curr_schema_ver;
    }

    fn reset(&self) {
        let mut state = self.write();
        state.is_started = true;
        state.latest_schema_ver = 0;
        state.delta_schema_infos.clear();
        state.restart_schema_ver = 0;
    }

    fn is_started(&self) -> bool {
        self.read().is_started
    }

    fn is_lease_expired(&self, now_unix_nanos: i64) -> bool {
        now_unix_nanos > self.read().latest_schema_expire
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `validator_test.go` reaches into unexported fields and calls
    // unexported methods from inside the package; these tests keep that
    // access by living in the module.
    impl SchemaValidator {
        fn delta_schema_infos(&self) -> Vec<DeltaSchemaInfo> {
            self.read().delta_schema_infos.clone()
        }

        fn latest_schema_ver(&self) -> i64 {
            self.read().latest_schema_ver
        }

        fn restart_schema_ver(&self) -> i64 {
            self.read().restart_schema_ver
        }

        fn is_related_tables_changed(&self, curr_ver: i64, table_ids: Option<&[i64]>) -> bool {
            self.read().is_related_tables_changed(curr_ver, table_ids)
        }

        fn enqueue(&self, schema_version: i64, change: Option<&RelatedSchemaChange>) {
            self.write().enqueue(schema_version, change);
        }
    }

    fn change(ids: &[i64], actions: &[u64]) -> RelatedSchemaChange {
        RelatedSchemaChange {
            phy_tbl_ids: ids.to_vec(),
            action_types: actions.to_vec(),
            amendable: false,
        }
    }

    fn delta(schema_version: i64, ids: &[i64], actions: &[u64]) -> DeltaSchemaInfo {
        DeltaSchemaInfo {
            schema_version,
            related_ids: ids.to_vec(),
            related_actions: actions.to_vec(),
        }
    }

    /// Go's `leaseGrantItem`.
    #[derive(Clone, Copy, Debug)]
    struct LeaseGrantItem {
        lease_grant_ts: u64,
        old_ver: i64,
        schema_ver: i64,
    }

    /// Go's `serverFunc` goroutine, which hands out one lease grant per
    /// channel receive: the version increments and the grant timestamp is
    /// `time.Now().UnixNano()`.
    ///
    /// The goroutine, the channel, and the wall clock are all dropped. Go's
    /// test never sleeps and never asserts on the real interval between
    /// grants — every expiry assertion it makes is driven by an explicit
    /// `Add(2 * lease)` — so a deterministic tick reproduces it exactly while
    /// removing the only source of flakiness.
    struct LeaseServer {
        version: i64,
        now_nanos: i64,
    }

    impl LeaseServer {
        /// Go seeds `leaseTS` with `time.Now().UnixNano()`; the value is used
        /// as a TSO, so any epoch-scale number reproduces the arithmetic.
        const START_NANOS: i64 = 1_700_000_000_000_000_000;
        /// Stands in for the wall-clock gap between two channel receives.
        const TICK_NANOS: i64 = 1_000_000;

        fn new() -> Self {
            Self {
                version: 0,
                now_nanos: Self::START_NANOS,
            }
        }

        fn grant(&mut self) -> LeaseGrantItem {
            let item = LeaseGrantItem {
                lease_grant_ts: self.now_nanos as u64,
                old_ver: self.version - 1,
                schema_ver: self.version,
            };
            self.version += 1;
            self.now_nanos += Self::TICK_NANOS;
            item
        }

        /// Go's `getGreaterVersionItem`.
        fn greater_version_item(&mut self, curr_ver: i64) -> LeaseGrantItem {
            let new_item = self.grant();
            assert!(
                new_item.schema_ver > curr_ver,
                "currVer {curr_ver}, newItem {new_item:?}"
            );
            new_item
        }
    }

    const LEASE: Duration = Duration::from_millis(10);

    // Go `subTestSchemaValidatorGeneral`.
    #[test]
    fn schema_validator_general() {
        let mut server = LeaseServer::new();
        let validator = SchemaValidator::new(LEASE);
        assert!(validator.is_started());

        for _ in 0..3 {
            // Reload can run arbitrarily, at any time.
            let item = server.grant();
            validator.update(item.lease_grant_ts, item.old_ver, item.schema_ver, None);
        }

        // Take a lease, check it's valid.
        let item = server.grant();
        validator.update(
            item.lease_grant_ts,
            item.old_ver,
            item.schema_ver,
            Some(&change(&[10], &[10])),
        );
        let (_, valid) = validator.check(item.lease_grant_ts, item.schema_ver, Some(&[10]), true);
        assert_eq!(valid, Result::Succ);

        // Stop the validator: its delta history is emptied.
        validator.stop();
        assert!(!validator.is_started());
        assert!(validator.is_related_tables_changed(item.schema_ver, Some(&[10])));
        let (_, valid) = validator.check(item.lease_grant_ts, item.schema_ver, Some(&[10]), true);
        assert_eq!(valid, Result::Unknown);
        validator.restart(validator.restart_schema_ver());

        // Increase the current time by 2 leases, check schema is invalid.
        let after_2_lease_nanos = server.now_nanos
            + 2 * i64::try_from(LEASE.as_nanos()).expect("lease fits in an i64 of nanoseconds");
        let ts = after_2_lease_nanos as u64;
        let (_, valid) = validator.check(ts, item.schema_ver, Some(&[10]), true);
        assert_eq!(
            valid,
            Result::Unknown,
            "validator latest schema ver {}, item schema ver {}, ts {}",
            validator.latest_schema_ver(),
            item.schema_ver,
            get_time_from_ts(ts)
        );

        // Make sure new_item's version is greater than item.schema_ver.
        let new_item = server.greater_version_item(item.schema_ver);
        let curr_ver = new_item.schema_ver;
        validator.update(new_item.lease_grant_ts, new_item.old_ver, curr_ver, None);
        let (_, valid) = validator.check(ts, item.schema_ver, None, true);
        assert_eq!(valid, Result::Fail, "currVer {curr_ver}, item {item:?}");
        let (_, valid) = validator.check(ts, item.schema_ver, Some(&[0]), true);
        assert_eq!(valid, Result::Fail, "currVer {curr_ver}, item {item:?}");

        // Check the latest schema version must have changed.
        assert!(item.schema_ver < validator.latest_schema_ver());

        // Make sure new_item's version is greater than curr_ver.
        let new_item = server.greater_version_item(curr_ver);
        // Update the current schema version and the delta table IDs 1, 2, 3.
        validator.update(
            ts,
            curr_ver,
            new_item.schema_ver,
            Some(&change(&[1, 2, 3], &[1, 2, 3])),
        );
        // Make sure the updated table IDs aren't covered by the same version.
        validator.update(ts, new_item.schema_ver, new_item.schema_ver, None);
        assert!(!validator.is_related_tables_changed(curr_ver, None));
        assert!(
            validator.is_related_tables_changed(curr_ver, Some(&[2])),
            "currVer {curr_ver}, newItem {new_item:?}"
        );
        // The current schema version is older than the oldest one retained.
        assert!(
            validator.is_related_tables_changed(-1, None),
            "currVer {curr_ver}, newItem {new_item:?}"
        );

        // All schema versions are expired.
        let ts = (after_2_lease_nanos
            + 2 * i64::try_from(LEASE.as_nanos()).expect("lease fits in an i64 of nanoseconds"))
            as u64;
        let (_, valid) = validator.check(ts, new_item.schema_ver, None, true);
        assert_eq!(valid, Result::Unknown, "schemaVer {}", new_item.schema_ver);
    }

    // Go `subTestEnqueue`.
    #[test]
    fn enqueue_merges_and_evicts() {
        let validator = SchemaValidator::new(LEASE);
        assert!(validator.is_started());

        // maxCnt is 0.
        validator.set_max_delta_schema_count(0);
        validator.enqueue(1, Some(&change(&[11], &[11])));
        assert_eq!(validator.delta_schema_infos().len(), 0);

        // maxCnt is 10.
        validator.set_max_delta_schema_count(10);
        let ds = [
            delta(0, &[1], &[1]),
            delta(1, &[1], &[1]),
            delta(2, &[1], &[1]),
            delta(3, &[2, 2], &[2, 2]),
            delta(4, &[2], &[2]),
            delta(5, &[1, 4], &[1, 4]),
            delta(6, &[1, 4], &[1, 4]),
            delta(7, &[3, 1, 3], &[3, 1, 3]),
            delta(8, &[1, 2, 3], &[1, 2, 3]),
            delta(9, &[1, 2, 3], &[1, 2, 3]),
        ];
        for d in &ds {
            validator.enqueue(
                d.schema_version,
                Some(&change(&d.related_ids, &d.related_actions)),
            );
        }
        validator.enqueue(10, Some(&change(&[1], &[1])));
        let mut ret = vec![
            delta(0, &[1], &[1]),
            delta(2, &[1], &[1]),
            delta(3, &[2, 2], &[2, 2]),
            delta(4, &[2], &[2]),
            delta(6, &[1, 4], &[1, 4]),
            delta(9, &[1, 2, 3], &[1, 2, 3]),
            delta(10, &[1], &[1]),
        ];
        assert_eq!(ret, validator.delta_schema_infos());

        // The items' related table IDs have a different order.
        validator.enqueue(11, Some(&change(&[1, 2, 3, 4], &[1, 2, 3, 4])));
        validator.enqueue(12, Some(&change(&[4, 1, 2, 3, 1], &[4, 1, 2, 3, 1])));
        validator.enqueue(13, Some(&change(&[4, 1, 3, 2, 5], &[4, 1, 3, 2, 5])));
        let last = ret.len() - 1;
        ret[last] = delta(13, &[4, 1, 3, 2, 5], &[4, 1, 3, 2, 5]);
        assert_eq!(ret, validator.delta_schema_infos());

        // The queue grows past maxCnt, so the front is evicted.
        validator.enqueue(14, Some(&change(&[1], &[1])));
        validator.enqueue(15, Some(&change(&[2], &[2])));
        validator.enqueue(16, Some(&change(&[3], &[3])));
        validator.enqueue(17, Some(&change(&[4], &[4])));
        ret.push(delta(14, &[1], &[1]));
        ret.push(delta(15, &[2], &[2]));
        ret.push(delta(16, &[3], &[3]));
        ret.push(delta(17, &[4], &[4]));
        assert_eq!(ret[1..], validator.delta_schema_infos()[..]);
    }

    // Go `subTestEnqueueActionType`.
    #[test]
    fn enqueue_distinguishes_action_types() {
        let validator = SchemaValidator::new(LEASE);
        assert!(validator.is_started());

        // maxCnt is 0.
        validator.set_max_delta_schema_count(0);
        validator.enqueue(1, Some(&change(&[11], &[11])));
        assert_eq!(validator.delta_schema_infos().len(), 0);

        // maxCnt is 10.
        validator.set_max_delta_schema_count(10);
        let ds = [
            delta(0, &[1], &[1]),
            delta(1, &[1], &[1]),
            delta(2, &[1], &[1]),
            delta(3, &[2, 2], &[2, 2]),
            delta(4, &[2], &[2]),
            delta(5, &[1, 4], &[1, 4]),
            delta(6, &[1, 4], &[1, 4]),
            delta(7, &[3, 1, 3], &[3, 1, 3]),
            delta(8, &[1, 2, 3], &[1, 2, 3]),
            delta(9, &[1, 2, 3], &[1, 2, 4]),
        ];
        for d in &ds {
            validator.enqueue(
                d.schema_version,
                Some(&change(&d.related_ids, &d.related_actions)),
            );
        }
        validator.enqueue(10, Some(&change(&[1], &[15])));
        let ret = vec![
            delta(0, &[1], &[1]),
            delta(2, &[1], &[1]),
            delta(3, &[2, 2], &[2, 2]),
            delta(4, &[2], &[2]),
            delta(6, &[1, 4], &[1, 4]),
            delta(8, &[1, 2, 3], &[1, 2, 3]),
            delta(9, &[1, 2, 3], &[1, 2, 4]),
            delta(10, &[1], &[15]),
        ];
        assert_eq!(ret, validator.delta_schema_infos());

        // The action flags are ORed per table: table 3 carries action 3 at
        // schema version 9, so a transaction at version 5 sees it changed.
        assert!(validator.is_related_tables_changed(5, Some(&[1, 2, 3, 4])));
    }

    // Not in Go's test, but the paths its assertions reach only obliquely:
    // Reset clears the restart floor that Restart installs, and the lease
    // expiry is the grant plus the lease less one millisecond.
    #[test]
    fn reset_clears_what_restart_installs() {
        let validator = SchemaValidator::new(LEASE);
        let grant_ts = (12_345u64 << PHYSICAL_SHIFT_BITS) | 7;
        validator.update(grant_ts, 0, 1, Some(&change(&[1], &[1])));
        validator.restart(9);
        assert_eq!(validator.restart_schema_ver(), 9);
        // A version below the restart floor fails outright.
        assert_eq!(
            validator.check(grant_ts, 8, Some(&[1]), true).1,
            Result::Fail
        );

        let expire = 12_345 * NANOS_PER_MILLI + 10 * NANOS_PER_MILLI - NANOS_PER_MILLI;
        assert!(!validator.is_lease_expired(expire));
        assert!(validator.is_lease_expired(expire + 1));

        validator.reset();
        assert!(validator.is_started());
        assert_eq!(validator.restart_schema_ver(), 0);
        assert_eq!(validator.latest_schema_ver(), 0);
        assert!(validator.delta_schema_infos().is_empty());
        // With the history gone, the same check can no longer be answered
        // from deltas.
        assert!(validator.is_related_tables_changed(8, Some(&[1])));
    }

    // MDL on makes `check` skip the delta scan when the caller does not ask
    // for it; Go reads the same switch from `vardef.IsMDLEnabled`.
    #[test]
    fn mdl_skips_the_delta_scan() {
        let validator = SchemaValidator::new(LEASE);
        let grant_ts = (12_345u64 << PHYSICAL_SHIFT_BITS) | 7;
        validator.update(grant_ts, 0, 1, Some(&change(&[1], &[1])));
        validator.update(grant_ts, 1, 2, Some(&change(&[1], &[1])));

        // Delta scanning finds table 1 changed between version 1 and 2.
        assert_eq!(
            validator.check(grant_ts, 1, Some(&[1]), true).1,
            Result::Fail
        );
        // MDL off still scans even when the caller says not to.
        assert_eq!(
            validator.check(grant_ts, 1, Some(&[1]), false).1,
            Result::Fail
        );
        // MDL on, and no request to scan: DDL waited for the transaction.
        validator.set_mdl_enabled(true);
        assert_eq!(
            validator.check(grant_ts, 1, Some(&[1]), false).1,
            Result::Succ
        );
    }
}
