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

//! COMPLETE transcreation of ONE file of Go `pkg/ddl`:
//! `ddl_running_jobs.go`. The rest of `pkg/ddl` is not transcreated here, so
//! this module is a complete FILE, not a complete PACKAGE.
//!
//! It is the DDL job scheduler's lock table: which schema objects the jobs
//! currently running hold, in which mode, and therefore whether the next job
//! the scheduler picked up may start. Every `pkg/ddl` symbol declared in that
//! file is here -- [`Objects`], [`RunningJobs`], and all eleven of their
//! methods plus the free function `hasSchemaConflict`.
//!
//! # The lock matrix
//!
//! Three object sets are tracked, each counting how many jobs hold each
//! database/table, placement policy, and resource group:
//!
//! - `exclusive` -- objects held by running jobs in exclusive mode.
//! - `shared` -- objects held by running jobs in shared mode.
//! - `pending` -- objects a job that could NOT run wants exclusively. These
//!   block later SHARED requests, which is what makes the lock fair: without
//!   them a stream of shared jobs could starve one exclusive job forever.
//!
//! An exclusive request conflicts with all three (`shared` is concurrently
//! drained by `remove_running` from another goroutine in Go, so `pending`
//! has to be consulted too). A shared request conflicts with `exclusive` and
//! `pending` only.
//!
//! # Narrowings, each named
//!
//! - `sync.RWMutex`: Go needs it because `removeRunning` is called from the
//!   worker goroutine while `jobScheduler` calls `checkRunnable`. Rust gets
//!   the same exclusion from `&self` / `&mut self`, so the mutex is DROPPED;
//!   a caller sharing this across threads wraps it in an `RwLock`, which is
//!   the same lock in the same place.
//! - `idsStrGetter func() string` + `sync.Once`: Go memoizes the comma-joined
//!   ID list behind a closure so `allIDs` rebuilds it at most once per
//!   change. That is exactly a [`OnceLock`] reset by
//!   [`RunningJobs::update_ids_str_getter`], which is what this uses.
//! - Map iteration order: Go's `map[int64]struct{}` yields IDs in random
//!   order and Go's own test sorts the result before comparing. The ID set
//!   here is a `BTreeSet`, so [`RunningJobs::all_ids`] is already ascending.
//!   Object maps are `BTreeMap` for the same reason; only lookup is ever
//!   performed on them, so ordering is not observable there.
//! - `intest.Assert` / `intest.EnableInternalCheck`: both are Go's
//!   test-build-only panics. They become `debug_assert!`, which panics in the
//!   same builds and vanishes in release, keeping Go's "should not happen"
//!   contract without a release-mode abort.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use tidb_model::job::{INVOLVING_ALL, INVOLVING_NONE};
use tidb_model::{InvolvingSchemaInfo, InvolvingSchemaInfoMode};

/// Go `objects`: counts of the involved objects.
///
/// If a count reaches zero the entry is deleted, keeping the maps small --
/// and the checks rely on that: a present table-level entry always means a
/// real holder.
#[derive(Clone, Debug, Default)]
pub struct Objects {
    /// Go `schemas`: database -> table -> count.
    ///
    /// A job related only to a database uses `model.InvolvingAll` as the
    /// table-level key. Removal makes sure no zero-length map survives at the
    /// table level.
    schemas: BTreeMap<String, BTreeMap<String, i64>>,
    /// Go `placementPolicies`.
    placement_policies: BTreeMap<String, i64>,
    /// Go `resourceGroups`.
    resource_groups: BTreeMap<String, i64>,
}

impl Objects {
    /// Go `newObjects`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `(*objects).empty`.
    #[must_use]
    pub fn empty(&self) -> bool {
        self.schemas.is_empty()
            && self.placement_policies.is_empty()
            && self.resource_groups.is_empty()
    }
}

/// Go `runningJobs`.
#[derive(Debug, Default)]
pub struct RunningJobs {
    ids: BTreeSet<i64>,
    /// Go `idsStrGetter`, whose `sync.Once` memoization this reproduces.
    ids_str: OnceLock<String>,

    exclusive: Objects,
    shared: Objects,
    /// To implement the fair lock semantics we save the pending exclusive
    /// object requests, so they block future shared object requests.
    pending: Objects,
}

impl RunningJobs {
    /// Go `newRunningJobs`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `(*runningJobs).checkRunnable`: whether the job may run.
    ///
    /// If the caller finds a runnable job and decides to add it, it must call
    /// [`Self::add_running`] before the next `check_runnable`. Otherwise it
    /// must call [`Self::add_pending`] before the next `check_runnable`.
    #[must_use]
    pub fn check_runnable(&self, job_id: i64, involves: &[InvolvingSchemaInfo]) -> bool {
        if self.ids.contains(&job_id) {
            // Should not happen.
            debug_assert!(false, "job {job_id} is already running");
            return false;
        }
        // Currently FLASHBACK CLUSTER is the only DDL that involves ALL
        // schemas.
        if self.exclusive.schemas.contains_key(INVOLVING_ALL) {
            return false;
        }

        if self.exclusive.empty() && self.shared.empty() && self.pending.empty() {
            return true;
        }

        for info in involves {
            let database = go_str(&info.database);
            let table = go_str(&info.table);
            let policy = go_str(&info.policy);
            let resource_group = go_str(&info.resource_group);

            debug_assert!(
                !(database == INVOLVING_NONE && table != INVOLVING_NONE),
                "job {job_id} is invalid. While database is empty, \
                 involved table name is not empty: {table}"
            );
            debug_assert!(
                !(database != INVOLVING_NONE && table == INVOLVING_NONE),
                "job {job_id} is invalid. While table is empty, \
                 involved database name is not empty: {database}"
            );

            if database == INVOLVING_ALL
                && table == INVOLVING_ALL
                && info.mode == InvolvingSchemaInfoMode::EXCLUSIVE
            {
                // Involving all databases and tables, whose only case is
                // FLASHBACK CLUSTER. Because `RunningJobs` is not totally
                // empty by now, this is a conflict.
                return false;
            }

            let to_check: &[&Objects] = match info.mode {
                // Exclusive objects conflict with running exclusive and
                // shared objects. And because shared entries are concurrently
                // removed by `remove_running` in another goroutine, pending
                // objects are checked as well.
                InvolvingSchemaInfoMode::EXCLUSIVE => {
                    &[&self.exclusive, &self.shared, &self.pending]
                }
                // Shared objects conflict with running exclusive objects and
                // pending exclusive objects.
                InvolvingSchemaInfoMode::SHARED => &[&self.exclusive, &self.pending],
                mode => panic!("unknown involving mode: {mode:?}"),
            };

            for checking_obj in to_check {
                if database != INVOLVING_NONE {
                    if has_schema_conflict(database, table, &checking_obj.schemas) {
                        return false;
                    }
                    // `InvolvingSchemaInfo` is like an enumerate type.
                    debug_assert!(
                        policy.is_empty() && resource_group.is_empty(),
                        "InvolvingSchemaInfo should be like an enumerate type: {info:?}"
                    );
                    continue;
                }

                if !policy.is_empty() {
                    if checking_obj.placement_policies.contains_key(policy) {
                        return false;
                    }
                    debug_assert!(
                        resource_group.is_empty(),
                        "InvolvingSchemaInfo should be like an enumerate type: {info:?}"
                    );
                    continue;
                }
                debug_assert!(
                    !resource_group.is_empty(),
                    "InvolvingSchemaInfo should be like an enumerate type: {info:?}"
                );
                if checking_obj.resource_groups.contains_key(resource_group) {
                    return false;
                }
            }
        }
        true
    }

    /// Go `(*runningJobs).addRunning`.
    ///
    /// Only add arguments that passed the last [`Self::check_runnable`]. The
    /// added jobs can be removed by [`Self::remove_running`].
    pub fn add_running(&mut self, job_id: i64, involves: &[InvolvingSchemaInfo]) {
        self.ids.insert(job_id);
        self.update_ids_str_getter();

        for info in involves {
            let to_add = match info.mode {
                InvolvingSchemaInfoMode::EXCLUSIVE => &mut self.exclusive,
                InvolvingSchemaInfoMode::SHARED => &mut self.shared,
                mode => panic!("unknown involving mode: {mode:?}"),
            };
            add_involved(to_add, info);
        }
    }

    /// Go `(*runningJobs).finishOrPendJob`.
    pub fn finish_or_pend_job(
        &mut self,
        job_id: i64,
        involves: &[InvolvingSchemaInfo],
        move_to_pending: bool,
    ) {
        self.remove_running_without_lock(job_id, involves);
        if move_to_pending {
            self.add_pending_without_lock(involves);
        }
    }

    /// Go `(*runningJobs).removeRunning`, which Go may call concurrently with
    /// `addRunning` and `checkRunnable`.
    pub fn remove_running(&mut self, job_id: i64, involves: &[InvolvingSchemaInfo]) {
        self.remove_running_without_lock(job_id, involves);
    }

    /// Go `(*runningJobs).removeRunningWithoutLock`.
    fn remove_running_without_lock(&mut self, job_id: i64, involves: &[InvolvingSchemaInfo]) {
        debug_assert!(self.ids.contains(&job_id), "job {job_id} is not running");
        self.ids.remove(&job_id);
        self.update_ids_str_getter();

        for info in involves {
            let to_remove = match info.mode {
                InvolvingSchemaInfoMode::EXCLUSIVE => &mut self.exclusive,
                InvolvingSchemaInfoMode::SHARED => &mut self.shared,
                mode => panic!("unknown involving mode: {mode:?}"),
            };

            let database = go_str(&info.database);
            let table = go_str(&info.table);
            let policy = go_str(&info.policy);
            let resource_group = go_str(&info.resource_group);

            if database != INVOLVING_NONE {
                if let Some(tables) = to_remove.schemas.get_mut(database) {
                    if table != INVOLVING_NONE {
                        if let Some(count) = tables.get_mut(table) {
                            *count -= 1;
                            if *count == 0 {
                                tables.remove(table);
                            }
                        }
                    }
                    if tables.is_empty() {
                        to_remove.schemas.remove(database);
                    }
                }
            }

            if !policy.is_empty() {
                decrement(&mut to_remove.placement_policies, policy);
            }

            if !resource_group.is_empty() {
                decrement(&mut to_remove.resource_groups, resource_group);
            }
        }
    }

    /// Go `(*runningJobs).addPending`: record the exclusive objects of jobs
    /// that cannot run, to block following jobs whose shared objects
    /// intersect them. That is the "fair lock" semantics.
    ///
    /// The pending jobs can be removed by [`Self::reset_all_pending`].
    pub fn add_pending(&mut self, involves: &[InvolvingSchemaInfo]) {
        self.add_pending_without_lock(involves);
    }

    /// Go `(*runningJobs).addPendingWithoutLock`.
    ///
    /// Note that unlike `addRunning`, this ignores `info.Mode` entirely: a
    /// pending request is always recorded as the exclusive want it is.
    fn add_pending_without_lock(&mut self, involves: &[InvolvingSchemaInfo]) {
        for info in involves {
            add_involved(&mut self.pending, info);
        }
    }

    /// Go `(*runningJobs).resetAllPending`, called when the caller finishes a
    /// round of looking for a runnable DDL job.
    pub fn reset_all_pending(&mut self) {
        self.pending = Objects::new();
    }

    /// Go `(*runningJobs).updateIDsStrGetter`: install a fresh lazily
    /// evaluated joiner for the changed ID set.
    fn update_ids_str_getter(&mut self) {
        self.ids_str = OnceLock::new();
    }

    /// Go `(*runningJobs).allIDs`.
    pub fn all_ids(&self) -> &str {
        self.ids_str.get_or_init(|| {
            let mut joined = String::new();
            for (position, id) in self.ids.iter().enumerate() {
                if position != 0 {
                    joined.push(',');
                }
                joined.push_str(&id.to_string());
            }
            joined
        })
    }
}

/// Go `hasSchemaConflict`.
fn has_schema_conflict(
    request_database: &str,
    request_table: &str,
    schemas: &BTreeMap<String, BTreeMap<String, i64>>,
) -> bool {
    let Some(tables) = schemas.get(request_database) else {
        return false;
    };
    if request_table == INVOLVING_ALL {
        // We rely on no zero-length map existing at the table level, so a
        // present table-level entry must conflict with `InvolvingAll`.
        return true;
    }
    tables.contains_key(INVOLVING_ALL) || tables.contains_key(request_table)
}

/// The body shared by Go's `addRunning` and `addPendingWithoutLock`, which
/// increment the same three counters on different `objects`.
fn add_involved(target: &mut Objects, info: &InvolvingSchemaInfo) {
    let database = go_str(&info.database);
    let table = go_str(&info.table);
    let policy = go_str(&info.policy);
    let resource_group = go_str(&info.resource_group);

    if database != INVOLVING_NONE {
        *target
            .schemas
            .entry(database.to_owned())
            .or_default()
            .entry(table.to_owned())
            .or_insert(0) += 1;
    }
    if policy != INVOLVING_NONE {
        *target
            .placement_policies
            .entry(policy.to_owned())
            .or_insert(0) += 1;
    }
    if resource_group != INVOLVING_NONE {
        *target
            .resource_groups
            .entry(resource_group.to_owned())
            .or_insert(0) += 1;
    }
}

/// Go's `m[k]--; if m[k] == 0 { delete(m, k) }`, including Go's behavior on a
/// missing key: the zero value is decremented to -1 and stored.
fn decrement(counts: &mut BTreeMap<String, i64>, key: &str) {
    let count = counts.entry(key.to_owned()).or_insert(0);
    *count -= 1;
    if *count == 0 {
        counts.remove(key);
    }
}

/// A `&str` view of a model `GoString` field. Every name reaching this file
/// is a schema object name, so it is valid UTF-8; a name that is not compares
/// unequal to every sentinel, which is the conservative answer.
fn go_str(value: &tidb_datatype::GoString) -> &str {
    std::str::from_utf8(value.as_bytes()).unwrap_or("")
}

#[cfg(test)]
mod tests {
    //! Go `pkg/ddl/ddl_running_jobs_test.go`, all three tests.

    use super::*;

    /// Go's test helper `mkJob`.
    fn mk_job(id: i64, schema_table_names: &[&str]) -> (i64, Vec<InvolvingSchemaInfo>) {
        let involves = schema_table_names
            .iter()
            .map(|schema_table_name| {
                let (schema, table) = schema_table_name
                    .split_once('.')
                    .expect("test names are `db.table`");
                InvolvingSchemaInfo {
                    database: schema.into(),
                    table: table.into(),
                    ..InvolvingSchemaInfo::default()
                }
            })
            .collect();
        (id, involves)
    }

    fn involves(
        entries: &[(&str, &str, &str, &str, InvolvingSchemaInfoMode)],
    ) -> Vec<InvolvingSchemaInfo> {
        entries
            .iter()
            .map(
                |(database, table, policy, resource_group, mode)| InvolvingSchemaInfo {
                    database: (*database).into(),
                    table: (*table).into(),
                    policy: (*policy).into(),
                    resource_group: (*resource_group).into(),
                    mode: *mode,
                },
            )
            .collect()
    }

    const EXCLUSIVE: InvolvingSchemaInfoMode = InvolvingSchemaInfoMode::EXCLUSIVE;
    const SHARED: InvolvingSchemaInfoMode = InvolvingSchemaInfoMode::SHARED;

    /// Go's test helper `checkInvariants`.
    fn check_invariants(jobs: &RunningJobs) {
        for checking_obj in [&jobs.exclusive, &jobs.shared, &jobs.pending] {
            for tables in checking_obj.schemas.values() {
                // A table-level entry must not have zero length.
                assert!(!tables.is_empty());
                for count in tables.values() {
                    assert!(*count > 0);
                }
            }
            for count in checking_obj.placement_policies.values() {
                assert!(*count > 0);
            }
            for count in checking_obj.resource_groups.values() {
                assert!(*count > 0);
            }
        }
    }

    /// Go's test helper `orderedAllIDs`. `all_ids` is already ascending here
    /// (see the module's map-ordering narrowing), so this only asserts that.
    fn ordered_all_ids(ids: &str) -> String {
        if ids.is_empty() {
            return String::new();
        }
        let mut parsed: Vec<i64> = ids
            .split(',')
            .map(|id| id.parse().expect("all_ids emits decimal job IDs"))
            .collect();
        let unsorted = parsed.clone();
        parsed.sort_unstable();
        assert_eq!(unsorted, parsed, "all_ids should already be ascending");
        ids.to_owned()
    }

    /// Go `TestRunningJobs`.
    #[test]
    fn running_jobs_matches_go() {
        let mut jobs = RunningJobs::new();
        assert_eq!("", jobs.all_ids());
        check_invariants(&jobs);

        let (id, involved) = mk_job(0, &["db1.t1"]);
        assert!(jobs.check_runnable(id, &involved));

        let (job_id1, involves1) = mk_job(1, &["db1.t1", "db1.t2"]);
        assert!(jobs.check_runnable(job_id1, &involves1));
        jobs.add_running(job_id1, &involves1);
        let (job_id2, involves2) = mk_job(2, &["db2.t3"]);
        assert!(jobs.check_runnable(job_id2, &involves2));
        jobs.add_running(job_id2, &involves2);
        assert_eq!("1,2", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let (id, involved) = mk_job(0, &["db1.t1"]);
        assert!(!jobs.check_runnable(id, &involved));
        let (id, involved) = mk_job(0, &["db1.t2"]);
        assert!(!jobs.check_runnable(id, &involved));
        let (id, involved) = mk_job(0, &["db3.t4", "db1.t1"]);
        assert!(!jobs.check_runnable(id, &involved));
        let (id, involved) = mk_job(0, &["db3.t4", "db4.t5"]);
        assert!(jobs.check_runnable(id, &involved));

        let (job_id3, involves3) = mk_job(3, &["db1.*"]);
        assert!(!jobs.check_runnable(job_id3, &involves3));
        jobs.remove_running(job_id1, &involves1);
        assert!(jobs.check_runnable(job_id3, &involves3));
        jobs.add_running(job_id3, &involves3);
        assert_eq!("2,3", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let (id, involved) = mk_job(0, &["db1.t100"]);
        assert!(!jobs.check_runnable(id, &involved));

        let (job_id4, involves4) = mk_job(4, &["db4.t100", "db2.t6"]);
        assert!(jobs.check_runnable(job_id4, &involves4));
        jobs.add_running(job_id4, &involves4);
        assert_eq!("2,3,4", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let (job_id5, involves5) = mk_job(5, &["*.*"]);
        assert!(!jobs.check_runnable(job_id5, &involves5));

        jobs.remove_running(job_id2, &involves2);
        jobs.remove_running(job_id3, &involves3);
        jobs.remove_running(job_id4, &involves4);
        assert_eq!("", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        assert!(jobs.check_runnable(job_id5, &involves5));
        jobs.add_running(job_id5, &involves5);
        assert_eq!("5", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let (id, involved) = mk_job(0, &["db1.t1"]);
        assert!(!jobs.check_runnable(id, &involved));
    }

    /// Go `TestSchemaPolicyAndResourceGroup`.
    #[test]
    fn schema_policy_and_resource_group_matches_go() {
        let mut jobs = RunningJobs::new();

        let (job_id1, involves1) = mk_job(1, &["db1.t1", "db1.t2"]);
        assert!(jobs.check_runnable(job_id1, &involves1));
        jobs.add_running(job_id1, &involves1);

        let failed_involves = involves(&[
            ("", "", "p0", "", EXCLUSIVE),
            ("db1", INVOLVING_ALL, "", "", EXCLUSIVE),
        ]);
        assert!(!jobs.check_runnable(0, &failed_involves));

        let failed_involves = involves(&[
            (INVOLVING_ALL, INVOLVING_ALL, "", "", EXCLUSIVE),
            ("", "", "", "g0", EXCLUSIVE),
        ]);
        assert!(!jobs.check_runnable(0, &failed_involves));

        let job_id2 = 2;
        let involves2 = involves(&[
            ("db2", INVOLVING_ALL, "", "", EXCLUSIVE),
            ("", "", "p0", "", EXCLUSIVE),
            ("", "", "", "g0", EXCLUSIVE),
        ]);
        assert!(jobs.check_runnable(job_id2, &involves2));
        jobs.add_running(job_id2, &involves2);

        let job_id3 = 3;
        let involves3 = involves(&[("", "", "p1", "", EXCLUSIVE), ("", "", "", "g1", EXCLUSIVE)]);
        assert!(jobs.check_runnable(job_id3, &involves3));
        jobs.add_running(job_id3, &involves3);
        assert_eq!("1,2,3", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let failed_involves = involves(&[("", "", "", "g0", EXCLUSIVE)]);
        assert!(!jobs.check_runnable(0, &failed_involves));

        jobs.remove_running(job_id2, &involves2);
        assert_eq!("1,3", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let job_id4 = 4;
        let involves4 = involves(&[
            ("", "", "p0", "", EXCLUSIVE),
            ("db3", "t3", "", "", EXCLUSIVE),
        ]);
        assert!(jobs.check_runnable(job_id4, &involves4));
        jobs.add_running(job_id4, &involves4);
        assert_eq!("1,3,4", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let failed_involves = involves(&[("db3", "t3", "", "", EXCLUSIVE)]);
        assert!(!jobs.check_runnable(0, &failed_involves));
        let failed_involves = involves(&[("", "", "p1", "", EXCLUSIVE)]);
        assert!(!jobs.check_runnable(0, &failed_involves));
    }

    /// Go `TestExclusiveShared`.
    #[test]
    fn exclusive_shared_matches_go() {
        let mut jobs = RunningJobs::new();

        let (job_id1, involves1) = mk_job(1, &["db1.t1", "db1.t2"]);
        assert!(jobs.check_runnable(job_id1, &involves1));
        jobs.add_running(job_id1, &involves1);

        let failed_involves = involves(&[
            ("db2", INVOLVING_ALL, "", "", EXCLUSIVE),
            ("db1", "t1", "", "", SHARED),
        ]);
        assert!(!jobs.check_runnable(0, &failed_involves));

        let job_id2 = 2;
        let involves2 = involves(&[
            ("db3", INVOLVING_ALL, "", "", EXCLUSIVE),
            ("db2", "t2", "", "", SHARED),
        ]);
        assert!(jobs.check_runnable(job_id2, &involves2));
        jobs.add_running(job_id2, &involves2);

        let job_id3 = 3;
        let involves3 = involves(&[
            ("db4", INVOLVING_ALL, "", "", EXCLUSIVE),
            ("db2", "t2", "", "", SHARED),
        ]);
        assert!(jobs.check_runnable(job_id3, &involves3));
        jobs.add_running(job_id3, &involves3);
        assert_eq!("1,2,3", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let pending_involves = involves(&[("db2", "t2", "", "", EXCLUSIVE)]);
        assert!(!jobs.check_runnable(0, &pending_involves));
        jobs.add_pending(&pending_involves);
        assert_eq!("1,2,3", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        // Because there is a pending job on db2.t2, the next job on db2.t2
        // must be blocked.
        let job_id4 = 4;
        let involves4 = involves(&[
            ("db100", INVOLVING_ALL, "", "", EXCLUSIVE),
            ("db2", "t2", "", "", SHARED),
        ]);
        assert!(!jobs.check_runnable(job_id4, &involves4));
        assert_eq!("1,2,3", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        // Mimic every running job finishing, and the next round of getting
        // jobs starting.
        jobs.reset_all_pending();
        jobs.remove_running(job_id1, &involves1);
        jobs.remove_running(job_id2, &involves2);
        jobs.remove_running(job_id3, &involves3);
        check_invariants(&jobs);

        assert!(jobs.check_runnable(0, &pending_involves));

        // New test round.

        let job_id5 = 5;
        let involves5 = involves(&[("", "", "p1", "", SHARED), ("", "", "p2", "", SHARED)]);
        assert!(jobs.check_runnable(job_id5, &involves5));
        jobs.add_running(job_id5, &involves5);

        let job_id6 = 6;
        let involves6 = involves(&[("", "", "p1", "", SHARED), ("", "", "", "g1", SHARED)]);
        assert!(jobs.check_runnable(job_id6, &involves6));
        jobs.add_running(job_id6, &involves6);

        assert_eq!("5,6", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        let pending_involves =
            involves(&[("", "", "p1", "", EXCLUSIVE), ("", "", "", "g2", EXCLUSIVE)]);
        assert!(!jobs.check_runnable(0, &pending_involves));
        jobs.add_pending(&pending_involves);

        let second_pending_involves =
            involves(&[("", "", "", "g2", EXCLUSIVE), ("", "", "", "g3", EXCLUSIVE)]);
        assert!(!jobs.check_runnable(0, &second_pending_involves));
        jobs.add_pending(&second_pending_involves);

        // Two shared p1 objects: test when one finishes and another round
        // starts.

        jobs.remove_running(job_id6, &involves6);
        assert_eq!("5", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);
        jobs.reset_all_pending();

        assert!(!jobs.check_runnable(0, &pending_involves));
        jobs.add_pending(&pending_involves);
        assert!(!jobs.check_runnable(0, &second_pending_involves));
        jobs.add_pending(&second_pending_involves);

        // Every shared p1 object is now removed.

        jobs.remove_running(job_id5, &involves5);
        assert_eq!("", ordered_all_ids(jobs.all_ids()));
        check_invariants(&jobs);

        // No p1 in exclusive or shared. But p1 exists in pending, so this job
        // still cannot run.
        let third_pending_involves = involves(&[("", "", "p1", "", EXCLUSIVE)]);
        assert!(!jobs.check_runnable(0, &third_pending_involves));
        jobs.add_pending(&third_pending_involves);

        // Now another round starts, and the first pending job can run.

        jobs.reset_all_pending();
        assert!(jobs.check_runnable(0, &pending_involves));
    }
}
