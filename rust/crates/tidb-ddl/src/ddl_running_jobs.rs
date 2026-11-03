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

//! Go `pkg/ddl/ddl_running_jobs.go`: the schema objects the running DDL jobs
//! hold, which decide whether the job scheduler may start another job now.

use std::collections::{BTreeSet, HashMap};
use std::sync::RwLock;

use tidb_model::job::{INVOLVING_ALL, INVOLVING_NONE};
use tidb_model::{InvolvingSchemaInfo, InvolvingSchemaInfoMode};

/// Go `objects`: how many jobs involve each object. A count that reaches
/// zero leaves the map, so no table-level map is ever empty.
#[derive(Debug, Default)]
struct Objects {
    /// database -> table -> count; a job on a whole database counts under
    /// table [`INVOLVING_ALL`].
    schemas: HashMap<Vec<u8>, HashMap<Vec<u8>, i64>>,
    placement_policies: HashMap<Vec<u8>, i64>,
    resource_groups: HashMap<Vec<u8>, i64>,
}

impl Objects {
    fn empty(&self) -> bool {
        self.schemas.is_empty()
            && self.placement_policies.is_empty()
            && self.resource_groups.is_empty()
    }
}

#[derive(Debug, Default)]
struct RunningJobsInner {
    ids: BTreeSet<i64>,
    exclusive: Objects,
    shared: Objects,
    /// The exclusive requests of jobs that could not run this round: they
    /// block later shared requests ("fair lock").
    pending: Objects,
}

/// Go `runningJobs`.
#[derive(Debug, Default)]
pub struct RunningJobs {
    mu: RwLock<RunningJobsInner>,
}

fn is_exclusive(info: &InvolvingSchemaInfo) -> bool {
    match info.mode {
        InvolvingSchemaInfoMode::EXCLUSIVE => true,
        InvolvingSchemaInfoMode::SHARED => false,
        mode => panic!("unknown involving mode: {}", mode.0),
    }
}

impl RunningJobs {
    /// Go `newRunningJobs`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, RunningJobsInner> {
        self.mu
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, RunningJobsInner> {
        self.mu
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `checkRunnable`: whether the job can run now. A caller that starts
    /// it must [`Self::add_running`] before the next check; otherwise it must
    /// [`Self::add_pending`].
    #[must_use]
    pub fn check_runnable(&self, job_id: i64, involves: &[InvolvingSchemaInfo]) -> bool {
        let j = self.read();
        if j.ids.contains(&job_id) {
            debug_assert!(false, "job {job_id} is already running");
            return false;
        }
        // Currently flashback cluster is the only DDL that involves ALL
        // schemas.
        if j.exclusive.schemas.contains_key(INVOLVING_ALL.as_bytes()) {
            return false;
        }
        if j.exclusive.empty() && j.shared.empty() && j.pending.empty() {
            return true;
        }
        for info in involves {
            debug_assert!(
                !(info.database == INVOLVING_NONE && info.table != INVOLVING_NONE),
                "job {job_id} is invalid. While database is empty, involved table name is not empty: {}",
                info.table
            );
            debug_assert!(
                !(info.database != INVOLVING_NONE && info.table == INVOLVING_NONE),
                "job {job_id} is invalid. While table is empty, involved database name is not empty: {}",
                info.database
            );
            if info.database == INVOLVING_ALL
                && info.table == INVOLVING_ALL
                && info.mode == InvolvingSchemaInfoMode::EXCLUSIVE
            {
                // FLASHBACK CLUSTER, and runningJobs is not empty.
                return false;
            }
            let to_check: &[&Objects] = if is_exclusive(info) {
                // Exclusive objects conflict with running exclusive and
                // shared ones, and with pending ones (shared ones are removed
                // concurrently by finishing workers).
                &[&j.exclusive, &j.shared, &j.pending]
            } else {
                // Shared objects conflict with running and pending exclusive
                // ones.
                &[&j.exclusive, &j.pending]
            };
            for checking in to_check {
                if info.database != INVOLVING_NONE {
                    if has_schema_conflict(
                        info.database.as_bytes(),
                        info.table.as_bytes(),
                        &checking.schemas,
                    ) {
                        return false;
                    }
                    continue;
                }
                if info.policy != INVOLVING_NONE {
                    if checking
                        .placement_policies
                        .contains_key(info.policy.as_bytes())
                    {
                        return false;
                    }
                    continue;
                }
                debug_assert!(
                    info.resource_group != INVOLVING_NONE,
                    "InvolvingSchemaInfo should be like an enumerate type: {info:?}"
                );
                if checking
                    .resource_groups
                    .contains_key(info.resource_group.as_bytes())
                {
                    return false;
                }
            }
        }
        true
    }

    /// Go `addRunning`: only what passed the last [`Self::check_runnable`].
    pub fn add_running(&self, job_id: i64, involves: &[InvolvingSchemaInfo]) {
        let mut j = self.write();
        j.ids.insert(job_id);
        for info in involves {
            let to_add = if is_exclusive(info) {
                &mut j.exclusive
            } else {
                &mut j.shared
            };
            add_objects(to_add, info);
        }
    }

    /// Go `finishOrPendJob`: the job no longer runs; an unfinished one keeps
    /// its objects pending so this round starts nothing else on them.
    pub fn finish_or_pend_job(
        &self,
        job_id: i64,
        involves: &[InvolvingSchemaInfo],
        move_to_pending: bool,
    ) {
        let mut j = self.write();
        remove_running(&mut j, job_id, involves);
        if move_to_pending {
            for info in involves {
                add_objects(&mut j.pending, info);
            }
        }
    }

    /// Go `removeRunning`.
    pub fn remove_running(&self, job_id: i64, involves: &[InvolvingSchemaInfo]) {
        remove_running(&mut self.write(), job_id, involves);
    }

    /// Go `addPending`: the objects of a job that cannot run this round.
    pub fn add_pending(&self, involves: &[InvolvingSchemaInfo]) {
        let mut j = self.write();
        for info in involves {
            add_objects(&mut j.pending, info);
        }
    }

    /// Go `resetAllPending`: the round of picking runnable jobs is over.
    pub fn reset_all_pending(&self) {
        self.write().pending = Objects::default();
    }

    /// Whether the job runs: Go's `job_id not in (allIDs())` clause.
    #[must_use]
    pub fn is_running(&self, job_id: i64) -> bool {
        self.read().ids.contains(&job_id)
    }

    /// Go `allIDs`: the running jobs' ids, comma separated.
    #[must_use]
    pub fn all_ids(&self) -> String {
        let j = self.read();
        j.ids
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Go `hasSchemaConflict`.
fn has_schema_conflict(
    request_database: &[u8],
    request_table: &[u8],
    schemas: &HashMap<Vec<u8>, HashMap<Vec<u8>, i64>>,
) -> bool {
    let Some(tbls) = schemas.get(request_database) else {
        return false;
    };
    // No table-level map is empty, so any entry conflicts with the whole
    // database.
    request_table == INVOLVING_ALL.as_bytes()
        || tbls.contains_key(INVOLVING_ALL.as_bytes())
        || tbls.contains_key(request_table)
}

fn add_objects(to_add: &mut Objects, info: &InvolvingSchemaInfo) {
    if info.database != INVOLVING_NONE {
        *to_add
            .schemas
            .entry(info.database.as_bytes().to_vec())
            .or_default()
            .entry(info.table.as_bytes().to_vec())
            .or_default() += 1;
    }
    if info.policy != INVOLVING_NONE {
        *to_add
            .placement_policies
            .entry(info.policy.as_bytes().to_vec())
            .or_default() += 1;
    }
    if info.resource_group != INVOLVING_NONE {
        *to_add
            .resource_groups
            .entry(info.resource_group.as_bytes().to_vec())
            .or_default() += 1;
    }
}

/// Go `removeRunningWithoutLock`.
fn remove_running(j: &mut RunningJobsInner, job_id: i64, involves: &[InvolvingSchemaInfo]) {
    debug_assert!(j.ids.contains(&job_id), "job {job_id} is not running");
    j.ids.remove(&job_id);
    for info in involves {
        let to_remove = if is_exclusive(info) {
            &mut j.exclusive
        } else {
            &mut j.shared
        };
        if info.database != INVOLVING_NONE {
            if let Some(db) = to_remove.schemas.get_mut(info.database.as_bytes()) {
                if info.table != INVOLVING_NONE {
                    decrement(db, info.table.as_bytes());
                }
                if db.is_empty() {
                    to_remove.schemas.remove(info.database.as_bytes());
                }
            }
        }
        if info.policy != INVOLVING_NONE {
            decrement(&mut to_remove.placement_policies, info.policy.as_bytes());
        }
        if info.resource_group != INVOLVING_NONE {
            decrement(
                &mut to_remove.resource_groups,
                info.resource_group.as_bytes(),
            );
        }
    }
}

/// Go `m[k]--; if m[k] == 0 { delete(m, k) }`.
fn decrement(counts: &mut HashMap<Vec<u8>, i64>, key: &[u8]) {
    let count = counts.entry(key.to_vec()).or_default();
    *count -= 1;
    if *count == 0 {
        counts.remove(key);
    }
}

#[cfg(test)]
mod tests {
    //! Go `pkg/ddl/ddl_running_jobs_test.go`.

    use super::*;

    fn mk_job(id: i64, schema_table_names: &[&str]) -> (i64, Vec<InvolvingSchemaInfo>) {
        let infos = schema_table_names
            .iter()
            .map(|name| {
                let (database, table) = name.split_once('.').unwrap();
                InvolvingSchemaInfo {
                    database: database.into(),
                    table: table.into(),
                    ..Default::default()
                }
            })
            .collect();
        (id, infos)
    }

    fn info(
        database: &str,
        table: &str,
        policy: &str,
        group: &str,
        shared: bool,
    ) -> InvolvingSchemaInfo {
        InvolvingSchemaInfo {
            database: database.into(),
            table: table.into(),
            policy: policy.into(),
            resource_group: group.into(),
            mode: if shared {
                InvolvingSchemaInfoMode::SHARED
            } else {
                InvolvingSchemaInfoMode::EXCLUSIVE
            },
        }
    }

    fn check_invariants(j: &RunningJobs) {
        let j = j.read();
        for checking in [&j.exclusive, &j.shared, &j.pending] {
            for tables in checking.schemas.values() {
                assert!(!tables.is_empty());
                assert!(tables.values().all(|v| *v > 0));
            }
            assert!(checking.placement_policies.values().all(|v| *v > 0));
            assert!(checking.resource_groups.values().all(|v| *v > 0));
        }
    }

    fn runnable(j: &RunningJobs, job: &(i64, Vec<InvolvingSchemaInfo>)) -> bool {
        j.check_runnable(job.0, &job.1)
    }

    #[test]
    fn test_running_jobs() {
        let j = RunningJobs::new();
        assert_eq!(j.all_ids(), "");
        check_invariants(&j);

        assert!(runnable(&j, &mk_job(0, &["db1.t1"])));
        let job1 = mk_job(1, &["db1.t1", "db1.t2"]);
        assert!(runnable(&j, &job1));
        j.add_running(job1.0, &job1.1);
        let job2 = mk_job(2, &["db2.t3"]);
        assert!(runnable(&j, &job2));
        j.add_running(job2.0, &job2.1);
        assert_eq!(j.all_ids(), "1,2");
        check_invariants(&j);

        assert!(!runnable(&j, &mk_job(0, &["db1.t1"])));
        assert!(!runnable(&j, &mk_job(0, &["db1.t2"])));
        assert!(!runnable(&j, &mk_job(0, &["db3.t4", "db1.t1"])));
        assert!(runnable(&j, &mk_job(0, &["db3.t4", "db4.t5"])));

        let job3 = mk_job(3, &["db1.*"]);
        assert!(!runnable(&j, &job3));
        j.remove_running(job1.0, &job1.1);
        assert!(runnable(&j, &job3));
        j.add_running(job3.0, &job3.1);
        assert_eq!(j.all_ids(), "2,3");
        check_invariants(&j);

        assert!(!runnable(&j, &mk_job(0, &["db1.t100"])));

        let job4 = mk_job(4, &["db4.t100", "db2.t6"]);
        assert!(runnable(&j, &job4));
        j.add_running(job4.0, &job4.1);
        assert_eq!(j.all_ids(), "2,3,4");
        check_invariants(&j);

        let job5 = mk_job(5, &["*.*"]);
        assert!(!runnable(&j, &job5));

        j.remove_running(job2.0, &job2.1);
        j.remove_running(job3.0, &job3.1);
        j.remove_running(job4.0, &job4.1);
        assert_eq!(j.all_ids(), "");
        check_invariants(&j);

        assert!(runnable(&j, &job5));
        j.add_running(job5.0, &job5.1);
        assert_eq!(j.all_ids(), "5");
        check_invariants(&j);

        assert!(!runnable(&j, &mk_job(0, &["db1.t1"])));
    }

    #[test]
    fn test_schema_policy_and_resource_group() {
        let j = RunningJobs::new();

        let job1 = mk_job(1, &["db1.t1", "db1.t2"]);
        assert!(runnable(&j, &job1));
        j.add_running(job1.0, &job1.1);

        let failed = vec![
            info("", "", "p0", "", false),
            info("db1", INVOLVING_ALL, "", "", false),
        ];
        assert!(!j.check_runnable(0, &failed));

        let failed = vec![
            info(INVOLVING_ALL, INVOLVING_ALL, "", "", false),
            info("", "", "", "g0", false),
        ];
        assert!(!j.check_runnable(0, &failed));

        let involves2 = vec![
            info("db2", INVOLVING_ALL, "", "", false),
            info("", "", "p0", "", false),
            info("", "", "", "g0", false),
        ];
        assert!(j.check_runnable(2, &involves2));
        j.add_running(2, &involves2);

        let involves3 = vec![info("", "", "p1", "", false), info("", "", "", "g1", false)];
        assert!(j.check_runnable(3, &involves3));
        j.add_running(3, &involves3);
        assert_eq!(j.all_ids(), "1,2,3");
        check_invariants(&j);

        assert!(!j.check_runnable(0, &[info("", "", "", "g0", false)]));

        j.remove_running(2, &involves2);
        assert_eq!(j.all_ids(), "1,3");
        check_invariants(&j);

        let involves4 = vec![
            info("", "", "p0", "", false),
            info("db3", "t3", "", "", false),
        ];
        assert!(j.check_runnable(4, &involves4));
        j.add_running(4, &involves4);
        assert_eq!(j.all_ids(), "1,3,4");
        check_invariants(&j);

        assert!(!j.check_runnable(0, &[info("db3", "t3", "", "", false)]));
        assert!(!j.check_runnable(0, &[info("", "", "p1", "", false)]));
    }

    #[test]
    fn test_exclusive_shared() {
        let j = RunningJobs::new();

        let job1 = mk_job(1, &["db1.t1", "db1.t2"]);
        assert!(runnable(&j, &job1));
        j.add_running(job1.0, &job1.1);

        let failed = vec![
            info("db2", INVOLVING_ALL, "", "", false),
            info("db1", "t1", "", "", true),
        ];
        assert!(!j.check_runnable(0, &failed));

        let involves2 = vec![
            info("db3", INVOLVING_ALL, "", "", false),
            info("db2", "t2", "", "", true),
        ];
        assert!(j.check_runnable(2, &involves2));
        j.add_running(2, &involves2);

        let involves3 = vec![
            info("db4", INVOLVING_ALL, "", "", false),
            info("db2", "t2", "", "", true),
        ];
        assert!(j.check_runnable(3, &involves3));
        j.add_running(3, &involves3);
        assert_eq!(j.all_ids(), "1,2,3");
        check_invariants(&j);

        let pending = vec![info("db2", "t2", "", "", false)];
        assert!(!j.check_runnable(0, &pending));
        j.add_pending(&pending);
        assert_eq!(j.all_ids(), "1,2,3");
        check_invariants(&j);

        // A pending job on db2.t2 blocks the next one on db2.t2.
        let involves4 = vec![
            info("db100", INVOLVING_ALL, "", "", false),
            info("db2", "t2", "", "", true),
        ];
        assert!(!j.check_runnable(4, &involves4));
        assert_eq!(j.all_ids(), "1,2,3");
        check_invariants(&j);

        // Every running job finished and the next round picks jobs.
        j.reset_all_pending();
        j.remove_running(job1.0, &job1.1);
        j.remove_running(2, &involves2);
        j.remove_running(3, &involves3);
        check_invariants(&j);

        assert!(j.check_runnable(0, &pending));

        let involves5 = vec![info("", "", "p1", "", true), info("", "", "p2", "", true)];
        assert!(j.check_runnable(5, &involves5));
        j.add_running(5, &involves5);

        let involves6 = vec![info("", "", "p1", "", true), info("", "", "", "g1", true)];
        assert!(j.check_runnable(6, &involves6));
        j.add_running(6, &involves6);

        assert_eq!(j.all_ids(), "5,6");
        check_invariants(&j);

        let pending = vec![info("", "", "p1", "", false), info("", "", "", "g2", false)];
        assert!(!j.check_runnable(0, &pending));
        j.add_pending(&pending);

        let second_pending = vec![info("", "", "", "g2", false), info("", "", "", "g3", false)];
        assert!(!j.check_runnable(0, &second_pending));
        j.add_pending(&second_pending);

        // Two shared p1 objects; one finishes and another round starts.
        j.remove_running(6, &involves6);
        assert_eq!(j.all_ids(), "5");
        check_invariants(&j);
        j.reset_all_pending();

        assert!(!j.check_runnable(0, &pending));
        j.add_pending(&pending);
        assert!(!j.check_runnable(0, &second_pending));
        j.add_pending(&second_pending);

        // Every shared p1 object is gone.
        j.remove_running(5, &involves5);
        assert_eq!(j.all_ids(), "");
        check_invariants(&j);

        // No p1 runs, but p1 is pending, so this job cannot run.
        let third_pending = vec![info("", "", "p1", "", false)];
        assert!(!j.check_runnable(0, &third_pending));
        j.add_pending(&third_pending);

        // Another round: the first pending job can run.
        j.reset_all_pending();
        assert!(j.check_runnable(0, &pending));
    }
}
