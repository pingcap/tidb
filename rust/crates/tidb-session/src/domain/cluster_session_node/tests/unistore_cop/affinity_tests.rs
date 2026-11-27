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

//! Go `pkg/ddl/affinity_test.go`: the PD affinity groups follow the outcome
//! of CREATE, ALTER, TRUNCATE TABLE and TRUNCATE PARTITION jobs, including
//! the ones that fail or are cancelled.

use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Mutex;
use tidb_model::GoShared;
use tidb_domain_affinity::{
    AffinityGroupKeyRange, AffinityGroupState, AffinityGroupStates, AffinityGroups, PdAffinityClient,
    PdHttpError,
};
use tidb_model::{ActionType, Job, SchemaState, TableInfo};

const PARTITIONS: &str =
    "partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)";

/// Go `truncateAffinityPDClient`: PD's affinity groups, each with the label
/// rule holding its key ranges, and the scripted failures.
#[derive(Default)]
struct TruncateAffinityPdClient {
    /// Group id -> its label rule's key ranges.
    label_rules: Mutex<BTreeMap<String, Vec<AffinityGroupKeyRange>>>,
    fail_create: AtomicBool,
    fail_delete: AtomicBool,
    /// Cancellation tests exhaust the three lightweight retries after a
    /// partial commit or a lost response.
    retry_failures: AtomicI64,
    retry_error: Mutex<Option<PdHttpError>>,
    fail_503_once: AtomicBool,
    fail_once: AtomicBool,
    fail_after_create: AtomicBool,
    last_created_groups: Mutex<AffinityGroups>,
    create_calls: AtomicI64,
    create_failures: AtomicI64,
    /// When positive, a creation request commits at most this many of its
    /// groups and then fails, after `partial_create_skip` requests succeed.
    partial_create_limit: AtomicI64,
    partial_create_skip: AtomicI64,
    partial_created_ids: Mutex<Vec<String>>,
    /// Every group id PD has committed, deleted ones included.
    ever_created: Mutex<BTreeSet<String>>,
}

impl TruncateAffinityPdClient {
    fn commit(&self, groups: &AffinityGroups) {
        let mut rules = self.label_rules.lock().unwrap();
        let mut ever = self.ever_created.lock().unwrap();
        for (id, ranges) in groups {
            rules.entry(id.clone()).or_insert_with(|| ranges.clone());
            ever.insert(id.clone());
        }
    }

    fn exhaust_creation_retries(&self, error: PdHttpError) -> PdHttpError {
        *self.retry_error.lock().unwrap() = Some(error.clone());
        self.retry_failures.store(3, Ordering::SeqCst);
        error
    }

    /// Go `createPartially`: commits the first `limit` groups in id order,
    /// then fails.
    fn create_partially(&self, groups: &AffinityGroups, limit: usize) -> PdHttpError {
        let committed: AffinityGroups = groups.iter().take(limit).map(|(id, ranges)| (id.clone(), ranges.clone())).collect();
        self.commit(&committed);
        *self.partial_created_ids.lock().unwrap() = committed.keys().cloned().collect();
        self.exhaust_creation_retries(PdHttpError::Message(format!(
            "injected affinity creation failure after {} of {} groups",
            committed.len(),
            groups.len()
        )))
    }

    fn require_ever_created(&self, groups: &AffinityGroups) {
        assert!(!groups.is_empty());
        let ever = self.ever_created.lock().unwrap();
        for id in groups.keys() {
            assert!(ever.contains(id), "group {id} must have been created before the job was cancelled");
        }
    }

    fn last_created(&self) -> AffinityGroups {
        self.last_created_groups.lock().unwrap().clone()
    }
}

impl PdAffinityClient for TruncateAffinityPdClient {
    fn create_affinity_groups(&self, groups: &AffinityGroups, _: bool) -> Result<AffinityGroupStates, PdHttpError> {
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        if self.retry_failures.load(Ordering::SeqCst) > 0 {
            self.retry_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(self.retry_error.lock().unwrap().clone().unwrap());
        }
        if self.fail_503_once.swap(false, Ordering::SeqCst) {
            self.create_failures.fetch_add(1, Ordering::SeqCst);
            return Err(PdHttpError::Message(
                "request pd http api failed with status: '503 Service Unavailable', body: 'injected transient creation failure'".to_owned(),
            ));
        }
        if self.fail_create.load(Ordering::SeqCst) || self.fail_once.swap(false, Ordering::SeqCst) {
            self.create_failures.fetch_add(1, Ordering::SeqCst);
            return Err(PdHttpError::Message("injected affinity creation failure".to_owned()));
        }
        if self.partial_create_limit.load(Ordering::SeqCst) > 0 && self.partial_create_skip.fetch_sub(1, Ordering::SeqCst) - 1 < 0 {
            let limit = self.partial_create_limit.swap(0, Ordering::SeqCst) as usize;
            return Err(self.create_partially(groups, limit));
        }
        self.commit(groups);
        *self.last_created_groups.lock().unwrap() = groups.clone();
        if self.fail_after_create.swap(false, Ordering::SeqCst) {
            return Err(self.exhaust_creation_retries(PdHttpError::Message(
                "injected response loss after affinity creation".to_owned(),
            )));
        }
        Ok(AffinityGroupStates::new())
    }

    fn get_affinity_groups(&self, ids: &[String]) -> Result<AffinityGroupStates, PdHttpError> {
        let rules = self.label_rules.lock().unwrap();
        Ok(ids
            .iter()
            .filter(|id| rules.contains_key(*id))
            .map(|id| (id.clone(), AffinityGroupState::default()))
            .collect())
    }

    fn get_all_affinity_groups(&self) -> Result<AffinityGroupStates, PdHttpError> {
        let ids: Vec<String> = self.label_rules.lock().unwrap().keys().cloned().collect();
        self.get_affinity_groups(&ids)
    }

    fn batch_delete_affinity_groups(&self, ids: &[String], _: bool) -> Result<(), PdHttpError> {
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(PdHttpError::Message("injected affinity deletion failure".to_owned()));
        }
        let mut rules = self.label_rules.lock().unwrap();
        for id in ids {
            rules.remove(id);
        }
        Ok(())
    }
}

/// Go `affinityDDLTest`.
struct AffinityDdlTest {
    stack: UnistoreClusterStack,
    cli: Arc<TruncateAffinityPdClient>,
    tk: crate::testkit::TestKit,
    tk_cancel: Arc<Mutex<crate::testkit::TestKit>>,
    schema_id: i64,
}

fn new_affinity_ddl_test() -> AffinityDdlTest {
    let (stack, _users) = cop_backed_stack();
    let cli = Arc::new(TruncateAffinityPdClient::default());
    tidb_domain_affinity::init_manager(Some(Arc::clone(&cli) as Arc<dyn PdAffinityClient>));
    let mut tk = testkit(&stack, 9301);
    tk.must_exec("use test");
    let tk_cancel = Arc::new(Mutex::new(testkit(&stack, 9302)));
    let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
    AffinityDdlTest { stack, cli, tk, tk_cancel, schema_id }
}

impl Drop for AffinityDdlTest {
    fn drop(&mut self) {
        tidb_domain_affinity::init_manager(None);
    }
}

impl AffinityDdlTest {
    fn table_info(&self, name: &str) -> TableInfo {
        self.stack.factory.catalog.get_latest().table_info_by_name("test", name).expect(name).1.read().clone_like_go()
    }

    /// Go `meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable`.
    fn persisted(&mut self, id: i64) -> Option<TableInfo> {
        let schema_id = self.schema_id;
        let session = self.tk.session();
        let domain = session.domain_services().unwrap().domain.upgrade().unwrap();
        let ts = domain.transactions.fresh_timestamp().unwrap();
        let mut meta = domain.get_snapshot_meta(ts).unwrap();
        tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| reader.table(schema_id, id)).unwrap()
    }

    fn groups_of(&self, info: &TableInfo) -> AffinityGroups {
        tidb_ddl::ddl_affinity::build_affinity_group_definitions(info, None).unwrap()
    }
}

fn json(info: &Option<TableInfo>) -> String {
    serde_json::to_string(info).unwrap()
}

/// Go `requireAffinityGroupsWithLabelRules`: both the groups and the label
/// rules holding their key ranges.
#[track_caller]
fn require_affinity_groups_with_label_rules(
    cli: &TruncateAffinityPdClient,
    groups: &AffinityGroups,
    should_exist: bool,
    msg: &str,
) {
    assert!(!groups.is_empty(), "{msg}");
    let ids: Vec<String> = groups.keys().cloned().collect();
    let states = tidb_domain_affinity::get_groups(&ids).unwrap();
    let rules = cli.label_rules.lock().unwrap();
    for (id, ranges) in groups {
        if should_exist {
            assert!(states.contains_key(id), "group {id}: {msg}");
            assert_eq!(rules.get(id), Some(ranges), "label rule of group {id} should exist: {msg}");
        } else {
            assert!(!states.contains_key(id), "group {id}: {msg}");
            assert!(!rules.contains_key(id), "label rule of group {id} should be deleted: {msg}");
        }
    }
}

fn job_arg(arg: &mut dyn std::any::Any) -> &mut Job {
    arg.downcast_mut::<Job>().expect("a job")
}

fn error_text(tk: &mut crate::testkit::TestKit, sql: &str) -> String {
    tk.exec_to_err(sql).unwrap_or_else(|| panic!("{sql} must fail")).error_string()
}

/// Go `TestAffinityTruncateTableFailureAtomicity`.
#[test]
fn test_affinity_truncate_table_failure_atomicity() {
    for with_policy in [false, true] {
        let mut s = new_affinity_ddl_test();
        s.tk.must_exec("create table t(a int) affinity='table'");
        s.tk.must_exec("insert into t values (1)");
        if with_policy {
            s.tk.must_exec("create masking policy mp on t(a) as a");
        }
        let before = s.table_info("t");
        let persisted_before = json(&s.persisted(before.id));
        for after_create in [false, true] {
            s.cli.fail_create.store(!after_create, Ordering::SeqCst);
            s.cli.fail_after_create.store(after_create, Ordering::SeqCst);
            let failure = if after_create {
                "injected response loss after affinity creation"
            } else {
                "injected affinity creation failure"
            };
            let error = error_text(&mut s.tk, "truncate table t");
            assert!(error.contains(failure), "policy={with_policy} loss={after_create}: {error}");
            s.cli.fail_create.store(false, Ordering::SeqCst);
            assert_eq!(
                json(&s.persisted(before.id)),
                persisted_before,
                "failed truncate must preserve persistent table metadata"
            );
            s.tk.must_query("select tidb_table_id from information_schema.tables where table_schema='test' and table_name='t'")
                .check([before.id.to_string()]);
            if with_policy {
                s.tk.must_query("select table_id from mysql.tidb_masking_policy where policy_name='mp'")
                    .check([before.id.to_string()]);
            }
            s.tk.must_query("select * from t").check(["1"]);
            require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&before), true, "failed truncate must keep the original groups");
            if after_create {
                require_affinity_groups_with_label_rules(&s.cli, &s.cli.last_created(), false, "failed truncate must clean up new table groups");
            }
        }
        s.tk.must_exec("truncate table t");
        s.tk.must_query("select * from t").check(Vec::<String>::new());
    }
}

/// Go `TestAffinityTruncatePartitionCreateResponseLoss`.
#[test]
fn test_affinity_truncate_partition_create_response_loss() {
    for fail_delete in [false, true] {
        let mut s = new_affinity_ddl_test();
        s.tk.must_exec(&format!("create table p(a int) affinity='partition' {PARTITIONS}"));
        s.tk.must_exec("insert into p values (1),(11)");
        let before = s.table_info("p");
        let persisted_before = json(&s.persisted(before.id));
        let original_groups = s.groups_of(&before);
        s.cli.fail_after_create.store(true, Ordering::SeqCst);
        s.cli.fail_delete.store(fail_delete, Ordering::SeqCst);
        // Go `TestAffinityTruncatePartitionCancelDuringCreate`: the in-step
        // rollback, not only the cancellation cleanup, removes the
        // replacement group.
        let removed_during_rollback = Arc::new(Mutex::new(None::<bool>));
        let rollback_hook = {
            let (cli, removed, original) = (Arc::clone(&s.cli), Arc::clone(&removed_during_rollback), original_groups.clone());
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |arg| {
                let job = job_arg(arg);
                let mut removed = removed.lock().unwrap();
                if job.type_ != ActionType::ACTION_TRUNCATE_TABLE_PARTITION || !job.is_cancelled() || removed.is_some() {
                    return;
                }
                let rules = cli.label_rules.lock().unwrap();
                *removed = Some(cli.last_created().keys().all(|id| original.contains_key(id) || !rules.contains_key(id)));
            })
        };
        let error = error_text(&mut s.tk, "alter table p truncate partition p0");
        drop(rollback_hook);
        assert!(error.contains("injected response loss"), "{error}");
        assert_eq!(
            *removed_during_rollback.lock().unwrap(),
            Some(!fail_delete),
            "the in-step rollback must remove the replacement group's label rule"
        );
        s.cli.fail_delete.store(false, Ordering::SeqCst);
        assert_eq!(json(&Some(s.table_info("p"))), json(&Some(before.clone_like_go())));
        assert_eq!(json(&s.persisted(before.id)), persisted_before);
        s.tk.must_query("select * from p order by a").check(["1", "11"]);
        require_affinity_groups_with_label_rules(&s.cli, &original_groups, true, "cleanup must preserve both original partition groups");
        let replacement_groups = s.cli.last_created();
        assert_eq!(replacement_groups.len(), 1, "the PD request must contain only the replacement partition group");
        for id in replacement_groups.keys() {
            assert!(!original_groups.contains_key(id));
        }
        require_affinity_groups_with_label_rules(&s.cli, &replacement_groups, fail_delete, "failed cleanup must not prevent cancellation");
    }
}

/// Go `TestAffinityTruncatePartitionFailure`.
#[test]
fn test_affinity_truncate_partition_failure() {
    let mut s = new_affinity_ddl_test();
    s.tk.must_exec(&format!("create table p(a int, b int, unique key idx(b) global) affinity='partition' {PARTITIONS}"));
    s.tk.must_exec("insert into p values (1,1),(2,2),(11,11),(12,12)");
    let before = s.table_info("p");
    s.cli.fail_create.store(true, Ordering::SeqCst);
    let error = error_text(&mut s.tk, "alter table p truncate partition p0");
    assert!(error.contains("injected affinity creation failure"), "{error}");
    s.cli.fail_create.store(false, Ordering::SeqCst);
    assert_eq!(
        json(&Some(s.table_info("p"))),
        json(&Some(before.clone_like_go())),
        "failed affinity creation must not publish new partition IDs or DDL state"
    );
    s.tk.must_query("select * from p order by a").check(["1 1", "2 2", "11 11", "12 12"]);
    s.tk.must_query("select * from p use index(idx) order by a").check(["1 1", "2 2", "11 11", "12 12"]);
    s.tk.must_exec("admin check table p");
    require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&before), true, "the groups must stay");

    // A failure after the replacement is visible must retry, never cancel the
    // job. This also protects jobs an older TiDB version started.
    let injected = Arc::new(AtomicBool::new(false));
    {
        let (cli, injected) = (Arc::clone(&s.cli), Arc::clone(&injected));
        let _step = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |arg| {
            let job = job_arg(arg);
            if job.type_ == ActionType::ACTION_TRUNCATE_TABLE_PARTITION
                && job.schema_state == SchemaState::DELETE_REORGANIZATION
                && !injected.swap(true, Ordering::SeqCst)
            {
                cli.fail_once.store(true, Ordering::SeqCst);
            }
        });
        s.tk.must_exec("alter table p truncate partition p0");
    }
    assert!(injected.load(Ordering::SeqCst));
    assert_eq!(
        s.cli.create_failures.load(Ordering::SeqCst),
        5,
        "four exhausted attempts before replacement and one during finalization"
    );
    let after = s.table_info("p");
    let definitions = |info: &TableInfo| info.get_partition_info().unwrap().read().definitions.snapshot();
    assert_ne!(definitions(&before)[0].id, definitions(&after)[0].id);
    assert_eq!(definitions(&before)[1].id, definitions(&after)[1].id);
    assert_eq!(after.get_partition_info().unwrap().read().ddl_state, SchemaState::NONE);
    assert!(after.get_partition_info().unwrap().read().dropping_definitions.snapshot().is_empty());
    s.tk.must_query("select * from p order by a").check(["11 11", "12 12"]);
    s.tk.must_exec("admin check table p");
    require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&after), true, "the new groups must exist");
    let old_id = tidb_ddl::ddl_affinity::get_partition_affinity_group_id(before.id, definitions(&before)[0].id);
    assert!(tidb_domain_affinity::get_groups(&[old_id]).unwrap().is_empty());
    let end_key = tidb_tablecodec::table_key::encode_table_prefix(definitions(&before)[0].id + 1)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    s.tk.must_query(&format!(
        "select count(*) from (select end_key from mysql.gc_delete_range union all select end_key from mysql.gc_delete_range_done) ranges_to_delete where end_key='{end_key}'"
    ))
    .check(["1"]);
    // Both the failed attempt and the successful retry reach PD.
    assert!(s.cli.create_calls.load(Ordering::SeqCst) > 2);
    // The retry above absorbs one failure inside PD's transient retries; a
    // whole failed final step (every retry refused) must also retry the
    // step, never cancel the job.
    {
        let (cli, armed) = (Arc::clone(&s.cli), Arc::new(AtomicBool::new(false)));
        let _before = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |arg| {
            let job = job_arg(arg);
            if job.type_ == ActionType::ACTION_TRUNCATE_TABLE_PARTITION
                && job.schema_state == SchemaState::DELETE_REORGANIZATION
                && !armed.swap(true, Ordering::SeqCst)
            {
                *cli.retry_error.lock().unwrap() = Some(PdHttpError::Message("injected affinity creation failure".to_owned()));
                cli.retry_failures.store(4, Ordering::SeqCst);
            }
        });
        let calls = s.cli.create_calls.load(Ordering::SeqCst);
        s.tk.must_exec("alter table p truncate partition p1");
        assert_eq!(s.cli.retry_failures.load(Ordering::SeqCst), 0, "the final step's creation was refused");
        assert!(s.cli.create_calls.load(Ordering::SeqCst) - calls >= 5, "one whole failed final step, then its retry");
    }
    s.tk.must_query("select * from p order by a").check(Vec::<String>::new());
    s.tk.must_exec("admin check table p");
    require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&s.table_info("p")), true, "the groups after the retried final step");

    for scenario in ["delete succeeds", "delete fails", "metadata not committed"] {
        s.tk.must_exec("drop table if exists pc");
        s.tk.must_exec(&format!("create table pc(a int) affinity='partition' {PARTITIONS}"));
        s.tk.must_exec("insert into pc values (1),(11)");
        let before = s.table_info("pc");
        let persisted_before = s.persisted(before.id).unwrap();
        s.cli.fail_delete.store(scenario == "delete fails", Ordering::SeqCst);
        let cancelled = Arc::new(AtomicBool::new(false));
        let replacement_id = Arc::new(AtomicI64::new(0));
        let mut guards = Vec::new();
        const UPDATE_HOOK: &str = "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr";
        if scenario == "metadata not committed" {
            // Fail the first step after PD has created the replacement groups,
            // before the replacement ids reach the persistent metadata.
            guards.push(tidb_failpoint::enable_for_test(UPDATE_HOOK, "return(1)"));
            let table_id = before.id;
            guards.push(tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |arg| {
                let job = job_arg(arg);
                if job.table_id == table_id && job.schema_state == SchemaState::WRITE_ONLY {
                    tidb_failpoint::disable(UPDATE_HOOK);
                }
            }));
        }
        {
            let (cancelled, replacement_id, tk_cancel) = (Arc::clone(&cancelled), Arc::clone(&replacement_id), Arc::clone(&s.tk_cancel));
            let (table_id, metadata_not_committed) = (before.id, scenario == "metadata not committed");
            guards.push(tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |arg| {
                let job = job_arg(arg);
                let ready_to_cancel = if metadata_not_committed {
                    // The restoration keeps the schema state public after the
                    // failed step resets.
                    job.schema_state == SchemaState::PUBLIC && job.error_count > 0
                } else {
                    job.schema_state == SchemaState::WRITE_ONLY
                };
                if job.table_id == table_id
                    && job.type_ == ActionType::ACTION_TRUNCATE_TABLE_PARTITION
                    && ready_to_cancel
                    && !cancelled.swap(true, Ordering::SeqCst)
                {
                    let args = tidb_model::get_truncate_table_args(job).unwrap().unwrap();
                    replacement_id.store(args.read().new_partition_ids.get().snapshot()[0], Ordering::SeqCst);
                    tk_cancel.lock().unwrap().must_exec(&format!("admin cancel ddl jobs {}", job.id));
                }
            }));
        }
        assert!(s.tk.exec_to_err("alter table pc truncate partition p0").is_some(), "{scenario}");
        drop(guards);
        s.cli.fail_delete.store(false, Ordering::SeqCst);
        assert!(cancelled.load(Ordering::SeqCst), "{scenario}: expected partition truncate cancellation");
        assert_ne!(replacement_id.load(Ordering::SeqCst), 0);
        let after_cancel = s.table_info("pc");
        assert_eq!(after_cancel.get_partition_info().unwrap().read().ddl_state, SchemaState::NONE, "{scenario}");
        let persisted = s.persisted(before.id).unwrap();
        assert_eq!(
            serde_json::to_string(&persisted.partition).unwrap(),
            serde_json::to_string(&persisted_before.partition).unwrap(),
            "{scenario}"
        );
        assert_eq!(definitions(&after_cancel).iter().map(|d| d.id).collect::<Vec<_>>(), definitions(&before).iter().map(|d| d.id).collect::<Vec<_>>());
        assert!(after_cancel.get_partition_info().unwrap().read().new_partition_ids.is_empty());
        assert_eq!(after_cancel.get_partition_info().unwrap().read().ddl_action, ActionType::ACTION_NONE);
        s.tk.must_query("select * from pc order by a").check(["1", "11"]);
        require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&before), true, "the original groups stay");
        let new_group_id = tidb_ddl::ddl_affinity::get_partition_affinity_group_id(before.id, replacement_id.load(Ordering::SeqCst));
        let groups = tidb_domain_affinity::get_groups(std::slice::from_ref(&new_group_id)).unwrap();
        if scenario == "delete fails" {
            assert!(groups.contains_key(&new_group_id), "failed cleanup must not prevent metadata rollback");
        } else {
            assert!(groups.is_empty(), "{scenario}: successful rollback must remove replacement groups");
        }
    }
}

/// Go `TestAffinityTruncateTableCancelKeepsOriginalGroups`.
#[test]
fn test_affinity_truncate_table_cancel_keeps_original_groups() {
    let mut s = new_affinity_ddl_test();
    for level in ["table", "partition"] {
        for (point, want_err) in [
            ("beforeUpdateSchemaVersion", "injected schema version update failure"),
            ("asyncNotifyEventError", "mock publish event error"),
            ("afterUpdateJobToTable", "Cancelled DDL job"),
            ("truncateTableErr", "occur an error after dropping table"),
        ] {
            if point == "truncateTableErr" && level != "partition" {
                continue;
            }
            s.tk.must_exec("drop table if exists t");
            if level == "partition" {
                s.tk.must_exec(&format!("create table t(a int) affinity='partition' {PARTITIONS}"));
            } else {
                s.tk.must_exec("create table t(a int) affinity='table'");
            }
            s.tk.must_exec("insert into t values (1),(11)");
            let before = s.table_info("t");
            let persisted_before = json(&s.persisted(before.id));
            let original_groups = s.groups_of(&before);
            let table_id = before.id;
            let matches = move |job: &Job| job.type_ == ActionType::ACTION_TRUNCATE_TABLE && job.table_id == table_id;
            let error = fail_and_cancel(&mut s, point, matches, |s| error_text(&mut s.tk, "truncate table t"));
            assert!(error.contains(want_err), "{level}/{point}: {error}");
            assert_eq!(json(&s.persisted(before.id)), persisted_before, "{level}/{point}: cancelled truncate must preserve table metadata");
            s.tk.must_query("select * from t order by a").check(["1", "11"]);
            require_affinity_groups_with_label_rules(&s.cli, &original_groups, true, "cancelled truncate must keep the original groups");
            let replacement_groups = s.cli.last_created();
            assert_eq!(replacement_groups.len(), original_groups.len(), "{level}/{point}");
            for id in replacement_groups.keys() {
                assert!(!original_groups.contains_key(id), "the failed attempt must create replacement groups");
            }
            require_affinity_groups_with_label_rules(&s.cli, &replacement_groups, false, "cancelled truncate must delete the replacement groups");

            s.tk.must_exec("truncate table t");
            s.tk.must_query("select * from t").check(Vec::<String>::new());
            let truncated = s.table_info("t");
            require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&truncated), true, "committed truncate must create the new groups");
            require_affinity_groups_with_label_rules(&s.cli, &original_groups, false, "committed truncate must delete the original groups");
        }
    }
}

/// Go `failAndCancel`: injects a failure after the PD group creation. Most
/// cases cancel the retry through a separate session; the event publication
/// uses the error limit, and `truncateTableErr` cancels before PD.
fn fail_and_cancel(
    s: &mut AffinityDdlTest,
    point: &str,
    matches: impl Fn(&Job) -> bool + Send + Sync + Clone + 'static,
    run: impl FnOnce(&mut AffinityDdlTest) -> String,
) -> String {
    const PREFIX: &str = "github.com/pingcap/tidb/pkg/ddl/";
    let failed = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut guards = Vec::new();
    let inject_schema_version = {
        let (failed, matches) = (Arc::clone(&failed), matches.clone());
        move |arg: &mut dyn std::any::Any| {
            let call = arg.downcast_mut::<(Job, Option<String>)>().unwrap();
            if matches(&call.0) && !failed.swap(true, Ordering::SeqCst) {
                call.1 = Some("injected schema version update failure".to_owned());
            }
        }
    };
    match point {
        "asyncNotifyEventError" => {
            s.tk.must_exec("set @@global.tidb_ddl_error_count_limit = 1");
            guards.push(tidb_failpoint::enable_for_test(&format!("{PREFIX}{point}"), "return"));
        }
        "truncateTableErr" => {
            guards.push(tidb_failpoint::enable_call_for_test(&format!("{PREFIX}beforeUpdateSchemaVersion"), inject_schema_version));
            guards.push(tidb_failpoint::enable_for_test(&format!("{PREFIX}{point}"), "1*return(false)->return(true)"));
        }
        "beforeUpdateSchemaVersion" => {
            guards.push(tidb_failpoint::enable_call_for_test(&format!("{PREFIX}{point}"), inject_schema_version));
        }
        "afterUpdateJobToTable" => {
            let (failed, matches) = (Arc::clone(&failed), matches.clone());
            guards.push(tidb_failpoint::enable_call_ref_for_test::<tidb_ddl::cluster_ddl::AfterUpdateJobToTable>(
                &format!("{PREFIX}{point}"),
                Arc::new(move |job, err| {
                    if matches(job) && !failed.swap(true, Ordering::SeqCst) {
                        *err = Some(tidb_ddl_build::plan_error::DdlPlanError::Unsupported(
                            "injected schema version update failure".to_owned(),
                        ));
                    }
                }),
            ));
        }
        other => panic!("unknown failure point {other}"),
    }
    if point != "asyncNotifyEventError" && point != "truncateTableErr" {
        let (failed, cancelled, tk_cancel) = (Arc::clone(&failed), Arc::clone(&cancelled), Arc::clone(&s.tk_cancel));
        guards.push(tidb_failpoint::enable_call_for_test(&format!("{PREFIX}beforeRefreshJob"), move |arg| {
            let job = job_arg(arg);
            if matches(job) && failed.load(Ordering::SeqCst) && !cancelled.swap(true, Ordering::SeqCst) {
                tk_cancel.lock().unwrap().must_exec(&format!("admin cancel ddl jobs {}", job.id));
            }
        }));
    }
    let error = run(s);
    drop(guards);
    if point == "asyncNotifyEventError" {
        s.tk.must_exec("set @@global.tidb_ddl_error_count_limit = default");
    } else {
        assert!(failed.load(Ordering::SeqCst), "the failed attempt must precede cancellation");
        if point != "truncateTableErr" {
            assert!(cancelled.load(Ordering::SeqCst), "the retry must be cancelled by the user");
        }
    }
    error
}

/// Go `captureCreateJob`: the table infos of the first job of type `tp`.
fn capture_create_job(tp: ActionType) -> (Arc<Mutex<Option<Job>>>, tidb_failpoint::Guard) {
    let captured = Arc::new(Mutex::new(None::<Job>));
    let sink = Arc::clone(&captured);
    let guard = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |arg| {
        let job = job_arg(arg);
        let mut captured = sink.lock().unwrap();
        if job.type_ == tp && job.schema_name.to_string() == "test" && captured.is_none() {
            *captured = Some(job.clone());
        }
    });
    (captured, guard)
}

fn captured_tables(captured: &Mutex<Option<Job>>) -> Vec<TableInfo> {
    let mut job = captured.lock().unwrap().clone().expect("the create job must have run");
    if job.type_ == ActionType::ACTION_CREATE_TABLES {
        let args = tidb_model::get_batch_create_table_args(&mut job).unwrap().unwrap();
        return args
            .read()
            .tables
            .get()
            .handles()
            .into_iter()
            .map(|table| table.unwrap().read().table_info.get().unwrap().read().clone_like_go())
            .collect();
    }
    let args = tidb_model::get_create_table_args(&mut job).unwrap().unwrap();
    let info = args.read().table_info.get().unwrap().read().clone_like_go();
    vec![info]
}

fn with_affinity(info: &TableInfo, level: &str) -> TableInfo {
    let mut info = info.clone_like_go();
    info.affinity = tidb_model::new_table_affinity_info_with_level(level).unwrap().map(GoShared::new);
    info
}

/// Go `TestAffinityCancelledDDLGroupCleanup`: partial PD creation, and a
/// later step failure followed by cancellation, for CREATE and ALTER.
#[test]
fn test_affinity_cancelled_ddl_group_cleanup() {
    let mut s = new_affinity_ddl_test();
    let fail_creation = |cli: &TruncateAffinityPdClient, skip: i64| {
        cli.partial_created_ids.lock().unwrap().clear();
        cli.partial_create_skip.store(skip, Ordering::SeqCst);
        cli.partial_create_limit.store(1, Ordering::SeqCst);
    };
    let require_partially_created = |cli: &TruncateAffinityPdClient, groups: &AffinityGroups| {
        assert_eq!(cli.partial_create_limit.load(Ordering::SeqCst), 0, "the partial creation failure must have been injected");
        let committed = cli.partial_created_ids.lock().unwrap().clone();
        assert!(!committed.is_empty());
        for id in &committed {
            assert!(groups.contains_key(id));
        }
    };

    for (level, delayed) in [("table", false), ("partition", false), ("partition", true)] {
        s.tk.must_exec("drop table if exists c");
        let mut create_sql = format!("create table c(a int) affinity='{level}'");
        if level == "partition" {
            create_sql = format!("{create_sql} {PARTITIONS}");
        }
        let (captured, capture) = capture_create_job(ActionType::ACTION_CREATE_TABLE);
        let error = if delayed {
            let sql = create_sql.clone();
            fail_and_cancel(&mut s, "beforeUpdateSchemaVersion", |job| job.type_ == ActionType::ACTION_CREATE_TABLE, move |s| error_text(&mut s.tk, &sql))
        } else {
            fail_creation(&s.cli, 0);
            error_text(&mut s.tk, &create_sql)
        };
        drop(capture);
        let want = if delayed { "injected schema version update failure" } else { "injected affinity creation failure after 1 of" };
        assert!(error.contains(want), "create/{level}/{delayed}: {error}");
        let created = captured_tables(&captured).remove(0);
        let groups = s.groups_of(&created);
        if delayed {
            s.cli.require_ever_created(&groups);
        } else {
            require_partially_created(&s.cli, &groups);
        }
        assert!(s.persisted(created.id).is_none());
        assert!(error_text(&mut s.tk, "select * from c").contains("doesn't exist"));
        require_affinity_groups_with_label_rules(&s.cli, &groups, false, "cancelled CREATE must delete unused groups");
        if !delayed {
            s.tk.must_exec(&create_sql);
            require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&s.table_info("c")), true, "the created table has its groups");
        }
    }

    // ALTER, with the old level set through SQL where SQL allows it.
    for (name, old_level, new_level, partitioned) in [
        ("none to partition", "", "partition", true),
        ("partition to none", "partition", "", true),
        ("unchanged table", "table", "table", false),
        ("unchanged partition", "partition", "partition", true),
    ] {
        for delayed in [false, true] {
            if (!delayed && new_level.is_empty()) || (delayed && old_level == new_level) {
                continue;
            }
            s.tk.must_exec("drop table if exists a");
            s.tk.must_exec(&if partitioned { format!("create table a(a int) {PARTITIONS}") } else { "create table a(a int)".to_owned() });
            if !old_level.is_empty() {
                s.tk.must_exec(&format!("alter table a affinity='{old_level}'"));
            }
            let before = s.table_info("a");
            let persisted_before = json(&s.persisted(before.id));
            let old_groups = s.groups_of(&before);
            let new_groups = (!new_level.is_empty()).then(|| s.groups_of(&with_affinity(&before, new_level)));
            let alter_sql = format!("alter table a affinity='{new_level}'");
            if delayed {
                let (sql, table_id) = (alter_sql.clone(), before.id);
                let error = fail_and_cancel(
                    &mut s,
                    "beforeUpdateSchemaVersion",
                    move |job| job.type_ == ActionType::ACTION_ALTER_TABLE_AFFINITY && job.table_id == table_id,
                    move |s| error_text(&mut s.tk, &sql),
                );
                assert!(error.contains("injected schema version update failure"), "alter/{name}: {error}");
                if let Some(new_groups) = &new_groups {
                    s.cli.require_ever_created(new_groups);
                }
            } else {
                fail_creation(&s.cli, 0);
                let error = error_text(&mut s.tk, &alter_sql);
                assert!(error.contains("injected affinity creation failure after 1 of"), "alter/{name}: {error}");
                require_partially_created(&s.cli, new_groups.as_ref().unwrap());
            }
            assert_eq!(json(&s.persisted(before.id)), persisted_before, "alter/{name}: cancelled ALTER must keep its metadata");
            if !old_groups.is_empty() {
                require_affinity_groups_with_label_rules(&s.cli, &old_groups, true, "cancelled ALTER must keep its original groups");
            }
            if let Some(new_groups) = &new_groups {
                require_affinity_groups_with_label_rules(&s.cli, new_groups, old_level == new_level, "cancelled ALTER must delete only groups of a new level");
            }
            if old_level != new_level {
                s.tk.must_exec(&alter_sql);
                if let Some(new_groups) = &new_groups {
                    require_affinity_groups_with_label_rules(&s.cli, new_groups, true, "committed ALTER must create the new groups");
                }
                if !old_groups.is_empty() {
                    require_affinity_groups_with_label_rules(&s.cli, &old_groups, false, "committed ALTER must delete the old groups");
                }
            }
        }
    }
}

/// Go `TestAffinityTruncateTablePartitionGrowth` (the response-loss
/// variant): a TRUNCATE queued before the table gained a partition reserves
/// the missing replacement id in its own step, then cancels on the PD
/// failure and leaves no replacement group behind.
#[test]
fn test_affinity_truncate_table_partition_growth() {
    let version = tidb_model::get_job_ver_in_use();
    for job_version in [tidb_model::JobVersion::V1, tidb_model::JobVersion::V2] {
        tidb_model::set_job_ver_in_use(job_version);
        let mut s = new_affinity_ddl_test();
        s.tk.must_exec("create table p(a int) partition by range(a) (partition p0 values less than (10))");
        s.tk.must_exec("insert into p values (1)");
        let original = s.table_info("p");
        let (read_table_tx, read_table_rx) = std::sync::mpsc::channel::<()>();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        let resume_rx = Mutex::new(resume_rx);
        let read_table_tx = Mutex::new(read_table_tx);
        let held = AtomicBool::new(false);
        let original_id = original.id;
        let _hold = tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/ddl/afterCheckTruncateTableMaterializedViewConstraints",
            move |arg| {
                if *arg.downcast_mut::<i64>().unwrap() == original_id && !held.swap(true, Ordering::SeqCst) {
                    read_table_tx.lock().unwrap().send(()).unwrap();
                    let _ = resume_rx.lock().unwrap().recv();
                }
            },
        );
        let job_id = Arc::new(AtomicI64::new(0));
        let initial_ids = Arc::new(AtomicI64::new(-1));
        let _step = {
            let (job_id, initial_ids, cli) = (Arc::clone(&job_id), Arc::clone(&initial_ids), Arc::clone(&s.cli));
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |arg| {
                let job = job_arg(arg);
                if job.type_ == ActionType::ACTION_TRUNCATE_TABLE
                    && job.table_id == original_id
                    && job_id.compare_exchange(0, job.id, Ordering::SeqCst, Ordering::SeqCst).is_ok()
                {
                    let args = tidb_model::get_truncate_table_args(job).unwrap().unwrap();
                    initial_ids.store(args.read().new_partition_ids.get().len() as i64, Ordering::SeqCst);
                    cli.fail_after_create.store(true, Ordering::SeqCst);
                }
            })
        };
        let mut tk = testkit(&s.stack, 9303);
        tk.must_exec("use test");
        let truncate = std::thread::spawn(move || error_text(&mut tk, "truncate table p"));
        read_table_rx.recv_timeout(std::time::Duration::from_secs(30)).expect("TRUNCATE reads the table");
        s.tk.must_exec("alter table p add partition (partition p1 values less than (20))");
        s.tk.must_exec("alter table p affinity='partition'");
        let original_groups = s.groups_of(&s.table_info("p"));
        resume_tx.send(()).unwrap();
        let error = truncate.join().unwrap();
        assert!(error.contains("injected response loss after affinity creation"), "{job_version:?}: {error}");
        assert_eq!(initial_ids.load(Ordering::SeqCst), 1);
        let created = s.cli.last_created();
        assert_eq!(created.len(), 2, "{job_version:?}");
        let after = s.table_info("p");
        assert_eq!(after.get_partition_info().unwrap().read().definitions.len(), 2);
        assert_eq!(after.id, original.id);
        s.tk.must_query("select * from p").check(["1"]);
        require_affinity_groups_with_label_rules(&s.cli, &original_groups, true, "old groups follow the metadata commit");
        require_affinity_groups_with_label_rules(&s.cli, &created, false, "all replacement groups follow the DDL outcome");
    }
    tidb_model::set_job_ver_in_use(version);
}

/// Go `TestAffinityCreateTransient503`.
#[test]
fn test_affinity_create_transient_503() {
    let mut s = new_affinity_ddl_test();
    s.cli.fail_503_once.store(true, Ordering::SeqCst);
    s.tk.must_exec("create table retry_create(a int) affinity='table'");
    assert_eq!(s.cli.create_failures.load(Ordering::SeqCst), 1);
    assert_eq!(s.cli.create_calls.load(Ordering::SeqCst), 2);
    require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&s.table_info("retry_create")), true, "created after one retry");
    s.tk.must_exec("create table retry_alter(a int)");
    s.cli.fail_503_once.store(true, Ordering::SeqCst);
    s.tk.must_exec("alter table retry_alter affinity='table'");
    assert_eq!(s.cli.create_failures.load(Ordering::SeqCst), 2);
    require_affinity_groups_with_label_rules(&s.cli, &s.groups_of(&s.table_info("retry_alter")), true, "altered after one retry");
}
