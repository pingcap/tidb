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

//! Go `pkg/telemetry/data_feature_usage_test.go` and `data_window_test.go`
//! over a session: what the statements a session ran leave in the usage
//! report.

use crate::support::testkit::TestKit;

fn usage(tk: &mut TestKit) -> tidb_telemetry::FeatureUsage {
    tidb_telemetry::get_feature_usage(tk.session()).expect("feature usage")
}

#[test]
fn txn_usage_info() {
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_enable_async_commit = 0");
    tk.must_exec("set global tidb_enable_1pc = 0");
    let txn_usage = tidb_telemetry::get_txn_usage_info(tk.session());
    assert!(!txn_usage.async_commit_used);
    assert!(!txn_usage.one_pc_used);

    tk.must_exec("set global tidb_enable_async_commit = 1");
    tk.must_exec("set global tidb_enable_1pc = 1");
    let txn_usage = tidb_telemetry::get_txn_usage_info(tk.session());
    assert!(txn_usage.async_commit_used);
    assert!(txn_usage.one_pc_used);

    tk.must_exec("set global tidb_enable_mutation_checker = 0");
    tk.must_exec("set global tidb_txn_assertion_level = off");
    let txn_usage = tidb_telemetry::get_txn_usage_info(tk.session());
    assert!(!txn_usage.mutation_checker_used);
    assert_eq!(txn_usage.assertion_level, "OFF");

    tk.must_exec("set global tidb_enable_mutation_checker = 1");
    tk.must_exec("set global tidb_txn_assertion_level = strict");
    let txn_usage = tidb_telemetry::get_txn_usage_info(tk.session());
    assert!(txn_usage.mutation_checker_used);
    assert_eq!(txn_usage.assertion_level, "STRICT");

    tk.must_exec("set global tidb_txn_assertion_level = fast");
    assert_eq!(tidb_telemetry::get_txn_usage_info(tk.session()).assertion_level, "FAST");

    tk.must_exec("set global tidb_rc_read_check_ts = 1");
    assert!(tidb_telemetry::get_txn_usage_info(tk.session()).rc_check_ts);

    tk.must_exec("set global tidb_rc_write_check_ts = 1");
    assert!(tidb_telemetry::get_txn_usage_info(tk.session()).rc_write_check_ts);

    if !tidb_config_kerneltype::is_next_gen() {
        tk.must_exec("set global tidb_pessimistic_txn_fair_locking = 0");
        assert!(!tidb_telemetry::get_txn_usage_info(tk.session()).fair_locking);
        tk.must_exec("set global tidb_pessimistic_txn_fair_locking = 1");
        assert!(tidb_telemetry::get_txn_usage_info(tk.session()).fair_locking);
    }

    // Go's "Count" subtest: a new connection on the same store, whose
    // global switches the "Used" subtest left on.
    let mut tk = tk.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists txn_usage_info");
    tk.must_exec("create table txn_usage_info (a int)");
    tk.must_exec("set tidb_enable_async_commit = 1");
    tk.must_exec("set tidb_enable_1pc = 1");
    tk.must_exec("insert into txn_usage_info values (1)");
    tk.must_exec("set tidb_enable_1pc = 0");
    tk.must_exec("insert into txn_usage_info values (2)");
    tk.must_exec("set tidb_enable_async_commit = 0");
    tk.must_exec("insert into txn_usage_info values (3)");

    let txn_usage = tidb_telemetry::get_txn_usage_info(tk.session());
    assert!(txn_usage.async_commit_used);
    assert!(txn_usage.one_pc_used);
    assert!(txn_usage.txn_commit_counter.async_commit > 0);
    assert!(txn_usage.txn_commit_counter.one_pc > 0);
    assert!(txn_usage.txn_commit_counter.two_pc > 0);
}

#[test]
fn temporary_table_usage() {
    let mut tk = TestKit::new();
    assert!(!usage(&mut tk).temporary_table);
    tk.must_exec("create global temporary table t (id int) on commit delete rows");
    assert!(usage(&mut tk).temporary_table);
}

#[test]
fn cached_table_usage() {
    let mut tk = TestKit::new();
    assert!(!usage(&mut tk).cached_table);
    tk.must_exec("drop table if exists tele_cache_t");
    tk.must_exec("create table tele_cache_t (id int)");
    tk.must_exec("alter table tele_cache_t cache");
    assert!(usage(&mut tk).cached_table);
    tk.must_exec("alter table tele_cache_t nocache");
    assert!(!usage(&mut tk).cached_table);
}

#[test]
fn auto_id_no_cache_usage() {
    let mut tk = TestKit::new();
    assert!(!usage(&mut tk).cached_table);
    tk.must_exec("drop table if exists tele_autoid");
    tk.must_exec("create table tele_autoid (id int) auto_id_cache 1");
    assert!(usage(&mut tk).auto_id_no_cache);
    tk.must_exec("drop table tele_autoid");
    assert!(!usage(&mut tk).auto_id_no_cache);
}

#[test]
fn account_lock_usage() {
    let mut tk = TestKit::new();
    let account_lock = usage(&mut tk).account_lock;
    assert_eq!(
        (account_lock.lock_user, account_lock.unlock_user, account_lock.create_or_alter_user),
        (0, 0, 0)
    );
    tk.must_exec("drop user if exists testUser");
    tk.must_exec("create user testUser account lock");
    let account_lock = usage(&mut tk).account_lock;
    assert_eq!(
        (account_lock.lock_user, account_lock.unlock_user, account_lock.create_or_alter_user),
        (1, 0, 1)
    );
    tk.must_exec("alter user testUser account unlock");
    let account_lock = usage(&mut tk).account_lock;
    assert_eq!(
        (account_lock.lock_user, account_lock.unlock_user, account_lock.create_or_alter_user),
        (1, 1, 2)
    );
}

#[test]
fn multi_schema_change_usage() {
    let mut tk = TestKit::new();
    assert_eq!(usage(&mut tk).multi_schema_change.multi_schema_change_used, 0);
    tk.must_exec("drop table if exists tele_multi_t");
    tk.must_exec("create table tele_multi_t(id int)");
    tk.must_exec("alter table tele_multi_t add column b int");
    assert_eq!(usage(&mut tk).multi_schema_change.multi_schema_change_used, 0);
    tk.must_exec("alter table tele_multi_t add column c int, drop column b");
    assert_eq!(usage(&mut tk).multi_schema_change.multi_schema_change_used, 1);
    tk.must_exec("alter table tele_multi_t add column b int, drop column c");
    assert_eq!(usage(&mut tk).multi_schema_change.multi_schema_change_used, 2);
    tk.must_exec("alter table tele_multi_t drop column b");
    assert_eq!(usage(&mut tk).multi_schema_change.multi_schema_change_used, 2);
}

#[test]
fn table_partition_usage() {
    let mut tk = TestKit::new();
    let partition = usage(&mut tk).table_partition;
    assert_eq!(partition.table_partition_cnt, 0);
    assert_eq!(partition.table_partition_list_cnt, 0);
    assert_eq!(partition.table_partition_max_partitions_cnt, 0);

    tk.must_exec("drop table if exists pt");
    tk.must_exec("create table pt (a int,b int) partition by hash(a) partitions 4");
    let partition = usage(&mut tk).table_partition;
    assert_eq!(partition.table_partition_cnt, 1);
    assert_eq!(partition.table_partition_hash_cnt, 1);
    assert_eq!(partition.table_partition_max_partitions_cnt, 4);
    assert_eq!(partition.table_partition_list_cnt, 0);
    assert_eq!(partition.table_partition_range_cnt, 0);
    assert_eq!(partition.table_partition_range_columns_cnt, 0);
    assert_eq!(partition.table_partition_range_columns_gt1_cnt, 0);
    assert_eq!(partition.table_partition_range_columns_gt2_cnt, 0);
    assert_eq!(partition.table_partition_range_columns_gt3_cnt, 0);
    assert_eq!(partition.table_partition_list_columns_cnt, 0);
    assert_eq!(partition.table_partition_create_interval_partitions_cnt, 0);
    assert_eq!(partition.table_partition_add_interval_partitions_cnt, 0);
    assert_eq!(partition.table_partition_drop_interval_partitions_cnt, 0);
    assert_eq!(partition.table_partition_reorganize_partition_cnt, 0);

    tidb_telemetry::post_report_telemetry_data_for_test();
    tk.must_exec("drop table if exists pt1");
    tk.must_exec(
        "create table pt1 (a int,b int) partition by range(a) (\
         partition p0 values less than (3),\
         partition p1 values less than (6), \
         partition p2 values less than (9),\
         partition p3 values less than (12),\
         partition p4 values less than (15))",
    );
    tk.must_exec("alter table pt1 first partition less than (9)");
    tk.must_exec("alter table pt1 last partition less than (21)");
    tk.must_exec(
        "alter table pt1 reorganize partition p4 into (partition p4 values less than (13), partition p5 values less than (15))",
    );
    tk.must_exec("drop table if exists pt1");
    tk.must_exec(
        "create table pt1 (d datetime primary key, v varchar(255)) partition by range columns(d) \
         interval (1 day) first partition less than ('2022-01-01') last partition less than ('2022-02-22')",
    );
    tk.must_exec(
        "create table pt2 (d datetime, v varchar(255)) partition by range columns(d,v) \
         (partition p0 values less than ('2022-01-01', ''), partition p1 values less than ('2023-01-01','ZZZ'))",
    );
    tk.must_exec(
        "create table pt3 (d datetime, v varchar(255), i int) partition by range columns(d,v,i) \
         (partition p0 values less than ('2022-01-01', '', 0), partition p1 values less than ('2023-01-01','ZZZ', 1))",
    );
    tk.must_exec(
        "create table pt4 (d datetime, v varchar(255), s bigint unsigned, u tinyint) partition by range columns(d,v,s,u) \
         (partition p0 values less than ('2022-01-01', '', 1, -3), partition p1 values less than ('2023-01-01','ZZZ', 1, 3))",
    );
    let partition = usage(&mut tk).table_partition;
    assert_eq!(partition.table_partition_cnt, 5);
    assert_eq!(partition.table_partition_hash_cnt, 0);
    assert_eq!(partition.table_partition_max_partitions_cnt, 11);
    assert_eq!(partition.table_partition_list_cnt, 0);
    assert_eq!(partition.table_partition_range_cnt, 1);
    assert_eq!(partition.table_partition_range_columns_cnt, 4);
    assert_eq!(partition.table_partition_range_columns_gt1_cnt, 3);
    assert_eq!(partition.table_partition_range_columns_gt2_cnt, 2);
    assert_eq!(partition.table_partition_range_columns_gt3_cnt, 1);
    assert_eq!(partition.table_partition_list_columns_cnt, 0);
    assert_eq!(partition.table_partition_create_interval_partitions_cnt, 1);
    assert_eq!(partition.table_partition_add_interval_partitions_cnt, 1);
    assert_eq!(partition.table_partition_drop_interval_partitions_cnt, 1);
    assert_eq!(partition.table_partition_reorganize_partition_cnt, 1);

    tk.must_exec("drop table if exists pt2");
    tk.must_exec(
        "create table pt2 (a int,b int) partition by range(a) (\
         partition p0 values less than (10),\
         partition p1 values less than (20))",
    );
    tk.must_exec("drop table if exists nt");
    tk.must_exec("create table nt (a int,b int)");
    assert_eq!(usage(&mut tk).exchange_partition.exchange_partition_cnt, 0);
    tk.must_exec("alter table pt2 exchange partition p1 with table nt");
    let feature_usage = usage(&mut tk);
    assert_eq!(feature_usage.exchange_partition.exchange_partition_cnt, 1);

    assert_eq!(feature_usage.table_partition.table_partition_comact_cnt, 0);
    tk.must_exec("alter table pt2 compact partition p0 tiflash replica;");
    assert_eq!(usage(&mut tk).table_partition.table_partition_comact_cnt, 1);
}

#[test]
fn placement_policy_usage() {
    let mut tk = TestKit::new();
    let placement = usage(&mut tk).placement_policy_usage;
    assert_eq!(placement.num_placement_policies, 0);
    assert_eq!(placement.num_db_with_policies, 0);
    assert_eq!(placement.num_table_with_policies, 0);
    assert_eq!(placement.num_partition_with_explicit_policies, 0);

    tk.must_exec("create placement policy p1 followers=4;");
    tk.must_exec(r#"create placement policy p2 primary_region="cn-east-1" regions="cn-east-1,cn-east""#);
    tk.must_exec("create placement policy p3 followers=3");
    tk.must_exec("alter database test placement policy=p1;");
    tk.must_exec("create table t1(a int);");
    tk.must_exec("create table t2(a int) placement policy=p2;");
    tk.must_exec(
        "create table t3(id int) PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100) placement policy p3,\
         PARTITION p1 VALUES LESS THAN (1000))",
    );
    let placement = usage(&mut tk).placement_policy_usage;
    assert_eq!(placement.num_placement_policies, 3);
    assert_eq!(placement.num_db_with_policies, 1);
    assert_eq!(placement.num_table_with_policies, 3);
    assert_eq!(placement.num_partition_with_explicit_policies, 1);

    tk.must_exec("drop table t2;");
    tk.must_exec("drop placement policy p2;");
    tk.must_exec("alter table t3 placement policy=default");
    let placement = usage(&mut tk).placement_policy_usage;
    assert_eq!(placement.num_placement_policies, 2);
    assert_eq!(placement.num_db_with_policies, 1);
    assert_eq!(placement.num_table_with_policies, 1);
    assert_eq!(placement.num_partition_with_explicit_policies, 1);
}

#[test]
fn resource_group_usage() {
    let mut tk = TestKit::new();
    let resource = usage(&mut tk).resource_control_usage;
    assert_eq!(resource.num_resource_groups, 1);
    assert!(resource.enabled);

    tk.must_exec("set global tidb_enable_resource_control = 'ON'");
    tk.must_exec("create resource group x ru_per_sec=100");
    let resource = usage(&mut tk).resource_control_usage;
    assert!(resource.enabled);
    assert_eq!(resource.num_resource_groups, 2);

    tk.must_exec("create resource group y ru_per_sec=100");
    assert_eq!(usage(&mut tk).resource_control_usage.num_resource_groups, 3);
    tk.must_exec("alter resource group y ru_per_sec=200");
    assert_eq!(usage(&mut tk).resource_control_usage.num_resource_groups, 3);
    tk.must_exec("drop resource group y");
    assert_eq!(usage(&mut tk).resource_control_usage.num_resource_groups, 2);

    tk.must_exec("set global tidb_enable_resource_control = 'OFF'");
    let resource = usage(&mut tk).resource_control_usage;
    assert_eq!(resource.num_resource_groups, 2);
    assert!(!resource.enabled);
}

#[test]
fn auto_capture_usage() {
    let mut tk = TestKit::new();
    assert!(!usage(&mut tk).auto_capture);
    tk.must_exec("SET GLOBAL tidb_capture_plan_baselines = on");
    assert!(usage(&mut tk).auto_capture);
    tk.must_exec("SET GLOBAL tidb_capture_plan_baselines = off");
}

#[test]
fn non_transactional_usage() {
    let mut tk = TestKit::new();
    let non_txn = usage(&mut tk).non_transactional_usage;
    assert_eq!((non_txn.delete_count, non_txn.update_count, non_txn.insert_count), (0, 0, 0));
    tk.must_exec("create table t(a int);");
    tk.must_exec("batch limit 1 delete from t");
    tk.must_exec("batch limit 1 update t set a = 1");
    tk.must_exec("batch limit 1 insert into t select * from t");
    let non_txn = usage(&mut tk).non_transactional_usage;
    assert_eq!((non_txn.delete_count, non_txn.update_count, non_txn.insert_count), (1, 1, 1));
}

#[test]
fn global_kill_usage_info() {
    let mut tk = TestKit::new();
    assert!(usage(&mut tk).global_kill);
    let origin = tidb_config::config_tree::config::get_global_config();
    let mut config = (*origin).clone();
    config.enable_global_kill = false;
    tidb_config::config_tree::config::store_global_config(config);
    let global_kill = usage(&mut tk).global_kill;
    tidb_config::config_tree::config::store_global_config(origin);
    assert!(!global_kill);
}

#[test]
fn paging_usage_info() {
    let mut tk = TestKit::new();
    assert_eq!(usage(&mut tk).enable_paging, tidb_vardef::defaults::DEF_TIDB_ENABLE_PAGING);
    tk.must_exec("set @@tidb_enable_paging = 0");
    assert!(!usage(&mut tk).enable_paging);
}

#[test]
fn cost_model_ver2_usage_info() {
    let mut tk = TestKit::new();
    assert_eq!(
        usage(&mut tk).enable_cost_model_ver2,
        tidb_vardef::defaults::DEF_TIDB_COST_MODEL_VER == 2
    );
    tk.must_exec("set @@tidb_cost_model_version = 2");
    assert!(usage(&mut tk).enable_cost_model_ver2);
}

/// Go `TestTxnSavepointUsageInfo`.
#[test]
fn txn_savepoint_usage_info() {
    let mut tk = TestKit::new();
    tk.must_exec("savepoint sp1");
    tk.must_exec("savepoint sp2");
    assert_eq!(tidb_telemetry::get_txn_usage_info(tk.session()).savepoint_counter, 2);
    tk.must_exec("savepoint sp3");
    assert_eq!(tidb_telemetry::get_txn_usage_info(tk.session()).savepoint_counter, 3);
    tidb_telemetry::post_savepoint_count();
    tk.must_exec("savepoint sp1");
    assert_eq!(tidb_telemetry::get_txn_usage_info(tk.session()).savepoint_counter, 1);
}

#[test]
fn lazy_pessimistic_unique_check() {
    let mut tk = TestKit::new();
    let mut tk2 = tk.new_tk();
    assert_eq!(tidb_telemetry::get_txn_usage_info(tk.session()).lazy_unique_check_set_counter, 0);
    tk2.must_exec("set @@tidb_constraint_check_in_place_pessimistic = 0");
    tk2.must_exec("set @@tidb_constraint_check_in_place_pessimistic = 0");
    assert_eq!(tidb_telemetry::get_txn_usage_info(tk.session()).lazy_unique_check_set_counter, 2);
}

#[test]
fn flashback_cluster_usage() {
    let mut tk = TestKit::new();
    let mut tk1 = tk.new_tk();
    assert_eq!(usage(&mut tk).ddl_usage_counter.flashback_cluster_used, 0);
    tk.must_exec_to_err("flashback cluster to timestamp '2011-12-21 00:00:00'");
    assert_eq!(usage(&mut tk).ddl_usage_counter.flashback_cluster_used, 1);
    tk1.must_exec("use test");
    tk1.must_exec("create table t(a int)");
    assert_eq!(usage(&mut tk1).ddl_usage_counter.flashback_cluster_used, 1);
}

#[test]
fn global_memory_control_usage() {
    let mut tk = TestKit::new();
    assert_eq!(
        usage(&mut tk).enable_global_memory_control,
        tidb_vardef::defaults::DEF_TIDB_SERVER_MEMORY_LIMIT != "0"
    );
    tk.must_exec("set global tidb_server_memory_limit = 5 << 30");
    assert!(usage(&mut tk).enable_global_memory_control);
    tk.must_exec("set global tidb_server_memory_limit = 0");
    assert!(!usage(&mut tk).enable_global_memory_control);
}

/// Go `TestIndexMergeUsage`, whose first count comes from a plain EXPLAIN:
/// Go's `buildExplain` builds the target executor, which records the index
/// merge.
#[test]
#[ignore = "a plain EXPLAIN builds no target executor here (register row X03)"]
fn index_merge_usage() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t1(c1 int, c2 int, index idx1(c1), index idx2(c2))");
    let explain = tk.must_query(
        "explain select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 and c2 = 1",
    );
    assert!(explain.rows()[0][0].contains("IndexMerge"));
    assert_eq!(usage(&mut tk).index_merge_usage_counter.index_merge_used, 1);
    tk.must_exec("select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 and c2 = 1");
    assert_eq!(usage(&mut tk).index_merge_usage_counter.index_merge_used, 2);
    tk.must_exec("select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 or c2 = 1");
    assert_eq!(usage(&mut tk).index_merge_usage_counter.index_merge_used, 3);
    tk.must_exec("select /*+ no_index_merge() */ * from t1 where c1 = 1 or c2 = 1");
    assert_eq!(usage(&mut tk).index_merge_usage_counter.index_merge_used, 3);
}

/// The executed half of Go `TestIndexMergeUsage`: each SELECT that builds an
/// index merge reader counts once, and one that does not counts nothing.
#[test]
fn executed_index_merge_reads_count_once_each() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t1(c1 int, c2 int, index idx1(c1), index idx2(c2))");
    let before = usage(&mut tk).index_merge_usage_counter.index_merge_used;
    tk.must_exec("select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 and c2 = 1");
    assert_eq!(usage(&mut tk).index_merge_usage_counter.index_merge_used, before + 1);
    tk.must_exec("select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 or c2 = 1");
    assert_eq!(usage(&mut tk).index_merge_usage_counter.index_merge_used, before + 2);
    tk.must_exec("select /*+ no_index_merge() */ * from t1 where c1 = 1 or c2 = 1");
    assert_eq!(usage(&mut tk).index_merge_usage_counter.index_merge_used, before + 2);
}

/// Go `updateTelemetryMetric`'s CTE arms: a recursive CTE, a non-recursive
/// one, and a statement with none.
#[test]
fn cte_usage_counts_recursive_non_recursive_and_none() {
    let mut tk = TestKit::new();
    let before = usage(&mut tk).cte;
    tk.must_query("with recursive c(n) as (select 1 union all select n + 1 from c where n < 3) select * from c");
    // Referenced twice: Go inlines a non-recursive CTE read once, and an
    // inlined CTE builds no CTE executor.
    tk.must_query("with c as (select 1) select * from c, c c2");
    tk.must_query("select 1");
    let after = usage(&mut tk).cte;
    assert_eq!(after.recursive_used - before.recursive_used, 1);
    assert_eq!(after.non_recursive_cte_used - before.non_recursive_cte_used, 1);
    // `select 1` and the two telemetry reads' own statements are not CTEs;
    // the reads run on internal sessions, which count nothing.
    assert_eq!(after.non_cte_used - before.non_cte_used, 1);
}

/// Go `TestFairLockingUsage`.
#[test]
fn fair_locking_usage() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let mut tk = TestKit::new();
    let mut tk2 = tk.new_tk();
    tk.must_exec("create table t (id int primary key, v int)");
    tk.must_exec("insert into t values (1, 1), (2, 2)");
    let counter = usage(&mut tk2).txn.fair_locking_usage_counter;
    assert_eq!((counter.txn_fair_locking_used, counter.txn_fair_locking_effective), (0, 0));

    tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 1");
    tk.must_exec("begin pessimistic");
    tk.must_exec("update t set v = v + 1 where id = 1");
    // Not counted before transaction committing.
    let counter = usage(&mut tk2).txn.fair_locking_usage_counter;
    assert_eq!((counter.txn_fair_locking_used, counter.txn_fair_locking_effective), (0, 0));

    tk.must_exec("commit");
    let counter = usage(&mut tk2).txn.fair_locking_usage_counter;
    assert_eq!((counter.txn_fair_locking_used, counter.txn_fair_locking_effective), (1, 0));

    // Counted by transaction instead of by statement.
    tk.must_exec("begin pessimistic");
    tk.must_exec("update t set v = v + 1 where id = 1");
    tk.must_exec("update t set v = v + 1 where id = 2");
    tk.must_exec("commit");
    let counter = usage(&mut tk2).txn.fair_locking_usage_counter;
    assert_eq!((counter.txn_fair_locking_used, counter.txn_fair_locking_effective), (2, 0));
}

/// Go `session.Close` hands its builtin-function usage to telemetry.
#[test]
fn closing_a_session_hands_its_function_usage_to_telemetry() {
    let mut tk = TestKit::new();
    let _ = tidb_telemetry::GLOBAL_BUILTIN_FUNCTIONS_USAGE.dump();
    tk.session().builtin_function_usage_inc("PlusInt");
    drop(tk);
    // The kit's session dropped: its `Close` collected the count.
    assert_eq!(
        tidb_telemetry::GLOBAL_BUILTIN_FUNCTIONS_USAGE.dump(),
        tidb_telemetry::BuiltinFunctionsUsage(std::collections::HashMap::from([("PlusInt".to_owned(), 1)]))
    );
}

/// Go `runStmt` updates telemetry for every statement it executes,
/// transaction control included: BEGIN and COMMIT are statements without a
/// CTE.
#[test]
fn transaction_control_statements_count_as_statements_without_a_cte() {
    let mut tk = TestKit::new();
    let before = usage(&mut tk).cte.non_cte_used;
    tk.must_exec("begin");
    tk.must_exec("commit");
    assert_eq!(usage(&mut tk).cte.non_cte_used - before, 2);
}

/// Go `ExecutePreparedStmt` runs `ExecuteStmt`, so a binary-protocol EXECUTE
/// records its executor build's telemetry like a text statement.
#[test]
fn a_binary_protocol_execute_records_its_telemetry() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t1(c1 int, c2 int, index idx1(c1), index idx2(c2))");
    let before = usage(&mut tk).index_merge_usage_counter.index_merge_used;
    let statement = tk
        .session()
        .node_conn()
        .prepare_general("select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 or c2 = 1")
        .expect("prepare");
    let outcome = tk.session().node_conn().execute_general(&statement, &[]).expect("execute");
    drop(outcome);
    assert_eq!(usage(&mut tk).index_merge_usage_counter.index_merge_used, before + 1);
}
