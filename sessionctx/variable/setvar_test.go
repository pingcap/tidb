// Copyright 2023 PingCAP, Inc.
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

package variable_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestSetVarNonStringOrEnum(t *testing.T) {
	testCases := []struct {
		varName string
		cases   []string
	}{
		{"tidb_opt_agg_push_down", []string{"0", "1"}},
		{"tidb_opt_derive_topn", []string{"0", "1"}},
		{"tidb_opt_broadcast_cartesian_join", []string{"0", "1"}},
		{"tidb_opt_mpp_outer_join_fixed_build_side", []string{"0", "1"}},
		{"tidb_opt_distinct_agg_push_down", []string{"0", "1"}},
		{"tidb_opt_skew_distinct_agg", []string{"0", "1"}},
		{"tidb_opt_three_stage_distinct_agg", []string{"0", "1"}},
		{"tidb_broadcast_join_threshold_size", []string{"0", "1"}},
		{"tidb_broadcast_join_threshold_count", []string{"0", "1"}},
		{"tidb_prefer_broadcast_join_by_exchange_data_size", []string{"0", "1"}},
		{"tidb_opt_write_row_id", []string{"0", "1"}},
		{"tidb_optimizer_selectivity_level", []string{"0", "1"}},
		{"tidb_enable_new_only_full_group_by_check", []string{"0", "1"}},
		{"tidb_enable_outer_join_reorder", []string{"0", "1"}},
		{"tidb_enable_null_aware_anti_join", []string{"0", "1"}},
		{"tidb_read_staleness", []string{"0", "-1"}},
		{"tidb_enable_paging", []string{"0", "1"}},
		{"tidb_distsql_scan_concurrency", []string{"1", "2"}},
		{"tidb_opt_insubq_to_join_and_agg", []string{"0", "1"}},
		{"tidb_opt_prefer_range_scan", []string{"0", "1"}},
		{"tidb_opt_enable_correlation_adjustment", []string{"0", "1"}},
		{"tidb_opt_limit_push_down_threshold", []string{"0", "1"}},
		{"tidb_opt_correlation_threshold", []string{"0", "1"}},
		{"tidb_opt_correlation_exp_factor", []string{"0", "1"}},
		{"tidb_opt_cpu_factor", []string{"0", "1"}},
		{"tidb_opt_copcpu_factor", []string{"0", "1"}},
		{"tidb_opt_tiflash_concurrency_factor", []string{"1", "2"}},
		{"tidb_opt_network_factor", []string{"0", "1"}},
		{"tidb_opt_scan_factor", []string{"0", "1"}},
		{"tidb_opt_desc_factor", []string{"0", "1"}},
		{"tidb_opt_seek_factor", []string{"0", "1"}},
		{"tidb_opt_memory_factor", []string{"0", "1"}},
		{"tidb_opt_disk_factor", []string{"0", "1"}},
		{"tidb_opt_concurrency_factor", []string{"1", "2"}},
		{"tidb_opt_force_inline_cte", []string{"0", "1"}},
		{"tidb_index_join_batch_size", []string{"1", "2"}},
		{"tidb_index_lookup_size", []string{"1", "2"}},
		{"tidb_index_serial_scan_concurrency", []string{"1", "2"}},
		{"tidb_allow_batch_cop", []string{"0", "1"}},
		{"tidb_allow_mpp", []string{"0", "1"}},
		{"tidb_enforce_mpp", []string{"0", "1"}},
		{"tidb_max_bytes_before_tiflash_external_join", []string{"0", "1"}},
		{"tidb_max_bytes_before_tiflash_external_group_by", []string{"0", "1"}},
		{"tidb_max_bytes_before_tiflash_external_sort", []string{"0", "1"}},
		{"tidb_min_paging_size", []string{"1", "2"}},
		{"tidb_max_paging_size", []string{"10", "20"}},
		{"tidb_enable_cascades_planner", []string{"0", "1"}},
		{"tidb_merge_join_concurrency", []string{"1", "2"}},
		{"tidb_index_merge_intersection_concurrency", []string{"1", "2"}},
		{"tidb_opt_projection_push_down", []string{"0", "1"}},
		{"tidb_enable_vectorized_expression", []string{"0", "1"}},
		{"tidb_opt_join_reorder_threshold", []string{"0", "1"}},
		{"tidb_enable_index_merge", []string{"0", "1"}},
		{"tidb_enable_extended_stats", []string{"0", "1"}},
		{"tidb_executor_concurrency", []string{"1", "2"}},
		{"tidb_enable_index_merge_join", []string{"0", "1"}},
		{"tidb_enable_ordered_result_mode", []string{"0", "1"}},
		{"tidb_enable_pseudo_for_outdated_stats", []string{"0", "1"}},
		{"tidb_stats_load_sync_wait", []string{"0", "1"}},
		{"tidb_cost_model_version", []string{"1", "2"}},
		{"tidb_index_join_double_read_penalty_cost_rate", []string{"0", "1"}},
		{"tidb_default_string_match_selectivity", []string{"0", "1"}},
		{"tidb_enable_prepared_plan_cache", []string{"0", "1"}},
		{"tidb_enable_non_prepared_plan_cache", []string{"0", "1"}},
		{"tidb_plan_cache_max_plan_size", []string{"0", "1"}},
		{"tidb_opt_range_max_size", []string{"0", "1"}},
		{"tidb_opt_advanced_join_hint", []string{"0", "1"}},
		{"tidb_opt_prefix_index_single_scan", []string{"0", "1"}},
		{"tidb_store_batch_size", []string{"1", "2"}},
		{"tidb_enable_inl_join_inner_multi_pattern", []string{"0", "1"}},
		{"tidb_opt_enable_late_materialization", []string{"0", "1"}},
		{"tidb_opt_ordering_index_selectivity_threshold", []string{"0", "1"}},
		{"tidb_opt_enable_mpp_shared_cte_execution", []string{"0", "1"}},
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	for _, c := range testCases {
		for _, setv := range c.cases {
			tk.MustQuery(fmt.Sprintf("select /*+ set_var(%v=%v) */ @@%v", c.varName, setv, c.varName)).Check(testkit.Rows(setv))
			r := tk.MustQuery(fmt.Sprintf("select @@%v", c.varName)).Rows()
			tk.MustExec(fmt.Sprintf("set @@%v=default", c.varName))
			tk.MustQuery(fmt.Sprintf("select @@%v", c.varName)).Check(r)
		}
	}
}

func TestSetVarStringOrEnum(t *testing.T) {
	testCases := []struct {
		varName string
		cases   []string
	}{
		{"tidb_read_consistency", []string{"strict", "weak"}},
		{"tidb_isolation_read_engines", []string{"tidb,tiflash", "tikv,tidb"}},
		{"tidb_replica_read", []string{"follower", "prefer-leader"}},
		{"tidb_partition_prune_mode", []string{"static", "dynamic"}},
		{"mpp_version", []string{"0", "1"}},
		{"tidb_opt_fix_control", []string{"44262:ON", "44389:ON,44823:ON"}},
		{"tidb_runtime_filter_mode", []string{"OFF", "LOCAL"}},
		{"sql_mode", []string{"", "ONLY_FULL_GROUP_BY"}},
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	for _, c := range testCases {
		for _, setv := range c.cases {
			tk.MustQuery(fmt.Sprintf("select /*+ set_var(%v=\"%v\") */ @@%v", c.varName, setv, c.varName)).Check(testkit.Rows(setv))
			r := tk.MustQuery(fmt.Sprintf("select @@%v", c.varName)).Rows()
			tk.MustExec(fmt.Sprintf("set @@%v=default", c.varName))
			tk.MustQuery(fmt.Sprintf("select @@%v", c.varName)).Check(r)
		}
	}
}

func TestSetVarHintBreakCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t (a INT, b INT, KEY(b));")
	tk.MustExec("SET tidb_enable_non_prepared_plan_cache = true;")
	tk.MustExec("SELECT * FROM t WHERE b < 10 AND a = 1;")
	tk.MustExec("SELECT * FROM t WHERE b < 5 AND a = 2;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustExec("SELECT /*+ SET_VAR(tidb_distsql_scan_concurrency=10) */ * FROM t WHERE b < 5 AND a = 2;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("SELECT * FROM t WHERE b < 5 AND a = 2;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}
