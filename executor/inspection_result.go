// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
)

type (
	// inspectionResult represents a abnormal diagnosis result
	inspectionResult struct {
		tp       string
		instance string
		// represents the diagnostics item, e.g: `ddl.lease` `raftstore.cpuusage`
		item string
		// diagnosis result value base on current cluster status
		actual   string
		expected string
		severity string
		detail   string
	}

	inspectionName string

	inspectionFilter struct {
		set       set.StringSet
		timeRange plannercore.QueryTimeRange
	}

	inspectionRule interface {
		name() string
		inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult
	}
)

func (n inspectionName) name() string {
	return string(n)
}

func (f inspectionFilter) enable(name string) bool {
	return len(f.set) == 0 || f.set.Exist(name)
}

func (f inspectionFilter) exist(name string) bool {
	return len(f.set) > 0 && f.set.Exist(name)
}

type (
	// configInspection is used to check whether a same configuration item has a
	// different value between different instance in the cluster
	configInspection struct{ inspectionName }

	// versionInspection is used to check whether the same component has different
	// version in the cluster
	versionInspection struct{ inspectionName }

	// currentLoadInspection is used to check the current load of memory/disk/cpu
	// have reached a high-level threshold
	currentLoadInspection struct{ inspectionName }

	// criticalErrorInspection is used to check are there some critical errors
	// occurred in the past
	criticalErrorInspection struct{ inspectionName }

	// thresholdCheckInspection is used to check some threshold value, like CPU usage, leader count change.
	thresholdCheckInspection struct{ inspectionName }
)

var inspectionRules = []inspectionRule{
	&configInspection{inspectionName: "config"},
	&versionInspection{inspectionName: "version"},
	&currentLoadInspection{inspectionName: "current-load"},
	&criticalErrorInspection{inspectionName: "critical-error"},
	&thresholdCheckInspection{inspectionName: "threshold-check"},
}

type inspectionResultRetriever struct {
	dummyCloser
	retrieved bool
	extractor *plannercore.InspectionResultTableExtractor
	timeRange plannercore.QueryTimeRange
}

func (e *inspectionResultRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipInspection {
		return nil, nil
	}
	e.retrieved = true

	// Some data of cluster-level memory tables will be retrieved many times in different inspection rules,
	// and the cost of retrieving some data is expensive. We use the `TableSnapshot` to cache those data
	// and obtain them lazily, and provide a consistent view of inspection tables for each inspection rules.
	// All cached snapshots should be released at the end of retrieving. So all diagnosis rules should query
	// `cluster_config/cluster_hardware/cluster_load/cluster_info` in `inspection_schema`.
	// e.g:
	// SELECT * FROM inspection_schema.cluster_config
	// instead of:
	// SELECT * FROM information_schema.cluster_config
	sctx.GetSessionVars().InspectionTableCache = map[string]variable.TableSnapshot{}
	defer func() { sctx.GetSessionVars().InspectionTableCache = nil }()

	failpoint.InjectContext(ctx, "mockMergeMockInspectionTables", func() {
		// Merge mock snapshots injected from failpoint for test purpose
		mockTables, ok := ctx.Value("__mockInspectionTables").(map[string]variable.TableSnapshot)
		if ok {
			for name, snap := range mockTables {
				sctx.GetSessionVars().InspectionTableCache[strings.ToLower(name)] = snap
			}
		}
	})

	rules := inspectionFilter{set: e.extractor.Rules}
	items := inspectionFilter{set: e.extractor.Items, timeRange: e.timeRange}
	var finalRows [][]types.Datum
	for _, r := range inspectionRules {
		name := r.name()
		if !rules.enable(name) {
			continue
		}
		results := r.inspect(ctx, sctx, items)
		if len(results) == 0 {
			continue
		}
		// make result stable
		sort.Slice(results, func(i, j int) bool {
			if lhs, rhs := results[i].item, results[j].item; lhs != rhs {
				return lhs < rhs
			}
			return results[i].actual < results[j].actual
		})
		for _, result := range results {
			finalRows = append(finalRows, types.MakeDatums(
				name,
				result.item,
				result.tp,
				result.instance,
				result.actual,
				result.expected,
				result.severity,
				result.detail,
			))
		}
	}
	return finalRows, nil
}

func (c configInspection) inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var results []inspectionResult
	results = append(results, c.inspectDiffConfig(ctx, sctx, filter)...)
	results = append(results, c.inspectCheckConfig(ctx, sctx, filter)...)
	return results
}

func (configInspection) inspectDiffConfig(_ context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration consistent
	ignoreConfigKey := []string{
		// TiDB
		"port",
		"host",
		"path",
		"advertise-address",
		"status.status-port",
		"log.file.filename",
		"log.slow-query-file",

		// PD
		"data-dir",
		"log-file",

		// TiKV
		"server.addr",
		"server.advertise-addr",
		"server.status-addr",
		"log-file",
		"raftstore.raftdb-path",
		"storage.data-dir",
	}
	sql := fmt.Sprintf("select type, `key`, count(distinct value) as c from inspection_schema.cluster_config where `key` not in ('%s') group by type, `key` having c > 1",
		strings.Join(ignoreConfigKey, "','"))
	rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check configuration consistency failed: %v", err))
	}

	var results []inspectionResult
	for _, row := range rows {
		if filter.enable(row.GetString(1)) {
			detail := fmt.Sprintf("execute the sql to see more detail: select * from information_schema.cluster_config where type='%s' and `key`='%s'",
				row.GetString(0), row.GetString(1))
			results = append(results, inspectionResult{
				tp:       row.GetString(0),
				instance: "",
				item:     row.GetString(1), // key
				actual:   "inconsistent",
				expected: "consistent",
				severity: "warning",
				detail:   detail,
			})
		}
	}
	return results
}

func (configInspection) inspectCheckConfig(_ context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration in reason.
	cases := []struct {
		tp     string
		key    string
		value  string
		detail string
	}{
		{
			tp:     "tidb",
			key:    "log.slow-threshold",
			value:  "0",
			detail: "slow-threshold = 0 will record every query to slow log, it may affect performance",
		},
	}

	var results []inspectionResult
	for _, cas := range cases {
		if !filter.enable(cas.key) {
			continue
		}
		sql := fmt.Sprintf("select instance from inspection_schema.cluster_config where type = '%s' and `key` = '%s' and value = '%s'",
			cas.tp, cas.key, cas.value)
		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check configuration in reason failed: %v", err))
		}

		for _, row := range rows {
			results = append(results, inspectionResult{
				tp:       cas.tp,
				instance: row.GetString(0),
				item:     cas.key,
				actual:   cas.value,
				expected: "not " + cas.value,
				severity: "warning",
				detail:   cas.detail,
			})
		}
	}
	return results
}

func (versionInspection) inspect(_ context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration consistent
	sql := "select type, count(distinct git_hash) as c from inspection_schema.cluster_info group by type having c > 1;"
	rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check version consistency failed: %v", err))
	}

	const name = "git_hash"
	var results []inspectionResult
	for _, row := range rows {
		if filter.enable(name) {
			results = append(results, inspectionResult{
				tp:       row.GetString(0),
				instance: "",
				item:     name,
				actual:   "inconsistent",
				expected: "consistent",
				severity: "critical",
				detail:   fmt.Sprintf("execute the sql to see more detail: select * from information_schema.cluster_info where type='%s'", row.GetString(0)),
			})
		}
	}
	return results
}

func (c currentLoadInspection) inspect(_ context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var commonResult = func(item, expected string, row chunk.Row) inspectionResult {
		return inspectionResult{
			tp:       row.GetString(0),
			instance: row.GetString(1),
			item:     item,
			actual:   row.GetString(2),
			expected: expected,
			severity: "warning",
		}
	}
	var diskResult = func(item, expected string, row chunk.Row) inspectionResult {
		return inspectionResult{
			tp:       row.GetString(0),
			instance: row.GetString(1),
			item:     item,
			actual:   row.GetString(3),
			expected: expected,
			severity: "warning",
			detail: fmt.Sprintf("execute the sql to see more detail: select * from information_schema.cluster_hardware where type='%s' and instance='%s' and device_type='disk' and device_name='%s'",
				row.GetString(0), row.GetString(1), row.GetString(2)),
		}
	}
	var rules = []struct {
		item     string
		sql      string
		expected string
		result   func(string, string, chunk.Row) inspectionResult
	}{
		{
			"virtual-memory-usage",
			"select type, instance, value from inspection_schema.cluster_load where device_type='memory' and device_name='virtual' and name='used-percent' and value > 0.7",
			"<0.7",
			commonResult,
		},
		{
			"swap-memory-usage",
			"select type, instance, value from inspection_schema.cluster_load where device_type='memory' and device_name='swap' and name='used-percent' and value > 0",
			"0",
			commonResult,
		},
		{
			"disk-usage",
			"select type, instance, device_name, value from inspection_schema.cluster_hardware where device_type='disk' and name='used-percent' and value > 70",
			"<70",
			diskResult,
		},
	}

	var results []inspectionResult
	for _, rule := range rules {
		if filter.enable(rule.item) {
			rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(rule.sql)
			if err != nil {
				sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check load %s failed: %v", rule.item, err))
				continue
			}
			for _, row := range rows {
				results = append(results, rule.result(rule.item, rule.expected, row))
			}
		}
	}
	results = append(results, c.inspectCPULoad(sctx, filter)...)
	return results
}

func (currentLoadInspection) inspectCPULoad(sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var results []inspectionResult
	for _, item := range []string{"load1", "load5", "load15"} {
		if !filter.enable(item) {
			continue
		}
		sql := fmt.Sprintf(`select t1.*, 0.7 * t2.cpu_core from
				(select type, instance, value from inspection_schema.cluster_load where device_type='cpu' and device_name='cpu' and name='%s') as t1 join
				(select type,instance, max(value) as cpu_core from inspection_schema.CLUSTER_HARDWARE where DEVICE_TYPE='cpu' and name='cpu-logical-cores' group by type,instance) as t2
				where t2.instance = t1.instance and t1.type=t2.type and t1.value > 0.7 * t2.cpu_core;`, item)
		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check load %s failed: %v", item, err))
			continue
		}
		for _, row := range rows {
			result := inspectionResult{
				tp:       row.GetString(0),
				instance: row.GetString(1),
				item:     "cpu-" + item,
				actual:   row.GetString(2),
				expected: fmt.Sprintf("<%.1f", row.GetFloat64(3)),
				severity: "warning",
				detail:   "cpu-" + item + " should less than (cpu_logical_cores * 0.7)",
			}
			results = append(results, result)
		}
	}
	return results
}

func (criticalErrorInspection) inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []struct {
		tp   string
		item string
		tbl  string
	}{
		{tp: "tidb", item: "failed-query-opm", tbl: "tidb_failed_query_opm"},
		{tp: "tikv", item: "critical-error", tbl: "tikv_critical_error"},
		{tp: "tidb", item: "panic-count", tbl: "tidb_panic_count"},
		{tp: "tidb", item: "binlog-error", tbl: "tidb_binlog_error_count"},
		{tp: "tidb", item: "pd-cmd-failed", tbl: "pd_cmd_fail_ops"},
		{tp: "tidb", item: "ticlient-region-error", tbl: "tidb_kv_region_error_ops"},
		{tp: "tidb", item: "lock-resolve", tbl: "tidb_lock_resolver_ops"},
		{tp: "tikv", item: "scheduler-is-busy", tbl: "tikv_scheduler_is_busy"},
		{tp: "tikv", item: "coprocessor-is-busy", tbl: "tikv_coprocessor_is_busy"},
		{tp: "tikv", item: "channel-is-full", tbl: "tikv_channel_full"},
		{tp: "tikv", item: "coprocessor-error", tbl: "tikv_coprocessor_request_error"},
		{tp: "tidb", item: "schema-lease-error", tbl: "tidb_schema_lease_error_opm"},
		{tp: "tidb", item: "txn-retry-error", tbl: "tidb_transaction_retry_error_ops"},
		{tp: "tikv", item: "grpc-errors", tbl: "tikv_grpc_errors"},
	}

	condition := filter.timeRange.Condition()
	var results []inspectionResult
	for _, rule := range rules {
		if filter.enable(rule.item) {
			def, found := infoschema.MetricTableMap[rule.tbl]
			if !found {
				sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("metrics table: %s not found", rule.tbl))
				continue
			}
			sql := fmt.Sprintf("select `%[1]s`,max(value) as max from `%[2]s`.`%[3]s` %[4]s group by `%[1]s` having max>=1.0",
				strings.Join(def.Labels, "`,`"), util.MetricSchemaName.L, rule.tbl, condition)
			rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(ctx, sql)
			if err != nil {
				sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
				continue
			}
			for _, row := range rows {
				var actual, detail string
				if rest := def.Labels[1:]; len(rest) > 0 {
					values := make([]string, 0, len(rest))
					// `i+1` and `1+len(rest)` means skip the first field `instance`
					for i := range rest {
						values = append(values, row.GetString(i+1))
					}
					// TODO: find a better way to construct the `actual` field
					actual = fmt.Sprintf("%.2f(%s)", row.GetFloat64(1+len(rest)), strings.Join(values, ", "))
					detail = fmt.Sprintf("select * from `%s`.`%s` %s and (`instance`,`%s`)=('%s','%s')",
						util.MetricSchemaName.L, rule.tbl, condition, strings.Join(rest, "`,`"), row.GetString(0), strings.Join(values, "','"))
				} else {
					actual = fmt.Sprintf("%.2f", row.GetFloat64(1))
					detail = fmt.Sprintf("select * from `%s`.`%s` %s and `instance`='%s'",
						util.MetricSchemaName.L, rule.tbl, condition, row.GetString(0))
				}
				result := inspectionResult{
					tp: rule.tp,
					// NOTE: all tables which can be inspected here whose first label must be `instance`
					instance: row.GetString(0),
					item:     rule.item,
					actual:   actual,
					expected: "0",
					severity: "warning",
					detail:   detail,
				}
				results = append(results, result)
			}
		}
	}
	return results
}

func (c thresholdCheckInspection) inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	inspects := []func(context.Context, sessionctx.Context, inspectionFilter) []inspectionResult{
		c.inspectThreshold1,
		c.inspectThreshold2,
		c.inspectThreshold3,
	}
	var results []inspectionResult
	for _, inspect := range inspects {
		re := inspect(ctx, sctx, filter)
		results = append(results, re...)
	}
	return results
}

func (thresholdCheckInspection) inspectThreshold1(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []struct {
		item      string
		component string
		configKey string
		threshold float64
	}{
		{
			item:      "coprocessor-normal-cpu",
			component: "cop_normal%",
			configKey: "readpool.coprocessor.normal-concurrency",
			threshold: 0.9},
		{
			item:      "coprocessor-high-cpu",
			component: "cop_high%",
			configKey: "readpool.coprocessor.high-concurrency",
			threshold: 0.9,
		},
		{
			item:      "coprocessor-low-cpu",
			component: "cop_low%",
			configKey: "readpool.coprocessor.low-concurrency",
			threshold: 0.9,
		},
		{
			item:      "grpc-cpu",
			component: "grpc%",
			configKey: "server.grpc-concurrency",
			threshold: 0.9,
		},
		{
			item:      "raftstore-cpu",
			component: "raftstore_%",
			configKey: "raftstore.store-pool-size",
			threshold: 0.8,
		},
		{
			item:      "apply-cpu",
			component: "apply_%",
			configKey: "raftstore.apply-pool-size",
			threshold: 0.8,
		},
		{
			item:      "storage-readpool-normal-cpu",
			component: "store_read_norm%",
			configKey: "readpool.storage.normal-concurrency",
			threshold: 0.9,
		},
		{
			item:      "storage-readpool-high-cpu",
			component: "store_read_high%",
			configKey: "readpool.storage.high-concurrency",
			threshold: 0.9,
		},
		{
			item:      "storage-readpool-low-cpu",
			component: "store_read_low%",
			configKey: "readpool.storage.low-concurrency",
			threshold: 0.9,
		},
		{
			item:      "scheduler-worker-cpu",
			component: "sched_%",
			configKey: "storage.scheduler-worker-pool-size",
			threshold: 0.85,
		},
		{
			item:      "split-check-cpu",
			component: "split_check",
			threshold: 0.9,
		},
	}

	condition := filter.timeRange.Condition()
	var results []inspectionResult
	for _, rule := range rules {
		if !filter.enable(rule.item) {
			continue
		}

		var sql string
		if len(rule.configKey) > 0 {
			sql = fmt.Sprintf("select t1.instance, t1.cpu, t2.threshold, t2.value from "+
				"(select instance, sum(value) as cpu from metrics_schema.tikv_thread_cpu %[4]s and name like '%[1]s' group by instance) as t1,"+
				"(select value * %[2]f as threshold, value from inspection_schema.cluster_config where type='tikv' and `key` = '%[3]s' limit 1) as t2 "+
				"where t1.cpu > t2.threshold;", rule.component, rule.threshold, rule.configKey, condition)
		} else {
			sql = fmt.Sprintf("select t1.instance, t1.cpu, %[2]f from "+
				"(select instance, sum(value) as cpu from metrics_schema.tikv_thread_cpu %[3]s and name like '%[1]s' group by instance) as t1 "+
				"where t1.cpu > %[2]f;", rule.component, rule.threshold, condition)
		}
		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(ctx, sql)
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
			continue
		}
		for _, row := range rows {
			actual := fmt.Sprintf("%.2f", row.GetFloat64(1))
			expected := ""
			if len(rule.configKey) > 0 {
				expected = fmt.Sprintf("< %.2f, config: %v=%v", row.GetFloat64(2), rule.configKey, row.GetString(3))
			} else {
				expected = fmt.Sprintf("< %.2f", row.GetFloat64(2))
			}
			detail := fmt.Sprintf("select instance, sum(value) as cpu from metrics_schema.tikv_thread_cpu %s and name like '%s' group by instance",
				condition, rule.component)
			result := inspectionResult{
				tp:       "tikv",
				instance: row.GetString(0),
				item:     rule.item,
				actual:   actual,
				expected: expected,
				severity: "warning",
				detail:   detail,
			}
			results = append(results, result)
		}
	}
	return results
}

func (thresholdCheckInspection) inspectThreshold2(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []struct {
		tp        string
		item      string
		tbl       string
		condition string
		threshold float64
		factor    float64
		isMin     bool
	}{
		{
			tp:        "tidb",
			item:      "tso-duration",
			tbl:       "pd_tso_wait_duration",
			condition: "quantile=0.999",
			threshold: 0.05,
		},
		{
			tp:        "tidb",
			item:      "get-token-duration",
			tbl:       "tidb_get_token_duration",
			condition: "quantile=0.999",
			threshold: 0.001,
			factor:    10e5, // the unit is microsecond
		},
		{
			tp:        "tidb",
			item:      "load-schema-duration",
			tbl:       "tidb_load_schema_duration",
			condition: "quantile=0.99",
			threshold: 1,
		},
		{
			tp:        "tikv",
			item:      "scheduler-cmd-duration",
			tbl:       "tikv_scheduler_command_duration",
			condition: "quantile=0.99",
			threshold: 0.1,
		},
		{
			tp:        "tikv",
			item:      "handle-snapshot-duration",
			tbl:       "tikv_handle_snapshot_duration",
			threshold: 30,
		},
		{
			tp:        "tikv",
			item:      "storage-write-duration",
			tbl:       "tikv_storage_async_request_duration",
			condition: "type='write'",
			threshold: 0.1,
		},
		{
			tp:        "tikv",
			item:      "storage-snapshot-duration",
			tbl:       "tikv_storage_async_request_duration",
			condition: "type='snapshot'",
			threshold: 0.05,
		},
		{
			tp:        "tikv",
			item:      "rocksdb-write-duration",
			tbl:       "tikv_engine_write_duration",
			condition: "type='write_max'",
			threshold: 0.1,
			factor:    10e5, // the unit is microsecond
		},
		{
			tp:        "tikv",
			item:      "rocksdb-get-duration",
			tbl:       "tikv_engine_max_get_duration",
			condition: "type='get_max'",
			threshold: 0.05,
			factor:    10e5,
		},
		{
			tp:        "tikv",
			item:      "rocksdb-seek-duration",
			tbl:       "tikv_engine_max_seek_duration",
			condition: "type='seek_max'",
			threshold: 0.05,
			factor:    10e5, // the unit is microsecond
		},
		{
			tp:        "tikv",
			item:      "scheduler-pending-cmd-count",
			tbl:       "tikv_scheduler_pending_commands",
			threshold: 1000,
		},
		{
			tp:        "tikv",
			item:      "index-block-cache-hit",
			tbl:       "tikv_block_index_cache_hit",
			condition: "value > 0",
			threshold: 0.95,
			isMin:     true,
		},
		{
			tp:        "tikv",
			item:      "filter-block-cache-hit",
			tbl:       "tikv_block_filter_cache_hit",
			condition: "value > 0",
			threshold: 0.95,
			isMin:     true,
		},
		{
			tp:        "tikv",
			item:      "data-block-cache-hit",
			tbl:       "tikv_block_data_cache_hit",
			condition: "value > 0",
			threshold: 0.80,
			isMin:     true,
		},
	}

	condition := filter.timeRange.Condition()
	var results []inspectionResult
	for _, rule := range rules {
		if !filter.enable(rule.item) {
			continue
		}
		var sql string
		cond := condition
		if len(rule.condition) > 0 {
			cond = fmt.Sprintf("%s and %s", cond, rule.condition)
		}
		if rule.factor == 0 {
			rule.factor = 1
		}
		if rule.isMin {
			sql = fmt.Sprintf("select instance, min(value)/%.0f as min_value from metrics_schema.%s %s group by instance having min_value < %f;", rule.factor, rule.tbl, cond, rule.threshold)
		} else {
			sql = fmt.Sprintf("select instance, max(value)/%.0f as max_value from metrics_schema.%s %s group by instance having max_value > %f;", rule.factor, rule.tbl, cond, rule.threshold)
		}
		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(ctx, sql)
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
			continue
		}
		for _, row := range rows {
			actual := fmt.Sprintf("%.3f", row.GetFloat64(1))
			expected := ""
			if rule.isMin {
				expected = fmt.Sprintf("> %.3f", rule.threshold)
			} else {
				expected = fmt.Sprintf("< %.3f", rule.threshold)
			}
			result := inspectionResult{
				tp:       rule.tp,
				instance: row.GetString(0),
				item:     rule.item,
				actual:   actual,
				expected: expected,
				severity: "warning",
				detail:   sql,
			}
			results = append(results, result)
		}
	}
	return results
}

type thresholdCheckRule interface {
	genSQL(timeRange plannercore.QueryTimeRange) string
	genResult(sql string, row chunk.Row) inspectionResult
	getItem() string
}

type compareStoreStatus struct {
	item      string
	tp        string
	threshold float64
}

func (c compareStoreStatus) genSQL(timeRange plannercore.QueryTimeRange) string {
	condition := fmt.Sprintf(`where t1.time>='%[1]s' and t1.time<='%[2]s' and
		 t2.time>='%[1]s' and t2.time<='%[2]s'`, timeRange.From.Format(plannercore.MetricTableTimeFormat),
		timeRange.To.Format(plannercore.MetricTableTimeFormat))
	return fmt.Sprintf(`select t1.address,t1.value,t2.address,t2.value,
				(t1.value-t2.value)/t1.value as ratio
				from metrics_schema.pd_scheduler_store_status t1 join metrics_schema.pd_scheduler_store_status t2
				%s and t1.type='%s' and t1.time = t2.time and
				t1.type=t2.type and t1.address != t2.address and
				(t1.value-t2.value)/t1.value>%v and t1.value > 0 group by t1.address,t1.value,t2.address,t2.value order by ratio desc`,
		condition, c.tp, c.threshold)
}

func (c compareStoreStatus) genResult(_ string, row chunk.Row) inspectionResult {
	addr1 := row.GetString(0)
	value1 := row.GetFloat64(1)
	addr2 := row.GetString(2)
	value2 := row.GetFloat64(3)
	ratio := row.GetFloat64(4)
	detail := fmt.Sprintf("%v %s is %v, much more than %v %s %v", addr1, c.tp, value1, addr2, c.tp, value2)
	return inspectionResult{
		tp:       "tikv",
		instance: addr2,
		item:     c.item,
		actual:   fmt.Sprintf("%.2f%%", ratio*100),
		expected: fmt.Sprintf("< %.2f%%", c.threshold*100),
		severity: "warning",
		detail:   detail,
	}
}

func (c compareStoreStatus) getItem() string {
	return c.item
}

type checkRegionHealth struct{}

func (c checkRegionHealth) genSQL(timeRange plannercore.QueryTimeRange) string {
	condition := timeRange.Condition()
	return fmt.Sprintf(`select instance, sum(value) as sum_value from metrics_schema.pd_region_health %s and
		type in ('extra-peer-region-count','learner-peer-region-count','pending-peer-region-count') having sum_value>100`, condition)
}

func (c checkRegionHealth) genResult(_ string, row chunk.Row) inspectionResult {
	detail := fmt.Sprintf("the count of extra-perr and learner-peer and pending-peer is %v, it means the scheduling is too frequent or too slow", row.GetFloat64(1))
	actual := fmt.Sprintf("%.2f", row.GetFloat64(1))
	return inspectionResult{
		tp:       "pd",
		instance: row.GetString(0),
		item:     c.getItem(),
		actual:   actual,
		expected: "< 100",
		severity: "warning",
		detail:   detail,
	}
}

func (c checkRegionHealth) getItem() string {
	return "region-health"
}

type checkStoreRegionTooMuch struct{}

func (c checkStoreRegionTooMuch) genSQL(timeRange plannercore.QueryTimeRange) string {
	condition := timeRange.Condition()
	return fmt.Sprintf(`select address,value from metrics_schema.pd_scheduler_store_status %s and type='region_count' and value > 20000;`, condition)
}

func (c checkStoreRegionTooMuch) genResult(sql string, row chunk.Row) inspectionResult {
	actual := fmt.Sprintf("%.2f", row.GetFloat64(1))
	return inspectionResult{
		tp:       "tikv",
		instance: row.GetString(0),
		item:     c.getItem(),
		actual:   actual,
		expected: "<= 20000",
		severity: "warning",
		detail:   sql,
	}
}

func (c checkStoreRegionTooMuch) getItem() string {
	return "region-count"
}

func (thresholdCheckInspection) inspectThreshold3(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []thresholdCheckRule{
		compareStoreStatus{
			item:      "leader-score-balance",
			tp:        "leader_score",
			threshold: 0.05,
		},
		compareStoreStatus{
			item:      "region-score-balance",
			tp:        "region_score",
			threshold: 0.05,
		},
		compareStoreStatus{
			item:      "store-available-balance",
			tp:        "store_available",
			threshold: 0.2,
		},
		checkRegionHealth{},
		checkStoreRegionTooMuch{},
	}

	var results []inspectionResult
	for _, rule := range rules {
		if !filter.enable(rule.getItem()) {
			continue
		}
		sql := rule.genSQL(filter.timeRange)
		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(ctx, sql)
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
			continue
		}
		for _, row := range rows {
			results = append(results, rule.genResult(sql, row))
		}
	}
	return results
}
