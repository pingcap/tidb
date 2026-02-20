// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"math"
	"strings"

	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

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
	exec := sctx.GetRestrictedSQLExecutor()
	sql := new(strings.Builder)
	for _, rule := range rules {
		if !filter.enable(rule.item) {
			continue
		}

		sql.Reset()
		if len(rule.configKey) > 0 {
			fmt.Fprintf(sql, `select t1.status_address, t1.cpu, (t2.value * %[2]f) as threshold, t2.value from
				(select status_address, max(sum_value) as cpu from (select instance as status_address, sum(value) as sum_value from metrics_schema.tikv_thread_cpu %[4]s and name like '%[1]s' group by instance, time) as tmp group by tmp.status_address) as t1 join
				(select instance, value from information_schema.cluster_config where type='tikv' and %[5]s = '%[3]s') as t2 join
				(select instance,status_address from information_schema.cluster_info where type='tikv') as t3
				on t1.status_address=t3.status_address and t2.instance=t3.instance where t1.cpu > (t2.value * %[2]f)`, rule.component, rule.threshold, rule.configKey, condition, "`key`")
		} else {
			fmt.Fprintf(sql, `select t1.instance, t1.cpu, %[2]f from
				(select instance, max(value) as cpu from metrics_schema.tikv_thread_cpu %[3]s and name like '%[1]s' group by instance) as t1
				where t1.cpu > %[2]f;`, rule.component, rule.threshold, condition)
		}
		rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
			continue
		}
		for _, row := range rows {
			actual := fmt.Sprintf("%.2f", row.GetFloat64(1))
			degree := math.Abs(row.GetFloat64(1)-row.GetFloat64(2)) / math.Max(row.GetFloat64(1), row.GetFloat64(2))
			expected := ""
			if len(rule.configKey) > 0 {
				expected = fmt.Sprintf("< %.2f, config: %v=%v", row.GetFloat64(2), rule.configKey, row.GetString(3))
			} else {
				expected = fmt.Sprintf("< %.2f", row.GetFloat64(2))
			}
			detail := fmt.Sprintf("the '%s' max cpu-usage of %s tikv is too high", rule.item, row.GetString(0))
			result := inspectionResult{
				tp:            "tikv",
				statusAddress: row.GetString(0),
				item:          rule.item,
				actual:        actual,
				expected:      expected,
				severity:      "warning",
				detail:        detail,
				degree:        degree,
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
		detail    string
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
			detail:    " %s tikv scheduler has too many pending commands",
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
	sql := new(strings.Builder)
	exec := sctx.GetRestrictedSQLExecutor()
	for _, rule := range rules {
		if !filter.enable(rule.item) {
			continue
		}
		cond := condition
		if len(rule.condition) > 0 {
			cond = fmt.Sprintf("%s and %s", cond, rule.condition)
		}
		if rule.factor == 0 {
			rule.factor = 1
		}
		sql.Reset()
		if rule.isMin {
			fmt.Fprintf(sql, "select instance, min(value)/%.0f as min_value from metrics_schema.%s %s group by instance having min_value < %f;", rule.factor, rule.tbl, cond, rule.threshold)
		} else {
			fmt.Fprintf(sql, "select instance, max(value)/%.0f as max_value from metrics_schema.%s %s group by instance having max_value > %f;", rule.factor, rule.tbl, cond, rule.threshold)
		}
		rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
			continue
		}
		for _, row := range rows {
			actual := fmt.Sprintf("%.3f", row.GetFloat64(1))
			degree := math.Abs(row.GetFloat64(1)-rule.threshold) / math.Max(row.GetFloat64(1), rule.threshold)
			expected := ""
			if rule.isMin {
				expected = fmt.Sprintf("> %.3f", rule.threshold)
			} else {
				expected = fmt.Sprintf("< %.3f", rule.threshold)
			}
			detail := rule.detail
			if len(detail) == 0 {
				if strings.HasSuffix(rule.item, "duration") {
					detail = fmt.Sprintf("max duration of %s %s %s is too slow", row.GetString(0), rule.tp, rule.item)
				} else if strings.HasSuffix(rule.item, "hit") {
					detail = fmt.Sprintf("min %s rate of %s %s is too low", rule.item, row.GetString(0), rule.tp)
				}
			} else {
				detail = fmt.Sprintf(detail, row.GetString(0))
			}
			result := inspectionResult{
				tp:            rule.tp,
				statusAddress: row.GetString(0),
				item:          rule.item,
				actual:        actual,
				expected:      expected,
				severity:      "warning",
				detail:        detail,
				degree:        degree,
			}
			results = append(results, result)
		}
	}
	return results
}

type ruleChecker interface {
	genSQL(timeRange plannerutil.QueryTimeRange) string
	genResult(sql string, row chunk.Row) inspectionResult
	getItem() string
}

type compareStoreStatus struct {
	item      string
	tp        string
	threshold float64
}

func (c compareStoreStatus) genSQL(timeRange plannerutil.QueryTimeRange) string {
	condition := fmt.Sprintf(`where t1.time>='%[1]s' and t1.time<='%[2]s' and
		 t2.time>='%[1]s' and t2.time<='%[2]s'`, timeRange.From.Format(plannerutil.MetricTableTimeFormat),
		timeRange.To.Format(plannerutil.MetricTableTimeFormat))
	return fmt.Sprintf(`
		SELECT t1.address,
        	max(t1.value),
        	t2.address,
        	min(t2.value),
         	max((t1.value-t2.value)/t1.value) AS ratio
		FROM metrics_schema.pd_scheduler_store_status t1
		JOIN metrics_schema.pd_scheduler_store_status t2 %s
        	AND t1.type='%s'
        	AND t1.time = t2.time
        	AND t1.type=t2.type
        	AND t1.address != t2.address
        	AND (t1.value-t2.value)/t1.value>%v
        	AND t1.value > 0
		GROUP BY  t1.address,t2.address
		ORDER BY  ratio desc`, condition, c.tp, c.threshold)
}

func (c compareStoreStatus) genResult(_ string, row chunk.Row) inspectionResult {
	addr1 := row.GetString(0)
	value1 := row.GetFloat64(1)
	addr2 := row.GetString(2)
	value2 := row.GetFloat64(3)
	ratio := row.GetFloat64(4)
	detail := fmt.Sprintf("%v max %s is %.2f, much more than %v min %s %.2f", addr1, c.tp, value1, addr2, c.tp, value2)
	return inspectionResult{
		tp:       "tikv",
		instance: addr2,
		item:     c.item,
		actual:   fmt.Sprintf("%.2f%%", ratio*100),
		expected: fmt.Sprintf("< %.2f%%", c.threshold*100),
		severity: "warning",
		detail:   detail,
		degree:   ratio,
	}
}

func (c compareStoreStatus) getItem() string {
	return c.item
}

type checkRegionHealth struct{}

func (checkRegionHealth) genSQL(timeRange plannerutil.QueryTimeRange) string {
	condition := timeRange.Condition()
	return fmt.Sprintf(`select instance, sum(value) as sum_value from metrics_schema.pd_region_health %s and
		type in ('extra-peer-region-count','learner-peer-region-count','pending-peer-region-count') having sum_value>100`, condition)
}

func (c checkRegionHealth) genResult(_ string, row chunk.Row) inspectionResult {
	detail := fmt.Sprintf("the count of extra-perr and learner-peer and pending-peer are %v, it means the scheduling is too frequent or too slow", row.GetFloat64(1))
	actual := fmt.Sprintf("%.2f", row.GetFloat64(1))
	degree := math.Abs(row.GetFloat64(1)-100) / math.Max(row.GetFloat64(1), 100)
	return inspectionResult{
		tp:       "pd",
		instance: row.GetString(0),
		item:     c.getItem(),
		actual:   actual,
		expected: "< 100",
		severity: "warning",
		detail:   detail,
		degree:   degree,
	}
}

func (checkRegionHealth) getItem() string {
	return "region-health"
}

type checkStoreRegionTooMuch struct{}

func (checkStoreRegionTooMuch) genSQL(timeRange plannerutil.QueryTimeRange) string {
	condition := timeRange.Condition()
	return fmt.Sprintf(`select address, max(value) from metrics_schema.pd_scheduler_store_status %s and type='region_count' and value > 20000 group by address`, condition)
}

func (c checkStoreRegionTooMuch) genResult(_ string, row chunk.Row) inspectionResult {
	actual := fmt.Sprintf("%.2f", row.GetFloat64(1))
	degree := math.Abs(row.GetFloat64(1)-20000) / math.Max(row.GetFloat64(1), 20000)
	return inspectionResult{
		tp:       "tikv",
		instance: row.GetString(0),
		item:     c.getItem(),
		actual:   actual,
		expected: "<= 20000",
		severity: "warning",
		detail:   fmt.Sprintf("%s tikv has too many regions", row.GetString(0)),
		degree:   degree,
	}
}

func (checkStoreRegionTooMuch) getItem() string {
	return "region-count"
}

func (thresholdCheckInspection) inspectThreshold3(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []ruleChecker{
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
	return checkRules(ctx, sctx, filter, rules)
}

func checkRules(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter, rules []ruleChecker) []inspectionResult {
	var results []inspectionResult
	exec := sctx.GetRestrictedSQLExecutor()
	for _, rule := range rules {
		if !filter.enable(rule.getItem()) {
			continue
		}
		sql := rule.genSQL(filter.timeRange)
		rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql)
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

func (thresholdCheckInspection) inspectForLeaderDrop(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	condition := filter.timeRange.Condition()
	threshold := 50.0
	sql := new(strings.Builder)
	fmt.Fprintf(sql, `select address,min(value) as mi,max(value) as mx from metrics_schema.pd_scheduler_store_status %s and type='leader_count' group by address having mx-mi>%v`, condition, threshold)
	exec := sctx.GetRestrictedSQLExecutor()

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
		return nil
	}
	var results []inspectionResult
	for _, row := range rows {
		address := row.GetString(0)
		sql.Reset()
		fmt.Fprintf(sql, `select time, value from metrics_schema.pd_scheduler_store_status %s and type='leader_count' and address = '%s' order by time`, condition, address)
		var subRows []chunk.Row
		subRows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
			continue
		}

		lastValue := float64(0)
		for i, subRows := range subRows {
			v := subRows.GetFloat64(1)
			if i == 0 {
				lastValue = v
				continue
			}
			if lastValue-v > threshold {
				level := "warning"
				if v == 0 {
					level = "critical"
				}
				results = append(results, inspectionResult{
					tp:       "tikv",
					instance: address,
					item:     "leader-drop",
					actual:   fmt.Sprintf("%.0f", lastValue-v),
					expected: fmt.Sprintf("<= %.0f", threshold),
					severity: level,
					detail:   fmt.Sprintf("%s tikv has too many leader-drop around time %s, leader count from %.0f drop to %.0f", address, subRows.GetTime(0), lastValue, v),
					degree:   lastValue - v,
				})
				break
			}
			lastValue = v
		}
	}
	return results
}
