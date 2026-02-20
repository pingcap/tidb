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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/size"
)

type (
	// inspectionResult represents a abnormal diagnosis result
	inspectionResult struct {
		tp            string
		instance      string
		statusAddress string
		// represents the diagnostics item, e.g: `ddl.lease` `raftstore.cpuusage`
		item string
		// diagnosis result value base on current cluster status
		actual   string
		expected string
		severity string
		detail   string
		// degree only used for sort.
		degree float64
	}

	inspectionName string

	inspectionFilter struct {
		set       set.StringSet
		timeRange plannerutil.QueryTimeRange
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

type (
	// configInspection is used to check whether a same configuration item has a
	// different value between different instance in the cluster
	configInspection struct{ inspectionName }

	// versionInspection is used to check whether the same component has different
	// version in the cluster
	versionInspection struct{ inspectionName }

	// nodeLoadInspection is used to check the node load of memory/disk/cpu
	// have reached a high-level threshold
	nodeLoadInspection struct{ inspectionName }

	// criticalErrorInspection is used to check are there some critical errors
	// occurred in the past
	criticalErrorInspection struct{ inspectionName }

	// thresholdCheckInspection is used to check some threshold value, like CPU usage, leader count change.
	thresholdCheckInspection struct{ inspectionName }
)

var inspectionRules = []inspectionRule{
	&configInspection{inspectionName: "config"},
	&versionInspection{inspectionName: "version"},
	&nodeLoadInspection{inspectionName: "node-load"},
	&criticalErrorInspection{inspectionName: "critical-error"},
	&thresholdCheckInspection{inspectionName: "threshold-check"},
}

type inspectionResultRetriever struct {
	dummyCloser
	retrieved               bool
	extractor               *plannercore.InspectionResultTableExtractor
	timeRange               plannerutil.QueryTimeRange
	instanceToStatusAddress map[string]string
	statusToInstanceAddress map[string]string
}

func (e *inspectionResultRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipInspection {
		return nil, nil
	}
	e.retrieved = true

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	// Some data of cluster-level memory tables will be retrieved many times in different inspection rules,
	// and the cost of retrieving some data is expensive. We use the `TableSnapshot` to cache those data
	// and obtain them lazily, and provide a consistent view of inspection tables for each inspection rules.
	// All cached snapshots should be released at the end of retrieving.
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

	if e.instanceToStatusAddress == nil {
		// Get cluster info.
		e.instanceToStatusAddress = make(map[string]string)
		e.statusToInstanceAddress = make(map[string]string)
		var rows []chunk.Row
		exec := sctx.GetRestrictedSQLExecutor()
		rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select instance,status_address from information_schema.cluster_info;")
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("get cluster info failed: %v", err))
		}
		for _, row := range rows {
			if row.Len() < 2 {
				continue
			}
			e.instanceToStatusAddress[row.GetString(0)] = row.GetString(1)
			e.statusToInstanceAddress[row.GetString(1)] = row.GetString(0)
		}
	}

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
		slices.SortFunc(results, func(i, j inspectionResult) int {
			if c := cmp.Compare(i.degree, j.degree); c != 0 {
				return -c
			}
			// lhs and rhs
			if c := cmp.Compare(i.item, j.item); c != 0 {
				return c
			}
			if c := cmp.Compare(i.actual, j.actual); c != 0 {
				return c
			}
			// lhs and rhs
			if c := cmp.Compare(i.tp, j.tp); c != 0 {
				return c
			}
			return cmp.Compare(i.instance, j.instance)
		})
		for _, result := range results {
			if len(result.instance) == 0 {
				result.instance = e.statusToInstanceAddress[result.statusAddress]
			}
			if len(result.statusAddress) == 0 {
				result.statusAddress = e.instanceToStatusAddress[result.instance]
			}
			finalRows = append(finalRows, types.MakeDatums(
				name,
				result.item,
				result.tp,
				result.instance,
				result.statusAddress,
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

func (configInspection) inspectDiffConfig(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration consistent
	ignoreConfigKey := []string{
		// TiDB
		"port",
		"status.status-port",
		"host",
		"path",
		"advertise-address",
		"status.status-port",
		"log.file.filename",
		"log.slow-query-file",
		"tmp-storage-path",

		// PD
		"advertise-client-urls",
		"advertise-peer-urls",
		"client-urls",
		"data-dir",
		"log-file",
		"log.file.filename",
		"metric.job",
		"name",
		"peer-urls",
		"initial-cluster",
		"initial-cluster-state",
		"join",

		// TiKV
		"server.addr",
		"server.advertise-addr",
		"server.advertise-status-addr",
		"server.status-addr",
		"log-file",
		"raftstore.raftdb-path",
		"storage.data-dir",
		"storage.block-cache.capacity",

		// TiProxy
		"proxy.advertise-addr",
	}
	exec := sctx.GetRestrictedSQLExecutor()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select type, `key`, count(distinct value) as c from information_schema.cluster_config where `key` not in (%?) group by type, `key` having c > 1", ignoreConfigKey)
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check configuration consistency failed: %v", err))
	}

	generateDetail := func(tp, item string) string {
		rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select value, instance from information_schema.cluster_config where type=%? and `key`=%?;", tp, item)
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check configuration consistency failed: %v", err))
			return fmt.Sprintf("the cluster has different config value of %[2]s, execute the sql to see more detail: select * from information_schema.cluster_config where type='%[1]s' and `key`='%[2]s'",
				tp, item)
		}
		m := make(map[string][]string)
		for _, row := range rows {
			value := row.GetString(0)
			instance := row.GetString(1)
			m[value] = append(m[value], instance)
		}
		groups := make([]string, 0, len(m))
		for k, v := range m {
			slices.Sort(v)
			groups = append(groups, fmt.Sprintf("%s config value is %s", strings.Join(v, ","), k))
		}
		slices.Sort(groups)
		return strings.Join(groups, "\n")
	}

	var results []inspectionResult
	for _, row := range rows {
		if filter.enable(row.GetString(1)) {
			detail := generateDetail(row.GetString(0), row.GetString(1))
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

func (c configInspection) inspectCheckConfig(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration in reason.
	cases := []struct {
		table  string
		tp     string
		key    string
		expect string
		cond   string
		detail string
	}{
		{
			table:  "cluster_config",
			key:    "log.slow-threshold",
			expect: "> 0",
			cond:   "type = 'tidb' and `key` = 'log.slow-threshold' and value = '0'",
			detail: "slow-threshold = 0 will record every query to slow log, it may affect performance",
		},
		{

			table:  "cluster_config",
			key:    "raftstore.sync-log",
			expect: "true",
			cond:   "type = 'tikv' and `key` = 'raftstore.sync-log' and value = 'false'",
			detail: "sync-log should be true to avoid recover region when the machine breaks down",
		},
		{
			table:  "cluster_systeminfo",
			key:    "transparent_hugepage_enabled",
			expect: "always madvise [never]",
			cond:   "system_name = 'kernel' and name = 'transparent_hugepage_enabled' and value not like '%[never]%'",
			detail: "Transparent HugePages can cause memory allocation delays during runtime, TiDB recommends that you disable Transparent HugePages on all TiDB servers",
		},
	}

	var results []inspectionResult
	var rows []chunk.Row
	sql := new(strings.Builder)
	exec := sctx.GetRestrictedSQLExecutor()
	for _, cas := range cases {
		if !filter.enable(cas.key) {
			continue
		}
		sql.Reset()
		fmt.Fprintf(sql, "select type,instance,value from information_schema.%s where %s", cas.table, cas.cond)
		stmt, err := exec.ParseWithParams(ctx, sql.String())
		if err == nil {
			rows, _, err = exec.ExecRestrictedStmt(ctx, stmt)
		}
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check configuration in reason failed: %v", err))
		}

		for _, row := range rows {
			results = append(results, inspectionResult{
				tp:       row.GetString(0),
				instance: row.GetString(1),
				item:     cas.key,
				actual:   row.GetString(2),
				expected: cas.expect,
				severity: "warning",
				detail:   cas.detail,
			})
		}
	}
	results = append(results, c.checkTiKVBlockCacheSizeConfig(ctx, sctx, filter)...)
	return results
}

func (c configInspection) checkTiKVBlockCacheSizeConfig(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	item := "storage.block-cache.capacity"
	if !filter.enable(item) {
		return nil
	}
	exec := sctx.GetRestrictedSQLExecutor()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select instance,value from information_schema.cluster_config where type='tikv' and `key` = 'storage.block-cache.capacity'")
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check configuration in reason failed: %v", err))
	}
	extractIP := func(addr string) string {
		if idx := strings.Index(addr, ":"); idx > -1 {
			return addr[0:idx]
		}
		return addr
	}

	ipToBlockSize := make(map[string]uint64)
	ipToCount := make(map[string]int)
	for _, row := range rows {
		ip := extractIP(row.GetString(0))
		size, err := c.convertReadableSizeToByteSize(row.GetString(1))
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check TiKV block-cache configuration in reason failed: %v", err))
			return nil
		}
		ipToBlockSize[ip] += size
		ipToCount[ip]++
	}

	rows, _, err = exec.ExecRestrictedSQL(ctx, nil, "select instance, value from metrics_schema.node_total_memory where time=now()")
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check configuration in reason failed: %v", err))
	}
	ipToMemorySize := make(map[string]float64)
	for _, row := range rows {
		ip := extractIP(row.GetString(0))
		size := row.GetFloat64(1)
		ipToMemorySize[ip] += size
	}

	var results []inspectionResult
	for ip, blockSize := range ipToBlockSize {
		if memorySize, ok := ipToMemorySize[ip]; ok {
			if float64(blockSize) > memorySize*0.45 {
				detail := fmt.Sprintf("There are %v TiKV server in %v node, the total 'storage.block-cache.capacity' of TiKV is more than (0.45 * total node memory)",
					ipToCount[ip], ip)
				results = append(results, inspectionResult{
					tp:       "tikv",
					instance: ip,
					item:     item,
					actual:   fmt.Sprintf("%v", blockSize),
					expected: fmt.Sprintf("< %.0f", memorySize*0.45),
					severity: "warning",
					detail:   detail,
				})
			}
		}
	}
	return results
}

func (configInspection) convertReadableSizeToByteSize(sizeStr string) (uint64, error) {
	rate := uint64(1)
	if strings.HasSuffix(sizeStr, "KiB") {
		rate = size.KB
	} else if strings.HasSuffix(sizeStr, "MiB") {
		rate = size.MB
	} else if strings.HasSuffix(sizeStr, "GiB") {
		rate = size.GB
	} else if strings.HasSuffix(sizeStr, "TiB") {
		rate = size.TB
	} else if strings.HasSuffix(sizeStr, "PiB") {
		rate = size.PB
	}
	if rate != 1 && len(sizeStr) > 3 {
		sizeStr = sizeStr[:len(sizeStr)-3]
	}
	sizeStr = strings.TrimSuffix(sizeStr, "B")
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return uint64(size) * rate, nil
}

func (versionInspection) inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	exec := sctx.GetRestrictedSQLExecutor()
	// check the configuration consistent
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select type, count(distinct git_hash) as c from information_schema.cluster_info group by type having c > 1;")
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
				detail:   fmt.Sprintf("the cluster has %[1]v different %[2]s versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='%[2]s'", row.GetUint64(1), row.GetString(0)),
			})
		}
	}
	return results
}

func (nodeLoadInspection) inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []ruleChecker{
		inspectCPULoad{item: "load1", tbl: "node_load1"},
		inspectCPULoad{item: "load5", tbl: "node_load5"},
		inspectCPULoad{item: "load15", tbl: "node_load15"},
		inspectVirtualMemUsage{},
		inspectSwapMemoryUsed{},
		inspectDiskUsage{},
	}
	return checkRules(ctx, sctx, filter, rules)
}

type inspectVirtualMemUsage struct{}

func (inspectVirtualMemUsage) genSQL(timeRange plannerutil.QueryTimeRange) string {
	sql := fmt.Sprintf("select instance, max(value) as max_usage from metrics_schema.node_memory_usage %s group by instance having max_usage >= 70", timeRange.Condition())
	return sql
}

func (i inspectVirtualMemUsage) genResult(_ string, row chunk.Row) inspectionResult {
	return inspectionResult{
		tp:       "node",
		instance: row.GetString(0),
		item:     i.getItem(),
		actual:   fmt.Sprintf("%.1f%%", row.GetFloat64(1)),
		expected: "< 70%",
		severity: "warning",
		detail:   "the memory-usage is too high",
	}
}

func (inspectVirtualMemUsage) getItem() string {
	return "virtual-memory-usage"
}

type inspectSwapMemoryUsed struct{}

func (inspectSwapMemoryUsed) genSQL(timeRange plannerutil.QueryTimeRange) string {
	sql := fmt.Sprintf("select instance, max(value) as max_used from metrics_schema.node_memory_swap_used %s group by instance having max_used > 0", timeRange.Condition())
	return sql
}

func (i inspectSwapMemoryUsed) genResult(_ string, row chunk.Row) inspectionResult {
	return inspectionResult{
		tp:       "node",
		instance: row.GetString(0),
		item:     i.getItem(),
		actual:   fmt.Sprintf("%.1f", row.GetFloat64(1)),
		expected: "0",
		severity: "warning",
	}
}

func (inspectSwapMemoryUsed) getItem() string {
	return "swap-memory-used"
}

type inspectDiskUsage struct{}

func (inspectDiskUsage) genSQL(timeRange plannerutil.QueryTimeRange) string {
	sql := fmt.Sprintf("select instance, device, max(value) as max_usage from metrics_schema.node_disk_usage %v and device like '/%%' group by instance, device having max_usage >= 70", timeRange.Condition())
	return sql
}

func (i inspectDiskUsage) genResult(_ string, row chunk.Row) inspectionResult {
	return inspectionResult{
		tp:       "node",
		instance: row.GetString(0),
		item:     i.getItem(),
		actual:   fmt.Sprintf("%.1f%%", row.GetFloat64(2)),
		expected: "< 70%",
		severity: "warning",
		detail:   "the disk-usage of " + row.GetString(1) + " is too high",
	}
}

func (inspectDiskUsage) getItem() string {
	return "disk-usage"
}

type inspectCPULoad struct {
	item string
	tbl  string
}

func (i inspectCPULoad) genSQL(timeRange plannerutil.QueryTimeRange) string {
	sql := fmt.Sprintf(`select t1.instance, t1.max_load , 0.7*t2.cpu_count from
			(select instance,max(value) as max_load  from metrics_schema.%[1]s %[2]s group by instance) as t1 join
			(select instance,max(value) as cpu_count from metrics_schema.node_virtual_cpus %[2]s group by instance) as t2
			on t1.instance=t2.instance where t1.max_load>(0.7*t2.cpu_count);`, i.tbl, timeRange.Condition())
	return sql
}

func (i inspectCPULoad) genResult(_ string, row chunk.Row) inspectionResult {
	return inspectionResult{
		tp:       "node",
		instance: row.GetString(0),
		item:     "cpu-" + i.item,
		actual:   fmt.Sprintf("%.1f", row.GetFloat64(1)),
		expected: fmt.Sprintf("< %.1f", row.GetFloat64(2)),
		severity: "warning",
		detail:   i.getItem() + " should less than (cpu_logical_cores * 0.7)",
	}
}

func (i inspectCPULoad) getItem() string {
	return "cpu-" + i.item
}

func (c criticalErrorInspection) inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	results := c.inspectError(ctx, sctx, filter)
	results = append(results, c.inspectForServerDown(ctx, sctx, filter)...)
	return results
}
func (criticalErrorInspection) inspectError(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []struct {
		tp   string
		item string
		tbl  string
	}{
		{tp: "tikv", item: "critical-error", tbl: "tikv_critical_error_total_count"},
		{tp: "tidb", item: "panic-count", tbl: "tidb_panic_count_total_count"},
		{tp: "tidb", item: "binlog-error", tbl: "tidb_binlog_error_total_count"},
		{tp: "tikv", item: "scheduler-is-busy", tbl: "tikv_scheduler_is_busy_total_count"},
		{tp: "tikv", item: "coprocessor-is-busy", tbl: "tikv_coprocessor_is_busy_total_count"},
		{tp: "tikv", item: "channel-is-full", tbl: "tikv_channel_full_total_count"},
		{tp: "tikv", item: "tikv_engine_write_stall", tbl: "tikv_engine_write_stall"},
	}

	condition := filter.timeRange.Condition()
	var results []inspectionResult
	exec := sctx.GetRestrictedSQLExecutor()
	sql := new(strings.Builder)
	for _, rule := range rules {
		if filter.enable(rule.item) {
			def, found := infoschema.MetricTableMap[rule.tbl]
			if !found {
				sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("metrics table: %s not found", rule.tbl))
				continue
			}
			sql.Reset()
			fmt.Fprintf(sql, "select `%[1]s`,sum(value) as total from `%[2]s`.`%[3]s` %[4]s group by `%[1]s` having total>=1.0",
				strings.Join(def.Labels, "`,`"), metadef.MetricSchemaName.L, rule.tbl, condition)
			rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
			if err != nil {
				sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
				continue
			}
			for _, row := range rows {
				var actual, detail string
				var degree float64
				if rest := def.Labels[1:]; len(rest) > 0 {
					values := make([]string, 0, len(rest))
					// `i+1` and `1+len(rest)` means skip the first field `instance`
					for i := range rest {
						values = append(values, row.GetString(i+1))
					}
					// TODO: find a better way to construct the `actual` field
					actual = fmt.Sprintf("%.2f(%s)", row.GetFloat64(1+len(rest)), strings.Join(values, ", "))
					degree = row.GetFloat64(1 + len(rest))
				} else {
					actual = fmt.Sprintf("%.2f", row.GetFloat64(1))
					degree = row.GetFloat64(1)
				}
				detail = fmt.Sprintf("the total number of errors about '%s' is too many", rule.item)
				result := inspectionResult{
					tp: rule.tp,
					// NOTE: all tables which can be inspected here whose first label must be `instance`
					statusAddress: row.GetString(0),
					item:          rule.item,
					actual:        actual,
					expected:      "0",
					severity:      "critical",
					detail:        detail,
					degree:        degree,
				}
				results = append(results, result)
			}
		}
	}
	return results
}

func (criticalErrorInspection) inspectForServerDown(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	item := "server-down"
	if !filter.enable(item) {
		return nil
	}
	condition := filter.timeRange.Condition()
	exec := sctx.GetRestrictedSQLExecutor()
	sql := new(strings.Builder)
	fmt.Fprintf(sql, `select t1.job,t1.instance, t2.min_time from
		(select instance,job from metrics_schema.up %[1]s group by instance,job having max(value)-min(value)>0) as t1 join
		(select instance,min(time) as min_time from metrics_schema.up %[1]s and value=0 group by instance,job) as t2 on t1.instance=t2.instance order by job`, condition)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
	}
	results := make([]inspectionResult, 0, len(rows))
	for _, row := range rows {
		if row.Len() < 3 {
			continue
		}
		detail := fmt.Sprintf("%s %s disconnect with prometheus around time '%s'", row.GetString(0), row.GetString(1), row.GetTime(2))
		result := inspectionResult{
			tp:            row.GetString(0),
			statusAddress: row.GetString(1),
			item:          item,
			actual:        "",
			expected:      "",
			severity:      "critical",
			detail:        detail,
			degree:        10000 + float64(len(results)),
		}
		results = append(results, result)
	}
	// Check from log.
	sql.Reset()
	fmt.Fprintf(sql, "select type,instance,time from information_schema.cluster_log %s and level = 'info' and message like '%%Welcome to'", condition)
	rows, _, err = exec.ExecRestrictedSQL(ctx, nil, sql.String())
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
	}
	for _, row := range rows {
		if row.Len() < 3 {
			continue
		}
		detail := fmt.Sprintf("%s %s restarted at time '%s'", row.GetString(0), row.GetString(1), row.GetString(2))
		result := inspectionResult{
			tp:       row.GetString(0),
			instance: row.GetString(1),
			item:     item,
			actual:   "",
			expected: "",
			severity: "critical",
			detail:   detail,
			degree:   10000 + float64(len(results)),
		}
		results = append(results, result)
	}
	return results
}

func (c thresholdCheckInspection) inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	inspects := []func(context.Context, sessionctx.Context, inspectionFilter) []inspectionResult{
		c.inspectThreshold1,
		c.inspectThreshold2,
		c.inspectThreshold3,
		c.inspectForLeaderDrop,
	}
	//nolint: prealloc
	var results []inspectionResult
	for _, inspect := range inspects {
		re := inspect(ctx, sctx, filter)
		results = append(results, re...)
	}
	return results
}

