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
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
)

type (
	// inspectionResult represents a abnormal diagnosis result
	inspectionResult struct {
		typ      string
		instance string
		// represents the diagnostics item, e.g: `ddl.lease` `raftstore.cpuusage`
		item string
		// diagnosis result value base on current cluster status
		actual   string
		expected string
		severity string
		detail   string
	}

	inspectionFilter struct{ set.StringSet }

	inspectionRule interface {
		name() string
		inspect(ctx context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult
	}
)

func (f inspectionFilter) enable(name string) bool {
	return len(f.StringSet) == 0 || f.Exist(name)
}

var inspectionRules = []inspectionRule{
	&configInspection{},
	&versionInspection{},
	&currentLoadInspection{},
}

type inspectionRetriever struct {
	dummyCloser
	retrieved bool
	extractor *plannercore.InspectionResultTableExtractor
}

func (e *inspectionRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
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

	rules := inspectionFilter{e.extractor.Rules}
	items := inspectionFilter{e.extractor.Items}
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
				result.typ,
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

type configInspection struct{}

func (configInspection) name() string {
	return "config"
}

func (configInspection) inspect(_ context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration consistent
	sql := "select type, `key`, count(distinct value) as c from inspection_schema.cluster_config group by type, `key` having c > 1"
	rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("check configuration consistency failed: %v", err))
	}

	var results []inspectionResult
	for _, row := range rows {
		if filter.enable(row.GetString(1)) {
			results = append(results, inspectionResult{
				typ:      row.GetString(0),
				instance: "",
				item:     row.GetString(1), // key
				actual:   "inconsistent",
				expected: "consistent",
				severity: "warning",
				detail: fmt.Sprintf("select * from information_schema.cluster_config where type='%s' and `key`='%s'",
					row.GetString(0), row.GetString(1)),
			})
		}
	}
	return results
}

type versionInspection struct{}

func (versionInspection) name() string {
	return "version"
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
				typ:      row.GetString(0),
				instance: "",
				item:     name,
				actual:   "inconsistent",
				expected: "consistent",
				severity: "critical",
				detail:   fmt.Sprintf("select * from information_schema.cluster_info where type='%s'", row.GetString(0)),
			})
		}
	}
	return results
}

type currentLoadInspection struct{}

func (currentLoadInspection) name() string {
	return "current-load"
}

func (currentLoadInspection) inspect(_ context.Context, sctx sessionctx.Context, filter inspectionFilter) []inspectionResult {
	var commonResult = func(item string, expected string, row chunk.Row) inspectionResult {
		return inspectionResult{
			typ:      row.GetString(0),
			instance: row.GetString(1),
			item:     item,
			actual:   row.GetString(2),
			expected: expected,
			severity: "warning",
		}
	}
	var diskResult = func(item string, expected string, row chunk.Row) inspectionResult {
		return inspectionResult{
			typ:      row.GetString(0),
			instance: row.GetString(1),
			item:     item,
			actual:   row.GetString(3),
			expected: expected,
			severity: "warning",
			detail: fmt.Sprintf("select * from information_schema.cluster_hardware where type='%s' and instance='%s' and device_type='disk' and device_name='%s'",
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
		{
			"cpu-load1",
			"select type, instance, value from inspection_schema.cluster_load where device_type='cpu' and device_name='cpu' and name='load1' and value>0.7;",
			"<0.7",
			commonResult,
		},
		{
			"cpu-load5",
			"select type, instance, value from inspection_schema.cluster_load where device_type='cpu' and device_name='cpu' and name='load5' and value>0.7;",
			"<0.7",
			commonResult,
		},
		{
			"cpu-load15",
			"select type, instance, value from inspection_schema.cluster_load where device_type='cpu' and device_name='cpu' and name='load15' and value>0.7;",
			"<0.7",
			commonResult,
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
	return results
}
