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

package core

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

func TestNewLineFieldsInfo(t *testing.T) {
	cases := []struct {
		sql      string
		expected LineFieldsInfo
	}{
		{
			"load data infile 'a' into table t",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields terminated by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "a",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields optionally enclosed by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "a",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  true,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields enclosed by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "a",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields escaped by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "a",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t lines starting by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "a",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t lines terminated by 'aa'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "aa",
			},
		},
	}
	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.sql, "", "")
		require.NoError(t, err, c.sql)
		ldStmt := stmt.(*ast.LoadDataStmt)
		lineFieldsInfo := NewLineFieldsInfo(ldStmt.FieldsInfo, ldStmt.LinesInfo)
		require.Equal(t, c.expected, lineFieldsInfo)
	}
}

func TestExplainRUSelectGateStatus(t *testing.T) {
	cases := []struct {
		sql      string
		expected explainRUStatus
	}{
		{"explain analyze format='ru' select 1", explainRUStatusSuccess},
		{"explain analyze format='ru' with cte as (select 1) select * from cte", explainRUStatusSuccess},
		{"explain analyze format='ru' select rand(), uuid()", explainRUStatusSuccess},
		{"explain analyze format='ru' select last_insert_id()", explainRUStatusSuccess},
		{"explain analyze format='ru' insert into t values (1)", explainRUStatusUnsupportedNonSelect},
		{"explain analyze format='ru' table t", explainRUStatusUnsupportedNonSelect},
		{"explain analyze format='ru' select 1 union table t", explainRUStatusUnsupportedNonSelect},
		{"explain analyze format='ru' select 1 into outfile '/tmp/explain_ru.csv'", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select @a := 1", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select 1 union select @a := 2", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' with cte as (select get_lock('x', 0)) select * from cte", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select release_lock('x')", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select release_all_locks()", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select last_insert_id(1)", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select nextval(seq)", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select setval(seq, 1)", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select sleep(1)", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select * from t for update skip locked", explainRUStatusUnsupportedLockingSelect},
		{"explain analyze format='ru' select * from t for share skip locked", explainRUStatusUnsupportedLockingSelect},
		{"explain analyze format='ru' select 1 union select * from t for update", explainRUStatusUnsupportedLockingSelect},
	}
	p := parser.New()
	for _, tc := range cases {
		stmt, err := p.ParseOneStmt(tc.sql, "", "")
		require.NoError(t, err, tc.sql)
		explain := stmt.(*ast.ExplainStmt)
		require.Equal(t, tc.expected, explainRUSelectGateStatus(explain.Stmt), tc.sql)
	}
	require.Equal(t, explainRUStatusUnsupportedNonSelect, explainRUSelectGateStatus(&ast.SelectStmt{Kind: ast.SelectStmtKindValues}))
	require.Equal(t, explainRUStatusUnsupportedNonSelect, explainRUValidateSetOprSelectList(&ast.SetOprSelectList{
		Selects: []ast.Node{&ast.SelectStmt{Kind: ast.SelectStmtKindValues}},
	}))
}

func TestExplainRURowFormatting(t *testing.T) {
	row := explainRURow{
		section:        explainRUSectionPlan,
		id:             "Projection_1",
		component:      "projection",
		operatorClass:  "tidb/projection_eval",
		actRows:        1,
		hasActRows:     true,
		inputRows:      2,
		hasInputRows:   true,
		outputRows:     1,
		hasOutputRows:  true,
		rowWidth:       8,
		hasRowWidth:    true,
		rowWidthSource: explainRUWidthSourcePlanStats,
		workRows:       2,
		hasWorkRows:    true,
		unit:           readBillingDemoUnitInputRows,
		count:          2,
		hasCount:       true,
		weight:         0.25,
		hasWeight:      true,
		previewRU:      6,
		hasPreviewRU:   true,
		source:         readBillingDemoInputSourceRuntimeRows,
		note:           "input_side=all,weight_version=v1",
	}
	require.Equal(t, []string{
		"plan", "Projection_1", "projection", "tidb/projection_eval", "1", "2", "1", "8.000000", "plan_stats", "2", "", "input_rows", "2", "0.250000", "6.000000", "runtime_act_rows", "input_side=all,weight_version=v1",
	}, row.toStrings())
}

func TestExplainRUPlanFormulaUsesRowsAndModeledBytes(t *testing.T) {
	tidbWeights, ok := readBillingDemoResolveWeights(readBillingDemoSiteTiDB, readBillingDemoOpClassProjection, readBillingDemoWeightVersion)
	require.True(t, ok)
	tikvWeights, ok := readBillingDemoResolveWeights(readBillingDemoSiteTiKV, readBillingDemoOpClassProjection, readBillingDemoWeightVersion)
	require.True(t, ok)
	require.NotEqual(t, tidbWeights, tikvWeights)
	_, ok = readBillingDemoResolveWeights(readBillingDemoSiteTiKV, readBillingDemoOpClassPointLookup, readBillingDemoWeightVersion)
	require.True(t, ok)
	_, ok = readBillingDemoResolveWeights(readBillingDemoSiteTiDB, readBillingDemoOpClassPointLookup, readBillingDemoWeightVersion)
	require.False(t, ok)

	weight, previewRU, ok := readBillingDemoUnitPreviewRU(
		readBillingDemoUnit{unit: readBillingDemoUnitInputBytes, value: 4096},
		tidbWeights,
	)
	require.True(t, ok)
	require.Equal(t, tidbWeights.byte, weight)
	require.Equal(t, 4096*tidbWeights.byte, previewRU)

	_, _, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: "scan_total_keys", value: 4}, tidbWeights)
	require.False(t, ok)

	ctx := mock.NewContext()
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	stats := &property.StatsInfo{RowCount: 5}
	for _, tc := range []struct {
		name    string
		site    string
		opClass string
		op      func() *FlatOperator
	}{
		{
			name:    "range scan",
			site:    readBillingDemoSiteTiKV,
			opClass: readBillingDemoOpClassRangeScan,
			op: func() *FlatOperator {
				scan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
				return &FlatOperator{Origin: scan, IsRoot: false, StoreType: kv.TiKV}
			},
		},
		{
			name:    "filter",
			site:    readBillingDemoSiteTiKV,
			opClass: readBillingDemoOpClassFilter,
			op: func() *FlatOperator {
				return &FlatOperator{Origin: physicalop.PhysicalSelection{}.Init(ctx, stats, 0), IsRoot: false, StoreType: kv.TiKV}
			},
		},
		{
			name:    "projection",
			site:    readBillingDemoSiteTiKV,
			opClass: readBillingDemoOpClassProjection,
			op: func() *FlatOperator {
				return &FlatOperator{Origin: physicalop.PhysicalProjection{}.Init(ctx, stats, 0), IsRoot: false, StoreType: kv.TiKV}
			},
		},
		{
			name:    "limit",
			site:    readBillingDemoSiteTiKV,
			opClass: readBillingDemoOpClassLimit,
			op: func() *FlatOperator {
				return &FlatOperator{Origin: physicalop.PhysicalLimit{}.Init(ctx, stats, 0), IsRoot: false, StoreType: kv.TiKV}
			},
		},
		{
			name:    "topn",
			site:    readBillingDemoSiteTiKV,
			opClass: readBillingDemoOpClassTopN,
			op: func() *FlatOperator {
				return &FlatOperator{Origin: physicalop.PhysicalTopN{}.Init(ctx, stats, 0), IsRoot: false, StoreType: kv.TiKV}
			},
		},
		{
			name:    "hash agg",
			site:    readBillingDemoSiteTiKV,
			opClass: readBillingDemoOpClassHashAgg,
			op: func() *FlatOperator {
				return &FlatOperator{Origin: (&physicalop.BasePhysicalAgg{}).InitForHash(ctx, stats, 0, schema), IsRoot: false, StoreType: kv.TiKV}
			},
		},
		{
			name:    "stream agg",
			site:    readBillingDemoSiteTiKV,
			opClass: readBillingDemoOpClassStreamAgg,
			op: func() *FlatOperator {
				return &FlatOperator{Origin: (&physicalop.BasePhysicalAgg{}).InitForStream(ctx, stats, 0, schema), IsRoot: false, StoreType: kv.TiKV}
			},
		},
	} {
		t.Run(tc.site+" "+tc.name, func(t *testing.T) {
			requireReadBillingDemoClass(t, tc.op(), tc.site, tc.opClass, true, "")
		})
	}
	requireReadBillingDemoClass(t, &FlatOperator{
		Origin:    physicalop.PhysicalIndexScan{}.Init(ctx, 0),
		IsRoot:    false,
		StoreType: kv.TiFlash,
	}, readBillingDemoSiteTiKV, readBillingDemoOpClassRangeScan, false, readBillingDemoReasonUnsupportedTiFlash)
	requireReadBillingDemoClass(t, &FlatOperator{
		Origin: physicalop.PhysicalExchangeReceiver{}.Init(ctx, stats),
		IsRoot: true,
	}, readBillingDemoSiteTiDB, readBillingDemoOpClassReaderReceive, false, readBillingDemoReasonUnsupportedMPP)
	indexMerge := &physicalop.PhysicalIndexMergeReader{}
	indexMerge.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, "IndexMerge", indexMerge, 0)
	requireReadBillingDemoClass(t, &FlatOperator{
		Origin: indexMerge,
		IsRoot: true,
	}, readBillingDemoSiteTiDB, readBillingDemoOpClassLookupReader, false, readBillingDemoReasonUnsupportedIndexMerge)
}

func requireReadBillingDemoClass(t *testing.T, op *FlatOperator, site, opClass string, supported bool, reason string) {
	t.Helper()
	operator, ok, actualReason := readBillingDemoClassifyOperator(op)
	require.Equal(t, supported, ok)
	require.Equal(t, reason, actualReason)
	require.Equal(t, site, operator.site)
	require.Equal(t, opClass, operator.opClass)
	if supported && readBillingDemoOperatorBillable(operator) {
		_, hasWeights := readBillingDemoResolveWeights(operator.site, operator.opClass, readBillingDemoWeightVersion)
		require.True(t, hasWeights, "missing read billing demo weights for %s/%s", operator.site, operator.opClass)
	}
}

func TestExplainRUComponentSnapshotStatusAndWeights(t *testing.T) {
	require.Equal(t, explainRUComponentSnapshotMissing, extractExplainRUTestSnapshotStatus(nil))
	require.Equal(t, explainRUComponentSnapshotMissing, extractExplainRUTestSnapshotStatus(&execdetails.RURuntimeStats{}))
	require.Equal(t, explainRUComponentSnapshotNonV2, extractExplainRUTestSnapshotStatus(&execdetails.RURuntimeStats{
		RUVersion: rmclient.RUVersionV1,
		Metrics:   &execdetails.RUV2Metrics{},
	}))
	require.Equal(t, explainRUComponentSnapshotNilMetrics, extractExplainRUTestSnapshotStatus(&execdetails.RURuntimeStats{
		RUVersion: rmclient.RUVersionV2,
	}))

	bypassedMetrics := &execdetails.RUV2Metrics{}
	bypassedMetrics.SetBypass(true)
	require.Equal(t, explainRUComponentSnapshotBypassed, extractExplainRUTestSnapshotStatus(&execdetails.RURuntimeStats{
		RUVersion: rmclient.RUVersionV2,
		Metrics:   bypassedMetrics,
	}))

	okStats := &execdetails.RURuntimeStats{
		RUVersion: rmclient.RUVersionV2,
		Metrics:   &execdetails.RUV2Metrics{},
	}
	snapshot, status := extractExplainRUTestSnapshot(okStats)
	require.Equal(t, explainRUComponentSnapshotOK, status)
	require.Same(t, okStats, snapshot)
}

func extractExplainRUTestSnapshotStatus(stats *execdetails.RURuntimeStats) explainRUComponentSnapshotStatus {
	_, status := extractExplainRUTestSnapshot(stats)
	return status
}

func extractExplainRUTestSnapshot(stats *execdetails.RURuntimeStats) (*execdetails.RURuntimeStats, explainRUComponentSnapshotStatus) {
	coll := execdetails.NewRuntimeStatsColl(nil)
	if stats != nil && (stats.RUVersion != 0 || stats.Metrics != nil) {
		coll.RegisterStats(1, stats)
	}
	return explainRUExtractComponentSnapshot(coll, 1)
}

func TestReadBillingDemoDirectCopInputRowsAndWidthUsesChildRows(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	stats := &property.StatsInfo{RowCount: 5}
	parent := (&physicalop.PhysicalHashAgg{}).InitForHash(ctx, stats, 0, schema).(*physicalop.PhysicalHashAgg)
	child := (&physicalop.PhysicalSelection{}).Init(ctx, stats, 0)
	childInput := physicalop.PhysicalTableScan{}.Init(ctx, 0)
	childInput.SetSchema(schema)
	child.SetChildren(childInput)
	tree := FlatPlanTree{
		{Origin: parent, ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: false, StoreType: kv.TiKV},
		{Origin: child, IsRoot: false, StoreType: kv.TiKV},
	}

	runtimeStats := execdetails.NewRuntimeStatsColl(nil)
	recordCopRows := func(planID int, rows uint64) {
		iterations := uint64(1)
		duration := uint64(1)
		runtimeStats.RecordOneCopTask(planID, kv.TiKV, &tipb.ExecutorExecutionSummary{
			NumProducedRows: &rows,
			NumIterations:   &iterations,
			TimeProcessedNs: &duration,
		})
	}
	recordCopRows(parent.ID(), 1)
	recordCopRows(child.ID(), 5)

	proj := physicalop.PhysicalProjection{}.Init(ctx, stats, 0)
	proj.SetSchema(schema)
	operator, supported, reason := readBillingDemoClassifyOperator(&FlatOperator{
		Origin:    proj,
		IsRoot:    false,
		StoreType: kv.TiKV,
	})
	require.True(t, supported)
	require.Empty(t, reason)
	require.Equal(t, readBillingDemoSiteTiKV, operator.site)
	require.Equal(t, readBillingDemoOpClassProjection, operator.opClass)

	rows, width, widthSource, ok := readBillingDemoDirectCopInputRowsAndWidth(
		ctx,
		runtimeStats,
		tree,
		0,
		8,
		explainRUWidthSourceSchemaFallback,
		runtimeStats.GetCopStats(parent.ID()),
	)
	require.True(t, ok)
	require.Equal(t, int64(5), rows)
	require.Equal(t, float64(8), width)
	require.Equal(t, explainRUWidthSourceSchemaTypeWidth, widthSource)
}

func TestReadBillingDemoRangeScanKeepsFixedEventForEmptyInput(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	scan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
	scan.SetSchema(schema)
	scan.StoreType = kv.TiKV
	scan.TblColHists = &statistics.HistColl{Pseudo: true}
	scan.TblCols = []*expression.Column{col}
	tree := FlatPlanTree{
		{Origin: scan, IsRoot: false, StoreType: kv.TiKV},
	}

	buildUnits := func(scanDetail *tikvutil.ScanDetail, producedRows uint64) ([]readBillingDemoUnit, bool) {
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		runtimeStats.RecordCopStats(scan.ID(), kv.TiKV, scanDetail, tikvutil.TimeDetail{}, nil)
		iterations := uint64(1)
		duration := uint64(1)
		runtimeStats.RecordOneCopTask(scan.ID(), kv.TiKV, &tipb.ExecutorExecutionSummary{
			NumProducedRows: &producedRows,
			NumIterations:   &iterations,
			TimeProcessedNs: &duration,
		})
		return readBillingDemoCopUnits(
			ctx,
			runtimeStats,
			tree,
			0,
			tree[0],
			readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "tablescan"},
		)
	}

	units, ok := buildUnits(&tikvutil.ScanDetail{}, 0)
	require.True(t, ok)
	require.Equal(t, 1.0, readBillingDemoUnitValue(units, readBillingDemoUnitFixedEvents, readBillingDemoInputSideAll))
	require.Equal(t, 0.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, 0.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
	require.Equal(t, readBillingDemoInputSourceScanDetail, readBillingDemoUnitSource(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.False(t, readBillingDemoUnitExists(units, "scan_total_keys", readBillingDemoInputSideAll))
	require.False(t, readBillingDemoUnitExists(units, "processed_key_size", readBillingDemoInputSideAll))

	units, ok = buildUnits(&tikvutil.ScanDetail{TotalKeys: 4, ProcessedKeysSize: 128}, 2)
	require.True(t, ok)
	require.Equal(t, 4.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, 128.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
	require.Equal(t, readBillingDemoInputSourceScanDetail, readBillingDemoUnitSource(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))

	units, ok = buildUnits(&tikvutil.ScanDetail{}, 2)
	require.True(t, ok)
	require.Equal(t, 2.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, readBillingDemoInputSourceRuntimeRows, readBillingDemoUnitSource(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Greater(t, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll), 0.0)
}

func TestReadBillingDemoHashJoinUnitsUseBuildProbeSides(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	stats := &property.StatsInfo{RowCount: 10}
	join := (&physicalop.PhysicalHashJoin{}).Init(ctx, stats, 0)
	left := physicalop.PhysicalProjection{}.Init(ctx, stats, 0)
	right := physicalop.PhysicalProjection{}.Init(ctx, stats, 0)
	join.SetSchema(schema)
	left.SetSchema(schema)
	right.SetSchema(schema)
	tree := FlatPlanTree{
		{Origin: join, ChildrenIdx: []int{1, 2}, IsRoot: true},
		{Origin: left, IsRoot: true, Label: BuildSide},
		{Origin: right, IsRoot: true, Label: ProbeSide},
	}

	runtimeStats := execdetails.NewRuntimeStatsColl(nil)
	recordRootRows := func(planID int, rows int) {
		runtimeStats.GetBasicRuntimeStats(planID, true).Record(time.Millisecond, rows)
	}
	recordRootRows(join.ID(), 6)
	recordRootRows(left.ID(), 4)
	recordRootRows(right.ID(), 6)

	units, ok := readBillingDemoRootUnits(
		ctx,
		runtimeStats,
		tree,
		0,
		tree[0],
		readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassHashJoin, operatorKind: "hashjoin"},
	)
	require.True(t, ok)
	require.Equal(t, 1.0, readBillingDemoUnitValue(units, readBillingDemoUnitFixedEvents, readBillingDemoInputSideAll))
	require.Equal(t, 4.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideBuild))
	require.Equal(t, 6.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideProbe))
}

func readBillingDemoUnitValue(units []readBillingDemoUnit, unitName, side string) float64 {
	for _, unit := range units {
		if unit.unit == unitName && unit.side == side {
			return unit.value
		}
	}
	return -1
}

func readBillingDemoUnitSource(units []readBillingDemoUnit, unitName, side string) string {
	for _, unit := range units {
		if unit.unit == unitName && unit.side == side {
			return unit.source
		}
	}
	return ""
}

func readBillingDemoUnitExists(units []readBillingDemoUnit, unitName, side string) bool {
	for _, unit := range units {
		if unit.unit == unitName && unit.side == side {
			return true
		}
	}
	return false
}
