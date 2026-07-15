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
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
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

func TestExplainRUTargetGateStatus(t *testing.T) {
	cases := []struct {
		sql      string
		expected explainRUStatus
	}{
		{"explain analyze format='ru' select 1", explainRUStatusSuccess},
		{"explain analyze format='ru' insert into t values (1)", explainRUStatusSuccess},
		{"explain analyze format='ru' insert ignore into t values (1)", explainRUStatusSuccess},
		{"explain analyze format='ru' insert into t values (1) on duplicate key update a = values(a)", explainRUStatusSuccess},
		{"explain analyze format='ru' update t set a = 2 where a = 1", explainRUStatusSuccess},
		{"explain analyze format='ru' delete from t where a = 1", explainRUStatusSuccess},
		{"explain analyze format='ru' replace into t values (1)", explainRUStatusUnsupportedNonSelect},
		{"explain analyze format='ru' select @a := 1", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' table t", explainRUStatusUnsupportedNonSelect},
	}
	p := parser.New()
	for _, tc := range cases {
		stmt, err := p.ParseOneStmt(tc.sql, "", "")
		require.NoError(t, err, tc.sql)
		explain := stmt.(*ast.ExplainStmt)
		require.Equal(t, tc.expected, explainRUTargetGateStatus(explain.Stmt), tc.sql)
	}

	for sql, expectedKind := range map[string]string{
		"insert into t values (1)":                                       "insert",
		"insert ignore into t values (1)":                                "insert_ignore",
		"insert into t values (1) on duplicate key update a = values(a)": "upsert",
		"update t set a = 2 where a = 1":                                 "update",
		"delete from t where a = 1":                                      "delete",
	} {
		stmt, err := parser.New().ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		kind, ok := explainRUWriteDMLKind(stmt)
		require.True(t, ok)
		require.Equal(t, expectedKind, kind)
	}
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
		rowWidthSource: explainRUWidthSourceRuntimeChunkAvg,
		workRows:       2,
		hasWorkRows:    true,
		unit:           readBillingDemoUnitInputRows,
		count:          2,
		hasCount:       true,
		weight:         0.25,
		hasWeight:      true,
		previewRU:      6,
		hasPreviewRU:   true,
		source:         readBillingDemoInputSourceRuntimeChunkBytes,
		note:           "input_side=all,weight_version=v2",
	}
	require.Equal(t, []string{
		"plan", "Projection_1", "projection", "tidb/projection_eval", "1", "2", "1", "8.000000", "runtime_chunk_avg", "2", "", "input_rows", "2", "0.250000", "6.000000", "runtime_chunk_bytes", "input_side=all,weight_version=v2",
	}, row.toStrings())
}

func TestExplainRUPlanFormulaAndOperatorClasses(t *testing.T) {
	require.Equal(t, "v2", readBillingDemoWeightVersion)
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
	topNWeights, ok := readBillingDemoResolveWeights(readBillingDemoSiteTiDB, readBillingDemoOpClassTopN, readBillingDemoWeightVersion)
	require.True(t, ok)
	require.Zero(t, topNWeights.row)
	require.NotZero(t, topNWeights.orderWork)
	weight, previewRU, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: readBillingDemoUnitOrderWork, value: 12}, topNWeights)
	require.True(t, ok)
	require.Equal(t, topNWeights.orderWork, weight)
	require.Equal(t, 12*topNWeights.orderWork, previewRU)
	tikvTopNWeights, ok := readBillingDemoResolveWeights(readBillingDemoSiteTiKV, readBillingDemoOpClassTopN, readBillingDemoWeightVersion)
	require.True(t, ok)
	require.Zero(t, tikvTopNWeights.row)
	require.NotZero(t, tikvTopNWeights.orderWork)

	_, _, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: "scan_total_keys", value: 4}, tidbWeights)
	require.False(t, ok)

	writeWeights, ok := readBillingDemoResolveWeights(readBillingDemoSiteTiKV, readBillingDemoOpClassKVWrite, readBillingDemoWeightVersion)
	require.True(t, ok)
	require.InEpsilon(t, readBillingDemoWriteKeyWeight, writeWeights.writeKey, 0.000001)
	require.Equal(t, readBillingDemoWriteByteWeight, writeWeights.writeByte)
	require.Zero(t, writeWeights.region)
	require.Zero(t, writeWeights.writeRPC)

	weight, previewRU, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: readBillingDemoUnitWriteKeys, value: 3}, writeWeights)
	require.True(t, ok)
	require.Equal(t, writeWeights.writeKey, weight)
	require.Equal(t, 3*writeWeights.writeKey, previewRU)
	weight, previewRU, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: readBillingDemoUnitWriteByte, value: 4096}, writeWeights)
	require.True(t, ok)
	require.Equal(t, writeWeights.writeByte, weight)
	require.Equal(t, 4096*writeWeights.writeByte, previewRU)
	for _, diagnosticUnit := range []string{readBillingDemoUnitPrewriteRegionNum, readBillingDemoUnitTiKVWriteRPCCount} {
		weight, previewRU, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: diagnosticUnit, value: 99}, writeWeights)
		require.True(t, ok)
		require.Zero(t, weight)
		require.Zero(t, previewRU)
	}

	mutationWeights, ok := readBillingDemoResolveWeights(readBillingDemoSiteTiDB, readBillingDemoOpClassKVMutation, readBillingDemoWeightVersion)
	require.True(t, ok)
	require.Zero(t, mutationWeights.mutationCount)
	require.Zero(t, mutationWeights.mutationByte)
	require.NotEqual(t, writeWeights.writeKey, mutationWeights.mutationCount)
	require.NotEqual(t, writeWeights.writeByte, mutationWeights.mutationByte)
	for _, mutationUnit := range []string{readBillingDemoUnitEncodedMutationCount, readBillingDemoUnitEncodedMutationBytes} {
		weight, previewRU, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: mutationUnit, value: 99}, mutationWeights)
		require.True(t, ok)
		require.Zero(t, weight)
		require.Zero(t, previewRU)
	}

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

	t.Run("root sort and topn preserve algorithmic work", func(t *testing.T) {
		child := physicalop.PhysicalProjection{}.Init(ctx, stats, 0)
		child.SetSchema(schema)
		sort := physicalop.PhysicalSort{}.Init(ctx, stats, 0, nil)
		topN := physicalop.PhysicalTopN{Offset: 1, Count: 3}.Init(ctx, stats, 0)
		topN.SetSchema(schema)
		for _, tc := range []struct {
			name             string
			plan             *FlatOperator
			expectedClass    string
			inputRows        int
			inputBytes       int64
			expectedWork     float64
			expectedWorkText string
		}{
			{name: "sort n log n preserves fractional work", plan: &FlatOperator{Origin: sort, ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB}, expectedClass: readBillingDemoOpClassSort, inputRows: 3, inputBytes: 30, expectedWork: 4.754887502163469, expectedWorkText: "4.754887502163469"},
			{name: "topn n log k", plan: &FlatOperator{Origin: topN, ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB}, expectedClass: readBillingDemoOpClassTopN, inputRows: 8, inputBytes: 80, expectedWork: 16, expectedWorkText: "16"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tree := FlatPlanTree{
					tc.plan,
					{Origin: child, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
				}
				runtimeStats := execdetails.NewRuntimeStatsColl(nil)
				for _, planID := range []int{tc.plan.Origin.ID(), child.ID()} {
					basic := runtimeStats.GetBasicRuntimeStats(planID, true)
					basic.Record(time.Millisecond, tc.inputRows)
					basic.RecordBytes(0, tc.inputBytes)
				}
				operator, supported, reason := readBillingDemoClassifyOperator(tree[0])
				require.True(t, supported)
				require.Empty(t, reason)
				require.Equal(t, tc.expectedClass, operator.opClass)
				units, reason, ok := readBillingDemoRootUnits(runtimeStats, tree, 0, tree[0], operator)
				require.True(t, ok)
				require.Empty(t, reason)
				require.Equal(t, float64(tc.inputRows), readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
				require.InDelta(t, tc.expectedWork, readBillingDemoUnitValue(units, readBillingDemoUnitOrderWork, readBillingDemoInputSideAll), 1e-12)
				require.Equal(t, readBillingDemoInputSourceRuntimeOrderingWork, readBillingDemoUnitSource(units, readBillingDemoUnitOrderWork, readBillingDemoInputSideAll))
				operator.status = readBillingDemoStatusOperatorOK
				operator.units = units
				operator.id = tc.plan.ExplainID().String()
				result := readBillingDemoResult{
					status:    readBillingDemoStatusSuccess,
					reason:    readBillingDemoReasonNone,
					operators: []readBillingDemoOperatorResult{operator},
				}
				weights, ok := readBillingDemoResolveWeights(operator.site, operator.opClass, readBillingDemoWeightVersion)
				require.True(t, ok)
				require.Zero(t, weights.row)
				rows := explainRUBuildReadBillingRows(result, explainRUComponentSnapshotOK)
				require.InEpsilon(t, weights.fixedEvent+float64(tc.inputBytes)*weights.byte+tc.expectedWork*weights.orderWork, rows[0].previewRU, 0.000001)
				var orderRows int
				for _, row := range rows {
					if row.unit == readBillingDemoUnitOrderWork {
						orderRows++
						require.InDelta(t, tc.expectedWork, row.workRows, 1e-12)
						require.Equal(t, tc.expectedWorkText, row.toStrings()[9])
					}
				}
				require.Equal(t, 1, orderRows)

				stats := buildReadBillingDemoStatementStats(result)
				require.Equal(t, "v2", stats.WeightVersion)
				var orderSamples int
				for _, sample := range stats.BaseUnits {
					if sample.Unit == readBillingDemoUnitOrderWork {
						orderSamples++
						require.InDelta(t, tc.expectedWork, sample.Value, 1e-12)
						require.Equal(t, readBillingDemoInputSourceRuntimeOrderingWork, sample.InputSource)
					}
				}
				require.Equal(t, 1, orderSamples)
			})
		}
	})

	t.Run("ordering work boundaries fail only on invalid evidence", func(t *testing.T) {
		sort := physicalop.PhysicalSort{}.Init(ctx, stats, 0, nil)
		sortOp := &FlatOperator{Origin: sort, IsRoot: true, StoreType: kv.TiDB}
		unit, ok := readBillingDemoOrderingWorkUnit(sortOp, readBillingDemoOpClassSort, 0)
		require.True(t, ok)
		require.Zero(t, unit.value)
		_, ok = readBillingDemoOrderingWorkUnit(sortOp, readBillingDemoOpClassSort, -1)
		require.False(t, ok)

		maxInt64 := int64(^uint64(0) >> 1)
		unit, ok = readBillingDemoOrderingWorkUnit(sortOp, readBillingDemoOpClassSort, maxInt64/2)
		require.True(t, ok)
		require.Greater(t, unit.value, float64(maxInt64))

		hugeTopN := physicalop.PhysicalTopN{Offset: ^uint64(0), Count: ^uint64(0)}.Init(ctx, stats, 0)
		unit, ok = readBillingDemoOrderingWorkUnit(&FlatOperator{Origin: hugeTopN, IsRoot: true, StoreType: kv.TiDB}, readBillingDemoOpClassTopN, 1)
		require.True(t, ok)
		require.Greater(t, unit.value, 64.0)
		require.LessOrEqual(t, unit.value, 65.0)

		malformedCopTopN := physicalop.PhysicalTopN{Offset: 1, Count: 3}.Init(ctx, stats, 0)
		_, ok = readBillingDemoOrderingWorkUnit(&FlatOperator{Origin: malformedCopTopN, IsRoot: false, StoreType: kv.TiKV}, readBillingDemoOpClassTopN, 8)
		require.False(t, ok)
	})
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

func TestReadBillingDemoWriteDMLResult(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder = &stmtctx.PreviewKVMutationRecorder{}
	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.RecordSet(5, 7)
	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.RecordDelete(3)
	for _, dmlKind := range []string{"insert", "update", "delete", "upsert"} {
		result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
		appendReadBillingDemoMutation(&result, ctx, dmlKind)
		require.NotEmpty(t, result.operators)
		op := result.operators[0]
		require.Equal(t, readBillingDemoSiteTiDB, op.site)
		require.Equal(t, readBillingDemoOpClassKVMutation, op.opClass)
		require.Equal(t, readBillingDemoOperatorMemDBMutation, op.operatorKind)
		require.Equal(t, dmlKind, op.dmlKind)
		require.Equal(t, readBillingDemoScopeStatementAttempted, op.scope)
		units := make(map[string]readBillingDemoUnit)
		for _, unit := range op.units {
			units[unit.unit] = unit
		}
		require.Equal(t, 2.0, units[readBillingDemoUnitEncodedMutationCount].value)
		require.Equal(t, 15.0, units[readBillingDemoUnitEncodedMutationBytes].value)
		weights, ok := readBillingDemoResolveWeights(op.site, op.opClass, readBillingDemoWeightVersion)
		require.True(t, ok)
		require.Zero(t, weights.mutationCount)
		require.Zero(t, weights.mutationByte)
		require.Contains(t, result.operators, readBillingDemoMutationDiagnostic(dmlKind, readBillingDemoReasonDMLAncillaryPartial))
	}

	ctx.GetSessionVars().SetInTxn(true)
	ctx.GetSessionVars().TxnCtx.CouldRetry = true
	retryableResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
	appendReadBillingDemoMutation(&retryableResult, ctx, "update")
	require.Contains(t, retryableResult.operators, readBillingDemoMutationDiagnostic("update", readBillingDemoReasonOptimisticReplayPartial))
	ctx.GetSessionVars().SetInTxn(false)
	ctx.GetSessionVars().TxnCtx.CouldRetry = false

	ruv2Metrics := execdetails.NewRUV2Metrics()
	ruv2Metrics.AddResourceManagerWriteCnt(7)
	commitDetail := &tikvutil.CommitDetails{
		WriteKeys:         3,
		WriteSize:         66,
		PrewriteRegionNum: 2,
	}
	result := buildWriteBillingDemoResultFromDetails("insert", commitDetail, ruv2Metrics)
	require.Equal(t, readBillingDemoStatusSuccess, result.status)
	require.Equal(t, readBillingDemoReasonNone, result.reason)
	require.Len(t, result.operators, 1)
	require.Equal(t, readBillingDemoStatusOperatorOK, result.operators[0].status)
	require.Equal(t, readBillingDemoOperatorTxnPrewrite, result.operators[0].operatorKind)
	require.Equal(t, "insert", result.operators[0].dmlKind)

	units := make(map[string]readBillingDemoUnit)
	for _, unit := range result.operators[0].units {
		units[unit.unit] = unit
	}
	require.Equal(t, 3.0, units[readBillingDemoUnitWriteKeys].value)
	require.Equal(t, 66.0, units[readBillingDemoUnitWriteByte].value)
	require.Equal(t, 2.0, units[readBillingDemoUnitPrewriteRegionNum].value)
	require.Equal(t, 7.0, units[readBillingDemoUnitTiKVWriteRPCCount].value)

	ctx.GetSessionVars().StmtCtx.MergeExecDetails(commitDetail)
	commitResult := buildTxnCommitBillingDemoResult(ctx, ruv2Metrics, nil)
	require.Equal(t, readBillingDemoStatusSuccess, commitResult.status)
	require.NotEmpty(t, commitResult.operators)
	require.Equal(t, readBillingDemoStatusOperatorOK, commitResult.operators[0].status)
	require.Empty(t, commitResult.operators[0].dmlKind)
	require.Equal(t, readBillingDemoScopeTxnPrewritePayload, commitResult.operators[0].scope)
	commitUnits := make(map[string]readBillingDemoUnit)
	for _, unit := range commitResult.operators[0].units {
		commitUnits[unit.unit] = unit
	}
	require.Equal(t, 3.0, commitUnits[readBillingDemoUnitWriteKeys].value)
	require.Equal(t, 66.0, commitUnits[readBillingDemoUnitWriteByte].value)

	rows := explainRUBuildReadBillingRows(result, explainRUComponentSnapshotOK)
	require.InEpsilon(t,
		3*readBillingDemoWriteKeyWeight+66*readBillingDemoWriteByteWeight,
		rows[0].previewRU,
		0.000001,
	)
	var diagnosticRows int
	for _, row := range rows[1:] {
		if row.unit == readBillingDemoUnitPrewriteRegionNum || row.unit == readBillingDemoUnitTiKVWriteRPCCount {
			diagnosticRows++
			require.True(t, row.hasWeight)
			require.Zero(t, row.weight)
			require.True(t, row.hasPreviewRU)
			require.Zero(t, row.previewRU)
			require.Contains(t, row.note, "diagnostic_only=true")
		}
	}
	require.Equal(t, 2, diagnosticRows)

	stats := buildReadBillingDemoStatementStats(result)
	require.Len(t, stats.BaseUnits, 4)

	partialResult := buildWriteBillingDemoResultFromDetails("delete", &tikvutil.CommitDetails{WriteKeys: 1, WriteSize: 2}, nil)
	require.Equal(t, readBillingDemoStatusSuccess, partialResult.status)
	require.Len(t, partialResult.operators, 3)
	rows = explainRUBuildReadBillingRows(partialResult, explainRUComponentSnapshotMissing)
	require.Contains(t, rows[0].note, "partial_missing_prewrite_region_num")
	require.Contains(t, rows[0].note, "partial_missing_write_rpc_count")

	missingResult := buildWriteBillingDemoResultFromDetails("update", nil, ruv2Metrics)
	require.Equal(t, readBillingDemoStatusSuccess, missingResult.status)
	require.Len(t, missingResult.operators, 1)
	require.Equal(t, readBillingDemoStatusPartial, missingResult.operators[0].status)
	require.Equal(t, readBillingDemoReasonMissingCommitDetail, missingResult.operators[0].reason)
	require.True(t, missingResult.operators[0].emitStatusRow)
	// Pipelined transactions expose a non-nil CommitDetails without logical
	// WriteKeys/WriteSize. The incomplete payload must not become billable zero
	// units merely because the detail object exists.
	pipelinedResult := buildTiKVWriteBillingDemoOperators("update", &tikvutil.CommitDetails{}, ruv2Metrics, true)
	require.Len(t, pipelinedResult, 1)
	require.Equal(t, readBillingDemoStatusPartial, pipelinedResult[0].status)
	require.Equal(t, readBillingDemoReasonPipelinedWritePartial, pipelinedResult[0].reason)
	require.True(t, pipelinedResult[0].emitStatusRow)
	require.Empty(t, pipelinedResult[0].units)
	require.Empty(t, buildReadBillingDemoStatementStats(readBillingDemoResult{
		status:    readBillingDemoStatusSuccess,
		operators: pipelinedResult,
	}).BaseUnits)

	missingWriteKeys := buildWriteBillingDemoResultFromDetails("update", &tikvutil.CommitDetails{WriteSize: 2}, ruv2Metrics)
	require.Equal(t, readBillingDemoStatusSuccess, missingWriteKeys.status)
	require.Contains(t, missingWriteKeys.operators, readBillingDemoWriteDiagnosticStatus("update", readBillingDemoReasonMissingWriteKeys))

	missingWriteByte := buildWriteBillingDemoResultFromDetails("update", &tikvutil.CommitDetails{WriteKeys: 1}, ruv2Metrics)
	require.Equal(t, readBillingDemoStatusSuccess, missingWriteByte.status)
	require.Contains(t, missingWriteByte.operators, readBillingDemoWriteDiagnosticStatus("update", readBillingDemoReasonMissingWriteByte))

	zeroResult := buildWriteBillingDemoResultFromDetails("update", &tikvutil.CommitDetails{}, ruv2Metrics)
	require.Equal(t, readBillingDemoStatusSuccess, zeroResult.status)
	require.Equal(t, readBillingDemoReasonZeroMutation, zeroResult.operators[0].reason)
	require.Equal(t, 0.0, zeroResult.operators[0].units[0].value)
	require.Equal(t, 0.0, zeroResult.operators[0].units[1].value)
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

func TestReadBillingDemoNonScanCopWithoutBytesFailsClosed(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	stats := &property.StatsInfo{RowCount: 5}
	reader := physicalop.PhysicalTableReader{}.Init(ctx, 0)
	proj := physicalop.PhysicalProjection{}.Init(ctx, stats, 0)
	scan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
	reader.SetSchema(schema)
	proj.SetSchema(schema)
	scan.SetSchema(schema)
	tree := FlatPlanTree{
		{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
		{Origin: proj, ChildrenIdx: []int{2}, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
		{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
	}

	runtimeStats := execdetails.NewRuntimeStatsColl(nil)
	operator, supported, reason := readBillingDemoClassifyOperator(tree[1])
	require.True(t, supported)
	require.Empty(t, reason)
	require.Equal(t, readBillingDemoSiteTiKV, operator.site)
	require.Equal(t, readBillingDemoOpClassProjection, operator.opClass)

	outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 1, operator)
	require.False(t, outcome.success)
	require.Nil(t, outcome.units)
	require.Equal(t, readBillingDemoReasonMissingCopChildRuntimeRows, outcome.failure.reason)
}

func TestReadBillingDemoRangeScanUsesProcessedKeyAverage(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	scan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
	scan.SetSchema(schema)
	reader := physicalop.PhysicalTableReader{}.Init(ctx, 0)
	reader.SetSchema(schema)
	tree := FlatPlanTree{
		{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
		{Origin: scan, ChildrenEndIdx: 1, IsRoot: false, StoreType: kv.TiKV},
	}

	rows, bytes, ok := readBillingDemoRangeScanInput(10, 5, 100)
	require.True(t, ok)
	require.Equal(t, int64(10), rows)
	require.Equal(t, 200.0, bytes)
	for _, tc := range []struct {
		totalKeys         int64
		processedKeys     int64
		processedKeysSize int64
	}{
		{0, 5, 100},
		{10, 0, 100},
		{10, 5, 0},
	} {
		_, _, ok = readBillingDemoRangeScanInput(tc.totalKeys, tc.processedKeys, tc.processedKeysSize)
		require.False(t, ok)
	}

	buildUnits := func(scanDetail *tikvutil.ScanDetail) readBillingDemoCopUnitOutcome {
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		runtimeStats.RecordCopStats(scan.ID(), kv.TiKV, scanDetail, tikvutil.TimeDetail{}, nil)
		return readBillingDemoCopUnits(
			newReadBillingDemoCopEstimator(tree, runtimeStats),
			1,
			readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "tablescan"},
		)
	}

	outcome := buildUnits(&tikvutil.ScanDetail{TotalKeys: 10, ProcessedKeys: 5, ProcessedKeysSize: 100})
	require.True(t, outcome.success)
	units := outcome.units
	require.Equal(t, 1.0, readBillingDemoUnitValue(units, readBillingDemoUnitFixedEvents, readBillingDemoInputSideAll))
	require.Equal(t, 10.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, 200.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
	require.Equal(t, readBillingDemoInputSourceScanDetail, readBillingDemoUnitSource(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, explainRUWidthSourceScanDetailProcessedAvg, readBillingDemoUnitWidthSource(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))

	outcome = buildUnits(&tikvutil.ScanDetail{})
	require.False(t, outcome.success)
	require.Nil(t, outcome.units)
	require.Equal(t, readBillingDemoReasonMissingScanWidthEvidence, outcome.failure.reason)
}

func TestReadBillingDemoCopInputEstimator(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	stats := &property.StatsInfo{RowCount: 10}
	reader := physicalop.PhysicalTableReader{}.Init(ctx, 0)
	limit := physicalop.PhysicalLimit{}.Init(ctx, stats, 0)
	projection := physicalop.PhysicalProjection{}.Init(ctx, stats, 0)
	selection := physicalop.PhysicalSelection{}.Init(ctx, stats, 0)
	scan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
	reader.SetSchema(schema)
	limit.SetSchema(schema)
	projection.SetSchema(schema)
	scan.SetSchema(schema)

	recordSummary := func(runtimeStats *execdetails.RuntimeStatsColl, planID int, rows uint64, detail *tikvutil.ScanDetail) {
		one := uint64(1)
		runtimeStats.RecordCopStats(planID, kv.TiKV, detail, tikvutil.TimeDetail{}, &tipb.ExecutorExecutionSummary{
			TimeProcessedNs: &one,
			NumProducedRows: &rows,
			NumIterations:   &one,
		})
	}

	t.Run("selection uses direct child rows and component scan width", func(t *testing.T) {
		tree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
			{Origin: selection, ChildrenIdx: []int{2}, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, scan.ID(), 4, nil)
		recordSummary(runtimeStats, selection.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
		operator, supported, _ := readBillingDemoClassifyOperator(tree[1])
		require.True(t, supported)
		outcome := readBillingDemoCopUnits(estimator, 1, operator)
		require.True(t, outcome.success)
		require.Equal(t, 4.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
		require.Equal(t, 80.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
		require.Equal(t, readBillingDemoInputSourceRuntimeChildActRows, readBillingDemoUnitSource(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
		require.Equal(t, explainRUWidthSourceScanDetailProcessedEstimate, readBillingDemoUnitWidthSource(outcome.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
		require.LessOrEqual(t, estimator.nodeVisits, 6*len(tree))
		require.LessOrEqual(t, estimator.edgeVisits, 4)
		require.LessOrEqual(t, estimator.auxiliaryEntryCount(), 12*len(tree))
	})

	t.Run("supported unary operator width propagation matrix", func(t *testing.T) {
		topN := physicalop.PhysicalTopN{Count: 8}.Init(ctx, stats, 0)
		topN.SetSchema(schema)
		hashAgg := (&physicalop.BasePhysicalAgg{}).InitForHash(ctx, stats, 0, schema)
		streamAgg := (&physicalop.BasePhysicalAgg{}).InitForStream(ctx, stats, 0, schema)
		cases := []struct {
			name              string
			node              *FlatOperator
			widthState        readBillingDemoCopWidthState
			expectedOrderWork float64
		}{
			{name: "selection", node: &FlatOperator{Origin: selection, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthKnown, expectedOrderWork: -1},
			{name: "limit", node: &FlatOperator{Origin: limit, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthKnown, expectedOrderWork: -1},
			{name: "topn", node: &FlatOperator{Origin: topN, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthKnown, expectedOrderWork: 12},
			{name: "projection", node: &FlatOperator{Origin: projection, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthBarrier, expectedOrderWork: -1},
			{name: "hashagg", node: &FlatOperator{Origin: hashAgg, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthBarrier, expectedOrderWork: -1},
			{name: "streamagg", node: &FlatOperator{Origin: streamAgg, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthBarrier, expectedOrderWork: -1},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				tc.node.ChildrenIdx = []int{2}
				tc.node.ChildrenEndIdx = 2
				tree := FlatPlanTree{
					{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
					tc.node,
					{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
				}
				runtimeStats := execdetails.NewRuntimeStatsColl(nil)
				recordSummary(runtimeStats, scan.ID(), 4, nil)
				recordSummary(runtimeStats, tc.node.Origin.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
				estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
				operator, supported, _ := readBillingDemoClassifyOperator(tc.node)
				require.True(t, supported)
				outcome := readBillingDemoCopUnits(estimator, 1, operator)
				require.True(t, outcome.success)
				require.Equal(t, 4.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
				require.Equal(t, 80.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
				require.Equal(t, tc.expectedOrderWork, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitOrderWork, readBillingDemoInputSideAll))
				require.Equal(t, tc.widthState, estimator.outputEstimate(1).widthState)
			})
		}
	})

	t.Run("selection output feeds projection input", func(t *testing.T) {
		tree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
			{Origin: projection, ChildrenIdx: []int{2}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: selection, ChildrenIdx: []int{3}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, scan.ID(), 5, nil)
		recordSummary(runtimeStats, selection.ID(), 3, nil)
		recordSummary(runtimeStats, projection.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		operator, _, _ := readBillingDemoClassifyOperator(tree[1])
		outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 1, operator)
		require.True(t, outcome.success)
		require.Equal(t, 3.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
		require.Equal(t, 60.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
	})

	t.Run("multi scan and multi detail components are ambiguous", func(t *testing.T) {
		otherScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		otherScan.SetSchema(schema)
		multiScanTree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
			{Origin: selection, ChildrenIdx: []int{2, 3}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			{Origin: otherScan, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, scan.ID(), 4, nil)
		recordSummary(runtimeStats, otherScan.ID(), 4, nil)
		recordSummary(runtimeStats, selection.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		estimator := newReadBillingDemoCopEstimator(multiScanTree, runtimeStats)
		require.Equal(t, readBillingDemoCopWidthAmbiguous, estimator.outputEstimate(2).widthState)
		scanOperator, _, _ := readBillingDemoClassifyOperator(multiScanTree[2])
		require.Equal(t, readBillingDemoReasonAmbiguousCopScanWidth, readBillingDemoCopUnits(estimator, 2, scanOperator).failure.reason)

		multiDetailTree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
			{Origin: selection, ChildrenIdx: []int{2}, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats = execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, scan.ID(), 4, &tikvutil.ScanDetail{TotalKeys: 2, ProcessedKeys: 2, ProcessedKeysSize: 40})
		recordSummary(runtimeStats, selection.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 3, ProcessedKeys: 3, ProcessedKeysSize: 60})
		estimator = newReadBillingDemoCopEstimator(multiDetailTree, runtimeStats)
		require.Equal(t, readBillingDemoCopWidthAmbiguous, estimator.outputEstimate(2).widthState)
	})

	t.Run("projection consumes width but blocks its parent", func(t *testing.T) {
		tree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
			{Origin: limit, ChildrenIdx: []int{2}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: projection, ChildrenIdx: []int{3}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, scan.ID(), 4, nil)
		recordSummary(runtimeStats, projection.ID(), 3, nil)
		recordSummary(runtimeStats, limit.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
		projectionOperator, _, _ := readBillingDemoClassifyOperator(tree[2])
		require.True(t, readBillingDemoCopUnits(estimator, 2, projectionOperator).success)
		limitOperator, _, _ := readBillingDemoClassifyOperator(tree[1])
		limitOutcome := readBillingDemoCopUnits(estimator, 1, limitOperator)
		require.False(t, limitOutcome.success)
		require.Equal(t, readBillingDemoReasonUnsupportedCopWidthTransform, limitOutcome.failure.reason)
		require.Equal(t, readBillingDemoCopFailureCurrent, limitOutcome.failure.kind)
	})

	t.Run("missing scan summary becomes projection cause across output edge", func(t *testing.T) {
		tree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
			{Origin: limit, ChildrenIdx: []int{2}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: projection, ChildrenIdx: []int{3}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, projection.ID(), 3, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		recordSummary(runtimeStats, limit.ID(), 2, nil)
		estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
		limitOperator, _, _ := readBillingDemoClassifyOperator(tree[1])
		outcome := readBillingDemoCopUnits(estimator, 1, limitOperator)
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoCopFailureIntrinsicCause, outcome.failure.kind)
		require.Equal(t, 2, outcome.failure.failingIdx)
		require.Equal(t, readBillingDemoReasonMissingCopChildRuntimeRows, outcome.failure.reason)
	})

	t.Run("select and DML materialize projection cause without skipping scan", func(t *testing.T) {
		limit.SetChildren(projection)
		projection.SetChildren(scan)
		reader.TablePlan = limit
		flat := FlattenPhysicalPlan(reader, true)
		require.Len(t, flat.Main, 4)

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		rootStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
		rootStats.Record(time.Millisecond, 2)
		rootStats.RecordBytes(0, 40)
		recordSummary(runtimeStats, projection.ID(), 3, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		recordSummary(runtimeStats, limit.ID(), 2, nil)
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

		selectResult := buildReadBillingDemoResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
		require.Equal(t, readBillingDemoStatusUnknownInput, selectResult.status)
		require.Equal(t, readBillingDemoReasonMissingCopChildRuntimeRows, selectResult.reason)
		require.Len(t, selectResult.operators, 1)
		require.Equal(t, flat.Main[2].ExplainID().String(), selectResult.operators[0].id)

		dmlResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
		appendReadBillingDemoDMLTree(&dmlResult, runtimeStats, flat.Main)
		var dependentCount, projectionPartialCount, scanOKCount int
		for _, operator := range dmlResult.operators {
			switch {
			case operator.id == flat.Main[1].ExplainID().String() && operator.status == readBillingDemoStatusPartial && operator.reason == readBillingDemoReasonDependentCopInputUnavailable:
				dependentCount++
			case operator.id == flat.Main[2].ExplainID().String() && operator.status == readBillingDemoStatusPartial && operator.reason == readBillingDemoReasonMissingCopChildRuntimeRows:
				projectionPartialCount++
			case operator.id == flat.Main[3].ExplainID().String() && operator.status == readBillingDemoStatusOperatorOK:
				scanOKCount++
			}
		}
		require.Equal(t, 1, dependentCount)
		require.Equal(t, 1, projectionPartialCount)
		require.Equal(t, 1, scanOKCount)
	})

	t.Run("DML keeps projection units when its output summary is missing", func(t *testing.T) {
		limit.SetChildren(projection)
		projection.SetChildren(scan)
		reader.TablePlan = limit
		flat := FlattenPhysicalPlan(reader, true)
		require.Len(t, flat.Main, 4)

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		rootStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
		rootStats.Record(time.Millisecond, 2)
		rootStats.RecordBytes(0, 40)
		recordSummary(runtimeStats, scan.ID(), 4, nil)
		recordSummary(runtimeStats, limit.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

		selectResult := buildReadBillingDemoResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
		require.Equal(t, readBillingDemoStatusUnknownInput, selectResult.status)
		require.Equal(t, readBillingDemoReasonMissingCopChildRuntimeRows, selectResult.reason)
		require.Len(t, selectResult.operators, 1)
		require.Equal(t, flat.Main[1].ExplainID().String(), selectResult.operators[0].id)

		dmlResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
		appendReadBillingDemoDMLTree(&dmlResult, runtimeStats, flat.Main)
		var limitPartial, projectionOK, scanOK int
		for _, operator := range dmlResult.operators {
			switch {
			case operator.id == flat.Main[1].ExplainID().String() && operator.status == readBillingDemoStatusPartial && operator.reason == readBillingDemoReasonMissingCopChildRuntimeRows:
				limitPartial++
			case operator.id == flat.Main[2].ExplainID().String() && operator.status == readBillingDemoStatusOperatorOK:
				projectionOK++
				require.Equal(t, 4.0, readBillingDemoUnitValue(operator.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
				require.Equal(t, 80.0, readBillingDemoUnitValue(operator.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
			case operator.id == flat.Main[3].ExplainID().String() && operator.status == readBillingDemoStatusOperatorOK:
				scanOK++
			}
		}
		require.Equal(t, 1, limitPartial)
		require.Equal(t, 1, projectionOK)
		require.Equal(t, 1, scanOK)
	})

	t.Run("unsupported descendant cause is preserved", func(t *testing.T) {
		sort := physicalop.PhysicalSort{}.Init(ctx, stats, 0, nil)
		limit.SetChildren(sort)
		sort.SetChildren(scan)
		reader.TablePlan = limit
		flat := FlattenPhysicalPlan(reader, true)
		require.Len(t, flat.Main, 4)

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		rootStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
		rootStats.Record(time.Millisecond, 2)
		rootStats.RecordBytes(0, 40)
		recordSummary(runtimeStats, scan.ID(), 4, nil)
		recordSummary(runtimeStats, sort.ID(), 3, nil)
		recordSummary(runtimeStats, limit.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

		selectResult := buildReadBillingDemoResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
		require.Equal(t, readBillingDemoStatusUnsupported, selectResult.status)
		require.Equal(t, readBillingDemoReasonUnsupportedOperator, selectResult.reason)
		require.Len(t, selectResult.operators, 1)
		require.Equal(t, flat.Main[2].ExplainID().String(), selectResult.operators[0].id)

		dmlResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
		appendReadBillingDemoDMLTree(&dmlResult, runtimeStats, flat.Main)
		var limitDependent, sortUnsupported, scanOK int
		for _, operator := range dmlResult.operators {
			switch {
			case operator.id == flat.Main[1].ExplainID().String() && operator.status == readBillingDemoStatusPartial && operator.reason == readBillingDemoReasonDependentCopInputUnavailable:
				limitDependent++
			case operator.id == flat.Main[2].ExplainID().String() && operator.status == readBillingDemoStatusPartial && operator.reason == readBillingDemoReasonUnsupportedOperator:
				sortUnsupported++
			case operator.id == flat.Main[3].ExplainID().String() && operator.status == readBillingDemoStatusOperatorOK:
				scanOK++
			}
		}
		require.Equal(t, 1, limitDependent)
		require.Equal(t, 1, sortUnsupported)
		require.Equal(t, 1, scanOK)
	})

	t.Run("invalid child rows and multi child arity fail explicitly", func(t *testing.T) {
		invalidTree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
			{Origin: selection, ChildrenIdx: []int{2}, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, scan.ID(), ^uint64(0), nil)
		recordSummary(runtimeStats, selection.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		operator, _, _ := readBillingDemoClassifyOperator(invalidTree[1])
		outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(invalidTree, runtimeStats), 1, operator)
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoCopFailureIntrinsicCause, outcome.failure.kind)
		require.Equal(t, 2, outcome.failure.failingIdx)
		require.Equal(t, readBillingDemoReasonInvalidCopRuntimeRows, outcome.failure.reason)

		otherScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		otherScan.SetSchema(schema)
		multiChildTree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
			{Origin: selection, ChildrenIdx: []int{2, 3}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			{Origin: otherScan, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
		}
		outcome = readBillingDemoCopUnits(newReadBillingDemoCopEstimator(multiChildTree, execdetails.NewRuntimeStatsColl(nil)), 1, operator)
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoStatusUnsupported, outcome.failure.status)
		require.Equal(t, readBillingDemoReasonUnsupportedCopMultiChild, outcome.failure.reason)
	})

	t.Run("malformed references stay linear and fail structurally", func(t *testing.T) {
		tree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1, 1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
			{Origin: scan, ChildrenEndIdx: 1, IsRoot: false, StoreType: kv.TiKV},
		}
		estimator := newReadBillingDemoCopEstimator(tree, execdetails.NewRuntimeStatsColl(nil))
		failure, ok := estimator.firstTreeFailure()
		require.True(t, ok)
		require.Equal(t, readBillingDemoReasonUnsupportedCopStructure, failure.reason)
		require.LessOrEqual(t, estimator.nodeVisits, 6*len(tree))
		require.LessOrEqual(t, estimator.edgeVisits, 4)

		const siblingCount = 128
		wideTree := make(FlatPlanTree, siblingCount+1)
		children := make([]int, siblingCount)
		wideTree[0] = &FlatOperator{Origin: reader, ChildrenIdx: children, ChildrenEndIdx: siblingCount, IsRoot: true, StoreType: kv.TiDB}
		for i := 1; i <= siblingCount; i++ {
			children[i-1] = i
			wideTree[i] = &FlatOperator{Origin: scan, ChildrenEndIdx: siblingCount, IsRoot: false, StoreType: kv.TiKV}
		}
		estimator = newReadBillingDemoCopEstimator(wideTree, execdetails.NewRuntimeStatsColl(nil))
		failure, ok = estimator.firstTreeFailure()
		require.True(t, ok)
		require.Equal(t, readBillingDemoReasonUnsupportedCopStructure, failure.reason)
		require.LessOrEqual(t, estimator.nodeVisits, 6*len(wideTree))
		require.LessOrEqual(t, estimator.edgeVisits, 2*siblingCount)
	})

	t.Run("special tree root may omit children end index", func(t *testing.T) {
		// CTE-definition and scalar-subquery synthetic roots populate ChildrenIdx
		// but intentionally leave ChildrenEndIdx at its zero value.
		tree := FlatPlanTree{
			{Origin: projection, ChildrenIdx: []int{1}, IsRoot: true, StoreType: kv.TiDB, Label: SeedPart},
			{Origin: reader, ChildrenIdx: []int{2}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
			{Origin: selection, ChildrenIdx: []int{3}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, scan.ID(), 4, nil)
		recordSummary(runtimeStats, selection.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
		_, failed := estimator.firstTreeFailure()
		require.False(t, failed)
		operator, _, _ := readBillingDemoClassifyOperator(tree[2])
		outcome := readBillingDemoCopUnits(estimator, 2, operator)
		require.True(t, outcome.success)
		require.Equal(t, 4.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
		require.Equal(t, 80.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
	})

	t.Run("zero rows are observed and partial summaries fail closed", func(t *testing.T) {
		tree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
			{Origin: selection, ChildrenIdx: []int{2}, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordSummary(runtimeStats, scan.ID(), 0, nil)
		recordSummary(runtimeStats, selection.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		operator, _, _ := readBillingDemoClassifyOperator(tree[1])
		outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 1, operator)
		require.True(t, outcome.success)
		require.Equal(t, 0.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
		require.Equal(t, 0.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))

		recordSummary(runtimeStats, selection.ID(), 0, nil)
		outcome = readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 1, operator)
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoReasonIncompleteCopRuntimeRows, outcome.failure.reason)
	})

	t.Run("sibling components keep scan detail isolated", func(t *testing.T) {
		indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		indexScan.SetSchema(schema)
		tree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1, 2}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
			{Origin: scan, ChildrenEndIdx: 1, IsRoot: false, StoreType: kv.TiKV},
			{Origin: indexScan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		runtimeStats.RecordCopStats(scan.ID(), kv.TiKV, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 50}, tikvutil.TimeDetail{}, nil)
		runtimeStats.RecordCopStats(indexScan.ID(), kv.TiKV, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 150}, tikvutil.TimeDetail{}, nil)
		estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
		require.Equal(t, 10.0, estimator.outputEstimate(1).avgRowWidth)
		require.Equal(t, 30.0, estimator.outputEstimate(2).avgRowWidth)
	})

	t.Run("full builder wires flattened table reader component", func(t *testing.T) {
		selection.SetChildren(scan)
		reader.TablePlan = selection
		flat := FlattenPhysicalPlan(reader, true)
		require.Len(t, flat.Main, 3)
		require.Equal(t, selection.ID(), flat.Main[1].Origin.ID())
		require.Equal(t, scan.ID(), flat.Main[2].Origin.ID())

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		rootStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
		rootStats.Record(time.Millisecond, 2)
		rootStats.RecordBytes(0, 40)
		recordSummary(runtimeStats, scan.ID(), 4, nil)
		recordSummary(runtimeStats, selection.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

		result := buildReadBillingDemoResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
		require.Equal(t, readBillingDemoStatusSuccess, result.status)
		var selectionResult readBillingDemoOperatorResult
		for _, operator := range result.operators {
			if operator.id == flat.Main[1].ExplainID().String() {
				selectionResult = operator
				break
			}
		}
		require.Equal(t, readBillingDemoStatusOperatorOK, selectionResult.status)
		require.Equal(t, 4.0, readBillingDemoUnitValue(selectionResult.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
		require.Equal(t, 80.0, readBillingDemoUnitValue(selectionResult.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
		statementStats := buildReadBillingDemoStatementStats(result)
		require.NotEmpty(t, statementStats.BaseUnits)
		for _, sample := range statementStats.BaseUnits {
			require.Equal(t, "v3", sample.ModelVersion)
		}
	})

	t.Run("full builder wires root sort order work", func(t *testing.T) {
		rootSort := physicalop.PhysicalSort{}.Init(ctx, stats, 0, nil)
		scan.SetStats(stats)
		reader.TablePlan = scan
		rootSort.SetChildren(reader)
		flat := FlattenPhysicalPlan(rootSort, true)
		require.Len(t, flat.Main, 3)
		require.Equal(t, rootSort.ID(), flat.Main[0].Origin.ID())
		require.Equal(t, reader.ID(), flat.Main[1].Origin.ID())
		require.Equal(t, scan.ID(), flat.Main[2].Origin.ID())

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		for _, rootEvidence := range []struct {
			planID int
			rows   int
			bytes  int64
		}{
			{planID: rootSort.ID(), rows: 8, bytes: 80},
			{planID: reader.ID(), rows: 8, bytes: 80},
		} {
			basic := runtimeStats.GetBasicRuntimeStats(rootEvidence.planID, true)
			basic.Record(time.Millisecond, rootEvidence.rows)
			basic.RecordBytes(0, rootEvidence.bytes)
		}
		recordSummary(runtimeStats, scan.ID(), 8, &tikvutil.ScanDetail{TotalKeys: 8, ProcessedKeys: 8, ProcessedKeysSize: 160})
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

		result := buildReadBillingDemoResult(ctx, rootSort, &ast.SelectStmt{}, nil, nil)
		require.Equal(t, readBillingDemoStatusSuccess, result.status)
		var sortResult readBillingDemoOperatorResult
		for _, operator := range result.operators {
			if operator.site == readBillingDemoSiteTiDB && operator.opClass == readBillingDemoOpClassSort {
				sortResult = operator
				break
			}
		}
		require.Equal(t, readBillingDemoStatusOperatorOK, sortResult.status)
		require.Equal(t, 8.0, readBillingDemoUnitValue(sortResult.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
		require.Equal(t, 80.0, readBillingDemoUnitValue(sortResult.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
		require.Equal(t, 24.0, readBillingDemoUnitValue(sortResult.units, readBillingDemoUnitOrderWork, readBillingDemoInputSideAll))
	})

	t.Run("full builder wires root and pushed topn order work", func(t *testing.T) {
		reqProp := property.NewPhysicalProperty(property.RootTaskType, nil, false, 0, false)
		rootTopN := physicalop.PhysicalTopN{Offset: 1, Count: 3}.Init(ctx, stats, 0, reqProp)
		scan.SetStats(stats)
		copTopN, globalTopN := getPushedDownTopN(rootTopN, scan, kv.TiKV)
		require.NotNil(t, copTopN)
		require.Nil(t, globalTopN)
		require.Zero(t, copTopN.Offset)
		require.Equal(t, uint64(4), copTopN.Count)
		copTopN.SetSchema(schema)
		rootTopN.SetSchema(schema)
		reader.TablePlan = copTopN
		rootTopN.SetChildren(reader)
		flat := FlattenPhysicalPlan(rootTopN, true)
		require.Len(t, flat.Main, 4)

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		for _, rootEvidence := range []struct {
			planID int
			rows   int
			bytes  int64
		}{
			{planID: rootTopN.ID(), rows: 3, bytes: 30},
			{planID: reader.ID(), rows: 4, bytes: 40},
		} {
			basic := runtimeStats.GetBasicRuntimeStats(rootEvidence.planID, true)
			basic.Record(time.Millisecond, rootEvidence.rows)
			basic.RecordBytes(0, rootEvidence.bytes)
		}
		recordSummary(runtimeStats, scan.ID(), 8, nil)
		recordSummary(runtimeStats, copTopN.ID(), 4, &tikvutil.ScanDetail{TotalKeys: 8, ProcessedKeys: 8, ProcessedKeysSize: 160})
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

		result := buildReadBillingDemoResult(ctx, rootTopN, &ast.SelectStmt{}, nil, nil)
		require.Equal(t, readBillingDemoStatusSuccess, result.status)
		orderWorkByClass := make(map[string]float64)
		for _, operator := range result.operators {
			if operator.opClass == readBillingDemoOpClassTopN {
				orderWorkByClass[operator.site+"/"+operator.opClass] = readBillingDemoUnitValue(operator.units, readBillingDemoUnitOrderWork, readBillingDemoInputSideAll)
			}
		}
		require.Equal(t, 8.0, orderWorkByClass[readBillingDemoSiteTiDB+"/"+readBillingDemoOpClassTopN])
		require.Equal(t, 16.0, orderWorkByClass[readBillingDemoSiteTiKV+"/"+readBillingDemoOpClassTopN])
	})

	t.Run("full builder keeps index lookup components isolated", func(t *testing.T) {
		indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		tableScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		indexScan.SetSchema(schema)
		tableScan.SetSchema(schema)
		lookup := (physicalop.PhysicalIndexLookUpReader{IndexPlan: indexScan, TablePlan: tableScan}).Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)
		flat := FlattenPhysicalPlan(lookup, true)
		require.Len(t, flat.Main, 3)

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		rootStats := runtimeStats.GetBasicRuntimeStats(lookup.ID(), true)
		rootStats.Record(time.Millisecond, 2)
		rootStats.RecordBytes(0, 40)
		runtimeStats.RecordCopStats(indexScan.ID(), kv.TiKV, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 50}, tikvutil.TimeDetail{}, nil)
		runtimeStats.RecordCopStats(tableScan.ID(), kv.TiKV, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 150}, tikvutil.TimeDetail{}, nil)
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

		result := buildReadBillingDemoResult(ctx, lookup, &ast.SelectStmt{}, nil, nil)
		require.Equal(t, readBillingDemoStatusSuccess, result.status)
		bytesByID := make(map[string]float64)
		for _, operator := range result.operators {
			if operator.opClass == readBillingDemoOpClassRangeScan {
				bytesByID[operator.id] = readBillingDemoUnitValue(operator.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll)
			}
		}
		require.Equal(t, 50.0, bytesByID[flat.Main[1].ExplainID().String()])
		require.Equal(t, 150.0, bytesByID[flat.Main[2].ExplainID().String()])
	})
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
		stats := runtimeStats.GetBasicRuntimeStats(planID, true)
		stats.Record(time.Millisecond, rows)
		stats.RecordBytes(0, int64(rows*10))
	}
	recordRootRows(join.ID(), 6)
	recordRootRows(left.ID(), 4)
	recordRootRows(right.ID(), 6)

	units, reason, ok := readBillingDemoRootUnits(
		runtimeStats,
		tree,
		0,
		tree[0],
		readBillingDemoOperatorResult{site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassHashJoin, operatorKind: "hashjoin"},
	)
	require.True(t, ok)
	require.Empty(t, reason)
	require.Equal(t, 1.0, readBillingDemoUnitValue(units, readBillingDemoUnitFixedEvents, readBillingDemoInputSideAll))
	require.Equal(t, 4.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideBuild))
	require.Equal(t, 6.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideProbe))
	require.Equal(t, 40.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideBuild))
	require.Equal(t, 60.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideProbe))
	require.Equal(t, readBillingDemoInputSourceRuntimeChunkBytes, readBillingDemoUnitSource(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideBuild))
	require.Equal(t, explainRUWidthSourceRuntimeChunkAvg, readBillingDemoUnitWidthSource(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideProbe))
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

func readBillingDemoUnitWidthSource(units []readBillingDemoUnit, unitName, side string) string {
	for _, unit := range units {
		if unit.unit == unitName && unit.side == side {
			return unit.widthSource
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
