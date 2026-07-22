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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
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

const (
	readBillingDemoWriteKeyWeight  = 0.6
	readBillingDemoWriteByteWeight = 0.00002
)

func buildWriteBillingDemoResultFromDetails(dmlKind string, _ *tikvutil.CommitDetails, ruv2Metrics *execdetails.RUV2Metrics) readBillingDemoResult {
	return readBillingDemoResult{
		status:    readBillingDemoStatusSuccess,
		reason:    readBillingDemoReasonNone,
		operators: buildTiKVWriteBillingDemoOperators(dmlKind, ruv2Metrics, false),
	}
}

func readBillingDemoWriteDiagnosticStatus(dmlKind, reason string) readBillingDemoOperatorResult {
	return readBillingDemoOperatorResult{
		id:           "txn_write@statement",
		site:         readBillingDemoSiteTiKV,
		opClass:      readBillingDemoOpClassKVWrite,
		operatorKind: readBillingDemoOperatorTxnWrite,
		dmlKind:      dmlKind,
		scope:        readBillingDemoScopeTxnPrewritePayload,
		status:       readBillingDemoStatusPartial,
		reason:       reason,
	}
}

type legacyReadBillingDemoWeights struct {
	fixedEvent, row, byte, orderWork float64
	mutationCount, mutationByte      float64
	writeKey, writeByte              float64
	writeRPC, region                 float64
}

func (legacyReadBillingDemoWeights) valid() bool { return true }

func (weights legacyReadBillingDemoWeights) unitWeight(unit string) (float64, bool) {
	switch unit {
	case readBillingDemoUnitFixedEvents:
		return weights.fixedEvent, true
	case readBillingDemoUnitInputRows:
		return weights.row, true
	case readBillingDemoUnitInputBytes:
		return weights.byte, true
	case readBillingDemoUnitOrderWork:
		return weights.orderWork, true
	case readBillingDemoUnitEncodedMutationCount:
		return weights.mutationCount, true
	case readBillingDemoUnitEncodedMutationBytes:
		return weights.mutationByte, true
	case readBillingDemoUnitWriteKeys:
		return weights.writeKey, true
	case readBillingDemoUnitWriteByte:
		return weights.writeByte, true
	case readBillingDemoUnitPrewriteRegionNum:
		return weights.region, true
	case readBillingDemoUnitTiKVWriteRPCCount:
		return weights.writeRPC, true
	default:
		return 0, false
	}
}

func readBillingDemoResolveWeights(site, opClass, version string) (legacyReadBillingDemoWeights, bool) {
	if version != readBillingDemoWeightVersion || opClass == readBillingDemoOpClassPointLookup && site != readBillingDemoSiteTiKV {
		return legacyReadBillingDemoWeights{}, false
	}
	w := legacyReadBillingDemoWeights{
		fixedEvent: 0.1, row: 0.2, byte: 0.3, orderWork: 0.4,
	}
	if opClass == readBillingDemoOpClassTopN {
		w.row = 0
	}
	if opClass == readBillingDemoOpClassKVWrite {
		w.writeKey = readBillingDemoWriteKeyWeight
		w.writeByte = readBillingDemoWriteByteWeight
	}
	return w, true
}

func TestReadBillingDemoV4FormulaContract(t *testing.T) {
	weights := readBillingDemoWeights{
		Version:   "test-v4-calibrated",
		CPUWeight: 2, ScanWeight: 3, NetWeight: 5, ReadRequestWeight: 7, WriteRequestWeight: 17,
		HashTableWeight: 11, JoinWeight: 13, MutationBytesPerCPUUnit: 10, Calibrated: true,
	}
	for _, tc := range []struct {
		unit   string
		value  float64
		weight float64
	}{
		{readBillingDemoUnitCPUWork, 4, 2}, {readBillingDemoUnitScanBytes, 6, 3},
		{readBillingDemoUnitNetBytes, 8, 5}, {readBillingDemoUnitReadRequestCount, 10, 7},
		{readBillingDemoUnitWriteRequestCount, 2, 17},
		{readBillingDemoUnitHashStateRows, 12, 11}, {readBillingDemoUnitJoinOutputRows, 14, 13},
	} {
		weight, ru, ok := readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: tc.unit, value: tc.value}, weights)
		require.True(t, ok)
		require.Equal(t, tc.weight, weight)
		require.Equal(t, tc.value*tc.weight, ru)
	}
	for _, invalid := range []float64{-1, math.NaN(), math.Inf(1)} {
		_, _, ok := readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: readBillingDemoUnitCPUWork, value: invalid}, weights)
		require.False(t, ok)
	}
	for _, invalidWeights := range []readBillingDemoWeights{
		{},
		{MutationBytesPerCPUUnit: 1, Calibrated: true},
		{Version: readBillingDemoWeightVersion, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{Version: "test", CPUWeight: -1, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{Version: "test", CPUWeight: math.NaN(), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{Version: "test", CPUWeight: math.Inf(1), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{Version: "test", ReadRequestWeight: -1, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{Version: "test", WriteRequestWeight: math.NaN(), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{Version: "test", MutationBytesPerCPUUnit: math.NaN(), Calibrated: true},
		{Version: "test", MutationBytesPerCPUUnit: math.Inf(1), Calibrated: true},
	} {
		require.False(t, readBillingDemoWeightsValid(invalidWeights))
	}

	oldWeights := readBillingDemoV4Weights
	readBillingDemoV4Weights = weights
	t.Cleanup(func() { readBillingDemoV4Weights = oldWeights })
	result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone, operators: []readBillingDemoOperatorResult{{
		id: "formula", site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassProjection,
		operatorKind: "projection", status: readBillingDemoStatusOperatorOK,
		units: []readBillingDemoUnit{
			{unit: readBillingDemoUnitCPUWork, value: 4}, {unit: readBillingDemoUnitScanBytes, value: 6},
			{unit: readBillingDemoUnitNetBytes, value: 8}, {unit: readBillingDemoUnitReadRequestCount, value: 10},
			{unit: readBillingDemoUnitWriteRequestCount, value: 2},
			{unit: readBillingDemoUnitHashStateRows, value: 12}, {unit: readBillingDemoUnitJoinOutputRows, value: 14},
		},
	}}}
	rows := explainRUBuildReadBillingRows(result, explainRUComponentSnapshotOK)
	require.True(t, rows[0].hasPreviewRU)
	require.Equal(t, 484.0, rows[0].previewRU)
	require.Contains(t, rows[0].note, "weight_version=test-v4-calibrated")
	require.Equal(t, "test-v4-calibrated", buildReadBillingDemoStatementStats(result).WeightVersion)
	overflowWeights := weights
	overflowWeights.CPUWeight = 1
	readBillingDemoV4Weights = overflowWeights
	overflowResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, operators: []readBillingDemoOperatorResult{{
		status: readBillingDemoStatusOperatorOK, opClass: readBillingDemoOpClassProjection,
		units: []readBillingDemoUnit{{unit: readBillingDemoUnitCPUWork, value: math.MaxFloat64}, {unit: readBillingDemoUnitCPUWork, value: math.MaxFloat64}},
	}}}
	overflowRows := explainRUBuildReadBillingRows(overflowResult, explainRUComponentSnapshotOK)
	require.False(t, overflowRows[0].hasPreviewRU)
	readBillingDemoV4Weights = weights

	readBillingDemoV4Weights = readBillingDemoWeights{}
	rows = explainRUBuildReadBillingRows(result, explainRUComponentSnapshotOK)
	require.False(t, rows[0].hasPreviewRU)
	require.Contains(t, rows[0].note, readBillingDemoReasonUncalibratedWeights)
	stats := buildReadBillingDemoStatementStats(result)
	require.Equal(t, "v4", stats.ModelVersion)
	require.Equal(t, "v3-resource-formula-uncalibrated", stats.WeightVersion)
	require.Equal(t, stmtsummary.ReadBillingDemoBaseUnitSummary{}, stats.Totals)

	t.Run("reader transport is emitted once and fails closed", func(t *testing.T) {
		ctx := mock.NewContext()
		reader := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
		scan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		flat := &FlatPhysicalPlan{Main: FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
			{Origin: scan, ChildrenEndIdx: 1, StoreType: kv.TiKV},
		}}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		basic := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
		basic.Record(time.Millisecond, 0)
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddResourceManagerReadCnt(3)
		metrics.AddTiKVCoprocessorResponseBytes(128)
		op, present := readBillingDemoReaderTransport(flat, runtimeStats, metrics, false)
		require.True(t, present)
		require.Equal(t, readBillingDemoStatusOperatorOK, op.status)
		require.Equal(t, 128.0, readBillingDemoUnitValue(op.units, readBillingDemoUnitNetBytes, readBillingDemoInputSideAll))
		require.Equal(t, 3.0, readBillingDemoUnitValue(op.units, readBillingDemoUnitReadRequestCount, readBillingDemoInputSideAll))

		runtimeStats.RecordExpectedCopTasks([]int{scan.ID()})
		op, _ = readBillingDemoReaderTransport(flat, runtimeStats, execdetails.NewRUV2Metrics(), false)
		require.Equal(t, readBillingDemoReasonMissingReaderTransport, op.reason)
		op, _ = readBillingDemoReaderTransport(flat, runtimeStats, metrics, true)
		require.Equal(t, readBillingDemoReasonAmbiguousReaderTransport, op.reason)
	})

	t.Run("point lookup transport is rpc only and emitted once", func(t *testing.T) {
		ctx := mock.NewContext()
		stats := &property.StatsInfo{RowCount: 1}
		tblInfo := &model.TableInfo{}
		pointPlan := physicalop.PointGetPlan{TblInfo: tblInfo}
		pointPlan.SetSchema(expression.NewSchema())
		point := pointPlan.Init(ctx, stats, 0)
		batch := (&physicalop.BatchPointGetPlan{TblInfo: tblInfo}).Init(ctx, stats, expression.NewSchema(), nil, 0)
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddResourceManagerReadCnt(3)

		testCases := []struct {
			name string
			flat *FlatPhysicalPlan
			kind string
		}{
			{
				name: "point get",
				flat: &FlatPhysicalPlan{Main: FlatPlanTree{{Origin: point, IsRoot: true, StoreType: kv.TiDB}}},
				kind: "point_get",
			},
			{
				name: "batch point get",
				flat: &FlatPhysicalPlan{Main: FlatPlanTree{{Origin: batch, IsRoot: true, StoreType: kv.TiDB}}},
				kind: "batch_point_get",
			},
			{
				name: "mixed point lookup",
				flat: &FlatPhysicalPlan{
					Main:             FlatPlanTree{{Origin: point, IsRoot: true, StoreType: kv.TiDB}},
					ScalarSubQueries: []FlatPlanTree{{{Origin: batch, IsRoot: true, StoreType: kv.TiDB}}},
				},
				kind: "mixed_point_lookup",
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				op, present := readBillingDemoPointLookupTransport(tc.flat, metrics, false)
				require.True(t, present)
				require.Equal(t, "point_lookup@statement", op.id)
				require.Equal(t, readBillingDemoStatusOperatorOK, op.status)
				require.Equal(t, readBillingDemoSiteTiKV, op.site)
				require.Equal(t, readBillingDemoOpClassPointLookup, op.opClass)
				require.Equal(t, tc.kind, op.operatorKind)
				require.Len(t, op.units, 1)
				require.Equal(t, 3.0, readBillingDemoUnitValue(op.units, readBillingDemoUnitReadRequestCount, readBillingDemoInputSideAll))
				require.True(t, readBillingDemoOperatorBillable(op))
			})
		}

		zeroOp, present := readBillingDemoPointLookupTransport(testCases[0].flat, execdetails.NewRUV2Metrics(), false)
		require.True(t, present)
		require.Equal(t, readBillingDemoStatusOperatorOK, zeroOp.status)
		require.Zero(t, readBillingDemoUnitValue(zeroOp.units, readBillingDemoUnitReadRequestCount, readBillingDemoInputSideAll))

		missingOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, nil, false)
		require.Equal(t, readBillingDemoReasonMissingReaderTransport, missingOp.reason)
		bypassedMetrics := execdetails.NewRUV2Metrics()
		bypassedMetrics.SetBypass(true)
		bypassedOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, bypassedMetrics, false)
		require.Equal(t, readBillingDemoReasonMissingReaderTransport, bypassedOp.reason)
		dmlOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, metrics, true)
		require.Equal(t, readBillingDemoReasonAmbiguousReaderTransport, dmlOp.reason)

		point.Lock = true
		lockOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, metrics, false)
		require.Equal(t, readBillingDemoReasonAmbiguousReaderTransport, lockOp.reason)
		point.Lock = false
		reader := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
		mixedReaderFlat := &FlatPhysicalPlan{
			Main:             testCases[0].flat.Main,
			ScalarSubQueries: []FlatPlanTree{{{Origin: reader, IsRoot: true, StoreType: kv.TiDB}}},
		}
		mixedReaderOp, _ := readBillingDemoPointLookupTransport(mixedReaderFlat, metrics, false)
		require.Equal(t, readBillingDemoReasonAmbiguousReaderTransport, mixedReaderOp.reason)

		physicalOp, supported, reason := readBillingDemoClassifyOperator(testCases[0].flat.Main[0])
		physicalOp.id = point.ExplainID().String()
		require.True(t, supported)
		require.Empty(t, reason)
		require.False(t, readBillingDemoOperatorBillable(physicalOp))
	})
}

func TestReadBillingDemoV4ExpressionCountsAndOrdering(t *testing.T) {
	ctx := mock.NewContext()
	stats := &property.StatsInfo{RowCount: 1}
	col := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	selection := physicalop.PhysicalSelection{Conditions: []expression.Expression{col, col}}.Init(ctx, stats, 0)
	projection := physicalop.PhysicalProjection{Exprs: []expression.Expression{col, col, col}}.Init(ctx, stats, 0)
	hashAgg := (&physicalop.BasePhysicalAgg{GroupByItems: []expression.Expression{col}, AggFuncs: []*aggregation.AggFuncDesc{nil, nil}}).InitForHash(ctx, stats, 0, expression.NewSchema(col))
	window := physicalop.PhysicalWindow{WindowFuncDescs: []*aggregation.WindowFuncDesc{nil}, PartitionBy: []property.SortItem{{Col: col}}, OrderBy: []property.SortItem{{Col: col}}, Frame: &logicalop.WindowFrame{Start: &logicalop.FrameBound{CalcFuncs: []expression.Expression{col}}, End: &logicalop.FrameBound{CalcFuncs: []expression.Expression{col, col}}}}.Init(ctx, stats, 0)
	for _, tc := range []struct {
		plan base.Plan
		want int64
	}{{selection, 2}, {projection, 3}, {hashAgg, 3}, {window, 6}} {
		got, ok := readBillingDemoExpressionCount(tc.plan)
		require.True(t, ok)
		require.Equal(t, tc.want, got)
	}

	baseJoin := physicalop.BasePhysicalJoin{
		LeftConditions:  expression.CNFExprs{col},
		RightConditions: expression.CNFExprs{col},
		OtherConditions: expression.CNFExprs{col},
		LeftJoinKeys:    []*expression.Column{col, col},
		RightJoinKeys:   []*expression.Column{col, col},
		OuterJoinKeys:   []*expression.Column{col, col},
		InnerJoinKeys:   []*expression.Column{col, col},
	}
	compareFilters := &physicalop.ColWithCmpFuncManager{OpType: []string{"gt", "lt"}}
	joins := []struct {
		name string
		plan base.Plan
		want int64
	}{
		{name: "hash join", plan: &physicalop.PhysicalHashJoin{BasePhysicalJoin: baseJoin, EqualConditions: []*expression.ScalarFunction{{}, {}}, NAEqualConditions: []*expression.ScalarFunction{{}}}, want: 6},
		{name: "merge join", plan: &physicalop.PhysicalMergeJoin{BasePhysicalJoin: baseJoin, CompareFuncs: []expression.CompareFunc{nil, nil}}, want: 5},
		{name: "index join", plan: &physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin, CompareFilters: compareFilters}, want: 7},
		{name: "index hash join", plan: &physicalop.PhysicalIndexHashJoin{PhysicalIndexJoin: physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin, OuterHashKeys: []*expression.Column{col, col, col}, InnerHashKeys: []*expression.Column{col, col, col}, CompareFilters: compareFilters}}, want: 8},
		{name: "index merge join", plan: &physicalop.PhysicalIndexMergeJoin{PhysicalIndexJoin: physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin, CompareFilters: compareFilters}, CompareFuncs: []expression.CompareFunc{nil, nil}, OuterCompareFuncs: []expression.CompareFunc{nil}, NeedOuterSort: true}, want: 8},
	}
	for _, tc := range joins {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := readBillingDemoExpressionCount(tc.plan)
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}
	invalidHashJoin := &physicalop.PhysicalHashJoin{BasePhysicalJoin: baseJoin}
	invalidHashJoin.RightJoinKeys = invalidHashJoin.RightJoinKeys[:1]
	_, ok := readBillingDemoExpressionCount(invalidHashJoin)
	require.False(t, ok)
	invalidIndexJoin := &physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin}
	invalidIndexJoin.InnerJoinKeys = invalidIndexJoin.InnerJoinKeys[:1]
	_, ok = readBillingDemoExpressionCount(invalidIndexJoin)
	require.False(t, ok)
	invalidIndexHashJoin := &physicalop.PhysicalIndexHashJoin{PhysicalIndexJoin: physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin, OuterHashKeys: []*expression.Column{col}}}
	_, ok = readBillingDemoExpressionCount(invalidIndexHashJoin)
	require.False(t, ok)
	invalidIndexMergeJoin := &physicalop.PhysicalIndexMergeJoin{PhysicalIndexJoin: physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin}, OuterCompareFuncs: []expression.CompareFunc{nil}}
	_, ok = readBillingDemoExpressionCount(invalidIndexMergeJoin)
	require.False(t, ok)

	topN := physicalop.PhysicalTopN{Offset: 3, Count: 5, ByItems: []*plannerutil.ByItems{{Expr: col}, {Expr: col}}}.Init(ctx, stats, 0)
	unit, ok := readBillingDemoOrderingWorkUnit(&FlatOperator{Origin: topN, IsRoot: true}, readBillingDemoOpClassTopN, 4)
	require.True(t, ok)
	require.Equal(t, 8.0, unit.value)
	for _, tc := range []struct {
		name          string
		rows          int64
		offset, count uint64
		want          float64
	}{
		{name: "zero rows", rows: 0, count: 1, want: 0},
		{name: "one row", rows: 1, count: 1, want: 1},
		{name: "bound saturates at input rows", rows: 4, offset: 3, count: 5, want: 8},
		{name: "huge bound saturates at one input row", rows: 1, offset: math.MaxUint64 - 1, count: 1, want: 1},
		{name: "zero count ignores offset", rows: 4, offset: math.MaxUint64, count: 0, want: 0},
	} {
		topN.Offset, topN.Count = tc.offset, tc.count
		unit, ok = readBillingDemoOrderingWorkUnit(&FlatOperator{Origin: topN, IsRoot: true}, readBillingDemoOpClassTopN, tc.rows)
		require.True(t, ok, tc.name)
		require.Equal(t, tc.want, unit.value, tc.name)
	}
	topN.Offset, topN.Count = math.MaxUint64, 1
	_, ok = readBillingDemoOrderingWorkUnit(&FlatOperator{Origin: topN, IsRoot: true}, readBillingDemoOpClassTopN, 1)
	require.False(t, ok)
	require.Equal(t, readBillingDemoReasonInvalidTopNBound, readBillingDemoOrderingFailureReason(&FlatOperator{Origin: topN, IsRoot: true}, readBillingDemoOpClassTopN))
	topN.Offset, topN.Count = 0, 1
	topN.ByItems = []*plannerutil.ByItems{{Expr: &expression.ScalarFunction{}}}
	require.False(t, readBillingDemoOrderingMaterialized(&FlatOperator{Origin: topN, IsRoot: false}, nil))
	sort := physicalop.PhysicalSort{ByItems: []*plannerutil.ByItems{{Expr: col}}}.Init(ctx, stats, 0)
	unit, ok = readBillingDemoOrderingWorkUnit(&FlatOperator{Origin: sort, IsRoot: true}, readBillingDemoOpClassSort, 3)
	require.True(t, ok)
	require.InDelta(t, 3*math.Log2(3), unit.value, 1e-12)

	formulaWeights := readBillingDemoWeights{
		Version:   "test-v4-calibrated",
		CPUWeight: 2, ScanWeight: 3, NetWeight: 5, ReadRequestWeight: 7, WriteRequestWeight: 17,
		HashTableWeight: 11, JoinWeight: 13, MutationBytesPerCPUUnit: 10, Calibrated: true,
	}
	weightedTotal := func(t *testing.T, units []readBillingDemoUnit) float64 {
		t.Helper()
		var total float64
		for _, unit := range units {
			_, ru, ok := readBillingDemoUnitPreviewRU(unit, formulaWeights)
			if _, semantic := readBillingDemoUnitWeight(formulaWeights, unit.unit); !semantic {
				continue
			}
			require.True(t, ok, "unit=%+v", unit)
			total += ru
		}
		return total
	}
	recordRoot := func(runtimeStats *execdetails.RuntimeStatsColl, planID int, rows int) {
		rootStats := runtimeStats.GetBasicRuntimeStats(planID, true)
		rootStats.Record(time.Millisecond, rows)
		rootStats.RecordBytes(0, int64(rows*8))
	}
	schema := expression.NewSchema(col)

	t.Run("root unary formulas use exact semantic terms", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			opClass   string
			buildPlan func() base.Plan
			wantRU    float64
		}{
			{name: "selection", opClass: readBillingDemoOpClassFilter, buildPlan: func() base.Plan {
				return physicalop.PhysicalSelection{Conditions: []expression.Expression{col, col}}.Init(ctx, stats, 0)
			}, wantRU: 16},
			{name: "projection", opClass: readBillingDemoOpClassProjection, buildPlan: func() base.Plan {
				return physicalop.PhysicalProjection{Exprs: []expression.Expression{col, col, col}}.Init(ctx, stats, 0)
			}, wantRU: 24},
			{name: "stream agg", opClass: readBillingDemoOpClassStreamAgg, buildPlan: func() base.Plan {
				return (&physicalop.BasePhysicalAgg{GroupByItems: []expression.Expression{col}, AggFuncs: []*aggregation.AggFuncDesc{nil, nil}}).InitForStream(ctx, stats, 0, schema)
			}, wantRU: 24},
			{name: "hash agg", opClass: readBillingDemoOpClassHashAgg, buildPlan: func() base.Plan {
				return (&physicalop.BasePhysicalAgg{GroupByItems: []expression.Expression{col}, AggFuncs: []*aggregation.AggFuncDesc{nil, nil}}).InitForHash(ctx, stats, 0, schema)
			}, wantRU: 46},
			{name: "limit", opClass: readBillingDemoOpClassLimit, buildPlan: func() base.Plan {
				return physicalop.PhysicalLimit{}.Init(ctx, stats, 0)
			}, wantRU: 8},
			{name: "union scan", opClass: readBillingDemoOpClassOverlayReader, buildPlan: func() base.Plan {
				return physicalop.PhysicalUnionScan{}.Init(ctx, stats, 0)
			}, wantRU: 8},
			{name: "window", opClass: readBillingDemoOpClassWindow, buildPlan: func() base.Plan {
				return physicalop.PhysicalWindow{WindowFuncDescs: []*aggregation.WindowFuncDesc{nil}, PartitionBy: []property.SortItem{{Col: col}}, OrderBy: []property.SortItem{{Col: col}}, Frame: &logicalop.WindowFrame{Start: &logicalop.FrameBound{CalcFuncs: []expression.Expression{col}}, End: &logicalop.FrameBound{CalcFuncs: []expression.Expression{col, col}}}}.Init(ctx, stats, 0)
			}, wantRU: 48},
			{name: "sort", opClass: readBillingDemoOpClassSort, buildPlan: func() base.Plan {
				return physicalop.PhysicalSort{ByItems: []*plannerutil.ByItems{{Expr: col}, {Expr: col}}}.Init(ctx, stats, 0)
			}, wantRU: 16},
			{name: "topn with offset", opClass: readBillingDemoOpClassTopN, buildPlan: func() base.Plan {
				return physicalop.PhysicalTopN{Offset: 3, Count: 5, ByItems: []*plannerutil.ByItems{{Expr: col}, {Expr: col}}}.Init(ctx, stats, 0)
			}, wantRU: 16},
		} {
			t.Run(tc.name, func(t *testing.T) {
				plan := tc.buildPlan()
				child := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
				child.SetSchema(schema)
				tree := FlatPlanTree{
					{Origin: plan, ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
					{Origin: child, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
				}
				runtimeStats := execdetails.NewRuntimeStatsColl(nil)
				recordRoot(runtimeStats, plan.ID(), 2)
				recordRoot(runtimeStats, child.ID(), 4)
				operator := readBillingDemoOperatorResult{id: plan.ExplainID().String(), opClass: tc.opClass}
				require.True(t, readBillingDemoOperatorBillable(operator))
				units, reason, ok := readBillingDemoRootUnits(runtimeStats, tree, 0, tree[0], operator)
				require.True(t, ok, reason)
				require.Empty(t, reason)
				require.Equal(t, tc.wantRU, weightedTotal(t, units))
				if tc.opClass == readBillingDemoOpClassOverlayReader {
					require.Equal(t, 4.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
					require.Equal(t, 4.0, readBillingDemoUnitValue(units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
					require.Equal(t, readBillingDemoInputSourceRuntimeChildActRows, readBillingDemoUnitSource(units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
					require.Equal(t, -1.0, readBillingDemoUnitValue(units, readBillingDemoUnitExpressionCount, readBillingDemoInputSideAll))
				}
			})
		}
	})

	t.Run("ordering rejects columns outside the child schema", func(t *testing.T) {
		plan := physicalop.PhysicalSort{ByItems: []*plannerutil.ByItems{{Expr: col}}}.Init(ctx, stats, 0)
		child := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
		child.SetSchema(expression.NewSchema())
		tree := FlatPlanTree{
			{Origin: plan, ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
			{Origin: child, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordRoot(runtimeStats, plan.ID(), 2)
		recordRoot(runtimeStats, child.ID(), 4)
		_, reason, ok := readBillingDemoRootUnits(runtimeStats, tree, 0, tree[0], readBillingDemoOperatorResult{opClass: readBillingDemoOpClassSort})
		require.False(t, ok)
		require.Equal(t, readBillingDemoReasonMissingOrderingProjection, reason)
	})

	t.Run("root join formulas use both inputs and subtype expressions", func(t *testing.T) {
		joinBase := physicalop.BasePhysicalJoin{
			OtherConditions: expression.CNFExprs{col},
			LeftJoinKeys:    []*expression.Column{col}, RightJoinKeys: []*expression.Column{col},
			OuterJoinKeys: []*expression.Column{col}, InnerJoinKeys: []*expression.Column{col},
		}
		for _, tc := range []struct {
			name      string
			opClass   string
			buildPlan func() base.Plan
			hashRows  int64
			wantRU    float64
		}{
			{name: "merge join", opClass: readBillingDemoOpClassMergeJoin, buildPlan: func() base.Plan {
				return physicalop.PhysicalMergeJoin{BasePhysicalJoin: joinBase, CompareFuncs: []expression.CompareFunc{nil}}.Init(ctx, stats, 0)
			}, wantRU: 66},
			{name: "hash join", opClass: readBillingDemoOpClassHashJoin, buildPlan: func() base.Plan {
				return physicalop.PhysicalHashJoin{BasePhysicalJoin: joinBase, EqualConditions: []*expression.ScalarFunction{{}}}.Init(ctx, stats, 0)
			}, hashRows: 3, wantRU: 99},
			{name: "index join", opClass: readBillingDemoOpClassLookupJoin, buildPlan: func() base.Plan {
				return physicalop.PhysicalIndexJoin{BasePhysicalJoin: joinBase}.Init(ctx, stats, 0)
			}, wantRU: 66},
			{name: "index hash join", opClass: readBillingDemoOpClassLookupJoin, buildPlan: func() base.Plan {
				indexJoin := physicalop.PhysicalIndexJoin{BasePhysicalJoin: joinBase, OuterHashKeys: []*expression.Column{col}, InnerHashKeys: []*expression.Column{col}}.Init(ctx, stats, 0)
				return physicalop.PhysicalIndexHashJoin{PhysicalIndexJoin: *indexJoin}.Init(ctx)
			}, wantRU: 66},
			{name: "index merge join", opClass: readBillingDemoOpClassLookupJoin, buildPlan: func() base.Plan {
				indexJoin := physicalop.PhysicalIndexJoin{BasePhysicalJoin: joinBase}.Init(ctx, stats, 0)
				return physicalop.PhysicalIndexMergeJoin{PhysicalIndexJoin: *indexJoin, CompareFuncs: []expression.CompareFunc{nil}}.Init(ctx)
			}, wantRU: 66},
		} {
			t.Run(tc.name, func(t *testing.T) {
				plan := tc.buildPlan()
				left := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
				right := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
				left.SetSchema(schema)
				right.SetSchema(schema)
				tree := FlatPlanTree{
					{Origin: plan, ChildrenIdx: []int{1, 2}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
					{Origin: left, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB, Label: BuildSide},
					{Origin: right, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB, Label: ProbeSide},
				}
				runtimeStats := execdetails.NewRuntimeStatsColl(nil)
				recordRoot(runtimeStats, plan.ID(), 2)
				recordRoot(runtimeStats, left.ID(), 4)
				recordRoot(runtimeStats, right.ID(), 6)
				if tc.opClass == readBillingDemoOpClassHashJoin {
					runtimeStats.RegisterStats(plan.ID(), &readBillingDemoHashStatsForTest{rows: tc.hashRows})
				}
				units, reason, ok := readBillingDemoRootUnits(runtimeStats, tree, 0, tree[0], readBillingDemoOperatorResult{opClass: tc.opClass})
				require.True(t, ok, reason)
				require.Empty(t, reason)
				require.Equal(t, tc.wantRU, weightedTotal(t, units))
			})
		}
	})

	t.Run("cop selection uses exact child rows without logical byte width", func(t *testing.T) {
		reader := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
		scan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		tree := FlatPlanTree{
			{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
			{Origin: selection, ChildrenIdx: []int{2}, ChildrenEndIdx: 2, StoreType: kv.TiKV},
			{Origin: scan, ChildrenEndIdx: 2, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		one := uint64(1)
		summary := func(rows uint64) *tipb.ExecutorExecutionSummary {
			return &tipb.ExecutorExecutionSummary{
				TimeProcessedNs: &one,
				NumProducedRows: &rows,
				NumIterations:   &one,
			}
		}
		runtimeStats.RecordExpectedCopTasks([]int{scan.ID(), selection.ID()})
		runtimeStats.RecordOneCopTask(scan.ID(), kv.TiKV, summary(4))
		runtimeStats.RecordCopStats(selection.ID(), kv.TiKV, &tikvutil.ScanDetail{TotalKeys: 4, ProcessedKeys: 4, ProcessedKeysSize: 40}, tikvutil.TimeDetail{}, summary(2))
		estimator := newReadBillingDemoCopEstimator(tree, runtimeStats)
		outcome := readBillingDemoCopUnits(estimator, 1, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassFilter, operatorKind: "selection"})
		require.True(t, outcome.success, "%+v", outcome.failure)
		require.Equal(t, 2.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitExpressionCount, readBillingDemoInputSideAll))
		require.Equal(t, 8.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
		require.Equal(t, -1.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))

		scanOutcome := readBillingDemoCopUnits(estimator, 2, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.True(t, scanOutcome.success, "%+v", scanOutcome.failure)
		require.Equal(t, 40.0, readBillingDemoUnitValue(scanOutcome.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))

		selection.SetChildren(scan)
		reader.TablePlan = selection
		recordRoot(runtimeStats, reader.ID(), 2)
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddResourceManagerReadCnt(1)
		metrics.AddTiKVCoprocessorResponseBytes(40)
		result := buildReadBillingDemoResult(ctx, reader, &ast.SelectStmt{}, nil, metrics)
		require.Equal(t, readBillingDemoStatusSuccess, result.status)
		seenScanBytes := false
		seenSelectionCPU := false
		for _, sample := range buildReadBillingDemoStatementStats(result).BaseUnits {
			switch {
			case sample.Site == readBillingDemoSiteTiKV && sample.OpClass == readBillingDemoOpClassRangeScan && sample.Unit == readBillingDemoUnitScanBytes:
				seenScanBytes = true
				require.Equal(t, 40.0, sample.Value)
			case sample.Site == readBillingDemoSiteTiKV && sample.OpClass == readBillingDemoOpClassFilter && sample.Unit == readBillingDemoUnitCPUWork:
				seenSelectionCPU = true
				require.Equal(t, 8.0, sample.Value)
			}
		}
		require.True(t, seenScanBytes)
		require.True(t, seenSelectionCPU)

		runtimeStats.RecordExpectedCopTasks([]int{scan.ID(), selection.ID()})
		failedResult := buildReadBillingDemoResult(ctx, reader, &ast.SelectStmt{}, nil, metrics)
		require.Equal(t, readBillingDemoStatusUnknownInput, failedResult.status)
		require.Equal(t, readBillingDemoReasonIncompleteCopRuntimeRows, failedResult.reason)
		require.Empty(t, buildReadBillingDemoStatementStats(failedResult).BaseUnits)
	})

	t.Run("cop scan detail provenance stays fail closed", func(t *testing.T) {
		buildEstimator := func(detail *tikvutil.ScanDetail, responses, scanSummaries, detailRecords int, holderSummaries bool) (*readBillingDemoCopEstimator, int) {
			localReader := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
			localSelection := physicalop.PhysicalSelection{Conditions: []expression.Expression{col}}.Init(ctx, stats, 0)
			localScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			tree := FlatPlanTree{
				{Origin: localReader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
				{Origin: localSelection, ChildrenIdx: []int{2}, ChildrenEndIdx: 2, StoreType: kv.TiKV},
				{Origin: localScan, ChildrenEndIdx: 2, StoreType: kv.TiKV},
			}
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			one := uint64(1)
			zero := uint64(0)
			summary := &tipb.ExecutorExecutionSummary{
				TimeProcessedNs: &one,
				NumProducedRows: &zero,
				NumIterations:   &one,
			}
			for range responses {
				runtimeStats.RecordExpectedCopTasks([]int{localScan.ID(), localSelection.ID()})
			}
			for range scanSummaries {
				runtimeStats.RecordOneCopTask(localScan.ID(), kv.TiKV, summary)
			}
			for range detailRecords {
				var holderSummary *tipb.ExecutorExecutionSummary
				if holderSummaries {
					holderSummary = summary
				}
				runtimeStats.RecordCopStats(localSelection.ID(), kv.TiKV, detail, tikvutil.TimeDetail{}, holderSummary)
			}
			return newReadBillingDemoCopEstimator(tree, runtimeStats), 2
		}

		estimator, scanIdx := buildEstimator(&tikvutil.ScanDetail{}, 1, 1, 1, true)
		outcome := readBillingDemoCopUnits(estimator, scanIdx, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.True(t, outcome.success, "%+v", outcome.failure)
		require.Zero(t, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))

		estimator, scanIdx = buildEstimator(&tikvutil.ScanDetail{TotalKeys: 4, ProcessedKeys: 4, ProcessedKeysSize: 40}, 2, 2, 2, true)
		outcome = readBillingDemoCopUnits(estimator, scanIdx, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.True(t, outcome.success, "%+v", outcome.failure)
		require.Equal(t, 80.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))

		estimator, scanIdx = buildEstimator(&tikvutil.ScanDetail{TotalKeys: 4, ProcessedKeys: 4, ProcessedKeysSize: 40}, 1, 1, 1, false)
		outcome = readBillingDemoCopUnits(estimator, scanIdx, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.True(t, outcome.success, "%+v", outcome.failure)

		estimator, scanIdx = buildEstimator(&tikvutil.ScanDetail{TotalKeys: 4, ProcessedKeys: 4}, 1, 1, 1, true)
		outcome = readBillingDemoCopUnits(estimator, scanIdx, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoReasonMissingScanWidthEvidence, outcome.failure.reason)

		estimator, scanIdx = buildEstimator(&tikvutil.ScanDetail{TotalKeys: 4, ProcessedKeys: 4, ProcessedKeysSize: 40}, 2, 1, 2, true)
		outcome = readBillingDemoCopUnits(estimator, scanIdx, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoReasonIncompleteCopRuntimeRows, outcome.failure.reason)

		estimator, scanIdx = buildEstimator(&tikvutil.ScanDetail{TotalKeys: 4, ProcessedKeys: 4, ProcessedKeysSize: 40}, 2, 2, 1, true)
		outcome = readBillingDemoCopUnits(estimator, scanIdx, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoReasonIncompleteCopRuntimeRows, outcome.failure.reason)

		estimator, scanIdx = buildEstimator(nil, 1, 1, 0, false)
		outcome = readBillingDemoCopUnits(estimator, scanIdx, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoReasonMissingScanWidthEvidence, outcome.failure.reason)

		localReader := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
		localSelection := physicalop.PhysicalSelection{Conditions: []expression.Expression{col}}.Init(ctx, stats, 0)
		localScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		tree := FlatPlanTree{
			{Origin: localReader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
			{Origin: localSelection, ChildrenIdx: []int{2}, ChildrenEndIdx: 2, StoreType: kv.TiKV},
			{Origin: localScan, ChildrenEndIdx: 2, StoreType: kv.TiKV},
		}
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		one := uint64(1)
		zero := uint64(0)
		summary := &tipb.ExecutorExecutionSummary{TimeProcessedNs: &one, NumProducedRows: &zero, NumIterations: &one}
		detail := &tikvutil.ScanDetail{TotalKeys: 4, ProcessedKeys: 4, ProcessedKeysSize: 40}
		runtimeStats.RecordExpectedCopTasks([]int{localScan.ID(), localSelection.ID()})
		runtimeStats.RecordCopStats(localScan.ID(), kv.TiKV, detail, tikvutil.TimeDetail{}, summary)
		runtimeStats.RecordCopStats(localSelection.ID(), kv.TiKV, detail, tikvutil.TimeDetail{}, summary)
		outcome = readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 2, readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "indexscan"})
		require.False(t, outcome.success)
		require.Equal(t, readBillingDemoReasonAmbiguousCopScanWidth, outcome.failure.reason)
	})
}

func TestReadBillingDemoV4WriteRequests(t *testing.T) {
	oldWeights := readBillingDemoV4Weights
	readBillingDemoV4Weights = readBillingDemoWeights{
		Version: "test-v4-calibrated", CPUWeight: 2, MutationBytesPerCPUUnit: 10, Calibrated: true,
	}
	t.Cleanup(func() { readBillingDemoV4Weights = oldWeights })
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder = &stmtctx.PreviewKVMutationRecorder{}
	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.RecordSet(5, 7)
	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.RecordDelete(3)
	result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
	appendReadBillingDemoMutation(&result, ctx, "update")
	mutation := result.operators[0]
	require.Equal(t, readBillingDemoOpClassKVMutation, mutation.opClass)
	require.Equal(t, readBillingDemoOperatorMemDBMutation, mutation.operatorKind)
	require.Equal(t, 3.5, readBillingDemoUnitValue(mutation.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
	require.Equal(t, 2.0, readBillingDemoUnitValue(mutation.units, readBillingDemoUnitEncodedMutationCount, readBillingDemoInputSideAll))
	require.Equal(t, 15.0, readBillingDemoUnitValue(mutation.units, readBillingDemoUnitEncodedMutationBytes, readBillingDemoInputSideAll))
	require.Equal(t, 1.0, readBillingDemoUnitValue(mutation.units, readBillingDemoUnitSetCount, readBillingDemoInputSideAll))
	require.Equal(t, 1.0, readBillingDemoUnitValue(mutation.units, readBillingDemoUnitDeleteCount, readBillingDemoInputSideAll))
	require.Equal(t, 8.0, readBillingDemoUnitValue(mutation.units, readBillingDemoUnitKeyBytes, readBillingDemoInputSideAll))
	require.Equal(t, 7.0, readBillingDemoUnitValue(mutation.units, readBillingDemoUnitValueBytes, readBillingDemoInputSideAll))

	rows := explainRUBuildReadBillingRows(result, explainRUComponentSnapshotOK)
	require.True(t, rows[0].hasPreviewRU)
	require.Equal(t, 7.0, rows[0].previewRU)
	var mutationCPUWorkRows int
	for _, row := range rows {
		if row.operatorClass == "tidb/kv_mutation" && row.component == readBillingDemoOperatorMemDBMutation && row.unit == readBillingDemoUnitCPUWork {
			mutationCPUWorkRows++
			require.Equal(t, 3.5, row.workRows)
			require.Equal(t, readBillingDemoInputSourceStmtMemDBMutation, row.source)
		}
	}
	require.Equal(t, 1, mutationCPUWorkRows)

	stats := buildReadBillingDemoStatementStats(result)
	var mutationCPUWorkSamples int
	for _, sample := range stats.BaseUnits {
		if sample.Unit == readBillingDemoUnitCPUWork {
			mutationCPUWorkSamples++
			require.Equal(t, readBillingDemoSiteTiDB, sample.Site)
			require.Equal(t, readBillingDemoOpClassKVMutation, sample.OpClass)
			require.Equal(t, readBillingDemoOperatorMemDBMutation, sample.OperatorKind)
			require.Equal(t, readBillingDemoInputSourceStmtMemDBMutation, sample.InputSource)
			require.Equal(t, readBillingDemoInputSideAll, sample.InputSide)
			require.Equal(t, 3.5, sample.Value)
		}
	}
	require.Equal(t, 1, mutationCPUWorkSamples)

	readBillingDemoV4Weights = readBillingDemoWeights{MutationBytesPerCPUUnit: 10}
	uncalibratedResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
	appendReadBillingDemoMutation(&uncalibratedResult, ctx, "update")
	require.Len(t, uncalibratedResult.operators[0].units, 6)
	require.Equal(t, -1.0, readBillingDemoUnitValue(uncalibratedResult.operators[0].units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
	require.Equal(t, 2.0, readBillingDemoUnitValue(uncalibratedResult.operators[0].units, readBillingDemoUnitEncodedMutationCount, readBillingDemoInputSideAll))
	require.Equal(t, 15.0, readBillingDemoUnitValue(uncalibratedResult.operators[0].units, readBillingDemoUnitEncodedMutationBytes, readBillingDemoInputSideAll))
	require.Len(t, uncalibratedResult.operators, 2)
	require.Equal(t, readBillingDemoReasonUncalibratedMutation, uncalibratedResult.operators[1].reason)

	dmlMetrics := execdetails.NewRUV2Metrics()
	dmlMetrics.AddResourceManagerWriteCnt(7)
	dml := buildTiKVWriteBillingDemoOperators("update", dmlMetrics, false)
	require.Equal(t, 7.0, readBillingDemoUnitValue(dml[0].units, readBillingDemoUnitWriteRequestCount, readBillingDemoInputSideAll))
	commitMetrics := execdetails.NewRUV2Metrics()
	commitMetrics.AddResourceManagerWriteCnt(2)
	commit := buildTiKVWriteBillingDemoOperators("", commitMetrics, false)
	require.Equal(t, 2.0, readBillingDemoUnitValue(commit[0].units, readBillingDemoUnitWriteRequestCount, readBillingDemoInputSideAll))
	require.Empty(t, commit[0].dmlKind)

	pipelinedDML := buildTiKVWriteBillingDemoOperators("update", dmlMetrics, true)
	require.Len(t, pipelinedDML, 1)
	require.Equal(t, readBillingDemoStatusPartial, pipelinedDML[0].status)
	require.Equal(t, readBillingDemoReasonPipelinedWritePartial, pipelinedDML[0].reason)
	require.Empty(t, pipelinedDML[0].units)

	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.MarkPipelined()
	pipelinedCommit := buildTxnCommitBillingDemoResult(ctx, commitMetrics, nil)
	require.Len(t, pipelinedCommit.operators, 1)
	require.Equal(t, readBillingDemoStatusPartial, pipelinedCommit.operators[0].status)
	require.Equal(t, readBillingDemoReasonPipelinedWritePartial, pipelinedCommit.operators[0].reason)
	require.Empty(t, pipelinedCommit.operators[0].units)
}

func TestExplainRUPlanFormulaAndOperatorClasses(t *testing.T) {
	t.Skip("v3 opclass-weight expectations are superseded by TestReadBillingDemoV4FormulaContract")
	require.Equal(t, "v3-resource-formula-uncalibrated", readBillingDemoWeightVersion)
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
	for _, outputUnit := range []string{readBillingDemoUnitOutputRows, readBillingDemoUnitOutputBytes} {
		_, _, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: outputUnit, value: 4}, tidbWeights)
		require.False(t, ok)
		require.True(t, readBillingDemoUnitDiagnosticOnly(outputUnit))
	}
	for _, outputClass := range []string{
		readBillingDemoOpClassHashAgg,
		readBillingDemoOpClassStreamAgg,
		readBillingDemoOpClassHashJoin,
		readBillingDemoOpClassMergeJoin,
		readBillingDemoOpClassLookupJoin,
	} {
		require.True(t, readBillingDemoOperatorHasOutputShadows(outputClass))
	}
	require.False(t, readBillingDemoOperatorHasOutputShadows(readBillingDemoOpClassProjection))

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
				require.Equal(t, "v3-resource-formula-uncalibrated", stats.WeightVersion)
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

	t.Run("root aggregations expose diagnostic output units without changing formula", func(t *testing.T) {
		child := physicalop.PhysicalProjection{}.Init(ctx, stats, 0)
		child.SetSchema(schema)
		for _, tc := range []struct {
			name    string
			opClass string
			agg     func() *FlatOperator
		}{
			{name: "hash agg", opClass: readBillingDemoOpClassHashAgg, agg: func() *FlatOperator {
				return &FlatOperator{Origin: (&physicalop.BasePhysicalAgg{}).InitForHash(ctx, stats, 0, schema), ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB}
			}},
			{name: "stream agg", opClass: readBillingDemoOpClassStreamAgg, agg: func() *FlatOperator {
				return &FlatOperator{Origin: (&physicalop.BasePhysicalAgg{}).InitForStream(ctx, stats, 0, schema), ChildrenIdx: []int{1}, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tree := FlatPlanTree{
					tc.agg(),
					{Origin: child, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB},
				}
				runtimeStats := execdetails.NewRuntimeStatsColl(nil)
				aggStats := runtimeStats.GetBasicRuntimeStats(tree[0].Origin.ID(), true)
				aggStats.Record(time.Millisecond, 2)
				aggStats.RecordBytes(80, 30)
				childStats := runtimeStats.GetBasicRuntimeStats(child.ID(), true)
				childStats.Record(time.Millisecond, 8)
				childStats.RecordBytes(0, 80)

				operator, supported, reason := readBillingDemoClassifyOperator(tree[0])
				require.True(t, supported)
				require.Empty(t, reason)
				require.Equal(t, tc.opClass, operator.opClass)
				units, reason, ok := readBillingDemoRootUnits(runtimeStats, tree, 0, tree[0], operator)
				require.True(t, ok)
				require.Empty(t, reason)
				require.Equal(t, 8.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
				require.Equal(t, 80.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
				require.Equal(t, 2.0, readBillingDemoUnitValue(units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
				require.Equal(t, 30.0, readBillingDemoUnitValue(units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
				require.Equal(t, readBillingDemoInputSourceRuntimeChunkBytes, readBillingDemoUnitSource(units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
				require.Equal(t, explainRUWidthSourceRuntimeChunkAvg, readBillingDemoUnitWidthSource(units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))

				operator.id = tree[0].ExplainID().String()
				operator.status = readBillingDemoStatusOperatorOK
				operator.reason = readBillingDemoReasonNone
				operator.actRows = 2
				operator.hasActRows = true
				operator.units = units
				result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone, operators: []readBillingDemoOperatorResult{operator}}
				rows := explainRUBuildReadBillingRows(result, explainRUComponentSnapshotOK)
				weights, ok := readBillingDemoResolveWeights(operator.site, operator.opClass, readBillingDemoWeightVersion)
				require.True(t, ok)
				require.InEpsilon(t, weights.fixedEvent+8*weights.row+80*weights.byte, rows[0].previewRU, 0.000001)
				seenOutputUnits := make(map[string]bool)
				for _, row := range rows {
					if row.unit != readBillingDemoUnitOutputRows && row.unit != readBillingDemoUnitOutputBytes {
						continue
					}
					seenOutputUnits[row.unit] = true
					require.False(t, row.hasWeight)
					require.False(t, row.hasPreviewRU)
					require.Contains(t, row.note, "diagnostic_only=true")
					if row.unit == readBillingDemoUnitOutputRows {
						require.Equal(t, 2.0, row.workRows)
						require.Equal(t, int64(2), row.count)
					} else {
						require.Equal(t, 30.0, row.workBytes)
					}
					rendered := row.toStrings()
					require.Empty(t, rendered[13])
					require.Empty(t, rendered[14])
				}
				require.Equal(t, map[string]bool{readBillingDemoUnitOutputRows: true, readBillingDemoUnitOutputBytes: true}, seenOutputUnits)

				statementStats := buildReadBillingDemoStatementStats(result)
				seenOutputUnits = make(map[string]bool)
				for _, sample := range statementStats.BaseUnits {
					if sample.Unit == readBillingDemoUnitOutputRows || sample.Unit == readBillingDemoUnitOutputBytes {
						seenOutputUnits[sample.Unit] = true
					}
				}
				require.Equal(t, map[string]bool{readBillingDemoUnitOutputRows: true, readBillingDemoUnitOutputBytes: true}, seenOutputUnits)
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

		hugeTopN := physicalop.PhysicalTopN{Offset: math.MaxUint64 - 1, Count: 1}.Init(ctx, stats, 0)
		unit, ok = readBillingDemoOrderingWorkUnit(&FlatOperator{Origin: hugeTopN, IsRoot: true, StoreType: kv.TiDB}, readBillingDemoOpClassTopN, 1)
		require.True(t, ok)
		require.Equal(t, 1.0, unit.value)

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
	t.Skip("v3 commit-detail formula expectations are superseded by TestReadBillingDemoV4WriteRequests")
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
	pipelinedResult := buildTiKVWriteBillingDemoOperators("update", ruv2Metrics, true)
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
		one := uint64(1)
		zero := uint64(0)
		runtimeStats.RecordExpectedCopTasks([]int{scan.ID()})
		runtimeStats.RecordCopStats(scan.ID(), kv.TiKV, scanDetail, tikvutil.TimeDetail{}, &tipb.ExecutorExecutionSummary{
			TimeProcessedNs: &one,
			NumProducedRows: &zero,
			NumIterations:   &one,
		})
		return readBillingDemoCopUnits(
			newReadBillingDemoCopEstimator(tree, runtimeStats),
			1,
			readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "tablescan"},
		)
	}

	outcome := buildUnits(&tikvutil.ScanDetail{TotalKeys: 10, ProcessedKeys: 5, ProcessedKeysSize: 100})
	require.True(t, outcome.success, "%+v", outcome.failure)
	units := outcome.units
	require.Equal(t, 1.0, readBillingDemoUnitValue(units, readBillingDemoUnitFixedEvents, readBillingDemoInputSideAll))
	require.Equal(t, 10.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, 200.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
	require.Equal(t, 200.0, readBillingDemoUnitValue(units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
	require.Equal(t, readBillingDemoInputSourceScanDetail, readBillingDemoUnitSource(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, explainRUWidthSourceScanDetailProcessedAvg, readBillingDemoUnitWidthSource(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))

	outcome = buildUnits(&tikvutil.ScanDetail{})
	require.True(t, outcome.success)
	require.Equal(t, 0.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
}

func TestReadBillingDemoCopInputEstimator(t *testing.T) {
	t.Skip("v3 byte-width estimator expectations do not apply to the v4 resource formula")
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
		runtimeStats.RecordExpectedCopTasks([]int{planID})
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
			expectOutputUnits bool
		}{
			{name: "selection", node: &FlatOperator{Origin: selection, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthKnown, expectedOrderWork: -1},
			{name: "limit", node: &FlatOperator{Origin: limit, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthKnown, expectedOrderWork: -1},
			{name: "topn", node: &FlatOperator{Origin: topN, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthKnown, expectedOrderWork: 8},
			{name: "projection", node: &FlatOperator{Origin: projection, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthBarrier, expectedOrderWork: -1},
			{name: "hashagg", node: &FlatOperator{Origin: hashAgg, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthBarrier, expectedOrderWork: -1, expectOutputUnits: true},
			{name: "streamagg", node: &FlatOperator{Origin: streamAgg, IsRoot: false, StoreType: kv.TiKV}, widthState: readBillingDemoCopWidthBarrier, expectedOrderWork: -1, expectOutputUnits: true},
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
				readerStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
				readerStats.Record(time.Millisecond, 2)
				readerStats.RecordBytes(0, 30)
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
				if tc.expectOutputUnits {
					require.Equal(t, 2.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
					require.Equal(t, 30.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
					require.Equal(t, readBillingDemoInputSourceRuntimeOperatorActRows, readBillingDemoUnitSource(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
					require.Equal(t, readBillingDemoInputSourceRuntimeReaderOutput, readBillingDemoUnitSource(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
					require.Equal(t, explainRUWidthSourceRuntimeReaderOutputChunkAvg, readBillingDemoUnitWidthSource(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
				} else {
					require.False(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
					require.False(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
				}
			})
		}
	})

	t.Run("aggregation output shadows fail open without attributable bytes", func(t *testing.T) {
		newHashAgg := func() *FlatOperator {
			agg := (&physicalop.BasePhysicalAgg{}).InitForHash(ctx, stats, 0, schema)
			return &FlatOperator{Origin: agg, IsRoot: false, StoreType: kv.TiKV}
		}
		t.Run("reader row mismatch keeps exact output rows only", func(t *testing.T) {
			agg := newHashAgg()
			agg.ChildrenIdx = []int{2}
			agg.ChildrenEndIdx = 2
			tree := FlatPlanTree{
				{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
				agg,
				{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			}
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			readerStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
			readerStats.Record(time.Millisecond, 3)
			readerStats.RecordBytes(0, 33)
			recordSummary(runtimeStats, scan.ID(), 4, nil)
			recordSummary(runtimeStats, agg.Origin.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
			operator, _, _ := readBillingDemoClassifyOperator(agg)
			outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 1, operator)
			require.True(t, outcome.success)
			require.Equal(t, 4.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
			require.Equal(t, 2.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
			require.False(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
		})

		t.Run("zero rows and bytes remain observed evidence", func(t *testing.T) {
			agg := newHashAgg()
			agg.ChildrenIdx = []int{2}
			agg.ChildrenEndIdx = 2
			tree := FlatPlanTree{
				{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
				agg,
				{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			}
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			readerStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
			readerStats.Record(time.Millisecond, 0)
			readerStats.RecordBytes(0, 0)
			recordSummary(runtimeStats, scan.ID(), 4, nil)
			recordSummary(runtimeStats, agg.Origin.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
			operator, _, _ := readBillingDemoClassifyOperator(agg)
			outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 1, operator)
			require.True(t, outcome.success)
			require.True(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
			require.True(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
			require.Zero(t, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
			require.Zero(t, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
		})

		t.Run("non top aggregation never borrows reader bytes", func(t *testing.T) {
			agg := newHashAgg()
			agg.ChildrenIdx = []int{3}
			agg.ChildrenEndIdx = 3
			tree := FlatPlanTree{
				{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
				{Origin: selection, ChildrenIdx: []int{2}, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
				agg,
				{Origin: scan, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			}
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			readerStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
			readerStats.Record(time.Millisecond, 2)
			readerStats.RecordBytes(0, 30)
			recordSummary(runtimeStats, scan.ID(), 4, nil)
			recordSummary(runtimeStats, agg.Origin.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
			recordSummary(runtimeStats, selection.ID(), 2, nil)
			operator, _, _ := readBillingDemoClassifyOperator(agg)
			outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 2, operator)
			require.True(t, outcome.success)
			require.Equal(t, 2.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
			require.False(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
		})

		t.Run("incomplete own summary omits both output units", func(t *testing.T) {
			agg := newHashAgg()
			agg.ChildrenIdx = []int{2}
			agg.ChildrenEndIdx = 2
			tree := FlatPlanTree{
				{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 2, IsRoot: true, StoreType: kv.TiDB},
				agg,
				{Origin: scan, ChildrenEndIdx: 2, IsRoot: false, StoreType: kv.TiKV},
			}
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			recordSummary(runtimeStats, scan.ID(), 2, nil)
			recordSummary(runtimeStats, scan.ID(), 2, nil)
			recordSummary(runtimeStats, agg.Origin.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
			operator, _, _ := readBillingDemoClassifyOperator(agg)
			outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 1, operator)
			require.True(t, outcome.success)
			require.Equal(t, 4.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
			require.False(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
			require.False(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
		})

		t.Run("missing expected response summaries omit shadows without changing formula inputs", func(t *testing.T) {
			agg := newHashAgg()
			agg.ChildrenIdx = []int{3}
			agg.ChildrenEndIdx = 3
			tree := FlatPlanTree{
				{Origin: limit, ChildrenIdx: []int{1}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
				{Origin: reader, ChildrenIdx: []int{2}, ChildrenEndIdx: 3, IsRoot: true, StoreType: kv.TiDB},
				agg,
				{Origin: scan, ChildrenEndIdx: 3, IsRoot: false, StoreType: kv.TiKV},
			}
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			readerStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
			readerStats.Record(time.Millisecond, 2)
			readerStats.RecordBytes(0, 30)
			recordSummary(runtimeStats, scan.ID(), 4, nil)
			recordSummary(runtimeStats, agg.Origin.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
			// This is the state after either a consumed response omitted every
			// summary or an unconsumed task contributed only its expectation. The
			// expected count must not be inferred from visible summaries.
			runtimeStats.RecordExpectedCopTasks([]int{scan.ID(), agg.Origin.ID()})
			operator, _, _ := readBillingDemoClassifyOperator(agg)
			outcome := readBillingDemoCopUnits(newReadBillingDemoCopEstimator(tree, runtimeStats), 2, operator)
			require.True(t, outcome.success)
			require.Equal(t, 4.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
			require.Equal(t, 80.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
			require.False(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
			require.False(t, readBillingDemoUnitExists(outcome.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
		})
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

	t.Run("full builder preserves pushed aggregation output shadows", func(t *testing.T) {
		aggScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		aggScan.SetSchema(schema)
		agg := (&physicalop.BasePhysicalAgg{}).InitForHash(ctx, stats, 0, schema)
		agg.SetChildren(aggScan)
		reader.TablePlan = agg
		flat := FlattenPhysicalPlan(reader, true)
		require.Len(t, flat.Main, 3)
		require.Equal(t, agg.ID(), flat.Main[1].Origin.ID())
		require.Equal(t, aggScan.ID(), flat.Main[2].Origin.ID())

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		readerStats := runtimeStats.GetBasicRuntimeStats(reader.ID(), true)
		readerStats.Record(time.Millisecond, 2)
		readerStats.RecordBytes(0, 30)
		recordSummary(runtimeStats, aggScan.ID(), 4, nil)
		recordSummary(runtimeStats, agg.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 5, ProcessedKeysSize: 100})
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

		result := buildReadBillingDemoResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
		require.Equal(t, readBillingDemoStatusSuccess, result.status)
		var aggResult readBillingDemoOperatorResult
		for _, operator := range result.operators {
			if operator.site == readBillingDemoSiteTiKV && operator.opClass == readBillingDemoOpClassHashAgg {
				aggResult = operator
				break
			}
		}
		require.Equal(t, readBillingDemoStatusOperatorOK, aggResult.status)
		require.Equal(t, 4.0, readBillingDemoUnitValue(aggResult.units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
		require.Equal(t, 2.0, readBillingDemoUnitValue(aggResult.units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
		require.Equal(t, 30.0, readBillingDemoUnitValue(aggResult.units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))

		seenStatementUnits := make(map[string]bool)
		for _, sample := range buildReadBillingDemoStatementStats(result).BaseUnits {
			if sample.Site == readBillingDemoSiteTiKV && sample.OpClass == readBillingDemoOpClassHashAgg &&
				(sample.Unit == readBillingDemoUnitOutputRows || sample.Unit == readBillingDemoUnitOutputBytes) {
				seenStatementUnits[sample.Unit] = true
			}
		}
		require.Equal(t, map[string]bool{readBillingDemoUnitOutputRows: true, readBillingDemoUnitOutputBytes: true}, seenStatementUnits)
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
	join.EqualConditions = []*expression.ScalarFunction{{}}
	join.LeftJoinKeys = []*expression.Column{col}
	join.RightJoinKeys = []*expression.Column{col}
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
	runtimeStats.RegisterStats(join.ID(), &readBillingDemoHashStatsForTest{rows: 3})

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
	require.Equal(t, 1.0, readBillingDemoUnitValue(units, readBillingDemoUnitExpressionCount, readBillingDemoInputSideAll))
	require.Equal(t, 10.0, readBillingDemoUnitValue(units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
	require.Equal(t, 3.0, readBillingDemoUnitValue(units, readBillingDemoUnitHashStateRows, readBillingDemoInputSideBuild))
	require.Equal(t, 6.0, readBillingDemoUnitValue(units, readBillingDemoUnitJoinOutputRows, readBillingDemoInputSideAll))
	require.Equal(t, 6.0, readBillingDemoUnitValue(units, readBillingDemoUnitOutputRows, readBillingDemoInputSideAll))
	require.Equal(t, 60.0, readBillingDemoUnitValue(units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
	require.Equal(t, readBillingDemoInputSourceHashJoinRuntime, readBillingDemoUnitSource(units, readBillingDemoUnitHashStateRows, readBillingDemoInputSideBuild))
	require.Equal(t, readBillingDemoInputSourceRuntimeChunkBytes, readBillingDemoUnitSource(units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
	require.Equal(t, explainRUWidthSourceRuntimeChunkAvg, readBillingDemoUnitWidthSource(units, readBillingDemoUnitOutputBytes, readBillingDemoInputSideAll))
}

type readBillingDemoHashStatsForTest struct{ rows int64 }

func (*readBillingDemoHashStatsForTest) String() string { return "" }
func (s *readBillingDemoHashStatsForTest) Clone() execdetails.RuntimeStats {
	return &readBillingDemoHashStatsForTest{rows: s.rows}
}
func (*readBillingDemoHashStatsForTest) Merge(execdetails.RuntimeStats) {}
func (*readBillingDemoHashStatsForTest) Tp() int                        { return 1_000_000 }
func (s *readBillingDemoHashStatsForTest) HashTableRows() int64         { return s.rows }

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
