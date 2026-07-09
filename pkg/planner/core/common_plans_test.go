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
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
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
		note:           "input_side=all,weight_version=v1",
	}
	require.Equal(t, []string{
		"plan", "Projection_1", "projection", "tidb/projection_eval", "1", "2", "1", "8.000000", "runtime_chunk_avg", "2", "", "input_rows", "2", "0.250000", "6.000000", "runtime_chunk_bytes", "input_side=all,weight_version=v1",
	}, row.toStrings())
}

func TestExplainRUPlanFormulaAndOperatorClasses(t *testing.T) {
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
	proj := physicalop.PhysicalProjection{}.Init(ctx, stats, 0)
	proj.SetSchema(schema)
	tree := FlatPlanTree{
		{Origin: proj, IsRoot: false, StoreType: kv.TiKV},
	}

	runtimeStats := execdetails.NewRuntimeStatsColl(nil)
	runtimeStats.RecordCopStats(proj.ID(), kv.TiKV, &tikvutil.ScanDetail{}, tikvutil.TimeDetail{}, nil)
	operator, supported, reason := readBillingDemoClassifyOperator(&FlatOperator{
		Origin:    proj,
		IsRoot:    false,
		StoreType: kv.TiKV,
	})
	require.True(t, supported)
	require.Empty(t, reason)
	require.Equal(t, readBillingDemoSiteTiKV, operator.site)
	require.Equal(t, readBillingDemoOpClassProjection, operator.opClass)

	units, actualReason, ok := readBillingDemoCopUnits(runtimeStats, tree, 0, operator)
	require.False(t, ok)
	require.Nil(t, units)
	require.Equal(t, readBillingDemoReasonMissingRuntimeBytes, actualReason)
}

func TestReadBillingDemoRangeScanUsesProcessedKeyAverage(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	scan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
	scan.SetSchema(schema)
	scan.StoreType = kv.TiKV
	scan.TblCols = []*expression.Column{col}
	tree := FlatPlanTree{
		{Origin: scan, IsRoot: false, StoreType: kv.TiKV},
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

	buildUnits := func(scanDetail *tikvutil.ScanDetail) ([]readBillingDemoUnit, string, bool) {
		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		runtimeStats.RecordCopStats(scan.ID(), kv.TiKV, scanDetail, tikvutil.TimeDetail{}, nil)
		return readBillingDemoCopUnits(
			runtimeStats,
			tree,
			0,
			readBillingDemoOperatorResult{site: readBillingDemoSiteTiKV, opClass: readBillingDemoOpClassRangeScan, operatorKind: "tablescan"},
		)
	}

	units, actualReason, ok := buildUnits(&tikvutil.ScanDetail{TotalKeys: 10, ProcessedKeys: 5, ProcessedKeysSize: 100})
	require.True(t, ok)
	require.Empty(t, actualReason)
	require.Equal(t, 1.0, readBillingDemoUnitValue(units, readBillingDemoUnitFixedEvents, readBillingDemoInputSideAll))
	require.Equal(t, 10.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, 200.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))
	require.Equal(t, readBillingDemoInputSourceScanDetail, readBillingDemoUnitSource(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
	require.Equal(t, explainRUWidthSourceScanDetailProcessedAvg, readBillingDemoUnitWidthSource(units, readBillingDemoUnitInputBytes, readBillingDemoInputSideAll))

	units, actualReason, ok = buildUnits(&tikvutil.ScanDetail{})
	require.False(t, ok)
	require.Nil(t, units)
	require.Equal(t, readBillingDemoReasonMissingScanDetail, actualReason)
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
