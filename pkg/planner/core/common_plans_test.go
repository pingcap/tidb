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
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
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
		{"explain analyze format='ru' select * from t for update skip locked", explainRUStatusSuccess},
		{"explain analyze format='ru' select * from t for share skip locked", explainRUStatusSuccess},
		{"explain analyze format='ru' select 1 union select * from t for update", explainRUStatusSuccess},
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
		operators: []readBillingDemoOperatorResult{buildTiKVWriteBillingDemoOperator(dmlKind, ruv2Metrics, false, false)},
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
	case readBillingDemoUnitWriteBytes:
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

type readBillingDemoRPCStatsForTest struct {
	counts             map[tikvrpc.CmdType]int64
	detail             tikvutil.ScanDetail
	detailRecords      uint64
	completedResponses uint64
	tp                 int
}

func (*readBillingDemoRPCStatsForTest) String() string {
	return ""
}

func (s *readBillingDemoRPCStatsForTest) Merge(other execdetails.RuntimeStats) {
	otherStats, ok := other.(*readBillingDemoRPCStatsForTest)
	if !ok {
		return
	}
	for cmd, count := range otherStats.counts {
		s.counts[cmd] += count
	}
	s.detail.Merge(&otherStats.detail)
	s.detailRecords += otherStats.detailRecords
	s.completedResponses += otherStats.completedResponses
}

func (s *readBillingDemoRPCStatsForTest) Clone() execdetails.RuntimeStats {
	cloned := &readBillingDemoRPCStatsForTest{
		counts:             make(map[tikvrpc.CmdType]int64, len(s.counts)),
		detail:             s.detail,
		detailRecords:      s.detailRecords,
		completedResponses: s.completedResponses,
		tp:                 s.tp,
	}
	for cmd, count := range s.counts {
		cloned.counts[cmd] = count
	}
	return cloned
}

func (s *readBillingDemoRPCStatsForTest) Tp() int {
	if s.tp != 0 {
		return s.tp
	}
	return execdetails.TpRuntimeStatsWithSnapshot
}

func (s *readBillingDemoRPCStatsForTest) GetCmdRPCCount(cmd tikvrpc.CmdType) int64 {
	return s.counts[cmd]
}

func (s *readBillingDemoRPCStatsForTest) GetScanDetailAndCoverage() (tikvutil.ScanDetail, uint64, uint64) {
	return s.detail, s.detailRecords, s.completedResponses
}

type readBillingDemoNilEmbeddedPointStatsForTest struct {
	*txnsnapshot.SnapshotRuntimeStats
}

func (*readBillingDemoNilEmbeddedPointStatsForTest) String() string {
	return ""
}

func (*readBillingDemoNilEmbeddedPointStatsForTest) Merge(execdetails.RuntimeStats) {}

func (*readBillingDemoNilEmbeddedPointStatsForTest) Clone() execdetails.RuntimeStats {
	return &readBillingDemoNilEmbeddedPointStatsForTest{}
}

func (*readBillingDemoNilEmbeddedPointStatsForTest) Tp() int {
	return execdetails.TpRuntimeStatsWithSnapshot
}

func TestReadBillingDemoV6FormulaContract(t *testing.T) {
	t.Run("subquery execution classes", func(t *testing.T) {
		ctx := mock.NewContext()
		wrapper := ScalarSubqueryEvalCtx{}.Init(ctx, 0)
		operator, supported, reason := readBillingDemoClassifyOperator(&FlatOperator{Origin: wrapper, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOpClassWrapper, operator.opClass)
		require.False(t, readBillingDemoOperatorBillable(operator))

		maxOneRow := physicalop.PhysicalMaxOneRow{}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
		operator, supported, reason = readBillingDemoClassifyOperator(&FlatOperator{Origin: maxOneRow, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOpClassLimit, operator.opClass)
		require.True(t, readBillingDemoOperatorBillable(operator))

		apply := physicalop.PhysicalApply{}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
		operator, supported, reason = readBillingDemoClassifyOperator(&FlatOperator{Origin: apply, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOpClassWrapper, operator.opClass)
		require.False(t, readBillingDemoOperatorBillable(operator))

		cte := physicalop.PhysicalCTE{}.Init(ctx, &property.StatsInfo{RowCount: 1})
		operator, supported, reason = readBillingDemoClassifyOperator(&FlatOperator{Origin: cte, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOpClassWrapper, operator.opClass)
		require.False(t, readBillingDemoOperatorBillable(operator))

		cteTable := physicalop.PhysicalCTETable{}.Init(ctx, &property.StatsInfo{RowCount: 1})
		operator, supported, reason = readBillingDemoClassifyOperator(&FlatOperator{Origin: cteTable, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOpClassWrapper, operator.opClass)
		require.False(t, readBillingDemoOperatorBillable(operator))

		shuffle := physicalop.PhysicalShuffle{SplitterType: physicalop.PartitionHashSplitterType}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
		operator, supported, reason = readBillingDemoClassifyOperator(&FlatOperator{Origin: shuffle, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOpClassShuffle, operator.opClass)
		require.Equal(t, readBillingDemoOperatorHashShuffle, operator.operatorKind)
		require.True(t, readBillingDemoOperatorBillable(operator))

		shuffle.SplitterType = physicalop.PartitionRangeSplitterType
		operator, supported, reason = readBillingDemoClassifyOperator(&FlatOperator{Origin: shuffle, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOperatorRangeShuffle, operator.operatorKind)

		receiver := physicalop.PhysicalShuffleReceiverStub{}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
		operator, supported, reason = readBillingDemoClassifyOperator(&FlatOperator{Origin: receiver, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOpClassWrapper, operator.opClass)
		require.False(t, readBillingDemoOperatorBillable(operator))
	})

	weights := readBillingDemoWeights{
		ModelVersion: readBillingDemoModelVersion, Version: "test-v6-calibrated",
		CPUWeight: 2, ScanWeight: 3, NetWeight: 5,
		HashTableWeight: 11, JoinWeight: 13, WriteKeyWeight: 17, WriteBytesWeight: 19,
		FrontendCompileWeight: 23, MutationBytesPerCPUUnit: 10, Calibrated: true,
	}
	for _, tc := range []struct {
		unit   string
		value  float64
		weight float64
	}{
		{readBillingDemoUnitCPUWork, 4, 2}, {readBillingDemoUnitScanBytes, 6, 3},
		{readBillingDemoUnitNetBytes, 8, 5},
		{readBillingDemoUnitHashStateRows, 12, 11}, {readBillingDemoUnitJoinOutputRows, 14, 13},
		{readBillingDemoUnitWriteKeys, 3, 17}, {readBillingDemoUnitWriteBytes, 20, 19},
		{readBillingDemoUnitFrontendCompileBytes, 7, 23},
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
		{ModelVersion: "v4", Version: "test-v4-calibrated", MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: readBillingDemoWeightVersion, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", CPUWeight: -1, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", CPUWeight: math.NaN(), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", CPUWeight: math.Inf(1), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", WriteKeyWeight: -1, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", WriteKeyWeight: math.NaN(), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", WriteKeyWeight: math.Inf(1), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", WriteBytesWeight: -1, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", WriteBytesWeight: math.NaN(), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", WriteBytesWeight: math.Inf(1), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", FrontendCompileWeight: -1, MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", FrontendCompileWeight: math.NaN(), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", FrontendCompileWeight: math.Inf(1), MutationBytesPerCPUUnit: 1, Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", MutationBytesPerCPUUnit: math.NaN(), Calibrated: true},
		{ModelVersion: readBillingDemoModelVersion, Version: "test", MutationBytesPerCPUUnit: math.Inf(1), Calibrated: true},
	} {
		require.False(t, readBillingDemoWeightsValid(invalidWeights))
	}

	frontendCtx := mock.NewContext()
	frontendStmt := &ast.SelectStmt{}
	frontendStmt.SetText(nil, "select '表'")
	frontendResult := readBillingDemoResult{status: readBillingDemoStatusSuccess}
	appendReadBillingDemoFrontend(&frontendResult, frontendCtx, frontendStmt)
	require.Len(t, frontendResult.operators, 1)
	frontend := frontendResult.operators[0]
	require.Equal(t, "frontend@statement", frontend.id)
	require.Equal(t, readBillingDemoOpClassSQLFrontend, frontend.opClass)
	require.Equal(t, readBillingDemoOperatorParserOptimizer, frontend.operatorKind)
	require.Equal(t, readBillingDemoStatusOperatorOK, frontend.status)
	require.Equal(t, float64(len(frontendStmt.OriginalText())), readBillingDemoUnitValue(frontend.units, readBillingDemoUnitFrontendCompileBytes, readBillingDemoInputSideAll))

	frontendCtx.GetSessionVars().FoundInPlanCache = true
	cacheHitResult := readBillingDemoResult{status: readBillingDemoStatusSuccess}
	appendReadBillingDemoFrontend(&cacheHitResult, frontendCtx, frontendStmt)
	require.Empty(t, cacheHitResult.operators)

	frontendCtx.GetSessionVars().FoundInPlanCache = false
	errorResult := readBillingDemoResult{status: readBillingDemoStatusError}
	appendReadBillingDemoFrontend(&errorResult, frontendCtx, frontendStmt)
	require.Empty(t, errorResult.operators)

	const contextOriginalSQL = "select 'from context'"
	frontendCtx.GetSessionVars().StmtCtx.OriginalSQL = contextOriginalSQL
	contextInputResult := readBillingDemoResult{status: readBillingDemoStatusSuccess}
	appendReadBillingDemoFrontend(&contextInputResult, frontendCtx, nil)
	require.Len(t, contextInputResult.operators, 1)
	require.Equal(t, float64(len(contextOriginalSQL)), readBillingDemoUnitValue(contextInputResult.operators[0].units, readBillingDemoUnitFrontendCompileBytes, readBillingDemoInputSideAll))

	frontendCtx.GetSessionVars().StmtCtx.OriginalSQL = ""
	missingInputResult := readBillingDemoResult{
		status: readBillingDemoStatusSuccess,
		operators: []readBillingDemoOperatorResult{{
			site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassProjection,
			operatorKind: "projection", status: readBillingDemoStatusOperatorOK,
			units: []readBillingDemoUnit{{unit: readBillingDemoUnitCPUWork, side: readBillingDemoInputSideAll, value: 1}},
		}},
	}
	appendReadBillingDemoFrontend(&missingInputResult, frontendCtx, nil)
	require.Equal(t, readBillingDemoStatusSuccess, missingInputResult.status)
	require.Empty(t, missingInputResult.reason)
	require.Len(t, missingInputResult.operators, 1)
	require.Equal(t, readBillingDemoOpClassProjection, missingInputResult.operators[0].opClass)
	require.Equal(t, float64(1), readBillingDemoUnitValue(missingInputResult.operators[0].units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
	missingInputStats := buildReadBillingDemoStatementStats(missingInputResult)
	require.Len(t, missingInputStats.BaseUnits, 1)
	require.Equal(t, readBillingDemoUnitCPUWork, missingInputStats.BaseUnits[0].Unit)
	require.Equal(t, float64(1), missingInputStats.BaseUnits[0].Value)

	oldWeights := readBillingDemoV6Weights
	readBillingDemoV6Weights = weights
	t.Cleanup(func() { readBillingDemoV6Weights = oldWeights })
	missingInputRows := explainRUBuildReadBillingRows(missingInputResult, explainRUComponentSnapshotOK)
	require.True(t, missingInputRows[0].hasPreviewRU)
	require.Equal(t, weights.CPUWeight, missingInputRows[0].previewRU)
	result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone, operators: []readBillingDemoOperatorResult{{
		id: "formula", site: readBillingDemoSiteTiDB, opClass: readBillingDemoOpClassProjection,
		operatorKind: "projection", status: readBillingDemoStatusOperatorOK,
		units: []readBillingDemoUnit{
			{unit: readBillingDemoUnitCPUWork, value: 4}, {unit: readBillingDemoUnitScanBytes, value: 6},
			{unit: readBillingDemoUnitNetBytes, value: 8},
			{unit: readBillingDemoUnitHashStateRows, value: 12}, {unit: readBillingDemoUnitJoinOutputRows, value: 14},
			{unit: readBillingDemoUnitWriteKeys, value: 3}, {unit: readBillingDemoUnitWriteBytes, value: 20},
			{unit: readBillingDemoUnitFrontendCompileBytes, value: 7},
		},
	}}}
	rows := explainRUBuildReadBillingRows(result, explainRUComponentSnapshotOK)
	require.True(t, rows[0].hasPreviewRU)
	require.Equal(t, 972.0, rows[0].previewRU)
	require.Contains(t, rows[0].note, "model_version=v6,weight_version=test-v6-calibrated")
	require.Equal(t, "test-v6-calibrated", buildReadBillingDemoStatementStats(result).WeightVersion)
	overflowWeights := weights
	overflowWeights.CPUWeight = 1
	readBillingDemoV6Weights = overflowWeights
	overflowResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, operators: []readBillingDemoOperatorResult{{
		status: readBillingDemoStatusOperatorOK, opClass: readBillingDemoOpClassProjection,
		units: []readBillingDemoUnit{{unit: readBillingDemoUnitCPUWork, value: math.MaxFloat64}, {unit: readBillingDemoUnitCPUWork, value: math.MaxFloat64}},
	}}}
	overflowRows := explainRUBuildReadBillingRows(overflowResult, explainRUComponentSnapshotOK)
	require.False(t, overflowRows[0].hasPreviewRU)
	readBillingDemoV6Weights = weights

	readBillingDemoV6Weights = readBillingDemoWeights{}
	rows = explainRUBuildReadBillingRows(result, explainRUComponentSnapshotOK)
	require.False(t, rows[0].hasPreviewRU)
	require.Contains(t, rows[0].note, readBillingDemoReasonUncalibratedWeights)
	stats := buildReadBillingDemoStatementStats(result)
	require.Equal(t, "v6", stats.ModelVersion)
	require.Equal(t, "v6-frontend-compile-work-uncalibrated", stats.WeightVersion)
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
		require.Len(t, op.units, 1)

		runtimeStats.RecordExpectedCopTasks([]int{scan.ID()})
		op, _ = readBillingDemoReaderTransport(flat, runtimeStats, execdetails.NewRUV2Metrics(), false)
		require.Equal(t, readBillingDemoReasonMissingReaderTransport, op.reason)

		runtimeStats.RegisterStats(reader.ID(), &readBillingDemoRPCStatsForTest{
			counts: map[tikvrpc.CmdType]int64{
				tikvrpc.CmdCop:       2,
				tikvrpc.CmdCopStream: 1,
			},
			tp: execdetails.TpSelectResultRuntimeStats,
		})
		dmlMetrics := execdetails.NewRUV2Metrics()
		dmlMetrics.AddResourceManagerReadCnt(99)
		dmlMetrics.AddTiKVCoprocessorResponseBytes(128)
		op, _ = readBillingDemoReaderTransport(flat, runtimeStats, dmlMetrics, true)
		require.Equal(t, readBillingDemoStatusOperatorOK, op.status)
		require.Equal(t, 128.0, readBillingDemoUnitValue(op.units, readBillingDemoUnitNetBytes, readBillingDemoInputSideAll))
		require.Len(t, op.units, 1)
	})

	t.Run("point lookup uses typed scan detail and coverage", func(t *testing.T) {
		ctx := mock.NewContext()
		stats := &property.StatsInfo{RowCount: 1}
		tblInfo := &model.TableInfo{}
		pointPlan := physicalop.PointGetPlan{TblInfo: tblInfo}
		pointPlan.SetSchema(expression.NewSchema())
		point := pointPlan.Init(ctx, stats, 0)
		batch := (&physicalop.BatchPointGetPlan{TblInfo: tblInfo}).Init(ctx, stats, expression.NewSchema(), nil, 0)
		pointLookupRuntimeStats := execdetails.NewRuntimeStatsColl(nil)
		pointLookupRuntimeStats.RegisterStats(point.ID(), &readBillingDemoRPCStatsForTest{
			detail:        tikvutil.ScanDetail{TotalKeys: 2, ProcessedKeys: 1, ProcessedKeysSize: 37},
			detailRecords: 2, completedResponses: 2,
		})
		pointLookupRuntimeStats.RegisterStats(batch.ID(), &readBillingDemoRPCStatsForTest{
			detail:        tikvutil.ScanDetail{TotalKeys: 5, ProcessedKeys: 3, ProcessedKeysSize: 111},
			detailRecords: 5, completedResponses: 5,
		})

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
		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				op, present := readBillingDemoPointLookupTransport(tc.flat, pointLookupRuntimeStats, false)
				require.True(t, present)
				require.Equal(t, "point_lookup@statement", op.id)
				require.Equal(t, readBillingDemoStatusOperatorOK, op.status)
				require.Equal(t, readBillingDemoSiteTiKV, op.site)
				require.Equal(t, readBillingDemoOpClassPointLookup, op.opClass)
				require.Equal(t, tc.kind, op.operatorKind)
				require.Len(t, op.units, 7)
				require.Equal(t, []float64{2, 5, 7}[i], readBillingDemoUnitValue(op.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
				require.Equal(t, []float64{37, 111, 148}[i], readBillingDemoUnitValue(op.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
				require.Equal(t, []float64{1, 3, 4}[i], readBillingDemoUnitValue(op.units, readBillingDemoUnitProcessedKeys, readBillingDemoInputSideAll))
				require.Equal(t, readBillingDemoInputSourceSnapshotRuntimeStats, op.units[0].source)
				require.True(t, readBillingDemoOperatorBillable(op))

				rendered := explainRUBuildReadBillingRows(readBillingDemoResult{
					status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone,
					operators: []readBillingDemoOperatorResult{op},
				}, explainRUComponentSnapshotOK)
				require.Len(t, rendered, 8)
				require.False(t, rendered[0].hasPreviewRU)
				require.Contains(t, rendered[0].note, "model_version=v6,weight_version=v6-frontend-compile-work-uncalibrated")
				renderedUnits := make(map[string]float64, len(op.units))
				for _, row := range rendered[1:] {
					require.Equal(t, explainRUSectionPlan, row.section)
					require.Equal(t, readBillingDemoSiteTiKV+"/"+readBillingDemoOpClassPointLookup, row.operatorClass)
					require.Equal(t, tc.kind, row.component)
					require.Equal(t, readBillingDemoInputSourceSnapshotRuntimeStats, row.source)
					require.Contains(t, row.note, "input_side=all")
					require.False(t, row.hasPreviewRU)
					switch {
					case row.hasCount:
						renderedUnits[row.unit] = float64(row.count)
					case row.hasWorkRows:
						renderedUnits[row.unit] = row.workRows
					case row.hasWorkBytes:
						renderedUnits[row.unit] = row.workBytes
					default:
						require.Failf(t, "point unit has no rendered value", "unit=%s", row.unit)
					}
				}
				require.Len(t, renderedUnits, 7)
				require.Equal(t, []float64{2, 5, 7}[i], renderedUnits[readBillingDemoUnitCPUWork])
				require.Equal(t, []float64{37, 111, 148}[i], renderedUnits[readBillingDemoUnitScanBytes])
				require.Equal(t, []float64{1, 3, 4}[i], renderedUnits[readBillingDemoUnitProcessedKeys])
			})
		}

		zeroRuntimeStats := execdetails.NewRuntimeStatsColl(nil)
		zeroRuntimeStats.RegisterStats(point.ID(), &readBillingDemoRPCStatsForTest{})
		zeroOp, present := readBillingDemoPointLookupTransport(testCases[0].flat, zeroRuntimeStats, false)
		require.True(t, present)
		require.Equal(t, readBillingDemoStatusOperatorOK, zeroOp.status)
		require.Zero(t, readBillingDemoUnitValue(zeroOp.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
		require.Zero(t, readBillingDemoUnitValue(zeroOp.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))

		for i := range 2 {
			localShortCircuitStats := execdetails.NewRuntimeStatsColl(nil)
			planID := []int{point.ID(), batch.ID()}[i]
			basic := localShortCircuitStats.GetBasicRuntimeStats(planID, true)
			basic.Record(time.Millisecond, 0)
			basic.RecordBytes(0, 0)
			localOp, present := readBillingDemoPointLookupTransport(testCases[i].flat, localShortCircuitStats, false)
			require.True(t, present)
			require.Equal(t, readBillingDemoStatusOperatorOK, localOp.status)
			require.Zero(t, readBillingDemoUnitValue(localOp.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
			require.Zero(t, readBillingDemoUnitValue(localOp.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
			require.Zero(t, readBillingDemoUnitValue(localOp.units, readBillingDemoUnitCompletedResponses, readBillingDemoInputSideAll))
		}

		incompleteLocalStats := execdetails.NewRuntimeStatsColl(nil)
		incompleteLocalStats.GetBasicRuntimeStats(point.ID(), true).Record(time.Millisecond, 0)
		incompleteLocalOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, incompleteLocalStats, false)
		require.Equal(t, readBillingDemoReasonMissingPointScanStats, incompleteLocalOp.reason)

		nonzeroLocalStats := execdetails.NewRuntimeStatsColl(nil)
		nonzeroBasic := nonzeroLocalStats.GetBasicRuntimeStats(point.ID(), true)
		nonzeroBasic.Record(time.Millisecond, 1)
		nonzeroBasic.RecordBytes(0, 8)
		nonzeroLocalOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, nonzeroLocalStats, false)
		require.Equal(t, readBillingDemoReasonMissingPointScanStats, nonzeroLocalOp.reason)

		presentZeroRuntimeStats := execdetails.NewRuntimeStatsColl(nil)
		presentZeroRuntimeStats.RegisterStats(point.ID(), &readBillingDemoRPCStatsForTest{detailRecords: 1, completedResponses: 1})
		presentZeroOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, presentZeroRuntimeStats, false)
		require.Equal(t, readBillingDemoStatusOperatorOK, presentZeroOp.status)
		require.Equal(t, 1.0, readBillingDemoUnitValue(presentZeroOp.units, readBillingDemoUnitDetailRecords, readBillingDemoInputSideAll))

		incompleteRuntimeStats := execdetails.NewRuntimeStatsColl(nil)
		incompleteRuntimeStats.RegisterStats(point.ID(), &readBillingDemoRPCStatsForTest{completedResponses: 1})
		incompleteOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, incompleteRuntimeStats, false)
		require.Equal(t, readBillingDemoReasonIncompletePointScanDetail, incompleteOp.reason)

		invalidRuntimeStats := execdetails.NewRuntimeStatsColl(nil)
		invalidRuntimeStats.RegisterStats(point.ID(), &readBillingDemoRPCStatsForTest{
			detail: tikvutil.ScanDetail{TotalKeys: -1}, detailRecords: 1, completedResponses: 1,
		})
		invalidOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, invalidRuntimeStats, false)
		require.Equal(t, readBillingDemoReasonInvalidPointScanDetail, invalidOp.reason)

		overflowRuntimeStats := execdetails.NewRuntimeStatsColl(nil)
		overflowRuntimeStats.RegisterStats(point.ID(), &readBillingDemoRPCStatsForTest{
			detail: tikvutil.ScanDetail{TotalKeys: math.MaxInt64}, detailRecords: 1, completedResponses: 1, tp: 1001,
		})
		overflowRuntimeStats.RegisterStats(point.ID(), &readBillingDemoRPCStatsForTest{
			detail: tikvutil.ScanDetail{TotalKeys: 1}, detailRecords: 1, completedResponses: 1, tp: 1002,
		})
		overflowOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, overflowRuntimeStats, false)
		require.Equal(t, readBillingDemoReasonInvalidPointScanDetail, overflowOp.reason)

		missingOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, nil, false)
		require.Equal(t, readBillingDemoReasonMissingPointScanStats, missingOp.reason)

		nilEmbeddedRuntimeStats := execdetails.NewRuntimeStatsColl(nil)
		nilEmbeddedRuntimeStats.RegisterStats(point.ID(), &readBillingDemoNilEmbeddedPointStatsForTest{})
		nilEmbeddedOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, nilEmbeddedRuntimeStats, false)
		require.Equal(t, readBillingDemoStatusUnknownInput, nilEmbeddedOp.status)
		require.Equal(t, readBillingDemoReasonInvalidPointScanDetail, nilEmbeddedOp.reason)

		dmlRuntimeStats := execdetails.NewRuntimeStatsColl(nil)
		dmlRuntimeStats.RegisterStats(point.ID(), &readBillingDemoRPCStatsForTest{
			detail:        tikvutil.ScanDetail{TotalKeys: 3, ProcessedKeys: 2, ProcessedKeysSize: 74},
			detailRecords: 3, completedResponses: 3,
		})
		dmlRuntimeStats.RegisterStats(batch.ID(), &readBillingDemoRPCStatsForTest{
			detail:        tikvutil.ScanDetail{TotalKeys: 3, ProcessedKeys: 2, ProcessedKeysSize: 74},
			detailRecords: 3, completedResponses: 3,
		})
		for i, expectedWork := range []int64{3, 3, 6} {
			dmlOp, present := readBillingDemoPointLookupTransport(testCases[i].flat, dmlRuntimeStats, true)
			require.True(t, present)
			require.Equal(t, readBillingDemoStatusOperatorOK, dmlOp.status)
			require.Equal(t, float64(expectedWork), readBillingDemoUnitValue(dmlOp.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
			require.Equal(t, readBillingDemoInputSourceSnapshotRuntimeStats, dmlOp.units[0].source)
		}
		missingDMLStatsOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, nil, true)
		require.Equal(t, readBillingDemoReasonMissingPointScanStats, missingDMLStatsOp.reason)

		point.Lock = true
		lockOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, pointLookupRuntimeStats, false)
		require.Equal(t, readBillingDemoStatusOperatorOK, lockOp.status)
		require.Equal(t, 2.0, readBillingDemoUnitValue(lockOp.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
		dmlLockOp, _ := readBillingDemoPointLookupTransport(testCases[0].flat, dmlRuntimeStats, true)
		require.Equal(t, readBillingDemoStatusOperatorOK, dmlLockOp.status)
		require.Equal(t, 3.0, readBillingDemoUnitValue(dmlLockOp.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
		point.Lock = false
		reader := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
		mixedReaderFlat := &FlatPhysicalPlan{
			Main:             testCases[0].flat.Main,
			ScalarSubQueries: []FlatPlanTree{{{Origin: reader, IsRoot: true, StoreType: kv.TiDB}}},
		}
		mixedReaderOp, _ := readBillingDemoPointLookupTransport(mixedReaderFlat, pointLookupRuntimeStats, false)
		require.Equal(t, readBillingDemoStatusOperatorOK, mixedReaderOp.status)
		mixedReaderDMLOp, _ := readBillingDemoPointLookupTransport(mixedReaderFlat, dmlRuntimeStats, true)
		require.Equal(t, readBillingDemoStatusOperatorOK, mixedReaderDMLOp.status)
		require.Equal(t, 3.0, readBillingDemoUnitValue(mixedReaderDMLOp.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))

		physicalOp, supported, reason := readBillingDemoClassifyOperator(testCases[0].flat.Main[0])
		physicalOp.id = point.ExplainID().String()
		require.True(t, supported)
		require.Empty(t, reason)
		require.False(t, readBillingDemoOperatorBillable(physicalOp))

		lock := physicalop.PhysicalLock{}.Init(ctx, stats)
		lockOp, supported, reason = readBillingDemoClassifyOperator(&FlatOperator{Origin: lock, IsRoot: true, StoreType: kv.TiDB})
		require.True(t, supported)
		require.Empty(t, reason)
		require.Equal(t, readBillingDemoOpClassWrapper, lockOp.opClass)
		require.False(t, readBillingDemoOperatorBillable(lockOp))
	})
}

func TestReadBillingDemoV6ExpressionCountsAndOrdering(t *testing.T) {
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
		ModelVersion: readBillingDemoModelVersion, Version: "test-v6-calibrated",
		CPUWeight: 2, ScanWeight: 3, NetWeight: 5,
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
	recordCopSummary := func(runtimeStats *execdetails.RuntimeStatsColl, planID int, rows uint64, detail *tikvutil.ScanDetail) {
		one := uint64(1)
		runtimeStats.RecordExpectedCopTasks([]int{planID})
		runtimeStats.RecordCopStats(planID, kv.TiKV, detail, tikvutil.TimeDetail{}, &tipb.ExecutorExecutionSummary{
			TimeProcessedNs: &one,
			NumProducedRows: &rows,
			NumIterations:   &one,
		})
	}
	schema := expression.NewSchema(col)

	t.Run("cte producer parts require execution evidence", func(t *testing.T) {
		seedLeaf := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
		seed := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
		seed.SetChildren(seedLeaf)
		recurLeaf := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
		recur := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
		recur.SetChildren(recurLeaf)
		cte := physicalop.PhysicalCTE{
			SeedPlan:  seed,
			RecurPlan: recur,
			CTE:       &logicalop.CTEClass{IDForStorage: 1},
		}.Init(ctx, stats)
		flat := FlattenPhysicalPlan(cte, true)
		require.Len(t, flat.CTEs, 1)
		cteTree := flat.CTEs[0]
		require.Len(t, cteTree[0].ChildrenIdx, 2)

		assertPartSkipped := func(t *testing.T, mask *readBillingDemoExecutionMask, partRoot int, skipped bool) {
			t.Helper()
			require.GreaterOrEqual(t, cteTree[partRoot].ChildrenEndIdx, partRoot)
			for idx := partRoot; idx <= cteTree[partRoot].ChildrenEndIdx; idx++ {
				require.Equal(t, skipped, mask.isSkipped(cteTree[idx]), "idx=%d operator=%s", idx, cteTree[idx].Origin.ExplainID())
			}
		}

		seedRoot, recurRoot := cteTree[0].ChildrenIdx[0], cteTree[0].ChildrenIdx[1]
		mask := buildReadBillingDemoExecutionMask(flat, execdetails.NewRuntimeStatsColl(nil))
		assertPartSkipped(t, mask, seedRoot, true)
		assertPartSkipped(t, mask, recurRoot, true)

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordRoot(runtimeStats, seed.ID(), 0)
		mask = buildReadBillingDemoExecutionMask(flat, runtimeStats)
		assertPartSkipped(t, mask, seedRoot, false)
		assertPartSkipped(t, mask, recurRoot, true)
	})

	t.Run("shuffle uses each data source once", func(t *testing.T) {
		buildShuffle := func(rows []int, keyCounts []int, concurrency int) (*physicalop.PhysicalShuffle, FlatPlanTree, *execdetails.RuntimeStatsColl) {
			t.Helper()
			require.Len(t, keyCounts, len(rows))
			dataSources := make([]base.PhysicalPlan, 0, len(rows))
			receivers := make([]base.PhysicalPlan, 0, len(rows))
			byItemArrays := make([][]expression.Expression, 0, len(rows))
			for i, rowCount := range rows {
				dataSource := physicalop.PhysicalTableDual{RowCount: rowCount}.Init(ctx, stats, 0)
				receiver := physicalop.PhysicalShuffleReceiverStub{DataSource: dataSource}.Init(ctx, stats, 0)
				dataSources = append(dataSources, dataSource)
				receivers = append(receivers, receiver)
				byItems := make([]expression.Expression, keyCounts[i])
				for j := range byItems {
					byItems[j] = col
				}
				byItemArrays = append(byItemArrays, byItems)
			}
			head := physicalop.PhysicalUnionAll{}.Init(ctx, stats, 0)
			head.SetChildren(receivers...)
			shuffle := physicalop.PhysicalShuffle{
				Concurrency:  concurrency,
				DataSources:  dataSources,
				SplitterType: physicalop.PartitionHashSplitterType,
				ByItemArrays: byItemArrays,
			}.Init(ctx, stats, 0)
			shuffle.SetChildren(head)
			tree := FlattenPhysicalPlan(shuffle, true).Main
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			recordRoot(runtimeStats, shuffle.ID(), 97)
			for i, dataSource := range dataSources {
				recordRoot(runtimeStats, receivers[i].ID(), rows[i]*concurrency)
				recordRoot(runtimeStats, dataSource.ID(), rows[i])
				occurrences := 0
				for _, node := range tree {
					if node.Origin.ID() == dataSource.ID() {
						occurrences++
					}
				}
				require.Equal(t, 1, occurrences)
			}
			return shuffle, tree, runtimeStats
		}

		shuffleUnits := func(t *testing.T, shuffle *physicalop.PhysicalShuffle, tree FlatPlanTree, runtimeStats *execdetails.RuntimeStatsColl) ([]readBillingDemoUnit, string, bool) {
			t.Helper()
			return readBillingDemoRootUnits(runtimeStats, tree, 0, tree[0], readBillingDemoOperatorResult{opClass: readBillingDemoOpClassShuffle})
		}

		shuffle, tree, runtimeStats := buildShuffle([]int{3}, []int{1}, 2)
		units, reason, ok := shuffleUnits(t, shuffle, tree, runtimeStats)
		require.True(t, ok, reason)
		require.Empty(t, reason)
		require.Equal(t, 6.0, readBillingDemoUnitValue(units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
		require.Equal(t, readBillingDemoInputSourceShuffleDataSourceRows, readBillingDemoUnitSource(units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
		require.Len(t, units, 1)

		result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
		status, failedOperator := appendReadBillingDemoTree(&result, ctx, runtimeStats, tree)
		require.Equal(t, readBillingDemoStatusSuccess, status, failedOperator)
		var shuffleCPUUnits, receiverOperators, dataSourceOperators int
		for _, operator := range result.operators {
			switch operator.operatorKind {
			case readBillingDemoOperatorHashShuffle:
				for _, unit := range operator.units {
					if unit.unit == readBillingDemoUnitCPUWork {
						shuffleCPUUnits++
					}
				}
			case "shufflereceiver":
				receiverOperators++
				require.Empty(t, operator.units)
			case "tabledual":
				dataSourceOperators++
			}
		}
		require.Equal(t, 1, shuffleCPUUnits)
		require.Equal(t, 1, receiverOperators)
		require.Equal(t, 1, dataSourceOperators)

		shuffle, tree, runtimeStats = buildShuffle([]int{3}, []int{2}, 2)
		units, reason, ok = shuffleUnits(t, shuffle, tree, runtimeStats)
		require.True(t, ok, reason)
		require.Equal(t, 9.0, readBillingDemoUnitValue(units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))

		shuffle, tree, runtimeStats = buildShuffle([]int{2, 5}, []int{1, 1}, 2)
		units, reason, ok = shuffleUnits(t, shuffle, tree, runtimeStats)
		require.True(t, ok, reason)
		require.Equal(t, 14.0, readBillingDemoUnitValue(units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
		shuffle.Concurrency = 8
		concurrentUnits, concurrentReason, concurrentOK := shuffleUnits(t, shuffle, tree, runtimeStats)
		require.True(t, concurrentOK, concurrentReason)
		require.Equal(t, 14.0, readBillingDemoUnitValue(concurrentUnits, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))

		shuffle, tree, runtimeStats = buildShuffle([]int{0, 5}, []int{1, 1}, 4)
		units, reason, ok = shuffleUnits(t, shuffle, tree, runtimeStats)
		require.True(t, ok, reason)
		require.Equal(t, 10.0, readBillingDemoUnitValue(units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))

		missingStats := execdetails.NewRuntimeStatsColl(nil)
		recordRoot(missingStats, shuffle.DataSources[1].ID(), 5)
		_, reason, ok = shuffleUnits(t, shuffle, tree, missingStats)
		require.False(t, ok)
		require.Equal(t, readBillingDemoReasonMissingRuntimeRows, reason)

		shuffle.ByItemArrays = shuffle.ByItemArrays[:1]
		_, reason, ok = shuffleUnits(t, shuffle, tree, runtimeStats)
		require.False(t, ok)
		require.Equal(t, readBillingDemoReasonInvalidShuffleStructure, reason)
		shuffle.ByItemArrays = append(shuffle.ByItemArrays, []expression.Expression{col})

		shuffle.DataSources[1] = shuffle.DataSources[0]
		_, reason, ok = shuffleUnits(t, shuffle, tree, runtimeStats)
		require.False(t, ok)
		require.Equal(t, readBillingDemoReasonInvalidShuffleStructure, reason)

		shuffle, tree, runtimeStats = buildShuffle([]int{1}, []int{1}, 2)
		aliasedMask := newReadBillingDemoExecutionMask()
		aliasedMask.planIDOccurrences[shuffle.DataSources[0].ID()] = 2
		_, reason, ok = readBillingDemoRootUnits(runtimeStats, tree, 0, tree[0], readBillingDemoOperatorResult{opClass: readBillingDemoOpClassShuffle}, aliasedMask)
		require.False(t, ok)
		require.Equal(t, readBillingDemoReasonInvalidShuffleStructure, reason)

		overflowStats := execdetails.NewRuntimeStatsColl(nil)
		recordRoot(overflowStats, shuffle.DataSources[0].ID(), math.MaxInt64)
		_, reason, ok = shuffleUnits(t, shuffle, tree, overflowStats)
		require.False(t, ok)
		require.Equal(t, readBillingDemoReasonInvalidShuffleWork, reason)
	})

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
					require.Equal(t, -1.0, readBillingDemoUnitValue(units, readBillingDemoUnitInputRows, readBillingDemoInputSideAll))
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

	t.Run("lookup join accepts a proven skipped inner", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			innerIdx  int
			buildPlan func(physicalop.BasePhysicalJoin) base.PhysicalPlan
			exprCount float64
		}{
			{
				name:     "index join inner first",
				innerIdx: 0,
				buildPlan: func(baseJoin physicalop.BasePhysicalJoin) base.PhysicalPlan {
					return physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin}.Init(ctx, stats, 0)
				},
				exprCount: 2,
			},
			{
				name:     "index hash join inner last",
				innerIdx: 1,
				buildPlan: func(baseJoin physicalop.BasePhysicalJoin) base.PhysicalPlan {
					indexJoin := physicalop.PhysicalIndexJoin{
						BasePhysicalJoin: baseJoin,
						OuterHashKeys:    []*expression.Column{col},
						InnerHashKeys:    []*expression.Column{col},
					}.Init(ctx, stats, 0)
					return physicalop.PhysicalIndexHashJoin{PhysicalIndexJoin: *indexJoin}.Init(ctx)
				},
				exprCount: 2,
			},
			{
				name:     "index merge join inner first",
				innerIdx: 0,
				buildPlan: func(baseJoin physicalop.BasePhysicalJoin) base.PhysicalPlan {
					indexJoin := physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin}.Init(ctx, stats, 0)
					return physicalop.PhysicalIndexMergeJoin{
						PhysicalIndexJoin: *indexJoin,
						CompareFuncs:      []expression.CompareFunc{nil},
					}.Init(ctx)
				},
				exprCount: 2,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				baseJoin := physicalop.BasePhysicalJoin{
					InnerChildIdx:   tc.innerIdx,
					OuterJoinKeys:   []*expression.Column{col},
					InnerJoinKeys:   []*expression.Column{col},
					OtherConditions: expression.CNFExprs{col},
				}
				join := tc.buildPlan(baseJoin)
				outer := physicalop.PhysicalUnionAll{}.Init(ctx, stats, 0)
				inner := physicalop.PhysicalUnionAll{}.Init(ctx, stats, 0)
				outer.SetSchema(schema)
				inner.SetSchema(schema)
				children := []base.PhysicalPlan{outer, inner}
				if tc.innerIdx == 0 {
					children[0], children[1] = children[1], children[0]
				}
				join.SetChildren(children...)

				runtimeStats := execdetails.NewRuntimeStatsColl(nil)
				recordRoot(runtimeStats, join.ID(), 0)
				recordRoot(runtimeStats, outer.ID(), 0)
				runtimeStats.GetBasicRuntimeStats(inner.ID(), true)
				ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats

				result := buildReadBillingDemoExecutionResult(ctx, join, &ast.SelectStmt{}, nil, nil)
				require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
				var joinOperator *readBillingDemoOperatorResult
				for i := range result.operators {
					operator := &result.operators[i]
					require.NotEqual(t, inner.ExplainID().String(), operator.id)
					if operator.id == join.ExplainID().String() {
						joinOperator = operator
					}
				}
				require.NotNil(t, joinOperator)
				require.Equal(t, tc.exprCount, readBillingDemoUnitValue(joinOperator.units, readBillingDemoUnitExpressionCount, readBillingDemoInputSideAll))
				require.Zero(t, readBillingDemoUnitValue(joinOperator.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
				require.Zero(t, readBillingDemoUnitValue(joinOperator.units, readBillingDemoUnitJoinOutputRows, readBillingDemoInputSideAll))
			})
		}
	})

	t.Run("index lookup accepts a proven skipped table leg", func(t *testing.T) {
		indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
		tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
		tableScan.Table = &model.TableInfo{}
		indexScan.SetSchema(schema)
		tableScan.SetSchema(schema)
		lookup := (physicalop.PhysicalIndexLookUpReader{IndexPlan: indexScan, TablePlan: tableScan}).Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)
		flat := FlattenPhysicalPlan(lookup, false)
		require.NotNil(t, flat)
		require.Len(t, flat.Main, 3)

		runtimeStats := execdetails.NewRuntimeStatsColl(nil)
		recordRoot(runtimeStats, lookup.ID(), 0)
		recordCopSummary(runtimeStats, indexScan.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 1})
		ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddResourceManagerReadCnt(1)
		metrics.AddTiKVCoprocessorResponseBytes(10)

		result := buildReadBillingDemoExecutionResult(ctx, lookup, &ast.SelectStmt{}, nil, metrics)
		require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
		var indexScanSeen, transportSeen bool
		for _, operator := range result.operators {
			require.NotEqual(t, tableScan.ExplainID().String(), operator.id)
			switch operator.id {
			case indexScan.ExplainID().String():
				indexScanSeen = true
				require.Zero(t, readBillingDemoUnitValue(operator.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
			case "reader_transport@statement":
				transportSeen = true
				require.Equal(t, "index_lookup", operator.operatorKind)
				require.Equal(t, 10.0, readBillingDemoUnitValue(operator.units, readBillingDemoUnitNetBytes, readBillingDemoInputSideAll))
			}
		}
		require.True(t, indexScanSeen)
		require.True(t, transportSeen)
	})

	t.Run("index merge accepts a proven skipped table leg", func(t *testing.T) {
		type indexMergeSkipFixture struct {
			flat         *FlatPhysicalPlan
			runtime      *execdetails.RuntimeStatsColl
			merge        *physicalop.PhysicalIndexMergeReader
			mergeNode    *FlatOperator
			partialRoots []*FlatOperator
			tableRoot    *FlatOperator
		}
		newFixture := func(t *testing.T, intersection bool) indexMergeSkipFixture {
			t.Helper()
			partialOne := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			partialTwo := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
			tableScan.Table = &model.TableInfo{}
			partialOne.SetSchema(schema)
			partialTwo.SetSchema(schema)
			tableScan.SetSchema(schema)
			merge := physicalop.PhysicalIndexMergeReader{
				IsIntersectionType: intersection,
				PartialPlansRaw:    []base.PhysicalPlan{partialOne, partialTwo},
				TablePlan:          tableScan,
			}.Init(ctx, 0)
			fixture := indexMergeSkipFixture{
				flat:    FlattenPhysicalPlan(merge, false),
				runtime: execdetails.NewRuntimeStatsColl(nil),
				merge:   merge,
			}
			require.NotNil(t, fixture.flat)
			for _, node := range fixture.flat.Main {
				switch node.Origin {
				case merge:
					fixture.mergeNode = node
				case partialOne, partialTwo:
					fixture.partialRoots = append(fixture.partialRoots, node)
				case tableScan:
					fixture.tableRoot = node
				}
			}
			require.NotNil(t, fixture.mergeNode)
			require.Len(t, fixture.partialRoots, 2)
			require.NotNil(t, fixture.tableRoot)
			return fixture
		}
		recordProof := func(fixture indexMergeSkipFixture, partialRows []uint64) {
			recordRoot(fixture.runtime, fixture.merge.ID(), 0)
			require.Len(t, partialRows, len(fixture.partialRoots))
			for i, partial := range fixture.partialRoots {
				rows := partialRows[i]
				totalKeys := int64(rows)
				if totalKeys == 0 {
					totalKeys = 1
				}
				recordCopSummary(fixture.runtime, partial.Origin.ID(), rows, &tikvutil.ScanDetail{
					TotalKeys:         totalKeys,
					ProcessedKeys:     int64(rows),
					ProcessedKeysSize: int64(rows) * 8,
				})
			}
		}

		for _, tc := range []struct {
			name         string
			intersection bool
			partialRows  []uint64
		}{
			{name: "union all empty", partialRows: []uint64{0, 0}},
			{name: "intersection disjoint", intersection: true, partialRows: []uint64{2, 3}},
			{name: "intersection one empty", intersection: true, partialRows: []uint64{0, 3}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fixture := newFixture(t, tc.intersection)
				recordProof(fixture, tc.partialRows)
				ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = fixture.runtime
				metrics := execdetails.NewRUV2Metrics()
				metrics.AddResourceManagerReadCnt(1)
				metrics.AddTiKVCoprocessorResponseBytes(10)

				result := buildReadBillingDemoExecutionResult(ctx, fixture.merge, &ast.SelectStmt{}, nil, metrics)
				require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
				partialSeen, transportSeen := 0, 0
				mergeSeen := false
				for _, operator := range result.operators {
					require.NotEqual(t, fixture.tableRoot.ExplainID().String(), operator.id)
					if operator.id == fixture.merge.ExplainID().String() {
						mergeSeen = true
						require.Empty(t, operator.units)
					}
					for i, partial := range fixture.partialRoots {
						if operator.id == partial.ExplainID().String() {
							partialSeen++
							require.Equal(t, float64(tc.partialRows[i]*8), readBillingDemoUnitValue(operator.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
						}
					}
					if operator.id == "reader_transport@statement" {
						transportSeen++
						require.Equal(t, "index_merge", operator.operatorKind)
						require.Equal(t, 10.0, readBillingDemoUnitValue(operator.units, readBillingDemoUnitNetBytes, readBillingDemoInputSideAll))
					}
				}
				require.Equal(t, 2, partialSeen)
				require.Equal(t, 1, transportSeen)
				require.True(t, mergeSeen)
			})
		}

		t.Run("normal hit keeps the table leg", func(t *testing.T) {
			fixture := newFixture(t, false)
			recordRoot(fixture.runtime, fixture.merge.ID(), 1)
			for _, partial := range fixture.partialRoots {
				recordCopSummary(fixture.runtime, partial.Origin.ID(), 1, &tikvutil.ScanDetail{TotalKeys: 1, ProcessedKeys: 1, ProcessedKeysSize: 8})
			}
			recordCopSummary(fixture.runtime, fixture.tableRoot.Origin.ID(), 1, &tikvutil.ScanDetail{TotalKeys: 1, ProcessedKeys: 1, ProcessedKeysSize: 16})
			mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
			require.False(t, mask.isSkipped(fixture.tableRoot))
			ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = fixture.runtime
			metrics := execdetails.NewRUV2Metrics()
			metrics.AddResourceManagerReadCnt(1)
			metrics.AddTiKVCoprocessorResponseBytes(10)
			result := buildReadBillingDemoExecutionResult(ctx, fixture.merge, &ast.SelectStmt{}, nil, metrics)
			require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
			tableSeen := false
			for _, operator := range result.operators {
				tableSeen = tableSeen || operator.id == fixture.tableRoot.ExplainID().String()
			}
			require.True(t, tableSeen)
		})

		t.Run("table evidence is not masked", func(t *testing.T) {
			fixture := newFixture(t, false)
			recordProof(fixture, []uint64{0, 0})
			fixture.runtime.GetBasicRuntimeStats(fixture.tableRoot.Origin.ID(), true).RecordBytes(0, 0)
			mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
			require.False(t, mask.isSkipped(fixture.tableRoot))
		})

		t.Run("plan ID alias is not masked", func(t *testing.T) {
			fixture := newFixture(t, true)
			recordProof(fixture, []uint64{2, 3})
			alias := *fixture.tableRoot
			alias.ChildrenIdx = nil
			alias.ChildrenEndIdx = 0
			fixture.flat.CTEs = append(fixture.flat.CTEs, FlatPlanTree{&alias})
			mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
			require.False(t, mask.isSkipped(fixture.tableRoot))
		})

		for _, tc := range []struct {
			name   string
			mutate func(indexMergeSkipFixture)
		}{
			{
				name: "missing root completion",
				mutate: func(fixture indexMergeSkipFixture) {
					for _, partial := range fixture.partialRoots {
						recordCopSummary(fixture.runtime, partial.Origin.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 1})
					}
				},
			},
			{
				name: "nonzero root output",
				mutate: func(fixture indexMergeSkipFixture) {
					recordProof(fixture, []uint64{0, 0})
					recordRoot(fixture.runtime, fixture.merge.ID(), 1)
				},
			},
			{
				name: "partial summary coverage mismatch",
				mutate: func(fixture indexMergeSkipFixture) {
					recordProof(fixture, []uint64{0, 0})
					fixture.runtime.RecordExpectedCopTasks([]int{fixture.partialRoots[0].Origin.ID()})
				},
			},
			{
				name: "malformed partial label",
				mutate: func(fixture indexMergeSkipFixture) {
					recordProof(fixture, []uint64{0, 0})
					fixture.partialRoots[0].Label = ProbeSide
				},
			},
			{
				name: "first child gap in parent closure",
				mutate: func(fixture indexMergeSkipFixture) {
					recordProof(fixture, []uint64{0, 0})
					gapPlan := physicalop.PhysicalTableDual{RowCount: 0}.Init(ctx, stats, 0)
					gap := &FlatOperator{Origin: gapPlan, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB}
					original := fixture.flat.Main
					fixture.flat.Main = FlatPlanTree{original[0], gap, original[1], original[2], original[3]}
					fixture.mergeNode.ChildrenIdx = []int{2, 3, 4}
					fixture.mergeNode.ChildrenEndIdx = 4
					fixture.partialRoots[0].ChildrenEndIdx = 2
					fixture.partialRoots[1].ChildrenEndIdx = 3
					fixture.tableRoot.ChildrenEndIdx = 4
				},
			},
		} {
			t.Run(tc.name+" is not masked", func(t *testing.T) {
				fixture := newFixture(t, false)
				tc.mutate(fixture)
				mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
				require.False(t, mask.isSkipped(fixture.tableRoot))
			})
		}

		t.Run("whole table subtree masking respects descendant evidence", func(t *testing.T) {
			partialOne := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			partialTwo := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
			tableScan.Table = &model.TableInfo{}
			tableSelection := physicalop.PhysicalSelection{Conditions: []expression.Expression{col}}.Init(ctx, stats, 0)
			partialOne.SetSchema(schema)
			partialTwo.SetSchema(schema)
			tableScan.SetSchema(schema)
			tableSelection.SetChildren(tableScan)
			merge := physicalop.PhysicalIndexMergeReader{
				PartialPlansRaw: []base.PhysicalPlan{partialOne, partialTwo},
				TablePlan:       tableSelection,
			}.Init(ctx, 0)
			flat := FlattenPhysicalPlan(merge, false)
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			recordRoot(runtimeStats, merge.ID(), 0)
			recordCopSummary(runtimeStats, partialOne.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 1})
			recordCopSummary(runtimeStats, partialTwo.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 1})

			mask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
			for _, node := range flat.Main {
				if node.Origin == tableSelection || node.Origin == tableScan {
					require.True(t, mask.isSkipped(node))
				}
			}

			runtimeStats.GetBasicRuntimeStats(tableScan.ID(), true).RecordBytes(0, 0)
			mask = buildReadBillingDemoExecutionMask(flat, runtimeStats)
			for _, node := range flat.Main {
				if node.Origin == tableSelection || node.Origin == tableScan {
					require.False(t, mask.isSkipped(node))
				}
			}
		})
	})

	t.Run("exact zero limits mask unexecuted descendants", func(t *testing.T) {
		assertFrontend := func(t *testing.T, result readBillingDemoResult, sql string) {
			t.Helper()
			for _, operator := range result.operators {
				if operator.id == "frontend@statement" {
					require.Equal(t, float64(len(sql)), readBillingDemoUnitValue(operator.units, readBillingDemoUnitFrontendCompileBytes, readBillingDemoInputSideAll))
					return
				}
			}
			require.Fail(t, "frontend unit is missing")
		}
		newStmt := func(sql string) *ast.SelectStmt {
			stmt := &ast.SelectStmt{}
			stmt.SetText(nil, sql)
			return stmt
		}
		newZeroLookup := func() *physicalop.PhysicalIndexLookUpReader {
			indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
			tableScan.Table = &model.TableInfo{}
			indexScan.SetSchema(schema)
			tableScan.SetSchema(schema)
			lookup := (physicalop.PhysicalIndexLookUpReader{
				IndexPlan:   indexScan,
				TablePlan:   tableScan,
				PushedLimit: &physicalop.PushedDownLimit{Count: 0},
			}).Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)
			lookup.SetSchema(schema)
			return lookup
		}
		newZeroMerge := func() *physicalop.PhysicalIndexMergeReader {
			partialOne := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			partialTwo := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
			tableScan.Table = &model.TableInfo{}
			partialOne.SetSchema(schema)
			partialTwo.SetSchema(schema)
			tableScan.SetSchema(schema)
			merge := physicalop.PhysicalIndexMergeReader{
				PartialPlansRaw: []base.PhysicalPlan{partialOne, partialTwo},
				TablePlan:       tableScan,
				PushedLimit:     &physicalop.PushedDownLimit{Count: 0},
			}.Init(ctx, 0)
			merge.SetSchema(schema)
			return merge
		}

		t.Run("explicit physical limit", func(t *testing.T) {
			child := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
			child.SetSchema(schema)
			limit := physicalop.PhysicalLimit{Offset: 7, Count: 0}.Init(ctx, stats, 0)
			limit.SetChildren(child)
			limit.SetSchema(schema)
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats
			const sql = "select a from t limit 7, 0"

			result := buildReadBillingDemoResult(ctx, limit, newStmt(sql), nil, execdetails.NewRUV2Metrics())
			require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
			limitSeen := false
			for _, operator := range result.operators {
				require.NotEqual(t, child.ExplainID().String(), operator.id)
				if operator.id == limit.ExplainID().String() {
					limitSeen = true
					require.Zero(t, readBillingDemoUnitValue(operator.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
					require.Equal(t, readBillingDemoInputSourcePhysicalPlan, readBillingDemoUnitSource(operator.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
				}
			}
			require.True(t, limitSeen)
			assertFrontend(t, result, sql)
		})

		for _, tc := range []struct {
			name string
			plan func() base.PhysicalPlan
		}{
			{
				name: "embedded index lookup limit",
				plan: func() base.PhysicalPlan {
					indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
					tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
					tableScan.Table = &model.TableInfo{}
					indexScan.SetSchema(schema)
					tableScan.SetSchema(schema)
					indexScan.SetStats(stats)
					tableScan.SetStats(stats)
					lookup := (physicalop.PhysicalIndexLookUpReader{
						IndexPlan:   indexScan,
						TablePlan:   tableScan,
						PushedLimit: &physicalop.PushedDownLimit{Offset: 9, Count: 0},
					}).Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)
					lookup.SetSchema(schema)
					return lookup
				},
			},
			{
				name: "embedded pushdown index lookup limit",
				plan: func() base.PhysicalPlan {
					indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
					tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
					tableScan.Table = &model.TableInfo{}
					indexScan.SetSchema(schema)
					tableScan.SetSchema(schema)
					indexScan.SetStats(stats)
					tableScan.SetStats(stats)
					lookup := (physicalop.PhysicalIndexLookUpReader{
						IndexPlan:   indexScan,
						TablePlan:   tableScan,
						PushedLimit: &physicalop.PushedDownLimit{Count: 0},
					}).Init(ctx, 0, plannerutil.IndexLookUpPushDownByHint)
					require.True(t, lookup.IndexLookUpPushDown)
					lookup.SetSchema(schema)
					return lookup
				},
			},
			{
				name: "embedded index merge limit",
				plan: func() base.PhysicalPlan {
					partialOne := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
					partialTwo := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
					tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
					tableScan.Table = &model.TableInfo{}
					partialOne.SetSchema(schema)
					partialTwo.SetSchema(schema)
					tableScan.SetSchema(schema)
					merge := physicalop.PhysicalIndexMergeReader{
						PartialPlansRaw: []base.PhysicalPlan{partialOne, partialTwo},
						TablePlan:       tableScan,
						PushedLimit:     &physicalop.PushedDownLimit{Offset: 11, Count: 0},
					}.Init(ctx, 0)
					merge.SetSchema(schema)
					return merge
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				plan := tc.plan()
				flat := FlattenPhysicalPlan(plan, true)
				require.NotNil(t, flat)
				require.Greater(t, len(flat.Main), 1)
				runtimeStats := execdetails.NewRuntimeStatsColl(nil)
				ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats
				sql := "select /* " + tc.name + " */ a from t limit 0"

				result := buildReadBillingDemoResult(ctx, plan, newStmt(sql), nil, execdetails.NewRUV2Metrics())
				require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
				for _, operator := range result.operators {
					require.NotEqual(t, "reader_transport@statement", operator.id)
					require.NotEqual(t, "point_lookup@statement", operator.id)
					for _, descendant := range flat.Main[1:] {
						require.NotEqual(t, descendant.ExplainID().String(), operator.id)
					}
				}
				assertFrontend(t, result, sql)
			})
		}

		t.Run("parents consume masked direct children as zero", func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				build    func() base.PhysicalPlan
				explicit bool
			}{
				{
					name: "projection over physical limit",
					build: func() base.PhysicalPlan {
						leaf := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
						leaf.SetSchema(schema)
						limit := physicalop.PhysicalLimit{Count: 0}.Init(ctx, stats, 0)
						limit.SetChildren(leaf)
						limit.SetSchema(schema)
						projection := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
						projection.SetChildren(limit)
						projection.SetSchema(schema)
						return projection
					},
					explicit: true,
				},
				{
					name: "selection over embedded index lookup",
					build: func() base.PhysicalPlan {
						selection := physicalop.PhysicalSelection{Conditions: []expression.Expression{col}}.Init(ctx, stats, 0)
						selection.SetChildren(newZeroLookup())
						return selection
					},
				},
				{
					name: "projection over embedded index merge",
					build: func() base.PhysicalPlan {
						projection := physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}.Init(ctx, stats, 0)
						projection.SetChildren(newZeroMerge())
						projection.SetSchema(schema)
						return projection
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					plan := tc.build()
					flat := FlattenPhysicalPlan(plan, true)
					require.NotNil(t, flat)
					require.Len(t, flat.Main[0].ChildrenIdx, 1)
					childIdx := flat.Main[0].ChildrenIdx[0]
					child := flat.Main[childIdx]
					runtimeStats := execdetails.NewRuntimeStatsColl(nil)
					recordRoot(runtimeStats, plan.ID(), 0)
					ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats
					mask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
					if tc.explicit {
						require.True(t, mask.isExplicitZeroLimit(child))
					} else {
						require.True(t, mask.suppressesTransportProducer(child))
					}
					for idx := childIdx + 1; idx <= child.ChildrenEndIdx; idx++ {
						require.True(t, mask.isSkipped(flat.Main[idx]))
					}

					result := buildReadBillingDemoExecutionResult(ctx, plan, &ast.SelectStmt{}, nil, execdetails.NewRUV2Metrics())
					require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
					parentSeen := false
					for _, operator := range result.operators {
						require.NotEqual(t, "reader_transport@statement", operator.id)
						if operator.id == plan.ExplainID().String() {
							parentSeen = true
							require.Zero(t, readBillingDemoUnitValue(operator.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
							require.Equal(t, readBillingDemoInputSourcePhysicalPlan, readBillingDemoUnitSource(operator.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
						}
					}
					require.True(t, parentSeen)
				})
			}
		})

		t.Run("join preserves the active side beside a masked child", func(t *testing.T) {
			active := physicalop.PhysicalTableDual{RowCount: 5}.Init(ctx, stats, 0)
			active.SetSchema(schema)
			zeroLookup := newZeroLookup()
			join := physicalop.PhysicalMergeJoin{
				BasePhysicalJoin: physicalop.BasePhysicalJoin{OtherConditions: expression.CNFExprs{col}},
			}.Init(ctx, stats, 0)
			join.SetChildren(active, zeroLookup)
			join.SetSchema(schema)
			flat := FlattenPhysicalPlan(join, true)
			require.NotNil(t, flat)
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			recordRoot(runtimeStats, join.ID(), 0)
			recordRoot(runtimeStats, active.ID(), 5)
			ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats
			mask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
			zeroChildSeen := false
			for _, childIdx := range flat.Main[0].ChildrenIdx {
				if flat.Main[childIdx].Origin == zeroLookup {
					zeroChildSeen = true
					require.True(t, mask.suppressesTransportProducer(flat.Main[childIdx]))
				}
			}
			require.True(t, zeroChildSeen)

			result := buildReadBillingDemoExecutionResult(ctx, join, &ast.SelectStmt{}, nil, execdetails.NewRUV2Metrics())
			require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
			joinSeen := false
			for _, operator := range result.operators {
				require.NotEqual(t, "reader_transport@statement", operator.id)
				if operator.id == join.ExplainID().String() {
					joinSeen = true
					require.Equal(t, 5.0, readBillingDemoUnitValue(operator.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
					require.Equal(t, readBillingDemoInputSourceRuntimeChildActRows, readBillingDemoUnitSource(operator.units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
					require.Zero(t, readBillingDemoUnitValue(operator.units, readBillingDemoUnitJoinOutputRows, readBillingDemoInputSideAll))
				}
			}
			require.True(t, joinSeen)
		})

		t.Run("contradictory and malformed candidates remain active", func(t *testing.T) {
			type fixture struct {
				lookup  *physicalop.PhysicalIndexLookUpReader
				flat    *FlatPhysicalPlan
				runtime *execdetails.RuntimeStatsColl
			}
			newFixture := func(t *testing.T) fixture {
				t.Helper()
				indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
				tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
				tableScan.Table = &model.TableInfo{}
				indexScan.SetSchema(schema)
				tableScan.SetSchema(schema)
				lookup := (physicalop.PhysicalIndexLookUpReader{
					IndexPlan:   indexScan,
					TablePlan:   tableScan,
					PushedLimit: &physicalop.PushedDownLimit{Count: 0},
				}).Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)
				lookup.SetSchema(schema)
				flat := FlattenPhysicalPlan(lookup, true)
				require.NotNil(t, flat)
				return fixture{lookup: lookup, flat: flat, runtime: execdetails.NewRuntimeStatsColl(nil)}
			}
			assertActive := func(t *testing.T, fixture fixture) {
				t.Helper()
				mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
				for _, node := range fixture.flat.Main {
					require.False(t, mask.isSkipped(node))
					require.False(t, mask.suppressesTransportProducer(node))
				}
			}

			for _, tc := range []struct {
				name   string
				mutate func(fixture)
			}{
				{
					name: "positive count",
					mutate: func(fixture fixture) {
						fixture.lookup.PushedLimit.Count = 1
					},
				},
				{
					name: "nonzero root output",
					mutate: func(fixture fixture) {
						recordRoot(fixture.runtime, fixture.lookup.ID(), 1)
					},
				},
				{
					name: "descendant byte evidence",
					mutate: func(fixture fixture) {
						fixture.runtime.GetBasicRuntimeStats(fixture.flat.Main[1].Origin.ID(), true).RecordBytes(0, 0)
					},
				},
				{
					name: "plan ID alias",
					mutate: func(fixture fixture) {
						alias := *fixture.flat.Main[1]
						alias.ChildrenIdx = nil
						alias.ChildrenEndIdx = 0
						fixture.flat.CTEs = append(fixture.flat.CTEs, FlatPlanTree{&alias})
					},
				},
				{
					name: "first child gap",
					mutate: func(fixture fixture) {
						gapPlan := physicalop.PhysicalTableDual{RowCount: 0}.Init(ctx, stats, 0)
						gap := &FlatOperator{Origin: gapPlan, ChildrenEndIdx: 1, IsRoot: true, StoreType: kv.TiDB}
						original := fixture.flat.Main
						fixture.flat.Main = FlatPlanTree{original[0], gap, original[1], original[2]}
						fixture.flat.Main[0].ChildrenIdx = []int{2, 3}
						fixture.flat.Main[0].ChildrenEndIdx = 3
						fixture.flat.Main[2].ChildrenEndIdx = 2
						fixture.flat.Main[3].ChildrenEndIdx = 3
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					fixture := newFixture(t)
					tc.mutate(fixture)
					assertActive(t, fixture)
				})
			}
		})

		t.Run("nested exact zero candidates keep the outermost", func(t *testing.T) {
			for _, tc := range []struct {
				name string
				plan func() base.PhysicalPlan
			}{
				{
					name: "limit over embedded lookup",
					plan: func() base.PhysicalPlan {
						limit := physicalop.PhysicalLimit{Count: 0}.Init(ctx, stats, 0)
						limit.SetChildren(newZeroLookup())
						limit.SetSchema(schema)
						return limit
					},
				},
				{
					name: "nested physical limits",
					plan: func() base.PhysicalPlan {
						leaf := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
						leaf.SetSchema(schema)
						inner := physicalop.PhysicalLimit{Count: 0}.Init(ctx, stats, 0)
						inner.SetChildren(leaf)
						inner.SetSchema(schema)
						outer := physicalop.PhysicalLimit{Count: 0}.Init(ctx, stats, 0)
						outer.SetChildren(inner)
						outer.SetSchema(schema)
						return outer
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					plan := tc.plan()
					flat := FlattenPhysicalPlan(plan, true)
					require.NotNil(t, flat)
					runtimeStats := execdetails.NewRuntimeStatsColl(nil)
					ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = runtimeStats
					mask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
					require.True(t, mask.isExplicitZeroLimit(flat.Main[0]))
					require.False(t, mask.suppressesTransportProducer(flat.Main[0]))
					for _, node := range flat.Main[1:] {
						require.True(t, mask.isSkipped(node))
						require.False(t, mask.isExplicitZeroLimit(node))
						require.False(t, mask.suppressesTransportProducer(node))
					}

					result := buildReadBillingDemoExecutionResult(ctx, plan, &ast.SelectStmt{}, nil, execdetails.NewRUV2Metrics())
					require.Equal(t, readBillingDemoStatusSuccess, result.status, result.reason)
					require.Len(t, result.operators, 1)
					require.Equal(t, plan.ExplainID().String(), result.operators[0].id)
					require.Zero(t, readBillingDemoUnitValue(result.operators[0].units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
					require.Equal(t, readBillingDemoInputSourcePhysicalPlan, readBillingDemoUnitSource(result.operators[0].units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
				})
			}
		})

		t.Run("independent real reader remains billable", func(t *testing.T) {
			zeroIndexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			zeroTableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
			zeroTableScan.Table = &model.TableInfo{}
			zeroIndexScan.SetSchema(schema)
			zeroTableScan.SetSchema(schema)
			zeroLookup := (physicalop.PhysicalIndexLookUpReader{
				IndexPlan:   zeroIndexScan,
				TablePlan:   zeroTableScan,
				PushedLimit: &physicalop.PushedDownLimit{Count: 0},
			}).Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)
			zeroLookup.SetSchema(schema)

			realScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
			realScan.Table = &model.TableInfo{}
			realScan.SetSchema(schema)
			realReader := physicalop.PhysicalTableReader{StoreType: kv.TiKV, TablePlan: realScan}.Init(ctx, 0)
			realReader.SetSchema(schema)
			zeroFlat := FlattenPhysicalPlan(zeroLookup, true)
			realFlat := FlattenPhysicalPlan(realReader, true)
			flat := &FlatPhysicalPlan{Main: zeroFlat.Main, ScalarSubQueries: []FlatPlanTree{realFlat.Main}}
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			recordRoot(runtimeStats, realReader.ID(), 1)
			recordCopSummary(runtimeStats, realScan.ID(), 1, &tikvutil.ScanDetail{TotalKeys: 1, ProcessedKeys: 1, ProcessedKeysSize: 8})
			mask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
			require.True(t, mask.suppressesTransportProducer(zeroFlat.Main[0]))
			for _, node := range zeroFlat.Main[1:] {
				require.True(t, mask.isSkipped(node))
			}
			for _, node := range realFlat.Main {
				require.False(t, mask.isSkipped(node))
				require.False(t, mask.suppressesTransportProducer(node))
			}

			result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
			status, operator := appendReadBillingDemoTree(&result, ctx, runtimeStats, zeroFlat.Main, mask)
			require.Equal(t, readBillingDemoStatusSuccess, status, operator.reason)
			status, operator = appendReadBillingDemoTree(&result, ctx, runtimeStats, realFlat.Main, mask)
			require.Equal(t, readBillingDemoStatusSuccess, status, operator.reason)
			metrics := execdetails.NewRUV2Metrics()
			metrics.AddResourceManagerReadCnt(1)
			metrics.AddTiKVCoprocessorResponseBytes(10)
			transport, present := readBillingDemoReaderTransport(flat, runtimeStats, metrics, false, mask)
			require.True(t, present)
			require.Equal(t, readBillingDemoStatusOperatorOK, transport.status, transport.reason)
			require.Equal(t, "table_reader", transport.operatorKind)
			require.Equal(t, 10.0, readBillingDemoUnitValue(transport.units, readBillingDemoUnitNetBytes, readBillingDemoInputSideAll))
			realScanSeen := false
			for _, operator := range result.operators {
				if operator.id == realScan.ExplainID().String() {
					realScanSeen = true
					require.Equal(t, 8.0, readBillingDemoUnitValue(operator.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
				}
			}
			require.True(t, realScanSeen)
		})
	})

	t.Run("index lookup skipped table proof is fail closed", func(t *testing.T) {
		type indexLookupSkipFixture struct {
			flat       *FlatPhysicalPlan
			runtime    *execdetails.RuntimeStatsColl
			lookup     *physicalop.PhysicalIndexLookUpReader
			lookupNode *FlatOperator
			indexRoot  *FlatOperator
			tableRoot  *FlatOperator
		}
		newFixture := func(t *testing.T) indexLookupSkipFixture {
			t.Helper()
			indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
			tableScan.Table = &model.TableInfo{}
			indexScan.SetSchema(schema)
			tableScan.SetSchema(schema)
			lookup := (physicalop.PhysicalIndexLookUpReader{IndexPlan: indexScan, TablePlan: tableScan}).Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)
			fixture := indexLookupSkipFixture{
				flat:    FlattenPhysicalPlan(lookup, false),
				runtime: execdetails.NewRuntimeStatsColl(nil),
				lookup:  lookup,
			}
			require.NotNil(t, fixture.flat)
			for _, node := range fixture.flat.Main {
				switch node.Origin {
				case lookup:
					fixture.lookupNode = node
				case indexScan:
					fixture.indexRoot = node
				case tableScan:
					fixture.tableRoot = node
				}
			}
			require.NotNil(t, fixture.lookupNode)
			require.NotNil(t, fixture.indexRoot)
			require.NotNil(t, fixture.tableRoot)
			return fixture
		}
		recordProof := func(fixture indexLookupSkipFixture) {
			recordRoot(fixture.runtime, fixture.lookup.ID(), 0)
			recordCopSummary(fixture.runtime, fixture.indexRoot.Origin.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 1})
		}
		assertNotSkipped := func(t *testing.T, fixture indexLookupSkipFixture) {
			t.Helper()
			mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
			require.False(t, mask.isSkipped(fixture.lookupNode))
			require.False(t, mask.isSkipped(fixture.indexRoot))
			require.False(t, mask.isSkipped(fixture.tableRoot))
		}

		for _, tc := range []struct {
			name   string
			mutate func(indexLookupSkipFixture)
		}{
			{
				name: "lookup zero rows without byte evidence",
				mutate: func(fixture indexLookupSkipFixture) {
					fixture.runtime.GetBasicRuntimeStats(fixture.lookup.ID(), true).Record(time.Millisecond, 0)
					recordCopSummary(fixture.runtime, fixture.indexRoot.Origin.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 1})
				},
			},
			{
				name: "index root produced rows",
				mutate: func(fixture indexLookupSkipFixture) {
					recordRoot(fixture.runtime, fixture.lookup.ID(), 0)
					recordCopSummary(fixture.runtime, fixture.indexRoot.Origin.ID(), 1, &tikvutil.ScanDetail{TotalKeys: 1, ProcessedKeys: 1, ProcessedKeysSize: 8})
				},
			},
			{
				name: "lookup root produced rows",
				mutate: func(fixture indexLookupSkipFixture) {
					recordRoot(fixture.runtime, fixture.lookup.ID(), 1)
					recordCopSummary(fixture.runtime, fixture.indexRoot.Origin.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 1})
				},
			},
			{
				name: "index summary coverage is incomplete",
				mutate: func(fixture indexLookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.RecordExpectedCopTasks([]int{fixture.indexRoot.Origin.ID()})
				},
			},
			{
				name: "table root basic byte evidence",
				mutate: func(fixture indexLookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.GetBasicRuntimeStats(fixture.tableRoot.Origin.ID(), true).RecordBytes(0, 0)
				},
			},
			{
				name: "table root expected task evidence",
				mutate: func(fixture indexLookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.RecordExpectedCopTasks([]int{fixture.tableRoot.Origin.ID()})
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fixture := newFixture(t)
				tc.mutate(fixture)
				assertNotSkipped(t, fixture)
			})
		}

		t.Run("plan ID alias", func(t *testing.T) {
			fixture := newFixture(t)
			recordProof(fixture)
			alias := *fixture.tableRoot
			alias.ChildrenIdx = nil
			alias.ChildrenEndIdx = 0
			fixture.flat.CTEs = append(fixture.flat.CTEs, FlatPlanTree{&alias})
			assertNotSkipped(t, fixture)
		})

		t.Run("pushdown remains unsupported by this mask", func(t *testing.T) {
			fixture := newFixture(t)
			fixture.lookup.IndexLookUpPushDown = true
			recordRoot(fixture.runtime, fixture.lookup.ID(), 2)
			recordCopSummary(fixture.runtime, fixture.indexRoot.Origin.ID(), 2, &tikvutil.ScanDetail{TotalKeys: 2, ProcessedKeys: 2, ProcessedKeysSize: 16})
			assertNotSkipped(t, fixture)
		})

		t.Run("whole table subtree is skipped", func(t *testing.T) {
			indexScan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			tableScan := physicalop.PhysicalTableScan{}.Init(ctx, 0)
			tableScan.Table = &model.TableInfo{}
			tableSelection := physicalop.PhysicalSelection{Conditions: []expression.Expression{col}}.Init(ctx, stats, 0)
			indexScan.SetSchema(schema)
			tableScan.SetSchema(schema)
			tableSelection.SetChildren(tableScan)
			lookup := (physicalop.PhysicalIndexLookUpReader{IndexPlan: indexScan, TablePlan: tableSelection}).Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)
			flat := FlattenPhysicalPlan(lookup, false)
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			recordRoot(runtimeStats, lookup.ID(), 0)
			recordCopSummary(runtimeStats, indexScan.ID(), 0, &tikvutil.ScanDetail{TotalKeys: 1})

			mask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
			for _, node := range flat.Main {
				switch node.Origin {
				case lookup, indexScan:
					require.False(t, mask.isSkipped(node))
				case tableSelection, tableScan:
					require.True(t, mask.isSkipped(node))
				}
			}
		})
	})

	t.Run("lookup join skipped inner proof is fail closed", func(t *testing.T) {
		type lookupSkipFixture struct {
			flat       *FlatPhysicalPlan
			runtime    *execdetails.RuntimeStatsColl
			join       *FlatOperator
			outer      *FlatOperator
			innerRoot  *FlatOperator
			innerCop   *FlatOperator
			innerCopID int
		}
		newFixture := func(t *testing.T) lookupSkipFixture {
			t.Helper()
			join := physicalop.PhysicalIndexJoin{BasePhysicalJoin: physicalop.BasePhysicalJoin{
				InnerChildIdx:   1,
				OuterJoinKeys:   []*expression.Column{col},
				InnerJoinKeys:   []*expression.Column{col},
				OtherConditions: expression.CNFExprs{col},
			}}.Init(ctx, stats, 0)
			outer := physicalop.PhysicalUnionAll{}.Init(ctx, stats, 0)
			scan := physicalop.PhysicalIndexScan{}.Init(ctx, 0)
			reader := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
			outer.SetSchema(schema)
			scan.SetSchema(schema)
			reader.SetSchema(schema)
			reader.TablePlan = scan
			join.SetChildren(outer, reader)

			fixture := lookupSkipFixture{
				flat:       FlattenPhysicalPlan(join, false),
				runtime:    execdetails.NewRuntimeStatsColl(nil),
				innerCopID: scan.ID(),
			}
			require.NotNil(t, fixture.flat)
			for _, node := range fixture.flat.Main {
				switch node.Origin {
				case join:
					fixture.join = node
				case outer:
					fixture.outer = node
				case reader:
					fixture.innerRoot = node
				case scan:
					fixture.innerCop = node
				}
			}
			require.NotNil(t, fixture.join)
			require.NotNil(t, fixture.outer)
			require.NotNil(t, fixture.innerRoot)
			require.NotNil(t, fixture.innerCop)
			return fixture
		}
		recordCompletedZero := func(runtimeStats *execdetails.RuntimeStatsColl, planID int, withBytes bool) {
			basic := runtimeStats.GetBasicRuntimeStats(planID, true)
			basic.Record(time.Millisecond, 0)
			if withBytes {
				basic.RecordBytes(0, 0)
			}
		}
		recordProof := func(fixture lookupSkipFixture) {
			recordCompletedZero(fixture.runtime, fixture.join.Origin.ID(), true)
			recordCompletedZero(fixture.runtime, fixture.outer.Origin.ID(), true)
		}
		assertNotSkipped := func(t *testing.T, fixture lookupSkipFixture) {
			t.Helper()
			mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
			require.False(t, mask.isSkipped(fixture.innerRoot))
			require.False(t, mask.isSkipped(fixture.innerCop))
			require.False(t, mask.isSkippedInner(fixture.join, fixture.innerRoot))
		}
		candidateForFixture := func(t *testing.T, fixture lookupSkipFixture, treeOrdinal int) readBillingDemoSkipCandidate {
			t.Helper()
			for idx, node := range fixture.flat.Main {
				if node != fixture.join {
					continue
				}
				candidate, ok := readBillingDemoSkipCandidateAt(fixture.flat.Main, treeOrdinal, idx)
				require.True(t, ok)
				return candidate
			}
			require.FailNow(t, "missing lookup join occurrence")
			return readBillingDemoSkipCandidate{}
		}

		for _, tc := range []struct {
			name   string
			mutate func(lookupSkipFixture)
		}{
			{
				name: "join zero rows without byte evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordCompletedZero(fixture.runtime, fixture.join.Origin.ID(), false)
					recordCompletedZero(fixture.runtime, fixture.outer.Origin.ID(), true)
				},
			},
			{
				name: "outer zero rows without byte evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordCompletedZero(fixture.runtime, fixture.join.Origin.ID(), true)
					recordCompletedZero(fixture.runtime, fixture.outer.Origin.ID(), false)
				},
			},
			{
				name: "inner root basic byte evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.GetBasicRuntimeStats(fixture.innerRoot.Origin.ID(), true).RecordBytes(0, 0)
				},
			},
			{
				name: "inner cop basic byte evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.GetBasicRuntimeStats(fixture.innerCopID, true).RecordBytes(0, 0)
				},
			},
			{
				name: "inner cop expected task evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.RecordExpectedCopTasks([]int{fixture.innerCopID})
				},
			},
			{
				name: "inner cop observed summary evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					zero, one := uint64(0), uint64(1)
					fixture.runtime.RecordOneCopTask(fixture.innerCopID, kv.TiKV, &tipb.ExecutorExecutionSummary{
						TimeProcessedNs: &one,
						NumProducedRows: &zero,
						NumIterations:   &one,
					})
				},
			},
			{
				name: "inner cop scan detail evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.RecordCopStats(fixture.innerCopID, kv.TiKV, &tikvutil.ScanDetail{}, tikvutil.TimeDetail{}, nil)
				},
			},
			{
				name: "inner select result group evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.RegisterStats(fixture.innerRoot.Origin.ID(), &readBillingDemoRPCStatsForTest{
						counts: map[tikvrpc.CmdType]int64{},
						tp:     execdetails.TpSelectResultRuntimeStats,
					})
				},
			},
			{
				name: "inner snapshot group evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.RegisterStats(fixture.innerRoot.Origin.ID(), &readBillingDemoRPCStatsForTest{
						counts: map[tikvrpc.CmdType]int64{},
						tp:     execdetails.TpRuntimeStatsWithSnapshot,
					})
				},
			},
			{
				name: "inner cop select result group evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.RegisterStats(fixture.innerCopID, &readBillingDemoRPCStatsForTest{
						counts: map[tikvrpc.CmdType]int64{},
						tp:     execdetails.TpSelectResultRuntimeStats,
					})
				},
			},
			{
				name: "inner cop snapshot group evidence",
				mutate: func(fixture lookupSkipFixture) {
					recordProof(fixture)
					fixture.runtime.RegisterStats(fixture.innerCopID, &readBillingDemoRPCStatsForTest{
						counts: map[tikvrpc.CmdType]int64{},
						tp:     execdetails.TpRuntimeStatsWithSnapshot,
					})
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fixture := newFixture(t)
				tc.mutate(fixture)
				assertNotSkipped(t, fixture)
			})
		}

		t.Run("whole inner reader subtree is skipped", func(t *testing.T) {
			fixture := newFixture(t)
			recordProof(fixture)
			mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
			require.True(t, mask.isSkipped(fixture.innerRoot))
			require.True(t, mask.isSkipped(fixture.innerCop))
			require.True(t, mask.isSkippedInner(fixture.join, fixture.innerRoot))
		})

		t.Run("multiple execution masks are rejected", func(t *testing.T) {
			fixture := newFixture(t)
			recordProof(fixture)
			mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)

			estimator := newReadBillingDemoCopEstimator(fixture.flat.Main, fixture.runtime, mask, mask)
			failure, ok := estimator.firstTreeFailure()
			require.True(t, ok)
			require.Equal(t, readBillingDemoReasonUnsupportedCopStructure, failure.reason)

			transport, present := readBillingDemoReaderTransport(
				fixture.flat,
				fixture.runtime,
				execdetails.NewRUV2Metrics(),
				false,
				mask,
				mask,
			)
			require.True(t, present)
			require.Equal(t, readBillingDemoStatusUnknownInput, transport.status)
			require.Equal(t, readBillingDemoReasonUnsupportedCopStructure, transport.reason)
		})

		for _, tc := range []struct {
			name  string
			alias func(*FlatPhysicalPlan, FlatPlanTree)
		}{
			{
				name: "cte plan ID alias",
				alias: func(flat *FlatPhysicalPlan, tree FlatPlanTree) {
					flat.CTEs = append(flat.CTEs, tree)
				},
			},
			{
				name: "scalar subquery plan ID alias",
				alias: func(flat *FlatPhysicalPlan, tree FlatPlanTree) {
					flat.ScalarSubQueries = append(flat.ScalarSubQueries, tree)
				},
			},
		} {
			t.Run(tc.name+" keeps active transport", func(t *testing.T) {
				fixture := newFixture(t)
				recordProof(fixture)
				alias := *fixture.innerRoot
				alias.ChildrenIdx = nil
				alias.ChildrenEndIdx = 0
				tc.alias(fixture.flat, FlatPlanTree{&alias})

				mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
				require.False(t, mask.isSkipped(fixture.innerRoot))
				metrics := execdetails.NewRUV2Metrics()
				metrics.AddResourceManagerReadCnt(1)
				metrics.AddTiKVCoprocessorResponseBytes(10)
				transport, present := readBillingDemoReaderTransport(fixture.flat, fixture.runtime, metrics, false, mask)
				require.True(t, present)
				require.Equal(t, readBillingDemoStatusOperatorOK, transport.status)
				require.Equal(t, "table_reader", transport.operatorKind)
				require.Len(t, transport.units, 1)
				require.Equal(t, 10.0, readBillingDemoUnitValue(transport.units, readBillingDemoUnitNetBytes, readBillingDemoInputSideAll))
			})
		}

		t.Run("independent safe candidates are both retained", func(t *testing.T) {
			mainFixture := newFixture(t)
			cteFixture := newFixture(t)
			runtimeStats := execdetails.NewRuntimeStatsColl(nil)
			for _, fixture := range []lookupSkipFixture{mainFixture, cteFixture} {
				recordCompletedZero(runtimeStats, fixture.join.Origin.ID(), true)
				recordCompletedZero(runtimeStats, fixture.outer.Origin.ID(), true)
			}
			flat := &FlatPhysicalPlan{
				Main: mainFixture.flat.Main,
				CTEs: []FlatPlanTree{cteFixture.flat.Main},
			}
			mask := buildReadBillingDemoExecutionMask(flat, runtimeStats)
			require.True(t, mask.isSkipped(mainFixture.innerRoot))
			require.True(t, mask.isSkipped(mainFixture.innerCop))
			require.True(t, mask.isSkipped(cteFixture.innerRoot))
			require.True(t, mask.isSkipped(cteFixture.innerCop))
		})

		t.Run("one conflict domain does not clear an independent candidate", func(t *testing.T) {
			conflictingFixture := newFixture(t)
			independentFixture := newFixture(t)
			conflicting := candidateForFixture(t, conflictingFixture, 0)
			independent := candidateForFixture(t, independentFixture, 1)
			nested := readBillingDemoSkipCandidate{
				treeOrdinal: conflicting.treeOrdinal,
				join: readBillingDemoPlanOccurrence{
					treeOrdinal: conflicting.treeOrdinal,
					idx:         conflicting.innerStart,
					node:        conflicting.innerRoot.node,
				},
				outer: readBillingDemoPlanOccurrence{
					treeOrdinal: conflicting.treeOrdinal,
					idx:         conflicting.innerEnd,
					node:        conflicting.innerNodes[len(conflicting.innerNodes)-1],
				},
				innerRoot: readBillingDemoPlanOccurrence{
					treeOrdinal: conflicting.treeOrdinal,
					idx:         conflicting.innerEnd,
					node:        conflicting.innerNodes[len(conflicting.innerNodes)-1],
				},
				innerStart: conflicting.innerEnd,
				innerEnd:   conflicting.innerEnd,
				innerNodes: []*FlatOperator{conflicting.innerNodes[len(conflicting.innerNodes)-1]},
			}
			survivors := readBillingDemoRemoveConflictingSkipCandidates([]readBillingDemoSkipCandidate{
				conflicting,
				nested,
				independent,
			})
			require.Equal(t, []readBillingDemoSkipCandidate{independent}, survivors)
		})

		t.Run("non laminar intervals are removed symmetrically", func(t *testing.T) {
			leftFixture := newFixture(t)
			rightFixture := newFixture(t)
			independentFixture := newFixture(t)
			left := candidateForFixture(t, leftFixture, 0)
			right := candidateForFixture(t, rightFixture, 0)
			independent := candidateForFixture(t, independentFixture, 1)
			left.innerStart, left.innerEnd = 1, 3
			right.innerStart, right.innerEnd = 2, 4
			survivors := readBillingDemoRemoveConflictingSkipCandidates([]readBillingDemoSkipCandidate{
				left,
				right,
				independent,
			})
			require.Equal(t, []readBillingDemoSkipCandidate{independent}, survivors)
		})

		t.Run("malformed inner interval cannot be masked", func(t *testing.T) {
			fixture := newFixture(t)
			recordProof(fixture)
			fixture.innerRoot.ChildrenEndIdx = len(fixture.flat.Main)
			_, ok := readBillingDemoSkipCandidateAt(fixture.flat.Main, 0, 0)
			require.False(t, ok)
			mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
			require.False(t, mask.isSkipped(fixture.innerRoot))
			require.False(t, mask.isSkipped(fixture.innerCop))

			result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
			status, _ := appendReadBillingDemoTree(&result, ctx, fixture.runtime, fixture.flat.Main, mask)
			require.NotEqual(t, readBillingDemoStatusSuccess, status)
		})

		for _, tc := range []struct {
			name   string
			mutate func(*FlatOperator, int, int)
		}{
			{
				name: "internal child self reference",
				mutate: func(innerRoot *FlatOperator, innerRootIdx, _ int) {
					innerRoot.ChildrenIdx[0] = innerRootIdx
				},
			},
			{
				name: "internal child skips beyond subtree",
				mutate: func(innerRoot *FlatOperator, _, innerCopIdx int) {
					innerRoot.ChildrenIdx[0] = innerCopIdx + 1
				},
			},
			{
				name: "internal child starts reverse",
				mutate: func(innerRoot *FlatOperator, innerRootIdx, innerCopIdx int) {
					innerRoot.ChildrenIdx = []int{innerCopIdx, innerRootIdx}
				},
			},
		} {
			t.Run(tc.name+" cannot be masked", func(t *testing.T) {
				fixture := newFixture(t)
				recordProof(fixture)
				joinIdx, innerRootIdx, innerCopIdx := -1, -1, -1
				for idx, node := range fixture.flat.Main {
					switch node {
					case fixture.join:
						joinIdx = idx
					case fixture.innerRoot:
						innerRootIdx = idx
					case fixture.innerCop:
						innerCopIdx = idx
					}
				}
				require.NotEqual(t, -1, joinIdx)
				require.NotEqual(t, -1, innerRootIdx)
				require.Equal(t, innerRootIdx+1, innerCopIdx)
				tc.mutate(fixture.innerRoot, innerRootIdx, innerCopIdx)

				_, ok := readBillingDemoSkipCandidateAt(fixture.flat.Main, 0, joinIdx)
				require.False(t, ok)
				mask := buildReadBillingDemoExecutionMask(fixture.flat, fixture.runtime)
				require.False(t, mask.isSkipped(fixture.innerRoot))
				require.False(t, mask.isSkipped(fixture.innerCop))

				result := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
				status, _ := appendReadBillingDemoTree(&result, ctx, fixture.runtime, fixture.flat.Main, mask)
				require.NotEqual(t, readBillingDemoStatusSuccess, status)
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
		result := buildReadBillingDemoExecutionResult(ctx, reader, &ast.SelectStmt{}, nil, metrics)
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
		failedResult := buildReadBillingDemoExecutionResult(ctx, reader, &ast.SelectStmt{}, nil, metrics)
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

func TestReadBillingDemoV6WriteCoverage(t *testing.T) {
	oldWeights := readBillingDemoV6Weights
	calibratedWeights := readBillingDemoWeights{
		ModelVersion: readBillingDemoModelVersion, Version: "test-v6-calibrated", CPUWeight: 2,
		WriteKeyWeight: 3, WriteBytesWeight: 5, MutationBytesPerCPUUnit: 10, Calibrated: true,
	}
	readBillingDemoV6Weights = calibratedWeights
	t.Cleanup(func() { readBillingDemoV6Weights = oldWeights })
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

	readBillingDemoV6Weights = readBillingDemoWeights{ModelVersion: readBillingDemoModelVersion, Version: readBillingDemoWeightVersion, MutationBytesPerCPUUnit: 10}
	uncalibratedResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
	appendReadBillingDemoMutation(&uncalibratedResult, ctx, "update")
	require.Len(t, uncalibratedResult.operators[0].units, 6)
	require.Equal(t, -1.0, readBillingDemoUnitValue(uncalibratedResult.operators[0].units, readBillingDemoUnitCPUWork, readBillingDemoInputSideAll))
	require.Equal(t, 2.0, readBillingDemoUnitValue(uncalibratedResult.operators[0].units, readBillingDemoUnitEncodedMutationCount, readBillingDemoInputSideAll))
	require.Equal(t, 15.0, readBillingDemoUnitValue(uncalibratedResult.operators[0].units, readBillingDemoUnitEncodedMutationBytes, readBillingDemoInputSideAll))
	require.Len(t, uncalibratedResult.operators, 2)
	require.Equal(t, readBillingDemoReasonUncalibratedMutation, uncalibratedResult.operators[1].reason)

	readBillingDemoV6Weights = calibratedWeights
	writeMetrics := execdetails.NewRUV2Metrics()
	writeMetrics.AddResourceManagerWriteCnt(7)
	writeMetrics.AddWriteKeys(3)
	writeMetrics.AddWriteSize(66)
	autocommitWrite := buildTiKVWriteBillingDemoOperator("update", writeMetrics, false, false)
	require.Equal(t, readBillingDemoStatusOperatorOK, autocommitWrite.status)
	require.Equal(t, readBillingDemoScopeTxnPrewritePayload, autocommitWrite.scope)
	require.Equal(t, 3.0, readBillingDemoUnitValue(autocommitWrite.units, readBillingDemoUnitWriteKeys, readBillingDemoInputSideAll))
	require.Equal(t, 66.0, readBillingDemoUnitValue(autocommitWrite.units, readBillingDemoUnitWriteBytes, readBillingDemoInputSideAll))
	for _, unit := range autocommitWrite.units {
		require.Equal(t, readBillingDemoInputSourceCommitDetail, unit.source)
		require.False(t, readBillingDemoUnitDiagnosticOnly(unit.unit))
	}

	ctx.GetSessionVars().SetInTxn(false)
	autocommitResult := buildWriteBillingDemoResult(ctx, nil, "update", writeMetrics, nil)
	var autocommitWriteOps int
	for _, op := range autocommitResult.operators {
		if op.opClass == readBillingDemoOpClassKVWrite {
			autocommitWriteOps++
			require.Equal(t, "update", op.dmlKind)
			require.Equal(t, 3.0, readBillingDemoUnitValue(op.units, readBillingDemoUnitWriteKeys, readBillingDemoInputSideAll))
		}
	}
	require.Equal(t, 1, autocommitWriteOps)

	ctx.GetSessionVars().SetInTxn(true)
	ctx.GetSessionVars().TxnCtx.IsPessimistic = true
	explicitResult := buildWriteBillingDemoResult(ctx, nil, "update", writeMetrics, nil)
	for _, op := range explicitResult.operators {
		require.NotEqual(t, readBillingDemoOpClassKVWrite, op.opClass)
	}

	commitResult := buildTxnCommitBillingDemoResult(ctx, writeMetrics, nil)
	require.Len(t, commitResult.operators, 1)
	commit := commitResult.operators[0]
	require.Equal(t, readBillingDemoStatusOperatorOK, commit.status)
	require.Empty(t, commit.dmlKind)
	require.Empty(t, commit.scope)
	require.Equal(t, 3.0, readBillingDemoUnitValue(commit.units, readBillingDemoUnitWriteKeys, readBillingDemoInputSideAll))
	require.Equal(t, 66.0, readBillingDemoUnitValue(commit.units, readBillingDemoUnitWriteBytes, readBillingDemoInputSideAll))

	for _, tc := range []struct {
		name    string
		metrics *execdetails.RUV2Metrics
		reason  string
	}{
		{name: "nil metrics", reason: readBillingDemoReasonMissingTiKVWriteCoverage},
		{name: "bypassed metrics", metrics: func() *execdetails.RUV2Metrics {
			metrics := execdetails.NewRUV2Metrics()
			metrics.SetBypass(true)
			return metrics
		}(), reason: readBillingDemoReasonMissingTiKVWriteCoverage},
		{name: "bytes without keys", metrics: func() *execdetails.RUV2Metrics {
			metrics := execdetails.NewRUV2Metrics()
			metrics.AddWriteSize(1)
			return metrics
		}(), reason: readBillingDemoReasonMissingWriteKeys},
		{name: "keys without bytes", metrics: func() *execdetails.RUV2Metrics {
			metrics := execdetails.NewRUV2Metrics()
			metrics.AddWriteKeys(1)
			return metrics
		}(), reason: readBillingDemoReasonMissingWriteBytes},
	} {
		t.Run(tc.name+" fails closed", func(t *testing.T) {
			op := buildTiKVWriteBillingDemoOperator("update", tc.metrics, false, false)
			require.Equal(t, readBillingDemoStatusPartial, op.status)
			require.Equal(t, tc.reason, op.reason)
			require.Empty(t, op.units)
		})
	}
	negativeWrite := buildTiKVWriteBillingDemoOperatorFromSnapshot("update", -1, 0, false, false, true)
	require.Equal(t, readBillingDemoStatusPartial, negativeWrite.status)
	require.Equal(t, readBillingDemoReasonMissingTiKVWriteCoverage, negativeWrite.reason)
	require.Empty(t, negativeWrite.units)
	negativeWrite = buildTiKVWriteBillingDemoOperatorFromSnapshot("update", 0, -1, false, false, true)
	require.Equal(t, readBillingDemoStatusPartial, negativeWrite.status)
	require.Equal(t, readBillingDemoReasonMissingTiKVWriteCoverage, negativeWrite.reason)
	require.Empty(t, negativeWrite.units)
	zeroWrite := buildTiKVWriteBillingDemoOperator("update", execdetails.NewRUV2Metrics(), false, false)
	require.Equal(t, readBillingDemoStatusOperatorOK, zeroWrite.status)
	require.Len(t, zeroWrite.units, 2)
	require.Zero(t, readBillingDemoUnitValue(zeroWrite.units, readBillingDemoUnitWriteKeys, readBillingDemoInputSideAll))
	require.Zero(t, readBillingDemoUnitValue(zeroWrite.units, readBillingDemoUnitWriteBytes, readBillingDemoInputSideAll))

	pipelinedDML := buildTiKVWriteBillingDemoOperator("update", writeMetrics, true, false)
	require.Equal(t, readBillingDemoStatusPartial, pipelinedDML.status)
	require.Equal(t, readBillingDemoReasonPipelinedWriteUnmodeled, pipelinedDML.reason)
	require.Empty(t, pipelinedDML.units)

	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.MarkPipelined()
	pipelinedDMLResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
	appendReadBillingDemoMutation(&pipelinedDMLResult, ctx, "update")
	pipelinedDMLResult.operators = append(pipelinedDMLResult.operators, pipelinedDML)
	pipelinedCommit := buildTxnCommitBillingDemoResult(ctx, writeMetrics, nil)
	require.Len(t, pipelinedCommit.operators, 1)
	require.Equal(t, readBillingDemoStatusPartial, pipelinedCommit.operators[0].status)
	require.Equal(t, readBillingDemoReasonPipelinedCommitUnmodeled, pipelinedCommit.operators[0].reason)
	require.Empty(t, pipelinedCommit.operators[0].units)

	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder = &stmtctx.PreviewKVMutationRecorder{}
	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.RecordSet(5, 7)
	ctx.GetSessionVars().StmtCtx.PreviewKVMutationRecorder.RecordDelete(3)
	calibratedResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
	appendReadBillingDemoMutation(&calibratedResult, ctx, "update")
	calibratedResult.operators = append(calibratedResult.operators, autocommitWrite)
	rows = explainRUBuildReadBillingRows(calibratedResult, explainRUComponentSnapshotOK)
	require.True(t, rows[0].hasPreviewRU)
	require.Equal(t, 346.0, rows[0].previewRU)

	readBillingDemoV6Weights = readBillingDemoWeights{ModelVersion: readBillingDemoModelVersion, Version: readBillingDemoWeightVersion}
	uncalibratedWriteResult := readBillingDemoResult{status: readBillingDemoStatusSuccess, reason: readBillingDemoReasonNone}
	appendReadBillingDemoMutation(&uncalibratedWriteResult, ctx, "update")
	uncalibratedWriteResult.operators = append(uncalibratedWriteResult.operators, autocommitWrite)
	rows = explainRUBuildReadBillingRows(uncalibratedWriteResult, explainRUComponentSnapshotOK)
	require.False(t, rows[0].hasPreviewRU)
	statementStats := buildReadBillingDemoStatementStats(uncalibratedWriteResult)
	require.Equal(t, stmtsummary.ReadBillingDemoBaseUnitSummary{}, statementStats.Totals)
	baseUnitValue := func(site, opClass, unit string) float64 {
		for _, sample := range statementStats.BaseUnits {
			if sample.Site == site && sample.OpClass == opClass && sample.Unit == unit {
				return sample.Value
			}
		}
		return -1
	}
	require.Equal(t, 3.0, baseUnitValue(readBillingDemoSiteTiKV, readBillingDemoOpClassKVWrite, readBillingDemoUnitWriteKeys))
	require.Equal(t, 66.0, baseUnitValue(readBillingDemoSiteTiKV, readBillingDemoOpClassKVWrite, readBillingDemoUnitWriteBytes))
	require.Equal(t, -1.0, baseUnitValue(readBillingDemoSiteTiDB, readBillingDemoOpClassKVMutation, readBillingDemoUnitCPUWork))
}

func TestExplainRUPlanFormulaAndOperatorClasses(t *testing.T) {
	t.Skip("v3 opclass-weight expectations are superseded by TestReadBillingDemoV6FormulaContract")
	require.Equal(t, "v6-frontend-compile-work-uncalibrated", readBillingDemoWeightVersion)
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
	weight, previewRU, ok = readBillingDemoUnitPreviewRU(readBillingDemoUnit{unit: readBillingDemoUnitWriteBytes, value: 4096}, writeWeights)
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
				require.Equal(t, "v6-frontend-compile-work-uncalibrated", stats.WeightVersion)
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
	t.Skip("v3 commit-detail formula expectations are superseded by TestReadBillingDemoV6WriteCoverage")
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
	require.Equal(t, 66.0, units[readBillingDemoUnitWriteBytes].value)
	require.Equal(t, 2.0, units[readBillingDemoUnitPrewriteRegionNum].value)
	require.Equal(t, 7.0, units[readBillingDemoUnitTiKVWriteRPCCount].value)

	ctx.GetSessionVars().StmtCtx.MergeExecDetails(commitDetail)
	commitResult := buildTxnCommitBillingDemoResult(ctx, ruv2Metrics, nil)
	require.Equal(t, readBillingDemoStatusSuccess, commitResult.status)
	require.NotEmpty(t, commitResult.operators)
	require.Equal(t, readBillingDemoStatusOperatorOK, commitResult.operators[0].status)
	require.Empty(t, commitResult.operators[0].dmlKind)
	require.Empty(t, commitResult.operators[0].scope)
	commitUnits := make(map[string]readBillingDemoUnit)
	for _, unit := range commitResult.operators[0].units {
		commitUnits[unit.unit] = unit
	}
	require.Equal(t, 3.0, commitUnits[readBillingDemoUnitWriteKeys].value)
	require.Equal(t, 66.0, commitUnits[readBillingDemoUnitWriteBytes].value)

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
	require.Contains(t, rows[0].note, "partial_missing_tikv_write_coverage")

	missingResult := buildWriteBillingDemoResultFromDetails("update", nil, ruv2Metrics)
	require.Equal(t, readBillingDemoStatusSuccess, missingResult.status)
	require.Len(t, missingResult.operators, 1)
	require.Equal(t, readBillingDemoStatusPartial, missingResult.operators[0].status)
	require.Equal(t, readBillingDemoReasonMissingCommitDetail, missingResult.operators[0].reason)
	require.True(t, missingResult.operators[0].emitStatusRow)
	// Pipelined transactions expose a non-nil CommitDetails without logical
	// WriteKeys/WriteSize. The incomplete payload must not become billable zero
	// units merely because the detail object exists.
	pipelinedResult := []readBillingDemoOperatorResult{buildTiKVWriteBillingDemoOperator("update", ruv2Metrics, true, false)}
	require.Len(t, pipelinedResult, 1)
	require.Equal(t, readBillingDemoStatusPartial, pipelinedResult[0].status)
	require.Equal(t, readBillingDemoReasonPipelinedWriteUnmodeled, pipelinedResult[0].reason)
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
	require.Contains(t, missingWriteByte.operators, readBillingDemoWriteDiagnosticStatus("update", readBillingDemoReasonMissingWriteBytes))

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
	rows, bytes, ok = readBillingDemoRangeScanInput(1, 0, 0)
	require.True(t, ok)
	require.Zero(t, rows)
	require.Zero(t, bytes)
	for _, tc := range []struct {
		totalKeys         int64
		processedKeys     int64
		processedKeysSize int64
	}{
		{-1, 0, 0},
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
	require.Len(t, units, 1)
	require.Equal(t, 200.0, readBillingDemoUnitValue(units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
	require.Equal(t, readBillingDemoInputSourceScanDetail, readBillingDemoUnitSource(units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
	require.Equal(t, explainRUWidthSourceScanDetailProcessedEstimate, readBillingDemoUnitWidthSource(units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))

	outcome = buildUnits(&tikvutil.ScanDetail{TotalKeys: 1})
	require.True(t, outcome.success, "%+v", outcome.failure)
	require.Len(t, outcome.units, 1)
	require.Equal(t, 0.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))

	outcome = buildUnits(&tikvutil.ScanDetail{})
	require.True(t, outcome.success, "%+v", outcome.failure)
	require.Len(t, outcome.units, 1)
	require.Equal(t, 0.0, readBillingDemoUnitValue(outcome.units, readBillingDemoUnitScanBytes, readBillingDemoInputSideAll))
}

func TestReadBillingDemoCopInputEstimator(t *testing.T) {
	t.Skip("v3 byte-width estimator expectations do not apply to the v6 resource formula")
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

		selectResult := buildReadBillingDemoExecutionResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
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

		selectResult := buildReadBillingDemoExecutionResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
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

		selectResult := buildReadBillingDemoExecutionResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
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

		result := buildReadBillingDemoExecutionResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
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

		result := buildReadBillingDemoExecutionResult(ctx, reader, &ast.SelectStmt{}, nil, nil)
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

		result := buildReadBillingDemoExecutionResult(ctx, rootSort, &ast.SelectStmt{}, nil, nil)
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

		result := buildReadBillingDemoExecutionResult(ctx, rootTopN, &ast.SelectStmt{}, nil, nil)
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

		result := buildReadBillingDemoExecutionResult(ctx, lookup, &ast.SelectStmt{}, nil, nil)
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
