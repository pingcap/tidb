// Copyright 2026 PingCAP, Inc.
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
	"math"
	"testing"
	"time"

	exec "github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/earlystopprofile"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func initEarlyStopProfileTestContext(t *testing.T, sctx sessionctx.Context) {
	t.Helper()
	sctx.GetSessionVars().CurrentDB = "test"
	normalized, sqlDigest := parser.NormalizeDigest("select * from t order by a limit 5000")
	sctx.GetSessionVars().StmtCtx.InitSQLDigest(normalized, sqlDigest)
	_, planDigest := parser.NormalizeDigest("TableReader(TableFullScan)->Limit")
	sctx.GetSessionVars().StmtCtx.SetPlanDigest("TableReader(TableFullScan)->Limit", planDigest)
	setEarlyStopProfileTestPlan(sctx)
}

func setEarlyStopProfileTestPlan(sctx sessionctx.Context) {
	planCtx := sctx.(base.PlanContext)
	tableReader := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(planCtx, 0)
	sctx.GetSessionVars().StmtCtx.SetPlan(tableReader)
}

func setEarlyStopProfileMultiScanTestPlan(sctx sessionctx.Context) {
	planCtx := sctx.(base.PlanContext)
	tableReader1 := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(planCtx, 0)
	tableReader2 := physicalop.PhysicalTableReader{StoreType: kv.TiKV}.Init(planCtx, 0)
	union := physicalop.PhysicalUnionAll{}.Init(planCtx, nil, 0)
	union.SetChildren(tableReader1, tableReader2)
	sctx.GetSessionVars().StmtCtx.SetPlan(union)
}

func setEarlyStopProfileIndexMergeTestPlan(sctx sessionctx.Context) {
	sctx.GetSessionVars().StmtCtx.SetPlan(&physicalop.PhysicalIndexMergeReader{})
}

func TestKeepOrderLimitScanConcurrencyCapWithProfile(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, sctx)

	key, ok := buildEarlyStopProfileKey(sctx, earlystopprofile.ReaderTypeTable, true, 5000)
	require.True(t, ok)
	for range 3 {
		earlystopprofile.Observe(earlystopprofile.Sample{
			Candidate: earlystopprofile.Candidate{
				Key:       key,
				LimitRows: 5000,
				CapUsed:   2,
			},
			ResultRows:    10,
			RequestCount:  16,
			ProcessedKeys: 500000,
			TotalKeys:     500000,
			Latency:       10 * time.Millisecond,
			Succeed:       true,
		})
	}

	cap := keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, true, 5000)
	require.Equal(t, 1, cap)
	candidates := sctx.GetSessionVars().StmtCtx.EarlyStopProfileCandidates()
	require.Len(t, candidates, 1)
	require.Equal(t, key, candidates[0].Key)
	require.Equal(t, uint64(5000), candidates[0].LimitRows)
	require.Equal(t, vardef.DefDistSQLScanConcurrency, candidates[0].BaseCap)
	require.Equal(t, 1, candidates[0].CapUsed)
}

func TestKeepOrderLimitScanConcurrencyCapWithProfileCanRecoverBeyondStaticThreshold(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, sctx)

	key, ok := buildEarlyStopProfileKey(sctx, earlystopprofile.ReaderTypeTable, true, 5000)
	require.True(t, ok)
	for range 3 {
		earlystopprofile.Observe(earlystopprofile.Sample{
			Candidate: earlystopprofile.Candidate{
				Key:       key,
				LimitRows: 5000,
				BaseCap:   vardef.DefDistSQLScanConcurrency,
				CapUsed:   vardef.DefDistSQLScanConcurrency,
			},
			ResultRows:    5000,
			RequestCount:  1,
			ProcessedKeys: 5000,
			TotalKeys:     5000,
			Latency:       10 * time.Millisecond,
			Succeed:       true,
		})
	}

	require.Equal(t, vardef.DefDistSQLScanConcurrency,
		keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, true, 5000))
}

func TestKeepOrderLimitLargeScanCanLearnWithoutStaticCap(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, sctx)

	limitRows := uint64(keepOrderLimitMediumScanRows + 1)
	key, ok := buildEarlyStopProfileKey(sctx, earlystopprofile.ReaderTypeTable, true, limitRows)
	require.True(t, ok)

	require.Equal(t, 0, keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, true, limitRows))
	candidates := sctx.GetSessionVars().StmtCtx.EarlyStopProfileCandidates()
	require.Len(t, candidates, 1)
	require.Equal(t, vardef.DefDistSQLScanConcurrency, candidates[0].BaseCap)
	require.Equal(t, vardef.DefDistSQLScanConcurrency, candidates[0].CapUsed)

	for range 3 {
		earlystopprofile.Observe(earlystopprofile.Sample{
			Candidate: earlystopprofile.Candidate{
				Key:       key,
				LimitRows: limitRows,
				BaseCap:   vardef.DefDistSQLScanConcurrency,
				CapUsed:   vardef.DefDistSQLScanConcurrency,
			},
			ResultRows:    10,
			RequestCount:  16,
			ProcessedKeys: 2000000,
			TotalKeys:     2000000,
			Latency:       10 * time.Millisecond,
			Succeed:       true,
		})
	}

	learnedSctx := defaultCtx()
	learnedSctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, learnedSctx)
	require.Equal(t, 1, keepOrderLimitScanConcurrencyCapWithProfile(learnedSctx, earlystopprofile.ReaderTypeTable, true, limitRows))
}

func TestKeepOrderLimitScanConcurrencyCapSkipsNonSingleScanPlan(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, sctx)
	setEarlyStopProfileMultiScanTestPlan(sctx)

	require.Equal(t, 0, keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, true, 5000))
	require.Empty(t, sctx.GetSessionVars().StmtCtx.EarlyStopProfileCandidates())
}

func TestKeepOrderLimitScanConcurrencyCapSkipsIndexMergePlan(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, sctx)
	setEarlyStopProfileIndexMergeTestPlan(sctx)

	require.Equal(t, 0, keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, true, 5000))
	require.Empty(t, sctx.GetSessionVars().StmtCtx.EarlyStopProfileCandidates())
}

func TestKeepOrderLimitScanConcurrencyCapWithProfileColdStartDoesNotUseStaticThreshold(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, sctx)

	require.Equal(t, 0, keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, true, 5000))
	candidates := sctx.GetSessionVars().StmtCtx.EarlyStopProfileCandidates()
	require.Len(t, candidates, 1)
	require.Equal(t, uint64(5000), candidates[0].LimitRows)
	require.Equal(t, vardef.DefDistSQLScanConcurrency, candidates[0].BaseCap)
	require.Equal(t, vardef.DefDistSQLScanConcurrency, candidates[0].CapUsed)
}

func TestObserveEarlyStopProfileUsesStmtContextDirectly(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, sctx)
	key, ok := buildEarlyStopProfileKey(sctx, earlystopprofile.ReaderTypeTable, true, 5000)
	require.True(t, ok)

	sctx.GetSessionVars().StmtCtx.AddEarlyStopProfileCandidate(earlystopprofile.Candidate{
		Key:       key,
		LimitRows: 5000,
		BaseCap:   2,
		CapUsed:   2,
	})
	for range 3 {
		observeEarlyStopProfile(
			sctx.GetSessionVars().StmtCtx,
			execdetails.ExecDetails{
				RequestCount: 16,
				CopExecDetails: execdetails.CopExecDetails{
					ScanDetail: &tikvutil.ScanDetail{
						ProcessedKeys: 500000,
						TotalKeys:     500000,
					},
				},
			},
			10,
			10*time.Millisecond,
			true,
			false,
		)
	}

	cap, ok := earlystopprofile.LookupCap(key)
	require.True(t, ok)
	require.Equal(t, 1, cap)
}

func TestObserveEarlyStopProfileUsesActRowsWithoutProcessedKeys(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, sctx)
	key, ok := buildEarlyStopProfileKeyWithSingleScanCheck(
		sctx, earlystopprofile.ReaderTypeIndexJoin, true, 1000, false)
	require.True(t, ok)

	const lookupPlanID = 42
	sctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	sctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(lookupPlanID, true).Record(time.Millisecond, 100000)
	sctx.GetSessionVars().StmtCtx.AddEarlyStopProfileCandidate(earlystopprofile.Candidate{
		Key:          key,
		LimitRows:    1000,
		BaseCap:      5,
		CapUsed:      5,
		LookupPlanID: lookupPlanID,
	})
	for range 3 {
		observeEarlyStopProfile(
			sctx.GetSessionVars().StmtCtx,
			execdetails.ExecDetails{RequestCount: 1},
			1000,
			10*time.Millisecond,
			true,
			false,
		)
	}

	cap, ok := earlystopprofile.LookupCap(key)
	require.True(t, ok)
	require.Equal(t, 1, cap)
}

func TestSelectEarlyStopProfileCandidate(t *testing.T) {
	indexJoinCandidate := earlystopprofile.Candidate{
		Key: earlystopprofile.Key{ReaderType: earlystopprofile.ReaderTypeIndexJoin},
	}
	tableCandidate := earlystopprofile.Candidate{
		Key: earlystopprofile.Key{ReaderType: earlystopprofile.ReaderTypeTable},
	}
	selected, ok := selectEarlyStopProfileCandidate([]earlystopprofile.Candidate{
		tableCandidate,
		indexJoinCandidate,
	})
	require.True(t, ok)
	require.Equal(t, earlystopprofile.ReaderTypeIndexJoin, selected.Key.ReaderType)

	_, ok = selectEarlyStopProfileCandidate([]earlystopprofile.Candidate{
		indexJoinCandidate,
		indexJoinCandidate,
	})
	require.False(t, ok)
}

func TestKeepOrderLimitScanConcurrencyCapWithProfileFallbacks(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	initEarlyStopProfileTestContext(t, sctx)

	require.Equal(t, 0, keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, false, 5000))
	require.Equal(t, 0, keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, true, 0))
	require.Equal(t, 2, keepOrderLimitScanConcurrencyCapWithProfile(sctx, earlystopprofile.ReaderTypeTable, true, 5000))
	require.Empty(t, sctx.GetSessionVars().StmtCtx.EarlyStopProfileCandidates())

	customSctx := defaultCtx()
	customSctx.GetSessionVars().EnableAdaptiveLimitScan = true
	initEarlyStopProfileTestContext(t, customSctx)
	customSctx.GetSessionVars().Concurrency.SetDistSQLScanConcurrency(vardef.DefDistSQLScanConcurrency + 1)
	require.Equal(t, 0, keepOrderLimitScanConcurrencyCapWithProfile(customSctx, earlystopprofile.ReaderTypeTable, true, 5000))
	require.Empty(t, customSctx.GetSessionVars().StmtCtx.EarlyStopProfileCandidates())
}

func TestMergeAdaptiveProfileCap(t *testing.T) {
	require.Equal(t, 0, mergeAdaptiveProfileCap(0, vardef.DefDistSQLScanConcurrency))
	require.Equal(t, 4, mergeAdaptiveProfileCap(4, vardef.DefDistSQLScanConcurrency))
	require.Equal(t, 1, mergeAdaptiveProfileCap(1, vardef.DefDistSQLScanConcurrency))
	require.Equal(t, 1, mergeAdaptiveProfileCap(4, 1))
}

func newAdaptiveIndexJoinTestPlan(
	planCtx base.PlanContext,
	outerProp *property.PhysicalProperty,
	innerProp *property.PhysicalProperty,
) *physicalop.PhysicalIndexJoin {
	indexJoin := physicalop.PhysicalIndexJoin{}.Init(planCtx, nil, 0, outerProp, innerProp)
	indexJoin.InnerChildIdx = 1
	indexJoin.SetChildren(
		physicalop.PhysicalProjection{}.Init(planCtx, nil, 0),
		physicalop.PhysicalProjection{}.Init(planCtx, nil, 0),
	)
	return indexJoin
}

func TestLimitRowsFromIndexJoinOuterProp(t *testing.T) {
	sctx := defaultCtx()
	planCtx := sctx.(base.PlanContext)
	outerProp := &property.PhysicalProperty{
		SortItems:   []property.SortItem{{Col: &expression.Column{UniqueID: 1}}},
		ExpectedCnt: 1000,
	}
	innerProp := &property.PhysicalProperty{}
	indexJoin := newAdaptiveIndexJoinTestPlan(planCtx, outerProp, innerProp)

	limitRows, ok := limitRowsFromIndexJoinOuterProp(indexJoin)
	require.True(t, ok)
	require.Equal(t, uint64(1000), limitRows)

	noOrderJoin := newAdaptiveIndexJoinTestPlan(planCtx,
		&property.PhysicalProperty{ExpectedCnt: 1000}, innerProp)
	_, ok = limitRowsFromIndexJoinOuterProp(noOrderJoin)
	require.False(t, ok)

	missingChildrenJoin := physicalop.PhysicalIndexJoin{}.Init(planCtx, nil, 0, outerProp, innerProp)
	missingChildrenJoin.InnerChildIdx = 1
	_, ok = limitRowsFromIndexJoinOuterProp(missingChildrenJoin)
	require.False(t, ok)

	defaultExpectedCntJoin := newAdaptiveIndexJoinTestPlan(planCtx,
		&property.PhysicalProperty{
			SortItems:   []property.SortItem{{Col: &expression.Column{UniqueID: 1}}},
			ExpectedCnt: math.MaxFloat64,
		},
		innerProp)
	_, ok = limitRowsFromIndexJoinOuterProp(defaultExpectedCntJoin)
	require.False(t, ok)

	fractionalExpectedCntJoin := newAdaptiveIndexJoinTestPlan(planCtx,
		&property.PhysicalProperty{
			SortItems:   []property.SortItem{{Col: &expression.Column{UniqueID: 1}}},
			ExpectedCnt: 1000.1,
		},
		innerProp)
	limitRows, ok = limitRowsFromIndexJoinOuterProp(fractionalExpectedCntJoin)
	require.True(t, ok)
	require.Equal(t, uint64(1001), limitRows)
}

func TestAdaptiveIndexJoinLimitSettingsForRows(t *testing.T) {
	settings := adaptiveIndexJoinLimitSettingsForRows(1000, 25000, 5)
	require.True(t, settings.Changed())
	require.Equal(t, 1000, settings.BatchSize)
	require.Equal(t, 1, settings.Concurrency)

	settings = adaptiveIndexJoinLimitSettingsForRows(50000, 25000, 5)
	require.True(t, settings.Changed())
	require.Equal(t, 0, settings.BatchSize)
	require.Equal(t, 2, settings.Concurrency)

	settings = adaptiveIndexJoinLimitSettingsForRows(200000, 25000, 5)
	require.False(t, settings.Changed())
}

func TestAdaptiveIndexJoinLimitSettingsUsesOuterProperty(t *testing.T) {
	earlystopprofile.ResetForTest()
	sctx := defaultCtx()
	sctx.GetSessionVars().EnableAdaptiveLimitScan = true
	sctx.GetSessionVars().IndexJoinBatchSize = 25000
	sctx.GetSessionVars().SetIndexLookupJoinConcurrency(5)
	initEarlyStopProfileTestContext(t, sctx)
	planCtx := sctx.(base.PlanContext)
	indexJoin := newAdaptiveIndexJoinTestPlan(planCtx,
		&property.PhysicalProperty{
			SortItems:   []property.SortItem{{Col: &expression.Column{UniqueID: 1}}},
			ExpectedCnt: 1000,
		},
		&property.PhysicalProperty{})

	settings, ok := adaptiveIndexJoinLimitSettings(sctx, indexJoin)
	require.True(t, ok)
	require.Equal(t, 1000, settings.BatchSize)
	require.Equal(t, 1, settings.Concurrency)
	require.True(t, settings.HasProfileKey)
	require.Equal(t, 1, settings.ProfileCapUsed)

	sctx.GetSessionVars().EnableAdaptiveLimitScan = false
	_, ok = adaptiveIndexJoinLimitSettings(sctx, indexJoin)
	require.False(t, ok)
}

func TestApplyAdaptiveIndexJoinLimitSettingsToLookupThroughProjection(t *testing.T) {
	sctx := defaultCtx()
	lookup := &IndexLookUpExecutor{
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			indexLookupSize:        25000,
			indexLookupConcurrency: 5,
		},
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 10),
	}
	projection := &ProjectionExec{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 11, lookup),
	}
	settings := adaptiveIndexJoinLimitSettingsResult{
		ScanConcurrencyCap: 1,
		LookupBatchSize:    1000,
		LookupConcurrency:  2,
		LookupResultChSize: 2,
	}

	applied := applyAdaptiveIndexJoinLimitSettingsToLookup(projection, settings)
	require.Same(t, lookup, applied)
	require.Equal(t, 1, lookup.keepOrderLimitScanConcurrencyCap)
	require.Equal(t, 1000, lookup.adaptiveLimitLookupBatchSize)
	require.Equal(t, 2, lookup.adaptiveLimitLookupConcurrency)
	require.Equal(t, 2, lookup.adaptiveLimitResultChSize)

	lookup.applyAdaptiveLimitLookupSettings()
	require.Equal(t, 1000, lookup.indexLookupSize)
	require.Equal(t, 2, lookup.indexLookupConcurrency)
	require.Equal(t, int32(2), lookup.lookupTableTaskChannelSize())
}
