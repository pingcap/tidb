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

package core

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/testkit/spfreshtest"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestVectorPropMatchDoesNotRequireSortItems(t *testing.T) {
	ctx := MockContext()
	ctx.Store = &mock.Store{Client: &mock.Client{RequestTypeSupportedChecker: kv.RequestTypeSupportedChecker{}}}
	vecColInfo := &model.ColumnInfo{
		ID:        1,
		Name:      pmodel.NewCIStr("vec"),
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       vecColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	stats := &property.StatsInfo{
		RowCount: 100,
		ColNDVs:  map[int64]float64{},
	}
	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{
		ID:      100,
		Name:    pmodel.NewCIStr("t"),
		Columns: []*model.ColumnInfo{vecColInfo},
	}
	ds.Columns = []*model.ColumnInfo{vecColInfo}
	ds.DBName = pmodel.NewCIStr("test")
	ds.PhysicalTableID = ds.TableInfo.ID
	ds.TableStats = stats
	ds.SetSchema(expression.NewSchema(vecCol))
	ds.SetStats(stats)

	prop := &property.PhysicalProperty{
		TaskTp:      property.MppTaskType,
		ExpectedCnt: math.MaxFloat64,
	}
	prop.VectorProp.VectorHelper = &expression.VectorHelper{}
	prop.VectorProp.Column = vecCol

	idx := &model.IndexInfo{
		ID:   1,
		Name: pmodel.NewCIStr("idx_vec"),
		VectorInfo: &model.VectorIndexInfo{
			Kind:           model.VectorIndexKindHNSW,
			Dimension:      3,
			DistanceMetric: model.DistanceMetricCosine,
		},
		Columns: []*model.IndexColumn{{
			Name:   pmodel.NewCIStr("vec"),
			Offset: 0,
		}},
	}
	path := &util.AccessPath{
		Index:            idx,
		IdxCols:          []*expression.Column{vecCol},
		FullIdxCols:      []*expression.Column{vecCol},
		IdxColLens:       []int{types.UnspecifiedLength},
		FullIdxColLens:   []int{types.UnspecifiedLength},
		StoreType:        kv.TiFlash,
		CountAfterAccess: stats.RowCount,
		IsSingleScan:     true,
	}

	require.False(t, isMatchSortProp(prop, true))
	require.False(t, isMatchSortPropDesc(prop, true))

	simplePath := &util.AccessPath{
		StoreType:        kv.TiFlash,
		CountAfterAccess: stats.RowCount,
	}
	task, err := convertToTableScan(ds, prop, &candidatePath{path: simplePath}, nil)
	require.NoError(t, err)
	require.Same(t, base.InvalidTask, task)

	ts, rowCount := getOriginalPhysicalTableScan(ds, prop, path, true)
	require.Equal(t, stats.RowCount, rowCount)
	require.False(t, ts.KeepOrder)
	require.False(t, ts.Desc)

	is := getOriginalPhysicalIndexScan(ds, prop, path, true, true)
	require.False(t, is.KeepOrder)
	require.False(t, is.Desc)
}

func TestConvertToIndexScanBlocksSPFreshIndexPath(t *testing.T) {
	vecColInfo := &model.ColumnInfo{
		ID:   1,
		Name: pmodel.NewCIStr("vec"),
	}
	buildDataSource := func(ctx *mock.Context) *logicalop.DataSource {
		ds := logicalop.DataSource{}.Init(ctx, 0)
		ds.TableInfo = &model.TableInfo{
			ID:      100,
			Name:    pmodel.NewCIStr("t"),
			Columns: []*model.ColumnInfo{vecColInfo},
		}
		ds.Columns = []*model.ColumnInfo{vecColInfo}
		ds.DBName = pmodel.NewCIStr("test")
		ds.PhysicalTableID = ds.TableInfo.ID
		ds.PushedDownConds = nil
		ds.SetSchema(expression.NewSchema(&expression.Column{
			UniqueID: 1,
			ID:       1,
			RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		}))
		ds.SetStats(&property.StatsInfo{RowCount: 100, ColNDVs: map[int64]float64{}})
		return ds
	}

	idx := &model.IndexInfo{
		ID:   1,
		Name: pmodel.NewCIStr("idx_vec"),
		Tp:   pmodel.IndexTypeSPFresh,
		VectorInfo: &model.VectorIndexInfo{
			Kind:           model.VectorIndexKindSPFresh,
			Dimension:      3,
			DistanceMetric: model.DistanceMetricCosine,
		},
		Columns: []*model.IndexColumn{{
			Name:   pmodel.NewCIStr("vec"),
			Offset: 0,
		}},
	}

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	cases := []struct {
		name         string
		prop         func() *property.PhysicalProperty
		isMatchProp  bool
		enableRemote bool
		setup        func(*mock.Context, *model.IndexInfo)

		expectInvalid bool
		expectScan    bool
		expectNilErr  bool
		expectWarning string
	}{
		{
			name: "non_vector_query",
			prop: func() *property.PhysicalProperty {
				return &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
			},
			isMatchProp:   false,
			enableRemote:  true,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_mismatch",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:   false,
			enableRemote:  true,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_non_root_task",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.CopSingleReadTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:   true,
			enableRemote:  true,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_root_task_match_remote_disabled",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:   true,
			enableRemote:  false,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_root_task_match_remote_enabled",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:  true,
			enableRemote: true,
			expectScan:   true,
			expectNilErr: true,
		},
		{
			name: "vector_query_candidate_budget_exact_boundary",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 8192
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:  true,
			enableRemote: true,
			expectScan:   true,
			expectNilErr: true,
		},
		{
			name: "vector_query_candidate_budget_over_boundary",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 8193
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:   true,
			enableRemote:  true,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_configured_candidate_budget_over_boundary",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 5121

				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:  true,
			enableRemote: true,
			setup: func(_ *mock.Context, idx *model.IndexInfo) {
				idx.VectorInfo.SearchConfig = &model.SPFreshVectorSearchConfig{OversampleFactor: 2}
			},
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_root_task_match_stale_read",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:  true,
			enableRemote: true,
			setup: func(ctx *mock.Context, _ *model.IndexInfo) {
				ctx.GetSessionVars().StmtCtx.IsStaleness = true
			},
			expectScan:    true,
			expectNilErr:  true,
			expectWarning: spfreshVectorIndexStaleReadWarning,
		},
		{
			name: "vector_query_root_task_match_explicit_transaction",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:  true,
			enableRemote: true,
			setup: func(ctx *mock.Context, _ *model.IndexInfo) {
				ctx.GetSessionVars().SetInTxn(true)
			},
			expectScan:    true,
			expectNilErr:  true,
			expectWarning: spfreshVectorIndexExplicitTxnWarning,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := mock.NewContext()
			ds := buildDataSource(ctx)
			testIdx := idx.Clone()
			spfreshtest.SetEnabled(t, tc.enableRemote)
			if tc.setup != nil {
				tc.setup(ctx, testIdx)
			}

			candidate := &candidatePath{
				path: &util.AccessPath{
					Index:        testIdx,
					IsSingleScan: true,
				},
				matchPropResult: property.PropNotMatched,
			}
			if tc.isMatchProp {
				candidate.matchPropResult = property.PropMatched
			}
			task, err := convertToIndexScan(ds, tc.prop(), candidate, nil)
			if tc.expectNilErr {
				require.NoError(t, err)
			}
			if tc.expectInvalid {
				require.Same(t, base.InvalidTask, task)
			}
			if tc.expectScan {
				require.False(t, task.Invalid())
				scan, ok := task.Plan().(*PhysicalSPFreshVectorScan)
				require.True(t, ok)
				require.Equal(t, tc.prop().VectorProp.TopK, scan.TopK)
				require.Equal(t, vec.Len(), scan.QueryVec.Len())
				_, ok = scan.CloneForPlanCache(ctx)
				require.True(t, ok)
			}
			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			if tc.expectWarning == "" {
				require.Empty(t, warnings)
			} else {
				require.Len(t, warnings, 1)
				require.Equal(t, tc.expectWarning, warnings[0].Err.Error())
			}
		})
	}
}

func TestPhysicalSPFreshVectorScanPlanCacheable(t *testing.T) {
	ctx := mock.NewContext()
	ctx.Store = &mock.Store{Client: &mock.Client{RequestTypeSupportedChecker: kv.RequestTypeSupportedChecker{}}}
	scan := PhysicalSPFreshVectorScan{}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)

	cacheable, reason := isPhysicalPlanCacheable(ctx, scan, 0, 0, false)

	require.True(t, cacheable)
	require.Empty(t, reason)
	scan.Filter = &SPFreshFilterExpr{}
	cacheable, reason = isPhysicalPlanCacheable(ctx, scan, 1, 0, false)
	require.False(t, cacheable)
	require.Equal(t, "SPFresh filter parameters require rebinding", reason)
}

func TestPhysicalSPFreshVectorScanCostModelV1(t *testing.T) {
	ctx := mock.NewContext()
	tableInfo := &model.TableInfo{ID: 1, Name: pmodel.NewCIStr("t")}
	scan := PhysicalSPFreshVectorScan{
		Table: tableInfo,
		TopK:  7,
	}.Init(ctx, &property.StatsInfo{RowCount: 7, ColNDVs: map[int64]float64{}}, 0)

	cost, err := scan.GetPlanCostVer1(property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
	require.NoError(t, err)

	expected := ctx.GetSessionVars().GetNetworkFactor(tableInfo) +
		float64(scan.TopK)*(ctx.GetSessionVars().GetNetworkFactor(tableInfo)+ctx.GetSessionVars().GetCPUFactor())
	require.Equal(t, expected, cost)
}

func TestSPFreshFilterExtractionOutcomes(t *testing.T) {
	cases := []struct {
		name               string
		filter             string
		expectUsable       bool
		expectFilter       bool
		expectExprChildren int
		expectInValues     []int64
	}{
		{
			name:         "supported_range_filter",
			filter:       "tenant_id >= 7",
			expectUsable: true,
			expectFilter: true,
		},
		{
			name:         "supported_bounded_range_filter",
			filter:       "tenant_id >= 7 and tenant_id <= 9",
			expectUsable: true,
			expectFilter: true,
		},
		{
			name:         "always_true_filter",
			filter:       "1 = 1",
			expectUsable: true,
		},
		{
			name:   "always_false_filter",
			filter: "1 = 0",
		},
		{
			name:   "is_null_filter",
			filter: "tenant_id is null",
		},
		{
			name:         "is_not_null_filter",
			filter:       "tenant_id is not null",
			expectUsable: true,
		},
		{
			name:               "in_with_null_filter",
			filter:             "tenant_id in (7, null)",
			expectUsable:       true,
			expectFilter:       true,
			expectExprChildren: 3,
		},
		{
			name:           "mixed_numeric_string_in_filter",
			filter:         "tenant_id in (7, '7', 8, '8')",
			expectUsable:   true,
			expectFilter:   true,
			expectInValues: []int64{7, 8},
		},
		{
			name:         "nondeterministic_constant",
			filter:       "rand() > 0.5",
			expectUsable: false,
		},
		{
			name:         "mutable_column_predicate",
			filter:       "tenant_id > sysdate()",
			expectUsable: false,
		},
		{
			name:         "unsupported_eval_context",
			filter:       "week(cast(tenant_id as date)) > 1",
			expectUsable: false,
		},
		{
			name:         "supported_eval_context",
			filter:       "week(cast(tenant_id as date), 3) > 1",
			expectUsable: true,
			expectFilter: true,
		},
		{
			name:         "unsigned_subtraction",
			filter:       "cast(tenant_id as unsigned) - 2 < 0",
			expectUsable: true,
			expectFilter: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds, idx, candidate := buildSPFreshFilterTestDataSource(t, tc.filter)

			filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
			require.Equal(t, tc.expectUsable, ok)
			if tc.expectFilter {
				require.NotNil(t, filterExpr)
				if tc.expectExprChildren > 0 {
					require.Len(t, filterExpr.Expr.Children, tc.expectExprChildren)
				}
				if len(tc.expectInValues) > 0 {
					requireSPFreshFilterPBInValues(t, filterExpr.Expr, tc.expectInValues)
				}
			} else {
				require.Nil(t, filterExpr)
			}
			requireSPFreshFilterPlan(t, ds, candidate, tc.expectUsable, tc.expectFilter)
		})
	}
}

func TestSPFreshFilterExtractionRejectsCorrelatedPredicate(t *testing.T) {
	ds, idx, candidate := buildSPFreshFilterTestDataSource(t, "")
	outerValue := types.NewDatum(int64(7))
	corCol := &expression.CorrelatedColumn{
		Column: expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)},
		Data:   &outerValue,
	}
	cond, err := expression.NewFunction(
		ds.SCtx().GetExprCtx(),
		ast.EQ,
		types.NewFieldType(mysql.TypeTiny),
		ds.Schema().Columns[1],
		corCol,
	)
	require.NoError(t, err)
	ds.AllConds = []expression.Expression{cond}
	ds.PushedDownConds = ds.AllConds

	filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
	require.False(t, ok)
	require.Nil(t, filterExpr)

	requireSPFreshFilterPlan(t, ds, candidate, false, false)
}

func TestSPFreshFilterExtractionRejectsOversizedPayload(t *testing.T) {
	ds, idx, _ := buildSPFreshFilterTypedTestDataSourceWithConds(
		t,
		"",
		"",
		spfreshFilterTestColumnSpec{mysqlType: mysql.TypeVarchar},
	)
	largeValue := types.NewStringDatum(string(make([]byte, maxSPFreshSearchFilterBytes)))
	cond, err := expression.NewFunction(
		ds.SCtx().GetExprCtx(),
		ast.EQ,
		types.NewFieldType(mysql.TypeTiny),
		ds.Schema().Columns[1],
		&expression.Constant{Value: largeValue, RetType: types.NewFieldType(mysql.TypeString)},
	)
	require.NoError(t, err)
	ds.AllConds = []expression.Expression{cond}

	filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
	require.False(t, ok)
	require.Nil(t, filterExpr)
}

func TestBuildSPFreshFilterExprColumnsEnforcesColumnLimit(t *testing.T) {
	ctx := MockContext()
	buildFilter := func(columnCount int) (map[int64]spfreshFilterColumn, expression.Expression) {
		available := make(map[int64]spfreshFilterColumn, columnCount)
		conditions := make([]expression.Expression, 0, columnCount)
		for i := 1; i <= columnCount; i++ {
			fieldType := types.NewFieldType(mysql.TypeLonglong)
			column := &expression.Column{
				UniqueID: int64(i),
				ID:       int64(i),
				RetType:  fieldType,
			}
			available[column.ID] = spfreshFilterColumn{
				source:    tipb.SPFreshFilterExprColumnSource_Stored,
				fieldType: *fieldType,
			}
			conditions = append(conditions, column)
		}
		return available, expression.ComposeCNFCondition(ctx.GetExprCtx(), conditions...)
	}

	available, filter := buildFilter(maxSPFreshSearchFilterColumns)
	exprColumns, schema, ok := buildSPFreshFilterExprColumns(available, filter)
	require.True(t, ok)
	require.Len(t, exprColumns, maxSPFreshSearchFilterColumns)
	require.Len(t, schema.Columns, maxSPFreshSearchFilterColumns)

	available, filter = buildFilter(maxSPFreshSearchFilterColumns + 1)
	exprColumns, schema, ok = buildSPFreshFilterExprColumns(available, filter)
	require.False(t, ok)
	require.Nil(t, exprColumns)
	require.Nil(t, schema)
}

type spfreshNoDAGClient struct{ kv.Client }

func (c spfreshNoDAGClient) IsRequestTypeSupported(reqType, subType int64) bool {
	if reqType == kv.ReqTypeDAG && subType == kv.ReqSubTypeBasic {
		return false
	}
	return c.Client.IsRequestTypeSupported(reqType, subType)
}

func TestSPFreshFilterColumnRefsUseDenseOffsetsWithoutDAGCapability(t *testing.T) {
	ds, idx, _ := buildSPFreshFilterTestDataSource(t, "")
	secondInfo := &model.ColumnInfo{
		ID:        42,
		Name:      pmodel.NewCIStr("status"),
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	secondCol := appendSPFreshFilterTestColumn(ds, secondInfo)
	idx.VectorInfo.StoredColumnIDs = append(idx.VectorInfo.StoredColumnIDs, secondInfo.ID)
	firstCond := buildSPFreshFilterTestCompareCond(t, ds, ds.Schema().Columns[1], 7)
	secondCond := buildSPFreshFilterTestCompareCond(t, ds, secondCol, 9)
	ds.AllConds = []expression.Expression{firstCond, secondCond}
	ds.PushedDownConds = ds.AllConds
	store := ds.SCtx().GetStore().(*mock.Store)
	store.Client = spfreshNoDAGClient{Client: store.Client}

	filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
	require.True(t, ok)
	require.NotNil(t, filterExpr)
	require.Len(t, filterExpr.ExprColumns, 2)

	var offsets []int64
	var collectOffsets func(*tipb.Expr)
	collectOffsets = func(expr *tipb.Expr) {
		if expr.GetTp() == tipb.ExprType_ColumnRef {
			remaining, offset, err := codec.DecodeInt(expr.GetVal())
			require.NoError(t, err)
			require.Empty(t, remaining)
			offsets = append(offsets, offset)
		}
		for _, child := range expr.GetChildren() {
			collectOffsets(child)
		}
	}
	collectOffsets(filterExpr.Expr)
	require.ElementsMatch(t, []int64{0, 1}, offsets)
}

func TestSPFreshFilterExtractionTypeBoundaries(t *testing.T) {
	cases := []struct {
		name       string
		filter     string
		mysqlType  byte
		unsigned   bool
		usable     bool
		hasFilter  bool
		sig        tipb.ScalarFuncSig
		childCount int
	}{
		{name: "unsigned_eq_negative", filter: "tenant_id = -1", mysqlType: mysql.TypeLonglong, unsigned: true},
		{name: "unsigned_gt_negative", filter: "tenant_id > -1", mysqlType: mysql.TypeLonglong, unsigned: true, usable: true},
		{name: "signed_integer_overflow", filter: "tenant_id = '9223372036854775808'", mysqlType: mysql.TypeLonglong},
		{name: "unsigned_integer_overflow", filter: "tenant_id > '18446744073709551616'", mysqlType: mysql.TypeLonglong, unsigned: true},
		{name: "fractional_string", filter: "tenant_id = '7.9'", mysqlType: mysql.TypeLonglong},
		{name: "date", filter: "tenant_id = '2026-01-05'", mysqlType: mysql.TypeDate, usable: true, hasFilter: true, sig: tipb.ScalarFuncSig_EQTime, childCount: 2},
		{name: "datetime", filter: "tenant_id in ('2026-01-05 12:34:56', '2026-01-06 01:02:03')", mysqlType: mysql.TypeDatetime, usable: true, hasFilter: true, sig: tipb.ScalarFuncSig_InTime, childCount: 3},
		{name: "duration", filter: "tenant_id >= '12:34:56'", mysqlType: mysql.TypeDuration, usable: true, hasFilter: true, sig: tipb.ScalarFuncSig_GEDuration, childCount: 2},
		{name: "year", filter: "tenant_id >= 2026", mysqlType: mysql.TypeYear, usable: true, hasFilter: true, sig: tipb.ScalarFuncSig_GEInt, childCount: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds, idx, _ := buildSPFreshFilterTypedTestDataSourceWithConds(
				t,
				tc.filter,
				tc.filter,
				spfreshFilterTestColumnSpec{mysqlType: tc.mysqlType, unsigned: tc.unsigned},
			)
			filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
			require.Equal(t, tc.usable, ok)
			require.Equal(t, tc.hasFilter, filterExpr != nil)
			if filterExpr != nil {
				require.Equal(t, tc.sig, filterExpr.Expr.Sig)
				require.Len(t, filterExpr.Expr.Children, tc.childCount)
			}
		})
	}
}

func TestSPFreshTimestampFilterConstantRewriteEncodesSessionTimeZone(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	localTime := types.NewTime(types.FromDate(2026, 1, 5, 10, 0, 0, 0), mysql.TypeDatetime, 0)
	localEncoded, err := codec.EncodeMySQLTime(loc, localTime, mysql.TypeDatetime, nil)
	require.NoError(t, err)
	expr := &tipb.Expr{
		Tp:  tipb.ExprType_ScalarFunc,
		Sig: tipb.ScalarFuncSig_GETime,
		Children: []*tipb.Expr{
			{
				Tp:        tipb.ExprType_ColumnRef,
				FieldType: expression.ToPBFieldType(types.NewFieldType(mysql.TypeTimestamp)),
			},
			{
				Tp:        tipb.ExprType_MysqlTime,
				Val:       localEncoded,
				FieldType: expression.ToPBFieldType(types.NewFieldType(mysql.TypeDatetime)),
			},
		},
	}

	require.True(t, rewriteSPFreshTimestampFilterConstants(expr, loc))
	timeConst := expr.Children[1]
	require.Equal(t, int32(mysql.TypeTimestamp), timeConst.GetFieldType().GetTp())

	remaining, packed, err := codec.DecodeUint(timeConst.GetVal())
	require.NoError(t, err)
	require.Empty(t, remaining)
	var rewritten types.Time
	require.NoError(t, rewritten.FromPackedUint(packed))
	require.Equal(t, "2026-01-05 02:00:00", rewritten.String())
}

func TestSPFreshFilterExtractionUsesAllConds(t *testing.T) {
	ds, idx, candidate := buildSPFreshFilterTestDataSourceWithConds(
		t,
		"tenant_id >= 7",
		"tenant_id >= 7",
	)
	statusColInfo := &model.ColumnInfo{
		ID:        3,
		Name:      pmodel.NewCIStr("status"),
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	statusCol := &expression.Column{
		UniqueID: 3,
		ID:       statusColInfo.ID,
		RetType:  statusColInfo.FieldType.Clone(),
	}
	ds.TableInfo.Columns = append(ds.TableInfo.Columns, statusColInfo)
	ds.Columns = append(ds.Columns, statusColInfo)
	ds.SetSchema(expression.NewSchema(ds.Schema().Columns[0], ds.Schema().Columns[1], statusCol))
	ds.ColsRequiringFullLen = append(ds.ColsRequiringFullLen, statusCol)
	statusValue := &expression.Constant{
		Value:   types.NewIntDatum(1),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	statusCond, err := expression.NewFunction(ds.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), statusCol, statusValue)
	require.NoError(t, err)
	ds.AllConds = append(ds.AllConds, statusCond)

	filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
	require.False(t, ok)
	require.Nil(t, filterExpr)

	requireSPFreshFilterPlan(t, ds, candidate, false, false)
}

func TestSPFreshFilterAvailableColumnsIncludesHandleDerivedColumns(t *testing.T) {
	t.Run("pk_is_handle_int_column", func(t *testing.T) {
		ds, idx, _ := buildSPFreshFilterTestDataSource(t, "")
		pkInfo := &model.ColumnInfo{
			ID:        3,
			Name:      pmodel.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}
		pkInfo.AddFlag(mysql.PriKeyFlag)
		pkCol := appendSPFreshFilterTestColumn(ds, pkInfo)
		ds.TableInfo.PKIsHandle = true
		cond := buildSPFreshFilterTestCompareCond(t, ds, pkCol, 100)
		ds.AllConds = []expression.Expression{cond}
		ds.PushedDownConds = []expression.Expression{cond}

		filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
		require.True(t, ok)
		require.NotNil(t, filterExpr)
		require.Len(t, filterExpr.ExprColumns, 1)
		require.Equal(t, pkInfo.ID, filterExpr.ExprColumns[0].GetColumnId())
		require.Equal(t, tipb.SPFreshFilterExprColumnSource_Handle, filterExpr.ExprColumns[0].GetSource())
	})

	t.Run("extra_handle_column_allowed_on_non_common_handle_table", func(t *testing.T) {
		ds, idx, _ := buildSPFreshFilterTestDataSource(t, "")
		extraHandleInfo := model.NewExtraHandleColInfo()
		extraHandleCol := appendSPFreshFilterTestColumn(ds, extraHandleInfo)
		cond := buildSPFreshFilterTestCompareCond(t, ds, extraHandleCol, 100)
		ds.AllConds = []expression.Expression{cond}
		ds.PushedDownConds = []expression.Expression{cond}

		filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
		require.True(t, ok)
		require.NotNil(t, filterExpr)
		require.Len(t, filterExpr.ExprColumns, 1)
		// ExtraHandleID is negative. TiKV accepts it for handle-derived filters
		// because handle values are bound by expression offset, not by column ID.
		require.Equal(t, int64(model.ExtraHandleID), filterExpr.ExprColumns[0].GetColumnId())
		require.Equal(t, tipb.SPFreshFilterExprColumnSource_Handle, filterExpr.ExprColumns[0].GetSource())
		require.True(t, mysql.HasNotNullFlag(uint(filterExpr.ExprColumns[0].GetFieldType().GetFlag())))
		require.Equal(t, int32(mysql.TypeLonglong), filterExpr.ExprColumns[0].GetFieldType().GetTp())
	})

	t.Run("extra_handle_column_rejected_on_common_handle_table", func(t *testing.T) {
		ds, idx, _ := buildSPFreshFilterTestDataSource(t, "")
		extraHandleCol := appendSPFreshFilterTestColumn(ds, model.NewExtraHandleColInfo())
		ds.TableInfo.IsCommonHandle = true
		cond := buildSPFreshFilterTestCompareCond(t, ds, extraHandleCol, 100)
		ds.AllConds = []expression.Expression{cond}
		ds.PushedDownConds = []expression.Expression{cond}

		filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
		require.False(t, ok)
		require.Nil(t, filterExpr)
	})

	t.Run("common_handle_primary_key_rejected_without_binding", func(t *testing.T) {
		ds, idx, _ := buildSPFreshFilterTestDataSource(t, "")
		pkInfo := &model.ColumnInfo{
			ID:        3,
			Name:      pmodel.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}
		pkInfo.AddFlag(mysql.PriKeyFlag)
		pkCol := appendSPFreshFilterTestColumn(ds, pkInfo)
		ds.TableInfo.IsCommonHandle = true
		ds.TableInfo.Indices = []*model.IndexInfo{{
			Name:    pmodel.NewCIStr("primary"),
			Primary: true,
			Columns: []*model.IndexColumn{{
				Name:   pkInfo.Name,
				Offset: pkInfo.Offset,
			}},
		}}
		cond := buildSPFreshFilterTestCompareCond(t, ds, pkCol, 100)
		ds.AllConds = []expression.Expression{cond}

		filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
		require.False(t, ok)
		require.Nil(t, filterExpr)
	})

	t.Run("common_handle_primary_key_allowed_when_stored", func(t *testing.T) {
		ds, idx, _ := buildSPFreshFilterTestDataSource(t, "")
		pkInfo := &model.ColumnInfo{
			ID:        3,
			Name:      pmodel.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}
		pkInfo.AddFlag(mysql.PriKeyFlag)
		pkCol := appendSPFreshFilterTestColumn(ds, pkInfo)
		idx.VectorInfo.StoredColumnIDs = append(idx.VectorInfo.StoredColumnIDs, pkInfo.ID)
		ds.TableInfo.IsCommonHandle = true
		ds.TableInfo.Indices = []*model.IndexInfo{{
			Name:    pmodel.NewCIStr("primary"),
			Primary: true,
			Columns: []*model.IndexColumn{{
				Name:   pkInfo.Name,
				Offset: pkInfo.Offset,
			}},
		}}
		cond := buildSPFreshFilterTestCompareCond(t, ds, pkCol, 100)
		ds.AllConds = []expression.Expression{cond}
		ds.PushedDownConds = []expression.Expression{cond}

		filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
		require.True(t, ok)
		require.NotNil(t, filterExpr)
		require.Len(t, filterExpr.ExprColumns, 1)
		require.Equal(t, pkInfo.ID, filterExpr.ExprColumns[0].GetColumnId())
		require.Equal(t, tipb.SPFreshFilterExprColumnSource_Stored, filterExpr.ExprColumns[0].GetSource())
	})

}

func TestSPFreshScanOutputColumns(t *testing.T) {
	for _, tc := range []struct {
		name        string
		requiredCol int
		wantIDs     []int64
	}{
		{name: "planner_state_unavailable", requiredCol: -1, wantIDs: []int64{1, 2}},
		{name: "required_subset", requiredCol: 1, wantIDs: []int64{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds, _, _ := buildSPFreshFilterTestDataSource(t, "")
			for i, column := range ds.Schema().Columns {
				column.Index = i
			}
			if tc.requiredCol < 0 {
				ds.ColsRequiringFullLen = nil
			} else {
				ds.ColsRequiringFullLen = []*expression.Column{ds.Schema().Columns[tc.requiredCol]}
			}

			outInfos, outSchemaCols := spfreshScanOutputColumns(ds)
			require.Len(t, outInfos, len(tc.wantIDs))
			require.Len(t, outSchemaCols, len(tc.wantIDs))
			for i, wantID := range tc.wantIDs {
				require.Equal(t, wantID, outInfos[i].ID)
				require.Equal(t, wantID, outSchemaCols[i].ID)
				require.Equal(t, i, outSchemaCols[i].Index)
			}
		})
	}
}

func TestSPFreshPlanAllowsProjectionOver256Columns(t *testing.T) {
	ds, idx, candidate := buildSPFreshFilterTestDataSource(t, "")
	for i := 0; i < 257; i++ {
		colInfo := &model.ColumnInfo{
			ID:        int64(i + 10),
			Name:      pmodel.NewCIStr(fmt.Sprintf("stored_%d", i)),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}
		appendSPFreshFilterTestColumn(ds, colInfo)
		idx.VectorInfo.StoredColumnIDs = append(idx.VectorInfo.StoredColumnIDs, colInfo.ID)
	}

	requireSPFreshFilterPlan(t, ds, candidate, true, false)
	outInfos, _ := spfreshScanOutputColumns(ds)
	require.Greater(t, len(outInfos), 256)
}

func buildSPFreshFilterTestDataSource(t *testing.T, filter string) (*logicalop.DataSource, *model.IndexInfo, *candidatePath) {
	return buildSPFreshFilterTestDataSourceWithConds(t, filter, filter)
}

func buildSPFreshFilterTestDataSourceWithConds(t *testing.T, allCondFilter, pushedDownFilter string) (*logicalop.DataSource, *model.IndexInfo, *candidatePath) {
	return buildSPFreshFilterTypedTestDataSourceWithConds(t, allCondFilter, pushedDownFilter, spfreshFilterTestColumnSpec{
		mysqlType: mysql.TypeLonglong,
	})
}

func requireSPFreshFilterPlan(
	t *testing.T,
	ds *logicalop.DataSource,
	candidate *candidatePath,
	expectSPFresh bool,
	expectFilter bool,
) {
	t.Helper()

	spfreshtest.SetEnabled(t, true)

	prop := buildSPFreshFilterTestProperty(t, ds)
	task, err := convertToIndexScan(ds, prop, candidate, nil)
	require.NoError(t, err)
	if !expectSPFresh {
		require.Same(t, base.InvalidTask, task)
		return
	}

	require.False(t, task.Invalid())
	scan, ok := task.Plan().(*PhysicalSPFreshVectorScan)
	require.True(t, ok)
	require.Equal(t, expectFilter, scan.Filter != nil)
	require.True(t, hasMatchingSPFreshVectorPath(ds, prop.VectorProp.VectorHelper))
}

func buildSPFreshFilterTestProperty(t *testing.T, ds *logicalop.DataSource) *property.PhysicalProperty {
	t.Helper()

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)
	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}
	prop.VectorProp.VectorHelper = &expression.VectorHelper{
		DistanceFnName: pmodel.NewCIStr("vec_cosine_distance"),
		Column:         ds.Schema().Columns[0],
	}
	prop.VectorProp.TopK = 10
	prop.VectorProp.Vec = vec
	return prop
}

type spfreshFilterTestColumnSpec struct {
	mysqlType byte
	unsigned  bool
}

func buildSPFreshFilterTypedTestDataSourceWithConds(
	t *testing.T,
	allCondFilter, pushedDownFilter string,
	spec spfreshFilterTestColumnSpec,
) (*logicalop.DataSource, *model.IndexInfo, *candidatePath) {
	t.Helper()

	ctx := MockContext()
	ctx.Store = &mock.Store{Client: &mock.Client{RequestTypeSupportedChecker: kv.RequestTypeSupportedChecker{}}}
	vecColInfo := &model.ColumnInfo{
		ID:        1,
		Name:      pmodel.NewCIStr("vec"),
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	filterColInfo := &model.ColumnInfo{
		ID:        2,
		Name:      pmodel.NewCIStr("tenant_id"),
		FieldType: *types.NewFieldType(spec.mysqlType),
	}
	filterColInfo.AddFlag(mysql.NotNullFlag)
	if spec.unsigned {
		filterColInfo.AddFlag(mysql.UnsignedFlag)
	}
	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       vecColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	filterCol := &expression.Column{
		UniqueID: 2,
		ID:       filterColInfo.ID,
		RetType:  filterColInfo.FieldType.Clone(),
	}

	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{
		ID:      100,
		Name:    pmodel.NewCIStr("t_spfresh_filter_req"),
		Columns: []*model.ColumnInfo{vecColInfo, filterColInfo},
	}
	ds.Columns = []*model.ColumnInfo{vecColInfo, filterColInfo}
	ds.DBName = pmodel.NewCIStr("test")
	ds.PhysicalTableID = ds.TableInfo.ID
	ds.SetSchema(expression.NewSchema(vecCol, filterCol))
	ds.SetStats(&property.StatsInfo{RowCount: 100, ColNDVs: map[int64]float64{}})
	ds.ColsRequiringFullLen = []*expression.Column{vecCol, filterCol}
	ds.AllConds = nil
	ds.PushedDownConds = nil

	names := types.NameSlice{
		&types.FieldName{DBName: ds.DBName, TblName: ds.TableInfo.Name, ColName: vecColInfo.Name},
		&types.FieldName{DBName: ds.DBName, TblName: ds.TableInfo.Name, ColName: filterColInfo.Name},
	}
	if allCondFilter != "" {
		conds, err := rewriteSimpleExpr(ctx.GetExprCtx(), allCondFilter, ds.Schema(), names)
		require.NoError(t, err)
		ds.AllConds = conds
	}
	if pushedDownFilter != "" {
		conds, err := rewriteSimpleExpr(ctx.GetExprCtx(), pushedDownFilter, ds.Schema(), names)
		require.NoError(t, err)
		ds.PushedDownConds = conds
	}

	idx := &model.IndexInfo{
		ID:   1,
		Name: pmodel.NewCIStr("idx_vec"),
		Tp:   pmodel.IndexTypeSPFresh,
		VectorInfo: &model.VectorIndexInfo{
			Kind:            model.VectorIndexKindSPFresh,
			Dimension:       3,
			DistanceMetric:  model.DistanceMetricCosine,
			StoredColumnIDs: []int64{filterColInfo.ID},
		},
		Columns: []*model.IndexColumn{{
			Name:   pmodel.NewCIStr("vec"),
			Offset: 0,
		}},
	}
	path := &util.AccessPath{
		Index:        idx,
		IsSingleScan: true,
	}
	ds.PossibleAccessPaths = []*util.AccessPath{path}

	return ds, idx, &candidatePath{
		path:            path,
		matchPropResult: property.PropMatched,
	}
}

func appendSPFreshFilterTestColumn(ds *logicalop.DataSource, colInfo *model.ColumnInfo) *expression.Column {
	colInfo.Offset = len(ds.TableInfo.Columns)
	col := &expression.Column{
		UniqueID: int64(colInfo.Offset + 1),
		ID:       colInfo.ID,
		RetType:  colInfo.FieldType.Clone(),
	}
	ds.TableInfo.Columns = append(ds.TableInfo.Columns, colInfo)
	ds.Columns = append(ds.Columns, colInfo)
	schemaCols := append([]*expression.Column(nil), ds.Schema().Columns...)
	schemaCols = append(schemaCols, col)
	ds.SetSchema(expression.NewSchema(schemaCols...))
	ds.ColsRequiringFullLen = append(ds.ColsRequiringFullLen, col)
	return col
}

func buildSPFreshFilterTestCompareCond(
	t *testing.T,
	ds *logicalop.DataSource,
	col *expression.Column,
	value int64,
) expression.Expression {
	t.Helper()
	con := &expression.Constant{
		Value:   types.NewIntDatum(value),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	cond, err := expression.NewFunction(ds.SCtx().GetExprCtx(), ast.GE, types.NewFieldType(mysql.TypeLonglong), col, con)
	require.NoError(t, err)
	return cond
}

func requireSPFreshFilterPBInValues(t *testing.T, pbExpr *tipb.Expr, expected []int64) {
	t.Helper()
	require.NotNil(t, pbExpr)
	require.Equal(t, tipb.ExprType_ScalarFunc, pbExpr.Tp)
	require.Equal(t, tipb.ScalarFuncSig_InInt, pbExpr.Sig)
	require.Len(t, pbExpr.Children, len(expected)+1)
	require.Equal(t, tipb.ExprType_ColumnRef, pbExpr.Children[0].Tp)

	decoded, err := expression.PBToExpr(MockContext().GetExprCtx(), pbExpr, []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)})
	require.NoError(t, err)
	sf, ok := decoded.(*expression.ScalarFunction)
	require.True(t, ok)
	require.Len(t, sf.GetArgs(), len(expected)+1)
	for i, value := range expected {
		con, ok := sf.GetArgs()[i+1].(*expression.Constant)
		require.True(t, ok)
		require.Equal(t, value, con.Value.GetInt64())
	}
}

func TestPhysicalSPFreshVectorScanExplainInfoUsesReadableFilterExpr(t *testing.T) {
	ctx := MockContext()
	ds, idx, _ := buildSPFreshFilterTestDataSource(t, "tenant_id >= 7")
	filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
	require.True(t, ok)
	require.NotNil(t, filterExpr)
	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	scan := PhysicalSPFreshVectorScan{
		Table:    ds.TableInfo,
		Columns:  ds.Columns,
		Index:    idx,
		DBName:   ds.DBName,
		TopK:     10,
		QueryVec: vec,
		Filter:   filterExpr,
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)

	require.Contains(t, scan.ExplainInfo(), "filter:ge(tenant_id, 7)")
	require.Contains(t, scan.ExplainNormalizedInfo(), "filter:ge(tenant_id, ?)")
	require.NotContains(t, scan.ExplainInfo(), "tp:ScalarFunc")
}

func TestPhysicalSPFreshVectorScanCloneClonesFilter(t *testing.T) {
	ctx := MockContext()
	ds, idx, _ := buildSPFreshFilterTestDataSource(t, "tenant_id >= 7")
	filterExpr, ok := extractSPFreshFilterExpr(ds, idx)
	require.True(t, ok)
	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)
	scan := PhysicalSPFreshVectorScan{
		Table: ds.TableInfo, Columns: ds.Columns, Index: idx, DBName: ds.DBName,
		TopK: 10, QueryVec: vec, Filter: filterExpr,
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)

	clonedPlan, err := scan.Clone(ctx)
	require.NoError(t, err)
	cloned := clonedPlan.(*PhysicalSPFreshVectorScan)
	require.NotSame(t, scan.Filter, cloned.Filter)
	require.NotSame(t, scan.Filter.Expr, cloned.Filter.Expr)
	require.NotSame(t, scan.Filter.ExprColumns[0], cloned.Filter.ExprColumns[0])
	cachePlan, ok := scan.CloneForPlanCache(MockContext())
	require.True(t, ok)
	cached := cachePlan.(*PhysicalSPFreshVectorScan)
	require.NotSame(t, scan.Filter.Expr, cached.Filter.Expr)
	require.NotSame(t, scan.Filter.ExprColumns[0], cached.Filter.ExprColumns[0])
	cached.QueryVec.Elements()[0] = 9
	cloned.QueryVec.Elements()[1] = 8
	require.Equal(t, []float32{1, 2, 3}, scan.QueryVec.Elements())
}
