// Copyright 2017 PingCAP, Inc.
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
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type visit struct {
	a1  unsafe.Pointer
	a2  unsafe.Pointer
	typ reflect.Type
}

func TestShow(t *testing.T) {
	node := &ast.ShowStmt{}
	tps := []ast.ShowStmtType{
		ast.ShowBinlogStatus,
		ast.ShowEngines,
		ast.ShowDatabases,
		ast.ShowTables,
		ast.ShowTableStatus,
		ast.ShowColumns,
		ast.ShowWarnings,
		ast.ShowCharset,
		ast.ShowVariables,
		ast.ShowStatus,
		ast.ShowCollation,
		ast.ShowCreateTable,
		ast.ShowCreateUser,
		ast.ShowGrants,
		ast.ShowTriggers,
		ast.ShowProcedureStatus,
		ast.ShowIndex,
		ast.ShowProcessList,
		ast.ShowCreateDatabase,
		ast.ShowEvents,
		ast.ShowMasterStatus,
		ast.ShowBackups,
		ast.ShowRestores,
	}
	for _, tp := range tps {
		node.Tp = tp
		schema, _ := buildShowSchema(node, false, false)
		for _, col := range schema.Columns {
			require.Greater(t, col.RetType.GetFlen(), 0)
		}
	}
}

func TestGetPathByIndexName(t *testing.T) {
	tblInfo := &model.TableInfo{
		Indices:    make([]*model.IndexInfo, 0),
		PKIsHandle: true,
	}

	accessPath := []*util.AccessPath{
		{IsIntHandlePath: true},
		{Index: &model.IndexInfo{Name: model.NewCIStr("idx")}},
		genTiFlashPath(tblInfo),
	}

	path := getPathByIndexName(accessPath, model.NewCIStr("idx"), tblInfo)
	require.NotNil(t, path)
	require.Equal(t, accessPath[1], path)

	// "id" is a prefix of "idx"
	path = getPathByIndexName(accessPath, model.NewCIStr("id"), tblInfo)
	require.NotNil(t, path)
	require.Equal(t, accessPath[1], path)

	path = getPathByIndexName(accessPath, model.NewCIStr("primary"), tblInfo)
	require.NotNil(t, path)
	require.Equal(t, accessPath[0], path)

	path = getPathByIndexName(accessPath, model.NewCIStr("not exists"), tblInfo)
	require.Nil(t, path)

	tblInfo = &model.TableInfo{
		Indices:    make([]*model.IndexInfo, 0),
		PKIsHandle: false,
	}

	path = getPathByIndexName(accessPath, model.NewCIStr("primary"), tblInfo)
	require.Nil(t, path)
}

func TestRewriterPool(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	builder, _ := NewPlanBuilder().Init(ctx, nil, hint.NewQBHintHandler(nil))

	// Make sure PlanBuilder.getExpressionRewriter() provides clean rewriter from pool.
	// First, pick one rewriter from the pool and make it dirty.
	builder.rewriterCounter++
	dirtyRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	dirtyRewriter.asScalar = true
	dirtyRewriter.planCtx.aggrMap = make(map[*ast.AggregateFuncExpr]int)
	dirtyRewriter.preprocess = func(ast.Node) ast.Node { return nil }
	dirtyRewriter.planCtx.insertPlan = &Insert{}
	dirtyRewriter.disableFoldCounter = 1
	dirtyRewriter.ctxStack = make([]expression.Expression, 2)
	dirtyRewriter.ctxNameStk = make([]*types.FieldName, 2)
	builder.rewriterCounter--
	// Then, pick again and check if it's cleaned up.
	builder.rewriterCounter++
	cleanRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	require.Equal(t, dirtyRewriter, cleanRewriter)
	require.Equal(t, false, cleanRewriter.asScalar)
	require.Nil(t, cleanRewriter.planCtx.aggrMap)
	require.Nil(t, cleanRewriter.preprocess)
	require.Nil(t, cleanRewriter.planCtx.insertPlan)
	require.Zero(t, cleanRewriter.disableFoldCounter)
	require.Len(t, cleanRewriter.ctxStack, 0)
	builder.rewriterCounter--
}

func TestDisableFold(t *testing.T) {
	// Functions like BENCHMARK() shall not be folded into result 0,
	// but normal outer function with constant args should be folded.
	// Types of expression and first layer of args will be validated.
	cases := []struct {
		SQL      string
		Expected expression.Expression
		Args     []expression.Expression
	}{
		{`select sin(length("abc"))`, &expression.Constant{}, nil},
		{`select benchmark(3, sin(123))`, &expression.ScalarFunction{}, []expression.Expression{
			&expression.Constant{},
			&expression.ScalarFunction{},
		}},
		{`select pow(length("abc"), benchmark(3, sin(123)))`, &expression.ScalarFunction{}, []expression.Expression{
			&expression.Constant{},
			&expression.ScalarFunction{},
		}},
	}

	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	for _, c := range cases {
		st, err := parser.New().ParseOneStmt(c.SQL, "", "")
		require.NoError(t, err)
		stmt := st.(*ast.SelectStmt)
		expr := stmt.Fields.Fields[0].Expr

		builder, _ := NewPlanBuilder().Init(ctx, nil, hint.NewQBHintHandler(nil))
		builder.rewriterCounter++
		rewriter := builder.getExpressionRewriter(context.TODO(), nil)
		require.NotNil(t, rewriter)
		require.Equal(t, 0, rewriter.disableFoldCounter)
		rewrittenExpression, _, err := rewriteExprNode(rewriter, expr, true)
		require.NoError(t, err)
		require.Equal(t, 0, rewriter.disableFoldCounter)
		builder.rewriterCounter--

		require.IsType(t, c.Expected, rewrittenExpression)
		for i, expectedArg := range c.Args {
			rewrittenArg := expression.GetFuncArg(rewrittenExpression, i)
			require.IsType(t, expectedArg, rewrittenArg)
		}
	}
}

func TestDeepClone(t *testing.T) {
	tp := types.NewFieldType(mysql.TypeLonglong)
	expr := &expression.Column{RetType: tp}
	byItems := []*util.ByItems{{Expr: expr}}
	sort1 := &PhysicalSort{ByItems: byItems}
	sort2 := &PhysicalSort{ByItems: byItems}
	checkDeepClone := func(p1, p2 base.PhysicalPlan) error {
		whiteList := []string{"*property.StatsInfo", "*sessionctx.Context", "*mock.Context"}
		return checkDeepClonedCore(reflect.ValueOf(p1), reflect.ValueOf(p2), typeName(reflect.TypeOf(p1)), whiteList, nil)
	}
	err := checkDeepClone(sort1, sort2)
	require.Error(t, err)
	require.Regexp(t, "invalid slice pointers, path PhysicalSort.ByItems", err.Error())

	byItems2 := []*util.ByItems{{Expr: expr}}
	sort2.ByItems = byItems2
	err = checkDeepClone(sort1, sort2)
	require.Error(t, err)
	require.Regexp(t, "same pointer, path PhysicalSort.ByItems.*Expression", err.Error())

	expr2 := &expression.Column{RetType: tp}
	byItems2[0].Expr = expr2
	err = checkDeepClone(sort1, sort2)
	require.Error(t, err)
	require.Regexp(t, "same pointer, path PhysicalSort.ByItems.*Expression.FieldType", err.Error())

	expr2.RetType = types.NewFieldType(mysql.TypeString)
	err = checkDeepClone(sort1, sort2)
	require.Error(t, err)
	require.Regexp(t, "different values, path PhysicalSort.ByItems.*Expression.FieldType.uint8", err.Error())

	expr2.RetType = types.NewFieldType(mysql.TypeLonglong)
	require.NoError(t, checkDeepClone(sort1, sort2))
}

func TestTablePlansAndTablePlanInPhysicalTableReaderClone(t *testing.T) {
	ctx := mock.NewContext()
	col, cst := &expression.Column{RetType: types.NewFieldType(mysql.TypeString)}, &expression.Constant{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	tblInfo := &model.TableInfo{}

	// table scan
	tableScan := &PhysicalTableScan{
		AccessCondition: []expression.Expression{col, cst},
		Table:           tblInfo,
	}
	tableScan = tableScan.Init(ctx, 0)
	tableScan.SetSchema(schema)

	// table reader
	tableReader := &PhysicalTableReader{
		tablePlan:  tableScan,
		TablePlans: []base.PhysicalPlan{tableScan},
		StoreType:  kv.TiFlash,
	}
	tableReader = tableReader.Init(ctx, 0)
	clonedPlan, err := tableReader.Clone()
	require.NoError(t, err)
	newTableReader, ok := clonedPlan.(*PhysicalTableReader)
	require.True(t, ok)
	require.True(t, newTableReader.tablePlan == newTableReader.TablePlans[0])
}

func TestPhysicalPlanClone(t *testing.T) {
	ctx := mock.NewContext()
	col, cst := &expression.Column{RetType: types.NewFieldType(mysql.TypeString)}, &expression.Constant{RetType: types.NewFieldType(mysql.TypeLonglong)}
	stats := &property.StatsInfo{RowCount: 1000}
	schema := expression.NewSchema(col)
	tblInfo := &model.TableInfo{}
	idxInfo := &model.IndexInfo{}
	aggDesc1, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, false)
	require.NoError(t, err)
	aggDesc2, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{cst}, true)
	require.NoError(t, err)
	aggDescs := []*aggregation.AggFuncDesc{aggDesc1, aggDesc2}

	// table scan
	tableScan := &PhysicalTableScan{
		AccessCondition: []expression.Expression{col, cst},
		Table:           tblInfo,
	}
	tableScan = tableScan.Init(ctx, 0)
	tableScan.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(tableScan))

	// table reader
	tableReader := &PhysicalTableReader{
		tablePlan:  tableScan,
		TablePlans: []base.PhysicalPlan{tableScan},
		StoreType:  kv.TiFlash,
	}
	tableReader = tableReader.Init(ctx, 0)
	require.NoError(t, checkPhysicalPlanClone(tableReader))

	// index scan
	indexScan := &PhysicalIndexScan{
		AccessCondition:  []expression.Expression{col, cst},
		Table:            tblInfo,
		Index:            idxInfo,
		dataSourceSchema: schema,
	}
	indexScan = indexScan.Init(ctx, 0)
	indexScan.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(indexScan))

	// index reader
	indexReader := &PhysicalIndexReader{
		indexPlan:     indexScan,
		IndexPlans:    []base.PhysicalPlan{indexScan},
		OutputColumns: []*expression.Column{col, col},
	}
	indexReader = indexReader.Init(ctx, 0)
	require.NoError(t, checkPhysicalPlanClone(indexReader))

	// index lookup
	indexLookup := &PhysicalIndexLookUpReader{
		IndexPlans:     []base.PhysicalPlan{indexReader},
		indexPlan:      indexScan,
		TablePlans:     []base.PhysicalPlan{tableReader},
		tablePlan:      tableScan,
		ExtraHandleCol: col,
		PushedLimit:    &PushedDownLimit{1, 2},
	}
	indexLookup = indexLookup.Init(ctx, 0)
	require.NoError(t, checkPhysicalPlanClone(indexLookup))

	// selection
	sel := &PhysicalSelection{Conditions: []expression.Expression{col, cst}}
	sel = sel.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(sel))

	// maxOneRow
	maxOneRow := &PhysicalMaxOneRow{}
	maxOneRow = maxOneRow.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(maxOneRow))

	// projection
	proj := &PhysicalProjection{Exprs: []expression.Expression{col, cst}}
	proj = proj.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(proj))

	// limit
	lim := &PhysicalLimit{Count: 1, Offset: 2}
	lim = lim.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(lim))

	// sort
	byItems := []*util.ByItems{{Expr: col}, {Expr: cst}}
	sort := &PhysicalSort{ByItems: byItems}
	sort = sort.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(sort))

	// topN
	topN := &PhysicalTopN{ByItems: byItems, Offset: 2333, Count: 2333}
	topN = topN.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(topN))

	// stream agg
	streamAgg := &PhysicalStreamAgg{basePhysicalAgg{
		AggFuncs:     aggDescs,
		GroupByItems: []expression.Expression{col, cst},
	}}
	streamAgg = streamAgg.initForStream(ctx, stats, 0)
	streamAgg.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(streamAgg))

	// hash agg
	hashAgg := &PhysicalHashAgg{basePhysicalAgg{
		AggFuncs:     aggDescs,
		GroupByItems: []expression.Expression{col, cst},
	}}
	hashAgg = hashAgg.initForHash(ctx, stats, 0)
	hashAgg.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(hashAgg))

	// hash join
	hashJoin := &PhysicalHashJoin{
		Concurrency:     4,
		UseOuterToBuild: true,
	}
	hashJoin = hashJoin.Init(ctx, stats, 0)
	hashJoin.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(hashJoin))

	// merge join
	mergeJoin := &PhysicalMergeJoin{
		CompareFuncs: []expression.CompareFunc{expression.CompareInt},
		Desc:         true,
	}
	mergeJoin = mergeJoin.Init(ctx, stats, 0)
	mergeJoin.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(mergeJoin))
}

//go:linkname valueInterface reflect.valueInterface
func valueInterface(v reflect.Value, safe bool) any

func typeName(t reflect.Type) string {
	path := t.String()
	tmp := strings.Split(path, ".")
	return tmp[len(tmp)-1]
}

func checkPhysicalPlanClone(p base.PhysicalPlan) error {
	cloned, err := p.Clone()
	if err != nil {
		return err
	}
	whiteList := []string{"*property.StatsInfo", "*sessionctx.Context", "*mock.Context", "*types.FieldType"}
	return checkDeepClonedCore(reflect.ValueOf(p), reflect.ValueOf(cloned), typeName(reflect.TypeOf(p)), whiteList, nil)
}

// checkDeepClonedCore is used to check if v2 is deep cloned from v1.
// It's modified from reflect.deepValueEqual. We cannot use reflect.DeepEqual here since they have different
// logic, for example, if two pointers point the same address, they will pass the DeepEqual check while failing in the DeepClone check.
func checkDeepClonedCore(v1, v2 reflect.Value, path string, whiteList []string, visited map[visit]bool) error {
	if !v1.IsValid() || !v2.IsValid() {
		if v1.IsValid() != v2.IsValid() {
			return errors.Errorf("invalid")
		}
		return nil
	}
	if v1.Type() != v2.Type() {
		return errors.Errorf("different type %v, %v, path %v", v1.Type(), v2.Type(), path)
	}

	if visited == nil {
		visited = make(map[visit]bool)
	}
	hard := func(k reflect.Kind) bool {
		switch k {
		case reflect.Map, reflect.Slice, reflect.Ptr, reflect.Interface:
			return true
		}
		return false
	}
	if v1.CanAddr() && v2.CanAddr() && hard(v1.Kind()) {
		addr1 := unsafe.Pointer(v1.UnsafeAddr())
		addr2 := unsafe.Pointer(v2.UnsafeAddr())
		if uintptr(addr1) > uintptr(addr2) {
			addr1, addr2 = addr2, addr1
		}
		typ := v1.Type()
		v := visit{addr1, addr2, typ}
		if visited[v] {
			return nil
		}
		visited[v] = true
	}

	switch v1.Kind() {
	case reflect.Array:
		for i := 0; i < v1.Len(); i++ {
			if err := checkDeepClonedCore(v1.Index(i), v2.Index(i), fmt.Sprintf("%v[%v]", path, i), whiteList, visited); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if (v1.IsNil() && v2.IsNil()) || (v1.Len() == 0 && v2.Len() == 0) {
			return nil
		}
		if v1.Len() != v2.Len() {
			return errors.Errorf("different slice lengths, len %v, %v, path %v", v1.Len(), v2.Len(), path)
		}
		if v1.IsNil() != v2.IsNil() {
			if v1.Len() == 0 && v2.Len() == 0 {
				return nil // nil and an empty slice are accepted
			}
			return errors.Errorf("different slices nil %v, %v, path %v", v1.IsNil(), v2.IsNil(), path)
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("invalid slice pointers, path %v", path)
		}
		for i := 0; i < v1.Len(); i++ {
			if err := checkDeepClonedCore(v1.Index(i), v2.Index(i), fmt.Sprintf("%v[%v]", path, i), whiteList, visited); err != nil {
				return err
			}
		}
	case reflect.Interface:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.IsNil() != v2.IsNil() {
			return errors.Errorf("invalid interfaces, path %v", path)
		}
		return checkDeepClonedCore(v1.Elem(), v2.Elem(), path, whiteList, visited)
	case reflect.Ptr:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.Pointer() == v2.Pointer() {
			typeName := v1.Type().String()
			inWhiteList := false
			for _, whiteName := range whiteList {
				if whiteName == typeName {
					inWhiteList = true
					break
				}
			}
			if inWhiteList {
				return nil
			}
			return errors.Errorf("same pointer, path %v", path)
		}
		return checkDeepClonedCore(v1.Elem(), v2.Elem(), path, whiteList, visited)
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			if err := checkDeepClonedCore(v1.Field(i), v2.Field(i), fmt.Sprintf("%v.%v", path, typeName(v1.Field(i).Type())), whiteList, visited); err != nil {
				return err
			}
		}
	case reflect.Map:
		if (v1.IsNil() && v2.IsNil()) || (v1.Len() == 0 && v2.Len() == 0) {
			return nil
		}
		if v1.IsNil() != v2.IsNil() || v1.Len() != v2.Len() {
			return errors.Errorf("different maps nil: %v, %v, len: %v, %v, path: %v", v1.IsNil(), v2.IsNil(), v1.Len(), v2.Len(), path)
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("invalid map pointers, path %v", path)
		}
		if len(v1.MapKeys()) != len(v2.MapKeys()) {
			return errors.Errorf("invalid map")
		}
		for _, k := range v1.MapKeys() {
			val1 := v1.MapIndex(k)
			val2 := v2.MapIndex(k)
			if !val1.IsValid() || !val2.IsValid() {
				if err := checkDeepClonedCore(val1, val2, fmt.Sprintf("%v[%v]", path, typeName(k.Type())), whiteList, visited); err != nil {
					return err
				}
			}
		}
	case reflect.Func:
		if v1.IsNil() != v2.IsNil() {
			return errors.Errorf("invalid functions, path %v", path)
		}
		return nil // assume that these functions are stateless
	default:
		if valueInterface(v1, false) != valueInterface(v2, false) {
			return errors.Errorf("different values, path %v", path)
		}
	}
	return nil
}

func TestHandleAnalyzeOptionsV1AndV2(t *testing.T) {
	require.Equal(t, len(analyzeOptionDefault), len(analyzeOptionDefaultV2), "analyzeOptionDefault and analyzeOptionDefaultV2 should have the same length")

	tests := []struct {
		name        string
		opts        []ast.AnalyzeOpt
		statsVer    int
		ExpectedErr string
	}{
		{
			name: "Too big TopN option",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptNumTopN,
					Value: ast.NewValueExpr(100000+1, "", ""),
				},
			},
			statsVer:    statistics.Version1,
			ExpectedErr: "Value of analyze option TOPN should not be larger than 100000",
		},
		{
			name: "Use SampleRate option in stats version 1",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptSampleRate,
					Value: ast.NewValueExpr(1, "", ""),
				},
			},
			statsVer:    statistics.Version1,
			ExpectedErr: "Version 1's statistics doesn't support the SAMPLERATE option, please set tidb_analyze_version to 2",
		},
		{
			name: "Too big SampleRate option",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptSampleRate,
					Value: ast.NewValueExpr(2, "", ""),
				},
			},
			statsVer:    statistics.Version2,
			ExpectedErr: "Value of analyze option SAMPLERATE should not larger than 1.000000, and should be greater than 0",
		},
		{
			name: "Too big NumBuckets option",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptNumBuckets,
					Value: ast.NewValueExpr(100000+1, "", ""),
				},
			},
			statsVer:    2,
			ExpectedErr: "Value of analyze option BUCKETS should be positive and not larger than 100000",
		},
		{
			name: "Set both sample num and sample rate",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptNumSamples,
					Value: ast.NewValueExpr(100, "", ""),
				},
				{
					Type:  ast.AnalyzeOptSampleRate,
					Value: ast.NewValueExpr(0.1, "", ""),
				},
			},
			statsVer:    statistics.Version2,
			ExpectedErr: "ou can only either set the value of the sample num or set the value of the sample rate. Don't set both of them",
		},
		{
			name: "Too big CMSketchDepth and CMSketchWidth option",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptCMSketchDepth,
					Value: ast.NewValueExpr(1024, "", ""),
				},
				{
					Type:  ast.AnalyzeOptCMSketchWidth,
					Value: ast.NewValueExpr(2048, "", ""),
				},
			},
			statsVer:    statistics.Version1,
			ExpectedErr: "cm sketch size(depth * width) should not larger than 1258291",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handleAnalyzeOptions(tt.opts, tt.statsVer)
			if tt.ExpectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.ExpectedErr)
			} else {
				require.NoError(t, err)
			}

			if tt.statsVer == statistics.Version2 {
				_, err := handleAnalyzeOptionsV2(tt.opts)
				if tt.ExpectedErr != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tt.ExpectedErr)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestGetFullAnalyzeColumnsInfo(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	pb, _ := NewPlanBuilder().Init(ctx, nil, hint.NewQBHintHandler(nil))

	// Create a new TableName instance.
	tableName := &ast.TableName{
		Schema: model.NewCIStr("test"),
		Name:   model.NewCIStr("my_table"),
	}
	columns := []*model.ColumnInfo{
		{
			ID:        1,
			Name:      model.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
		{
			ID:        2,
			Name:      model.NewCIStr("name"),
			FieldType: *types.NewFieldType(mysql.TypeString),
		},
		{
			ID:        3,
			Name:      model.NewCIStr("age"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
	}
	tableName.TableInfo = &model.TableInfo{
		Columns: columns,
	}

	// Test case 1: DefaultChoice.
	cols, _, err := pb.getFullAnalyzeColumnsInfo(tableName, model.DefaultChoice, nil, nil, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, columns, cols)

	// Test case 2: AllColumns.
	cols, _, err = pb.getFullAnalyzeColumnsInfo(tableName, model.AllColumns, nil, nil, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, columns, cols)

	mustAnalyzedCols := &calcOnceMap{data: make(map[int64]struct{})}

	// TODO(hi-rustin): Find a better way to mock SQL execution.
	// Test case 3: PredicateColumns.

	// Test case 4: ColumnList.
	specifiedCols := []*model.ColumnInfo{columns[0], columns[2]}
	mustAnalyzedCols.data[3] = struct{}{}
	cols, _, err = pb.getFullAnalyzeColumnsInfo(tableName, model.ColumnList, specifiedCols, nil, mustAnalyzedCols, false, false)
	require.NoError(t, err)
	require.Equal(t, specifiedCols, cols)
}

func TestRequireInsertAndSelectPriv(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	pb, _ := NewPlanBuilder().Init(ctx, nil, hint.NewQBHintHandler(nil))

	tables := []*ast.TableName{
		{
			Schema: model.NewCIStr("test"),
			Name:   model.NewCIStr("t1"),
		},
		{
			Schema: model.NewCIStr("test"),
			Name:   model.NewCIStr("t2"),
		},
	}

	pb.requireInsertAndSelectPriv(tables)
	require.Len(t, pb.visitInfo, 4)
	require.Equal(t, "test", pb.visitInfo[0].db)
	require.Equal(t, "t1", pb.visitInfo[0].table)
	require.Equal(t, mysql.InsertPriv, pb.visitInfo[0].privilege)
	require.Equal(t, mysql.SelectPriv, pb.visitInfo[1].privilege)
}
