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
	_ "unsafe" // required by go:linkname

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestShow(t *testing.T) {
	node := &ast.ShowStmt{}
	tps := []ast.ShowStmtType{
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
	builder, _ := NewPlanBuilder().Init(MockContext(), nil, &hint.BlockHintProcessor{})

	// Make sure PlanBuilder.getExpressionRewriter() provides clean rewriter from pool.
	// First, pick one rewriter from the pool and make it dirty.
	builder.rewriterCounter++
	dirtyRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	dirtyRewriter.asScalar = true
	dirtyRewriter.aggrMap = make(map[*ast.AggregateFuncExpr]int)
	dirtyRewriter.preprocess = func(ast.Node) ast.Node { return nil }
	dirtyRewriter.insertPlan = &Insert{}
	dirtyRewriter.disableFoldCounter = 1
	dirtyRewriter.ctxStack = make([]expression.Expression, 2)
	dirtyRewriter.ctxNameStk = make([]*types.FieldName, 2)
	builder.rewriterCounter--
	// Then, pick again and check if it's cleaned up.
	builder.rewriterCounter++
	cleanRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	require.Equal(t, dirtyRewriter, cleanRewriter)
	require.Equal(t, false, cleanRewriter.asScalar)
	require.Nil(t, cleanRewriter.aggrMap)
	require.Nil(t, cleanRewriter.preprocess)
	require.Nil(t, cleanRewriter.insertPlan)
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
	for _, c := range cases {
		st, err := parser.New().ParseOneStmt(c.SQL, "", "")
		require.NoError(t, err)
		stmt := st.(*ast.SelectStmt)
		expr := stmt.Fields.Fields[0].Expr

		builder, _ := NewPlanBuilder().Init(ctx, nil, &hint.BlockHintProcessor{})
		builder.rewriterCounter++
		rewriter := builder.getExpressionRewriter(context.TODO(), nil)
		require.NotNil(t, rewriter)
		require.Equal(t, 0, rewriter.disableFoldCounter)
		rewrittenExpression, _, err := builder.rewriteExprNode(rewriter, expr, true)
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
	checkDeepClone := func(p1, p2 PhysicalPlan) error {
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

func TestPhysicalPlanClone(t *testing.T) {
	ctx := mock.NewContext()
	col, cst := &expression.Column{RetType: types.NewFieldType(mysql.TypeString)}, &expression.Constant{RetType: types.NewFieldType(mysql.TypeLonglong)}
	stats := &property.StatsInfo{RowCount: 1000}
	schema := expression.NewSchema(col)
	tblInfo := &model.TableInfo{}
	idxInfo := &model.IndexInfo{}
	hist := &statistics.Histogram{Bounds: chunk.New(nil, 0, 0)}
	aggDesc1, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, false)
	require.NoError(t, err)
	aggDesc2, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{cst}, true)
	require.NoError(t, err)
	aggDescs := []*aggregation.AggFuncDesc{aggDesc1, aggDesc2}

	// table scan
	tableScan := &PhysicalTableScan{
		AccessCondition: []expression.Expression{col, cst},
		Table:           tblInfo,
		Hist:            hist,
	}
	tableScan = tableScan.Init(ctx, 0)
	tableScan.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(tableScan))

	// table reader
	tableReader := &PhysicalTableReader{
		tablePlan:  tableScan,
		TablePlans: []PhysicalPlan{tableScan},
		StoreType:  kv.TiFlash,
	}
	tableReader = tableReader.Init(ctx, 0)
	require.NoError(t, checkPhysicalPlanClone(tableReader))

	// index scan
	indexScan := &PhysicalIndexScan{
		AccessCondition:  []expression.Expression{col, cst},
		Table:            tblInfo,
		Index:            idxInfo,
		Hist:             hist,
		dataSourceSchema: schema,
	}
	indexScan = indexScan.Init(ctx, 0)
	indexScan.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(indexScan))

	// index reader
	indexReader := &PhysicalIndexReader{
		indexPlan:     indexScan,
		IndexPlans:    []PhysicalPlan{indexScan},
		OutputColumns: []*expression.Column{col, col},
	}
	indexReader = indexReader.Init(ctx, 0)
	require.NoError(t, checkPhysicalPlanClone(indexReader))

	// index lookup
	indexLookup := &PhysicalIndexLookUpReader{
		IndexPlans:     []PhysicalPlan{indexReader},
		indexPlan:      indexScan,
		TablePlans:     []PhysicalPlan{tableReader},
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
func valueInterface(v reflect.Value, safe bool) interface{}

func typeName(t reflect.Type) string {
	path := t.String()
	tmp := strings.Split(path, ".")
	return tmp[len(tmp)-1]
}

func checkPhysicalPlanClone(p PhysicalPlan) error {
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

type visit struct {
	a1  unsafe.Pointer
	a2  unsafe.Pointer
	typ reflect.Type
}
