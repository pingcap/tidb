package executor

import (
	"testing"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
)

func TestHashAggTable(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().WindowingUseHighPrecision = false
	tracker := memory.NewTracker(-19, -1)

	args := []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeFloat), Index: 0}}
	desc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncSum, args, false)
	if err != nil {
		panic(err)
	}
	agg1, agg2 := desc.Split([]int{0, 1})

	f1 := aggfuncs.Build(ctx, agg1, 0)
	f2 := aggfuncs.Build(ctx, agg2, 0)
	aggFuncs := []aggfuncs.AggFunc{f1, f2}
	pr1, _ := f1.AllocPartialResult()
	pr2, _ := f2.AllocPartialResult()
	prs := []aggfuncs.PartialResult{pr1, pr2}
	table := NewHashAggResultTable(ctx, aggFuncs, true, tracker)

	if err := table.Put("key", prs); err != nil {
		panic(err)
	}

	if err := table.spill(); err != nil {
		panic(err)
	}

	if err := table.Put("key1", prs); err != nil {
		panic(err)
	}

	_, ok, err := table.Get("key")
	if !ok || err != nil {
		panic(nil)
	}
}
