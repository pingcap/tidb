package ranger

import (
	"testing"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

var benchmarkBuildFromBinOpSink []*point

func BenchmarkBuildFromBinOpRandomRanges(b *testing.B) {
	exprCtx := exprstatic.NewExprContext()
	sctx := &rangerctx.RangerContext{
		TypeCtx: types.DefaultStmtNoWarningContext,
		ErrCtx:  errctx.StrictNoWarningContext,
		ExprCtx: exprCtx,
	}
	intType := types.NewFieldType(mysql.TypeLong)
	intType.AddFlag(mysql.NotNullFlag)
	boolType := types.NewFieldType(mysql.TypeTiny)
	column := &expression.Column{UniqueID: 1, ID: 1, RetType: intType}

	makeComparison := func(op string, value int64) *expression.ScalarFunction {
		constant := &expression.Constant{
			Value:   types.NewIntDatum(value),
			RetType: intType,
		}
		return expression.NewFunctionInternal(exprCtx, op, boolType, column, constant).(*expression.ScalarFunction)
	}

	rangeBounds := make([]*expression.ScalarFunction, 0, 20)
	for i := 0; i < 10; i++ {
		low := int64(i*100 + 1)
		rangeBounds = append(rangeBounds,
			makeComparison(ast.GE, low),
			makeComparison(ast.LE, low+5),
		)
	}

	cases := []struct {
		name  string
		exprs []*expression.ScalarFunction
		size  int
	}{
		{name: "bounds=10", exprs: rangeBounds, size: 2},
		{name: "eq", exprs: []*expression.ScalarFunction{makeComparison(ast.EQ, 101)}, size: 2},
		{name: "ne", exprs: []*expression.ScalarFunction{makeComparison(ast.NE, 101)}, size: 4},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			rb := builder{sctx: sctx}
			for _, expr := range tc.exprs {
				points := rb.buildFromBinOp(expr, intType, types.UnspecifiedLength, true)
				if rb.err != nil {
					b.Fatal(rb.err)
				}
				if len(points) != tc.size {
					b.Fatalf("unexpected baseline result length: %d", len(points))
				}
			}

			var points []*point
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for _, expr := range tc.exprs {
					points = rb.buildFromBinOp(expr, intType, types.UnspecifiedLength, true)
				}
			}
			b.StopTimer()

			benchmarkBuildFromBinOpSink = points
			if rb.err != nil {
				b.Fatal(rb.err)
			}
			if len(points) != tc.size {
				b.Fatalf("unexpected result length: %d", len(points))
			}
		})
	}
}

func TestPointStorageIndependence(t *testing.T) {
	pair := newPointPair(
		point{value: types.NewIntDatum(1), start: true},
		point{value: types.NewIntDatum(2)},
	)
	sibling := newPointPair(
		point{value: types.NewIntDatum(3), start: true},
		point{value: types.NewIntDatum(4)},
	)
	if len(pair) != 2 || cap(pair) != 2 || pair[0] == pair[1] {
		t.Fatal("unexpected point-pair storage")
	}
	pair[0].value.SetInt64(99)
	pair[0].excl = true
	if pair[1].value.GetInt64() != 2 || pair[1].excl {
		t.Fatal("points in one pair alias")
	}
	if sibling[0].value.GetInt64() != 3 || sibling[0].excl {
		t.Fatal("independent point pairs alias")
	}

	quad := newPointQuad(
		point{value: types.NewIntDatum(1)},
		point{value: types.NewIntDatum(2)},
		point{value: types.NewIntDatum(3)},
		point{value: types.NewIntDatum(4)},
	)
	if len(quad) != 4 || cap(quad) != 4 {
		t.Fatal("unexpected point-quad storage")
	}
	for i := range quad {
		for j := i + 1; j < len(quad); j++ {
			if quad[i] == quad[j] {
				t.Fatal("points in quad alias")
			}
		}
	}
	quad[0].value.SetInt64(100)
	for i := 1; i < len(quad); i++ {
		if quad[i].value.GetInt64() != int64(i+1) {
			t.Fatal("point mutation changed a sibling")
		}
	}
}
