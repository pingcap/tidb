package ranger

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

var (
	benchmarkExtractAccessesSink      []expression.Expression
	benchmarkExtractFiltersSink       []expression.Expression
	benchmarkExtractNewConditionsSink []expression.Expression
	benchmarkExtractColumnValuesSink  []*valueInfo
	benchmarkExtractEmptyRangeSink    bool
)

func BenchmarkExtractEqAndInConditionRandomRanges(b *testing.B) {
	sctx, cols, lengths, rangeConditions, eqConditions := makeExtractEqAndInBenchmarkInput(b)

	for _, test := range []struct {
		name       string
		conditions []expression.Expression
	}{
		{name: "range-pair", conditions: rangeConditions},
		{name: "surviving-eq", conditions: eqConditions},
	} {
		b.Run(test.name, func(b *testing.B) {
			var accesses, filters, newConditions []expression.Expression
			var columnValues []*valueInfo
			var emptyRange bool

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				accesses, filters, newConditions, columnValues, emptyRange =
					ExtractEqAndInCondition(sctx, test.conditions, cols, lengths)
			}
			b.StopTimer()

			benchmarkExtractAccessesSink = accesses
			benchmarkExtractFiltersSink = filters
			benchmarkExtractNewConditionsSink = newConditions
			benchmarkExtractColumnValuesSink = columnValues
			benchmarkExtractEmptyRangeSink = emptyRange
		})
	}
}

func TestExtractEqAndInConditionNoPotentialOwnership(t *testing.T) {
	sctx, cols, lengths, conditions, _ := makeExtractEqAndInBenchmarkInput(t)
	accesses, filters, newConditions, columnValues, emptyRange :=
		ExtractEqAndInCondition(sctx, conditions, cols, lengths)

	if accesses == nil || len(accesses) != 0 {
		t.Fatalf("unexpected accesses: nil=%v len=%d", accesses == nil, len(accesses))
	}
	if filters != nil {
		t.Fatalf("unexpected filters: %v", filters)
	}
	if emptyRange {
		t.Fatal("unexpected empty range")
	}
	if len(newConditions) != len(conditions) {
		t.Fatalf("unexpected new condition count: %d", len(newConditions))
	}
	if len(columnValues) != len(cols) || columnValues[0] != nil {
		t.Fatalf("unexpected column values: %v", columnValues)
	}
	for i := range conditions {
		if newConditions[i] != conditions[i] {
			t.Fatalf("condition %d changed", i)
		}
	}

	newConditions[0] = nil
	if conditions[0] == nil {
		t.Fatal("new conditions alias the input slice")
	}
}

func TestExtractEqAndInConditionSurvivingAccess(t *testing.T) {
	sctx, cols, lengths, _, conditions := makeExtractEqAndInBenchmarkInput(t)
	accesses, filters, newConditions, columnValues, emptyRange :=
		ExtractEqAndInCondition(sctx, conditions, cols, lengths)

	if emptyRange {
		t.Fatal("unexpected empty range")
	}
	if len(accesses) != 1 {
		t.Fatalf("unexpected accesses: %v", accesses)
	}
	if filters != nil {
		t.Fatalf("unexpected filters: %v", filters)
	}
	if len(newConditions) != 0 {
		t.Fatalf("unexpected new conditions: %v", newConditions)
	}
	if len(columnValues) != len(cols) || columnValues[0] == nil {
		t.Fatalf("unexpected column values: %v", columnValues)
	}
}

type extractEqAndInTestContext interface {
	Helper()
	Fatalf(format string, args ...any)
}

func makeExtractEqAndInBenchmarkInput(t extractEqAndInTestContext) (
	*rangerctx.RangerContext,
	[]*expression.Column,
	[]int,
	[]expression.Expression,
	[]expression.Expression,
) {
	t.Helper()
	sctx := mock.NewContext()
	intType := types.NewFieldType(mysql.TypeLonglong)
	col := &expression.Column{UniqueID: 1, RetType: intType}
	low := &expression.Constant{Value: types.NewIntDatum(100), RetType: intType}
	high := &expression.Constant{Value: types.NewIntDatum(105), RetType: intType}
	boolType := types.NewFieldType(mysql.TypeTiny)

	ge, err := expression.NewFunction(sctx, ast.GE, boolType, col, low)
	if err != nil {
		t.Fatalf("build GE condition: %v", err)
	}
	le, err := expression.NewFunction(sctx, ast.LE, boolType, col, high)
	if err != nil {
		t.Fatalf("build LE condition: %v", err)
	}
	eqLow, err := expression.NewFunction(sctx, ast.EQ, boolType, col, low)
	if err != nil {
		t.Fatalf("build first EQ condition: %v", err)
	}
	eqSecond, err := expression.NewFunction(sctx, ast.EQ, boolType, col, low)
	if err != nil {
		t.Fatalf("build second EQ condition: %v", err)
	}

	return sctx.GetRangerCtx(),
		[]*expression.Column{col},
		[]int{types.UnspecifiedLength},
		[]expression.Expression{ge, le},
		[]expression.Expression{eqLow, eqSecond}
}
