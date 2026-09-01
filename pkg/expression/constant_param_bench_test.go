package expression

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	benchmarkParamInt    int64
	benchmarkParamNull   bool
	benchmarkParamResult int64
)

func benchmarkParamConstant(value types.Datum) (*Constant, EvalContext) {
	params := variable.NewPlanCacheParamList()
	params.Append(value)
	ctx := exprstatic.NewEvalContext(exprstatic.WithParamList(params))
	con := &Constant{
		RetType:     types.NewFieldType(mysql.TypeUnspecified),
		ParamMarker: &ParamMarker{order: 0},
	}
	return con, ctx
}

func TestParamMarkerEvalIntKinds(t *testing.T) {
	tests := []struct {
		name     string
		datum    types.Datum
		expected int64
		isNull   bool
	}{
		{name: "signed", datum: types.NewIntDatum(42), expected: 42},
		{name: "unsigned", datum: types.NewUintDatum(math.MaxUint64), expected: -1},
		{name: "numeric-string", datum: types.NewStringDatum("42"), expected: 42},
		{name: "binary-literal", datum: types.NewBinaryLiteralDatum([]byte{0x2a}), expected: 42},
		{name: "null", datum: types.NewDatum(nil), isNull: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			con, ctx := benchmarkParamConstant(tt.datum)
			value, isNull, err := con.EvalInt(ctx, chunk.Row{})
			if err != nil {
				t.Fatal(err)
			}
			if value != tt.expected || isNull != tt.isNull {
				t.Fatalf("EvalInt() = (%d, %t), want (%d, %t)", value, isNull, tt.expected, tt.isNull)
			}
			if con.GetType(ctx) == con.GetType(ctx) {
				t.Fatal("GetType returned a shared pointer for a prepared parameter")
			}
		})
	}
}

func BenchmarkParamMarkerIntHotPath(b *testing.B) {
	b.Run("EvalInt/signed", func(b *testing.B) {
		con, ctx := benchmarkParamConstant(types.NewIntDatum(42))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			value, isNull, err := con.EvalInt(ctx, chunk.Row{})
			if err != nil {
				b.Fatal(err)
			}
			benchmarkParamInt = value
			benchmarkParamNull = isNull
		}
	})

	b.Run("EvalInt/unsigned", func(b *testing.B) {
		con, ctx := benchmarkParamConstant(types.NewUintDatum(math.MaxUint64))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			value, isNull, err := con.EvalInt(ctx, chunk.Row{})
			if err != nil {
				b.Fatal(err)
			}
			benchmarkParamInt = value
			benchmarkParamNull = isNull
		}
	})

	b.Run("CompareInt/param_vs_constant", func(b *testing.B) {
		con, ctx := benchmarkParamConstant(types.NewIntDatum(42))
		fixed := NewInt64Const(40)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, isNull, err := CompareInt(ctx, con, fixed, chunk.Row{}, chunk.Row{})
			if err != nil {
				b.Fatal(err)
			}
			benchmarkParamResult = result
			benchmarkParamNull = isNull
		}
	})

	b.Run("EvalInt/plain_constant", func(b *testing.B) {
		con := NewInt64Const(42)
		ctx := exprstatic.NewEvalContext()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			value, isNull, err := con.EvalInt(ctx, chunk.Row{})
			if err != nil {
				b.Fatal(err)
			}
			benchmarkParamInt = value
			benchmarkParamNull = isNull
		}
	})
}
