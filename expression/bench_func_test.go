package expression

import (
	//"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
	"testing"
	//"github.com/ngaut/log"
)

var (
	con0 = &Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong)}
	con1 = &Constant{Value: types.NewDecimalDatum(types.NewDecFromStringForTest("1.2")), RetType: types.NewFieldType(mysql.TypeNewDecimal)}
	con2 = &Constant{Value: types.NewStringDatum("1.2"), RetType: types.NewFieldType(mysql.TypeVarString)}
	con3 = &Constant{Value: types.NewFloat64Datum(1), RetType: types.NewFieldType(mysql.TypeDouble)}
)

// int(1) + decimal(1.2) < string(1.2) - double(1)
//func buildCmpFunc() Expression {
//	ctx := mock.NewContext()
//	ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
//
//	leftArg, err := NewFunction(ctx, ast.Plus, types.NewFieldType(0), con0, con1)
//	if err != nil {
//		log.Warning(err.Error())
//	}
//	rightArg, err := NewFunction(ctx, ast.Minus, types.NewFieldType(0), con2, con3)
//	if err != nil {
//		log.Warning(err.Error())
//	}
//	f, err := NewFunction(ctx, ast.LT, types.NewFieldType(0), leftArg, rightArg)
//	if err != nil {
//		log.Warning(err.Error())
//	}
//	return f
//}
//
//func BenchmarkCmpFunc(b *testing.B) {
//	b.StopTimer()
//	f := buildCmpFunc()
//	b.StartTimer()
//
//	for i := 0; i < b.N; i++ {
//		f.Eval(nil)
//	}
//}
//
func buildConcatFunc() Expression {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
	f, _ := NewFunction(ctx, ast.Concat, types.NewFieldType(0), con0, con1, con2, con3)
	return f
}

func BenchmarkConcatFunc(b *testing.B) {
	b.StopTimer()
	f := buildConcatFunc()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		f.Eval(nil)
	}
}

// int(1) + decimal(1.2)
//func buildPlusFunc() Expression {
//	ctx := mock.NewContext()
//	ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
//	f, _ := NewFunction(ctx, ast.Plus, types.NewFieldType(0), con0, con1)
//	return f
//}
//
//func BenchmarkPlusFunc(b *testing.B) {
//	b.StopTimer()
//	f := buildPlusFunc()
//	b.StartTimer()
//
//	for i := 0; i < b.N; i++ {
//		f.Eval(nil)
//	}
//}
