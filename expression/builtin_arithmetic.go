package expression

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
	"math"
)

var (
	_ functionClass = &arithmeticPlusFunctionClass{}
	_ functionClass = &arithmeticFunctionClass{}
)

var (
	_ builtinFunc = &builtinArithmeticPlusRealSig{}
	_ builtinFunc = &builtinArithmeticPlusIntSig{}
	_ builtinFunc = &builtinArithmeticPlusDecimalSig{}
	_ builtinFunc = &builtinArithmeticSig{}
)

type arithmeticPlusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticPlusFunctionClass) setFlenDecimal4Real(retTp, a, b *types.FieldType) {
	if a.Decimal != types.UnspecifiedLength && b.Decimal != types.UnspecifiedLength {
		retTp.Decimal = int(math.Max(float64(a.Decimal), float64(b.Decimal)))
		if a.Flen == types.UnspecifiedLength || b.Flen == types.UnspecifiedLength {
			retTp.Flen = types.UnspecifiedLength
		} else {
			digitsInt := int(math.Max(float64(a.Flen-a.Decimal-1), float64(a.Flen-b.Decimal-1)))
			retTp.Flen = digitsInt + retTp.Decimal + 3
			retTp.Flen = int(math.Min(float64(retTp.Flen), float64(mysql.MaxRealWidth)))
		}
	} else {
		retTp.Decimal = types.UnspecifiedLength
		retTp.Flen = mysql.MaxRealWidth
	}
}

func (c *arithmeticPlusFunctionClass) setFlenDecimal4Int(retTp, a, b *types.FieldType) {
	retTp.Decimal = 0
	if a.Flen == types.UnspecifiedLength || b.Flen == types.UnspecifiedLength {
		retTp.Flen = types.UnspecifiedLength
	} else {
		retTp.Flen = int(math.Max(float64(a.Flen), float64(b.Flen))) + 1
		retTp.Flen = int(math.Min(float64(retTp.Flen), float64(mysql.MaxRealWidth)))
	}
}

func (c *arithmeticPlusFunctionClass) getEvalTp(ft *types.FieldType) evalTp {
	switch ft.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeBit, mysql.TypeYear:
		return tpInt
	case mysql.TypeNewDecimal, mysql.TypeDatetime, mysql.TypeDuration:
		return tpDecimal
	default:
		return tpReal
	}
	return tpReal
}

func (c *arithmeticPlusFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	evalTpA := c.getEvalTp(args[0].GetType())
	evalTpB := c.getEvalTp(args[1].GetType())
	if evalTpA == tpInt && evalTpB == tpInt {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		c.setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticPlusIntSig{baseIntBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	} else if evalTpA == tpDecimal || evalTpB == tpDecimal {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpDecimal, tpDecimal, tpDecimal)
		if err != nil {
			return nil, errors.Trace(err)
		}
		c.setFlenDecimal4Real(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticPlusDecimalSig{baseDecimalBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	} else {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
		if err != nil {
			return nil, errors.Trace(err)
		}
		c.setFlenDecimal4Real(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticPlusRealSig{baseRealBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
}

type builtinArithmeticPlusIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinArithmeticPlusIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx

	a, isNull, err := s.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	b, isNull, err := s.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	return a + b, false, nil
}

type builtinArithmeticPlusRealSig struct {
	baseRealBuiltinFunc
}

func (s *builtinArithmeticPlusRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx

	a, isNull, err := s.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	b, isNull, err := s.args[1].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	return a + b, false, nil
}

type builtinArithmeticPlusDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (s *builtinArithmeticPlusDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx

	a, isNull, err := s.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}

	b, isNull, err := s.args[1].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}

	c := &types.MyDecimal{}
	err = types.DecimalAdd(a, b, c)
	if err != nil {
		return nil, true, errors.Trace(err)
	}
	return c, false, nil
}

type arithmeticFunctionClass struct {
	baseFunctionClass
	op opcode.Op
}

func (c *arithmeticFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinArithmeticSig{newBaseBuiltinFunc(args, ctx), c.op}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinArithmeticSig struct {
	baseBuiltinFunc
	op opcode.Op
}

func (s *builtinArithmeticSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := s.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := s.ctx.GetSessionVars().StmtCtx
	a, err := types.CoerceArithmetic(sc, args[0])
	if err != nil {
		return d, errors.Trace(err)
	}

	b, err := types.CoerceArithmetic(sc, args[1])
	if err != nil {
		return d, errors.Trace(err)
	}
	a, b, err = types.CoerceDatum(sc, a, b)
	if err != nil {
		return d, errors.Trace(err)
	}
	if a.IsNull() || b.IsNull() {
		return
	}

	switch s.op {
	case opcode.Minus:
		return types.ComputeMinus(a, b)
	case opcode.Mul:
		return types.ComputeMul(a, b)
	case opcode.Div:
		return types.ComputeDiv(sc, a, b)
	case opcode.Mod:
		return types.ComputeMod(sc, a, b)
	case opcode.IntDiv:
		return types.ComputeIntDiv(sc, a, b)
	default:
		return d, errInvalidOperation.Gen("invalid op %v in arithmetic operation", s.op)
	}
}
