package mocktikv

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	"time"
	"github.com/pingcap/tidb/sessionctx/variable"
)

func pbToExpr(expr *tipb.Expr, colIDs map[int64]int, sc *variable.StatementContext) (expression.Expression, error) {
	switch expr.Tp {
	case tipb.ExprType_ColumnRef:
		_, id, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		offset, ok := colIDs[id]
		if !ok {
			return nil, errors.Errorf("Can't find column id %d", id)
		}
		return &expression.Column{Index: offset}, nil
	case tipb.ExprType_Null:
		return &expression.Constant{}, nil
	case tipb.ExprType_Int64:
		return evalInt(expr.Val)
	case tipb.ExprType_Uint64:
		return evalUint(expr.Val)
	case tipb.ExprType_String:
		return evalString(expr.Val)
	case tipb.ExprType_Bytes:
		return &expression.Constant{Value: types.NewBytesDatum(expr.Val)}, nil
	case tipb.ExprType_Float32:
		return evalFloat(expr.Val, true)
	case tipb.ExprType_Float64:
		return evalFloat(expr.Val, false)
	case tipb.ExprType_MysqlDecimal:
		return evalDecimal(expr.Val)
	case tipb.ExprType_MysqlDuration:
		return evalDuration(expr.Val)
	}
	args := make([]expression.Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		arg, err := pbToExpr(child, colIDs, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		args = append(args, arg)
	}
	// Then it must be a scalar function.
	return expression.NewDistSQLFunction(sc, expr.Tp, args)
}

func evalInt(val []byte) (*expression.Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid int % x", val)
	}
	d.SetInt64(i)
	return &expression.Constant{Value: d}, nil
}

func evalUint(val []byte) (*expression.Constant, error) {
	var d types.Datum
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid uint % x", val)
	}
	d.SetUint64(u)
	return &expression.Constant{Value: d}, nil
}

func evalString(val []byte) (*expression.Constant, error) {
	var d types.Datum
	d.SetBytesAsString(val)
	return &expression.Constant{Value: d}, nil
}

func evalFloat(val []byte, f32 bool) (*expression.Constant, error) {
	var d types.Datum
	_, f, err := codec.DecodeFloat(val)
	if err != nil {
		return nil, errors.Errorf("invalid float % x", val)
	}
	if f32 {
		d.SetFloat32(float32(f))
	} else {
		d.SetFloat64(f)
	}
	return &expression.Constant{Value: d}, nil
}

func evalDecimal(val []byte) (*expression.Constant, error) {
	_, dec, err := codec.DecodeDecimal(val)
	if err != nil {
		return nil, errors.Errorf("invalid decimal % x", val)
	}
	return &expression.Constant{Value: dec}, nil
}

func evalDuration(val []byte) (*expression.Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid duration %d", i)
	}
	d.SetMysqlDuration(types.Duration{Duration: time.Duration(i), Fsp: types.MaxFsp})
	return &expression.Constant{Value: d}, nil
}
