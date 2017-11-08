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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/codec"
)

var (
	// One stands for a number 1.
	One = &Constant{
		Value:   types.NewDatum(1),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Zero stands for a number 0.
	Zero = &Constant{
		Value:   types.NewDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Null stands for null constant.
	Null = &Constant{
		Value:   types.NewDatum(nil),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
)

// Constant stands for a constant value.
type Constant struct {
	Value        types.Datum
	RetType      *types.FieldType
	DeferredExpr Expression // parameter getter expression
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
	if c.DeferredExpr != nil {
		dt, err := c.Eval(nil)
		if err != nil {
			log.Errorf("Fail to eval constant, err: %s", err.Error())
			return ""
		}
		c.Value.SetValue(dt.GetValue())
	}
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// MarshalJSON implements json.Marshaler interface.
func (c *Constant) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf("\"%s\"", c))
	return buffer.Bytes(), nil
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	con := *c
	return &con
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	return c.RetType
}

// Eval implements Expression interface.
func (c *Constant) Eval(_ types.Row) (types.Datum, error) {
	if c.DeferredExpr != nil {
		if sf, sfOK := c.DeferredExpr.(*ScalarFunction); sfOK {
			dt, err := sf.Eval(nil)
			if err != nil {
				return c.Value, err
			}
			if dt.IsNull() {
				c.Value.SetNull()
				return c.Value, nil
			}
			retType := types.NewFieldType(c.RetType.Tp)
			if retType.Tp == mysql.TypeUnspecified {
				retType.Tp = mysql.TypeVarString
			}
			val, err := dt.ConvertTo(sf.GetCtx().GetSessionVars().StmtCtx, retType)
			if err != nil {
				return c.Value, err
			}
			c.Value.SetValue(val.GetValue())
		}
	}
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(_ types.Row, sc *variable.StatementContext) (int64, bool, error) {
	if c.DeferredExpr != nil {
		dt, err := c.DeferredExpr.Eval(nil)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if dt.IsNull() {
			return 0, true, nil
		}
		val, err := dt.ToInt64(sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		c.Value.SetInt64(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return 0, true, nil
		}
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral || c.Value.Kind() == types.KindString {
		res, err := c.Value.ToInt64(sc)
		return res, err != nil, errors.Trace(err)
	}
	return c.Value.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(_ types.Row, sc *variable.StatementContext) (float64, bool, error) {
	if c.DeferredExpr != nil {
		dt, err := c.DeferredExpr.Eval(nil)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if dt.IsNull() {
			return 0, true, nil
		}
		val, err := dt.ToFloat64(sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		c.Value.SetFloat64(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return 0, true, nil
		}
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral || c.Value.Kind() == types.KindString {
		res, err := c.Value.ToFloat64(sc)
		return res, err != nil, errors.Trace(err)
	}
	return c.Value.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(_ types.Row, sc *variable.StatementContext) (string, bool, error) {
	if c.DeferredExpr != nil {
		dt, err := c.DeferredExpr.Eval(nil)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		if dt.IsNull() {
			return "", true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return "", true, errors.Trace(err)
		}
		c.Value.SetString(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return "", true, nil
		}
	}
	res, err := c.Value.ToString()
	return res, err != nil, errors.Trace(err)
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(_ types.Row, sc *variable.StatementContext) (*types.MyDecimal, bool, error) {
	if c.DeferredExpr != nil {
		dt, err := c.DeferredExpr.Eval(nil)
		if err != nil {
			return nil, true, errors.Trace(err)
		}
		if dt.IsNull() {
			return nil, true, nil
		}
		c.Value.SetValue(dt.GetValue())
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return nil, true, nil
		}
	}
	res, err := c.Value.ToDecimal(sc)
	return res, err != nil, errors.Trace(err)
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(_ types.Row, sc *variable.StatementContext) (val types.Time, isNull bool, err error) {
	if c.DeferredExpr != nil {
		dt, err := c.DeferredExpr.Eval(nil)
		if err != nil {
			return types.Time{}, true, errors.Trace(err)
		}
		if dt.IsNull() {
			return types.Time{}, true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return types.Time{}, true, errors.Trace(err)
		}
		tim, err := types.ParseDatetime(sc, val)
		if err != nil {
			return types.Time{}, true, errors.Trace(err)
		}
		c.Value.SetMysqlTime(tim)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return types.Time{}, true, nil
		}
	}
	return c.Value.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(_ types.Row, sc *variable.StatementContext) (val types.Duration, isNull bool, err error) {
	if c.DeferredExpr != nil {
		dt, err := c.DeferredExpr.Eval(nil)
		if err != nil {
			return types.Duration{}, true, errors.Trace(err)
		}
		if dt.IsNull() {
			return types.Duration{}, true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return types.Duration{}, true, errors.Trace(err)
		}
		dur, err := types.ParseDuration(val, types.MaxFsp)
		if err != nil {
			return types.Duration{}, true, errors.Trace(err)
		}
		c.Value.SetMysqlDuration(dur)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return types.Duration{}, true, nil
		}
	}
	return c.Value.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of Constant.
func (c *Constant) EvalJSON(_ types.Row, sc *variable.StatementContext) (json.JSON, bool, error) {
	if c.DeferredExpr != nil {
		dt, err := c.DeferredExpr.Eval(nil)
		if err != nil {
			return json.JSON{}, true, errors.Trace(err)
		}
		if dt.IsNull() {
			return json.JSON{}, true, nil
		}
		val, err := dt.ConvertTo(sc, types.NewFieldType(mysql.TypeJSON))
		if err != nil {
			return json.JSON{}, true, errors.Trace(err)
		}
		c.Value.SetMysqlJSON(val.GetMysqlJSON())
		c.GetType().Tp = mysql.TypeJSON
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return json.JSON{}, true, nil
		}
	}
	return c.Value.GetMysqlJSON(), false, nil
}

// Equal implements Expression interface.
func (c *Constant) Equal(b Expression, ctx context.Context) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	_, err1 := y.Eval(nil)
	_, err2 := c.Eval(nil)
	if err1 != nil || err2 != nil {
		return false
	}
	con, err := c.Value.CompareDatum(ctx.GetSessionVars().StmtCtx, &y.Value)
	if err != nil || con != 0 {
		return false
	}
	return true
}

// IsCorrelated implements Expression interface.
func (c *Constant) IsCorrelated() bool {
	return false
}

// Decorrelate implements Expression interface.
func (c *Constant) Decorrelate(_ *Schema) Expression {
	return c
}

// HashCode implements Expression interface.
func (c *Constant) HashCode() []byte {
	_, err := c.Eval(nil)
	if err != nil {
		terror.Log(errors.Trace(err))
	}
	bytes, err := codec.EncodeValue(nil, c.Value)
	terror.Log(errors.Trace(err))
	return bytes
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ *Schema) {
}
