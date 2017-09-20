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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
)

// One stands for a number 1.
var (
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
	Value   types.Datum
	RetType *types.FieldType
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
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

// GetTypeClass implements Expression interface.
func (c *Constant) GetTypeClass() types.TypeClass {
	return c.RetType.ToClass()
}

// Eval implements Expression interface.
func (c *Constant) Eval(_ []types.Datum) (types.Datum, error) {
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(_ []types.Datum, sc *variable.StatementContext) (int64, bool, error) {
	if c.GetType().Tp == mysql.TypeNull {
		return 0, true, nil
	}
	val, isNull, err := evalExprToInt(c, nil, sc)
	return val, isNull, errors.Trace(err)
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(_ []types.Datum, sc *variable.StatementContext) (float64, bool, error) {
	if c.GetType().Tp == mysql.TypeNull {
		return 0, true, nil
	}
	val, isNull, err := evalExprToReal(c, nil, sc)
	return val, isNull, errors.Trace(err)
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(_ []types.Datum, sc *variable.StatementContext) (string, bool, error) {
	if c.GetType().Tp == mysql.TypeNull {
		return "", true, nil
	}
	val, isNull, err := evalExprToString(c, nil, sc)
	return val, isNull, errors.Trace(err)
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(_ []types.Datum, sc *variable.StatementContext) (*types.MyDecimal, bool, error) {
	if c.GetType().Tp == mysql.TypeNull {
		return nil, true, nil
	}
	val, isNull, err := evalExprToDecimal(c, nil, sc)
	return val, isNull, errors.Trace(err)
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(_ []types.Datum, sc *variable.StatementContext) (val types.Time, isNull bool, err error) {
	if c.GetType().Tp == mysql.TypeNull {
		return val, true, nil
	}
	val, isNull, err = evalExprToTime(c, nil, sc)
	return val, isNull, errors.Trace(err)
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(_ []types.Datum, sc *variable.StatementContext) (val types.Duration, isNull bool, err error) {
	if c.GetType().Tp == mysql.TypeNull {
		return val, true, nil
	}
	val, isNull, err = evalExprToDuration(c, nil, sc)
	return val, isNull, errors.Trace(err)
}

// EvalJSON returns JSON representation of Constant.
func (c *Constant) EvalJSON(_ []types.Datum, sc *variable.StatementContext) (json.JSON, bool, error) {
	val, isNull, err := evalExprToJSON(c, nil, sc)
	return val, isNull, errors.Trace(err)
}

// Equal implements Expression interface.
func (c *Constant) Equal(b Expression, ctx context.Context) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	con, err := c.Value.CompareDatum(ctx.GetSessionVars().StmtCtx, y.Value)
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
	var bytes []byte
	bytes, _ = codec.EncodeValue(bytes, c.Value)
	return bytes
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ *Schema) {
}
