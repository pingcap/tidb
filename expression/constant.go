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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
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

// Eval implements Expression interface.
func (c *Constant) Eval(_ []types.Datum) (types.Datum, error) {
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(_ []types.Datum, sc *variable.StatementContext) (int64, bool, error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return 0, true, nil
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral {
		res, err := c.Value.ToInt64(sc)
		return res, err != nil, errors.Trace(err)
	}
	return c.Value.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(_ []types.Datum, sc *variable.StatementContext) (float64, bool, error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return 0, true, nil
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral {
		res, err := c.Value.ToFloat64(sc)
		return res, err != nil, errors.Trace(err)
	}
	return c.Value.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(_ []types.Datum, sc *variable.StatementContext) (string, bool, error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return "", true, nil
	}
	res, err := c.Value.ToString()
	return res, err != nil, errors.Trace(err)
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(_ []types.Datum, sc *variable.StatementContext) (*types.MyDecimal, bool, error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return nil, true, nil
	}
	res, err := c.Value.ToDecimal(sc)
	return res, err != nil, errors.Trace(err)
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(_ []types.Datum, sc *variable.StatementContext) (val types.Time, isNull bool, err error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return types.Time{}, true, nil
	}
	return c.Value.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(_ []types.Datum, sc *variable.StatementContext) (val types.Duration, isNull bool, err error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return types.Duration{}, true, nil
	}
	return c.Value.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of Constant.
func (c *Constant) EvalJSON(_ []types.Datum, sc *variable.StatementContext) (json.JSON, bool, error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return json.JSON{}, true, nil
	}
	return c.Value.GetMysqlJSON(), false, nil
}

// Equal implements Expression interface.
func (c *Constant) Equal(b Expression, ctx context.Context) bool {
	y, ok := b.(*Constant)
	if !ok {
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
	bytes, err := codec.EncodeValue(nil, c.Value)
	terror.Log(errors.Trace(err))
	return bytes
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ *Schema) {
}
