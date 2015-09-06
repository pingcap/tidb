// Copyright 2015 PingCAP, Inc.
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

package expressions

import (
	"fmt"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ expression.Expression = (*Conversion)(nil)
)

// Conversion is the conversion expression like "type ()".
type Conversion struct {
	// Tp is the conversion type.
	Tp byte
	// Val is the expression to be converted.
	Val expression.Expression
}

// Clone implements the Expression Clone interface.
func (c *Conversion) Clone() (expression.Expression, error) {
	Val, err := c.Val.Clone()
	if err != nil {
		return nil, err
	}

	return &Conversion{Tp: c.Tp, Val: Val}, nil
}

// IsStatic implements the Expression IsStatic interface.
func (c *Conversion) IsStatic() bool {
	return c.Val.IsStatic()
}

// String implements the Expression String interface.
func (c *Conversion) String() string {
	return fmt.Sprintf("%s(%s)", types.TypeStr(c.Tp), c.Val)
}

// Eval implements the Expression Eval interface.
func (c *Conversion) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	Val, err := c.Val.Eval(ctx, args)
	if err != nil {
		return
	}
	ft := types.NewFieldType(c.Tp)
	return types.Convert(Val, ft)
}
