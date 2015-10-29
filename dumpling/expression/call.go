// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package expression

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression/builtin"
)

var (
	_ Expression = (*Call)(nil)
)

// Call is for function expression.
type Call struct {
	// F is the function name.
	F string
	// Args is the function args.
	Args []Expression
	// Distinct only affetcts sum, avg, count, group_concat,
	// so we can ignore it in other functions
	Distinct bool

	// distinctKey is the unique key when using same Call object for different Eval args.
	// We have already use Call pointer to store aggregate result, so here we will need
	// another unique key to keep aggregate distincter.
	distinctKey *int
}

// NewCall creates a Call expression with function name f, function args arg and
// a distinct flag whether this function supports distinct or not.
func NewCall(f string, args []Expression, distinct bool) (v Expression, err error) {
	x := builtin.Funcs[strings.ToLower(f)]
	if x.F == nil {
		return nil, errors.Errorf("undefined: %s", f)
	}

	if g, min, max := len(args), x.MinArgs, x.MaxArgs; g < min || (max != -1 && g > max) {
		a := []interface{}{}
		for _, v := range args {
			a = append(a, v)
		}
		return nil, badNArgs(min, f, a)
	}

	c := Call{F: f, Distinct: distinct, distinctKey: new(int)}
	for _, val := range args {
		if !val.IsStatic() {
			c.Args = append(c.Args, val)
			continue
		}

		v, err := val.Eval(nil, nil)
		if err != nil {
			return nil, err
		}

		c.Args = append(c.Args, Value{v})
	}

	return &c, nil
}

// Clone implements the Expression Clone interface.
func (c *Call) Clone() Expression {
	list := cloneExpressionList(c.Args)
	return &Call{F: c.F, Args: list, Distinct: c.Distinct}
}

// IsStatic implements the Expression IsStatic interface.
func (c *Call) IsStatic() bool {
	v := builtin.Funcs[strings.ToLower(c.F)]
	if v.F == nil || !v.IsStatic {
		return false
	}

	for _, v := range c.Args {
		if !v.IsStatic() {
			return false
		}
	}
	return true
}

// String implements the Expression String interface.
func (c *Call) String() string {
	distinct := ""
	if c.Distinct {
		distinct = "DISTINCT "
	}
	a := []string{}
	for _, v := range c.Args {
		a = append(a, v.String())
	}
	return fmt.Sprintf("%s(%s%s)", c.F, distinct, strings.Join(a, ", "))
}

// Eval implements the Expression Eval interface.
func (c *Call) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	f, ok := builtin.Funcs[strings.ToLower(c.F)]
	if !ok {
		return nil, errors.Errorf("unknown function %s", c.F)
	}

	a := make([]interface{}, len(c.Args))
	for i, arg := range c.Args {
		if v, err = arg.Eval(ctx, args); err != nil {
			return nil, err
		}
		a[i] = v
	}

	if c.distinctKey == nil {
		// create an unique distinct key if not.
		c.distinctKey = new(int)
	}

	if args != nil {
		args[builtin.ExprEvalFn] = c
		args[builtin.ExprEvalArgCtx] = ctx
		aggDistinct, ok := args[c.distinctKey]
		if !ok {
			// create an aggregate distinct if not.
			aggDistinct = builtin.CreateAggregateDistinct(c.F, c.Distinct)
			args[c.distinctKey] = aggDistinct
		}

		args[builtin.ExprAggDistinct] = aggDistinct
	}
	return f.F(a, args)
}

// Accept implements Expression Accept interface.
func (c *Call) Accept(v Visitor) (Expression, error) {
	return v.VisitCall(c)
}

func badNArgs(min int, s string, args []interface{}) error {
	a := []string{}
	for _, v := range args {
		a = append(a, fmt.Sprintf("%v", v))
	}
	switch len(args) < min {
	case true:
		return errors.Errorf("missing argument to %s(%s)", s, strings.Join(a, ", "))
	default: //case false:
		return errors.Errorf("too many arguments to %s(%s)", s, strings.Join(a, ", "))
	}
}
