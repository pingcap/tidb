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
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Expression = (*FunctionTrim)(nil)
)

const (
	// TrimBothDefault trims from both direction by default.
	TrimBothDefault = iota
	// TrimBoth trims from both direction with explicit notation.
	TrimBoth
	// TrimLeading trims from left.
	TrimLeading
	// TrimTrailing trims from right.
	TrimTrailing
)

// FunctionTrim remove leading/trailing/both remstr.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
type FunctionTrim struct {
	Str       Expression
	RemStr    Expression
	Direction int
}

// Clone implements the Expression Clone interface.
func (f *FunctionTrim) Clone() Expression {
	nf := &FunctionTrim{
		Str:       f.Str.Clone(),
		Direction: f.Direction,
	}
	if f.RemStr != nil {
		nf.RemStr = f.RemStr.Clone()
	}
	return nf
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionTrim) IsStatic() bool {
	return f.Str.IsStatic() && (f.RemStr == nil || f.RemStr.IsStatic())
}

func (f *FunctionTrim) getDirectionStr() string {
	switch f.Direction {
	case TrimBoth:
		return "BOTH"
	case TrimLeading:
		return "LEADING"
	case TrimTrailing:
		return "TRAILING"
	default:
		return ""
	}
}

// String implements the Expression String interface.
func (f *FunctionTrim) String() string {
	if f.Direction == TrimBothDefault {
		if f.RemStr == nil {
			return fmt.Sprintf("TRIM(%s)", f.Str)
		}
		return fmt.Sprintf("TRIM(%s FROM %s)", f.RemStr, f.Str)
	}
	if f.RemStr == nil {
		return fmt.Sprintf("TRIM(%s FROM %s)", f.getDirectionStr(), f.Str)
	}
	return fmt.Sprintf("TRIM(%s %s FROM %s)", f.getDirectionStr(), f.RemStr, f.Str)
}

const spaceChars = "\n\t\r "

// Eval implements the Expression Eval interface.
func (f *FunctionTrim) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	// eval str
	fs, err := f.Str.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if types.IsNil(fs) {
		return nil, nil
	}
	str, err := types.ToString(fs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	remstr := ""
	// eval remstr
	if f.RemStr != nil {
		fs, err = f.RemStr.Eval(ctx, args)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if types.IsNil(fs) {
			return nil, nil
		}
		remstr, err = types.ToString(fs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// Do trim
	if f.Direction == TrimLeading {
		if len(remstr) > 0 {
			return trimLeft(str, remstr), nil
		}
		return strings.TrimLeft(str, spaceChars), nil
	} else if f.Direction == TrimTrailing {
		if len(remstr) > 0 {
			return trimRight(str, remstr), nil
		}
		return strings.TrimRight(str, spaceChars), nil
	}
	if len(remstr) > 0 {
		x := trimLeft(str, remstr)
		x = trimRight(x, remstr)
		return x, nil
	}
	return strings.Trim(str, spaceChars), nil
}

func trimLeft(str, remstr string) string {
	for {
		x := strings.TrimPrefix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

func trimRight(str, remstr string) string {
	for {
		x := strings.TrimSuffix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

// Accept implements Expression Accept interface.
func (f *FunctionTrim) Accept(v Visitor) (Expression, error) {
	return v.VisitFunctionTrim(f)
}
