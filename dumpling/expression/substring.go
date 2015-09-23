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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
)

// FunctionSubstring returns the substring as specified.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring
type FunctionSubstring struct {
	StrExpr Expression
	Pos     Expression
	Len     Expression
}

// Clone implements the Expression Clone interface.
func (f *FunctionSubstring) Clone() Expression {
	expr := f.StrExpr.Clone()
	nf := &FunctionSubstring{
		StrExpr: expr,
		Pos:     f.Pos,
		Len:     f.Len,
	}
	return nf
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionSubstring) IsStatic() bool {
	return f.StrExpr.IsStatic()
}

// String implements the Expression String interface.
func (f *FunctionSubstring) String() string {
	if f.Len != nil {
		return fmt.Sprintf("SUBSTRING(%s, %s, %s)", f.StrExpr.String(), f.Pos.String(), f.Len.String())
	}
	return fmt.Sprintf("SUBSTRING(%s, %s)", f.StrExpr.String(), f.Pos.String())
}

// Eval implements the Expression Eval interface.
func (f *FunctionSubstring) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	fs, err := f.StrExpr.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	str, ok := fs.(string)
	if !ok {
		return nil, errors.Errorf("Substring invalid args, need string but get %T", fs)
	}

	t, err := f.Pos.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p, ok := t.(int64)
	if !ok {
		return nil, errors.Errorf("Substring invalid pos args, need int but get %T", t)
	}
	pos := int(p)

	length := -1
	if f.Len != nil {
		t, err := f.Len.Eval(ctx, args)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p, ok := t.(int64)
		if !ok {
			return nil, errors.Errorf("Substring invalid len args, need int but get %T", t)
		}
		length = int(p)
	}
	// The forms without a len argument return a substring from string str starting at position pos.
	// The forms with a len argument return a substring len characters long from string str, starting at position pos.
	// The forms that use FROM are standard SQL syntax. It is also possible to use a negative value for pos.
	// In this case, the beginning of the substring is pos characters from the end of the string, rather than the beginning.
	// A negative value may be used for pos in any of the forms of this function.
	if pos < 0 {
		pos = len(str) + pos
	} else {
		pos--
	}
	if pos > len(str) || pos <= 0 {
		pos = len(str)
	}
	end := len(str)
	if length != -1 {
		end = pos + length
	}
	if end > len(str) {
		end = len(str)
	}
	return str[pos:end], nil
}

// Accept implements Expression Accept interface.
func (f *FunctionSubstring) Accept(v Visitor) (Expression, error) {
	return v.VisitFunctionSubstring(f)
}
