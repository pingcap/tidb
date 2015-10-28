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
	_ Expression = (*FunctionSubstring)(nil)
	_ Expression = (*FunctionSubstringIndex)(nil)
	_ Expression = (*FunctionLocate)(nil)
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
	str, err := types.ToString(fs)
	if err != nil {
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

// FunctionSubstringIndex returns the substring as specified.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
type FunctionSubstringIndex struct {
	StrExpr Expression
	Delim   Expression
	Count   Expression
}

// Clone implements the Expression Clone interface.
func (f *FunctionSubstringIndex) Clone() Expression {
	nf := &FunctionSubstringIndex{
		StrExpr: f.StrExpr.Clone(),
		Delim:   f.Delim.Clone(),
		Count:   f.Count.Clone(),
	}
	return nf
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionSubstringIndex) IsStatic() bool {
	return f.StrExpr.IsStatic() && f.Delim.IsStatic() && f.Count.IsStatic()
}

// String implements the Expression String interface.
func (f *FunctionSubstringIndex) String() string {
	return fmt.Sprintf("SUBSTRING_INDEX(%s, %s, %s)", f.StrExpr, f.Delim, f.Count)
}

// Eval implements the Expression Eval interface.
func (f *FunctionSubstringIndex) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	fs, err := f.StrExpr.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	str, err := types.ToString(fs)
	if err != nil {
		return nil, errors.Errorf("Substring_Index invalid args, need string but get %T", fs)
	}

	t, err := f.Delim.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	delim, err := types.ToString(t)
	if err != nil {
		return nil, errors.Errorf("Substring_Index invalid delim, need string but get %T", t)
	}

	t, err = f.Count.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c, err := types.ToInt64(t)
	if err != nil {
		return nil, errors.Trace(err)
	}
	count := int(c)
	strs := strings.Split(str, delim)
	var (
		start = 0
		end   = len(strs)
	)
	if count > 0 {
		// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
		if count < end {
			end = count
		}
	} else {
		// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
		count = -count
		if count < end {
			start = end - count
		}
	}
	substrs := strs[start:end]
	return strings.Join(substrs, delim), nil
}

// Accept implements Expression Accept interface.
func (f *FunctionSubstringIndex) Accept(v Visitor) (Expression, error) {
	return v.VisitFunctionSubstringIndex(f)
}

// FunctionLocate returns the position of the first occurrence of substring.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
type FunctionLocate struct {
	Str    Expression
	SubStr Expression
	Pos    Expression
}

// Clone implements the Expression Clone interface.
func (f *FunctionLocate) Clone() Expression {
	nf := &FunctionLocate{
		Str:    f.Str.Clone(),
		SubStr: f.SubStr.Clone(),
	}
	if f.Pos != nil {
		nf.Pos = f.Pos.Clone()
	}
	return nf
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionLocate) IsStatic() bool {
	return f.Str.IsStatic() && f.SubStr.IsStatic() && (f.Pos == nil || f.Pos.IsStatic())
}

// String implements the Expression String interface.
func (f *FunctionLocate) String() string {
	if f.Pos != nil {
		return fmt.Sprintf("LOCATE(%s, %s, %s)", f.SubStr, f.Str, f.Pos)
	}
	return fmt.Sprintf("LOCATE(%s, %s)", f.SubStr, f.Str)
}

// Eval implements the Expression Eval interface.
func (f *FunctionLocate) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
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
	// eval substr
	fs, err = f.SubStr.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if types.IsNil(fs) {
		return nil, nil
	}
	substr, err := types.ToString(fs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// eval pos
	pos := 0
	if f.Pos != nil {
		t, err := f.Pos.Eval(ctx, args)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p, err := types.ToInt64(t)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pos = int(p)
	}
	// eval locate
	if pos < 0 || pos > len(str) {
		return 0, errors.Errorf("Locate invalid pos args: %d", pos)
	}
	str = str[pos:]
	i := strings.Index(str, substr)
	return i + 1 + pos, nil
}

// Accept implements Expression Accept interface.
func (f *FunctionLocate) Accept(v Visitor) (Expression, error) {
	return v.VisitFunctionLocate(f)
}
