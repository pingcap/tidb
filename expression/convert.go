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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"

	"golang.org/x/text/transform"
)

// FunctionConvert provides a way to convert data between different character sets.
// See: https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
type FunctionConvert struct {
	Expr    Expression
	Charset string
}

// Clone implements the Expression Clone interface.
func (f *FunctionConvert) Clone() Expression {
	expr := f.Expr.Clone()
	nf := &FunctionConvert{
		Expr:    expr,
		Charset: f.Charset,
	}
	return nf
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionConvert) IsStatic() bool {
	return f.Expr.IsStatic()
}

// String implements the Expression String interface.
func (f *FunctionConvert) String() string {
	return fmt.Sprintf("CONVERT(%s AS %s)", f.Expr.String(), f.Charset)
}

// Eval implements the Expression Eval interface.
func (f *FunctionConvert) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	value, err := f.Expr.Eval(ctx, args)
	if err != nil {
		return nil, err
	}

	// Casting nil to any type returns nil
	if value == nil {
		return nil, nil
	}
	str, ok := value.(string)
	if !ok {
		return nil, nil
	}
	if strings.ToLower(f.Charset) == "ascii" {
		return value, nil
	} else if strings.ToLower(f.Charset) == "utf8mb4" {
		return value, nil
	}

	encoding, _ := Lookup(f.Charset)
	if encoding == nil {
		return nil, fmt.Errorf("unknown encoding: %s", f.Charset)
	}

	target, _, err := transform.String(encoding.NewDecoder(), str)
	if err != nil {
		log.Errorf("Convert %s to %s with error: %v", str, f.Charset, err)
		return nil, errors.Trace(err)
	}
	return target, nil
}

// Accept implements Expression Accept interface.
func (f *FunctionConvert) Accept(v Visitor) (Expression, error) {
	return v.VisitFunctionConvert(f)
}
