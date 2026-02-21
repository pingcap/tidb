// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ResolveIndices implements Expression interface.
func (sf *ScalarFunction) ResolveIndices(schema *Schema) (Expression, error) {
	newSf := sf.Clone()
	err := newSf.resolveIndices(schema)
	return newSf, err
}

func (sf *ScalarFunction) resolveIndices(schema *Schema) error {
	for _, arg := range sf.GetArgs() {
		err := arg.resolveIndices(schema)
		if err != nil {
			return err
		}
	}
	return nil
}

// ResolveIndicesByVirtualExpr implements Expression interface.
func (sf *ScalarFunction) ResolveIndicesByVirtualExpr(ctx EvalContext, schema *Schema) (Expression, bool) {
	newSf := sf.Clone()
	isOK := newSf.resolveIndicesByVirtualExpr(ctx, schema)
	return newSf, isOK
}

func (sf *ScalarFunction) resolveIndicesByVirtualExpr(ctx EvalContext, schema *Schema) bool {
	for _, arg := range sf.GetArgs() {
		isOk := arg.resolveIndicesByVirtualExpr(ctx, schema)
		if !isOk {
			return false
		}
	}
	return true
}

// RemapColumn remaps columns with provided mapping and returns new expression
func (sf *ScalarFunction) RemapColumn(m map[int64]*Column) (Expression, error) {
	newSf, ok := sf.Clone().(*ScalarFunction)
	if !ok {
		return nil, errors.New("failed to cast to scalar function")
	}
	for i, arg := range sf.GetArgs() {
		newArg, err := arg.RemapColumn(m)
		if err != nil {
			return nil, err
		}
		newSf.GetArgs()[i] = newArg
	}
	// clear hash code
	newSf.hashcode = nil
	return newSf, nil
}

// GetSingleColumn returns (Col, Desc) when the ScalarFunction is equivalent to (Col, Desc)
// when used as a sort key, otherwise returns (nil, false).
//
// Can only handle:
// - ast.Plus
// - ast.Minus
// - ast.UnaryMinus
func (sf *ScalarFunction) GetSingleColumn(reverse bool) (*Column, bool) {
	switch sf.FuncName.String() {
	case ast.Plus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *Column:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp, reverse
		case *ScalarFunction:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp.GetSingleColumn(reverse)
		case *Constant:
			switch rtp := args[1].(type) {
			case *Column:
				return rtp, reverse
			case *ScalarFunction:
				return rtp.GetSingleColumn(reverse)
			}
		}
		return nil, false
	case ast.Minus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *Column:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp, reverse
		case *ScalarFunction:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp.GetSingleColumn(reverse)
		case *Constant:
			switch rtp := args[1].(type) {
			case *Column:
				return rtp, !reverse
			case *ScalarFunction:
				return rtp.GetSingleColumn(!reverse)
			}
		}
		return nil, false
	case ast.UnaryMinus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *Column:
			return tp, !reverse
		case *ScalarFunction:
			return tp.GetSingleColumn(!reverse)
		}
		return nil, false
	}
	return nil, false
}

// Coercibility returns the coercibility value which is used to check collations.
func (sf *ScalarFunction) Coercibility() Coercibility {
	if !sf.Function.HasCoercibility() {
		sf.SetCoercibility(deriveCoercibilityForScalarFunc(sf))
	}
	return sf.Function.Coercibility()
}

// HasCoercibility ...
func (sf *ScalarFunction) HasCoercibility() bool {
	return sf.Function.HasCoercibility()
}

// SetCoercibility sets a specified coercibility for this expression.
func (sf *ScalarFunction) SetCoercibility(val Coercibility) {
	sf.Function.SetCoercibility(val)
}

// CharsetAndCollation gets charset and collation.
func (sf *ScalarFunction) CharsetAndCollation() (string, string) {
	return sf.Function.CharsetAndCollation()
}

// SetCharsetAndCollation sets charset and collation.
func (sf *ScalarFunction) SetCharsetAndCollation(chs, coll string) {
	sf.Function.SetCharsetAndCollation(chs, coll)
}

// Repertoire returns the repertoire value which is used to check collations.
func (sf *ScalarFunction) Repertoire() Repertoire {
	return sf.Function.Repertoire()
}

// SetRepertoire sets a specified repertoire for this expression.
func (sf *ScalarFunction) SetRepertoire(r Repertoire) {
	sf.Function.SetRepertoire(r)
}

// IsExplicitCharset return the charset is explicit set or not.
func (sf *ScalarFunction) IsExplicitCharset() bool {
	return sf.Function.IsExplicitCharset()
}

// SetExplicitCharset set the charset is explicit or not.
func (sf *ScalarFunction) SetExplicitCharset(explicit bool) {
	sf.Function.SetExplicitCharset(explicit)
}

const emptyScalarFunctionSize = int64(unsafe.Sizeof(ScalarFunction{}))

// MemoryUsage return the memory usage of ScalarFunction
func (sf *ScalarFunction) MemoryUsage() (sum int64) {
	if sf == nil {
		return
	}

	sum = emptyScalarFunctionSize + int64(len(sf.FuncName.L)+len(sf.FuncName.O)) + int64(cap(sf.hashcode))
	if sf.RetType != nil {
		sum += sf.RetType.MemoryUsage()
	}
	if sf.Function != nil {
		sum += sf.Function.MemoryUsage()
	}
	return sum
}
