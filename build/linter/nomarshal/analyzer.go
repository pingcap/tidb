// Copyright 2024 PingCAP, Inc.
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

package nomarshal

import (
	"go/ast"
	"go/types"

	"github.com/fatih/structtag"
	"github.com/pkg/errors"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the analyzer struct of nomarshal.
var Analyzer = &analysis.Analyzer{
	Name:     "nomarshal",
	Doc:      "nomarshal avoids some interface/struct types to be used in json.Marshal",
	Run:      run,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
}

func run(pass *analysis.Pass) (any, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	jsonPkg := ""
	for _, imp := range pass.Pkg.Imports() {
		if imp.Path() == "encoding/json" {
			jsonPkg = imp.Name()
			break
		}
	}
	if jsonPkg == "" {
		return nil, nil
	}

	nodeFilter := []ast.Node{
		(*ast.CallExpr)(nil),
	}
	inspect.Preorder(nodeFilter, func(n ast.Node) {
		expr, ok := n.(*ast.CallExpr)
		if !ok {
			return
		}

		if !isJSONMarshal(expr, jsonPkg) {
			return
		}

		for _, arg := range expr.Args {
			typ := pass.TypesInfo.Types[arg].Type

			err := isTypValidImpl(typ, nil)
			if err != nil {
				pass.Reportf(arg.Pos(), "Invalid type %s: %s", typ.String(), err.Error())
			}
		}
	})
	return nil, nil
}

func isJSONMarshal(expr *ast.CallExpr, jsonPkg string) bool {
	selector, ok := expr.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	x, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	return x.Name == jsonPkg && selector.Sel.Name == "Marshal"
}

func isTypValidImpl(typ types.Type, visitedTyp map[types.Type]struct{}) error {
	// TODO: add a cache to accelerate the check

	if visitedTyp == nil {
		visitedTyp = make(map[types.Type]struct{})
	}
	if _, ok := visitedTyp[typ]; ok {
		// recursive type is directly allowed
		return nil
	}
	visitedTyp[typ] = struct{}{}
	defer func() {
		delete(visitedTyp, typ)
	}()

	switch typ := typ.(type) {
	case *types.Named:
		// Check if the type implements json.Marshaler
		for i := 0; i < typ.NumMethods(); i++ {
			f := typ.Method(i)
			if f.Name() == "NoMarshalMark" {
				return errors.New("type has NoMarshalMark method")
			}
		}

		return isTypValidImpl(typ.Underlying(), visitedTyp)
	case *types.Struct:
		// Check if the struct has json tags
		for i := 0; i < typ.NumFields(); i++ {
			tag := typ.Tag(i)
			field := typ.Field(i)

			// skip all unexported and not embedded fields
			if !field.Exported() && !field.Embedded() {
				continue
			}

			// every not-embedded field should have json tag
			tags, err := structtag.Parse(tag)
			if err != nil {
				return errors.Wrapf(err, "failed to parse tag %s for field %s", tag, field.Name())
			}
			t, _ := tags.Get("json")

			// tag "-" means this field is ignored.
			if t == nil || t.Name != "-" {
				err := isTypValidImpl(field.Type(), visitedTyp)
				if err != nil {
					return errors.Wrapf(err, "field %s has invalid type for `json.Marshal`", field.Name())
				}
			}
		}
	case *types.Interface:
		// Check if the type implements json.Marshaler
		for i := 0; i < typ.NumMethods(); i++ {
			f := typ.Method(i)
			if f.Name() == "MarshalJSON" || f.Name() == "MarshalText" {
				// TODO: also check the signature, or use types.Implements
				return nil
			}
		}
		return errors.New("interface type does not implement json.Marshaler")
	case *types.Pointer:
		return isTypValidImpl(typ.Elem(), visitedTyp)
	case *types.Slice:
		return isTypValidImpl(typ.Elem(), visitedTyp)
	case *types.Array:
		return isTypValidImpl(typ.Elem(), visitedTyp)
	case *types.Tuple:
		for i := 0; i < typ.Len(); i++ {
			err := isTypValidImpl(typ.At(i).Type(), visitedTyp)
			if err != nil {
				return err
			}
		}
	case *types.Signature, *types.Chan:
		return errors.New("function and channel is not allowed")
	case *types.Map:
		err := isTypValidImpl(typ.Key(), visitedTyp)
		if err != nil {
			return errors.Wrap(err, "type of map key is invalid")
		}
		err = isTypValidImpl(typ.Elem(), visitedTyp)
		if err != nil {
			return errors.Wrap(err, "type of map value is invalid")
		}
	case *types.Basic:
		// do nothing
		return nil
	case *types.TypeParam:
		iface := typ.Constraint().Underlying()
		return errors.Wrap(isTypValidImpl(iface, visitedTyp), "type param constraint is invalid")
	default:
		return errors.Errorf("Unknown type: %s %t", typ.String(), typ)
	}

	return nil
}

// func init() {
// 	util.SkipAnalyzerByConfig(Analyzer)
// 	util.SkipAnalyzer(Analyzer)
// }
