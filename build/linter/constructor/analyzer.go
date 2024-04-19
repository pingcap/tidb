// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package constructor

import (
	"go/ast"
	"go/types"
	"slices"
	"strings"

	"github.com/fatih/structtag"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the analyzer struct of constructor.
// constructor only allows constructing a struct manually in some specific functions, which is specified with tags for
// `constructor.Constructor` field.
//
// It can detect the following pattern and give error (if not in constructor functions):
//
// 1. Create struct directly, like `SomeStruct{}` or `&SomeStruct{}`.
// 2. Create struct with `new`: `new(SomeStruct)`
// 3. Struct literal in slice or other struct: `[]SomeStruct{{}}`
// 4. Define variables through `var`: `var a SomeStruct`.
// 5. Implicit zero value in other struct literal: `type OtherStruct struct{SomeStruct}; other := OtherStruct{}`
//
// TODO: verify whether this linter can work well with generics
var Analyzer = &analysis.Analyzer{
	Name:     "constructor",
	Doc:      "Check developers don't create structs manually without using constructors",
	Requires: []*analysis.Analyzer{},
	Run:      run,
}

func getConstructorList(t types.Type) []string {
	structTyp, ok := t.(*types.Struct)
	if !ok {
		var ptr *types.Pointer
		// It's also possible to construct a pointer directly, e.g. []*Struct{{}}
		if ptr, ok = t.(*types.Pointer); !ok {
			return nil
		}

		structTyp, ok = ptr.Elem().Underlying().(*types.Struct)
		if !ok {
			return nil
		}
	}
	var ctors []string
	for i := 0; i < structTyp.NumFields(); i++ {
		field := structTyp.Field(i)
		named, ok := field.Type().(*types.Named)
		if !ok {
			continue
		}
		if named.Obj().Name() == "Constructor" && named.Obj().Pkg().Path() == "github.com/pingcap/tidb/util/linter/constructor" {
			tags, err := structtag.Parse(structTyp.Tag(i))
			// skip invalid tags
			if err != nil {
				continue
			}
			ctorTag, err := tags.Get("ctor")
			if err != nil {
				continue
			}
			ctors = strings.Split(ctorTag.Value(), ",")
			continue
		}

		if fieldStruct, ok := named.Underlying().(*types.Struct); ok {
			ctors = append(ctors, getConstructorList(fieldStruct)...)
		}
	}
	return ctors
}

func assertInConstructor(pass *analysis.Pass, n ast.Node, stack []ast.Node, ctors []string) bool {
	// check whether this call is in `ctor`
	for i := len(stack) - 1; i >= 0; i-- {
		funcDecl, ok := stack[i].(*ast.FuncDecl)
		if !ok {
			continue
		}

		if !slices.Contains(ctors, funcDecl.Name.Name) {
			pass.Reportf(n.Pos(), "struct can only be constructed in constructors %s", strings.Join(ctors, ", "))
			return false
		}

		return true
	}
	return true
}

func handleCompositeLit(pass *analysis.Pass, n *ast.CompositeLit, push bool, stack []ast.Node) bool {
	if !push {
		return false
	}

	t := pass.TypesInfo.TypeOf(n).Underlying()
	if t == nil {
		return true
	}

	ctors := getConstructorList(t)
	if len(ctors) == 0 {
		return true
	}

	return assertInConstructor(pass, n, stack, ctors)
}

func handleCallExpr(pass *analysis.Pass, n *ast.CallExpr, push bool, stack []ast.Node) bool {
	fun, ok := n.Fun.(*ast.Ident)
	if !ok {
		return true
	}
	if fun.Name != "new" || len(n.Args) == 0 {
		return true
	}

	t := pass.TypesInfo.TypeOf(n).Underlying()
	ctors := getConstructorList(t)
	if len(ctors) == 0 {
		return true
	}

	return assertInConstructor(pass, n, stack, ctors)
}

func handleValueSpec(pass *analysis.Pass, n *ast.ValueSpec, _ bool, stack []ast.Node) bool {
	t := pass.TypesInfo.TypeOf(n.Type)
	if t == nil {
		return true
	}

	ctors := getConstructorList(t.Underlying())
	if len(ctors) == 0 {
		return true
	}

	return assertInConstructor(pass, n, stack, ctors)
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		i := inspector.New([]*ast.File{file})

		i.WithStack([]ast.Node{&ast.CompositeLit{}, &ast.CallExpr{}, &ast.ValueSpec{}}, func(n ast.Node, push bool, stack []ast.Node) bool {
			switch n := n.(type) {
			case *ast.CompositeLit:
				return handleCompositeLit(pass, n, push, stack)
			case *ast.CallExpr:
				return handleCallExpr(pass, n, push, stack)
			case *ast.ValueSpec:
				return handleValueSpec(pass, n, push, stack)
			}

			return true
		})
	}
	return nil, nil
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
