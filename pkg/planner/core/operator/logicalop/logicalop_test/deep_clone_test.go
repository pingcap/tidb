// Copyright 2026 PingCAP, Inc.
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

package logicalop

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalPlanDeepCloneMethodCoverage(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	srcDir := filepath.Clean(filepath.Join(filepath.Dir(file), ".."))

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, srcDir, func(info fs.FileInfo) bool {
		return strings.HasSuffix(info.Name(), ".go") && !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)

	pkg, ok := pkgs["logicalop"]
	require.True(t, ok)

	logicalPlanStructs := make(map[string]struct{})
	explicitDeepCloneMethods := make(map[string]struct{})

	for _, fileAST := range pkg.Files {
		for _, decl := range fileAST.Decls {
			switch x := decl.(type) {
			case *ast.GenDecl:
				if x.Tok != token.TYPE {
					continue
				}
				for _, spec := range x.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					structType, ok := typeSpec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					if hasLogicalPlanBase(structType) {
						logicalPlanStructs[typeSpec.Name.Name] = struct{}{}
					}
				}
			case *ast.FuncDecl:
				if x.Name.Name != "DeepClone" || x.Recv == nil || len(x.Recv.List) != 1 {
					continue
				}
				recvName := receiverTypeName(x.Recv.List[0].Type)
				if recvName != "" {
					explicitDeepCloneMethods[recvName] = struct{}{}
				}
			}
		}
	}

	require.NotEmpty(t, logicalPlanStructs)
	for typeName := range logicalPlanStructs {
		_, ok = explicitDeepCloneMethods[typeName]
		require.Truef(t, ok, "missing explicit DeepClone() method for %s", typeName)
	}
}

func hasLogicalPlanBase(st *ast.StructType) bool {
	for _, field := range st.Fields.List {
		// Embedded field.
		if len(field.Names) != 0 {
			continue
		}
		typeName := receiverTypeName(field.Type)
		if typeName == "BaseLogicalPlan" || typeName == "LogicalSchemaProducer" {
			return true
		}
	}
	return false
}

func receiverTypeName(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.StarExpr:
		return receiverTypeName(x.X)
	case *ast.SelectorExpr:
		return x.Sel.Name
	default:
		return ""
	}
}
