// Copyright 2026 PingCAP, Inc.
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

package bootstrap

import (
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"

	"golang.org/x/tools/go/analysis"
)

func TestBootstrapTableNameConstants(t *testing.T) {
	const source = `package session
var systemTablesOfBaseNextGenVersion = []TableBasicInfo{
    {ID: metadef.UserTableID, Name: TABLE_NAME, SQL: metadef.CreateUserTable},
}
var versionedBootstrapSchemas = []versionedBootstrapSchema{
    {ver: base, databases: []DatabaseBasicInfo{{ID: id, Name: name, Tables: systemTablesOfBaseNextGenVersion}}},
}`
	const invalidName = "must be a string literal or a string constant defined in metadef"
	cases := []struct {
		name, expr, packagePath, want string
		value                         constant.Value
		variable, noTypeInfo, unused  bool
	}{
		{name: "literal", expr: `"user"`},
		{name: "mismatched literal", expr: `"db"`, want: "must match the name of the table"},
		{name: "string constant", expr: "metadef.UserTableName", value: constant.MakeString("user")},
		{name: "mismatched constant", expr: "metadef.UserTableName", value: constant.MakeString("db"), want: "must match the name of the table"},
		{name: "integer literal", expr: "123", want: invalidName},
		{name: "integer constant", expr: "metadef.UserTableName", value: constant.MakeInt64(123), want: invalidName},
		{name: "string variable", expr: "metadef.UserTableName", variable: true, want: invalidName},
		{name: "foreign qualifier", expr: "other.UserTableName", value: constant.MakeString("user"), want: invalidName},
		{name: "foreign package", expr: "metadef.UserTableName", packagePath: "example.com/metadef", value: constant.MakeString("user"), want: invalidName},
		{name: "local identifier", expr: "userTableName", want: invalidName},
		{name: "constant without type information", expr: "metadef.UserTableName", noTypeInfo: true, want: invalidName},
		{name: "unused definition", expr: `"user"`, unused: true, want: "do not match the defined schema variables"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Errorf("analyzer panicked: %v", recovered)
				}
			}()
			input := strings.Replace(source, "TABLE_NAME", tc.expr, 1)
			if tc.unused {
				input = strings.Replace(input, "Tables: systemTablesOfBaseNextGenVersion", "Tables: missingTables", 1)
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "/session/bootstrap.go", input, 0)
			if err != nil {
				t.Fatal(err)
			}
			var diagnostics []string
			pass := &analysis.Pass{
				Fset: fset, Files: []*ast.File{file},
				Report: func(d analysis.Diagnostic) { diagnostics = append(diagnostics, d.Message) },
			}
			if !tc.noTypeInfo {
				packagePath := tc.packagePath
				if packagePath == "" {
					packagePath = "github.com/pingcap/tidb/pkg/meta/metadef"
				}
				pkg := types.NewPackage(packagePath, "metadef")
				pass.TypesInfo = &types.Info{Uses: make(map[*ast.Ident]types.Object)}
				ast.Inspect(file, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if !ok || selector.Sel.Name != "UserTableName" {
						return true
					}
					if tc.variable {
						pass.TypesInfo.Uses[selector.Sel] = types.NewVar(selector.Pos(), pkg, selector.Sel.Name, types.Typ[types.String])
					} else if tc.value != nil {
						constantType := types.Typ[types.UntypedString]
						if tc.value.Kind() == constant.Int {
							constantType = types.Typ[types.UntypedInt]
						}
						pass.TypesInfo.Uses[selector.Sel] = types.NewConst(selector.Pos(), pkg, selector.Sel.Name, constantType, tc.value)
					}
					return true
				})
			}
			if _, err := run(pass); err != nil {
				t.Fatal(err)
			}
			got := strings.Join(diagnostics, "\n")
			if tc.want == "" && len(diagnostics) != 0 {
				t.Fatalf("unexpected diagnostics: %s", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("expected diagnostic containing %q, got %q", tc.want, got)
			}
		})
	}
}
