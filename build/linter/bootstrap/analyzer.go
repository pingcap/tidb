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

package bootstrap

import (
	"go/ast"
	"go/token"
	"maps"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

// Analyzer is the analyzer struct of unconvert.
var Analyzer = &analysis.Analyzer{
	Name:     "bootstrap",
	Doc:      "Check developers don't forget something in TiDB bootstrap logic",
	Requires: []*analysis.Analyzer{},
	Run:      run,
}

const (
	bootstrapCodeFile = "/bootstrap.go"
	upgradeCodeFile   = "/upgrade_def.go"
)

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		if strings.HasSuffix(pass.Fset.File(file.Pos()).Name(), bootstrapCodeFile) {
			checkBootstrapDotGo(pass, file)
		}
		if strings.HasSuffix(pass.Fset.File(file.Pos()).Name(), upgradeCodeFile) {
			checkUpgradeDotGo(pass, file)
		}
	}
	return nil, nil
}

// check whether the spec is a slice variable definition.
func isSliceVarDefNode(spec ast.Spec) (varName, eleTpName string, lit *ast.CompositeLit, ok bool) {
	valSpec, ok := spec.(*ast.ValueSpec)
	if !ok {
		return "", "", nil, false
	}
	if len(valSpec.Names) != 1 || len(valSpec.Values) != 1 {
		return "", "", nil, false
	}
	compLit, ok := valSpec.Values[0].(*ast.CompositeLit)
	if !ok {
		return "", "", nil, false
	}
	arrTp, ok := compLit.Type.(*ast.ArrayType)
	if !ok {
		return "", "", nil, false
	}
	varTpIdent, ok := arrTp.Elt.(*ast.Ident)
	if !ok {
		return "", "", nil, false
	}
	return valSpec.Names[0].Name, varTpIdent.Name, compLit, true
}

func checkSystemTablesDefinitionNode(pass *analysis.Pass, varName string, compLit *ast.CompositeLit) {
	for _, elt := range compLit.Elts {
		idIdentPkg := elt.(*ast.CompositeLit).Elts[0].(*ast.KeyValueExpr).Value.(*ast.SelectorExpr).X.(*ast.Ident).Name
		if idIdentPkg != "metadef" {
			pass.Reportf(elt.Pos(), "table ID must be defined in metadef pkg, but got %q", idIdentPkg)
		}
		idIdentName := elt.(*ast.CompositeLit).Elts[0].(*ast.KeyValueExpr).Value.(*ast.SelectorExpr).Sel.Name
		quotedName := elt.(*ast.CompositeLit).Elts[1].(*ast.KeyValueExpr).Value.(*ast.BasicLit).Value
		tableName, err := strconv.Unquote(quotedName)
		if err != nil {
			pass.Reportf(elt.Pos(), "the name of the table in %s must be a string literal, but got %q", varName, quotedName)
			continue
		}
		sqlPkgName := elt.(*ast.CompositeLit).Elts[2].(*ast.KeyValueExpr).Value.(*ast.SelectorExpr).X.(*ast.Ident).Name
		if sqlPkgName != "metadef" {
			pass.Reportf(elt.Pos(), "Create table SQL must be defined in metadef pkg, but got %q", sqlPkgName)
		}
		sqlIdentName := elt.(*ast.CompositeLit).Elts[2].(*ast.KeyValueExpr).Value.(*ast.SelectorExpr).Sel.Name
		if !strings.HasSuffix(idIdentName, "TableID") {
			pass.Reportf(elt.Pos(), "the name of the constant of table ID in %s must end with TableID, but got %q", varName, idIdentName)
		}
		if !strings.HasPrefix(sqlIdentName, "Create") || !strings.HasSuffix(sqlIdentName, "Table") {
			pass.Reportf(elt.Pos(), "the name of the constant of the create table SQL in %s must start 'CreateXXXTable' style, but got %q", varName, sqlIdentName)
		}
		if strings.TrimSuffix(idIdentName, "TableID") !=
			strings.TrimSuffix(strings.TrimPrefix(sqlIdentName, "Create"), "Table") {
			pass.Reportf(elt.Pos(), "the name of the constant of table ID in %s must match the name of the create table SQL, but got %q and %q", varName, idIdentName, sqlIdentName)
		}
		nameInCamel := strings.ReplaceAll(tableName, "_", "")
		if strings.ToLower(strings.TrimSuffix(idIdentName, "TableID")) != nameInCamel {
			pass.Reportf(elt.Pos(), "the name of the constant of table ID in %s must match the name of the table, but got %q and %q", varName, idIdentName, tableName)
		}
	}
}

func checkVersionedBootstrapSchema(pass *analysis.Pass, compLit *ast.CompositeLit, schemaDefVarNames map[string]struct{}) {
	nameUsedInVersionedBootstrapSchema := make(map[string]struct{})
	for _, elt := range compLit.Elts {
		// must be {ver: xxx, databases: xxx}
		databasesKVNode := elt.(*ast.CompositeLit).Elts[1].(*ast.KeyValueExpr)
		if databasesKVNode.Key.(*ast.Ident).Name != "databases" {
			pass.Reportf(databasesKVNode.Pos(), "the 2nd field of versionedBootstrapSchema must be 'databases'")
			continue
		}
		for _, dbDefNode := range databasesKVNode.Value.(*ast.CompositeLit).Elts {
			dbDefNodeLit := dbDefNode.(*ast.CompositeLit)
			// {ID:xx, Name: xx, Tables: xx...}
			if len(dbDefNodeLit.Elts) >= 3 {
				tablesVarNode, ok := dbDefNodeLit.Elts[2].(*ast.KeyValueExpr).Value.(*ast.Ident)
				if !ok {
					pass.Reportf(dbDefNodeLit.Elts[2].Pos(), "the Tables field of database definition must be a variable")
					continue
				}
				nameUsedInVersionedBootstrapSchema[tablesVarNode.Name] = struct{}{}
			}
		}
	}
	if !maps.Equal(schemaDefVarNames, nameUsedInVersionedBootstrapSchema) {
		pass.Reportf(compLit.Pos(), "the variables used in versionedBootstrapSchema do not match the defined schema variables, %v vs %v", schemaDefVarNames, nameUsedInVersionedBootstrapSchema)
	}
}

func checkBootstrapDotGo(pass *analysis.Pass, file *ast.File) {
	foundVarNames := make(map[string]struct{})
	schemaDefNodeCount := 0
	versionedBootstrapSchemaDefCount := 0
	for _, decl := range file.Decls {
		switch v := decl.(type) {
		case *ast.GenDecl:
			for _, spec := range v.Specs {
				varName, eleTpName, compLit, ok := isSliceVarDefNode(spec)
				if !ok {
					continue
				}
				if eleTpName == "TableBasicInfo" {
					schemaDefNodeCount++
					checkSystemTablesDefinitionNode(pass, varName, compLit)
					foundVarNames[varName] = struct{}{}
				} else if eleTpName == "versionedBootstrapSchema" {
					versionedBootstrapSchemaDefCount++
					checkVersionedBootstrapSchema(pass, compLit, foundVarNames)
				}
			}
		}
	}
	if schemaDefNodeCount < 1 {
		pass.Reportf(file.Pos(), "there must be at least one schema definition variable defined")
	}
	if versionedBootstrapSchemaDefCount != 1 {
		pass.Reportf(file.Pos(), "there must be exactly one versionedBootstrapSchema variable defined")
	}
}

func checkUpgradeDotGo(pass *analysis.Pass, file *ast.File) {
	var (
		maxVerVariable    int
		maxVerVariablePos token.Pos
		curVerVariable    int
		curVerVariablePos token.Pos
		maxVerFunc        int
		maxVerFuncPos     token.Pos
		maxVerFuncUsed    int
		maxVerFuncUsedPos token.Pos
		err               error
	)

	for _, decl := range file.Decls {
		switch v := decl.(type) {
		case *ast.GenDecl:
			switch {
			case len(v.Specs) == 1:
				spec := v.Specs[0]
				v2, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				if len(v2.Names) != 1 {
					continue
				}
				switch v2.Names[0].Name {
				case "upgradeToVerFunctions":
					composeLit := v2.Values[0].(*ast.CompositeLit)
					lastElm := composeLit.Elts[len(composeLit.Elts)-1]
					ident := lastElm.(*ast.CompositeLit).Elts[1].(*ast.KeyValueExpr).Value.(*ast.Ident)
					maxVerFuncUsed, err = strconv.Atoi(ident.Name[len("upgradeToVer"):])
					if err != nil {
						panic("unexpected value of upgradeToVerFunctions: " + ident.Name)
					}
					maxVerFuncUsedPos = lastElm.Pos()
				case "currentBootstrapVersion":
					valueIdent := v2.Values[0].(*ast.Ident)
					curVerVariablePos = valueIdent.Pos()
					value := v2.Values[0].(*ast.Ident).Name
					curVerVariable, err = strconv.Atoi(value[len("version"):])
					if err != nil {
						panic("unexpected value of currentBootstrapVersion: " + value)
					}
				default:
					continue
				}
			case v.Tok == token.CONST && len(v.Specs) > 1:
				for _, spec := range v.Specs {
					v2, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					if len(v2.Names) != 1 {
						continue
					}
					name := v2.Names[0].Name
					if !strings.HasPrefix(name, "version") {
						continue
					}

					valInName, err := strconv.Atoi(name[len("version"):])
					if err != nil {
						continue
					}

					if valInName < maxVerVariable {
						pass.Reportf(spec.Pos(), "version variable %q is not valid, we should have a increment list of version variables", name)
						continue
					}

					maxVerVariable = valInName
					maxVerVariablePos = v2.Names[0].Pos()

					if len(v2.Values) != 1 {
						pass.Reportf(spec.Pos(), "the value of version variable %q must be specified explicitly", name)
						continue
					}

					valStr := v2.Values[0].(*ast.BasicLit).Value
					val, err := strconv.Atoi(valStr)
					if err != nil {
						pass.Reportf(spec.Pos(), "unexpected value of version variable %q: %q", name, valStr)
						continue
					}

					if val != valInName {
						pass.Reportf(spec.Pos(), "the value of version variable %q must be '%d', but now is '%d'", name, valInName, val)
						continue
					}
				}
			}
		case *ast.FuncDecl:
			name := v.Name.Name
			if !strings.HasPrefix(name, "upgradeToVer") {
				continue
			}
			t, err := strconv.Atoi(name[len("upgradeToVer"):])
			if err != nil {
				continue
			}
			if t > maxVerFunc {
				maxVerFunc = t
				maxVerFuncPos = v.Pos()
			}
		}
	}
	minv := min(maxVerVariable, maxVerFunc, maxVerFuncUsed, curVerVariable)
	maxv := max(maxVerVariable, maxVerFunc, maxVerFuncUsed, curVerVariable)
	if minv == maxv && minv != 0 {
		return
	}
	pass.Reportf(maxVerFuncUsedPos, "found inconsistent bootstrap versions:")
	pass.Reportf(maxVerFuncUsedPos, "max version function used: %d", maxVerFuncUsed)
	pass.Reportf(maxVerFuncPos, "max version function: %d", maxVerFunc)
	pass.Reportf(maxVerVariablePos, "max version variable: %d", maxVerVariable)
	pass.Reportf(curVerVariablePos, "current version variable: %d", curVerVariable)
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
