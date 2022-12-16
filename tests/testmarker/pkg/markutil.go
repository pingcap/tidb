// Copyright 2022 PingCAP, Inc.
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

package types

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"log"
	"regexp"
	"strconv"
)

// MarkInfo describes the data structure of the feature mapping of a test case
type MarkInfo struct {
	Features []FeatureMarkInfo `yaml:"features,omitempty"`
	Issues   []IssueMarkInfo   `yaml:"issues,omitempty"`
	// File records the file where the test case is defined
	File string `yaml:"file"`
	// TestName is the name of the test method
	TestName string    `yaml:"test_name"`
	Pos      token.Pos `yaml:"-"`
}

// FeatureMarkInfo describes the data structure of the feature mapping of a test case
type FeatureMarkInfo struct {
	ID          string   `yaml:"id"`
	Description []string `yaml:"description,omitempty"`
}

// IssueMarkInfo describes the data structure of the issue mapping of a test case
type IssueMarkInfo struct {
	ID       int    `yaml:"id"`
	IssueURL string `yaml:"url"`
}

// WalkTestFile walks the test file and returns the MarkInfo
func WalkTestFile(fset *token.FileSet, f *ast.File, filename string) []*MarkInfo {
	var ret []*MarkInfo
	var localpkg string
	ast.Inspect(f, func(node ast.Node) bool {
		switch e := node.(type) {
		case *ast.ImportSpec:
			if e.Path.Value == `"github.com/pingcap/tidb/testkit/marker"` {
				if e.Name == nil {
					localpkg = "marker"
				} else {
					localpkg = e.Name.Name
				}
			}
		case *ast.FuncDecl:
			body := e.Body
			if body == nil {
				return false
			}
			testname, ok := tryGetTestName(e)
			if !ok {
				return false
			}
			markInfo := walkTestFuncDeclBody(fset, body, filename, testname, localpkg)
			if markInfo != nil {
				markInfo.Pos = e.Pos()
				ret = append(ret, markInfo)
			}
			return false
		}
		return true
	})
	return ret
}

func tryGetTestName(e *ast.FuncDecl) (string, bool) {
	re := regexp.MustCompile(`^Test`)
	if !re.MatchString(e.Name.Name) || len(e.Type.Params.List) != 1 {
		return "", false
	}
	param := e.Type.Params.List[0]
	if len(param.Names) != 1 || param.Names[0].Name != "t" {
		return "", false
	}
	switch t := param.Type.(type) {
	case *ast.StarExpr:
		switch x := t.X.(type) {
		case *ast.SelectorExpr:
			if x.X.(*ast.Ident).Name == "testing" && x.Sel.Name == "T" {
				return e.Name.Name, true
			}
		}
	}
	return "", false
}

func walkTestFuncDeclBody(fset *token.FileSet, body *ast.BlockStmt, filename, testname, localpkg string) *MarkInfo {
	markInfo := &MarkInfo{File: filename, TestName: testname}
	ast.Inspect(body, func(node ast.Node) bool {
		switch e := node.(type) {
		case *ast.ExprStmt:
			if _, ok := e.X.(*ast.CallExpr); !ok {
				return false
			}
			callExpr := e.X.(*ast.CallExpr)
			if isMarkCall(callExpr, localpkg) {
				walkerMarkAsCallExpr(fset, callExpr, markInfo, localpkg)
			}
			return false
		}
		return true
	})
	return markInfo
}

func walkerMarkAsCallExpr(fset *token.FileSet, callExpr *ast.CallExpr, markInfo *MarkInfo, localpkg string) {
	if len(callExpr.Args) == 0 {
		return
	}
	marktype := parseMarkType(callExpr.Args[1], localpkg)
	switch marktype {
	case "Feature":
		if len(callExpr.Args) < 3 {
			return
		}
		var feature FeatureMarkInfo
		callExpr.Pos()
		for i := 2; i < len(callExpr.Args); i++ {
			arg := mustPrintArg(fset, callExpr.Args[i])
			if i == 2 {
				feature.ID = arg
			} else {
				feature.Description = append(feature.Description, arg)
			}
		}
		appendFeatureMark(markInfo, feature)
	case "Issue":
		if len(callExpr.Args) < 3 {
			return
		}
		var issue IssueMarkInfo
		arg := mustPrintArg(fset, callExpr.Args[2])
		id, err := strconv.ParseInt(arg, 10, 64)
		if err != nil {
			log.Fatalf("invalid issue id %s", arg)
		}
		issue.ID = int(id)
		issue.IssueURL = fmt.Sprintf("https://github.com/pingcap/tidb/issues/%d", issue.ID)
		appendIssueMark(markInfo, issue)
	default:
		log.Fatalf("unknown mark type")
	}
}

func appendFeatureMark(markInfo *MarkInfo, feature FeatureMarkInfo) {
	for _, f := range markInfo.Features {
		if f.ID == feature.ID {
			return
		}
	}
	markInfo.Features = append(markInfo.Features, feature)
}
func appendIssueMark(markInfo *MarkInfo, issue IssueMarkInfo) {
	for _, i := range markInfo.Issues {
		if i.ID == issue.ID {
			return
		}
	}
	markInfo.Issues = append(markInfo.Issues, issue)
}

func mustPrintArg(fset *token.FileSet, arg ast.Expr) string {
	buf := bytes.NewBuffer([]byte{})
	err := printer.Fprint(buf, fset, arg)
	if err != nil {
		log.Fatal(err)
	}
	exprStr := buf.String()
	if len(exprStr) > 1 && exprStr[0] == '`' || exprStr[0] == '"' || exprStr[0] == '\'' {
		exprStr, err = strconv.Unquote(exprStr)
		if err != nil {
			log.Fatal(err)
		}
	}
	return exprStr
}

func isMarkCall(callExpr *ast.CallExpr, localpkg string) bool {
	var callee string
	if _, ok := callExpr.Fun.(*ast.Ident); ok && localpkg == "." {
		callee = callExpr.Fun.(*ast.Ident).Name
	}
	if _, ok := callExpr.Fun.(*ast.SelectorExpr); ok {
		if pkg, ok := callExpr.Fun.(*ast.SelectorExpr).X.(*ast.Ident); ok && localpkg == pkg.Name {
			callee = callExpr.Fun.(*ast.SelectorExpr).Sel.Name
		}
	}
	return callee == "As"
}

func parseMarkType(marktype ast.Expr, localpkg string) string {
	switch e := marktype.(type) {
	case *ast.SelectorExpr:
		if _, ok := e.X.(*ast.Ident); ok && localpkg == e.X.(*ast.Ident).Name {
			return e.Sel.Name
		}
	case *ast.Ident:
		if localpkg == "." {
			return e.Name
		}
	}
	return ""
}
