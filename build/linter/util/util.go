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

package util

import (
	"fmt"
	"go/ast"
	"go/token"
	"reflect"
	"strings"

	"golang.org/x/tools/go/analysis"
	"honnef.co/go/tools/analysis/report"
)

// Directive is a comment of the form '//lint:<command>
// [arguments...]'. It represents instructions to the static analysis
// tool.
type Directive struct {
	Command   string
	Arguments []string
	Directive *ast.Comment
	Node      ast.Node
}

func parseDirective(s string) (cmd string, args []string) {
	if strings.HasPrefix(s, "//lint:") {
		if !strings.HasPrefix(s, "//lint:") {
			return "", nil
		}
		s = strings.TrimPrefix(s, "//lint:")
		fields := strings.Split(s, " ")
		return fields[0], fields[1:]
	}
	s = strings.TrimPrefix(s, "//nolint: ")
	return "", []string{s}
}

// ParseDirectives extracts all directives from a list of Go files.
func ParseDirectives(files []*ast.File, fset *token.FileSet) []Directive {
	var dirs []Directive
	for _, f := range files {
		// OPT(dh): in our old code, we skip all the comment map work if we
		// couldn't find any directives, benchmark if that's actually
		// worth doing
		cm := ast.NewCommentMap(fset, f, f.Comments)
		for node, cgs := range cm {
			for _, cg := range cgs {
				for _, c := range cg.List {
					if !strings.HasPrefix(c.Text, "//lint:") && !strings.HasPrefix(c.Text, "//nolint:") {
						continue
					}
					cmd, args := parseDirective(c.Text)
					d := Directive{
						Command:   cmd,
						Arguments: args,
						Directive: c,
						Node:      node,
					}
					dirs = append(dirs, d)
				}
			}
		}
	}
	return dirs
}

func doDirectives(pass *analysis.Pass) (interface{}, error) {
	return ParseDirectives(pass.Files, pass.Fset), nil
}

// Directives is a fact that contains a list of directives.
var Directives = &analysis.Analyzer{
	Name:             "directives",
	Doc:              "extracts linter directives",
	Run:              doDirectives,
	RunDespiteErrors: true,
	ResultType:       reflect.TypeOf([]Directive{}),
}

// MungeAnalyzer updates an analyzer from `staticcheck` to make it work on nogo.
// The staticcheck analyzers don't look at "lint:ignore" directives, so if you
// integrate them into `nogo` unchanged, you'll get spurious build failures for
// issues that are actually explicitly ignored. So for each staticcheck analyzer
// we add `facts.Directives` to the list of dependencies, then cross-check
// each reported diagnostic to make sure it's not ignored before allowing it
// through.
func MungeAnalyzer(analyzer *analysis.Analyzer) {
	// Add facts.directives to the list of dependencies for this analyzer.
	analyzer.Requires = analyzer.Requires[0:len(analyzer.Requires):len(analyzer.Requires)]
	analyzer.Requires = append(analyzer.Requires, Directives)
	oldRun := analyzer.Run
	analyzer.Run = func(p *analysis.Pass) (interface{}, error) {
		pass := *p
		oldReport := p.Report
		pass.Report = func(diag analysis.Diagnostic) {
			dirs := pass.ResultOf[Directives].([]Directive)
			for _, dir := range dirs {
				cmd := dir.Command
				args := dir.Arguments
				switch cmd {
				case "ignore", "":
					ignorePos := report.DisplayPosition(pass.Fset, dir.Node.Pos())
					nodePos := report.DisplayPosition(pass.Fset, diag.Pos)
					if ignorePos.Filename != nodePos.Filename || ignorePos.Line != nodePos.Line {
						continue
					}
					for _, check := range strings.Split(args[0], ",") {
						if check == analyzer.Name {
							// Skip reporting the diagnostic.
							return
						}
					}
				case "file-ignore":
					ignorePos := report.DisplayPosition(pass.Fset, dir.Node.Pos())
					nodePos := report.DisplayPosition(pass.Fset, diag.Pos)
					if ignorePos.Filename == nodePos.Filename {
						// Skip reporting the diagnostic.
						return
					}
				default:
					// Unknown directive, ignore
					continue
				}
			}
			oldReport(diag)
		}
		return oldRun(&pass)
	}
}

// FormatCode is to format code for nogo.
func FormatCode(code string) string {
	if strings.Contains(code, "`") {
		return code // TODO: properly escape or remove
	}

	return fmt.Sprintf("`%s`", code)
}
