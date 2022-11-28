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
	"io/ioutil"
	"reflect"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/loader"
	"honnef.co/go/tools/analysis/report"
)

type skipType int

const (
	skipNone skipType = iota
	skipLinter
	skipFile
)

// Directive is a comment of the form '//lint:<command> [arguments...]' and `//nolint:<command>`.
// It represents instructions to the static analysis tool.
type Directive struct {
	Command   skipType
	Linters   []string
	Directive *ast.Comment
	Node      ast.Node
}

func parseDirective(s string) (cmd skipType, args []string) {
	if strings.HasPrefix(s, "//lint:") {
		s = strings.TrimPrefix(s, "//lint:")
		fields := strings.Split(s, " ")
		switch fields[0] {
		case "ignore":
			return skipLinter, fields[1:]
		case "file-ignore":
			return skipFile, fields[1:]
		}
		return skipNone, nil
	}
	s = strings.TrimPrefix(s, "//nolint:")
	return skipLinter, []string{s}
}

// ParseDirectives extracts all directives from a list of Go files.
func ParseDirectives(files []*ast.File, fset *token.FileSet) []Directive {
	var dirs []Directive
	for _, f := range files {
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
						Linters:   args,
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

// SkipAnalyzer updates an analyzer from `staticcheck` and `golangci-linter` to make it work on nogo.
// They have "lint:ignore" or "nolint" to make the analyzer ignore the code.
func SkipAnalyzer(analyzer *analysis.Analyzer) {
	analyzer.Requires = append(analyzer.Requires, Directives)
	oldRun := analyzer.Run
	analyzer.Run = func(p *analysis.Pass) (interface{}, error) {
		pass := *p
		oldReport := p.Report
		pass.Report = func(diag analysis.Diagnostic) {
			dirs := pass.ResultOf[Directives].([]Directive)
			for _, dir := range dirs {
				cmd := dir.Command
				linters := dir.Linters
				switch cmd {
				case skipLinter:
					ignorePos := report.DisplayPosition(pass.Fset, dir.Node.Pos())
					nodePos := report.DisplayPosition(pass.Fset, diag.Pos)
					if ignorePos.Filename != nodePos.Filename || ignorePos.Line != nodePos.Line {
						continue
					}
					for _, check := range strings.Split(linters[0], ",") {
						if strings.TrimSpace(check) == analyzer.Name {
							return
						}
					}
				case skipFile:
					ignorePos := report.DisplayPosition(pass.Fset, dir.Node.Pos())
					nodePos := report.DisplayPosition(pass.Fset, diag.Pos)
					if ignorePos.Filename == nodePos.Filename {
						return
					}
				default:
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

// MakeFakeLoaderPackageInfo creates a fake loader.PackageInfo for a given package.
func MakeFakeLoaderPackageInfo(pass *analysis.Pass) *loader.PackageInfo {
	var errs []error

	typeInfo := pass.TypesInfo

	return &loader.PackageInfo{
		Pkg:                   pass.Pkg,
		Importable:            true, // not used
		TransitivelyErrorFree: true, // not used

		// use compiled (preprocessed) go files AST;
		// AST linters use not preprocessed go files AST
		Files:  pass.Files,
		Errors: errs,
		Info:   *typeInfo,
	}
}

// ReadFile reads a file and adds it to the FileSet
// so that we can report errors against it using lineStart.
func ReadFile(fset *token.FileSet, filename string) ([]byte, *token.File, error) {
	//nolint: gosec
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}
	tf := fset.AddFile(filename, -1, len(content))
	tf.SetLinesForContent(content)
	return content, tf, nil
}

// FindOffset returns the offset of a given position in a file.
func FindOffset(fileText string, line, column int) int {
	// we count our current line and column position
	currentCol := 1
	currentLine := 1

	for offset, ch := range fileText {
		// see if we found where we wanted to go to
		if currentLine == line && currentCol == column {
			return offset
		}

		// line break - increment the line counter and reset the column
		if ch == '\n' {
			currentLine++
			currentCol = 1
		} else {
			currentCol++
		}
	}
	return -1 //not found
}
