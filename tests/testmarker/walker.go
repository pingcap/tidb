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

package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/ghemawat/stream"
	"github.com/zabawaba99/go-gitignore"
	"gopkg.in/yaml.v2"
)

var (
	//go:embed rules.yaml
	rulesYaml string
	rules     []*RuleSpec

	// records all the test cases that should be marked
	newTestCases  map[string][]string = make(map[string][]string)
	testNameRegex                     = regexp.MustCompile(`^\+func\s+(Test.*)\(`)
)

// MarkInfo describes the data structure of the feature mapping of a test case
type MarkInfo struct {
	Features []featureMarkInfo `yaml:"features,omitempty"`
	Issues   []issueMarkInfo   `yaml:"issues,omitempty"`
	// TestName is the name of the test method
	TestName string `yaml:"testName,omitempty"`
	// File records the file where the test case is defined
	File string `yaml:"file"`
}

type featureMarkInfo struct {
	ID          string `yaml:"id"`
	Description []any  `yaml:"description"`
}

type issueMarkInfo struct {
	ID       int    `yaml:"id"`
	IssueURL string `yaml:"url"`
}

// RuleSpec describes the data structure of the mapping spec
type RuleSpec struct {
	Path    string   `yaml:"path"`
	Exclude []string `yaml:"exclude"`
}

func main() {
	fset := token.NewFileSet()
	var fms []*MarkInfo
	// run at the root of the tidb repo
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			d, err := parser.ParseDir(fset, path, nil, parser.ParseComments)
			if err != nil {
				log.Fatal(err)
			}
			for _, f := range d {
				for n, f := range f.Files {
					fms = append(fms, walkMarker(f, n)...)
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	indexNewTests()
	checkNewTestsMarked(fms)
	printMarkInfos(fms)
}

func walkMarker(f *ast.File, filename string) []*MarkInfo {
	var ret []*MarkInfo
	ast.Inspect(f, func(node ast.Node) bool {
		switch e := node.(type) {
		case *ast.FuncDecl:
			body := e.Body
			if body == nil {
				return false
			}
			markInfo := walkTestFuncDeclBody(body, filename)
			if markInfo != nil {
				markInfo.TestName = e.Name.Name
				ret = append(ret, markInfo)
			}
			return false
		}
		return true
	})
	return ret
}

func walkTestFuncDeclBody(body *ast.BlockStmt, filename string) *MarkInfo {
	markInfo := &MarkInfo{File: filename}
	ast.Inspect(body, func(node ast.Node) bool {
		switch e := node.(type) {
		case *ast.ExprStmt:
			if _, ok := e.X.(*ast.CallExpr); !ok {
				return false
			}
			callExpr := e.X.(*ast.CallExpr)
			if _, ok := callExpr.Fun.(*ast.SelectorExpr); !ok {
				return false
			}
			callee := callExpr.Fun.(*ast.SelectorExpr).Sel.Name
			if callee == "As" {
				walkerMarkAsCallExpr(callExpr, markInfo)
			}
		}
		return true
	})
	if len(markInfo.Features) == 0 && len(markInfo.Issues) == 0 {
		return nil
	}
	return markInfo
}

func walkerMarkAsCallExpr(callExpr *ast.CallExpr, markInfo *MarkInfo) {
	if len(callExpr.Args) == 0 {
		return
	}
	marktype := parseMarkType(callExpr.Args[1])
	switch marktype {
	case "Feature":
		if len(callExpr.Args) < 3 {
			return
		}
		var feature featureMarkInfo
		for i := 2; i < len(callExpr.Args); i++ {
			switch e := callExpr.Args[i].(type) {
			case *ast.BasicLit:
				lit := parseBasicLit(e)
				if i == 2 {
					feature.ID = lit
				} else {
					feature.Description = append(feature.Description, lit)
				}
			}
		}
		appendFeatureMark(markInfo, feature)
	case "Issue":
		if len(callExpr.Args) < 3 {
			return
		}
		var issue issueMarkInfo
		switch e := callExpr.Args[2].(type) {
		case *ast.BasicLit:
			lit := parseBasicLit(e)
			id, err := strconv.ParseInt(lit, 10, 64)
			if err != nil {
				log.Fatalf("invalid issue id %s", lit)
			}
			issue.ID = int(id)
			issue.IssueURL = fmt.Sprintf("https://github.com/pingcap/tidb/issues/%d", issue.ID)
			appendIssueMark(markInfo, issue)
		}
	default:
		log.Fatalf("unknown mark type")
	}
}

func appendFeatureMark(markInfo *MarkInfo, feature featureMarkInfo) {
	for _, f := range markInfo.Features {
		if f.ID == feature.ID {
			return
		}
	}
	markInfo.Features = append(markInfo.Features, feature)
}
func appendIssueMark(markInfo *MarkInfo, issue issueMarkInfo) {
	for _, i := range markInfo.Issues {
		if i.ID == issue.ID {
			return
		}
	}
	markInfo.Issues = append(markInfo.Issues, issue)
}

func parseBasicLit(lit *ast.BasicLit) string {
	switch lit.Kind {
	case token.STRING:
		return lit.Value[1 : len(lit.Value)-1]
	default:
		return lit.Value
	}
}

func parseMarkType(marktype ast.Expr) string {
	switch e := marktype.(type) {
	case *ast.SelectorExpr:
		return e.Sel.Name
	}
	return ""
}

func checkNewTestsMarked(fms []*MarkInfo) {
	allok := true
	for filename, tests := range newTestCases {
	loopTests:
		for _, test := range tests {
			for _, fm := range fms {
				if fm.File == filename && fm.TestName == test {
					continue loopTests
				}
			}
			log.Printf("test cases %s:%s is not marked\n", filename, test)
			allok = false
		}
	}
	if !allok {
		os.Exit(1)
	}
}

func printMarkInfos(fms []*MarkInfo) {
	sort.Slice(fms, func(i, j int) bool {
		if fms[i].File != fms[j].File {
			return fms[i].File < fms[j].File
		}
		return fms[i].TestName < fms[j].TestName
	})

	out, err := yaml.Marshal(fms)
	if err != nil {
		log.Fatalf("failed to marshal mark infos: %v", err)
	}
	fmt.Println(string(out))
}

func needCheckMarkInfo(filePath string, rules []*RuleSpec) bool {
	filePath = string(filepath.Separator) + filepath.Clean(filePath)
	matchPattern := func(pattern string, filePath string) bool {
		lastFilePath := ""
		for filePath != lastFilePath {
			if gitignore.Match(pattern, filePath) {
				return true
			}
			lastFilePath = filePath
			filePath = filepath.Dir(filePath)
		}
		return false
	}

	path := filePath
	lastFilePath := ""
	for path != lastFilePath {
		// Rules are matched backwards
		for i := len(rules) - 1; i >= 0; i-- {
			if gitignore.Match(rules[i].Path, path) {
				for _, exclude := range rules[i].Exclude {
					if matchPattern(exclude, filePath) {
						return false
					}
				}
				return true
			}
		}
		lastFilePath = path
		path = filepath.Dir(path)
	}
	return false
}

func indexNewTests() {
	filterOnDirCmd(func(filenames stream.Filter) error {
		err := stream.ForEach(stream.Sequence(filenames,
			stream.Grep(`.*_test.go`),
		), func(filename string) {
			if !needCheckMarkInfo(filename, rules) {
				return
			}
			filterOnDirCmd(func(diffs stream.Filter) error {
				tests, err := indexNewTest(diffs)
				if err != nil {
					return err
				}
				if len(tests) == 0 {
					return nil
				}
				if newTestCases[filename] == nil {
					newTestCases[filename] = make([]string, 0)
				}
				newTestCases[filename] = append(newTestCases[filename], tests...)
				return nil
			}, ".", "git", "diff", "origin/master", "--", filename)
		})
		return err
	}, ".", "git", "diff", "--name-only", "origin/master")
}

func indexNewTest(filter stream.Filter) (tests []string, err error) {
	err = stream.ForEach(filter, func(line string) {
		if !testNameRegex.MatchString(line) {
			return
		}
		testcase := testNameRegex.FindStringSubmatch(line)[1]
		tests = append(tests, testcase)
	})
	return
}

type filterHandler func(stream.Filter) error

func filterOnDirCmd(handler filterHandler,
	dir string, name string, args ...string) {
	cmd, stderr, filter, err := dirCmd(dir, name, args...)
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	if err := handler(filter); err != nil {
		log.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		if out := stderr.String(); len(out) > 0 {
			log.Fatalf("err=%s, stderr=%s", err, out)
		}
	}
}

func dirCmd(
	dir string, name string, args ...string) (*exec.Cmd, *bytes.Buffer, stream.Filter, error) {
	//nolint:gosec
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stderr := new(bytes.Buffer)
	cmd.Stderr = stderr
	return cmd, stderr, stream.ReadLines(stdout), nil
}

func init() {
	err := yaml.Unmarshal([]byte(rulesYaml), &rules)
	if err != nil {
		log.Fatalf("failed to unmarshal rules.yaml: %v", err)
	}
}
