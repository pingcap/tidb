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
	"gopkg.in/yaml.v2"
)

var (
	//go:embed .testmarker.yaml
	rulesYaml string
	config    Config

	addTestRegex    = regexp.MustCompile(`^\+func\s+(Test.*)\(t \*testing\.T\)`)
	deleteTestRegex = regexp.MustCompile(`^\-func\s+(Test.*)\(t \*testing\.T\)`)
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
	Description []any  `yaml:"description,omitempty"`
}

type issueMarkInfo struct {
	ID       int    `yaml:"id"`
	IssueURL string `yaml:"url"`
}

// Config describes the data structure of the mapping spec
type Config struct {
	OnlyFiles    []string `yaml:"only_files"`
	ExcludeFiles []string `yaml:"exclude_files"`

	onlyFiles    []*regexp.Regexp `yaml:"-"`
	excludeFiles []*regexp.Regexp `yaml:"-"`
}

func (c *Config) init(yamlStr string) {
	err := yaml.Unmarshal([]byte(yamlStr), c)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range c.OnlyFiles {
		c.onlyFiles = append(c.onlyFiles, regexp.MustCompile(f))
	}
	for _, f := range c.ExcludeFiles {
		c.excludeFiles = append(c.excludeFiles, regexp.MustCompile(f))
	}
}

func (c *Config) matchFile(filename string) bool {
	for _, ignore := range c.excludeFiles {
		if ignore.MatchString(filename) {
			return false
		}
	}
	for _, only := range c.onlyFiles {
		if only.MatchString(filename) {
			return true
		}
	}
	return false
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
	testMap := indexIncrementalTest()
	checkIncrementalTest(fms, testMap)
	showMarkInfo(fms)
}

func walkMarker(f *ast.File, filename string) []*MarkInfo {
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
			// if not import marker package, just return
			if localpkg == "" {
				return false
			}
			body := e.Body
			if body == nil {
				return false
			}
			markInfo := walkTestFuncDeclBody(body, filename, localpkg)
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

func walkTestFuncDeclBody(body *ast.BlockStmt, filename string, localpkg string) *MarkInfo {
	markInfo := &MarkInfo{File: filename}
	ast.Inspect(body, func(node ast.Node) bool {
		switch e := node.(type) {
		case *ast.ExprStmt:
			if _, ok := e.X.(*ast.CallExpr); !ok {
				return false
			}
			callExpr := e.X.(*ast.CallExpr)
			if isMarkCall(callExpr, localpkg) {
				walkerMarkAsCallExpr(callExpr, markInfo, localpkg)
			}
			return false
		}
		return true
	})
	if len(markInfo.Features) == 0 && len(markInfo.Issues) == 0 {
		return nil
	}
	return markInfo
}

func walkerMarkAsCallExpr(callExpr *ast.CallExpr, markInfo *MarkInfo, localpkg string) {
	if len(callExpr.Args) == 0 {
		return
	}
	marktype := parseMarkType(callExpr.Args[1], localpkg)
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

func checkIncrementalTest(fms []*MarkInfo, newTestMap map[string][]string) {
	allok := true
	for filename, tests := range newTestMap {
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
		log.Println(`Add a marker at the start of incremental tests, please.
   1) Marking the test is for a feature:
     marker.As(t, marker.Feature, "FD-$ID", "the description...")
   2) Or mark it for an issue:
     marker.As(t, marker.Issue, issue-id)`)
		os.Exit(1)
	}
}

func showMarkInfo(fms []*MarkInfo) {
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

func shouldCheckMarker(filePath string, config *Config) bool {
	filePath = string(filepath.Separator) + filepath.Clean(filePath)
	return config.matchFile(filePath)
}

// indexIncrementalTest collect incremental test cases from git diff from origin/master,
// consider that inventory cases may be moved to other files, we need also check the deleted files and lines.
func indexIncrementalTest() map[string][]string {
	newTestMap := make(map[string][]string)
	deletedTests := make(map[string]struct{})

	handleGitPatch := func(handler func(filename string, patch stream.Filter) error) {
		closeFilter := func(filter stream.Filter) {
			//nolint: errcheck
			stream.Run(filter)
		}
		filterOnDirCmd(func(filenames stream.Filter) error {
			err := stream.ForEach(stream.Sequence(filenames,
				stream.Grep(`.*_test.go`),
			), func(nameStatus string) {
				switch nameStatus[0] {
				case 'A', 'D', 'M':
					re := regexp.MustCompile(`^[^\s]+\s+([^\s]+)$`)
					filename := re.FindStringSubmatch(nameStatus)[1]
					filterOnDirCmd(func(patch stream.Filter) error {
						defer closeFilter(patch)
						return handler(filename, patch)
					}, ".", "git", "diff", "--color=never", "origin/master", "--", filename)
				case 'R':
					re := regexp.MustCompile(`^[^\s]+\s+([^\s]+)\s+([^\s]+)$`)
					for _, filename := range re.FindStringSubmatch(nameStatus)[1:] {
						filterOnDirCmd(func(patch stream.Filter) error {
							defer closeFilter(patch)
							return handler(filename, patch)
						}, ".", "git", "diff", "--color=never", "origin/master", "--", filename)
					}
				}
			})
			return err
		}, ".", "git", "diff", "--color=never", "--name-status", "origin/master")
	}

	handleGitPatch(func(_ string, patch stream.Filter) error {
		tests, err := findDeleteTest(patch)
		if err != nil {
			return err
		}
		for _, test := range tests {
			deletedTests[test] = struct{}{}
		}
		return nil
	})

	handleGitPatch(func(filename string, patch stream.Filter) error {
		if !shouldCheckMarker(filename, &config) {
			return nil
		}
		tests, err := findAddTest(patch)
		if err != nil {
			return err
		}
		if len(tests) == 0 {
			return nil
		}
		for _, test := range tests {
			// since a test case maybe deleted from a file and appears on other file, we don't regard them as incremental test cases.
			if _, exist := deletedTests[test]; !exist {
				newTestMap[filename] = append(newTestMap[filename], test)
			}
		}
		return nil
	})
	return newTestMap
}

func findDeleteTest(filter stream.Filter) (tests []string, err error) {
	err = stream.ForEach(filter, func(line string) {
		if !deleteTestRegex.MatchString(line) {
			return
		}
		testcase := deleteTestRegex.FindStringSubmatch(line)[1]
		tests = append(tests, testcase)
	})
	return
}

func findAddTest(filter stream.Filter) (tests []string, err error) {
	err = stream.ForEach(filter, func(line string) {
		if !addTestRegex.MatchString(line) {
			return
		}
		testcase := addTestRegex.FindStringSubmatch(line)[1]
		tests = append(tests, testcase)
	})
	return
}

func filterOnDirCmd(handler func(stream.Filter) error,
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
	config.init(rulesYaml)
}
