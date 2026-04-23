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

package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/bazelbuild/buildtools/build"
)

const (
	goRulesModule   = "@io_bazel_rules_go//go:def.bzl"
	goLibrarySymbol = "go_library"
	goTestSymbol    = "go_test"
	megaBuildPath   = "pkg/testkit/mega/BUILD.bazel"
	repoImportPath  = "github.com/pingcap/tidb/"
)

type megaConfig struct {
	deps  []string
	pkgs  map[string]megaPackage
	files []string
}

type megaPackage struct {
	importPath string
	label      string
}

func loadMegaConfig() (*megaConfig, error) {
	files := []string{
		"pkg/testkit/mega/main_test.go",
		"pkg/testkit/mega/mega_imports_test.go",
		"pkg/testkit/mega/mega_main_test.go",
	}
	deps, err := collectRepoLabels(files)
	if err != nil {
		return nil, err
	}

	cfg := &megaConfig{
		deps:  deps,
		pkgs:  make(map[string]megaPackage, len(deps)),
		files: files,
	}
	for _, dep := range deps {
		relDir := strings.TrimPrefix(dep, "//")
		cfg.pkgs[relDir] = megaPackage{
			importPath: repoImportPath + relDir,
			label:      dep,
		}
	}
	return cfg, nil
}

func collectRepoLabels(paths []string) ([]string, error) {
	labels := make(map[string]struct{})
	for _, path := range paths {
		imports, err := goFileImports(path)
		if err != nil {
			return nil, err
		}
		for _, importPath := range imports {
			label, ok := importPathToLabel(importPath)
			if !ok {
				continue
			}
			labels[label] = struct{}{}
		}
	}

	result := make([]string, 0, len(labels))
	for label := range labels {
		result = append(result, label)
	}
	sort.Strings(result)
	return result, nil
}

func goFileImports(path string) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		return nil, err
	}
	imports := make([]string, 0, len(file.Imports))
	for _, imp := range file.Imports {
		value, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return nil, err
		}
		imports = append(imports, value)
	}
	return imports, nil
}

func importPathToLabel(importPath string) (string, bool) {
	if !strings.HasPrefix(importPath, repoImportPath) {
		return "", false
	}
	return "//" + strings.TrimPrefix(importPath, repoImportPath), true
}

func maybeFixMegaBuild(path string, buildfile *build.File, cfg *megaConfig) error {
	if path != megaBuildPath || cfg == nil {
		return nil
	}
	rule := buildfile.RuleNamed("mega_test")
	if rule == nil {
		return nil
	}
	rule.SetAttr("deps", stringListExpr(cfg.deps))
	return nil
}

func validateMegaBuild(cfg *megaConfig) error {
	if cfg == nil {
		return nil
	}
	return validateMegaBuildFile(megaBuildPath, cfg)
}

func validateMegaBuildFile(path string, cfg *megaConfig) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	buildfile, err := build.ParseBuild(path, data)
	if err != nil {
		return err
	}
	rule := buildfile.RuleNamed("mega_test")
	if rule == nil {
		return fmt.Errorf("mega_test rule not found in %s", path)
	}
	got := rule.AttrStrings("deps")
	if !slices.Equal(got, cfg.deps) {
		return fmt.Errorf("mega_test deps mismatch in %s: got %v want %v", path, got, cfg.deps)
	}
	return nil
}

func maybeFixMegaImportedPackage(path string, buildfile *build.File, cfg *megaConfig) error {
	if cfg == nil {
		return nil
	}
	relDir := filepath.ToSlash(filepath.Dir(path))
	pkg, ok := cfg.pkgs[relDir]
	if !ok {
		return nil
	}
	return fixMegaImportedBuild(buildfile, relDir, pkg.importPath)
}

func fixMegaImportedBuild(buildfile *build.File, relDir, importPath string) error {
	generated, err := generatedMegaSources(relDir)
	if err != nil {
		return err
	}
	if len(generated) == 0 {
		return nil
	}

	goTest := firstGoTest(buildfile)
	library := goLibraryForImportPath(buildfile, importPath)

	if library == nil {
		if goTest == nil {
			return nil
		}
		ensureGoLoadSymbol(buildfile, goLibrarySymbol)
		library = newGoLibraryRule(filepath.Base(relDir), importPath)
		setGoLibrarySources(library, allNonTestGoFiles(relDir), hasFailpointInjection(relDir))
		mergeRuleDeps(library, nil, goTest.AttrStrings("deps"), generated)
		ensurePublicVisibility(library)
		insertRuleBeforeKind(buildfile, library, goTestSymbol)
		return nil
	}

	ensureGeneratedSources(library, generated, hasFailpointInjection(relDir))
	if goTest != nil {
		mergeRuleDeps(library, library.AttrStrings("deps"), goTest.AttrStrings("deps"), generated)
	} else {
		mergeRuleDeps(library, library.AttrStrings("deps"), nil, generated)
	}
	ensurePublicVisibility(library)
	return nil
}

func maybeFixFailpointLibraries(path string, buildfile *build.File) error {
	relDir := filepath.ToSlash(filepath.Dir(path))
	if !hasFailpointInjection(relDir) {
		return nil
	}
	for _, rule := range buildfile.Rules(goLibrarySymbol) {
		ensureFailpointBinding(rule)
	}
	return nil
}

func hasFailpointInjection(dir string) bool {
	files := allNonTestGoFiles(dir)
	for _, file := range files {
		content, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			continue
		}
		text := string(content)
		if strings.Contains(text, "failpoint.Inject(") ||
			strings.Contains(text, "failpoint.InjectContext(") ||
			strings.Contains(text, "failpoint.InjectCall(") {
			return true
		}
	}
	return false
}

func generatedMegaSources(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "register_gen.go" || name == "main_mega_init.go" || strings.HasSuffix(name, "_intest.go") {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	return files, nil
}

func allNonTestGoFiles(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "binding__failpoint_binding__.go" {
			continue
		}
		files = append(files, name)
	}
	sort.Strings(files)
	return files
}

func firstGoTest(buildfile *build.File) *build.Rule {
	rules := buildfile.Rules(goTestSymbol)
	if len(rules) == 0 {
		return nil
	}
	return rules[0]
}

func goLibraryForImportPath(buildfile *build.File, importPath string) *build.Rule {
	for _, rule := range buildfile.Rules(goLibrarySymbol) {
		if rule.AttrString("importpath") == importPath {
			return rule
		}
	}
	return nil
}

func ensureGeneratedSources(rule *build.Rule, generated []string, withBinding bool) {
	srcs := rule.Attr("srcs")
	values, hasBinding, ok := explicitSourceValues(srcs)
	if !ok {
		return
	}
	merged := appendUnique(values, generated...)
	setSourceExpr(rule, merged, hasBinding || withBinding)
}

func ensureFailpointBinding(rule *build.Rule) {
	srcs := rule.Attr("srcs")
	values, hasBinding, ok := explicitSourceValues(srcs)
	if !ok || hasBinding {
		return
	}
	setSourceExpr(rule, values, true)
}

func setGoLibrarySources(rule *build.Rule, files []string, withBinding bool) {
	setSourceExpr(rule, files, withBinding)
}

func setSourceExpr(rule *build.Rule, files []string, withBinding bool) {
	list := stringListExpr(files)
	if withBinding {
		rule.SetAttr("srcs", &build.BinaryExpr{
			X:  list,
			Op: "+",
			Y:  bindingGlobExpr(),
		})
		return
	}
	rule.SetAttr("srcs", list)
}

func explicitSourceValues(expr build.Expr) ([]string, bool, bool) {
	switch x := expr.(type) {
	case *build.ListExpr:
		return exprStrings(x.List), false, true
	case *build.BinaryExpr:
		if x.Op != "+" || !isBindingGlobExpr(x.Y) {
			return nil, false, false
		}
		left, _, ok := explicitSourceValues(x.X)
		if !ok {
			return nil, false, false
		}
		return left, true, true
	default:
		return nil, false, false
	}
}

func isBindingGlobExpr(expr build.Expr) bool {
	call, ok := expr.(*build.CallExpr)
	if !ok {
		return false
	}
	ident, ok := call.X.(*build.Ident)
	if !ok || ident.Name != "glob" || len(call.List) != 1 {
		return false
	}
	list, ok := call.List[0].(*build.ListExpr)
	if !ok || len(list.List) != 1 {
		return false
	}
	str, ok := list.List[0].(*build.StringExpr)
	return ok && str.Value == "binding__failpoint_binding__.go"
}

func bindingGlobExpr() build.Expr {
	return &build.CallExpr{
		X: &build.Ident{Name: "glob"},
		List: []build.Expr{
			&build.ListExpr{
				List: []build.Expr{
					&build.StringExpr{Value: "binding__failpoint_binding__.go"},
				},
			},
		},
	}
}

func mergeRuleDeps(rule *build.Rule, base, extra []string, generated []string) {
	merged := appendUnique(base, extra...)
	if slicesContain(generated, "register_gen.go") {
		merged = appendUnique(merged, "//pkg/testkit/mega/register")
	}
	if len(merged) == 0 {
		return
	}
	rule.SetAttr("deps", stringListExpr(merged))
}

func newGoLibraryRule(name, importPath string) *build.Rule {
	rule := build.NewRule(&build.CallExpr{X: &build.Ident{Name: goLibrarySymbol}})
	rule.SetAttr("name", &build.StringExpr{Value: name})
	rule.SetAttr("importpath", &build.StringExpr{Value: importPath})
	return rule
}

func ensurePublicVisibility(rule *build.Rule) {
	rule.SetAttr("visibility", stringListExpr([]string{"//visibility:public"}))
}

func ensureGoLoadSymbol(buildfile *build.File, symbol string) {
	for _, stmt := range buildfile.Stmt {
		load, ok := stmt.(*build.LoadStmt)
		if !ok || load.Module.Value != goRulesModule {
			continue
		}
		existing := make(map[string]struct{}, len(load.To))
		for _, ident := range load.To {
			existing[ident.Name] = struct{}{}
		}
		if _, ok := existing[symbol]; ok {
			return
		}
		load.From = append(load.From, &build.Ident{Name: symbol})
		load.To = append(load.To, &build.Ident{Name: symbol})
		sortLoadSymbols(load)
		return
	}

	load := &build.LoadStmt{
		Module: &build.StringExpr{Value: goRulesModule},
		From:   []*build.Ident{{Name: symbol}},
		To:     []*build.Ident{{Name: symbol}},
	}
	buildfile.Stmt = append([]build.Expr{load}, buildfile.Stmt...)
}

func sortLoadSymbols(load *build.LoadStmt) {
	type pair struct {
		from string
		to   string
	}
	pairs := make([]pair, 0, len(load.From))
	for i := range load.From {
		pairs = append(pairs, pair{
			from: load.From[i].Name,
			to:   load.To[i].Name,
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].to == pairs[j].to {
			return pairs[i].from < pairs[j].from
		}
		return pairs[i].to < pairs[j].to
	})
	load.From = load.From[:0]
	load.To = load.To[:0]
	for _, pair := range pairs {
		load.From = append(load.From, &build.Ident{Name: pair.from})
		load.To = append(load.To, &build.Ident{Name: pair.to})
	}
}

func insertRuleBeforeKind(buildfile *build.File, rule *build.Rule, kind string) {
	for i, stmt := range buildfile.Stmt {
		call, ok := stmt.(*build.CallExpr)
		if !ok {
			continue
		}
		ident, ok := call.X.(*build.Ident)
		if !ok || ident.Name != kind {
			continue
		}
		buildfile.Stmt = append(buildfile.Stmt[:i], append([]build.Expr{rule.Call}, buildfile.Stmt[i:]...)...)
		return
	}
	buildfile.Stmt = append(buildfile.Stmt, rule.Call)
}

func stringListExpr(values []string) *build.ListExpr {
	list := &build.ListExpr{ForceMultiLine: true}
	for _, value := range values {
		list.List = append(list.List, &build.StringExpr{Value: value})
	}
	return list
}

func exprStrings(exprs []build.Expr) []string {
	values := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		str, ok := expr.(*build.StringExpr)
		if !ok {
			continue
		}
		values = append(values, str.Value)
	}
	return values
}

func appendUnique(base []string, extra ...string) []string {
	seen := make(map[string]struct{}, len(base)+len(extra))
	result := make([]string, 0, len(base)+len(extra))
	for _, value := range base {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	for _, value := range extra {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func slicesContain(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
