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

// Mega test generator - simplified version for processing test files in-place
//
// Scans packages for RunXXX functions in _test.go files with //go:build intest tag
// and generates TestXXX wrapper functions and register_gen.go

//go:build ignore
// +build ignore

package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

var (
	pkgPath = flag.String("pkg", "", "Package path to process (e.g., ./pkg/kv)")
	megaDir = flag.String("mega-dir", "pkg/mega", "Mega package directory")
	dryRun  = flag.Bool("dry-run", false, "Print what would be generated without writing files")
)

func main() {
	flag.Parse()

	if *pkgPath == "" {
		fmt.Fprintln(os.Stderr, "Error: -pkg is required")
		fmt.Fprintln(os.Stderr, "Usage: mega-gen-pkg -pkg ./pkg/kv")
		os.Exit(1)
	}

	if err := generate(*pkgPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

type runFunc struct {
	Name       string // e.g., "TestInterface" (without Run prefix)
	SourceFile string // The source file where this function is defined
}

func generate(pkgPath string) error {
	// Find all _test.go files with //go:build intest tag
	files, packageName, err := findIntestTestFiles(pkgPath)
	if err != nil {
		return fmt.Errorf("find intest test files: %w", err)
	}

	if len(files) == 0 {
		fmt.Printf("No _test.go files with //go:build intest found in %s\n", pkgPath)
		return nil
	}

	// Find all RunXXX functions
	runFuncs, err := findRunFunctions(files)
	if err != nil {
		return fmt.Errorf("find run functions: %w", err)
	}

	if len(runFuncs) == 0 {
		fmt.Printf("No RunXXX functions found in %s\n", pkgPath)
		return nil
	}

	fmt.Printf("Found %d RunXXX functions in package %s (%d files)\n", len(runFuncs), packageName, len(files))

	if *dryRun {
		for _, f := range runFuncs {
			fmt.Printf("  Would generate wrapper for: %s (from %s)\n", f.Name, f.SourceFile)
		}
		return nil
	}

	// Generate wrapper files
	if err := generateWrappers(pkgPath, files, runFuncs, packageName); err != nil {
		return fmt.Errorf("generate wrappers: %w", err)
	}

	// Generate register_gen.go
	if err := generateRegister(pkgPath, packageName, runFuncs); err != nil {
		return fmt.Errorf("generate register: %w", err)
	}

	// Generate mega test functions in pkg/mega
	if err := generateMegaTests(*megaDir, packageName, runFuncs); err != nil {
		return fmt.Errorf("generate mega tests: %w", err)
	}

	fmt.Printf("Generated wrappers, register, and mega tests for %s\n", packageName)
	return nil
}

// findIntestTestFiles finds all _test.go files that have //go:build intest tag
func findIntestTestFiles(pkgPath string) ([]string, string, error) {
	var files []string
	var packageName string
	fset := token.NewFileSet()

	err := filepath.Walk(pkgPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Only process _test.go files
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}

		// Skip generated files
		if strings.HasSuffix(path, "_gen_test.go") {
			return nil
		}

		// Check if file has //go:build intest tag
		if !hasIntestBuildTag(path) {
			return nil
		}

		// Parse to get package name
		node, err := parser.ParseFile(fset, path, nil, parser.PackageClauseOnly)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}

		if node.Name != nil && packageName == "" {
			packageName = node.Name.Name
		}

		files = append(files, path)
		return nil
	})

	return files, packageName, err
}

func hasIntestBuildTag(filePath string) bool {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return false
	}

	contentStr := string(content)
	return strings.Contains(contentStr, "//go:build intest") ||
		strings.Contains(contentStr, "// +build intest")
}

func findRunFunctions(files []string) ([]runFunc, error) {
	fset := token.NewFileSet()
	var runFuncs []runFunc

	for _, file := range files {
		funcs, err := parseFileForRunFuncs(fset, file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to parse %s: %v\n", file, err)
			continue
		}
		for i := range funcs {
			funcs[i].SourceFile = filepath.Base(file)
		}
		runFuncs = append(runFuncs, funcs...)
	}

	return runFuncs, nil
}

func parseFileForRunFuncs(fset *token.FileSet, file string) ([]runFunc, error) {
	node, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var funcs []runFunc

	ast.Inspect(node, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok {
			return true
		}

		// Check if function name starts with "Run"
		if !strings.HasPrefix(fn.Name.Name, "Run") {
			return true
		}

		// Check if function has exactly one parameter of type *testing.T
		if fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
			return true
		}

		// Check if the parameter is *testing.T
		param := fn.Type.Params.List[0]
		if !isTestingTPtr(param.Type) {
			return true
		}

		// Extract the TestXXX name (remove "Run" prefix)
		baseName := strings.TrimPrefix(fn.Name.Name, "Run")
		if baseName == "" {
			return true
		}

		funcs = append(funcs, runFunc{
			Name: baseName,
		})

		return true
	})

	return funcs, nil
}

func isTestingTPtr(expr ast.Expr) bool {
	// Check if expr is *testing.T
	starExpr, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}

	selExpr, ok := starExpr.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	// Check if package is "testing"
	ident, ok := selExpr.X.(*ast.Ident)
	if !ok || ident.Name != "testing" {
		return false
	}

	// Check if type is "T"
	return selExpr.Sel.Name == "T"
}

func generateWrappers(pkgPath string, files []string, runFuncs []runFunc, packageName string) error {
	// Map source file -> functions in that file
	fileFuncs := make(map[string][]runFunc)
	for _, fn := range runFuncs {
		sourceFile := strings.TrimSuffix(fn.SourceFile, "_test.go")
		fileFuncs[sourceFile] = append(fileFuncs[sourceFile], fn)
	}

	// Generate one wrapper file per source file
	for sourceFile, funcs := range fileFuncs {
		// Output file: source_file_gen_test.go
		wrapperFileName := sourceFile + "_gen_test.go"
		wrapperPath := filepath.Join(pkgPath, wrapperFileName)

		content := generateWrapperFileContent(funcs, packageName)

		if err := os.WriteFile(wrapperPath, []byte(content), 0644); err != nil {
			return fmt.Errorf("write %s: %w", wrapperPath, err)
		}

		fmt.Printf("  Generated: %s (%d wrappers)\n", wrapperFileName, len(funcs))
	}

	return nil
}

func generateWrapperFileContent(funcs []runFunc, packageName string) string {
	var sb strings.Builder

	sb.WriteString("// Code generated by mega-gen. DO NOT EDIT.\n")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("package %s\n", packageName))
	sb.WriteString("\n")
	sb.WriteString("import \"testing\"\n")
	sb.WriteString("\n")

	for _, fn := range funcs {
		sb.WriteString(fmt.Sprintf("func %s(t *testing.T) {\n", fn.Name))
		sb.WriteString(fmt.Sprintf("\t%s(t)\n", "Run"+fn.Name))
		sb.WriteString("}\n\n")
	}

	return sb.String()
}

func generateRegister(pkgPath, packageName string, runFuncs []runFunc) error {
	// For external tests (xxx_test), we only generate a bridge file in the test package
	// For internal tests, we generate register_test_gen.go directly
	isExternalTest := strings.HasSuffix(packageName, "_test")

	if isExternalTest {
		// Generate bridge file in xxx_test package that exports and registers the test functions
		bridgeContent := generateBridgeContent(packageName, runFuncs)
		bridgePath := filepath.Join(pkgPath, "register_bridge_test_gen.go")
		if err := os.WriteFile(bridgePath, []byte(bridgeContent), 0644); err != nil {
			return err
		}
		fmt.Printf("  Generated: register_bridge_test_gen.go (%d functions)\n", len(runFuncs))
	} else {
		// For internal tests, generate register directly
		content := generateRegisterContent(packageName, runFuncs)
		registerPath := filepath.Join(pkgPath, "register_test_gen.go")
		if err := os.WriteFile(registerPath, []byte(content), 0644); err != nil {
			return err
		}
		fmt.Printf("  Generated: register_test_gen.go (%d functions)\n", len(runFuncs))
	}

	return nil
}

func generateBridgeContent(packageName string, funcs []runFunc) string {
	var sb strings.Builder

	sb.WriteString("// Code generated by mega-gen. DO NOT EDIT.\n")
	sb.WriteString("\n")
	sb.WriteString("//go:build intest\n")
	sb.WriteString("// +build intest\n")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("package %s\n", packageName))
	sb.WriteString("\n")
	sb.WriteString("import (\n")
	sb.WriteString("\t\"testing\"\n")
	sb.WriteString("\t\"github.com/pingcap/tidb/pkg/mega/register\"\n")
	sb.WriteString(")\n")
	sb.WriteString("\n")
	sb.WriteString("// ExportedMegaTests exports all RunXXX functions for registration by main package\n")
	sb.WriteString("func ExportedMegaTests() map[string]func(*testing.T) {\n")
	sb.WriteString("\treturn map[string]func(*testing.T){\n")

	for _, fn := range funcs {
		sb.WriteString(fmt.Sprintf("\t\t\"%s\": Run%s,\n", fn.Name, fn.Name))
	}

	sb.WriteString("\t}\n")
	sb.WriteString("}\n")
	sb.WriteString("\n")
	sb.WriteString("func init() {\n")
	sb.WriteString("\t// Auto-register all tests\n")
	sb.WriteString("\tfor name, fn := range ExportedMegaTests() {\n")
	sb.WriteString(fmt.Sprintf("\t\tregister.Register(\"%s\", name, fn)\n", packageName))
	sb.WriteString("\t}\n")
	sb.WriteString("}\n")

	return sb.String()
}

func generateRegisterContent(packageName string, funcs []runFunc) string {
	var sb strings.Builder

	// For external tests (package xxx_test), generate in the main package (xxx)
	// to avoid package conflicts in the same directory
	mainPackageName := packageName
	registerPkgName := packageName
	if strings.HasSuffix(packageName, "_test") {
		mainPackageName = strings.TrimSuffix(packageName, "_test")
		registerPkgName = packageName // Keep _test suffix for registry key
	}

	sb.WriteString("// Code generated by mega-gen. DO NOT EDIT.\n")
	sb.WriteString("\n")
	sb.WriteString("//go:build intest\n")
	sb.WriteString("// +build intest\n")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("package %s\n", mainPackageName))
	sb.WriteString("\n")
	sb.WriteString("import (\n")
	sb.WriteString("\t\"github.com/pingcap/tidb/pkg/mega/register\"\n")
	sb.WriteString(")\n")
	sb.WriteString("\n")
	sb.WriteString("func init() {\n")

	for _, fn := range funcs {
		sb.WriteString(fmt.Sprintf("\tregister.Register(\"%s\", \"%s\", Run%s)\n", registerPkgName, fn.Name, fn.Name))
	}

	sb.WriteString("}\n")

	return sb.String()
}

func generateMegaTests(megaDir, packageName string, runFuncs []runFunc) error {
	if len(runFuncs) == 0 {
		return nil
	}

	content := generateMegaTestsContent(packageName, runFuncs)

	megaTestFileName := fmt.Sprintf("%s_generated_test.go", packageName)
	megaTestPath := filepath.Join(megaDir, megaTestFileName)

	if err := os.WriteFile(megaTestPath, []byte(content), 0644); err != nil {
		return err
	}

	fmt.Printf("  Generated: pkg/mega/%s\n", megaTestFileName)
	return nil
}

func generateMegaTestsContent(packageName string, funcs []runFunc) string {
	var sb strings.Builder

	sb.WriteString("// Code generated by mega-gen. DO NOT EDIT.\n")
	sb.WriteString("\n")
	sb.WriteString("package mega\n")
	sb.WriteString("\n")
	sb.WriteString("import (\n")
	sb.WriteString("\t\"testing\"\n")
	sb.WriteString(")\n")
	sb.WriteString("\n")

	for _, fn := range funcs {
		testName := fmt.Sprintf("Test_%s_%s", packageName, fn.Name)
		sb.WriteString(fmt.Sprintf("func %s(t *testing.T) {\n", testName))
		sb.WriteString(fmt.Sprintf("\trunMegaTest(t, \"%s\", \"%s\")\n", packageName, fn.Name))
		sb.WriteString("}\n\n")
	}

	return sb.String()
}
