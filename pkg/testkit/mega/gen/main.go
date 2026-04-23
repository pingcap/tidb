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
	pkgName   = flag.String("pkg", "", "Logical package name for mega registry (e.g., 'ddl', 'executor')")
	outputDir = flag.String("output", "", "Output directory (e.g., 'pkg/ddl/test')")
	megaDir   = flag.String("mega-dir", "pkg/mega", "Mega package directory for generating test functions")
)

func main() {
	flag.Parse()

	if *pkgName == "" {
		fmt.Fprintln(os.Stderr, "Error: -pkg is required")
		os.Exit(1)
	}

	if *outputDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -output is required")
		os.Exit(1)
	}

	if err := generate(*pkgName, *outputDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

type runFunc struct {
	Name       string // e.g., "GetTimeZone"
	SourceFile string // The source file where this function is defined
}

type packageInfo struct {
	Name  string   // Package name (e.g., "ddltest")
	Files []string // List of .go files
}

func generate(pkgName, outputDir string) error {
	// 1. Scan the directory for .go files (excluding _test.go)
	files, err := findGoFiles(outputDir)
	if err != nil {
		return fmt.Errorf("find go files: %w", err)
	}

	// 2. Parse each file and find RunXxx functions, also extract package name
	runFuncs, packageName, err := findRunFunctions(files)
	if err != nil {
		return fmt.Errorf("find run functions: %w", err)
	}

	if len(runFuncs) == 0 {
		fmt.Printf("No RunXxx functions found in %s\n", outputDir)
		return nil
	}

	fmt.Printf("Found %d RunXxx functions in package %s\n", len(runFuncs), packageName)

	// 3. Generate _test.go files (one per source file)
	if err := generateTestFiles(outputDir, files, runFuncs, packageName); err != nil {
		return fmt.Errorf("generate test files: %w", err)
	}

	// 4. Generate register_gen.go
	if err := generateRegister(outputDir, pkgName, runFuncs, packageName); err != nil {
		return fmt.Errorf("generate register: %w", err)
	}

	// 5. Generate mega test functions
	if err := generateMegaTests(*megaDir, pkgName, runFuncs); err != nil {
		return fmt.Errorf("generate mega tests: %w", err)
	}

	fmt.Printf("Generated test files, register, and mega tests for %s\n", pkgName)
	return nil
}

func findGoFiles(dir string) ([]string, error) {
	var files []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// Only process .go files that are not _test.go
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

func findRunFunctions(files []string) (map[string][]runFunc, string, error) {
	fset := token.NewFileSet()
	result := make(map[string][]runFunc) // file -> list of functions
	var packageName string

	for _, file := range files {
		funcs, pkgName, err := parseFileForRunFuncs(fset, file)
		if err != nil {
			return nil, "", fmt.Errorf("parse %s: %w", file, err)
		}
		if packageName == "" && pkgName != "" {
			packageName = pkgName
		}
		if len(funcs) > 0 {
			for i := range funcs {
				funcs[i].SourceFile = file
			}
			result[file] = funcs
		}
	}

	return result, packageName, nil
}

func parseFileForRunFuncs(fset *token.FileSet, file string) ([]runFunc, string, error) {
	node, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
	if err != nil {
		return nil, "", err
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

		// Extract the base name (remove "Run" prefix)
		baseName := strings.TrimPrefix(fn.Name.Name, "Run")
		if baseName == "" {
			return true
		}

		funcs = append(funcs, runFunc{
			Name: baseName,
		})

		return true
	})

	return funcs, node.Name.Name, nil
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

func generateTestFiles(outputDir string, files []string, runFuncs map[string][]runFunc, packageName string) error {
	for file, funcs := range runFuncs {
		// Determine the output file name
		// If input is "db.go", output is "db_test.go"
		baseName := filepath.Base(file)
		testFileName := strings.TrimSuffix(baseName, ".go") + "_test.go"
		testFilePath := filepath.Join(filepath.Dir(file), testFileName)

		// Generate the test file content
		content := generateTestFileContent(funcs, packageName)

		// Write the file
		if err := os.WriteFile(testFilePath, []byte(content), 0644); err != nil {
			return err
		}

		fmt.Printf("Generated %s\n", testFilePath)
	}

	return nil
}

func generateTestFileContent(funcs []runFunc, packageName string) string {
	var sb strings.Builder

	sb.WriteString("// Code generated by mega-gen. DO NOT EDIT.\n")
	sb.WriteString("// Actual source is in the corresponding .go file.\n")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("package %s\n", packageName))
	sb.WriteString("\n")
	sb.WriteString("import \"testing\"\n")
	sb.WriteString("\n")

	for _, fn := range funcs {
		sb.WriteString(fmt.Sprintf("func Test%s(t *testing.T) {\n", fn.Name))
		sb.WriteString(fmt.Sprintf("\tRun%s(t)\n", fn.Name))
		sb.WriteString("}\n")
		sb.WriteString("\n")
	}

	return sb.String()
}

func generateRegister(outputDir, pkgName string, runFuncs map[string][]runFunc, packageName string) error {
	// Collect all functions from all files
	var allFuncs []runFunc
	for _, funcs := range runFuncs {
		allFuncs = append(allFuncs, funcs...)
	}

	// Generate the register file
	content := generateRegisterContent(pkgName, allFuncs, packageName)

	registerPath := filepath.Join(outputDir, "register_gen.go")
	if err := os.WriteFile(registerPath, []byte(content), 0644); err != nil {
		return err
	}

	fmt.Printf("Generated %s\n", registerPath)
	return nil
}

func generateRegisterContent(pkgName string, funcs []runFunc, packageName string) string {
	var sb strings.Builder

	sb.WriteString("// Code generated by mega-gen. DO NOT EDIT.\n")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("package %s\n", packageName))
	sb.WriteString("\n")
	sb.WriteString("import (\n")
	sb.WriteString("\t\"github.com/pingcap/tidb/pkg/testkit/mega/register\"\n")
	sb.WriteString(")\n")
	sb.WriteString("\n")
	sb.WriteString("func init() {\n")

	for _, fn := range funcs {
		sb.WriteString(fmt.Sprintf("\tregister.Register(\"%s\", \"%s\", Run%s)\n", pkgName, fn.Name, fn.Name))
	}

	sb.WriteString("}\n")

	return sb.String()
}

func generateMegaTests(megaDir, pkgName string, runFuncs map[string][]runFunc) error {
	// Collect all functions from all files
	var allFuncs []runFunc
	for _, funcs := range runFuncs {
		allFuncs = append(allFuncs, funcs...)
	}

	if len(allFuncs) == 0 {
		return nil
	}

	// Generate the mega test file
	content := generateMegaTestsContent(pkgName, allFuncs)

	// Use a consistent filename based on package name
	megaTestFileName := fmt.Sprintf("%s_generated_test.go", pkgName)
	megaTestPath := filepath.Join(megaDir, megaTestFileName)

	if err := os.WriteFile(megaTestPath, []byte(content), 0644); err != nil {
		return err
	}

	fmt.Printf("Generated %s\n", megaTestPath)
	return nil
}

func generateMegaTestsContent(pkgName string, funcs []runFunc) string {
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
		// Generate test function with name like Test_ddl_GetTimeZone
		// Sanitize pkgName for use in function names (replace / with _)
		sanitizedPkgName := strings.ReplaceAll(pkgName, "/", "_")
		testName := fmt.Sprintf("Test_%s_%s", sanitizedPkgName, fn.Name)
		sb.WriteString(fmt.Sprintf("func %s(t *testing.T) {\n", testName))
		sb.WriteString(fmt.Sprintf("\trunMegaTest(t, \"%s\", \"%s\")\n", pkgName, fn.Name))
		sb.WriteString("}\n")
		sb.WriteString("\n")
	}

	return sb.String()
}
