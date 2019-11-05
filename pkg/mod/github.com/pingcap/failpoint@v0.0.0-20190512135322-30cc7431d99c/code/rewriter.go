// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package code

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
)

const (
	packagePath     = "github.com/pingcap/failpoint"
	packageName     = "failpoint"
	evalFunction    = "Eval"
	evalCtxFunction = "EvalContext"
	extendPkgName   = "_curpkg_"
	// It is an indicator to indicate the label is converted from `failpoint.Label("...")`
	// We use an illegal suffix to avoid conflict with the user's code
	// So `failpoint.Label("label1")` will be converted to `label1-tmp-marker:` in expression
	// rewrite and be converted to the legal form in label statement organization.
	labelSuffix = "-tmp-marker"
)

// Rewriter represents a rewriting tool for converting the failpoint marker functions to
// corresponding statements in Golang. It will traverse the specified path and filter
// out files which do not have failpoint injection sites, and rewrite the remain files.
type Rewriter struct {
	rewriteDir    string
	currentPath   string
	currentFile   *ast.File
	currsetFset   *token.FileSet
	failpointName string
	rewritten     bool

	output io.Writer
}

// NewRewriter returns a non-nil rewriter which is used to rewrite the specified path
func NewRewriter(path string) *Rewriter {
	return &Rewriter{
		rewriteDir: path,
	}
}

// SetOutput sets a writer and the rewrite results will write to the writer instead of generate a stash file
func (r *Rewriter) SetOutput(out io.Writer) {
	r.output = out
}

func (r *Rewriter) pos(pos token.Pos) string {
	p := r.currsetFset.Position(pos)
	return fmt.Sprintf("%s:%d", p.Filename, p.Line)
}

func (r *Rewriter) rewriteFuncLit(fn *ast.FuncLit) error {
	return r.rewriteStmts(fn.Body.List)
}

func (r *Rewriter) rewriteAssign(v *ast.AssignStmt) error {
	// fn1, fn2, fn3, ... := func(){...}, func(){...}, func(){...}, ...
	// x, fn := 100, func() {
	//     failpoint.Marker(fpname, func() {
	//         ...
	//     })
	// }
	// ch := <-func() chan interface{} {
	//     failpoint.Marker(fpname, func() {
	//         ...
	//     })
	// }
	for _, v := range v.Rhs {
		err := r.rewriteExpr(v)
		if err != nil {
			return err
		}
	}
	return nil
}

// rewriteInitStmt rewrites non-nil initialization statement
func (r *Rewriter) rewriteInitStmt(v ast.Stmt) error {
	var err error
	switch stmt := v.(type) {
	case *ast.ExprStmt:
		err = r.rewriteExpr(stmt.X)
	case *ast.AssignStmt:
		err = r.rewriteAssign(stmt)
	}
	return err
}

func (r *Rewriter) rewriteIfStmt(v *ast.IfStmt) error {
	// if a, b := func() {...}, func() int {...}(); cond {...}
	// if func() {...}(); cond {...}
	if v.Init != nil {
		err := r.rewriteInitStmt(v.Init)
		if err != nil {
			return err
		}
	}

	if err := r.rewriteExpr(v.Cond); err != nil {
		return err
	}

	err := r.rewriteStmts(v.Body.List)
	if err != nil {
		return err
	}
	if v.Else != nil {
		if elseIf, ok := v.Else.(*ast.IfStmt); ok {
			return r.rewriteIfStmt(elseIf)
		}
		if els, ok := v.Else.(*ast.BlockStmt); ok {
			return r.rewriteStmts(els.List)
		}
	}
	return nil
}

func (r *Rewriter) rewriteExpr(expr ast.Expr) error {
	if expr == nil {
		return nil
	}

	switch ex := expr.(type) {
	case *ast.BadExpr,
		*ast.Ident,
		*ast.Ellipsis,
		*ast.BasicLit,
		*ast.ArrayType,
		*ast.StructType,
		*ast.FuncType,
		*ast.InterfaceType,
		*ast.MapType,
		*ast.ChanType:
	// expressions that can not inject failpoint
	case *ast.SelectorExpr:
		return r.rewriteExpr(ex.X)

	case *ast.IndexExpr:
		// func()[]int {}()[func()int{}()]
		if err := r.rewriteExpr(ex.X); err != nil {
			return err
		}
		return r.rewriteExpr(ex.Index)

	case *ast.SliceExpr:
		// array[low:high:max]
		// => func()[]int {}()[func()int{}():func()int{}():func()int{}()]
		if err := r.rewriteExpr(ex.Low); err != nil {
			return err
		}
		if err := r.rewriteExpr(ex.High); err != nil {
			return err
		}
		if err := r.rewriteExpr(ex.Max); err != nil {
			return err
		}
		return r.rewriteExpr(ex.X)

	case *ast.FuncLit:
		// return func(){...},
		return r.rewriteFuncLit(ex)

	case *ast.CompositeLit:
		// []int{func() int {...}()}
		for _, elt := range ex.Elts {
			if err := r.rewriteExpr(elt); err != nil {
				return err
			}
		}

	case *ast.CallExpr:
		// return func() int {...}()
		if fn, ok := ex.Fun.(*ast.FuncLit); ok {
			err := r.rewriteFuncLit(fn)
			if err != nil {
				return err
			}
		}

		// return fn(func() int{...})
		for _, arg := range ex.Args {
			if fn, ok := arg.(*ast.FuncLit); ok {
				err := r.rewriteFuncLit(fn)
				if err != nil {
					return err
				}
			}
		}

	case *ast.StarExpr:
		// *func() *T{}()
		return r.rewriteExpr(ex.X)

	case *ast.UnaryExpr:
		// !func() {...}()
		return r.rewriteExpr(ex.X)

	case *ast.BinaryExpr:
		// a && func() bool {...} ()
		// func() bool {...} () && a
		// func() bool {...} () && func() bool {...} () && a
		// func() bool {...} () && a && func() bool {...} () && a
		err := r.rewriteExpr(ex.X)
		if err != nil {
			return err
		}
		return r.rewriteExpr(ex.Y)

	case *ast.ParenExpr:
		// (func() {...}())
		return r.rewriteExpr(ex.X)

	case *ast.TypeAssertExpr:
		// (func() {...}()).(type)
		return r.rewriteExpr(ex.X)

	case *ast.KeyValueExpr:
		// Key: (func() {...}())
		return r.rewriteExpr(ex.Value)

	default:
		fmt.Printf("unspport expression: %T\n", expr)
	}
	return nil
}

func (r *Rewriter) rewriteExprs(exprs []ast.Expr) error {
	for _, expr := range exprs {
		err := r.rewriteExpr(expr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Rewriter) rewriteStmts(stmts []ast.Stmt) error {
	for i, block := range stmts {
		switch v := block.(type) {
		case *ast.DeclStmt:
			// var fn1, fn2, fn3, ... = func(){...}, func(){...}, func(){...}, ...
			// var x, fn = 100, func() {
			//     failpoint.Marker(fpname, func() {
			//         ...
			//     })
			// }
			specs := v.Decl.(*ast.GenDecl).Specs
			for _, spec := range specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, v := range vs.Values {
					fn, ok := v.(*ast.FuncLit)
					if !ok {
						continue
					}
					err := r.rewriteStmts(fn.Body.List)
					if err != nil {
						return err
					}
				}
			}

		case *ast.ExprStmt:
			// failpoint.Marker("failpoint.name", func(context.Context, *failpoint.Arg)) {...}
			// failpoint.Break()
			// failpoint.Break("label")
			// failpoint.Continue()
			// failpoint.Fallthrough()
			// failpoint.Continue("label")
			// failpoint.Goto("label")
			// failpoint.Label("label")
			call, ok := v.X.(*ast.CallExpr)
			if !ok {
				break
			}
			selectorExpr, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				break
			}
			packageName, ok := selectorExpr.X.(*ast.Ident)
			if !ok || packageName.Name != r.failpointName {
				break
			}
			exprRewriter, found := exprRewriters[selectorExpr.Sel.Name]
			if !found {
				break
			}
			rewritten, stmt, err := exprRewriter(r, call)
			if err != nil {
				return err
			}
			if !rewritten {
				continue
			}

			if ifStmt, ok := stmt.(*ast.IfStmt); ok {
				err := r.rewriteIfStmt(ifStmt)
				if err != nil {
					return err
				}
			}

			stmts[i] = stmt
			r.rewritten = true

		case *ast.AssignStmt:
			// x := (func() {...} ())
			err := r.rewriteAssign(v)
			if err != nil {
				return err
			}

		case *ast.GoStmt:
			// go func() {...}()
			// go func(fn) {...}(func(){...})
			err := r.rewriteExpr(v.Call)
			if err != nil {
				return err
			}

		case *ast.DeferStmt:
			// defer func() {...}()
			// defer func(fn) {...}(func(){...})
			err := r.rewriteExpr(v.Call)
			if err != nil {
				return err
			}

		case *ast.ReturnStmt:
			// return func() {...}()
			// return func(fn) {...}(func(){...})
			err := r.rewriteExprs(v.Results)
			if err != nil {
				return err
			}

		case *ast.BlockStmt:
			// {
			//     func() {...}()
			// }
			err := r.rewriteStmts(v.List)
			if err != nil {
				return err
			}

		case *ast.IfStmt:
			// if func() {...}() {...}
			err := r.rewriteIfStmt(v)
			if err != nil {
				return err
			}

		case *ast.CaseClause:
			// case func() int {...}() > 100 && func () bool {...}()
			if len(v.List) > 0 {
				err := r.rewriteExprs(v.List)
				if err != nil {
					return err
				}
			}
			// case func() int {...}() > 100 && func () bool {...}():
			//     fn := func(){...}
			//     fn()
			if len(v.Body) > 0 {
				err := r.rewriteStmts(v.Body)
				if err != nil {
					return err
				}
			}

		case *ast.SwitchStmt:
			// switch x := func() {...}(); {...}
			if v.Init != nil {
				err := r.rewriteAssign(v.Init.(*ast.AssignStmt))
				if err != nil {
					return err
				}
			}

			// switch (func() {...}()) {...}
			if err := r.rewriteExpr(v.Tag); err != nil {
				return err
			}

			// switch x {
			// case 1:
			// 	func() {...}()
			// }
			err := r.rewriteStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.CommClause:
			// select {
			// case ch := <-func() chan bool {...}():
			// case <- fromCh:
			// case toCh <- x:
			// case <- func() chan bool {...}():
			// default:
			// }
			if v.Comm != nil {
				if assign, ok := v.Comm.(*ast.AssignStmt); ok {
					err := r.rewriteAssign(assign)
					if err != nil {
						return err
					}
				}
				if expr, ok := v.Comm.(*ast.ExprStmt); ok {
					err := r.rewriteExpr(expr.X)
					if err != nil {
						return err
					}
				}
			}
			err := r.rewriteStmts(v.Body)
			if err != nil {
				return err
			}

		case *ast.SelectStmt:
			if len(v.Body.List) < 1 {
				continue
			}
			err := r.rewriteStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.ForStmt:
			// for i := func() int {...}(); i < func() int {...}(); i += func() int {...}() {...}
			// for iter.Begin(); !iter.End(); iter.Next() {...}
			if v.Init != nil {
				err := r.rewriteInitStmt(v.Init)
				if err != nil {
					return err
				}
			}
			if v.Cond != nil {
				err := r.rewriteExpr(v.Cond)
				if err != nil {
					return err
				}
			}
			if v.Post != nil {
				assign, ok := v.Post.(*ast.AssignStmt)
				if ok {
					err := r.rewriteAssign(assign)
					if err != nil {
						return err
					}
				}
			}
			err := r.rewriteStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.RangeStmt:
			// for i := range func() {...}() {...}
			if err := r.rewriteExpr(v.X); err != nil {
				return err
			}
			err := r.rewriteStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.TypeSwitchStmt:
			if v.Assign != nil {
				// 	switch x := (func () {...}()).(type) {...}
				if assign, ok := v.Assign.(*ast.AssignStmt); ok {
					err := r.rewriteAssign(assign)
					if err != nil {
						return err
					}
				}
				// 	switch (func () {...}()).(type) {...}
				if expr, ok := v.Assign.(*ast.ExprStmt); ok {
					err := r.rewriteExpr(expr.X)
					if err != nil {
						return err
					}
				}
			}
			err := r.rewriteStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.SendStmt:
			// 	ch <- func () {...}()
			err := r.rewriteExprs([]ast.Expr{v.Chan, v.Value})
			if err != nil {
				return err
			}

		case *ast.LabeledStmt:
			// Label:
			//     func () {...}()
			stmts := []ast.Stmt{v.Stmt}
			err := r.rewriteStmts(stmts)
			if err != nil {
				return err
			}
			v.Stmt = stmts[0]

		case *ast.IncDecStmt:
			// func() *FooType {...}().Field++
			// func() *FooType {...}().Field--
			err := r.rewriteExpr(v.X)
			if err != nil {
				return err
			}

		case *ast.BranchStmt:
			// ignore keyword token (BREAK, CONTINUE, GOTO, FALLTHROUGH)

		default:
			fmt.Printf("unsupported statement: %T in %s\n", v, r.pos(v.Pos()))
		}
	}

	// Label statement must ahead of for loop
	for i := 0; i < len(stmts); i++ {
		stmt := stmts[i]
		if label, ok := stmt.(*ast.LabeledStmt); ok && strings.HasSuffix(label.Label.Name, labelSuffix) {
			label.Label.Name = label.Label.Name[:len(label.Label.Name)-len(labelSuffix)]
			label.Stmt = stmts[i+1]
			stmts[i+1] = &ast.EmptyStmt{}
		}
	}
	return nil
}

func (r *Rewriter) rewriteFuncDecl(fn *ast.FuncDecl) error {
	return r.rewriteStmts(fn.Body.List)
}

// RewriteFile rewrites a single file
func (r *Rewriter) RewriteFile(path string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%s %v\n%s", r.currentPath, e, debug.Stack())
		}
	}()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return err
	}
	if len(file.Decls) < 1 {
		return nil
	}
	r.currentPath = path
	r.currentFile = file
	r.currsetFset = fset
	r.rewritten = false

	var failpointImport *ast.ImportSpec
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, "`\"") == packagePath {
			failpointImport = imp
			break
		}
	}
	if failpointImport == nil {
		panic("import path should be check before rewrite")
	}
	if failpointImport.Name != nil {
		r.failpointName = failpointImport.Name.Name
	} else {
		r.failpointName = packageName
	}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if err := r.rewriteFuncDecl(fn); err != nil {
			return err
		}
	}

	if !r.rewritten {
		return nil
	}

	if r.output != nil {
		return printer.Fprint(r.output, fset, file)
	}

	// Generate binding code
	found, err := isBindingFileExists(path)
	if err != nil {
		return err
	}
	if !found {
		err := writeBindingFile(path, file.Name.Name)
		if err != nil {
			return err
		}
	}

	// Backup origin file and replace content
	targetPath := path + failpointStashFileSuffix
	if err := os.Rename(path, targetPath); err != nil {
		return err
	}

	newFile, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer newFile.Close()
	return printer.Fprint(newFile, fset, file)
}

// Rewrite does the rewrite action for specified path. It contains the main steps:
//
// 1. Filter out failpoint binding files and files that have no suffix `.go`
// 2. Filter out files which have not imported failpoint package (implying no failpoints)
// 3. Parse file to `ast.File` and rewrite the AST
// 4. Create failpoint binding file (which contains `_curpkg_` function) if it does not exist
// 5. Rename original file to `original-file-name + __failpoint_stash__`
// 6. Replace original file content base on the new AST
func (r *Rewriter) Rewrite() error {
	var files []string
	err := filepath.Walk(r.rewriteDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, failpointBindingFileName) {
			return nil
		}
		// Will rewrite a file only if the file has imported "github.com/pingcap/failpoint"
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		if len(file.Imports) < 1 {
			return nil
		}
		for _, imp := range file.Imports {
			// import path maybe in the form of:
			//
			// 1. normal import
			//    - "github.com/pingcap/failpoint"
			//    - `github.com/pingcap/failpoint`
			// 2. ignore import
			//    - _ "github.com/pingcap/failpoint"
			//    - _ `github.com/pingcap/failpoint`
			// 3. alias import
			//    - alias "github.com/pingcap/failpoint"
			//    - alias `github.com/pingcap/failpoint`
			// we should trim '"' or '`' before compare it.
			if strings.Trim(imp.Path.Value, "`\"") == packagePath {
				files = append(files, path)
				break
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, file := range files {
		err := r.RewriteFile(file)
		if err != nil {
			return err
		}
	}
	return nil
}
