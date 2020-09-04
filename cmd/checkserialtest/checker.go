package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

const packageName = "failpoint"
const packagePath = "github.com/pingcap/failpoint"

type checkMode uint

const (
	CollateTestSuiteInfo checkMode = iota
	CheckFailPoint
)

type FailpointChecker struct {
	dir                 string
	currentPath         string
	currentFile         *ast.File
	currsetFset         *token.FileSet
	isCurrentTestSerial bool
	failpointName       string
	currSuiteName       string
	currTestName        string
	rewritten           bool
	Mode                checkMode
	testSuiteMap        map[string]map[string]bool
	errList             []error
}

// NewRewriter returns a non-nil rewriter which is used to rewrite the specified path
func NewChecker(path string, mode checkMode) *FailpointChecker {
	return &FailpointChecker{
		dir:          path,
		testSuiteMap: make(map[string]map[string]bool),
		Mode:         mode,
		errList:      make([]error, 0),
	}
}

func (r *FailpointChecker) pos(pos token.Pos) string {
	p := r.currsetFset.Position(pos)
	return fmt.Sprintf("%s:%d", p.Filename, p.Line)
}

func (r *FailpointChecker) check(isTest bool) error {
	var files []string
	err := filepath.Walk(r.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if !isTest && strings.Contains(path, "checkserialtest/testdata") {
			return nil
		}
		// Will rewrite a file only if the file has imported "github.com/pingcap/failpoint"
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		if len(file.Imports) < 1 {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return err
	}

	for _, file := range files {
		err := r.CheckFile(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *FailpointChecker) CheckFile(path string) error {
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

	if r.testSuiteMap[r.currentFile.Name.Name] == nil {
		r.testSuiteMap[r.currentFile.Name.Name] = make(map[string]bool)
	}

	var failpointImport *ast.ImportSpec
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, "`\"") == packagePath {
			failpointImport = imp
			break
		}
	}
	if failpointImport != nil && failpointImport.Name != nil {
		r.failpointName = failpointImport.Name.Name
	} else {
		r.failpointName = packageName
	}

	for _, decl := range file.Decls {
		if gd, ok := decl.(*ast.GenDecl); ok {
			r.collateFromGenDecl(gd)
		}
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if err := r.CheckFuncDecl(fn); err != nil {
			return err
		}
	}
	return nil
}

func (r *FailpointChecker) collateFromGenDecl(gd *ast.GenDecl) {
	if r.Mode != CollateTestSuiteInfo {
		return
	}
	for _, sp := range gd.Specs {
		vs, ok := sp.(*ast.ValueSpec)
		if !ok {
			return
		}
		for _, v := range vs.Values {
			ce, ok := v.(*ast.CallExpr)
			if !ok || len(ce.Args) != 1 {
				continue
			}
			id, ok := ce.Fun.(*ast.Ident)
			if !ok {
				continue
			}
			arg0, ok := ce.Args[0].(*ast.UnaryExpr)
			if !ok {
				continue
			}
			cl, ok := arg0.X.(*ast.CompositeLit)
			if !ok {
				continue
			}
			ident, ok := cl.Type.(*ast.Ident)
			if !ok {
				continue
			}
			name := ident.Name
			if strings.EqualFold(id.Name, "Suite") {
				r.testSuiteMap[r.currentFile.Name.Name][name] = false
			} else if strings.EqualFold(id.Name, "SerialSuite") {
				r.testSuiteMap[r.currentFile.Name.Name][name] = true
			}
		}
	}
}

// CheckFuncDecl check the outermost function like
// func (s *TestSuiteA) TestXXX() {
// ...
// }
func (r *FailpointChecker) CheckFuncDecl(fn *ast.FuncDecl) error {
	if fn.Body == nil {
		return nil
	}
	if r.Mode == CheckFailPoint {
		if fn.Recv == nil {
			return nil
		}
		fl := fn.Recv.List
		if len(fl) != 1 {
			return nil
		}
		star, ok := fl[0].Type.(*ast.StarExpr)
		if !ok {
			return nil
		}
		ident, ok := star.X.(*ast.Ident)
		if !ok {
			return nil
		}
		packageMap, ok := r.testSuiteMap[r.currentFile.Name.Name]
		if !ok {
			return errors.New("didn't find the package")
		}
		isSerial, ok := packageMap[ident.Name]
		if ok {
			r.isCurrentTestSerial = isSerial
			r.currSuiteName = ident.Name
			r.currTestName = fn.Name.Name
			return r.CheckStmts(fn.Body.List)
		}
	}
	return nil
}

func (r *FailpointChecker) CheckStmts(stmts []ast.Stmt) error {
	for _, block := range stmts {
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
					err := r.CheckStmts(fn.Body.List)
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
			switch expr := call.Fun.(type) {
			case *ast.FuncLit:
				err := r.checkFuncLit(expr)
				if err != nil {
					return err
				}
			case *ast.SelectorExpr:
				if err := r.checkSelector(*expr); err != nil {
					return err
				}
				if len(call.Args) > 0 {
					return r.checkExpr(call.Args[0])
				} else {
					return nil
				}
			}

		case *ast.AssignStmt:
			// x := (func() {...} ())
			err := r.CheckAssign(v)
			if err != nil {
				return err
			}

		case *ast.GoStmt:
			// go func() {...}()
			// go func(fn) {...}(func(){...})
			err := r.checkExpr(v.Call)
			if err != nil {
				return err
			}

		case *ast.DeferStmt:
			// defer func() {...}()
			// defer func(fn) {...}(func(){...})
			err := r.checkExpr(v.Call)
			if err != nil {
				return err
			}

		case *ast.ReturnStmt:
			// return func() {...}()
			// return func(fn) {...}(func(){...})
			err := r.checkExprs(v.Results)
			if err != nil {
				return err
			}

		case *ast.BlockStmt:
			// {
			//     func() {...}()
			// }
			err := r.CheckStmts(v.List)
			if err != nil {
				return err
			}

		case *ast.IfStmt:
			// if func() {...}() {...}
			err := r.checkIfStmt(v)
			if err != nil {
				return err
			}

		case *ast.CaseClause:
			// case func() int {...}() > 100 && func () bool {...}()
			if len(v.List) > 0 {
				err := r.checkExprs(v.List)
				if err != nil {
					return err
				}
			}
			// case func() int {...}() > 100 && func () bool {...}():
			//     fn := func(){...}
			//     fn()
			if len(v.Body) > 0 {
				err := r.CheckStmts(v.Body)
				if err != nil {
					return err
				}
			}

		case *ast.SwitchStmt:
			// switch x := func() {...}(); {...}
			if v.Init != nil {
				err := r.CheckAssign(v.Init.(*ast.AssignStmt))
				if err != nil {
					return err
				}
			}

			// switch (func() {...}()) {...}
			if err := r.checkExpr(v.Tag); err != nil {
				return err
			}

			// switch x {
			// case 1:
			// 	func() {...}()
			// }
			err := r.CheckStmts(v.Body.List)
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
					err := r.CheckAssign(assign)
					if err != nil {
						return err
					}
				}
				if expr, ok := v.Comm.(*ast.ExprStmt); ok {
					err := r.checkExpr(expr.X)
					if err != nil {
						return err
					}
				}
			}
			err := r.CheckStmts(v.Body)
			if err != nil {
				return err
			}

		case *ast.SelectStmt:
			if len(v.Body.List) < 1 {
				continue
			}
			err := r.CheckStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.ForStmt:
			// for i := func() int {...}(); i < func() int {...}(); i += func() int {...}() {...}
			// for iter.Begin(); !iter.End(); iter.Next() {...}
			if v.Init != nil {
				err := r.checkInitStmt(v.Init)
				if err != nil {
					return err
				}
			}
			if v.Cond != nil {
				err := r.checkExpr(v.Cond)
				if err != nil {
					return err
				}
			}
			if v.Post != nil {
				assign, ok := v.Post.(*ast.AssignStmt)
				if ok {
					err := r.CheckAssign(assign)
					if err != nil {
						return err
					}
				}
			}
			err := r.CheckStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.RangeStmt:
			// for i := range func() {...}() {...}
			if err := r.checkExpr(v.X); err != nil {
				return err
			}
			err := r.CheckStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.TypeSwitchStmt:
			if v.Assign != nil {
				// 	switch x := (func () {...}()).(type) {...}
				if assign, ok := v.Assign.(*ast.AssignStmt); ok {
					err := r.CheckAssign(assign)
					if err != nil {
						return err
					}
				}
				// 	switch (func () {...}()).(type) {...}
				if expr, ok := v.Assign.(*ast.ExprStmt); ok {
					err := r.checkExpr(expr.X)
					if err != nil {
						return err
					}
				}
			}
			err := r.CheckStmts(v.Body.List)
			if err != nil {
				return err
			}

		case *ast.SendStmt:
			// 	ch <- func () {...}()
			err := r.checkExprs([]ast.Expr{v.Chan, v.Value})
			if err != nil {
				return err
			}

		case *ast.LabeledStmt:
			// Label:
			//     func () {...}()
			stmts := []ast.Stmt{v.Stmt}
			err := r.CheckStmts(stmts)
			if err != nil {
				return err
			}
			v.Stmt = stmts[0]

		case *ast.IncDecStmt:
			// func() *FooType {...}().Field++
			// func() *FooType {...}().Field--
			err := r.checkExpr(v.X)
			if err != nil {
				return err
			}

		case *ast.BranchStmt:
			// ignore keyword token (BREAK, CONTINUE, GOTO, FALLTHROUGH)

		default:
			fmt.Printf("unsupported statement: %T in %s\n", v, r.pos(v.Pos()))
		}
	}
	return nil
}

func (r *FailpointChecker) checkExpr(expr ast.Expr) error {
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
	case *ast.SelectorExpr:
		return r.checkSelector(*ex)

	case *ast.IndexExpr:
		// func()[]int {}()[func()int{}()]
		if err := r.checkExpr(ex.X); err != nil {
			return err
		}
		return r.checkExpr(ex.Index)

	case *ast.SliceExpr:
		// array[low:high:max]
		// => func()[]int {}()[func()int{}():func()int{}():func()int{}()]
		if err := r.checkExpr(ex.Low); err != nil {
			return err
		}
		if err := r.checkExpr(ex.High); err != nil {
			return err
		}
		if err := r.checkExpr(ex.Max); err != nil {
			return err
		}
		return r.checkExpr(ex.X)

	case *ast.FuncLit:
		// return func(){...},
		return r.checkFuncLit(ex)

	case *ast.CompositeLit:
		// []int{func() int {...}()}
		for _, elt := range ex.Elts {
			if err := r.checkExpr(elt); err != nil {
				return err
			}
		}

	case *ast.CallExpr:
		if err := r.checkExpr(ex.Fun); err != nil {
			return err
		}
		// return func() int {...}()
		if fn, ok := ex.Fun.(*ast.FuncLit); ok {
			err := r.checkFuncLit(fn)
			if err != nil {
				return err
			}
		}

		// return fn(func() int{...})
		for _, arg := range ex.Args {
			if fn, ok := arg.(*ast.FuncLit); ok {
				err := r.checkFuncLit(fn)
				if err != nil {
					return err
				}
			}
		}

	case *ast.StarExpr:
		// *func() *T{}()
		return r.checkExpr(ex.X)

	case *ast.UnaryExpr:
		// !func() {...}()
		return r.checkExpr(ex.X)

	case *ast.BinaryExpr:
		// a && func() bool {...} ()
		// func() bool {...} () && a
		// func() bool {...} () && func() bool {...} () && a
		// func() bool {...} () && a && func() bool {...} () && a
		err := r.checkExpr(ex.X)
		if err != nil {
			return err
		}
		return r.checkExpr(ex.Y)

	case *ast.ParenExpr:
		// (func() {...}())
		return r.checkExpr(ex.X)

	case *ast.TypeAssertExpr:
		// (func() {...}()).(type)
		return r.checkExpr(ex.X)

	case *ast.KeyValueExpr:
		// Key: (func() {...}())
		return r.checkExpr(ex.Value)

	default:
		fmt.Printf("unspport expression: %T\n", expr)
	}
	return nil
}

func (r *FailpointChecker) checkFuncLit(fn *ast.FuncLit) error {
	return r.CheckStmts(fn.Body.List)
}

func (r *FailpointChecker) CheckAssign(v *ast.AssignStmt) error {
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
		err := r.checkExpr(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *FailpointChecker) checkExprs(exprs []ast.Expr) error {
	for _, expr := range exprs {
		err := r.checkExpr(expr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *FailpointChecker) checkIfStmt(v *ast.IfStmt) error {
	// if a, b := func() {...}, func() int {...}(); cond {...}
	// if func() {...}(); cond {...}
	if v.Init != nil {
		err := r.checkInitStmt(v.Init)
		if err != nil {
			return err
		}
	}

	if err := r.checkExpr(v.Cond); err != nil {
		return err
	}

	err := r.CheckStmts(v.Body.List)
	if err != nil {
		return err
	}
	if v.Else != nil {
		if elseIf, ok := v.Else.(*ast.IfStmt); ok {
			return r.checkIfStmt(elseIf)
		}
		if els, ok := v.Else.(*ast.BlockStmt); ok {
			return r.CheckStmts(els.List)
		}
	}
	return nil
}

func (r *FailpointChecker) checkInitStmt(v ast.Stmt) error {
	var err error
	switch stmt := v.(type) {
	case *ast.ExprStmt:
		err = r.checkExpr(stmt.X)
	case *ast.AssignStmt:
		err = r.CheckAssign(stmt)
	}
	return err
}

func (r *FailpointChecker) checkSelector(v ast.SelectorExpr) error {
	if r.Mode != CheckFailPoint {
		return nil
	}
	packageName, ok := v.X.(*ast.Ident)
	if ok && packageName.Name == r.failpointName && strings.EqualFold(v.Sel.Name, "Enable") {
		// Find a usage of failpoint, check that whether the testSuite is serial.
		if !r.isCurrentTestSerial {
			err := errors.New(fmt.Sprintf("Find failpoint in non-serial testSuite, package: %s, test name: %s", r.currentFile.Name, r.currTestName))
			r.errList = append(r.errList, err)
			return nil
		} else {
			return nil
		}
	} else {
		return r.checkExpr(v.X)
	}
}
