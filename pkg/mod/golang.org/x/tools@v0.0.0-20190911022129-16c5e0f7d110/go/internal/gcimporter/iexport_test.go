// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This is a copy of bexport_test.go for iexport.go.

// +build go1.11

package gcimporter_test

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"math/big"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/buildutil"
	"golang.org/x/tools/go/internal/gcimporter"
	"golang.org/x/tools/go/loader"
)

func TestIExportData_stdlib(t *testing.T) {
	if runtime.Compiler == "gccgo" {
		t.Skip("gccgo standard library is inaccessible")
	}
	if runtime.GOOS == "android" {
		t.Skipf("incomplete std lib on %s", runtime.GOOS)
	}
	if isRace {
		t.Skipf("stdlib tests take too long in race mode and flake on builders")
	}

	// Load, parse and type-check the program.
	ctxt := build.Default // copy
	ctxt.GOPATH = ""      // disable GOPATH
	conf := loader.Config{
		Build:       &ctxt,
		AllowErrors: true,
	}
	for _, path := range buildutil.AllPackages(conf.Build) {
		conf.Import(path)
	}

	// Create a package containing type and value errors to ensure
	// they are properly encoded/decoded.
	f, err := conf.ParseFile("haserrors/haserrors.go", `package haserrors
const UnknownValue = "" + 0
type UnknownType undefined
`)
	if err != nil {
		t.Fatal(err)
	}
	conf.CreateFromFiles("haserrors", f)

	prog, err := conf.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	numPkgs := len(prog.AllPackages)
	if want := 248; numPkgs < want {
		t.Errorf("Loaded only %d packages, want at least %d", numPkgs, want)
	}

	var sorted []*types.Package
	for pkg := range prog.AllPackages {
		sorted = append(sorted, pkg)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Path() < sorted[j].Path()
	})

	for _, pkg := range sorted {
		info := prog.AllPackages[pkg]
		if info.Files == nil {
			continue // empty directory
		}
		exportdata, err := gcimporter.IExportData(conf.Fset, pkg)
		if err != nil {
			t.Fatal(err)
		}
		if exportdata[0] == 'i' {
			exportdata = exportdata[1:] // trim the 'i' in the header
		} else {
			t.Fatalf("unexpected first character of export data: %v", exportdata[0])
		}

		imports := make(map[string]*types.Package)
		fset2 := token.NewFileSet()
		n, pkg2, err := gcimporter.IImportData(fset2, imports, exportdata, pkg.Path())
		if err != nil {
			t.Errorf("IImportData(%s): %v", pkg.Path(), err)
			continue
		}
		if n != len(exportdata) {
			t.Errorf("IImportData(%s) decoded %d bytes, want %d",
				pkg.Path(), n, len(exportdata))
		}

		// Compare the packages' corresponding members.
		for _, name := range pkg.Scope().Names() {
			if !ast.IsExported(name) {
				continue
			}
			obj1 := pkg.Scope().Lookup(name)
			obj2 := pkg2.Scope().Lookup(name)
			if obj2 == nil {
				t.Fatalf("%s.%s not found, want %s", pkg.Path(), name, obj1)
				continue
			}

			fl1 := fileLine(conf.Fset, obj1)
			fl2 := fileLine(fset2, obj2)
			if fl1 != fl2 {
				t.Errorf("%s.%s: got posn %s, want %s",
					pkg.Path(), name, fl2, fl1)
			}

			if err := cmpObj(obj1, obj2); err != nil {
				t.Errorf("%s.%s: %s\ngot:  %s\nwant: %s",
					pkg.Path(), name, err, obj2, obj1)
			}
		}
	}
}

// TestVeryLongFile tests the position of an import object declared in
// a very long input file.  Line numbers greater than maxlines are
// reported as line 1, not garbage or token.NoPos.
func TestIExportData_long(t *testing.T) {
	// parse and typecheck
	longFile := "package foo" + strings.Repeat("\n", 123456) + "var X int"
	fset1 := token.NewFileSet()
	f, err := parser.ParseFile(fset1, "foo.go", longFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	var conf types.Config
	pkg, err := conf.Check("foo", fset1, []*ast.File{f}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// export
	exportdata, err := gcimporter.IExportData(fset1, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if exportdata[0] == 'i' {
		exportdata = exportdata[1:] // trim the 'i' in the header
	} else {
		t.Fatalf("unexpected first character of export data: %v", exportdata[0])
	}

	// import
	imports := make(map[string]*types.Package)
	fset2 := token.NewFileSet()
	_, pkg2, err := gcimporter.IImportData(fset2, imports, exportdata, pkg.Path())
	if err != nil {
		t.Fatalf("IImportData(%s): %v", pkg.Path(), err)
	}

	// compare
	posn1 := fset1.Position(pkg.Scope().Lookup("X").Pos())
	posn2 := fset2.Position(pkg2.Scope().Lookup("X").Pos())
	if want := "foo.go:1:1"; posn2.String() != want {
		t.Errorf("X position = %s, want %s (orig was %s)",
			posn2, want, posn1)
	}
}

func TestIExportData_typealiases(t *testing.T) {
	// parse and typecheck
	fset1 := token.NewFileSet()
	f, err := parser.ParseFile(fset1, "p.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var conf types.Config
	pkg1, err := conf.Check("p", fset1, []*ast.File{f}, nil)
	if err == nil {
		// foo in undeclared in src; we should see an error
		t.Fatal("invalid source type-checked without error")
	}
	if pkg1 == nil {
		// despite incorrect src we should see a (partially) type-checked package
		t.Fatal("nil package returned")
	}
	checkPkg(t, pkg1, "export")

	// export
	exportdata, err := gcimporter.IExportData(fset1, pkg1)
	if err != nil {
		t.Fatal(err)
	}
	if exportdata[0] == 'i' {
		exportdata = exportdata[1:] // trim the 'i' in the header
	} else {
		t.Fatalf("unexpected first character of export data: %v", exportdata[0])
	}

	// import
	imports := make(map[string]*types.Package)
	fset2 := token.NewFileSet()
	_, pkg2, err := gcimporter.IImportData(fset2, imports, exportdata, pkg1.Path())
	if err != nil {
		t.Fatalf("IImportData(%s): %v", pkg1.Path(), err)
	}
	checkPkg(t, pkg2, "import")
}

// cmpObj reports how x and y differ. They are assumed to belong to different
// universes so cannot be compared directly. It is an adapted version of
// equalObj in bexport_test.go.
func cmpObj(x, y types.Object) error {
	if reflect.TypeOf(x) != reflect.TypeOf(y) {
		return fmt.Errorf("%T vs %T", x, y)
	}
	xt := x.Type()
	yt := y.Type()
	switch x.(type) {
	case *types.Var, *types.Func:
		// ok
	case *types.Const:
		xval := x.(*types.Const).Val()
		yval := y.(*types.Const).Val()
		equal := constant.Compare(xval, token.EQL, yval)
		if !equal {
			// try approx. comparison
			xkind := xval.Kind()
			ykind := yval.Kind()
			if xkind == constant.Complex || ykind == constant.Complex {
				equal = same(constant.Real(xval), constant.Real(yval)) &&
					same(constant.Imag(xval), constant.Imag(yval))
			} else if xkind == constant.Float || ykind == constant.Float {
				equal = same(xval, yval)
			} else if xkind == constant.Unknown && ykind == constant.Unknown {
				equal = true
			}
		}
		if !equal {
			return fmt.Errorf("unequal constants %s vs %s", xval, yval)
		}
	case *types.TypeName:
		xt = xt.Underlying()
		yt = yt.Underlying()
	default:
		return fmt.Errorf("unexpected %T", x)
	}
	return equalType(xt, yt)
}

// Use the same floating-point precision (512) as cmd/compile
// (see Mpprec in cmd/compile/internal/gc/mpfloat.go).
const mpprec = 512

// same compares non-complex numeric values and reports if they are approximately equal.
func same(x, y constant.Value) bool {
	xf := constantToFloat(x)
	yf := constantToFloat(y)
	d := new(big.Float).Sub(xf, yf)
	d.Abs(d)
	eps := big.NewFloat(1.0 / (1 << (mpprec - 1))) // allow for 1 bit of error
	return d.Cmp(eps) < 0
}

// copy of the function with the same name in iexport.go.
func constantToFloat(x constant.Value) *big.Float {
	var f big.Float
	f.SetPrec(mpprec)
	if v, exact := constant.Float64Val(x); exact {
		// float64
		f.SetFloat64(v)
	} else if num, denom := constant.Num(x), constant.Denom(x); num.Kind() == constant.Int {
		// TODO(gri): add big.Rat accessor to constant.Value.
		n := valueToRat(num)
		d := valueToRat(denom)
		f.SetRat(n.Quo(n, d))
	} else {
		// Value too large to represent as a fraction => inaccessible.
		// TODO(gri): add big.Float accessor to constant.Value.
		_, ok := f.SetString(x.ExactString())
		if !ok {
			panic("should not reach here")
		}
	}
	return &f
}

// copy of the function with the same name in iexport.go.
func valueToRat(x constant.Value) *big.Rat {
	// Convert little-endian to big-endian.
	// I can't believe this is necessary.
	bytes := constant.Bytes(x)
	for i := 0; i < len(bytes)/2; i++ {
		bytes[i], bytes[len(bytes)-1-i] = bytes[len(bytes)-1-i], bytes[i]
	}
	return new(big.Rat).SetInt(new(big.Int).SetBytes(bytes))
}
