// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"go/constant"
	"go/token"
	"go/types"
	"io"
	"math/big"
)

// TODO(gri) use tabwriter for alignment?

func print(w io.Writer, pkg *types.Package, filter func(types.Object) bool) {
	var p printer
	p.pkg = pkg
	p.printPackage(pkg, filter)
	p.printGccgoExtra(pkg)
	io.Copy(w, &p.buf)
}

type printer struct {
	pkg    *types.Package
	buf    bytes.Buffer
	indent int  // current indentation level
	last   byte // last byte written
}

func (p *printer) print(s string) {
	// Write the string one byte at a time. We care about the presence of
	// newlines for indentation which we will see even in the presence of
	// (non-corrupted) Unicode; no need to read one rune at a time.
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch != '\n' && p.last == '\n' {
			// Note: This could lead to a range overflow for very large
			// indentations, but it's extremely unlikely to happen for
			// non-pathological code.
			p.buf.WriteString("\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t"[:p.indent])
		}
		p.buf.WriteByte(ch)
		p.last = ch
	}
}

func (p *printer) printf(format string, args ...interface{}) {
	p.print(fmt.Sprintf(format, args...))
}

// methodsFor returns the named type and corresponding methods if the type
// denoted by obj is not an interface and has methods. Otherwise it returns
// the zero value.
func methodsFor(obj *types.TypeName) (*types.Named, []*types.Selection) {
	named, _ := obj.Type().(*types.Named)
	if named == nil {
		// A type name's type can also be the
		// exported basic type unsafe.Pointer.
		return nil, nil
	}
	if _, ok := named.Underlying().(*types.Interface); ok {
		// ignore interfaces
		return nil, nil
	}
	methods := combinedMethodSet(named)
	if len(methods) == 0 {
		return nil, nil
	}
	return named, methods
}

func (p *printer) printPackage(pkg *types.Package, filter func(types.Object) bool) {
	// collect objects by kind
	var (
		consts   []*types.Const
		typem    []*types.Named    // non-interface types with methods
		typez    []*types.TypeName // interfaces or types without methods
		vars     []*types.Var
		funcs    []*types.Func
		builtins []*types.Builtin
		methods  = make(map[*types.Named][]*types.Selection) // method sets for named types
	)
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		if obj.Exported() {
			// collect top-level exported and possibly filtered objects
			if filter == nil || filter(obj) {
				switch obj := obj.(type) {
				case *types.Const:
					consts = append(consts, obj)
				case *types.TypeName:
					// group into types with methods and types without
					if named, m := methodsFor(obj); named != nil {
						typem = append(typem, named)
						methods[named] = m
					} else {
						typez = append(typez, obj)
					}
				case *types.Var:
					vars = append(vars, obj)
				case *types.Func:
					funcs = append(funcs, obj)
				case *types.Builtin:
					// for unsafe.Sizeof, etc.
					builtins = append(builtins, obj)
				}
			}
		} else if filter == nil {
			// no filtering: collect top-level unexported types with methods
			if obj, _ := obj.(*types.TypeName); obj != nil {
				// see case *types.TypeName above
				if named, m := methodsFor(obj); named != nil {
					typem = append(typem, named)
					methods[named] = m
				}
			}
		}
	}

	p.printf("package %s  // %q\n", pkg.Name(), pkg.Path())

	p.printDecl("const", len(consts), func() {
		for _, obj := range consts {
			p.printObj(obj)
			p.print("\n")
		}
	})

	p.printDecl("var", len(vars), func() {
		for _, obj := range vars {
			p.printObj(obj)
			p.print("\n")
		}
	})

	p.printDecl("type", len(typez), func() {
		for _, obj := range typez {
			p.printf("%s ", obj.Name())
			typ := obj.Type()
			if isAlias(obj) {
				p.print("= ")
				p.writeType(p.pkg, typ)
			} else {
				p.writeType(p.pkg, typ.Underlying())
			}
			p.print("\n")
		}
	})

	// non-interface types with methods
	for _, named := range typem {
		first := true
		if obj := named.Obj(); obj.Exported() {
			if first {
				p.print("\n")
				first = false
			}
			p.printf("type %s ", obj.Name())
			p.writeType(p.pkg, named.Underlying())
			p.print("\n")
		}
		for _, m := range methods[named] {
			if obj := m.Obj(); obj.Exported() {
				if first {
					p.print("\n")
					first = false
				}
				p.printFunc(m.Recv(), obj.(*types.Func))
				p.print("\n")
			}
		}
	}

	if len(funcs) > 0 {
		p.print("\n")
		for _, obj := range funcs {
			p.printFunc(nil, obj)
			p.print("\n")
		}
	}

	// TODO(gri) better handling of builtins (package unsafe only)
	if len(builtins) > 0 {
		p.print("\n")
		for _, obj := range builtins {
			p.printf("func %s() // builtin\n", obj.Name())
		}
	}

	p.print("\n")
}

func (p *printer) printDecl(keyword string, n int, printGroup func()) {
	switch n {
	case 0:
		// nothing to do
	case 1:
		p.printf("\n%s ", keyword)
		printGroup()
	default:
		p.printf("\n%s (\n", keyword)
		p.indent++
		printGroup()
		p.indent--
		p.print(")\n")
	}
}

// absInt returns the absolute value of v as a *big.Int.
// v must be a numeric value.
func absInt(v constant.Value) *big.Int {
	// compute big-endian representation of v
	b := constant.Bytes(v) // little-endian
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return new(big.Int).SetBytes(b)
}

var (
	one = big.NewRat(1, 1)
	ten = big.NewRat(10, 1)
)

// floatString returns the string representation for a
// numeric value v in normalized floating-point format.
func floatString(v constant.Value) string {
	if constant.Sign(v) == 0 {
		return "0.0"
	}
	// x != 0

	// convert |v| into a big.Rat x
	x := new(big.Rat).SetFrac(absInt(constant.Num(v)), absInt(constant.Denom(v)))

	// normalize x and determine exponent e
	// (This is not very efficient, but also not speed-critical.)
	var e int
	for x.Cmp(ten) >= 0 {
		x.Quo(x, ten)
		e++
	}
	for x.Cmp(one) < 0 {
		x.Mul(x, ten)
		e--
	}

	// TODO(gri) Values such as 1/2 are easier to read in form 0.5
	// rather than 5.0e-1. Similarly, 1.0e1 is easier to read as
	// 10.0. Fine-tune best exponent range for readability.

	s := x.FloatString(100) // good-enough precision

	// trim trailing 0's
	i := len(s)
	for i > 0 && s[i-1] == '0' {
		i--
	}
	s = s[:i]

	// add a 0 if the number ends in decimal point
	if len(s) > 0 && s[len(s)-1] == '.' {
		s += "0"
	}

	// add exponent and sign
	if e != 0 {
		s += fmt.Sprintf("e%+d", e)
	}
	if constant.Sign(v) < 0 {
		s = "-" + s
	}

	// TODO(gri) If v is a "small" fraction (i.e., numerator and denominator
	// are just a small number of decimal digits), add the exact fraction as
	// a comment. For instance: 3.3333...e-1 /* = 1/3 */

	return s
}

// valString returns the string representation for the value v.
// Setting floatFmt forces an integer value to be formatted in
// normalized floating-point format.
// TODO(gri) Move this code into package constant.
func valString(v constant.Value, floatFmt bool) string {
	switch v.Kind() {
	case constant.Int:
		if floatFmt {
			return floatString(v)
		}
	case constant.Float:
		return floatString(v)
	case constant.Complex:
		re := constant.Real(v)
		im := constant.Imag(v)
		var s string
		if constant.Sign(re) != 0 {
			s = floatString(re)
			if constant.Sign(im) >= 0 {
				s += " + "
			} else {
				s += " - "
				im = constant.UnaryOp(token.SUB, im, 0) // negate im
			}
		}
		// im != 0, otherwise v would be constant.Int or constant.Float
		return s + floatString(im) + "i"
	}
	return v.String()
}

func (p *printer) printObj(obj types.Object) {
	p.print(obj.Name())

	typ, basic := obj.Type().Underlying().(*types.Basic)
	if basic && typ.Info()&types.IsUntyped != 0 {
		// don't write untyped types
	} else {
		p.print(" ")
		p.writeType(p.pkg, obj.Type())
	}

	if obj, ok := obj.(*types.Const); ok {
		floatFmt := basic && typ.Info()&(types.IsFloat|types.IsComplex) != 0
		p.print(" = ")
		p.print(valString(obj.Val(), floatFmt))
	}
}

func (p *printer) printFunc(recvType types.Type, obj *types.Func) {
	p.print("func ")
	sig := obj.Type().(*types.Signature)
	if recvType != nil {
		p.print("(")
		p.writeType(p.pkg, recvType)
		p.print(") ")
	}
	p.print(obj.Name())
	p.writeSignature(p.pkg, sig)
}

// combinedMethodSet returns the method set for a named type T
// merged with all the methods of *T that have different names than
// the methods of T.
//
// combinedMethodSet is analogous to types/typeutil.IntuitiveMethodSet
// but doesn't require a MethodSetCache.
// TODO(gri) If this functionality doesn't change over time, consider
// just calling IntuitiveMethodSet eventually.
func combinedMethodSet(T *types.Named) []*types.Selection {
	// method set for T
	mset := types.NewMethodSet(T)
	var res []*types.Selection
	for i, n := 0, mset.Len(); i < n; i++ {
		res = append(res, mset.At(i))
	}

	// add all *T methods with names different from T methods
	pmset := types.NewMethodSet(types.NewPointer(T))
	for i, n := 0, pmset.Len(); i < n; i++ {
		pm := pmset.At(i)
		if obj := pm.Obj(); mset.Lookup(obj.Pkg(), obj.Name()) == nil {
			res = append(res, pm)
		}
	}

	return res
}
