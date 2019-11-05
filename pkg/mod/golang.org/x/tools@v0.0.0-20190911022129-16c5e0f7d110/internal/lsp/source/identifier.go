// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package source

import (
	"context"
	"go/ast"
	"go/token"
	"go/types"
	"strconv"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/internal/lsp/protocol"
	"golang.org/x/tools/internal/span"
	"golang.org/x/tools/internal/telemetry/trace"
	errors "golang.org/x/xerrors"
)

// IdentifierInfo holds information about an identifier in Go source.
type IdentifierInfo struct {
	Name string
	View View
	File ParseGoHandle
	mappedRange

	Type struct {
		mappedRange
		Object types.Object
	}

	Declaration Declaration

	pkgs             []Package
	ident            *ast.Ident
	wasEmbeddedField bool
	qf               types.Qualifier
}

type Declaration struct {
	mappedRange
	node        ast.Node
	obj         types.Object
	wasImplicit bool
}

// Identifier returns identifier information for a position
// in a file, accounting for a potentially incomplete selector.
func Identifier(ctx context.Context, view View, f GoFile, pos protocol.Position) (*IdentifierInfo, error) {
	file, pkgs, m, err := fileToMapper(ctx, view, f.URI())
	if err != nil {
		return nil, err
	}
	spn, err := m.PointSpan(pos)
	if err != nil {
		return nil, err
	}
	rng, err := spn.Range(m.Converter)
	if err != nil {
		return nil, err
	}
	return findIdentifier(ctx, view, pkgs, file, rng.Start)
}

func findIdentifier(ctx context.Context, view View, pkgs []Package, file *ast.File, pos token.Pos) (*IdentifierInfo, error) {
	if result, err := identifier(ctx, view, pkgs, file, pos); err != nil || result != nil {
		return result, err
	}
	// If the position is not an identifier but immediately follows
	// an identifier or selector period (as is common when
	// requesting a completion), use the path to the preceding node.
	ident, err := identifier(ctx, view, pkgs, file, pos-1)
	if ident == nil && err == nil {
		err = errors.New("no identifier found")
	}
	return ident, err
}

// identifier checks a single position for a potential identifier.
func identifier(ctx context.Context, view View, pkgs []Package, file *ast.File, pos token.Pos) (*IdentifierInfo, error) {
	ctx, done := trace.StartSpan(ctx, "source.identifier")
	defer done()

	var err error

	// Handle import specs separately, as there is no formal position for a package declaration.
	if result, err := importSpec(ctx, view, file, pkgs, pos); result != nil || err != nil {
		return result, err
	}
	path, _ := astutil.PathEnclosingInterval(file, pos, pos)
	if path == nil {
		return nil, errors.Errorf("can't find node enclosing position")
	}
	uri := span.FileURI(view.Session().Cache().FileSet().Position(pos).Filename)
	pkg, err := bestPackage(uri, pkgs)
	if err != nil {
		return nil, err
	}
	var ph ParseGoHandle
	for _, h := range pkg.GetHandles() {
		if h.File().Identity().URI == uri {
			ph = h
		}
	}
	result := &IdentifierInfo{
		View:  view,
		File:  ph,
		qf:    qualifier(file, pkg.GetTypes(), pkg.GetTypesInfo()),
		pkgs:  pkgs,
		ident: searchForIdent(path[0]),
	}
	// No identifier at the given position.
	if result.ident == nil {
		return nil, nil
	}
	for _, n := range path[1:] {
		if field, ok := n.(*ast.Field); ok {
			result.wasEmbeddedField = len(field.Names) == 0
			break
		}
	}
	result.Name = result.ident.Name
	if result.mappedRange, err = posToRange(ctx, view, result.ident.Pos(), result.ident.End()); err != nil {
		return nil, err
	}
	result.Declaration.obj = pkg.GetTypesInfo().ObjectOf(result.ident)
	if result.Declaration.obj == nil {
		// If there was no types.Object for the declaration, there might be an implicit local variable
		// declaration in a type switch.
		if objs := typeSwitchVar(pkg.GetTypesInfo(), path); len(objs) > 0 {
			// There is no types.Object for the declaration of an implicit local variable,
			// but all of the types.Objects associated with the usages of this variable can be
			// used to connect it back to the declaration.
			// Preserve the first of these objects and treat it as if it were the declaring object.
			result.Declaration.obj = objs[0]
			result.Declaration.wasImplicit = true
		} else {
			// Probably a type error.
			return nil, errors.Errorf("no object for ident %v", result.Name)
		}
	}

	// Handle builtins separately.
	if result.Declaration.obj.Parent() == types.Universe {
		decl, ok := lookupBuiltinDecl(view, result.Name).(ast.Node)
		if !ok {
			return nil, errors.Errorf("no declaration for %s", result.Name)
		}
		result.Declaration.node = decl
		if result.Declaration.mappedRange, err = nameToMappedRange(ctx, view, decl.Pos(), result.Name); err != nil {
			return nil, err
		}
		return result, nil
	}

	if result.wasEmbeddedField {
		// The original position was on the embedded field declaration, so we
		// try to dig out the type and jump to that instead.
		if v, ok := result.Declaration.obj.(*types.Var); ok {
			if typObj := typeToObject(v.Type()); typObj != nil {
				result.Declaration.obj = typObj
			}
		}
	}

	for _, obj := range pkg.GetTypesInfo().Implicits {
		if obj.Pos() == result.Declaration.obj.Pos() {
			// Mark this declaration as implicit, since it will not
			// appear in a (*types.Info).Defs map.
			result.Declaration.wasImplicit = true
			break
		}
	}

	if result.Declaration.mappedRange, err = objToMappedRange(ctx, view, result.Declaration.obj); err != nil {
		return nil, err
	}
	if result.Declaration.node, err = objToNode(ctx, view, pkg, result.Declaration.obj); err != nil {
		return nil, err
	}
	typ := pkg.GetTypesInfo().TypeOf(result.ident)
	if typ == nil {
		return result, nil
	}

	result.Type.Object = typeToObject(typ)
	if result.Type.Object != nil {
		// Identifiers with the type "error" are a special case with no position.
		if hasErrorType(result.Type.Object) {
			return result, nil
		}
		if result.Type.mappedRange, err = objToMappedRange(ctx, view, result.Type.Object); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func searchForIdent(n ast.Node) *ast.Ident {
	switch node := n.(type) {
	case *ast.Ident:
		return node
	case *ast.SelectorExpr:
		return node.Sel
	case *ast.StarExpr:
		return searchForIdent(node.X)
	}
	return nil
}

func typeToObject(typ types.Type) types.Object {
	switch typ := typ.(type) {
	case *types.Named:
		return typ.Obj()
	case *types.Pointer:
		return typeToObject(typ.Elem())
	default:
		return nil
	}
}

func hasErrorType(obj types.Object) bool {
	return types.IsInterface(obj.Type()) && obj.Pkg() == nil && obj.Name() == "error"
}

func objToNode(ctx context.Context, view View, pkg Package, obj types.Object) (ast.Decl, error) {
	uri := span.FileURI(view.Session().Cache().FileSet().Position(obj.Pos()).Filename)
	_, declAST, _, err := pkg.FindFile(ctx, uri, obj.Pos())
	if declAST == nil {
		return nil, err
	}
	path, _ := astutil.PathEnclosingInterval(declAST, obj.Pos(), obj.Pos())
	if path == nil {
		return nil, errors.Errorf("no path for object %v", obj.Name())
	}
	for _, node := range path {
		switch node := node.(type) {
		case *ast.GenDecl:
			// Type names, fields, and methods.
			switch obj.(type) {
			case *types.TypeName, *types.Var, *types.Const, *types.Func:
				return node, nil
			}
		case *ast.FuncDecl:
			// Function signatures.
			if _, ok := obj.(*types.Func); ok {
				return node, nil
			}
		}
	}
	return nil, nil // didn't find a node, but don't fail
}

// importSpec handles positions inside of an *ast.ImportSpec.
func importSpec(ctx context.Context, view View, fAST *ast.File, pkgs []Package, pos token.Pos) (*IdentifierInfo, error) {
	var imp *ast.ImportSpec
	for _, spec := range fAST.Imports {
		if spec.Path.Pos() <= pos && pos < spec.Path.End() {
			imp = spec
		}
	}
	if imp == nil {
		return nil, nil
	}
	importPath, err := strconv.Unquote(imp.Path.Value)
	if err != nil {
		return nil, errors.Errorf("import path not quoted: %s (%v)", imp.Path.Value, err)
	}
	uri := span.FileURI(view.Session().Cache().FileSet().Position(pos).Filename)
	pkg, err := bestPackage(uri, pkgs)
	if err != nil {
		return nil, err
	}
	var ph ParseGoHandle
	for _, h := range pkg.GetHandles() {
		if h.File().Identity().URI == uri {
			ph = h
		}
	}
	result := &IdentifierInfo{
		View: view,
		File: ph,
		Name: importPath,
		pkgs: pkgs,
	}
	if result.mappedRange, err = posToRange(ctx, view, imp.Path.Pos(), imp.Path.End()); err != nil {
		return nil, err
	}
	// Consider the "declaration" of an import spec to be the imported package.
	importedPkg, err := pkg.GetImport(ctx, importPath)
	if err != nil {
		return nil, err
	}
	if importedPkg.GetSyntax(ctx) == nil {
		return nil, errors.Errorf("no syntax for for %q", importPath)
	}
	// Heuristic: Jump to the longest (most "interesting") file of the package.
	var dest *ast.File
	for _, f := range importedPkg.GetSyntax(ctx) {
		if dest == nil || f.End()-f.Pos() > dest.End()-dest.Pos() {
			dest = f
		}
	}
	if dest == nil {
		return nil, errors.Errorf("package %q has no files", importPath)
	}
	if result.Declaration.mappedRange, err = posToRange(ctx, view, dest.Pos(), dest.End()); err != nil {
		return nil, err
	}
	result.Declaration.node = imp
	return result, nil
}

// typeSwitchVar handles the special case of a local variable implicitly defined in a type switch.
// In such cases, the definition of the implicit variable will not be recorded in the *types.Info.Defs  map,
// but rather in the *types.Info.Implicits map.
func typeSwitchVar(info *types.Info, path []ast.Node) []types.Object {
	if len(path) < 3 {
		return nil
	}
	// Check for [Ident AssignStmt TypeSwitchStmt...]
	if _, ok := path[0].(*ast.Ident); !ok {
		return nil
	}
	if _, ok := path[1].(*ast.AssignStmt); !ok {
		return nil
	}
	sw, ok := path[2].(*ast.TypeSwitchStmt)
	if !ok {
		return nil
	}

	var res []types.Object
	for _, stmt := range sw.Body.List {
		obj := info.Implicits[stmt.(*ast.CaseClause)]
		if obj != nil {
			res = append(res, obj)
		}
	}
	return res
}
