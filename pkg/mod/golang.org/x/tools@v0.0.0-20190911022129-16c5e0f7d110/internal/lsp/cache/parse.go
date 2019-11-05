// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cache

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"reflect"

	"golang.org/x/tools/internal/lsp/source"
	"golang.org/x/tools/internal/lsp/telemetry"
	"golang.org/x/tools/internal/memoize"
	"golang.org/x/tools/internal/telemetry/log"
	"golang.org/x/tools/internal/telemetry/trace"
	errors "golang.org/x/xerrors"
)

// Limits the number of parallel parser calls per process.
var parseLimit = make(chan struct{}, 20)

// parseKey uniquely identifies a parsed Go file.
type parseKey struct {
	file source.FileIdentity
	mode source.ParseMode
}

type parseGoHandle struct {
	handle *memoize.Handle
	file   source.FileHandle
	mode   source.ParseMode
}

type parseGoData struct {
	memoize.NoCopy

	ast *ast.File
	err error
}

func (c *cache) ParseGoHandle(fh source.FileHandle, mode source.ParseMode) source.ParseGoHandle {
	key := parseKey{
		file: fh.Identity(),
		mode: mode,
	}
	h := c.store.Bind(key, func(ctx context.Context) interface{} {
		data := &parseGoData{}
		data.ast, data.err = parseGo(ctx, c, fh, mode)
		return data
	})
	return &parseGoHandle{
		handle: h,
		file:   fh,
		mode:   mode,
	}
}

func (h *parseGoHandle) File() source.FileHandle {
	return h.file
}

func (h *parseGoHandle) Mode() source.ParseMode {
	return h.mode
}

func (h *parseGoHandle) Parse(ctx context.Context) (*ast.File, error) {
	v := h.handle.Get(ctx)
	if v == nil {
		return nil, ctx.Err()
	}
	data := v.(*parseGoData)
	return data.ast, data.err
}

func (h *parseGoHandle) Cached(ctx context.Context) (*ast.File, error) {
	v := h.handle.Cached()
	if v == nil {
		return nil, errors.Errorf("no cached value for %s", h.file.Identity().URI)
	}
	data := v.(*parseGoData)
	return data.ast, data.err
}

func hashParseKey(ph source.ParseGoHandle) string {
	b := bytes.NewBuffer(nil)
	b.WriteString(ph.File().Identity().String())
	b.WriteString(string(ph.Mode()))
	return hashContents(b.Bytes())
}

func hashParseKeys(phs []source.ParseGoHandle) string {
	b := bytes.NewBuffer(nil)
	for _, ph := range phs {
		b.WriteString(hashParseKey(ph))
	}
	return hashContents(b.Bytes())
}

func parseGo(ctx context.Context, c *cache, fh source.FileHandle, mode source.ParseMode) (*ast.File, error) {
	ctx, done := trace.StartSpan(ctx, "cache.parseGo", telemetry.File.Of(fh.Identity().URI.Filename()))
	defer done()

	buf, _, err := fh.Read(ctx)
	if err != nil {
		return nil, err
	}
	parseLimit <- struct{}{}
	defer func() { <-parseLimit }()
	parserMode := parser.AllErrors | parser.ParseComments
	if mode == source.ParseHeader {
		parserMode = parser.ImportsOnly | parser.ParseComments
	}
	ast, err := parser.ParseFile(c.fset, fh.Identity().URI.Filename(), buf, parserMode)
	if ast != nil {
		if mode == source.ParseExported {
			trimAST(ast)
		}
		// Fix any badly parsed parts of the AST.
		tok := c.fset.File(ast.Pos())
		if err := fix(ctx, ast, tok, buf); err != nil {
			log.Error(ctx, "failed to fix AST", err)
		}
	}
	if ast == nil {
		return nil, err
	}
	return ast, err
}

// trimAST clears any part of the AST not relevant to type checking
// expressions at pos.
func trimAST(file *ast.File) {
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			return false
		}
		switch n := n.(type) {
		case *ast.FuncDecl:
			n.Body = nil
		case *ast.BlockStmt:
			n.List = nil
		case *ast.CaseClause:
			n.Body = nil
		case *ast.CommClause:
			n.Body = nil
		case *ast.CompositeLit:
			// Leave elts in place for [...]T
			// array literals, because they can
			// affect the expression's type.
			if !isEllipsisArray(n.Type) {
				n.Elts = nil
			}
		}
		return true
	})
}

func isEllipsisArray(n ast.Expr) bool {
	at, ok := n.(*ast.ArrayType)
	if !ok {
		return false
	}
	_, ok = at.Len.(*ast.Ellipsis)
	return ok
}

// fix inspects the AST and potentially modifies any *ast.BadStmts so that it can be
// type-checked more effectively.
func fix(ctx context.Context, n ast.Node, tok *token.File, src []byte) error {
	var (
		ancestors []ast.Node
		err       error
	)
	ast.Inspect(n, func(n ast.Node) bool {
		if n == nil {
			if len(ancestors) > 0 {
				ancestors = ancestors[:len(ancestors)-1]
			}
			return false
		}
		switch n := n.(type) {
		case *ast.BadStmt:
			var parent ast.Node
			if len(ancestors) > 0 {
				parent = ancestors[len(ancestors)-1]
			}
			err = parseDeferOrGoStmt(n, parent, tok, src) // don't shadow err
			if err == nil {
				// Recursively fix any BadStmts in our fixed node.
				err = fix(ctx, parent, tok, src)
			} else {
				err = errors.Errorf("unable to parse defer or go from *ast.BadStmt: %v", err)
			}
			return false
		default:
			ancestors = append(ancestors, n)
			return true
		}
	})
	return err
}

// parseDeferOrGoStmt tries to parse an *ast.BadStmt into a defer or a go statement.
//
// go/parser packages a statement of the form "defer x." as an *ast.BadStmt because
// it does not include a call expression. This means that go/types skips type-checking
// this statement entirely, and we can't use the type information when completing.
// Here, we try to generate a fake *ast.DeferStmt or *ast.GoStmt to put into the AST,
// instead of the *ast.BadStmt.
func parseDeferOrGoStmt(bad *ast.BadStmt, parent ast.Node, tok *token.File, src []byte) error {
	// Check if we have a bad statement containing either a "go" or "defer".
	s := &scanner.Scanner{}
	s.Init(tok, src, nil, 0)

	var (
		pos token.Pos
		tkn token.Token
	)
	for {
		if tkn == token.EOF {
			return errors.Errorf("reached the end of the file")
		}
		if pos >= bad.From {
			break
		}
		pos, tkn, _ = s.Scan()
	}

	var stmt ast.Stmt
	switch tkn {
	case token.DEFER:
		stmt = &ast.DeferStmt{
			Defer: pos,
		}
	case token.GO:
		stmt = &ast.GoStmt{
			Go: pos,
		}
	default:
		return errors.Errorf("no defer or go statement found")
	}

	var (
		from, to, last   token.Pos
		lastToken        token.Token
		braceDepth       int
		phantomSelectors []token.Pos
	)
FindTo:
	for {
		to, tkn, _ = s.Scan()

		if from == token.NoPos {
			from = to
		}

		switch tkn {
		case token.EOF:
			break FindTo
		case token.SEMICOLON:
			// If we aren't in nested braces, end of statement means
			// end of expression.
			if braceDepth == 0 {
				break FindTo
			}
		case token.LBRACE:
			braceDepth++
		}

		// This handles the common dangling selector case. For example in
		//
		// defer fmt.
		// y := 1
		//
		// we notice the dangling period and end our expression.
		//
		// If the previous token was a "." and we are looking at a "}",
		// the period is likely a dangling selector and needs a phantom
		// "_". Likewise if the currnet token is on a different line than
		// the period, the period is likely a dangling selector.
		if lastToken == token.PERIOD && (tkn == token.RBRACE || tok.Line(to) > tok.Line(last)) {
			// Insert phantom "_" selector after the dangling ".".
			phantomSelectors = append(phantomSelectors, last+1)
			// If we aren't in a block then end the expression after the ".".
			if braceDepth == 0 {
				to = last + 1
				break
			}
		}

		lastToken = tkn
		last = to

		switch tkn {
		case token.RBRACE:
			braceDepth--
			if braceDepth <= 0 {
				if braceDepth == 0 {
					// +1 to include the "}" itself.
					to += 1
				}
				break FindTo
			}
		}
	}

	if !from.IsValid() || tok.Offset(from) >= len(src) {
		return errors.Errorf("invalid from position")
	}

	if !to.IsValid() || tok.Offset(to) >= len(src) {
		return errors.Errorf("invalid to position %d", to)
	}

	// Insert any phantom selectors needed to prevent dangling "." from messing
	// up the AST.
	exprBytes := make([]byte, 0, int(to-from)+len(phantomSelectors))
	for i, b := range src[tok.Offset(from):tok.Offset(to)] {
		if len(phantomSelectors) > 0 && from+token.Pos(i) == phantomSelectors[0] {
			exprBytes = append(exprBytes, '_')
			phantomSelectors = phantomSelectors[1:]
		}
		exprBytes = append(exprBytes, b)
	}

	if len(phantomSelectors) > 0 {
		exprBytes = append(exprBytes, '_')
	}

	// Wrap our expression to make it a valid Go file we can pass to ParseFile.
	fileSrc := bytes.Join([][]byte{
		[]byte("package fake;func _(){"),
		exprBytes,
		[]byte("}"),
	}, nil)

	// Use ParseFile instead of ParseExpr because ParseFile has
	// best-effort behavior, whereas ParseExpr fails hard on any error.
	fakeFile, err := parser.ParseFile(token.NewFileSet(), "", fileSrc, 0)
	if fakeFile == nil {
		return errors.Errorf("error reading fake file source: %v", err)
	}

	// Extract our expression node from inside the fake file.
	if len(fakeFile.Decls) == 0 {
		return errors.Errorf("error parsing fake file: %v", err)
	}
	fakeDecl, _ := fakeFile.Decls[0].(*ast.FuncDecl)
	if fakeDecl == nil || len(fakeDecl.Body.List) == 0 {
		return errors.Errorf("no statement in %s: %v", exprBytes, err)
	}
	exprStmt, ok := fakeDecl.Body.List[0].(*ast.ExprStmt)
	if !ok {
		return errors.Errorf("no expr in %s: %v", exprBytes, err)
	}
	expr := exprStmt.X

	// parser.ParseExpr returns undefined positions.
	// Adjust them for the current file.
	offsetPositions(expr, from-1-(expr.Pos()-1))

	// Package the expression into a fake *ast.CallExpr and re-insert
	// into the function.
	call := &ast.CallExpr{
		Fun:    expr,
		Lparen: to,
		Rparen: to,
	}
	switch stmt := stmt.(type) {
	case *ast.DeferStmt:
		stmt.Call = call
	case *ast.GoStmt:
		stmt.Call = call
	}
	switch parent := parent.(type) {
	case *ast.BlockStmt:
		for i, s := range parent.List {
			if s == bad {
				parent.List[i] = stmt
				break
			}
		}
	}
	return nil
}

var tokenPosType = reflect.TypeOf(token.NoPos)

// offsetPositions applies an offset to the positions in an ast.Node.
func offsetPositions(expr ast.Expr, offset token.Pos) {
	ast.Inspect(expr, func(n ast.Node) bool {
		if n == nil {
			return false
		}

		v := reflect.ValueOf(n).Elem()

		switch v.Kind() {
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				f := v.Field(i)
				if f.Type() != tokenPosType {
					continue
				}

				if !f.CanSet() {
					continue
				}

				f.SetInt(int64(f.Interface().(token.Pos) + offset))
			}
		}

		return true
	})
}
