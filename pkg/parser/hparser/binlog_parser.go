package hparser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseBinlogStmt parses BINLOG 'str'
func (p *HandParser) parseBinlogStmt() *ast.BinlogStmt {
	p.expect(tokBinlog)
	stmt := Alloc[ast.BinlogStmt](p.arena)
	tok := p.next()
	if tok.Tp != tokStringLit {
		p.error(tok.Offset, "expected string literal after BINLOG")
		return nil
	}
	stmt.Str = tok.Lit
	return stmt
}
