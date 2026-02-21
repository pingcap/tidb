package parser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseBinlogStmt parses BINLOG 'str'
func (p *HandParser) parseBinlogStmt() *ast.BinlogStmt {
	p.expect(binlog)
	stmt := Alloc[ast.BinlogStmt](p.arena)
	tok := p.next()
	if tok.Tp != stringLit {
		p.error(tok.Offset, "expected string literal after BINLOG")
		return nil
	}
	stmt.Str = tok.Lit
	return stmt
}
