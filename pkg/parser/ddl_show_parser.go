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
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ---------------------------------------------------------------------------
// SHOW statement
// ---------------------------------------------------------------------------

// parseShowStmt parses SHOW variants.
func (p *HandParser) parseShowStmt() ast.StmtNode {
	p.expect(show)

	stmt := p.arena.AllocShowStmt()

	switch p.peek().Tp {
	case binaryType:
		// SHOW BINARY LOG STATUS
		p.next()
		if p.peek().IsKeyword("LOG") {
			p.next()
			if p.peekKeyword(status, "STATUS") {
				p.next()
				stmt.Tp = ast.ShowBinlogStatus
				return stmt
			}
		}
		return p.showSyntaxError()

	case replica:
		// SHOW REPLICA STATUS
		p.next()
		if p.peekKeyword(status, "STATUS") {
			p.next()
			stmt.Tp = ast.ShowReplicaStatus
			return stmt
		}
		return p.showSyntaxError()

	case tables:
		p.next()
		stmt.Tp = ast.ShowTables
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case distribution:
		p.next()
		isSingular := false
		if _, ok := p.accept(jobs); !ok {
			p.expect(job)
			isSingular = true
		}
		stmt.Tp = ast.ShowDistributionJobs

		if isSingular {
			val := p.parseExpression(precNone)
			if val != nil {
				if v, ok := val.(ast.ValueExpr); ok {
					if i, ok := v.GetValue().(int64); ok {
						stmt.DistributionJobID = &i
					}
				}
			}
		}

		if stmt.DistributionJobID == nil {
			p.parseShowLikeOrWhere(stmt)
		}
		return stmt

	case full:
		p.next()
		stmt.Full = true
		switch p.peek().Tp {
		case tables:
			p.next()
			stmt.Tp = ast.ShowTables
			stmt.DBName = p.parseShowDatabaseNameOpt()
			p.parseShowLikeOrWhere(stmt)
			return stmt
		case columns, fields:
			p.next()
			stmt.Tp = ast.ShowColumns
			p.parseShowTableClause(stmt)
			p.parseShowLikeOrWhere(stmt)
			return stmt
		case processlist:
			p.next()
			stmt.Tp = ast.ShowProcessList
			return stmt
		default:
			return p.showSyntaxError()
		}

	case columns, fields:
		p.next()
		stmt.Tp = ast.ShowColumns
		p.parseShowTableClause(stmt)
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case create:
		p.next()
		if result := p.parseShowCreate(); result != nil {
			return result
		}
		return p.showSyntaxError()

	case variables, status, warnings:
		switch p.next().Tp {
		case variables:
			stmt.Tp = ast.ShowVariables
		case status:
			stmt.Tp = ast.ShowStatus
		default:
			stmt.Tp = ast.ShowWarnings
		}
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case global:
		p.next()
		return p.parseShowScopedStmt(stmt, true)

	case session:
		p.next()
		return p.parseShowScopedStmt(stmt, false)

	case processlist:
		p.next()
		stmt.Tp = ast.ShowProcessList
		return stmt

	case index, keys:
		// SHOW {INDEX|KEYS|INDEXES} {FROM|IN} tbl [{FROM|IN} db] [WHERE expr]
		p.next()
		p.parseShowIndexStmt(stmt)
		return stmt

	case character:
		// SHOW CHARACTER SET [LIKE|WHERE]
		p.next()
		p.expect(set)
		stmt.Tp = ast.ShowCharset
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case procedure, function, analyze:
		// SHOW {PROCEDURE|FUNCTION|ANALYZE} STATUS [LIKE|WHERE]
		var tp ast.ShowStmtType
		switch p.next().Tp {
		case procedure:
			tp = ast.ShowProcedureStatus
		case function:
			tp = ast.ShowFunctionStatus
		default:
			tp = ast.ShowAnalyzeStatus
		}
		if p.peekKeyword(status, "STATUS") {
			p.next()
			stmt.Tp = tp
			p.parseShowLikeOrWhere(stmt)
			return stmt
		}
		return p.showSyntaxError()

	case cancel:
		return p.parseCancelStmt()
	case traffic:
		return p.parseShowTrafficStmt()

	case importKwd:
		return p.parseShowImportStmt()

	case database, databases:
		// SHOW DATABASES
		p.next()
		stmt.Tp = ast.ShowDatabases
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case placement:
		p.next()
		return p.parseShowPlacement(stmt)

	case tableKwd:
		p.next()
		if result := p.parseShowTable(stmt); result != nil {
			return result
		}
		return p.showSyntaxError()

	case grants:
		p.next()
		return p.parseShowGrants()

	case open:
		p.next()
		p.accept(tables)
		stmt.Tp = ast.ShowOpenTables
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case builtins:
		p.next()
		stmt.Tp = ast.ShowBuiltins
		return stmt

	default:
		if result := p.parseShowIdentBased(stmt); result != nil {
			return result
		}
		return p.showSyntaxError()
	}
}

// showSyntaxError generates a syntax error at the current position and returns nil.
// Used when SHOW has been consumed but the following tokens don't form a valid SHOW statement.
func (p *HandParser) showSyntaxError() ast.StmtNode {
	pk := p.peek()
	p.errorNear(pk.Offset+len(pk.Lit), pk.Offset)
	return nil
}

// parseShowPlacement handles: SHOW PLACEMENT [LABELS | FOR {DATABASE|TABLE} ...] [LIKE|WHERE]
// parser.y grammar (ShowPlacementTarget):
//
//	DATABASE db     → ShowPlacementForDatabase
//	TABLE tbl       → ShowPlacementForTable
//	TABLE tbl PARTITION p → ShowPlacementForPartition
//
// Caller already consumed SHOW and PLACEMENT tokens.
func (p *HandParser) parseShowPlacement(stmt *ast.ShowStmt) ast.StmtNode {
	// SHOW PLACEMENT LABELS
	if p.peek().IsKeyword("LABELS") {
		p.next()
		stmt.Tp = ast.ShowPlacementLabels
		p.parseShowLikeOrWhere(stmt)
		return stmt
	}

	// SHOW PLACEMENT FOR {DATABASE db | TABLE tbl [PARTITION p]}
	if _, ok := p.accept(forKwd); ok {
		if _, ok := p.accept(database); ok {
			// SHOW PLACEMENT FOR DATABASE db
			tok, ok := p.expect(identifier)
			if !ok {
				return nil
			}
			stmt.Tp = ast.ShowPlacementForDatabase
			stmt.DBName = tok.Lit
		} else if _, ok := p.accept(tableKwd); ok {
			stmt.Table = p.parseTableName()
			if _, ok := p.accept(partition); ok {
				// SHOW PLACEMENT FOR TABLE tbl PARTITION p
				tok, ok := p.expect(identifier)
				if !ok {
					return nil
				}
				stmt.Tp = ast.ShowPlacementForPartition
				stmt.Partition = ast.NewCIStr(tok.Lit)
			} else {
				// SHOW PLACEMENT FOR TABLE tbl
				stmt.Tp = ast.ShowPlacementForTable
			}
		} else {
			p.syntaxErrorAt(p.peek())
			return nil
		}
		// After FOR clause, only LIKE/WHERE or statement end is valid.
		next := p.peek().Tp
		if next != ';' && next != EOF && next != like && next != where {
			p.syntaxErrorAt(p.peek())
			return nil
		}
	} else {
		// SHOW PLACEMENT (no FOR clause)
		stmt.Tp = ast.ShowPlacement
	}

	p.parseShowLikeOrWhere(stmt)
	return stmt
}

// parseShowTable handles: SHOW TABLE {STATUS|t [PARTITION ...] [INDEX ...] {REGIONS|NEXT_ROW_ID|DISTRIBUTIONS}}
// Caller already consumed SHOW and TABLE tokens.
func (p *HandParser) parseShowTable(stmt *ast.ShowStmt) ast.StmtNode {
	// SHOW TABLE STATUS [FROM db] [LIKE ...]
	if p.peekKeyword(status, "STATUS") {
		p.next()
		stmt.Tp = ast.ShowTableStatus
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	}
	table := p.parseTableName()
	stmt.Table = table

	// Optional PARTITION (p1, p2, ...)
	if _, ok := p.accept(partition); ok {
		p.expect('(')
		for {
			tok, ok := p.accept(identifier)
			if !ok {
				p.syntaxErrorAt(p.peek())
				return nil
			}
			table.PartitionNames = append(table.PartitionNames, ast.NewCIStr(tok.Lit))

			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}

	// Optional INDEX idx
	if _, ok := p.accept(index); ok {
		tok, ok := p.accept(identifier)
		if !ok {
			p.syntaxErrorAt(p.peek())
			return nil
		}
		stmt.IndexName = ast.NewCIStr(tok.Lit)
	}

	// Check for REGIONS, NEXT_ROW_ID, or DISTRIBUTIONS
	switch p.peek().Tp {
	case regions:
		stmt.Tp = ast.ShowRegions
	case next_row_id:
		stmt.Tp = ast.ShowTableNextRowId
	case distributions:
		stmt.Tp = ast.ShowDistributions
	}
	if stmt.Tp != 0 {
		p.next()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	}
	// Fallback: Check for TABLE STATUS
	if p.peekKeyword(status, "STATUS") {
		p.next()
		stmt.Tp = ast.ShowTableStatus
		stmt.Table = nil
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	}
	p.syntaxErrorAt(p.peek())
	return nil
}

func (p *HandParser) parseShowDatabaseNameOpt() string {
	if name, ok := p.acceptFromOrIn(); ok {
		return name
	}
	return ""
}

// parseShowTableClause parses: FROM|IN tablename [FROM|IN dbname]
func (p *HandParser) parseShowTableClause(stmt *ast.ShowStmt) {
	if _, ok := p.acceptAny(from, in); ok {
		stmt.Table = p.parseTableName()
		stmt.DBName = p.parseShowDatabaseNameOpt()
	}
}

// parseShowIndexStmt parses the common body of SHOW {INDEX|KEYS|INDEXES}:
// {FROM|IN} tbl [{FROM|IN} db] [WHERE expr]
func (p *HandParser) parseShowIndexStmt(stmt *ast.ShowStmt) {
	stmt.Tp = ast.ShowIndex
	p.expectFromOrIn()
	stmt.Table = p.parseTableName()
	if dbName, ok := p.acceptFromOrIn(); ok {
		stmt.Table.Schema = ast.NewCIStr(dbName)
	}
	p.parseShowLikeOrWhere(stmt)
}

// parseShowLikeOrWhere parses optional: [LIKE 'pat' | WHERE expr]
// parseShowScopedStmt handles SHOW {GLOBAL|SESSION} {VARIABLES|STATUS|BINDINGS} [LIKE|WHERE].
func (p *HandParser) parseShowScopedStmt(stmt *ast.ShowStmt, isGlobal bool) ast.StmtNode {
	stmt.GlobalScope = isGlobal
	switch p.peek().Tp {
	case variables:
		p.next()
		stmt.Tp = ast.ShowVariables
	case status:
		p.next()
		stmt.Tp = ast.ShowStatus
	default:
		if !p.peek().IsKeyword("BINDINGS") {
			return nil
		}
		p.next()
		stmt.Tp = ast.ShowBindings
	}
	p.parseShowLikeOrWhere(stmt)
	return stmt
}

func (p *HandParser) parseShowLikeOrWhere(stmt *ast.ShowStmt) {
	if _, ok := p.accept(like); ok {
		pattern := Alloc[ast.PatternLikeOrIlikeExpr](p.arena)
		pattern.Pattern = p.parseExpression(precNone)
		pattern.IsLike = true
		pattern.Escape = '\\'
		stmt.Pattern = pattern
	} else if _, ok := p.accept(where); ok {
		stmt.Where = p.parseExpression(precNone)
	}
}
