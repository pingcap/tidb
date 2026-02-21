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
	p.expect(57542)

	stmt := Alloc[ast.ShowStmt](p.arena)

	switch p.peek().Tp {
	case 57373:
		// SHOW BINARY LOG STATUS
		p.next()
		if p.peek().IsKeyword("LOG") {
			p.next()
			if p.peekKeyword(57933, "STATUS") {
				p.next()
				stmt.Tp = ast.ShowBinlogStatus
				return stmt
			}
		}
		return p.showSyntaxError()

	case 57865:
		// SHOW REPLICA STATUS
		p.next()
		if p.peekKeyword(57933, "STATUS") {
			p.next()
			stmt.Tp = ast.ShowReplicaStatus
			return stmt
		}
		return p.showSyntaxError()

	case 57944:
		p.next()
		stmt.Tp = ast.ShowTables
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case 58162:
		p.next()
		isSingular := false
		if _, ok := p.accept(58167); !ok {
			p.expect(58166)
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

	case 57730:
		p.next()
		stmt.Full = true
		switch p.peek().Tp {
		case 57944:
			p.next()
			stmt.Tp = ast.ShowTables
			stmt.DBName = p.parseShowDatabaseNameOpt()
			p.parseShowLikeOrWhere(stmt)
			return stmt
		case 57653, 57722:
			p.next()
			stmt.Tp = ast.ShowColumns
			p.parseShowTableClause(stmt)
			p.parseShowLikeOrWhere(stmt)
			return stmt
		case 57845:
			p.next()
			stmt.Tp = ast.ShowProcessList
			return stmt
		default:
			return p.showSyntaxError()
		}

	case 57653, 57722:
		p.next()
		stmt.Tp = ast.ShowColumns
		p.parseShowTableClause(stmt)
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case 57389:
		p.next()
		if result := p.parseShowCreate(); result != nil {
			return result
		}
		return p.showSyntaxError()

	case 57978, 57933, 57984:
		switch p.next().Tp {
		case 57978:
			stmt.Tp = ast.ShowVariables
		case 57933:
			stmt.Tp = ast.ShowStatus
		default:
			stmt.Tp = ast.ShowWarnings
		}
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case 57733:
		p.next()
		return p.parseShowScopedStmt(stmt, true)

	case 57899:
		p.next()
		return p.parseShowScopedStmt(stmt, false)

	case 57845:
		p.next()
		stmt.Tp = ast.ShowProcessList
		return stmt

	case 57449, 57468:
		// SHOW {INDEX|KEYS|INDEXES} {FROM|IN} tbl [{FROM|IN} db] [WHERE expr]
		p.next()
		p.parseShowIndexStmt(stmt)
		return stmt

	case 57382:
		// SHOW CHARACTER SET [LIKE|WHERE]
		p.next()
		p.expect(57541)
		stmt.Tp = ast.ShowCharset
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case 57519, 57731, 57366:
		// SHOW {PROCEDURE|FUNCTION|ANALYZE} STATUS [LIKE|WHERE]
		var tp ast.ShowStmtType
		switch p.next().Tp {
		case 57519:
			tp = ast.ShowProcedureStatus
		case 57731:
			tp = ast.ShowFunctionStatus
		default:
			tp = ast.ShowAnalyzeStatus
		}
		if p.peekKeyword(57933, "STATUS") {
			p.next()
			stmt.Tp = tp
			p.parseShowLikeOrWhere(stmt)
			return stmt
		}
		return p.showSyntaxError()

	case 58153:
		return p.parseCancelStmt()
	case 58107:
		return p.parseShowTrafficStmt()

	case 57746:
		return p.parseShowImportStmt()

	case 57398, 57399:
		// SHOW DATABASES
		p.next()
		stmt.Tp = ast.ShowDatabases
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case 58054:
		p.next()
		return p.parseShowPlacement(stmt)

	case 57556:
		p.next()
		if result := p.parseShowTable(stmt); result != nil {
			return result
		}
		return p.showSyntaxError()

	case 57734:
		p.next()
		return p.parseShowGrants()

	case 57818:
		p.next()
		p.accept(57944)
		stmt.Tp = ast.ShowOpenTables
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt

	case 58142:
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

// parseShowPlacement handles: SHOW PLACEMENT [LABELS | FOR {DATABASE|TABLE|PARTITION} ...] [LIKE|WHERE]
// Caller already consumed SHOW and PLACEMENT tokens.
func (p *HandParser) parseShowPlacement(stmt *ast.ShowStmt) ast.StmtNode {
	// SHOW PLACEMENT LABELS
	if p.peek().IsKeyword("LABELS") {
		p.next()
		stmt.Tp = ast.ShowPlacementLabels
		p.parseShowLikeOrWhere(stmt)
		return stmt
	}
	stmt.Tp = ast.ShowPlacement

	if _, ok := p.accept(57431); ok {
		if _, ok := p.accept(57398); ok {
			if tok, ok := p.expect(57346); ok {
				stmt.DBName = tok.Lit
			} else {
				return nil
			}
		} else if _, ok := p.accept(57556); ok {
			stmt.Table = p.parseTableName()
			if _, ok := p.accept(57515); ok {
				if tok, ok := p.expect(57346); ok {
					stmt.Partition = ast.NewCIStr(tok.Lit)
				} else {
					return nil
				}
			}
		} else {
			p.error(p.peek().Offset, "expected DATABASE, TABLE after FOR")
			return nil
		}
		// After FOR clause, only LIKE/WHERE or statement end is valid.
		// Reject unexpected trailing tokens like "TABLE tb1" after "DATABASE db1".
		next := p.peek().Tp
		if next != ';' && next != EOF && next != 57476 && next != 57587 {
			p.error(p.peek().Offset, "unexpected token after SHOW PLACEMENT FOR clause")
			return nil
		}
	}

	p.parseShowLikeOrWhere(stmt)
	return stmt
}

// parseShowTable handles: SHOW TABLE {STATUS|t [PARTITION ...] [INDEX ...] {REGIONS|NEXT_ROW_ID|DISTRIBUTIONS}}
// Caller already consumed SHOW and TABLE tokens.
func (p *HandParser) parseShowTable(stmt *ast.ShowStmt) ast.StmtNode {
	// SHOW TABLE STATUS [FROM db] [LIKE ...]
	if p.peekKeyword(57933, "STATUS") {
		p.next()
		stmt.Tp = ast.ShowTableStatus
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	}
	table := p.parseTableName()
	stmt.Table = table

	// Optional PARTITION (p1, p2, ...)
	if _, ok := p.accept(57515); ok {
		p.expect('(')
		for {
			tok, ok := p.accept(57346)
			if !ok {
				p.error(p.peek().Offset, "expected partition name")
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
	if _, ok := p.accept(57449); ok {
		if tok, ok := p.accept(57346); ok {
			stmt.IndexName = ast.NewCIStr(tok.Lit)
		} else {
			p.error(p.peek().Offset, "expected index name")
			return nil
		}
	}

	// Check for REGIONS, NEXT_ROW_ID, or DISTRIBUTIONS
	switch p.peek().Tp {
	case 58174:
		stmt.Tp = ast.ShowRegions
	case 58051:
		stmt.Tp = ast.ShowTableNextRowId
	case 58163:
		stmt.Tp = ast.ShowDistributions
	}
	if stmt.Tp != 0 {
		p.next()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	}
	// Fallback: Check for TABLE STATUS
	if p.peekKeyword(57933, "STATUS") {
		p.next()
		stmt.Tp = ast.ShowTableStatus
		stmt.Table = nil
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	}
	p.error(p.peek().Offset, "expected REGIONS or NEXT_ROW_ID after SHOW TABLE")
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
	if _, ok := p.acceptAny(57434, 57448); ok {
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
	case 57978:
		p.next()
		stmt.Tp = ast.ShowVariables
	case 57933:
		p.next()
		stmt.Tp = ast.ShowStatus
	default:
		if p.peek().IsKeyword("BINDINGS") {
			p.next()
			stmt.Tp = ast.ShowBindings
		} else {
			return nil
		}
	}
	p.parseShowLikeOrWhere(stmt)
	return stmt
}

func (p *HandParser) parseShowLikeOrWhere(stmt *ast.ShowStmt) {
	if _, ok := p.accept(57476); ok {
		pattern := Alloc[ast.PatternLikeOrIlikeExpr](p.arena)
		pattern.Pattern = p.parseExpression(precNone)
		pattern.IsLike = true
		pattern.Escape = '\\'
		stmt.Pattern = pattern
	} else if _, ok := p.accept(57587); ok {
		stmt.Where = p.parseExpression(precNone)
	}
}
