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

// binding_parser.go handles parsing of SQL binding statements:
//   - CREATE [GLOBAL|SESSION] BINDING
//   - DROP [GLOBAL|SESSION] BINDING
//   - SET BINDING ENABLED/DISABLED
//   - SHOW GRANTS (historically co-located with binding code)

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseCreateBindingStmt parses:
//
//	CREATE [GLOBAL | SESSION] BINDING FOR select_stmt USING select_stmt
//	CREATE [GLOBAL | SESSION] BINDING FROM HISTORY USING PLAN DIGEST ...
//	CREATE [GLOBAL | SESSION] BINDING USING select_stmt  (wildcard, no FOR)
//
// Called after "CREATE" consumed, scope identifier processed, BINDING consumed.
func (p *HandParser) parseCreateBindingStmt(globalScope bool) ast.StmtNode {
	stmt := Alloc[ast.CreateBindingStmt](p.arena)
	stmt.GlobalScope = globalScope

	if _, ok := p.accept(forKwd); ok {
		// FOR select_stmt USING select_stmt
		stmt.OriginNode = p.parseAndSetText()

		p.expect(using)
		stmt.HintedNode = p.parseAndSetText()
	} else if _, ok := p.accept(from); ok {
		// FROM HISTORY USING PLAN DIGEST 'digest1', 'digest2', ...
		p.next() // HISTORY
		p.expect(using)
		// PLAN keyword
		if p.peek().IsKeyword("PLAN") {
			p.next()
		}
		// DIGEST keyword
		if p.peek().IsKeyword("DIGEST") {
			p.next()
		}
		stmt.PlanDigests = p.parseStringOrUserVarList()
	} else if _, ok := p.accept(using); ok {
		// USING select_stmt (wildcard binding — no FOR)
		hintedStmt := p.parseAndSetText()
		stmt.HintedNode = hintedStmt
		stmt.OriginNode = hintedStmt
	}

	return stmt
}

// parseDropBindingStmt parses: DROP [GLOBAL | SESSION] BINDING
// [FOR select_stmt [USING select_stmt] | FOR SQL DIGEST str]
func (p *HandParser) parseDropBindingStmt(globalScope bool) ast.StmtNode {
	stmt := Alloc[ast.DropBindingStmt](p.arena)
	stmt.GlobalScope = globalScope

	if _, ok := p.accept(forKwd); ok {
		if p.peekKeyword(sql, "SQL") {
			// FOR SQL DIGEST 'str'
			p.next() // SQL
			p.next() // DIGEST
			stmt.SQLDigests = p.parseStringOrUserVarList()
		} else {
			stmt.OriginNode = p.parseAndSetText()
			if _, ok := p.accept(using); ok {
				stmt.HintedNode = p.parseAndSetText()
			}
		}
	}

	return stmt
}

// parseShowGrants parses: SHOW GRANTS [FOR user [USING role, ...]]
func (p *HandParser) parseShowGrants() ast.StmtNode {
	stmt := p.arena.AllocShowStmt()
	stmt.Tp = ast.ShowGrants

	if _, ok := p.accept(forKwd); ok {
		stmt.User = p.parseUserIdentity()

		// USING role, ...
		if _, ok := p.accept(using); ok {
			for {
				role := p.parseRoleIdentity()
				if role == nil {
					break
				}
				stmt.Roles = append(stmt.Roles, role)
				if _, ok := p.accept(','); !ok {
					break
				}
			}
		}
	}

	return stmt
}

// parseAndSetText parses a statement and sets its text from the source.
// Used by binding statement parsing to avoid repeating the offset-capture pattern.
func (p *HandParser) parseAndSetText() ast.StmtNode {
	startOff := p.peek().Offset
	stmt := p.parseStatement()
	if stmt != nil {
		endOff := len(p.lexer.src)
		if p.peek().Tp != EOF {
			endOff = p.peek().Offset
		}
		if endOff > startOff {
			stmt.SetText(nil, strings.TrimRight(p.lexer.src[startOff:endOff], "; \t\n"))
		}
	}
	return stmt
}

// parseCreateStatisticsStmt parses: CREATE STATISTICS stats_name (type) ON tbl(cols...)
// Called after "CREATE" has been peeked and "STATISTICS" is the next identifier.
func (p *HandParser) parseCreateStatisticsStmt() ast.StmtNode {
	p.next() // consume STATISTICS

	stmt := Alloc[ast.CreateStatisticsStmt](p.arena)

	// Optional IF NOT EXISTS
	stmt.IfNotExists = p.acceptIfNotExists()

	stmt.StatsName = p.next().Lit

	// (type)
	p.expect('(')
	typeTok := p.next()
	switch strings.ToUpper(typeTok.Lit) {
	case "CARDINALITY":
		stmt.StatsType = ast.StatsTypeCardinality
	case "DEPENDENCY":
		stmt.StatsType = ast.StatsTypeDependency
	case "CORRELATION":
		stmt.StatsType = ast.StatsTypeCorrelation
	}
	p.expect(')')

	// ON tbl(cols)
	p.expect(on)
	stmt.Table = p.parseTableName()
	p.expect('(')
	for {
		col := p.parseColumnName()
		if col == nil {
			break
		}
		stmt.Columns = append(stmt.Columns, col)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	p.expect(')')

	return stmt
}

// parseDropStatisticsStmt parses: DROP STATISTICS stats_name
func (p *HandParser) parseDropStatisticsStmt() ast.StmtNode {
	p.next() // consume STATISTICS

	stmt := Alloc[ast.DropStatisticsStmt](p.arena)
	stmt.StatsName = p.next().Lit
	return stmt
}

// parseLoadStatsStmt parses: LOAD STATS 'path'
func (p *HandParser) parseLoadStatsStmt() ast.StmtNode {
	p.expect(load)
	// Skip STATS keyword (identifier)
	p.next()
	stmt := Alloc[ast.LoadStatsStmt](p.arena)
	tok, ok := p.expectAny(stringLit)
	if !ok {
		return nil
	}
	stmt.Path = tok.Lit
	return stmt
}

// parseLockStatsStmt parses: LOCK STATS table [, table ...] [PARTITION (p0, p1, ...)]
func (p *HandParser) parseLockStatsStmt() ast.StmtNode {
	p.expect(lock)
	p.next() // consume STATS
	stmt := Alloc[ast.LockStatsStmt](p.arena)
	stmt.Tables = p.parseStatsTablesAndPartitions()
	return stmt
}

// parseUnlockStatsStmt parses: UNLOCK STATS table [, table ...] [PARTITION (p0, p1, ...)]
func (p *HandParser) parseUnlockStatsStmt() ast.StmtNode {
	p.expect(unlock)
	p.next() // consume STATS
	stmt := Alloc[ast.UnlockStatsStmt](p.arena)
	stmt.Tables = p.parseStatsTablesAndPartitions()
	return stmt
}
