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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ParseSQL parses one or more SQL statements and returns the AST nodes.
// Callers should first call Init() with a Lexer and source SQL string.
func (p *HandParser) ParseSQL() ([]ast.StmtNode, []error, error) {
	p.result = p.result[:0]
	p.errs = p.errs[:0]
	p.warns = p.warns[:0]

	// stmtStartPos tracks the start of the next statement's text range.
	// Starting at 0 ensures that prefixes like /*! are included in the first
	// statement's Text(). After each statement, it advances to endOff so that
	// subsequent statements include any intervening whitespace or comment markers.
	// This mirrors the Scanner.stmtStartPos behavior.
	stmtStartPos := 0

	for p.peek().Tp != EOF {
		if p.peek().Tp == ';' {
			p.next()
			// Update stmtStartPos past the semicolon so it doesn't appear
			// in the next statement's Text().
			stmtStartPos = p.peek().Offset
			continue
		}
		startOff := stmtStartPos
		stmt := p.parseStatement()
		if len(p.errs) > 0 {
			return nil, p.warns, p.errs[0]
		}
		if stmt == nil {
			// Statement parsing failed. Return accumulated errors if any.
			if len(p.errs) > 0 {
				return nil, p.warns, p.errs[0]
			}
			// No errors recorded — generate an error for the unrecognized statement.
			pk := p.peek()
			if pk.Tp != EOF {
				p.errorNear(pk.Offset+len(pk.Lit), pk.Offset)
				return nil, p.warns, p.errs[0]
			}
			break
		}
		// Set the original text of the statement from source offsets.
		// Try consuming the optional ';' BEFORE computing endOff.
		// If ';' is consumed, endOff = semicolonOffset + 1 (just past ';'),
		// matching stmtText() which uses the scanner reader position
		// right after consuming ';' (before whitespace is skipped for the next token).
		// If no ';', endOff = next token's offset.
		var endOff int
		if semiTok, ok := p.accept(';'); ok {
			endOff = semiTok.Offset + 1 // just past the semicolon
		} else {
			endOff = p.peek().Offset
		}
		if endOff > startOff {
			stmt.SetText(nil, p.src[startOff:endOff])
		}
		stmtStartPos = endOff
		p.result = append(p.result, stmt)
	}

	if len(p.errs) > 0 {
		return nil, p.warns, p.errs[0]
	}

	// Set flags on all statements.
	for _, stmt := range p.result {
		ast.SetFlag(stmt)
	}

	return p.result, p.warns, nil
}

// parseStatement dispatches to the appropriate statement parser based on the
// leading token. Returns nil for unrecognized statement types, triggering
// when the hand parser encounters an unsupported statement.
func (p *HandParser) parseStatement() ast.StmtNode {
	switch p.peek().Tp {
	// --- DML ---
	case 57540:
		if s := p.parseSelectStmt(); s != nil {
			return p.maybeParseUnion(s).(ast.StmtNode)
		}
		return nil

	case '(':
		// Parenthesized SELECT/TABLE/VALUES at statement level.
		// Delegate to parseSubquery which handles nested parens, WITH, etc.
		if res := p.parseSubquery(); res != nil {
			// parseSubquery returns ResultSetNode. maybeParseUnion handles union.
			// Result is ResultSetNode (SelectStmt or SetOprStmt), which are StmtNodes.
			unionRes := p.maybeParseUnion(res)
			if unionRes == nil {
				return nil
			}
			return unionRes.(ast.StmtNode)
		}
		return nil

	case 57453:
		if s := p.parseInsertStmt(false); s != nil {
			return s
		}
		return nil

	case 57530:
		if s := p.parseInsertStmt(true); s != nil {
			return s
		}
		return nil

	case 57573:
		if s := p.parseUpdateStmt(); s != nil {
			return s
		}
		return nil

	case 57407:
		if s := p.parseDeleteStmt(); s != nil {
			return s
		}
		return nil

	case 57480:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseLoadStatsStmt()
		}
		return p.parseLoadDataStmt()
	case 57556, 57580:
		var s ast.StmtNode
		if p.peek().Tp == 57556 {
			s = p.parseTableStmt()
		} else {
			s = p.parseValuesStmt()
		}
		if s != nil {
			if rs, ok := s.(ast.ResultSetNode); ok {
				return p.maybeParseUnion(rs).(ast.StmtNode)
			}
			return s
		}
		return nil

	case 57437:
		return p.parseGrantStmt()

	case 57533:
		return p.parseRevokeStmt()

	case 57626:
		return p.parseBinlogStmt()

	case 58180:
		return p.parseSplitRegionStmt()
	case 58161:
		return p.parseDistributeTableStmt()

	// --- Transaction control ---
	case 57621, 57925:
		return p.parseBeginStmt()
	case 57656:
		return p.parseCommitStmt()
	case 57878:
		return p.parseRollbackStmt()
	case 57886:
		return p.parseSavepointStmt()
	case 57527:
		return p.parseReleaseSavepointStmt()

	// --- Trivial admin ---
	case 57575:
		return p.parseUseStmt()
	case 57693:
		return p.parseDoStmt()
	case 57570:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseUnlockStatsStmt()
		}
		return p.parseUnlockTablesStmt()
	case 57469:
		return p.parseKillStmt()
	case 57726:
		return p.parseFlushStmt()
	case 57856:
		return p.parseRecommendStmt()
	case 57958:
		return p.parseTraceStmt()
	case 57904:
		return p.parseShutdownStmt()
	case 57871:
		return p.parseRestartStmt()
	case 57737:
		return p.parseHelpStmt()

	// --- SET / SHOW ---
	case 57541:
		return p.speculate(p.parseSetStmt)
	case 57542:
		return p.parseShowStmt()

	// --- DDL ---
	case 57389:
		return p.speculate(p.parseCreateStmt)
	case 57365:
		return p.parseAlterStmt()
	case 57415:
		return p.speculate(p.parseDropStmt)
	case 57963:
		return p.speculate(p.parseTruncateTableStmt)
	case 57528:
		return p.parseRenameStmt()
	case 57366:
		return p.speculate(p.parseAnalyzeTableStmt)
	case 58021:
		return p.speculate(p.parseFlashbackStmt)

	// --- Admin / Utility ---
	case 57506:
		return p.parseOptimizeTableStmt()
	case 58056:
		return p.parsePlanReplayerStmt() // PLAN REPLAYER
	case 57852:
		return p.parseQueryWatchStmt() // QUERY WATCH

	// --- EXPLAIN / DESCRIBE ---
	case 57424, 57410, 57409:
		return p.speculate(p.parseExplainStmt)

	// --- LOCK TABLES ---
	case 57483:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseLockStatsStmt()
		}
		return p.parseLockTablesStmt()

	// --- Admin helpers ---
	case 58122:
		return p.speculate(p.parseAdminStmt)
	case 57377:
		return p.speculate(p.parseCallStmt)

	// --- IMPORT / BRIE / TRAFFIC / REFRESH ---
	case 57746:
		return p.parseImportIntoStmt()
	case 58153:
		return p.parseCancelStmt()
	case 57618, 57872:
		return p.parseBRIEStmt()
	case 58107:
		return p.parseTrafficStmt()
	case 57831:
		p.next() // consume PAUSE
		return p.parsePauseBackupLogsStmt()
	case 57874:
		p.next() // consume RESUME
		return p.parseResumeBackupLogsStmt()
	case 58082:
		p.next() // consume STOP
		return p.parseStopBackupLogsStmt()
	case 57849:
		p.next() // consume PURGE
		return p.parsePurgeBackupLogsStmt()
	case 58123:
		return p.parseNonTransactionalDMLStmt()
	case 57859:
		return p.parseRefreshStmt()

	case 57590:
		return p.parseWithStmt()
	case 57634:
		return p.parseCalibrateResourceStmt()
	case 57346:
		switch strings.ToUpper(p.peek().Lit) {
		case "SLOW":
			return p.parseSlowQueryStmt()
		case "SPLIT":
			return p.parseSplitRegionStmt()
		case "TRACE":
			return p.parseTraceStmt()
		case "RECOMMEND":
			return p.parseRecommendStmt()
		case "FLUSH":
			return p.parseFlushStmt()
		case "KILL":
			return p.parseKillStmt()
		default:
			// Fallback to error for other identifiers — use yacc-compatible format.
			tok := p.peek()
			p.errorNear(tok.Offset+len(tok.Lit), tok.Offset)
			return nil
		}

	// --- PREPARE / EXECUTE / DEALLOCATE ---
	case 57840:
		return p.parsePrepareStmt()
	case 57715:
		return p.parseExecuteStmt()
	case 57683:
		return p.parseDeallocateStmt()
	case 57857:
		return p.parseRecoverTableStmt()

	default:
		// Check for SLOW keyword (might be returned as tokSlow if keyword, or identifier)
		// Since we don't have tokSlow alias, and it might be a keyword token, check Literal.
		if p.peek().IsKeyword("SLOW") {
			return p.parseSlowQueryStmt()
		}

		// No fallback — the hand parser is the sole parser.
		tok := p.peek()
		p.errorNear(tok.Offset+len(tok.Lit), tok.Offset)
		return nil
	}
}

// parseCreateStmt dispatches CREATE TABLE / INDEX / SEQUENCE / PLACEMENT POLICY / DATABASE / RESOURCE GROUP.
func (p *HandParser) parseCreateStmt() ast.StmtNode {
	// Peek past CREATE to see what follows.
	mark := p.lexer.Mark()
	p.next() // consume CREATE

	// [UNIQUE | FULLTEXT | SPATIAL]
	var indexType int
	switch p.peek().Tp {
	case 57569:
		p.next()
		indexType = 1
	case 57435:
		p.next()
		indexType = 2
	case 57544:
		p.next()
		indexType = 3
	case 57979:
		p.next()
		indexType = 4
	case 57652:
		p.next()
		indexType = 5
	}

	switch p.peek().Tp {
	case 57449, 57467:
		p.lexer.Restore(mark)
		return p.parseCreateIndexStmt()
	case 57896:
		p.lexer.Restore(mark)
		return p.parseCreateSequenceStmt()
	case 58054:
		p.lexer.Restore(mark)
		return p.parseCreatePlacementPolicyStmt()
	case 57509:
		// CREATE OR REPLACE ... — peek past OR REPLACE to disambiguate.
		mark2 := p.lexer.Mark()
		p.next() // consume OR
		p.next() // consume REPLACE
		nextTp := p.peek().Tp
		p.lexer.Restore(mark2) // restore back to OR

		switch nextTp {
		case 57980, 57603, 57685, 57545:
			p.lexer.Restore(mark)
			return p.parseCreateViewStmt()
		}

		p.lexer.Restore(mark)
		return p.parseCreatePlacementPolicyStmt()
	case 57398: // 57398 == 57398
		p.lexer.Restore(mark)
		return p.parseCreateDatabaseStmt()
	case 57869:
		p.lexer.Restore(mark)
		return p.parseCreateResourceGroupStmt()
	case 57519:
		p.lexer.Restore(mark)
		return p.parseCreateProcedureStmt()
	case 57980:
		p.lexer.Restore(mark)
		return p.parseCreateViewStmt()
	case 57556:
		_ = indexType
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case 57947:
		_ = indexType
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case 57975:
		p.lexer.Restore(mark)
		return p.parseCreateUserStmt()
	case 57877:
		p.lexer.Restore(mark)
		return p.parseCreateUserStmt()
	case 57603, 57685, 57545:
		// CREATE ALGORITHM = ... VIEW, CREATE DEFINER = ... VIEW, CREATE SQL SECURITY ... VIEW
		p.lexer.Restore(mark)
		return p.parseCreateViewStmt()
	case 57733:
		// CREATE GLOBAL BINDING ...
		p.next() // consume GLOBAL
		if p.peek().Tp == 57623 {
			p.next() // consume BINDING
			return p.parseCreateBindingStmt(true)
		}
		// CREATE GLOBAL TEMPORARY TABLE ...
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case 57899:
		// CREATE SESSION BINDING ...
		p.next() // consume SESSION
		if p.peek().Tp == 57623 {
			p.next() // consume BINDING
			return p.parseCreateBindingStmt(false)
		}
		p.lexer.Restore(mark)
		return nil
	case 57623:
		// CREATE BINDING (default session scope)
		p.next() // consume BINDING
		return p.parseCreateBindingStmt(false)
	case 58181:
		return p.parseCreateStatisticsStmt()
	default:
		// Check identifier-based keywords
		if p.peek().IsIdent() {
			switch strings.ToUpper(p.peek().Lit) {
			case "STATISTICS":
				return p.parseCreateStatisticsStmt()
			case "BINDING":
				// CREATE BINDING (default session scope)
				p.next() // consume BINDING
				return p.parseCreateBindingStmt(false)
			}
		}
		// For unrecognized CREATE statements, fall back.
		_ = indexType
		p.lexer.Restore(mark)
		return nil
	}
}

// parseAlterStmt dispatches ALTER TABLE / DATABASE / PLACEMENT POLICY.
func (p *HandParser) parseAlterStmt() ast.StmtNode {
	mark := p.lexer.Mark()
	p.next() // consume ALTER

	switch p.peek().Tp {
	case 57398: // 57398 == 57398
		p.lexer.Restore(mark)
		return p.parseAlterDatabaseStmt()
	case 58054:
		p.lexer.Restore(mark)
		return p.parseAlterPlacementPolicyStmt()
	case 57896:
		p.lexer.Restore(mark)
		return p.parseAlterSequenceStmt()
	case 57869:
		p.lexer.Restore(mark)
		return p.parseAlterResourceGroupStmt()
	case 57752:
		p.lexer.Restore(mark)
		return p.parseAlterInstanceStmt()
	case 57520:
		p.lexer.Restore(mark)
		return p.parseAlterRangeStmt()
	case 57975:
		p.lexer.Restore(mark)
		return p.parseAlterUserStmt()
	default:
		p.lexer.Restore(mark)
		return p.parseAlterTableStmt()
	}
}

// parseRenameStmt dispatches RENAME TABLE / USER.
func (p *HandParser) parseRenameStmt() ast.StmtNode {
	mark := p.lexer.Mark()
	p.next() // consume RENAME

	switch p.peek().Tp {
	case 57975:
		p.lexer.Restore(mark)
		return p.parseRenameUserStmt()
	default:
		p.lexer.Restore(mark)
		return p.speculate(p.parseRenameTableStmt)
	}
}
