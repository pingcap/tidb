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
			stmt.SetText(p.connectionEncoding, p.src[startOff:endOff])
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
	case selectKwd:
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

	case insert:
		if s := p.parseInsertStmt(false); s != nil {
			return s
		}
		return nil

	case replace:
		if s := p.parseInsertStmt(true); s != nil {
			return s
		}
		return nil

	case update:
		if s := p.parseUpdateStmt(); s != nil {
			return s
		}
		return nil

	case deleteKwd:
		if s := p.parseDeleteStmt(); s != nil {
			return s
		}
		return nil

	case load:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseLoadStatsStmt()
		}
		return p.parseLoadDataStmt()
	case tableKwd, values:
		var s ast.StmtNode
		if p.peek().Tp == tableKwd {
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

	case grant:
		return p.parseGrantStmt()

	case revoke:
		return p.parseRevokeStmt()

	case binlog:
		return p.parseBinlogStmt()

	case split:
		return p.parseSplitRegionStmt()
	case distribute:
		return p.parseDistributeTableStmt()

	// --- Transaction control ---
	case begin, start:
		return p.parseBeginStmt()
	case commit:
		return p.parseCommitStmt()
	case rollback:
		return p.parseRollbackStmt()
	case savepoint:
		return p.parseSavepointStmt()
	case release:
		return p.parseReleaseSavepointStmt()

	// --- Trivial admin ---
	case use:
		return p.parseUseStmt()
	case do:
		return p.parseDoStmt()
	case unlock:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseUnlockStatsStmt()
		}
		return p.parseUnlockTablesStmt()
	case kill:
		return p.parseKillStmt()
	case flush:
		return p.parseFlushStmt()
	case recommend:
		return p.parseRecommendStmt()
	case trace:
		return p.parseTraceStmt()
	case shutdown:
		return p.parseShutdownStmt()
	case restart:
		return p.parseRestartStmt()
	case help:
		return p.parseHelpStmt()

	// --- SET / SHOW ---
	case set:
		return p.speculate(p.parseSetStmt)
	case show:
		return p.parseShowStmt()

	// --- DDL ---
	case create:
		return p.speculate(p.parseCreateStmt)
	case alter:
		return p.parseAlterStmt()
	case drop:
		return p.speculate(p.parseDropStmt)
	case truncate:
		return p.speculate(p.parseTruncateTableStmt)
	case rename:
		return p.parseRenameStmt()
	case analyze:
		return p.speculate(p.parseAnalyzeTableStmt)
	case flashback:
		return p.speculate(p.parseFlashbackStmt)

	// --- Admin / Utility ---
	case optimize:
		return p.parseOptimizeTableStmt()
	case plan:
		return p.parsePlanReplayerStmt() // PLAN REPLAYER
	case query:
		return p.parseQueryWatchStmt() // QUERY WATCH

	// --- EXPLAIN / DESCRIBE ---
	case explain, describe, desc:
		return p.speculate(p.parseExplainStmt)

	// --- LOCK TABLES ---
	case lock:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseLockStatsStmt()
		}
		return p.parseLockTablesStmt()

	// --- Admin helpers ---
	case admin:
		return p.speculate(p.parseAdminStmt)
	case call:
		return p.speculate(p.parseCallStmt)

	// --- IMPORT / BRIE / TRAFFIC / REFRESH ---
	case importKwd:
		return p.parseImportIntoStmt()
	case cancel:
		return p.parseCancelStmt()
	case backup, restore:
		return p.parseBRIEStmt()
	case traffic:
		return p.parseTrafficStmt()
	case pause:
		p.next() // consume PAUSE
		return p.parsePauseBackupLogsStmt()
	case resume:
		p.next() // consume RESUME
		return p.parseResumeBackupLogsStmt()
	case stop:
		p.next() // consume STOP
		return p.parseStopBackupLogsStmt()
	case purge:
		p.next() // consume PURGE
		return p.parsePurgeBackupLogsStmt()
	case batch:
		return p.parseNonTransactionalDMLStmt()
	case refresh:
		return p.parseRefreshStmt()

	case with:
		return p.parseWithStmt()
	case calibrate:
		return p.parseCalibrateResourceStmt()
	case identifier:
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
	case prepare:
		return p.parsePrepareStmt()
	case execute:
		return p.parseExecuteStmt()
	case deallocate:
		return p.parseDeallocateStmt()
	case recover:
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
	case unique:
		p.next()
		indexType = 1
	case fulltext:
		p.next()
		indexType = 2
	case spatial:
		p.next()
		indexType = 3
	case vectorType:
		p.next()
		indexType = 4
	case columnar:
		p.next()
		indexType = 5
	}

	switch p.peek().Tp {
	case index, key:
		p.lexer.Restore(mark)
		return p.parseCreateIndexStmt()
	case sequence:
		p.lexer.Restore(mark)
		return p.parseCreateSequenceStmt()
	case placement:
		p.lexer.Restore(mark)
		return p.parseCreatePlacementPolicyStmt()
	case or:
		// CREATE OR REPLACE ... — peek past OR REPLACE to disambiguate.
		mark2 := p.lexer.Mark()
		p.next() // consume OR
		p.next() // consume REPLACE
		nextTp := p.peek().Tp
		p.lexer.Restore(mark2) // restore back to OR

		switch nextTp {
		case view, algorithm, definer, sql:
			p.lexer.Restore(mark)
			return p.parseCreateViewStmt()
		}

		p.lexer.Restore(mark)
		return p.parseCreatePlacementPolicyStmt()
	case database: // database == database
		p.lexer.Restore(mark)
		return p.parseCreateDatabaseStmt()
	case resource:
		p.lexer.Restore(mark)
		return p.parseCreateResourceGroupStmt()
	case procedure:
		p.lexer.Restore(mark)
		return p.parseCreateProcedureStmt()
	case view:
		p.lexer.Restore(mark)
		return p.parseCreateViewStmt()
	case tableKwd:
		_ = indexType
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case temporary:
		_ = indexType
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case user:
		p.lexer.Restore(mark)
		return p.parseCreateUserStmt()
	case role:
		p.lexer.Restore(mark)
		return p.parseCreateUserStmt()
	case algorithm, definer, sql:
		// CREATE ALGORITHM = ... VIEW, CREATE DEFINER = ... VIEW, CREATE SQL SECURITY ... VIEW
		p.lexer.Restore(mark)
		return p.parseCreateViewStmt()
	case global:
		// CREATE GLOBAL BINDING ...
		p.next() // consume GLOBAL
		if p.peek().Tp == binding {
			p.next() // consume BINDING
			return p.parseCreateBindingStmt(true)
		}
		// CREATE GLOBAL TEMPORARY TABLE ...
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case session:
		// CREATE SESSION BINDING ...
		p.next() // consume SESSION
		if p.peek().Tp == binding {
			p.next() // consume BINDING
			return p.parseCreateBindingStmt(false)
		}
		p.lexer.Restore(mark)
		return nil
	case binding:
		// CREATE BINDING (default session scope)
		p.next() // consume BINDING
		return p.parseCreateBindingStmt(false)
	case statistics:
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
		// Report syntax error at the unrecognized token (not at CREATE).
		tok := p.peek()
		p.syntaxErrorAt(tok)
		return nil
	}
}

// parseAlterStmt dispatches ALTER TABLE / DATABASE / PLACEMENT POLICY.
func (p *HandParser) parseAlterStmt() ast.StmtNode {
	mark := p.lexer.Mark()
	p.next() // consume ALTER

	switch p.peek().Tp {
	case database: // database == database
		p.lexer.Restore(mark)
		return p.parseAlterDatabaseStmt()
	case placement:
		p.lexer.Restore(mark)
		return p.parseAlterPlacementPolicyStmt()
	case sequence:
		p.lexer.Restore(mark)
		return p.parseAlterSequenceStmt()
	case resource:
		p.lexer.Restore(mark)
		return p.parseAlterResourceGroupStmt()
	case instance:
		p.lexer.Restore(mark)
		return p.parseAlterInstanceStmt()
	case rangeKwd:
		p.lexer.Restore(mark)
		return p.parseAlterRangeStmt()
	case user:
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
	case user:
		p.lexer.Restore(mark)
		return p.parseRenameUserStmt()
	default:
		p.lexer.Restore(mark)
		return p.speculate(p.parseRenameTableStmt)
	}
}
