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

package hparser

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ParseSQL parses one or more SQL statements and returns the AST nodes.
// Callers should first call Init() with a LexFunc and source SQL string.
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
	case tokSelect:
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

	case tokInsert:
		if s := p.parseInsertStmt(false); s != nil {
			return s
		}
		return nil

	case tokReplace:
		if s := p.parseInsertStmt(true); s != nil {
			return s
		}
		return nil

	case tokUpdate:
		if s := p.parseUpdateStmt(); s != nil {
			return s
		}
		return nil

	case tokDelete:
		if s := p.parseDeleteStmt(); s != nil {
			return s
		}
		return nil

	case tokLoad:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseLoadStatsStmt()
		}
		return p.parseLoadDataStmt()
	case tokTable, tokValues:
		var s ast.StmtNode
		if p.peek().Tp == tokTable {
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

	case tokGrant:
		return p.parseGrantStmt()

	case tokRevoke:
		return p.parseRevokeStmt()

	case tokBinlog:
		return p.parseBinlogStmt()

	case tokSplit:
		return p.parseSplitRegionStmt()
	case tokDistribute:
		return p.parseDistributeTableStmt()

	// --- Transaction control ---
	case tokBegin, tokStart:
		return p.parseBeginStmt()
	case tokCommit:
		return p.parseCommitStmt()
	case tokRollback:
		return p.parseRollbackStmt()
	case tokSavepoint:
		return p.parseSavepointStmt()
	case tokRelease:
		return p.parseReleaseSavepointStmt()

	// --- Trivial admin ---
	case tokUse:
		return p.parseUseStmt()
	case tokDo:
		return p.parseDoStmt()
	case tokUnlock:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseUnlockStatsStmt()
		}
		return p.parseUnlockTablesStmt()
	case tokKill:
		return p.parseKillStmt()
	case tokFlush:
		return p.parseFlushStmt()
	case tokRecommend:
		return p.parseRecommendStmt()
	case tokTrace:
		return p.parseTraceStmt()
	case tokShutdown:
		return p.parseShutdownStmt()
	case tokRestart:
		return p.parseRestartStmt()
	case tokHelp:
		return p.parseHelpStmt()

	// --- SET / SHOW ---
	case tokSet:
		return p.speculate(p.parseSetStmt)
	case tokShow:
		return p.parseShowStmt()

	// --- DDL ---
	case tokCreate:
		return p.speculate(p.parseCreateStmt)
	case tokAlter:
		return p.parseAlterStmt()
	case tokDrop:
		return p.speculate(p.parseDropStmt)
	case tokTruncate:
		return p.speculate(p.parseTruncateTableStmt)
	case tokRename:
		return p.parseRenameStmt()
	case tokAnalyze:
		return p.speculate(p.parseAnalyzeTableStmt)
	case tokFlashback:
		return p.speculate(p.parseFlashbackStmt)

	// --- Admin / Utility ---
	case tokOptimize:
		return p.parseOptimizeTableStmt()
	case tokPlan:
		return p.parsePlanReplayerStmt() // PLAN REPLAYER
	case tokQuery:
		return p.parseQueryWatchStmt() // QUERY WATCH

	// --- EXPLAIN / DESCRIBE ---
	case tokExplain, tokDescribe, tokDesc:
		return p.speculate(p.parseExplainStmt)

	// --- LOCK TABLES ---
	case tokLock:
		if p.peekN(1).IsKeyword("STATS") {
			return p.parseLockStatsStmt()
		}
		return p.parseLockTablesStmt()

	// --- Admin helpers ---
	case tokAdmin:
		return p.speculate(p.parseAdminStmt)
	case tokCall:
		return p.speculate(p.parseCallStmt)

	// --- IMPORT / BRIE / TRAFFIC / REFRESH ---
	case tokImport:
		return p.parseImportIntoStmt()
	case tokCancel:
		return p.parseCancelStmt()
	case tokBackup, tokRestore:
		return p.parseBRIEStmt()
	case tokTraffic:
		return p.parseTrafficStmt()
	case tokPause:
		p.next() // consume PAUSE
		return p.parsePauseBackupLogsStmt()
	case tokResume:
		p.next() // consume RESUME
		return p.parseResumeBackupLogsStmt()
	case tokStop:
		p.next() // consume STOP
		return p.parseStopBackupLogsStmt()
	case tokPurge:
		p.next() // consume PURGE
		return p.parsePurgeBackupLogsStmt()
	case tokBatch:
		return p.parseNonTransactionalDMLStmt()
	case tokRefresh:
		return p.parseRefreshStmt()

	case tokWith:
		return p.parseWithStmt()
	case tokCalibrate:
		return p.parseCalibrateResourceStmt()
	case tokIdentifier:
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
	case tokPrepare:
		return p.parsePrepareStmt()
	case tokExecute:
		return p.parseExecuteStmt()
	case tokDeallocate:
		return p.parseDeallocateStmt()
	case tokRecover:
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
	case tokUnique:
		p.next()
		indexType = 1
	case tokFulltext:
		p.next()
		indexType = 2
	case tokSpatial:
		p.next()
		indexType = 3
	case tokVector:
		p.next()
		indexType = 4
	case tokColumnar:
		p.next()
		indexType = 5
	}

	switch p.peek().Tp {
	case tokIndex, tokKey:
		p.lexer.Restore(mark)
		return p.parseCreateIndexStmt()
	case tokSequence:
		p.lexer.Restore(mark)
		return p.parseCreateSequenceStmt()
	case tokPlacement:
		p.lexer.Restore(mark)
		return p.parseCreatePlacementPolicyStmt()
	case tokOr:
		// CREATE OR REPLACE ... — peek past OR REPLACE to disambiguate.
		mark2 := p.lexer.Mark()
		p.next() // consume OR
		p.next() // consume REPLACE
		nextTp := p.peek().Tp
		p.lexer.Restore(mark2) // restore back to OR

		switch nextTp {
		case tokView, tokAlgorithm, tokDefiner, tokSql:
			p.lexer.Restore(mark)
			return p.parseCreateViewStmt()
		}

		p.lexer.Restore(mark)
		return p.parseCreatePlacementPolicyStmt()
	case tokDatabase: // tokSchema == tokDatabase
		p.lexer.Restore(mark)
		return p.parseCreateDatabaseStmt()
	case tokResourceGroup:
		p.lexer.Restore(mark)
		return p.parseCreateResourceGroupStmt()
	case tokProcedure:
		p.lexer.Restore(mark)
		return p.parseCreateProcedureStmt()
	case tokView:
		p.lexer.Restore(mark)
		return p.parseCreateViewStmt()
	case tokTable:
		_ = indexType
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case tokTemporary:
		_ = indexType
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case tokUser:
		p.lexer.Restore(mark)
		return p.parseCreateUserStmt()
	case tokRole:
		p.lexer.Restore(mark)
		return p.parseCreateUserStmt()
	case tokAlgorithm, tokDefiner, tokSql:
		// CREATE ALGORITHM = ... VIEW, CREATE DEFINER = ... VIEW, CREATE SQL SECURITY ... VIEW
		p.lexer.Restore(mark)
		return p.parseCreateViewStmt()
	case tokGlobal:
		// CREATE GLOBAL BINDING ...
		p.next() // consume GLOBAL
		if p.peek().Tp == tokBinding {
			p.next() // consume BINDING
			return p.parseCreateBindingStmt(true)
		}
		// CREATE GLOBAL TEMPORARY TABLE ...
		p.lexer.Restore(mark)
		return p.parseCreateTableStmt()
	case tokSession:
		// CREATE SESSION BINDING ...
		p.next() // consume SESSION
		if p.peek().Tp == tokBinding {
			p.next() // consume BINDING
			return p.parseCreateBindingStmt(false)
		}
		p.lexer.Restore(mark)
		return nil
	case tokBinding:
		// CREATE BINDING (default session scope)
		p.next() // consume BINDING
		return p.parseCreateBindingStmt(false)
	case tokStatistics:
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
	case tokDatabase: // tokSchema == tokDatabase
		p.lexer.Restore(mark)
		return p.parseAlterDatabaseStmt()
	case tokPlacement:
		p.lexer.Restore(mark)
		return p.parseAlterPlacementPolicyStmt()
	case tokSequence:
		p.lexer.Restore(mark)
		return p.parseAlterSequenceStmt()
	case tokResourceGroup:
		p.lexer.Restore(mark)
		return p.parseAlterResourceGroupStmt()
	case tokInstance:
		p.lexer.Restore(mark)
		return p.parseAlterInstanceStmt()
	case tokRange:
		p.lexer.Restore(mark)
		return p.parseAlterRangeStmt()
	case tokUser:
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
	case tokUser:
		p.lexer.Restore(mark)
		return p.parseRenameUserStmt()
	default:
		p.lexer.Restore(mark)
		return p.speculate(p.parseRenameTableStmt)
	}
}
