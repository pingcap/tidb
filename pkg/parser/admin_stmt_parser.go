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
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseIntList parses a comma-separated list of integer literals.
func (p *HandParser) parseIntList() []int64 {
	var ids []int64
	for {
		if p.peek().Tp == intLit {
			val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
			ids = append(ids, val)
		}
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return ids
}

// parseBeginStmt parses BEGIN and START TRANSACTION statements.
//
// Grammar:
//
//	BEGIN [PESSIMISTIC | OPTIMISTIC]
//	START TRANSACTION [READ WRITE | READ ONLY [AS OF ...] |
//	                   WITH CONSISTENT SNAPSHOT |
//	                   WITH CAUSAL CONSISTENCY ONLY]
func (p *HandParser) parseBeginStmt() ast.StmtNode {
	stmt := Alloc[ast.BeginStmt](p.arena)

	if _, ok := p.accept(begin); ok {
		// BEGIN [PESSIMISTIC | OPTIMISTIC]
		if tok, ok := p.acceptAny(pessimistic, optimistic); ok {
			if tok.Tp == pessimistic {
				stmt.Mode = ast.Pessimistic
			} else {
				stmt.Mode = ast.Optimistic
			}
		}
		return stmt
	}

	// START TRANSACTION ...
	p.expect(start)
	p.expect(transaction)

	switch p.peek().Tp {
	case read:
		p.next()
		if _, ok := p.accept(write); !ok {
			if _, ok := p.accept(only); ok {
				stmt.ReadOnly = true
				if _, ok := p.accept(asof); ok {
					p.expect(timestampType)
					ts := p.parseExpression(precNone)
					if ts == nil {
						p.syntaxError(p.peek().Offset)
						return nil
					}
					asOf := Alloc[ast.AsOfClause](p.arena)
					asOf.TsExpr = ts
					stmt.AsOf = asOf
				}
			}
		}
	case with:
		p.next()
		if _, ok := p.accept(consistent); ok {
			p.expect(snapshot)
			// START TRANSACTION WITH CONSISTENT SNAPSHOT — default mode
		} else if _, ok := p.accept(causal); ok {
			p.expect(consistency)
			p.expect(only)
			stmt.CausalConsistencyOnly = true
		}
	}

	return stmt
}

// parseCommitStmt parses:  COMMIT [AND [NO] CHAIN [NO RELEASE] | RELEASE | NO RELEASE]
func (p *HandParser) parseCommitStmt() ast.StmtNode {
	stmt := Alloc[ast.CommitStmt](p.arena)
	p.expect(commit)
	stmt.CompletionType = p.parseCompletionType()
	return stmt
}

// parseRollbackStmt parses:  ROLLBACK [TO [SAVEPOINT] ident | CompletionType]
func (p *HandParser) parseRollbackStmt() ast.StmtNode {
	stmt := Alloc[ast.RollbackStmt](p.arena)
	p.expect(rollback)

	if _, ok := p.accept(to); ok {
		// ROLLBACK TO [SAVEPOINT] Identifier
		p.accept(savepoint) // optional
		// yacc: ROLLBACK TO Identifier — requires valid identifier
		stmt.SavepointName = p.parseName()
	} else {
		stmt.CompletionType = p.parseCompletionType()
	}
	return stmt
}

// parseCompletionType parses the optional completion clause for COMMIT/ROLLBACK:
//
//	AND CHAIN [NO RELEASE] | AND NO CHAIN [RELEASE | NO RELEASE] |
//	RELEASE | NO RELEASE
func (p *HandParser) parseCompletionType() ast.CompletionType {
	switch p.peek().Tp {
	case and:
		p.next()
		if _, ok := p.accept(chain); ok {
			// AND CHAIN [NO RELEASE]
			if _, ok := p.accept(no); ok {
				p.expect(release)
			}
			return ast.CompletionTypeChain
		}
		// AND NO CHAIN [RELEASE | NO RELEASE]
		p.expect(no)
		p.expect(chain)
		if _, ok := p.accept(release); ok {
			return ast.CompletionTypeRelease
		}
		if _, ok := p.accept(no); ok {
			p.expect(release)
		}
		return ast.CompletionTypeDefault
	case release:
		p.next()
		return ast.CompletionTypeRelease
	case no:
		p.next()
		p.expect(release)
		return ast.CompletionTypeDefault
	}
	return ast.CompletionTypeDefault
}

// parseSavepointStmt parses:  SAVEPOINT Identifier
func (p *HandParser) parseSavepointStmt() ast.StmtNode {
	stmt := Alloc[ast.SavepointStmt](p.arena)
	p.expect(savepoint)
	// yacc: SAVEPOINT Identifier — requires valid identifier
	stmt.Name = p.parseName()
	return stmt
}

// parseReleaseSavepointStmt parses:  RELEASE SAVEPOINT Identifier
func (p *HandParser) parseReleaseSavepointStmt() ast.StmtNode {
	stmt := Alloc[ast.ReleaseSavepointStmt](p.arena)
	p.expect(release)
	p.expect(savepoint)
	// yacc: RELEASE SAVEPOINT Identifier — requires valid identifier
	stmt.Name = p.parseName()
	return stmt
}

// parseUseStmt parses:  USE dbname
// dbname must be an identifier-like token (including non-reserved keywords).
func (p *HandParser) parseUseStmt() ast.StmtNode {
	stmt := Alloc[ast.UseStmt](p.arena)
	p.expect(use)
	tok := p.peek()
	if !isIdentLike(tok.Tp) {
		p.syntaxErrorAt(p.peek())
		return nil
	}
	p.next()
	stmt.DBName = tok.Lit
	return stmt
}

// parseDoStmt parses:  DO expr [, expr ...]
func (p *HandParser) parseDoStmt() ast.StmtNode {
	stmt := Alloc[ast.DoStmt](p.arena)
	p.expect(do)
	// Parse comma-separated expression list.
	firstExpr := p.parseExpression(precNone)
	if firstExpr == nil {
		return nil
	}
	stmt.Exprs = []ast.ExprNode{firstExpr}
	for {
		if _, ok := p.accept(','); !ok {
			break
		}
		expr := p.parseExpression(precNone)
		if expr == nil {
			return nil
		}
		stmt.Exprs = append(stmt.Exprs, expr)
	}
	return stmt
}

// parseUnlockTablesStmt parses:  UNLOCK TABLES
func (p *HandParser) parseUnlockTablesStmt() ast.StmtNode {
	stmt := Alloc[ast.UnlockTablesStmt](p.arena)
	p.expect(unlock)
	// Accept TABLES or TABLE (MySQL supports both forms)
	if _, ok := p.accept(tables); !ok {
		p.expect(tableKwd)
	}
	return stmt
}

// parseShutdownStmt parses:  SHUTDOWN
func (p *HandParser) parseShutdownStmt() ast.StmtNode {
	stmt := Alloc[ast.ShutdownStmt](p.arena)
	p.expect(shutdown)
	return stmt
}

// parseRestartStmt parses:  RESTART
func (p *HandParser) parseRestartStmt() ast.StmtNode {
	stmt := Alloc[ast.RestartStmt](p.arena)
	p.expect(restart)
	return stmt
}

// parseHelpStmt parses:  HELP 'topic'
func (p *HandParser) parseHelpStmt() ast.StmtNode {
	stmt := Alloc[ast.HelpStmt](p.arena)
	p.expect(help)
	if tok, ok := p.expect(stringLit); ok {
		stmt.Topic = tok.Lit
	}
	return stmt
}

// parseAdminStmt parses ADMIN statements:
//
//	ADMIN REPAIR TABLE t CREATE TABLE ...
//	ADMIN SHOW DDL ...
//	etc.
func (p *HandParser) parseAdminStmt() ast.StmtNode {
	p.expect(admin)

	stmt := Alloc[ast.AdminStmt](p.arena)

	switch p.peek().Tp {
	case repair:
		p.next()
		p.expect(tableKwd)
		repairStmt := Alloc[ast.RepairTableStmt](p.arena)
		repairStmt.Table = p.parseTableName()
		cs := p.parseCreateTableStmt()
		if cs != nil {
			if ct, ok := cs.(*ast.CreateTableStmt); ok {
				repairStmt.CreateStmt = ct
			}
		}
		return repairStmt

	case show:
		p.next()
		return p.parseAdminShow(stmt)

	case checksum:
		p.next()
		p.expect(tableKwd)
		stmt.Tp = ast.AdminChecksumTable
		stmt.Tables = p.parseTableNameList()
		return stmt

	case create:
		p.next()
		// ADMIN CREATE WORKLOAD SNAPSHOT
		if p.peek().IsKeyword("WORKLOAD") {
			p.next()
			if p.peek().IsKeyword("SNAPSHOT") {
				p.next()
			}
			stmt.Tp = ast.AdminWorkloadRepoCreate
			return stmt
		}
		return nil

	case check:
		p.next()
		if _, ok := p.accept(tableKwd); ok {
			stmt.Tp = ast.AdminCheckTable
			stmt.Tables = p.parseTableNameList()
			return stmt
		}
		if _, ok := p.accept(index); ok {
			// ADMIN CHECK INDEX t idx [(begin, end), ...]
			stmt.Tp = ast.AdminCheckIndex
			tbl := p.parseTableName()
			if tbl == nil {
				return nil
			}
			stmt.Tables = []*ast.TableName{tbl}
			// yacc: Identifier — requires valid identifier for index name
			stmt.Index = p.parseName()
			// Parse optional handle ranges: (begin, end), (begin, end), ...
			if p.peek().Tp == '(' {
				stmt.Tp = ast.AdminCheckIndexRange
				for {
					p.expect('(')
					var hr ast.HandleRange
					beginTok := p.next()
					if v, ok := tokenItemToInt64(beginTok.Item); ok {
						hr.Begin = v
					} else {
						p.error(beginTok.Offset, "integer value is out of range")
					}
					p.expect(',')
					endTok := p.next()
					if v, ok := tokenItemToInt64(endTok.Item); ok {
						hr.End = v
					} else {
						p.error(endTok.Offset, "integer value is out of range")
					}
					p.expect(')')
					stmt.HandleRanges = append(stmt.HandleRanges, hr)
					if _, ok := p.accept(','); !ok {
						break
					}
				}
			}
			return stmt
		}
		return nil

	case cancel:
		p.next()
		return p.parseAdminDDLJobs(stmt, ast.AdminCancelDDLJobs)

	default:
		return p.parseAdminKeywordBased(stmt)
	}
}

// parseAdminDDLJobs parses the common pattern: DDL JOBS id,...
// Used by ADMIN CANCEL/PAUSE/RESUME DDL JOBS.
func (p *HandParser) parseAdminDDLJobs(stmt *ast.AdminStmt, stmtType ast.AdminStmtType) ast.StmtNode {
	if p.peek().IsKeyword("DDL") {
		p.next()
		p.next() // JOBS
		stmt.Tp = stmtType
		if p.peek().Tp != intLit {
			return nil
		}
		stmt.JobIDs = p.parseIntList()
		return stmt
	}
	return nil
}

// parseAdminShow handles ADMIN SHOW sub-commands.
// Caller already consumed ADMIN and SHOW tokens.
func (p *HandParser) parseAdminShow(stmt *ast.AdminStmt) ast.StmtNode {
	// ADMIN SHOW DDL [JOBS [num] | JOB QUERIES id,...]
	if p.peek().IsKeyword("DDL") {
		p.next()
		stmt.Tp = ast.AdminShowDDL

		// Optional JOBS
		if p.peek().IsKeyword("JOBS") {
			p.next()
			stmt.Tp = ast.AdminShowDDLJobs
			// Optional job number
			if p.peek().Tp == intLit {
				val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
				stmt.JobNumber = val
			}
			// Optional WHERE clause for DDL JOBS
			if _, ok := p.accept(where); ok {
				stmt.Where = p.parseExpression(precNone)
			}
		} else if p.peek().IsKeyword("JOB") {
			p.next()
			// JOB QUERIES id,...
			if p.peek().IsKeyword("QUERIES") {
				p.next()
				// Yacc has two distinct alternatives:
				// 1. QUERIES NumList → AdminShowDDLJobQueries (int list)
				// 2. QUERIES LIMIT ... → AdminShowDDLJobQueriesWithRange
				if _, ok := p.accept(limit); ok {
					stmt.Tp = ast.AdminShowDDLJobQueriesWithRange
					if p.peek().Tp == intLit {
						val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
						stmt.LimitSimple.Count = uint64(val)
						// Check for offset, count form: LIMIT offset, count
						if _, ok := p.accept(','); ok {
							stmt.LimitSimple.Offset = stmt.LimitSimple.Count
							if p.peek().Tp == intLit {
								val2, _ := strconv.ParseInt(p.next().Lit, 10, 64)
								stmt.LimitSimple.Count = uint64(val2)
							}
						}
						// Check for LIMIT count OFFSET offset form
						if p.peek().IsKeyword("OFFSET") {
							p.next()
							if p.peek().Tp == intLit {
								val2, _ := strconv.ParseInt(p.next().Lit, 10, 64)
								stmt.LimitSimple.Offset = uint64(val2)
							}
						}
					}
				} else {
					stmt.Tp = ast.AdminShowDDLJobQueries
					stmt.JobIDs = p.parseIntList()
				}
			}
		}
		return stmt
	}
	// ADMIN SHOW SLOW ...
	if p.peek().IsKeyword("SLOW") {
		p.next()
		stmt.Tp = ast.AdminShowSlow
		stmt.ShowSlow = &ast.ShowSlow{}
		if p.peek().IsKeyword("RECENT") {
			p.next()
			stmt.ShowSlow.Tp = ast.ShowSlowRecent
			val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
			stmt.ShowSlow.Count = uint64(val)
		} else if p.peek().IsKeyword("TOP") {
			p.next()
			stmt.ShowSlow.Tp = ast.ShowSlowTop
			if p.peek().IsKeyword("INTERNAL") {
				p.next()
				stmt.ShowSlow.Kind = ast.ShowSlowKindInternal
			} else if p.peek().IsKeyword("ALL") {
				p.next()
				stmt.ShowSlow.Kind = ast.ShowSlowKindAll
			}
			val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
			stmt.ShowSlow.Count = uint64(val)
		}
		return stmt
	}
	// ADMIN SHOW BDR ROLE
	if p.peek().IsKeyword("BDR") {
		p.next()
		if tok := p.next(); tok.Tp != role && !tok.IsKeyword("ROLE") {
			p.syntaxErrorAt(tok)
			return nil
		}
		stmt.Tp = ast.AdminShowBDRRole
		return stmt
	}
	// ADMIN SHOW tablename NEXT_ROW_ID
	tbl := p.parseTableName()
	if tbl != nil && p.peek().Tp == next_row_id {
		p.next()
		stmt.Tp = ast.AdminShowNextRowID
		stmt.Tables = []*ast.TableName{tbl}
		return stmt
	}
	return nil
}

// parseAdminKeywordBased handles keyword-based ADMIN sub-commands.
// Uses IsKeyword() because keywords like PAUSE, RESUME, etc. have dedicated token types.
func (p *HandParser) parseAdminKeywordBased(stmt *ast.AdminStmt) ast.StmtNode {
	if p.peek().IsKeyword("RECOVER") {
		p.next()
		p.expect(index)
		return p.parseAdminIndexOp(stmt, ast.AdminRecoverIndex)
	}
	if p.peek().IsKeyword("CLEANUP") {
		p.next()
		if _, ok := p.accept(index); ok {
			return p.parseAdminIndexOp(stmt, ast.AdminCleanupIndex)
		}
		if _, ok := p.accept(tableKwd); ok {
			// ADMIN CLEANUP TABLE LOCK t, ...
			p.expect(lock)
			cleanupStmt := Alloc[ast.CleanupTableLockStmt](p.arena)
			cleanupStmt.Tables = p.parseTableNameList()
			return cleanupStmt
		}
		return nil
	}
	if p.peek().IsKeyword("RELOAD") {
		p.next()
		if p.peek().IsKeyword("STATISTICS") || p.peek().IsKeyword("STATS_EXTENDED") {
			p.next()
			stmt.Tp = ast.AdminReloadStatistics
		} else if p.peek().IsKeyword("OPT_RULE_BLACKLIST") {
			p.next()
			stmt.Tp = ast.AdminReloadOptRuleBlacklist
		} else if p.peek().IsKeyword("EXPR_PUSHDOWN_BLACKLIST") {
			p.next()
			stmt.Tp = ast.AdminReloadExprPushdownBlacklist
		} else if p.peek().IsKeyword("BINDINGS") {
			p.next()
			stmt.Tp = ast.AdminReloadBindings
		} else if p.peek().IsKeyword("CLUSTER") {
			p.next()
			if p.peek().IsKeyword("BINDINGS") {
				p.next()
			}
			stmt.Tp = ast.AdminReloadClusterBindings
		}
		return stmt
	}
	if p.peek().IsKeyword("FLUSH") {
		p.next()
		if p.peek().IsKeyword("BINDINGS") {
			p.next()
			stmt.Tp = ast.AdminFlushBindings
		} else if p.peek().IsKeyword("PLAN_CACHE") {
			p.next()
			stmt.Tp = ast.AdminFlushPlanCache
			stmt.StatementScope = ast.StatementScopeSession // default scope matches yacc
		} else {
			// yacc: "ADMIN" "FLUSH" StatementScope "PLAN_CACHE"
			var scope ast.StatementScope
			switch {
			case p.peek().IsKeyword("SESSION"):
				p.next()
				scope = ast.StatementScopeSession
			case p.peek().IsKeyword("INSTANCE"):
				p.next()
				scope = ast.StatementScopeInstance
			case p.peek().IsKeyword("GLOBAL"):
				p.next()
				scope = ast.StatementScopeGlobal
			}
			// yacc: PLAN_CACHE is mandatory after scope
			p.expectKeyword(0, "PLAN_CACHE")
			stmt.Tp = ast.AdminFlushPlanCache
			stmt.StatementScope = scope
		}
		return stmt
	}
	if p.peek().IsKeyword("CAPTURE") || p.peek().IsKeyword("EVOLVE") {
		if p.peek().IsKeyword("CAPTURE") {
			stmt.Tp = ast.AdminCaptureBindings
		} else {
			stmt.Tp = ast.AdminEvolveBindings
		}
		p.next()
		// yacc: "ADMIN" "CAPTURE"/"EVOLVE" "BINDINGS" — BINDINGS is mandatory
		p.expectKeyword(0, "BINDINGS")
		return stmt
	}
	if p.peek().IsKeyword("PAUSE") {
		p.next()
		return p.parseAdminDDLJobs(stmt, ast.AdminPauseDDLJobs)
	}
	if p.peek().IsKeyword("RESUME") {
		p.next()
		return p.parseAdminDDLJobs(stmt, ast.AdminResumeDDLJobs)
	}
	if p.peek().IsKeyword("PLUGINS") {
		p.next()
		if p.peek().IsKeyword("ENABLE") {
			stmt.Tp = ast.AdminPluginEnable
		} else {
			stmt.Tp = ast.AdminPluginDisable
		}
		p.next() // consume ENABLE or DISABLE
		for {
			if p.peek().Tp == ';' || p.peek().Tp == EOF {
				break
			}
			stmt.Plugins = append(stmt.Plugins, p.next().Lit)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		return stmt
	}
	if p.peek().IsKeyword("SET") || p.peek().IsKeyword("UNSET") {
		isSet := p.peek().IsKeyword("SET")
		p.next()
		if p.peek().IsKeyword("BDR") {
			p.next()
			p.next() // ROLE
			if isSet {
				stmt.Tp = ast.AdminSetBDRRole
				if p.peek().IsKeyword("PRIMARY") {
					p.next()
					stmt.BDRRole = ast.BDRRolePrimary
				} else if p.peek().IsKeyword("SECONDARY") {
					p.next()
					stmt.BDRRole = ast.BDRRoleSecondary
				}
			} else {
				stmt.Tp = ast.AdminUnsetBDRRole
			}
			return stmt
		}
		return nil
	}
	if p.peek().IsKeyword("ALTER") {
		p.next()
		if p.peek().IsKeyword("DDL") {
			p.next()
			p.next() // JOBS
			stmt.Tp = ast.AdminAlterDDLJob
			val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
			stmt.JobNumber = val
			for {
				if p.peek().Tp == ';' || p.peek().Tp == EOF {
					break
				}
				var opt ast.AlterJobOption
				opt.Name = strings.ToLower(p.next().Lit)
				p.expectAny(eq, assignmentEq)
				opt.Value = p.parseSignedLiteral()
				if opt.Value == nil {
					return nil
				}
				stmt.AlterJobOptions = append(stmt.AlterJobOptions, &opt)
				if _, ok := p.accept(','); !ok {
					break
				}
			}
			return stmt
		}
		return nil
	}
	return nil
}

// parseAdminIndexOp is the shared implementation for ADMIN RECOVER/CLEANUP INDEX.
// Parses: table_name index_name (both required per yacc grammar)
func (p *HandParser) parseAdminIndexOp(stmt *ast.AdminStmt, stmtType ast.AdminStmtType) ast.StmtNode {
	stmt.Tp = stmtType
	tbl := p.parseTableName()
	if tbl == nil {
		return nil
	}
	stmt.Tables = []*ast.TableName{tbl}
	// yacc: Identifier — requires valid identifier for index name
	stmt.Index = p.parseName()
	return stmt
}
