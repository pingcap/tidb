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

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseIntList parses a comma-separated list of integer literals.
func (p *HandParser) parseIntList() []int64 {
	var ids []int64
	for {
		if p.peek().Tp == 58197 {
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

	if _, ok := p.accept(57621); ok {
		// BEGIN [PESSIMISTIC | OPTIMISTIC]
		if tok, ok := p.acceptAny(58172, 58171); ok {
			if tok.Tp == 58172 {
				stmt.Mode = ast.Pessimistic
			} else {
				stmt.Mode = ast.Optimistic
			}
		}
		return stmt
	}

	// START TRANSACTION ...
	p.expect(57925)
	p.expect(57960)

	switch p.peek().Tp {
	case 57522:
		p.next()
		if _, ok := p.accept(57591); ok {
			// START TRANSACTION READ WRITE — default mode
		} else if _, ok := p.accept(57816); ok {
			stmt.ReadOnly = true
			if _, ok := p.accept(57347); ok {
				p.expect(57954)
				if ts := p.parseExpression(precNone); ts != nil {
					asOf := Alloc[ast.AsOfClause](p.arena)
					asOf.TsExpr = ts
					stmt.AsOf = asOf
				} else {
					p.syntaxError(p.peek().Offset)
					return nil
				}
			}
		}
	case 57590:
		p.next()
		if _, ok := p.accept(57667); ok {
			p.expect(57911)
			// START TRANSACTION WITH CONSISTENT SNAPSHOT — default mode
		} else if _, ok := p.accept(57637); ok {
			p.expect(57666)
			p.expect(57816)
			stmt.CausalConsistencyOnly = true
		}
	}

	return stmt
}

// parseCommitStmt parses:  COMMIT [AND [NO] CHAIN [NO RELEASE] | RELEASE | NO RELEASE]
func (p *HandParser) parseCommitStmt() ast.StmtNode {
	stmt := Alloc[ast.CommitStmt](p.arena)
	p.expect(57656)
	stmt.CompletionType = p.parseCompletionType()
	return stmt
}

// parseRollbackStmt parses:  ROLLBACK [TO [SAVEPOINT] ident | CompletionType]
func (p *HandParser) parseRollbackStmt() ast.StmtNode {
	stmt := Alloc[ast.RollbackStmt](p.arena)
	p.expect(57878)

	if _, ok := p.accept(57564); ok {
		// ROLLBACK TO [SAVEPOINT] ident
		p.accept(57886) // optional
		tok := p.next()
		stmt.SavepointName = tok.Lit
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
	case 57367:
		p.next()
		if _, ok := p.accept(57638); ok {
			// AND CHAIN [NO RELEASE]
			if _, ok := p.accept(57799); ok {
				p.expect(57527)
			}
			return ast.CompletionTypeChain
		}
		// AND NO CHAIN [RELEASE | NO RELEASE]
		p.expect(57799)
		p.expect(57638)
		if _, ok := p.accept(57527); ok {
			return ast.CompletionTypeRelease
		}
		if _, ok := p.accept(57799); ok {
			p.expect(57527)
		}
		return ast.CompletionTypeDefault
	case 57527:
		p.next()
		return ast.CompletionTypeRelease
	case 57799:
		p.next()
		p.expect(57527)
		return ast.CompletionTypeDefault
	}
	return ast.CompletionTypeDefault
}

// parseSavepointStmt parses:  SAVEPOINT ident
func (p *HandParser) parseSavepointStmt() ast.StmtNode {
	stmt := Alloc[ast.SavepointStmt](p.arena)
	p.expect(57886)
	tok := p.next()
	stmt.Name = tok.Lit
	return stmt
}

// parseReleaseSavepointStmt parses:  RELEASE SAVEPOINT ident
func (p *HandParser) parseReleaseSavepointStmt() ast.StmtNode {
	stmt := Alloc[ast.ReleaseSavepointStmt](p.arena)
	p.expect(57527)
	p.expect(57886)
	tok := p.next()
	stmt.Name = tok.Lit
	return stmt
}

// parseUseStmt parses:  USE dbname
// dbname must be an identifier-like token (including non-reserved keywords).
func (p *HandParser) parseUseStmt() ast.StmtNode {
	stmt := Alloc[ast.UseStmt](p.arena)
	p.expect(57575)
	tok := p.peek()
	if !isIdentLike(tok.Tp) {
		p.errorNear(p.peek().Offset, p.peek().Offset)
		return nil
	}
	p.next()
	stmt.DBName = tok.Lit
	return stmt
}

// parseDoStmt parses:  DO expr [, expr ...]
func (p *HandParser) parseDoStmt() ast.StmtNode {
	stmt := Alloc[ast.DoStmt](p.arena)
	p.expect(57693)
	// Parse comma-separated expression list.
	stmt.Exprs = []ast.ExprNode{p.parseExpression(precNone)}
	for {
		if _, ok := p.accept(','); !ok {
			break
		}
		stmt.Exprs = append(stmt.Exprs, p.parseExpression(precNone))
	}
	return stmt
}

// parseUnlockTablesStmt parses:  UNLOCK TABLES
func (p *HandParser) parseUnlockTablesStmt() ast.StmtNode {
	stmt := Alloc[ast.UnlockTablesStmt](p.arena)
	p.expect(57570)
	// Accept TABLES or TABLE (MySQL supports both forms)
	if _, ok := p.accept(57944); !ok {
		p.expect(57556)
	}
	return stmt
}

// parseShutdownStmt parses:  SHUTDOWN
func (p *HandParser) parseShutdownStmt() ast.StmtNode {
	stmt := Alloc[ast.ShutdownStmt](p.arena)
	p.expect(57904)
	return stmt
}

// parseRestartStmt parses:  RESTART
func (p *HandParser) parseRestartStmt() ast.StmtNode {
	stmt := Alloc[ast.RestartStmt](p.arena)
	p.expect(57871)
	return stmt
}

// parseHelpStmt parses:  HELP 'topic'
func (p *HandParser) parseHelpStmt() ast.StmtNode {
	stmt := Alloc[ast.HelpStmt](p.arena)
	p.expect(57737)
	tok := p.next()
	stmt.Topic = tok.Lit
	return stmt
}

// parseAdminStmt parses ADMIN statements:
//
//	ADMIN REPAIR TABLE t CREATE TABLE ...
//	ADMIN SHOW DDL ...
//	etc.
func (p *HandParser) parseAdminStmt() ast.StmtNode {
	p.expect(58122)

	stmt := Alloc[ast.AdminStmt](p.arena)

	switch p.peek().Tp {
	case 57863:
		p.next()
		p.expect(57556)
		repairStmt := Alloc[ast.RepairTableStmt](p.arena)
		repairStmt.Table = p.parseTableName()
		cs := p.parseCreateTableStmt()
		if cs != nil {
			if ct, ok := cs.(*ast.CreateTableStmt); ok {
				repairStmt.CreateStmt = ct
			}
		}
		return repairStmt

	case 57542:
		p.next()
		return p.parseAdminShow(stmt)

	case 57641:
		p.next()
		p.expect(57556)
		stmt.Tp = ast.AdminChecksumTable
		stmt.Tables = p.parseTableNameList()
		return stmt

	case 57389:
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

	case 57383:
		p.next()
		if _, ok := p.accept(57556); ok {
			stmt.Tp = ast.AdminCheckTable
			stmt.Tables = p.parseTableNameList()
			return stmt
		}
		if _, ok := p.accept(57449); ok {
			// ADMIN CHECK INDEX t idx [(begin, end), ...]
			stmt.Tp = ast.AdminCheckIndex
			stmt.Tables = []*ast.TableName{p.parseTableName()}
			if p.peek().Tp != '(' && p.peek().Tp != ';' && p.peek().Tp != EOF {
				// Accept any token that could be an index name
				tok := p.next()
				stmt.Index = tok.Lit
			}
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

	case 58153:
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
		if p.peek().Tp != 58197 {
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
			if p.peek().Tp == 58197 {
				val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
				stmt.JobNumber = val
			}
			// Optional WHERE clause for DDL JOBS
			if _, ok := p.accept(57587); ok {
				stmt.Where = p.parseExpression(precNone)
			}
		} else if p.peek().IsKeyword("JOB") {
			p.next()
			// JOB QUERIES id,...
			if p.peek().IsKeyword("QUERIES") {
				p.next()
				stmt.Tp = ast.AdminShowDDLJobQueries
				stmt.JobIDs = p.parseIntList()
				// Optional LIMIT [offset,] count or LIMIT count OFFSET offset
				if _, ok := p.accept(57477); ok {
					stmt.Tp = ast.AdminShowDDLJobQueriesWithRange
					if p.peek().Tp == 58197 {
						val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
						stmt.LimitSimple.Count = uint64(val)
						// Check for offset, count form
						if _, ok := p.accept(','); ok {
							stmt.LimitSimple.Offset = stmt.LimitSimple.Count
							if p.peek().Tp == 58197 {
								val2, _ := strconv.ParseInt(p.next().Lit, 10, 64)
								stmt.LimitSimple.Count = uint64(val2)
							}
						}
						// Check for LIMIT count OFFSET offset form
						if p.peek().IsKeyword("OFFSET") {
							p.next()
							if p.peek().Tp == 58197 {
								val2, _ := strconv.ParseInt(p.next().Lit, 10, 64)
								stmt.LimitSimple.Offset = uint64(val2)
							}
						}
					}
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
			if p.peek().Tp == 58197 {
				val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
				stmt.ShowSlow.Count = uint64(val)
			}
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
			if p.peek().Tp == 58197 {
				val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
				stmt.ShowSlow.Count = uint64(val)
			}
		}
		return stmt
	}
	// ADMIN SHOW BDR ROLE
	if p.peek().IsKeyword("BDR") {
		p.next()
		p.next() // ROLE
		stmt.Tp = ast.AdminShowBDRRole
		return stmt
	}
	// ADMIN SHOW tablename NEXT_ROW_ID
	tbl := p.parseTableName()
	if tbl != nil && p.peek().Tp == 58051 {
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
		p.expect(57449)
		return p.parseAdminIndexOp(stmt, ast.AdminRecoverIndex)
	}
	if p.peek().IsKeyword("CLEANUP") {
		p.next()
		if _, ok := p.accept(57449); ok {
			return p.parseAdminIndexOp(stmt, ast.AdminCleanupIndex)
		}
		if _, ok := p.accept(57556); ok {
			// ADMIN CLEANUP TABLE LOCK t, ...
			p.accept(57483)
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
		} else {
			// SESSION, INSTANCE, or GLOBAL followed by optional PLAN_CACHE
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
			if p.peek().IsKeyword("PLAN_CACHE") {
				p.next()
			}
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
		p.next() // BINDINGS
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
			if p.peek().Tp == 58197 {
				val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
				stmt.JobNumber = val
			}
			for {
				if p.peek().Tp == ';' || p.peek().Tp == EOF {
					break
				}
				var opt ast.AlterJobOption
				opt.Name = p.next().Lit
				p.expectAny(58202, 58201)
				opt.Value = p.parseExpression(precNone)
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
// Parses: table_name [index_name]
func (p *HandParser) parseAdminIndexOp(stmt *ast.AdminStmt, stmtType ast.AdminStmtType) ast.StmtNode {
	stmt.Tp = stmtType
	stmt.Tables = []*ast.TableName{p.parseTableName()}
	if p.peek().Tp != '(' && p.peek().Tp != ';' && p.peek().Tp != EOF {
		stmt.Index = p.next().Lit
	}
	return stmt
}
