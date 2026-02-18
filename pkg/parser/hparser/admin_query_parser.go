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
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseSlowQueryStmt parses SLOW QUERY [WHERE ...] [ORDER BY ...] [LIMIT ...].
// Returns a *ast.PlanReplayerStmt which serves as a container for SLOW QUERY.
func (p *HandParser) parseSlowQueryStmt() ast.StmtNode {
	stmt := Alloc[ast.PlanReplayerStmt](p.arena)
	// stmt_parser already peeked "SLOW" (if dispatched from there), but we need to consume it.
	// Since tokSlow might not exist, we consume it as identifier.
	p.next() // consume SLOW
	p.expect(tokQuery)

	if _, ok := p.accept(tokWhere); ok {
		stmt.Where = p.parseExpression(precNone)
	}
	if p.peek().Tp == tokOrder {
		stmt.OrderBy = p.parseOrderByClause()
	}
	if p.peek().Tp == tokLimit {
		stmt.Limit = p.parseLimitClause()
	}
	return stmt
}

// parseCallStmt parses:
//
//	CALL procedure_name(args...)
func (p *HandParser) parseCallStmt() ast.StmtNode {
	p.expect(tokCall)
	stmt := Alloc[ast.CallStmt](p.arena)

	// Parse procedure name: [schema.]name[(args...)]
	if !isIdentLike(p.peek().Tp) {
		return nil
	}
	nameTok := p.next()
	var schema, procName string

	if _, ok := p.accept('.'); ok {
		// schema.procedure
		schema = nameTok.Lit
		procTok := p.next()
		procName = procTok.Lit
	} else {
		procName = nameTok.Lit
	}

	fc := &ast.FuncCallExpr{
		Tp:     ast.FuncCallExprTypeGeneric,
		Schema: ast.NewCIStr(schema),
		FnName: ast.NewCIStr(procName),
	}
	fc.SetOriginTextPosition(nameTok.Offset)

	// Optional argument list
	if _, ok := p.accept('('); ok {
		if p.peek().Tp != ')' {
			for {
				arg := p.parseExpression(precNone)
				if arg == nil {
					return nil
				}
				fc.Args = append(fc.Args, arg)
				if _, ok := p.accept(','); !ok {
					break
				}
			}
		}
		p.expect(')')
	}

	stmt.Procedure = fc
	return stmt
}

// parseOptimizeTableStmt parses:
//
//	OPTIMIZE [NO_WRITE_TO_BINLOG | LOCAL] TABLE t [, t2 ...]
func (p *HandParser) parseOptimizeTableStmt() ast.StmtNode {
	p.expect(tokOptimize)

	stmt := Alloc[ast.OptimizeTableStmt](p.arena)
	stmt.NoWriteToBinLog = p.acceptNoWriteToBinlog()

	p.expectAny(tokTable, tokTables)
	stmt.Tables = p.parseTableNameList()
	return stmt
}

// parsePlanReplayerStmt parses PLAN REPLAYER statements.
func (p *HandParser) parsePlanReplayerStmt() ast.StmtNode {
	stmt := Alloc[ast.PlanReplayerStmt](p.arena)
	p.expect(tokPlan)
	p.expect(tokReplayer)

	if _, ok := p.accept(tokLoad); ok {
		stmt.Load = true
		if tok, ok := p.expect(tokStringLit); ok {
			stmt.File = tok.Lit
		}
		return stmt
	}

	// Check for CAPTURE (keyword token)
	if _, ok := p.accept(tokCapture); ok {
		// CAPTURE [REMOVE]
		if _, ok := p.accept(tokRemove); ok {
			stmt.Remove = true
		} else {
			stmt.Capture = true
		}

		if tok, ok := p.expect(tokStringLit); ok {
			stmt.SQLDigest = tok.Lit
		}
		if tok, ok := p.expect(tokStringLit); ok {
			stmt.PlanDigest = tok.Lit
		}
		return stmt
	}

	return p.parsePlanReplayerDump(stmt)
}

func (p *HandParser) parsePlanReplayerDump(stmt *ast.PlanReplayerStmt) ast.StmtNode {
	// DUMP is optional
	p.accept(tokDump)

	// WITH STATS AS OF TIMESTAMP <expr>
	if _, ok := p.accept(tokWith); ok {
		p.expect(tokStatsKwd)  // STATS keyword token
		p.expect(tokAsOf)      // AS OF is a single merged token
		p.expect(tokTimestamp) // TIMESTAMP keyword token

		stmt.HistoricalStatsInfo = &ast.AsOfClause{}
		stmt.HistoricalStatsInfo.TsExpr = p.parseExpression(precNone)
	}

	p.expect(tokExplain)
	if _, ok := p.accept(tokAnalyze); ok {
		stmt.Analyze = true
	}

	// Check if next is string literal (File path) - PLAN REPLAYER DUMP EXPLAIN 'file'
	if p.peek().Tp == tokStringLit {
		tok := p.next()
		stmt.File = tok.Lit
		return stmt
	}

	// Capture start offset for nested statement text
	start := p.peek().Offset
	nested := p.parseStatement()
	// Capture end offset
	end := p.peek().Offset
	// If the parser didn't advance (nil stmt), start=end is fine.
	// If a semicolon follows, it's not consumed yet, so offset points to it (or EOF).
	// This covers the statement text.
	if nested != nil {
		nested.SetText(nil, p.src[start:end])
	}

	if plan, ok := nested.(*ast.PlanReplayerStmt); ok {
		// If the nested statement is a SLOW QUERY container (Stmt==nil, no Load/Capture/Remove),
		// merge it into the current stmt to avoid double PLAN REPLAYER print in Restore.
		if plan.Stmt == nil && !plan.Load && !plan.Capture && !plan.Remove {
			stmt.Where = plan.Where
			stmt.OrderBy = plan.OrderBy
			stmt.Limit = plan.Limit
			// Keep stmt.Stmt nil so Restore prints "SLOW QUERY".
			return stmt
		}
	}
	stmt.Stmt = nested
	return stmt
}

// parseQueryWatchStmt parses QUERY WATCH statements.
func (p *HandParser) parseQueryWatchStmt() ast.StmtNode {
	p.expect(tokQuery)
	p.expect(tokWatch)

	if _, ok := p.accept(tokAdd); ok {
		stmt := Alloc[ast.AddQueryWatchStmt](p.arena)
		// Parse options
		var seenRG, seenAction, seenText bool
		for {
			start := p.peek().Offset
			opt := p.parseQueryWatchOption()
			if opt == nil {
				break
			}

			switch opt.Tp {
			case ast.QueryWatchResourceGroup:
				if seenRG {
					p.error(start, "Duplicate RESOURCE GROUP option")
					return nil
				}
				seenRG = true
			case ast.QueryWatchAction:
				if seenAction {
					p.error(start, "Duplicate ACTION option")
					return nil
				}
				seenAction = true
			case ast.QueryWatchType:
				if seenText {
					p.error(start, "Duplicate SQL TEXT/DIGEST option")
					return nil
				}
				seenText = true
			}

			stmt.QueryWatchOptionList = append(stmt.QueryWatchOptionList, opt)
		}
		return stmt

	} else if _, ok := p.accept(tokRemove); ok { // Support REMOVE keyword as per Restore output
		stmt := Alloc[ast.DropQueryWatchStmt](p.arena)
		if tok, ok := p.accept(tokIntLit); ok {
			id, _ := strconv.ParseInt(tok.Lit, 10, 64)
			stmt.IntValue = id
		} else {
			// Try RESOURCE GROUP
			if _, ok := p.accept(tokResource); ok {
				p.expect(tokGroup)
				if p.peek().Tp == tokSingleAtIdent {
					stmt.GroupNameExpr = p.parseExpression(precNone)
				} else if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
					stmt.GroupNameStr = ast.NewCIStr(tok.Lit)
				}
			} else {
				p.error(p.peek().Offset, "Missing QUERY WATCH REMOVE target")
				return nil
			}
		}
		return stmt
	} else if _, ok := p.accept(tokDrop); ok { // Logic fallback for DROP keyword
		// Check if tests use DROP or REMOVE
		stmt := Alloc[ast.DropQueryWatchStmt](p.arena)
		if tok, ok := p.accept(tokIntLit); ok {
			id, _ := strconv.ParseInt(tok.Lit, 10, 64)
			stmt.IntValue = id
		}
		return stmt
	}

	return nil
}

func (p *HandParser) parseQueryWatchOption() *ast.QueryWatchOption {
	opt := Alloc[ast.QueryWatchOption](p.arena)

	// RESOURCE GROUP ...
	if _, ok := p.accept(tokResource); ok {
		p.expect(tokGroup)
		opt.Tp = ast.QueryWatchResourceGroup
		rgOpt := Alloc[ast.QueryWatchResourceGroupOption](p.arena)

		if p.peek().Tp == tokSingleAtIdent {
			// @var expression
			rgOpt.GroupNameExpr = p.parseExpression(precNone)
		} else if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			rgOpt.GroupNameStr = ast.NewCIStr(tok.Lit)
		}
		opt.ResourceGroupOption = rgOpt
		return opt
	}

	// ACTION ...
	// Since tokAction defined in token_alias check it first
	if _, ok := p.accept(tokAction); ok {
		opt.Tp = ast.QueryWatchAction
		p.accept(tokEq) // Optional =

		actionOpt := Alloc[ast.ResourceGroupRunawayActionOption](p.arena)

		if _, ok := p.accept(tokKill); ok {
			actionOpt.Type = ast.RunawayActionKill
		} else if _, ok := p.accept(tokCooldown); ok {
			actionOpt.Type = ast.RunawayActionCooldown
		} else if p.peek().IsKeyword("DRYRUN") {
			p.next()
			actionOpt.Type = ast.RunawayActionDryRun
		} else if _, ok := p.accept(tokSwitchGroup); ok {
			actionOpt.Type = ast.RunawayActionSwitchGroup
			p.expect('(')
			if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
				actionOpt.SwitchGroupName = ast.NewCIStr(tok.Lit)
			}
			p.expect(')')
		}

		opt.ActionOption = actionOpt
		return opt
	}

	// SQL TEXT / SQL DIGEST / PLAN DIGEST — use token-based matching
	if _, ok := p.accept(tokSql); ok {
		if _, ok := p.accept(tokDigest); ok {
			// SQL DIGEST '...'
			opt.Tp = ast.QueryWatchType
			textOpt := Alloc[ast.QueryWatchTextOption](p.arena)
			textOpt.Type = ast.WatchSimilar
			textOpt.PatternExpr = p.parseExpression(precNone)
			opt.TextOption = textOpt
			return opt
		} else if _, ok := p.accept(tokText); ok {
			// SQL TEXT [EXACT|SIMILAR] TO '...'
			opt.Tp = ast.QueryWatchType
			textOpt := Alloc[ast.QueryWatchTextOption](p.arena)
			textOpt.TypeSpecified = true

			if _, ok := p.accept(tokExact); ok {
				textOpt.Type = ast.WatchExact
			} else if _, ok := p.accept(tokSimilar); ok {
				textOpt.Type = ast.WatchSimilar
			} else if _, ok := p.accept(tokPlan); ok {
				textOpt.Type = ast.WatchPlan
			}

			p.expect(tokTo)
			textOpt.PatternExpr = p.parseExpression(precNone)
			opt.TextOption = textOpt
			return opt
		}
	} else if _, ok := p.accept(tokPlan); ok {
		if _, ok := p.accept(tokDigest); ok {
			// PLAN DIGEST '...'
			opt.Tp = ast.QueryWatchType
			textOpt := Alloc[ast.QueryWatchTextOption](p.arena)
			textOpt.Type = ast.WatchPlan
			textOpt.PatternExpr = p.parseExpression(precNone)
			opt.TextOption = textOpt
			return opt
		}
	}

	return nil
}

// parseCompactTableStmt parses ALTER TABLE ... COMPACT
// This is a helper called by parseAlterTableStmt
func (p *HandParser) parseCompactTableStmt(table *ast.TableName) *ast.CompactTableStmt {
	stmt := Alloc[ast.CompactTableStmt](p.arena)
	stmt.Table = table
	stmt.ReplicaKind = ast.CompactReplicaKindAll // Default: compact all replicas

	// Parse partition names (Optional)
	// COMPACT PARTITION p1, p2
	if _, ok := p.accept(tokPartition); ok {
		stmt.PartitionNames = p.parseIdentList()
	}

	// Parse ReplicaKind
	// COMPACT [TIFLASH|TIKV] REPLICA
	// tiflash is keyword token 58192, tikv is identifier, replica is keyword token 57865
	if _, ok := p.accept(tokTiFlash); ok {
		p.accept(tokReplica) // REPLICA keyword token
		stmt.ReplicaKind = ast.CompactReplicaKindTiFlash
	} else if p.peek().IsKeyword("TIKV") {
		p.next()
		p.accept(tokReplica) // REPLICA keyword token
		stmt.ReplicaKind = ast.CompactReplicaKindTiKV
	}

	return stmt
}
