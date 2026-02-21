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

// parseSlowQueryStmt parses SLOW QUERY [WHERE ...] [ORDER BY ...] [LIMIT ...].
// Returns a *ast.PlanReplayerStmt which serves as a container for SLOW QUERY.
func (p *HandParser) parseSlowQueryStmt() ast.StmtNode {
	stmt := Alloc[ast.PlanReplayerStmt](p.arena)
	// stmt_parser already peeked "SLOW" (if dispatched from there), but we need to consume it.
	// Since tokSlow might not exist, we consume it as identifier.
	p.next() // consume SLOW
	p.expect(57852)

	if _, ok := p.accept(57587); ok {
		stmt.Where = p.parseExpression(precNone)
	}
	if p.peek().Tp == 57510 {
		stmt.OrderBy = p.parseOrderByClause()
	}
	if p.peek().Tp == 57477 {
		stmt.Limit = p.parseLimitClause()
	}
	return stmt
}

// parseCallStmt parses:
//
//	CALL procedure_name(args...)
func (p *HandParser) parseCallStmt() ast.StmtNode {
	p.expect(57377)
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
	p.expect(57506)

	stmt := Alloc[ast.OptimizeTableStmt](p.arena)
	stmt.NoWriteToBinLog = p.acceptNoWriteToBinlog()

	p.expectAny(57556, 57944)
	stmt.Tables = p.parseTableNameList()
	return stmt
}

// parsePlanReplayerStmt parses PLAN REPLAYER statements.
func (p *HandParser) parsePlanReplayerStmt() ast.StmtNode {
	stmt := Alloc[ast.PlanReplayerStmt](p.arena)
	p.expect(58056)
	p.expect(58066)

	if _, ok := p.accept(57480); ok {
		stmt.Load = true
		if tok, ok := p.expect(57353); ok {
			stmt.File = tok.Lit
		}
		return stmt
	}

	// Check for CAPTURE (keyword token)
	if _, ok := p.accept(57635); ok {
		// CAPTURE [REMOVE]
		if _, ok := p.accept(57861); ok {
			stmt.Remove = true
		} else {
			stmt.Capture = true
		}

		if tok, ok := p.expect(57353); ok {
			stmt.SQLDigest = tok.Lit
		}
		if tok, ok := p.expect(57353); ok {
			stmt.PlanDigest = tok.Lit
		}
		return stmt
	}

	return p.parsePlanReplayerDump(stmt)
}

func (p *HandParser) parsePlanReplayerDump(stmt *ast.PlanReplayerStmt) ast.StmtNode {
	// DUMP is optional
	p.accept(58015)

	// WITH STATS AS OF TIMESTAMP <expr>
	if _, ok := p.accept(57590); ok {
		p.expect(58182)  // STATS keyword token
		p.expect(57347)      // AS OF is a single merged token
		p.expect(57954) // TIMESTAMP keyword token

		stmt.HistoricalStatsInfo = &ast.AsOfClause{}
		stmt.HistoricalStatsInfo.TsExpr = p.parseExpression(precNone)
	}

	p.expect(57424)
	if _, ok := p.accept(57366); ok {
		stmt.Analyze = true
	}

	// Check if next is string literal (File path) - PLAN REPLAYER DUMP EXPLAIN 'file'
	if p.peek().Tp == 57353 {
		tok := p.next()
		stmt.File = tok.Lit
		return stmt
	}

	// Check for parenthesized string list: PLAN REPLAYER DUMP EXPLAIN ('sql1' [, 'sql2' ...])
	if p.peek().Tp == '(' && p.peekN(1).Tp == 57353 {
		p.next() // consume '('
		for {
			tok, ok := p.expect(57353)
			if !ok {
				return nil
			}
			stmt.StmtList = append(stmt.StmtList, tok.Lit)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
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
	p.expect(57852)
	p.expect(58121)

	if _, ok := p.accept(57363); ok {
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

	} else if _, ok := p.accept(57861); ok { // Support REMOVE keyword as per Restore output
		stmt := Alloc[ast.DropQueryWatchStmt](p.arena)
		if tok, ok := p.accept(58197); ok {
			id, _ := strconv.ParseInt(tok.Lit, 10, 64)
			stmt.IntValue = id
		} else {
			// Try RESOURCE GROUP
			if _, ok := p.accept(57869); ok {
				p.expect(57438)
				if p.peek().Tp == 57354 {
					stmt.GroupNameExpr = p.parseExpression(precNone)
				} else if tok, ok := p.expectAny(57346, 57353); ok {
					stmt.GroupNameStr = ast.NewCIStr(tok.Lit)
				}
			} else {
				p.error(p.peek().Offset, "Missing QUERY WATCH REMOVE target")
				return nil
			}
		}
		return stmt
	} else if _, ok := p.accept(57415); ok { // Logic fallback for DROP keyword
		// Check if tests use DROP or REMOVE
		stmt := Alloc[ast.DropQueryWatchStmt](p.arena)
		if tok, ok := p.accept(58197); ok {
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
	if _, ok := p.accept(57869); ok {
		p.expect(57438)
		opt.Tp = ast.QueryWatchResourceGroup
		rgOpt := Alloc[ast.QueryWatchResourceGroupOption](p.arena)

		if p.peek().Tp == 57354 {
			// @var expression
			rgOpt.GroupNameExpr = p.parseExpression(precNone)
		} else if tok, ok := p.expectAny(57346, 57353); ok {
			rgOpt.GroupNameStr = ast.NewCIStr(tok.Lit)
		}
		opt.ResourceGroupOption = rgOpt
		return opt
	}

	// ACTION ...
	// Since 57596 defined in token_alias check it first
	if _, ok := p.accept(57596); ok {
		opt.Tp = ast.QueryWatchAction
		p.accept(58202) // Optional =

		actionOpt := Alloc[ast.ResourceGroupRunawayActionOption](p.arena)

		if _, ok := p.accept(57469); ok {
			actionOpt.Type = ast.RunawayActionKill
		} else if _, ok := p.accept(58006); ok {
			actionOpt.Type = ast.RunawayActionCooldown
		} else if p.peek().IsKeyword("DRYRUN") {
			p.next()
			actionOpt.Type = ast.RunawayActionDryRun
		} else if _, ok := p.accept(58089); ok {
			actionOpt.Type = ast.RunawayActionSwitchGroup
			p.expect('(')
			if tok, ok := p.expectAny(57346, 57353); ok {
				actionOpt.SwitchGroupName = ast.NewCIStr(tok.Lit)
			}
			p.expect(')')
		}

		opt.ActionOption = actionOpt
		return opt
	}

	// SQL TEXT / SQL DIGEST / PLAN DIGEST — use token-based matching
	if _, ok := p.accept(57545); ok {
		if _, ok := p.accept(57687); ok {
			// SQL DIGEST '...'
			opt.Tp = ast.QueryWatchType
			textOpt := Alloc[ast.QueryWatchTextOption](p.arena)
			textOpt.Type = ast.WatchSimilar
			textOpt.PatternExpr = p.parseExpression(precNone)
			opt.TextOption = textOpt
			return opt
		} else if _, ok := p.accept(57949); ok {
			// SQL TEXT [EXACT|SIMILAR] TO '...'
			opt.Tp = ast.QueryWatchType
			textOpt := Alloc[ast.QueryWatchTextOption](p.arena)
			textOpt.TypeSpecified = true

			if _, ok := p.accept(58017); ok {
				textOpt.Type = ast.WatchExact
			} else if _, ok := p.accept(58073); ok {
				textOpt.Type = ast.WatchSimilar
			} else if _, ok := p.accept(58056); ok {
				textOpt.Type = ast.WatchPlan
			}

			p.expect(57564)
			textOpt.PatternExpr = p.parseExpression(precNone)
			opt.TextOption = textOpt
			return opt
		}
	} else if _, ok := p.accept(58056); ok {
		if _, ok := p.accept(57687); ok {
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
	if _, ok := p.accept(57515); ok {
		stmt.PartitionNames = p.parseIdentList()
	}

	// Parse ReplicaKind
	// COMPACT [TIFLASH|TIKV] REPLICA
	// tiflash is keyword token 58192, tikv is identifier, replica is keyword token 57865
	if _, ok := p.accept(58192); ok {
		p.accept(57865) // REPLICA keyword token
		stmt.ReplicaKind = ast.CompactReplicaKindTiFlash
	} else if p.peek().IsKeyword("TIKV") {
		p.next()
		p.accept(57865) // REPLICA keyword token
		stmt.ReplicaKind = ast.CompactReplicaKindTiKV
	}

	return stmt
}
