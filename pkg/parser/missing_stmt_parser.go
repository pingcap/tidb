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

// parseKillStmt parses:  KILL [TIDB] [CONNECTION | QUERY] expr
func (p *HandParser) parseKillStmt() ast.StmtNode {
	p.next() // consume KILL (may be kill or identifier)
	stmt := Alloc[ast.KillStmt](p.arena)

	// Check for TIDB prefix
	if _, ok := p.acceptKeyword(tidb, "TIDB"); ok {
		stmt.TiDBExtension = true
	}

	// Optional CONNECTION or QUERY
	if _, ok := p.accept(connection); ok {
		// CONNECTION is the default (Query=false)
	} else if _, ok := p.accept(query); ok {
		stmt.Query = true
	}

	stmt.Expr = p.parseExpression(precNone)
	return stmt
}

// parseTraceStmt parses: TRACE [FORMAT = format_string | PLAN [TARGET = target]] stmt
func (p *HandParser) parseTraceStmt() ast.StmtNode {
	p.next() // consume TRACE

	stmt := Alloc[ast.TraceStmt](p.arena)
	stmt.Format = "row" // default format — Restore skips FORMAT when "row"

	// Check for PLAN
	if p.peek().IsKeyword("PLAN") {
		p.next()
		stmt.TracePlan = true
		// Optional TARGET = '...'
		if p.peek().IsKeyword("TARGET") {
			p.next()
			p.expectAny(eq, assignmentEq)
			stmt.TracePlanTarget = p.next().Lit
		}
	} else if p.peek().Tp == format {
		// Check for FORMAT = '...'
		p.next()
		p.expectAny(eq, assignmentEq)
		formatTok := p.next()
		stmt.Format = formatTok.Lit
	}

	// The rest is a SQL statement
	innerStartOff := p.peek().Offset
	stmt.Stmt = p.parseStatement()
	if stmt.Stmt == nil {
		return nil
	}
	// Set text on inner statement so stmt.Stmt.Text() returns original SQL fragment
	innerEndOff := p.peek().Offset
	if p.peek().Tp == EOF {
		innerEndOff = len(p.src)
	}
	if innerEndOff > innerStartOff {
		stmt.Stmt.SetText(nil, strings.TrimRight(p.src[innerStartOff:innerEndOff], "; \t\n"))
	}
	return stmt
}

// parseRecommendStmt parses: RECOMMEND INDEX [RUN [duration] | STATUS | CANCEL | SET opt=val, ...]
func (p *HandParser) parseRecommendStmt() ast.StmtNode {
	p.next() // consume RECOMMEND

	stmt := Alloc[ast.RecommendIndexStmt](p.arena)

	// INDEX keyword
	if p.peek().Tp == index {
		p.next()
	}

	// Sub-command — use IsKeyword to match regardless of token type.
	// Action must be lowercase to match Restore() expectations.
	if p.peek().IsKeyword("RUN") {
		p.next()
		stmt.Action = "run"
		// Optional FOR sql_string
		if _, ok := p.accept(forKwd); ok {
			if p.peek().Tp == stringLit {
				stmt.SQL = p.next().Lit
			}
		}
		// Optional WITH opt = val, ...
		if _, ok := p.accept(with); ok {
			stmt.Options = p.parseRecommendOptions()
		}
	} else if p.peek().IsKeyword("STATUS") || p.peek().IsKeyword("CANCEL") {
		stmt.Action = strings.ToLower(p.next().Lit)
	} else if p.peek().IsKeyword("SHOW") {
		p.next()
		stmt.Action = "show"
		// Consume OPTION if present
		if p.peek().IsKeyword("OPTION") {
			p.next()
		}
	} else if p.peek().IsKeyword("APPLY") || p.peek().IsKeyword("IGNORE") {
		stmt.Action = strings.ToLower(p.next().Lit)
		if p.peek().Tp == intLit {
			val, _ := strconv.ParseInt(p.next().Lit, 10, 64)
			stmt.ID = val
		}
	} else if p.peek().IsKeyword("SET") {
		p.next()
		stmt.Action = "set"
		stmt.Options = p.parseRecommendOptions()
	} else {
		// Unknown sub-command, consume and use its literal
		subTok := p.next()
		stmt.Action = strings.ToLower(subTok.Lit)
	}

	return stmt
}

// parseFlushStmt parses: FLUSH [NO_WRITE_TO_BINLOG | LOCAL] flush_option
func (p *HandParser) parseFlushStmt() ast.StmtNode {
	p.next() // consume FLUSH

	stmt := Alloc[ast.FlushStmt](p.arena)

	// Optional NO_WRITE_TO_BINLOG or LOCAL
	stmt.NoWriteToBinLog = p.acceptNoWriteToBinlog()

	// Flush target
	switch p.peek().Tp {
	case tables, tableKwd:
		p.next()
		stmt.Tp = ast.FlushTables
		// Optional table names
		if p.peek().IsIdent() && p.peek().Tp != with {
			for {
				tbl := p.parseTableName()
				if tbl == nil {
					break
				}
				stmt.Tables = append(stmt.Tables, tbl)
				if _, ok := p.accept(','); !ok {
					break
				}
			}
		}
		// WITH READ LOCK
		if _, ok := p.accept(with); ok {
			if _, ok := p.accept(read); ok {
				p.expect(lock)
				stmt.ReadLock = true
			}
		}
	case privileges:
		p.next()
		stmt.Tp = ast.FlushPrivileges
	case status:
		p.next()
		stmt.Tp = ast.FlushStatus
	case binaryType:
		// FLUSH BINARY LOGS
		p.next()
		if p.peek().IsKeyword("LOGS") {
			p.next()
			stmt.Tp = ast.FlushLogs
			stmt.LogType = ast.LogTypeBinary
		}
	default:
		tok := p.peek()
		switch strings.ToUpper(tok.Lit) {
		case "LOGS":
			p.next()
			stmt.Tp = ast.FlushLogs
			// Optional log type after LOGS
			if p.peek().IsIdent() {
				if lt, ok := logTypeFromName(strings.ToUpper(p.peek().Lit)); ok {
					p.next()
					stmt.LogType = lt
				}
			}
		case "ENGINE", "ERROR", "GENERAL", "SLOW":
			// FLUSH ENGINE/ERROR/GENERAL/SLOW LOGS
			lt, _ := logTypeFromName(strings.ToUpper(tok.Lit))
			p.next()
			if p.peek().IsKeyword("LOGS") {
				p.next()
				stmt.Tp = ast.FlushLogs
				stmt.LogType = lt
			}
		case "TIDB":
			p.next()
			if p.peek().IsKeyword("PLUGINS") {
				p.next()
				stmt.Tp = ast.FlushTiDBPlugin
				for {
					if p.peek().IsIdent() {
						stmt.Plugins = append(stmt.Plugins, p.next().Lit)
					}
					if _, ok := p.accept(','); !ok {
						break
					}
				}
			}
		case "CLIENT_ERRORS_SUMMARY":
			p.next()
			stmt.Tp = ast.FlushClientErrorsSummary
		case "HOSTS":
			p.next()
			stmt.Tp = ast.FlushHosts
		case "STATS_DELTA":
			p.next()
			stmt.Tp = ast.FlushStatsDelta
			// Optional CLUSTER
			if p.peek().IsKeyword("CLUSTER") {
				p.next()
				stmt.IsCluster = true
			}
		default:
			p.error(tok.Offset, "unsupported FLUSH target '%s'", tok.Lit)
			return nil
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

// parseDropBindingStmt parses: DROP [GLOBAL | SESSION] BINDING [FOR select_stmt [USING select_stmt] | FOR SQL DIGEST str]
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
	stmt := Alloc[ast.ShowStmt](p.arena)
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

// parseLoadStatsStmt parses: LOAD STATS 'path'
func (p *HandParser) parseLoadStatsStmt() ast.StmtNode {
	p.expect(load)
	// Skip STATS keyword (identifier)
	p.next()
	stmt := Alloc[ast.LoadStatsStmt](p.arena)
	tok := p.next()
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

// parseCalibrateResourceStmt parses: CALIBRATE RESOURCE [WORKLOAD workload_type] [options...]
func (p *HandParser) parseCalibrateResourceStmt() ast.StmtNode {
	p.next() // consume CALIBRATE
	// Expect RESOURCE
	if p.peek().IsKeyword("RESOURCE") {
		p.next()
	}

	stmt := Alloc[ast.CalibrateResourceStmt](p.arena)

	// Parse options: WORKLOAD, START_TIME, END_TIME, DURATION
	for {
		pk := p.peek()
		if pk.Tp == ';' || pk.Tp == EOF {
			break
		}
		// Skip optional comma between options
		if pk.Tp == ',' {
			p.next()
			continue
		}

		// Match by token type (for keyword tokens) or by string (for identifiers)
		switch {
		case pk.Tp == workload || pk.IsKeyword("WORKLOAD"):
			p.next()
			wlTok := p.next()
			switch strings.ToUpper(wlTok.Lit) {
			case "TPCC":
				stmt.Tp = ast.TPCC
			case "OLTP_READ_WRITE":
				stmt.Tp = ast.OLTPREADWRITE
			case "OLTP_READ_ONLY":
				stmt.Tp = ast.OLTPREADONLY
			case "OLTP_WRITE_ONLY":
				stmt.Tp = ast.OLTPWRITEONLY
			case "TPCH_10":
				stmt.Tp = ast.TPCH10
			default:
				p.error(wlTok.Offset, "unknown CALIBRATE workload: %s", wlTok.Lit)
				return nil
			}
		case pk.Tp == startTime || pk.IsKeyword("START_TIME") ||
			pk.Tp == endTime || pk.IsKeyword("END_TIME"):
			var optTp ast.DynamicCalibrateType
			if pk.Tp == startTime || pk.IsKeyword("START_TIME") {
				optTp = ast.CalibrateStartTime
			} else {
				optTp = ast.CalibrateEndTime
			}
			p.next()
			p.acceptEqOrAssign()
			opt := Alloc[ast.DynamicCalibrateResourceOption](p.arena)
			opt.Tp = optTp
			opt.Ts = p.parseExpression(precNone)
			stmt.DynamicCalibrateResourceOptionList = append(stmt.DynamicCalibrateResourceOptionList, opt)
		case pk.IsKeyword("DURATION"):
			p.next()
			p.acceptEqOrAssign()
			opt := Alloc[ast.DynamicCalibrateResourceOption](p.arena)
			opt.Tp = ast.CalibrateDuration
			// Check for string literal or INTERVAL
			if p.peek().Tp == stringLit {
				opt.StrValue = p.next().Lit
			} else if _, ok := p.accept(interval); ok {
				opt.Ts = p.parseExpression(precNone)
				opt.Unit = p.parseTimeUnit().Unit
			} else {
				opt.Ts = p.parseExpression(precNone)
			}
			stmt.DynamicCalibrateResourceOptionList = append(stmt.DynamicCalibrateResourceOptionList, opt)
		default:
			// Unknown option, stop parsing options
			return stmt
		}
	}
	return stmt
}

// parseRecommendOptions parses a comma-separated list of option=value pairs for RECOMMEND INDEX.
func (p *HandParser) parseRecommendOptions() []ast.RecommendIndexOption {
	var opts []ast.RecommendIndexOption
	for {
		var opt ast.RecommendIndexOption
		nameTok := p.next()
		opt.Option = nameTok.Lit
		p.expectAny(eq, assignmentEq)
		opt.Value = p.parseExpression(precNone).(ast.ValueExpr)
		opts = append(opts, opt)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return opts
}

// logTypeFromName maps an uppercased log type name to its ast.LogType constant.
func logTypeFromName(name string) (ast.LogType, bool) {
	switch name {
	case "BINARY":
		return ast.LogTypeBinary, true
	case "ENGINE":
		return ast.LogTypeEngine, true
	case "ERROR":
		return ast.LogTypeError, true
	case "GENERAL":
		return ast.LogTypeGeneral, true
	case "SLOW":
		return ast.LogTypeSlow, true
	}
	return 0, false
}
