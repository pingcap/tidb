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

// parseTrafficStmt parses TRAFFIC CAPTURE/REPLAY statements.
func (p *HandParser) parseTrafficStmt() ast.StmtNode {
	p.expect(58107)
	tok := p.next()
	stmt := &ast.TrafficStmt{}

	switch strings.ToUpper(tok.Lit) {
	case "CAPTURE":
		stmt.OpType = ast.TrafficOpCapture
		// TO 'path'
		if _, ok := p.expect(57564); ok {
			if tok, ok := p.expect(57353); ok {
				stmt.Dir = tok.Lit
			}
		} else {
			// expect(57564) already logged error
			return nil
		}

		// Options: DURATION='...', ENCRYPTION_METHOD='...', COMPRESS=true
		for p.peek().Tp != 0 && p.peek().Tp != ';' {
			optTok := p.next()
			optName := strings.ToUpper(optTok.Lit)
			p.accept(58202)
			opt := &ast.TrafficOption{}
			switch optName {
			case "DURATION":
				opt.OptionType = ast.TrafficOptionDuration
				if tok, ok := p.expect(57353); ok {
					opt.StrValue = tok.Lit
				}
				// check duration
				if len(opt.StrValue) > 0 {
					lastChar := opt.StrValue[len(opt.StrValue)-1]
					if lastChar >= '0' && lastChar <= '9' {
						p.error(tok.Offset, "invalid duration %s", opt.StrValue)
					}
				}
			case "ENCRYPTION_METHOD":
				opt.OptionType = ast.TrafficOptionEncryptionMethod
				if tok, ok := p.expect(57353); ok {
					opt.StrValue = tok.Lit
				}
			case "COMPRESS":
				opt.OptionType = ast.TrafficOptionCompress
				boolTok := p.next()

				// Check source for quotes
				isQuoted := false
				if boolTok.Offset < len(p.src) {
					char := p.src[boolTok.Offset]
					if char == '\'' || char == '"' {
						isQuoted = true
					}
				}

				if isQuoted {
					p.error(boolTok.Offset, "invalid boolean value for COMPRESS (string literal not allowed)")
				} else if strings.ToUpper(boolTok.Lit) != "TRUE" && strings.ToUpper(boolTok.Lit) != "FALSE" {
					p.error(boolTok.Offset, "invalid boolean value for COMPRESS: %s", boolTok.Lit)
				}
				opt.BoolValue = strings.ToUpper(boolTok.Lit) == "TRUE"
			default:
				p.error(optTok.Offset, "unknown option %s", optName)
			}
			stmt.Options = append(stmt.Options, opt)
		}
	case "REPLAY":
		stmt.OpType = ast.TrafficOpReplay
		// FROM 'path'
		if _, ok := p.expect(57434); ok {
			if tok, ok := p.expect(57353); ok {
				stmt.Dir = tok.Lit
			}
		} else {
			return nil
		}

		// Options: USER='root', PASSWORD='...', SPEED=1.0, READONLY=true
		for p.peek().Tp != 0 && p.peek().Tp != ';' {
			optTok := p.next()
			optName := strings.ToUpper(optTok.Lit)
			p.accept(58202)
			opt := &ast.TrafficOption{}
			switch optName {
			case "USER":
				opt.OptionType = ast.TrafficOptionUsername
				if tok, ok := p.expect(57353); ok {
					opt.StrValue = tok.Lit
				}
			case "PASSWORD":
				opt.OptionType = ast.TrafficOptionPassword
				if tok, ok := p.expect(57353); ok {
					opt.StrValue = tok.Lit
				}
			case "SPEED":
				opt.OptionType = ast.TrafficOptionSpeed
				val := p.next()
				opt.StrValue = val.Lit
				if f, err := strconv.ParseFloat(val.Lit, 64); err == nil {
					if ast.NewValueExpr != nil {
						opt.FloatValue = ast.NewValueExpr(f, "", "")
					}
				} else {
					p.error(val.Offset, "invalid float value for SPEED: %s", val.Lit)
				}
			case "READ_ONLY", "READONLY":
				opt.OptionType = ast.TrafficOptionReadOnly
				boolTok := p.next()
				opt.BoolValue = strings.ToUpper(boolTok.Lit) == "TRUE"
			default:
				p.error(optTok.Offset, "unknown option %s", optName)
			}
			stmt.Options = append(stmt.Options, opt)
		}
	default:
		p.error(tok.Offset, "expected CAPTURE or REPLAY after TRAFFIC")
		return nil
	}

	return stmt
}

// parseRefreshStmt parses REFRESH STATS obj_list [FULL|LITE] [CLUSTER].
func (p *HandParser) parseRefreshStmt() ast.StmtNode {
	p.expect(57859)

	// STATS keyword (identifier, not reserved).
	tok := p.next()
	if !tok.IsKeyword("STATS") {
		p.error(tok.Offset, "expected STATS after REFRESH")
		return nil
	}

	stmt := &ast.RefreshStatsStmt{}

	// Parse object list: *.*  |  db.*  |  db.table  |  table
	for {
		obj := &ast.RefreshObject{}

		firstTok := p.next()
		if firstTok.Tp == '*' || firstTok.Lit == "*" {
			// *.* (global)
			p.expect('.')
			p.expect('*')
			obj.RefreshObjectScope = ast.RefreshObjectScopeGlobal
		} else {
			// Could be db.* or db.table or table
			name := firstTok.Lit
			if _, ok := p.accept('.'); ok {
				// db.* or db.table
				if _, ok := p.accept('*'); ok {
					obj.RefreshObjectScope = ast.RefreshObjectScopeDatabase
					obj.DBName = ast.NewCIStr(name)
				} else {
					tblTok := p.next()
					obj.RefreshObjectScope = ast.RefreshObjectScopeTable
					obj.DBName = ast.NewCIStr(name)
					obj.TableName = ast.NewCIStr(tblTok.Lit)
				}
			} else {
				// Just table name.
				obj.RefreshObjectScope = ast.RefreshObjectScopeTable
				obj.TableName = ast.NewCIStr(name)
			}
		}

		stmt.RefreshObjects = append(stmt.RefreshObjects, obj)

		if _, ok := p.accept(','); !ok {
			break
		}
	}

	// Optional FULL | LITE mode.
	if tok := p.peek(); tok.IsKeyword("FULL") {
		p.next()
		mode := ast.RefreshStatsModeFull
		stmt.RefreshMode = &mode
	} else if tok.IsKeyword("LITE") {
		p.next()
		mode := ast.RefreshStatsModeLite
		stmt.RefreshMode = &mode
	}

	// Optional CLUSTER.
	if tok := p.peek(); tok.IsKeyword("CLUSTER") {
		p.next()
		stmt.IsClusterWide = true
	}

	return stmt
}

// parseShowTrafficStmt parses SHOW TRAFFIC JOBS.
func (p *HandParser) parseShowTrafficStmt() ast.StmtNode {
	return p.parseTrafficJobStmt(ast.TrafficOpShow)
}

// parseCancelTrafficStmt parses CANCEL TRAFFIC JOBS.
func (p *HandParser) parseCancelTrafficStmt() ast.StmtNode {
	return p.parseTrafficJobStmt(ast.TrafficOpCancel)
}

// parseTrafficJobStmt is the shared implementation for SHOW/CANCEL TRAFFIC JOBS.
func (p *HandParser) parseTrafficJobStmt(opType ast.TrafficOpType) ast.StmtNode {
	p.expect(58107)
	p.expect(58167)
	return &ast.TrafficStmt{OpType: opType}
}
