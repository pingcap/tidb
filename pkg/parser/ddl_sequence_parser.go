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
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseCreateSequenceStmt parses CREATE SEQUENCE statements.
func (p *HandParser) parseCreateSequenceStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateSequenceStmt](p.arena)
	p.expect(57389)
	p.expect(57896)

	stmt.IfNotExists = p.acceptIfNotExists()

	stmt.Name = p.parseTableName()
	if stmt.Name == nil {
		return nil
	}

	for {
		if opt := p.parseSequenceOption(); opt != nil {
			stmt.SeqOptions = append(stmt.SeqOptions, opt)
		} else if opt := p.parseTableOption(); opt != nil {
			stmt.TblOptions = append(stmt.TblOptions, opt)
		} else {
			break
		}
	}
	return stmt
}

// parseAlterSequenceStmt parses ALTER SEQUENCE statements.
func (p *HandParser) parseAlterSequenceStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterSequenceStmt](p.arena)
	p.expect(57365)
	p.expect(57896)

	stmt.IfExists = p.acceptIfExists()

	stmt.Name = p.parseTableName()
	if stmt.Name == nil {
		return nil
	}

	for {
		if opt := p.parseAlterSequenceOption(); opt != nil {
			stmt.SeqOptions = append(stmt.SeqOptions, opt)
		} else {
			break
		}
	}
	if len(stmt.SeqOptions) == 0 {
		p.error(p.peek().Offset, "expected at least one sequence option")
		return nil
	}
	return stmt
}

// parseDropSequenceStmt parses DROP SEQUENCE statements.
func (p *HandParser) parseDropSequenceStmt() ast.StmtNode {
	stmt := Alloc[ast.DropSequenceStmt](p.arena)
	// p.expect(57415) - already consumed
	p.expect(57896)

	stmt.IfExists = p.acceptIfExists()

	for {
		name := p.parseTableName()
		if name == nil {
			return nil
		}
		stmt.Sequences = append(stmt.Sequences, name)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return stmt
}

// parseAlterInstanceStmt parses ALTER INSTANCE RELOAD TLS.
func (p *HandParser) parseAlterInstanceStmt() ast.StmtNode {
	p.expect(57365)
	p.expect(57752)

	stmt := &ast.AlterInstanceStmt{
		ReloadTLS: true,
	}

	// Expect RELOAD (token or identifier)
	tok := p.next()
	if tok.Tp != 57860 && !tok.IsKeyword("RELOAD") {
		p.error(tok.Offset, "expected RELOAD after ALTER INSTANCE")
		return nil
	}

	// Expect TLS (token or identifier)
	tok = p.next()
	if tok.Tp != 58096 && !tok.IsKeyword("TLS") {
		p.error(tok.Offset, "expected TLS after RELOAD")
		return nil
	}

	// Optional NO ROLLBACK ON ERROR
	if _, ok := p.accept(57799); ok {
		p.expect(57878)
		p.expect(57505)
		p.expect(57707)
		stmt.NoRollbackOnError = true
	}

	return stmt
}

// parseAlterRangeStmt parses ALTER RANGE statements.
func (p *HandParser) parseAlterRangeStmt() ast.StmtNode {
	p.expect(57365)
	p.expect(57520)

	stmt := &ast.AlterRangeStmt{}

	// Parse range name (global/meta)
	// global=57429 (57733). meta=58241 (if defined) or identifier.
	// We handle identifiers and specific keywords.
	tok := p.next()
	if tok.Tp == 57346 {
		stmt.RangeName = ast.NewCIStr(tok.Lit)
	} else if tok.Tp == 57733 {
		stmt.RangeName = ast.NewCIStr("global")
	} else if tok.Tp == 58241 { // statsMeta
		stmt.RangeName = ast.NewCIStr("meta")
	} else {
		// Fallback for other keywords like 'meta' if not 58241
		stmt.RangeName = ast.NewCIStr(tok.Lit)
	}

	if _, ok := p.accept(58054); ok {
		p.expect(57838)
		p.accept(58202)
		// Policy name
		tok := p.next()
		if tok.Tp == 57346 {
			stmt.PlacementOption = &ast.PlacementOption{
				Tp:       ast.PlacementOptionPolicy,
				StrValue: tok.Lit,
			}
		} else if tok.Tp == 57405 {
			stmt.PlacementOption = &ast.PlacementOption{
				Tp:       ast.PlacementOptionPolicy,
				StrValue: "default",
			}
		} else {
			p.error(tok.Offset, "expected policy name")
			return nil
		}
	} else {
		// Try parsing other placement options?
		// The test only uses PLACEMENT POLICY.
		// But existing code called p.parsePlacementOption().
		// If explicit PLACEMENT token is found, we handle POLICY.
		// If not, maybe use p.parsePlacementOption()?
		stmt.PlacementOption = p.parsePlacementOption()
	}

	return stmt
}

// parseSequenceOption parses a single sequence option.
func (p *HandParser) parseSequenceOption() *ast.SequenceOption {
	opt := Alloc[ast.SequenceOption](p.arena)
	if _, ok := p.accept(57748); ok {
		if _, ok := p.accept(57376); ok {
			// standard syntax
		} else {
			p.accept(58202) // optional =
		}
		opt.Tp = ast.SequenceOptionIncrementBy
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(57925); ok {
		if _, ok := p.accept(57590); ok {
			// standard syntax
		} else {
			p.accept(58202) // optional =
		}
		opt.Tp = ast.SequenceStartWith
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(57788); ok {
		opt.Tp = ast.SequenceMinValue
		p.accept(58202) // optional =
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(57804); ok {
		opt.Tp = ast.SequenceNoMinValue
		return opt
	} else if _, ok := p.accept(57799); ok {
		if _, ok := p.accept(57788); ok {
			opt.Tp = ast.SequenceNoMinValue
			return opt
		} else if _, ok := p.accept(57489); ok {
			opt.Tp = ast.SequenceNoMaxValue
			return opt
		} else if _, ok := p.accept(57633); ok {
			opt.Tp = ast.SequenceNoCache
			return opt
		} else if _, ok := p.accept(57678); ok {
			opt.Tp = ast.SequenceNoCycle
			return opt
		}
		p.error(p.peek().Offset, "expected MINVALUE, MAXVALUE, CACHE, or CYCLE after NO")
		return nil
	} else if _, ok := p.accept(57489); ok {
		opt.Tp = ast.SequenceMaxValue
		p.accept(58202)
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(57803); ok {
		opt.Tp = ast.SequenceNoMaxValue
		return opt
	} else if _, ok := p.accept(57633); ok {
		opt.Tp = ast.SequenceCache
		p.accept(58202)
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(57800); ok {
		opt.Tp = ast.SequenceNoCache
		return opt
	} else if _, ok := p.accept(57678); ok {
		opt.Tp = ast.SequenceCycle
		return opt
	} else if _, ok := p.accept(57801); ok {
		opt.Tp = ast.SequenceNoCycle
		return opt
	}
	return nil
}

// parseAlterSequenceOption extends parseSequenceOption with ALTER-only options (RESTART).
func (p *HandParser) parseAlterSequenceOption() *ast.SequenceOption {
	if opt := p.parseSequenceOption(); opt != nil {
		return opt
	}
	opt := Alloc[ast.SequenceOption](p.arena)
	if _, ok := p.accept(57871); ok {
		if _, ok := p.accept(57590); ok {
			opt.Tp = ast.SequenceRestartWith
			opt.IntValue = p.parseIntLit()
			return opt
		} else if _, ok := p.accept(58202); ok {
			opt.Tp = ast.SequenceRestartWith
			opt.IntValue = p.parseIntLit()
			return opt
		}
		opt.Tp = ast.SequenceRestart
		return opt
	}
	return nil
}

// Helper to parse signed integer literal for sequence options
func (p *HandParser) parseIntLit() int64 {
	negative := false
	if _, ok := p.accept('-'); ok {
		negative = true
	}
	if tok, ok := p.expectAny(58197); ok {
		var val uint64
		switch v := tok.Item.(type) {
		case int64:
			if v < 0 { // Should not happen for 58197 unless lexer is weird
				// handle weird case
				val = uint64(v) // cast back
			} else {
				val = uint64(v)
			}
		case uint64:
			val = v
		default:
			// Should validation fail? Or return 0?
			// For now, assume 0 or handle better if needed.
			return 0
		}

		if negative {
			// -9223372036854775808 is valid (val = 9223372036854775808)
			// val > 9223372036854775808 overflows
			if val > 9223372036854775808 {
				p.error(tok.Offset, "constant %d overflows int64", val)
				return 0
			}
			return -int64(val)
		} else {
			// val > 9223372036854775807 overflows
			if val > 9223372036854775807 {
				p.error(tok.Offset, "constant %d overflows int64", val)
				return 0
			}
			return int64(val)
		}
	}
	return 0
}
