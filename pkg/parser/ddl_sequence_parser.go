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
	p.expect(create)
	p.expect(sequence)

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
	p.expect(alter)
	p.expect(sequence)

	stmt.IfExists = p.acceptIfExists()

	stmt.Name = p.parseTableName()
	if stmt.Name == nil {
		return nil
	}

	for {
		opt := p.parseAlterSequenceOption()
		if opt == nil {
			break
		}
		stmt.SeqOptions = append(stmt.SeqOptions, opt)
	}
	if len(stmt.SeqOptions) == 0 {
		p.syntaxErrorAt(p.peek())
		return nil
	}
	return stmt
}

// parseDropSequenceStmt parses DROP SEQUENCE statements.
func (p *HandParser) parseDropSequenceStmt() ast.StmtNode {
	stmt := Alloc[ast.DropSequenceStmt](p.arena)
	// p.expect(drop) - already consumed
	p.expect(sequence)

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
	p.expect(alter)
	p.expect(instance)

	stmt := &ast.AlterInstanceStmt{
		ReloadTLS: true,
	}

	// Expect RELOAD (token or identifier)
	tok := p.next()
	if tok.Tp != reload && !tok.IsKeyword("RELOAD") {
		p.syntaxErrorAt(tok)
		return nil
	}

	// Expect TLS (token or identifier)
	tok = p.next()
	if tok.Tp != tls && !tok.IsKeyword("TLS") {
		p.syntaxErrorAt(tok)
		return nil
	}

	// Optional NO ROLLBACK ON ERROR
	if _, ok := p.accept(no); ok {
		p.expect(rollback)
		p.expect(on)
		p.expect(errorKwd)
		stmt.NoRollbackOnError = true
	}

	return stmt
}

// parseAlterRangeStmt parses ALTER RANGE statements.
func (p *HandParser) parseAlterRangeStmt() ast.StmtNode {
	p.expect(alter)
	p.expect(rangeKwd)

	stmt := &ast.AlterRangeStmt{}

	// Parse range name (global/meta)
	// global=float4Type (global). meta=statsMeta (if defined) or identifier.
	// We handle identifiers and specific keywords.
	tok := p.next()
	if tok.Tp == identifier {
		stmt.RangeName = ast.NewCIStr(tok.Lit)
	} else if tok.Tp == global {
		stmt.RangeName = ast.NewCIStr("global")
	} else if tok.Tp == statsMeta { // statsMeta
		stmt.RangeName = ast.NewCIStr("meta")
	} else {
		// Fallback for other keywords like 'meta' if not statsMeta
		stmt.RangeName = ast.NewCIStr(tok.Lit)
	}

	if _, ok := p.accept(placement); ok {
		p.expect(policy)
		p.accept(eq)
		// Policy name
		tok := p.next()
		if tok.Tp == identifier {
			stmt.PlacementOption = &ast.PlacementOption{
				Tp:       ast.PlacementOptionPolicy,
				StrValue: tok.Lit,
			}
		} else if tok.Tp == defaultKwd {
			stmt.PlacementOption = &ast.PlacementOption{
				Tp:       ast.PlacementOptionPolicy,
				StrValue: "default",
			}
		} else {
			p.syntaxErrorAt(tok)
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
	if _, ok := p.accept(increment); ok {
		if _, ok := p.accept(by); !ok {
			p.accept(eq) // optional =
		}
		opt.Tp = ast.SequenceOptionIncrementBy
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(start); ok {
		if _, ok := p.accept(with); !ok {
			p.accept(eq) // optional =
		}
		opt.Tp = ast.SequenceStartWith
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(minValue); ok {
		opt.Tp = ast.SequenceMinValue
		p.accept(eq) // optional =
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(nominvalue); ok {
		opt.Tp = ast.SequenceNoMinValue
		return opt
	} else if _, ok := p.accept(no); ok {
		if _, ok := p.accept(minValue); ok {
			opt.Tp = ast.SequenceNoMinValue
			return opt
		} else if _, ok := p.accept(maxValue); ok {
			opt.Tp = ast.SequenceNoMaxValue
			return opt
		} else if _, ok := p.accept(cache); ok {
			opt.Tp = ast.SequenceNoCache
			return opt
		} else if _, ok := p.accept(cycle); ok {
			opt.Tp = ast.SequenceNoCycle
			return opt
		}
		p.syntaxErrorAt(p.peek())
		return nil
	} else if _, ok := p.accept(maxValue); ok {
		opt.Tp = ast.SequenceMaxValue
		p.accept(eq)
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(nomaxvalue); ok {
		opt.Tp = ast.SequenceNoMaxValue
		return opt
	} else if _, ok := p.accept(cache); ok {
		opt.Tp = ast.SequenceCache
		p.accept(eq)
		opt.IntValue = p.parseIntLit()
		return opt
	} else if _, ok := p.accept(nocache); ok {
		opt.Tp = ast.SequenceNoCache
		return opt
	} else if _, ok := p.accept(cycle); ok {
		opt.Tp = ast.SequenceCycle
		return opt
	} else if _, ok := p.accept(nocycle); ok {
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
	if _, ok := p.accept(restart); ok {
		if _, ok := p.accept(with); ok {
			opt.Tp = ast.SequenceRestartWith
			opt.IntValue = p.parseIntLit()
			return opt
		} else if _, ok := p.accept(eq); ok {
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
	if tok, ok := p.expectAny(intLit); ok {
		var val uint64
		switch v := tok.Item.(type) {
		case int64:
			val = uint64(v)
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
		}
		// val > 9223372036854775807 overflows
		if val > 9223372036854775807 {
			p.error(tok.Offset, "constant %d overflows int64", val)
			return 0
		}
		return int64(val)
	}
	return 0
}
