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

// getMaskingPolicyRestrictOp maps a restrict operation name to its bitmask value.
func getMaskingPolicyRestrictOp(name string) (ast.MaskingPolicyRestrictOps, bool) {
	switch strings.ToUpper(name) {
	case ast.MaskingPolicyRestrictNameInsertIntoSelect:
		return ast.MaskingPolicyRestrictOpInsertIntoSelect, true
	case ast.MaskingPolicyRestrictNameUpdateSelect:
		return ast.MaskingPolicyRestrictOpUpdateSelect, true
	case ast.MaskingPolicyRestrictNameDeleteSelect:
		return ast.MaskingPolicyRestrictOpDeleteSelect, true
	case ast.MaskingPolicyRestrictNameCTAS:
		return ast.MaskingPolicyRestrictOpCTAS, true
	}
	return ast.MaskingPolicyRestrictOpNone, false
}

// parseCreateMaskingPolicyStmt parses:
//
//	CREATE [OR REPLACE] MASKING POLICY [IF NOT EXISTS] PolicyName
//	  ON TableName '(' Identifier ')' AS Expression
//	  [RESTRICT ON '(' RestrictOperationList ')' | RESTRICT ON NONE]
//	  [ENABLE | DISABLE]
func (p *HandParser) parseCreateMaskingPolicyStmt() ast.StmtNode {
	stmt := &ast.CreateMaskingPolicyStmt{}

	p.expect(create)
	if _, ok := p.accept(or); ok {
		p.expect(replace)
		stmt.OrReplace = true
	}
	p.expect(masking)
	p.expect(policy)

	stmt.IfNotExists = p.acceptIfNotExists()

	if stmt.OrReplace && stmt.IfNotExists {
		p.syntaxErrorAt(p.peek())
		return nil
	}

	// PolicyName: Identifier
	if tok, ok := p.expectIdentLike(); ok {
		stmt.PolicyName = ast.NewCIStr(tok.Lit)
	}

	// ON TableName
	p.expect(on)
	stmt.Table = p.parseTableName()

	// '(' Identifier ')'
	p.expect('(')
	if tok, ok := p.expectIdentLike(); ok {
		stmt.Column = p.arena.AllocColumnName()
		stmt.Column.Name = ast.NewCIStr(tok.Lit)
	}
	p.expect(')')

	// AS Expression
	p.expect(as)
	stmt.Expr = p.parseExpression(precNone)

	// Optional: RESTRICT ON ...
	stmt.RestrictOps = p.parseMaskingPolicyRestrictOnOpt()

	// Optional: ENABLE | DISABLE
	stmt.MaskingPolicyState = p.parseMaskingPolicyStateOpt()

	return stmt
}

// parseMaskingPolicyRestrictOnOpt parses the optional RESTRICT ON clause:
//
//	[RESTRICT ON '(' RestrictOperationList ')']
//	[RESTRICT ON NONE]
func (p *HandParser) parseMaskingPolicyRestrictOnOpt() ast.MaskingPolicyRestrictOps {
	if _, ok := p.accept(restrict); !ok {
		return ast.MaskingPolicyRestrictOpNone
	}
	p.expect(on)
	if _, ok := p.accept(none); ok {
		return ast.MaskingPolicyRestrictOpNone
	}
	p.expect('(')
	ops := p.parseMaskingPolicyRestrictOperationList()
	p.expect(')')
	return ops
}

// parseMaskingPolicyRestrictOperationList parses a comma-separated list of restrict operations.
func (p *HandParser) parseMaskingPolicyRestrictOperationList() ast.MaskingPolicyRestrictOps {
	var ops ast.MaskingPolicyRestrictOps
	for {
		tok, ok := p.expectIdentLike()
		if !ok {
			break
		}
		op, valid := getMaskingPolicyRestrictOp(tok.Lit)
		if !valid {
			p.error(tok.Offset, "unsupported masking policy restrict operation: %s", tok.Lit)
			break
		}
		ops |= op
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return ops
}

// parseMaskingPolicyStateOpt parses the optional ENABLE/DISABLE state.
func (p *HandParser) parseMaskingPolicyStateOpt() ast.MaskingPolicyState {
	if _, ok := p.accept(enable); ok {
		return ast.MaskingPolicyState{Enabled: true, Explicit: true}
	}
	if _, ok := p.accept(disable); ok {
		return ast.MaskingPolicyState{Enabled: false, Explicit: true}
	}
	// Default: enabled but not explicitly stated.
	return ast.MaskingPolicyState{Enabled: true, Explicit: false}
}

// parseAlterAddMaskingPolicy parses:
//
//	ADD MASKING POLICY PolicyName ON '(' Identifier ')' AS Expression
//	  [RESTRICT ON ...] [ENABLE | DISABLE]
//
// Caller has already consumed ADD and confirmed MASKING POLICY follows.
func (p *HandParser) parseAlterAddMaskingPolicy(spec *ast.AlterTableSpec) {
	// MASKING already consumed by caller
	p.expect(policy)
	spec.Tp = ast.AlterTableAddMaskingPolicy

	if tok, ok := p.expectIdentLike(); ok {
		spec.MaskingPolicyName = ast.NewCIStr(tok.Lit)
	}

	// ON '(' Identifier ')'
	p.expect(on)
	p.expect('(')
	if tok, ok := p.expectIdentLike(); ok {
		spec.MaskingPolicyColumn = p.arena.AllocColumnName()
		spec.MaskingPolicyColumn.Name = ast.NewCIStr(tok.Lit)
	}
	p.expect(')')

	// AS Expression
	p.expect(as)
	spec.MaskingPolicyExpr = p.parseExpression(precNone)

	// Optional: RESTRICT ON ...
	spec.MaskingPolicyRestrictOps = p.parseMaskingPolicyRestrictOnOpt()

	// Optional: ENABLE | DISABLE
	spec.MaskingPolicyState = p.parseMaskingPolicyStateOpt()
}

// parseAlterDropMaskingPolicy parses:
//
//	DROP MASKING POLICY PolicyName
//
// Caller has already consumed DROP MASKING POLICY.
func (p *HandParser) parseAlterDropMaskingPolicy(spec *ast.AlterTableSpec) {
	spec.Tp = ast.AlterTableDropMaskingPolicy
	if tok, ok := p.expectIdentLike(); ok {
		spec.MaskingPolicyName = ast.NewCIStr(tok.Lit)
	}
}

// parseAlterModifyMaskingPolicy parses:
//
//	MODIFY MASKING POLICY PolicyName SET EXPRESSION = Expression
//	MODIFY MASKING POLICY PolicyName SET RESTRICT ON '(' RestrictOperationList ')'
//	MODIFY MASKING POLICY PolicyName SET RESTRICT ON NONE
//
// Caller has already consumed MODIFY MASKING POLICY.
func (p *HandParser) parseAlterModifyMaskingPolicy(spec *ast.AlterTableSpec) {
	if tok, ok := p.expectIdentLike(); ok {
		spec.MaskingPolicyName = ast.NewCIStr(tok.Lit)
	}

	p.expect(set)

	// Disambiguate: SET RESTRICT ON ... vs SET Identifier = Expression
	if _, ok := p.accept(restrict); ok {
		// SET RESTRICT ON '(' ... ')' | SET RESTRICT ON NONE
		spec.Tp = ast.AlterTableModifyMaskingPolicyRestrictOn
		p.expect(on)
		if _, ok := p.accept(none); ok {
			spec.MaskingPolicyRestrictOps = ast.MaskingPolicyRestrictOpNone
		} else {
			p.expect('(')
			spec.MaskingPolicyRestrictOps = p.parseMaskingPolicyRestrictOperationList()
			p.expect(')')
		}
	} else {
		// SET Identifier = Expression — must be "expression"
		tok, ok := p.expectIdentLike()
		if !ok {
			return
		}
		if !strings.EqualFold(tok.Lit, "expression") {
			p.error(tok.Offset, "unsupported masking policy modify option: %s", tok.Lit)
			return
		}
		p.expect(eq)
		spec.Tp = ast.AlterTableModifyMaskingPolicyExpression
		spec.MaskingPolicyExpr = p.parseExpression(precNone)
	}
}
