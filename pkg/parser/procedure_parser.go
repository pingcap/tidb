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
	"github.com/pingcap/tidb/pkg/parser/types"
)

// skipToSemiOrEnd consumes tokens until reaching ';', END, or EOF.
// Used to recover from unparseable inner statements and prevent infinite loops.
func (p *HandParser) skipToSemiOrEnd() {
	for {
		tok := p.peek()
		if tok.Tp == 0 || tok.Tp == ';' || tok.Tp == 57701 || tok.Tp == 57572 {
			return
		}
		// Also stop at ELSEIF, ELSE which terminate IF branches.
		if tok.Tp == 57417 || tok.Tp == 57418 {
			return
		}
		p.next()
	}
}

// parseProcedureStatementsUntil parses inner procedure statements until one of
// the given stop token types is encountered. Used by block, while, repeat, if, case.
func (p *HandParser) parseProcedureStatementsUntil(stopTokens ...int) []ast.StmtNode {
	var stmts []ast.StmtNode
	for {
		tok := p.peek()
		for _, stop := range stopTokens {
			if tok.Tp == stop {
				return stmts
			}
		}
		s := p.parseProcedureInnerStatement()
		if s != nil {
			stmts = append(stmts, s)
		} else {
			p.skipToSemiOrEnd()
		}
		p.accept(';')
	}
}

// wrapWithLabel wraps a statement node in a ProcedureLabelBlock or ProcedureLabelLoop
// if a label is present. Handles the optional end-label consumption.
func (p *HandParser) wrapWithLabel(label string, block ast.StmtNode, isLoop bool) ast.StmtNode {
	if label == "" {
		return block
	}
	endLabel := ""
	if tok := p.peek(); tok.IsIdent() || tok.Tp == 57346 {
		endLabel = p.next().Lit
	}
	if isLoop {
		return &ast.ProcedureLabelLoop{
			LabelName: label,
			Block:     block,
			LabelEnd:  endLabel,
		}
	}
	// For BEGIN...END blocks, use ProcedureLabelBlock
	if pb, ok := block.(*ast.ProcedureBlock); ok {
		return &ast.ProcedureLabelBlock{
			LabelName: label,
			Block:     pb,
			LabelEnd:  endLabel,
		}
	}
	return block
}

// parseCreateProcedureStmt parses:
//
//	CREATE PROCEDURE [IF NOT EXISTS] name ( params ) body
func (p *HandParser) parseCreateProcedureStmt() ast.StmtNode {
	p.expect(57389)
	p.expect(57519)

	stmt := &ast.ProcedureInfo{}

	// [IF NOT EXISTS]
	stmt.IfNotExists = p.acceptIfNotExists()

	stmt.ProcedureName = p.parseTableName()

	// ( params )
	p.expect('(')
	stmt.ProcedureParam = p.parseProcedureParams()
	p.expect(')')

	// Body
	stmt.ProcedureBody = p.parseProcedureBodyStatement()
	return stmt
}

// parseProcedureParams parses procedure parameter list.
func (p *HandParser) parseProcedureParams() []*ast.StoreParameter {
	var params []*ast.StoreParameter
	if p.peek().Tp == ')' {
		return params
	}
	for {
		param := &ast.StoreParameter{}

		// [IN | OUT | INOUT]
		switch p.peek().Tp {
		case 57448:
			p.next()
			param.Paramstatus = ast.MODE_IN
		case 57511:
			p.next()
			param.Paramstatus = ast.MODE_OUT
		case 57452:
			p.next()
			param.Paramstatus = ast.MODE_INOUT
		default:
			param.Paramstatus = ast.MODE_IN
		}

		param.ParamName = p.next().Lit
		param.ParamType = p.parseFieldTypeProcedure()

		params = append(params, param)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return params
}

// parseFieldTypeProcedure parses a field type for procedure params/declarations.
func (p *HandParser) parseFieldTypeProcedure() *types.FieldType {
	tp := p.parseFieldType()
	if tp != nil {
		return tp
	}
	return types.NewFieldType(0)
}

// parseProcedureBodyStatement parses the body of a procedure.
func (p *HandParser) parseProcedureBodyStatement() ast.StmtNode {
	tok := p.peek()

	// Check for label
	if tok.IsIdent() && p.peekN(1).Tp == ':' {
		label := p.next().Lit
		p.next() // consume ':'
		return p.parseLabeledBody(label)
	}

	// Reuse inner statement logic for shared cases (BEGIN/IF/WHILE/REPEAT/CASE)
	switch tok.Tp {
	case 57621, 57445, 57588, 57529, 57379:
		return p.parseProcedureInnerStatement()
	default:
		return p.parseProcedureStatementList()
	}
}

// parseLabeledBody parses a labeled block/loop after "label:".
func (p *HandParser) parseLabeledBody(label string) ast.StmtNode {
	switch p.peek().Tp {
	case 57588:
		return p.parseProcedureWhile(label)
	case 57529:
		return p.parseProcedureRepeat(label)
	default:
		return p.parseProcedureBlock(label)
	}
}

// parseProcedureBlock parses BEGIN ... END [label]
func (p *HandParser) parseProcedureBlock(label string) ast.StmtNode {
	p.expect(57621)

	block := &ast.ProcedureBlock{}
	var vars []ast.DeclNode

	// Parse DECLARE statements first
	for p.peek().Tp == 57684 {
		decl := p.parseProcedureDeclaration()
		if decl != nil {
			vars = append(vars, decl)
		}
		p.accept(';')
	}

	block.ProcedureVars = vars
	block.ProcedureProcStmts = p.parseProcedureStatementsUntil(57701)

	p.expect(57701) // consume END
	return p.wrapWithLabel(label, block, false)
}

// parseProcedureDeclaration parses DECLARE statements.
func (p *HandParser) parseProcedureDeclaration() ast.DeclNode {
	p.expect(57684)

	tok := p.peek()
	if tok.Tp == 57423 {
		return p.parseProcedureHandlerDecl(ast.PROCEDUR_EXIT)
	}
	if tok.Tp == 57387 {
		return p.parseProcedureHandlerDecl(ast.PROCEDUR_CONTINUE)
	}

	firstName := p.next().Lit

	if p.peek().Tp == 57397 {
		return p.parseProcedureCursor(firstName)
	}

	// Variable declaration
	names := []string{firstName}
	for {
		if _, ok := p.accept(','); !ok {
			break
		}
		names = append(names, p.next().Lit)
	}

	decl := &ast.ProcedureDecl{
		DeclNames: names,
		DeclType:  p.parseFieldTypeProcedure(),
	}

	if _, ok := p.accept(57405); ok {
		decl.DeclDefault = p.parseExpression(precNone)
	}
	return decl
}

// parseProcedureHandlerDecl parses: {EXIT|CONTINUE} HANDLER FOR condition [, cond ...] stmt
func (p *HandParser) parseProcedureHandlerDecl(handlerType int) *ast.ProcedureErrorControl {
	p.next() // consume EXIT/CONTINUE
	p.expect(57735)
	p.expect(57431)

	handler := &ast.ProcedureErrorControl{
		ControlHandle: handlerType,
	}

	var conditions []ast.ErrNode
	for {
		cond := p.parseProcedureHandlerCondition()
		if cond != nil {
			conditions = append(conditions, cond)
		}
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	handler.ErrorCon = conditions
	handler.Operate = p.parseProcedureInnerStatement()
	return handler
}

// parseProcedureHandlerCondition parses a handler condition.
func (p *HandParser) parseProcedureHandlerCondition() ast.ErrNode {
	switch p.peek().Tp {
	case 57548:
		p.next()
		return &ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_SQLWARNING}
	case 57546:
		p.next()
		return &ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_SQLEXCEPTION}
	case 57547:
		p.next()
		val := p.next().Lit
		return &ast.ProcedureErrorState{CodeStatus: val}
	case 58197:
		tok := p.next()
		errNum := tokenItemToUint64(tok.Item)
		return &ast.ProcedureErrorVal{ErrorNum: errNum}
	default:
		if p.peek().Tp == 57498 {
			p.next()
			p.next() // "FOUND"
			return &ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_NOT_FOUND}
		}
		return nil
	}
}

// parseProcedureCursor parses: CURSOR FOR select_stmt
func (p *HandParser) parseProcedureCursor(name string) *ast.ProcedureCursor {
	p.expect(57397)
	p.expect(57431)
	cursor := &ast.ProcedureCursor{CurName: name}
	cursor.Selectstring = p.parseSelectStmt()
	return cursor
}

// parseProcedureInnerStatement parses one inner procedure statement.
func (p *HandParser) parseProcedureInnerStatement() ast.StmtNode {
	tok := p.peek()

	if tok.IsIdent() && p.peekN(1).Tp == ':' {
		label := p.next().Lit
		p.next()
		return p.parseLabeledBody(label)
	}

	switch tok.Tp {
	case 57621:
		return p.parseProcedureBlock("")
	case 57445:
		return p.parseProcedureIf()
	case 57588:
		return p.parseProcedureWhile("")
	case 57529:
		return p.parseProcedureRepeat("")
	case 57379:
		return p.parseProcedureCase()
	case 57818:
		return p.parseProcedureOpen()
	case 57647:
		return p.parseProcedureClose()
	case 57426:
		return p.parseProcedureFetch()
	case 57474:
		return p.parseProcedureLeave()
	case 57465:
		return p.parseProcedureIterate()
	default:
		return p.parseStatement()
	}
}

// parseProcedureIf parses: IF expr THEN stmts [ELSEIF ...] [ELSE ...] END IF
func (p *HandParser) parseProcedureIf() ast.StmtNode {
	p.expect(57445)
	ifBlock := p.parseProcedureIfBlock()
	p.expect(57701)
	p.expect(57445)
	return &ast.ProcedureIfInfo{IfBody: ifBlock}
}

// parseProcedureIfBlock parses: expr THEN stmts [ELSEIF ...] [ELSE ...]
func (p *HandParser) parseProcedureIfBlock() *ast.ProcedureIfBlock {
	block := &ast.ProcedureIfBlock{}
	block.IfExpr = p.parseExpression(precNone)
	p.expect(57559)

	block.ProcedureIfStmts = p.parseProcedureStatementsUntil(57701, 57417, 57418)

	tok := p.peek()
	if tok.Tp == 57418 {
		p.next()
		elseIfBlock := p.parseProcedureIfBlock()
		block.ProcedureElseStmt = &ast.ProcedureElseIfBlock{
			ProcedureIfStmt: elseIfBlock,
		}
	} else if _, ok := p.accept(57417); ok {
		block.ProcedureElseStmt = &ast.ProcedureElseBlock{
			ProcedureIfStmts: p.parseProcedureStatementsUntil(57701),
		}
	}
	return block
}

// parseProcedureWhile parses: WHILE expr DO stmts END WHILE [label]
func (p *HandParser) parseProcedureWhile(label string) ast.StmtNode {
	p.expect(57588)

	block := &ast.ProcedureWhileStmt{}
	block.Condition = p.parseExpression(precNone)
	p.expect(57693)

	block.Body = p.parseProcedureStatementsUntil(57701)

	p.expect(57701)
	p.expect(57588)

	return p.wrapWithLabel(label, block, true)
}

// parseProcedureRepeat parses: REPEAT stmts UNTIL expr END REPEAT [label]
func (p *HandParser) parseProcedureRepeat(label string) ast.StmtNode {
	p.expect(57529)

	block := &ast.ProcedureRepeatStmt{}

	block.Body = p.parseProcedureStatementsUntil(57572)

	p.expect(57572)
	block.Condition = p.parseExpression(precNone)
	p.expect(57701)
	p.expect(57529)

	return p.wrapWithLabel(label, block, true)
}

// parseProcedureCase parses: CASE [expr] WHEN ... THEN ... [ELSE ...] END CASE
// Produces SimpleCaseStmt (CASE expr WHEN ...) or SearchCaseStmt (CASE WHEN ...).
func (p *HandParser) parseProcedureCase() ast.StmtNode {
	p.expect(57379)

	hasCondition := p.peek().Tp != 57586
	var condExpr ast.ExprNode
	if hasCondition {
		condExpr = p.parseExpression(precNone)
	}

	// Parse WHEN clauses and ELSE clause.
	if hasCondition {
		return p.parseSimpleCase(condExpr)
	}
	return p.parseSearchCase()
}

// parseSimpleCase parses WHEN clauses for CASE expr WHEN val THEN stmts ...
func (p *HandParser) parseSimpleCase(condExpr ast.ExprNode) ast.StmtNode {
	caseStmt := &ast.SimpleCaseStmt{Condition: condExpr}
	p.parseProcedureCaseWhenElse(func() {
		whenStmt := &ast.SimpleWhenThenStmt{}
		whenStmt.Expr = p.parseExpression(precNone)
		p.expect(57559)
		whenStmt.ProcedureStmts = p.parseProcedureStatementsUntil(57586, 57701, 57417)
		caseStmt.WhenCases = append(caseStmt.WhenCases, whenStmt)
	}, func(elseStmts []ast.StmtNode) {
		caseStmt.ElseCases = elseStmts
	})
	return caseStmt
}

// parseSearchCase parses WHEN clauses for CASE WHEN cond THEN stmts ...
func (p *HandParser) parseSearchCase() ast.StmtNode {
	caseStmt := &ast.SearchCaseStmt{}
	p.parseProcedureCaseWhenElse(func() {
		whenStmt := &ast.SearchWhenThenStmt{}
		whenStmt.Expr = p.parseExpression(precNone)
		p.expect(57559)
		whenStmt.ProcedureStmts = p.parseProcedureStatementsUntil(57586, 57701, 57417)
		caseStmt.WhenCases = append(caseStmt.WhenCases, whenStmt)
	}, func(elseStmts []ast.StmtNode) {
		caseStmt.ElseCases = elseStmts
	})
	return caseStmt
}

// parseProcedureCaseWhenElse drives the WHEN/ELSE/END CASE loop for procedure CASE statements.
// parseWhen is called for each WHEN clause (after consuming WHEN token).
// setElse is called with the ELSE clause body if present.
func (p *HandParser) parseProcedureCaseWhenElse(parseWhen func(), setElse func([]ast.StmtNode)) {
	for p.peek().Tp == 57586 {
		p.next()
		parseWhen()
	}
	if _, ok := p.accept(57417); ok {
		setElse(p.parseProcedureCaseElse())
	}
	p.expect(57701)
	p.expect(57379)
}

// parseProcedureCaseElse parses the ELSE clause body for CASE statements.
func (p *HandParser) parseProcedureCaseElse() []ast.StmtNode {
	return p.parseProcedureStatementsUntil(57701)
}

// parseProcedureOpen parses: OPEN cursor_name
func (p *HandParser) parseProcedureOpen() ast.StmtNode {
	return p.parseProcedureCursorOp(57818, true)
}

// parseProcedureClose parses: CLOSE cursor_name
func (p *HandParser) parseProcedureClose() ast.StmtNode {
	return p.parseProcedureCursorOp(57647, false)
}

// parseProcedureCursorOp parses OPEN/CLOSE cursor_name.
func (p *HandParser) parseProcedureCursorOp(tok int, isOpen bool) ast.StmtNode {
	p.expect(tok)
	name := p.next().Lit
	if isOpen {
		return &ast.ProcedureOpenCur{CurName: name}
	}
	return &ast.ProcedureCloseCur{CurName: name}
}

// parseProcedureFetch parses: FETCH cursor_name INTO var [, var ...]
func (p *HandParser) parseProcedureFetch() ast.StmtNode {
	p.expect(57426)
	name := p.next().Lit
	p.expect(57463)

	var vars []string
	for {
		vars = append(vars, p.next().Lit)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return &ast.ProcedureFetchInto{CurName: name, Variables: vars}
}

// parseProcedureLeave parses: LEAVE label_name
func (p *HandParser) parseProcedureLeave() ast.StmtNode {
	return p.parseProcedureJump(57474, true)
}

// parseProcedureIterate parses: ITERATE label_name
func (p *HandParser) parseProcedureIterate() ast.StmtNode {
	return p.parseProcedureJump(57465, false)
}

// parseProcedureJump parses LEAVE/ITERATE label_name.
func (p *HandParser) parseProcedureJump(tok int, isLeave bool) ast.StmtNode {
	p.expect(tok)
	return &ast.ProcedureJump{Name: p.next().Lit, IsLeave: isLeave}
}

// parseProcedureStatementList handles naked procedure bodies.
func (p *HandParser) parseProcedureStatementList() ast.StmtNode {
	first := p.parseStatement()
	if first == nil {
		return nil
	}

	if _, ok := p.accept(';'); !ok {
		return first
	}

	// Multiple statements — wrap in a ProcedureBlock
	block := &ast.ProcedureBlock{}
	block.ProcedureProcStmts = append(block.ProcedureProcStmts, first)

	for {
		if p.peek().Tp == 0 {
			break
		}
		s := p.parseProcedureInnerStatement()
		if s != nil {
			block.ProcedureProcStmts = append(block.ProcedureProcStmts, s)
		} else {
			p.skipToSemiOrEnd()
		}
		if _, ok := p.accept(';'); !ok {
			break
		}
	}
	return block
}
