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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// skipToSemiOrEnd consumes tokens until reaching ';', END, or EOF.
// Used to recover from unparseable inner statements and prevent infinite loops.
func (p *HandParser) skipToSemiOrEnd() {
	for {
		tok := p.peek()
		if tok.Tp == 0 || tok.Tp == ';' || tok.Tp == tokEnd || tok.Tp == tokUntil {
			return
		}
		// Also stop at ELSEIF, ELSE which terminate IF branches.
		if tok.Tp == tokElse || tok.Tp == tokElseIf {
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
	if tok := p.peek(); tok.IsIdent() || tok.Tp == tokIdentifier {
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
	p.expect(tokCreate)
	p.expect(tokProcedure)

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
		case tokIn:
			p.next()
			param.Paramstatus = ast.MODE_IN
		case tokOut:
			p.next()
			param.Paramstatus = ast.MODE_OUT
		case tokInout:
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
	case tokBegin, tokIf, tokWhile, tokRepeat, tokCase:
		return p.parseProcedureInnerStatement()
	default:
		return p.parseProcedureStatementList()
	}
}

// parseLabeledBody parses a labeled block/loop after "label:".
func (p *HandParser) parseLabeledBody(label string) ast.StmtNode {
	switch p.peek().Tp {
	case tokWhile:
		return p.parseProcedureWhile(label)
	case tokRepeat:
		return p.parseProcedureRepeat(label)
	default:
		return p.parseProcedureBlock(label)
	}
}

// parseProcedureBlock parses BEGIN ... END [label]
func (p *HandParser) parseProcedureBlock(label string) ast.StmtNode {
	p.expect(tokBegin)

	block := &ast.ProcedureBlock{}
	var vars []ast.DeclNode

	// Parse DECLARE statements first
	for p.peek().Tp == tokDeclare {
		decl := p.parseProcedureDeclaration()
		if decl != nil {
			vars = append(vars, decl)
		}
		p.accept(';')
	}

	block.ProcedureVars = vars
	block.ProcedureProcStmts = p.parseProcedureStatementsUntil(tokEnd)

	p.expect(tokEnd) // consume END
	return p.wrapWithLabel(label, block, false)
}

// parseProcedureDeclaration parses DECLARE statements.
func (p *HandParser) parseProcedureDeclaration() ast.DeclNode {
	p.expect(tokDeclare)

	tok := p.peek()
	if tok.Tp == tokExit {
		return p.parseProcedureHandlerDecl(ast.PROCEDUR_EXIT)
	}
	if tok.Tp == tokContinue {
		return p.parseProcedureHandlerDecl(ast.PROCEDUR_CONTINUE)
	}

	firstName := p.next().Lit

	if p.peek().Tp == tokCursor {
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

	if _, ok := p.accept(tokDefault); ok {
		decl.DeclDefault = p.parseExpression(precNone)
	}
	return decl
}

// parseProcedureHandlerDecl parses: {EXIT|CONTINUE} HANDLER FOR condition [, cond ...] stmt
func (p *HandParser) parseProcedureHandlerDecl(handlerType int) *ast.ProcedureErrorControl {
	p.next() // consume EXIT/CONTINUE
	p.expect(tokHandler)
	p.expect(tokFor)

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
	case tokSqlWarning:
		p.next()
		return &ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_SQLWARNING}
	case tokSqlException:
		p.next()
		return &ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_SQLEXCEPTION}
	case tokSqlState:
		p.next()
		val := p.next().Lit
		return &ast.ProcedureErrorState{CodeStatus: val}
	case tokIntLit:
		tok := p.next()
		errNum := tokenItemToUint64(tok.Item)
		return &ast.ProcedureErrorVal{ErrorNum: errNum}
	default:
		if p.peek().Tp == tokNot {
			p.next()
			p.next() // "FOUND"
			return &ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_NOT_FOUND}
		}
		return nil
	}
}

// parseProcedureCursor parses: CURSOR FOR select_stmt
func (p *HandParser) parseProcedureCursor(name string) *ast.ProcedureCursor {
	p.expect(tokCursor)
	p.expect(tokFor)
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
	case tokBegin:
		return p.parseProcedureBlock("")
	case tokIf:
		return p.parseProcedureIf()
	case tokWhile:
		return p.parseProcedureWhile("")
	case tokRepeat:
		return p.parseProcedureRepeat("")
	case tokCase:
		return p.parseProcedureCase()
	case tokOpen:
		return p.parseProcedureOpen()
	case tokClose:
		return p.parseProcedureClose()
	case tokFetch:
		return p.parseProcedureFetch()
	case tokLeave:
		return p.parseProcedureLeave()
	case tokIterate:
		return p.parseProcedureIterate()
	default:
		return p.parseStatement()
	}
}

// parseProcedureIf parses: IF expr THEN stmts [ELSEIF ...] [ELSE ...] END IF
func (p *HandParser) parseProcedureIf() ast.StmtNode {
	p.expect(tokIf)
	ifBlock := p.parseProcedureIfBlock()
	p.expect(tokEnd)
	p.expect(tokIf)
	return &ast.ProcedureIfInfo{IfBody: ifBlock}
}

// parseProcedureIfBlock parses: expr THEN stmts [ELSEIF ...] [ELSE ...]
func (p *HandParser) parseProcedureIfBlock() *ast.ProcedureIfBlock {
	block := &ast.ProcedureIfBlock{}
	block.IfExpr = p.parseExpression(precNone)
	p.expect(tokThen)

	block.ProcedureIfStmts = p.parseProcedureStatementsUntil(tokEnd, tokElse, tokElseIf)

	tok := p.peek()
	if tok.Tp == tokElseIf {
		p.next()
		elseIfBlock := p.parseProcedureIfBlock()
		block.ProcedureElseStmt = &ast.ProcedureElseIfBlock{
			ProcedureIfStmt: elseIfBlock,
		}
	} else if _, ok := p.accept(tokElse); ok {
		block.ProcedureElseStmt = &ast.ProcedureElseBlock{
			ProcedureIfStmts: p.parseProcedureStatementsUntil(tokEnd),
		}
	}
	return block
}

// parseProcedureWhile parses: WHILE expr DO stmts END WHILE [label]
func (p *HandParser) parseProcedureWhile(label string) ast.StmtNode {
	p.expect(tokWhile)

	block := &ast.ProcedureWhileStmt{}
	block.Condition = p.parseExpression(precNone)
	p.expect(tokDo)

	block.Body = p.parseProcedureStatementsUntil(tokEnd)

	p.expect(tokEnd)
	p.expect(tokWhile)

	return p.wrapWithLabel(label, block, true)
}

// parseProcedureRepeat parses: REPEAT stmts UNTIL expr END REPEAT [label]
func (p *HandParser) parseProcedureRepeat(label string) ast.StmtNode {
	p.expect(tokRepeat)

	block := &ast.ProcedureRepeatStmt{}

	block.Body = p.parseProcedureStatementsUntil(tokUntil)

	p.expect(tokUntil)
	block.Condition = p.parseExpression(precNone)
	p.expect(tokEnd)
	p.expect(tokRepeat)

	return p.wrapWithLabel(label, block, true)
}

// parseProcedureCase parses: CASE [expr] WHEN ... THEN ... [ELSE ...] END CASE
// Produces SimpleCaseStmt (CASE expr WHEN ...) or SearchCaseStmt (CASE WHEN ...).
func (p *HandParser) parseProcedureCase() ast.StmtNode {
	p.expect(tokCase)

	hasCondition := p.peek().Tp != tokWhen
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
		p.expect(tokThen)
		whenStmt.ProcedureStmts = p.parseProcedureStatementsUntil(tokWhen, tokEnd, tokElse)
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
		p.expect(tokThen)
		whenStmt.ProcedureStmts = p.parseProcedureStatementsUntil(tokWhen, tokEnd, tokElse)
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
	for p.peek().Tp == tokWhen {
		p.next()
		parseWhen()
	}
	if _, ok := p.accept(tokElse); ok {
		setElse(p.parseProcedureCaseElse())
	}
	p.expect(tokEnd)
	p.expect(tokCase)
}

// parseProcedureCaseElse parses the ELSE clause body for CASE statements.
func (p *HandParser) parseProcedureCaseElse() []ast.StmtNode {
	return p.parseProcedureStatementsUntil(tokEnd)
}

// parseProcedureOpen parses: OPEN cursor_name
func (p *HandParser) parseProcedureOpen() ast.StmtNode {
	return p.parseProcedureCursorOp(tokOpen, true)
}

// parseProcedureClose parses: CLOSE cursor_name
func (p *HandParser) parseProcedureClose() ast.StmtNode {
	return p.parseProcedureCursorOp(tokClose, false)
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
	p.expect(tokFetch)
	name := p.next().Lit
	p.expect(tokInto)

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
	return p.parseProcedureJump(tokLeave, true)
}

// parseProcedureIterate parses: ITERATE label_name
func (p *HandParser) parseProcedureIterate() ast.StmtNode {
	return p.parseProcedureJump(tokIterate, false)
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
