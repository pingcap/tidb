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
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// normalizeDDLFuncName normalizes time-function aliases to their canonical names
// in DDL contexts (DEFAULT, ON UPDATE). NOW/LOCALTIME/LOCALTIMESTAMP → CURRENT_TIMESTAMP,
// CURDATE → CURRENT_DATE, CURTIME → CURRENT_TIME.
func normalizeDDLFuncName(fc *ast.FuncCallExpr) {
	switch fc.FnName.L {
	case ast.Now, ast.LocalTime, ast.LocalTimestamp:
		fc.FnName = ast.NewCIStr(ast.CurrentTimestamp)
	case ast.Curdate:
		fc.FnName = ast.NewCIStr(ast.CurrentDate)
	case ast.Curtime:
		fc.FnName = ast.NewCIStr(ast.CurrentTime)
	}
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

// parseCreateTableStmt parses CREATE TABLE statements.
func (p *HandParser) parseCreateTableStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateTableStmt](p.arena)
	p.expect(create)

	// [TEMPORARY | GLOBAL TEMPORARY]
	if _, ok := p.accept(temporary); ok {
		stmt.TemporaryKeyword = ast.TemporaryLocal
	} else if _, ok := p.accept(global); ok {
		p.expect(temporary)
		stmt.TemporaryKeyword = ast.TemporaryGlobal
	}

	p.expect(tableKwd)

	// [IF NOT EXISTS]
	stmt.IfNotExists = p.acceptIfNotExists()

	stmt.Table = p.parseTableName()
	if stmt.Table == nil {
		return nil
	}

	// LIKE table
	if _, ok := p.accept(like); ok {
		// CREATE TABLE ... LIKE table
		stmt.ReferTable = p.parseTableName()
		// Optional ( ... ) not supported in MySQL for LIKE but TiDB/MariaDB might allow hints/options?
		// Standard MySQL syntax: CREATE TABLE t1 LIKE t2
		if stmt.ReferTable == nil {
			return nil
		}
	} else if _, ok := p.accept('('); ok {
		// ( ... )
		// Check for (LIKE table) syntax
		if _, ok := p.accept(like); ok {
			stmt.ReferTable = p.parseTableName()
			p.expect(')')
		} else {
			cols, constraints := p.parseTableElementList()
			if cols == nil && constraints == nil {
				// Error or empty list?
				return nil
			}
			stmt.Cols = cols
			stmt.Constraints = constraints
			p.expect(')')
		}
	}
	// CREATE TABLE t SELECT ... (CTAS without columns definition) handled later.

	if stmt.ReferTable == nil {
		// [Table Options]
		stmt.Options = p.parseCreateTableOptions()

		// [PARTITION BY ...]
		if p.peekKeyword(partition, "PARTITION") {
			stmt.Partition = p.parsePartitionOptions()
		}

		// [DuplicateOpt]
		if _, ok := p.acceptKeyword(ignore, "IGNORE"); ok {
			stmt.OnDuplicate = ast.OnDuplicateKeyHandlingIgnore
		} else if _, ok := p.acceptKeyword(replace, "REPLACE"); ok {
			stmt.OnDuplicate = ast.OnDuplicateKeyHandlingReplace
		}

		// [AS] SELECT|TABLE|VALUES Stmt | (SELECT ...) | WITH CTE SELECT
		p.accept(as) // optional AS
		if tok := p.peek(); tok.Tp == selectKwd {
			selStmt := p.parseSelectStmt()
			if selStmt != nil {
				stmt.Select = p.maybeParseUnion(selStmt)
			}
		} else if tok.Tp == '(' {
			// Parenthesized subquery: AS (SELECT ... UNION ...)
			sub := p.parseSubquery()
			if sub != nil {
				stmt.Select = p.maybeParseUnion(sub)
			}
		} else if tok.Tp == with {
			// WITH CTE AS (...) SELECT ...
			if withStmt := p.parseWithStmt(); withStmt != nil {
				if rs, ok := withStmt.(ast.ResultSetNode); ok {
					stmt.Select = rs
				}
			}
		} else if tok.Tp == tableKwd {
			if tblStmt := p.parseTableStmt(); tblStmt != nil {
				stmt.Select = tblStmt.(ast.ResultSetNode)
			}
		} else if tok.Tp == values {
			valStmt := p.parseValuesStmt()
			stmt.Select = valStmt.(ast.ResultSetNode)
		}
	}

	// [ON COMMIT DELETE ROWS | ON COMMIT PRESERVE ROWS] — only valid for GLOBAL TEMPORARY
	if p.peek().Tp == on {
		onOff := p.peek().Offset
		p.next() // consume ON
		p.expect(commit)
		if _, ok := p.accept(deleteKwd); ok {
			p.expect(rows)
			stmt.OnCommitDelete = true
		} else if _, ok := p.accept(preserve); ok {
			p.expect(rows)
			// OnCommitDelete remains false — PRESERVE ROWS is the default.
		}
		if stmt.TemporaryKeyword != ast.TemporaryGlobal {
			// Yacc: append error but still produce the statement
			p.error(onOff, "ON COMMIT can only be used with GLOBAL TEMPORARY tables")
		}
	} else if stmt.TemporaryKeyword == ast.TemporaryGlobal {
		// Yacc: append error but still produce the statement
		p.error(p.peek().Offset, "GLOBAL TEMPORARY and ON COMMIT DELETE ROWS must appear together")
	}

	// [SPLIT ...]
	for p.peek().Tp == split {
		opt := p.parseSplitIndexOption()
		if opt == nil {
			return nil
		}
		stmt.SplitIndex = append(stmt.SplitIndex, opt)
	}

	return stmt
}

// parseTableElementList parses comma-separated column definitions and constraints.
func (p *HandParser) parseTableElementList() ([]*ast.ColumnDef, []*ast.Constraint) {
	var cols []*ast.ColumnDef
	var constraints []*ast.Constraint

	for {
		// Check for constraint keywords
		tp := p.peek().Tp
		if tp == constraint || tp == primary || tp == key || tp == index ||
			tp == unique || tp == foreign || tp == fulltext || tp == check ||
			tp == vectorType || tp == columnar {
			cons := p.parseConstraint()
			if cons == nil {
				return nil, nil
			}
			constraints = append(constraints, cons)
		} else {
			// Assume column definition
			col := p.parseColumnDef()
			if col == nil {
				return nil, nil
			}
			cols = append(cols, col)
		}

		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return cols, constraints
}

// parseColumnDef parses a column definition: col_name data_type [options]
func (p *HandParser) parseColumnDef() *ast.ColumnDef {
	col := Alloc[ast.ColumnDef](p.arena)

	// Parse column name.
	// We handle identifiers and string literals (if quoted).
	// Keywords allowed as identifiers should be handled by lexer or mapped here?
	// Column name: accept identifiers, string literals, and unreserved keywords.
	// Keywords with Tp >= identifier can be used as column names since the context
	// is unambiguous (always followed by a data type).
	tok := p.peek()
	if !isIdentLike(tok.Tp) {
		p.syntaxErrorAt(tok)
		return nil
	}
	col.Name = p.parseColumnName()
	if col.Name == nil {
		return nil
	}

	// Handle SERIAL pseudo-type: SERIAL = BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE KEY
	// Yacc grammar: ColumnName "SERIAL" ColumnOptionListOpt
	// SERIAL is not a real field type; it must be handled at the ColumnDef level
	// because it injects column options (NOT NULL, AUTO_INCREMENT, UNIQUE KEY).
	if _, ok := p.accept(serial); ok {
		col.Tp = types.NewFieldType(mysql.TypeLonglong)
		col.Tp.AddFlag(mysql.UnsignedFlag)
		col.Options = []*ast.ColumnOption{
			{Tp: ast.ColumnOptionNotNull},
			{Tp: ast.ColumnOptionAutoIncrement},
			{Tp: ast.ColumnOptionUniqKey},
		}
		// Parse optional additional column options after SERIAL
		col.Options = append(col.Options, p.parseColumnOptions(col.Tp)...)
		if err := col.Validate(); err != nil {
			p.errs = append(p.errs, err)
			return nil
		}
		return col
	}

	// Parse data type
	col.Tp = p.parseFieldType()
	if col.Tp == nil {
		p.syntaxErrorAt(p.peek())
		return nil
	}

	col.Options = p.parseColumnOptions(col.Tp)

	// Validate column definition (e.g., generated column + DEFAULT is illegal).
	if err := col.Validate(); err != nil {
		p.errs = append(p.errs, err)
		return nil
	}

	return col
}

// parseColumnOptions parses column options: NOT NULL, DEFAULT 1, PRIMARY KEY, etc.
func (p *HandParser) parseColumnOptions(_ *types.FieldType) []*ast.ColumnOption {
	var options []*ast.ColumnOption
	for {
		option := Alloc[ast.ColumnOption](p.arena)
		switch p.peek().Tp {
		case not, not2:
			p.next()
			if _, ok := p.accept(null); !ok {
				// Parse error: expected NULL after NOT
				return nil
			}
			option.Tp = ast.ColumnOptionNotNull
		case null:
			p.next()
			option.Tp = ast.ColumnOptionNull
		case defaultKwd:
			p.next()
			option.Tp = ast.ColumnOptionDefaultValue
			// MySQL grammar: DEFAULT value is a signed_literal or NOW()/CURRENT_TIMESTAMP.
			// Use parsePrefixExpr which handles literals, function calls, parenthesized
			// expressions, and unary +/- but does NOT consume infix operators like
			// NOT/AND/OR, which avoids greedily consuming column options like NOT NULL.
			option.Expr = p.parsePrefixExpr(0)
			if option.Expr == nil {
				return nil
			}

			// Unwrap nested parentheses for inspection and normalization
			var inner ast.ExprNode = option.Expr
			for {
				pExpr, ok := inner.(*ast.ParenthesesExpr)
				if !ok {
					break
				}
				inner = pExpr.Expr
			}

			// Reject bare identifiers (ColumnNameExpr).
			// The yacc parser requires these to be function calls (with parens), not identifiers.
			if cn, ok := option.Expr.(*ast.ColumnNameExpr); ok {
				p.error(p.peek().Offset, "Invalid default value: %s", cn.Name.Name.O)
				return nil
			}

			// Normalize NOW/LOCALTIME/LOCALTIMESTAMP → CURRENT_TIMESTAMP in DDL DEFAULT.
			if fc, ok := inner.(*ast.FuncCallExpr); ok {
				normalizeDDLFuncName(fc)
			}

			// Use the unwrapped and normalized expression
			option.Expr = inner

		case autoIncrement:
			p.next()
			option.Tp = ast.ColumnOptionAutoIncrement
		case serial:
			// SERIAL DEFAULT VALUE = NOT NULL AUTO_INCREMENT UNIQUE KEY
			p.next()
			p.expect(defaultKwd)
			p.expect(value)
			options = append(options, &ast.ColumnOption{Tp: ast.ColumnOptionNotNull})
			options = append(options, &ast.ColumnOption{Tp: ast.ColumnOptionAutoIncrement})
			option.Tp = ast.ColumnOptionUniqKey
		case primary, key:
			if p.next().Tp == primary {
				if _, ok := p.accept(key); !ok {
					return nil
				}
			}
			option.Tp = ast.ColumnOptionPrimaryKey
			// Handle CLUSTERED/NONCLUSTERED after PRIMARY KEY
			if _, ok := p.accept(clustered); ok {
				option.PrimaryKeyTp = ast.PrimaryKeyTypeClustered
			} else if _, ok := p.accept(nonclustered); ok {
				option.PrimaryKeyTp = ast.PrimaryKeyTypeNonClustered
			}
			p.parseGlobalLocalOption(option)
		case unique:
			p.next()
			p.accept(key) // KEY is optional for UNIQUE
			option.Tp = ast.ColumnOptionUniqKey
			p.parseGlobalLocalOption(option)
		case comment:
			p.next()
			option.Tp = ast.ColumnOptionComment
			if p.peek().Tp != stringLit {
				p.syntaxErrorAt(p.peek())
				return nil
			}
			expr := p.newValueExpr(p.peek().Lit)
			if valExpr, ok := expr.(ast.ValueExpr); ok {
				valExpr.GetType().SetCharset("")
				valExpr.GetType().SetCollate("")
			}
			option.Expr = expr
			p.next()
		case secondaryEngineAttribute:
			p.next()
			option.Tp = ast.ColumnOptionSecondaryEngineAttribute
			p.accept(eq) // optional =
			if p.peek().Tp != stringLit {
				p.syntaxErrorAt(p.peek())
				return nil
			}
			option.StrValue = p.peek().Lit
			p.next()
		case on:
			p.next()
			p.expect(update)
			option.Tp = ast.ColumnOptionOnUpdate
			// yacc: "ON" "UPDATE" NowSymOptionFraction
			// Only NOW-family and CURDATE-family functions are valid here.
			option.Expr = p.parseNowSymOptionFraction()
			if option.Expr == nil {
				return nil
			}
		case as, generated:
			if p.next().Tp == generated {
				p.expect(always)
				p.expect(as)
			}
			option.Tp = ast.ColumnOptionGenerated
			p.parseGeneratedColumnBody(option)
		case constraint:
			// CONSTRAINT [name] CHECK (expr) — only valid at column level
			// Peek ahead to verify it's followed by CHECK (with optional name in between)
			peekOff := 1
			if isIdentLike(p.peekN(peekOff).Tp) {
				peekOff++
			}
			if p.peekN(peekOff).Tp != check {
				// NOT a column-level CHECK constraint — stop column options
				return options
			}
			p.next() // consume CONSTRAINT
			if isIdentLike(p.peek().Tp) {
				option.ConstraintName = p.next().Lit
			}
			p.next() // consume CHECK
			option.Tp = ast.ColumnOptionCheck
			option.Enforced = true
			p.expect('(')
			option.Expr = p.parseExpression(precNone)
			if option.Expr == nil {
				return nil
			}
			p.expect(')')
			// EnforcedOrNotOrNotNullOpt (same handling as CHECK without CONSTRAINT prefix)
			if p.peek().Tp == not || p.peek().Tp == not2 {
				if p.peekN(1).Tp == null {
					// CONSTRAINT [name] CHECK (expr) NOT NULL → inject both CHECK and NOT NULL
					p.next() // consume NOT
					p.next() // consume NULL
					options = append(options, option)
					options = append(options, &ast.ColumnOption{Tp: ast.ColumnOptionNotNull})
					continue
				} else if p.peekN(1).IsKeyword("ENFORCED") {
					p.next()
					p.next()
					option.Enforced = false
				}
			} else if p.peek().IsKeyword("ENFORCED") {
				p.next()
				option.Enforced = true
			}
		case check:
			p.next()
			option.Tp = ast.ColumnOptionCheck
			option.Enforced = true // default is ENFORCED
			p.expect('(')
			option.Expr = p.parseExpression(precNone)
			if option.Expr == nil {
				return nil
			}
			p.expect(')')
			// EnforcedOrNotOrNotNullOpt: parser.y line 3732
			// NOT NULL → injects both CHECK and a separate ColumnOptionNotNull
			// NOT ENFORCED → sets Enforced = false
			// ENFORCED → sets Enforced = true (default)
			if p.peek().Tp == not || p.peek().Tp == not2 {
				if p.peekN(1).Tp == null {
					// CHECK (expr) NOT NULL → inject separate NOT NULL option
					p.next() // consume NOT
					p.next() // consume NULL
					options = append(options, option)
					options = append(options, &ast.ColumnOption{Tp: ast.ColumnOptionNotNull})
					continue
				} else if p.peekN(1).IsKeyword("ENFORCED") {
					p.next() // consume NOT
					p.next() // consume ENFORCED
					option.Enforced = false
				}
			} else if p.peek().IsKeyword("ENFORCED") {
				p.next()
				option.Enforced = true
			}
		case references:
			option.Tp = ast.ColumnOptionReference
			option.Refer = p.parseReferenceDef()
		case collate:
			// Check for duplicate COLLATE using the same logic as the yacc
			// ColumnOptionList rule: HasCollateOption is set to true ONLY in
			// the base case (first option), and checked-but-never-updated in
			// the recursive case. This means duplicates are only detected
			// when COLLATE is the very first column option.
			isDuplicate := len(options) > 0 && options[0].Tp == ast.ColumnOptionCollate
			// Consume COLLATE and CollationName BEFORE checking duplicate,
			// matching yacc: the ColumnOption rule "COLLATE" CollationName
			// consumes both tokens, then the action code checks for duplicates.
			// This makes the error position point to the token AFTER the
			// collation name (e.g. ')'), not at the COLLATE keyword.
			p.next()
			option.Tp = ast.ColumnOptionCollate
			// yacc: ColumnOption "COLLATE" CollationName — no EqOpt (unlike table-level COLLATE)
			tok := p.peek()
			if isIdentLike(tok.Tp) || tok.Tp == stringLit {
				info, err := charset.GetCollationByName(tok.Lit)
				if err != nil {
					p.errs = append(p.errs, err)
					return nil
				}
				option.StrValue = info.Name
				p.next()
			} else if tok.Tp == binaryType || tok.Tp == byteType {
				option.StrValue = charset.CollationBin
				p.next()
			} else {
				p.syntaxErrorAt(tok)
				return nil
			}
			if isDuplicate {
				// Use EndOffset for line/column (matching yacc reader position after
				// scanning) and Offset for near text (matching yacc lastScanOffset).
				nextTok := p.peek()
				near := p.buildNearStringAt(nextTok.EndOffset, nextTok.Offset)
				p.errs = append(p.errs, ErrParse.GenWithStackByArgs("Multiple COLLATE clauses", near))
				return nil
			}
		case columnFormat:
			p.next()
			option.Tp = ast.ColumnOptionColumnFormat
			switch {
			case p.peek().Tp == defaultKwd:
				p.next()
				option.StrValue = "DEFAULT"
			case p.peek().Tp == fixed:
				p.next()
				option.StrValue = "FIXED"
			case p.peek().IsKeyword("DYNAMIC"):
				p.next()
				option.StrValue = "DYNAMIC"
			default:
				p.syntaxErrorAt(p.peek())
				return nil
			}
		case storage:
			p.next()
			option.Tp = ast.ColumnOptionStorage
			switch p.peek().Tp {
			case defaultKwd:
				p.next()
				option.StrValue = "DEFAULT"
			case disk:
				p.next()
				option.StrValue = "DISK"
			case memory:
				p.next()
				option.StrValue = "MEMORY"
			default:
				p.syntaxErrorAt(p.peek())
				return nil
			}
			p.warnNear(p.peek().Offset, "The STORAGE clause is parsed but ignored by all storage engines.")
		case autoRandom:
			p.next()
			option.Tp = ast.ColumnOptionAutoRandom
			option.AutoRandOpt.ShardBits = types.UnspecifiedLength
			option.AutoRandOpt.RangeBits = types.UnspecifiedLength
			if p.peek().Tp == '(' {
				p.next() // consume '('
				if tok, ok := p.expect(intLit); ok {
					option.AutoRandOpt.ShardBits = int(tokenItemToUint64(tok.Item))
				}
				if p.peek().Tp == ',' {
					p.next()
					if tok, ok := p.expect(intLit); ok {
						option.AutoRandOpt.RangeBits = int(tokenItemToUint64(tok.Item))
					}
				}
				p.expect(')')
			}
		default:
			// End of options
			return options
		}
		options = append(options, option)
	}
}

// parseGeneratedColumnBody parses the body of a generated column: (expr) [VIRTUAL|STORED].
func (p *HandParser) parseGeneratedColumnBody(option *ast.ColumnOption) {
	p.expect('(')
	startOff := p.peek().Offset
	option.Expr = p.parseExpression(precNone)
	endOff := p.peek().Offset // ')' token offset
	if option.Expr != nil && endOff > startOff && endOff <= len(p.src) {
		option.Expr.SetText(p.connectionEncoding, strings.TrimSpace(p.src[startOff:endOff]))
	}
	p.expect(')')
	// VIRTUAL / STORED
	if _, ok := p.accept(stored); ok {
		option.Stored = true
	} else {
		p.accept(virtual) // optional explicit VIRTUAL
	}
}

// parseGlobalLocalOption parses optional GLOBAL/LOCAL suffix for column key options.
func (p *HandParser) parseGlobalLocalOption(option *ast.ColumnOption) {
	if _, ok := p.accept(global); ok {
		option.StrValue = "Global"
	} else {
		p.accept(local)
	}
}

// parseNowSymOptionFraction parses the NowSymOptionFraction production from yacc:
//
//	NowSym (bare CURRENT_TIMESTAMP/LOCALTIME/LOCALTIMESTAMP)
//	NowSymFunc '(' ')' or '(' NUM ')'
//	CurdateSym '(' ')' (CURDATE/CURRENT_DATE with parens)
//	CURRENT_DATE (bare)
//
// All results are normalized to CURRENT_TIMESTAMP or CURRENT_DATE FuncCallExpr.
func (p *HandParser) parseNowSymOptionFraction() ast.ExprNode {
	tok := p.peek()
	node := p.arena.AllocFuncCallExpr()

	switch tok.Tp {
	case currentTs, localTime, localTs:
		// NowSym / NowSymFunc: bare keyword or with optional parens + fsp
		p.next()
		node.FnName = ast.NewCIStr("CURRENT_TIMESTAMP")
		if _, ok := p.accept('('); ok {
			if p.peek().Tp == intLit {
				node.Args = []ast.ExprNode{p.parseIntLitPrecision()}
			}
			p.expect(')')
		}
		return node
	case builtinNow, now:
		// NOW() — must have parens (it's a builtin function token)
		p.next()
		node.FnName = ast.NewCIStr("CURRENT_TIMESTAMP")
		p.expect('(')
		if p.peek().Tp == intLit {
			node.Args = []ast.ExprNode{p.parseIntLitPrecision()}
		}
		p.expect(')')
		return node
	case currentDate:
		// CURRENT_DATE — bare or with parens
		p.next()
		node.FnName = ast.NewCIStr("CURRENT_DATE")
		if _, ok := p.accept('('); ok {
			p.expect(')')
		}
		return node
	case curDate, builtinFnCurDate:
		// CURDATE() — must have parens
		p.next()
		node.FnName = ast.NewCIStr("CURRENT_DATE")
		p.expect('(')
		p.expect(')')
		return node
	default:
		p.syntaxErrorAt(tok)
		return nil
	}
}
