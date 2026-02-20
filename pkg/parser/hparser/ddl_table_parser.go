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
	p.expect(tokCreate)

	// [TEMPORARY | GLOBAL TEMPORARY]
	if _, ok := p.accept(tokTemporary); ok {
		stmt.TemporaryKeyword = ast.TemporaryLocal
	} else if _, ok := p.accept(tokGlobal); ok {
		p.expect(tokTemporary)
		stmt.TemporaryKeyword = ast.TemporaryGlobal
	}

	p.expect(tokTable)

	// [IF NOT EXISTS]
	stmt.IfNotExists = p.acceptIfNotExists()

	stmt.Table = p.parseTableName()
	if stmt.Table == nil {
		return nil
	}

	// LIKE table
	if _, ok := p.accept(tokLike); ok {
		// CREATE TABLE ... LIKE table
		stmt.ReferTable = p.parseTableName()
		// Optional ( ... ) not supported in MySQL for LIKE but TiDB/MariaDB might allow hints/options?
		// Standard MySQL syntax: CREATE TABLE t1 LIKE t2
		if stmt.ReferTable == nil {
			return nil
		}
		// Fall through to handle trailing options (if any)? MySQL doesn't allow options after LIKE table.
		// But let's check parseCreateTableOptions.
		// TiDB parser.y: CreateTableStmt -> ... LikeTableWithOrWithoutParen OnCommitOpt
		// LikeTableWithOrWithoutParen -> LIKE TableName | '(' LIKE TableName ')'
		return stmt
	}

	// ( ... )
	if _, ok := p.accept('('); ok {
		// Check for (LIKE table) syntax
		if _, ok := p.accept(tokLike); ok {
			stmt.ReferTable = p.parseTableName()
			p.expect(')')
			return stmt
		}
		cols, constraints := p.parseTableElementList()
		if cols == nil && constraints == nil {
			// Error or empty list?
			return nil
		}
		stmt.Cols = cols
		stmt.Constraints = constraints
		p.expect(')')
	} else {
		// CREATE TABLE t SELECT ... (CTAS without columns definition)
		// Handled later.
	}

	// [Table Options]
	stmt.Options = p.parseCreateTableOptions()

	// [PARTITION BY ...]
	if p.peekKeyword(tokPartition, "PARTITION") {
		stmt.Partition = p.parsePartitionOptions()
	}

	// [DuplicateOpt]
	if _, ok := p.acceptKeyword(tokIgnore, "IGNORE"); ok {
		stmt.OnDuplicate = ast.OnDuplicateKeyHandlingIgnore
	} else if _, ok := p.acceptKeyword(tokReplace, "REPLACE"); ok {
		stmt.OnDuplicate = ast.OnDuplicateKeyHandlingReplace
	}

	// [AS] SELECT|TABLE|VALUES Stmt | (SELECT ...)
	p.accept(tokAs)
	if tok := p.peek(); tok.Tp == tokSelect {
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
	} else if tok.Tp == tokTable {
		stmt.Select = p.parseTableStmt().(ast.ResultSetNode)
	} else if tok.Tp == tokValues {
		stmt.Select = p.parseValuesStmt().(ast.ResultSetNode)
	}

	// [ON COMMIT DELETE ROWS | ON COMMIT PRESERVE ROWS] — only valid for GLOBAL TEMPORARY
	if p.peek().Tp == tokOn {
		if stmt.TemporaryKeyword != ast.TemporaryGlobal {
			// ON COMMIT is only valid for GLOBAL TEMPORARY tables.
			p.error(p.peek().Offset, "ON COMMIT can only be used with GLOBAL TEMPORARY tables")
			return nil
		}
		p.next() // consume ON
		p.expect(tokCommit)
		if _, ok := p.accept(tokDelete); ok {
			p.expect(tokRows)
			stmt.OnCommitDelete = true
		} else if _, ok := p.accept(tokPreserve); ok {
			p.expect(tokRows)
			// OnCommitDelete remains false — PRESERVE ROWS is the default.
		}
	} else if stmt.TemporaryKeyword == ast.TemporaryGlobal {
		// GLOBAL TEMPORARY requires ON COMMIT DELETE ROWS or ON COMMIT PRESERVE ROWS.
		p.error(p.peek().Offset, "GLOBAL TEMPORARY and ON COMMIT DELETE ROWS must appear together")
		return nil
	}

	// [SPLIT ...]
	for p.peek().Tp == tokSplit {
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
		if tp == tokConstraint || tp == tokPrimary || tp == tokKey || tp == tokIndex ||
			tp == tokUnique || tp == tokForeign || tp == tokFulltext || tp == tokCheck ||
			tp == tokVector || tp == tokColumnar {
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
	// Keywords with Tp >= tokIdentifier can be used as column names since the context
	// is unambiguous (always followed by a data type).
	tok := p.peek()
	if !isIdentLike(tok.Tp) {
		p.error(tok.Offset, "expected column name, got %d", tok.Tp)
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
	if _, ok := p.accept(tokSerial); ok {
		col.Tp = types.NewFieldType(mysql.TypeLonglong)
		col.Tp.AddFlag(mysql.UnsignedFlag)
		col.Options = []*ast.ColumnOption{
			{Tp: ast.ColumnOptionNotNull},
			{Tp: ast.ColumnOptionAutoIncrement},
			{Tp: ast.ColumnOptionUniqKey},
		}
		// Parse optional additional column options after SERIAL
		col.Options = append(col.Options, p.parseColumnOptions(col.Tp, false)...)
		if err := col.Validate(); err != nil {
			p.errs = append(p.errs, err)
			return nil
		}
		return col
	}

	// Parse data type
	col.Tp = p.parseFieldType()
	if col.Tp == nil {
		p.error(p.peek().Offset, "expected data type for column '%s'", col.Name.Name.O)
		return nil
	}

	// Parse column options. Tell parseColumnOptions whether the field type
	// already consumed an explicit COLLATE (so a second one is a duplicate).
	hasExplicitCollate := p.lastFieldTypeExplicitCollate
	col.Options = p.parseColumnOptions(col.Tp, hasExplicitCollate)

	// Validate column definition (e.g., generated column + DEFAULT is illegal).
	if err := col.Validate(); err != nil {
		p.errs = append(p.errs, err)
		return nil
	}

	return col
}

// parseColumnOptions parses column options: NOT NULL, DEFAULT 1, PRIMARY KEY, etc.
func (p *HandParser) parseColumnOptions(tp *types.FieldType, hasExplicitCollate bool) []*ast.ColumnOption {
	var options []*ast.ColumnOption
	for {
		option := Alloc[ast.ColumnOption](p.arena)
		switch p.peek().Tp {
		case tokNot:
			p.next()
			if _, ok := p.accept(tokNull); ok {
				option.Tp = ast.ColumnOptionNotNull
			} else {
				// Parse error: expected NULL after NOT
				return nil
			}
		case tokNull:
			p.next()
			option.Tp = ast.ColumnOptionNull
		case tokDefault:
			p.next()
			option.Tp = ast.ColumnOptionDefaultValue
			option.Expr = p.parseExpression(precNone)

			// Unwrap nested parentheses for inspection and normalization
			var inner ast.ExprNode = option.Expr
			for {
				if pExpr, ok := inner.(*ast.ParenthesesExpr); ok {
					inner = pExpr.Expr
				} else {
					break
				}
			}

			// Reject bare identifiers (ColumnNameExpr).
			// The yacc parser requires these to be function calls (with parens), not identifiers.
			if cn, ok := option.Expr.(*ast.ColumnNameExpr); ok {
				p.error(0, "Invalid default value: %s", cn.Name.Name.O)
				return nil
			}

			// Normalize NOW/LOCALTIME/LOCALTIMESTAMP → CURRENT_TIMESTAMP in DDL DEFAULT.
			if fc, ok := inner.(*ast.FuncCallExpr); ok {
				normalizeDDLFuncName(fc)
			}

			// Use the unwrapped and normalized expression
			option.Expr = inner

			// Wrap other function calls (like RAND, UUID) in ParenthesesExpr to match Restore behavior.
			// Restore adds parentheses around non-standard default functions.
			// To ensure round-trip AST equality, we must wrap them here.
			if fc, ok := option.Expr.(*ast.FuncCallExpr); ok {
				name := fc.FnName.L
				if name != ast.CurrentTimestamp && name != ast.Now && name != ast.LocalTime && name != ast.LocalTimestamp &&
					name != ast.CurrentDate && name != ast.Curdate &&
					name != ast.CurrentTime && name != ast.Curtime {
					option.Expr = &ast.ParenthesesExpr{
						Expr: option.Expr,
					}
				}
			}

		case tokAutoInc:
			p.next()
			option.Tp = ast.ColumnOptionAutoIncrement
		case tokSerial:
			// SERIAL DEFAULT VALUE = NOT NULL AUTO_INCREMENT UNIQUE KEY
			p.next()
			p.expect(tokDefault)
			p.expect(tokValue)
			options = append(options, &ast.ColumnOption{Tp: ast.ColumnOptionNotNull})
			options = append(options, &ast.ColumnOption{Tp: ast.ColumnOptionAutoIncrement})
			option.Tp = ast.ColumnOptionUniqKey
		case tokPrimary, tokKey:
			if p.next().Tp == tokPrimary {
				if _, ok := p.accept(tokKey); !ok {
					return nil
				}
			}
			option.Tp = ast.ColumnOptionPrimaryKey
			// Handle CLUSTERED/NONCLUSTERED after PRIMARY KEY
			if _, ok := p.accept(tokClustered); ok {
				option.PrimaryKeyTp = ast.PrimaryKeyTypeClustered
			} else if _, ok := p.accept(tokNonClustered); ok {
				option.PrimaryKeyTp = ast.PrimaryKeyTypeNonClustered
			}
			p.parseGlobalLocalOption(option)
		case tokUnique:
			p.next()
			p.accept(tokKey) // KEY is optional for UNIQUE
			option.Tp = ast.ColumnOptionUniqKey
			p.parseGlobalLocalOption(option)
		case tokComment:
			p.next()
			option.Tp = ast.ColumnOptionComment
			if p.peek().Tp == tokStringLit {
				expr := p.newValueExpr(p.peek().Lit)
				if valExpr, ok := expr.(ast.ValueExpr); ok {
					valExpr.GetType().SetCharset("")
					valExpr.GetType().SetCollate("")
				}
				option.Expr = expr
				p.next()
			} else {
				p.error(p.peek().Offset, "expected string literal for COMMENT")
				return nil
			}
		case tokSecondaryEngineAttribute:
			p.next()
			option.Tp = ast.ColumnOptionSecondaryEngineAttribute
			if _, ok := p.accept(tokEq); ok {
				// optional =
			}
			if p.peek().Tp == tokStringLit {
				option.StrValue = p.peek().Lit
				p.next()
			} else {
				p.error(p.peek().Offset, "expected string literal for SECONDARY_ENGINE_ATTRIBUTE")
				return nil
			}
		case tokOn:
			p.next()
			p.expect(tokUpdate)
			option.Tp = ast.ColumnOptionOnUpdate
			option.Expr = p.parseExpression(precNone) // CURRENT_TIMESTAMP etc.

			// ON UPDATE only accepts specific function calls, not arbitrary expressions or parenthesized expressions.
			// Parser.y: | "ON" "UPDATE" NowSymOptionFraction
			// NowSymOptionFraction allows: NowSym, NowSymFunc '(' ')', etc. but NOT parenthesized wrapper.
			if _, ok := option.Expr.(*ast.ParenthesesExpr); ok {
				p.error(0, "Invalid ON UPDATE clause: parenthesized expression not allowed")
				return nil
			}

			// Reject bare identifiers (ColumnNameExpr)
			if cn, ok := option.Expr.(*ast.ColumnNameExpr); ok {
				switch strings.ToLower(cn.Name.Name.L) {
				case ast.Now, ast.LocalTime, ast.LocalTimestamp, ast.CurrentTimestamp:
					p.error(0, "%s as ON UPDATE requires parentheses", cn.Name.Name.O)
					return nil
				}
				// Other identifiers also invalid
				p.error(0, "Invalid ON UPDATE clause: expected function")
				return nil
			}

			// Must be FuncCallExpr
			if fc, ok := option.Expr.(*ast.FuncCallExpr); ok {
				normalizeDDLFuncName(fc)
			} else {
				// Reject other types (ValueExpr, etc)
				p.error(0, "Invalid ON UPDATE clause")
				return nil
			}
		case tokAs, tokGenerated:
			if p.next().Tp == tokGenerated {
				p.expect(tokAlways)
				p.expect(tokAs)
			}
			option.Tp = ast.ColumnOptionGenerated
			p.parseGeneratedColumnBody(option)
		case tokConstraint:
			// CONSTRAINT [name] CHECK (expr) — only valid at column level
			// Peek ahead to verify it's followed by CHECK (with optional name in between)
			peekOff := 1
			if isIdentLike(p.peekN(peekOff).Tp) {
				peekOff++
			}
			if p.peekN(peekOff).Tp != tokCheck {
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
			p.expect(')')
			if p.peek().Tp == tokNot && p.peekN(1).IsKeyword("ENFORCED") {
				p.next()
				p.next()
				option.Enforced = false
			} else if p.peek().IsKeyword("ENFORCED") {
				p.next()
				option.Enforced = true
			}
		case tokCheck:
			p.next()
			option.Tp = ast.ColumnOptionCheck
			option.Enforced = true // default is ENFORCED
			p.expect('(')
			option.Expr = p.parseExpression(precNone)
			p.expect(')')
			// EnforcedOrNotOrNotNullOpt: parser.y line 3732
			// NOT NULL → injects both CHECK and a separate ColumnOptionNotNull
			// NOT ENFORCED → sets Enforced = false
			// ENFORCED → sets Enforced = true (default)
			if p.peek().Tp == tokNot {
				if p.peekN(1).Tp == tokNull {
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
		case tokReferences:
			option.Tp = ast.ColumnOptionReference
			option.Refer = p.parseReferenceDef()
		case tokCollate:
			// Check for duplicate COLLATE: either already consumed by
			// parseStringOptions on the field type, or already present
			// as a prior column option. Implicit type collation (e.g. BINARY)
			// does NOT count as duplicate.
			if hasExplicitCollate {
				p.error(p.peek().Offset, "duplicate COLLATE clause")
				return nil
			}
			for _, prev := range options {
				if prev.Tp == ast.ColumnOptionCollate {
					p.error(p.peek().Offset, "duplicate COLLATE clause")
					return nil
				}
			}
			p.next()
			option.Tp = ast.ColumnOptionCollate
			tok := p.peek()
			if isIdentLike(tok.Tp) || tok.Tp == tokStringLit {
				info, err := charset.GetCollationByName(tok.Lit)
				if err != nil {
					p.errs = append(p.errs, err)
					return nil
				}
				option.StrValue = info.Name
				p.next()
			} else if tok.Tp == tokBinary || tok.Tp == tokByte {
				option.StrValue = charset.CollationBin
				p.next()
			} else {
				p.error(tok.Offset, "expected collation name")
				return nil
			}
		case tokColumnFormat:
			p.next()
			option.Tp = ast.ColumnOptionColumnFormat
			switch {
			case p.peek().Tp == tokDefault:
				p.next()
				option.StrValue = "DEFAULT"
			case p.peek().Tp == tokFixed:
				p.next()
				option.StrValue = "FIXED"
			case p.peek().IsKeyword("DYNAMIC"):
				p.next()
				option.StrValue = "DYNAMIC"
			default:
				p.error(p.peek().Offset, "expected DEFAULT, FIXED, or DYNAMIC for COLUMN_FORMAT")
				return nil
			}
		case tokStorage:
			p.next()
			option.Tp = ast.ColumnOptionStorage
			switch p.peek().Tp {
			case tokDefault:
				p.next()
				option.StrValue = "DEFAULT"
			case tokDisk:
				p.next()
				option.StrValue = "DISK"
			case tokMemory:
				p.next()
				option.StrValue = "MEMORY"
			default:
				p.error(p.peek().Offset, "expected DEFAULT, DISK, or MEMORY for STORAGE")
				return nil
			}
			p.warn("The STORAGE clause is parsed but ignored by all storage engines.")
		case tokAutoRandom:
			p.next()
			option.Tp = ast.ColumnOptionAutoRandom
			option.AutoRandOpt.ShardBits = types.UnspecifiedLength
			option.AutoRandOpt.RangeBits = types.UnspecifiedLength
			if p.peek().Tp == '(' {
				p.next() // consume '('
				if tok, ok := p.expect(tokIntLit); ok {
					option.AutoRandOpt.ShardBits = int(tokenItemToUint64(tok.Item))
				}
				if p.peek().Tp == ',' {
					p.next()
					if tok, ok := p.expect(tokIntLit); ok {
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
		option.Expr.SetText(nil, strings.TrimSpace(p.src[startOff:endOff]))
	}
	p.expect(')')
	// VIRTUAL / STORED
	if _, ok := p.accept(tokStored); ok {
		option.Stored = true
	} else {
		p.accept(tokVirtual) // optional explicit VIRTUAL
	}
}

// parseGlobalLocalOption parses optional GLOBAL/LOCAL suffix for column key options.
func (p *HandParser) parseGlobalLocalOption(option *ast.ColumnOption) {
	if _, ok := p.accept(tokGlobal); ok {
		option.StrValue = "Global"
	} else {
		p.accept(tokLocal)
	}
}
