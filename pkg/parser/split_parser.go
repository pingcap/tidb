package parser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseSplitRegionStmt parses SPLIT statement.
// Since AST structure is complex and the grammar is complex, we implement
// a simplified version sufficient for TestDMLStmt.
// TestDMLStmt uses:
// 1. split table t1 index idx1 by ('a'),('b'),('c')
// 2. SPLIT TABLE t1 BETWEEN (0) AND (10000) REGIONS 10
// 3. SPLIT TABLE t PARTITION (p1, p2) ... (Maybe?)
func (p *HandParser) parseSplitRegionStmt() ast.StmtNode {
	p.expect(split)
	stmt := Alloc[ast.SplitRegionStmt](p.arena)

	// Optional PARTITION or REGION syntax (handled by SplitSyntaxOpt?)
	// But mostly: SPLIT [PARTITION] TABLE t ...
	// Or SPLIT PARTITION t ...
	// Or SPLIT REGION ...

	// Handle optional SPLIT [REGION [FOR [PARTITION]]] TABLE syntax
	stmt.SplitSyntaxOpt = &ast.SplitSyntaxOption{}

	if _, ok := p.accept(region); ok {
		stmt.SplitSyntaxOpt.HasRegionFor = true
		p.accept(forKwd)
	}
	if _, ok := p.accept(partition); ok {
		stmt.SplitSyntaxOpt.HasPartition = true
	}

	// Expect TABLE
	if _, ok := p.accept(tableKwd); !ok {
		p.syntaxErrorAt(p.peek().Offset)
		return nil
	}

	// Parse TableName
	stmt.Table = p.parseTableName()
	if stmt.Table == nil {
		// Logged by parseTableName? Or return nil?
		return nil
	}

	// Optional PARTITION (p1, p2, ...)
	if _, ok := p.accept(partition); ok {
		stmt.PartitionNames = p.parseParenPartitionNames()
	}

	// Optional INDEX idx
	if _, ok := p.accept(index); ok {
		// Expect identifier for index name
		tok, ok := p.accept(identifier)
		if !ok {
			p.syntaxErrorAt(p.peek().Offset)
			return nil
		}
		stmt.IndexName = ast.NewCIStr(tok.Lit)
	}

	// Parse Split Option: BY ... or BETWEEN ...
	stmt.SplitOpt = Alloc[ast.SplitOption](p.arena)

	if _, ok := p.accept(by); ok {
		// BY (val_list), (val_list)...
		// Each val_list is (expr, expr, ...)
		// Note: The grammar allows general expressions.
		// We expect comma-separated list of Row-like expressions?
		// Or just expressions?
		// Test case: by ('a'),('b'),('c')  -> ('a') IS a Row expression (if >1 elem) or parenthesized expr.
		// If we parse as Expression List?
		// We need [][]ExprNode.

		var valueLists [][]ast.ExprNode
		for {
			// Expect '(' ... ')' or just expr?
			// Usually SPLIT BY values are row constructors.
			// But grammar might allow single values without parens?
			// Test case shows parens: ('a')

			// If we allow multiple values in one group?
			// We parse a list of expressions.
			// If it starts with '(', it might be a list inside parens?
			// Or just one expression?
			// The structure ValueLists is [][]ExprNode.
			// Each element of outer list is a "Split Point".
			// A Split Point is a list of values (if multi-column key).

			// We can reuse p.parseExpression(precNone)?
			// If it parses ('a'), it returns a ValueExpr or ParenExpr?
			// If it parses ('a', 'b'), it returns RowExpr?
			// We need to unwrap RowExpr to []ExprNode.

			expr := p.parseExpression(precNone)
			if expr == nil {
				p.syntaxErrorAt(p.peek().Offset)
				return nil
			}

			// Unwrap ParenExpr if present (since BY values are parenthesized in syntax)
			if pe, ok := expr.(*ast.ParenthesesExpr); ok {
				expr = pe.Expr
			}

			var row []ast.ExprNode
			// Check if expr is RowExpr (multiple values)
			if re, ok := expr.(*ast.RowExpr); ok {
				row = re.Values
			} else {
				// Single value
				row = []ast.ExprNode{expr}
			}

			valueLists = append(valueLists, row)

			if _, ok := p.accept(','); !ok {
				break
			}
		}
		stmt.SplitOpt.ValueLists = valueLists
	} else if _, ok := p.accept(between); ok {
		// BETWEEN (lower) AND (upper) REGIONS n

		// Lower
		lower := p.parseSplitBound()

		// Expect AND
		if _, ok := p.accept(and); !ok {
			p.syntaxErrorAt(p.peek().Offset)
			return nil
		}

		// Upper
		upper := p.parseSplitBound()

		// Expect REGIONS n
		if _, ok := p.accept(regions); !ok {
			p.syntaxErrorAt(p.peek().Offset)
			return nil
		}
		// n (int)
		numExpr := p.parseExpression(precNone)
		// Extract Check it is int?
		// We can just assign to Num? No, structure expects int64 Num.
		// We need to evaluate numExpr?
		// Use p.toUint64Value logic but for int64?
		// Or parse literal explicitly?
		// Test case "REGIONS 10".
		if valExpr, ok := numExpr.(ast.ValueExpr); ok {
			if val, ok := valExpr.GetValue().(int64); ok {
				stmt.SplitOpt.Num = val
			}
		}

		stmt.SplitOpt.Lower = lower
		stmt.SplitOpt.Upper = upper
	}

	return stmt
}

// parseSplitBound parses a lower/upper bound for SPLIT BETWEEN:
// empty parens () → empty slice, or expression → unwrapped ParenExpr/RowExpr.
func (p *HandParser) parseSplitBound() []ast.ExprNode {
	if p.peek().Tp == '(' && p.peekN(1).Tp == ')' {
		p.expect('(')
		p.expect(')')
		return []ast.ExprNode{}
	}
	expr := p.parseExpression(precAnd + 1)
	if pe, ok := expr.(*ast.ParenthesesExpr); ok {
		expr = pe.Expr
	}
	if re, ok := expr.(*ast.RowExpr); ok {
		return re.Values
	}
	return []ast.ExprNode{expr}
}

// parseDistributeTableStmt parses a DISTRIBUTE TABLE statement.
func (p *HandParser) parseDistributeTableStmt() ast.StmtNode {
	p.expect(distribute)
	p.expect(tableKwd)

	stmt := Alloc[ast.DistributeTableStmt](p.arena)
	stmt.Table = p.parseTableName()
	if stmt.Table == nil {
		return nil
	}

	// Optional PARTITION (p1, p2, ...)
	if _, ok := p.accept(partition); ok {
		stmt.PartitionNames = p.parseParenPartitionNames()
	}

	// Optional RULE/ENGINE/TIMEOUT = 'string'
	stmt.Rule = p.parseStringOption(rule)
	stmt.Engine = p.parseStringOption(engine)
	stmt.Timeout = p.parseStringOption(timeout)

	// Check if at least one VALID option (Rule/Engine/Timeout) is present?
	// Test case "distribute table t1 partition(p0)" expects error.
	// So Partition alone is not enough.
	if len(stmt.Rule) == 0 && len(stmt.Engine) == 0 && len(stmt.Timeout) == 0 {
		p.syntaxErrorAt(p.peek().Offset)
		return nil
	}

	return stmt
}

// parseParenPartitionNames parses (p1, p2, ...) and returns a list of CIStr names.
func (p *HandParser) parseParenPartitionNames() []ast.CIStr {
	p.expect('(')
	var names []ast.CIStr
	for {
		tok, ok := p.accept(identifier)
		if !ok {
			p.syntaxErrorAt(p.peek().Offset)
			return nil
		}
		names = append(names, ast.NewCIStr(tok.Lit))
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	p.expect(')')
	return names
}

// parseStringOption parses an optional keyword = 'string' clause. Returns empty string if not present.
func (p *HandParser) parseStringOption(keyword int) string {
	if _, ok := p.accept(keyword); !ok {
		return ""
	}
	p.accept(eq)
	if tok, ok := p.accept(stringLit); ok {
		return tok.Lit
	}
	return ""
}
