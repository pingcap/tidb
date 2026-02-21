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
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

func (p *HandParser) parsePartitionOptions() *ast.PartitionOptions {
	p.expect(partition)
	p.expect(by)

	opt := Alloc[ast.PartitionOptions](p.arena)

	// [LINEAR] HASH | KEY | RANGE | LIST
	if _, ok := p.accept(linear); ok {
		opt.Linear = true
	}

	tok := p.next()
	switch tok.Tp {
	case hash:
		opt.Tp = ast.PartitionTypeHash
		opt.Expr = p.parsePartitionExpr("invalid expression in partition definition")
		if opt.Expr == nil {
			return nil
		}
	case key:
		opt.Tp = ast.PartitionTypeKey
		p.parseKeyAlgorithmAndColumns(&opt.PartitionMethod)
	case rangeKwd:
		opt.Tp = ast.PartitionTypeRange
		if _, ok := p.acceptAny(columns, fields); ok {
			opt.ColumnNames = p.parseColumnsNameList()
			if len(opt.ColumnNames) == 0 {
				p.error(p.peek().Offset, "COLUMNS partition requires at least one column")
				return nil
			}
		} else {
			opt.Expr = p.parsePartitionExpr("invalid expression in partition definition")
			if opt.Expr == nil {
				return nil
			}
		}

		if _, ok := p.accept(interval); ok {
			p.expect('(')
			intervalExpr := p.parseExpression(0)

			var unit *ast.TimeUnitExpr
			if tok := p.peek(); tok.Tp != ')' && tok.Tp != 0 {
				unit = p.parseTimeUnit()
			}

			p.expect(')')
			pi := Alloc[ast.PartitionInterval](p.arena)
			pi.IntervalExpr.Expr = intervalExpr
			if unit != nil {
				pi.IntervalExpr.TimeUnit = unit.Unit
			}

			// FIRST PARTITION LESS THAN (expr)
			if _, ok := p.accept(first); ok {
				expr := p.parsePartitionLessThanBound()
				pi.FirstRangeEnd = &expr
			}

			// LAST PARTITION LESS THAN (expr)
			if _, ok := p.accept(last); ok {
				expr := p.parsePartitionLessThanBound()
				pi.LastRangeEnd = &expr
			}

			// NULL PARTITION
			if _, ok := p.accept(null); ok {
				p.expect(partition)
				pi.NullPart = true
			}

			// MAXVALUE PARTITION
			if _, ok := p.accept(maxValue); ok {
				p.expect(partition)
				pi.MaxValPart = true
			}

			if pi.FirstRangeEnd != nil && pi.LastRangeEnd == nil {
				p.error(p.peek().Offset, "FIRST PARTITION must be followed by LAST PARTITION")
				return nil
			}

			opt.Interval = pi
		}
	case list:
		opt.Tp = ast.PartitionTypeList
		if _, ok := p.acceptAny(columns, fields); ok {
			opt.ColumnNames = p.parseColumnsNameList()
			if len(opt.ColumnNames) == 0 {
				p.error(p.peek().Offset, "COLUMNS partition requires at least one column")
				return nil
			}
		} else {
			opt.Expr = p.parsePartitionExpr("invalid expression in partition definition")
			if opt.Expr == nil {
				return nil
			}
		}
	case systemTime:
		opt.Tp = ast.PartitionTypeSystemTime
		// INTERVAL expr unit
		if _, ok := p.accept(interval); ok {
			opt.Expr = p.parseExpression(0)
			unit := p.parseTimeUnit()
			if unit != nil {
				opt.Unit = unit.Unit
			}
		}
		// LIMIT N
		if _, ok := p.accept(limit); ok {
			opt.Limit = p.parseUint64()
		}
		// Cannot have both INTERVAL and LIMIT
		if opt.Expr != nil && opt.Limit > 0 {
			p.error(p.peek().Offset, "SYSTEM_TIME partition cannot have both INTERVAL and LIMIT")
			return nil
		}
	default:
		return nil
	}

	// [SUBPARTITION BY ...]
	if p.peekKeyword(subpartition, "SUBPARTITION") {
		p.next()
		p.expect(by)

		subOpt := Alloc[ast.PartitionMethod](p.arena)

		// [LINEAR] HASH | KEY
		if _, ok := p.accept(linear); ok {
			subOpt.Linear = true
		}

		tok := p.next()
		switch tok.Tp {
		case hash:
			subOpt.Tp = ast.PartitionTypeHash
			subOpt.Expr = p.parsePartitionExpr("invalid expression in subpartition definition")
			if subOpt.Expr == nil {
				return nil
			}
		case key:
			subOpt.Tp = ast.PartitionTypeKey
			p.parseKeyAlgorithmAndColumns(subOpt)
		default:
			p.error(tok.Offset, "Only HASH/KEY partitions are supported for subpartitions")
			return nil
		}

		// [SUBPARTITIONS num]
		if _, ok := p.acceptKeyword(subpartitions, "SUBPARTITIONS"); ok {
			subOpt.Num = p.parseUint64()
			if subOpt.Num == 0 {
				p.error(p.peek().Offset, "Number of subpartitions must be a positive integer")
				return nil
			}
		}

		opt.Sub = subOpt
	}

	// [PARTITIONS num]
	if _, ok := p.acceptKeyword(partitions, "PARTITIONS"); ok {
		opt.Num = p.parseUint64()
		if opt.Num == 0 {
			p.error(p.peek().Offset, "Number of partitions must be a positive integer")
			return nil
		}
	}

	// [(PARTITION p1 VALUES ... [options] [(SUBPARTITION ...)] , ...)]
	if _, ok := p.accept('('); ok {
		for {
			def := p.parsePartitionDef(opt.Tp)
			if def == nil {
				break
			}
			opt.Definitions = append(opt.Definitions, def)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		if len(opt.Definitions) == 0 {
			p.error(p.peek().Offset, "partition definitions list cannot be empty")
			return nil
		}
		p.expect(')')
	}

	// UPDATE INDEXES (idx global|local, ...)
	if _, ok := p.accept(update); ok {
		p.expect(indexes)
		p.expect('(')
		for {
			tok := p.next()
			if !isIdentLike(tok.Tp) {
				p.syntaxError(tok.Offset)
				return nil
			}
			c := Alloc[ast.Constraint](p.arena)
			c.Name = tok.Lit
			c.Option = Alloc[ast.IndexOption](p.arena)
			if _, ok := p.accept(global); ok {
				c.Option.Global = true
			} else {
				p.expect(local)
			}
			opt.UpdateIndexes = append(opt.UpdateIndexes, c)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}

	if err := opt.Validate(); err != nil {
		p.errs = append(p.errs, err)
		return nil
	}

	return opt
}

// isValidPartitionExpr validates that an expression is allowed in a partition
// VALUES LESS THAN clause. MySQL's PartitionNumOrExpr production disallows
// logical operators like NOT, OR, AND, XOR, IS NULL, IS TRUE, etc.
func (p *HandParser) isValidPartitionExpr(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *ast.BinaryOperationExpr:
		// Reject logical operators: OR, AND, LogicXor
		switch e.Op {
		case opcode.LogicOr, opcode.LogicAnd, opcode.LogicXor:
			return false
		}
	case *ast.UnaryOperationExpr:
		// Reject NOT
		if e.Op == opcode.Not || e.Op == opcode.Not2 {
			return false
		}
	case *ast.PatternInExpr:
		// Reject IN expressions
		return false
	case *ast.IsNullExpr:
		return false
	case *ast.IsTruthExpr:
		return false
	}
	return true
}

// parsePartitionExpr parses a partition expression (expr) check validity.
func (p *HandParser) parsePartitionExpr(errMsg string) ast.ExprNode {
	p.expect('(')
	expr := p.parseExpression(0)
	if !p.isValidPartitionExpr(expr) {
		p.error(p.peek().Offset, "%s", errMsg)
		return nil
	}
	p.expect(')')
	return expr
}

// parseSplitIndexOption parses SPLIT PRIMARY KEY / INDEX ... options.
func (p *HandParser) parseSplitIndexOption() *ast.SplitIndexOption {
	p.expect(split)
	opt := Alloc[ast.SplitIndexOption](p.arena)

	if _, ok := p.accept(region); ok {
		// handle optional REGION
	}

	if _, ok := p.accept(primary); ok {
		p.expect(key)
		opt.PrimaryKey = true
	} else if _, ok := p.accept(index); ok {
		if tok, ok := p.expect(identifier); ok {
			opt.IndexName = ast.NewCIStr(tok.Lit)
		} else {
			p.error(p.peek().Offset, "expected index name")
			return nil
		}
	} else if _, ok := p.accept(tableKwd); ok {
		opt.TableLevel = true
	} else {
		// Implicit Table Level if followed by BETWEEN or BY
		if tp := p.peek().Tp; tp == between || tp == by {
			opt.TableLevel = true
		} else {
			p.error(p.peek().Offset, "expected PRIMARY KEY, INDEX or TABLE after SPLIT")
			return nil
		}
	}

	opt.SplitOpt = p.parseSplitOption()
	return opt
}

// parseSplitOption parses BETWEEN ... AND ... REGIONS ...
func (p *HandParser) parseSplitOption() *ast.SplitOption {
	opt := Alloc[ast.SplitOption](p.arena)
	if _, ok := p.accept(between); ok {
		// Lower
		p.expect('(')
		for {
			expr := p.parseExpression(precNone)
			opt.Lower = append(opt.Lower, expr)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')

		p.expect(and)

		// Upper
		p.expect('(')
		for {
			expr := p.parseExpression(precNone)
			opt.Upper = append(opt.Upper, expr)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')

		p.expect(regions)
		opt.Num = p.parseIntLit()
	} else if _, ok := p.accept(by); ok {
		// BY ...
		// (val1), (val2) ...
		for {
			var exprs []ast.ExprNode
			p.expect('(')
			for {
				expr := p.parseExpression(precNone)
				exprs = append(exprs, expr)
				if _, ok := p.accept(','); !ok {
					break
				}
			}
			p.expect(')')
			opt.ValueLists = append(opt.ValueLists, exprs)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}
	return opt
}

// parseColumnsNameList parses a parenthesized column name list: (col1, col2, ...)
func (p *HandParser) parseColumnsNameList() []*ast.ColumnName {
	p.expect('(')
	var cols []*ast.ColumnName
	if p.peek().Tp != ')' {
		for {
			col := p.parseColumnName()
			if col == nil {
				break
			}
			cols = append(cols, col)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}
	p.expect(')')
	return cols
}

// parseKeyAlgorithmAndColumns parses [ALGORITHM = N] (col, col, ...) for KEY partitions.
// Works for both PartitionOptions (main) and PartitionMethod (sub).
func (p *HandParser) parseKeyAlgorithmAndColumns(opt *ast.PartitionMethod) {
	if _, ok := p.accept(algorithm); ok {
		p.expectAny(eq, assignmentEq)
		algTok, _ := p.expect(intLit)
		alg := &ast.PartitionKeyAlgorithm{}
		if v, ok := algTok.Item.(int64); ok {
			alg.Type = uint64(v)
		} else if v, ok := algTok.Item.(uint64); ok {
			alg.Type = v
		}
		if alg.Type != 1 && alg.Type != 2 {
			p.error(algTok.Offset, "Unknown partition key algorithm")
			return
		}
		opt.KeyAlgorithm = alg
	}
	opt.ColumnNames = p.parseColumnsNameList()
}

// parsePartitionLessThanBound parses: PARTITION LESS THAN (expr)
func (p *HandParser) parsePartitionLessThanBound() ast.ExprNode {
	p.expect(partition)
	p.expect(less)
	p.expect(than)
	p.expect('(')
	expr := p.parseExpression(0)
	p.expect(')')
	return expr
}
