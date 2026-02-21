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

// parseConstraint parses a table constraint: [CONSTRAINT name] PRIMARY KEY ...
func (p *HandParser) parseConstraint() *ast.Constraint {
	cons := Alloc[ast.Constraint](p.arena)

	// Optional CONSTRAINT [symbol]
	if _, ok := p.accept(57386); ok {
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
	}

	switch p.peek().Tp {
	case 57518:
		p.next()
		p.expect(57467)
		cons.Tp = ast.ConstraintPrimaryKey
		// Optional index name: PRIMARY KEY pk_name (col)
		if isIdentLike(p.peek().Tp) && p.peek().Tp != '(' {
			cons.Name = p.next().Lit
		}
		// Optional USING BTREE/HASH before column list
		p.parseIndexDefinition(cons)
	case 57569:
		p.next()
		// UNIQUE [INDEX|KEY] [index_name]
		if p.peek().Tp == 57449 || p.peek().Tp == 57467 {
			p.next()
		}
		cons.Tp = ast.ConstraintUniq
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		// Optional USING BTREE/HASH before column list
		p.parseIndexDefinition(cons)
	case 57433:
		p.next()
		p.expect(57467)
		cons.Tp = ast.ConstraintForeignKey
		// IF NOT EXISTS
		cons.IfNotExists = p.acceptIfNotExists()
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		cons.Keys = p.parseIndexPartSpecifications()
		cons.Refer = p.parseReferenceDef()
	case 57383:
		p.next()
		cons.Tp = ast.ConstraintCheck
		p.expect('(')
		cons.Expr = p.parseExpression(precNone)
		p.expect(')')
		if _, ok := p.accept(57498); ok {
			p.expect(57702)
			cons.Enforced = false
		} else if _, ok := p.accept(57702); ok {
			cons.Enforced = true
		} else {
			cons.Enforced = true
		}
	case 57449, 57467:
		p.next()
		cons.Tp = ast.ConstraintIndex
		// IF NOT EXISTS
		cons.IfNotExists = p.acceptIfNotExists()
		if isIdentLike(p.peek().Tp) {
			tok := p.next()
			cons.Name = tok.Lit
			// Distinguish between no name (no token) and explicitly empty name (e.g. ``).
			if tok.Lit == "" {
				cons.IsEmptyIndex = true
			}
		}
		// Optional USING BTREE/HASH before column list
		p.parseIndexDefinition(cons)
	case 57435:
		p.next()
		if p.peek().Tp == 57467 || p.peek().Tp == 57449 {
			p.next()
		}
		cons.Tp = ast.ConstraintFulltext
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		cons.Keys = p.parseIndexPartSpecifications()
		cons.Option = p.parseIndexOptions()
	case 57979, 57652:
		var consTp ast.ConstraintType
		if p.peek().Tp == 57979 {
			consTp = ast.ConstraintVector
		} else {
			consTp = ast.ConstraintColumnar
		}
		p.next()
		p.expect(57449)
		cons.IfNotExists = p.acceptIfNotExists()
		if isIdentLike(p.peek().Tp) && p.peek().Tp != '(' {
			cons.Name = p.next().Lit
			if len(cons.Name) == 0 {
				cons.IsEmptyIndex = true
			}
		} else {
			cons.IsEmptyIndex = false
		}
		p.parseIndexDefinition(cons)
		cons.Tp = consTp
	default:
		return nil
	}
	return cons
}

// parseIndexDefinition parses: [USING BTREE/HASH] (col_list) [index_options]
func (p *HandParser) parseIndexDefinition(cons *ast.Constraint) {
	cons.Option = p.parseOptionalUsingIndexType()
	cons.Keys = p.parseIndexPartSpecifications()
	cons.Option = p.mergeIndexOptions(cons.Option, p.parseIndexOptions())
}

// parseIndexPartSpecifications parses (col1 [(length)] [ASC|DESC], ...)
// Also supports expression indexes: ((expr))
func (p *HandParser) parseIndexPartSpecifications() []*ast.IndexPartSpecification {
	p.expect('(')
	var parts []*ast.IndexPartSpecification
	for {
		part := Alloc[ast.IndexPartSpecification](p.arena)
		part.Length = types.UnspecifiedLength // -1 = no prefix length specified
		// Expression index: ((expr))
		if p.peek().Tp == '(' {
			p.next() // consume '('
			part.Expr = p.parseExpression(precNone)
			p.expect(')')
		} else if isIdentLike(p.peek().Tp) {
			part.Column = Alloc[ast.ColumnName](p.arena)
			part.Column.Name = ast.NewCIStr(p.next().Lit)
			if p.peek().Tp == '(' {
				part.Length = p.parseFieldLen()
			}
		} else {
			return nil
		}

		if _, ok := p.accept(57370); ok {
			// default
		} else if _, ok := p.accept(57409); ok {
			part.Desc = true
		}

		parts = append(parts, part)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	p.expect(')')
	return parts
}

// parseReferenceDef parses REFERENCES tbl (cols) [MATCH FULL|PARTIAL|SIMPLE] [ON DELETE opt] [ON UPDATE opt]
func (p *HandParser) parseReferenceDef() *ast.ReferenceDef {
	p.expect(57525)
	ref := Alloc[ast.ReferenceDef](p.arena)
	ref.Table = p.parseTableName()
	// Column list is optional (yacc: IndexPartSpecificationListOpt)
	if p.peek().Tp == '(' {
		ref.IndexPartSpecifications = p.parseIndexPartSpecifications()
	}

	// Always initialize OnDelete/OnUpdate — ReferenceDef.Accept() calls them unconditionally.
	ref.OnDelete = &ast.OnDeleteOpt{ReferOpt: ast.ReferOptionNoOption}
	ref.OnUpdate = &ast.OnUpdateOpt{ReferOpt: ast.ReferOptionNoOption}

	// Parse optional MATCH FULL | PARTIAL | SIMPLE
	if _, ok := p.accept(57488); ok {
		switch {
		case p.peek().IsKeyword("FULL"):
			p.next()
			ref.Match = ast.MatchFull
		case p.peek().IsKeyword("PARTIAL"):
			p.next()
			ref.Match = ast.MatchPartial
		case p.peek().IsKeyword("SIMPLE"):
			p.next()
			ref.Match = ast.MatchSimple
		}
	}

	// Parse optional ON DELETE / ON UPDATE
	for p.peek().Tp == 57505 {
		p.next() // consume ON
		switch p.peek().Tp {
		case 57407:
			p.next()
			ref.OnDelete = &ast.OnDeleteOpt{ReferOpt: p.parseReferAction()}
		case 57573:
			p.next()
			ref.OnUpdate = &ast.OnUpdateOpt{ReferOpt: p.parseReferAction()}
		default:
			return ref
		}
	}
	return ref
}

// parseReferAction parses a referential action: RESTRICT | CASCADE | SET NULL | NO ACTION | SET DEFAULT
func (p *HandParser) parseReferAction() ast.ReferOptionType {
	switch p.peek().Tp {
	case 57532:
		p.next()
		return ast.ReferOptionRestrict
	case 57378:
		p.next()
		return ast.ReferOptionCascade
	case 57541:
		p.next()
		if _, ok := p.accept(57502); ok {
			return ast.ReferOptionSetNull
		}
		if _, ok := p.accept(57405); ok {
			return ast.ReferOptionSetDefault
		}
		return ast.ReferOptionNoOption
	case 57799:
		p.next()
		p.accept(57596)
		return ast.ReferOptionNoAction
	default:
		return ast.ReferOptionNoOption
	}
}

// parseOptionalUsingIndexType parses optional USING BTREE/HASH before the column list.
// Returns nil if no USING clause is present.
func (p *HandParser) parseOptionalUsingIndexType() *ast.IndexOption {
	if p.peek().Tp != 57576 && p.peek().Tp != 57968 {
		return nil
	}
	p.next() // consume USING or TYPE
	opt := Alloc[ast.IndexOption](p.arena)
	opt.Tp = p.resolveIndexType()
	return opt
}

// resolveIndexType maps the current token to an IndexType constant and consumes it.
func (p *HandParser) resolveIndexType() ast.IndexType {
	switch p.peek().Tp {
	case 57631:
		p.next()
		return ast.IndexTypeBtree
	case 57736:
		p.next()
		return ast.IndexTypeHash
	case 57883:
		p.next()
		return ast.IndexTypeRtree
	case 58050:
		p.next()
		return ast.IndexTypeHNSW
	case 57742:
		p.next()
		return ast.IndexTypeHypo
	case 58033:
		p.next()
		return ast.IndexTypeInverted
	default:
		return ast.IndexTypeInvalid
	}
}

// mergeIndexOptions merges two index options, preferring the second for non-zero fields.
// If pre is nil, returns post. If post is nil, returns pre.
func (p *HandParser) mergeIndexOptions(pre, post *ast.IndexOption) *ast.IndexOption {
	if pre == nil {
		return post
	}
	if post == nil {
		return pre
	}
	// Merge: USING from pre, other fields from post
	if post.Tp == ast.IndexTypeInvalid && pre.Tp != ast.IndexTypeInvalid {
		post.Tp = pre.Tp
	}
	return post
}

// parseIndexOptions parses index options: USING BTREE/HASH, COMMENT '...', VISIBLE/INVISIBLE
// It returns a single *ast.IndexOption by merging multiple options.
func (p *HandParser) parseIndexOptions() *ast.IndexOption {
	opt := Alloc[ast.IndexOption](p.arena)
	for {
		switch p.peek().Tp {
		case 57576, 57968:
			p.next()
			resolved := p.resolveIndexType()
			if resolved == ast.IndexTypeInvalid {
				return opt
			}
			opt.Tp = resolved
		case 57760:
			p.next()
			p.accept(58202)
			if tok, ok := p.expectAny(58197, 58196); ok {
				opt.KeyBlockSize = tokenItemToUint64(tok.Item)
			}
		case 57597:
			p.next()
			opt.AddColumnarReplicaOnDemand = 1
		case 57655:
			p.next()
			if tok, ok := p.expect(57353); ok {
				opt.Comment = tok.Lit
			}
		case 57590:
			p.next()
			p.expect(57825)
			if tok, ok := p.expect(57346); ok {
				opt.ParserName = ast.NewCIStr(tok.Lit)
			}
		case 57981, 57753:
			if p.next().Tp == 57981 {
				opt.Visibility = ast.IndexVisibilityVisible
			} else {
				opt.Visibility = ast.IndexVisibilityInvisible
			}
		case 57649, 57805:
			if p.next().Tp == 57649 {
				opt.PrimaryKeyTp = ast.PrimaryKeyTypeClustered
			} else {
				opt.PrimaryKeyTp = ast.PrimaryKeyTypeNonClustered
			}
		case 57842:
			p.next()
			p.accept(58202)
			if _, ok := p.accept('('); ok {
				opt.SplitOpt = p.parseSplitOption()
				p.expect(')')
			} else {
				if tok, ok := p.expectAny(58197, 58196); ok {
					opt.SplitOpt = Alloc[ast.SplitOption](p.arena)
					if val, ok := tok.Item.(int64); ok {
						opt.SplitOpt.Num = val
					} else if val, ok := tok.Item.(uint64); ok {
						opt.SplitOpt.Num = int64(val)
					}
				}
			}
		case 57587:
			p.next()
			opt.Condition = p.parseExpression(precNone)
		case 57733, 57770:
			opt.Global = p.next().Tp == 57733
		case 57890:
			p.next()
			if _, ok := p.accept(58202); ok {
			}
			if tok, ok := p.expect(57353); ok {
				opt.SecondaryEngineAttr = tok.Lit
			}
		default:
			if opt.IsEmpty() {
				return nil
			}
			return opt
		}
	}
}
