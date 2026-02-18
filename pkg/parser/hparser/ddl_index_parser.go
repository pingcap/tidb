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

// parseConstraint parses a table constraint: [CONSTRAINT name] PRIMARY KEY ...
func (p *HandParser) parseConstraint() *ast.Constraint {
	cons := Alloc[ast.Constraint](p.arena)

	// Optional CONSTRAINT [symbol]
	if _, ok := p.accept(tokConstraint); ok {
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
	}

	switch p.peek().Tp {
	case tokPrimary:
		p.next()
		p.expect(tokKey)
		cons.Tp = ast.ConstraintPrimaryKey
		// Optional index name: PRIMARY KEY pk_name (col)
		if isIdentLike(p.peek().Tp) && p.peek().Tp != '(' {
			cons.Name = p.next().Lit
		}
		// Optional USING BTREE/HASH before column list
		p.parseIndexDefinition(cons)
	case tokUnique:
		p.next()
		// UNIQUE [INDEX|KEY] [index_name]
		if p.peek().Tp == tokIndex || p.peek().Tp == tokKey {
			p.next()
		}
		cons.Tp = ast.ConstraintUniq
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		// Optional USING BTREE/HASH before column list
		p.parseIndexDefinition(cons)
	case tokForeign:
		p.next()
		p.expect(tokKey)
		cons.Tp = ast.ConstraintForeignKey
		// IF NOT EXISTS
		cons.IfNotExists = p.acceptIfNotExists()
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		cons.Keys = p.parseIndexPartSpecifications()
		cons.Refer = p.parseReferenceDef()
	case tokCheck:
		p.next()
		cons.Tp = ast.ConstraintCheck
		p.expect('(')
		cons.Expr = p.parseExpression(precNone)
		p.expect(')')
		if _, ok := p.accept(tokNot); ok {
			p.expect(tokEnforced)
			cons.Enforced = false
		} else if _, ok := p.accept(tokEnforced); ok {
			cons.Enforced = true
		} else {
			cons.Enforced = true
		}
	case tokIndex, tokKey:
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
	case tokFulltext:
		p.next()
		if p.peek().Tp == tokKey || p.peek().Tp == tokIndex {
			p.next()
		}
		cons.Tp = ast.ConstraintFulltext
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		cons.Keys = p.parseIndexPartSpecifications()
		cons.Option = p.parseIndexOptions()
	case tokVector, tokColumnar:
		var consTp ast.ConstraintType
		if p.peek().Tp == tokVector {
			consTp = ast.ConstraintVector
		} else {
			consTp = ast.ConstraintColumnar
		}
		p.next()
		p.expect(tokIndex)
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

		if _, ok := p.accept(tokAsc); ok {
			// default
		} else if _, ok := p.accept(tokDesc); ok {
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
	p.expect(tokReferences)
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
	if _, ok := p.accept(tokMatch); ok {
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
	for p.peek().Tp == tokOn {
		p.next() // consume ON
		switch p.peek().Tp {
		case tokDelete:
			p.next()
			ref.OnDelete = &ast.OnDeleteOpt{ReferOpt: p.parseReferAction()}
		case tokUpdate:
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
	case tokRestrict:
		p.next()
		return ast.ReferOptionRestrict
	case tokCascade:
		p.next()
		return ast.ReferOptionCascade
	case tokSet:
		p.next()
		if _, ok := p.accept(tokNull); ok {
			return ast.ReferOptionSetNull
		}
		if _, ok := p.accept(tokDefault); ok {
			return ast.ReferOptionSetDefault
		}
		return ast.ReferOptionNoOption
	case tokNo:
		p.next()
		p.accept(tokAction)
		return ast.ReferOptionNoAction
	default:
		return ast.ReferOptionNoOption
	}
}

// parseOptionalUsingIndexType parses optional USING BTREE/HASH before the column list.
// Returns nil if no USING clause is present.
func (p *HandParser) parseOptionalUsingIndexType() *ast.IndexOption {
	if p.peek().Tp != tokUsing && p.peek().Tp != tokType {
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
	case tokBtree:
		p.next()
		return ast.IndexTypeBtree
	case tokHash:
		p.next()
		return ast.IndexTypeHash
	case tokRtree:
		p.next()
		return ast.IndexTypeRtree
	case tokHnsw:
		p.next()
		return ast.IndexTypeHNSW
	case tokHypo:
		p.next()
		return ast.IndexTypeHypo
	case tokInverted:
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
		case tokUsing, tokType:
			p.next()
			resolved := p.resolveIndexType()
			if resolved == ast.IndexTypeInvalid {
				return opt
			}
			opt.Tp = resolved
		case tokKeyBlockSize:
			p.next()
			p.accept(tokEq)
			if tok, ok := p.expectAny(tokIntLit, tokDecLit); ok {
				opt.KeyBlockSize = tokenItemToUint64(tok.Item)
			}
		case tokAddColumnarReplicaOnDemand:
			p.next()
			opt.AddColumnarReplicaOnDemand = 1
		case tokComment:
			p.next()
			if tok, ok := p.expect(tokStringLit); ok {
				opt.Comment = tok.Lit
			}
		case tokWith:
			p.next()
			p.expect(tokParser)
			if tok, ok := p.expect(tokIdentifier); ok {
				opt.ParserName = ast.NewCIStr(tok.Lit)
			}
		case tokVisible, tokInvisible:
			if p.next().Tp == tokVisible {
				opt.Visibility = ast.IndexVisibilityVisible
			} else {
				opt.Visibility = ast.IndexVisibilityInvisible
			}
		case tokClustered, tokNonClustered:
			if p.next().Tp == tokClustered {
				opt.PrimaryKeyTp = ast.PrimaryKeyTypeClustered
			} else {
				opt.PrimaryKeyTp = ast.PrimaryKeyTypeNonClustered
			}
		case tokPreSplitRegions:
			p.next()
			p.accept(tokEq)
			if _, ok := p.accept('('); ok {
				opt.SplitOpt = p.parseSplitOption()
				p.expect(')')
			} else {
				if tok, ok := p.expectAny(tokIntLit, tokDecLit); ok {
					opt.SplitOpt = Alloc[ast.SplitOption](p.arena)
					if val, ok := tok.Item.(int64); ok {
						opt.SplitOpt.Num = val
					} else if val, ok := tok.Item.(uint64); ok {
						opt.SplitOpt.Num = int64(val)
					}
				}
			}
		case tokWhere:
			p.next()
			opt.Condition = p.parseExpression(precNone)
		case tokGlobal, tokLocal:
			opt.Global = p.next().Tp == tokGlobal
		case tokSecondaryEngineAttribute:
			p.next()
			if _, ok := p.accept(tokEq); ok {
			}
			if tok, ok := p.expect(tokStringLit); ok {
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
