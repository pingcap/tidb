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
	cons := p.arena.AllocConstraint()

	// Optional CONSTRAINT [symbol]
	if _, ok := p.accept(constraint); ok {
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
	}

	switch p.peek().Tp {
	case primary:
		p.next()
		p.expect(key)
		cons.Tp = ast.ConstraintPrimaryKey
		// Optional index name: PRIMARY KEY pk_name (col)
		if isIdentLike(p.peek().Tp) && p.peek().Tp != '(' {
			cons.Name = p.next().Lit
		}
		// Optional USING BTREE/HASH before column list
		p.parseIndexDefinition(cons)
	case unique:
		p.next()
		// UNIQUE [INDEX|KEY] [index_name]
		if p.peek().Tp == index || p.peek().Tp == key {
			p.next()
		}
		cons.Tp = ast.ConstraintUniq
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		// Optional USING BTREE/HASH before column list
		p.parseIndexDefinition(cons)
	case foreign:
		p.next()
		p.expect(key)
		cons.Tp = ast.ConstraintForeignKey
		// IF NOT EXISTS
		cons.IfNotExists = p.acceptIfNotExists()
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		cons.Keys = p.parseIndexPartSpecifications()
		cons.Refer = p.parseReferenceDef()
	case check:
		p.next()
		cons.Tp = ast.ConstraintCheck
		p.expect('(')
		cons.Expr = p.parseExpression(precNone)
		p.expect(')')
		if _, ok := p.accept(not); ok {
			p.expect(enforced)
			cons.Enforced = false
		} else {
			p.accept(enforced)
			cons.Enforced = true
		}
	case index, key:
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
	case fulltext:
		p.next()
		if p.peek().Tp == key || p.peek().Tp == index {
			p.next()
		}
		cons.Tp = ast.ConstraintFulltext
		if isIdentLike(p.peek().Tp) {
			cons.Name = p.next().Lit
		}
		cons.Keys = p.parseIndexPartSpecifications()
		cons.Option = p.parseIndexOptions()
	case vectorType, columnar:
		var consTp ast.ConstraintType
		if p.peek().Tp == vectorType {
			consTp = ast.ConstraintVector
		} else {
			consTp = ast.ConstraintColumnar
		}
		p.next()
		p.expect(index)
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
			part.Column = p.arena.AllocColumnName()
			part.Column.Name = ast.NewCIStr(p.next().Lit)
			if p.peek().Tp == '(' {
				part.Length = p.parseFieldLen()
			}
		} else {
			return nil
		}

		if _, ok := p.accept(asc); !ok {
			if _, ok := p.accept(desc); ok {
				part.Desc = true
			}
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
	tok := p.next()
	if tok.Tp != references {
		p.errorNear(tok.Offset+len(tok.Lit), tok.Offset)
		return nil
	}
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
	if _, ok := p.accept(match); ok {
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
		p.warnNear(p.peek().Offset, "The MATCH clause is parsed but ignored by all storage engines.")
	}

	// Parse optional ON DELETE / ON UPDATE
	hasOnDelete, hasOnUpdate := false, false
	for p.peek().Tp == on {
		switch p.peekN(1).Tp {
		case deleteKwd:
			if hasOnDelete {
				p.errorNear(p.peekN(1).Offset+6, p.peekN(1).Offset)
				return nil
			}
			hasOnDelete = true
			p.next() // consume ON
			p.next() // consume DELETE
			ref.OnDelete = &ast.OnDeleteOpt{ReferOpt: p.parseReferAction()}
		case update:
			if hasOnUpdate {
				p.errorNear(p.peekN(1).Offset+6, p.peekN(1).Offset)
				return nil
			}
			hasOnUpdate = true
			p.next() // consume ON
			p.next() // consume UPDATE
			ref.OnUpdate = &ast.OnUpdateOpt{ReferOpt: p.parseReferAction()}
		default:
			// "ON" not followed by DELETE or UPDATE; leave the tokens for later context.
			return ref
		}
	}
	return ref
}

// parseReferAction parses a referential action: RESTRICT | CASCADE | SET NULL | NO ACTION | SET DEFAULT
func (p *HandParser) parseReferAction() ast.ReferOptionType {
	switch p.peek().Tp {
	case restrict:
		p.next()
		return ast.ReferOptionRestrict
	case cascade:
		p.next()
		return ast.ReferOptionCascade
	case set:
		p.next()
		if _, ok := p.accept(null); ok {
			return ast.ReferOptionSetNull
		}
		if _, ok := p.accept(defaultKwd); ok {
			p.warnNear(p.peek().Offset, "The SET DEFAULT clause is parsed but ignored by all storage engines.")
			return ast.ReferOptionSetDefault
		}
		return ast.ReferOptionNoOption
	case no:
		p.next()
		p.accept(action)
		return ast.ReferOptionNoAction
	default:
		return ast.ReferOptionNoOption
	}
}

// parseOptionalUsingIndexType parses optional USING BTREE/HASH before the column list.
// Returns nil if no USING clause is present.
func (p *HandParser) parseOptionalUsingIndexType() *ast.IndexOption {
	if p.peek().Tp != using && p.peek().Tp != tp {
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
	case btree:
		p.next()
		return ast.IndexTypeBtree
	case hash:
		p.next()
		return ast.IndexTypeHash
	case rtree:
		p.next()
		return ast.IndexTypeRtree
	case hnsw:
		p.next()
		return ast.IndexTypeHNSW
	case hypo:
		p.next()
		return ast.IndexTypeHypo
	case inverted:
		p.next()
		return ast.IndexTypeInverted
	default:
		return ast.IndexTypeInvalid
	}
}

// mergeIndexOptions merges two index options, preferring the second for non-zero fields.
// If pre is nil, returns post. If post is nil, returns pre.
func (*HandParser) mergeIndexOptions(pre, post *ast.IndexOption) *ast.IndexOption {
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
		case using, tp:
			p.next()
			resolved := p.resolveIndexType()
			if resolved == ast.IndexTypeInvalid {
				return opt
			}
			opt.Tp = resolved
		case keyBlockSize:
			p.next()
			p.accept(eq)
			if tok, ok := p.expectAny(intLit, decLit); ok {
				opt.KeyBlockSize = tokenItemToUint64(tok.Item)
			}
		case addColumnarReplicaOnDemand:
			p.next()
			opt.AddColumnarReplicaOnDemand = 1
		case comment:
			p.next()
			if tok, ok := p.expect(stringLit); ok {
				opt.Comment = tok.Lit
			}
		case with:
			p.next()
			p.expect(parser)
			if tok, ok := p.expect(identifier); ok {
				opt.ParserName = ast.NewCIStr(tok.Lit)
			}
			p.warnNear(p.peek().Offset, "The WITH PARASER clause is parsed but ignored by all storage engines.")
		case visible, invisible:
			if p.next().Tp == visible {
				opt.Visibility = ast.IndexVisibilityVisible
			} else {
				opt.Visibility = ast.IndexVisibilityInvisible
			}
		case clustered, nonclustered:
			if p.next().Tp == clustered {
				opt.PrimaryKeyTp = ast.PrimaryKeyTypeClustered
			} else {
				opt.PrimaryKeyTp = ast.PrimaryKeyTypeNonClustered
			}
		case preSplitRegions:
			p.next()
			p.accept(eq)
			if _, ok := p.accept('('); ok {
				opt.SplitOpt = p.parseSplitOption()
				p.expect(')')
			} else {
				if tok, ok := p.expectAny(intLit, decLit); ok {
					opt.SplitOpt = Alloc[ast.SplitOption](p.arena)
					if val, ok := tok.Item.(int64); ok {
						opt.SplitOpt.Num = val
					} else if val, ok := tok.Item.(uint64); ok {
						opt.SplitOpt.Num = int64(val)
					}
				}
			}
		case where:
			p.next()
			opt.Condition = p.parseExpression(precNone)
		case global, local:
			opt.Global = p.next().Tp == global
		case secondaryEngineAttribute:
			p.next()
			p.accept(eq)
			if tok, ok := p.expect(stringLit); ok {
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
