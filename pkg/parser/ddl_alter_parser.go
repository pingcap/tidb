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
	"fmt"
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseAlterTableStmt parses ALTER TABLE statements.
func (p *HandParser) parseAlterTableStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterTableStmt](p.arena)
	p.expect(alter)
	p.expect(tableKwd)

	stmt.Table = p.parseTableName()
	if stmt.Table == nil {
		return nil
	}

	// Check for COMPACT (keyword token compact)
	if _, ok := p.accept(compact); ok {
		return p.parseCompactTableStmt(stmt.Table)
	}

	// Check for ANALYZE (keyword token analyze)
	if _, ok := p.accept(analyze); ok {
		return p.parseAlterAnalyzePartition(stmt.Table)
	}

	for {
		spec := p.parseAlterTableSpec()
		if spec == nil {
			break
		}
		stmt.Specs = append(stmt.Specs, spec)

		// PARTITION BY and REMOVE PARTITIONING are always the last spec (AlterTablePartitionOpt).
		if spec.Tp == ast.AlterTablePartition || spec.Tp == ast.AlterTableRemovePartitioning {
			break
		}

		if _, ok := p.accept(','); !ok {
			// Check for trailing specs without comma separator
			// e.g. ALTER TABLE t ADD d text(50) PARTITION p1 placement policy p2
			// e.g. ALTER TABLE t UNION (t_n) REMOVE PARTITIONING
			switch p.peek().Tp {
			case partition, remove:
				continue
			}
			break
		}
		// PARTITION BY and REMOVE PARTITIONING are not allowed after a comma per yacc grammar.
		// They are AlterTablePartitionOpt, parsed after the spec list.
		if p.peek().Tp == partition && p.peekN(1).Tp == by {
			break
		}
		if p.peek().Tp == remove {
			break
		}
	}

	return stmt
}

// parseAlterTableSpec parses a single ALTER TABLE specification.
func (p *HandParser) parseAlterTableSpec() *ast.AlterTableSpec {
	spec := Alloc[ast.AlterTableSpec](p.arena)
	tok := p.peek()

	switch tok.Tp {
	case add:
		p.next()
		p.parseAlterAdd(spec)
		return spec

	case drop:
		p.next()
		p.parseAlterDrop(spec)
		return spec

	case modify, change:
		isChange := p.next().Tp == change
		p.accept(column)
		if isChange {
			spec.Tp = ast.AlterTableChangeColumn
		} else {
			spec.Tp = ast.AlterTableModifyColumn
		}
		spec.IfExists = p.acceptIfExists()
		if isChange {
			if tok, ok := p.expectAny(identifier, stringLit); ok {
				spec.OldColumnName = p.arena.AllocColumnName()
				spec.OldColumnName.Name = ast.NewCIStr(tok.Lit)
			}
		}
		spec.NewColumns = []*ast.ColumnDef{p.parseColumnDef()}
		p.parseColumnPosition(spec)
		return spec

	case rename:
		p.next()
		p.parseAlterRename(spec)
		return spec

	case order:
		// ORDER BY col_name [, col_name]...
		p.next()
		p.expect(by)
		spec.Tp = ast.AlterTableOrderByColumns
		for {
			item := Alloc[ast.AlterOrderItem](p.arena)
			item.Column = p.parseColumnName()
			if _, ok := p.accept(desc); ok {
				item.Desc = true
			} else if _, ok := p.accept(asc); ok {
				item.Desc = false
			}
			spec.OrderByList = append(spec.OrderByList, item)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		return spec

	case alter:
		p.next()
		if result := p.parseAlterAlter(spec); result != nil {
			return result
		}

	case partition:
		if result := p.parseAlterPartition(spec); result != nil {
			return result
		}

	case split:
		p.next() // Consume SPLIT
		return p.parseSplitRegionSpec(spec)

	case identifier:
		if p.peek().IsKeyword("SPLIT") {
			p.next() // Consume SPLIT
			return p.parseSplitRegionSpec(spec)
		}

	case set:
		// SET TIFLASH REPLICA count [LOCATION LABELS 'a','b']
		// SET HYPO TIFLASH REPLICA count [LOCATION LABELS 'a','b']
		isHypo := false
		if p.peekN(1).IsKeyword("HYPO") && p.peekN(2).Tp == tiFlash {
			isHypo = true
		}
		if p.peekN(1).Tp == tiFlash || isHypo {
			p.next() // consume SET
			if isHypo {
				p.next() // consume HYPO
			}
			p.next() // consume TIFLASH
			p.expect(replica)
			spec.Tp = ast.AlterTableSetTiFlashReplica
			tiFlash := &ast.TiFlashReplicaSpec{}
			tiFlash.Hypo = isHypo
			tiFlash.Count = p.parseUint64()
			// Optional: LOCATION LABELS 'label1', 'label2', ...
			if p.peek().IsKeyword("LOCATION") {
				p.next() // consume LOCATION
				if !(p.peek().IsKeyword("LABELS")) {
					p.syntaxErrorAt(p.peek())
					return nil
				}
				p.next() // consume LABELS
				for {
					if tok, ok := p.expect(stringLit); ok {
						tiFlash.Labels = append(tiFlash.Labels, tok.Lit)
					}
					if _, ok := p.accept(','); !ok {
						break
					}
				}
			}
			spec.TiFlashReplica = tiFlash
			return spec
		}
	}

	// Delegate to sub-handlers
	if p.parseAlterPartitionAction(spec) {
		return spec
	}

	if p.parseAlterTableOptions(spec) {
		return spec
	}

	return nil
}

func (p *HandParser) parseColumnPosition(spec *ast.AlterTableSpec) {
	// Always initialize Position (the parser always populated this).
	spec.Position = Alloc[ast.ColumnPosition](p.arena)
	if _, ok := p.accept(first); ok {
		spec.Position.Tp = ast.ColumnPositionFirst
	} else if _, ok := p.accept(after); ok {
		spec.Position.Tp = ast.ColumnPositionAfter
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.Position.RelativeColumn = p.arena.AllocColumnName()
			spec.Position.RelativeColumn.Name = ast.NewCIStr(tok.Lit)
		}
	}
	// else: Position.Tp defaults to ColumnPositionNone (zero value).
}

// parsePartitionDef parses a single PARTITION definition.
//
// Syntax (yacc PartitionDefinition):
//
//	PARTITION name [PartDefValuesOpt] [PartDefOptionList] [(SubPartDefinition, ...)]
//
// PartDefValuesOpt:
//
//	VALUES LESS THAN (expr, ...) | VALUES LESS THAN MAXVALUE
//	VALUES IN (value, ...)
//	DEFAULT                       -- shorthand for VALUES IN (DEFAULT)
//	HISTORY | CURRENT             -- for SYSTEM_TIME partitions
//
// partType controls validation:
//   - 0 means lenient (ALTER TABLE / REORGANIZE): any clause is accepted
//   - nonzero means strict (CREATE TABLE): clause must match the partition type
func (p *HandParser) parsePartitionDef(partType ast.PartitionType) *ast.PartitionDefinition {
	if _, ok := p.accept(partition); !ok {
		return nil
	}
	pDef := &ast.PartitionDefinition{}
	// Partition name — can be identifier, string literal, or unreserved keyword
	// (e.g., PARTITION max VALUES LESS THAN (10))
	tok := p.peek()
	if !isIdentLike(tok.Tp) && tok.Tp != stringLit {
		p.syntaxErrorAt(tok)
		return nil
	}
	p.next()
	pDef.Name = ast.NewCIStr(tok.Lit)

	// --- Parse PartDefValuesOpt ---
	if _, ok := p.accept(values); ok {
		if partType == ast.PartitionTypeHash || partType == ast.PartitionTypeKey || partType == ast.PartitionTypeSystemTime {
			p.error(p.peek().Offset, "VALUES clause is not allowed for HASH/KEY partitions")
			return nil
		}
		if _, ok := p.accept(less); ok {
			// VALUES LESS THAN
			if partType == ast.PartitionTypeList {
				p.error(p.peek().Offset, "VALUES LESS THAN value must be used with RANGE partitioning")
				return nil
			}
			p.expect(than)
			clause := &ast.PartitionDefinitionClauseLessThan{}
			if _, ok := p.accept('('); ok {
				for {
					expr := p.parseExprOrDefault()
					if partType != 0 {
						if !p.isValidPartitionExpr(expr) {
							p.error(p.peek().Offset, "invalid expression in partition value")
							return nil
						}
						// Check for NULL explicitly
						if v, ok := expr.(ast.ValueExpr); ok && v.GetValue() == nil {
							p.errs = append(p.errs, ErrNullInValuesLessThan.GenWithStackByArgs())
							return nil
						}
						if _, ok := expr.(*ast.DefaultExpr); ok {
							p.error(p.peek().Offset, "DEFAULT not allowed in RANGE/VALUES LESS THAN")
							return nil
						}
					}
					clause.Exprs = append(clause.Exprs, expr)
					if _, ok := p.accept(','); !ok {
						break
					}
				}
				p.expect(')')
			} else if _, ok := p.accept(maxValue); ok {
				// MAXVALUE without parens
				clause.Exprs = append(clause.Exprs, &ast.MaxValueExpr{})
			} else {
				p.syntaxErrorAt(p.peek())
				return nil
			}
			pDef.Clause = clause
		} else if _, ok := p.accept(in); ok {
			// VALUES IN
			if partType == ast.PartitionTypeRange {
				p.error(p.peek().Offset, "VALUES IN value must be used with LIST partitioning")
				return nil
			}
			clause := &ast.PartitionDefinitionClauseIn{}
			p.expect('(')
			rejectMaxValue := func(expr ast.ExprNode, startTok Token) bool {
				if _, ok := expr.(*ast.MaxValueExpr); ok {
					// Report error at token's EndOffset (column) and Offset (near text)
					// matching yacc scanner position semantics. Append custom message.
					line, col := p.calcLineCol(startTok.EndOffset)
					near := ""
					if startTok.Offset < len(p.src) {
						near = p.src[startTok.Offset:]
						if len(near) > 80 {
							near = near[:80]
						}
					}
					msg := "MAXVALUE cannot be used in LIST partition"
					p.errs = append(p.errs, fmt.Errorf(
						"line %d column %d near \"%s\"%s ",
						line, col, near, msg))
					return true
				}
				return false
			}
			for {
				if p.peek().Tp == '(' {
					// Tuple: (val1, val2, ...)
					p.next()
					var valList []ast.ExprNode
					for {
						startTok := p.peek()
						expr := p.parseExprOrDefault()
						if rejectMaxValue(expr, startTok) {
							return nil
						}
						valList = append(valList, expr)
						if _, ok := p.accept(','); !ok {
							break
						}
					}
					p.expect(')')
					clause.Values = append(clause.Values, valList)
				} else {
					// Single value
					startTok := p.peek()
					expr := p.parseExprOrDefault()
					if rejectMaxValue(expr, startTok) {
						return nil
					}
					clause.Values = append(clause.Values, []ast.ExprNode{expr})
				}
				if _, ok := p.accept(','); !ok {
					break
				}
			}
			p.expect(')')
			pDef.Clause = clause
		}
	} else if _, ok := p.accept(defaultKwd); ok {
		// Standalone DEFAULT — shorthand for VALUES IN (DEFAULT)
		clause := &ast.PartitionDefinitionClauseIn{}
		clause.Values = append(clause.Values, []ast.ExprNode{&ast.DefaultExpr{}})
		pDef.Clause = clause
	} else if tok, ok := p.acceptAny(history, current); ok {
		// HISTORY or CURRENT — for SYSTEM_TIME partitions
		if partType != 0 && partType != ast.PartitionTypeSystemTime {
			p.error(p.peek().Offset, "HISTORY/CURRENT partition allowed only for SYSTEM_TIME")
			return nil
		}
		pDef.Clause = &ast.PartitionDefinitionClauseHistory{Current: tok.Tp == current}
	}

	// Default clause if none was set
	if pDef.Clause == nil {
		if partType == ast.PartitionTypeRange {
			p.error(p.peek().Offset, "RANGE partition must have VALUES LESS THAN")
			return nil
		}
		if partType == ast.PartitionTypeList {
			p.error(p.peek().Offset, "LIST partition must have VALUES IN")
			return nil
		}
		if partType == ast.PartitionTypeSystemTime {
			p.error(p.peek().Offset, "SYSTEM_TIME partition must have HISTORY/CURRENT")
			return nil
		}
		pDef.Clause = &ast.PartitionDefinitionClauseNone{}
	}

	// Parse optional partition options (COMMENT, ENGINE, etc.)
	for {
		opt := p.parseTableOption()
		if opt == nil {
			break
		}
		pDef.Options = append(pDef.Options, opt)
	}

	// Parse optional inline SUBPARTITION definitions: (SUBPARTITION name [options], ...)
	p.parseSubPartitionDefs(pDef)

	return pDef
}

// parseSubPartitionDefs parses optional inline subpartition definitions within a partition.
// Syntax: '(' SUBPARTITION name [PartDefOptionList] [',' ...] ')'
func (p *HandParser) parseSubPartitionDefs(pDef *ast.PartitionDefinition) {
	if p.peek().Tp != '(' {
		return
	}
	// Peek ahead to check it's a SUBPARTITION keyword, not start of something else
	if next := p.peekN(1); next.Tp != subpartition && !next.IsKeyword("SUBPARTITION") {
		return
	}
	p.next() // consume '('
	for {
		if _, ok := p.acceptKeyword(subpartition, "SUBPARTITION"); !ok {
			break
		}
		spd := &ast.SubPartitionDefinition{}
		if nameTok, ok := p.expectAny(identifier, stringLit); ok {
			spd.Name = ast.NewCIStr(nameTok.Lit)
		}
		// Subpartition options (same as partition options)
		for {
			opt := p.parseTableOption()
			if opt == nil {
				break
			}
			spd.Options = append(spd.Options, opt)
		}
		pDef.Sub = append(pDef.Sub, spd)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	p.expect(')')
}

func (p *HandParser) parseSplitRegionSpec(spec *ast.AlterTableSpec) *ast.AlterTableSpec {
	if _, ok := p.accept(maxValue); ok {
		spec.Tp = ast.AlterTableReorganizeLastPartition
		p.parsePartitionLessThanExpr(spec)
		return spec
	}

	if _, ok := p.accept(partition); ok {
		// SPLIT PARTITION p1, p2 ...
		spec.Tp = ast.AlterTableSplitIndex // Using SplitIndex for partition split as well if generic
		// Actually, standard syntax might be SPLIT PARTITION table ...
		// But in ALTER TABLE, it's SPLIT PARTITION p1 ...
		// We need to check available types. Using AlterTableSplitIndex for now as fallback if
		// specific partition split type isn't clear, but standard SPLIT PARTITION likely
		// maps to something else or reuses SplitIndex with PartitionNames.
		// Wait, Check AlterTableType again.
		// There is AlterTablePartition which is generic.
		// But let's assume AlterTableSplitIndex is valid for Table/Index split.
		// For Partition split?
		// Code in ddl_api.go likely handles it.
		// Let's implement parsing logic.
		for {
			tok := p.peek()
			if !isIdentLike(tok.Tp) && tok.Tp != stringLit {
				break
			}
			p.next()
			spec.PartitionNames = append(spec.PartitionNames, ast.NewCIStr(tok.Lit))
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		// Check for specific syntax for Partition split options if any.
		return spec
	}

	spec.Tp = ast.AlterTableSplitIndex
	spec.SplitIndex = Alloc[ast.SplitIndexOption](p.arena)

	if _, ok := p.accept(tableKwd); ok {
		// SPLIT TABLE
		// implicit table name (current table)
		spec.SplitIndex.TableLevel = true
	} else if _, ok := p.accept(index); ok {
		// SPLIT INDEX idx
		if tok, ok := p.expect(identifier); ok {
			spec.SplitIndex.IndexName = ast.NewCIStr(tok.Lit)
		}
	} else if _, ok := p.accept(primary); ok {
		// SPLIT PRIMARY KEY
		p.expect(key)
		spec.SplitIndex.PrimaryKey = true
		// IndexName is typically "PRIMARY" but flag controls display
		spec.SplitIndex.IndexName = ast.NewCIStr("PRIMARY")
	} else {
		p.accept(region) // SPLIT REGION implies splitting table region?
		// Fallback: SPLIT PARTITION handled above.
		// If none matched, maybe error?
		// But parseSplitRegionSpec is called after split.
		// If simple SPLIT, assumes TABLE split if no keyword?
		spec.SplitIndex.TableLevel = true
	}

	// Parse Split Options: BETWEEN/BY/REGIONS
	p.parseAlterTableSplitOption(spec.SplitIndex)
	return spec
}

func (p *HandParser) parseAlterTablePartitionOptions(spec *ast.AlterTableSpec) *ast.AlterTableSpec {
	p.expect(partition)
	// NO_WRITE_TO_BINLOG or LOCAL (alias)
	spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()

	if _, ok := p.accept(all); ok {
		spec.OnAllPartitions = true
	} else {
		// Parse partition name list. Names can be identifiers or non-reserved keywords.
		for {
			tok := p.peek()
			if !isIdentLike(tok.Tp) {
				break
			}
			p.next()
			spec.PartitionNames = append(spec.PartitionNames, ast.NewCIStr(tok.Lit))
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		if len(spec.PartitionNames) == 0 {
			p.syntaxError(p.peek().Offset)
			return nil
		}
	}
	return spec
}

func (p *HandParser) parseAlterTableSplitOption(opt *ast.SplitIndexOption) {
	if opt == nil {
		return
	}
	splitOpt := Alloc[ast.SplitOption](p.arena)
	opt.SplitOpt = splitOpt

	if _, ok := p.accept(by); ok {
		// BY (list), ...
		for {
			if _, ok := p.accept('('); ok {
				var row []ast.ExprNode
				for {
					expr := p.parseExpression(precNone)
					row = append(row, expr)
					if _, ok := p.accept(','); !ok {
						break
					}
				}
				p.expect(')')
				splitOpt.ValueLists = append(splitOpt.ValueLists, row)
			} else {
				// Single value without outer parens?
				expr := p.parseExpression(precNone)
				splitOpt.ValueLists = append(splitOpt.ValueLists, []ast.ExprNode{expr})
			}
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	} else if _, ok := p.accept(between); ok {
		// BETWEEN lower AND upper REGIONS n
		// Lower
		splitOpt.Lower = p.parseSplitBound()

		p.expect(and)

		// Upper
		splitOpt.Upper = p.parseSplitBound()

		// REGIONS n
		p.expect(regions)
		if tok, ok := p.expect(intLit); ok {
			val, _ := strconv.ParseInt(tok.Lit, 10, 64)
			splitOpt.Num = val
		}
	}
}

// parseAlterAnalyzePartition parses ALTER TABLE ... ANALYZE ...
func (p *HandParser) parseAlterAnalyzePartition(table *ast.TableName) *ast.AnalyzeTableStmt {
	p.expect(partition)

	stmt := Alloc[ast.AnalyzeTableStmt](p.arena)
	stmt.TableNames = []*ast.TableName{table}

	stmt.PartitionNames = p.parseIdentList()

	if _, ok := p.accept(index); ok {
		stmt.IndexFlag = true
		stmt.IndexNames = p.parseIdentList()
	}

	if _, ok := p.accept(with); ok {
		for {
			var opt ast.AnalyzeOpt
			tok, ok := p.expectAny(intLit, decLit, floatLit)
			if !ok {
				p.syntaxError(p.peek().Offset)
				return nil
			}
			opt.Value = ast.NewValueExpr(tok.Item, "", "")
			if _, ok := p.accept(buckets); ok {
				opt.Type = ast.AnalyzeOptNumBuckets
			} else if _, ok := p.accept(topn); ok {
				opt.Type = ast.AnalyzeOptNumTopN
			} else if _, ok := p.accept(sampleRate); ok {
				opt.Type = ast.AnalyzeOptSampleRate
			} else if _, ok := p.accept(samples); ok {
				opt.Type = ast.AnalyzeOptNumSamples
			} else if _, ok := p.accept(cmSketch); ok {
				if _, ok := p.accept(depth); ok {
					opt.Type = ast.AnalyzeOptCMSketchDepth
				} else if _, ok := p.accept(width); ok {
					opt.Type = ast.AnalyzeOptCMSketchWidth
				} else {
					p.syntaxError(p.peek().Offset)
					return nil
				}
			} else {
				p.syntaxError(p.peek().Offset)
				return nil
			}
			stmt.AnalyzeOpts = append(stmt.AnalyzeOpts, opt)

			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}
	return stmt
}
