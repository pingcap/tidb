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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

// Sub-handlers extracted from parseAlterTableSpec for maintainability.
// Each method corresponds to a major ALTER TABLE clause and populates
// the spec fields. Methods that require early returns pass back a
// non-nil *AlterTableSpec or nil to indicate "continue to default return".

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// parseAlterAdd handles ALTER TABLE ... ADD {COLUMN|PARTITION|CONSTRAINT|STATS_EXTENDED|(col_defs)}.
func (p *HandParser) parseAlterAdd(spec *ast.AlterTableSpec) {
	// ADD [COLUMN] | ADD {INDEX|KEY|CONSTRAINT|...}
	if _, ok := p.accept(column); ok {
		ifNotExists := p.acceptIfNotExists()

		if p.peek().Tp == '(' {
			p.next()
			spec.Tp = ast.AlterTableAddColumns
			spec.IfNotExists = ifNotExists
			for {
				tp := p.peek().Tp
				if isConstraintToken(tp) {
					spec.NewConstraints = append(spec.NewConstraints, p.parseConstraint())
				} else {
					spec.NewColumns = append(spec.NewColumns, p.parseColumnDef())
				}
				if _, ok := p.accept(','); !ok {
					break
				}
			}
			p.expect(')')
		} else {
			// ADD COLUMN [IF NOT EXISTS] col_def
			spec.Tp = ast.AlterTableAddColumns
			spec.IfNotExists = ifNotExists
			spec.NewColumns = []*ast.ColumnDef{p.parseColumnDef()}
			p.parseColumnPosition(spec)
		}
	} else if _, ok := p.accept(partition); ok {
		spec.Tp = ast.AlterTableAddPartitions
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		// ADD PARTITION [IF NOT EXISTS] [( PARTITION defs )]
		spec.IfNotExists = p.acceptIfNotExists()
		if _, ok := p.accept(partitions); ok {
			spec.Num = p.parseUint64()
		} else if _, ok := p.accept('('); ok {
			for {
				pDef := p.parsePartitionDef(0)
				if pDef == nil {
					break
				}
				spec.PartDefinitions = append(spec.PartDefinitions, pDef)
				if _, ok := p.accept(','); !ok {
					break
				}
			}
			p.expect(')')
		}
	} else if isConstraintToken(p.peek().Tp) {
		spec.Tp = ast.AlterTableAddConstraint
		spec.Constraint = p.parseConstraint()
	} else if _, ok := p.accept(statsExtended); ok {
		spec.Tp = ast.AlterTableAddStatistics
		spec.IfNotExists = p.acceptIfNotExists()
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.Statistics = &ast.StatisticsSpec{StatsName: tok.Lit}
			// Parse stats type: CARDINALITY | CORRELATION | DEPENDENCY
			if _, ok := p.accept(cardinality); ok {
				spec.Statistics.StatsType = ast.StatsTypeCardinality
			} else if _, ok := p.accept(correlation); ok {
				spec.Statistics.StatsType = ast.StatsTypeCorrelation
			} else if _, ok := p.accept(dependency); ok {
				spec.Statistics.StatsType = ast.StatsTypeDependency
			}
			// Parse column list: (col1, col2, ...)
			if _, ok := p.accept('('); ok {
				for {
					if tok, ok := p.expectAny(identifier, stringLit); ok {
						col := p.arena.AllocColumnName()
						col.Name = ast.NewCIStr(tok.Lit)
						spec.Statistics.Columns = append(spec.Statistics.Columns, col)
					}
					if _, ok := p.accept(','); !ok {
						break
					}
				}
				p.expect(')')
			}
		}
	} else if p.peek().Tp == '(' {
		p.next()
		spec.Tp = ast.AlterTableAddColumns
		for {
			tp := p.peek().Tp
			if isConstraintToken(tp) {
				spec.NewConstraints = append(spec.NewConstraints, p.parseConstraint())
			} else {
				spec.NewColumns = append(spec.NewColumns, p.parseColumnDef())
			}
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	} else {
		// Assume ADD COLUMN by default if none of the above
		spec.Tp = ast.AlterTableAddColumns
		spec.NewColumns = []*ast.ColumnDef{p.parseColumnDef()}
		p.parseColumnPosition(spec)
	}
}

// parseAlterDrop handles ALTER TABLE ...
// DROP {COLUMN|INDEX|KEY|PRIMARY KEY|FOREIGN KEY|CHECK|CONSTRAINT|PARTITION|STATS_EXTENDED|FIRST PARTITION}.
func (p *HandParser) parseAlterDrop(spec *ast.AlterTableSpec) {
	parseDropColumn := func() {
		spec.Tp = ast.AlterTableDropColumn
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.OldColumnName = p.arena.AllocColumnName()
			spec.OldColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.acceptAny(restrict, cascade)
	}

	if _, ok := p.accept(column); ok {
		parseDropColumn()
	} else if _, ok := p.accept(primary); ok {
		p.expect(key)
		spec.Tp = ast.AlterTableDropPrimaryKey
	} else if _, ok := p.acceptAny(index, key); ok {
		spec.Tp = ast.AlterTableDropIndex
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.Name = tok.Lit
		}
	} else if _, ok := p.accept(foreign); ok {
		p.expect(key)
		spec.Tp = ast.AlterTableDropForeignKey
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.Name = tok.Lit
		}
	} else if _, ok := p.acceptAny(check, constraint); ok {
		spec.Tp = ast.AlterTableDropCheck
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			c := p.arena.AllocConstraint()
			c.Name = tok.Lit
			spec.Constraint = c
		}
	} else if _, ok := p.accept(statsExtended); ok {
		spec.Tp = ast.AlterTableDropStatistics
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.Statistics = &ast.StatisticsSpec{StatsName: tok.Lit}
		}
	} else if _, ok := p.acceptKeyword(first, "FIRST"); ok {
		spec.Tp = ast.AlterTableDropFirstPartition
		p.parsePartitionLessThanExpr(spec)
		if _, ok := p.acceptKeyword(first, "FIRST"); ok {
			spec.Tp = ast.AlterTableReorganizeFirstPartition
			p.parsePartitionLessThanExpr(spec)
		}
	} else if _, ok := p.accept(partition); ok {
		spec.Tp = ast.AlterTableDropPartition
		spec.IfExists = p.acceptIfExists()
		spec.PartitionNames = p.parseIdentList()
	} else {
		parseDropColumn()
	}
}

// parseAlterRename handles ALTER TABLE ... RENAME {TO|AS|COLUMN|INDEX|KEY|new_tbl}.
func (p *HandParser) parseAlterRename(spec *ast.AlterTableSpec) {
	if _, ok := p.acceptAny(to, as); ok {
		spec.Tp = ast.AlterTableRenameTable
		spec.NewTable = p.parseTableName()
	} else if _, ok := p.accept(column); ok {
		spec.Tp = ast.AlterTableRenameColumn
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.OldColumnName = p.arena.AllocColumnName()
			spec.OldColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.expect(to)
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.NewColumnName = p.arena.AllocColumnName()
			spec.NewColumnName.Name = ast.NewCIStr(tok.Lit)
		}
	} else if _, ok := p.acceptAny(index, key); ok {
		spec.Tp = ast.AlterTableRenameIndex
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.FromKey = ast.NewCIStr(tok.Lit)
		}
		p.expect(to)
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.ToKey = ast.NewCIStr(tok.Lit)
		}
	} else {
		// RENAME [TO|=] new_tbl
		spec.Tp = ast.AlterTableRenameTable
		if _, ok := p.accept(to); !ok {
			p.accept(eq)
		}
		spec.NewTable = p.parseTableName()
	}
}

// parseAlterAlter handles ALTER TABLE ... ALTER {INDEX|CHECK|CONSTRAINT|[COLUMN] col_name}.
// Returns non-nil spec for branches that need early return from parseAlterTableSpec,
// or nil if the caller should fall through to the default return.
func (p *HandParser) parseAlterAlter(spec *ast.AlterTableSpec) *ast.AlterTableSpec {
	// ALTER INDEX index_name {VISIBLE|INVISIBLE}
	if _, ok := p.accept(index); ok {
		spec.Tp = ast.AlterTableIndexInvisible
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.IndexName = ast.NewCIStr(tok.Lit)
		}
		if _, ok := p.accept(visible); ok {
			spec.Visibility = ast.IndexVisibilityVisible
		} else if _, ok := p.accept(invisible); ok {
			spec.Visibility = ast.IndexVisibilityInvisible
		} else {
			p.syntaxError(p.peek().Offset)
			return nil
		}
		return spec
	}

	// ALTER CHECK|CONSTRAINT name {ENFORCED | NOT ENFORCED}
	if _, ok := p.acceptAny(check, constraint); ok {
		spec.Tp = ast.AlterTableAlterCheck
		spec.Constraint = p.arena.AllocConstraint()
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			spec.Constraint.Name = tok.Lit
		}
		if _, ok := p.accept(enforced); ok {
			spec.Constraint.Enforced = true
		} else if _, ok := p.accept(not); ok {
			p.expect(enforced)
			spec.Constraint.Enforced = false
		} else {
			p.syntaxError(p.peek().Offset)
			return nil
		}
		return spec
	}

	// ALTER [COLUMN] col_name {SET DEFAULT expr | DROP DEFAULT}
	p.accept(column)
	spec.OldColumnName = p.parseColumnName()
	if _, ok := p.accept(set); ok {
		p.expect(defaultKwd)
		spec.Tp = ast.AlterTableAlterColumn
		colOpt := Alloc[ast.ColumnOption](p.arena)
		if _, ok := p.accept('('); ok {
			colOpt.Expr = p.parseExpression(precNone)
			p.expect(')')
		} else {
			expr := p.parseExpression(precNone)
			// Enforce constraints: SignedLiteral
			isValid := false
			if _, ok := expr.(ast.ValueExpr); ok {
				isValid = true
			} else if u, ok := expr.(*ast.UnaryOperationExpr); ok {
				if _, ok := u.V.(ast.ValueExpr); ok {
					isValid = true
				}
			}
			if !isValid {
				p.error(expr.OriginTextPosition(), "Invalid default value for ALTER COLUMN")
				return nil
			}
			colOpt.Expr = expr
		}
		spec.NewColumns = []*ast.ColumnDef{{
			Name:    spec.OldColumnName,
			Options: []*ast.ColumnOption{colOpt},
		}}
	} else if _, ok := p.accept(drop); ok {
		p.expect(defaultKwd)
		spec.Tp = ast.AlterTableAlterColumn
		// No default value options = drop default
		spec.NewColumns = []*ast.ColumnDef{{
			Name: spec.OldColumnName,
		}}
	}

	if spec.Tp == ast.AlterTableAlterColumn {
		return spec
	}
	return nil
}

// parseAlterPartition handles ALTER TABLE ... PARTITION {BY|name [ATTRIBUTES|options]}.
// Returns non-nil spec for branches that need early return from parseAlterTableSpec,
// or nil if the caller should fall through to the default return.
func (p *HandParser) parseAlterPartition(spec *ast.AlterTableSpec) *ast.AlterTableSpec {
	// PARTITION BY ... — re-partitioning (look ahead for BY without consuming PARTITION)
	if p.peekN(1).Tp == by {
		spec.Tp = ast.AlterTablePartition
		spec.Partition = p.parsePartitionOptions()
		return spec
	}
	// PARTITION p0 PLACEMENT POLICY = p1
	p.next()
	if tok, ok := p.expectAny(identifier, stringLit); ok {
		spec.PartitionNames = append(spec.PartitionNames, ast.NewCIStr(tok.Lit))
	}
	// PARTITION p ATTRIBUTES [=] DEFAULT|'str'
	if p.peek().Tp == attributes {
		p.next()
		p.accept(eq)
		spec.Tp = ast.AlterTablePartitionAttributes
		attrSpec := &ast.AttributesSpec{}
		if _, ok := p.accept(defaultKwd); ok {
			attrSpec.Default = true
		} else if tok, ok := p.expect(stringLit); ok {
			attrSpec.Attributes = tok.Lit
		}
		spec.AttributesSpec = attrSpec
		return spec
	}
	spec.Tp = ast.AlterTablePartitionOptions
	// Parse partition options (PLACEMENT POLICY, etc.)
	for {
		opt := p.parseTableOption()
		if opt == nil {
			break
		}
		spec.Options = append(spec.Options, opt)
	}
	return spec
}

// parseAlterReorganize handles ALTER TABLE ... REORGANIZE PARTITION.
func (p *HandParser) parseAlterReorganize(spec *ast.AlterTableSpec) *ast.AlterTableSpec {
	if _, ok := p.accept(partition); ok {
		// NoWriteToBinLogAliasOpt
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		spec.Tp = ast.AlterTableReorganizePartition
		// Check for empty form: REORGANIZE PARTITION [NoWriteToBinLog]
		if p.peek().Tp == ',' || p.peek().Tp == ';' || p.peek().Tp == 0 {
			spec.OnAllPartitions = true
			return spec
		}
		// PartitionNameList INTO ( PartitionDefinitionList )
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
			spec.OnAllPartitions = true
			return spec
		}
		p.expect(into)
		p.expect('(')
		for {
			pDef := p.parsePartitionDef(0)
			if pDef == nil {
				break
			}
			spec.PartDefinitions = append(spec.PartDefinitions, pDef)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}
	return spec
}

// parseAlterPartitionAction handles ALTER TABLE
// ... {COALESCE|TRUNCATE|EXCHANGE|REBUILD|OPTIMIZE|REPAIR|
//
//	CHECK|IMPORT|DISCARD|FIRST|LAST|MERGE|REORGANIZE} PARTITION ...
//
// Returns true if a partition action was parsed, false otherwise.
func (p *HandParser) parseAlterPartitionAction(spec *ast.AlterTableSpec) bool {
	tok := p.peek()
	switch tok.Tp {
	case coalesce:
		// COALESCE PARTITION n
		p.next()
		p.expect(partition)
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		spec.Tp = ast.AlterTableCoalescePartitions
		spec.Num = p.parseUint64()
		return true

	case truncate:
		// TRUNCATE PARTITION {name|ALL}
		p.next()
		p.expect(partition)
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		spec.Tp = ast.AlterTableTruncatePartition
		if _, ok := p.accept(all); ok {
			spec.OnAllPartitions = true
		} else {
			spec.PartitionNames = p.parseIdentList()
		}
		return true

	case exchange:
		p.next() // Consume EXCHANGE
		p.expect(partition)
		spec.Tp = ast.AlterTableExchangePartition

		tok := p.peek()
		if !isIdentLike(tok.Tp) {
			p.syntaxError(tok.Offset)
			return true // Syntactic error handled, return true to stop switch
		}
		p.next()
		spec.PartitionNames = append(spec.PartitionNames, ast.NewCIStr(tok.Lit))

		p.expect(with)
		p.expect(tableKwd)
		spec.NewTable = p.parseTableName()

		spec.WithValidation = true
		if _, ok := p.accept(without); ok {
			p.expect(validation)
			spec.WithValidation = false
		} else if _, ok := p.accept(with); ok {
			p.expect(validation)
			spec.WithValidation = true
		}
		return true

	case rebuild, optimize, repair, check:
		p.next()
		switch tok.Tp {
		case rebuild:
			spec.Tp = ast.AlterTableRebuildPartition
		case optimize:
			spec.Tp = ast.AlterTableOptimizePartition
		case repair:
			spec.Tp = ast.AlterTableRepairPartition
		case check:
			spec.Tp = ast.AlterTableCheckPartitions
		}
		p.parseAlterTablePartitionOptions(spec)
		return true

	case importKwd, discard:
		isImport := p.next().Tp == importKwd
		if p.peek().Tp == tablespace {
			p.next()
			if isImport {
				spec.Tp = ast.AlterTableImportTablespace
			} else {
				spec.Tp = ast.AlterTableDiscardTablespace
			}
			return true
		}
		if isImport {
			spec.Tp = ast.AlterTableImportPartitionTablespace
		} else {
			spec.Tp = ast.AlterTableDiscardPartitionTablespace
		}
		p.parseAlterTablePartitionOptions(spec)
		p.expect(tablespace)
		return true

	case first, last:
		if p.next().Tp == first {
			spec.Tp = ast.AlterTableDropFirstPartition
		} else {
			spec.Tp = ast.AlterTableAddLastPartition
		}
		p.parsePartitionLessThanExpr(spec)
		return true

	case merge:
		p.next() // Consume MERGE
		p.parseMergeFirstPartition(spec)
		return true

	case reorganize:
		p.next()
		p.parseAlterReorganize(spec)
		return true

	case remove:
		p.next()
		if _, ok := p.accept(ttl); ok {
			spec.Tp = ast.AlterTableRemoveTTL
		} else {
			p.expect(partitioning)
			spec.Tp = ast.AlterTableRemovePartitioning
		}
		return true

	case identifier:
		// Check for MERGE/SPLIT keywords if identifier
		if p.peek().IsKeyword("MERGE") {
			p.next() // Consume MERGE
			p.parseMergeFirstPartition(spec)
			return true
		}
		return false
	}
	return false
}

// parseMergeFirstPartition handles: MERGE FIRST PARTITION LESS THAN (expr)
// Shared between the merge case and the identifier "MERGE" fallback.
func (p *HandParser) parseMergeFirstPartition(spec *ast.AlterTableSpec) {
	if _, ok := p.accept(first); ok {
		spec.Tp = ast.AlterTableReorganizeFirstPartition
		p.parsePartitionLessThanExpr(spec)
	}
}

// parseAlterTableOptions handles ALTER TABLE
// ... {FORCE|ALGORITHM|LOCK|READ|CONVERT|WITH|WITHOUT|STATS_OPTIONS|
//
//	ATTRIBUTES|CACHE|NOCACHE|SECONDARY_LOAD|SECONDARY_UNLOAD|ENABLE|DISABLE} ...
//
// and generic table options. Returns true if options were parsed, false otherwise.
func (p *HandParser) parseAlterTableOptions(spec *ast.AlterTableSpec) bool {
	tok := p.peek()
	switch tok.Tp {
	case force:
		p.next()
		if p.peek().Tp == autoIncrement || p.peek().Tp == autoRandomBase {
			opt := p.arena.AllocTableOption()
			if !p.parseForceAutoOption(opt) {
				p.syntaxError(p.peek().Offset)
				return true // Handled but error
			}
			spec.Tp = ast.AlterTableOption
			spec.Options = []*ast.TableOption{opt}
			return true
		}
		spec.Tp = ast.AlterTableForce
		return true

	case algorithm:
		// ALGORITHM = {DEFAULT|COPY|INPLACE|INSTANT}
		p.next()
		p.accept(eq)
		spec.Tp = ast.AlterTableAlgorithm
		tok := p.next()
		switch tok.Tp {
		case defaultKwd:
			spec.Algorithm = ast.AlgorithmTypeDefault
		default:
			switch strings.ToUpper(tok.Lit) {
			case "COPY":
				spec.Algorithm = ast.AlgorithmTypeCopy
			case "INPLACE":
				spec.Algorithm = ast.AlgorithmTypeInplace
			case "INSTANT":
				spec.Algorithm = ast.AlgorithmTypeInstant
			default:
				spec.Algorithm = ast.AlgorithmTypeDefault
			}
		}
		return true

	case lock:
		// LOCK [=] {DEFAULT | NONE | SHARED | EXCLUSIVE}
		p.next()
		p.accept(eq)
		spec.Tp = ast.AlterTableLock
		if _, ok := p.accept(defaultKwd); ok {
			spec.LockType = ast.LockTypeDefault
		} else if _, ok := p.accept(none); ok {
			spec.LockType = ast.LockTypeNone
		} else if _, ok := p.accept(shared); ok {
			spec.LockType = ast.LockTypeShared
		} else if _, ok := p.accept(exclusive); ok {
			spec.LockType = ast.LockTypeExclusive
		} else {
			// Unknown lock type
			tok := p.next()
			p.errs = append(p.errs, terror.ClassParser.NewStd(mysql.ErrUnknownAlterLock).GenWithStackByArgs(tok.Lit))
			return true
		}
		return true

	case read:
		// READ {ONLY | WRITE}
		p.next()
		spec.Tp = ast.AlterTableWriteable
		if _, ok := p.accept(only); ok {
			spec.Writeable = false
		} else {
			p.expect(write)
			spec.Writeable = true
		}
		return true

	case convert:
		// CONVERT TO CHARACTER SET charset [COLLATE collation]
		p.next()
		p.expect(to)
		spec.Tp = ast.AlterTableOption
		opt := p.arena.AllocTableOption()
		opt.Tp = ast.TableOptionCharset
		opt.UintValue = ast.TableOptionCharsetWithConvertTo
		// CharsetKw: CHARACTER SET | CHARSET | CHAR SET
		p.acceptCharsetKw()
		if _, ok := p.accept(defaultKwd); ok {
			opt.Default = true
		} else if tok, ok := p.acceptStringName(); ok {
			opt.StrValue = tok.Lit
		}
		spec.Options = []*ast.TableOption{opt}
		if _, ok := p.accept(collate); ok {
			collOpt := p.arena.AllocTableOption()
			collOpt.Tp = ast.TableOptionCollate
			if tok, ok := p.acceptStringName(); ok {
				collOpt.StrValue = strings.ToLower(tok.Lit)
			}
			spec.Options = append(spec.Options, collOpt)
		}
		return true

	case with, without:
		if p.next().Tp == with {
			spec.Tp = ast.AlterTableWithValidation
		} else {
			spec.Tp = ast.AlterTableWithoutValidation
		}
		p.expect(validation)
		return true

	case statsOptions:
		p.next()
		p.accept(eq)
		spec.Tp = ast.AlterTableStatsOptions
		statsSpec := &ast.StatsOptionsSpec{}
		if _, ok := p.accept(defaultKwd); ok {
			statsSpec.Default = true
		} else if tok, ok := p.expect(stringLit); ok {
			statsSpec.StatsOptions = tok.Lit
		}
		spec.StatsOptionsSpec = statsSpec
		return true

	case attributes:
		p.next()
		p.accept(eq)
		spec.Tp = ast.AlterTableAttributes
		attrSpec := &ast.AttributesSpec{}
		if _, ok := p.accept(defaultKwd); ok {
			attrSpec.Default = true
		} else if tok, ok := p.expect(stringLit); ok {
			attrSpec.Attributes = tok.Lit
		}
		spec.AttributesSpec = attrSpec
		return true

	case cache, nocache, secondaryLoad, secondaryUnload:
		p.next()
		switch tok.Tp {
		case cache:
			spec.Tp = ast.AlterTableCache
		case nocache:
			spec.Tp = ast.AlterTableNoCache
		case secondaryLoad:
			spec.Tp = ast.AlterTableSecondaryLoad
		case secondaryUnload:
			spec.Tp = ast.AlterTableSecondaryUnload
		}
		return true

	case enable, disable:
		if p.next().Tp == enable {
			spec.Tp = ast.AlterTableEnableKeys
		} else {
			spec.Tp = ast.AlterTableDisableKeys
		}
		p.expect(keys)
		return true
	}

	// Try to parse generic ALTER TABLE options (ROW_FORMAT, KEY_BLOCK_SIZE, etc.)
	opt := p.parseTableOption()
	if opt != nil {
		spec.Tp = ast.AlterTableOption
		spec.Options = []*ast.TableOption{opt}
		// Parse additional options without commas
		for {
			opt := p.parseTableOption()
			if opt == nil {
				break
			}
			spec.Options = append(spec.Options, opt)
		}
		return true
	}

	return false
}
