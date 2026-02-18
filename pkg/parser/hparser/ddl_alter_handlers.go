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

package hparser

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
	if _, ok := p.accept(tokColumn); ok {
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
	} else if _, ok := p.accept(tokPartition); ok {
		spec.Tp = ast.AlterTableAddPartitions
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		// ADD PARTITION [IF NOT EXISTS] [( PARTITION defs )]
		spec.IfNotExists = p.acceptIfNotExists()
		if _, ok := p.accept(tokPartitions); ok {
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
	} else if _, ok := p.accept(tokStatsExtended); ok {
		spec.Tp = ast.AlterTableAddStatistics
		spec.IfNotExists = p.acceptIfNotExists()
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.Statistics = &ast.StatisticsSpec{StatsName: tok.Lit}
			// Parse stats type: CARDINALITY | CORRELATION | DEPENDENCY
			if _, ok := p.accept(tokCardinality); ok {
				spec.Statistics.StatsType = ast.StatsTypeCardinality
			} else if _, ok := p.accept(tokCorrelation); ok {
				spec.Statistics.StatsType = ast.StatsTypeCorrelation
			} else if _, ok := p.accept(tokDependency); ok {
				spec.Statistics.StatsType = ast.StatsTypeDependency
			}
			// Parse column list: (col1, col2, ...)
			if _, ok := p.accept('('); ok {
				for {
					if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
						col := Alloc[ast.ColumnName](p.arena)
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

// parseAlterDrop handles ALTER TABLE ... DROP {COLUMN|INDEX|KEY|PRIMARY KEY|FOREIGN KEY|CHECK|CONSTRAINT|PARTITION|STATS_EXTENDED|FIRST PARTITION}.
func (p *HandParser) parseAlterDrop(spec *ast.AlterTableSpec) {
	parseDropColumn := func() {
		spec.Tp = ast.AlterTableDropColumn
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.OldColumnName = Alloc[ast.ColumnName](p.arena)
			spec.OldColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.acceptAny(tokRestrict, tokCascade)
	}

	if _, ok := p.accept(tokColumn); ok {
		parseDropColumn()
	} else if _, ok := p.accept(tokPrimary); ok {
		p.expect(tokKey)
		spec.Tp = ast.AlterTableDropPrimaryKey
	} else if _, ok := p.acceptAny(tokIndex, tokKey); ok {
		spec.Tp = ast.AlterTableDropIndex
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.Name = tok.Lit
		}
	} else if _, ok := p.accept(tokForeign); ok {
		p.expect(tokKey)
		spec.Tp = ast.AlterTableDropForeignKey
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.Name = tok.Lit
		}
	} else if _, ok := p.acceptAny(tokCheck, tokConstraint); ok {
		spec.Tp = ast.AlterTableDropCheck
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			c := Alloc[ast.Constraint](p.arena)
			c.Name = tok.Lit
			spec.Constraint = c
		}
	} else if _, ok := p.accept(tokStatsExtended); ok {
		spec.Tp = ast.AlterTableDropStatistics
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.Statistics = &ast.StatisticsSpec{StatsName: tok.Lit}
		}
	} else if _, ok := p.acceptKeyword(tokFirst, "FIRST"); ok {
		spec.Tp = ast.AlterTableDropFirstPartition
		p.parsePartitionLessThanExpr(spec)
		if _, ok := p.acceptKeyword(tokFirst, "FIRST"); ok {
			spec.Tp = ast.AlterTableReorganizeFirstPartition
			p.parsePartitionLessThanExpr(spec)
		}
	} else if _, ok := p.accept(tokPartition); ok {
		spec.Tp = ast.AlterTableDropPartition
		spec.IfExists = p.acceptIfExists()
		spec.PartitionNames = p.parseIdentList()
	} else {
		parseDropColumn()
	}
}

// parseAlterRename handles ALTER TABLE ... RENAME {TO|AS|COLUMN|INDEX|KEY|new_tbl}.
func (p *HandParser) parseAlterRename(spec *ast.AlterTableSpec) {
	if _, ok := p.acceptAny(tokTo, tokAs); ok {
		spec.Tp = ast.AlterTableRenameTable
		spec.NewTable = p.parseTableName()
	} else if _, ok := p.accept(tokColumn); ok {
		spec.Tp = ast.AlterTableRenameColumn
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.OldColumnName = Alloc[ast.ColumnName](p.arena)
			spec.OldColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.expect(tokTo)
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.NewColumnName = Alloc[ast.ColumnName](p.arena)
			spec.NewColumnName.Name = ast.NewCIStr(tok.Lit)
		}
	} else if _, ok := p.acceptAny(tokIndex, tokKey); ok {
		spec.Tp = ast.AlterTableRenameIndex
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.FromKey = ast.NewCIStr(tok.Lit)
		}
		p.expect(tokTo)
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.ToKey = ast.NewCIStr(tok.Lit)
		}
	} else {
		// RENAME [TO|=] new_tbl
		spec.Tp = ast.AlterTableRenameTable
		if _, ok := p.accept(tokTo); !ok {
			p.accept(tokEq)
		}
		spec.NewTable = p.parseTableName()
	}
}

// parseAlterAlter handles ALTER TABLE ... ALTER {INDEX|CHECK|CONSTRAINT|[COLUMN] col_name}.
// Returns non-nil spec for branches that need early return from parseAlterTableSpec,
// or nil if the caller should fall through to the default return.
func (p *HandParser) parseAlterAlter(spec *ast.AlterTableSpec) *ast.AlterTableSpec {
	// ALTER INDEX index_name {VISIBLE|INVISIBLE}
	if _, ok := p.accept(tokIndex); ok {
		spec.Tp = ast.AlterTableIndexInvisible
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.IndexName = ast.NewCIStr(tok.Lit)
		}
		if _, ok := p.accept(tokVisible); ok {
			spec.Visibility = ast.IndexVisibilityVisible
		} else if _, ok := p.accept(tokInvisible); ok {
			spec.Visibility = ast.IndexVisibilityInvisible
		} else {
			p.syntaxError(p.peek().Offset)
			return nil
		}
		return spec
	}

	// ALTER CHECK|CONSTRAINT name {ENFORCED | NOT ENFORCED}
	if _, ok := p.acceptAny(tokCheck, tokConstraint); ok {
		spec.Tp = ast.AlterTableAlterCheck
		spec.Constraint = Alloc[ast.Constraint](p.arena)
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			spec.Constraint.Name = tok.Lit
		}
		if _, ok := p.accept(tokEnforced); ok {
			spec.Constraint.Enforced = true
		} else if _, ok := p.accept(tokNot); ok {
			p.expect(tokEnforced)
			spec.Constraint.Enforced = false
		} else {
			p.syntaxError(p.peek().Offset)
			return nil
		}
		return spec
	}

	// ALTER [COLUMN] col_name {SET DEFAULT expr | DROP DEFAULT}
	p.accept(tokColumn)
	spec.OldColumnName = p.parseColumnName()
	if _, ok := p.accept(tokSet); ok {
		p.expect(tokDefault)
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
	} else if _, ok := p.accept(tokDrop); ok {
		p.expect(tokDefault)
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
	if p.peekN(1).Tp == tokBy {
		spec.Tp = ast.AlterTablePartition
		spec.Partition = p.parsePartitionOptions()
		return spec
	}
	// PARTITION p0 PLACEMENT POLICY = p1
	p.next()
	if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
		spec.PartitionNames = append(spec.PartitionNames, ast.NewCIStr(tok.Lit))
	}
	// PARTITION p ATTRIBUTES [=] DEFAULT|'str'
	if p.peek().Tp == tokAttributes {
		p.next()
		p.accept(tokEq)
		spec.Tp = ast.AlterTablePartitionAttributes
		attrSpec := &ast.AttributesSpec{}
		if _, ok := p.accept(tokDefault); ok {
			attrSpec.Default = true
		} else if tok, ok := p.expect(tokStringLit); ok {
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
	if _, ok := p.accept(tokPartition); ok {
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
		p.expect(tokInto)
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

// parseAlterPartitionAction handles ALTER TABLE ... {COALESCE|TRUNCATE|EXCHANGE|REBUILD|OPTIMIZE|REPAIR|CHECK|IMPORT|DISCARD|FIRST|LAST|MERGE|REORGANIZE} PARTITION ...
// Returns true if a partition action was parsed, false otherwise.
func (p *HandParser) parseAlterPartitionAction(spec *ast.AlterTableSpec) bool {
	tok := p.peek()
	switch tok.Tp {

	case tokCoalesce:
		// COALESCE PARTITION n
		p.next()
		p.expect(tokPartition)
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		spec.Tp = ast.AlterTableCoalescePartitions
		spec.Num = p.parseUint64()
		return true

	case tokTruncate:
		// TRUNCATE PARTITION {name|ALL}
		p.next()
		p.expect(tokPartition)
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		spec.Tp = ast.AlterTableTruncatePartition
		if _, ok := p.accept(tokAll); ok {
			spec.OnAllPartitions = true
		} else {
			spec.PartitionNames = p.parseIdentList()
		}
		return true

	case tokExchange:
		p.next() // Consume EXCHANGE
		p.expect(tokPartition)
		spec.Tp = ast.AlterTableExchangePartition

		tok := p.peek()
		if !isIdentLike(tok.Tp) {
			p.syntaxError(tok.Offset)
			return true // Syntactic error handled, return true to stop switch
		}
		p.next()
		spec.PartitionNames = append(spec.PartitionNames, ast.NewCIStr(tok.Lit))

		p.expect(tokWith)
		p.expect(tokTable)
		spec.NewTable = p.parseTableName()

		spec.WithValidation = true
		if _, ok := p.accept(tokWithout); ok {
			p.expect(tokValidation)
			spec.WithValidation = false
		} else if _, ok := p.accept(tokWith); ok {
			p.expect(tokValidation)
			spec.WithValidation = true
		}
		return true

	case tokRebuild, tokOptimize, tokRepair, tokCheck:
		p.next()
		switch tok.Tp {
		case tokRebuild:
			spec.Tp = ast.AlterTableRebuildPartition
		case tokOptimize:
			spec.Tp = ast.AlterTableOptimizePartition
		case tokRepair:
			spec.Tp = ast.AlterTableRepairPartition
		case tokCheck:
			spec.Tp = ast.AlterTableCheckPartitions
		}
		p.parseAlterTablePartitionOptions(spec)
		return true

	case tokImport, tokDiscard:
		isImport := p.next().Tp == tokImport
		if p.peek().Tp == tokTablespace {
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
		p.expect(tokTablespace)
		return true

	case tokFirst, tokLast:
		if p.next().Tp == tokFirst {
			spec.Tp = ast.AlterTableDropFirstPartition
		} else {
			spec.Tp = ast.AlterTableAddLastPartition
		}
		p.parsePartitionLessThanExpr(spec)
		return true

	case tokMerge:
		p.next() // Consume MERGE
		p.parseMergeFirstPartition(spec)
		return true

	case tokReorganize:
		p.next()
		p.parseAlterReorganize(spec)
		return true

	case tokRemove:
		p.next()
		if _, ok := p.accept(tokTTL); ok {
			spec.Tp = ast.AlterTableRemoveTTL
		} else {
			p.expect(tokPartitioning)
			spec.Tp = ast.AlterTableRemovePartitioning
		}
		return true

	case tokIdentifier:
		// Check for MERGE/SPLIT keywords if tokIdentifier
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
// Shared between the tokMerge case and the tokIdentifier "MERGE" fallback.
func (p *HandParser) parseMergeFirstPartition(spec *ast.AlterTableSpec) {
	if _, ok := p.accept(tokFirst); ok {
		spec.Tp = ast.AlterTableReorganizeFirstPartition
		p.parsePartitionLessThanExpr(spec)
	}
}

// parseAlterTableOptions handles ALTER TABLE ... {FORCE|ALGORITHM|LOCK|READ|CONVERT|WITH|WITHOUT|STATS_OPTIONS|ATTRIBUTES|CACHE|NOCACHE|SECONDARY_LOAD|SECONDARY_UNLOAD|ENABLE|DISABLE} ...
// and generic table options. Returns true if options were parsed, false otherwise.
func (p *HandParser) parseAlterTableOptions(spec *ast.AlterTableSpec) bool {
	tok := p.peek()
	switch tok.Tp {
	case tokForce:
		p.next()
		if p.peek().Tp == tokAutoInc || p.peek().Tp == tokAutoRandomBase {
			opt := Alloc[ast.TableOption](p.arena)
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

	case tokAlgorithm:
		// ALGORITHM = {DEFAULT|COPY|INPLACE|INSTANT}
		p.next()
		p.accept(tokEq)
		spec.Tp = ast.AlterTableAlgorithm
		tok := p.next()
		switch {
		case tok.Tp == tokDefault:
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

	case tokLock:
		// LOCK [=] {DEFAULT | NONE | SHARED | EXCLUSIVE}
		p.next()
		p.accept(tokEq)
		spec.Tp = ast.AlterTableLock
		if _, ok := p.accept(tokDefault); ok {
			spec.LockType = ast.LockTypeDefault
		} else if _, ok := p.accept(tokNone); ok {
			spec.LockType = ast.LockTypeNone
		} else if _, ok := p.accept(tokShared); ok {
			spec.LockType = ast.LockTypeShared
		} else if _, ok := p.accept(tokExclusive); ok {
			spec.LockType = ast.LockTypeExclusive
		} else {
			// Unknown lock type
			tok := p.next()
			p.errs = append(p.errs, terror.ClassParser.NewStd(mysql.ErrUnknownAlterLock).GenWithStackByArgs(tok.Lit))
			return true
		}
		return true

	case tokRead:
		// READ {ONLY | WRITE}
		p.next()
		spec.Tp = ast.AlterTableWriteable
		if _, ok := p.accept(tokOnly); ok {
			spec.Writeable = false
		} else {
			p.expect(tokWrite)
			spec.Writeable = true
		}
		return true

	case tokConvert:
		// CONVERT TO CHARACTER SET charset [COLLATE collation]
		p.next()
		p.expect(tokTo)
		spec.Tp = ast.AlterTableOption
		opt := Alloc[ast.TableOption](p.arena)
		opt.Tp = ast.TableOptionCharset
		opt.UintValue = ast.TableOptionCharsetWithConvertTo
		// CharsetKw: CHARACTER SET | CHARSET | CHAR SET
		p.acceptCharsetKw()
		if _, ok := p.accept(tokDefault); ok {
			opt.Default = true
		} else if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			opt.StrValue = tok.Lit
		}
		spec.Options = []*ast.TableOption{opt}
		if _, ok := p.accept(tokCollate); ok {
			collOpt := Alloc[ast.TableOption](p.arena)
			collOpt.Tp = ast.TableOptionCollate
			if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
				collOpt.StrValue = strings.ToLower(tok.Lit)
			}
			spec.Options = append(spec.Options, collOpt)
		}
		return true

	case tokWith, tokWithout:
		if p.next().Tp == tokWith {
			spec.Tp = ast.AlterTableWithValidation
		} else {
			spec.Tp = ast.AlterTableWithoutValidation
		}
		p.expect(tokValidation)
		return true

	case tokStatsOptions:
		p.next()
		p.accept(tokEq)
		spec.Tp = ast.AlterTableStatsOptions
		statsSpec := &ast.StatsOptionsSpec{}
		if _, ok := p.accept(tokDefault); ok {
			statsSpec.Default = true
		} else if tok, ok := p.expect(tokStringLit); ok {
			statsSpec.StatsOptions = tok.Lit
		}
		spec.StatsOptionsSpec = statsSpec
		return true

	case tokAttributes:
		p.next()
		p.accept(tokEq)
		spec.Tp = ast.AlterTableAttributes
		attrSpec := &ast.AttributesSpec{}
		if _, ok := p.accept(tokDefault); ok {
			attrSpec.Default = true
		} else if tok, ok := p.expect(tokStringLit); ok {
			attrSpec.Attributes = tok.Lit
		}
		spec.AttributesSpec = attrSpec
		return true

	case tokCache, tokNoCache, tokSecondaryLoad, tokSecondaryUnload:
		p.next()
		switch tok.Tp {
		case tokCache:
			spec.Tp = ast.AlterTableCache
		case tokNoCache:
			spec.Tp = ast.AlterTableNoCache
		case tokSecondaryLoad:
			spec.Tp = ast.AlterTableSecondaryLoad
		case tokSecondaryUnload:
			spec.Tp = ast.AlterTableSecondaryUnload
		}
		return true

	case tokEnable, tokDisable:
		if p.next().Tp == tokEnable {
			spec.Tp = ast.AlterTableEnableKeys
		} else {
			spec.Tp = ast.AlterTableDisableKeys
		}
		p.expect(tokKeys)
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
