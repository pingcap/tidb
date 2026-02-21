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
	if _, ok := p.accept(57385); ok {
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
	} else if _, ok := p.accept(57515); ok {
		spec.Tp = ast.AlterTableAddPartitions
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		// ADD PARTITION [IF NOT EXISTS] [( PARTITION defs )]
		spec.IfNotExists = p.acceptIfNotExists()
		if _, ok := p.accept(57828); ok {
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
	} else if _, ok := p.accept(58185); ok {
		spec.Tp = ast.AlterTableAddStatistics
		spec.IfNotExists = p.acceptIfNotExists()
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.Statistics = &ast.StatisticsSpec{StatsName: tok.Lit}
			// Parse stats type: CARDINALITY | CORRELATION | DEPENDENCY
			if _, ok := p.accept(58154); ok {
				spec.Statistics.StatsType = ast.StatsTypeCardinality
			} else if _, ok := p.accept(58157); ok {
				spec.Statistics.StatsType = ast.StatsTypeCorrelation
			} else if _, ok := p.accept(58159); ok {
				spec.Statistics.StatsType = ast.StatsTypeDependency
			}
			// Parse column list: (col1, col2, ...)
			if _, ok := p.accept('('); ok {
				for {
					if tok, ok := p.expectAny(57346, 57353); ok {
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
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.OldColumnName = Alloc[ast.ColumnName](p.arena)
			spec.OldColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.acceptAny(57532, 57378)
	}

	if _, ok := p.accept(57385); ok {
		parseDropColumn()
	} else if _, ok := p.accept(57518); ok {
		p.expect(57467)
		spec.Tp = ast.AlterTableDropPrimaryKey
	} else if _, ok := p.acceptAny(57449, 57467); ok {
		spec.Tp = ast.AlterTableDropIndex
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.Name = tok.Lit
		}
	} else if _, ok := p.accept(57433); ok {
		p.expect(57467)
		spec.Tp = ast.AlterTableDropForeignKey
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.Name = tok.Lit
		}
	} else if _, ok := p.acceptAny(57383, 57386); ok {
		spec.Tp = ast.AlterTableDropCheck
		if tok, ok := p.expectAny(57346, 57353); ok {
			c := Alloc[ast.Constraint](p.arena)
			c.Name = tok.Lit
			spec.Constraint = c
		}
	} else if _, ok := p.accept(58185); ok {
		spec.Tp = ast.AlterTableDropStatistics
		spec.IfExists = p.acceptIfExists()
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.Statistics = &ast.StatisticsSpec{StatsName: tok.Lit}
		}
	} else if _, ok := p.acceptKeyword(57724, "FIRST"); ok {
		spec.Tp = ast.AlterTableDropFirstPartition
		p.parsePartitionLessThanExpr(spec)
		if _, ok := p.acceptKeyword(57724, "FIRST"); ok {
			spec.Tp = ast.AlterTableReorganizeFirstPartition
			p.parsePartitionLessThanExpr(spec)
		}
	} else if _, ok := p.accept(57515); ok {
		spec.Tp = ast.AlterTableDropPartition
		spec.IfExists = p.acceptIfExists()
		spec.PartitionNames = p.parseIdentList()
	} else {
		parseDropColumn()
	}
}

// parseAlterRename handles ALTER TABLE ... RENAME {TO|AS|COLUMN|INDEX|KEY|new_tbl}.
func (p *HandParser) parseAlterRename(spec *ast.AlterTableSpec) {
	if _, ok := p.acceptAny(57564, 57369); ok {
		spec.Tp = ast.AlterTableRenameTable
		spec.NewTable = p.parseTableName()
	} else if _, ok := p.accept(57385); ok {
		spec.Tp = ast.AlterTableRenameColumn
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.OldColumnName = Alloc[ast.ColumnName](p.arena)
			spec.OldColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.expect(57564)
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.NewColumnName = Alloc[ast.ColumnName](p.arena)
			spec.NewColumnName.Name = ast.NewCIStr(tok.Lit)
		}
	} else if _, ok := p.acceptAny(57449, 57467); ok {
		spec.Tp = ast.AlterTableRenameIndex
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.FromKey = ast.NewCIStr(tok.Lit)
		}
		p.expect(57564)
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.ToKey = ast.NewCIStr(tok.Lit)
		}
	} else {
		// RENAME [TO|=] new_tbl
		spec.Tp = ast.AlterTableRenameTable
		if _, ok := p.accept(57564); !ok {
			p.accept(58202)
		}
		spec.NewTable = p.parseTableName()
	}
}

// parseAlterAlter handles ALTER TABLE ... ALTER {INDEX|CHECK|CONSTRAINT|[COLUMN] col_name}.
// Returns non-nil spec for branches that need early return from parseAlterTableSpec,
// or nil if the caller should fall through to the default return.
func (p *HandParser) parseAlterAlter(spec *ast.AlterTableSpec) *ast.AlterTableSpec {
	// ALTER INDEX index_name {VISIBLE|INVISIBLE}
	if _, ok := p.accept(57449); ok {
		spec.Tp = ast.AlterTableIndexInvisible
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.IndexName = ast.NewCIStr(tok.Lit)
		}
		if _, ok := p.accept(57981); ok {
			spec.Visibility = ast.IndexVisibilityVisible
		} else if _, ok := p.accept(57753); ok {
			spec.Visibility = ast.IndexVisibilityInvisible
		} else {
			p.syntaxError(p.peek().Offset)
			return nil
		}
		return spec
	}

	// ALTER CHECK|CONSTRAINT name {ENFORCED | NOT ENFORCED}
	if _, ok := p.acceptAny(57383, 57386); ok {
		spec.Tp = ast.AlterTableAlterCheck
		spec.Constraint = Alloc[ast.Constraint](p.arena)
		if tok, ok := p.expectAny(57346, 57353); ok {
			spec.Constraint.Name = tok.Lit
		}
		if _, ok := p.accept(57702); ok {
			spec.Constraint.Enforced = true
		} else if _, ok := p.accept(57498); ok {
			p.expect(57702)
			spec.Constraint.Enforced = false
		} else {
			p.syntaxError(p.peek().Offset)
			return nil
		}
		return spec
	}

	// ALTER [COLUMN] col_name {SET DEFAULT expr | DROP DEFAULT}
	p.accept(57385)
	spec.OldColumnName = p.parseColumnName()
	if _, ok := p.accept(57541); ok {
		p.expect(57405)
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
	} else if _, ok := p.accept(57415); ok {
		p.expect(57405)
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
	if p.peekN(1).Tp == 57376 {
		spec.Tp = ast.AlterTablePartition
		spec.Partition = p.parsePartitionOptions()
		return spec
	}
	// PARTITION p0 PLACEMENT POLICY = p1
	p.next()
	if tok, ok := p.expectAny(57346, 57353); ok {
		spec.PartitionNames = append(spec.PartitionNames, ast.NewCIStr(tok.Lit))
	}
	// PARTITION p ATTRIBUTES [=] DEFAULT|'str'
	if p.peek().Tp == 57609 {
		p.next()
		p.accept(58202)
		spec.Tp = ast.AlterTablePartitionAttributes
		attrSpec := &ast.AttributesSpec{}
		if _, ok := p.accept(57405); ok {
			attrSpec.Default = true
		} else if tok, ok := p.expect(57353); ok {
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
	if _, ok := p.accept(57515); ok {
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
		p.expect(57463)
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

	case 57650:
		// COALESCE PARTITION n
		p.next()
		p.expect(57515)
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		spec.Tp = ast.AlterTableCoalescePartitions
		spec.Num = p.parseUint64()
		return true

	case 57963:
		// TRUNCATE PARTITION {name|ALL}
		p.next()
		p.expect(57515)
		spec.NoWriteToBinlog = p.acceptNoWriteToBinlog()
		spec.Tp = ast.AlterTableTruncatePartition
		if _, ok := p.accept(57364); ok {
			spec.OnAllPartitions = true
		} else {
			spec.PartitionNames = p.parseIdentList()
		}
		return true

	case 57713:
		p.next() // Consume EXCHANGE
		p.expect(57515)
		spec.Tp = ast.AlterTableExchangePartition

		tok := p.peek()
		if !isIdentLike(tok.Tp) {
			p.syntaxError(tok.Offset)
			return true // Syntactic error handled, return true to stop switch
		}
		p.next()
		spec.PartitionNames = append(spec.PartitionNames, ast.NewCIStr(tok.Lit))

		p.expect(57590)
		p.expect(57556)
		spec.NewTable = p.parseTableName()

		spec.WithValidation = true
		if _, ok := p.accept(57987); ok {
			p.expect(57976)
			spec.WithValidation = false
		} else if _, ok := p.accept(57590); ok {
			p.expect(57976)
			spec.WithValidation = true
		}
		return true

	case 57855, 57506, 57863, 57383:
		p.next()
		switch tok.Tp {
		case 57855:
			spec.Tp = ast.AlterTableRebuildPartition
		case 57506:
			spec.Tp = ast.AlterTableOptimizePartition
		case 57863:
			spec.Tp = ast.AlterTableRepairPartition
		case 57383:
			spec.Tp = ast.AlterTableCheckPartitions
		}
		p.parseAlterTablePartitionOptions(spec)
		return true

	case 57746, 57691:
		isImport := p.next().Tp == 57746
		if p.peek().Tp == 57945 {
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
		p.expect(57945)
		return true

	case 57724, 57763:
		if p.next().Tp == 57724 {
			spec.Tp = ast.AlterTableDropFirstPartition
		} else {
			spec.Tp = ast.AlterTableAddLastPartition
		}
		p.parsePartitionLessThanExpr(spec)
		return true

	case 57785:
		p.next() // Consume MERGE
		p.parseMergeFirstPartition(spec)
		return true

	case 57862:
		p.next()
		p.parseAlterReorganize(spec)
		return true

	case 57861:
		p.next()
		if _, ok := p.accept(57965); ok {
			spec.Tp = ast.AlterTableRemoveTTL
		} else {
			p.expect(57827)
			spec.Tp = ast.AlterTableRemovePartitioning
		}
		return true

	case 57346:
		// Check for MERGE/SPLIT keywords if 57346
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
// Shared between the 57785 case and the 57346 "MERGE" fallback.
func (p *HandParser) parseMergeFirstPartition(spec *ast.AlterTableSpec) {
	if _, ok := p.accept(57724); ok {
		spec.Tp = ast.AlterTableReorganizeFirstPartition
		p.parsePartitionLessThanExpr(spec)
	}
}

// parseAlterTableOptions handles ALTER TABLE ... {FORCE|ALGORITHM|LOCK|READ|CONVERT|WITH|WITHOUT|STATS_OPTIONS|ATTRIBUTES|CACHE|NOCACHE|SECONDARY_LOAD|SECONDARY_UNLOAD|ENABLE|DISABLE} ...
// and generic table options. Returns true if options were parsed, false otherwise.
func (p *HandParser) parseAlterTableOptions(spec *ast.AlterTableSpec) bool {
	tok := p.peek()
	switch tok.Tp {
	case 57432:
		p.next()
		if p.peek().Tp == 57612 || p.peek().Tp == 57614 {
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

	case 57603:
		// ALGORITHM = {DEFAULT|COPY|INPLACE|INSTANT}
		p.next()
		p.accept(58202)
		spec.Tp = ast.AlterTableAlgorithm
		tok := p.next()
		switch {
		case tok.Tp == 57405:
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

	case 57483:
		// LOCK [=] {DEFAULT | NONE | SHARED | EXCLUSIVE}
		p.next()
		p.accept(58202)
		spec.Tp = ast.AlterTableLock
		if _, ok := p.accept(57405); ok {
			spec.LockType = ast.LockTypeDefault
		} else if _, ok := p.accept(57806); ok {
			spec.LockType = ast.LockTypeNone
		} else if _, ok := p.accept(57903); ok {
			spec.LockType = ast.LockTypeShared
		} else if _, ok := p.accept(57714); ok {
			spec.LockType = ast.LockTypeExclusive
		} else {
			// Unknown lock type
			tok := p.next()
			p.errs = append(p.errs, terror.ClassParser.NewStd(mysql.ErrUnknownAlterLock).GenWithStackByArgs(tok.Lit))
			return true
		}
		return true

	case 57522:
		// READ {ONLY | WRITE}
		p.next()
		spec.Tp = ast.AlterTableWriteable
		if _, ok := p.accept(57816); ok {
			spec.Writeable = false
		} else {
			p.expect(57591)
			spec.Writeable = true
		}
		return true

	case 57388:
		// CONVERT TO CHARACTER SET charset [COLLATE collation]
		p.next()
		p.expect(57564)
		spec.Tp = ast.AlterTableOption
		opt := Alloc[ast.TableOption](p.arena)
		opt.Tp = ast.TableOptionCharset
		opt.UintValue = ast.TableOptionCharsetWithConvertTo
		// CharsetKw: CHARACTER SET | CHARSET | CHAR SET
		p.acceptCharsetKw()
		if _, ok := p.accept(57405); ok {
			opt.Default = true
		} else if tok, ok := p.expectAny(57346, 57353); ok {
			opt.StrValue = tok.Lit
		}
		spec.Options = []*ast.TableOption{opt}
		if _, ok := p.accept(57384); ok {
			collOpt := Alloc[ast.TableOption](p.arena)
			collOpt.Tp = ast.TableOptionCollate
			if tok, ok := p.expectAny(57346, 57353); ok {
				collOpt.StrValue = strings.ToLower(tok.Lit)
			}
			spec.Options = append(spec.Options, collOpt)
		}
		return true

	case 57590, 57987:
		if p.next().Tp == 57590 {
			spec.Tp = ast.AlterTableWithValidation
		} else {
			spec.Tp = ast.AlterTableWithoutValidation
		}
		p.expect(57976)
		return true

	case 57929:
		p.next()
		p.accept(58202)
		spec.Tp = ast.AlterTableStatsOptions
		statsSpec := &ast.StatsOptionsSpec{}
		if _, ok := p.accept(57405); ok {
			statsSpec.Default = true
		} else if tok, ok := p.expect(57353); ok {
			statsSpec.StatsOptions = tok.Lit
		}
		spec.StatsOptionsSpec = statsSpec
		return true

	case 57609:
		p.next()
		p.accept(58202)
		spec.Tp = ast.AlterTableAttributes
		attrSpec := &ast.AttributesSpec{}
		if _, ok := p.accept(57405); ok {
			attrSpec.Default = true
		} else if tok, ok := p.expect(57353); ok {
			attrSpec.Attributes = tok.Lit
		}
		spec.AttributesSpec = attrSpec
		return true

	case 57633, 57800, 57891, 57892:
		p.next()
		switch tok.Tp {
		case 57633:
			spec.Tp = ast.AlterTableCache
		case 57800:
			spec.Tp = ast.AlterTableNoCache
		case 57891:
			spec.Tp = ast.AlterTableSecondaryLoad
		case 57892:
			spec.Tp = ast.AlterTableSecondaryUnload
		}
		return true

	case 57696, 57689:
		if p.next().Tp == 57696 {
			spec.Tp = ast.AlterTableEnableKeys
		} else {
			spec.Tp = ast.AlterTableDisableKeys
		}
		p.expect(57468)
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
