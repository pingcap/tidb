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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ---------------------------------------------------------------------------
// DROP TABLE / DROP VIEW / DROP DATABASE / DROP INDEX
// ---------------------------------------------------------------------------

// parseDropStmt dispatches DROP TABLE/VIEW/DATABASE/INDEX.
func (p *HandParser) parseDropStmt() ast.StmtNode {
	mark := p.lexer.Mark()
	p.expect(drop)

	switch p.peek().Tp {
	case temporary:
		p.next()
		return p.parseDropTable(ast.TemporaryLocal)
	case global:
		mark := p.lexer.Mark()
		p.next()
		if p.peek().Tp == binding {
			p.next() // consume BINDING
			return p.parseDropBindingStmt(true)
		}
		if _, ok := p.accept(temporary); ok {
			return p.parseDropTable(ast.TemporaryGlobal)
		}
		p.lexer.Restore(mark)
		return nil
	case session:
		p.next()
		if p.peek().Tp == binding {
			p.next() // consume BINDING
			return p.parseDropBindingStmt(false)
		}
		return nil
	case binding:
		p.next()
		return p.parseDropBindingStmt(false) // default session scope
	case statistics:
		return p.parseDropStatisticsStmt()
	case tableKwd, tables:
		return p.parseDropTable(ast.TemporaryNone)
	case view:
		p.next()
		return p.parseDropView()
	case sequence:
		return p.parseDropSequenceStmt()
	case user, role:
		p.lexer.Restore(mark)
		return p.parseDropUserStmt()
	case resource:
		return p.parseDropResourceGroupStmt()
	case placement:
		return p.parseDropPlacementPolicyStmt()
	case database: // database alias
		p.next()
		return p.parseDropDatabase()
	case index:
		p.next()
		return p.parseDropIndex()
	case procedure:
		return p.parseDropProcedureStmt()
	case prepare:
		// DROP PREPARE stmt_name -> same as DEALLOCATE PREPARE
		// parseDeallocateStmt expects deallocate, then prepare.
		// Here we consumed drop. Next is prepare.
		// We can call a helper or handle it here.
		// parseDeallocateStmt: p.expect(deallocate); p.accept(prepare);
		// So we can't reuse parseDeallocateStmt directly if it strictly expects DEALLOCATE.
		// Let's modify parseDeallocateStmt to be more flexible or duplicate logic.
		// Duplicating logic is simpler for now:
		stmt := Alloc[ast.DeallocateStmt](p.arena)
		p.next() // consume PREPARE
		stmt.Name = p.parseName()
		return stmt
	case stats:
		return p.parseDropStatsStmt()
	default:
		if p.peek().IsKeyword("HYPO") && p.peekN(1).Tp == index {
			p.next() // consume HYPO
			p.next() // consume INDEX
			stmt := p.parseDropIndex()
			if s, ok := stmt.(*ast.DropIndexStmt); ok {
				s.IsHypo = true
			}
			return stmt
		}
		return nil
	}
}

// parseDropStatsStmt parses: DROP STATS t1 [, t2, ...] [GLOBAL | PARTITION p1, p2, ...]
func (p *HandParser) parseDropStatsStmt() ast.StmtNode {
	stmt := Alloc[ast.DropStatsStmt](p.arena)
	p.next() // consume STATS

	stmt.Tables = p.parseTableNameList()
	if stmt.Tables == nil {
		return nil
	}

	// Optional: GLOBAL or PARTITION p1, p2, ...
	if p.peek().IsKeyword("GLOBAL") {
		p.next()
		stmt.IsGlobalStats = true
		p.warns = append(p.warns, ErrWarnDeprecatedSyntax.FastGenByArgs("DROP STATS ... GLOBAL", "DROP STATS ..."))
	} else if _, ok := p.accept(partition); ok {
		for {
			nameTok := p.peek()
			if !isIdentLike(nameTok.Tp) || nameTok.Tp == stringLit {
				p.syntaxErrorAt(nameTok)
				return nil
			}
			p.next()
			stmt.PartitionNames = append(stmt.PartitionNames, ast.NewCIStr(nameTok.Lit))
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.warns = append(p.warns, ErrWarnDeprecatedSyntaxNoReplacement.FastGenByArgs("'DROP STATS ... PARTITION ...'", ""))
	}

	return stmt
}

// parseDropDatabase parses: DATABASE [IF EXISTS] dbname
func (p *HandParser) parseDropDatabase() ast.StmtNode {
	stmt := Alloc[ast.DropDatabaseStmt](p.arena)
	stmt.IfExists = p.acceptIfExists()
	tok := p.next()
	if tok.Tp == EOF {
		return nil
	}
	stmt.Name = ast.NewCIStr(tok.Lit)
	return stmt
}

// parseTableNameList parses: tablename [, tablename ...]
// Returns nil if the first table name cannot be parsed.
func (p *HandParser) parseTableNameList() []*ast.TableName {
	first := p.parseTableName()
	if first == nil {
		return nil
	}
	list := []*ast.TableName{first}
	for {
		if _, ok := p.accept(','); !ok {
			break
		}
		tn := p.parseTableName()
		if tn == nil {
			return nil
		}
		list = append(list, tn)
	}
	return list
}

// parseDropTable parses: [TEMPORARY|GLOBAL TEMPORARY] TABLE [IF EXISTS] t1, t2 [RESTRICT|CASCADE]
func (p *HandParser) parseDropTable(tmpKw ast.TemporaryKeyword) ast.StmtNode {
	p.acceptAny(tableKwd, tables)
	return p.parseDropTableOrView(false, tmpKw)
}

// parseDropView parses: VIEW [IF EXISTS] t1, t2 [RESTRICT|CASCADE]
func (p *HandParser) parseDropView() ast.StmtNode {
	return p.parseDropTableOrView(true, ast.TemporaryNone)
}

// parseDropTableOrView is the shared implementation for DROP TABLE and DROP VIEW.
func (p *HandParser) parseDropTableOrView(isView bool, tmpKw ast.TemporaryKeyword) ast.StmtNode {
	stmt := Alloc[ast.DropTableStmt](p.arena)
	stmt.IsView = isView
	stmt.TemporaryKeyword = tmpKw
	stmt.IfExists = p.acceptIfExists()
	stmt.Tables = p.parseTableNameList()
	if stmt.Tables == nil {
		return nil
	}
	p.acceptRestrictOrCascade()
	return stmt
}

// parseDropIndex parses: INDEX indexname ON tablename [LOCK|ALGORITHM ...]
func (p *HandParser) parseDropIndex() ast.StmtNode {
	stmt := Alloc[ast.DropIndexStmt](p.arena)
	stmt.IfExists = p.acceptIfExists()
	tok := p.next()
	stmt.IndexName = tok.Lit
	p.expect(on)
	stmt.Table = p.parseTableName()
	stmt.LockAlg = p.parseIndexLockAndAlgorithm()
	return stmt
}

// ---------------------------------------------------------------------------
// TRUNCATE TABLE
// ---------------------------------------------------------------------------

// parseTruncateTableStmt parses: TRUNCATE [TABLE] tablename
func (p *HandParser) parseTruncateTableStmt() ast.StmtNode {
	stmt := Alloc[ast.TruncateTableStmt](p.arena)
	p.expect(truncate)
	p.accept(tableKwd) // optional TABLE keyword
	stmt.Table = p.parseTableName()
	if stmt.Table == nil {
		return nil
	}
	return stmt
}

// ---------------------------------------------------------------------------
// RENAME TABLE
// ---------------------------------------------------------------------------

// parseRenameTableStmt parses: RENAME TABLE t1 TO t2 [, t3 TO t4 ...]
func (p *HandParser) parseRenameTableStmt() ast.StmtNode {
	stmt := Alloc[ast.RenameTableStmt](p.arena)
	p.expect(rename)
	p.expect(tableKwd)

	first := p.parseTableToTable()
	if first == nil {
		return nil
	}
	stmt.TableToTables = []*ast.TableToTable{first}

	for {
		if _, ok := p.accept(','); !ok {
			break
		}
		next := p.parseTableToTable()
		if next == nil {
			return nil
		}
		stmt.TableToTables = append(stmt.TableToTables, next)
	}
	return stmt
}

// parseTableToTable parses: old_table TO new_table
func (p *HandParser) parseTableToTable() *ast.TableToTable {
	oldTable := p.parseTableName()
	if oldTable == nil {
		return nil
	}
	p.expect(to)
	newTable := p.parseTableName()
	if newTable == nil {
		return nil
	}
	return &ast.TableToTable{OldTable: oldTable, NewTable: newTable}
}

// ---------------------------------------------------------------------------
// ANALYZE TABLE
// ---------------------------------------------------------------------------

// parseAnalyzeTableStmt parses: ANALYZE [NO_WRITE_TO_BINLOG|LOCAL] [INCREMENTAL] TABLE t1[, t2]
//
//	[PARTITION p1[, p2]]
//	[INDEX [idx1[, idx2]] | ALL COLUMNS | PREDICATE COLUMNS | COLUMNS c1[, c2]]
//	[UPDATE HISTOGRAM ON c1[, c2] | DROP HISTOGRAM ON c1[, c2]]
//	[WITH n BUCKETS, n TOPN, ...]
func (p *HandParser) parseAnalyzeTableStmt() ast.StmtNode {
	stmt := Alloc[ast.AnalyzeTableStmt](p.arena)
	p.expect(analyze)

	// Optional: NO_WRITE_TO_BINLOG or LOCAL
	stmt.NoWriteToBinLog = p.acceptNoWriteToBinlog()

	// Optional: INCREMENTAL
	if p.peek().IsKeyword("INCREMENTAL") {
		p.next()
		stmt.Incremental = true
	}

	p.expect(tableKwd)
	stmt.TableNames = p.parseTableNameList()
	if stmt.TableNames == nil {
		return nil
	}

	// Optional: PARTITION p1[, p2]
	if _, ok := p.accept(partition); ok {
		stmt.PartitionNames = p.parseIdentList()
	}

	// Optional: UPDATE HISTOGRAM ON c1[,c2] | DROP HISTOGRAM ON c1[,c2]
	if p.peek().Tp == update || p.peek().Tp == drop {
		histTok := p.peek()
		if p.peekN(1).IsKeyword("HISTOGRAM") {
			p.next() // consume UPDATE/DROP
			p.next() // consume HISTOGRAM
			if histTok.Tp == update {
				stmt.HistogramOperation = ast.HistogramOperationUpdate
			} else {
				stmt.HistogramOperation = ast.HistogramOperationDrop
			}
			p.expect(on)
			// Parse column names (simple identifiers, NOT dotted names)
			cols, ok := p.parseSimpleColumnNameList()
			if !ok {
				return nil
			}
			stmt.ColumnNames = cols
			// Parse optional WITH clause after histogram
			p.parseAnalyzeWithOpts(stmt)
			return stmt
		}
	}

	// Optional: INDEX [idx1[, idx2]]
	if _, ok := p.accept(index); ok {
		stmt.IndexFlag = true
		// Parse optional index names. PRIMARY is a valid index name here.
		for isIdentLike(p.peek().Tp) || p.peek().Tp == primary {
			idxTok := p.next()
			stmt.IndexNames = append(stmt.IndexNames, ast.NewCIStr(idxTok.Lit))
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.parseAnalyzeWithOpts(stmt)
		return stmt
	}

	// Optional: ALL COLUMNS | PREDICATE COLUMNS | COLUMNS c1[, c2]
	if p.peek().Tp == all && p.peekN(1).IsKeyword("COLUMNS") {
		p.next() // ALL
		p.next() // COLUMNS
		stmt.ColumnChoice = ast.AllColumns
	} else if p.peek().IsKeyword("PREDICATE") && p.peekN(1).IsKeyword("COLUMNS") {
		p.next() // PREDICATE
		p.next() // COLUMNS
		stmt.ColumnChoice = ast.PredicateColumns
	} else if p.peek().IsKeyword("COLUMNS") {
		p.next() // COLUMNS
		stmt.ColumnChoice = ast.ColumnList
		cols, ok := p.parseSimpleColumnNameList()
		if !ok {
			return nil
		}
		stmt.ColumnNames = cols
	}

	p.parseAnalyzeWithOpts(stmt)
	return stmt
}

// parseAnalyzeWithOpts parses optional WITH n BUCKETS, n TOPN, etc.
func (p *HandParser) parseAnalyzeWithOpts(stmt *ast.AnalyzeTableStmt) {
	if _, ok := p.accept(with); !ok {
		return
	}
	for {
		// Parse value: yacc uses NUM for most options but NumLiteral for SAMPLERATE.
		// Since the value comes before the keyword, accept the full NumLiteral set
		// (intLit | floatLit | decLit) and let downstream validation handle type mismatches.
		valTok, ok := p.expectAny(intLit, floatLit, decLit)
		if !ok {
			return
		}
		opt := ast.AnalyzeOpt{}
		opt.Value = ast.NewValueExpr(valTok.Item, "", "")

		// Parse option type keyword
		next := p.next()
		switch strings.ToUpper(next.Lit) {
		case "BUCKETS":
			opt.Type = ast.AnalyzeOptNumBuckets
		case "TOPN":
			opt.Type = ast.AnalyzeOptNumTopN
		case "CMSKETCH":
			// CMSKETCH WIDTH or CMSKETCH DEPTH
			sub := p.next()
			if sub.IsKeyword("WIDTH") {
				opt.Type = ast.AnalyzeOptCMSketchWidth
			} else {
				opt.Type = ast.AnalyzeOptCMSketchDepth
			}
		case "SAMPLES":
			opt.Type = ast.AnalyzeOptNumSamples
		case "SAMPLERATE":
			opt.Type = ast.AnalyzeOptSampleRate
		}
		stmt.AnalyzeOpts = append(stmt.AnalyzeOpts, opt)

		if _, ok := p.accept(','); !ok {
			break
		}
	}
}

// parseDropProcedureStmt parses: DROP PROCEDURE [IF EXISTS] sp_name
func (p *HandParser) parseDropProcedureStmt() ast.StmtNode {
	stmt := Alloc[ast.DropProcedureStmt](p.arena)
	p.expect(procedure)
	stmt.IfExists = p.acceptIfExists()
	stmt.ProcedureName = p.parseTableName()
	return stmt
}
