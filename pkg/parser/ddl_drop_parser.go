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
	p.expect(57415)

	switch p.peek().Tp {
	case 57947:
		p.next()
		return p.parseDropTable(ast.TemporaryLocal)
	case 57733:
		mark := p.lexer.Mark()
		p.next()
		if p.peek().Tp == 57623 {
			p.next() // consume BINDING
			return p.parseDropBindingStmt(true)
		}
		if _, ok := p.accept(57947); ok {
			return p.parseDropTable(ast.TemporaryGlobal)
		}
		p.lexer.Restore(mark)
		return nil
	case 57899:
		p.next()
		if p.peek().Tp == 57623 {
			p.next() // consume BINDING
			return p.parseDropBindingStmt(false)
		}
		return nil
	case 57623:
		p.next()
		return p.parseDropBindingStmt(false) // default session scope
	case 58181:
		return p.parseDropStatisticsStmt()
	case 57556, 57944:
		return p.parseDropTable(ast.TemporaryNone)
	case 57980:
		p.next()
		return p.parseDropView()
	case 57896:
		return p.parseDropSequenceStmt()
	case 57975, 57877:
		p.lexer.Restore(mark)
		return p.parseDropUserStmt()
	case 57869:
		return p.parseDropResourceGroupStmt()
	case 58054:
		return p.parseDropPlacementPolicyStmt()
	case 57398: // 57398 alias
		p.next()
		return p.parseDropDatabase()
	case 57449:
		p.next()
		return p.parseDropIndex()
	case 57519:
		return p.parseDropProcedureStmt()
	case 57840:
		// DROP PREPARE stmt_name -> same as DEALLOCATE PREPARE
		// parseDeallocateStmt expects 57683, then 57840.
		// Here we consumed 57415. Next is 57840.
		// We can call a helper or handle it here.
		// parseDeallocateStmt: p.expect(57683); p.accept(57840);
		// So we can't reuse parseDeallocateStmt directly if it strictly expects DEALLOCATE.
		// Let's modify parseDeallocateStmt to be more flexible or duplicate logic.
		// Duplicating logic is simpler for now:
		stmt := Alloc[ast.DeallocateStmt](p.arena)
		p.next() // consume PREPARE
		stmt.Name = p.parseName()
		return stmt
	case 58182:
		return p.parseDropStatsStmt()
	default:
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
	} else if _, ok := p.accept(57515); ok {
		for {
			nameTok := p.next()
			stmt.PartitionNames = append(stmt.PartitionNames, ast.NewCIStr(nameTok.Lit))
			if _, ok := p.accept(','); !ok {
				break
			}
		}
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
	p.acceptAny(57556, 57944)
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
	p.expect(57505)
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
	p.expect(57963)
	p.accept(57556) // optional TABLE keyword
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
	p.expect(57528)
	p.expect(57556)

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
	p.expect(57564)
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
	p.expect(57366)

	// Optional: NO_WRITE_TO_BINLOG or LOCAL
	stmt.NoWriteToBinLog = p.acceptNoWriteToBinlog()

	// Optional: INCREMENTAL
	if p.peek().IsKeyword("INCREMENTAL") {
		p.next()
		stmt.Incremental = true
	}

	p.expect(57556)
	stmt.TableNames = p.parseTableNameList()
	if stmt.TableNames == nil {
		return nil
	}

	// Optional: PARTITION p1[, p2]
	if _, ok := p.accept(57515); ok {
		stmt.PartitionNames = p.parseIdentList()
	}

	// Optional: UPDATE HISTOGRAM ON c1[,c2] | DROP HISTOGRAM ON c1[,c2]
	if p.peek().Tp == 57573 || p.peek().Tp == 57415 {
		histTok := p.peek()
		if p.peekN(1).IsKeyword("HISTOGRAM") {
			p.next() // consume UPDATE/DROP
			p.next() // consume HISTOGRAM
			if histTok.Tp == 57573 {
				stmt.HistogramOperation = ast.HistogramOperationUpdate
			} else {
				stmt.HistogramOperation = ast.HistogramOperationDrop
			}
			p.expect(57505)
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
	if _, ok := p.accept(57449); ok {
		stmt.IndexFlag = true
		// Parse optional index names
		for isIdentLike(p.peek().Tp) {
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
	if p.peek().Tp == 57364 && p.peekN(1).IsKeyword("COLUMNS") {
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
	if _, ok := p.accept(57590); !ok {
		return
	}
	for {
		// Parse value (integer or decimal)
		valTok, ok := p.expectAny(58197, 58196)
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
	p.expect(57519)
	stmt.IfExists = p.acceptIfExists()
	stmt.ProcedureName = p.parseTableName()
	return stmt
}
