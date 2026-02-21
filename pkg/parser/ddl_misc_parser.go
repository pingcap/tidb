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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// ---------------------------------------------------------------------------
// CREATE INDEX
// ---------------------------------------------------------------------------

// parseCreateIndexStmt parses:
//
//	CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX [IF NOT EXISTS] idx ON tbl (col, ...) [options]
func (p *HandParser) parseCreateIndexStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateIndexStmt](p.arena)
	p.expect(57389)

	// [UNIQUE | FULLTEXT | SPATIAL | VECTOR | COLUMNAR]
	switch p.peek().Tp {
	case 57569:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeUnique
	case 57435:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeFulltext
	case 57544:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeSpatial
	case 57979:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeVector
	case 57652:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeColumnar
	}

	p.expect(57449)

	// [IF NOT EXISTS]
	stmt.IfNotExists = p.acceptIfNotExists()

	// index_name
	if tok := p.peek(); tok.IsIdent() {
		stmt.IndexName = p.next().Lit
	}

	// Optional USING/TYPE before ON (IndexNameAndTypeOpt)
	if preOpt := p.parseOptionalUsingIndexType(); preOpt != nil {
		stmt.IndexOption = preOpt
	}

	// ON table_name
	p.expect(57505)
	stmt.Table = p.parseTableName()

	// (col1, col2, ...)
	stmt.IndexPartSpecifications = p.parseIndexPartSpecifications()

	// [index options]
	postOpt := p.parseIndexOptions()
	if postOpt != nil {
		if stmt.IndexOption != nil && postOpt.Tp == ast.IndexTypeInvalid {
			// Pre-ON type was set; merge into post-ON options
			postOpt.Tp = stmt.IndexOption.Tp
		}
		stmt.IndexOption = postOpt
	}
	// Ensure IndexOption is never nil — preprocessor accesses it unconditionally.
	if stmt.IndexOption == nil {
		stmt.IndexOption = &ast.IndexOption{}
	}

	// IndexLockAndAlgorithmOpt: LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}, ALGORITHM [=] {DEFAULT|INPLACE|COPY|INSTANT}
	stmt.LockAlg = p.parseIndexLockAndAlgorithm()

	return stmt
}

// ---------------------------------------------------------------------------

// parseIndexLockAndAlgorithm parses optional LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}
// and/or ALGORITHM [=] {DEFAULT|INPLACE|COPY|INSTANT} after index options.
func (p *HandParser) parseIndexLockAndAlgorithm() *ast.IndexLockAndAlgorithm {
	lockTp := ast.LockTypeDefault
	algTp := ast.AlgorithmTypeDefault
	found := false

	for i := 0; i < 2; i++ {
		if _, ok := p.accept(57483); ok {
			found = true
			p.accept(58202)
			tok := p.next()
			if tok.Tp == 57405 {
				lockTp = ast.LockTypeDefault
			} else {
				switch strings.ToUpper(tok.Lit) {
				case "NONE":
					lockTp = ast.LockTypeNone
				case "SHARED":
					lockTp = ast.LockTypeShared
				case "EXCLUSIVE":
					lockTp = ast.LockTypeExclusive
				default:
					p.errs = append(p.errs, terror.ClassParser.NewStd(mysql.ErrUnknownAlterLock).GenWithStackByArgs(tok.Lit))
					return nil
				}
			}
		} else if _, ok := p.accept(57603); ok {
			found = true
			p.accept(58202)
			tok := p.next()
			if tok.Tp == 57405 {
				algTp = ast.AlgorithmTypeDefault
			} else {
				switch tok.Tp {
				case 58007:
					algTp = ast.AlgorithmTypeCopy
				case 58030:
					algTp = ast.AlgorithmTypeInplace
				case 58031:
					algTp = ast.AlgorithmTypeInstant
				default:
					p.errs = append(p.errs, terror.ClassParser.NewStd(mysql.ErrUnknownAlterAlgorithm).GenWithStackByArgs(tok.Lit))
					return nil
				}
			}
		} else {
			break
		}
	}

	if !found || (lockTp == ast.LockTypeDefault && algTp == ast.AlgorithmTypeDefault) {
		return nil
	}
	return &ast.IndexLockAndAlgorithm{
		LockTp:      lockTp,
		AlgorithmTp: algTp,
	}
}

// ---------------------------------------------------------------------------
// RECOVER TABLE
// ---------------------------------------------------------------------------

// parseRecoverTableStmt parses:
//   - RECOVER TABLE BY JOB int64
//   - RECOVER TABLE tablename
//   - RECOVER TABLE tablename int64
func (p *HandParser) parseRecoverTableStmt() ast.StmtNode {
	stmt := Alloc[ast.RecoverTableStmt](p.arena)
	p.next() // consume RECOVER
	p.expect(57556)

	// BY JOB int64
	if _, ok := p.accept(57376); ok {
		p.expect(58166)
		v, ok := p.parseInt64()
		if !ok {
			return nil
		}
		stmt.JobID = v
		return stmt
	}

	// tablename [int64]
	stmt.Table = p.parseTableName()
	if tok, ok := p.acceptAny(58197); ok {
		v, valid := tokenItemToInt64(tok.Item)
		if !valid {
			p.error(tok.Offset, "integer value is out of range")
			return nil
		}
		stmt.JobNum = v
	}
	return stmt
}

// ---------------------------------------------------------------------------
// CREATE / ALTER DATABASE
// ---------------------------------------------------------------------------

// parseCreateDatabaseStmt parses:
//
//	CREATE DATABASE [IF NOT EXISTS] db [options]
func (p *HandParser) parseCreateDatabaseStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateDatabaseStmt](p.arena)
	p.expect(57389)
	p.expect(57398)

	stmt.IfNotExists = p.acceptIfNotExists()

	nameTok := p.next()
	if !isIdentLike(nameTok.Tp) {
		p.error(nameTok.Offset, "expected database name")
		return nil
	}
	stmt.Name = ast.NewCIStr(nameTok.Lit)
	stmt.Options = p.parseDatabaseOptions()
	return stmt
}

// parseAlterDatabaseStmt parses:
//
//	ALTER DATABASE db [options]
func (p *HandParser) parseAlterDatabaseStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterDatabaseStmt](p.arena)
	p.expect(57365)
	p.expect(57398)

	// Database name is optional — if next token is a db option keyword, skip name.
	// Note: 57639 (CHARSET) is NOT included here because in the parser,
	// standalone CHARSET is consumed as the db name (shift/reduce preference).
	// Only multi-word option starts (CHARACTER SET, CHAR SET) skip the name.
	peek := p.peek()
	isDatabaseOption := peek.Tp == 57382 || peek.Tp == 57381 ||
		peek.Tp == 57384 || peek.Tp == 57405 || peek.Tp == 57698 ||
		peek.Tp == 58054 || peek.Tp == 57541
	if !isDatabaseOption {
		nameTok := p.next()
		if !isIdentLike(nameTok.Tp) {
			p.error(nameTok.Offset, "")
			return nil
		}
		stmt.Name = ast.NewCIStr(nameTok.Lit)
	} else {
		stmt.AlterDefaultDatabase = true
	}
	stmt.Options = p.parseDatabaseOptions()
	if len(stmt.Options) == 0 {
		p.error(p.peek().Offset, "")
		return nil
	}
	return stmt
}

// parseDatabaseOptions parses database options like CHARSET, COLLATE, etc.
func (p *HandParser) parseDatabaseOptions() []*ast.DatabaseOption {
	var opts []*ast.DatabaseOption
	for {
		opt := &ast.DatabaseOption{}
		switch p.peek().Tp {
		case 57639, 57382, 57381:
			// yacc CharsetKw: CHARACTER SET | CHARSET | CHAR SET
			p.next()
			if p.peek().Tp == 57541 {
				p.next()
			}
			p.accept(58202)
			opt.Tp = ast.DatabaseOptionCharset
			rawCharset := p.next().Lit
			opt.Value = strings.ToLower(rawCharset)
			// Validate charset — empty check needed because ValidCharsetAndCollation
			// defaults empty to utf8
			if opt.Value == "" || !charset.ValidCharsetAndCollation(opt.Value, "") {
				p.errs = append(p.errs, fmt.Errorf("[parser:1115]Unknown character set: '%s'", rawCharset))
				return nil
			}
		case 57384:
			p.next()
			p.accept(58202)
			opt.Tp = ast.DatabaseOptionCollate
			rawCollation := p.next().Lit
			opt.Value = strings.ToLower(rawCollation)
			// Validate collation
			if _, err := charset.GetCollationByName(opt.Value); err != nil {
				p.errs = append(p.errs, fmt.Errorf("[ddl:1273]Unknown collation: '%s'", rawCollation))
				return nil
			}
		case 58054:
			p.next()
			p.accept(57838)
			// Accept both '=' and SET as separator (MySQL supports both)
			if _, ok := p.accept(58202); !ok {
				p.accept(57541)
			}
			opt.Tp = ast.DatabaseOptionPlacementPolicy
			tok := p.next()
			if tok.Tp == 57405 {
				opt.Value = "DEFAULT"
			} else if tok.Tp == 57353 {
				opt.Value = tok.Lit
			} else {
				opt.Value = tok.Lit
			}
		case 57698:
			p.next()
			p.accept(58202)
			opt.Tp = ast.DatabaseOptionEncryption
			valTok := p.peek()
			if valTok.Tp != 57353 {
				// ENCRYPTION requires a quoted string value
				return nil
			}
			opt.Value = p.next().Lit
			// Validate ENCRYPTION value — must be 'Y' or 'N'
			if !strings.EqualFold(opt.Value, "Y") && !strings.EqualFold(opt.Value, "N") {
				p.errs = append(p.errs, fmt.Errorf("[parser:1525]Incorrect argument (should be Y or N) value: '%s'", opt.Value))
				return nil
			}
		case 57541:
			// SET TIFLASH REPLICA count [LOCATION LABELS 'label1', 'label2', ...]
			if p.peekN(1).Tp != 58192 {
				return opts
			}
			p.next() // consume SET
			p.next() // consume TIFLASH
			p.expect(57865)
			opt.Tp = ast.DatabaseSetTiFlashReplica
			tiFlash := &ast.TiFlashReplicaSpec{}
			tiFlash.Count = p.parseUint64()
			if p.peek().IsKeyword("LOCATION") {
				p.next()
				if !(p.peek().IsKeyword("LABELS")) {
					p.error(p.peek().Offset, "expected LABELS after LOCATION")
					return nil
				}
				p.next()
				for {
					if tok, ok := p.expect(57353); ok {
						tiFlash.Labels = append(tiFlash.Labels, tok.Lit)
					}
					if _, ok := p.accept(','); !ok {
						break
					}
				}
			}
			opt.TiFlashReplica = tiFlash
		default:
			// Check for DEFAULT keyword before option
			if _, ok := p.accept(57405); ok {
				continue // re-loop after consuming DEFAULT
			}
			return opts
		}
		opts = append(opts, opt)
	}
}

// ---------------------------------------------------------------------------

// parseTSOValue parses a TSO integer value from an IntLit token.
// Returns (tso, true) on success, (0, false) on failure.
func (p *HandParser) parseTSOValue() (uint64, bool) {
	tok, ok := p.expect(58197)
	if !ok {
		return 0, false
	}
	var tso uint64
	if v, ok := tok.Item.(uint64); ok {
		tso = v
	} else if v, ok := tok.Item.(int64); ok {
		if v <= 0 {
			return 0, false
		}
		tso = uint64(v)
	}
	if tso == 0 {
		return 0, false
	}
	return tso, true
}

// ---------------------------------------------------------------------------
// FLASHBACK DATABASE
// ---------------------------------------------------------------------------

// parseFlashbackStmt parses all FLASHBACK forms:
//   - FLASHBACK CLUSTER TO TIMESTAMP 'str'
//   - FLASHBACK TABLE tablelist TO TIMESTAMP 'str'
//   - FLASHBACK DATABASE/SCHEMA name TO TIMESTAMP 'str'
//   - FLASHBACK TABLE tablename [TO newname]
//   - FLASHBACK DATABASE/SCHEMA name [TO newname]
func (p *HandParser) parseFlashbackStmt() ast.StmtNode {
	p.expect(58021)

	switch p.peek().Tp {
	case 57648:
		// FLASHBACK CLUSTER TO TIMESTAMP 'str' | FLASHBACK CLUSTER TO TSO num
		p.next()
		tsStmt := p.parseFlashbackWithTS()
		if tsStmt == nil {
			if len(p.errs) == 0 {
				p.syntaxError(p.peek().Offset)
			}
			return nil
		}
		return tsStmt
	case 57556:
		p.next()
		// Parse table name(s)
		tables := p.parseTableNameList()
		if tables == nil {
			return nil // No table name → parse failure (e.g., "flashback table to timestamp ...")
		}
		// Check if next is TO TIMESTAMP or TO TSO — then it's FlashbackToTimestampStmt
		// NOTE: toTimestamp is a compound token that fuses "TO TIMESTAMP". But "flashback table t TO timestamp"
		// should parse as FlashBackTableStmt with NewName=timestamp, NOT as FlashbackToTimestampStmt.
		// So we peek past toTimestamp to check if a string literal follows.
		// Check if next is TO TIMESTAMP or TO TSO — then it's FlashbackToTimestampStmt
		if tsStmt := p.parseFlashbackWithTS(); tsStmt != nil {
			tsStmt.Tables = tables
			return tsStmt
		}
		if len(p.errs) > 0 {
			return nil
		}
		// Otherwise FlashBackTableStmt: FLASHBACK TABLE tablename [TO newname]
		tblStmt := Alloc[ast.FlashBackTableStmt](p.arena)
		if len(tables) > 0 {
			tblStmt.Table = tables[0]
		}
		// Handle compound 57348 without string literal → treat as TO + identifier "timestamp"
		if _, ok := p.accept(57348); ok {
			tblStmt.NewName = "timestamp"
		} else if _, ok := p.accept(57564); ok {
			tblStmt.NewName = p.next().Lit
		}
		return tblStmt
	case 57398: /* SCHEMA is same token */
		p.next()
		// Parse database name
		nameTok := p.next()
		if !isIdentLike(nameTok.Tp) {
			return nil // No valid database name
		}
		dbName := ast.NewCIStr(nameTok.Lit)
		// Check if next is TO TIMESTAMP or TO TSO
		if tsStmt := p.parseFlashbackWithTS(); tsStmt != nil {
			tsStmt.DBName = dbName
			return tsStmt
		}
		if len(p.errs) > 0 {
			return nil
		}
		// FlashBackDatabaseStmt: FLASHBACK DATABASE name [TO newname]
		stmt := Alloc[ast.FlashBackDatabaseStmt](p.arena)
		stmt.DBName = dbName
		if _, ok := p.accept(57564); ok {
			stmt.NewName = p.next().Lit
		}
		return stmt
	default:
		p.syntaxError(p.peek().Offset)
		return nil
	}
}

func (p *HandParser) parseFlashbackWithTS() *ast.FlashBackToTimestampStmt {
	if p.peek().Tp == 57348 && p.peekN(1).Tp == 57353 {
		p.next() // consume toTimestamp
		tsStmt := Alloc[ast.FlashBackToTimestampStmt](p.arena)
		if tok, ok := p.expect(57353); ok {
			tsStmt.FlashbackTS = ast.NewValueExpr(tok.Lit, "", "")
		}
		return tsStmt
	} else if p.peek().Tp == 57349 {
		p.next()
		tso, ok := p.parseTSOValue()
		if !ok {
			p.error(p.peek().Offset, "expected positive integer for TSO value")
			return nil
		}
		tsStmt := Alloc[ast.FlashBackToTimestampStmt](p.arena)
		tsStmt.FlashbackTSO = tso
		return tsStmt
	}
	return nil
}

// ---------------------------------------------------------------------------
// CREATE VIEW
// ---------------------------------------------------------------------------

// parseCreateViewStmt parses:
//
//	CREATE [OR REPLACE] [ALGORITHM = {UNDEFINED|MERGE|TEMPTABLE}]
//	  [DEFINER = user] [SQL SECURITY {DEFINER|INVOKER}]
//	  VIEW name [(col_list)] AS select_stmt
func (p *HandParser) parseCreateViewStmt() ast.StmtNode {
	p.expect(57389)
	stmt := Alloc[ast.CreateViewStmt](p.arena)

	// Set defaults (Algorithm=UNDEFINED and Security=DEFINER are zero values).
	// CheckOption=CASCADED suppresses "WITH LOCAL CHECK OPTION" in Restore.
	definer := Alloc[auth.UserIdentity](p.arena)
	definer.CurrentUser = true
	stmt.Definer = definer
	stmt.CheckOption = ast.CheckOptionCascaded

	// [OR REPLACE]
	if _, ok := p.accept(57509); ok {
		p.expect(57530)
		stmt.OrReplace = true
	}

	// [ALGORITHM = {UNDEFINED|MERGE|TEMPTABLE}]
	if _, ok := p.accept(57603); ok {
		p.expect(58202)
		switch p.peek().Tp {
		case 57785:
			p.next()
			stmt.Algorithm = ast.AlgorithmMerge
		case 57948:
			p.next()
			stmt.Algorithm = ast.AlgorithmTemptable
		default:
			// UNDEFINED or any other value
			p.next()
			stmt.Algorithm = ast.AlgorithmUndefined
		}
	}

	// [DEFINER = user]
	if _, ok := p.accept(57685); ok {
		p.expect(58202)
		if _, ok := p.accept(57396); ok {
			// already set to CurrentUser — DEFINER = CURRENT_USER
			if _, ok := p.accept('('); ok {
				p.accept(')')
			}
		} else {
			stmt.Definer = p.parseUserIdentity()
		}
	}

	// [SQL SECURITY {DEFINER|INVOKER}]
	if _, ok := p.accept(57545); ok {
		if _, ok := p.accept(57893); ok {
			if _, ok := p.accept(57754); ok {
				stmt.Security = ast.SecurityInvoker
			} else {
				// DEFINER or any other value → accept the token
				p.accept(57685)
				// default is DEFINER (zero value)
			}
		}
	}

	p.expect(57980)

	// View name
	stmt.ViewName = p.parseTableName()

	// Optional column list: (col1, col2, ...)
	if _, ok := p.accept('('); ok {
		for {
			if tok, ok := p.expectAny(57346); ok {
				stmt.Cols = append(stmt.Cols, ast.NewCIStr(tok.Lit))
			}
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}

	p.expect(57369)

	// Record the start offset of the select body (right after AS keyword).
	selectStartOff := p.peek().Offset

	// AS select_stmt (SELECT, TABLE, VALUES, WITH, or parenthesized)
	switch p.peek().Tp {
	case 57540:
		sel := p.parseSelectStmt()
		stmt.Select = p.maybeParseUnion(sel).(ast.StmtNode)
	case 57590:
		// CTE: WITH ... SELECT ... — call the CTE parser directly.
		if withNode := p.parseWithStmt(); withNode != nil {
			stmt.Select = withNode
		}
	case 57556:
		stmt.Select = p.parseTableStmt()
	case 57580:
		stmt.Select = p.parseValuesStmt()
	case '(':
		// (SELECT ...) or (TABLE t) or (VALUES ...)
		p.next() // consume '('
		var inner ast.ResultSetNode
		switch p.peek().Tp {
		case 57556:
			inner = p.parseTableStmt().(*ast.SelectStmt)
		case 57580:
			inner = p.parseValuesStmt().(*ast.SelectStmt)
		default:
			inner = p.parseSelectStmt()
		}
		// Handle UNION/EXCEPT/INTERSECT inside parentheses.
		inner = p.maybeParseUnion(inner)
		p.expect(')')
		if inner != nil {
			switch n := inner.(type) {
			case *ast.SelectStmt:
				n.IsInBraces = true
			case *ast.SetOprStmt:
				n.IsInBraces = true
			}
			stmt.Select = inner.(ast.StmtNode)
		}
	default:
		stmt.Select = p.parseSelectStmt()
	}

	// Compute end offset BEFORE parsing optional WITH CHECK OPTION.
	// This is the end of the select body text.
	selectEndOff := p.peek().Offset

	// [WITH [LOCAL|CASCADED] CHECK OPTION]
	if _, ok := p.accept(57590); ok {
		// LOCAL
		if _, ok := p.accept(57770); ok {
			stmt.CheckOption = ast.CheckOptionLocal
		}
		// CASCADED is the default — accept it but don't change.
		p.accept(57636)
		// CHECK OPTION
		p.expect(57383)
		p.accept(57507)
	}

	// Set text on the select body node, mirroring the original parser.
	if stmt.Select != nil {
		stmt.Select.SetText(nil, strings.TrimSpace(p.src[selectStartOff:selectEndOff]))
	}

	return stmt
}

// ---------------------------------------------------------------------------
// PREPARE / EXECUTE / DEALLOCATE
// ---------------------------------------------------------------------------

// parsePrepareStmt parses: PREPARE stmt_name FROM preparable_stmt
func (p *HandParser) parsePrepareStmt() ast.StmtNode {
	stmt := Alloc[ast.PrepareStmt](p.arena)
	p.expect(57840)
	stmt.Name = p.parseName()
	p.expect(57434)

	if tok, ok := p.accept(57353); ok {
		stmt.SQLText = tok.Lit
	} else {
		// Must be user variable @var
		expr := p.parseVariableExpr()
		if v, ok := expr.(*ast.VariableExpr); ok {
			stmt.SQLVar = v
		} else {
			p.error(p.peek().Offset, "expected string literal or user variable for PREPARE")
			return nil
		}
	}
	return stmt
}

// parseExecuteStmt parses: EXECUTE stmt_name [USING @var_name [, @var_name] ...]
func (p *HandParser) parseExecuteStmt() ast.StmtNode {
	stmt := Alloc[ast.ExecuteStmt](p.arena)
	p.expect(57715)
	stmt.Name = p.parseName()

	if _, ok := p.accept(57576); ok {
		for {
			expr := p.parseVariableExpr()
			stmt.UsingVars = append(stmt.UsingVars, expr)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}
	return stmt
}

// parseDeallocateStmt parses: {DEALLOCATE | DROP} PREPARE stmt_name
func (p *HandParser) parseDeallocateStmt() ast.StmtNode {
	stmt := Alloc[ast.DeallocateStmt](p.arena)
	// Consumes DEALLOCATE if called from dispatch, or DROP if called from parseDropStmt?
	// If called from dispatch (57683), we expect 57683.
	// If called from parseDropStmt (57415 + 57840), we expect 57840 (Drop consumed).
	// But here I name it parseDeallocateStmt and use p.expect(57683).
	// Only for distinct DEALLOCATE statement.
	// For DROP PREPARE, I'll need a different path or logic.
	p.expect(57683)
	p.accept(57840)
	stmt.Name = p.parseName()
	return stmt
}

// parseName parses a simple identifier name.
func (p *HandParser) parseName() string {
	tok := p.next()
	if tok.Tp < 57346 && tok.Tp != 57352 {
		p.error(tok.Offset, "expected identifier")
		return ""
	}
	return tok.Lit
}
