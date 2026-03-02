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
	"github.com/pingcap/tidb/pkg/parser/charset"
)

// ---------------------------------------------------------------------------
// CREATE INDEX
// ---------------------------------------------------------------------------

// parseCreateIndexStmt parses:
//
//	CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX [IF NOT EXISTS] idx ON tbl (col, ...) [options]
func (p *HandParser) parseCreateIndexStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateIndexStmt](p.arena)
	p.expect(create)

	// [UNIQUE | FULLTEXT | SPATIAL | VECTOR | COLUMNAR]
	switch p.peek().Tp {
	case unique:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeUnique
	case fulltext:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeFulltext
	case spatial:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeSpatial
	case vectorType:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeVector
	case columnar:
		p.next()
		stmt.KeyType = ast.IndexKeyTypeColumnar
	}

	p.expect(index)

	// [IF NOT EXISTS]
	stmt.IfNotExists = p.acceptIfNotExists()

	// index_name
	if tok := p.peek(); isIdentLike(tok.Tp) {
		stmt.IndexName = p.next().Lit
	}

	// Optional USING/TYPE before ON (IndexNameAndTypeOpt)
	if preOpt := p.parseOptionalUsingIndexType(); preOpt != nil {
		stmt.IndexOption = preOpt
	}

	// ON table_name
	p.expect(on)
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

	for range 2 {
		if _, ok := p.accept(lock); ok {
			found = true
			p.accept(eq)
			tok := p.next()
			if tok.Tp == defaultKwd {
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
					p.errs = append(p.errs, ErrUnknownAlterLock.GenWithStackByArgs(tok.Lit))
					return nil
				}
			}
		} else if _, ok := p.accept(algorithm); ok {
			found = true
			p.accept(eq)
			tok := p.next()
			if tok.Tp == defaultKwd {
				algTp = ast.AlgorithmTypeDefault
			} else {
				switch tok.Tp {
				case copyKwd:
					algTp = ast.AlgorithmTypeCopy
				case inplace:
					algTp = ast.AlgorithmTypeInplace
				case instant:
					algTp = ast.AlgorithmTypeInstant
				default:
					p.errs = append(p.errs, ErrUnknownAlterAlgorithm.GenWithStackByArgs(tok.Lit))
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
	p.expect(tableKwd)

	// BY JOB int64
	if _, ok := p.accept(by); ok {
		p.expect(job)
		v, ok := p.parseInt64()
		if !ok {
			return nil
		}
		stmt.JobID = v
		return stmt
	}

	// tablename [int64]
	stmt.Table = p.parseTableName()
	if tok, ok := p.acceptAny(intLit); ok {
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
	p.expect(create)
	p.expect(database)

	stmt.IfNotExists = p.acceptIfNotExists()

	nameTok := p.next()
	if !isIdentLike(nameTok.Tp) {
		p.syntaxErrorAt(nameTok)
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
	p.expect(alter)
	p.expect(database)

	// Database name is optional — if next token is a db option keyword, skip name.
	// Note: charsetKwd (CHARSET) is NOT included here because in the parser,
	// standalone CHARSET is consumed as the db name (shift/reduce preference).
	// Only multi-word option starts (CHARACTER SET, CHAR SET) skip the name.
	peek := p.peek()
	isDatabaseOption := peek.Tp == character || peek.Tp == charType ||
		peek.Tp == collate || peek.Tp == defaultKwd || peek.Tp == encryption ||
		peek.Tp == placement || peek.Tp == set
	if !isDatabaseOption {
		nameTok := p.next()
		if !isIdentLike(nameTok.Tp) {
			p.syntaxErrorAt(nameTok)
			return nil
		}
		stmt.Name = ast.NewCIStr(nameTok.Lit)
	} else {
		stmt.AlterDefaultDatabase = true
	}
	stmt.Options = p.parseDatabaseOptions()
	if len(stmt.Options) == 0 {
		p.syntaxErrorAt(p.peek())
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
		case charsetKwd, character, charType:
			// yacc CharsetKw: CHARACTER SET | CHARSET | CHAR SET
			p.next()
			if p.peek().Tp == set {
				p.next()
			}
			p.accept(eq)
			opt.Tp = ast.DatabaseOptionCharset
			// yacc CharsetName → StringName | binaryType
			// Reserved keywords like DEFAULT are not valid charset names.
			csTok := p.next()
			if !isIdentLike(csTok.Tp) && csTok.Tp != binaryType {
				p.syntaxErrorAt(csTok)
				return nil
			}
			rawCharset := csTok.Lit
			cs, err := charset.GetCharsetInfo(rawCharset)
			if err != nil || cs.Name == "" {
				p.errs = append(p.errs, ErrUnknownCharacterSet.GenWithStackByArgs(rawCharset))
				return nil
			}
			opt.Value = cs.Name
		case collate:
			p.next()
			p.accept(eq)
			opt.Tp = ast.DatabaseOptionCollate
			// yacc CollationName → StringName | binaryType
			collTok := p.next()
			if !isIdentLike(collTok.Tp) && collTok.Tp != binaryType {
				p.syntaxErrorAt(collTok)
				return nil
			}
			rawCollation := collTok.Lit
			coll, err := charset.GetCollationByName(rawCollation)
			if err != nil {
				p.errs = append(p.errs, err)
				return nil
			}
			opt.Value = coll.Name
		case placement:
			p.next()
			p.accept(policy)
			opt.Tp = ast.DatabaseOptionPlacementPolicy
			// yacc: PLACEMENT POLICY SET DEFAULT | PLACEMENT POLICY EqOpt (stringLit | PolicyName | DEFAULT)
			if p.peek().Tp == set && p.peekN(1).Tp == defaultKwd {
				p.next() // SET
				p.next() // DEFAULT
				opt.Value = "DEFAULT"
			} else {
				p.accept(eq)
				tok := p.next()
				if tok.Tp == defaultKwd {
					opt.Value = "DEFAULT"
				} else {
					opt.Value = tok.Lit
				}
			}
		case encryption:
			p.next()
			p.accept(eq)
			opt.Tp = ast.DatabaseOptionEncryption
			valTok := p.peek()
			if valTok.Tp != stringLit {
				// ENCRYPTION requires a quoted string value
				return nil
			}
			opt.Value = p.next().Lit
			// Validate ENCRYPTION value — must be 'Y' or 'N'
			if !strings.EqualFold(opt.Value, "Y") && !strings.EqualFold(opt.Value, "N") {
				p.errs = append(p.errs, ErrWrongValue.GenWithStackByArgs("argument (should be Y or N)", opt.Value))
				return nil
			}
		case set:
			// SET TIFLASH REPLICA count [LOCATION LABELS 'label1', 'label2', ...]
			if p.peekN(1).Tp != tiFlash {
				return opts
			}
			p.next() // consume SET
			p.next() // consume TIFLASH
			p.expect(replica)
			opt.Tp = ast.DatabaseSetTiFlashReplica
			tiFlash := &ast.TiFlashReplicaSpec{}
			tiFlash.Count = p.parseUint64()
			if p.peek().IsKeyword("LOCATION") {
				p.next()
				if !(p.peek().IsKeyword("LABELS")) {
					p.syntaxErrorAt(p.peek())
					return nil
				}
				p.next()
				for {
					if tok, ok := p.expect(stringLit); ok {
						tiFlash.Labels = append(tiFlash.Labels, tok.Lit)
					}
					if _, ok := p.accept(','); !ok {
						break
					}
				}
			}
			opt.TiFlashReplica = tiFlash
		default:
			// yacc DefaultKwdOpt: DEFAULT is only valid before CharsetKw, COLLATE,
			// ENCRYPTION, or PLACEMENT POLICY in database options.
			if p.peek().Tp == defaultKwd {
				next := p.peekN(1).Tp
				if next == charsetKwd || next == character || next == charType ||
					next == collate || next == encryption || next == placement {
					p.next() // consume DEFAULT
					continue // re-loop
				}
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
	tok, ok := p.expect(intLit)
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
	p.expect(flashback)

	switch p.peek().Tp {
	case cluster:
		// FLASHBACK CLUSTER TO TIMESTAMP 'str' | FLASHBACK CLUSTER TO TSO num
		p.next()
		tsStmt := p.parseFlashbackWithTS()
		if tsStmt == nil {
			if len(p.errs) == 0 {
				p.syntaxErrorAt(p.peek())
			}
			return nil
		}
		return tsStmt
	case tableKwd:
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
		// Handle compound toTimestamp without string literal → treat as TO + identifier "timestamp"
		if _, ok := p.accept(toTimestamp); ok {
			tblStmt.NewName = "timestamp"
		} else if _, ok := p.accept(to); ok {
			tblStmt.NewName = p.next().Lit
		}
		return tblStmt
	case database: /* SCHEMA is same token */
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
		if _, ok := p.accept(to); ok {
			stmt.NewName = p.next().Lit
		}
		return stmt
	default:
		p.syntaxErrorAt(p.peek())
		return nil
	}
}

func (p *HandParser) parseFlashbackWithTS() *ast.FlashBackToTimestampStmt {
	if p.peek().Tp == toTimestamp && p.peekN(1).Tp == stringLit {
		p.next() // consume toTimestamp
		tsStmt := Alloc[ast.FlashBackToTimestampStmt](p.arena)
		if tok, ok := p.expect(stringLit); ok {
			tsStmt.FlashbackTS = ast.NewValueExpr(tok.Lit, "", "")
		}
		return tsStmt
	} else if p.peek().Tp == toTSO {
		p.next()
		tso, ok := p.parseTSOValue()
		if !ok {
			p.syntaxErrorAt(p.peek())
			return nil
		}
		tsStmt := Alloc[ast.FlashBackToTimestampStmt](p.arena)
		tsStmt.FlashbackTSO = tso
		return tsStmt
	}
	return nil
}
