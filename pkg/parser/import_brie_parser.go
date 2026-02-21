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

// parseImportIntoStmt parses:
//
//	IMPORT INTO table [(cols)] [SET ...] FROM 'path' [FORMAT 'fmt'] [WITH opts]
//	IMPORT INTO table [(cols)] [SET ...] FROM SELECT ...
//	IMPORT INTO table [(cols)] [SET ...] FROM (SELECT ...)
func (p *HandParser) parseImportIntoStmt() ast.StmtNode {
	stmt := &ast.ImportIntoStmt{}

	p.expect(57746)
	p.expect(57463)

	stmt.Table = p.parseTableName()
	if stmt.Table == nil {
		return nil
	}

	// Optional column list: (col1, @var1, col2)
	if _, ok := p.accept('('); ok {
		stmt.ColumnsAndUserVars = make([]*ast.ColumnNameOrUserVar, 0)
		for {
			if tok, ok := p.accept(57354); ok {
				// @varname — singleAtIdentifier contains "@name", strip leading @.
				varName := tok.Lit
				if len(varName) > 0 && varName[0] == '@' {
					varName = varName[1:]
				}
				node := &ast.ColumnNameOrUserVar{UserVar: &ast.VariableExpr{Name: varName}}
				stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
			} else if tok, ok := p.accept(57346); ok {
				node := &ast.ColumnNameOrUserVar{ColumnName: &ast.ColumnName{Name: ast.NewCIStr(tok.Lit)}}
				stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
			} else {
				break
			}
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}

	// Optional SET assignments.
	if _, ok := p.accept(57541); ok {
		stmt.ColumnAssignments = make([]*ast.Assignment, 0)
		for {
			col := p.parseColumnName()
			if col == nil {
				break
			}
			p.expectAny(58202, 58201)
			expr := p.parseExpression(precNone)
			stmt.ColumnAssignments = append(stmt.ColumnAssignments, &ast.Assignment{Column: col, Expr: expr})
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	// FROM clause.
	p.expect(57434)

	// Check: FROM 'path' | FROM SELECT | FROM (SELECT) | FROM WITH CTE SELECT
	if p.peek().Tp == 57353 {
		// FROM 'path'
		pathTok := p.next()
		stmt.Path = pathTok.Lit

		// Optional FORMAT 'fmt'
		if _, ok := p.accept(57728); ok {
			if tok, ok := p.expect(57353); ok {
				stmt.Format = sptr(tok.Lit)
			}
		}
	} else if p.peek().Tp == '(' {
		// FROM (SELECT ...)
		p.next() // consume '('
		sel := p.parseSelectStmt()
		if sel == nil {
			return nil
		}
		// Handle UNION inside parens and wrap in TableSource to preserve parens in Restore
		unionSel := p.maybeParseUnion(sel)
		ts := Alloc[ast.TableSource](p.arena)
		ts.Source = unionSel
		stmt.Select = ts
		p.expect(')')
	} else if p.peek().Tp == 57540 || p.peek().Tp == 57590 {
		// FROM SELECT ... or FROM WITH CTE SELECT ...
		// Validate: no user vars or SET in SELECT source.
		if len(stmt.ColumnsAndUserVars) > 0 {
			for _, cuv := range stmt.ColumnsAndUserVars {
				if cuv.UserVar != nil {
					p.error(p.peek().Offset, "Cannot use user variable(%s) in IMPORT INTO FROM SELECT statement", cuv.UserVar.Name)
					return nil
				}
			}
		}
		if len(stmt.ColumnAssignments) > 0 {
			p.error(p.peek().Offset, "Cannot use SET clause in IMPORT INTO FROM SELECT statement.")
			return nil
		}

		if p.peek().Tp == 57590 {
			// WITH CTE form: WITH name AS (SELECT ...) SELECT ...
			// Consume the CTE clause, then parse the trailing SELECT.
			withTok := p.next() // consume WITH
			// Parse CTE name
			cteName := p.next()
			p.expect(57369)
			p.expect('(')
			// Parse inner select
			cteSelect := p.parseSelectStmt()
			p.expect(')')
			// Now parse the main SELECT
			mainSelect := p.parseSelectStmt()
			if mainSelect == nil {
				return nil
			}
			// Attach CTE to the main select
			cte := &ast.CommonTableExpression{
				Name:  ast.NewCIStr(cteName.Lit),
				Query: &ast.SubqueryExpr{Query: cteSelect},
			}
			mainSelect.With = &ast.WithClause{CTEs: []*ast.CommonTableExpression{cte}}
			_ = withTok
			stmt.Select = p.maybeParseUnion(mainSelect)
		} else {
			sel := p.parseSelectStmt()
			if sel == nil {
				return nil
			}
			// Try UNION/EXCEPT/INTERSECT
			stmt.Select = p.maybeParseUnion(sel)
		}
	}

	// Optional WITH options.
	if _, ok := p.accept(57590); ok {
		stmt.Options = make([]*ast.LoadDataOpt, 0)
		for {
			optTok := p.next()
			opt := &ast.LoadDataOpt{Name: strings.ToLower(optTok.Lit)}
			if p.acceptEqOrAssign() {
				opt.Value = p.parseExpression(precNone)
			}
			stmt.Options = append(stmt.Options, opt)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	return stmt
}

// parseCancelStmt parses:
//
//	CANCEL IMPORT JOB <id>
//	CANCEL DISTRIBUTION JOB <id>
func (p *HandParser) parseCancelStmt() ast.StmtNode {
	p.expect(58153)

	switch p.peek().Tp {
	case 57746:
		p.next()
		p.expect(58166)
		stmt := &ast.ImportIntoActionStmt{
			Tp:    ast.ImportIntoCancel,
			JobID: int64(p.parseUint64()),
		}
		return stmt
	case 58162:
		p.next()
		p.expect(58166)
		stmt := &ast.CancelDistributionJobStmt{
			JobID: int64(p.parseUint64()),
		}
		return stmt
	case 58107:
		return p.parseCancelTrafficStmt()
	case 58000:
		p.next() // consume BR
		return p.parseCancelBRJobStmt()
	default:
		p.error(p.peek().Offset, "expected IMPORT or DISTRIBUTION after CANCEL")
		return nil
	}
}

// parseShowImportStmt parses SHOW IMPORT variants. SHOW has already been consumed.
func (p *HandParser) parseShowImportStmt() ast.StmtNode {
	p.expect(57746)

	switch p.peek().Tp {
	case 58167:
		p.next()
		stmt := &ast.ShowStmt{Tp: ast.ShowImportJobs}
		if _, ok := p.accept(57587); ok {
			stmt.Where = p.parseExpression(precNone)
		}
		return stmt
	case 58166:
		p.next()
		stmt := &ast.ShowStmt{Tp: ast.ShowImportJobs}
		jobID := int64(p.parseUint64())
		stmt.ImportJobID = &jobID
		return stmt
	case 57439:
		p.next()
		return &ast.ShowStmt{Tp: ast.ShowImportGroups}
	case 57438:
		p.next()
		stmt := &ast.ShowStmt{Tp: ast.ShowImportGroups}
		if tok, ok := p.expect(57353); ok {
			stmt.ShowGroupKey = tok.Lit
		}
		return stmt
	}

	return nil
}

// parseBRIEStmt parses RESTORE and BACKUP statements.
//
//	RESTORE DATABASE {* | db1[,db2...]} FROM 'path' [opts]
//	BACKUP DATABASE {* | db1[,db2...]} TO 'path' [opts]
func (p *HandParser) parseBRIEStmt() ast.StmtNode {
	stmt := &ast.BRIEStmt{}

	tok := p.next() // RESTORE or BACKUP
	switch tok.Tp {
	case 57872:
		stmt.Kind = ast.BRIEKindRestore
	case 57618:
		stmt.Kind = ast.BRIEKindBackup
	}

	// Check for special sub-statement types
	nextLit := strings.ToUpper(p.peek().Lit)

	// BACKUP LOGS TO ... → StreamStart
	if stmt.Kind == ast.BRIEKindBackup && nextLit == "LOGS" {
		p.next() // consume LOGS
		stmt.Kind = ast.BRIEKindStreamStart
		// TO 'path'
		if tok := p.peek(); tok.IsKeyword("TO") {
			p.next()
			if tok, ok := p.expect(57353); ok {
				stmt.Storage = tok.Lit
			}
		}
		p.parseBRIEOptions(stmt)
		return stmt
	}

	// RESTORE POINT FROM ... → RestorePIT
	if stmt.Kind == ast.BRIEKindRestore && nextLit == "POINT" {
		p.next() // consume POINT
		stmt.Kind = ast.BRIEKindRestorePIT
		if _, ok := p.accept(57434); ok {
			if tok, ok := p.expect(57353); ok {
				stmt.Storage = tok.Lit
			}
		}
		p.parseBRIEOptions(stmt)
		return stmt
	}

	// DATABASE, SCHEMA, or TABLE target
	if _, ok := p.accept(57398); ok {
		p.parseBRIEDatabaseList(stmt)
	} else if tok := p.peek(); tok.IsKeyword("SCHEMA") {
		p.next() // consume SCHEMA (alias for DATABASE)
		p.parseBRIEDatabaseList(stmt)
	} else if _, ok := p.accept(57556); ok {
		for {
			tn := p.parseTableName()
			if tn == nil {
				break
			}
			stmt.Tables = append(stmt.Tables, tn)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	} else {
		// Missing DATABASE/TABLE/LOGS → error
		p.error(p.peek().Offset, "expected DATABASE, TABLE, LOGS, or POINT after BACKUP/RESTORE")
		return nil
	}

	// Validate: database names must not contain '.' (e.g., "BACKUP DATABASE a.b" is invalid)
	for _, schema := range stmt.Schemas {
		if strings.Contains(schema, ".") {
			p.error(tok.Offset, "invalid database name: %s", schema)
			return nil
		}
	}

	// FROM / TO
	if _, ok := p.accept(57434); ok {
		if tok, ok := p.expect(57353); ok {
			stmt.Storage = tok.Lit
		}
	} else if tok := p.peek(); tok.IsKeyword("TO") {
		p.next()
		if tok, ok := p.expect(57353); ok {
			stmt.Storage = tok.Lit
		}
	}

	p.parseBRIEOptions(stmt)
	return stmt
}

// parseBRIEDatabaseList parses the database list for BACKUP/RESTORE DATABASE.
func (p *HandParser) parseBRIEDatabaseList(stmt *ast.BRIEStmt) {
	if _, ok := p.accept('*'); ok {
		// All databases — no schemas stored.
		return
	}
	for {
		tok := p.next()
		if tok.Tp == '*' {
			p.error(tok.Offset, "wildcard '*' cannot be mixed with named databases")
			return
		}
		name := tok.Lit
		// Check for schema.table format which is invalid
		if p.peek().Tp == '.' {
			p.next() // consume '.'
			name += "." + p.next().Lit
		}
		stmt.Schemas = append(stmt.Schemas, name)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
}

// parseBRIEOptions parses BRIE options: key[=]value pairs.
func (p *HandParser) parseBRIEOptions(stmt *ast.BRIEStmt) {
	for {
		if p.peek().Tp == 0 || p.peek().Tp == ';' {
			break
		}
		optTok := p.next()
		optName := strings.ToUpper(optTok.Lit)
		p.accept(58202) // optional '=' separator

		optType := brieOptionNameToType(optName)
		opt := &ast.BRIEOption{Tp: optType}

		// Value: try string literal first, then keyword level/boolean, then integer.
		if tok, ok := p.accept(57353); ok {
			opt.StrValue = tok.Lit
		} else if brieLevel, ok := parseBRIELevelValue(p); ok {
			opt.UintValue = uint64(brieLevel)
		} else {
			opt.UintValue = p.parseUint64()

			// Handle special value suffixes.
			switch optType {
			case ast.BRIEOptionRateLimit:
				// rate_limit 500 MB/second → UintValue = 500 * 1048576
				if tok := p.peek(); tok.IsKeyword("MB") {
					p.next()      // consume MB
					p.accept('/') // consume /
					p.next()      // consume SECOND
					opt.UintValue *= 1048576
				}
			case ast.BRIEOptionBackupTS, ast.BRIEOptionLastBackupTS:
				// Check for time-ago expression: N MINUTE/SECOND/HOUR AGO
				if tok := p.peek(); isTimeUnit(tok.Lit) {
					p.next() // consume time unit
					unit := strings.ToUpper(tok.Lit)
					// Check for optional AGO
					if ago := p.peek(); ago.IsKeyword("AGO") {
						p.next() // consume AGO
					}
					// Convert to microseconds
					opt.UintValue = opt.UintValue * timeUnitToMicroseconds(unit) * 1000
					opt.Tp = ast.BRIEOptionBackupTimeAgo
				} else {
					// Pure integer → TSO type
					if optType == ast.BRIEOptionBackupTS {
						opt.Tp = ast.BRIEOptionBackupTSO
					} else {
						opt.Tp = ast.BRIEOptionLastBackupTSO
					}
				}
			}
		}

		stmt.Options = append(stmt.Options, opt)
	}
}

// parseBRIEStreamStmt parses: {PAUSE|RESUME|STOP} BACKUP LOGS [options]
// The leading keyword (PAUSE/RESUME/STOP) must already be consumed.
func (p *HandParser) parseBRIEStreamStmt(kind ast.BRIEKind) ast.StmtNode {
	p.expect(57618)
	p.next() // consume LOGS
	stmt := &ast.BRIEStmt{Kind: kind}
	p.parseBRIEOptions(stmt)
	return stmt
}

// parsePauseBackupLogsStmt parses: PAUSE BACKUP LOGS [options]
func (p *HandParser) parsePauseBackupLogsStmt() ast.StmtNode {
	return p.parseBRIEStreamStmt(ast.BRIEKindStreamPause)
}

// parseResumeBackupLogsStmt parses: RESUME BACKUP LOGS
func (p *HandParser) parseResumeBackupLogsStmt() ast.StmtNode {
	return p.parseBRIEStreamStmt(ast.BRIEKindStreamResume)
}

// parseStopBackupLogsStmt parses: STOP BACKUP LOGS
func (p *HandParser) parseStopBackupLogsStmt() ast.StmtNode {
	return p.parseBRIEStreamStmt(ast.BRIEKindStreamStop)
}

// parsePurgeBackupLogsStmt parses: PURGE BACKUP LOGS FROM 'path' [options]
func (p *HandParser) parsePurgeBackupLogsStmt() ast.StmtNode {
	// PURGE already consumed by caller
	p.expect(57618)
	// consume LOGS
	p.next()
	stmt := &ast.BRIEStmt{Kind: ast.BRIEKindStreamPurge}
	if _, ok := p.accept(57434); ok {
		if tok, ok := p.expect(57353); ok {
			stmt.Storage = tok.Lit
		}
	}
	p.parseBRIEOptions(stmt)
	return stmt
}

// parseShowBRJobStmt parses: SHOW BR JOB [QUERY] <id>
func (p *HandParser) parseShowBRJobStmt() ast.StmtNode {
	// SHOW already consumed, BR consumed by caller
	p.expect(58166)
	stmt := &ast.BRIEStmt{Kind: ast.BRIEKindShowJob}
	if tok := p.peek(); tok.IsKeyword("QUERY") {
		p.next()
		stmt.Kind = ast.BRIEKindShowQuery
	}
	stmt.JobID = int64(p.parseUint64())
	return stmt
}

// parseCancelBRJobStmt parses: CANCEL BR JOB <id>
func (p *HandParser) parseCancelBRJobStmt() ast.StmtNode {
	// CANCEL already consumed, BR consumed by caller
	p.expect(58166)
	stmt := &ast.BRIEStmt{Kind: ast.BRIEKindCancelJob}
	stmt.JobID = int64(p.parseUint64())
	return stmt
}

// parseShowBackupLogsStmt parses: SHOW BACKUP LOGS STATUS | SHOW BACKUP LOGS METADATA FROM 'path'
// Also: SHOW BACKUP METADATA FROM 'path'
func (p *HandParser) parseShowBackupLogsStmt() ast.StmtNode {
	// SHOW already consumed, BACKUP consumed by caller
	nextLit := strings.ToUpper(p.peek().Lit)
	if nextLit == "LOGS" {
		p.next() // consume LOGS
		statusOrMeta := strings.ToUpper(p.peek().Lit)
		if statusOrMeta == "STATUS" {
			p.next()
			return &ast.BRIEStmt{Kind: ast.BRIEKindStreamStatus}
		} else if statusOrMeta == "METADATA" {
			p.next()
			return p.parseBRIEMetadataStmt(ast.BRIEKindStreamMetaData)
		}
	} else if nextLit == "METADATA" {
		p.next() // consume METADATA
		return p.parseBRIEMetadataStmt(ast.BRIEKindShowBackupMeta)
	}
	return nil
}

// parseBRIEMetadataStmt parses: [FROM 'path'] for SHOW BACKUP [LOGS] METADATA.
func (p *HandParser) parseBRIEMetadataStmt(kind ast.BRIEKind) ast.StmtNode {
	stmt := &ast.BRIEStmt{Kind: kind}
	if _, ok := p.accept(57434); ok {
		if tok, ok := p.expect(57353); ok {
			stmt.Storage = tok.Lit
		}
	}
	return stmt
}

// brieOptionNameToType maps BRIE option keyword names to their BRIEOptionType.
func brieOptionNameToType(name string) ast.BRIEOptionType {
	switch name {
	case "SNAPSHOT":
		return ast.BRIEOptionBackupTS
	case "LAST_BACKUP":
		return ast.BRIEOptionLastBackupTS
	case "RATE_LIMIT":
		return ast.BRIEOptionRateLimit
	case "CONCURRENCY":
		return ast.BRIEOptionConcurrency
	case "CHECKSUM":
		return ast.BRIEOptionChecksum
	case "SEND_CREDENTIALS_TO_TIKV":
		return ast.BRIEOptionSendCreds
	case "CHECKPOINT":
		return ast.BRIEOptionCheckpoint
	case "ONLINE":
		return ast.BRIEOptionOnline
	case "ANALYZE":
		return ast.BRIEOptionAnalyze
	case "BACKEND":
		return ast.BRIEOptionBackend
	case "ON_DUPLICATE":
		return ast.BRIEOptionOnDuplicate
	case "CSV_DELIMITER":
		return ast.BRIEOptionCSVDelimiter
	case "CSV_HEADER":
		return ast.BRIEOptionCSVHeader
	case "CSV_NULL":
		return ast.BRIEOptionCSVNull
	case "CSV_SEPARATOR":
		return ast.BRIEOptionCSVSeparator
	case "CSV_BACKSLASH_ESCAPE":
		return ast.BRIEOptionCSVBackslashEscape
	case "CSV_NOT_NULL":
		return ast.BRIEOptionCSVNotNull
	case "CSV_TRIM_LAST_SEPARATORS":
		return ast.BRIEOptionCSVTrimLastSeparators
	case "FULL_BACKUP_STORAGE":
		return ast.BRIEOptionFullBackupStorage
	case "RESTORED_TS":
		return ast.BRIEOptionRestoredTS
	case "START_TS":
		return ast.BRIEOptionStartTS
	case "UNTIL_TS":
		return ast.BRIEOptionUntilTS
	case "GC_TTL":
		return ast.BRIEOptionGCTTL
	case "ENCRYPTION_METHOD":
		return ast.BRIEOptionEncryptionMethod
	case "ENCRYPTION_KEY_FILE":
		return ast.BRIEOptionEncryptionKeyFile
	case "IGNORE_STATS":
		return ast.BRIEOptionIgnoreStats
	case "LOAD_STATS":
		return ast.BRIEOptionLoadStats
	case "WAIT_TIFLASH_READY":
		return ast.BRIEOptionWaitTiflashReady
	case "WITH_SYS_TABLE":
		return ast.BRIEOptionWithSysTable
	case "CHECKSUM_CONCURRENCY":
		return ast.BRIEOptionChecksumConcurrency
	case "COMPRESSION_LEVEL":
		return ast.BRIEOptionCompressionLevel
	case "COMPRESSION_TYPE":
		return ast.BRIEOptionCompression
	case "SKIP_SCHEMA_FILES":
		return ast.BRIEOptionSkipSchemaFiles
	case "STRICT_FORMAT":
		return ast.BRIEOptionStrictFormat
	case "TIKV_IMPORTER":
		return ast.BRIEOptionTiKVImporter
	case "RESUME":
		return ast.BRIEOptionResume
	default:
		return ast.BRIEOptionRateLimit // fallback
	}
}

// parseBRIELevelValue checks if the next token is a BRIE level/boolean keyword
// (OFF, OPTIONAL, REQUIRED, TRUE, FALSE, COLUMNS) and returns the corresponding
// BRIEOptionLevel value. Returns (value, true) if matched, otherwise (0, false).
func parseBRIELevelValue(p *HandParser) (ast.BRIEOptionLevel, bool) {
	tok := p.peek()
	switch strings.ToUpper(tok.Lit) {
	case "OFF", "FALSE":
		p.next()
		return ast.BRIEOptionLevelOff, true
	case "REQUIRED", "TRUE":
		p.next()
		return ast.BRIEOptionLevelRequired, true
	case "OPTIONAL":
		p.next()
		return ast.BRIEOptionLevelOptional, true
	}
	return 0, false
}

// isTimeUnit returns true if the literal is a time unit keyword.
func isTimeUnit(lit string) bool {
	switch strings.ToUpper(lit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "YEAR":
		return true
	}
	return false
}

// timeUnitToMicroseconds converts a time unit name to its microsecond equivalent.
func timeUnitToMicroseconds(unit string) uint64 {
	switch unit {
	case "MICROSECOND":
		return 1
	case "SECOND":
		return 1000000
	case "MINUTE":
		return 60000000
	case "HOUR":
		return 3600000000
	case "DAY":
		return 86400000000
	case "WEEK":
		return 604800000000
	case "MONTH":
		return 2592000000000 // 30 days
	case "YEAR":
		return 31536000000000 // 365 days
	}
	return 1
}
