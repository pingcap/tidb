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

// showSimpleEntry describes a trivial SHOW variant that only sets a type,
// optionally attaches a mysql.* table reference, and parses LIKE/WHERE.
type showSimpleEntry struct {
	tp          ast.ShowStmtType
	tableName   string // if non-empty, sets stmt.Table = mysql.<tableName>
	noLikeWhere bool   // if true, skip parseShowLikeOrWhere
	withDBName  bool   // if true, parse optional FROM/IN database name
}

// showSimpleTypes maps uppercase identifier tokens to their simple SHOW entries.
// All entries follow the same 3-step pattern: consume token, set type, parse LIKE/WHERE.
var showSimpleTypes = map[string]showSimpleEntry{
	"DATABASES":            {tp: ast.ShowDatabases},
	"ENGINES":              {tp: ast.ShowEngines},
	"COLLATION":            {tp: ast.ShowCollation},
	"ERRORS":               {tp: ast.ShowErrors},
	"PLUGINS":              {tp: ast.ShowPlugins},
	"PRIVILEGES":           {tp: ast.ShowPrivileges, noLikeWhere: true},
	"CHARSET":              {tp: ast.ShowCharset},
	"CONFIG":               {tp: ast.ShowConfig},
	"BUILTINS":             {tp: ast.ShowBuiltins, noLikeWhere: true},
	"PROFILES":             {tp: ast.ShowProfiles, noLikeWhere: true},
	"TRIGGERS":             {tp: ast.ShowTriggers, withDBName: true},
	"EVENTS":               {tp: ast.ShowEvents, withDBName: true},
	"STATS_EXTENDED":       {tp: ast.ShowStatsExtended},
	"STATS_META":           {tp: ast.ShowStatsMeta, tableName: "STATS_META"},
	"STATS_LOCKED":         {tp: ast.ShowStatsLocked, tableName: "STATS_TABLE_LOCKED"},
	"STATS_HISTOGRAMS":     {tp: ast.ShowStatsHistograms, tableName: "STATS_HISTOGRAMS"},
	"STATS_BUCKETS":        {tp: ast.ShowStatsBuckets, tableName: "STATS_BUCKETS"},
	"STATS_HEALTHY":        {tp: ast.ShowStatsHealthy},
	"STATS_TOPN":           {tp: ast.ShowStatsTopN},
	"HISTOGRAMS_IN_FLIGHT": {tp: ast.ShowHistogramsInFlight},
	"COLUMN_STATS_USAGE":   {tp: ast.ShowColumnStatsUsage},
	"BACKUPS":              {tp: ast.ShowBackups},
	"RESTORES":             {tp: ast.ShowRestores},
	"AFFINITY":             {tp: ast.ShowAffinity},
	"IMPORTS":              {tp: ast.ShowImports},
	"SESSION_STATES":       {tp: ast.ShowSessionStates},
	"BINDINGS":             {tp: ast.ShowBindings},
}

// parseShowIdentBased handles identifier-based SHOW variants (STATS_*, ENGINES, COLLATION, etc.).
// Caller passes the pre-allocated ShowStmt.
func (p *HandParser) parseShowIdentBased(stmt *ast.ShowStmt) ast.StmtNode {
	tok := p.peek()
	if !isIdentLike(tok.Tp) {
		return nil
	}
	upper := strings.ToUpper(tok.Lit)

	// Table-driven dispatch for trivial SHOW types.
	if entry, ok := showSimpleTypes[upper]; ok {
		p.next()
		stmt.Tp = entry.tp
		if entry.tableName != "" {
			stmt.Table = &ast.TableName{Name: ast.NewCIStr(entry.tableName), Schema: ast.NewCIStr("mysql")}
		}
		if entry.withDBName {
			stmt.DBName = p.parseShowDatabaseNameOpt()
		}
		if !entry.noLikeWhere {
			p.parseShowLikeOrWhere(stmt)
		}
		return stmt
	}

	// Cases with extra logic that cannot be table-driven.
	switch upper {
	case "PROFILE":
		p.next()
		stmt.Tp = ast.ShowProfile
		// Parse optional profile types: CPU, MEMORY, BLOCK IO, etc.
		stmt.ShowProfileTypes = p.parseShowProfileTypes()
		// FOR QUERY N
		if _, ok := p.accept(forKwd); ok {
			p.expect(query)
			if tok, ok := p.expect(intLit); ok {
				v := tok.Item.(int64)
				stmt.ShowProfileArgs = &v
			}
		}
		// LIMIT N [OFFSET N]
		if p.peek().Tp == limit {
			stmt.ShowProfileLimit = p.parseLimitClause()
		}
		return stmt
	case "GRANTS":
		p.next()
		stmt.Tp = ast.ShowGrants
		if _, ok := p.accept(forKwd); ok {
			stmt.User = p.parseUserIdentity()
			if _, ok := p.accept(using); ok {
				stmt.Roles, _ = parseCommaListPtr(p, p.parseRoleIdentity)
			}
		}
		return stmt

	case "OPEN":
		p.next()
		p.expect(tables)
		stmt.Tp = ast.ShowOpenTables
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "TABLE":
		p.next()
		if p.peekKeyword(status, "STATUS") {
			p.next()
			stmt.Tp = ast.ShowTableStatus
			stmt.DBName = p.parseShowDatabaseNameOpt()
			p.parseShowLikeOrWhere(stmt)
			return stmt
		}
		return nil
	case "PLACEMENT":
		p.next()
		if p.peek().IsKeyword("LABELS") {
			p.next()
			stmt.Tp = ast.ShowPlacementLabels
			p.parseShowLikeOrWhere(stmt)
			return stmt
		}
		stmt.Tp = ast.ShowPlacement
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "BINDING_CACHE":
		p.next()
		if p.peekKeyword(status, "STATUS") {
			p.next()
			stmt.Tp = ast.ShowBindingCacheStatus
			p.parseShowLikeOrWhere(stmt)
			return stmt
		}
		return nil
	case "BACKUP":
		p.next()
		return p.parseShowBackupLogsStmt()
	case "BR":
		p.next()
		return p.parseShowBRJobStmt()
	case "EXTENDED":
		// SHOW EXTENDED [FULL] {COLUMNS|FIELDS} {FROM|IN} tbl ...
		p.next()
		stmt.Extended = true
		if p.peekKeyword(full, "FULL") {
			p.next()
			stmt.Full = true
		}
		if p.peekKeyword(columns, "COLUMNS") || p.peekKeyword(fields, "FIELDS") {
			p.next()
			stmt.Tp = ast.ShowColumns
			p.parseShowTableClause(stmt)
			p.parseShowLikeOrWhere(stmt)
			return stmt
		}
		return nil
	case "SLAVE":
		p.next()
		if p.peekKeyword(status, "STATUS") {
			p.next()
			stmt.Tp = ast.ShowReplicaStatus
			return stmt
		}
		return nil
	}
	return nil
}

// parseShowCreate parses: SHOW CREATE TABLE|VIEW|DATABASE ...
func (p *HandParser) parseShowCreate() ast.StmtNode {
	stmt := p.arena.AllocShowStmt()

	switch p.peek().Tp {
	case tableKwd:
		p.next()
		stmt.Tp = ast.ShowCreateTable
		stmt.Table = p.expectTableName()
		return stmt
	case view:
		p.next()
		stmt.Tp = ast.ShowCreateView
		stmt.Table = p.expectTableName()
		return stmt
	case procedure:
		p.next()
		stmt.Tp = ast.ShowCreateProcedure
		stmt.Procedure = p.expectTableName()
		return stmt
	case database:
		p.next()
		stmt.Tp = ast.ShowCreateDatabase
		if _, ok := p.accept(ifKwd); ok {
			p.expect(not)
			p.expect(exists)
			stmt.IfNotExists = true
		}
		tok := p.next()
		stmt.DBName = tok.Lit
		return stmt
	case sequence:
		p.next()
		stmt.Tp = ast.ShowCreateSequence
		stmt.Table = p.expectTableName()
		return stmt
	case user:
		p.next()
		stmt.Tp = ast.ShowCreateUser
		if userIdent := p.parseUserIdentity(); userIdent != nil {
			stmt.User = userIdent
			return stmt
		}
		return nil
	default:
		// Handle identifier-based CREATE sub-types
		if isIdentLike(p.peek().Tp) {
			switch strings.ToUpper(p.peek().Lit) {
			case "PLACEMENT":
				p.next()
				// SHOW CREATE PLACEMENT POLICY name
				if p.peek().IsKeyword("POLICY") {
					p.next()
					stmt.Tp = ast.ShowCreatePlacementPolicy
					policyTok := p.next()
					stmt.DBName = policyTok.Lit
					return stmt
				}
				return nil
			case "RESOURCE":
				p.next()
				// SHOW CREATE RESOURCE GROUP name
				if p.peek().IsKeyword("GROUP") {
					p.next()
					stmt.Tp = ast.ShowCreateResourceGroup
					nameTok := p.next()
					stmt.ResourceGroupName = nameTok.Lit
					return stmt
				}
				return nil
			}
		}
		return nil
	}
}

// showProfileTypes maps profile type names to their AST constants.
var showProfileTypes = map[string]int{
	"ALL":    ast.ProfileTypeAll,
	"CPU":    ast.ProfileTypeCPU,
	"IPC":    ast.ProfileTypeIpc,
	"MEMORY": ast.ProfileTypeMemory,
	"SOURCE": ast.ProfileTypeSource,
	"SWAPS":  ast.ProfileTypeSwaps,
}

// parseShowProfileTypes parses the optional profile type list in SHOW PROFILE.
// Grammar: [type [, type] ...] where type = ALL | BLOCK IO | CPU | IPC | MEMORY | ...
func (p *HandParser) parseShowProfileTypes() []int {
	var types []int
	for {
		tok := p.peek()
		if !isIdentLike(tok.Tp) {
			break
		}
		upper := strings.ToUpper(tok.Lit)
		if v, ok := showProfileTypes[upper]; ok {
			p.next()
			types = append(types, v)
		} else if upper == "BLOCK" {
			p.next()
			// Expect IO after BLOCK
			if p.peek().IsKeyword("IO") {
				p.next()
			}
			types = append(types, ast.ProfileTypeBlockIo)
		} else if upper == "CONTEXT" {
			p.next()
			// Expect SWITCHES after CONTEXT
			if p.peek().IsKeyword("SWITCHES") {
				p.next()
			}
			types = append(types, ast.ProfileTypeContextSwitch)
		} else if upper == "PAGE" {
			p.next()
			// Expect FAULTS after PAGE
			if p.peek().IsKeyword("FAULTS") {
				p.next()
			}
			types = append(types, ast.ProfileTypePageFaults)
		} else {
			break
		}
		// Consume optional comma separator
		if p.peek().Tp != ',' {
			break
		}
		p.next()
	}
	return types
}
