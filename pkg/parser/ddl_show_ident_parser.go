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

// parseShowIdentBased handles identifier-based SHOW variants (STATS_*, ENGINES, COLLATION, etc.).
// Caller passes the pre-allocated ShowStmt.
func (p *HandParser) parseShowIdentBased(stmt *ast.ShowStmt) ast.StmtNode {
	tok := p.peek()
	if !isIdentLike(tok.Tp) {
		return nil
	}
	switch strings.ToUpper(tok.Lit) {
	case "DATABASES":
		p.next()
		stmt.Tp = ast.ShowDatabases
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "ENGINES":
		p.next()
		stmt.Tp = ast.ShowEngines
		return stmt
	case "COLLATION":
		p.next()
		stmt.Tp = ast.ShowCollation
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "GRANTS":
		p.next()
		stmt.Tp = ast.ShowGrants
		if _, ok := p.accept(forKwd); ok {
			stmt.User = p.parseUserIdentity()
			if _, ok := p.accept(using); ok {
				for {
					role := p.parseRoleIdentity()
					if role == nil {
						break
					}
					stmt.Roles = append(stmt.Roles, role)
					if _, ok := p.accept(','); !ok {
						break
					}
				}
			}
		}
		return stmt
	case "ERRORS":
		p.next()
		stmt.Tp = ast.ShowErrors
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "PLUGINS":
		p.next()
		stmt.Tp = ast.ShowPlugins
		return stmt
	case "PRIVILEGES":
		p.next()
		stmt.Tp = ast.ShowPrivileges
		return stmt
	case "TRIGGERS":
		p.next()
		stmt.Tp = ast.ShowTriggers
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "EVENTS":
		p.next()
		stmt.Tp = ast.ShowEvents
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "OPEN":
		p.next()
		// SHOW OPEN TABLES [FROM db] [LIKE ...]
		p.accept(tables)
		stmt.Tp = ast.ShowOpenTables
		stmt.DBName = p.parseShowDatabaseNameOpt()
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "TABLE":
		p.next()
		// SHOW TABLE STATUS [FROM db] [LIKE ...]
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
	case "SESSION_STATES":
		p.next()
		stmt.Tp = ast.ShowSessionStates
		return stmt
	case "BINDINGS":
		p.next()
		stmt.Tp = ast.ShowBindings
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "PROFILES":
		p.next()
		stmt.Tp = ast.ShowProfiles
		return stmt
	case "PROFILE":
		p.next()
		stmt.Tp = ast.ShowProfile
		// Parse optional profile types (CPU, MEMORY, BLOCK IO, etc.)
		for {
			pk := p.peek()
			switch strings.ToUpper(pk.Lit) {
			case "CPU":
				p.next()
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypeCPU)
			case "MEMORY":
				p.next()
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypeMemory)
			case "BLOCK":
				p.next()
				if p.peek().IsKeyword("IO") {
					p.next()
				}
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypeBlockIo)
			case "CONTEXT":
				p.next()
				if p.peek().IsKeyword("SWITCHES") {
					p.next()
				}
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypeContextSwitch)
			case "IPC":
				p.next()
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypeIpc)
			case "PAGE":
				p.next()
				if p.peek().IsKeyword("FAULTS") {
					p.next()
				}
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypePageFaults)
			case "SWAPS":
				p.next()
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypeSwaps)
			case "SOURCE":
				p.next()
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypeSource)
			case "ALL":
				p.next()
				stmt.ShowProfileTypes = append(stmt.ShowProfileTypes, ast.ProfileTypeAll)
			default:
				goto doneProfileTypes
			}
			// comma between types
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	doneProfileTypes:
		// FOR QUERY n
		if _, ok := p.accept(forKwd); ok {
			p.expect(query)
			v := int64(p.parseUint64())
			stmt.ShowProfileArgs = &v
		}
		// LIMIT
		if p.peek().Tp == limit {
			stmt.ShowProfileLimit = p.parseLimitClause()
		}
		return stmt
	case "INDEXES":
		// SHOW INDEXES {FROM|IN} tbl [{FROM|IN} db] [WHERE expr]
		p.next()
		p.parseShowIndexStmt(stmt)
		return stmt
	case "CHARSET":
		p.next()
		stmt.Tp = ast.ShowCharset
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "MASTER":
		p.next()
		if p.peekKeyword(status, "STATUS") {
			p.next()
			stmt.Tp = ast.ShowMasterStatus
			return stmt
		}
		return nil
	case "CONFIG":
		p.next()
		stmt.Tp = ast.ShowConfig
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "BUILTINS":
		p.next()
		stmt.Tp = ast.ShowBuiltins
		return stmt
	case "STATS_EXTENDED":
		p.next()
		stmt.Tp = ast.ShowStatsExtended
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "STATS_META":
		p.next()
		stmt.Tp = ast.ShowStatsMeta
		stmt.Table = &ast.TableName{Name: ast.NewCIStr("STATS_META"), Schema: ast.NewCIStr("mysql")}
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "STATS_LOCKED":
		p.next()
		stmt.Tp = ast.ShowStatsLocked
		stmt.Table = &ast.TableName{Name: ast.NewCIStr("STATS_TABLE_LOCKED"), Schema: ast.NewCIStr("mysql")}
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "STATS_HISTOGRAMS":
		p.next()
		stmt.Tp = ast.ShowStatsHistograms
		stmt.Table = &ast.TableName{Name: ast.NewCIStr("STATS_HISTOGRAMS"), Schema: ast.NewCIStr("mysql")}
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "STATS_BUCKETS":
		p.next()
		stmt.Tp = ast.ShowStatsBuckets
		stmt.Table = &ast.TableName{Name: ast.NewCIStr("STATS_BUCKETS"), Schema: ast.NewCIStr("mysql")}
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "STATS_HEALTHY":
		p.next()
		stmt.Tp = ast.ShowStatsHealthy
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "STATS_TOPN":
		p.next()
		stmt.Tp = ast.ShowStatsTopN
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "HISTOGRAMS_IN_FLIGHT":
		p.next()
		stmt.Tp = ast.ShowHistogramsInFlight
		return stmt
	case "COLUMN_STATS_USAGE":
		p.next()
		stmt.Tp = ast.ShowColumnStatsUsage
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "BINDING_CACHE":
		p.next()
		if p.peekKeyword(status, "STATUS") {
			p.next()
			stmt.Tp = ast.ShowBindingCacheStatus
			return stmt
		}
		return nil
	case "BACKUPS":
		p.next()
		stmt.Tp = ast.ShowBackups
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "RESTORES":
		p.next()
		stmt.Tp = ast.ShowRestores
		p.parseShowLikeOrWhere(stmt)
		return stmt
	case "BACKUP":
		p.next()
		return p.parseShowBackupLogsStmt()
	case "BR":
		p.next()
		return p.parseShowBRJobStmt()
	case "AFFINITY":
		// parser.y: ShowTargetFilterable → "AFFINITY" → ShowStmt{Tp: ShowAffinity}
		p.next()
		stmt.Tp = ast.ShowAffinity
		return stmt
	case "IMPORTS":
		// SHOW IMPORTS — ShowImports is the correct AST constant for this path
		p.next()
		stmt.Tp = ast.ShowImports
		p.parseShowLikeOrWhere(stmt)
		return stmt
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
		stmt.Table = p.parseTableName()
		return stmt
	case view:
		p.next()
		stmt.Tp = ast.ShowCreateView
		stmt.Table = p.parseTableName()
		return stmt
	case procedure:
		p.next()
		stmt.Tp = ast.ShowCreateProcedure
		stmt.Procedure = p.parseTableName()
		return stmt
	case database:
		p.next()
		stmt.Tp = ast.ShowCreateDatabase
		if _, ok := p.accept(ifKwd); ok {
			p.accept(not)
			p.accept(exists)
			stmt.IfNotExists = true
		}
		tok := p.next()
		stmt.DBName = tok.Lit
		return stmt
	case sequence:
		p.next()
		stmt.Tp = ast.ShowCreateSequence
		stmt.Table = p.parseTableName()
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
