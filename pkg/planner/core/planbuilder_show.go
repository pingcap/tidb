// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	semv2 "github.com/pingcap/tidb/pkg/util/sem/v2"
)

func (b *PlanBuilder) buildShow(ctx context.Context, show *ast.ShowStmt) (base.Plan, error) {
	tnW := b.resolveCtx.GetTableName(show.Table)
	p := logicalop.LogicalShow{
		ShowContents: logicalop.ShowContents{
			Tp:                    show.Tp,
			CountWarningsOrErrors: show.CountWarningsOrErrors,
			DBName:                show.DBName,
			Table:                 tnW,
			Partition:             show.Partition,
			Column:                show.Column,
			IndexName:             show.IndexName,
			ResourceGroupName:     show.ResourceGroupName,
			Flag:                  show.Flag,
			User:                  show.User,
			Roles:                 show.Roles,
			Full:                  show.Full,
			IfNotExists:           show.IfNotExists,
			GlobalScope:           show.GlobalScope,
			Extended:              show.Extended,
			Limit:                 show.Limit,
			ImportJobID:           show.ImportJobID,
			DistributionJobID:     show.DistributionJobID,
			ImportGroupKey:        show.ShowGroupKey,
		},
	}.Init(b.ctx)
	isView := false
	isSequence := false
	isTempTableLocal := false
	// It depends on ShowPredicateExtractor now
	buildPattern := true

	switch show.Tp {
	case ast.ShowDatabases, ast.ShowVariables, ast.ShowTables, ast.ShowColumns, ast.ShowTableStatus, ast.ShowCollation, ast.ShowAffinity:
		if (show.Tp == ast.ShowTables || show.Tp == ast.ShowTableStatus) && p.DBName == "" {
			return nil, plannererrors.ErrNoDB
		}
		if extractor := newShowBaseExtractor(*show); extractor.Extract() {
			p.Extractor = extractor
			buildPattern = false
		}
	case ast.ShowCreateTable, ast.ShowCreateSequence, ast.ShowPlacementForTable, ast.ShowPlacementForPartition:
		var err error
		if table, err := b.is.TableByName(ctx, show.Table.Schema, show.Table.Name); err == nil {
			isView = table.Meta().IsView()
			isSequence = table.Meta().IsSequence()
			isTempTableLocal = table.Meta().TempTableType == model.TempTableLocal
		}
		user := b.ctx.GetSessionVars().User
		if isView {
			if user != nil {
				err = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SHOW VIEW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
		} else {
			if user != nil {
				err = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SHOW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
			}
			if isTempTableLocal {
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AllPrivMask, show.Table.Schema.L, show.Table.Name.L, "", err)
			} else {
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AllPrivMask&(^mysql.CreateTMPTablePriv), show.Table.Schema.L, show.Table.Name.L, "", err)
			}
		}
	case ast.ShowConfig:
		privErr := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ConfigPriv, "", "", "", privErr)
	case ast.ShowCreateView:
		var err error
		user := b.ctx.GetSessionVars().User
		if user != nil {
			err = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
		if user != nil {
			err = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SHOW VIEW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
	case ast.ShowBackups:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or BACKUP_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"BACKUP_ADMIN"}, false, err)
	case ast.ShowRestores:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESTORE_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"RESTORE_ADMIN"}, false, err)
	case ast.ShowTableNextRowId:
		p := &ShowNextRowID{TableName: show.Table}
		p.SetSchemaAndNames(buildShowNextRowID())
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, show.Table.Schema.L, show.Table.Name.L, "", plannererrors.ErrPrivilegeCheckFail)
		return p, nil
	case ast.ShowStatsExtended, ast.ShowStatsHealthy, ast.ShowStatsTopN, ast.ShowHistogramsInFlight, ast.ShowColumnStatsUsage:
		var err error
		if user := b.ctx.GetSessionVars().User; user != nil {
			err = plannererrors.ErrDBaccessDenied.GenWithStackByArgs(user.AuthUsername, user.AuthHostname, mysql.SystemDB)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, mysql.SystemDB, "", "", err)
		if show.Tp == ast.ShowStatsHealthy {
			if extractor := newShowBaseExtractor(*show); extractor.Extract() {
				p.Extractor = extractor
				buildPattern = false
			}
		}
	case ast.ShowStatsBuckets, ast.ShowStatsHistograms, ast.ShowStatsMeta, ast.ShowStatsLocked:
		var err error
		if user := b.ctx.GetSessionVars().User; user != nil {
			err = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SHOW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
		if show.Tp == ast.ShowStatsMeta {
			if extractor := newShowBaseExtractor(*show); extractor.Extract() {
				p.Extractor = extractor
				buildPattern = false
			}
		}
	case ast.ShowRegions:
		tableInfo, err := b.is.TableByName(ctx, show.Table.Schema, show.Table.Name)
		if err != nil {
			return nil, err
		}
		if tableInfo.Meta().TempTableType != model.TempTableNone {
			return nil, plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("show table regions")
		}
	case ast.ShowDistributions:
		tableInfo, err := b.is.TableByName(ctx, show.Table.Schema, show.Table.Name)
		if err != nil {
			return nil, err
		}
		if tableInfo.Meta().TempTableType != model.TempTableNone {
			return nil, plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("show table distributions")
		}
	case ast.ShowReplicaStatus:
		return nil, dbterror.ErrNotSupportedYet.GenWithStackByArgs("SHOW {REPLICA | SLAVE} STATUS")
	}

	schema, names := buildShowSchema(show, isView, isSequence)
	p.SetSchema(schema)
	p.SetOutputNames(names)
	for _, col := range p.Schema().Columns {
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
	}
	var err error
	var np base.LogicalPlan
	np = p
	// If we have ShowPredicateExtractor, we do not buildSelection with Pattern
	if show.Pattern != nil && buildPattern {
		show.Pattern.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: p.OutputNames()[0].ColName},
		}
		np, err = b.buildSelection(ctx, np, show.Pattern, nil)
		if err != nil {
			return nil, err
		}
	}
	if show.Where != nil {
		np, err = b.buildSelection(ctx, np, show.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if show.Limit != nil {
		np, err = b.buildLimit(np, show.Limit)
		if err != nil {
			return nil, err
		}
	}
	if np != p {
		b.optFlag |= rule.FlagEliminateProjection
		fieldsLen := len(p.Schema().Columns)
		proj := logicalop.LogicalProjection{Exprs: make([]expression.Expression, 0, fieldsLen)}.Init(b.ctx, 0)
		schema := expression.NewSchema(make([]*expression.Column, 0, fieldsLen)...)
		for _, col := range p.Schema().Columns {
			proj.Exprs = append(proj.Exprs, col)
			newCol := col.Clone().(*expression.Column)
			newCol.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
			schema.Append(newCol)
		}
		proj.SetSchema(schema)
		proj.SetChildren(np)
		proj.SetOutputNames(np.OutputNames())
		np = proj
	}
	if show.Tp == ast.ShowVariables || show.Tp == ast.ShowStatus {
		b.curClause = orderByClause
		orderByCol := np.Schema().Columns[0].Clone().(*expression.Column)
		sort := logicalop.LogicalSort{
			ByItems: []*util.ByItems{{Expr: orderByCol}},
		}.Init(b.ctx, b.getSelectOffset())
		sort.SetChildren(np)
		np = sort
	}
	return np, nil
}

func (b *PlanBuilder) buildSimple(ctx context.Context, node ast.StmtNode) (base.Plan, error) {
	p := &Simple{Statement: node, ResolveCtx: b.resolveCtx}

	switch raw := node.(type) {
	case *ast.FlushStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("RELOAD")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ReloadPriv, "", "", "", err)
	case *ast.AlterInstanceStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
	case *ast.RenameUserStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "", err)
	case *ast.GrantStmt:
		var err error
		b.visitInfo, err = collectVisitInfoFromGrantStmt(b.ctx, b.visitInfo, raw)
		if err != nil {
			return nil, err
		}
		for _, user := range raw.Users {
			b.visitInfo = appendVisitInfoIsRestrictedUser(ctx, b.visitInfo, b.ctx, user.User, "RESTRICTED_USER_ADMIN")
		}
	case *ast.BRIEStmt:
		p.SetSchemaAndNames(buildBRIESchema(raw.Kind))
		if raw.Kind == ast.BRIEKindRestore {
			err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESTORE_ADMIN")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"RESTORE_ADMIN"}, false, err)
		} else {
			err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or BACKUP_ADMIN")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"BACKUP_ADMIN"}, false, err)
		}
	case *ast.CalibrateResourceStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESOURCE_GROUP_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"RESOURCE_GROUP_ADMIN"}, false, err)
		p.SetSchemaAndNames(buildCalibrateResourceSchema())
	case *ast.AddQueryWatchStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESOURCE_GROUP_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"RESOURCE_GROUP_ADMIN"}, false, err)
		p.SetSchemaAndNames(buildAddQueryWatchSchema())
	case *ast.DropQueryWatchStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESOURCE_GROUP_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"RESOURCE_GROUP_ADMIN"}, false, err)
	case *ast.GrantRoleStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or ROLE_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"ROLE_ADMIN"}, false, err)
		// If any of the roles are RESTRICTED, require RESTRICTED_ROLE_ADMIN
		for _, role := range raw.Roles {
			b.visitInfo = appendVisitInfoIsRestrictedUser(ctx, b.visitInfo, b.ctx, &auth.UserIdentity{Username: role.Username, Hostname: role.Hostname},
				"RESTRICTED_USER_ADMIN")
		}
	case *ast.RevokeRoleStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or ROLE_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"ROLE_ADMIN"}, false, err)
		// Check if any of the users are RESTRICTED
		for _, user := range raw.Users {
			b.visitInfo = appendVisitInfoIsRestrictedUser(ctx, b.visitInfo, b.ctx, user, "RESTRICTED_USER_ADMIN")
		}
		// If any of the roles are RESTRICTED, require RESTRICTED_ROLE_ADMIN
		for _, role := range raw.Roles {
			b.visitInfo = appendVisitInfoIsRestrictedUser(ctx, b.visitInfo, b.ctx, &auth.UserIdentity{Username: role.Username, Hostname: role.Hostname},
				"RESTRICTED_USER_ADMIN")
		}
	case *ast.RevokeStmt:
		var err error
		b.visitInfo, err = collectVisitInfoFromRevokeStmt(ctx, b.ctx, b.visitInfo, raw)
		if err != nil {
			return nil, err
		}
		for _, user := range raw.Users {
			b.visitInfo = appendVisitInfoIsRestrictedUser(ctx, b.visitInfo, b.ctx, user.User, "RESTRICTED_USER_ADMIN")
		}
	case *ast.KillStmt:
		// All users can kill their own connections regardless.
		// If you have the SUPER privilege, you can kill all threads and statements unless SEM is enabled.
		// In which case you require RESTRICTED_CONNECTION_ADMIN to kill connections that belong to RESTRICTED_USER_ADMIN users.
		sm := b.ctx.GetSessionManager()
		if sm != nil {
			if pi, ok := sm.GetProcessInfo(raw.ConnectionID); ok {
				loginUser := b.ctx.GetSessionVars().User
				if pi.User != loginUser.Username {
					err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or CONNECTION_ADMIN")
					b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"CONNECTION_ADMIN"}, false, err)
					b.visitInfo = appendVisitInfoIsRestrictedUser(ctx, b.visitInfo, b.ctx, &auth.UserIdentity{Username: pi.User, Hostname: pi.Host}, "RESTRICTED_CONNECTION_ADMIN")
				}
			} else if handleutil.GlobalAutoAnalyzeProcessList.Contains(raw.ConnectionID) {
				// Only the users with SUPER or CONNECTION_ADMIN privilege can kill auto analyze.
				err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or CONNECTION_ADMIN")
				b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"CONNECTION_ADMIN"}, false, err)
			}
		}
	case *ast.UseStmt:
		if raw.DBName == "" {
			return nil, plannererrors.ErrNoDB
		}
	case *ast.DropUserStmt:
		// The main privilege checks for DROP USER are currently performed in executor/simple.go
		// because they use complex OR conditions (not supported by visitInfo).
		for _, user := range raw.UserList {
			b.visitInfo = appendVisitInfoIsRestrictedUser(ctx, b.visitInfo, b.ctx, user, "RESTRICTED_USER_ADMIN")
		}
	case *ast.SetPwdStmt:
		if raw.User != nil {
			b.visitInfo = appendVisitInfoIsRestrictedUser(ctx, b.visitInfo, b.ctx, raw.User, "RESTRICTED_USER_ADMIN")
		}
	case *ast.ShutdownStmt:
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShutdownPriv, "", "", "", nil)
	case *ast.BeginStmt:
		readTS := b.ctx.GetSessionVars().TxnReadTS.PeakTxnReadTS()
		if raw.AsOf != nil {
			startTS, err := staleread.CalculateAsOfTsExpr(ctx, b.ctx, raw.AsOf.TsExpr)
			if err != nil {
				return nil, err
			}
			if err := sessionctx.ValidateSnapshotReadTS(ctx, b.ctx.GetStore(), startTS, true); err != nil {
				return nil, err
			}
			p.StaleTxnStartTS = startTS
		} else if readTS > 0 {
			p.StaleTxnStartTS = readTS
			// consume read ts here
			b.ctx.GetSessionVars().TxnReadTS.UseTxnReadTS()
		} else if b.ctx.GetSessionVars().EnableExternalTSRead && !b.ctx.GetSessionVars().InRestrictedSQL {
			// try to get the stale ts from external timestamp
			startTS, err := staleread.GetExternalTimestamp(ctx, b.ctx.GetSessionVars().StmtCtx)
			if err != nil {
				return nil, err
			}
			if err := sessionctx.ValidateSnapshotReadTS(ctx, b.ctx.GetStore(), startTS, true); err != nil {
				return nil, err
			}
			p.StaleTxnStartTS = startTS
		}
	case *ast.SetResourceGroupStmt:
		if vardef.EnableResourceControlStrictMode.Load() {
			err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESOURCE_GROUP_ADMIN or RESOURCE_GROUP_USER")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"RESOURCE_GROUP_ADMIN", "RESOURCE_GROUP_USER"}, false, err)
		}
	case *ast.DropProcedureStmt:
		procName := fmt.Sprintf("%s.%s", ast.NewCIStr(b.ctx.GetSessionVars().CurrentDB).O, raw.ProcedureName.Name.O)
		err := plannererrors.ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", procName)
		if !raw.IfExists {
			return nil, err
		}
		b.ctx.GetSessionVars().StmtCtx.AppendNote(err)
	}
	return p, nil
}
func collectVisitInfoFromRevokeStmt(ctx context.Context, sctx base.PlanContext, vi []visitInfo, stmt *ast.RevokeStmt) ([]visitInfo, error) {
	// To use REVOKE, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	// This supports a local revoke SELECT on tablename, but does
	// not add dbName to the visitInfo of a *.* grant.
	if dbName == "" && stmt.Level.Level != ast.GrantLevelGlobal {
		if sctx.GetSessionVars().CurrentDB == "" {
			return nil, plannererrors.ErrNoDB
		}
		dbName = strings.ToLower(sctx.GetSessionVars().CurrentDB)
	}
	var nonDynamicPrivilege bool
	var allPrivs []mysql.PrivilegeType
	for _, item := range stmt.Privs {
		if semv2.IsEnabled() {
			if (len(item.Name) > 0 && semv2.IsRestrictedPrivilege(strings.ToUpper(item.Name))) ||
				(len(item.Name) == 0 && semv2.IsRestrictedPrivilege(strings.ToUpper(item.Priv.String()))) {
				// In `semv2`, we'll support to limit non-dynamic privileges unless the user has the `RESTRICTED_PRIV_ADMIN` privilege.
				// For example, `File` privilege might be restricted.
				// It's also controlled by the `GRANT OPTION`, so the user will also need the `GRANT OPTION` for this privilege.
				err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_PRIV_ADMIN")
				vi = appendDynamicVisitInfo(vi, []string{"RESTRICTED_PRIV_ADMIN"}, false, err)
			}
		}
		if item.Priv == mysql.ExtendedPriv {
			vi = appendDynamicVisitInfo(vi, []string{strings.ToUpper(item.Name)}, true, nil) // verified in MySQL: requires the dynamic grant option to revoke.
			continue
		}
		nonDynamicPrivilege = true
		if item.Priv == mysql.AllPriv {
			switch stmt.Level.Level {
			case ast.GrantLevelGlobal:
				allPrivs = mysql.AllGlobalPrivs
			case ast.GrantLevelDB:
				allPrivs = mysql.AllDBPrivs
			case ast.GrantLevelTable:
				allPrivs = mysql.AllTablePrivs
			}
			break
		}
		vi = appendVisitInfo(vi, item.Priv, dbName, tableName, "", nil)
	}

	for _, priv := range allPrivs {
		vi = appendVisitInfo(vi, priv, dbName, tableName, "", nil)
	}
	for _, u := range stmt.Users {
		// For SEM, make sure the users are not restricted
		vi = appendVisitInfoIsRestrictedUser(ctx, vi, sctx, u.User, "RESTRICTED_USER_ADMIN")
	}
	if nonDynamicPrivilege {
		// Dynamic privileges use their own GRANT OPTION. If there were any non-dynamic privilege requests,
		// we need to attach the "GLOBAL" version of the GRANT OPTION.
		vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "", nil)
	}
	return vi, nil
}

// appendVisitInfoIsRestrictedUser appends additional visitInfo if the user has a
// special privilege called "RESTRICTED_USER_ADMIN". It only applies when SEM is enabled.
func appendVisitInfoIsRestrictedUser(ctx context.Context, visitInfo []visitInfo, sctx base.PlanContext, user *auth.UserIdentity, priv string) []visitInfo {
	if !sem.IsEnabled() {
		return visitInfo
	}
	checker := privilege.GetPrivilegeManager(sctx)
	if checker != nil && checker.RequestDynamicVerificationWithUser(ctx, "RESTRICTED_USER_ADMIN", false, user) {
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs(priv)
		visitInfo = appendDynamicVisitInfo(visitInfo, []string{priv}, false, err)
	}
	return visitInfo
}

func collectVisitInfoFromGrantStmt(sctx base.PlanContext, vi []visitInfo, stmt *ast.GrantStmt) ([]visitInfo, error) {
	// To use GRANT, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	// This supports a local revoke SELECT on tablename, but does
	// not add dbName to the visitInfo of a *.* grant.
	if dbName == "" && stmt.Level.Level != ast.GrantLevelGlobal {
		if sctx.GetSessionVars().CurrentDB == "" {
			return nil, plannererrors.ErrNoDB
		}
		dbName = sctx.GetSessionVars().CurrentDB
	}
	var nonDynamicPrivilege bool
	var allPrivs []mysql.PrivilegeType
	authErr := genAuthErrForGrantStmt(sctx, dbName)
	for _, item := range stmt.Privs {
		if semv2.IsEnabled() {
			if (len(item.Name) > 0 && semv2.IsRestrictedPrivilege(strings.ToUpper(item.Name))) ||
				(len(item.Name) == 0 && semv2.IsRestrictedPrivilege(strings.ToUpper(item.Priv.String()))) {
				// In `semv2`, we'll support to limit non-dynamic privileges unless the user has the `RESTRICTED_PRIV_ADMIN` privilege.
				// For example, `File` privilege might be restricted.
				// It's also controlled by the `GRANT OPTION`, so the user will also need the `GRANT OPTION` for this privilege.
				err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_PRIV_ADMIN")
				vi = appendDynamicVisitInfo(vi, []string{"RESTRICTED_PRIV_ADMIN"}, false, err)
			}
		}
		if item.Priv == mysql.ExtendedPriv {
			// The observed MySQL behavior is that the error is:
			// ERROR 1227 (42000): Access denied; you need (at least one of) the GRANT OPTION privilege(s) for this operation
			// This is ambiguous, because it doesn't say the GRANT OPTION for which dynamic privilege.

			// In privilege/privileges/cache.go:RequestDynamicVerification SUPER+Grant_Priv will also be accepted here by TiDB, but it is *not* by MySQL.
			// This extension is currently required because:
			// - The visitInfo system does not accept OR conditions. There are many scenarios where SUPER or a DYNAMIC privilege are supported,
			//   this is the one case where SUPER is not intended to be an alternative.
			// - The "ALL" privilege for TiDB does not include all dynamic privileges. This could be fixed by a bootstrap task to assign all SUPER users
			//   with dynamic privileges.

			err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("GRANT OPTION")
			vi = appendDynamicVisitInfo(vi, []string{item.Name}, true, err)
			continue
		}
		nonDynamicPrivilege = true
		if item.Priv == mysql.AllPriv {
			switch stmt.Level.Level {
			case ast.GrantLevelGlobal:
				allPrivs = mysql.AllGlobalPrivs
			case ast.GrantLevelDB:
				allPrivs = mysql.AllDBPrivs
			case ast.GrantLevelTable:
				allPrivs = mysql.AllTablePrivs
			}
			break
		}
		vi = appendVisitInfo(vi, item.Priv, dbName, tableName, "", authErr)
	}

	for _, priv := range allPrivs {
		vi = appendVisitInfo(vi, priv, dbName, tableName, "", authErr)
	}
	if nonDynamicPrivilege {
		// Dynamic privileges use their own GRANT OPTION. If there were any non-dynamic privilege requests,
		// we need to attach the "GLOBAL" version of the GRANT OPTION.
		vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "", authErr)
	}
	return vi, nil
}

func genAuthErrForGrantStmt(sctx base.PlanContext, dbName string) error {
	if !strings.EqualFold(dbName, metadef.PerformanceSchemaName.L) {
		return nil
	}
	user := sctx.GetSessionVars().User
	if user == nil {
		return nil
	}
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return plannererrors.ErrDBaccessDenied.FastGenByArgs(u, h, dbName)
}
