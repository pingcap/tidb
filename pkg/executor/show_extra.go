// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"context"
	gjson "encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
)

func (e *ShowExec) fetchShowIndex() error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()

	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	statsTbl := h.GetPhysicalTableStats(tb.Meta().ID, tb.Meta())

	checker := privilege.GetPrivilegeManager(e.Ctx())
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	if checker != nil && e.Ctx().GetSessionVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", mysql.AllPrivMask) {
		return e.tableAccessDenied("SELECT", tb.Meta().Name.O)
	}

	if tb.Meta().PKIsHandle {
		var pkCol *table.Column
		for _, col := range tb.Cols() {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				pkCol = col
				break
			}
		}
		colStats := statsTbl.GetCol(pkCol.ID)
		var ndv int64
		if colStats != nil {
			ndv = colStats.NDV
		}
		e.appendRow([]any{
			tb.Meta().Name.O, // Table
			0,                // Non_unique
			"PRIMARY",        // Key_name
			1,                // Seq_in_index
			pkCol.Name.O,     // Column_name
			"A",              // Collation
			ndv,              // Cardinality
			nil,              // Sub_part
			nil,              // Packed
			"",               // Null
			"BTREE",          // Index_type
			"",               // Comment
			"",               // Index_comment
			"YES",            // Index_visible
			nil,              // Expression
			"YES",            // Clustered
			"NO",             // Global_index
		})
	}
	for _, idx := range tb.Indices() {
		idxInfo := idx.Meta()
		if idxInfo.State != model.StatePublic {
			continue
		}
		isClustered := "NO"
		if tb.Meta().IsCommonHandle && idxInfo.Primary {
			isClustered = "YES"
		}

		isGlobalIndex := "NO"
		if idxInfo.Global {
			isGlobalIndex = "YES"
		}

		for i, col := range idxInfo.Columns {
			nonUniq := 1
			if idx.Meta().Unique {
				nonUniq = 0
			}

			var subPart any
			if col.Length != types.UnspecifiedLength {
				subPart = col.Length
			}

			tblCol := tb.Meta().Columns[col.Offset]
			nullVal := "YES"
			if mysql.HasNotNullFlag(tblCol.GetFlag()) {
				nullVal = ""
			}

			visible := "YES"
			if idx.Meta().Invisible {
				visible = "NO"
			}

			colName := col.Name.O
			var expression any
			if tblCol.Hidden {
				colName = "NULL"
				expression = tblCol.GeneratedExprString
			}

			colStats := statsTbl.GetCol(tblCol.ID)
			var ndv int64
			if colStats != nil {
				ndv = colStats.NDV
			}

			e.appendRow([]any{
				tb.Meta().Name.O,       // Table
				nonUniq,                // Non_unique
				idx.Meta().Name.O,      // Key_name
				i + 1,                  // Seq_in_index
				colName,                // Column_name
				"A",                    // Collation
				ndv,                    // Cardinality
				subPart,                // Sub_part
				nil,                    // Packed
				nullVal,                // Null
				idx.Meta().Tp.String(), // Index_type
				"",                     // Comment
				idx.Meta().Comment,     // Index_comment
				visible,                // Index_visible
				expression,             // Expression
				isClustered,            // Clustered
				isGlobalIndex,          // Global_index
			})
		}
	}
	return nil
}

// fetchShowCharset gets all charset information and fill them into e.rows.
// See http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	descs := charset.GetSupportedCharsets()
	sessVars := e.Ctx().GetSessionVars()
	for _, desc := range descs {
		defaultCollation := desc.DefaultCollation
		if desc.Name == charset.CharsetUTF8MB4 {
			var err error
			defaultCollation, err = sessVars.GetSessionOrGlobalSystemVar(context.Background(), vardef.DefaultCollationForUTF8MB4)
			if err != nil {
				return err
			}
		}
		e.appendRow([]any{
			desc.Name,
			desc.Desc,
			defaultCollation,
			desc.Maxlen,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowMasterStatus() error {
	tso := e.Ctx().GetSessionVars().TxnCtx.StartTS
	e.appendRow([]any{"tidb-binlog", tso, "", "", ""})
	return nil
}

func (e *ShowExec) fetchShowVariables(ctx context.Context) (err error) {
	var (
		value       string
		sessionVars = e.Ctx().GetSessionVars()
	)
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	if e.GlobalScope {
		// Collect global scope variables,
		// 1. Exclude the variables of ScopeSession in variable.SysVars;
		// 2. If the variable is ScopeNone, it's a read-only variable, return the default value of it,
		// 		otherwise, fetch the value from table `mysql.Global_Variables`.
		for _, v := range variable.GetSysVars() {
			if v.Scope != vardef.ScopeSession {
				if v.IsNoop && !vardef.EnableNoopVariables.Load() {
					continue
				}
				if fieldFilter != "" && v.Name != fieldFilter {
					continue
				} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name) {
					continue
				}
				if infoschema.SysVarHiddenForSem(e.Ctx(), v.Name) {
					continue
				}
				value, err = sessionVars.GetGlobalSystemVar(ctx, v.Name)
				if err != nil {
					return errors.Trace(err)
				}
				e.appendRow([]any{v.Name, value})
			}
		}
		return nil
	}

	// Collect session scope variables,
	// If it is a session only variable, use the default value defined in code,
	//   otherwise, fetch the value from table `mysql.Global_Variables`.
	for _, v := range variable.GetSysVars() {
		if v.IsNoop && !vardef.EnableNoopVariables.Load() {
			continue
		}
		if fieldFilter != "" && v.Name != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name) {
			continue
		}
		if infoschema.SysVarHiddenForSem(e.Ctx(), v.Name) || v.InternalSessionVariable {
			continue
		}
		value, err = sessionVars.GetSessionOrGlobalSystemVar(context.Background(), v.Name)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]any{v.Name, value})
	}
	return nil
}

func (e *ShowExec) fetchShowStatus() error {
	sessionVars := e.Ctx().GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		return errors.Trace(err)
	}
	checker := privilege.GetPrivilegeManager(e.Ctx())
	for status, v := range statusVars {
		if e.GlobalScope && v.Scope == vardef.ScopeSession {
			continue
		}
		// Skip invisible status vars if permission fails.
		if sem.IsEnabled() && sem.IsInvisibleStatusVar(status) {
			if checker == nil || !checker.RequestDynamicVerification(sessionVars.ActiveRoles, "RESTRICTED_STATUS_ADMIN", false) {
				continue
			}
		}
		switch v.Value.(type) {
		case []any, nil:
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		value, err := types.ToString(v.Value)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]any{status, value})
	}
	return nil
}

// isUTF8MB4AndDefaultCollation returns if the cs is utf8mb4 and the co is DefaultCollationForUTF8MB4.
func isUTF8MB4AndDefaultCollation(sessVars *variable.SessionVars, cs, co string) (isUTF8MB4 bool, isDefault bool, err error) {
	if cs != charset.CharsetUTF8MB4 {
		return false, false, nil
	}
	defaultCollation, err := sessVars.GetSessionOrGlobalSystemVar(context.Background(), vardef.DefaultCollationForUTF8MB4)
	if err != nil {
		return false, false, err
	}
	if co == defaultCollation {
		return true, true, nil
	}
	return true, false, nil
}

func (e *ShowExec) fetchShowCollation() error {
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)
	if e.Extractor != nil {
		fieldPatternsLike = e.Extractor.FieldPatternLike()
		fieldFilter = e.Extractor.Field()
	}

	sessVars := e.Ctx().GetSessionVars()
	collations := collate.GetSupportedCollations()
	for _, v := range collations {
		isDefault := ""
		isUTF8MB4, isDefaultCollation, err := isUTF8MB4AndDefaultCollation(sessVars, v.CharsetName, v.Name)
		if err != nil {
			return err
		}
		if isUTF8MB4 && isDefaultCollation {
			isDefault = "Yes"
		} else if !isUTF8MB4 && v.IsDefault {
			isDefault = "Yes"
		}
		if fieldFilter != "" && strings.ToLower(v.Name) != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name) {
			continue
		}
		e.appendRow([]any{
			v.Name,
			v.CharsetName,
			v.ID,
			isDefault,
			"Yes",
			v.Sortlen,
			v.PadAttribute,
		})
	}
	return nil
}

// fetchShowCreateUser composes 'show create user' result.
func (e *ShowExec) fetchShowCreateUser(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)

	userName, hostName := e.User.Username, e.User.Hostname
	sessVars := e.Ctx().GetSessionVars()
	if e.User.CurrentUser {
		userName = sessVars.User.AuthUsername
		hostName = sessVars.User.AuthHostname
	} else {
		// Show create user requires the SELECT privilege on mysql.user.
		// Ref https://dev.mysql.com/doc/refman/5.7/en/show-create-user.html
		activeRoles := sessVars.ActiveRoles
		if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.SelectPriv) {
			return e.tableAccessDenied("SELECT", mysql.UserTable)
		}
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil,
		`SELECT plugin, Account_locked, user_attributes->>'$.metadata', Token_issuer,
        Password_reuse_history, Password_reuse_time, Password_expired, Password_lifetime,
        user_attributes->>'$.Password_locking.failed_login_attempts',
        user_attributes->>'$.Password_locking.password_lock_time_days', authentication_string,
        Max_user_connections
		FROM %n.%n WHERE User=%? AND Host=%?`,
		mysql.SystemDB, mysql.UserTable, userName, strings.ToLower(hostName))
	if err != nil {
		return errors.Trace(err)
	}

	if len(rows) == 0 {
		// FIXME: the error returned is not escaped safely
		return exeerrors.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER",
			fmt.Sprintf("'%s'@'%s'", e.User.Username, e.User.Hostname))
	}

	authPlugin, err := e.Ctx().GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.DefaultAuthPlugin)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 1 && rows[0].GetString(0) != "" {
		authPlugin = rows[0].GetString(0)
	}

	accountLockedRaw := rows[0].GetString(1)
	accountLocked := "LOCK"
	if accountLockedRaw[len(accountLockedRaw)-1:] == "N" {
		accountLocked = "UNLOCK"
	}

	userAttributes := rows[0].GetString(2)
	if len(userAttributes) > 0 {
		userAttributes = fmt.Sprintf(" ATTRIBUTE '%s'", userAttributes)
	}

	tokenIssuer := rows[0].GetString(3)
	if len(tokenIssuer) > 0 {
		tokenIssuer = " token_issuer " + tokenIssuer
	}

	var passwordHistory string
	if rows[0].IsNull(4) {
		passwordHistory = "DEFAULT"
	} else {
		passwordHistory = strconv.FormatUint(rows[0].GetUint64(4), 10)
	}

	var passwordReuseInterval string
	if rows[0].IsNull(5) {
		passwordReuseInterval = "DEFAULT"
	} else {
		passwordReuseInterval = strconv.FormatUint(rows[0].GetUint64(5), 10) + " DAY"
	}

	passwordExpired := rows[0].GetEnum(6).String()
	passwordLifetime := int64(-1)
	if !rows[0].IsNull(7) {
		passwordLifetime = rows[0].GetInt64(7)
	}
	passwordExpiredStr := "PASSWORD EXPIRE DEFAULT"
	if passwordExpired == "Y" {
		passwordExpiredStr = "PASSWORD EXPIRE"
	} else if passwordLifetime == 0 {
		passwordExpiredStr = "PASSWORD EXPIRE NEVER"
	} else if passwordLifetime > 0 {
		passwordExpiredStr = fmt.Sprintf("PASSWORD EXPIRE INTERVAL %d DAY", passwordLifetime)
	}

	failedLoginAttempts := rows[0].GetString(8)
	if len(failedLoginAttempts) > 0 {
		failedLoginAttempts = " FAILED_LOGIN_ATTEMPTS " + failedLoginAttempts
	}

	passwordLockTimeDays := rows[0].GetString(9)
	if len(passwordLockTimeDays) > 0 {
		if passwordLockTimeDays == "-1" {
			passwordLockTimeDays = " PASSWORD_LOCK_TIME UNBOUNDED"
		} else {
			passwordLockTimeDays = " PASSWORD_LOCK_TIME " + passwordLockTimeDays
		}
	}
	authData := rows[0].GetString(10)

	maxUserConnections := rows[0].GetInt64(11)
	maxUserConnectionsStr := ""
	if maxUserConnections > 0 {
		maxUserConnectionsStr = fmt.Sprintf(" WITH MAX_USER_CONNECTIONS %d", maxUserConnections)
	}

	rows, _, err = exec.ExecRestrictedSQL(ctx, nil, `SELECT Priv FROM %n.%n WHERE User=%? AND Host=%?`, mysql.SystemDB, mysql.GlobalPrivTable, userName, hostName)
	if err != nil {
		return errors.Trace(err)
	}

	require := "NONE"
	if len(rows) == 1 {
		privData := rows[0].GetString(0)
		var privValue privileges.GlobalPrivValue
		err = gjson.Unmarshal(hack.Slice(privData), &privValue)
		if err != nil {
			return errors.Trace(err)
		}
		require = privValue.RequireStr()
	}

	authStr := ""
	if !(authPlugin == mysql.AuthSocket && authData == "") {
		authStr = fmt.Sprintf(" AS '%s'", authData)
	}

	// FIXME: the returned string is not escaped safely
	showStr := fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED WITH '%s'%s REQUIRE %s%s%s %s ACCOUNT %s PASSWORD HISTORY %s PASSWORD REUSE INTERVAL %s%s%s%s",
		e.User.Username, e.User.Hostname, authPlugin, authStr, require, tokenIssuer, maxUserConnectionsStr, passwordExpiredStr, accountLocked, passwordHistory, passwordReuseInterval, failedLoginAttempts, passwordLockTimeDays, userAttributes)
	e.appendRow([]any{showStr})
	return nil
}

func (e *ShowExec) fetchShowGrants(ctx context.Context) error {
	vars := e.Ctx().GetSessionVars()
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	if e.User == nil || e.User.CurrentUser {
		// The input is a "SHOW GRANTS" statement with no users *or* SHOW GRANTS FOR CURRENT_USER()
		// In these cases we include the active roles for showing privileges.
		e.User = &auth.UserIdentity{Username: vars.User.AuthUsername, Hostname: vars.User.AuthHostname}
		if len(e.Roles) == 0 {
			e.Roles = vars.ActiveRoles
		}
	} else {
		userName := vars.User.AuthUsername
		hostName := vars.User.AuthHostname
		// Show grant user requires the SELECT privilege on mysql schema.
		// Ref https://dev.mysql.com/doc/refman/8.0/en/show-grants.html
		if userName != e.User.Username || hostName != e.User.Hostname {
			if !checker.RequestVerification(vars.ActiveRoles, mysql.SystemDB, "", "", mysql.SelectPriv) {
				return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(userName, hostName, mysql.SystemDB)
			}
		}
	}
	// This is for the syntax SHOW GRANTS FOR x USING role
	for _, r := range e.Roles {
		if r.Hostname == "" {
			r.Hostname = "%"
		}
		if !checker.FindEdge(ctx, r, e.User) {
			return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(r.String(), e.User.String())
		}
	}
	gs, err := checker.ShowGrants(ctx, e.Ctx(), e.User, e.Roles)
	if err != nil {
		return errors.Trace(err)
	}
	for _, g := range gs {
		e.appendRow([]any{g})
	}
	return nil
}

func (e *ShowExec) fetchShowPrivileges() error {
	e.appendRow([]any{"Alter", "Tables", "To alter the table"})
	e.appendRow([]any{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"})
	e.appendRow([]any{"Config", "Server Admin", "To use SHOW CONFIG and SET CONFIG statements"})
	e.appendRow([]any{"Create", "Databases,Tables,Indexes", "To create new databases and tables"})
	e.appendRow([]any{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"})
	e.appendRow([]any{"Create role", "Server Admin", "To create new roles"})
	e.appendRow([]any{"Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"})
	e.appendRow([]any{"Create view", "Tables", "To create new views"})
	e.appendRow([]any{"Create user", "Server Admin", "To create new users"})
	e.appendRow([]any{"Delete", "Tables", "To delete existing rows"})
	e.appendRow([]any{"Drop", "Databases,Tables", "To drop databases, tables, and views"})
	e.appendRow([]any{"Drop role", "Server Admin", "To drop roles"})
	e.appendRow([]any{"Event", "Server Admin", "To create, alter, drop and execute events"})
	e.appendRow([]any{"Execute", "Functions,Procedures", "To execute stored routines"})
	e.appendRow([]any{"File", "File access on server", "To read and write files on the server"})
	e.appendRow([]any{"Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"})
	e.appendRow([]any{"Index", "Tables", "To create or drop indexes"})
	e.appendRow([]any{"Insert", "Tables", "To insert data into tables"})
	e.appendRow([]any{"Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"})
	e.appendRow([]any{"Process", "Server Admin", "To view the plain text of currently executing queries"})
	e.appendRow([]any{"Proxy", "Server Admin", "To make proxy user possible"})
	e.appendRow([]any{"References", "Databases,Tables", "To have references on tables"})
	e.appendRow([]any{"Reload", "Server Admin", "To reload or refresh tables, logs and privileges"})
	e.appendRow([]any{"Replication client", "Server Admin", "To ask where the slave or master servers are"})
	e.appendRow([]any{"Replication slave", "Server Admin", "To read binary log events from the master"})
	e.appendRow([]any{"Select", "Tables", "To retrieve rows from table"})
	e.appendRow([]any{"Show databases", "Server Admin", "To see all databases with SHOW DATABASES"})
	e.appendRow([]any{"Show view", "Tables", "To see views with SHOW CREATE VIEW"})
	e.appendRow([]any{"Shutdown", "Server Admin", "To shut down the server"})
	e.appendRow([]any{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."})
	e.appendRow([]any{"Trigger", "Tables", "To use triggers"})
	e.appendRow([]any{"Create tablespace", "Server Admin", "To create/alter/drop tablespaces"})
	e.appendRow([]any{"Update", "Tables", "To update existing rows"})
	e.appendRow([]any{"Usage", "Server Admin", "No privileges - allow connect only"})

	for _, priv := range privileges.GetDynamicPrivileges() {
		e.appendRow([]any{priv, "Server Admin", ""})
	}
	return nil
}

func (*ShowExec) fetchShowTriggers() error {
	return nil
}

func (*ShowExec) fetchShowProcedureStatus() error {
	return nil
}

func (e *ShowExec) fetchShowPlugins() error {
	tiPlugins := plugin.GetAll()
	for _, ps := range tiPlugins {
		for _, p := range ps {
			e.appendRow([]any{p.Name, p.StateValue(), p.Kind.String(), p.Path, p.License, strconv.Itoa(int(p.Version))})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowWarnings(errOnly bool) error {
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	if e.CountWarningsOrErrors {
		errCount, warnCount := stmtCtx.NumErrorWarnings()
		if errOnly {
			e.appendRow([]any{int64(errCount)})
		} else {
			e.appendRow([]any{int64(warnCount)})
		}
		return nil
	}
	for _, w := range stmtCtx.GetWarnings() {
		if errOnly && w.Level != contextutil.WarnLevelError {
			continue
		}
		warn := errors.Cause(w.Err)
		switch x := warn.(type) {
		case *terror.Error:
			sqlErr := terror.ToSQLError(x)
			e.appendRow([]any{w.Level, int64(sqlErr.Code), sqlErr.Message})
		default:
			var err string
			if warn != nil {
				err = warn.Error()
			}
			e.appendRow([]any{w.Level, int64(mysql.ErrUnknown), err})
		}
	}
	return nil
}

func (e *ShowExec) getTable() (table.Table, error) {
	if e.Table == nil {
		return nil, errors.New("table not found")
	}
	tb, ok := e.is.TableByID(context.Background(), e.Table.TableInfo.ID)
	if !ok {
		return nil, errors.Errorf("table %s not found", e.Table.Name)
	}
	return tb, nil
}

func (e *ShowExec) dbAccessDenied() error {
	user := e.Ctx().GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(u, h, e.DBName)
}

func (e *ShowExec) tableAccessDenied(access string, table string) error {
	user := e.Ctx().GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return exeerrors.ErrTableaccessDenied.GenWithStackByArgs(access, u, h, table)
}

func (e *ShowExec) appendRow(row []any) {
	for i, col := range row {
		switch x := col.(type) {
		case nil:
			e.result.AppendNull(i)
		case int:
			e.result.AppendInt64(i, int64(x))
		case int64:
			e.result.AppendInt64(i, x)
		case uint64:
			e.result.AppendUint64(i, x)
		case float64:
			e.result.AppendFloat64(i, x)
		case float32:
			e.result.AppendFloat32(i, x)
		case string:
			e.result.AppendString(i, x)
		case []byte:
			e.result.AppendBytes(i, x)
		case types.BinaryLiteral:
			e.result.AppendBytes(i, x)
		case *types.MyDecimal:
			e.result.AppendMyDecimal(i, x)
		case types.Time:
			e.result.AppendTime(i, x)
		case types.BinaryJSON:
			e.result.AppendJSON(i, x)
		case types.VectorFloat32:
			e.result.AppendVectorFloat32(i, x)
		case types.Duration:
			e.result.AppendDuration(i, x)
		case types.Enum:
			e.result.AppendEnum(i, x)
		case types.Set:
			e.result.AppendSet(i, x)
		default:
			e.result.AppendNull(i)
		}
	}
}
