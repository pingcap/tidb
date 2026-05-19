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

// Bootstrap, upgrade, and amendment for starter deployment mode.
// Every entry point is a no-op outside starter mode.

package session

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// starterVersionVar is persisted in mysql.tidb; tracked separately from
	// tidbServerVersionVar so starter-only state can evolve independently.
	starterVersionVar = "starter_version"
	// currentStarterVersion is the latest starter bootstrap version.
	// Bump and append to starterUpgradeChain when adding starter-only changes.
	currentStarterVersion int64 = 1

	// starterAmendedVar marks that branch/restore amendment has run on this
	// cluster, so we skip on subsequent starts even if keyspace meta hasn't
	// been updated by the orchestrator yet.
	starterAmendedVar = "starter_amended"

	starterMaxExecutionTime = int(30 * time.Minute / time.Millisecond)
	starterReplicaRead      = "leader"
)

// starterUpgradeChain runs in order on every server start when the persisted
// starter version is below currentStarterVersion.
var starterUpgradeChain = []func(sessionapi.Session, int64){}

// doStarterBootstrap is called from doDMLWorks inside the bootstrap
// transaction. The root account has already been written; this adds the
// remaining starter state.
func doStarterBootstrap(s sessionapi.Session) {
	bootstrapStarterVariables(s)
	bootstrapStarterRoleAdmin(s)
	bootstrapStarterCloudAdmin(s, prefixedStarterUser("cloud_admin"))

	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?, "Starter bootstrap version. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, starterVersionVar, currentStarterVersion,
	)
}

// starterRootUserName is the value doDMLWorks uses for the root account.
func starterRootUserName() string {
	return prefixedStarterUser("root")
}

// prefixedStarterUser returns "<keyspace>.<name>" in starter mode, else the
// bare name. Once #68355 lands, callers can use
// keyspace.GetUsernamePolicy().GetUsernameVariants instead.
func prefixedStarterUser(name string) string {
	if !deploymode.IsStarter() {
		return name
	}
	prefix := config.GetGlobalKeyspaceName()
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

// runStarterUpgrade applies pending starter upgrade steps. No-op when
// already at the latest version.
func runStarterUpgrade(store kv.Storage) {
	if !deploymode.IsStarter() {
		return
	}
	s := mustCreateStarterSession(store, "starter upgrade")
	defer s.ClearValue(sessionctx.Initing)

	ver, err := getStarterVersion(s)
	terror.MustNil(err)
	if ver >= currentStarterVersion {
		return
	}
	for _, fn := range starterUpgradeChain {
		fn(s, ver)
	}
	updateStarterVersion(s)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	if _, err := s.ExecuteInternal(ctx, "COMMIT"); err != nil {
		// Another node may have committed the same upgrade concurrently.
		time.Sleep(time.Second)
		v, err1 := getStarterVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("[starter-upgrade] verify after commit failed", zap.Error(err1))
		}
		if v >= currentStarterVersion {
			return
		}
		logutil.BgLogger().Fatal("[starter-upgrade] commit failed",
			zap.Int64("from", ver), zap.Int64("to", currentStarterVersion), zap.Error(err))
	}
}

// runStarterBranchAmend wipes inherited user/privilege rows and reinstalls
// the starter admin set on first start of a branch cluster.
func runStarterBranchAmend(store kv.Storage) {
	if !deploymode.IsStarter() || !config.GetGlobalConfig().Starter.IsBranch {
		return
	}
	s := mustCreateStarterSession(store, "starter branch amend")
	defer s.ClearValue(sessionctx.Initing)

	if isStarterAmended(s, config.GetGlobalConfig().Starter.IsBranchBootstrapped) {
		return
	}
	amendStarterDBUsers(s)
}

// runStarterRestoreAmend is the same as runStarterBranchAmend but triggered
// by the restored-cluster flag.
func runStarterRestoreAmend(store kv.Storage) {
	if !deploymode.IsStarter() {
		return
	}
	flag := config.GetGlobalConfig().Starter.IsBootstrappedForRestore
	if flag == "" {
		return
	}
	s := mustCreateStarterSession(store, "starter restore amend")
	defer s.ClearValue(sessionctx.Initing)

	if isStarterAmended(s, flag) {
		return
	}
	amendStarterDBUsers(s)
}

// mustCreateStarterSession opens a bootstrap-style session for starter
// post-bootstrap work. Fatals on failure since the server cannot proceed.
func mustCreateStarterSession(store kv.Storage, op string) *session {
	s, err := createSession(store)
	if err != nil {
		logutil.BgLogger().Fatal(op+": createSession failed", zap.Error(err))
	}
	s.sessionVars.EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly
	s.SetValue(sessionctx.Initing, true)
	return s
}

// isStarterAmended reports done via either the orchestrator-supplied flag
// or the local starterAmendedVar marker (fallback when the orchestrator
// hasn't written keyspace meta back yet).
func isStarterAmended(s sessionapi.Session, configFlag string) bool {
	if configFlag != "" {
		if done, _ := strconv.ParseBool(configFlag); done {
			return true
		}
	}
	sVal, isNull, err := getTiDBVar(s, starterAmendedVar)
	if err != nil {
		logutil.BgLogger().Fatal("starter amend: status check failed", zap.Error(errors.Trace(err)))
	}
	return !isNull && sVal == varTrue
}

// amendStarterDBUsers wipes user/privilege rows and reinstalls the starter
// admin set in a single transaction so clients never see an empty user table.
func amendStarterDBUsers(s sessionapi.Session) {
	mustExecute(s, "BEGIN")
	mustExecute(s, "DELETE FROM mysql.db")
	mustExecute(s, "DELETE FROM mysql.default_roles")
	mustExecute(s, "DELETE FROM mysql.global_grants")
	mustExecute(s, "DELETE FROM mysql.global_priv")
	mustExecute(s, "DELETE FROM mysql.role_edges")
	mustExecute(s, "DELETE FROM mysql.user")

	bootstrapStarterRoot(s, prefixedStarterUser("root"))
	bootstrapStarterRoleAdmin(s)
	bootstrapStarterCloudAdmin(s, prefixedStarterUser("cloud_admin"))

	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "Starter amendment marker. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, starterAmendedVar, varTrue, varTrue,
	)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	if _, err := s.ExecuteInternal(ctx, "COMMIT"); err != nil {
		logutil.BgLogger().Fatal("starter amend: commit failed", zap.Error(err))
	}
	// TODO: also write keyspace meta via PD once the meta-client write path
	// is available, so the orchestrator can observe completion directly.
}

// bootstrapStarterVariables sets the starter-mode global variable defaults.
func bootstrapStarterVariables(s sessionapi.Session) {
	upserts := []struct {
		name string
		val  any
	}{
		{vardef.TiDBAnalyzeVersion, 2},
		{vardef.TiDBRedactLog, vardef.On},
		{vardef.TiDBStmtSummaryRefreshInterval, 60},
		{vardef.TiDBStmtSummaryMaxStmtCount, 1000},
		{vardef.TiDBEnableAsyncCommit, vardef.Off},
		{vardef.TiDBEnable1PC, vardef.Off},
		{vardef.MaxExecutionTime, starterMaxExecutionTime},
		{vardef.TiDBPessimisticTransactionFairLocking, vardef.Off},
		{vardef.TiDBReplicaRead, starterReplicaRead},
	}
	for _, u := range upserts {
		mustExecute(s,
			`INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
			mysql.SystemDB, mysql.GlobalVariablesTable, u.name, u.val, u.val,
		)
	}
}

// bootstrapStarterRoot installs the starter-flavored root account: full
// data access, no SHUTDOWN, no CONFIG.
func bootstrapStarterRoot(s sessionapi.Session, userName string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", User = %?, authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		starterAllPrivsClause+
		`Account_locked = "N", Shutdown_priv = "N", `+
		`Reload_priv = "Y", FILE_priv = "Y", Config_priv = "N", `+
		`Create_Tablespace_Priv = "Y", User_attributes = NULL, Token_issuer = "" `,
		userName,
	)
}

// bootstrapStarterRoleAdmin creates a locked role_admin account that owns
// ROLE_ADMIN so role grants can be issued without super-user credentials.
func bootstrapStarterRoleAdmin(s sessionapi.Session) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", User = "role_admin", authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		starterAllPrivsClause+
		`Account_locked = "Y", Shutdown_priv = "N", `+
		`Reload_priv = "Y", FILE_priv = "Y", Config_priv = "N", `+
		`Create_Tablespace_Priv = "Y", User_attributes = NULL, Token_issuer = "";`,
	)
	insertStarterGlobalGrant(s, "role_admin", "ROLE_ADMIN", "N")
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.global_priv SET `+
		`Host = "%", User = "role_admin", Priv = "{}";`,
	)
}

// bootstrapStarterCloudAdmin creates the cloud_admin control-plane account.
// No Super_priv; constrained dynamic privileges.
func bootstrapStarterCloudAdmin(s sessionapi.Session, userName string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", User = %?, authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", Insert_priv = "Y", Update_priv = "Y", `+
		`Delete_priv = "Y", Create_priv = "Y", Drop_priv = "Y", `+
		`Process_priv = "Y", Grant_priv = "N", References_priv = "Y", `+
		`Alter_priv = "Y", Show_db_priv = "Y", Super_priv = "N", `+
		`Create_tmp_table_priv = "N", Lock_tables_priv = "N", Execute_priv = "N", `+
		`Create_view_priv = "Y", Show_view_priv = "N", Create_routine_priv = "N", `+
		`Alter_routine_priv = "N", Index_priv = "Y", Create_user_priv = "Y", `+
		`Event_priv = "N", Repl_slave_priv = "N", Repl_client_priv = "N", `+
		`Trigger_priv = "N", Create_role_priv = "Y", Drop_role_priv = "N", `+
		`Account_locked = "N", Shutdown_priv = "Y", Reload_priv = "Y", `+
		`FILE_priv = "N", Config_priv = "Y", Create_Tablespace_Priv = "N", `+
		`User_attributes = NULL, Token_issuer = "" `,
		userName,
	)

	for _, priv := range cloudAdminDynamicPrivs {
		insertStarterGlobalGrant(s, userName, priv, "N")
	}
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.global_priv SET `+
		`Host = "%", User = %?, Priv = "{}"`,
		userName,
	)
}

var cloudAdminDynamicPrivs = []string{
	"DASHBOARD_CLIENT",
	"SYSTEM_VARIABLES_ADMIN",
	"CONNECTION_ADMIN",
	"RESTRICTED_VARIABLES_ADMIN",
	"RESTRICTED_STATUS_ADMIN",
	"RESTRICTED_CONNECTION_ADMIN",
	"RESTRICTED_USER_ADMIN",
	"RESTRICTED_TABLES_ADMIN",
	"RESTRICTED_REPLICA_WRITER_ADMIN",
	"BACKUP_ADMIN",
	"RESTORE_ADMIN",
	"SYSTEM_USER",
	"RESTRICTED_AUDIT_ADMIN",
}

// starterAllPrivsClause is the static-privilege block shared by the starter
// root and role_admin REPLACE statements. Caller writes Account_locked,
// Shutdown_priv, and trailing columns since they differ between the two.
const starterAllPrivsClause = `` +
	`Select_priv = "Y", Insert_priv = "Y", Update_priv = "Y", ` +
	`Delete_priv = "Y", Create_priv = "Y", Drop_priv = "Y", ` +
	`Process_priv = "Y", Grant_priv = "Y", References_priv = "Y", ` +
	`Alter_priv = "Y", Show_db_priv = "Y", Super_priv = "Y", ` +
	`Create_tmp_table_priv = "Y", Lock_tables_priv = "Y", Execute_priv = "Y", ` +
	`Create_view_priv = "Y", Show_view_priv = "Y", Create_routine_priv = "Y", ` +
	`Alter_routine_priv = "Y", Index_priv = "Y", Create_user_priv = "Y", ` +
	`Event_priv = "Y", Repl_slave_priv = "Y", Repl_client_priv = "Y", ` +
	`Trigger_priv = "Y", Create_role_priv = "Y", Drop_role_priv = "Y", `

func insertStarterGlobalGrant(s sessionapi.Session, user, priv, withGrant string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.global_grants SET `+
		`USER = %?, HOST = "%", PRIV = %?, WITH_GRANT_OPTION = %?`,
		user, priv, withGrant,
	)
}

func getStarterVersion(s sessionapi.Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, starterVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

func updateStarterVersion(s sessionapi.Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "Starter bootstrap version. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, starterVersionVar, currentStarterVersion, currentStarterVersion,
	)
}
