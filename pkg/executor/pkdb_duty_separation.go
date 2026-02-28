// Copyright 2023-2023 PingCAP, Inc.

package executor

import (
	"context"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// DutyRoles specifies the 4 admin roles and their grants.
var DutyRoles = map[string][]string{
	"system_admin": {
		`create role IF NOT EXISTS system_admin`,
		`GRANT CREATE, DROP, REFERENCES, ALTER, INDEX, CREATE TABLESPACE, CREATE VIEW, CREATE TEMPORARY TABLES, CREATE ROUTINE, ALTER ROUTINE, RELOAD, RESOURCE_GROUP_ADMIN ON *.* TO 'system_admin'@'%' WITH GRANT OPTION`,
	},
	"security_admin": {
		`create role IF NOT EXISTS security_admin`,
		`GRANT CREATE USER, CREATE ROLE, DROP ROLE, SHUTDOWN, RELOAD, CONFIG, CONNECTION_ADMIN, PLACEMENT_ADMIN, RESTRICTED_CONNECTION_ADMIN, RESTRICTED_REPLICA_WRITER_ADMIN, RESTRICTED_STATUS_ADMIN, RESTRICTED_TABLES_ADMIN, RESTRICTED_USER_ADMIN, RESTRICTED_VARIABLES_ADMIN, ROLE_ADMIN, SYSTEM_VARIABLES_ADMIN ON *.* TO 'security_admin'@'%' WITH GRANT OPTION`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON mysql.* TO 'security_admin'@'%'`,
	},
	"database_admin": {
		`create role IF NOT EXISTS database_admin`,
		`GRANT SELECT, INSERT, UPDATE, DELETE, LOCK TABLES, PROCESS, SHOW DATABASES, EXECUTE, TRIGGER, SHOW VIEW, EVENT, FILE, REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, BACKUP_ADMIN, RESTORE_ADMIN, DASHBOARD_CLIENT, RESOURCE_GROUP_USER ON *.* TO 'database_admin'@'%'  WITH GRANT OPTION`,
	},
	"audit_admin": {
		`create role IF NOT EXISTS audit_admin`,
		`GRANT RELOAD, SYSTEM_VARIABLES_ADMIN, RESTRICTED_VARIABLES_ADMIN, AUDIT_ADMIN, RESTRICTED_AUDIT_ADMIN ON *.* TO 'audit_admin'@'%' WITH GRANT OPTION`,
	},
}

// IsAdminRole checks whether `username` belong to admin roles.
func IsAdminRole(username string) bool {
	if _, exist := DutyRoles[username]; exist {
		return true
	}

	return false
}

// MockSetDutySeparationModeEnable enables the mode of duty-separation if need to test duty-separation.
// Because the default value of cfg.Security.TidbEnableDutySeparationMode is false.
func MockSetDutySeparationModeEnable() func() {
	// set cfg.Security.TidbEnableDutySeparationMode enable
	cfg := config.GetGlobalConfig()
	cfg.Security.TidbEnableDutySeparationMode = true
	config.StoreGlobalConfig(cfg)

	return func() {
		defaultCfg := config.NewConfig()
		config.StoreGlobalConfig(defaultCfg)
	}
}

func checkExclusiveRoleGranting(
	ctx context.Context,
	exec sqlexec.SQLExecutor,
	grantingRoles []*auth.RoleIdentity,
	targetUsers []*auth.UserIdentity,
) error {
	if !config.GetGlobalConfig().Security.TidbEnableDutySeparationMode {
		return nil
	}
	// Check if any of the roles being granted are exclusive roles
	// Only system_admin, security_admin, and audit_admin are mutually exclusive
	exclusiveRoles := []string{"system_admin", "security_admin", "audit_admin", "database_admin"}
	// Special roles that cannot coexist with write privileges on user databases
	dmlConflictRoles := []string{"system_admin", "security_admin", "audit_admin"}

	hasExclusiveRole := false
	grantingExclusiveRoles := make([]string, 0)
	hasWriteConflictRole := false
	for _, role := range grantingRoles {
		if slices.Contains(exclusiveRoles, role.Username) {
			hasExclusiveRole = true
			grantingExclusiveRoles = append(grantingExclusiveRoles, role.Username)
		}
		if slices.Contains(dmlConflictRoles, role.Username) {
			hasWriteConflictRole = true
		}
	}

	// Check if trying to grant multiple exclusive roles in the same statement
	if len(grantingExclusiveRoles) > 1 {
		return errors.Errorf("violation of separation of duty: cannot grant multiple exclusive roles (%s) in a single statement",
			strings.Join(grantingExclusiveRoles, ", "))
	}

	// If granting an exclusive role, check for conflicts
	if hasExclusiveRole {
		for _, user := range targetUsers {
			// Check if user already has any exclusive roles using helper
			existingRole, err := queryRoleForUser(ctx, exec, user.Username, user.Hostname, exclusiveRoles)
			if err != nil {
				return err
			}

			// Check if user already has one of the exclusive roles
			if existingRole != "" {
				// Check if trying to grant a different exclusive role
				for _, role := range grantingRoles {
					if slices.Contains(exclusiveRoles, role.Username) && role.Username != existingRole {
						return errors.Errorf("violation of separation of duty: user %s@%s already has role %s, cannot grant role %s",
							user.Username, user.Hostname, existingRole, role.Username)
					}
				}
			}
		}
	}

	// If granting write-conflict roles (system_admin, security_admin, audit_admin),
	// check if user has SELECT/INSERT/UPDATE/DELETE privileges on non-mysql databases
	if hasWriteConflictRole {
		for _, user := range targetUsers {
			hasDataPriv, err := userOrRolesHaveDataPrivileges(ctx, exec, user.Username, user.Hostname)
			if err != nil {
				return err
			}
			if hasDataPriv {
				return errors.Errorf("violation of separation of duty: user %s@%s has SELECT/INSERT/UPDATE/DELETE privileges on user databases, cannot grant %s role",
					user.Username, user.Hostname, grantingExclusiveRoles[0])
			}
		}
	}

	// Check if any role being granted has data privileges, and if target users have special admin roles
	for _, role := range grantingRoles {
		// Check if this role has data privileges on user databases
		hasDataPriv, err := roleHasDataPrivilegesOnUserDB(ctx, exec, role.Username)
		if err != nil {
			return err
		}
		if !hasDataPriv {
			continue
		}

		// This role has data privileges, check if target users have special admin roles
		for _, user := range targetUsers {
			specialRole, err := queryRoleForUser(ctx, exec, user.Username, user.Hostname, dmlConflictRoles)
			if err != nil {
				return err
			}

			if specialRole != "" {
				return errors.Errorf("violation of separation of duty: cannot grant role %s with SELECT/INSERT/UPDATE/DELETE privileges to user %s@%s who has %s role",
					role.Username, user.Username, user.Hostname, specialRole)
			}
		}
	}

	return nil
}

func checkSpecialRoleGrantingByRoot(sctx sessionctx.Context, grantingRoles []*auth.RoleIdentity) error {
	if !variable.EnableDutySeparationMode.Load() {
		return nil
	}

	var specialRoles []string
	for _, role := range grantingRoles {
		if _, ok := DutyRoles[role.Username]; ok {
			specialRoles = append(specialRoles, role.Username)
		}
	}
	if len(specialRoles) == 0 {
		return nil
	}

	user := sctx.GetSessionVars().User
	if user == nil {
		if intest.InTest {
			return nil
		}
		return errors.New("violation of separation of duty: only root user can grant special roles")
	}

	currentUser := user.AuthUsername
	if currentUser == "" {
		currentUser = user.Username
	}
	if currentUser != "root" {
		if intest.InTest && currentUser == "" {
			return nil
		}
		if len(specialRoles) == 1 {
			return errors.Errorf("violation of separation of duty: only root user can grant role %s", specialRoles[0])
		}
		return errors.Errorf("violation of separation of duty: only root user can grant roles %s", strings.Join(specialRoles, ", "))
	}
	return nil
}

// roleHasDataPrivilegesOnUserDB checks if a role has SELECT/INSERT/UPDATE/DELETE privileges
// on any database except 'mysql'
func roleHasDataPrivilegesOnUserDB(ctx context.Context, exec sqlexec.SQLExecutor, roleName string) (bool, error) {
	// Roles always have '%' as host
	return userHasDataPrivilegesOnUserDB(ctx, exec, roleName, "%")
}

// userOrRolesHaveDataPrivileges checks if a user has data privileges either directly
// or through any of its granted roles
func userOrRolesHaveDataPrivileges(ctx context.Context, exec sqlexec.SQLExecutor, username, hostname string) (bool, error) {
	// First check if user has direct data privileges
	hasDataPriv, err := userHasDataPrivilegesOnUserDB(ctx, exec, username, hostname)
	if err != nil {
		return false, err
	}
	if hasDataPriv {
		return true, nil
	}

	// Then check if any of the user's roles have data privileges
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT FROM_USER FROM %n.%n WHERE TO_USER=%? AND TO_HOST=%?`,
		mysql.SystemDB, mysql.RoleEdgeTable, username, hostname)

	req, err := executeQuery(ctx, exec, sql.String())
	if err != nil {
		return false, err
	}

	// Check each role for data privileges
	for i := 0; i < req.NumRows(); i++ {
		roleName := req.GetRow(i).GetString(0)
		hasDataPriv, err := roleHasDataPrivilegesOnUserDB(ctx, exec, roleName)
		if err != nil {
			return false, err
		}
		if hasDataPriv {
			return true, nil
		}
	}

	return false, nil
}

// userHasDataPrivilegesOnUserDB checks if a user has SELECT/INSERT/UPDATE/DELETE privileges
// on any database except 'mysql'
func userHasDataPrivilegesOnUserDB(ctx context.Context, exec sqlexec.SQLExecutor, username, hostname string) (bool, error) {
	// Check global privileges
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT Select_priv, Insert_priv, Update_priv, Delete_priv FROM %n.%n WHERE User=%? AND Host=%?`,
		mysql.SystemDB, mysql.UserTable, username, hostname)

	req, err := executeQuery(ctx, exec, sql.String())
	if err != nil {
		return false, err
	}

	if req.NumRows() > 0 {
		row := req.GetRow(0)
		if row.GetEnum(0).String() == "Y" || row.GetEnum(1).String() == "Y" || row.GetEnum(2).String() == "Y" || row.GetEnum(3).String() == "Y" {
			return true, nil
		}
	}

	// Check database-level privileges (excluding mysql database)
	sql.Reset()
	sqlescape.MustFormatSQL(sql, `SELECT DB, Select_priv, Insert_priv, Update_priv, Delete_priv FROM %n.%n WHERE User=%? AND Host=%? AND DB != 'mysql'`,
		mysql.SystemDB, mysql.DBTable, username, hostname)

	req, err = executeQuery(ctx, exec, sql.String())
	if err != nil {
		return false, err
	}

	if req.NumRows() > 0 {
		iter := chunk.NewIterator4Chunk(req)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			if row.GetEnum(1).String() == "Y" || row.GetEnum(2).String() == "Y" || row.GetEnum(3).String() == "Y" || row.GetEnum(4).String() == "Y" {
				return true, nil
			}
		}
	}

	// Check table-level privileges (excluding mysql database)
	sql.Reset()
	sqlescape.MustFormatSQL(sql, `SELECT DB, Table_priv FROM %n.%n WHERE User=%? AND Host=%? AND DB != 'mysql'`,
		mysql.SystemDB, mysql.TablePrivTable, username, hostname)

	req, err = executeQuery(ctx, exec, sql.String())
	if err != nil {
		return false, err
	}

	if req.NumRows() > 0 {
		iter := chunk.NewIterator4Chunk(req)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			tablePriv := row.GetSet(1).Name
			if strings.Contains(tablePriv, "Select") || strings.Contains(tablePriv, "Insert") || strings.Contains(tablePriv, "Update") || strings.Contains(tablePriv, "Delete") {
				return true, nil
			}
		}
	}

	// Check column-level privileges (excluding mysql database)
	sql.Reset()
	sqlescape.MustFormatSQL(sql, `SELECT DB, Column_priv FROM %n.%n WHERE User=%? AND Host=%? AND DB != 'mysql'`,
		mysql.SystemDB, mysql.ColumnPrivTable, username, hostname)

	req, err = executeQuery(ctx, exec, sql.String())
	if err != nil {
		return false, err
	}

	if req.NumRows() > 0 {
		iter := chunk.NewIterator4Chunk(req)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			columnPriv := row.GetSet(1).Name
			if strings.Contains(columnPriv, "Select") || strings.Contains(columnPriv, "Insert") || strings.Contains(columnPriv, "Update") {
				return true, nil
			}
		}
	}

	return false, nil
}

// checkDataPrivGrantToSpecialRoles checks if we're trying to grant SELECT/INSERT/UPDATE/DELETE privileges
// to users or roles who have system_admin, security_admin, or audit_admin roles (in duty separation mode)
func (e *GrantExec) checkDataPrivGrantToSpecialRoles(ctx context.Context, internalSession sessionctx.Context) error {
	if !config.GetGlobalConfig().Security.TidbEnableDutySeparationMode {
		return nil
	}

	// Check if we're granting data privileges on non-mysql databases
	isGrantingDataPriv := false
	for _, priv := range e.Privs {
		if priv.Priv == mysql.SelectPriv || priv.Priv == mysql.InsertPriv || priv.Priv == mysql.UpdatePriv || priv.Priv == mysql.DeletePriv ||
			priv.Priv == mysql.AllPriv {
			// Check if this is for mysql database - if so, it's OK
			if e.Level.Level == ast.GrantLevelDB && strings.EqualFold(e.Level.DBName, mysql.SystemDB) {
				continue
			}
			if e.Level.Level == ast.GrantLevelTable && strings.EqualFold(e.Level.DBName, mysql.SystemDB) {
				continue
			}
			// Also allow if the current DB is mysql and no DB is specified
			if e.Level.Level == ast.GrantLevelDB && e.Level.DBName == "" &&
				strings.EqualFold(e.Ctx().GetSessionVars().CurrentDB, mysql.SystemDB) {
				continue
			}
			if e.Level.Level == ast.GrantLevelTable && e.Level.DBName == "" &&
				strings.EqualFold(e.Ctx().GetSessionVars().CurrentDB, mysql.SystemDB) {
				continue
			}
			isGrantingDataPriv = true
			break
		}
	}

	if !isGrantingDataPriv {
		return nil
	}

	// Check if any of the target users has the special roles
	specialRoles := []string{"system_admin", "security_admin", "audit_admin"}
	for _, user := range e.Users {
		roleName, err := queryRoleForUser(ctx, internalSession.GetSQLExecutor(), user.User.Username, user.User.Hostname, specialRoles)
		if err != nil {
			return err
		}

		if roleName != "" {
			return errors.Errorf("violation of separation of duty: user %s@%s has role %s, cannot grant SELECT/INSERT/UPDATE/DELETE privileges on user databases",
				user.User.Username, user.User.Hostname, roleName)
		}

		// Also check if this user is a role, and if so, check if it's granted to users with special admin roles
		// Check if any user has this role AND has special admin roles
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT TO_USER, TO_HOST FROM %n.%n WHERE FROM_USER = %? AND FROM_HOST = %?`,
			mysql.SystemDB, mysql.RoleEdgeTable, user.User.Username, user.User.Hostname)

		req, err := executeQuery(ctx, internalSession.GetSQLExecutor(), sql.String())
		if err != nil {
			return err
		}

		// For each user that has this role, check if they have special admin roles
		if req.NumRows() > 0 {
			iter := chunk.NewIterator4Chunk(req)
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				toUser := row.GetString(0)
				toHost := row.GetString(1)

				specialRole, err := queryRoleForUser(ctx, internalSession.GetSQLExecutor(), toUser, toHost, specialRoles)
				if err != nil {
					return err
				}

				if specialRole != "" {
					return errors.Errorf("violation of separation of duty: cannot grant SELECT/INSERT/UPDATE/DELETE privileges to role %s@%s because it is granted to user %s@%s who has %s role",
						user.User.Username, user.User.Hostname, toUser, toHost, specialRole)
				}
			}
		}
	}

	return nil
}

// Helper function to execute SQL and get a single string result
func queryRoleForUser(ctx context.Context, exec sqlexec.SQLExecutor, username, hostname string, roles []string) (string, error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT FROM_USER FROM %n.%n WHERE TO_HOST = %? AND TO_USER = %? AND FROM_USER IN (`,
		mysql.SystemDB, mysql.RoleEdgeTable, hostname, username)
	for i, role := range roles {
		if i > 0 {
			sql.WriteString(", ")
		}
		sqlescape.MustFormatSQL(sql, "%?", role)
	}
	sql.WriteString(")")

	req, err := executeQuery(ctx, exec, sql.String())
	if err != nil {
		return "", err
	}

	if req.NumRows() > 0 {
		return req.GetRow(0).GetString(0), nil
	}
	return "", nil
}

// Helper function to execute SQL and return chunk
func executeQuery(ctx context.Context, exec sqlexec.SQLExecutor, query string) (*chunk.Chunk, error) {
	recordSet, err := exec.ExecuteInternal(ctx, query)
	if err != nil {
		return nil, err
	}
	req := recordSet.NewChunk(nil)
	err = recordSet.Next(ctx, req)
	if err != nil {
		_ = recordSet.Close()
		return nil, err
	}
	if err := recordSet.Close(); err != nil {
		return nil, err
	}
	return req, nil
}
