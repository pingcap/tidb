// Copyright 2023-2023 PingCAP, Inc.

package executor

import (
	"github.com/pingcap/tidb/pkg/config"
)

// DutyRoles specifies the 4 admin roles and their grants.
var DutyRoles = map[string][]string{
	"system_admin": {
		`create role IF NOT EXISTS system_admin`,
		`GRANT CREATE, DROP, REFERENCES, ALTER, INDEX, CREATE TABLESPACE, CREATE VIEW, CREATE TEMPORARY TABLES, CREATE ROUTINE, ALTER ROUTINE, RELOAD, SYSTEM_USER ON *.* TO 'system_admin'@'%' WITH GRANT OPTION`,
	},
	"security_admin": {
		`create role IF NOT EXISTS security_admin`,
		`GRANT CREATE USER, CREATE ROLE, DROP ROLE, SHUTDOWN, RELOAD, CONFIG, CONNECTION_ADMIN, PLACEMENT_ADMIN, RESTRICTED_CONNECTION_ADMIN, RESTRICTED_REPLICA_WRITER_ADMIN, RESTRICTED_STATUS_ADMIN, RESTRICTED_TABLES_ADMIN, RESTRICTED_USER_ADMIN, RESTRICTED_VARIABLES_ADMIN, ROLE_ADMIN, SYSTEM_USER, SYSTEM_VARIABLES_ADMIN ON *.* TO 'security_admin'@'%' WITH GRANT OPTION`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON mysql.* TO 'security_admin'@'%'`,
	},
	"database_admin": {
		`create role IF NOT EXISTS database_admin`,
		`GRANT SELECT, INSERT, UPDATE, DELETE, LOCK TABLES, PROCESS, SHOW DATABASES, EXECUTE, TRIGGER, SHOW VIEW, EVENT, FILE, REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, BACKUP_ADMIN, RESTORE_ADMIN, DASHBOARD_CLIENT, SYSTEM_USER ON *.* TO 'database_admin'@'%'  WITH GRANT OPTION`,
	},
	"audit_admin": {
		`create role IF NOT EXISTS audit_admin`,
		`GRANT RELOAD, SYSTEM_VARIABLES_ADMIN, RESTRICTED_VARIABLES_ADMIN, SYSTEM_USER, AUDIT_ADMIN, RESTRICTED_AUDIT_ADMIN ON *.* TO 'audit_admin'@'%' WITH GRANT OPTION`,
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
