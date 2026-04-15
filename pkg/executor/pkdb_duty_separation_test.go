// Copyright 2023-2023 PingCAP, Inc.

package executor_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestIsAdminRole(t *testing.T) {
	cases := []struct {
		username    string
		isAdminRole bool
	}{
		// do not belong to admin roles.
		{
			"abc",
			false,
		},
		// 4 admin roles
		{
			"system_admin",
			true,
		},
		{
			"security_admin",
			true,
		},
		{
			"database_admin",
			true,
		},
		{
			"audit_admin",
			true,
		},
		// check username is case-sensitive.
		{
			"System_admin",
			false,
		},
	}

	for _, c := range cases {
		r := executor.IsAdminRole(c.username)
		require.Equal(t, r, c.isAdminRole)
	}
}

func TestAdminRolesCount(t *testing.T) {
	require.Equal(t, 4, len(executor.DutyRoles))
}

func TestDynamicPrivilegesAssignedToSingleDutyRole(t *testing.T) {
	audit.Register4Test()

	dynamicPrivs := privileges.GetDynamicPrivileges()
	dynamicPrivSet := make(map[string]struct{}, len(dynamicPrivs))
	for _, priv := range dynamicPrivs {
		dynamicPrivSet[strings.ToUpper(priv)] = struct{}{}
	}

	assigned := make(map[string]map[string]struct{}, len(dynamicPrivs))
	for role, grants := range executor.DutyRoles {
		for _, stmt := range grants {
			privs, ok := extractGrantPrivileges(stmt)
			if !ok {
				continue
			}
			for _, priv := range privs {
				privUpper := strings.ToUpper(priv)
				if _, ok := dynamicPrivSet[privUpper]; !ok {
					continue
				}
				if assigned[privUpper] == nil {
					assigned[privUpper] = make(map[string]struct{})
				}
				assigned[privUpper][role] = struct{}{}
			}
		}
	}

	var missing []string
	duplicate := make(map[string][]string)
	for privUpper := range dynamicPrivSet {
		roles := assigned[privUpper]
		if len(roles) == 0 && privUpper != "SYSTEM_USER" {
			missing = append(missing, privUpper)
			continue
		}
		if len(roles) > 1 {
			roleList := make([]string, 0, len(roles))
			for role := range roles {
				roleList = append(roleList, role)
			}
			sort.Strings(roleList)
			duplicate[privUpper] = roleList
		}
	}
	sort.Strings(missing)

	delete(duplicate, "SYSTEM_VARIABLES_ADMIN")     // audit admin and security admin
	delete(duplicate, "RESTRICTED_VARIABLES_ADMIN") // audit admin and security admin
	if len(missing) > 0 || len(duplicate) > 0 {
		t.Fatalf("dynamic privileges must map to exactly one duty role; missing=%v duplicate=%v", missing, duplicate)
	}
}

func TestGrantAdminRoles(t *testing.T) {
	restoreFunc := executor.MockSetDutySeparationModeEnable()
	defer restoreFunc()

	// register extensions
	audit.Register4Test()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// check a user(root) and 4 roles(audit_admin, database_admin, security_admin, system_admin)
	tk.MustQuery("SELECT user, Account_locked FROM mysql.user ORDER BY user;").Check(testkit.Rows(
		"audit_admin Y",
		"database_admin Y",
		"root N",
		"security_admin Y",
		"system_admin Y",
	))
}

func TestDutySeparationBlockUserTableDML(t *testing.T) {
	restoreFunc := executor.MockSetDutySeparationModeEnable()
	defer restoreFunc()

	// register extensions
	audit.Register4Test()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("CREATE USER 'dbadmin'@'%'")
	tk.MustExec("GRANT database_admin TO 'dbadmin'@'%'")
	tk.MustExec("CREATE USER 'secadmin'@'%'")
	tk.MustExec("GRANT security_admin TO 'secadmin'@'%'")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "dbadmin", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("SET ROLE ALL")
	err := tk.ExecToErr("UPDATE mysql.user SET Account_locked=Account_locked WHERE User='root'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "UPDATE command denied")
	err = tk.ExecToErr("UPDATE mysql.db SET Select_priv=Select_priv WHERE User='root'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "UPDATE command denied")
	err = tk.ExecToErr("UPDATE mysql.role_edges SET TO_USER=TO_USER WHERE TO_USER='root'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "UPDATE command denied")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "secadmin", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("SET ROLE ALL")
	tk.MustExec("UPDATE mysql.user SET Account_locked=Account_locked WHERE User='root'")
	tk.MustExec("UPDATE mysql.db SET Select_priv=Select_priv WHERE User='root'")
}

func TestForbiddingDropAdminRoles(t *testing.T) {
	restoreFunc := executor.MockSetDutySeparationModeEnable()
	defer restoreFunc()

	// register extensions
	audit.Register4Test()

	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)

	tk.MustExec("create user username1")
	tk.MustExec("drop user username1")

	tk.MustExec("create role rolename1")
	tk.MustExec("drop role rolename1")

	for roleName := range executor.DutyRoles {
		_, err := tk.Exec(fmt.Sprintf("drop user %v", roleName))
		require.Error(t, err)
		require.True(t,
			strings.Contains(
				err.Error(),
				fmt.Sprintf("Operation DROP USER failed for %v@%%", roleName),
			),
		)
	}
}

func TestExclusiveRoleGranting(t *testing.T) {
	restoreFunc := executor.MockSetDutySeparationModeEnable()
	defer restoreFunc()

	// register extensions
	audit.Register4Test()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Create test users
	tk.MustExec("CREATE USER 'user1'@'localhost';")
	tk.MustExec("CREATE USER 'user2'@'localhost';")
	tk.MustExec("CREATE USER 'user3'@'localhost';")

	// Test 1: Grant system_admin to user1 should succeed
	tk.MustExec("GRANT system_admin TO 'user1'@'localhost';")
	tk.MustQuery("SELECT FROM_USER FROM mysql.role_edges WHERE TO_USER='user1' AND TO_HOST='localhost';").Check(testkit.Rows("system_admin"))

	// Test 2: Grant the same role again should succeed (idempotent)
	tk.MustExec("GRANT system_admin TO 'user1'@'localhost';")

	// Test 3: Grant security_admin to user1 should fail (already has system_admin)
	err := tk.ExecToErr("GRANT security_admin TO 'user1'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "already has role system_admin"))
	require.True(t, strings.Contains(err.Error(), "cannot grant role security_admin"))

	// Test 4: Grant audit_admin to user1 should fail (already has system_admin)
	err = tk.ExecToErr("GRANT audit_admin TO 'user1'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "already has role system_admin"))
	require.True(t, strings.Contains(err.Error(), "cannot grant role audit_admin"))

	// Test 5: Grant security_admin to user2 should succeed (no existing exclusive roles)
	tk.MustExec("GRANT security_admin TO 'user2'@'localhost';")
	tk.MustQuery("SELECT FROM_USER FROM mysql.role_edges WHERE TO_USER='user2' AND TO_HOST='localhost';").Check(testkit.Rows("security_admin"))

	// Test 6: Grant system_admin to user2 should fail (already has security_admin)
	err = tk.ExecToErr("GRANT system_admin TO 'user2'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "already has role security_admin"))
	require.True(t, strings.Contains(err.Error(), "cannot grant role system_admin"))

	// Test 7: Grant audit_admin to user3 should succeed
	tk.MustExec("GRANT audit_admin TO 'user3'@'localhost';")
	tk.MustQuery("SELECT FROM_USER FROM mysql.role_edges WHERE TO_USER='user3' AND TO_HOST='localhost';").Check(testkit.Rows("audit_admin"))

	// Test 8: Grant system_admin to user3 should fail (already has audit_admin)
	err = tk.ExecToErr("GRANT system_admin TO 'user3'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "already has role audit_admin"))
	require.True(t, strings.Contains(err.Error(), "cannot grant role system_admin"))

	// Test 9: Multiple users in one grant statement - one has conflict
	tk.MustExec("CREATE USER 'user4'@'localhost';")
	err = tk.ExecToErr("GRANT security_admin TO 'user1'@'localhost', 'user4'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	// Verify user4 did not get the role (transaction should rollback)
	tk.MustQuery("SELECT COUNT(*) FROM mysql.role_edges WHERE TO_USER='user4' AND TO_HOST='localhost';").Check(testkit.Rows("0"))

	// Test 10: Multiple exclusive roles in one grant statement - should fail
	tk.MustExec("CREATE USER 'user5'@'localhost';")
	err = tk.ExecToErr("GRANT security_admin, system_admin TO 'user5'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Clean up
	tk.MustExec("DROP USER 'user1'@'localhost';")
	tk.MustExec("DROP USER 'user2'@'localhost';")
	tk.MustExec("DROP USER 'user3'@'localhost';")
	tk.MustExec("DROP USER 'user4'@'localhost';")
	tk.MustExec("DROP USER 'user5'@'localhost';")
}

func TestGrantDutyRolesOnlyByRoot(t *testing.T) {
	restoreFunc := executor.MockSetDutySeparationModeEnable()
	defer restoreFunc()

	audit.Register4Test()

	store := testkit.CreateMockStore(t)
	rootTk := testkit.NewTestKit(t, store)

	rootTk.MustExec("CREATE USER 'grantor'@'%'")
	rootTk.MustExec("CREATE USER 'target'@'%'")
	rootTk.MustExec("GRANT ROLE_ADMIN ON *.* TO 'grantor'@'%'")

	grantorTk := testkit.NewTestKit(t, store)
	require.NoError(t, grantorTk.Session().Auth(&auth.UserIdentity{Username: "grantor", Hostname: "%"}, nil, nil, nil))

	roles := []string{"system_admin", "security_admin", "database_admin", "audit_admin"}
	for _, role := range roles {
		err := grantorTk.ExecToErr(fmt.Sprintf("GRANT %s TO 'target'@'%%';", role))
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
		require.True(t, strings.Contains(err.Error(), "only root user"))
	}

	rootTk.MustExec("DROP USER 'grantor'@'%'")
	rootTk.MustExec("DROP USER 'target'@'%'")
}

func TestWritePrivilegeConflictWithSpecialRoles(t *testing.T) {
	restoreFunc := executor.MockSetDutySeparationModeEnable()
	defer restoreFunc()

	// register extensions
	audit.Register4Test()

	store := testkit.CreateMockStore(t)
	rootTk := testkit.NewTestKit(t, store)

	// Create test database and table for privilege testing
	rootTk.MustExec("CREATE DATABASE IF NOT EXISTS testdb;")
	rootTk.MustExec("CREATE TABLE IF NOT EXISTS testdb.t1 (id INT);")

	// Create test users
	rootTk.MustExec("CREATE USER 'user_write'@'localhost';")
	rootTk.MustExec("CREATE USER 'user_role'@'localhost';")
	rootTk.MustExec("CREATE USER 'user_clean'@'localhost';")

	// Test 1: Grant write privilege to user, then try to grant special role - should fail
	rootTk.MustExec("GRANT INSERT ON testdb.* TO 'user_write'@'localhost';")
	err := rootTk.ExecToErr("GRANT system_admin TO 'user_write'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "INSERT/UPDATE/DELETE privileges"))

	// Test 2: Grant special role to user, then try to grant write privilege - should fail
	rootTk.MustExec("GRANT security_admin TO 'user_role'@'localhost';")
	err = rootTk.ExecToErr("GRANT UPDATE ON testdb.* TO 'user_role'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "has role security_admin"))

	// Test 3: Grant DELETE privilege should also conflict
	err = rootTk.ExecToErr("GRANT DELETE ON testdb.t1 TO 'user_role'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 4: Granting write privilege on mysql database should be allowed
	rootTk.MustExec("GRANT INSERT ON mysql.* TO 'user_role'@'localhost';")
	rootTk.MustQuery("SELECT Insert_priv FROM mysql.db WHERE User='user_role' AND DB='mysql';").Check(testkit.Rows("Y"))

	// Test 5: Granting SELECT privilege on user databases should also conflict
	err = rootTk.ExecToErr("GRANT SELECT ON testdb.* TO 'user_role'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 6: Test with audit_admin role
	rootTk.MustExec("CREATE USER 'user_audit'@'localhost';")
	rootTk.MustExec("GRANT audit_admin TO 'user_audit'@'localhost';")
	err = rootTk.ExecToErr("GRANT INSERT, UPDATE, DELETE ON testdb.* TO 'user_audit'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 7: database_admin should not have this restriction
	rootTk.MustExec("CREATE USER 'user_dbadmin'@'localhost';")
	rootTk.MustExec("GRANT database_admin TO 'user_dbadmin'@'localhost';")
	rootTk.MustExec("GRANT INSERT, UPDATE ON testdb.* TO 'user_dbadmin'@'localhost';")
	rootTk.MustQuery("SELECT Insert_priv, Update_priv FROM mysql.db WHERE User='user_dbadmin' AND DB='testdb';").
		Check(testkit.Rows("Y Y"))

	// Test 8: Granting ALL privileges should also fail for special roles
	err = rootTk.ExecToErr("GRANT ALL ON testdb.* TO 'user_role'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 9: Test with global privileges
	rootTk.MustExec("CREATE USER 'user_global_write'@'localhost';")
	rootTk.MustExec("GRANT UPDATE ON *.* TO 'user_global_write'@'localhost';")
	err = rootTk.ExecToErr("GRANT system_admin TO 'user_global_write'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 10: Grant special role first, then try global write privilege
	rootTk.MustExec("CREATE USER 'user_role_global'@'localhost';")
	rootTk.MustExec("GRANT audit_admin TO 'user_role_global'@'localhost';")
	err = rootTk.ExecToErr("GRANT DELETE ON *.* TO 'user_role_global'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 11: User with no conflict should work fine
	rootTk.MustExec("GRANT system_admin TO 'user_clean'@'localhost';")
	rootTk.MustQuery("SELECT FROM_USER FROM mysql.role_edges WHERE TO_USER='user_clean' AND TO_HOST='localhost';").
		Check(testkit.Rows("system_admin"))

	// Test 12: User with SELECT privilege on user database should not be granted special roles
	rootTk.MustExec("CREATE USER 'user_select'@'localhost';")
	rootTk.MustExec("GRANT SELECT ON testdb.* TO 'user_select'@'localhost';")
	err = rootTk.ExecToErr("GRANT security_admin TO 'user_select'@'localhost';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Clean up
	rootTk.MustExec("DROP USER 'user_write'@'localhost';")
	rootTk.MustExec("DROP USER 'user_role'@'localhost';")
	rootTk.MustExec("DROP USER 'user_clean'@'localhost';")
	rootTk.MustExec("DROP USER 'user_audit'@'localhost';")
	rootTk.MustExec("DROP USER 'user_dbadmin'@'localhost';")
	rootTk.MustExec("DROP USER 'user_global_write'@'localhost';")
	rootTk.MustExec("DROP USER 'user_role_global'@'localhost';")
	rootTk.MustExec("DROP USER 'user_select'@'localhost';")
	rootTk.MustExec("DROP DATABASE testdb;")
}

// TestRolePrivilegeConflictWithSpecialRoles tests that roles with data privileges
// cannot be granted to users with special admin roles
func TestRolePrivilegeConflictWithSpecialRoles(t *testing.T) {
	audit.Register4Test()
	defer executor.MockSetDutySeparationModeEnable()()

	store := testkit.CreateMockStore(t)
	rootTk := testkit.NewTestKit(t, store)

	// Setup test database
	rootTk.MustExec("CREATE DATABASE IF NOT EXISTS test;")
	rootTk.MustExec("USE test;")
	rootTk.MustExec("CREATE TABLE IF NOT EXISTS t1 (id INT);")

	// Test 1: Create a role with INSERT/UPDATE privileges, should not be grantable to user with security_admin
	rootTk.MustExec("CREATE ROLE 'data_writer';")
	rootTk.MustExec("GRANT INSERT, UPDATE ON test.* TO 'data_writer';")
	rootTk.MustExec("CREATE USER 'u_tester'@'%';")
	rootTk.MustExec("GRANT security_admin TO 'u_tester'@'%';")

	err := rootTk.ExecToErr("GRANT 'data_writer' TO 'u_tester'@'%';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "cannot grant role data_writer"))

	// Test 2: Create a role with SELECT privilege, should not be grantable to user with audit_admin
	rootTk.MustExec("CREATE ROLE 'data_reader';")
	rootTk.MustExec("GRANT SELECT ON test.* TO 'data_reader';")
	rootTk.MustExec("CREATE USER 'u_auditor'@'%';")
	rootTk.MustExec("GRANT audit_admin TO 'u_auditor'@'%';")

	err = rootTk.ExecToErr("GRANT 'data_reader' TO 'u_auditor'@'%';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "cannot grant role data_reader"))

	// Test 3: Role with DELETE privilege should not be grantable to user with system_admin
	rootTk.MustExec("CREATE ROLE 'data_deleter';")
	rootTk.MustExec("GRANT DELETE ON test.* TO 'data_deleter';")
	rootTk.MustExec("CREATE USER 'u_sysadmin'@'%';")
	rootTk.MustExec("GRANT system_admin TO 'u_sysadmin'@'%';")

	err = rootTk.ExecToErr("GRANT 'data_deleter' TO 'u_sysadmin'@'%';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 4: Role without data privileges should be grantable to users with special roles
	rootTk.MustExec("CREATE ROLE 'clean_role';")
	rootTk.MustExec("GRANT CREATE ON test.* TO 'clean_role';")
	rootTk.MustExec("GRANT 'clean_role' TO 'u_tester'@'%';")
	rootTk.MustQuery("SELECT FROM_USER FROM mysql.role_edges WHERE TO_USER='u_tester' AND FROM_USER='clean_role';").
		Check(testkit.Rows("clean_role"))

	// Test 5: Role with privileges on mysql database should be OK
	rootTk.MustExec("CREATE ROLE 'mysql_role';")
	rootTk.MustExec("GRANT SELECT, INSERT ON mysql.* TO 'mysql_role';")
	rootTk.MustExec("GRANT 'mysql_role' TO 'u_tester'@'%';")
	rootTk.MustQuery("SELECT FROM_USER FROM mysql.role_edges WHERE TO_USER='u_tester' AND FROM_USER='mysql_role';").
		Check(testkit.Rows("mysql_role"))

	// Test 6: database_admin should be allowed to be granted roles with data privileges
	rootTk.MustExec("CREATE USER 'u_dbadmin'@'%';")
	rootTk.MustExec("GRANT database_admin TO 'u_dbadmin'@'%';")
	rootTk.MustExec("GRANT 'data_writer' TO 'u_dbadmin'@'%';")
	rootTk.MustQuery("SELECT FROM_USER FROM mysql.role_edges WHERE TO_USER='u_dbadmin' AND FROM_USER='data_writer';").
		Check(testkit.Rows("data_writer"))

	// Test 7: Granting data privileges to a role that is already granted to a user with special admin role should fail
	rootTk.MustExec("CREATE ROLE 'future_data_role';")
	rootTk.MustExec("CREATE USER 'u_special'@'%';")
	rootTk.MustExec("GRANT security_admin TO 'u_special'@'%';")
	rootTk.MustExec("GRANT 'future_data_role' TO 'u_special'@'%';")

	err = rootTk.ExecToErr("GRANT SELECT ON test.* TO 'future_data_role';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))
	require.True(t, strings.Contains(err.Error(), "cannot grant SELECT/INSERT/UPDATE/DELETE privileges to role future_data_role"))

	// Test 8: Granting INSERT privilege to a role used by user with audit_admin should fail
	rootTk.MustExec("CREATE ROLE 'another_role';")
	rootTk.MustExec("CREATE USER 'u_audit2'@'%';")
	rootTk.MustExec("GRANT audit_admin TO 'u_audit2'@'%';")
	rootTk.MustExec("GRANT 'another_role' TO 'u_audit2'@'%';")

	err = rootTk.ExecToErr("GRANT INSERT, UPDATE ON test.* TO 'another_role';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 9: Role with global SELECT privilege
	rootTk.MustExec("CREATE ROLE 'global_reader';")
	rootTk.MustExec("GRANT SELECT ON *.* TO 'global_reader';")

	err = rootTk.ExecToErr("GRANT 'global_reader' TO 'u_tester'@'%';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Test 10: Role with data privileges should not be grantable to another user with special admin role.
	rootTk.MustExec("CREATE USER 'u_data_reader'@'%';")
	rootTk.MustExec("GRANT 'data_reader' TO 'u_data_reader'@'%';")
	err = rootTk.ExecToErr("GRANT 'security_admin' TO 'u_data_reader'@'%';")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "violation of separation of duty"))

	// Clean up
	rootTk.MustExec("DROP ROLE 'data_writer';")
	rootTk.MustExec("DROP ROLE 'data_reader';")
	rootTk.MustExec("DROP ROLE 'data_deleter';")
	rootTk.MustExec("DROP ROLE 'clean_role';")
	rootTk.MustExec("DROP ROLE 'mysql_role';")
	rootTk.MustExec("DROP ROLE 'future_data_role';")
	rootTk.MustExec("DROP ROLE 'another_role';")
	rootTk.MustExec("DROP ROLE 'global_reader';")
	rootTk.MustExec("DROP USER 'u_tester'@'%';")
	rootTk.MustExec("DROP USER 'u_auditor'@'%';")
	rootTk.MustExec("DROP USER 'u_sysadmin'@'%';")
	rootTk.MustExec("DROP USER 'u_dbadmin'@'%';")
	rootTk.MustExec("DROP USER 'u_special'@'%';")
	rootTk.MustExec("DROP USER 'u_audit2'@'%';")
	rootTk.MustExec("DROP USER 'u_data_reader'@'%';")
}

func extractGrantPrivileges(stmt string) ([]string, bool) {
	stmt = strings.TrimSpace(stmt)
	stmtUpper := strings.ToUpper(stmt)
	if !strings.HasPrefix(stmtUpper, "GRANT ") {
		return nil, false
	}
	onIndex := strings.Index(stmtUpper, " ON ")
	if onIndex == -1 {
		return nil, false
	}
	privPart := strings.TrimSpace(stmt[len("GRANT "):onIndex])
	if privPart == "" {
		return nil, false
	}

	parts := strings.Split(privPart, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts, true
}
