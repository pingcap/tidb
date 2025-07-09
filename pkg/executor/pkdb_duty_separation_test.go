// Copyright 2023-2023 PingCAP, Inc.

package executor_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/parser/auth"
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
