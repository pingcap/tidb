// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMFAUserFactorLifecycle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("CREATE USER 'mfa_lifecycle'@'%' IDENTIFIED BY 'first' AND IDENTIFIED WITH authentication_ldap_simple AS 'uid=second'")
	tk.MustQuery("SELECT JSON_UNQUOTE(user_attributes->'$.multi_factor_authentication[0].plugin'), JSON_UNQUOTE(user_attributes->'$.multi_factor_authentication[0].authentication_string') FROM mysql.user WHERE User='mfa_lifecycle'").Check(testkit.Rows("authentication_ldap_simple uid=second"))

	tk.MustExec("ALTER USER 'mfa_lifecycle'@'%' ADD 3 FACTOR IDENTIFIED WITH authentication_ldap_simple AS 'uid=third'")
	tk.MustExec("ALTER USER 'mfa_lifecycle'@'%' MODIFY 3 FACTOR IDENTIFIED WITH authentication_ldap_simple AS 'uid=third-new' MODIFY 2 FACTOR IDENTIFIED WITH authentication_ldap_simple AS 'uid=second-new'")
	tk.MustQuery("SELECT JSON_UNQUOTE(user_attributes->'$.multi_factor_authentication[0].authentication_string'), JSON_UNQUOTE(user_attributes->'$.multi_factor_authentication[1].authentication_string') FROM mysql.user WHERE User='mfa_lifecycle'").Check(testkit.Rows("uid=second-new uid=third-new"))

	tk.MustExec("ALTER USER 'mfa_lifecycle'@'%' DROP 2 FACTOR DROP 3 FACTOR")
	row := tk.MustQuery("SELECT user_attributes->'$.multi_factor_authentication' FROM mysql.user WHERE User='mfa_lifecycle'").Rows()
	require.Len(t, row, 1)
	require.Equal(t, "<nil>", row[0][0])
}
