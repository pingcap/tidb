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

package sem

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestRestrictedHint(t *testing.T) {
	sem := buildSEMFromConfig(&Config{
		RestrictedVariables: []VariableRestriction{
			{Name: vardef.TiDBMemQuotaQuery, Hidden: true},
		},
		RestrictedHints: []string{"resource_group", "memory_quota", "max_execution_time"},
	})

	// A hint with no backing variable is restricted unconditionally.
	require.Error(t, sem.isRestrictedHint("resource_group"))
	// A variable-overriding hint whose variable is hidden is restricted.
	require.Error(t, sem.isRestrictedHint("memory_quota"))
	// A variable-overriding hint whose variable is still tunable is allowed.
	require.NoError(t, sem.isRestrictedHint("max_execution_time"))
	// A hint not listed in restricted_hints is allowed.
	require.NoError(t, sem.isRestrictedHint("use_index"))
}

func TestRestrictedUserStmt(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("starter username policy is nextgen-only")
	}

	restoreConfig := config.RestoreFunc()
	originalMode := deploymode.Get()
	t.Cleanup(func() {
		restoreConfig()
		require.NoError(t, deploymode.Set(originalMode))
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "ks"
	})
	require.NoError(t, deploymode.Set(deploymode.Starter))

	sem := buildSEMFromConfig(&Config{
		RestrictedUsers: []string{"root", "cloud_admin"},
		RestrictedRoles: []string{"cloud_admin"},
	})

	p := parser.New()
	cs, collate := charset.GetDefaultCharsetAndCollate()
	mustParse := func(sql string) ast.StmtNode {
		stmt, err := p.ParseOneStmt(sql, cs, collate)
		require.NoError(t, err, sql)
		return stmt
	}

	restrictedSQL := []string{
		"DROP USER 'ks.root'@'%'",
		"RENAME USER 'ks.cloud_admin'@'%' TO 'ks.cloud_admin2'@'%'",
		"GRANT 'ks.app_role'@'%' TO 'ks.root'@'%'",
		"REVOKE 'ks.app_role'@'%' FROM 'ks.cloud_admin'@'%'",
		"SET DEFAULT ROLE 'ks.app_role'@'%' TO 'ks.root'@'%'",
		"GRANT 'ks.cloud_admin'@'%' TO 'ks.app_user'@'%'",
		"REVOKE 'ks.cloud_admin'@'%' FROM 'ks.app_user'@'%'",
		"SET ROLE 'ks.cloud_admin'@'%'",
	}
	for _, sql := range restrictedSQL {
		require.Error(t, sem.checkRestrictedUserStmt(mustParse(sql)), sql)
	}

	// Wildcard role activation is blocked outright because the expansion may
	// include a protected role.
	require.Error(t, sem.checkRestrictedUserStmt(mustParse("SET ROLE ALL")))
	require.Error(t, sem.checkRestrictedUserStmt(mustParse("SET ROLE DEFAULT")))
	require.Error(t, sem.checkRestrictedUserStmt(mustParse("SET DEFAULT ROLE ALL TO u")))

	// Unrelated statements are unaffected.
	allowedSQL := []string{
		"SELECT 1",
		"SET ROLE NONE",
		"DROP USER 'ks.app_user'@'%'",
		"RENAME USER 'ks.app_user'@'%' TO 'ks.app_user2'@'%'",
		"GRANT 'ks.app_role'@'%' TO 'ks.app_user'@'%'",
		"REVOKE 'ks.app_role'@'%' FROM 'ks.app_user'@'%'",
		"SET DEFAULT ROLE 'ks.app_role'@'%' TO 'ks.app_user'@'%'",
	}
	for _, sql := range allowedSQL {
		require.NoError(t, sem.checkRestrictedUserStmt(mustParse(sql)), sql)
	}

	// With nothing configured, the check is a no-op.
	require.NoError(t, buildSEMFromConfig(&Config{}).checkRestrictedUserStmt(mustParse("SET ROLE ALL")))
}
