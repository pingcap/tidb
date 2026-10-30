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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

// TestAlterUserHasPrivilegedOptions enumerates every statement-level option
// family on ast.AlterUserStmt so that the allowlist in
// alterUserHasPrivilegedOptions cannot silently drift: if a new option field
// is added to the AST, extend both the helper and this table.
func TestAlterUserHasPrivilegedOptions(t *testing.T) {
	cases := []struct {
		sql        string
		privileged bool
	}{
		// Pure password / dual-password shapes: NOT privileged options.
		{"ALTER USER u IDENTIFIED BY 'x'", false},
		{"ALTER USER u IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD", false},
		{"ALTER USER u DISCARD OLD PASSWORD", false},
		{"ALTER USER USER() IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD", false},
		// AuthTokenOrTLSOptions.
		{"ALTER USER u REQUIRE SSL", true},
		{"ALTER USER u REQUIRE NONE", true},
		// ResourceOptions.
		{"ALTER USER u WITH MAX_USER_CONNECTIONS 10", true},
		// PasswordOrLockOptions.
		{"ALTER USER u PASSWORD EXPIRE", true},
		{"ALTER USER u ACCOUNT LOCK", true},
		{"ALTER USER u FAILED_LOGIN_ATTEMPTS 3", true},
		{"ALTER USER u PASSWORD HISTORY 5", true},
		// CommentOrAttributeOption.
		{"ALTER USER u COMMENT 'c'", true},
		{"ALTER USER u ATTRIBUTE '{\"k\": \"v\"}'", true},
		// ResourceGroupNameOption.
		{"ALTER USER u RESOURCE GROUP rg1", true},
		// Options combined with dual-password clauses stay privileged.
		{"ALTER USER u DISCARD OLD PASSWORD COMMENT 'c'", true},
		{"ALTER USER u IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD ACCOUNT LOCK", true},
	}
	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.sql, "", "")
		require.NoError(t, err, c.sql)
		alterStmt, ok := stmt.(*ast.AlterUserStmt)
		require.True(t, ok, c.sql)
		require.Equal(t, c.privileged, alterUserHasPrivilegedOptions(alterStmt), c.sql)
	}
}
