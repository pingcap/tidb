// Copyright 2023 PingCAP, Inc.
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

package internal

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/stretchr/testify/require"
)

// UtilCleanBindingEnv cleans the binding environment.
func UtilCleanBindingEnv(tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec("delete from mysql.bind_info where source != 'builtin'")
	dom.BindHandle().Clear()
}

// UtilNormalizeWithDefaultDB normalizes the SQL and returns the normalized SQL and its digest.
func UtilNormalizeWithDefaultDB(t *testing.T, sql string) (stmt ast.StmtNode, normalized, digest string) {
	testParser := parser.New()
	stmt, err := testParser.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	normalized, digestResult := parser.NormalizeDigestForBinding(utilparser.RestoreWithDefaultDB(stmt, "test", ""))
	return stmt, normalized, digestResult.String()
}
