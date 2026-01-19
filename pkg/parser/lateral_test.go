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

package parser_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

func TestLateralParsing(t *testing.T) {
	p := parser.New()

	testCases := []struct {
		name         string
		sql          string
		expectError  bool
		checkLateral bool // whether to verify Lateral flag is set
	}{
		{
			name:         "LATERAL with comma syntax",
			sql:          "SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt",
			expectError:  false,
			checkLateral: true,
		},
		{
			name:         "LATERAL with LEFT JOIN",
			sql:          "SELECT * FROM t1 LEFT JOIN LATERAL (SELECT t1.b) AS dt ON true",
			expectError:  false,
			checkLateral: true,
		},
		{
			name:         "LATERAL with CROSS JOIN",
			sql:          "SELECT * FROM t1 CROSS JOIN LATERAL (SELECT t1.c) AS dt",
			expectError:  false,
			checkLateral: true,
		},
		{
			name:         "LATERAL with RIGHT JOIN",
			sql:          "SELECT * FROM t1 RIGHT JOIN LATERAL (SELECT t1.d) AS dt ON true",
			expectError:  false, // Parser allows it, planner will reject
			checkLateral: true,
		},
		{
			name:         "LATERAL with INNER JOIN",
			sql:          "SELECT * FROM t1 JOIN LATERAL (SELECT t1.e) AS dt ON true",
			expectError:  false,
			checkLateral: true,
		},
		{
			name:         "LATERAL with complex subquery",
			sql:          "SELECT * FROM t1, LATERAL (SELECT t1.a, COUNT(*) FROM t2 WHERE t2.x = t1.x GROUP BY t1.a) AS dt",
			expectError:  false,
			checkLateral: true,
		},
		{
			name:         "LATERAL with nested subquery",
			sql:          "SELECT * FROM t1, LATERAL (SELECT * FROM (SELECT t1.a) AS inner_dt) AS dt",
			expectError:  false,
			checkLateral: true,
		},
		{
			name:         "Multiple LATERAL joins",
			sql:          "SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt1, LATERAL (SELECT t1.b) AS dt2",
			expectError:  false,
			checkLateral: true,
		},
		{
			name:        "Non-LATERAL derived table",
			sql:         "SELECT * FROM t1, (SELECT a FROM t2) AS dt",
			expectError: false,
			// Lateral flag should be false for non-LATERAL
		},
		{
			name:         "LATERAL with WHERE clause",
			sql:          "SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.x = t1.x) AS dt WHERE dt.y > 10",
			expectError:  false,
			checkLateral: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(tc.sql, "", "")

			if tc.expectError {
				require.Error(t, err, "Expected parsing to fail for: %s", tc.sql)
				return
			}

			require.NoError(t, err, "Failed to parse: %s", tc.sql)
			require.NotNil(t, stmt)

			// Test round-trip: parse -> restore -> parse again
			var sb strings.Builder
			restoreCtx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &sb)
			err = stmt.Restore(restoreCtx)
			require.NoError(t, err, "Failed to restore statement")

			restored := sb.String()
			if tc.checkLateral {
				// Verify LATERAL keyword is preserved in restoration
				require.Contains(t, restored, "LATERAL", "LATERAL keyword missing in restored SQL: %s", restored)
			}

			// Parse the restored SQL to ensure it's valid (round-trip test)
			stmt2, err := p.ParseOneStmt(restored, "", "")
			require.NoError(t, err, "Failed to parse restored SQL: %s", restored)
			require.NotNil(t, stmt2)

			// Verify AST flag: check if LATERAL table sources exist
			if tc.checkLateral {
				selectStmt, ok := stmt.(*ast.SelectStmt)
				require.True(t, ok, "Statement should be SelectStmt")
				require.NotNil(t, selectStmt.From, "FROM clause should not be nil")

				// Verify at least one LATERAL table source exists in the FROM clause
				foundLateral := checkLateralFlag(selectStmt.From.TableRefs)
				require.True(t, foundLateral, "LATERAL flag not set in AST for: %s", tc.sql)
			}
		})
	}
}

// checkLateralFlag recursively searches for LATERAL table sources in a ResultSetNode
func checkLateralFlag(node ast.ResultSetNode) bool {
	if node == nil {
		return false
	}

	switch n := node.(type) {
	case *ast.TableSource:
		// Check if this TableSource has the Lateral flag set
		if n.Lateral {
			return true
		}
		// Recursively check the Source within TableSource
		return checkLateralFlag(n.Source)
	case *ast.Join:
		// Check both left and right sides of join
		return checkLateralFlag(n.Left) || checkLateralFlag(n.Right)
	}

	return false
}
