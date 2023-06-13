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

package dbutil

import (
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/stretchr/testify/require"
)

func TestGetRedactedSQL(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"select * from t", "select * from t"},
		{"import into t from ':'", "import into t from ':'"},
		{"import into t from 's3://b/a.csv?access-key=123'", "IMPORT INTO `t` FROM 's3://b/a.csv?access-key=redacted'"},
		{"import into t from 's3://b/a.csv?secret-access-key=123'", "IMPORT INTO `t` FROM 's3://b/a.csv?secret-access-key=redacted'"},
		{"import into t from 's3://b/a.csv?secret-access-key=123' format 'csv'", "IMPORT INTO `t` FROM 's3://b/a.csv?secret-access-key=redacted' FORMAT 'csv'"},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			p := parser.New()
			stmt, err := p.ParseOneStmt(tt.sql, "", "")
			require.NoError(t, err)
			if got := GetRedactedSQL(stmt); got != tt.want {
				t.Errorf("GetRedactedSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
