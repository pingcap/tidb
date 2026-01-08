// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/stretchr/testify/require"
)

func TestSQLRules(t *testing.T) {
	type testCase struct {
		rule     SQLRule
		stmt     string
		expected bool
	}

	testCases := []testCase{
		{
			rule:     TimeToLiveSQLRule,
			stmt:     "CREATE TABLE t (a DATETIME) TTL = a + INTERVAL 1 DAY",
			expected: true,
		},
		{
			rule:     TimeToLiveSQLRule,
			stmt:     "ALTER TABLE t TTL = a + INTERVAL 1 DAY",
			expected: true,
		},
		{
			rule:     TimeToLiveSQLRule,
			stmt:     "ALTER TABLE t REMOVE TTL",
			expected: true,
		},
		{
			rule:     AlterTableAttributesRule,
			stmt:     "ALTER TABLE t ATTRIBUTES 'merge_option=deny'",
			expected: true,
		},
		{
			rule:     ImportWithExternalIDRule,
			stmt:     "IMPORT INTO xxxx FROM 's3://xxx/xxx?external-id=xxx'",
			expected: true,
		},
		{
			rule:     SelectIntoFileRule,
			stmt:     "SELECT * FROM t1 INTO OUTFILE '/tmp/t1.txt' ",
			expected: true,
		},
		{
			rule:     ImportFromLocalRule,
			stmt:     "IMPORT INTO t1 FROM '/bucket/path/to/file.csv'",
			expected: true,
		},
		{
			rule:     ImportFromLocalRule,
			stmt:     "IMPORT INTO t1 FROM 'file:///bucket/path/to/file.csv'",
			expected: true,
		},
		{
			rule:     ImportFromLocalRule,
			stmt:     "LOAD DATA INFILE '/bucket/path/to/file.csv' INTO TABLE t1",
			expected: true,
		},
		{
			rule:     ImportFromLocalRule,
			stmt:     "LOAD DATA INFILE 'file:///bucket/path/to/file.csv' INTO TABLE t1",
			expected: true,
		},
		{
			rule:     ImportFromLocalRule,
			stmt:     "LOAD DATA LOCAL INFILE 'file:///bucket/path/to/file.csv' INTO TABLE t1",
			expected: false,
		},
	}

	charset, collate := charset.GetDefaultCharsetAndCollate()
	for _, c := range testCases {
		p := parser.New()

		stmt, err := p.ParseOneStmt(c.stmt, charset, collate)
		if err != nil {
			panic(err)
		}
		result := c.rule(stmt)
		require.Equal(t, c.expected, result, "SQL rule %s failed for statement: %s", c.rule, c.stmt)
	}
}
