// Copyright 2022 PingCAP, Inc.
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

func TestIndex(t *testing.T) {
	testCases := []struct {
		sql     string
		indices []string
		cols    []string
	}{
		{
			`
 			CREATE TABLE itest (a int(11) NOT NULL,
 			b double NOT NULL DEFAULT '2',
 			c varchar(10) NOT NULL,
 			d time DEFAULT NULL,
 			PRIMARY KEY (a, b),
 			UNIQUE KEY d(d))
 			`,
			[]string{"PRIMARY", "d"},
			[]string{"a", "b", "d"},
		}, {
			`
 			CREATE TABLE jtest (
 				a int(11) NOT NULL,
 				b varchar(10) DEFAULT NULL,
 				c varchar(255) DEFAULT NULL,
 				KEY c(c),
 				UNIQUE KEY b(b, c),
 				PRIMARY KEY (a)
 			) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
 			`,
			[]string{"PRIMARY", "b", "c"},
			[]string{"a", "b", "c"},
		}, {
			`
 			CREATE TABLE mtest (
 				a int(24),
 				KEY test (a))
 			`,
			[]string{"test"},
			[]string{"a"},
		},
		{
			`
 			CREATE TABLE mtest (
				a int(24),
				b int(24),
				KEY test1 (a),
				KEY test2 (b))
 			`,
			[]string{"test1", "test2"},
			[]string{"a", "b"},
		},
		{
			`
 			CREATE TABLE mtest (
				a int(24),
				b int(24),
				UNIQUE KEY test1 (a),
				UNIQUE KEY test2 (b))
 			`,
			[]string{"test1", "test2"},
			[]string{"a", "b"},
		},
	}

	for _, testCase := range testCases {
		tableInfo, err := GetTableInfoBySQL(testCase.sql, parser.New())
		require.NoError(t, err)

		indices := FindAllIndex(tableInfo)
		for i, index := range indices {
			require.Equal(t, testCase.indices[i], index.Name.O)
		}

		cols := FindAllColumnWithIndex(tableInfo)
		for j, col := range cols {
			require.Equal(t, testCase.cols[j], col.Name.O)
		}
	}
}
