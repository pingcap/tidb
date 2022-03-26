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
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/schemacmp"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	sql     string
	columns []string
	indexs  []string
	colLen  [][]int
	colName string
	fineCol bool
}

func TestTable(t *testing.T) {
	testCases := []*testCase{
		{
			`
			CREATE TABLE htest (
				a int(11) PRIMARY KEY
			) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
			`,
			[]string{"a"},
			[]string{mysql.PrimaryKeyName},
			[][]int{{types.UnspecifiedLength}},
			"c",
			false,
		}, {
			`
			CREATE TABLE itest (a int(11) NOT NULL,
				b double NOT NULL DEFAULT '2',
				c varchar(10) NOT NULL,
				d time DEFAULT NULL,
				PRIMARY KEY (a, b),
				UNIQUE KEY d (d))
			`,
			[]string{"a", "b", "c", "d"},
			[]string{mysql.PrimaryKeyName, "d"},
			[][]int{{types.UnspecifiedLength, types.UnspecifiedLength}, {types.UnspecifiedLength}},
			"a",
			true,
		}, {
			`
			CREATE TABLE jtest (
				a int(11) NOT NULL,
				b varchar(10) DEFAULT NULL,
				c varchar(255) DEFAULT NULL,
				PRIMARY KEY (a)
			) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
			`,
			[]string{"a", "b", "c"},
			[]string{mysql.PrimaryKeyName},
			[][]int{{types.UnspecifiedLength}},
			"c",
			true,
		}, {
			`
			CREATE TABLE mtest (
				a int(24),
				KEY test (a))
			`,
			[]string{"a"},
			[]string{"test"},
			[][]int{{types.UnspecifiedLength}},
			"d",
			false,
		}, {
			`
			CREATE TABLE ntest (
				a int(24) PRIMARY KEY CLUSTERED
			)
			`,
			[]string{"a"},
			[]string{mysql.PrimaryKeyName},
			[][]int{{types.UnspecifiedLength}},
			"d",
			false,
		}, {
			`
			CREATE TABLE otest (
				a int(11) NOT NULL,
				b varchar(10) DEFAULT NULL,
				c varchar(255) DEFAULT NULL,
				PRIMARY KEY (a)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
			`,
			[]string{"a", "b", "c"},
			[]string{mysql.PrimaryKeyName},
			[][]int{{types.UnspecifiedLength}},
			"c",
			true,
		},
	}

	for _, testCase := range testCases {
		tableInfo, err := GetTableInfoBySQL(testCase.sql, parser.New())
		require.NoError(t, err)
		for i, column := range tableInfo.Columns {
			require.Equal(t, column.Name.O, testCase.columns[i])
		}

		require.Len(t, tableInfo.Indices, len(testCase.indexs))
		for j, index := range tableInfo.Indices {
			require.Equal(t, index.Name.O, testCase.indexs[j])
			for k, indexCol := range index.Columns {
				require.Equal(t, testCase.colLen[j][k], indexCol.Length)
			}
		}

		col := FindColumnByName(tableInfo.Columns, testCase.colName)
		require.Equal(t, col != nil, testCase.fineCol)
	}
}

func TestTableStructEqual(t *testing.T) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo1, err := GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`id` int(24) NOT NULL, `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo2, err := GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	createTableSQL3 := `CREATE TABLE "test"."atest" ("id" int(24), "name" varchar(24), "birthday" datetime, "update_time" time, "money" decimal(20,2), unique key("id"))`
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	tableInfo3, err := GetTableInfoBySQL(createTableSQL3, p)
	require.NoError(t, err)

	equal, _ := EqualTableInfo(tableInfo1, tableInfo2)
	require.Equal(t, true, equal)

	equal, _ = EqualTableInfo(tableInfo1, tableInfo3)
	require.Equal(t, false, equal)
}

func TestSchemacmpEncode(t *testing.T) {
	createTableSQL := "CREATE TABLE `test`.`atest` (`id` int(24), primary key(`id`))"
	tableInfo, err := GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	table := schemacmp.Encode(tableInfo)
	require.Equal(t, "CREATE TABLE `tbl`(`id` INT(24) NOT NULL, PRIMARY KEY (`id`)) CHARSET UTF8MB4 COLLATE UTF8MB4_BIN", table.String())
}
