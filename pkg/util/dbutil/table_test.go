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

package dbutil_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/pingcap/tidb/pkg/util/schemacmp"
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
		tableInfo, err := dbutiltest.GetTableInfoBySQL(testCase.sql, parser.New())
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

		col := dbutil.FindColumnByName(tableInfo.Columns, testCase.colName)
		require.Equal(t, col != nil, testCase.fineCol)
	}
}

// EqualTableInfo returns true if this two table info have same columns and indices
func EqualTableInfo(tableInfo1, tableInfo2 *model.TableInfo) (bool, string) {
	// check columns
	if len(tableInfo1.Columns) != len(tableInfo2.Columns) {
		return false, fmt.Sprintf("column num not equal, one is %d another is %d", len(tableInfo1.Columns), len(tableInfo2.Columns))
	}

	for j, col := range tableInfo1.Columns {
		if col.Name.O != tableInfo2.Columns[j].Name.O {
			return false, fmt.Sprintf("column name not equal, one is %s another is %s", col.Name.O, tableInfo2.Columns[j].Name.O)
		}
		if col.GetType() != tableInfo2.Columns[j].GetType() {
			return false, fmt.Sprintf("column %s's type not equal, one is %v another is %v", col.Name.O, col.GetType(), tableInfo2.Columns[j].GetType())
		}
	}

	// check index
	if len(tableInfo1.Indices) != len(tableInfo2.Indices) {
		return false, fmt.Sprintf("index num not equal, one is %d another is %d", len(tableInfo1.Indices), len(tableInfo2.Indices))
	}

	index2Map := make(map[string]*model.IndexInfo)
	for _, index := range tableInfo2.Indices {
		index2Map[index.Name.O] = index
	}

	for _, index1 := range tableInfo1.Indices {
		index2, ok := index2Map[index1.Name.O]
		if !ok {
			return false, fmt.Sprintf("index %s not exists", index1.Name.O)
		}

		if len(index1.Columns) != len(index2.Columns) {
			return false, fmt.Sprintf("index %s's columns num not equal, one is %d another is %d", index1.Name.O, len(index1.Columns), len(index2.Columns))
		}
		for j, col := range index1.Columns {
			if col.Name.O != index2.Columns[j].Name.O {
				return false, fmt.Sprintf("index %s's column not equal, one has %s another has %s", index1.Name.O, col.Name.O, index2.Columns[j].Name.O)
			}
		}
	}

	return true, ""
}

func TestTableStructEqual(t *testing.T) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo1, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`id` int(24) NOT NULL, `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	createTableSQL3 := `CREATE TABLE "test"."atest" ("id" int(24), "name" varchar(24), "birthday" datetime, "update_time" time, "money" decimal(20,2), unique key("id"))`
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	tableInfo3, err := dbutiltest.GetTableInfoBySQL(createTableSQL3, p)
	require.NoError(t, err)

	equal, _ := EqualTableInfo(tableInfo1, tableInfo2)
	require.Equal(t, true, equal)

	equal, _ = EqualTableInfo(tableInfo1, tableInfo3)
	require.Equal(t, false, equal)
}

func TestSchemacmpEncode(t *testing.T) {
	createTableSQL := "CREATE TABLE `test`.`atest` (`id` int(24), primary key(`id`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	table := schemacmp.Encode(tableInfo)
	require.Equal(t, "CREATE TABLE `tbl`(`id` INT(24) NOT NULL, PRIMARY KEY (`id`)) CHARSET UTF8MB4 COLLATE UTF8MB4_BIN", table.String())
}
