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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/schemacmp"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDBSuite{})

type testDBSuite struct{}

type testCase struct {
	sql     string
	columns []string
	indexs  []string
	colLen  [][]int
	colName string
	fineCol bool
}

func (*testDBSuite) TestTable(c *C) {
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
		c.Assert(err, IsNil)
		for i, column := range tableInfo.Columns {
			c.Assert(testCase.columns[i], Equals, column.Name.O)
		}

		c.Assert(tableInfo.Indices, HasLen, len(testCase.indexs))
		for j, index := range tableInfo.Indices {
			c.Assert(testCase.indexs[j], Equals, index.Name.O)
			for k, indexCol := range index.Columns {
				c.Assert(indexCol.Length, Equals, testCase.colLen[j][k])
			}
		}

		col := FindColumnByName(tableInfo.Columns, testCase.colName)
		c.Assert(testCase.fineCol, Equals, col != nil)
	}
}

func (*testDBSuite) TestTableStructEqual(c *C) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo1, err := GetTableInfoBySQL(createTableSQL1, parser.New())
	c.Assert(err, IsNil)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`id` int(24) NOT NULL, `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo2, err := GetTableInfoBySQL(createTableSQL2, parser.New())
	c.Assert(err, IsNil)

	createTableSQL3 := `CREATE TABLE "test"."atest" ("id" int(24), "name" varchar(24), "birthday" datetime, "update_time" time, "money" decimal(20,2), unique key("id"))`
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	tableInfo3, err := GetTableInfoBySQL(createTableSQL3, p)
	c.Assert(err, IsNil)

	equal, _ := EqualTableInfo(tableInfo1, tableInfo2)
	c.Assert(equal, Equals, true)

	equal, _ = EqualTableInfo(tableInfo1, tableInfo3)
	c.Assert(equal, Equals, false)
}

func (*testDBSuite) TestSchemacmpEncode(c *C) {
	createTableSQL := "CREATE TABLE `test`.`atest` (`id` int(24), primary key(`id`))"
	tableInfo, err := GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, IsNil)

	table := schemacmp.Encode(tableInfo)
	c.Assert(table.String(), Equals, "CREATE TABLE `tbl`(`id` INT(24) NOT NULL, PRIMARY KEY (`id`)) CHARSET UTF8MB4 COLLATE UTF8MB4_BIN")
}
