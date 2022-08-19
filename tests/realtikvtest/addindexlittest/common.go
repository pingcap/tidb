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

package addindexlittest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

const (
	tableNum      = 3
	nonPartTabNum = 1
)

type suiteContext struct {
	t           *testing.T
	tk          *testkit.TestKit
	IsUnique    bool
	IsPK        bool
	HasWorkload bool
	EndWorkload bool
	TableNum    int
	ColNum      int
	RowNum      int
	Wl          workload
}

func newSuiteContext(t *testing.T, tk *testkit.TestKit) *suiteContext {
	return &suiteContext{
		t:        t,
		tk:       tk,
		TableNum: 3,
		ColNum:   28,
		RowNum:   64,
	}
}

type workload interface {
	StartWorkload(ctx *suiteContext, ID ...int) error
	StopWorkload(ctx *suiteContext) error
}

func genTableStr(tableName string) string {
	tableDef := "create table addindexlit." + tableName + " (" +
		"c0 int, c1 bit(8), c2 boolean, c3 tinyint default 3, c4 smallint not null, c5 mediumint," +
		"c6 int, c7 bigint, c8 float, c9 double, c10 decimal(13,7), c11 date, c12 time, c13 datetime," +
		"c14 timestamp, c15 year, c16 char(10), c17 varchar(10), c18 text, c19 tinytext, c20 mediumtext," +
		"c21 longtext, c22 binary(20), c23 varbinary(30), c24 blob, c25 tinyblob, c26 MEDIUMBLOB, c27 LONGBLOB," +
		"c28 json, c29 INT AS (JSON_EXTRACT(c28, '$.population')))"
	return tableDef
}

func genPartTableStr() (tableDefs []string) {
	num := nonPartTabNum
	// Range table def
	tableDefs = append(tableDefs, "CREATE TABLE addindexlit.t"+strconv.Itoa(num)+" ("+
		"c0 int, c1 bit(8), c2 boolean, c3 tinyint default 3, c4 smallint not null, c5 mediumint,"+
		"c6 int, c7 bigint, c8 float, c9 double, c10 decimal(13,7), c11 date, c12 time, c13 datetime,"+
		"c14 timestamp, c15 year, c16 char(10), c17 varchar(10), c18 text, c19 tinytext, c20 mediumtext,"+
		"c21 longtext, c22 binary(20), c23 varbinary(30), c24 blob, c25 tinyblob, c26 MEDIUMBLOB, c27 LONGBLOB,"+
		"c28 json, c29 INT AS (JSON_EXTRACT(c28, '$.population')))"+
		" PARTITION BY RANGE (`c0`)"+
		" (PARTITION `p0` VALUES LESS THAN (10),"+
		" PARTITION `p1` VALUES LESS THAN (20),"+
		" PARTITION `p2` VALUES LESS THAN (30),"+
		" PARTITION `p3` VALUES LESS THAN (40),"+
		" PARTITION `p4` VALUES LESS THAN (50),"+
		" PARTITION `p5` VALUES LESS THAN (60),"+
		" PARTITION `p6` VALUES LESS THAN (70),"+
		" PARTITION `p7` VALUES LESS THAN (80),"+
		" PARTITION `p8` VALUES LESS THAN MAXVALUE)")
	num++
	// Hash part table
	tableDefs = append(tableDefs, "CREATE TABLE addindexlit.t"+strconv.Itoa(num)+" ("+
		"c0 int, c1 bit(8), c2 boolean, c3 tinyint default 3, c4 smallint not null, c5 mediumint,"+
		"c6 int, c7 bigint, c8 float, c9 double, c10 decimal(13,7), c11 date, c12 time, c13 datetime,"+
		"c14 timestamp, c15 year, c16 char(10), c17 varchar(10), c18 text, c19 tinytext, c20 mediumtext,"+
		"c21 longtext, c22 binary(20), c23 varbinary(30), c24 blob, c25 tinyblob, c26 MEDIUMBLOB, c27 LONGBLOB,"+
		"c28 json, c29 INT AS (JSON_EXTRACT(c28, '$.population')))"+
		" PARTITION BY HASH (c0) PARTITIONS 4")
	return
}

func createTable(ctx *suiteContext) {
	for i := 0; i < nonPartTabNum; i++ {
		tableName := "t" + strconv.Itoa(i)
		tableDef := genTableStr(tableName)
		_, err := ctx.tk.Exec(tableDef)
		require.NoError(ctx.t, err)
	}
	tableDefs := genPartTableStr()
	for _, tableDef := range tableDefs {
		_, err := ctx.tk.Exec(tableDef)
		require.NoError(ctx.t, err)
	}
}

func insertRows(ctx *suiteContext) {
	var (
		insStr string
		values []string = []string{
			" (1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 1111.1111, '2001-01-01', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
			" (2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 1112.1111, '2001-01-02', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
			" (3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 1113.1111, '2001-01-03', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
			" (4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 1114.1111, '2001-01-04', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
			" (5, 5, 1, 1, 1, 1, 5, 1, 1.0, 1.0, 1111.1111, '2001-01-05', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'eeee', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
			" (6, 2, 2, 2, 2, 2, 6, 2, 2.0, 2.0, 1112.1111, '2001-01-06', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'ffff', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
			" (7, 3, 3, 3, 3, 3, 7, 3, 3.0, 3.0, 1113.1111, '2001-01-07', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'gggg', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
			" (8, 4, 4, 4, 4, 4, 8, 4, 4.0, 4.0, 1114.1111, '2001-01-08', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'hhhh', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
			" (9, 1, 1, 1, 1, 1, 9, 1, 1.0, 1.0, 1111.1111, '2001-01-09', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'iiii', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
			" (10, 2, 2, 2, 2, 2, 10, 2, 2.0, 2.0, 1112.1111, '2001-01-10', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'jjjj', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
			" (11, 3, 3, 3, 3, 3, 11, 3, 3.0, 3.0, 1113.1111, '2001-01-11', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'kkkk', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
			" (12, 4, 4, 4, 4, 4, 12, 4, 4.0, 4.0, 1114.1111, '2001-01-12', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'llll', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
			" (13, 5, 1, 1, 1, 1, 13, 1, 1.0, 1.0, 1111.1111, '2001-01-13', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'mmmm', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
			" (14, 2, 2, 2, 2, 2, 14, 2, 2.0, 2.0, 1112.1111, '2001-01-14', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'nnnn', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
			" (15, 3, 3, 3, 3, 3, 15, 3, 3.0, 3.0, 1113.1111, '2001-01-15', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'oooo', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
			" (16, 4, 4, 4, 4, 4, 16, 4, 4.0, 4.0, 1114.1111, '2001-01-16', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'pppp', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
			" (17, 1, 1, 1, 1, 1, 17, 1, 1.0, 1.0, 1111.1111, '2001-01-17', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'qqqq', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
			" (18, 2, 2, 2, 2, 2, 18, 2, 2.0, 2.0, 1112.1111, '2001-01-18', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'rrrr', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
			" (19, 3, 3, 3, 3, 3, 19, 3, 3.0, 3.0, 1113.1111, '2001-01-19', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'ssss', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
			" (20, 4, 4, 4, 4, 4, 20, 4, 4.0, 4.0, 1114.1111, '2001-01-20', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'tttt', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
			" (21, 5, 1, 1, 1, 1, 21, 1, 1.0, 1.0, 1111.1111, '2001-01-21', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'uuuu', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
			" (22, 2, 2, 2, 2, 2, 22, 2, 2.0, 2.0, 1112.1111, '2001-01-22', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'vvvv', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
			" (23, 3, 3, 3, 3, 3, 23, 3, 3.0, 3.0, 1113.1111, '2001-01-23', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'wwww', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
			" (24, 4, 4, 4, 4, 4, 24, 4, 4.0, 4.0, 1114.1111, '2001-01-24', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'xxxx', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
			" (25, 1, 1, 1, 1, 1, 25, 1, 1.0, 1.0, 1111.1111, '2001-01-25', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'yyyy', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
			" (26, 2, 2, 2, 2, 2, 26, 2, 2.0, 2.0, 1112.1111, '2001-01-26', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'zzzz', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
			" (27, 3, 3, 3, 3, 3, 27, 3, 3.0, 3.0, 1113.1111, '2001-01-27', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaab', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
			" (28, 4, 4, 4, 4, 4, 28, 4, 4.0, 4.0, 1114.1111, '2001-01-28', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaac', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
			" (29, 5, 1, 1, 1, 1, 29, 1, 1.0, 1.0, 1111.1111, '2001-01-29', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaad', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
			" (30, 2, 2, 2, 2, 2, 30, 2, 2.0, 2.0, 1112.1111, '2001-01-30', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaae', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
			" (31, 3, 3, 3, 3, 3, 31, 3, 3.0, 3.0, 1113.1111, '2001-01-31', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaaf', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
			" (32, 4, 4, 4, 4, 4, 32, 4, 4.0, 4.0, 1114.1111, '2001-02-01', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaag', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
			" (33, 1, 1, 1, 1, 1, 33, 1, 1.0, 1.0, 1111.1111, '2001-02-02', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaah', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
			" (34, 2, 2, 2, 2, 2, 34, 2, 2.0, 2.0, 1112.1111, '2001-02-03', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaai', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
			" (35, 3, 3, 3, 3, 3, 35, 3, 3.0, 3.0, 1113.1111, '2001-02-05', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaaj', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
			" (36, 4, 4, 4, 4, 4, 36, 4, 4.0, 4.0, 1114.1111, '2001-02-04', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaak', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
			" (37, 5, 1, 1, 1, 1, 37, 1, 1.0, 1.0, 1111.1111, '2001-02-06', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaal', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
			" (38, 2, 2, 2, 2, 2, 38, 2, 2.0, 2.0, 1112.1111, '2001-02-07', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaam', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
			" (39, 3, 3, 3, 3, 3, 39, 3, 3.0, 3.0, 1113.1111, '2001-02-08', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaan', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
			" (40, 4, 4, 4, 4, 4, 40, 4, 4.0, 4.0, 1114.1111, '2001-02-09', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaao', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
			" (41, 1, 1, 1, 1, 1, 41, 1, 1.0, 1.0, 1111.1111, '2001-02-10', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaap', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
			" (42, 2, 2, 2, 2, 2, 42, 2, 2.0, 2.0, 1112.1111, '2001-02-11', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaaq', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
			" (43, 3, 3, 3, 3, 3, 43, 3, 3.0, 3.0, 1113.1111, '2001-02-12', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaar', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
			" (44, 4, 4, 4, 4, 4, 44, 4, 4.0, 4.0, 1114.1111, '2001-02-13', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaas', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
			" (45, 5, 1, 1, 1, 1, 45, 1, 1.0, 1.0, 1111.1111, '2001-02-14', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaat', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
			" (46, 2, 2, 2, 2, 2, 46, 2, 2.0, 2.0, 1112.1111, '2001-02-15', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaau', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
			" (47, 3, 3, 3, 3, 3, 47, 3, 3.0, 3.0, 1113.1111, '2001-02-16', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaav', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
			" (48, 4, 4, 4, 4, 4, 48, 4, 4.0, 4.0, 1114.1111, '2001-02-17', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaaw', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
			" (49, 1, 1, 1, 1, 1, 49, 1, 1.0, 1.0, 1111.1111, '2001-02-18', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaax', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
			" (50, 2, 2, 2, 2, 2, 50, 2, 2.0, 2.0, 1112.1111, '2001-02-19', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaay', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
			" (51, 3, 3, 3, 3, 3, 51, 3, 3.0, 3.0, 1113.1111, '2001-02-20', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaaz', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
			" (52, 4, 4, 4, 4, 4, 52, 4, 4.0, 4.0, 1114.1111, '2001-02-21', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaba', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
			" (53, 5, 1, 1, 1, 1, 53, 1, 1.0, 1.0, 1111.1111, '2001-02-22', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaca', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
			" (54, 2, 2, 2, 2, 2, 54, 2, 2.0, 2.0, 1112.1111, '2001-02-23', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aada', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
			" (55, 3, 3, 3, 3, 3, 55, 3, 3.0, 3.0, 1113.1111, '2001-02-24', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaea', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
			" (56, 4, 4, 4, 4, 4, 56, 4, 4.0, 4.0, 1114.1111, '2001-02-25', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aafa', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
			" (57, 1, 1, 1, 1, 1, 57, 1, 1.0, 1.0, 1111.1111, '2001-02-26', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaga', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
			" (58, 2, 2, 2, 2, 2, 58, 2, 2.0, 2.0, 1112.1111, '2001-02-27', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaha', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
			" (59, 3, 3, 3, 3, 3, 59, 3, 3.0, 3.0, 1113.1111, '2001-02-28', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaia', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
			" (60, 4, 4, 4, 4, 4, 60, 4, 4.0, 4.0, 1114.1111, '2001-03-01', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaja', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
			" (61, 5, 1, 1, 1, 1, 61, 1, 1.0, 1.0, 1111.1111, '2001-03-02', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaka', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
			" (62, 2, 2, 2, 2, 2, 62, 2, 2.0, 2.0, 1112.1111, '2001-03-03', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aala', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
			" (63, 3, 3, 3, 3, 3, 63, 3, 3.0, 3.0, 1113.1111, '2001-03-04', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aama', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
			" (64, 4, 4, 4, 4, 4, 64, 4, 4.0, 4.0, 1114.1111, '2001-03-05', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aana', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
		}
	)
	for i := 0; i < tableNum; i++ {
		insStr = "insert into addindexlit.t" + strconv.Itoa(i) + " (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28) values"
		for _, value := range values {
			insStr := insStr + value
			_, err := ctx.tk.Exec(insStr)
			require.NoError(ctx.t, err)
		}
	}
}

func createIndexOneCol(ctx *suiteContext, tableID int, colID int) error {
	addIndexStr := " add index idx"
	var ddlStr string
	if ctx.IsPK {
		addIndexStr = " add primary key idx"
	} else if ctx.IsUnique {
		addIndexStr = " add unique index idx"
	}
	length := 4
	if ctx.IsUnique && colID == 19 {
		length = 16
	}
	if !(ctx.IsPK || ctx.IsUnique) || tableID == 0 || (ctx.IsPK && tableID > 0) {
		if colID >= 18 && colID < 29 {
			ddlStr = "alter table addindexlit.t" + strconv.Itoa(tableID) + addIndexStr + strconv.Itoa(colID) + "(c" + strconv.Itoa(colID) + "(" + strconv.Itoa(length) + "))"
		} else {
			ddlStr = "alter table addindexlit.t" + strconv.Itoa(tableID) + addIndexStr + strconv.Itoa(colID) + "(c" + strconv.Itoa(colID) + ")"
		}
	} else if (ctx.IsUnique) && tableID > 0 {
		if colID >= 18 && colID < 29 {
			ddlStr = "alter table addindexlit.t" + strconv.Itoa(tableID) + addIndexStr + strconv.Itoa(colID) + "(c0, c" + strconv.Itoa(colID) + "(" + strconv.Itoa(length) + "))"
		} else {
			ddlStr = "alter table addindexlit.t" + strconv.Itoa(tableID) + addIndexStr + strconv.Itoa(colID) + "(c0, c" + strconv.Itoa(colID) + ")"
		}
	}
	fmt.Printf("log ddlStr: %q\n", ddlStr)
	_, err := ctx.tk.Exec(ddlStr)
	if err != nil {
		if ctx.IsUnique || ctx.IsPK {
			pos := strings.Index(err.Error(), "Error 1062: Duplicate entry")
			require.Greater(ctx.t, pos, 0)
		} else {
			require.NoError(ctx.t, err)
		}
	}
	return err
}

func createIndexTwoCols(ctx *suiteContext, tableID int, indexID int, colID1 int, colID2 int) error {
	var colID1Str, colID2Str string
	addIndexStr := " add index idx"
	if ctx.IsPK {
		addIndexStr = " add primary key idx"
	} else if ctx.IsUnique {
		addIndexStr = " add unique index idx"
	}
	if colID1 >= 18 && colID1 < 29 {
		colID1Str = strconv.Itoa(colID1) + "(4)"
	} else {
		colID1Str = strconv.Itoa(colID1)
	}
	if colID2 >= 18 && colID2 < 29 {
		colID2Str = strconv.Itoa(colID2) + "(4)"
	} else {
		colID2Str = strconv.Itoa(colID2)
	}
	ddlStr := "alter table addindexlit.t" + strconv.Itoa(tableID) + addIndexStr + strconv.Itoa(indexID) + "(c" + colID1Str + ", c" + colID2Str + ")"
	fmt.Printf("log ddlStr: %q\n", ddlStr)
	_, err := ctx.tk.Exec(ddlStr)
	if err != nil {
		fmt.Printf("create index failed, ddl: %q, err: %q", ddlStr, err.Error())
	}
	require.NoError(ctx.t, err)
	return err
}

func checkResult(ctx *suiteContext, tableName string, indexID int) {
	_, err := ctx.tk.Exec("admin check index " + tableName + " idx" + strconv.Itoa(indexID))
	fmt.Printf("log ddlStr: %q\n", "admin check index "+tableName+" idx"+strconv.Itoa(indexID))
	require.NoError(ctx.t, err)
	require.Equal(ctx.t, ctx.tk.Session().AffectedRows(), uint64(0))
	_, err = ctx.tk.Exec("alter table " + tableName + " drop index idx" + strconv.Itoa(indexID))
	if err != nil {
		fmt.Printf("drop index failed, ddl: %q, err: %q", "alter table "+tableName+" drop index idx"+strconv.Itoa(indexID), err.Error())
	}
	require.NoError(ctx.t, err)
}

func checkTableResult(ctx *suiteContext, tableName string) {
	_, err := ctx.tk.Exec("admin check table " + tableName)
	fmt.Printf("log ddlStr: %q\n", "admin check table "+tableName)
	require.NoError(ctx.t, err)
	require.Equal(ctx.t, ctx.tk.Session().AffectedRows(), uint64(0))
}

func testOneColFrame(ctx *suiteContext, colIDs [][]int, f func(*suiteContext, int, string, int) error) {
	for tableID := 0; tableID < ctx.TableNum; tableID++ {
		tableName := "addindexlit.t" + strconv.Itoa(tableID)
		for _, i := range colIDs[tableID] {
			if ctx.HasWorkload {
				// Start workload
				go func() {
					_ = ctx.Wl.StartWorkload(ctx, tableID, i)
				}()
			}
			err := f(ctx, tableID, tableName, i)
			if err != nil {
				if ctx.IsUnique || ctx.IsPK {
					pos := strings.Index(err.Error(), "Error 1062: Duplicate entry")
					require.Greater(ctx.t, pos, 0)
				} else {
					fmt.Printf("create index failed, err: %q", err.Error())
					require.NoError(ctx.t, err)
				}
			}
			if ctx.HasWorkload {
				// Stop workload
				_ = ctx.Wl.StopWorkload(ctx)
			}
			if err == nil {
				checkResult(ctx, tableName, i)
			}
		}
	}
}

func testTwoColsFrame(ctx *suiteContext, iIDs [][]int, jIDs [][]int, f func(*suiteContext, int, string, int, int, int) error) {
	for tableID := 0; tableID < ctx.TableNum; tableID++ {
		tableName := "addindexlit.t" + strconv.Itoa(tableID)
		indexID := 0
		for _, i := range iIDs[tableID] {
			for _, j := range jIDs[tableID] {
				if ctx.HasWorkload {
					// Start workload
					go func() {
						_ = ctx.Wl.StartWorkload(ctx, tableID, i, j)
					}()
				}
				err := f(ctx, tableID, tableName, indexID, i, j)
				if err != nil {
					fmt.Printf("create index failed, err: %q", err.Error())
				}
				require.NoError(ctx.t, err)
				if ctx.HasWorkload {
					// Stop workload
					_ = ctx.Wl.StopWorkload(ctx)
				}
				if err == nil && i != j {
					checkResult(ctx, tableName, indexID)
				}
				indexID++
			}
		}
	}
}

func testOneIndexFrame(ctx *suiteContext, colID int, f func(*suiteContext, int, string, int) error) {
	for tableID := 0; tableID < ctx.TableNum; tableID++ {
		tableName := "addindexlit.t" + strconv.Itoa(tableID)
		if ctx.HasWorkload {
			// Start workload
			go func() {
				_ = ctx.Wl.StartWorkload(ctx, tableID, colID)
			}()
		}
		err := f(ctx, tableID, tableName, colID)
		if err != nil {
			fmt.Printf("create index failed, err: %q", err.Error())
		}
		require.NoError(ctx.t, err)
		if ctx.HasWorkload {
			// Stop workload
			go func() {
				_ = ctx.Wl.StopWorkload(ctx)
			}()
		}
		if err == nil {
			if ctx.IsPK {
				checkTableResult(ctx, tableName)
			} else {
				checkResult(ctx, tableName, colID)
			}
		}
	}
}

func addIndexLitNonUnique(ctx *suiteContext, tableID int, tableName string, indexID int) (err error) {
	ctx.IsPK = false
	ctx.IsUnique = false
	err = createIndexOneCol(ctx, tableID, indexID)
	return err
}

func addIndexLitUnique(ctx *suiteContext, tableID int, tableName string, indexID int) (err error) {
	ctx.IsPK = false
	ctx.IsUnique = true
	if indexID == 0 || indexID == 6 || indexID == 11 || indexID == 19 || tableID > 0 {
		err = createIndexOneCol(ctx, tableID, indexID)
		if err != nil {
			fmt.Printf("create index failed, err: %q", err.Error())
		} else {
			fmt.Printf("create index success: %q, %q\n", tableName, indexID)
		}
		require.NoError(ctx.t, err)
	} else {
		err = createIndexOneCol(ctx, tableID, indexID)
		if err != nil {
			pos := strings.Index(err.Error(), "1062")
			require.Greater(ctx.t, pos, 0)
			if pos > 0 {
				fmt.Printf("create index failed: %q, %q, %q\n", tableName, indexID, err.Error())
			}
		}
	}
	return err
}

func addIndexLitPK(ctx *suiteContext, tableID int, tableName string, colID int) (err error) {
	ctx.IsPK = true
	ctx.IsUnique = false
	err = createIndexOneCol(ctx, tableID, 0)
	return err
}

func addIndexLitGenCol(ctx *suiteContext, tableID int, tableName string, colID int) (err error) {
	ctx.IsPK = false
	ctx.IsUnique = false
	err = createIndexOneCol(ctx, tableID, 29)
	return err
}

func addIndexLitMultiCols(ctx *suiteContext, tableID int, tableName string, indexID int, colID1 int, colID2 int) (err error) {
	ctx.IsPK = false
	ctx.IsUnique = false
	if colID1 != colID2 {
		err = createIndexTwoCols(ctx, tableID, indexID, colID1, colID2)
		if err != nil {
			fmt.Printf("create index failed, err: %q", err.Error())
		}
		require.NoError(ctx.t, err)
	}
	return err
}
