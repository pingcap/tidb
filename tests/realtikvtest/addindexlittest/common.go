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

func createTable(ctx *suiteContext) (err error) {
	for i := 0; i < nonPartTabNum; i++ {
		tableName := "t" + strconv.Itoa(i)
		tableDef := genTableStr(tableName)
		_, err = ctx.tk.Exec(tableDef)
		require.NoError(ctx.t, err)
	}
	tableDefs := genPartTableStr()
	for _, tableDef := range tableDefs {
		_, err = ctx.tk.Exec(tableDef)
		require.NoError(ctx.t, err)
	}
	return err
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
