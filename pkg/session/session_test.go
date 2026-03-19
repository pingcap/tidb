// Copyright 2024 PingCAP, Inc.
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

package session

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/stretchr/testify/require"
)

func TestGetStartMode(t *testing.T) {
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion))
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion+1))
	require.Equal(t, ddl.Upgrade, getStartMode(currentBootstrapVersion-1))
	require.Equal(t, ddl.Bootstrap, getStartMode(0))
}

func TestDDLTableVersionTables(t *testing.T) {
	require.True(t, slices.IsSortedFunc(ddlTableVersionTables, func(a, b versionedDDLTables) int {
		return cmp.Compare(a.ver, b.ver)
	}), "ddlTableVersionTables should be sorted by version")
	allDDLTables := make([]TableBasicInfo, 0, len(ddlTableVersionTables)*2)
	for _, v := range ddlTableVersionTables {
		allDDLTables = append(allDDLTables, v.tables...)
	}
	testTableBasicInfoSlice(t, allDDLTables, " mysql.%s (")
}

func testTableBasicInfoSlice(t *testing.T, allTables []TableBasicInfo, sqlFmt string) {
	t.Helper()
	require.True(t, slices.IsSortedFunc(allTables, func(a, b TableBasicInfo) int {
		if a.ID == b.ID {
			t.Errorf("table IDs should be unique, a=%d, b=%d", a.ID, b.ID)
		}
		if a.Name == b.Name {
			t.Errorf("table names should be unique, a=%s, b=%s", a.Name, b.Name)
		}
		return cmp.Compare(b.ID, a.ID)
	}), "tables should be sorted by table ID in descending order")
	for _, vt := range allTables {
		require.Greater(t, vt.ID, metadef.ReservedGlobalIDLowerBound, "table ID should be greater than ReservedGlobalIDLowerBound")
		require.LessOrEqual(t, vt.ID, metadef.ReservedGlobalIDUpperBound, "table ID should be less than or equal to ReservedGlobalIDUpperBound")
		require.Equal(t, strings.ToLower(vt.Name), vt.Name, "table name should be in lower case")
		require.Contains(t, vt.SQL, fmt.Sprintf(sqlFmt, vt.Name),
			fmt.Sprintf("table SQL should contain table name and follow the format %s", sqlFmt))
	}
}

func TestMemArbitratorSession(t *testing.T) {
	require.Equal(t, int64(15), approxParseSQLTokenCnt("/*select * from **/SELECT x FROM `t\\`` # abc \nwhere a = 1.23 and b = 'abc\"d\\'e' -- abc \nand c_1_2 in \"abc'd\\\"e\" # (1,2,3)\n"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("select @@version @a")) // not select ... from ...
	require.Equal(t, int64(0), approxParseSQLTokenCnt("set @a=1"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("desc analyze table t"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("analyze table t"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("/*select * from **/explain show warnings"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("/*select * from **/desc show columns from t"))
	require.Equal(t, int64(5), approxParseSQLTokenCnt("insert into t values 1"))
	require.Equal(t, int64(5), approxParseSQLTokenCnt("update t set a=1"))
	require.Equal(t, int64(6), approxParseSQLTokenCnt("delete from t where a=1"))
	require.Equal(t, int64(5), approxParseSQLTokenCnt("replace into t values 1"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("prepare stmt1 from 'select * from t where a=? and b=?'"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("execute stmt1 using @a,@b,@c"))
	require.Equal(t, int64(10), approxParseSQLTokenCnt("select * from `a_1`.`b_2` where c1 = ? and c2 = ?"))
	require.Equal(t, int64(9), approxCompilePlanTokenCnt("select * from `a_1`.`b_2` where c1 = ? and c2 = ?", true))
	require.Equal(t, int64(0), approxCompilePlanTokenCnt("select @@version @a", true))
	require.Equal(t, int64(3), approxCompilePlanTokenCnt("select @@version @a", false))
}
