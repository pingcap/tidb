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
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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
	testTableBasicInfoSlice(t, allDDLTables)
}

func testTableBasicInfoSlice(t *testing.T, allTables []TableBasicInfo) {
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
		require.Contains(t, vt.SQL, fmt.Sprintf(" mysql.%s (", vt.Name),
			"table SQL should contain table name and follow the format 'mysql.<table_name> ('")
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

func TestAdvisoryLockRestoresInnodbLockWaitTimeout(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	pool := dom.SysSessionPool()
	ctx := context.Background()

	t.Run("get_lock", func(t *testing.T) {
		res, err := pool.Get()
		require.NoError(t, err)
		se := res.(*session)

		orig, err := se.sessionVars.GetSessionOrGlobalSystemVar(ctx, vardef.InnodbLockWaitTimeout)
		require.NoError(t, err)
		origSec, err := strconv.ParseInt(orig, 10, 64)
		require.NoError(t, err)

		// Ensure we set a different value from the original.
		newTimeout := origSec + 23
		lock := &advisoryLock{
			session: se,
			ctx:     ctx,
			clean: func() {
				defer pool.Destroy(res)

				got, err := se.sessionVars.GetSessionOrGlobalSystemVar(ctx, vardef.InnodbLockWaitTimeout)
				require.NoError(t, err)
				require.Equal(t, orig, got)
				require.Equal(t, origSec*1000, se.sessionVars.LockWaitTimeout)
			},
		}

		err = lock.GetLock("tidb_test_restore_lock_wait_timeout_get_lock", newTimeout)
		require.NoError(t, err)
		gotAfterSet, err := se.sessionVars.GetSessionOrGlobalSystemVar(ctx, vardef.InnodbLockWaitTimeout)
		require.NoError(t, err)
		require.Equal(t, strconv.FormatInt(newTimeout, 10), gotAfterSet)
		lock.Close()
	})

	t.Run("is_used_lock", func(t *testing.T) {
		res, err := pool.Get()
		require.NoError(t, err)
		se := res.(*session)

		require.NoError(t, se.sessionVars.SetSystemVar(vardef.InnodbLockWaitTimeout, "2"))
		orig, err := se.sessionVars.GetSessionOrGlobalSystemVar(ctx, vardef.InnodbLockWaitTimeout)
		require.NoError(t, err)
		require.Equal(t, "2", orig)

		lock := &advisoryLock{
			session: se,
			ctx:     ctx,
			clean: func() {
				defer pool.Destroy(res)

				got, err := se.sessionVars.GetSessionOrGlobalSystemVar(ctx, vardef.InnodbLockWaitTimeout)
				require.NoError(t, err)
				require.Equal(t, "2", got)
				require.Equal(t, int64(2000), se.sessionVars.LockWaitTimeout)
			},
		}
		require.NoError(t, lock.IsUsedLock("tidb_test_restore_lock_wait_timeout_is_used_lock"))
	})
}
