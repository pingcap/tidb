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
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestGetStartMode(t *testing.T) {
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion))
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion+1))
	require.Equal(t, ddl.Upgrade, getStartMode(currentBootstrapVersion-1))
	require.Equal(t, ddl.Bootstrap, getStartMode(0))
}

func TestBootstrapSessionImplUserKSVersionGuard(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("keyspace guard only applies to next-gen kernel")
	}

	const (
		systemKeyspaceID uint32 = 0xFFFFFF - 1
		userKeyspaceID   uint32 = 0xFFFFFF - 2
	)

	newKSStore := func(t *testing.T, keyspaceID uint32, keyspaceName string) kv.Storage {
		t.Helper()
		store, err := mockstore.NewMockStore(
			mockstore.WithStoreType(mockstore.EmbedUnistore),
			mockstore.WithCurrentKeyspaceMeta(&keyspacepb.KeyspaceMeta{
				Id:   keyspaceID,
				Name: keyspaceName,
			}),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, store.Close())
		})
		return store
	}

	setBootstrapVersion := func(t *testing.T, store kv.Storage, ver int64) {
		t.Helper()
		txn, err := store.Begin()
		require.NoError(t, err)
		err = meta.NewMutator(txn).FinishBootstrap(ver)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(context.Background()))
		store.SetOption(StoreBootstrappedKey, nil)
	}

	t.Run("fatal when user target version is ahead of system version", func(t *testing.T) {
		systemStore := newKSStore(t, systemKeyspaceID, keyspace.System)
		userStore := newKSStore(t, userKeyspaceID, "user_keyspace_guard_fatal")
		setBootstrapVersion(t, systemStore, currentBootstrapVersion-1)
		setBootstrapVersion(t, userStore, currentBootstrapVersion-1)

		originSystemStore := kvstore.GetSystemStorage()
		kvstore.SetSystemStorage(systemStore)
		t.Cleanup(func() {
			kvstore.SetSystemStorage(originSystemStore)
		})

		createSessionCalled := false
		createSessionStub := func(_ kv.Storage, _ int) ([]*session, error) {
			createSessionCalled = true
			return nil, errors.New("must-not-be-called")
		}

		conf := new(log.Config)
		lg, p, err := log.InitLogger(conf, zap.WithFatalHook(zapcore.WriteThenPanic))
		require.NoError(t, err)
		restoreLog := log.ReplaceGlobals(lg, p)
		defer restoreLog()

		fatal2panic := false
		_, _ = func() (_ *domain.Domain, err error) {
			defer func() {
				if recover() != nil {
					fatal2panic = true
				}
			}()
			return bootstrapSessionImpl(context.Background(), userStore, createSessionStub)
		}()
		require.True(t, fatal2panic)
		require.False(t, createSessionCalled)
	})
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
