// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func getFunctionName(f func(sessionapi.Session, int64)) (string, error) {
	if f == nil {
		return "", errors.New("function is nil")
	}

	funcPtr := reflect.ValueOf(f).Pointer()
	if funcPtr == 0 {
		return "", errors.New("invalid function pointer")
	}

	fullName := runtime.FuncForPC(funcPtr).Name()
	if fullName == "" {
		return "", errors.New("unable to retrieve function name")
	}

	parts := strings.Split(fullName, ".")
	if len(parts) == 0 {
		return "", errors.New("invalid function name structure")
	}

	return parts[len(parts)-1], nil
}

func TestUpgradeToVerFunctionsCheck(t *testing.T) {
	var lastVer int64
	var firstVersionAfterReleaseNextGen202603 int64
	var firstVersionAfterReleaseNextGen202609 int64
	for _, verFn := range upgradeToVerFunctions {
		require.Greater(t, verFn.version, lastVer, "upgradeToVerFunctions should be in ascending order")
		lastVer = verFn.version
		if firstVersionAfterReleaseNextGen202603 == 0 && verFn.version > version256 {
			firstVersionAfterReleaseNextGen202603 = verFn.version
		}
		if firstVersionAfterReleaseNextGen202609 == 0 && verFn.version > version285 {
			firstVersionAfterReleaseNextGen202609 = verFn.version
		}
		require.NotNil(t, verFn.fn, "upgradeToVerFunctions should not have nil function")
		name, err := getFunctionName(verFn.fn)
		require.NoError(t, err, "getFunctionName should not return an error")
		require.Regexp(t, fmt.Sprintf(`^upgradeToVer%d$`, verFn.version), name, "function name should match upgradeToVer pattern")
	}
	require.Equal(t, int64(277), firstVersionAfterReleaseNextGen202603,
		"versions 257 through 276 should be reserved for release-nextgen-202603")
	require.Equal(t, int64(316), firstVersionAfterReleaseNextGen202609,
		"versions 286 through 315 should be reserved for release-nextgen-202609")
	require.Equal(t, currentBootstrapVersion, lastVer, "last version in upgradeToVerFunctions should match currentBootstrapVersion")
}

func TestUpgradeVersion285TTLTaskScanIndexID(t *testing.T) {
	defer memory.CleanupGlobalMemArbitratorForTest()

	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "ALTER TABLE mysql.tidb_ttl_task DROP COLUMN scan_index_id")
	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).FinishBootstrap(version285-1))
	require.NoError(t, txn.Commit(context.Background()))
	RevertVersionAndVariables(t, se, version285-1)
	store.SetOption(StoreBootstrappedKey, nil)

	dom.Close()
	newDom, err := BootstrapSession(store)
	require.NoError(t, err)
	defer newDom.Close()

	se = CreateSessionAndSetID(t, store)
	ver, err := GetBootstrapVersion(se)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	rs := MustExecToRecodeSet(t, se, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'mysql' AND table_name = 'tidb_ttl_task' AND column_name = 'scan_index_id'")
	req := rs.NewChunk(nil)
	require.NoError(t, rs.Next(context.Background(), req))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, int64(1), req.GetRow(0).GetInt64(0))
	require.NoError(t, rs.Close())
}

func TestUpgradeToVer321StorageClassTransitionHistory(t *testing.T) {
	if kerneltype.IsNextGen() {
		originalMode := deploymode.Get()
		require.NoError(t, deploymode.Set(deploymode.Starter))
		t.Cleanup(func() { require.NoError(t, deploymode.Set(originalMode)) })
	}

	for _, legacy := range []bool{false, true} {
		if legacy && kerneltype.IsClassic() {
			continue
		}
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) {
			store, dom := CreateStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()
			se := CreateSessionAndSetID(t, store)
			RevertVersionAndVariables(t, se, version320)
			se.Close()
			dom.Close()
			domap.Delete(store)

			ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
			require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
				m := meta.NewMutator(txn)
				if legacy {
					// Move mysql to a Classic database ID, preserving table IDs so
					// the bootstrapped rows remain readable after the layout change.
					tables, err := m.ListTables(ctx, metadef.SystemDatabaseID)
					require.NoError(t, err)
					require.NoError(t, m.CreateSysDatabaseByID("mysql", 1))
					for _, tbl := range tables {
						if tbl.Name.L != metadef.TiDBStorageClassTransitionHistoryTableName {
							require.NoError(t, m.CreateTableOrView(1, tbl))
						}
					}
					require.NoError(t, m.DropDatabase(metadef.SystemDatabaseID))
				} else if kerneltype.IsNextGen() {
					// Native NextGen must leave table creation to its separate
					// versioned schema bootstrap, even if this table is absent.
					require.NoError(t, m.DropTableOrView(metadef.SystemDatabaseID, metadef.TiDBStorageClassTransitionHistoryTableID))
				}
				return m.FinishBootstrap(version320)
			}))
			store.SetOption(StoreBootstrappedKey, nil)

			dom, err := BootstrapSession(store)
			require.NoError(t, err)
			defer dom.Close()
			se = CreateSessionAndSetID(t, store)
			defer se.Close()
			is := dom.InfoSchema()
			db, ok := is.SchemaByName(ast.NewCIStr("mysql"))
			require.True(t, ok)
			if legacy {
				require.Equal(t, int64(1), db.ID)
			}
			tbl, err := is.TableByName(ctx, db.Name, ast.NewCIStr(metadef.TiDBStorageClassTransitionHistoryTableName))
			if !legacy {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Less(t, tbl.Meta().ID, metadef.ReservedGlobalIDLowerBound)
				MustExec(t, se, "SELECT * FROM mysql.tidb_storage_class_transition_history")
				// Replaying the migration must preserve the existing table.
				upgradeToVer321(se, version320)
				replayed, err := dom.InfoSchema().TableByName(ctx, db.Name, tbl.Meta().Name)
				require.NoError(t, err)
				require.Equal(t, tbl.Meta().ID, replayed.Meta().ID)
			}
			ver, err := GetBootstrapVersion(se)
			require.NoError(t, err)
			require.Equal(t, currentBootstrapVersion, ver)
			require.Greater(t, ver, int64(version320))
		})
	}
}
