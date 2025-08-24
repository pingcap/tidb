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

package restore_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/stretchr/testify/require"
)

func TestTransferBoolToValue(t *testing.T) {
	require.Equal(t, "ON", restore.TransferBoolToValue(true))
	require.Equal(t, "OFF", restore.TransferBoolToValue(false))
}

func TestGetTableSchema(t *testing.T) {
	m, err := mock.NewCluster()
	require.Nil(t, err)
	defer m.Stop()
	dom := m.Domain

	_, err = restore.GetTableSchema(dom, ast.NewCIStr("test"), ast.NewCIStr("tidb"))
	require.Error(t, err)
	tableInfo, err := restore.GetTableSchema(dom, ast.NewCIStr("mysql"), ast.NewCIStr("tidb"))
	require.NoError(t, err)
	require.Equal(t, ast.NewCIStr("tidb"), tableInfo.Name)
}

func TestAssertUserDBsEmpty(t *testing.T) {
	m, err := mock.NewCluster()
	require.Nil(t, err)
	defer m.Stop()
	dom := m.Domain

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR)
	se, err := session.CreateSession(dom.Store())
	require.Nil(t, err)

	err = restore.AssertUserDBsEmpty(dom)
	require.Nil(t, err)

	_, err = se.ExecuteInternal(ctx, "CREATE DATABASE d1;")
	require.Nil(t, err)
	err = restore.AssertUserDBsEmpty(dom)
	require.Error(t, err)
	require.Contains(t, err.Error(), "d1.")

	_, err = se.ExecuteInternal(ctx, "CREATE TABLE d1.test(id int);")
	require.Nil(t, err)
	err = restore.AssertUserDBsEmpty(dom)
	require.Error(t, err)
	require.Contains(t, err.Error(), "d1.test")

	_, err = se.ExecuteInternal(ctx, "DROP DATABASE d1;")
	require.Nil(t, err)
	for i := 0; i < 15; i += 1 {
		_, err = se.ExecuteInternal(ctx, fmt.Sprintf("CREATE DATABASE d%d;", i))
		require.Nil(t, err)
	}
	err = restore.AssertUserDBsEmpty(dom)
	require.Error(t, err)
	containCount := 0
	for i := 0; i < 15; i += 1 {
		if strings.Contains(err.Error(), fmt.Sprintf("d%d.", i)) {
			containCount += 1
		}
	}
	require.Equal(t, 10, containCount)

	for i := 0; i < 15; i += 1 {
		_, err = se.ExecuteInternal(ctx, fmt.Sprintf("CREATE TABLE d%d.t1(id int);", i))
		require.Nil(t, err)
	}
	err = restore.AssertUserDBsEmpty(dom)
	require.Error(t, err)
	containCount = 0
	for i := 0; i < 15; i += 1 {
		if strings.Contains(err.Error(), fmt.Sprintf("d%d.t1", i)) {
			containCount += 1
		}
	}
	require.Equal(t, 10, containCount)
}

func TestGetTSWithRetry(t *testing.T) {
	t.Run("PD leader is healthy:", func(t *testing.T) {
		retryTimes := -1000
		pDClient := split.NewFakePDClient(nil, false, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.NoError(t, err)
	})

	t.Run("PD leader failure:", func(t *testing.T) {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/set-attempt-to-one", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/set-attempt-to-one"))
		}()
		retryTimes := -1000
		pDClient := split.NewFakePDClient(nil, true, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.Error(t, err)
	})

	t.Run("PD leader switch successfully", func(t *testing.T) {
		retryTimes := 0
		pDClient := split.NewFakePDClient(nil, true, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.NoError(t, err)
	})
}

func TestParseLogRestoreTableIDsBlocklistFileName(t *testing.T) {
	restoreCommitTs, snapshotBackupTs, parsed := restore.ParseLogRestoreTableIDsBlocklistFileName("RFFFFFFFFFFFFFFFF_SFFFFFFFFFFFFFFFF.meta")
	require.True(t, parsed)
	require.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), restoreCommitTs)
	require.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), snapshotBackupTs)
	unparsedFilenames := []string{
		"KFFFFFFFFFFFFFFFF_SFFFFFFFFFFFFFFFF.meta",
		"RFFFFFFFFFFFFFFFF.SFFFFFFFFFFFFFFFF.meta",
		"RFFFFFFFFFFFFFFFF_KFFFFFFFFFFFFFFFF.meta",
		"RFFFFFFFFFFFFFFFF_SFFFFFFFFFFFFFFFF.mata",
		"RFFFFFFFKFFFFFFFF_SFFFFFFFFFFFFFFFF.meta",
		"RFFFFFFFFFFFFFFFF_SFFFFFFFFKFFFFFFF.meta",
	}
	for _, filename := range unparsedFilenames {
		_, _, parsed := restore.ParseLogRestoreTableIDsBlocklistFileName(filename)
		require.False(t, parsed)
	}
}

func TestLogRestoreTableIDsBlocklistFile(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	stg, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	name, data, err := restore.MarshalLogRestoreTableIDsBlocklistFile(0xFFFFFCDEFFFFF, 0xFFFFFFABCFFFF, 0xFFFFFCCCFFFFF, []int64{1, 2, 3}, []int64{4})
	require.NoError(t, err)
	restoreCommitTs, snapshotBackupTs, parsed := restore.ParseLogRestoreTableIDsBlocklistFileName(name)
	require.True(t, parsed)
	require.Equal(t, uint64(0xFFFFFCDEFFFFF), restoreCommitTs)
	require.Equal(t, uint64(0xFFFFFFABCFFFF), snapshotBackupTs)
	err = stg.WriteFile(ctx, name, data)
	require.NoError(t, err)
	data, err = stg.ReadFile(ctx, name)
	require.NoError(t, err)
	blocklist, err := restore.UnmarshalLogRestoreTableIDsBlocklistFile(data)
	require.NoError(t, err)
	require.Equal(t, uint64(0xFFFFFCDEFFFFF), blocklist.RestoreCommitTs)
	require.Equal(t, uint64(0xFFFFFFABCFFFF), blocklist.SnapshotBackupTs)
	require.Equal(t, uint64(0xFFFFFCCCFFFFF), blocklist.RewriteTs)
	require.Equal(t, []int64{1, 2, 3}, blocklist.TableIds)
	require.Equal(t, []int64{4}, blocklist.DbIds)
}

func writeBlocklistFile(
	ctx context.Context, t *testing.T, s storage.ExternalStorage,
	restoreCommitTs, snapshotBackupTs, rewriteTs uint64, tableIds, dbIds []int64,
) {
	name, data, err := restore.MarshalLogRestoreTableIDsBlocklistFile(restoreCommitTs, snapshotBackupTs, rewriteTs, tableIds, dbIds)
	require.NoError(t, err)
	err = s.WriteFile(ctx, name, data)
	require.NoError(t, err)
}

func fakeTrackerID(tableIds []int64) *utils.PiTRIdTracker {
	tracker := utils.NewPiTRIdTracker()
	for _, tableId := range tableIds {
		tracker.TableIdToDBIds[tableId] = make(map[int64]struct{})
	}
	return tracker
}

func TestCheckTableTrackerContainsTableIDsFromBlocklistFiles(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	stg, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	writeBlocklistFile(ctx, t, stg, 100, 10, 50, []int64{100, 101, 102}, []int64{103})
	writeBlocklistFile(ctx, t, stg, 200, 20, 60, []int64{200, 201, 202}, []int64{203})
	writeBlocklistFile(ctx, t, stg, 300, 30, 70, []int64{300, 301, 302}, []int64{303})
	tableNameByTableId := func(tableId int64) string {
		return fmt.Sprintf("table_%d", tableId)
	}
	dbNameByDbId := func(dbId int64) string {
		return fmt.Sprintf("db_%d", dbId)
	}
	checkTableIDLost := func(tableId int64) bool {
		return false
	}
	checkTableIDLost2 := func(tableId int64) bool {
		return true
	}
	rewriteTss := make([]uint64, 0)
	var mu sync.Mutex
	cleanErr := func(rewriteTs uint64) {
		mu.Lock()
		rewriteTss = append(rewriteTss, rewriteTs)
		mu.Unlock()
	}
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{300, 301, 302}), 250, 300, tableNameByTableId, dbNameByDbId, checkTableIDLost, checkTableIDLost, cleanErr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table_300")
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{200, 201, 202}), 250, 300, tableNameByTableId, dbNameByDbId, checkTableIDLost, checkTableIDLost, cleanErr)
	require.NoError(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{200, 201, 202}), 250, 300, tableNameByTableId, dbNameByDbId, checkTableIDLost2, checkTableIDLost2, cleanErr)
	require.NoError(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{100, 101, 102}), 250, 300, tableNameByTableId, dbNameByDbId, checkTableIDLost, checkTableIDLost, cleanErr)
	require.NoError(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{100, 101, 102}), 250, 300, tableNameByTableId, dbNameByDbId, checkTableIDLost2, checkTableIDLost2, cleanErr)
	require.NoError(t, err)

	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{300, 301, 302}), 1, 25, tableNameByTableId, dbNameByDbId, checkTableIDLost, checkTableIDLost, cleanErr)
	require.NoError(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{300, 301, 302}), 1, 25, tableNameByTableId, dbNameByDbId, checkTableIDLost2, checkTableIDLost2, cleanErr)
	require.NoError(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{200, 201, 202}), 1, 25, tableNameByTableId, dbNameByDbId, checkTableIDLost, checkTableIDLost, cleanErr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table_200")
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{100, 101, 102}), 1, 25, tableNameByTableId, dbNameByDbId, checkTableIDLost, checkTableIDLost, cleanErr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table_100")
}

func filesCount(ctx context.Context, s storage.ExternalStorage) int {
	count := 0
	s.WalkDir(ctx, &storage.WalkOption{SubDir: restore.LogRestoreTableIDBlocklistFilePrefix}, func(path string, size int64) error {
		count += 1
		return nil
	})
	return count
}

func TestTruncateLogRestoreTableIDsBlocklistFiles(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	stg, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	writeBlocklistFile(ctx, t, stg, 100, 10, 50, []int64{100, 101, 102}, []int64{103})
	writeBlocklistFile(ctx, t, stg, 200, 20, 60, []int64{200, 201, 202}, []int64{203})
	writeBlocklistFile(ctx, t, stg, 300, 30, 70, []int64{300, 301, 302}, []int64{303})

	err = restore.TruncateLogRestoreTableIDsBlocklistFiles(ctx, stg, 50)
	require.NoError(t, err)
	require.Equal(t, 3, filesCount(ctx, stg))

	err = restore.TruncateLogRestoreTableIDsBlocklistFiles(ctx, stg, 250)
	require.NoError(t, err)
	require.Equal(t, 1, filesCount(ctx, stg))

	err = restore.TruncateLogRestoreTableIDsBlocklistFiles(ctx, stg, 350)
	require.NoError(t, err)
	require.Equal(t, 0, filesCount(ctx, stg))
}
