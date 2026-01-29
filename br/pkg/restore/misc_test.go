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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
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

	_, err = restore.GetTableSchema(dom, model.NewCIStr("test"), model.NewCIStr("tidb"))
	require.Error(t, err)
	tableInfo, err := restore.GetTableSchema(dom, model.NewCIStr("mysql"), model.NewCIStr("tidb"))
	require.NoError(t, err)
	require.Equal(t, model.NewCIStr("tidb"), tableInfo.Name)
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

type fakeMetaClient struct {
	split.SplitClient
	regions []*split.RegionInfo
	t       *testing.T
}

func (fmc *fakeMetaClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*split.RegionInfo, error) {
	i, ok := slices.BinarySearchFunc(fmc.regions, key, func(regionInfo *split.RegionInfo, k []byte) int {
		startCmpRet := bytes.Compare(regionInfo.Region.StartKey, k)
		if startCmpRet <= 0 && (len(regionInfo.Region.EndKey) == 0 || bytes.Compare(regionInfo.Region.EndKey, k) > 0) {
			return 0
		}
		return startCmpRet
	})
	require.True(fmc.t, ok)
	endI, ok := slices.BinarySearchFunc(fmc.regions, endKey, func(regionInfo *split.RegionInfo, k []byte) int {
		if len(k) == 0 {
			if len(regionInfo.Region.EndKey) == 0 {
				return 0
			}
			return -1
		}
		startCmpRet := bytes.Compare(regionInfo.Region.StartKey, k)
		if startCmpRet <= 0 && (len(regionInfo.Region.EndKey) == 0 || bytes.Compare(regionInfo.Region.EndKey, k) > 0) {
			return 0
		}
		return startCmpRet
	})
	require.True(fmc.t, ok)
	if !bytes.Equal(fmc.regions[endI].Region.StartKey, endKey) {
		endI += 1
	}
	if endI > i+limit {
		endI = i + limit
	}
	if endI > len(fmc.regions) {
		endI = len(fmc.regions)
	}
	return fmc.regions[i:endI], nil
}

func NewFakeMetaClient(t *testing.T, keys [][]byte) *fakeMetaClient {
	regions := make([]*split.RegionInfo, 0, len(keys)+1)
	lastEndKey := []byte{}
	for _, key := range keys {
		regions = append(regions, &split.RegionInfo{
			Region: &metapb.Region{
				StartKey: lastEndKey,
				EndKey:   key,
			},
		})
		lastEndKey = key
	}
	regions = append(regions, &split.RegionInfo{
		Region: &metapb.Region{
			StartKey: lastEndKey,
			EndKey:   []byte{},
		},
	})
	return &fakeMetaClient{
		regions: regions,
		t:       t,
	}
}

func TestFakeRegionScanner(t *testing.T) {
	keys := make([][]byte, 0, 100)
	for i := range 50 {
		keys = append(keys, fmt.Appendf(nil, "%02d5", 2*i))
	}
	metaClient := NewFakeMetaClient(t, keys)

	checkRegionsFn := func(regionInfos []*split.RegionInfo, startKey, endKey []byte) {
		require.Equal(t, regionInfos[0].Region.StartKey, startKey)
		require.Equal(t, regionInfos[len(regionInfos)-1].Region.EndKey, endKey)
	}

	ctx := context.Background()
	regionInfos, err := metaClient.ScanRegions(ctx, []byte("20"), []byte("30"), 1)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("185"), []byte("205"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("185"), []byte("30"), 1)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("185"), []byte("205"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("20"), []byte("30"), 5)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("185"), []byte("285"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("185"), []byte("30"), 5)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("185"), []byte("285"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("20"), []byte("30"), 20)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("185"), []byte("305"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("185"), []byte("305"), 20)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("185"), []byte("305"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("0001"), []byte("30"), 2)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte(""), []byte("025"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("0001"), []byte("10"), 20)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte(""), []byte("105"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("90"), []byte(""), 20)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("885"), []byte(""))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("90"), []byte(""), 2)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("885"), []byte("925"))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("885"), []byte(""), 20)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("885"), []byte(""))
	regionInfos, err = metaClient.ScanRegions(ctx, []byte("885"), []byte(""), 2)
	require.NoError(t, err)
	checkRegionsFn(regionInfos, []byte("885"), []byte("925"))
}

func newBackupFileSet(oldPrefix, newPrefix int, keys [][2]int) restore.BackupFileSet {
	sstFiles := make([]*backuppb.File, 0, len(keys))
	for _, key := range keys {
		sstFiles = append(sstFiles, &backuppb.File{
			StartKey: fmt.Appendf(nil, "%02d%d", oldPrefix, key[0]),
			EndKey:   fmt.Appendf(nil, "%02d%d", oldPrefix, key[1]),
		})
	}
	return restore.BackupFileSet{
		TableID:  int64(oldPrefix),
		SSTFiles: sstFiles,
		RewriteRules: &restoreutils.RewriteRules{
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: fmt.Appendf(nil, "%02d", oldPrefix),
					NewKeyPrefix: fmt.Appendf(nil, "%02d", newPrefix),
				},
			},
		},
	}
}

func TestRegionScanner(t *testing.T) {
	keys := make([][]byte, 0, 100)
	for i := range 50 {
		keys = append(keys, fmt.Appendf(nil, "%02d5", 2*i))
	}
	oldKeyMap := make([]int, 100)
	for i := range 100 {
		oldKeyMap[i] = i
	}
	rand.Shuffle(len(oldKeyMap), func(i, j int) {
		oldKeyMap[i], oldKeyMap[j] = oldKeyMap[j], oldKeyMap[i]
	})
	metaClient := NewFakeMetaClient(t, keys)
	ctx := context.Background()
	input := []restore.BackupFileSet{
		newBackupFileSet(oldKeyMap[1], 1, [][2]int{{5, 7}}),
		newBackupFileSet(oldKeyMap[4], 4, [][2]int{{2, 7}}),
		newBackupFileSet(oldKeyMap[8], 8, [][2]int{{2, 4}, {3, 7}, {6, 8}}),
		newBackupFileSet(oldKeyMap[12], 12, [][2]int{{1, 2}}),
		newBackupFileSet(oldKeyMap[12], 12, [][2]int{{6, 7}}),
		newBackupFileSet(oldKeyMap[14], 14, [][2]int{{1, 2}, {6, 7}}),
		newBackupFileSet(oldKeyMap[15], 15, [][2]int{{1, 5}}),
		newBackupFileSet(oldKeyMap[20], 20, [][2]int{{1, 5}}),
		newBackupFileSet(oldKeyMap[21], 21, [][2]int{{1, 2}}),
		newBackupFileSet(oldKeyMap[24], 24, [][2]int{{2, 4}}),
		newBackupFileSet(oldKeyMap[24], 24, [][2]int{{3, 7}}),
		newBackupFileSet(oldKeyMap[24], 24, [][2]int{{6, 8}}),
		newBackupFileSet(oldKeyMap[28], 28, [][2]int{{1, 9}}),
		newBackupFileSet(oldKeyMap[28], 28, [][2]int{{2, 4}}),
		newBackupFileSet(oldKeyMap[30], 30, [][2]int{{1, 4}}),
		newBackupFileSet(oldKeyMap[32], 32, [][2]int{{1, 2}, {6, 7}}),
		newBackupFileSet(oldKeyMap[34], 34, [][2]int{{1, 2}, {6, 7}}),
	}
	rand.Shuffle(len(input), func(i, j int) {
		input[i], input[j] = input[j], input[i]
	})
	output := []restore.BatchBackupFileSet{
		{
			newBackupFileSet(oldKeyMap[1], 1, [][2]int{{5, 7}}),
		},
		{
			newBackupFileSet(oldKeyMap[4], 4, [][2]int{{2, 7}}),
		},
		{
			newBackupFileSet(oldKeyMap[8], 8, [][2]int{{2, 4}, {3, 7}, {6, 8}}),
		},
		{
			newBackupFileSet(oldKeyMap[12], 12, [][2]int{{1, 2}}),
		},
		{
			newBackupFileSet(oldKeyMap[12], 12, [][2]int{{6, 7}}),
			newBackupFileSet(oldKeyMap[14], 14, [][2]int{{1, 2}, {6, 7}}),
			newBackupFileSet(oldKeyMap[15], 15, [][2]int{{1, 5}}),
		},
		{
			newBackupFileSet(oldKeyMap[20], 20, [][2]int{{1, 5}}),
			newBackupFileSet(oldKeyMap[21], 21, [][2]int{{1, 2}}),
		},
		{
			newBackupFileSet(oldKeyMap[24], 24, [][2]int{{2, 4}, {3, 7}, {6, 8}}),
		},
		{
			newBackupFileSet(oldKeyMap[28], 28, [][2]int{{1, 9}, {2, 4}}),
			newBackupFileSet(oldKeyMap[30], 30, [][2]int{{1, 4}}),
		},
		{
			newBackupFileSet(oldKeyMap[32], 32, [][2]int{{1, 2}, {6, 7}}),
			newBackupFileSet(oldKeyMap[34], 34, [][2]int{{1, 2}, {6, 7}}),
		},
	}
	output_i := 0
	restore.GroupOverlappedBackupFileSetsIter(ctx, metaClient, input, func(bbfs restore.BatchBackupFileSet) {
		expectSets := output[output_i]
		require.Equal(t, len(expectSets), len(bbfs))
		for i, bbf := range bbfs {
			t.Logf("output_i: %d, i: %d", output_i, i)
			expectSet := expectSets[i]
			require.Equal(t, len(expectSet.SSTFiles), len(bbf.SSTFiles))
			for j, file := range bbf.SSTFiles {
				require.Equal(t, expectSet.SSTFiles[j].StartKey, file.StartKey)
				require.Equal(t, expectSet.SSTFiles[j].EndKey, file.EndKey)
			}
			require.Equal(t, len(expectSet.RewriteRules.Data), len(bbf.RewriteRules.Data))
			for j, data := range bbf.RewriteRules.Data {
				require.Equal(t, expectSet.RewriteRules.Data[j].NewKeyPrefix, data.NewKeyPrefix)
			}
		}
		output_i += 1
	})
	require.Equal(t, len(output), output_i)
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
	name, data, err := restore.MarshalLogRestoreTableIDsBlocklistFile(0xFFFFFCDEFFFFF, 0xFFFFFFABCFFFF, []int64{1, 2, 3})
	require.NoError(t, err)
	restoreCommitTs, snapshotBackupTs, parsed := restore.ParseLogRestoreTableIDsBlocklistFileName(name)
	require.True(t, parsed)
	require.Equal(t, uint64(0xFFFFFCDEFFFFF), restoreCommitTs)
	require.Equal(t, uint64(0xFFFFFFABCFFFF), snapshotBackupTs)
	err = stg.WriteFile(ctx, name, data)
	require.NoError(t, err)
	data, err = stg.ReadFile(ctx, name)
	require.NoError(t, err)
	restoreCommitTs, snapshotBackupTs, tableIds, err := restore.UnmarshalLogRestoreTableIDsBlocklistFile(data)
	require.NoError(t, err)
	require.Equal(t, uint64(0xFFFFFCDEFFFFF), restoreCommitTs)
	require.Equal(t, uint64(0xFFFFFFABCFFFF), snapshotBackupTs)
	require.Equal(t, []int64{1, 2, 3}, tableIds)
}

func writeBlocklistFile(
	ctx context.Context, t *testing.T, s storage.ExternalStorage,
	restoreCommitTs, snapshotBackupTs uint64, tableIds []int64,
) {
	name, data, err := restore.MarshalLogRestoreTableIDsBlocklistFile(restoreCommitTs, snapshotBackupTs, tableIds)
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
	writeBlocklistFile(ctx, t, stg, 100, 10, []int64{100, 101, 102})
	writeBlocklistFile(ctx, t, stg, 200, 20, []int64{200, 201, 202})
	writeBlocklistFile(ctx, t, stg, 300, 30, []int64{300, 301, 302})
	tableNameByTableID := func(tableID int64) string {
		return fmt.Sprintf("table_%d", tableID)
	}
	checkTableIDLost := func(tableId int64) bool {
		return false
	}
	checkTableIDLost2 := func(tableId int64) bool {
		return true
	}
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{300, 301, 302}), 250, 300, tableNameByTableID, checkTableIDLost)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table_300")
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{200, 201, 202}), 250, 300, tableNameByTableID, checkTableIDLost)
	require.NoError(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{200, 201, 202}), 250, 300, tableNameByTableID, checkTableIDLost2)
	require.Error(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{100, 101, 102}), 250, 300, tableNameByTableID, checkTableIDLost)
	require.NoError(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{100, 101, 102}), 250, 300, tableNameByTableID, checkTableIDLost2)
	require.Error(t, err)

	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{300, 301, 302}), 1, 25, tableNameByTableID, checkTableIDLost)
	require.NoError(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{300, 301, 302}), 1, 25, tableNameByTableID, checkTableIDLost2)
	require.Error(t, err)
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{200, 201, 202}), 1, 25, tableNameByTableID, checkTableIDLost)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table_200")
	err = restore.CheckTableTrackerContainsTableIDsFromBlocklistFiles(ctx, stg, fakeTrackerID([]int64{100, 101, 102}), 1, 25, tableNameByTableID, checkTableIDLost)
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
	writeBlocklistFile(ctx, t, stg, 100, 10, []int64{100, 101, 102})
	writeBlocklistFile(ctx, t, stg, 200, 20, []int64{200, 201, 202})
	writeBlocklistFile(ctx, t, stg, 300, 30, []int64{300, 301, 302})

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
