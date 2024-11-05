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

package checkpoint_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestCheckpointMetaForBackup(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)

	checkpointMeta := &checkpoint.CheckpointMetadataForBackup{
		ConfigHash: []byte("123456"),
		BackupTS:   123456,
	}

	err = checkpoint.SaveCheckpointMetadata(ctx, s, checkpointMeta)
	require.NoError(t, err)

	checkpointMeta2, err := checkpoint.LoadCheckpointMetadata(ctx, s)
	require.NoError(t, err)
	require.Equal(t, checkpointMeta.ConfigHash, checkpointMeta2.ConfigHash)
	require.Equal(t, checkpointMeta.BackupTS, checkpointMeta2.BackupTS)
}

func TestCheckpointMetaForRestore(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	dom := s.Mock.Domain
	g := gluetidb.New()
	se, err := g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)

	checkpointMetaForSnapshotRestore := &checkpoint.CheckpointMetadataForSnapshotRestore{
		UpstreamClusterID: 123,
		RestoredTS:        321,
		SchedulersConfig: &pdutil.ClusterConfig{
			Schedulers: []string{"1", "2"},
			ScheduleCfg: map[string]any{
				"1": "2",
				"2": "1",
			},
		},
	}
	err = checkpoint.SaveCheckpointMetadataForSnapshotRestore(ctx, se, checkpointMetaForSnapshotRestore)
	require.NoError(t, err)
	checkpointMetaForSnapshotRestore2, err := checkpoint.LoadCheckpointMetadataForSnapshotRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor())
	require.NoError(t, err)
	require.Equal(t, checkpointMetaForSnapshotRestore.SchedulersConfig, checkpointMetaForSnapshotRestore2.SchedulersConfig)
	require.Equal(t, checkpointMetaForSnapshotRestore.UpstreamClusterID, checkpointMetaForSnapshotRestore2.UpstreamClusterID)
	require.Equal(t, checkpointMetaForSnapshotRestore.RestoredTS, checkpointMetaForSnapshotRestore2.RestoredTS)

	checkpointMetaForLogRestore := &checkpoint.CheckpointMetadataForLogRestore{
		UpstreamClusterID: 123,
		RestoredTS:        222,
		StartTS:           111,
		RewriteTS:         333,
		GcRatio:           "1.0",
		TiFlashItems:      map[int64]model.TiFlashReplicaInfo{1: {Count: 1}},
	}
	err = checkpoint.SaveCheckpointMetadataForLogRestore(ctx, se, checkpointMetaForLogRestore)
	require.NoError(t, err)
	checkpointMetaForLogRestore2, err := checkpoint.LoadCheckpointMetadataForLogRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor())
	require.NoError(t, err)
	require.Equal(t, checkpointMetaForLogRestore.UpstreamClusterID, checkpointMetaForLogRestore2.UpstreamClusterID)
	require.Equal(t, checkpointMetaForLogRestore.RestoredTS, checkpointMetaForLogRestore2.RestoredTS)
	require.Equal(t, checkpointMetaForLogRestore.StartTS, checkpointMetaForLogRestore2.StartTS)
	require.Equal(t, checkpointMetaForLogRestore.RewriteTS, checkpointMetaForLogRestore2.RewriteTS)
	require.Equal(t, checkpointMetaForLogRestore.GcRatio, checkpointMetaForLogRestore2.GcRatio)
	require.Equal(t, checkpointMetaForLogRestore.TiFlashItems, checkpointMetaForLogRestore2.TiFlashItems)

	exists := checkpoint.ExistsCheckpointProgress(ctx, dom)
	require.False(t, exists)
	err = checkpoint.SaveCheckpointProgress(ctx, se, &checkpoint.CheckpointProgress{
		Progress: checkpoint.InLogRestoreAndIdMapPersist,
	})
	require.NoError(t, err)
	progress, err := checkpoint.LoadCheckpointProgress(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor())
	require.NoError(t, err)
	require.Equal(t, checkpoint.InLogRestoreAndIdMapPersist, progress.Progress)

	taskInfo, err := checkpoint.TryToGetCheckpointTaskInfo(ctx, s.Mock.Domain, se.GetSessionCtx().GetRestrictedSQLExecutor())
	require.NoError(t, err)
	require.Equal(t, uint64(123), taskInfo.Metadata.UpstreamClusterID)
	require.Equal(t, uint64(222), taskInfo.Metadata.RestoredTS)
	require.Equal(t, uint64(111), taskInfo.Metadata.StartTS)
	require.Equal(t, uint64(333), taskInfo.Metadata.RewriteTS)
	require.Equal(t, "1.0", taskInfo.Metadata.GcRatio)
	require.Equal(t, true, taskInfo.HasSnapshotMetadata)
	require.Equal(t, checkpoint.InLogRestoreAndIdMapPersist, taskInfo.Progress)

	exists = checkpoint.ExistsCheckpointIngestIndexRepairSQLs(ctx, dom)
	require.False(t, exists)
	err = checkpoint.SaveCheckpointIngestIndexRepairSQLs(ctx, se, &checkpoint.CheckpointIngestIndexRepairSQLs{
		SQLs: []checkpoint.CheckpointIngestIndexRepairSQL{
			{
				IndexID:    1,
				SchemaName: pmodel.NewCIStr("2"),
				TableName:  pmodel.NewCIStr("3"),
				IndexName:  "4",
				AddSQL:     "5",
				AddArgs:    []any{"6", "7", "8"},
			},
		},
	})
	require.NoError(t, err)
	repairSQLs, err := checkpoint.LoadCheckpointIngestIndexRepairSQLs(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor())
	require.NoError(t, err)
	require.Equal(t, repairSQLs.SQLs[0].IndexID, int64(1))
	require.Equal(t, repairSQLs.SQLs[0].SchemaName, pmodel.NewCIStr("2"))
	require.Equal(t, repairSQLs.SQLs[0].TableName, pmodel.NewCIStr("3"))
	require.Equal(t, repairSQLs.SQLs[0].IndexName, "4")
	require.Equal(t, repairSQLs.SQLs[0].AddSQL, "5")
	require.Equal(t, repairSQLs.SQLs[0].AddArgs, []any{"6", "7", "8"})
}

type mockTimer struct {
	p int64
	l int64
}

func NewMockTimer(p, l int64) *mockTimer {
	return &mockTimer{p: p, l: l}
}

func (t *mockTimer) GetTS(ctx context.Context) (int64, int64, error) {
	return t.p, t.l, nil
}

func TestCheckpointBackupRunner(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	os.MkdirAll(base+checkpoint.CheckpointDataDirForBackup, 0o755)
	os.MkdirAll(base+checkpoint.CheckpointChecksumDirForBackup, 0o755)

	cipher := &backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
		CipherKey:  []byte("01234567890123456789012345678901"),
	}
	checkpointRunner, err := checkpoint.StartCheckpointBackupRunnerForTest(
		ctx, s, cipher, 5*time.Second, NewMockTimer(10, 10))
	require.NoError(t, err)

	data := map[string]struct {
		StartKey string
		EndKey   string
		Name     string
		Name2    string
	}{
		"a": {
			StartKey: "a",
			EndKey:   "b",
			Name:     "c",
			Name2:    "d",
		},
		"A": {
			StartKey: "A",
			EndKey:   "B",
			Name:     "C",
			Name2:    "D",
		},
		"1": {
			StartKey: "1",
			EndKey:   "2",
			Name:     "3",
			Name2:    "4",
		},
	}

	data2 := map[string]struct {
		StartKey string
		EndKey   string
		Name     string
		Name2    string
	}{
		"+": {
			StartKey: "+",
			EndKey:   "-",
			Name:     "*",
			Name2:    "/",
		},
	}

	for _, d := range data {
		err = checkpoint.AppendForBackup(ctx, checkpointRunner, "a", []byte(d.StartKey), []byte(d.EndKey), []*backuppb.File{
			{Name: d.Name},
			{Name: d.Name2},
		})
		require.NoError(t, err)
	}

	checkpointRunner.FlushChecksum(ctx, 1, 1, 1, 1)
	checkpointRunner.FlushChecksum(ctx, 2, 2, 2, 2)
	checkpointRunner.FlushChecksum(ctx, 3, 3, 3, 3)
	checkpointRunner.FlushChecksum(ctx, 4, 4, 4, 4)

	for _, d := range data2 {
		err = checkpoint.AppendForBackup(ctx, checkpointRunner, "+", []byte(d.StartKey), []byte(d.EndKey), []*backuppb.File{
			{Name: d.Name},
			{Name: d.Name2},
		})
		require.NoError(t, err)
	}

	checkpointRunner.WaitForFinish(ctx, true)

	checker := func(groupKey string, resp checkpoint.BackupValueType) {
		require.NotNil(t, resp)
		d, ok := data[string(resp.StartKey)]
		if !ok {
			d, ok = data2[string(resp.StartKey)]
			require.True(t, ok)
		}
		require.Equal(t, d.StartKey, string(resp.StartKey))
		require.Equal(t, d.EndKey, string(resp.EndKey))
		require.Equal(t, d.Name, resp.Files[0].Name)
		require.Equal(t, d.Name2, resp.Files[1].Name)
	}

	_, err = checkpoint.WalkCheckpointFileForBackup(ctx, s, cipher, checker)
	require.NoError(t, err)

	checkpointMeta := &checkpoint.CheckpointMetadataForBackup{
		ConfigHash: []byte("123456"),
		BackupTS:   123456,
	}

	err = checkpoint.SaveCheckpointMetadata(ctx, s, checkpointMeta)
	require.NoError(t, err)
	meta, err := checkpoint.LoadCheckpointMetadata(ctx, s)
	require.NoError(t, err)

	var i int64
	for i = 1; i <= 4; i++ {
		require.Equal(t, meta.CheckpointChecksum[i].Crc64xor, uint64(i))
	}
}

func TestCheckpointRestoreRunner(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	g := gluetidb.New()
	se, err := g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)

	err = checkpoint.SaveCheckpointMetadataForSnapshotRestore(ctx, se, &checkpoint.CheckpointMetadataForSnapshotRestore{})
	require.NoError(t, err)
	checkpointRunner, err := checkpoint.StartCheckpointRestoreRunnerForTest(ctx, se, 5*time.Second, 3*time.Second)
	require.NoError(t, err)

	data := map[string]struct {
		RangeKey string
		Name     string
		Name2    string
	}{
		"a": {
			RangeKey: "a",
		},
		"A": {
			RangeKey: "A",
		},
		"1": {
			RangeKey: "1",
		},
	}

	data2 := map[string]struct {
		RangeKey string
		Name     string
		Name2    string
	}{
		"+": {
			RangeKey: "+",
		},
	}

	for _, d := range data {
		err = checkpoint.AppendRangesForRestore(ctx, checkpointRunner, 1, d.RangeKey)
		require.NoError(t, err)
	}

	checkpointRunner.FlushChecksum(ctx, 1, 1, 1, 1)
	checkpointRunner.FlushChecksum(ctx, 2, 2, 2, 2)
	checkpointRunner.FlushChecksum(ctx, 3, 3, 3, 3)
	checkpointRunner.FlushChecksum(ctx, 4, 4, 4, 4)

	for _, d := range data2 {
		err = checkpoint.AppendRangesForRestore(ctx, checkpointRunner, 2, d.RangeKey)
		require.NoError(t, err)
	}

	checkpointRunner.WaitForFinish(ctx, true)

	se, err = g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)
	respCount := 0
	checker := func(tableID int64, resp checkpoint.RestoreValueType) {
		require.NotNil(t, resp)
		d, ok := data[resp.RangeKey]
		if !ok {
			d, ok = data2[resp.RangeKey]
			require.Equal(t, tableID, int64(2))
			require.True(t, ok)
		} else {
			require.Equal(t, tableID, int64(1))
		}
		require.Equal(t, d.RangeKey, resp.RangeKey)
		respCount += 1
	}

	_, err = checkpoint.LoadCheckpointDataForSnapshotRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor(), checker)
	require.NoError(t, err)
	require.Equal(t, 4, respCount)

	checksum, _, err := checkpoint.LoadCheckpointChecksumForRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor())
	require.NoError(t, err)

	var i int64
	for i = 1; i <= 4; i++ {
		require.Equal(t, checksum[i].Crc64xor, uint64(i))
	}

	err = checkpoint.RemoveCheckpointDataForSnapshotRestore(ctx, s.Mock.Domain, se)
	require.NoError(t, err)

	exists := checkpoint.ExistsSnapshotRestoreCheckpoint(ctx, s.Mock.Domain)
	require.False(t, exists)
	exists = s.Mock.Domain.InfoSchema().SchemaExists(pmodel.NewCIStr(checkpoint.SnapshotRestoreCheckpointDatabaseName))
	require.False(t, exists)
}

func TestCheckpointRunnerRetry(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	g := gluetidb.New()
	se, err := g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)

	err = checkpoint.SaveCheckpointMetadataForSnapshotRestore(ctx, se, &checkpoint.CheckpointMetadataForSnapshotRestore{})
	require.NoError(t, err)
	checkpointRunner, err := checkpoint.StartCheckpointRestoreRunnerForTest(ctx, se, 100*time.Millisecond, 300*time.Millisecond)
	require.NoError(t, err)

	err = failpoint.Enable("github.com/pingcap/tidb/br/pkg/checkpoint/failed-after-checkpoint-flushes", "return(true)")
	require.NoError(t, err)
	defer func() {
		err = failpoint.Disable("github.com/pingcap/tidb/br/pkg/checkpoint/failed-after-checkpoint-flushes")
		require.NoError(t, err)
	}()
	err = checkpoint.AppendRangesForRestore(ctx, checkpointRunner, 1, "123")
	require.NoError(t, err)
	err = checkpoint.AppendRangesForRestore(ctx, checkpointRunner, 2, "456")
	require.NoError(t, err)
	err = checkpointRunner.FlushChecksum(ctx, 1, 1, 1, 1)
	require.NoError(t, err)
	err = checkpointRunner.FlushChecksum(ctx, 2, 2, 2, 2)
	time.Sleep(time.Second)
	err = failpoint.Disable("github.com/pingcap/tidb/br/pkg/checkpoint/failed-after-checkpoint-flushes")
	require.NoError(t, err)
	err = checkpoint.AppendRangesForRestore(ctx, checkpointRunner, 3, "789")
	require.NoError(t, err)
	err = checkpointRunner.FlushChecksum(ctx, 3, 3, 3, 3)
	require.NoError(t, err)
	checkpointRunner.WaitForFinish(ctx, true)
	se, err = g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)
	recordSet := make(map[string]int)
	_, err = checkpoint.LoadCheckpointDataForSnapshotRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor(),
		func(tableID int64, rangeKey checkpoint.RestoreValueType) {
			recordSet[fmt.Sprintf("%d_%s", tableID, rangeKey)] += 1
		})
	require.NoError(t, err)
	require.LessOrEqual(t, 1, recordSet["1_{123}"])
	require.LessOrEqual(t, 1, recordSet["2_{456}"])
	require.LessOrEqual(t, 1, recordSet["3_{789}"])
	items, _, err := checkpoint.LoadCheckpointChecksumForRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor())
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d_%d_%d", items[1].Crc64xor, items[1].TotalBytes, items[1].TotalKvs), "1_1_1")
	require.Equal(t, fmt.Sprintf("%d_%d_%d", items[2].Crc64xor, items[2].TotalBytes, items[2].TotalKvs), "2_2_2")
	require.Equal(t, fmt.Sprintf("%d_%d_%d", items[3].Crc64xor, items[3].TotalBytes, items[3].TotalKvs), "3_3_3")
}

func TestCheckpointRunnerNoRetry(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	g := gluetidb.New()
	se, err := g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)

	err = checkpoint.SaveCheckpointMetadataForSnapshotRestore(ctx, se, &checkpoint.CheckpointMetadataForSnapshotRestore{})
	require.NoError(t, err)
	checkpointRunner, err := checkpoint.StartCheckpointRestoreRunnerForTest(ctx, se, 100*time.Millisecond, 300*time.Millisecond)
	require.NoError(t, err)

	err = checkpoint.AppendRangesForRestore(ctx, checkpointRunner, 1, "123")
	require.NoError(t, err)
	err = checkpoint.AppendRangesForRestore(ctx, checkpointRunner, 2, "456")
	require.NoError(t, err)
	err = checkpointRunner.FlushChecksum(ctx, 1, 1, 1, 1)
	require.NoError(t, err)
	err = checkpointRunner.FlushChecksum(ctx, 2, 2, 2, 2)
	require.NoError(t, err)
	time.Sleep(time.Second)
	checkpointRunner.WaitForFinish(ctx, true)
	se, err = g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)
	recordSet := make(map[string]int)
	_, err = checkpoint.LoadCheckpointDataForSnapshotRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor(),
		func(tableID int64, rangeKey checkpoint.RestoreValueType) {
			recordSet[fmt.Sprintf("%d_%s", tableID, rangeKey)] += 1
		})
	require.NoError(t, err)
	require.Equal(t, 1, recordSet["1_{123}"])
	require.Equal(t, 1, recordSet["2_{456}"])
	items, _, err := checkpoint.LoadCheckpointChecksumForRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor())
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d_%d_%d", items[1].Crc64xor, items[1].TotalBytes, items[1].TotalKvs), "1_1_1")
	require.Equal(t, fmt.Sprintf("%d_%d_%d", items[2].Crc64xor, items[2].TotalBytes, items[2].TotalKvs), "2_2_2")
}

func TestCheckpointLogRestoreRunner(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	g := gluetidb.New()
	se, err := g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)

	err = checkpoint.SaveCheckpointMetadataForLogRestore(ctx, se, &checkpoint.CheckpointMetadataForLogRestore{})
	require.NoError(t, err)
	checkpointRunner, err := checkpoint.StartCheckpointLogRestoreRunnerForTest(ctx, se, 5*time.Second)
	require.NoError(t, err)

	data := map[string]map[int][]struct {
		table int64
		foff  int
	}{
		"a": {
			0: {{1, 0}, {2, 1}},
			1: {{1, 0}},
		},
		"A": {
			0: {{3, 1}},
		},
	}

	data2 := map[string]map[int][]struct {
		table int64
		foff  int
	}{
		"+": {
			1: {{1, 0}},
		},
	}

	for k, d := range data {
		for g, fs := range d {
			for _, f := range fs {
				err = checkpoint.AppendRangeForLogRestore(ctx, checkpointRunner, k, f.table, g, f.foff)
				require.NoError(t, err)
			}
		}
	}

	for k, d := range data2 {
		for g, fs := range d {
			for _, f := range fs {
				err = checkpoint.AppendRangeForLogRestore(ctx, checkpointRunner, k, f.table, g, f.foff)
				require.NoError(t, err)
			}
		}
	}

	checkpointRunner.WaitForFinish(ctx, true)

	se, err = g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)
	respCount := 0
	checker := func(metaKey string, resp checkpoint.LogRestoreValueMarshaled) {
		require.NotNil(t, resp)
		d, ok := data[metaKey]
		if !ok {
			d, ok = data2[metaKey]
			require.True(t, ok)
		}
		fs, ok := d[resp.Goff]
		require.True(t, ok)
		for _, f := range fs {
			foffs, exists := resp.Foffs[f.table]
			if !exists {
				continue
			}
			for _, foff := range foffs {
				if f.foff == foff {
					respCount += 1
					return
				}
			}
		}
		require.FailNow(t, "not found in the original data")
	}

	_, err = checkpoint.LoadCheckpointDataForLogRestore(ctx, se.GetSessionCtx().GetRestrictedSQLExecutor(), checker)
	require.NoError(t, err)
	require.Equal(t, 4, respCount)

	err = checkpoint.RemoveCheckpointDataForLogRestore(ctx, s.Mock.Domain, se)
	require.NoError(t, err)

	exists := checkpoint.ExistsLogRestoreCheckpointMetadata(ctx, s.Mock.Domain)
	require.False(t, exists)
	exists = s.Mock.Domain.InfoSchema().SchemaExists(pmodel.NewCIStr(checkpoint.LogRestoreCheckpointDatabaseName))
	require.False(t, exists)
}

func getLockData(p, l int64) ([]byte, error) {
	lock := checkpoint.CheckpointLock{
		LockId:   oracle.ComposeTS(p, l),
		ExpireAt: p + 10,
	}
	return json.Marshal(lock)
}

func TestCheckpointRunnerLock(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	os.MkdirAll(base+checkpoint.CheckpointDataDirForBackup, 0o755)
	os.MkdirAll(base+checkpoint.CheckpointChecksumDirForBackup, 0o755)

	cipher := &backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
		CipherKey:  []byte("01234567890123456789012345678901"),
	}

	data, err := getLockData(10, 20)
	require.NoError(t, err)
	err = s.WriteFile(ctx, checkpoint.CheckpointLockPathForBackup, data)
	require.NoError(t, err)

	_, err = checkpoint.StartCheckpointBackupRunnerForTest(ctx, s, cipher, 5*time.Second, NewMockTimer(10, 10))
	require.Error(t, err)

	runner, err := checkpoint.StartCheckpointBackupRunnerForTest(ctx, s, cipher, 5*time.Second, NewMockTimer(30, 10))
	require.NoError(t, err)

	_, err = checkpoint.StartCheckpointBackupRunnerForTest(ctx, s, cipher, 5*time.Second, NewMockTimer(40, 10))
	require.Error(t, err)

	runner.WaitForFinish(ctx, true)
}
