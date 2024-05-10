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
	"os"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestCheckpointMeta(t *testing.T) {
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

	taskName := "test123"
	checkpointMetaForRestore := &checkpoint.CheckpointMetadataForRestore{
		SchedulersConfig: &pdutil.ClusterConfig{
			Schedulers: []string{"1", "2"},
			ScheduleCfg: map[string]any{
				"1": "2",
				"2": "1",
			},
		},
		GcRatio: "123",
	}
	err = checkpoint.SaveCheckpointMetadataForRestore(ctx, s, checkpointMetaForRestore, taskName)
	require.NoError(t, err)

	checkpointMetaForRestore2, err := checkpoint.LoadCheckpointMetadataForRestore(ctx, s, taskName)
	require.NoError(t, err)
	require.Equal(t, checkpointMetaForRestore.SchedulersConfig, checkpointMetaForRestore2.SchedulersConfig)
	require.Equal(t, checkpointMetaForRestore.GcRatio, checkpointMetaForRestore2.GcRatio)

	exists, err := checkpoint.ExistsCheckpointTaskInfo(ctx, s, 123)
	require.NoError(t, err)
	require.False(t, exists)
	err = checkpoint.SaveCheckpointTaskInfoForLogRestore(ctx, s, &checkpoint.CheckpointTaskInfoForLogRestore{
		Progress:     checkpoint.InLogRestoreAndIdMapPersist,
		StartTS:      1,
		RestoreTS:    2,
		RewriteTS:    3,
		TiFlashItems: map[int64]model.TiFlashReplicaInfo{1: {Count: 1}},
	}, 123)
	require.NoError(t, err)
	taskInfo, err := checkpoint.LoadCheckpointTaskInfoForLogRestore(ctx, s, 123)
	require.NoError(t, err)
	require.Equal(t, taskInfo.Progress, checkpoint.InLogRestoreAndIdMapPersist)
	require.Equal(t, taskInfo.StartTS, uint64(1))
	require.Equal(t, taskInfo.RestoreTS, uint64(2))
	require.Equal(t, taskInfo.RewriteTS, uint64(3))
	require.Equal(t, taskInfo.TiFlashItems[1].Count, uint64(1))

	exists, err = checkpoint.ExistsCheckpointIngestIndexRepairSQLs(ctx, s, "123")
	require.NoError(t, err)
	require.False(t, exists)
	err = checkpoint.SaveCheckpointIngestIndexRepairSQLs(ctx, s, &checkpoint.CheckpointIngestIndexRepairSQLs{
		SQLs: []checkpoint.CheckpointIngestIndexRepairSQL{
			{
				IndexID:    1,
				SchemaName: model.NewCIStr("2"),
				TableName:  model.NewCIStr("3"),
				IndexName:  "4",
				AddSQL:     "5",
				AddArgs:    []any{"6", "7", "8"},
			},
		},
	}, "123")
	require.NoError(t, err)
	repairSQLs, err := checkpoint.LoadCheckpointIngestIndexRepairSQLs(ctx, s, "123")
	require.NoError(t, err)
	require.Equal(t, repairSQLs.SQLs[0].IndexID, int64(1))
	require.Equal(t, repairSQLs.SQLs[0].SchemaName, model.NewCIStr("2"))
	require.Equal(t, repairSQLs.SQLs[0].TableName, model.NewCIStr("3"))
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
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	taskName := "test"

	cipher := &backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
		CipherKey:  []byte("01234567890123456789012345678901"),
	}
	checkpointRunner, err := checkpoint.StartCheckpointRestoreRunnerForTest(ctx, s, cipher, 5*time.Second, taskName)
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
	}

	_, err = checkpoint.WalkCheckpointFileForRestore(ctx, s, cipher, taskName, checker)
	require.NoError(t, err)

	checksum, _, err := checkpoint.LoadCheckpointChecksumForRestore(ctx, s, taskName)
	require.NoError(t, err)

	var i int64
	for i = 1; i <= 4; i++ {
		require.Equal(t, checksum[i].Crc64xor, uint64(i))
	}
}

func TestCheckpointLogRestoreRunner(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	taskName := "test"

	cipher := &backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
		CipherKey:  []byte("01234567890123456789012345678901"),
	}
	checkpointRunner, err := checkpoint.StartCheckpointLogRestoreRunnerForTest(ctx, s, cipher, 5*time.Second, taskName)
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
					return
				}
			}
		}
		require.FailNow(t, "not found in the original data")
	}

	_, err = checkpoint.WalkCheckpointFileForRestore(ctx, s, cipher, taskName, checker)
	require.NoError(t, err)
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
