// Copyright 2026 PingCAP, Inc.
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

package backup

import (
	"context"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type fixedCheckpointTimer struct{}

func (fixedCheckpointTimer) GetTS(context.Context) (int64, int64, error) {
	return 1, 1, nil
}

func TestBuildProgressRangeTreeLoadsCheckpointDataFromMetadataStorage(t *testing.T) {
	ctx := context.Background()
	rootStorage := objstore.NewMemStorage()
	metadataStorage := repo.NewPrefixedStorage(rootStorage, repo.SnapshotMetadataDir(repo.BackupID(0x1234)))
	cipher := &backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}

	runner, err := checkpoint.StartCheckpointBackupRunnerForTest(ctx, metadataStorage, cipher, time.Hour, fixedCheckpointTimer{})
	require.NoError(t, err)
	require.NoError(t, checkpoint.AppendForBackup(ctx, runner, []byte("a"), []byte("b"), []*backuppb.File{{Name: "1.sst"}}))
	runner.WaitForFinish(ctx, true)

	client := &Client{
		storage:     rootStorage,
		metaStorage: metadataStorage,
		cipher:      cipher,
		checkpointMeta: &checkpoint.CheckpointMetadataForBackup{
			LoadCheckpointDataMap: true,
		},
	}
	metaWriter := metautil.NewMetaWriter(objstore.NewMemStorage(), metautil.MetaFileSize, false, "", cipher)
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	progressTree, err := client.BuildProgressRangeTree(ctx, []rtree.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}, metaWriter, func(ProgressUnit) {})
	require.NoError(t, err)

	incomplete, err := progressTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Len(t, incomplete, 0)
	require.NoError(t, metaWriter.FinishWriteMetas(ctx, metautil.AppendDataFile))
}

type checkpointTestMgr struct {
	ClientMgr
}

func (checkpointTestMgr) GetPDClient() pd.Client {
	return checkpointTestPDClient{}
}

type checkpointTestPDClient struct {
	pd.Client
}

func (checkpointTestPDClient) GetTS(context.Context) (int64, int64, error) {
	return 1, 1, nil
}

func TestCheckCheckpointRejectsAnotherCluster(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	cfgHash := []byte("hash")
	newClient := func(clusterID uint64) *Client {
		return &Client{
			mgr:         checkpointTestMgr{},
			clusterID:   clusterID,
			storage:     storage,
			metaStorage: storage,
			cipher:      &backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT},
		}
	}

	first := newClient(1)
	require.NoError(t, first.CheckCheckpoint(cfgHash))
	require.NoError(t, first.StartCheckpointRunner(ctx, cfgHash, 2, 0, "gc-service", func(ProgressUnit) {}))
	first.WaitForFinishCheckpoint(ctx, true)

	other := newClient(3)
	require.NoError(t, other.LoadCheckpointMetadataFromStorage(ctx, storage))
	err := other.CheckCheckpoint(cfgHash)
	require.Error(t, err)
	require.Contains(t, err.Error(), "created by the cluster 1 instead of the current cluster 3")

	same := newClient(1)
	require.NoError(t, same.LoadCheckpointMetadataFromStorage(ctx, storage))
	require.NoError(t, same.CheckCheckpoint(cfgHash))
	backupTS, err := same.GetTS(ctx, 0, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(2), backupTS)
	require.NoError(t, same.StartCheckpointRunner(ctx, cfgHash, backupTS, 0, "gc-service", func(ProgressUnit) {}))
	same.WaitForFinishCheckpoint(ctx, true)

	// legacy checkpoint metadata without the cluster id
	require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, storage, &checkpoint.CheckpointMetadataForBackup{ConfigHash: cfgHash, BackupTS: 2}))
	legacy := newClient(3)
	require.NoError(t, legacy.LoadCheckpointMetadataFromStorage(ctx, storage))
	require.NoError(t, legacy.CheckCheckpoint(cfgHash))
}
