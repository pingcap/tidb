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
	"encoding/base64"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

type fixedCheckpointTimer struct{}

func (fixedCheckpointTimer) GetTS(context.Context) (int64, int64, error) {
	return 1, 1, nil
}

func TestBuildProgressRangeTreeLoadsCheckpointDataFromMetadataStorage(t *testing.T) {
	ctx := context.Background()
	rootStorage, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	metadataStorage := repo.NewPrefixedStorage(rootStorage, repo.SnapshotMetadataDir(repo.BackupID(0x1234)))
	cipher := &backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	groupKey := base64.URLEncoding.EncodeToString([]byte("a"))

	runner, err := checkpoint.StartCheckpointBackupRunnerForTest(ctx, metadataStorage, cipher, time.Hour, fixedCheckpointTimer{})
	require.NoError(t, err)
	require.NoError(t, checkpoint.AppendForBackup(ctx, runner, groupKey, []byte("a"), []byte("b"), []*backuppb.File{{Name: "1.sst"}}))
	runner.WaitForFinish(ctx, true)

	client := &Client{
		storage:     rootStorage,
		metaStorage: metadataStorage,
		cipher:      cipher,
	}
	rangeDataMap, err := client.loadCheckpointRanges(ctx, func() {})
	require.NoError(t, err)
	client.checkpointMeta = &checkpoint.CheckpointMetadataForBackup{CheckpointDataMap: rangeDataMap}

	progressTree, err := client.BuildProgressRangeTree([]rtree.Range{{StartKey: []byte("a"), EndKey: []byte("b")}})
	require.NoError(t, err)
	incomplete := progressTree.Iter().GetIncompleteRanges()
	require.Len(t, incomplete, 0)
}
