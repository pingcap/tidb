// Copyright 2023 PingCAP, Inc.
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

package checkpoint

import (
	"context"
	"fmt"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type RestoreKeyType = int64
type CheckpointRestoreRunner = CheckpointRunner[RestoreKeyType]

const (
	CheckpointDataDirForRestoreFormat     = CheckpointDir + "/restore-%s/data"
	CheckpointChecksumDirForRestoreFormat = CheckpointDir + "/restore-%s/checksum"
	CheckpointMetaPathForRestore          = CheckpointDir + "/restore-%s/checkpoint.meta"
)

func getCheckpointMetaPathByName(taskName string) string {
	return fmt.Sprintf(CheckpointMetaPathForRestore, taskName)
}

func getCheckpointDataDirByName(taskName string) string {
	return fmt.Sprintf(CheckpointDataDirForRestoreFormat, taskName)
}

func getCheckpointChecksumDirByName(taskName string) string {
	return fmt.Sprintf(CheckpointChecksumDirForRestoreFormat, taskName)
}

func flushPositionForRestore(taskName string) flushPosition {
	return flushPosition{
		CheckpointDataDir:     getCheckpointDataDirByName(taskName),
		CheckpointChecksumDir: getCheckpointChecksumDirByName(taskName),
	}
}

func StartCheckpointRunnerForRestore(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo, taskName string) (*CheckpointRestoreRunner, error) {
	runner := newCheckpointRunner[RestoreKeyType](ctx, storage, cipher, nil, flushPositionForRestore(taskName))

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(ctx, tickDurationForFlush, 0)
	return runner, nil
}

// walk the whole checkpoint range files and retrieve the metadata of restored ranges
// and return the total time cost in the past executions
func WalkCheckpointFileForRestore(ctx context.Context, s storage.ExternalStorage, cipher *backuppb.CipherInfo, taskName string, fn func(tableID int64, rg *rtree.Range)) (time.Duration, error) {
	return walkCheckpointFile(ctx, s, cipher, getCheckpointDataDirByName(taskName), fn)
}

func LoadCheckpointChecksumForRestore(ctx context.Context, s storage.ExternalStorage, taskName string) (map[int64]*ChecksumItem, time.Duration, error) {
	return loadCheckpointChecksum(ctx, s, getCheckpointChecksumDirByName(taskName))
}

func LoadCheckpointMetaForRestore(ctx context.Context, s storage.ExternalStorage, taskName string) (*CheckpointMetadata, error) {
	return loadCheckpointMeta(ctx, s, getCheckpointMetaPathByName(taskName))
}

func SaveCheckpointMetadataForRestore(ctx context.Context, s storage.ExternalStorage, meta *CheckpointMetadata, taskName string) error {
	return saveCheckpointMetadata(ctx, s, meta, getCheckpointMetaPathByName(taskName))
}

func ExistsRestoreCheckpoint(ctx context.Context, s storage.ExternalStorage, taskName string) (bool, error) {
	return s.FileExists(ctx, getCheckpointMetaPathByName(taskName))
}
