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
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type BackupKeyType = string
type BackupValueType = RangeType

const (
	CheckpointMetaPath             = "checkpoint.meta"
	CheckpointDataDirForBackup     = CheckpointDir + "/data"
	CheckpointChecksumDirForBackup = CheckpointDir + "/checksum"
	CheckpointLockPathForBackup    = CheckpointDir + "/checkpoint.lock"
)

func flushPositionForBackup() flushPosition {
	return flushPosition{
		CheckpointDataDir:     CheckpointDataDirForBackup,
		CheckpointChecksumDir: CheckpointChecksumDirForBackup,
		CheckpointLockPath:    CheckpointLockPathForBackup,
	}
}

// only for test
func StartCheckpointBackupRunnerForTest(
	ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	tick time.Duration,
	timer GlobalTimer,
) (*CheckpointRunner[BackupKeyType, BackupValueType], error) {
	runner := newCheckpointRunner[BackupKeyType, BackupValueType](ctx, storage, cipher, timer, flushPositionForBackup())

	err := runner.initialLock(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to initialize checkpoint lock.")
	}
	runner.startCheckpointMainLoop(ctx, tick, tick)
	return runner, nil
}

func StartCheckpointRunnerForBackup(
	ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	timer GlobalTimer,
) (*CheckpointRunner[BackupKeyType, BackupValueType], error) {
	runner := newCheckpointRunner[BackupKeyType, BackupValueType](ctx, storage, cipher, timer, flushPositionForBackup())

	err := runner.initialLock(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	runner.startCheckpointMainLoop(ctx, tickDurationForFlush, tickDurationForLock)
	return runner, nil
}

func AppendForBackup(
	ctx context.Context,
	r *CheckpointRunner[BackupKeyType, BackupValueType],
	groupKey BackupKeyType,
	startKey []byte,
	endKey []byte,
	files []*backuppb.File,
) error {
	return r.Append(ctx, &CheckpointMessage[BackupKeyType, BackupValueType]{
		GroupKey: groupKey,
		Group: []BackupValueType{
			{
				Range: &rtree.Range{
					StartKey: startKey,
					EndKey:   endKey,
					Files:    files,
				},
			},
		},
	})
}

// walk the whole checkpoint range files and retrieve the metadata of backed up ranges
// and return the total time cost in the past executions
func WalkCheckpointFileForBackup(
	ctx context.Context,
	s storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	fn func(BackupKeyType, BackupValueType),
) (time.Duration, error) {
	return walkCheckpointFile(ctx, s, cipher, CheckpointDataDirForBackup, fn)
}

type CheckpointMetadataForBackup struct {
	GCServiceId string        `json:"gc-service-id"`
	ConfigHash  []byte        `json:"config-hash"`
	BackupTS    uint64        `json:"backup-ts"`
	Ranges      []rtree.Range `json:"ranges"`

	CheckpointChecksum map[int64]*ChecksumItem    `json:"-"`
	CheckpointDataMap  map[string]rtree.RangeTree `json:"-"`
}

// load checkpoint metadata from the external storage
func LoadCheckpointMetadata(ctx context.Context, s storage.ExternalStorage) (*CheckpointMetadataForBackup, error) {
	m := &CheckpointMetadataForBackup{}
	err := loadCheckpointMeta(ctx, s, CheckpointMetaPath, m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m.CheckpointChecksum, _, err = loadCheckpointChecksum(ctx, s, CheckpointChecksumDirForBackup)
	return m, errors.Trace(err)
}

// save the checkpoint metadata into the external storage
func SaveCheckpointMetadata(ctx context.Context, s storage.ExternalStorage, meta *CheckpointMetadataForBackup) error {
	return saveCheckpointMetadata(ctx, s, meta, CheckpointMetaPath)
}
