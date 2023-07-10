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
	"encoding/json"
	"fmt"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type RestoreKeyType = int64
type RestoreValueType struct {
	// the file key of a range
	RangeKey string
}

func (rv RestoreValueType) IdentKey() []byte {
	return []byte(rv.RangeKey)
}

const (
	CheckpointRestoreDirFormat            = CheckpointDir + "/restore-%s"
	CheckpointDataDirForRestoreFormat     = CheckpointRestoreDirFormat + "/data"
	CheckpointChecksumDirForRestoreFormat = CheckpointRestoreDirFormat + "/checksum"
	CheckpointMetaPathForRestoreFormat    = CheckpointRestoreDirFormat + "/checkpoint.meta"
)

func getCheckpointMetaPathByName(taskName string) string {
	return fmt.Sprintf(CheckpointMetaPathForRestoreFormat, taskName)
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

func valueMarshalerForRestore(group *RangeGroup[RestoreKeyType, RestoreValueType]) ([]byte, error) {
	return json.Marshal(group)
}

// only for test
func StartCheckpointRestoreRunnerForTest(
	ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	tick time.Duration,
	taskName string,
) (*CheckpointRunner[RestoreKeyType, RestoreValueType], error) {
	runner := newCheckpointRunner[RestoreKeyType, RestoreValueType](
		ctx, storage, cipher, nil, flushPositionForRestore(taskName), valueMarshalerForRestore)

	runner.startCheckpointMainLoop(ctx, tick, tick, 0)
	return runner, nil
}

func StartCheckpointRunnerForRestore(
	ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	taskName string,
) (*CheckpointRunner[RestoreKeyType, RestoreValueType], error) {
	runner := newCheckpointRunner[RestoreKeyType, RestoreValueType](
		ctx, storage, cipher, nil, flushPositionForRestore(taskName), valueMarshalerForRestore)

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(ctx, defaultTickDurationForFlush, defaultTckDurationForChecksum, 0)
	return runner, nil
}

func AppendRangesForRestore(
	ctx context.Context,
	r *CheckpointRunner[RestoreKeyType, RestoreValueType],
	tableID RestoreKeyType,
	rangeKey string,
) error {
	return r.Append(ctx, &CheckpointMessage[RestoreKeyType, RestoreValueType]{
		GroupKey: tableID,
		Group: []RestoreValueType{
			{RangeKey: rangeKey},
		},
	})
}

// walk the whole checkpoint range files and retrieve the metadata of restored ranges
// and return the total time cost in the past executions
func WalkCheckpointFileForRestore[K KeyType, V ValueType](
	ctx context.Context,
	s storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	taskName string,
	fn func(K, V),
) (time.Duration, error) {
	return walkCheckpointFile(ctx, s, cipher, getCheckpointDataDirByName(taskName), fn)
}

func LoadCheckpointChecksumForRestore(
	ctx context.Context,
	s storage.ExternalStorage,
	taskName string,
) (map[int64]*ChecksumItem, time.Duration, error) {
	return loadCheckpointChecksum(ctx, s, getCheckpointChecksumDirByName(taskName))
}

type CheckpointMetadataForRestore struct {
	SchedulersConfig *pdutil.ClusterConfig `json:"schedulers-config,omitempty"`
	GcRatio          string                `json:"gc-ratio,omitempty"`
}

func LoadCheckpointMetadataForRestore(
	ctx context.Context,
	s storage.ExternalStorage,
	taskName string,
) (*CheckpointMetadataForRestore, error) {
	m := &CheckpointMetadataForRestore{}
	err := loadCheckpointMeta(ctx, s, getCheckpointMetaPathByName(taskName), m)
	return m, err
}

func SaveCheckpointMetadataForRestore(
	ctx context.Context,
	s storage.ExternalStorage,
	meta *CheckpointMetadataForRestore,
	taskName string,
) error {
	return saveCheckpointMetadata(ctx, s, meta, getCheckpointMetaPathByName(taskName))
}

func ExistsRestoreCheckpoint(
	ctx context.Context,
	s storage.ExternalStorage,
	taskName string,
) (bool, error) {
	return s.FileExists(ctx, getCheckpointMetaPathByName(taskName))
}

func RemoveCheckpointDataForRestore(ctx context.Context, s storage.ExternalStorage, taskName string) error {
	prefix := fmt.Sprintf(CheckpointRestoreDirFormat, taskName)
	return removeCheckpointData(ctx, s, prefix)
}
