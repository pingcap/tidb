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
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/parser/model"
)

type LogRestoreKeyType = string
type LogRestoreValueType struct {
	// downstream table id
	TableID int64
	// group index in the metadata
	Goff int
	// file index in the group
	Foff int
}

func (l LogRestoreValueType) IdentKey() []byte {
	return []byte(fmt.Sprint(l.Goff, '.', l.Foff, '.', l.TableID))
}

// only for test
func StartCheckpointLogRestoreRunnerForTest(
	ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	tick time.Duration,
	taskName string,
) (*CheckpointRunner[LogRestoreKeyType, LogRestoreValueType], error) {
	runner := newCheckpointRunner[LogRestoreKeyType, LogRestoreValueType](
		ctx, storage, cipher, nil, flushPositionForRestore(taskName))

	runner.startCheckpointMainLoop(ctx, tick, 0)
	return runner, nil
}

func StartCheckpointRunnerForLogRestore(ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	taskName string,
) (*CheckpointRunner[LogRestoreKeyType, LogRestoreValueType], error) {
	runner := newCheckpointRunner[LogRestoreKeyType, LogRestoreValueType](
		ctx, storage, cipher, nil, flushPositionForRestore(taskName))

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(ctx, tickDurationForFlush, 0)
	return runner, nil
}

func AppendRangeForLogRestore(
	ctx context.Context,
	r *CheckpointRunner[LogRestoreKeyType, LogRestoreValueType],
	groupKey LogRestoreKeyType,
	tableID int64,
	goff int,
	foff int,
) error {
	return r.Append(ctx, &CheckpointMessage[LogRestoreKeyType, LogRestoreValueType]{
		GroupKey: groupKey,
		Group: []LogRestoreValueType{
			{
				TableID: tableID,
				Goff:    goff,
				Foff:    foff,
			},
		},
	})
}

const (
	CheckpointTaskInfoForLogRestorePathFormat = CheckpointDir + "/restore-%d/taskInfo.meta"
)

func getCheckpointTaskInfoPathByID(clusterID uint64) string {
	return fmt.Sprintf(CheckpointTaskInfoForLogRestorePathFormat, clusterID)
}

// A progress type for snapshot + log restore.
//
// Before the id-maps is persist into external storage, the snapshot restore and
// id-maps constructure can be repeated. So if the progress is in `InSnapshotRestore`,
// it can retry from snapshot restore.
//
// After the id-maps is persist into external storage, there are some meta-kvs has
// been restored into the cluster, such as `rename ddl`. Where would be a situation:
//
// the first execution:
//
//	table A created in snapshot restore is renamed to table B in log restore
//	     table A (id 80)       -------------->        table B (id 80)
//	  ( snapshot restore )                            ( log restore )
//
// the second execution if don't skip snasphot restore:
//
//	table A is created again in snapshot restore, because there is no table named A
//	     table A (id 81)       -------------->   [not in id-maps, so ignored]
//	  ( snapshot restore )                            ( log restore )
//
// Finally, there is a duplicated table A in the cluster.
// Therefore, need to skip snapshot restore when the progress is `InLogRestoreAndIdMapPersist`.
type RestoreProgress int

const (
	InSnapshotRestore RestoreProgress = iota
	// Only when the id-maps is persist, status turns into it.
	InLogRestoreAndIdMapPersist
)

// CheckpointTaskInfo is unique information within the same cluster id. It represents the last
// restore task executed for this cluster.
type CheckpointTaskInfoForLogRestore struct {
	// the progress for this task
	Progress RestoreProgress `json:"progress"`
	// a task marker to distinguish the different tasks
	StartTS   uint64 `json:"start-ts"`
	RestoreTS uint64 `json:"restore-ts"`
	// updated in the progress of `InLogRestoreAndIdMapPersist`
	RewriteTS uint64 `json:"rewrite-ts"`
	// tiflash recorder items with snapshot restore records
	TiFlashItems map[int64]model.TiFlashReplicaInfo `json:"tiflash-recorder,omitempty"`
}

func LoadCheckpointTaskInfoForLogRestore(
	ctx context.Context,
	s storage.ExternalStorage,
	clusterID uint64,
) (*CheckpointTaskInfoForLogRestore, error) {
	m := &CheckpointTaskInfoForLogRestore{}
	err := loadCheckpointMeta(ctx, s, getCheckpointTaskInfoPathByID(clusterID), m)
	return m, err
}

func SaveCheckpointTaskInfoForLogRestore(
	ctx context.Context,
	s storage.ExternalStorage,
	meta *CheckpointTaskInfoForLogRestore,
	clusterID uint64,
) error {
	return saveCheckpointMetadata(ctx, s, meta, getCheckpointTaskInfoPathByID(clusterID))
}

func ExistsCheckpointTaskInfo(
	ctx context.Context,
	s storage.ExternalStorage,
	clusterID uint64,
) (bool, error) {
	return s.FileExists(ctx, getCheckpointTaskInfoPathByID(clusterID))
}
