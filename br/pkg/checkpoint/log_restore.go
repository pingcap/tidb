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

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
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

type LogRestoreValueMarshaled struct {
	// group index in the metadata
	Goff int `json:"goff"`
	// downstream table id -> file indexes in the group
	Foffs map[int64][]int `json:"foffs"`
}

func (l LogRestoreValueMarshaled) IdentKey() []byte {
	log.Fatal("unimplement!")
	return nil
}

// valueMarshalerForLogRestore convert the checkpoint data‘s format to an smaller space-used format
// input format :
//
//	"group-key":"...",
//	"groups":[
//	  ["TableId": 1, "Goff": 0, "Foff": 0],
//	  ["TableId": 1, "Goff": 0, "Foff": 1],
//	  ...
//	],
//
// converted format :
//
//	"group-key":"...",
//	"groups":[
//	  ["Goff": 0, "Foffs":{"1", [0, 1]}],
//	  ...
//	],
func valueMarshalerForLogRestore(group *RangeGroup[LogRestoreKeyType, LogRestoreValueType]) ([]byte, error) {
	// goff -> table-id -> []foff
	gMap := make(map[int]map[int64][]int)
	for _, g := range group.Group {
		fMap, exists := gMap[g.Goff]
		if !exists {
			fMap = make(map[int64][]int)
			gMap[g.Goff] = fMap
		}

		fMap[g.TableID] = append(fMap[g.TableID], g.Foff)
	}

	logValues := make([]LogRestoreValueMarshaled, 0, len(gMap))
	for goff, foffs := range gMap {
		logValues = append(logValues, LogRestoreValueMarshaled{
			Goff:  goff,
			Foffs: foffs,
		})
	}

	return json.Marshal(&RangeGroup[LogRestoreKeyType, LogRestoreValueMarshaled]{
		GroupKey: group.GroupKey,
		Group:    logValues,
	})
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
		ctx, storage, cipher, nil, flushPositionForRestore(taskName), valueMarshalerForLogRestore)

	runner.startCheckpointMainLoop(ctx, tick, tick, 0)
	return runner, nil
}

func StartCheckpointRunnerForLogRestore(ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	taskName string,
) (*CheckpointRunner[LogRestoreKeyType, LogRestoreValueType], error) {
	runner := newCheckpointRunner[LogRestoreKeyType, LogRestoreValueType](
		ctx, storage, cipher, nil, flushPositionForRestore(taskName), valueMarshalerForLogRestore)

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(ctx, defaultTickDurationForFlush, defaultTckDurationForChecksum, 0)
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
	CheckpointIngestIndexRepairSQLPathFormat  = CheckpointDir + "/restore-%s/ingest-repair.meta"
)

func getCheckpointTaskInfoPathByID(clusterID uint64) string {
	return fmt.Sprintf(CheckpointTaskInfoForLogRestorePathFormat, clusterID)
}

func getCheckpointIngestIndexRepairPathByTaskName(taskName string) string {
	return fmt.Sprintf(CheckpointIngestIndexRepairSQLPathFormat, taskName)
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

func removeCheckpointTaskInfoForLogRestore(ctx context.Context, s storage.ExternalStorage, clusterID uint64) error {
	fileName := getCheckpointTaskInfoPathByID(clusterID)
	exists, err := s.FileExists(ctx, fileName)
	if err != nil {
		return errors.Trace(err)
	}

	if !exists {
		log.Warn("the task info file doesn't exist", zap.String("file", fileName))
		return nil
	}

	return s.DeleteFile(ctx, fileName)
}

type CheckpointIngestIndexRepairSQL struct {
	IndexID    int64       `json:"index-id"`
	SchemaName model.CIStr `json:"schema-name"`
	TableName  model.CIStr `json:"table-name"`
	IndexName  string      `json:"index-name"`
	AddSQL     string      `json:"add-sql"`
	AddArgs    []any       `json:"add-args"`
}

type CheckpointIngestIndexRepairSQLs struct {
	SQLs []CheckpointIngestIndexRepairSQL
}

func LoadCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	s storage.ExternalStorage,
	taskName string,
) (*CheckpointIngestIndexRepairSQLs, error) {
	m := &CheckpointIngestIndexRepairSQLs{}
	err := loadCheckpointMeta(ctx, s, getCheckpointIngestIndexRepairPathByTaskName(taskName), m)
	return m, err
}

func ExistsCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	s storage.ExternalStorage,
	taskName string,
) (bool, error) {
	return s.FileExists(ctx, getCheckpointIngestIndexRepairPathByTaskName(taskName))
}

func SaveCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	s storage.ExternalStorage,
	meta *CheckpointIngestIndexRepairSQLs,
	taskName string,
) error {
	return saveCheckpointMetadata(ctx, s, meta, getCheckpointIngestIndexRepairPathByTaskName(taskName))
}

func RemoveCheckpointDataForLogRestore(
	ctx context.Context,
	s storage.ExternalStorage,
	taskName string,
	clusterID uint64,
) error {
	if err := removeCheckpointTaskInfoForLogRestore(ctx, s, clusterID); err != nil {
		return errors.Annotatef(err,
			"failed to remove the task info file: clusterId is %d, taskName is %s",
			clusterID,
			taskName,
		)
	}
	prefix := fmt.Sprintf(CheckpointRestoreDirFormat, taskName)
	return removeCheckpointData(ctx, s, prefix)
}
