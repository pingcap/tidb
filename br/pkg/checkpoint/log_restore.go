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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
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

type LogRestoreValueMarshaled struct {
	// group index in the metadata
	Goff int `json:"goff"`
	// downstream table id -> file indexes in the group
	Foffs map[int64][]int `json:"foffs"`
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

func newTableCheckpointStorage(se glue.Session, checkpointDBName string) *tableCheckpointStorage {
	return &tableCheckpointStorage{
		se:               se,
		checkpointDBName: checkpointDBName,
	}
}

// only for test
func StartCheckpointLogRestoreRunnerForTest(
	ctx context.Context,
	tick time.Duration,
	manager LogMetaManagerT,
) (*CheckpointRunner[LogRestoreKeyType, LogRestoreValueType], error) {
	cfg := DefaultTickDurationConfig()
	cfg.tickDurationForChecksum = tick
	cfg.tickDurationForFlush = tick
	return manager.StartCheckpointRunner(ctx, cfg, valueMarshalerForLogRestore)
}

// Notice that the session is owned by the checkpoint runner, and it will be also closed by it.
func StartCheckpointRunnerForLogRestore(
	ctx context.Context,
	manager LogMetaManagerT,
) (*CheckpointRunner[LogRestoreKeyType, LogRestoreValueType], error) {
	return manager.StartCheckpointRunner(ctx, DefaultTickDurationConfig(), valueMarshalerForLogRestore)
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

type CheckpointMetadataForLogRestore struct {
	UpstreamClusterID uint64 `json:"upstream-cluster-id"`
	RestoredTS        uint64 `json:"restored-ts"`
	StartTS           uint64 `json:"start-ts"`
	RewriteTS         uint64 `json:"rewrite-ts"`
	GcRatio           string `json:"gc-ratio"`
	// tiflash recorder items with snapshot restore records
	TiFlashItems map[int64]model.TiFlashReplicaInfo `json:"tiflash-recorder,omitempty"`
}

// RestoreProgress is a progress type for snapshot + log restore.
//
// Before the id-maps is persisted into external storage, the snapshot restore and
// id-maps building can be retried. So if the progress is in `InSnapshotRestore`,
// it can retry from snapshot restore.
//
// After the id-maps is persisted into external storage, there are some meta-kvs has
// been restored into the cluster, such as `rename ddl`. A situation could be:
//
// the first execution:
//
//	table A created in snapshot restore is renamed to table B in log restore
//	     table A (id 80)       -------------->        table B (id 80)
//	  ( snapshot restore )                            ( log restore )
//
// the second execution if don't skip snapshot restore:
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
	// Only when the id-maps is persisted, status turns into it.
	InLogRestoreAndIdMapPersisted
)

type CheckpointProgress struct {
	Progress RestoreProgress `json:"progress"`
}

// TaskInfoForLogRestore is tied to a specific cluster.
// It represents the last restore task executed in this cluster.
type TaskInfoForLogRestore struct {
	Metadata            *CheckpointMetadataForLogRestore
	HasSnapshotMetadata bool
	// the progress for this task
	Progress RestoreProgress
}

func (t *TaskInfoForLogRestore) IdMapSaved() bool {
	return t.Progress == InLogRestoreAndIdMapPersisted
}

func GetCheckpointTaskInfo(
	ctx context.Context,
	snapshotManager SnapshotMetaManagerT,
	logManager LogMetaManagerT,
) (*TaskInfoForLogRestore, error) {
	var (
		metadata *CheckpointMetadataForLogRestore
		progress RestoreProgress

		hasSnapshotMetadata bool = false
	)
	// get the progress
	if exists, err := logManager.ExistsCheckpointProgress(ctx); err != nil {
		return nil, errors.Trace(err)
	} else if exists {
		checkpointProgress, err := logManager.LoadCheckpointProgress(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		progress = checkpointProgress.Progress
	}
	// get the checkpoint metadata
	if exists, err := logManager.ExistsCheckpointMetadata(ctx); err != nil {
		return nil, errors.Trace(err)
	} else if exists {
		metadata, err = logManager.LoadCheckpointMetadata(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// exists the snapshot checkpoint metadata
	if snapshotManager != nil {
		existsSnapshotMetadata, err := snapshotManager.ExistsCheckpointMetadata(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		hasSnapshotMetadata = existsSnapshotMetadata
	}

	return &TaskInfoForLogRestore{
		Metadata:            metadata,
		HasSnapshotMetadata: hasSnapshotMetadata,
		Progress:            progress,
	}, nil
}

type CheckpointIngestIndexRepairSQL struct {
	IndexID    int64        `json:"index-id"`
	SchemaName pmodel.CIStr `json:"schema-name"`
	TableName  pmodel.CIStr `json:"table-name"`
	IndexName  string       `json:"index-name"`
	AddSQL     string       `json:"add-sql"`
	AddArgs    []any        `json:"add-args"`

	OldIndexIDFound bool `json:"-"`
	IndexRepaired   bool `json:"-"`
}

type CheckpointForeignKeyUpdateSQL struct {
	FKID       int64  `json:"fk-id"`
	SchemaName string `json:"schema-name"`
	TableName  string `json:"table-name"`
	FKName     string `json:"fk-name"`
	AddSQL     string `json:"add-sql"`
	AddArgs    []any  `json:"add-args"`

	OldForeignKeyFound bool `json:"-"`
	ForeignKeyUpdated  bool `json:"-"`
}

type CheckpointIngestIndexRepairSQLs struct {
	SQLs   []CheckpointIngestIndexRepairSQL
	FKSQLs []CheckpointForeignKeyUpdateSQL
}
