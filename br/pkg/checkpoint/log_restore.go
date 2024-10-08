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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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

func newTableCheckpointStorage(se glue.Session, checkpointDBName string) *tableCheckpointStorage {
	return &tableCheckpointStorage{
		se:               se,
		checkpointDBName: checkpointDBName,
	}
}

// only for test
func StartCheckpointLogRestoreRunnerForTest(
	ctx context.Context,
	se glue.Session,
	tick time.Duration,
) (*CheckpointRunner[LogRestoreKeyType, LogRestoreValueType], error) {
	runner := newCheckpointRunner[LogRestoreKeyType, LogRestoreValueType](
		newTableCheckpointStorage(se, LogRestoreCheckpointDatabaseName),
		nil, valueMarshalerForLogRestore)

	runner.startCheckpointMainLoop(ctx, tick, tick, 0, defaultRetryDuration)
	return runner, nil
}

func StartCheckpointRunnerForLogRestore(
	ctx context.Context,
	se glue.Session,
) (*CheckpointRunner[LogRestoreKeyType, LogRestoreValueType], error) {
	runner := newCheckpointRunner[LogRestoreKeyType, LogRestoreValueType](
		newTableCheckpointStorage(se, LogRestoreCheckpointDatabaseName),
		nil, valueMarshalerForLogRestore)

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(
		ctx,
		defaultTickDurationForFlush, defaultTickDurationForChecksum, 0, defaultRetryDuration)
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

// load the whole checkpoint range data and retrieve the metadata of restored ranges
// and return the total time cost in the past executions
func LoadCheckpointDataForLogRestore[K KeyType, V ValueType](
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	fn func(K, V),
) (time.Duration, error) {
	return selectCheckpointData(ctx, execCtx, LogRestoreCheckpointDatabaseName, fn)
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

func LoadCheckpointMetadataForLogRestore(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
) (*CheckpointMetadataForLogRestore, error) {
	m := &CheckpointMetadataForLogRestore{}
	err := selectCheckpointMeta(ctx, execCtx, LogRestoreCheckpointDatabaseName, checkpointMetaTableName, m)
	return m, err
}

func SaveCheckpointMetadataForLogRestore(
	ctx context.Context,
	se glue.Session,
	meta *CheckpointMetadataForLogRestore,
) error {
	err := initCheckpointTable(ctx, se, LogRestoreCheckpointDatabaseName, []string{checkpointDataTableName})
	if err != nil {
		return errors.Trace(err)
	}
	return insertCheckpointMeta(ctx, se, LogRestoreCheckpointDatabaseName, checkpointMetaTableName, meta)
}

func ExistsLogRestoreCheckpointMetadata(
	ctx context.Context,
	dom *domain.Domain,
) bool {
	return dom.InfoSchema().
		TableExists(pmodel.NewCIStr(LogRestoreCheckpointDatabaseName), pmodel.NewCIStr(checkpointMetaTableName))
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

type CheckpointProgress struct {
	Progress RestoreProgress `json:"progress"`
}

func LoadCheckpointProgress(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
) (*CheckpointProgress, error) {
	m := &CheckpointProgress{}
	err := selectCheckpointMeta(ctx, execCtx, LogRestoreCheckpointDatabaseName, checkpointProgressTableName, m)
	return m, errors.Trace(err)
}

func SaveCheckpointProgress(
	ctx context.Context,
	se glue.Session,
	meta *CheckpointProgress,
) error {
	return insertCheckpointMeta(ctx, se, LogRestoreCheckpointDatabaseName, checkpointProgressTableName, meta)
}

func ExistsCheckpointProgress(
	ctx context.Context,
	dom *domain.Domain,
) bool {
	return dom.InfoSchema().
		TableExists(pmodel.NewCIStr(LogRestoreCheckpointDatabaseName), pmodel.NewCIStr(checkpointProgressTableName))
}

// CheckpointTaskInfo is unique information within the same cluster id. It represents the last
// restore task executed for this cluster.
type CheckpointTaskInfoForLogRestore struct {
	Metadata            *CheckpointMetadataForLogRestore
	HasSnapshotMetadata bool
	// the progress for this task
	Progress RestoreProgress
}

func TryToGetCheckpointTaskInfo(
	ctx context.Context,
	dom *domain.Domain,
	execCtx sqlexec.RestrictedSQLExecutor,
) (*CheckpointTaskInfoForLogRestore, error) {
	var (
		metadata *CheckpointMetadataForLogRestore
		progress RestoreProgress
		err      error
	)
	// get the progress
	if ExistsCheckpointProgress(ctx, dom) {
		checkpointProgress, err := LoadCheckpointProgress(ctx, execCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		progress = checkpointProgress.Progress
	}
	// get the checkpoint metadata
	if ExistsLogRestoreCheckpointMetadata(ctx, dom) {
		metadata, err = LoadCheckpointMetadataForLogRestore(ctx, execCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	hasSnapshotMetadata := ExistsSnapshotRestoreCheckpoint(ctx, dom)

	return &CheckpointTaskInfoForLogRestore{
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
}

type CheckpointIngestIndexRepairSQLs struct {
	SQLs []CheckpointIngestIndexRepairSQL
}

func LoadCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
) (*CheckpointIngestIndexRepairSQLs, error) {
	m := &CheckpointIngestIndexRepairSQLs{}
	err := selectCheckpointMeta(ctx, execCtx, LogRestoreCheckpointDatabaseName, checkpointIngestTableName, m)
	return m, errors.Trace(err)
}

func ExistsCheckpointIngestIndexRepairSQLs(ctx context.Context, dom *domain.Domain) bool {
	return dom.InfoSchema().
		TableExists(pmodel.NewCIStr(LogRestoreCheckpointDatabaseName), pmodel.NewCIStr(checkpointIngestTableName))
}

func SaveCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	se glue.Session,
	meta *CheckpointIngestIndexRepairSQLs,
) error {
	return insertCheckpointMeta(ctx, se, LogRestoreCheckpointDatabaseName, checkpointIngestTableName, meta)
}

func RemoveCheckpointDataForLogRestore(ctx context.Context, dom *domain.Domain, se glue.Session) error {
	return dropCheckpointTables(ctx, dom, se, LogRestoreCheckpointDatabaseName,
		[]string{checkpointDataTableName, checkpointMetaTableName, checkpointProgressTableName, checkpointIngestTableName})
}
