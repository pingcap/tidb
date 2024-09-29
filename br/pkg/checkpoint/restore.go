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
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/domain"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

type RestoreKeyType = int64
type RestoreValueType struct {
	// the file key of a range
	RangeKey string
}

func (rv RestoreValueType) IdentKey() []byte {
	return []byte(rv.RangeKey)
}

func valueMarshalerForRestore(group *RangeGroup[RestoreKeyType, RestoreValueType]) ([]byte, error) {
	return json.Marshal(group)
}

// only for test
func StartCheckpointRestoreRunnerForTest(
	ctx context.Context,
	se glue.Session,
	tick time.Duration,
) (*CheckpointRunner[RestoreKeyType, RestoreValueType], error) {
	runner := newCheckpointRunner[RestoreKeyType, RestoreValueType](
		newTableCheckpointStorage(se, SnapshotRestoreCheckpointDatabaseName),
		nil, valueMarshalerForRestore)

	runner.startCheckpointMainLoop(ctx, tick, tick, 0)
	return runner, nil
}

func StartCheckpointRunnerForRestore(
	ctx context.Context,
	se glue.Session,
) (*CheckpointRunner[RestoreKeyType, RestoreValueType], error) {
	runner := newCheckpointRunner[RestoreKeyType, RestoreValueType](
		newTableCheckpointStorage(se, SnapshotRestoreCheckpointDatabaseName),
		nil, valueMarshalerForRestore)

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

// load the whole checkpoint range data and retrieve the metadata of restored ranges
// and return the total time cost in the past executions
func LoadCheckpointDataForSnapshotRestore[K KeyType, V ValueType](
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	fn func(K, V),
) (time.Duration, error) {
	return selectCheckpointData(ctx, execCtx, SnapshotRestoreCheckpointDatabaseName, fn)
}

func LoadCheckpointChecksumForRestore(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
) (map[int64]*ChecksumItem, time.Duration, error) {
	return selectCheckpointChecksum(ctx, execCtx, SnapshotRestoreCheckpointDatabaseName)
}

type CheckpointMetadataForSnapshotRestore struct {
	UpstreamClusterID uint64                `json:"upstream-cluster-id"`
	RestoredTS        uint64                `json:"restored-ts"`
	SchedulersConfig  *pdutil.ClusterConfig `json:"schedulers-config"`
}

func LoadCheckpointMetadataForSnapshotRestore(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
) (*CheckpointMetadataForSnapshotRestore, error) {
	m := &CheckpointMetadataForSnapshotRestore{}
	err := selectCheckpointMeta(ctx, execCtx, SnapshotRestoreCheckpointDatabaseName, checkpointMetaTableName, m)
	return m, err
}

func SaveCheckpointMetadataForSnapshotRestore(
	ctx context.Context,
	se glue.Session,
	meta *CheckpointMetadataForSnapshotRestore,
) error {
	err := initCheckpointTable(ctx, se, SnapshotRestoreCheckpointDatabaseName,
		[]string{checkpointDataTableName, checkpointChecksumTableName})
	if err != nil {
		return errors.Trace(err)
	}
	return insertCheckpointMeta(ctx, se, SnapshotRestoreCheckpointDatabaseName, checkpointMetaTableName, meta)
}

func ExistsSnapshotRestoreCheckpoint(
	ctx context.Context,
	dom *domain.Domain,
) bool {
	return dom.InfoSchema().
		TableExists(pmodel.NewCIStr(SnapshotRestoreCheckpointDatabaseName), pmodel.NewCIStr(checkpointMetaTableName))
}

func RemoveCheckpointDataForSnapshotRestore(ctx context.Context, dom *domain.Domain, se glue.Session) error {
	return dropCheckpointTables(ctx, dom, se, SnapshotRestoreCheckpointDatabaseName,
		[]string{checkpointDataTableName, checkpointChecksumTableName, checkpointMetaTableName})
}
