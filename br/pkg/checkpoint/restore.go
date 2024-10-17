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
	// the file name, used for compacted restore
	Name string
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
	retryDuration time.Duration,
) (*CheckpointRunner[RestoreKeyType, RestoreValueType], error) {
	runner := newCheckpointRunner[RestoreKeyType, RestoreValueType](
		newTableCheckpointStorage(se, SnapshotRestoreCheckpointDatabaseName),
		nil, valueMarshalerForRestore)

	runner.startCheckpointMainLoop(ctx, tick, tick, 0, retryDuration)
	return runner, nil
}

// Notice that the session is owned by the checkpoint runner, and it will be also closed by it.
func StartCheckpointRunnerForRestore(
	ctx context.Context,
	se glue.Session,
) (*CheckpointRunner[RestoreKeyType, RestoreValueType], error) {
	runner := newCheckpointRunner[RestoreKeyType, RestoreValueType](
		newTableCheckpointStorage(se, CompactedRestoreCheckpointDatabaseName),
		nil, valueMarshalerForRestore)

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(
		ctx,
		defaultTickDurationForFlush, defaultTickDurationForChecksum, 0, defaultRetryDuration)
	return runner, nil
}

func AppendRangesForRestore(
	ctx context.Context,
	r *CheckpointRunner[RestoreKeyType, RestoreValueType],
	tableID RestoreKeyType,
	rangeKey string,
	name string,
) error {
	return r.Append(ctx, &CheckpointMessage[RestoreKeyType, RestoreValueType]{
		GroupKey: tableID,
		Group: []RestoreValueType{
			{RangeKey: rangeKey, Name: name},
		},
	})
}

// load the whole checkpoint range data and retrieve the metadata of restored ranges
// and return the total time cost in the past executions
func LoadCheckpointDataForSstRestore[K KeyType, V ValueType](
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	dbName string,
	fn func(K, V),
) (time.Duration, error) {
	return selectCheckpointData(ctx, execCtx, dbName, fn)
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

func LoadCheckpointMetadataForSstRestore(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	dbName string,
) (*CheckpointMetadataForSnapshotRestore, error) {
	m := &CheckpointMetadataForSnapshotRestore{}
	err := selectCheckpointMeta(ctx, execCtx, dbName, checkpointMetaTableName, m)
	return m, err
}

func SaveCheckpointMetadataForSstRestore(
	ctx context.Context,
	se glue.Session,
	dbName string,
	meta *CheckpointMetadataForSnapshotRestore,
) error {
	err := initCheckpointTable(ctx, se, dbName,
		[]string{checkpointDataTableName, checkpointChecksumTableName})
	if err != nil {
		return errors.Trace(err)
	}
	return insertCheckpointMeta(ctx, se, dbName, checkpointMetaTableName, meta)
}

func ExistsSstRestoreCheckpoint(
	ctx context.Context,
	dom *domain.Domain,
	dbName string,
) bool {
	return dom.InfoSchema().
		TableExists(pmodel.NewCIStr(dbName), pmodel.NewCIStr(checkpointMetaTableName))
}

func RemoveCheckpointDataForSstRestore(ctx context.Context, dom *domain.Domain, se glue.Session, dbName string) error {
	return dropCheckpointTables(ctx, dom, se, dbName,
		[]string{checkpointDataTableName, checkpointChecksumTableName, checkpointMetaTableName})
}
