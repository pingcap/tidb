// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type SnapshotMetaManagerT = MetaManager[
	RestoreKeyType, RestoreValueType, RestoreValueType, CheckpointMetadataForSnapshotRestore,
]
type LogMetaManagerT = LogMetaManager[
	LogRestoreKeyType, LogRestoreValueType, LogRestoreValueMarshaled, CheckpointMetadataForLogRestore,
]

type tickDurationConfig struct {
	tickDurationForFlush    time.Duration
	tickDurationForChecksum time.Duration
	retryDuration           time.Duration
}

func DefaultTickDurationConfig() tickDurationConfig {
	return tickDurationConfig{
		tickDurationForFlush:    defaultTickDurationForFlush,
		tickDurationForChecksum: defaultTickDurationForChecksum,
		retryDuration:           defaultRetryDuration,
	}
}

type MetaManager[K KeyType, SV, LV ValueType, M any] interface {
	fmt.Stringer

	LoadCheckpointData(context.Context, func(K, LV) error) (time.Duration, error)
	LoadCheckpointChecksum(context.Context) (map[int64]*ChecksumItem, time.Duration, error)
	LoadCheckpointMetadata(context.Context) (*M, error)
	SaveCheckpointMetadata(context.Context, *M) error
	ExistsCheckpointMetadata(context.Context) (bool, error)
	RemoveCheckpointData(context.Context) error

	// start checkpoint runner
	StartCheckpointRunner(
		context.Context, tickDurationConfig, func(*RangeGroup[K, SV]) ([]byte, error),
	) (*CheckpointRunner[K, SV], error)

	// close session
	Close()
}

type LogMetaManager[K KeyType, SV, LV ValueType, M any] interface {
	MetaManager[K, SV, LV, M]

	LoadCheckpointProgress(context.Context) (*CheckpointProgress, error)
	SaveCheckpointProgress(context.Context, *CheckpointProgress) error
	ExistsCheckpointProgress(context.Context) (bool, error)

	LoadCheckpointIngestIndexRepairSQLs(context.Context) (*CheckpointIngestIndexRepairSQLs, error)
	SaveCheckpointIngestIndexRepairSQLs(context.Context, *CheckpointIngestIndexRepairSQLs) error
	ExistsCheckpointIngestIndexRepairSQLs(context.Context) (bool, error)

	TryGetStorage() objstore.ExternalStorage
}

type TableMetaManager[K KeyType, SV, LV ValueType, M any] struct {
	se       glue.Session
	runnerSe glue.Session
	dom      *domain.Domain
	dbName   string
}

func NewLogTableMetaManager(
	g glue.Glue,
	dom *domain.Domain,
	dbName string,
	restoreID uint64,
) (LogMetaManagerT, error) {
	se, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}
	runnerSe, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TableMetaManager[
		LogRestoreKeyType, LogRestoreValueType, LogRestoreValueMarshaled, CheckpointMetadataForLogRestore,
	]{
		se:       se,
		runnerSe: runnerSe,
		dom:      dom,
		dbName:   fmt.Sprintf("%s_%d", dbName, restoreID),
	}, nil
}

func NewSnapshotTableMetaManager(
	g glue.Glue,
	dom *domain.Domain,
	dbName string,
	restoreID uint64,
) (SnapshotMetaManagerT, error) {
	se, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}
	runnerSe, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TableMetaManager[
		RestoreKeyType, RestoreValueType, RestoreValueType, CheckpointMetadataForSnapshotRestore,
	]{
		se:       se,
		runnerSe: runnerSe,
		dom:      dom,
		dbName:   fmt.Sprintf("%s_%d", dbName, restoreID),
	}, nil
}

func (manager *TableMetaManager[K, SV, LV, M]) String() string {
	return fmt.Sprintf("databases[such as %s]", manager.dbName)
}

func (manager *TableMetaManager[K, SV, LV, M]) Close() {
	if manager.se != nil {
		manager.se.Close()
	}
	if manager.runnerSe != nil {
		manager.runnerSe.Close()
	}
}

// LoadCheckpointData loads the whole checkpoint range data and retrieve the metadata of restored ranges
// and return the total time cost in the past executions
func (manager *TableMetaManager[K, SV, LV, M]) LoadCheckpointData(
	ctx context.Context,
	fn func(K, LV) error,
) (time.Duration, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	return selectCheckpointData(ctx, execCtx, manager.dbName, fn)
}

func (manager *TableMetaManager[K, SV, LV, M]) LoadCheckpointChecksum(
	ctx context.Context,
) (map[int64]*ChecksumItem, time.Duration, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	return selectCheckpointChecksum(ctx, execCtx, manager.dbName)
}

func (manager *TableMetaManager[K, SV, LV, M]) LoadCheckpointMetadata(
	ctx context.Context,
) (*M, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	m := new(M)
	err := selectCheckpointMeta(ctx, execCtx, manager.dbName, checkpointMetaTableName, m)
	return m, err
}

func (manager *TableMetaManager[K, SV, LV, M]) SaveCheckpointMetadata(
	ctx context.Context,
	meta *M,
) error {
	err := initCheckpointTable(ctx, manager.se, manager.dbName,
		[]string{checkpointDataTableName, checkpointChecksumTableName})
	if err != nil {
		return errors.Trace(err)
	}
	if meta != nil {
		return insertCheckpointMeta(ctx, manager.se, manager.dbName, checkpointMetaTableName, meta)
	}
	return nil
}

func (manager *TableMetaManager[K, SV, LV, M]) ExistsCheckpointMetadata(
	ctx context.Context,
) (bool, error) {
	// we only check the existence of the checkpoint data table
	// because the checkpoint metadata is not used for restore
	return manager.dom.InfoSchema().
		TableExists(ast.NewCIStr(manager.dbName), ast.NewCIStr(checkpointMetaTableName)), nil
}

func (manager *TableMetaManager[K, SV, LV, M]) RemoveCheckpointData(
	ctx context.Context,
) error {
	return dropCheckpointTables(ctx, manager.dom, manager.se, manager.dbName, []string{
		checkpointDataTableName,
		checkpointChecksumTableName,
		checkpointMetaTableName,
		checkpointProgressTableName,
		checkpointIngestTableName,
	})
}

func (manager *TableMetaManager[K, SV, LV, M]) LoadCheckpointProgress(
	ctx context.Context,
) (*CheckpointProgress, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	m := &CheckpointProgress{}
	err := selectCheckpointMeta(ctx, execCtx, manager.dbName, checkpointProgressTableName, m)
	return m, errors.Trace(err)
}

func (manager *TableMetaManager[K, SV, LV, M]) SaveCheckpointProgress(
	ctx context.Context,
	meta *CheckpointProgress,
) error {
	return insertCheckpointMeta(ctx, manager.se, manager.dbName, checkpointProgressTableName, meta)
}

func (manager *TableMetaManager[K, SV, LV, M]) ExistsCheckpointProgress(
	ctx context.Context,
) (bool, error) {
	return manager.dom.InfoSchema().
		TableExists(ast.NewCIStr(manager.dbName), ast.NewCIStr(checkpointProgressTableName)), nil
}

func (manager *TableMetaManager[K, SV, LV, M]) LoadCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
) (*CheckpointIngestIndexRepairSQLs, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	m := &CheckpointIngestIndexRepairSQLs{}
	err := selectCheckpointMeta(ctx, execCtx, manager.dbName, checkpointIngestTableName, m)
	return m, errors.Trace(err)
}

func (manager *TableMetaManager[K, SV, LV, M]) SaveCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	meta *CheckpointIngestIndexRepairSQLs,
) error {
	return insertCheckpointMeta(ctx, manager.se, manager.dbName, checkpointIngestTableName, meta)
}

func (manager *TableMetaManager[K, SV, LV, M]) ExistsCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
) (bool, error) {
	return manager.dom.InfoSchema().
		TableExists(ast.NewCIStr(manager.dbName), ast.NewCIStr(checkpointIngestTableName)), nil
}

func (manager *TableMetaManager[K, SV, LV, M]) StartCheckpointRunner(
	ctx context.Context,
	cfg tickDurationConfig,
	valueMarshaler func(*RangeGroup[K, SV]) ([]byte, error),
) (*CheckpointRunner[K, SV], error) {
	runner := newCheckpointRunner(
		newTableCheckpointStorage(manager.runnerSe, manager.dbName),
		nil, valueMarshaler)
	manager.runnerSe = nil
	// for restore, no need to set lock
	runner.startCheckpointMainLoop(
		ctx,
		cfg.tickDurationForFlush, cfg.tickDurationForChecksum, 0, cfg.retryDuration)
	return runner, nil
}

func (manager *TableMetaManager[K, SV, LV, M]) TryGetStorage() objstore.ExternalStorage {
	return nil
}

type StorageMetaManager[K KeyType, SV, LV ValueType, M any] struct {
	storage objstore.ExternalStorage
	cipher  *backuppb.CipherInfo
	clusterID string
	taskName  string
}

func NewSnapshotStorageMetaManager(
	storage objstore.ExternalStorage,
	cipher *backuppb.CipherInfo,
	clusterID uint64,
	prefix string,
	restoreID uint64,
) SnapshotMetaManagerT {
	return &StorageMetaManager[
		RestoreKeyType, RestoreValueType, RestoreValueType, CheckpointMetadataForSnapshotRestore,
	]{
		storage:   storage,
		cipher:    cipher,
		clusterID: fmt.Sprintf("%d", clusterID),
		taskName:  fmt.Sprintf("%d/%s_%d", clusterID, prefix, restoreID),
	}
}

func NewLogStorageMetaManager(
	storage objstore.ExternalStorage,
	cipher *backuppb.CipherInfo,
	clusterID uint64,
	prefix string,
	restoreID uint64,
) LogMetaManagerT {
	return &StorageMetaManager[
		LogRestoreKeyType, LogRestoreValueType, LogRestoreValueMarshaled, CheckpointMetadataForLogRestore,
	]{
		storage:   storage,
		cipher:    cipher,
		clusterID: fmt.Sprintf("%d", clusterID),
		taskName:  fmt.Sprintf("%d/%s_%d", clusterID, prefix, restoreID),
	}
}

func (manager *StorageMetaManager[K, SV, LV, M]) String() string {
	return fmt.Sprintf("path[%s]", fmt.Sprintf(CheckpointRestoreDirFormat, manager.clusterID))
}

func (manager *StorageMetaManager[K, SV, LV, M]) Close() {}

func (manager *StorageMetaManager[K, SV, LV, M]) LoadCheckpointData(
	ctx context.Context,
	fn func(K, LV) error,
) (time.Duration, error) {
	return walkCheckpointFile(ctx, manager.storage, manager.cipher, getCheckpointDataDirByName(manager.taskName), fn)
}

func (manager *StorageMetaManager[K, SV, LV, M]) LoadCheckpointChecksum(
	ctx context.Context,
) (map[int64]*ChecksumItem, time.Duration, error) {
	return loadCheckpointChecksum(ctx, manager.storage, getCheckpointChecksumDirByName(manager.taskName))
}

func (manager *StorageMetaManager[K, SV, LV, M]) LoadCheckpointMetadata(
	ctx context.Context,
) (*M, error) {
	m := new(M)
	err := loadCheckpointMeta(ctx, manager.storage, getCheckpointMetaPathByName(manager.taskName), m)
	return m, errors.Trace(err)
}

func (manager *StorageMetaManager[K, SV, LV, M]) SaveCheckpointMetadata(
	ctx context.Context,
	meta *M,
) error {
	return saveCheckpointMetadata(ctx, manager.storage, meta, getCheckpointMetaPathByName(manager.taskName))
}

func (manager *StorageMetaManager[K, SV, LV, M]) ExistsCheckpointMetadata(
	ctx context.Context,
) (bool, error) {
	return manager.storage.FileExists(ctx, getCheckpointMetaPathByName(manager.taskName))
}

func (manager *StorageMetaManager[K, SV, LV, M]) RemoveCheckpointData(
	ctx context.Context,
) error {
	prefix := fmt.Sprintf(CheckpointRestoreDirFormat, manager.taskName)
	return removeCheckpointData(ctx, manager.storage, prefix)
}

func (manager *StorageMetaManager[K, SV, LV, M]) LoadCheckpointProgress(
	ctx context.Context,
) (*CheckpointProgress, error) {
	m := &CheckpointProgress{}
	err := loadCheckpointMeta(ctx, manager.storage, getCheckpointProgressPathByName(manager.taskName), m)
	return m, errors.Trace(err)
}

func (manager *StorageMetaManager[K, SV, LV, M]) SaveCheckpointProgress(
	ctx context.Context,
	meta *CheckpointProgress,
) error {
	return saveCheckpointMetadata(ctx, manager.storage, meta, getCheckpointProgressPathByName(manager.taskName))
}

func (manager *StorageMetaManager[K, SV, LV, M]) ExistsCheckpointProgress(
	ctx context.Context,
) (bool, error) {
	return manager.storage.FileExists(ctx, getCheckpointProgressPathByName(manager.taskName))
}

func (manager *StorageMetaManager[K, SV, LV, M]) LoadCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
) (*CheckpointIngestIndexRepairSQLs, error) {
	m := &CheckpointIngestIndexRepairSQLs{}
	err := loadCheckpointMeta(ctx, manager.storage, getCheckpointIngestIndexPathByName(manager.taskName), m)
	return m, errors.Trace(err)
}

func (manager *StorageMetaManager[K, SV, LV, M]) SaveCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	meta *CheckpointIngestIndexRepairSQLs,
) error {
	return saveCheckpointMetadata(ctx, manager.storage, meta, getCheckpointIngestIndexPathByName(manager.taskName))
}

func (manager *StorageMetaManager[K, SV, LV, M]) ExistsCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
) (bool, error) {
	return manager.storage.FileExists(ctx, getCheckpointIngestIndexPathByName(manager.taskName))
}

func (manager *StorageMetaManager[K, SV, LV, M]) StartCheckpointRunner(
	ctx context.Context,
	cfg tickDurationConfig,
	valueMarshaler func(*RangeGroup[K, SV]) ([]byte, error),
) (*CheckpointRunner[K, SV], error) {
	checkpointStorage, err := newExternalCheckpointStorage(
		ctx, manager.storage, nil, flushPathForRestore(manager.taskName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	runner := newCheckpointRunner(checkpointStorage, manager.cipher, valueMarshaler)

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(
		ctx,
		cfg.tickDurationForFlush, cfg.tickDurationForChecksum, 0, cfg.retryDuration)
	return runner, nil
}

func (manager *StorageMetaManager[K, SV, LV, M]) TryGetStorage() objstore.ExternalStorage {
	return manager.storage
}
