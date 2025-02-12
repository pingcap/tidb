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

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type MetaManager[K KeyType, V ValueType, M any] interface {
	LoadCheckpointData(context.Context, func(K, V)) (time.Duration, error)
	LoadCheckpointChecksum(context.Context) (map[int64]*ChecksumItem, time.Duration, error)
	LoadCheckpointMetadata(context.Context) (*M, error)
	SaveCheckpointMetadata(context.Context, *M) error
	ExistsCheckpointMetadata(context.Context) (bool, error)
	RemoveCheckpointDataForSstRestore(context.Context) error

	// only for log restore
	LoadCheckpointProgress(context.Context) (*CheckpointProgress, error)
	SaveCheckpointProgress(context.Context, *CheckpointProgress) error
	ExistsCheckpointProgress(context.Context) (bool, error)

	LoadCheckpointIngestIndexRepairSQLs(context.Context) (*CheckpointIngestIndexRepairSQLs, error)
	SaveCheckpointIngestIndexRepairSQLs(context.Context, *CheckpointIngestIndexRepairSQLs) error
	ExistsCheckpointIngestIndexRepairSQLs(context.Context) (bool, error)

	// start checkpoint runner
	StartCheckpointRunner(context.Context, func(*RangeGroup[K, V]) ([]byte, error)) (*CheckpointRunner[K, V], error)
}

type TableCheckpointMetaManager[K KeyType, V ValueType, M any] struct {
	se     glue.Session
	dom    *domain.Domain
	dbName string
}

// load the whole checkpoint range data and retrieve the metadata of restored ranges
// and return the total time cost in the past executions
func (manager *TableCheckpointMetaManager[K, V, M]) LoadCheckpointData(
	ctx context.Context,
	fn func(K, V),
) (time.Duration, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	return selectCheckpointData(ctx, execCtx, manager.dbName, fn)
}

func (manager *TableCheckpointMetaManager[K, V, M]) LoadCheckpointChecksum(
	ctx context.Context,
) (map[int64]*ChecksumItem, time.Duration, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	return selectCheckpointChecksum(ctx, execCtx, manager.dbName)
}

func (manager *TableCheckpointMetaManager[K, V, M]) LoadCheckpointMetadata(
	ctx context.Context,
) (*M, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	m := new(M)
	err := selectCheckpointMeta(ctx, execCtx, manager.dbName, checkpointMetaTableName, m)
	return m, err
}

func (manager *TableCheckpointMetaManager[K, V, M]) SaveCheckpointMetadata(
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

func (manager *TableCheckpointMetaManager[K, V, M]) ExistsCheckpointMetadata(
	ctx context.Context,
) (bool, error) {
	// we only check the existence of the checkpoint data table
	// because the checkpoint metadata is not used for restore
	return manager.dom.InfoSchema().
		TableExists(ast.NewCIStr(manager.dbName), ast.NewCIStr(checkpointMetaTableName)), nil
}

func (manager *TableCheckpointMetaManager[K, V, M]) RemoveCheckpointDataForSstRestore(
	ctx context.Context,
) error {
	return dropCheckpointTables(ctx, manager.dom, manager.se, manager.dbName,
		[]string{checkpointDataTableName, checkpointChecksumTableName, checkpointMetaTableName, checkpointProgressTableName, checkpointIngestTableName})
}

func (manager *TableCheckpointMetaManager[K, V, M]) LoadCheckpointProgress(
	ctx context.Context,
) (*CheckpointProgress, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	m := &CheckpointProgress{}
	err := selectCheckpointMeta(ctx, execCtx, manager.dbName, checkpointProgressTableName, m)
	return m, errors.Trace(err)
}

func (manager *TableCheckpointMetaManager[K, V, M]) SaveCheckpointProgress(
	ctx context.Context,
	meta *CheckpointProgress,
) error {
	return insertCheckpointMeta(ctx, manager.se, manager.dbName, checkpointProgressTableName, meta)
}

func (manager *TableCheckpointMetaManager[K, V, M]) ExistsCheckpointProgress(
	ctx context.Context,
) (bool, error) {
	return manager.dom.InfoSchema().
		TableExists(ast.NewCIStr(manager.dbName), ast.NewCIStr(checkpointProgressTableName)), nil
}

func (manager *TableCheckpointMetaManager[K, V, M]) LoadCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
) (*CheckpointIngestIndexRepairSQLs, error) {
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	m := &CheckpointIngestIndexRepairSQLs{}
	err := selectCheckpointMeta(ctx, execCtx, manager.dbName, checkpointIngestTableName, m)
	return m, errors.Trace(err)
}

func (manager *TableCheckpointMetaManager[K, V, M]) SaveCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	meta *CheckpointIngestIndexRepairSQLs,
) error {
	return insertCheckpointMeta(ctx, manager.se, manager.dbName, checkpointIngestTableName, meta)
}

func (manager *TableCheckpointMetaManager[K, V, M]) ExistsCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
) (bool, error) {
	return manager.dom.InfoSchema().
		TableExists(ast.NewCIStr(manager.dbName), ast.NewCIStr(checkpointIngestTableName)), nil
}

func (manager *TableCheckpointMetaManager[K, V, M]) StartCheckpointRunner(
	ctx context.Context,
	valueMarshaler func(*RangeGroup[K, V]) ([]byte, error),
) (*CheckpointRunner[K, V], error) {
	runner := newCheckpointRunner(
		newTableCheckpointStorage(manager.se, manager.dbName),
		nil, valueMarshaler)

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(
		ctx,
		defaultTickDurationForFlush, defaultTickDurationForChecksum, 0, defaultRetryDuration)
	return runner, nil
}

type StorageCheckpointMetaManager[K KeyType, V ValueType, M any] struct {
	storage  storage.ExternalStorage
	cipher   *backuppb.CipherInfo
	taskName string
}

func (manager *StorageCheckpointMetaManager[K, V, M]) LoadCheckpointData(
	ctx context.Context,
	fn func(K, V),
) (time.Duration, error) {
	return walkCheckpointFile(ctx, manager.storage, manager.cipher, getCheckpointDataDirByName(manager.taskName), fn)
}

func (manager *StorageCheckpointMetaManager[K, V, M]) LoadCheckpointChecksum(
	ctx context.Context,
) (map[int64]*ChecksumItem, time.Duration, error) {
	return loadCheckpointChecksum(ctx, manager.storage, getCheckpointChecksumDirByName(manager.taskName))
}

func (manager *StorageCheckpointMetaManager[K, V, M]) LoadCheckpointMetadata(
	ctx context.Context,
) (*M, error) {
	m := new(M)
	err := loadCheckpointMeta(ctx, manager.storage, getCheckpointMetaPathByName(manager.taskName), m)
	return m, errors.Trace(err)
}

func (manager *StorageCheckpointMetaManager[K, V, M]) SaveCheckpointMetadata(
	ctx context.Context,
	meta *M,
) error {
	return saveCheckpointMetadata(ctx, manager.storage, meta, getCheckpointMetaPathByName(manager.taskName))
}

func (manager *StorageCheckpointMetaManager[K, V, M]) ExistsCheckpointMetadata(
	ctx context.Context,
) (bool, error) {
	return manager.storage.FileExists(ctx, getCheckpointMetaPathByName(manager.taskName))
}

func (manager *StorageCheckpointMetaManager[K, V, M]) RemoveCheckpointDataForSstRestore(
	ctx context.Context,
) error {
	prefix := fmt.Sprintf(CheckpointRestoreDirFormat, manager.taskName)
	return removeCheckpointData(ctx, manager.storage, prefix)
}

func (manager *StorageCheckpointMetaManager[K, V, M]) LoadCheckpointProgress(
	ctx context.Context,
) (*CheckpointProgress, error) {
	m := &CheckpointProgress{}
	err := loadCheckpointMeta(ctx, manager.storage, getCheckpointProgressPathByName(manager.taskName), m)
	return m, errors.Trace(err)
}

func (manager *StorageCheckpointMetaManager[K, V, M]) SaveCheckpointProgress(
	ctx context.Context,
	meta *CheckpointProgress,
) error {
	return saveCheckpointMetadata(ctx, manager.storage, meta, getCheckpointProgressPathByName(manager.taskName))
}

func (manager *StorageCheckpointMetaManager[K, V, M]) ExistsCheckpointProgress(
	ctx context.Context,
) (bool, error) {
	return manager.storage.FileExists(ctx, getCheckpointProgressPathByName(manager.taskName))
}

func (manager *StorageCheckpointMetaManager[K, V, M]) LoadCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
) (*CheckpointIngestIndexRepairSQLs, error) {
	m := &CheckpointIngestIndexRepairSQLs{}
	err := loadCheckpointMeta(ctx, manager.storage, getCheckpointIngestIndexPathByName(manager.taskName), m)
	return m, errors.Trace(err)
}

func (manager *StorageCheckpointMetaManager[K, V, M]) SaveCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
	meta *CheckpointIngestIndexRepairSQLs,
) error {
	return saveCheckpointMetadata(ctx, manager.storage, meta, getCheckpointIngestIndexPathByName(manager.taskName))
}

func (manager *StorageCheckpointMetaManager[K, V, M]) ExistsCheckpointIngestIndexRepairSQLs(
	ctx context.Context,
) (bool, error) {
	return manager.storage.FileExists(ctx, getCheckpointIngestIndexPathByName(manager.taskName))
}

func (manager *StorageCheckpointMetaManager[K, V, M]) StartCheckpointRunner(
	ctx context.Context,
	valueMarshaler func(*RangeGroup[K, V]) ([]byte, error),
) (*CheckpointRunner[K, V], error) {
	checkpointStorage, err := newExternalCheckpointStorage(ctx, manager.storage, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	runner := newCheckpointRunner(checkpointStorage, manager.cipher, valueMarshaler)

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(
		ctx,
		defaultTickDurationForFlush, defaultTickDurationForChecksum, 0, defaultRetryDuration)
	return runner, nil
}
