// Copyright 2026 PingCAP, Inc.
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

package task

import (
	"context"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/repo"
	taskrepo "github.com/pingcap/tidb/br/pkg/task/repo"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

type snapshotStorageRef = taskrepo.SnapshotStorageRef

type preparedRepoV1SnapshotBackup = taskrepo.PreparedRepoV1SnapshotBackup

type snapshotBackupStorageParams struct {
	onPending        snapshotRepoOnPendingAction
	cfgHash          []byte
	createdBy        string
	allocateBackupID func(context.Context) (repo.BackupID, error)
}

func validateSnapshotBackupRepoConfig(cfg *BackupConfig) error {
	return taskrepo.ValidateSnapshotBackupRepoConfig(cfg.SnapshotRepoBackupOptions.Layout, cfg.UseCheckpoint)
}

func prepareRepoV1SnapshotBackup(
	ctx context.Context,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
	params snapshotBackupStorageParams,
) (*preparedRepoV1SnapshotBackup, error) {
	return taskrepo.PrepareRepoV1SnapshotBackup(ctx, rootBackend, rootStorage, taskrepo.SnapshotBackupStorageParams{
		OnPending:        taskrepo.OnPendingAction(params.onPending),
		ConfigHash:       params.cfgHash,
		CreatedBy:        params.createdBy,
		AllocateBackupID: params.allocateBackupID,
	})
}

func loadSnapshotBackupMeta(
	ctx context.Context,
	cfg *RestoreConfig,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
) (*snapshotStorageRef, *backuppb.BackupMeta, error) {
	return taskrepo.LoadSnapshotBackupMeta(ctx, cfg.Layout, cfg.BackupID, rootBackend, rootStorage, &cfg.Config.CipherInfo)
}

// ResolveSnapshotBackupMeta resolves one snapshot backup reference and reads its backupmeta.
func ResolveSnapshotBackupMeta(
	ctx context.Context,
	storageName string,
	cfg *Config,
	ref repo.SnapshotRef,
) (*backuppb.StorageBackend, storeapi.Storage, *backuppb.BackupMeta, error) {
	return taskrepo.ResolveSnapshotBackupMeta(ctx, taskrepo.Config{
		BackendOptions: cfg.BackendOptions,
		Storage:        storageName,
		CipherInfo:     cfg.CipherInfo,
		NoCreds:        cfg.NoCreds,
		SendCreds:      cfg.SendCreds,
	}, ref)
}

func writePendingSnapshot(ctx context.Context, rootStorage storeapi.Storage, pendingFile string) error {
	return rootStorage.WriteFile(ctx, pendingFile, []byte("{}"))
}

func repoCreatedBy(version string) string {
	return taskrepo.RepoCreatedBy(version)
}
