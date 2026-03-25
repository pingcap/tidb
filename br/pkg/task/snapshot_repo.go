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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/repo/snapshotpaths"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

type resolvedSnapshotStorage struct {
	Layout          repo.Layout
	BackupID        repo.BackupID
	RootBackend     *backuppb.StorageBackend
	RootStorage     storeapi.Storage
	MetadataStorage storeapi.Storage
	PendingFile     string
}

type pendingSnapshot struct {
	LayoutVersion  int    `json:"layout_version"`
	BackupID       string `json:"backup_id"`
	MetadataPrefix string `json:"metadata_prefix"`
	CreatedAt      string `json:"created_at"`
}

type snapshotRepoBackupLifecycle struct {
	resolvedStorage      *resolvedSnapshotStorage
	removePendingOnClose bool
	pendingMarkerCreated bool
	backupMetaDurable    bool
	checkpointCleanedUp  bool
}

func newSnapshotRepoBackupLifecycle(resolved *resolvedSnapshotStorage) *snapshotRepoBackupLifecycle {
	if resolved == nil || !resolved.Layout.IsRepoV1() {
		return nil
	}
	return &snapshotRepoBackupLifecycle{resolvedStorage: resolved}
}

func (l *snapshotRepoBackupLifecycle) Attach(client *backup.Client) {
	if l == nil {
		return
	}
	client.SetMetadataStorage(l.resolvedStorage.MetadataStorage)
}

func (l *snapshotRepoBackupLifecycle) Close(ctx context.Context) {
	if l == nil || !l.removePendingOnClose || !l.pendingMarkerCreated || !l.backupMetaDurable || !l.checkpointCleanedUp {
		return
	}
	if err := l.resolvedStorage.RootStorage.DeleteFile(ctx, l.resolvedStorage.PendingFile); err != nil {
		log.Warn("failed to remove repo-v1 pending marker",
			zap.String("path", l.resolvedStorage.PendingFile),
			zap.Error(err))
	}
}

func (l *snapshotRepoBackupLifecycle) StartCheckpoint(
	ctx context.Context,
	client *backup.Client,
	cfgHash []byte,
	backupTS uint64,
	safePointID string,
	progressCallBack func(backup.ProgressUnit),
) error {
	pendingFile, err := l.pendingFileForCheckpoint()
	if err != nil {
		return errors.Trace(err)
	}
	if pendingFile != "" {
		if err := writePendingSnapshot(ctx, l.resolvedStorage.RootStorage, pendingFile, l.resolvedStorage.BackupID); err != nil {
			return errors.Trace(err)
		}
		l.pendingMarkerCreated = true
	}
	if err := client.StartCheckpointRunner(ctx, cfgHash, backupTS, safePointID, progressCallBack); err != nil {
		l.rollbackStartCheckpoint(ctx, client)
		return errors.Trace(err)
	}
	return nil
}

func (l *snapshotRepoBackupLifecycle) pendingFileForCheckpoint() (string, error) {
	if l == nil {
		return "", nil
	}
	if l.resolvedStorage == nil {
		return "", errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 backup lifecycle is missing resolved storage")
	}
	if l.resolvedStorage.PendingFile == "" {
		return "", errors.Annotatef(
			berrors.ErrInvalidArgument,
			"repo-v1 backup lifecycle is missing pending marker path for backup %s",
			l.resolvedStorage.BackupID,
		)
	}
	return l.resolvedStorage.PendingFile, nil
}

func (l *snapshotRepoBackupLifecycle) rollbackStartCheckpoint(ctx context.Context, client *backup.Client) {
	if l == nil {
		return
	}
	removePendingMarker := true
	if removeErr := checkpoint.RemoveCheckpointDataForBackup(ctx, client.GetStorage()); removeErr != nil {
		log.Warn("failed to roll back checkpoint metadata after checkpoint setup failure", zap.Error(removeErr))
		removePendingMarker = false
	}
	if l.pendingMarkerCreated && removePendingMarker {
		if removeErr := l.resolvedStorage.RootStorage.DeleteFile(ctx, l.resolvedStorage.PendingFile); removeErr != nil {
			log.Warn("failed to roll back repo-v1 pending marker after checkpoint setup failure",
				zap.String("path", l.resolvedStorage.PendingFile),
				zap.Error(removeErr))
		}
		l.pendingMarkerCreated = false
	}
}

func (l *snapshotRepoBackupLifecycle) FinishCheckpoint(ctx context.Context, client *backup.Client, flush bool) {
	client.WaitForFinishCheckpoint(ctx, flush)
	if flush {
		return
	}
	if removeErr := checkpoint.RemoveCheckpointDataForBackup(ctx, client.GetStorage()); removeErr != nil {
		log.Warn("failed to remove checkpoint data for backup", zap.Error(removeErr))
		return
	}
	if l != nil {
		l.checkpointCleanedUp = true
	}
	log.Info("the checkpoint data for backup is removed.")
}

func (l *snapshotRepoBackupLifecycle) MarkCheckpointCleanupSkipped() {
	if l == nil {
		return
	}
	l.checkpointCleanedUp = true
}

func (l *snapshotRepoBackupLifecycle) MarkBackupMetaDurable() {
	if l == nil {
		return
	}
	l.backupMetaDurable = true
}

func (l *snapshotRepoBackupLifecycle) MarkSuccess() {
	if l == nil {
		return
	}
	log.Info("completed repo-v1 snapshot backup", zap.String("backup-id", l.resolvedStorage.BackupID.String()))
	if l.pendingMarkerCreated {
		l.removePendingOnClose = true
	}
}

func (l *snapshotRepoBackupLifecycle) RewriteStorageBackend() func(storeID uint64, backend *backuppb.StorageBackend) error {
	if l == nil {
		return nil
	}
	backupID := l.resolvedStorage.BackupID
	return func(storeID uint64, backend *backuppb.StorageBackend) error {
		return errors.Trace(rewriteDataBackendForStore(backend, storeID, backupID))
	}
}

func validateSnapshotBackupRepoConfig(cfg *BackupConfig) error {
	if !cfg.Layout.IsRepoV1() {
		return nil
	}
	if !cfg.UseCheckpoint {
		return errors.Annotatef(
			berrors.ErrInvalidArgument,
			"--%s=%s requires --%s",
			flagStorageLayout, repo.LayoutRepoV1, flagUseCheckpoint,
		)
	}
	return nil
}

func snapshotRegistrationBackupID(layout repo.Layout, backupID repo.BackupID) string {
	if !layout.IsRepoV1() || backupID.IsZero() {
		return ""
	}
	return backupID.String()
}

func prepareSnapshotBackupStorage(
	ctx context.Context,
	cfg *BackupConfig,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
	client *backup.Client,
	cfgHash []byte,
	createdBy string,
) (*resolvedSnapshotStorage, error) {
	resolved := &resolvedSnapshotStorage{
		Layout:          cfg.Layout,
		RootBackend:     rootBackend,
		RootStorage:     rootStorage,
		MetadataStorage: rootStorage,
	}
	if !cfg.Layout.IsRepoV1() {
		return resolved, nil
	}
	if err := validateRepoV1Backend(rootBackend); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.EnsureRepo(ctx, rootStorage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, createdBy); err != nil {
		return nil, errors.Trace(err)
	}

	cfgHashHex := hex.EncodeToString(cfgHash)
	unfinished, err := resolveUnfinishedPendingBackups(ctx, rootStorage, cfgHashHex)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch len(unfinished) {
	case 0:
	case 1:
		return nil, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"found unfinished repo-v1 backup %s for config hash %s; explicit resume/discard is required before starting a new backup",
			unfinished[0], cfgHashHex,
		)
	default:
		ids := make([]string, 0, len(unfinished))
		for _, id := range unfinished {
			ids = append(ids, id.String())
		}
		return nil, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"found multiple unfinished repo-v1 backups for config hash %s: %s",
			cfgHashHex, strings.Join(ids, ", "),
		)
	}

	backupTS, err := client.GetCurrentTS(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "allocate repo-v1 backup id")
	}
	backupID, err := repo.NewBackupID(backupTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resolved.BackupID = backupID
	resolved.MetadataStorage = repo.NewPrefixedStorage(rootStorage, snapshotpaths.MetadataDir(backupID))
	resolved.PendingFile = snapshotpaths.PendingFile(cfgHashHex, backupID)
	log.Info("prepared repo-v1 snapshot backup storage",
		zap.String("backup-id", backupID.String()),
		zap.String("metadata-uri", resolved.MetadataStorage.URI()))
	return resolved, nil
}

func resolveSnapshotRestoreStorage(
	ctx context.Context,
	cfg *RestoreConfig,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
) (*resolvedSnapshotStorage, error) {
	resolved := &resolvedSnapshotStorage{
		Layout:          cfg.Layout,
		BackupID:        cfg.BackupID,
		RootBackend:     rootBackend,
		RootStorage:     rootStorage,
		MetadataStorage: rootStorage,
	}
	if !cfg.Layout.IsRepoV1() {
		return resolved, nil
	}
	if err := validateRepoV1Backend(rootBackend); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.LoadRepoMeta(ctx, rootStorage, snapshotpaths.RepoMetaPath); err != nil {
		return nil, errors.Annotate(err, "load repo-v1 metadata")
	}
	resolved.MetadataStorage = repo.NewPrefixedStorage(rootStorage, snapshotpaths.MetadataDir(cfg.BackupID))
	return resolved, nil
}

func resolveSnapshotBackupMeta(
	ctx context.Context,
	cfg *RestoreConfig,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
) (*resolvedSnapshotStorage, *backuppb.BackupMeta, error) {
	resolved, err := resolveSnapshotRestoreStorage(ctx, cfg, rootBackend, rootStorage)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	backupMeta, err := ReadBackupMetaFromStorage(ctx, metautil.MetaFile, resolved.MetadataStorage, &cfg.Config)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return resolved, backupMeta, nil
}

func resolveUnfinishedPendingBackups(ctx context.Context, rootStorage storeapi.Storage, cfgHashHex string) ([]repo.BackupID, error) {
	ids, err := listPendingBackups(ctx, rootStorage, cfgHashHex)
	if err != nil {
		return nil, errors.Trace(err)
	}
	unfinished := make([]repo.BackupID, 0, len(ids))
	for _, id := range ids {
		metadataStorage := repo.NewPrefixedStorage(rootStorage, snapshotpaths.MetadataDir(id))
		hasBackupMeta, err := metadataStorage.FileExists(ctx, metautil.MetaFile)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if hasBackupMeta {
			if err := rootStorage.DeleteFile(ctx, snapshotpaths.PendingFile(cfgHashHex, id)); err != nil {
				return nil, errors.Annotatef(err, "remove stale pending marker for %s", id)
			}
			continue
		}
		hasCheckpoint, err := metadataStorage.FileExists(ctx, checkpoint.CheckpointMetaPathForBackup)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !hasCheckpoint {
			return nil, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found inconsistent repo-v1 pending backup %s: pending marker exists but neither %s nor %s was found",
				id, metautil.MetaFile, checkpoint.CheckpointMetaPathForBackup,
			)
		}
		unfinished = append(unfinished, id)
	}
	return unfinished, nil
}

func listPendingBackups(ctx context.Context, rootStorage storeapi.Storage, cfgHashHex string) ([]repo.BackupID, error) {
	pendingDir := snapshotpaths.PendingDir(cfgHashHex)
	ids := make([]repo.BackupID, 0)
	err := rootStorage.WalkDir(ctx, &storeapi.WalkOption{SubDir: pendingDir}, func(filePath string, _ int64) error {
		if path.Ext(filePath) != ".json" {
			return nil
		}
		id, err := repo.ParseBackupIDStorageName(strings.TrimSuffix(path.Base(filePath), ".json"))
		if err != nil {
			return errors.Annotatef(err, "parse pending backup marker %s", filePath)
		}
		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, nil
}

func writePendingSnapshot(ctx context.Context, rootStorage storeapi.Storage, pendingFile string, backupID repo.BackupID) error {
	payload, err := json.Marshal(&pendingSnapshot{
		LayoutVersion:  repo.RepoVersion,
		BackupID:       backupID.String(),
		MetadataPrefix: snapshotpaths.MetadataDir(backupID),
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return errors.Trace(err)
	}
	return rootStorage.WriteFile(ctx, pendingFile, payload)
}

func rewriteDataBackendForStore(backend *backuppb.StorageBackend, storeID uint64, backupID repo.BackupID) error {
	if backend == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 request is missing storage backend")
	}
	prefix := snapshotpaths.StoreDataPrefix(storeID, backupID)
	switch {
	case backend.GetLocal() != nil:
		backend.GetLocal().Path = path.Join(backend.GetLocal().Path, prefix)
	case backend.GetS3() != nil:
		backend.GetS3().Prefix = joinBackendPrefix(backend.GetS3().Prefix, prefix)
	case backend.GetGcs() != nil:
		backend.GetGcs().Prefix = joinBackendPrefix(backend.GetGcs().Prefix, prefix)
	case backend.GetAzureBlobStorage() != nil:
		backend.GetAzureBlobStorage().Prefix = joinBackendPrefix(backend.GetAzureBlobStorage().Prefix, prefix)
	default:
		return errors.Errorf("repo-v1 is unsupported for backend %T", backend.Backend)
	}
	return nil
}

func joinBackendPrefix(current, suffix string) string {
	return strings.Trim(path.Join("/", current, suffix), "/")
}

func validateRepoV1Backend(backend *backuppb.StorageBackend) error {
	switch {
	case backend.GetLocal() != nil:
		return nil
	case backend.GetS3() != nil:
		return nil
	case backend.GetGcs() != nil:
		return nil
	case backend.GetAzureBlobStorage() != nil:
		return nil
	case backend.GetNoop() != nil:
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 doesn't support noop storage")
	case backend.GetHdfs() != nil:
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 doesn't support hdfs storage")
	default:
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 doesn't support backend %T", backend.Backend)
	}
}

func repoCreatedBy(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return "br"
	}
	return fmt.Sprintf("br %s", version)
}
