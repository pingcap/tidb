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
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

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
}

type preparedSnapshotBackupStorage struct {
	resolvedSnapshotStorage
	pendingFile      string
	resumeCheckpoint bool
}

type snapshotBackupStorageParams struct {
	onPending        snapshotRepoOnPendingAction
	cfgHash          []byte
	createdBy        string
	allocateBackupID func(context.Context) (repo.BackupID, error)
}

type snapshotRepoBackupLifecycle struct {
	resolvedStorage      *resolvedSnapshotStorage
	pendingFile          string
	removePendingOnClose bool
	pendingMarkerCreated bool
	backupMetaDurable    bool
	checkpointCleanedUp  bool
}

func newSnapshotRepoBackupLifecycle(prepared *preparedSnapshotBackupStorage) *snapshotRepoBackupLifecycle {
	if prepared == nil || !prepared.Layout.IsRepoV1() {
		return nil
	}
	return &snapshotRepoBackupLifecycle{
		resolvedStorage: &prepared.resolvedSnapshotStorage,
		pendingFile:     prepared.pendingFile,
	}
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
	if err := l.resolvedStorage.RootStorage.DeleteFile(ctx, l.pendingFile); err != nil {
		log.Warn("failed to remove repo-v1 pending marker",
			zap.String("path", l.pendingFile),
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
		if err := writePendingSnapshot(ctx, l.resolvedStorage.RootStorage, pendingFile); err != nil {
			return errors.Trace(err)
		}
		l.pendingMarkerCreated = true
	}
	if err := client.StartCheckpointRunner(ctx, cfgHash, backupTS, l.repoCheckpointBackupID(), safePointID, progressCallBack); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (l *snapshotRepoBackupLifecycle) repoCheckpointBackupID() uint64 {
	if l == nil || l.resolvedStorage == nil {
		return 0
	}
	return uint64(l.resolvedStorage.BackupID)
}

func (l *snapshotRepoBackupLifecycle) pendingFileForCheckpoint() (string, error) {
	if l == nil {
		return "", nil
	}
	if l.resolvedStorage == nil {
		return "", errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 backup lifecycle is missing resolved storage")
	}
	if l.pendingFile == "" {
		return "", errors.Annotatef(
			berrors.ErrInvalidArgument,
			"repo-v1 backup lifecycle is missing pending marker path for backup %s",
			l.resolvedStorage.BackupID,
		)
	}
	return l.pendingFile, nil
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

func (l *snapshotRepoBackupLifecycle) RewriteResponseFiles() func(storeID uint64, files []*backuppb.File) ([]*backuppb.File, error) {
	if l == nil {
		return nil
	}
	backupID := l.resolvedStorage.BackupID
	return func(storeID uint64, files []*backuppb.File) ([]*backuppb.File, error) {
		return rewriteDataFilesForStore(files, storeID, backupID)
	}
}

func (l *snapshotRepoBackupLifecycle) BeforeFirstRequestToStore() func(storeID uint64, request backuppb.BackupRequest) error {
	if l == nil {
		return nil
	}
	return func(storeID uint64, request backuppb.BackupRequest) error {
		backend := request.GetStorageBackend()
		if backend == nil || backend.GetLocal() == nil {
			return nil
		}
		if backend.GetLocal().Path == "" {
			return errors.Annotatef(
				berrors.ErrInvalidArgument,
				"repo-v1 local backend for store %d is missing target path",
				storeID,
			)
		}
		if err := os.MkdirAll(backend.GetLocal().Path, 0o750); err != nil {
			return errors.Annotatef(err, "prepare repo-v1 local backend path for store %d", storeID)
		}
		return nil
	}
}

func validateSnapshotBackupRepoConfig(cfg *BackupConfig) error {
	if !cfg.SnapshotRepoBackupOptions.Layout.IsRepoV1() {
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
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
	params snapshotBackupStorageParams,
) (*preparedSnapshotBackupStorage, error) {
	if err := validateRepoV1Backend(rootBackend); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.EnsureRepo(ctx, rootStorage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, params.createdBy); err != nil {
		return nil, errors.Trace(err)
	}

	cfgHashStorageName := snapshotpaths.PendingConfigHashStorageName(params.cfgHash)
	unfinished, err := resolveUnfinishedPendingBackups(ctx, rootStorage, params.cfgHash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupID, resume, err := resolveRepoV1BackupID(params.onPending, unfinished, cfgHashStorageName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !resume {
		backupID, err = params.allocateBackupID(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	resolved := &preparedSnapshotBackupStorage{
		resolvedSnapshotStorage: resolvedSnapshotStorage{
			Layout:          repo.LayoutRepoV1,
			BackupID:        backupID,
			RootBackend:     rootBackend,
			RootStorage:     rootStorage,
			MetadataStorage: repo.NewPrefixedStorage(rootStorage, snapshotpaths.MetadataDir(backupID)),
		},
		pendingFile:      snapshotpaths.PendingFile(params.cfgHash, backupID),
		resumeCheckpoint: resume,
	}
	log.Info("created repo-v1 snapshot backup storage",
		zap.String("backup-id", backupID.String()),
		zap.String("metadata-uri", resolved.MetadataStorage.URI()),
		zap.Bool("resumed?", resume))
	return resolved, nil
}

func resolveRepoV1BackupID(
	onPending snapshotRepoOnPendingAction,
	unfinished []repo.BackupID,
	cfgHashStorageName string,
) (repo.BackupID, bool, error) {
	switch onPending {
	case "", snapshotRepoOnPendingError:
		if len(unfinished) == 0 {
			return 0, false, nil
		}
		if len(unfinished) == 1 {
			return 0, false, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found unfinished repo-v1 backup %s for config hash %s; use --%s=%s to resume it or --%s=%s to start a fresh attempt",
				unfinished[0], cfgHashStorageName, flagOnPending, snapshotRepoOnPendingResume, flagOnPending, snapshotRepoOnPendingNew,
			)
		}
		return 0, false, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"found multiple unfinished repo-v1 backups for config hash %s: %s",
			cfgHashStorageName, formatRepoV1BackupIDs(unfinished),
		)
	case snapshotRepoOnPendingResume:
		switch len(unfinished) {
		case 0:
			return 0, false, nil
		case 1:
			return unfinished[0], true, nil
		default:
			return 0, false, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found multiple unfinished repo-v1 backups for config hash %s: %s; cannot resume an ambiguous backup",
				cfgHashStorageName, formatRepoV1BackupIDs(unfinished),
			)
		}
	case snapshotRepoOnPendingNew:
		return 0, false, nil
	default:
		return 0, false, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"unknown repo-v1 on-pending action %q",
			onPending,
		)
	}
}

func formatRepoV1BackupIDs(ids []repo.BackupID) string {
	if len(ids) == 0 {
		return ""
	}
	items := make([]string, 0, len(ids))
	for _, id := range ids {
		items = append(items, id.String())
	}
	return strings.Join(items, ", ")
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

func resolveUnfinishedPendingBackups(ctx context.Context, rootStorage storeapi.Storage, cfgHash []byte) ([]repo.BackupID, error) {
	entries, err := listPendingBackups(ctx, rootStorage, cfgHash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	unfinished := make([]repo.BackupID, 0, len(entries))
	for _, entry := range entries {
		metadataStorage := repo.NewPrefixedStorage(rootStorage, snapshotpaths.MetadataDir(entry.backupID))
		hasBackupMeta, err := metadataStorage.FileExists(ctx, metautil.MetaFile)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if hasBackupMeta {
			if err := rootStorage.DeleteFile(ctx, entry.path); err != nil {
				return nil, errors.Annotatef(err, "remove stale pending marker for %s", entry.backupID)
			}
			continue
		}
		hasCheckpoint, err := metadataStorage.FileExists(ctx, checkpoint.CheckpointMetaPathForBackup)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !hasCheckpoint {
			if removeErr := checkpoint.RemoveCheckpointDataForBackup(ctx, metadataStorage); removeErr != nil {
				return nil, errors.Annotatef(removeErr, "remove transient checkpoint state for %s", entry.backupID)
			}
			if removeErr := rootStorage.DeleteFile(ctx, entry.path); removeErr != nil {
				return nil, errors.Annotatef(removeErr, "remove transient pending marker for %s", entry.backupID)
			}
			continue
		}
		unfinished = append(unfinished, entry.backupID)
	}
	return unfinished, nil
}

type pendingBackupEntry struct {
	backupID repo.BackupID
	path     string
}

func listPendingBackups(ctx context.Context, rootStorage storeapi.Storage, cfgHash []byte) ([]pendingBackupEntry, error) {
	entries := make([]pendingBackupEntry, 0)
	err := rootStorage.WalkDir(ctx, &storeapi.WalkOption{SubDir: snapshotpaths.PendingDir(cfgHash)}, func(filePath string, _ int64) error {
		if path.Ext(filePath) != ".json" {
			return nil
		}
		id, err := repo.ParseBackupIDStorageName(strings.TrimSuffix(path.Base(filePath), ".json"))
		if err != nil {
			return errors.Annotatef(err, "parse pending backup marker %s", filePath)
		}
		entries = append(entries, pendingBackupEntry{backupID: id, path: filePath})
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].backupID < entries[j].backupID
	})
	return entries, nil
}

func writePendingSnapshot(ctx context.Context, rootStorage storeapi.Storage, pendingFile string) error {
	return rootStorage.WriteFile(ctx, pendingFile, []byte("{}"))
}

func rewriteDataBackendForStore(backend *backuppb.StorageBackend, storeID uint64, backupID repo.BackupID) error {
	if backend == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 request is missing storage backend")
	}
	prefix := snapshotpaths.StoreDataPrefix(storeID, backupID)
	switch {
	case backend.GetLocal() != nil:
		backend.GetLocal().Path = joinObjectStorageKey(backend.GetLocal().Path, prefix)
	case backend.GetS3() != nil:
		backend.GetS3().Prefix = joinObjectStorageKey(backend.GetS3().Prefix, prefix)
	case backend.GetGcs() != nil:
		backend.GetGcs().Prefix = joinObjectStorageKey(backend.GetGcs().Prefix, prefix)
	case backend.GetAzureBlobStorage() != nil:
		backend.GetAzureBlobStorage().Prefix = joinObjectStorageKey(backend.GetAzureBlobStorage().Prefix, prefix)
	default:
		return errors.Errorf("repo-v1 is unsupported for backend %T", backend.Backend)
	}
	return nil
}

func rewriteDataFilesForStore(files []*backuppb.File, storeID uint64, backupID repo.BackupID) ([]*backuppb.File, error) {
	prefix := snapshotpaths.StoreDataPrefix(storeID, backupID)
	rewritten := make([]*backuppb.File, 0, len(files))
	for _, file := range files {
		if file == nil {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 backup response contains nil file")
		}
		fileCopy := *file
		fileCopy.Name = joinObjectStorageKey(prefix, file.GetName())
		rewritten = append(rewritten, &fileCopy)
	}
	return rewritten, nil
}

func joinObjectStorageKey(base, suffix string) string {
	switch {
	case base == "":
		return suffix
	case suffix == "":
		return base
	case strings.HasSuffix(base, "/") && strings.HasPrefix(suffix, "/"):
		return base + strings.TrimLeft(suffix, "/")
	case strings.HasSuffix(base, "/") || strings.HasPrefix(suffix, "/"):
		return base + suffix
	default:
		return base + "/" + suffix
	}
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
