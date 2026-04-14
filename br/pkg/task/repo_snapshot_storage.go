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
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/repo/snapshotpaths"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

type snapshotStorageRef struct {
	Layout          repo.Layout
	BackupID        repo.BackupID
	RootBackend     *backuppb.StorageBackend
	RootStorage     storeapi.Storage
	MetadataStorage storeapi.Storage
}

type preparedRepoV1SnapshotBackup struct {
	snapshotStorageRef
	pendingMarkerPath    string
	resumeFromCheckpoint bool
}

type snapshotBackupStorageParams struct {
	onPending        snapshotRepoOnPendingAction
	cfgHash          []byte
	createdBy        string
	allocateBackupID func(context.Context) (repo.BackupID, error)
}

func prepareRepoV1LocalBackendForStore(storeID uint64, request backuppb.BackupRequest) error {
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

type repoV1BackupAttempt struct {
	backupID             repo.BackupID
	resumeFromCheckpoint bool
}

func prepareRepoV1SnapshotBackup(
	ctx context.Context,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
	params snapshotBackupStorageParams,
) (*preparedRepoV1SnapshotBackup, error) {
	if err := validateRepoV1Backend(rootBackend); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.EnsureRepo(ctx, rootStorage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, params.createdBy); err != nil {
		return nil, errors.Trace(err)
	}

	cfgHashDirName := snapshotpaths.PendingConfigHashStorageName(params.cfgHash)
	resumableBackups, err := collectResumablePendingBackups(ctx, rootStorage, params.cfgHash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	attempt, err := chooseRepoV1BackupAttempt(params.onPending, resumableBackups, cfgHashDirName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if attempt.backupID.IsZero() {
		attempt.backupID, err = params.allocateBackupID(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	prepared := &preparedRepoV1SnapshotBackup{
		snapshotStorageRef: snapshotStorageRef{
			Layout:          repo.LayoutRepoV1,
			BackupID:        attempt.backupID,
			RootBackend:     rootBackend,
			RootStorage:     rootStorage,
			MetadataStorage: snapshotMetadataStorage(rootStorage, attempt.backupID),
		},
		pendingMarkerPath:    snapshotpaths.PendingFile(params.cfgHash, attempt.backupID),
		resumeFromCheckpoint: attempt.resumeFromCheckpoint,
	}
	log.Info("created repo-v1 snapshot backup storage",
		zap.String("backup-id", attempt.backupID.String()),
		zap.String("metadata-uri", prepared.MetadataStorage.URI()),
		zap.Bool("resumed?", attempt.resumeFromCheckpoint))
	return prepared, nil
}

func chooseRepoV1BackupAttempt(
	onPending snapshotRepoOnPendingAction,
	resumableBackups []repo.BackupID,
	cfgHashDirName string,
) (repoV1BackupAttempt, error) {
	switch onPending {
	case "", snapshotRepoOnPendingError:
		if len(resumableBackups) == 0 {
			return repoV1BackupAttempt{}, nil
		}
		if len(resumableBackups) == 1 {
			return repoV1BackupAttempt{}, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found unfinished repo-v1 backup %s for config hash %s; use --%s=%s to resume it or --%s=%s to start a fresh attempt",
				resumableBackups[0], cfgHashDirName, flagOnPending, snapshotRepoOnPendingResume, flagOnPending, snapshotRepoOnPendingNew,
			)
		}
		return repoV1BackupAttempt{}, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"found multiple unfinished repo-v1 backups for config hash %s: %s",
			cfgHashDirName, formatRepoV1BackupIDs(resumableBackups),
		)
	case snapshotRepoOnPendingResume:
		switch len(resumableBackups) {
		case 0:
			return repoV1BackupAttempt{}, nil
		case 1:
			return repoV1BackupAttempt{
				backupID:             resumableBackups[0],
				resumeFromCheckpoint: true,
			}, nil
		default:
			return repoV1BackupAttempt{}, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found multiple unfinished repo-v1 backups for config hash %s: %s; cannot resume an ambiguous backup",
				cfgHashDirName, formatRepoV1BackupIDs(resumableBackups),
			)
		}
	case snapshotRepoOnPendingNew:
		return repoV1BackupAttempt{}, nil
	default:
		return repoV1BackupAttempt{}, errors.Annotatef(
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

func snapshotMetadataStorage(rootStorage storeapi.Storage, backupID repo.BackupID) storeapi.Storage {
	return repo.NewPrefixedStorage(rootStorage, snapshotpaths.MetadataDir(backupID))
}

func openSnapshotStorageForRestore(
	ctx context.Context,
	cfg *RestoreConfig,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
) (*snapshotStorageRef, error) {
	resolved := &snapshotStorageRef{
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
	resolved.MetadataStorage = snapshotMetadataStorage(rootStorage, cfg.BackupID)
	return resolved, nil
}

func loadSnapshotBackupMeta(
	ctx context.Context,
	cfg *RestoreConfig,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
) (*snapshotStorageRef, *backuppb.BackupMeta, error) {
	resolved, err := openSnapshotStorageForRestore(ctx, cfg, rootBackend, rootStorage)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	backupMeta, err := ReadBackupMetaFromStorage(ctx, metautil.MetaFile, resolved.MetadataStorage, &cfg.Config)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return resolved, backupMeta, nil
}

// ResolveSnapshotBackupMeta resolves one snapshot backup reference and reads its backupmeta.
func ResolveSnapshotBackupMeta(
	ctx context.Context,
	storageName string,
	cfg *Config,
	layout repo.Layout,
	backupID repo.BackupID,
) (*backuppb.StorageBackend, storeapi.Storage, *backuppb.BackupMeta, error) {
	rootBackend, rootStorage, err := GetStorage(ctx, storageName, cfg)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	resolved, backupMeta, err := loadSnapshotBackupMeta(ctx, &RestoreConfig{
		Config:   *cfg,
		Layout:   layout,
		BackupID: backupID,
	}, rootBackend, rootStorage)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return resolved.RootBackend, resolved.MetadataStorage, backupMeta, nil
}

type pendingBackupState string

const (
	pendingBackupStateStaleMarker pendingBackupState = "stale-marker"
	pendingBackupStateResumable   pendingBackupState = "resumable"
	pendingBackupStateTransient   pendingBackupState = "transient"
)

// A pending marker can mean three different things when BR starts up:
//   - final backupmeta already exists: the marker is stale and can be removed.
//   - checkpoint metadata exists: the backup is resumable.
//   - neither exists: the marker was left behind before checkpoint metadata became
//     durable, so it should be cleaned up together with any transient checkpoint
//     files.
func collectResumablePendingBackups(ctx context.Context, rootStorage storeapi.Storage, cfgHash []byte) ([]repo.BackupID, error) {
	entries, err := listPendingBackups(ctx, rootStorage, cfgHash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resumable := make([]repo.BackupID, 0, len(entries))
	for _, entry := range entries {
		metadataStorage := snapshotMetadataStorage(rootStorage, entry.backupID)
		state, err := inspectPendingBackupState(ctx, metadataStorage)
		if err != nil {
			return nil, errors.Annotatef(err, "inspect pending backup %s", entry.backupID)
		}
		switch state {
		case pendingBackupStateStaleMarker:
			if err := removeStalePendingMarker(ctx, rootStorage, entry); err != nil {
				return nil, errors.Trace(err)
			}
		case pendingBackupStateTransient:
			if err := cleanupTransientPendingBackup(ctx, rootStorage, metadataStorage, entry); err != nil {
				return nil, errors.Trace(err)
			}
		case pendingBackupStateResumable:
			resumable = append(resumable, entry.backupID)
		default:
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "unknown pending backup state %q", state)
		}
	}
	return resumable, nil
}

func inspectPendingBackupState(ctx context.Context, metadataStorage storeapi.Storage) (pendingBackupState, error) {
	hasBackupMeta, err := metadataStorage.FileExists(ctx, metautil.MetaFile)
	if err != nil {
		return "", errors.Trace(err)
	}
	if hasBackupMeta {
		return pendingBackupStateStaleMarker, nil
	}
	hasCheckpoint, err := metadataStorage.FileExists(ctx, checkpoint.CheckpointMetaPathForBackup)
	if err != nil {
		return "", errors.Trace(err)
	}
	if hasCheckpoint {
		return pendingBackupStateResumable, nil
	}
	return pendingBackupStateTransient, nil
}

func removeStalePendingMarker(ctx context.Context, rootStorage storeapi.Storage, entry pendingBackupEntry) error {
	if err := rootStorage.DeleteFile(ctx, entry.path); err != nil {
		return errors.Annotatef(err, "remove stale pending marker for %s", entry.backupID)
	}
	return nil
}

func cleanupTransientPendingBackup(
	ctx context.Context,
	rootStorage storeapi.Storage,
	metadataStorage storeapi.Storage,
	entry pendingBackupEntry,
) error {
	if removeErr := checkpoint.RemoveCheckpointDataForBackup(ctx, metadataStorage); removeErr != nil {
		return errors.Annotatef(removeErr, "remove transient checkpoint state for %s", entry.backupID)
	}
	if removeErr := rootStorage.DeleteFile(ctx, entry.path); removeErr != nil {
		return errors.Annotatef(removeErr, "remove transient pending marker for %s", entry.backupID)
	}
	return nil
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
