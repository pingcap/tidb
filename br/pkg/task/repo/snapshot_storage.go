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

package taskrepo

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	taskcommon "github.com/pingcap/tidb/br/pkg/task/common"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	gozap "go.uber.org/zap"
)

const (
	flagStorageLayout = "storage-layout"
	flagUseCheckpoint = "use-checkpoint"
	flagOnPending     = "on-pending"
)

type OnPendingAction string

const (
	OnPendingError  OnPendingAction = "error"
	OnPendingResume OnPendingAction = "resume"
	OnPendingNew    OnPendingAction = "new"
)

type Config struct {
	objstore.BackendOptions

	Storage    string
	CipherInfo backuppb.CipherInfo
	NoCreds    bool
	SendCreds  bool
}

type SnapshotStorageRef struct {
	BackupID    repo.BackupID
	RootBackend *backuppb.StorageBackend
	RootStorage storeapi.Storage
}

type PreparedRepoV1SnapshotBackup struct {
	SnapshotStorageRef
	PendingMarkerPath    string
	ResumeFromCheckpoint bool
}

func (prepared *PreparedRepoV1SnapshotBackup) RewriteStoreRequest(storeID uint64, request *backuppb.BackupRequest) error {
	if prepared == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 backup adapter is missing prepared storage")
	}
	if request == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 backup adapter is missing request")
	}
	if err := rewriteDataBackendForStore(request.StorageBackend, storeID, prepared.BackupID); err != nil {
		return errors.Trace(err)
	}
	if err := prepareRepoV1LocalBackendForStore(storeID, *request); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (prepared *PreparedRepoV1SnapshotBackup) RewriteStoreResponseFiles(storeID uint64, files []*backuppb.File) ([]*backuppb.File, error) {
	if prepared == nil {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 backup adapter is missing prepared storage")
	}
	return rewriteDataFilesForStore(files, storeID, prepared.BackupID)
}

type SnapshotBackupStorageParams struct {
	OnPending        OnPendingAction
	ConfigHash       []byte
	CreatedBy        string
	AllocateBackupID func(context.Context) (repo.BackupID, error)
}

func ValidateSnapshotBackupRepoConfig(layout repo.Layout, useCheckpoint bool) error {
	if !layout.IsRepoV1() {
		return nil
	}
	if !useCheckpoint {
		return errors.Annotatef(
			berrors.ErrInvalidArgument,
			"--%s=%s requires --%s",
			flagStorageLayout,
			repo.LayoutRepoV1,
			flagUseCheckpoint,
		)
	}
	return nil
}

type repoV1BackupAttempt struct {
	backupID             repo.BackupID
	resumeFromCheckpoint bool
}

func PrepareRepoV1SnapshotBackup(
	ctx context.Context,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
	params SnapshotBackupStorageParams,
) (*PreparedRepoV1SnapshotBackup, error) {
	if err := validateRepoV1Backend(rootBackend); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.EnsureRepo(ctx, rootStorage, params.CreatedBy); err != nil {
		return nil, errors.Trace(err)
	}

	cfgHashDirName := repo.PendingConfigHashStorageName(params.ConfigHash)
	resumableBackups, err := collectResumablePendingBackups(ctx, rootStorage, params.ConfigHash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	attempt, err := chooseRepoV1BackupAttempt(params.OnPending, resumableBackups, cfgHashDirName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if attempt.backupID.IsZero() {
		attempt.backupID, err = params.AllocateBackupID(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	prepared := &PreparedRepoV1SnapshotBackup{
		SnapshotStorageRef: SnapshotStorageRef{
			BackupID:    attempt.backupID,
			RootBackend: rootBackend,
			RootStorage: rootStorage,
		},
		PendingMarkerPath:    repo.PendingFile(params.ConfigHash, attempt.backupID),
		ResumeFromCheckpoint: attempt.resumeFromCheckpoint,
	}
	log.Info("created repo-v1 snapshot backup storage",
		gozap.String("backup-id", attempt.backupID.String()),
		gozap.String("metadata-uri", prepared.MetadataStorage().URI()),
		gozap.Bool("resumed?", attempt.resumeFromCheckpoint))
	return prepared, nil
}

func ActivateSnapshotBackupResume(
	ctx context.Context,
	client *backup.Client,
	prepared *PreparedRepoV1SnapshotBackup,
	cfgHash []byte,
) error {
	if prepared == nil || !prepared.ResumeFromCheckpoint {
		return nil
	}
	if err := client.LoadCheckpointMetadataFromStorage(ctx, prepared.MetadataStorage()); err != nil {
		return errors.Annotatef(err, "load repo-v1 checkpoint metadata for backup %s", prepared.BackupID)
	}
	checkpointMeta := client.GetCheckpointMetadata()
	// BackupID == 0 means legacy checkpoint metadata without this field, so only enforce the match when it is set.
	if checkpointMeta != nil && checkpointMeta.BackupID != 0 && checkpointMeta.BackupID != uint64(prepared.BackupID) {
		return errors.Annotatef(
			berrors.ErrInvalidArgument,
			"repo-v1 checkpoint metadata backup id %d doesn't match resumed backup %s",
			checkpointMeta.BackupID,
			prepared.BackupID,
		)
	}
	if err := client.CheckCheckpoint(cfgHash); err != nil {
		return errors.Annotatef(err, "validate repo-v1 checkpoint metadata for backup %s", prepared.BackupID)
	}
	return nil
}

func chooseRepoV1BackupAttempt(
	onPending OnPendingAction,
	resumableBackups []repo.BackupID,
	cfgHashDirName string,
) (repoV1BackupAttempt, error) {
	switch onPending {
	case "", OnPendingError:
		switch len(resumableBackups) {
		case 0:
			return repoV1BackupAttempt{}, nil
		case 1:
			return repoV1BackupAttempt{}, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found unfinished repo-v1 backup %s for config hash %s; use --%s=%s to resume it or --%s=%s to start a fresh attempt",
				resumableBackups[0], cfgHashDirName, flagOnPending, OnPendingResume, flagOnPending, OnPendingNew,
			)
		default:
			return repoV1BackupAttempt{}, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found multiple unfinished repo-v1 backups for config hash %s: %s",
				cfgHashDirName, formatRepoV1BackupIDs(resumableBackups),
			)
		}
	case OnPendingResume:
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
	case OnPendingNew:
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
	items := make([]string, 0, len(ids))
	for _, id := range ids {
		items = append(items, id.String())
	}
	return strings.Join(items, ", ")
}

// MetadataStorage returns the metadata storage derived from the snapshot reference.
func (ref *SnapshotStorageRef) MetadataStorage() storeapi.Storage {
	if ref.BackupID.IsZero() {
		return ref.RootStorage
	}
	return repo.NewPrefixedStorage(ref.RootStorage, repo.SnapshotMetadataDir(ref.BackupID))
}

// Validate checks repo-specific invariants for the snapshot reference.
func (ref *SnapshotStorageRef) Validate(ctx context.Context) error {
	if ref.BackupID.IsZero() {
		return nil
	}
	// validateRepoV1Backend is the coarse "can repo-v1 address this backend at
	// all?" gate. A resolved snapshot reference needs one more runtime-capability
	// check: repo-v1 snapshot operations later rely on WalkDir StartAfter, and
	// that support is determined by the opened storage implementation/URI rather
	// than the protobuf backend kind alone.
	if err := validateRepoV1Backend(ref.RootBackend); err != nil {
		return errors.Trace(err)
	}
	if _, err := repo.LoadRepoMeta(ctx, ref.RootStorage); err != nil {
		return errors.Annotate(err, "load repo-v1 metadata")
	}
	if err := repo.ValidateRepoV1StartAfterSupport(ref.RootStorage); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadBackupMeta loads backup metadata from the derived metadata storage.
func (ref *SnapshotStorageRef) LoadBackupMeta(
	ctx context.Context,
	cipherInfo *backuppb.CipherInfo,
) (*backuppb.BackupMeta, error) {
	backupMeta, err := taskcommon.ReadBackupMetaFromStorage(ctx, metautil.MetaFile, ref.MetadataStorage(), cipherInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return backupMeta, nil
}

// A pending marker can mean two things when BR starts up:
//   - checkpoint metadata exists: the backup is resumable.
//   - otherwise the backup is not resumable and should be cleaned up together
//     with any leftover checkpoint files.
func collectResumablePendingBackups(ctx context.Context, rootStorage storeapi.Storage, cfgHash []byte) ([]repo.BackupID, error) {
	pendingBackups, err := repo.SnapshotOpsExtension(rootStorage).ListPendingBackupsForConfigHash(ctx, cfgHash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resumable := make([]repo.BackupID, 0, len(pendingBackups))
	for _, pending := range pendingBackups {
		metadataStorage := repo.NewPrefixedStorage(rootStorage, repo.SnapshotMetadataDir(pending.BackupID))
		switch pending.State {
		case repo.PendingBackupStateStale:
			if err := cleanupNonResumablePendingBackup(ctx, rootStorage, metadataStorage, pending); err != nil {
				return nil, errors.Trace(err)
			}
		case repo.PendingBackupStateUnfinished:
			resumable = append(resumable, pending.BackupID)
		default:
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "unknown pending backup state %q", pending.State)
		}
	}
	return resumable, nil
}

func cleanupNonResumablePendingBackup(
	ctx context.Context,
	rootStorage storeapi.Storage,
	metadataStorage storeapi.Storage,
	pending repo.PendingBackup,
) error {
	if removeErr := checkpoint.RemoveCheckpointDataForBackup(ctx, metadataStorage); removeErr != nil {
		return errors.Annotatef(removeErr, "remove checkpoint state for stale pending backup %s", pending.BackupID)
	}
	for _, markerPath := range pending.MarkerPaths {
		if removeErr := rootStorage.DeleteFile(ctx, markerPath); removeErr != nil {
			return errors.Annotatef(removeErr, "remove pending marker for stale backup %s", pending.BackupID)
		}
	}
	return nil
}

func writePendingSnapshot(ctx context.Context, rootStorage storeapi.Storage, pendingFile string) error {
	return rootStorage.WriteFile(ctx, pendingFile, []byte("{}"))
}

func rewriteDataBackendForStore(backend *backuppb.StorageBackend, storeID uint64, backupID repo.BackupID) error {
	if backend == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo-v1 request is missing storage backend")
	}
	prefix := repo.SnapshotStoreDataPrefix(storeID, backupID)
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
	prefix := repo.SnapshotStoreDataPrefix(storeID, backupID)
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

// validateRepoV1Backend validates repo-v1 backend kinds before storage is
// opened. It intentionally does not validate WalkDir StartAfter support,
// because that depends on the resolved storeapi.Storage implementation and is
// checked later by SnapshotStorageRef.Validate.
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
	// TiKV fails to open a local backend when the path prefix does not exist.
	// In production, this assumes the prefix is a mounted network volume so TiKV
	// can access the directory we create here.
	if err := os.MkdirAll(backend.GetLocal().Path, 0o750); err != nil {
		return errors.Annotatef(err, "prepare repo-v1 local backend path for store %d", storeID)
	}
	return nil
}

func RepoCreatedBy(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return "br"
	}
	return fmt.Sprintf("br %s", version)
}
