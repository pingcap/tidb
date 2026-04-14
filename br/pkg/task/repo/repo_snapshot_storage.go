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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/utils"
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
	Layout          repo.Layout
	BackupID        repo.BackupID
	RootBackend     *backuppb.StorageBackend
	RootStorage     storeapi.Storage
	MetadataStorage storeapi.Storage
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

func GetStorage(
	ctx context.Context,
	storageName string,
	cfg *Config,
) (*backuppb.StorageBackend, storeapi.Storage, error) {
	backendOptions := objstore.BackendOptions{}
	if cfg != nil {
		backendOptions = cfg.BackendOptions
	}
	u, err := objstore.ParseBackend(storageName, &backendOptions)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	s, err := objstore.New(ctx, u, storageOpts(cfg))
	if err != nil {
		return nil, nil, errors.Annotate(err, "create storage failed")
	}
	return u, s, nil
}

func storageOpts(cfg *Config) *storeapi.Options {
	opts := &storeapi.Options{}
	if cfg != nil {
		opts.NoCredentials = cfg.NoCreds
		opts.SendCredentials = cfg.SendCreds
	}
	return opts
}

func decodeBackupMeta(metaData []byte, cipherInfo *backuppb.CipherInfo) (*backuppb.BackupMeta, error) {
	ci := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	if cipherInfo != nil {
		ci = *cipherInfo
	}
	var iv []byte
	if ci.CipherType != encryptionpb.EncryptionMethod_PLAINTEXT {
		iv = metaData[:metautil.CrypterIvLen]
	}
	decryptBackupMeta, err := utils.Decrypt(metaData[len(iv):], &ci, iv)
	if err != nil {
		return nil, errors.Annotate(err, "decrypt failed with wrong key")
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = proto.Unmarshal(decryptBackupMeta, backupMeta); err != nil {
		return nil, errors.Annotate(err, "parse backupmeta failed because of wrong aes cipher")
	}
	return backupMeta, nil
}

func ReadBackupMetaFromStorage(
	ctx context.Context,
	fileName string,
	storage storeapi.Storage,
	cipherInfo *backuppb.CipherInfo,
) (*backuppb.BackupMeta, error) {
	metaData, err := storage.ReadFile(ctx, fileName)
	if err != nil {
		return nil, errors.Annotate(err, "load backupmeta failed")
	}
	return decodeBackupMeta(metaData, cipherInfo)
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
			Layout:          repo.LayoutRepoV1,
			BackupID:        attempt.backupID,
			RootBackend:     rootBackend,
			RootStorage:     rootStorage,
			MetadataStorage: snapshotMetadataStorage(rootStorage, attempt.backupID),
		},
		PendingMarkerPath:    repo.PendingFile(params.ConfigHash, attempt.backupID),
		ResumeFromCheckpoint: attempt.resumeFromCheckpoint,
	}
	log.Info("created repo-v1 snapshot backup storage",
		gozap.String("backup-id", attempt.backupID.String()),
		gozap.String("metadata-uri", prepared.MetadataStorage.URI()),
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
	if err := client.LoadCheckpointMetadataFromStorage(ctx, prepared.MetadataStorage); err != nil {
		return errors.Annotatef(err, "load repo-v1 checkpoint metadata for backup %s", prepared.BackupID)
	}
	checkpointMeta := client.GetCheckpointMetadata()
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
		if len(resumableBackups) == 0 {
			return repoV1BackupAttempt{}, nil
		}
		if len(resumableBackups) == 1 {
			return repoV1BackupAttempt{}, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found unfinished repo-v1 backup %s for config hash %s; use --%s=%s to resume it or --%s=%s to start a fresh attempt",
				resumableBackups[0], cfgHashDirName, flagOnPending, OnPendingResume, flagOnPending, OnPendingNew,
			)
		}
		return repoV1BackupAttempt{}, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"found multiple unfinished repo-v1 backups for config hash %s: %s",
			cfgHashDirName, formatRepoV1BackupIDs(resumableBackups),
		)
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
	return repo.NewPrefixedStorage(rootStorage, repo.SnapshotMetadataDir(backupID))
}

func openSnapshotStorageForRestore(
	ctx context.Context,
	layout repo.Layout,
	backupID repo.BackupID,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
) (*SnapshotStorageRef, error) {
	resolved := &SnapshotStorageRef{
		Layout:          layout,
		BackupID:        backupID,
		RootBackend:     rootBackend,
		RootStorage:     rootStorage,
		MetadataStorage: rootStorage,
	}
	if !layout.IsRepoV1() {
		return resolved, nil
	}
	if err := validateRepoV1Backend(rootBackend); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.LoadRepoMeta(ctx, rootStorage); err != nil {
		return nil, errors.Annotate(err, "load repo-v1 metadata")
	}
	resolved.MetadataStorage = snapshotMetadataStorage(rootStorage, backupID)
	return resolved, nil
}

func LoadSnapshotBackupMeta(
	ctx context.Context,
	layout repo.Layout,
	backupID repo.BackupID,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
	cipherInfo *backuppb.CipherInfo,
) (*SnapshotStorageRef, *backuppb.BackupMeta, error) {
	resolved, err := openSnapshotStorageForRestore(ctx, layout, backupID, rootBackend, rootStorage)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	backupMeta, err := ReadBackupMetaFromStorage(ctx, metautil.MetaFile, resolved.MetadataStorage, cipherInfo)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return resolved, backupMeta, nil
}

func ResolveSnapshotBackupMeta(
	ctx context.Context,
	cfg Config,
	ref repo.SnapshotRef,
) (*backuppb.StorageBackend, storeapi.Storage, *backuppb.BackupMeta, error) {
	rootBackend, rootStorage, err := GetStorage(ctx, cfg.Storage, &cfg)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	resolved, backupMeta, err := LoadSnapshotBackupMeta(ctx, ref.Layout, ref.BackupID, rootBackend, rootStorage, &cfg.CipherInfo)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return resolved.RootBackend, resolved.MetadataStorage, backupMeta, nil
}

// A pending marker can mean three different things when BR starts up:
//   - final backupmeta already exists: the marker is stale and can be removed.
//   - checkpoint metadata exists: the backup is resumable.
//   - neither exists: the marker was left behind before checkpoint metadata became
//     durable, so it should be cleaned up together with any transient checkpoint
//     files.
func collectResumablePendingBackups(ctx context.Context, rootStorage storeapi.Storage, cfgHash []byte) ([]repo.BackupID, error) {
	pendingBackups, err := repo.ListPendingBackupsForConfigHash(ctx, rootStorage, cfgHash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resumable := make([]repo.BackupID, 0, len(pendingBackups))
	for _, pending := range pendingBackups {
		metadataStorage := snapshotMetadataStorage(rootStorage, pending.BackupID)
		switch pending.State {
		case repo.PendingBackupStateStale:
			if err := deletePendingMarkers(ctx, rootStorage, pending); err != nil {
				return nil, errors.Trace(err)
			}
		case repo.PendingBackupStateTransient:
			if err := cleanupTransientPendingBackup(ctx, rootStorage, metadataStorage, pending); err != nil {
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

func deletePendingMarkers(ctx context.Context, rootStorage storeapi.Storage, pending repo.PendingBackup) error {
	for _, markerPath := range pending.MarkerPaths {
		if err := rootStorage.DeleteFile(ctx, markerPath); err != nil {
			return errors.Annotatef(err, "remove stale pending marker for %s", pending.BackupID)
		}
	}
	return nil
}

func cleanupTransientPendingBackup(
	ctx context.Context,
	rootStorage storeapi.Storage,
	metadataStorage storeapi.Storage,
	pending repo.PendingBackup,
) error {
	if removeErr := checkpoint.RemoveCheckpointDataForBackup(ctx, metadataStorage); removeErr != nil {
		return errors.Annotatef(removeErr, "remove transient checkpoint state for %s", pending.BackupID)
	}
	for _, markerPath := range pending.MarkerPaths {
		if removeErr := rootStorage.DeleteFile(ctx, markerPath); removeErr != nil {
			return errors.Annotatef(removeErr, "remove transient pending marker for %s", pending.BackupID)
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
