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
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	gozap "go.uber.org/zap"
)

const flagStorageLayout = "storage-layout"

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

type PreparedRepoSnapshotBackup struct {
	SnapshotStorageRef
	PendingMarkerPath    string
	ResumeFromCheckpoint bool
}

func (prepared *PreparedRepoSnapshotBackup) RewriteStoreRequest(storeID uint64, request *backuppb.BackupRequest) error {
	if prepared == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo backup adapter is missing prepared storage")
	}
	if request == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo backup adapter is missing request")
	}
	if err := rewriteDataBackendForStore(request.StorageBackend, storeID, prepared.BackupID); err != nil {
		return errors.Trace(err)
	}
	if err := prepareRepoLocalBackendForStore(storeID, *request); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (prepared *PreparedRepoSnapshotBackup) RewriteStoreResponseFiles(storeID uint64, files []*backuppb.File) ([]*backuppb.File, error) {
	if prepared == nil {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "repo backup adapter is missing prepared storage")
	}
	return rewriteDataFilesForStore(files, storeID, prepared.BackupID)
}

type SnapshotBackupStorageParams struct {
	UseCheckpoint bool
	// SkipPrompt resumes the matching unfinished backup without asking the user.
	SkipPrompt       bool
	ConfigHash       []byte
	CreatedBy        string
	AllocateBackupID func(context.Context) (repo.BackupID, error)
}

type repoBackupAttempt struct {
	backupID             repo.BackupID
	resumeFromCheckpoint bool
}

func PrepareRepoSnapshotBackup(
	ctx context.Context,
	console glue.ConsoleOperations,
	rootBackend *backuppb.StorageBackend,
	rootStorage storeapi.Storage,
	params SnapshotBackupStorageParams,
) (*PreparedRepoSnapshotBackup, error) {
	if err := validateRepoBackend(rootBackend); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.EnsureRepo(ctx, rootStorage, params.CreatedBy); err != nil {
		return nil, errors.Trace(err)
	}

	resumableBackups, err := collectResumablePendingBackups(ctx, rootStorage, params.ConfigHash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	attempt, err := chooseRepoBackupAttempt(
		console,
		params.UseCheckpoint,
		params.SkipPrompt,
		rootStorage,
		params.ConfigHash,
		resumableBackups,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if attempt.backupID.IsZero() {
		attempt.backupID, err = params.AllocateBackupID(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	prepared := &PreparedRepoSnapshotBackup{
		SnapshotStorageRef: SnapshotStorageRef{
			BackupID:    attempt.backupID,
			RootBackend: rootBackend,
			RootStorage: rootStorage,
		},
		PendingMarkerPath:    repo.PendingFile(params.ConfigHash, attempt.backupID),
		ResumeFromCheckpoint: attempt.resumeFromCheckpoint,
	}
	log.Info("created repo snapshot backup storage",
		gozap.String("backup-id", attempt.backupID.String()),
		gozap.String("metadata-uri", prepared.MetadataStorage().URI()),
		gozap.Bool("resumed?", attempt.resumeFromCheckpoint))
	return prepared, nil
}

func ActivateSnapshotBackupResume(
	ctx context.Context,
	client *backup.Client,
	prepared *PreparedRepoSnapshotBackup,
	cfgHash []byte,
) error {
	if prepared == nil || !prepared.ResumeFromCheckpoint {
		return nil
	}
	if err := client.LoadCheckpointMetadataFromStorage(ctx, prepared.MetadataStorage()); err != nil {
		return errors.Annotatef(err, "load repo checkpoint metadata for backup %s", prepared.BackupID)
	}
	checkpointMeta := client.GetCheckpointMetadata()
	// BackupID == 0 means legacy checkpoint metadata without this field, so only enforce the match when it is set.
	if checkpointMeta != nil && checkpointMeta.BackupID != 0 && checkpointMeta.BackupID != uint64(prepared.BackupID) {
		return errors.Annotatef(
			berrors.ErrInvalidArgument,
			"repo checkpoint metadata backup id %d doesn't match resumed backup %s",
			checkpointMeta.BackupID,
			prepared.BackupID,
		)
	}
	if err := client.CheckCheckpoint(cfgHash); err != nil {
		return errors.Annotatef(err, "validate repo checkpoint metadata for backup %s", prepared.BackupID)
	}
	return nil
}

func chooseRepoBackupAttempt(
	console glue.ConsoleOperations,
	useCheckpoint bool,
	skipPrompt bool,
	rootStorage storeapi.Storage,
	cfgHash []byte,
	resumableBackups []repo.BackupID,
) (repoBackupAttempt, error) {
	if !useCheckpoint {
		return repoBackupAttempt{}, nil
	}
	cfgHashDirName := repo.PendingConfigHashStorageName(cfgHash)
	switch len(resumableBackups) {
	case 0:
		return repoBackupAttempt{}, nil
	case 1:
		backupID := resumableBackups[0]
		if !confirmRepoSnapshotResume(console, skipPrompt, rootStorage, cfgHash, backupID) {
			log.Info("repo snapshot backup resume prompt was declined",
				gozap.String("backup-id", backupID.String()),
				gozap.String("config-hash", cfgHashDirName))
			return repoBackupAttempt{}, errors.Annotatef(
				berrors.ErrOperationAborted,
				"declined to resume unfinished repo snapshot backup %s; no new backup was created; to start a new backup, rerun this backup command with `--use-checkpoint=false`; to clean up pending backups, run `br repo snapshot pending discard --storage <repo-storage> --backup-id %s`",
				backupID,
				backupID,
			)
		}
		return repoBackupAttempt{
			backupID:             backupID,
			resumeFromCheckpoint: true,
		}, nil
	default:
		return repoBackupAttempt{}, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"found multiple unfinished repo backups for config hash %s: %s; cannot resume an ambiguous backup; use --use-checkpoint=false to start a fresh attempt",
			cfgHashDirName, formatRepoBackupIDs(resumableBackups),
		)
	}
}

func confirmRepoSnapshotResume(
	console glue.ConsoleOperations,
	skipPrompt bool,
	rootStorage storeapi.Storage,
	cfgHash []byte,
	backupID repo.BackupID,
) bool {
	if skipPrompt || !console.IsInteractive() {
		return true
	}
	console.Println("Found an unfinished repo snapshot backup that matches this backup command.")
	info := console.CreateTable()
	info.Add("backup-id", backupID.String())
	info.Add("backup-time", formatRepoSnapshotBackupTime(backupID))
	info.Add("config-hash", repo.PendingConfigHashStorageName(cfgHash))
	info.Add("metadata-uri", repo.NewPrefixedStorage(rootStorage, repo.SnapshotMetadataDir(backupID)).URI())
	info.Add("pending-marker", repo.PendingFile(cfgHash, backupID))
	info.Print()
	console.Println("Choosing yes resumes from its checkpoint. Choosing no aborts this backup command and leaves the unfinished backup unchanged.")
	return promptRepoSnapshotResume(console, "Resume this backup? ")
}

func promptRepoSnapshotResume(console glue.ConsoleOperations, prompt string) bool {
	for {
		ans := ""
		console.Print(prompt + "(Y/n) ")
		if n, err := console.Scanln(&ans); err != nil || n == 0 {
			return true
		}
		switch strings.ToLower(strings.TrimSpace(ans)) {
		case "", "y":
			return true
		case "n":
			return false
		}
	}
}

func formatRepoBackupIDs(ids []repo.BackupID) string {
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
	// validateRepoBackend is the coarse "can repo address this backend at
	// all?" gate. A resolved snapshot reference needs one more runtime-capability
	// check: repo snapshot operations later rely on WalkDir StartAfter, and
	// that support is determined by the opened storage implementation/URI rather
	// than the protobuf backend kind alone.
	if err := validateRepoBackend(ref.RootBackend); err != nil {
		return errors.Trace(err)
	}
	if _, err := repo.LoadRepoMeta(ctx, ref.RootStorage); err != nil {
		return errors.Annotate(err, "load repo metadata")
	}
	if err := repo.ValidateRepoStartAfterSupport(ref.RootStorage); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetBackupMetaBytes loads backup metadata bytes from the derived metadata
// storage and decrypts them when needed.
func (ref *SnapshotStorageRef) GetBackupMetaBytes(
	ctx context.Context,
	cipherInfo *backuppb.CipherInfo,
) ([]byte, error) {
	backupMetaBytes, err := ref.MetadataStorage().ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMetaBytes, err = metautil.DecryptFullBackupMetaIfNeeded(backupMetaBytes, cipherInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return backupMetaBytes, nil
}

// LoadBackupMeta loads backup metadata from the derived metadata storage.
func (ref *SnapshotStorageRef) LoadBackupMeta(
	ctx context.Context,
	cipherInfo *backuppb.CipherInfo,
) (*backuppb.BackupMeta, error) {
	backupMetaBytes, err := ref.GetBackupMetaBytes(ctx, cipherInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = backupMeta.Unmarshal(backupMetaBytes); err != nil {
		return nil, errors.Annotate(err, "parse backupmeta failed")
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
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo request is missing storage backend")
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
		return errors.Errorf("repo is unsupported for backend %T", backend.Backend)
	}
	return nil
}

func rewriteDataFilesForStore(files []*backuppb.File, storeID uint64, backupID repo.BackupID) ([]*backuppb.File, error) {
	prefix := repo.SnapshotStoreDataPrefix(storeID, backupID)
	rewritten := make([]*backuppb.File, 0, len(files))
	for _, file := range files {
		if file == nil {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "repo backup response contains nil file")
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

// validateRepoBackend validates repo backend kinds before storage is
// opened. It intentionally does not validate WalkDir StartAfter support,
// because that depends on the resolved storeapi.Storage implementation and is
// checked later by SnapshotStorageRef.Validate.
func validateRepoBackend(backend *backuppb.StorageBackend) error {
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
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo doesn't support noop storage")
	case backend.GetHdfs() != nil:
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo doesn't support hdfs storage")
	default:
		return errors.Annotatef(berrors.ErrInvalidArgument, "repo doesn't support backend %T", backend.Backend)
	}
}

func prepareRepoLocalBackendForStore(storeID uint64, request backuppb.BackupRequest) error {
	backend := request.GetStorageBackend()
	if backend == nil || backend.GetLocal() == nil {
		return nil
	}
	if backend.GetLocal().Path == "" {
		return errors.Annotatef(
			berrors.ErrInvalidArgument,
			"repo local backend for store %d is missing target path",
			storeID,
		)
	}
	// TiKV fails to open a local backend when the path prefix does not exist.
	// In production, this assumes the prefix is a mounted network volume so TiKV
	// can access the directory we create here.
	if err := os.MkdirAll(backend.GetLocal().Path, 0o750); err != nil {
		return errors.Annotatef(err, "prepare repo local backend path for store %d", storeID)
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
