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
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/storage"
	taskcommon "github.com/pingcap/tidb/br/pkg/task/common"
	"github.com/tikv/client-go/v2/oracle"
)

type RepoSnapshotListConfig struct {
	Config
}

// RepoSnapshotBackupStatus is the lifecycle status shown by snapshot list.
type RepoSnapshotBackupStatus string

const (
	// RepoSnapshotBackupStatusDone means completed snapshot metadata exists.
	RepoSnapshotBackupStatusDone RepoSnapshotBackupStatus = "DONE"
	// RepoSnapshotBackupStatusPending means the snapshot only has pending backup markers.
	RepoSnapshotBackupStatusPending RepoSnapshotBackupStatus = "PENDING"
	// RepoSnapshotBackupStatusPartialRemoved means a previous delete reached this backup.
	RepoSnapshotBackupStatusPartialRemoved RepoSnapshotBackupStatus = "PARTIAL_REMOVED"
)

// RepoSnapshotListItem is one row returned by the snapshot list operation.
type RepoSnapshotListItem struct {
	BackupID repo.BackupID
	Status   RepoSnapshotBackupStatus
}

type RepoSnapshotGetConfig struct {
	Config
	BackupID repo.BackupID
	View     string
}

type RepoSnapshotDeleteConfig struct {
	Config
	BackupID   repo.BackupID
	SkipPrompt bool
}

type RepoSnapshotDeleteResult = repo.SnapshotDeleteResult

type repoSnapshotMetaView string

const (
	repoSnapshotMetaViewBasic  repoSnapshotMetaView = "basic"
	repoSnapshotMetaViewTables repoSnapshotMetaView = "tables"
	repoSnapshotMetaViewFiles  repoSnapshotMetaView = "files"
)

type repoSnapshotBasicView struct {
	ClusterID            uint64 `json:"cluster-id"`
	ClusterVersion       string `json:"cluster-version"`
	BRVersion            string `json:"br-version"`
	Version              int32  `json:"version"`
	StartVersion         uint64 `json:"start-version"`
	EndVersion           uint64 `json:"end-version"`
	IsRawKV              bool   `json:"is-raw-kv"`
	IsTxnKV              bool   `json:"is-txn-kv"`
	APIVersion           int32  `json:"api-version"`
	NewCollationsEnabled string `json:"new-collations-enabled,omitempty"`
	BackupSize           uint64 `json:"backup-size"`
	BackupResult         string `json:"backup-result,omitempty"`
	Mode                 int32  `json:"mode"`
}

type repoSnapshotTableView struct {
	DBName         string `json:"db-name"`
	TableName      string `json:"table-name"`
	KVCount        uint64 `json:"kv-count"`
	KVSize         uint64 `json:"kv-size"`
	TiFlashReplica int    `json:"tiflash-replica"`
}

type repoSnapshotFileView struct {
	Name       string `json:"name"`
	StartKey   string `json:"start-key"`
	EndKey     string `json:"end-key"`
	CF         string `json:"cf"`
	Size       uint64 `json:"size"`
	TotalKVs   uint64 `json:"total-kvs"`
	TotalBytes uint64 `json:"total-bytes"`
	SHA256     string `json:"sha256"`
}

type repoSnapshotDeletePreview struct {
	HasBasic   bool
	Basic      repoSnapshotBasicView
	HasPending bool
	Pending    repo.PendingBackup
}

// RunRepoSnapshotListItems lists snapshot backups with their display status.
func RunRepoSnapshotListItems(
	ctx context.Context,
	console glue.ConsoleOperations,
	cfg RepoSnapshotListConfig,
) ([]RepoSnapshotListItem, error) {
	return runRepoSnapshotSpinnerTask(ctx, console, "Listing snapshot backups...", nil, func(taskCtx context.Context) ([]RepoSnapshotListItem, error) {
		return withSnapshotRepoStorage(taskCtx, &cfg.Config, func(storage storage.Storage) ([]RepoSnapshotListItem, error) {
			return listRepoSnapshotBackups(taskCtx, storage)
		})
	})
}

func RunRepoSnapshotGet(
	ctx context.Context,
	console glue.ConsoleOperations,
	cfg RepoSnapshotGetConfig,
) ([]byte, error) {
	var out bytes.Buffer
	if err := RunRepoSnapshotGetTo(ctx, console, cfg, &out); err != nil {
		return nil, errors.Trace(err)
	}
	return out.Bytes(), nil
}

func RunRepoSnapshotGetTo(
	ctx context.Context,
	console glue.ConsoleOperations,
	cfg RepoSnapshotGetConfig,
	out io.Writer,
) error {
	if out == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "output writer is required")
	}
	extraFields := []glue.ExtraField{glue.WithConstExtraField("backup-id", cfg.BackupID.String())}
	_, err := runRepoSnapshotSpinnerTask(ctx, console, "Loading snapshot metadata...", extraFields, func(taskCtx context.Context) (struct{}, error) {
		return withSnapshotRepoStorage(taskCtx, &cfg.Config, func(storage storage.Storage) (struct{}, error) {
			view, err := normalizeRepoSnapshotMetaView(cfg.View)
			if err != nil {
				return struct{}{}, errors.Trace(err)
			}
			metadataStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(cfg.BackupID))
			backupMeta, err := taskcommon.ReadBackupMetaFromStorage(taskCtx, metautil.MetaFile, metadataStorage, &cfg.CipherInfo)
			if err != nil {
				return struct{}{}, errors.Trace(err)
			}
			reader := metautil.NewMetaReader(backupMeta, metadataStorage, &cfg.CipherInfo)
			if err := renderRepoSnapshotMetaView(taskCtx, reader, view, out); err != nil {
				return struct{}{}, errors.Trace(err)
			}
			return struct{}{}, nil
		})
	})
	return errors.Trace(err)
}

func RunRepoSnapshotDelete(
	ctx context.Context,
	console glue.ConsoleOperations,
	cfg RepoSnapshotDeleteConfig,
) (RepoSnapshotDeleteResult, error) {
	var deleteResult RepoSnapshotDeleteResult
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storage.Storage) (RepoSnapshotDeleteResult, error) {
		if err := confirmRepoSnapshotDelete(ctx, console, storage, &cfg); err != nil {
			return RepoSnapshotDeleteResult{}, errors.Trace(err)
		}
		extraFields := append(
			[]glue.ExtraField{glue.WithConstExtraField("backup-id", cfg.BackupID.String())},
			withSnapshotMutationDeletedExtraFields(func() RepoSnapshotDeleteResult { return deleteResult })...,
		)
		return runRepoSnapshotDynamicProgressTask(
			ctx,
			console,
			"Deleting snapshot backup...",
			extraFields,
			func(progress repoSnapshotDynamicProgressTaskContext) (RepoSnapshotDeleteResult, error) {
				snapshotOps := repo.SnapshotOpsExtension(storage)
				result, err := snapshotOps.DeleteSnapshot(
					progress.Context,
					cfg.BackupID,
					repo.WithMutationDiscoveredProgress(func(count int) { progress.AddTotal(int64(count)) }),
					repo.WithMutationDeletedProgress(func(count int) { progress.Advance(int64(count)) }),
				)
				deleteResult = result
				return result, err
			},
		)
	})
}

func listRepoSnapshotBackups(ctx context.Context, storage storage.Storage) ([]RepoSnapshotListItem, error) {
	deletingIDs, err := repo.ListDeletingSnapshotIDs(ctx, storage)
	if err != nil {
		return nil, errors.Trace(err)
	}
	completedIDs, err := repo.ListCompletedSnapshotIDs(ctx, storage)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pendingBackups, err := repo.SnapshotOpsExtension(storage).ListPendingBackups(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	backups := make([]RepoSnapshotListItem, 0, len(deletingIDs)+len(completedIDs)+len(pendingBackups))
	seen := make(map[repo.BackupID]struct{}, len(deletingIDs)+len(completedIDs)+len(pendingBackups))
	appendBackup := func(backupID repo.BackupID, status RepoSnapshotBackupStatus) {
		if _, ok := seen[backupID]; ok {
			return
		}
		seen[backupID] = struct{}{}
		backups = append(backups, RepoSnapshotListItem{BackupID: backupID, Status: status})
	}
	for _, backupID := range deletingIDs {
		appendBackup(backupID, RepoSnapshotBackupStatusPartialRemoved)
	}
	for _, backupID := range completedIDs {
		appendBackup(backupID, RepoSnapshotBackupStatusDone)
	}
	for _, pending := range pendingBackups {
		appendBackup(pending.BackupID, RepoSnapshotBackupStatusPending)
	}
	slices.SortFunc(backups, func(a, b RepoSnapshotListItem) int {
		return cmp.Compare(a.BackupID, b.BackupID)
	})
	return backups, nil
}

func withDeletedObjectsExtraField(key string, count func() int) glue.ExtraField {
	return glue.WithCallbackExtraField(key, func() string {
		return strconv.Itoa(count())
	})
}

func withDeletedCountExtraFields(metadata, data, pending func() int) []glue.ExtraField {
	return []glue.ExtraField{
		withDeletedObjectsExtraField("metadata#", metadata),
		withDeletedObjectsExtraField("data#", data),
		withDeletedObjectsExtraField("pending#", pending),
	}
}

func withSnapshotMutationDeletedExtraFields(result func() RepoSnapshotDeleteResult) []glue.ExtraField {
	return withDeletedCountExtraFields(
		func() int { return result().MetadataDeleted },
		func() int { return result().DataDeleted },
		func() int { return result().PendingDeleted },
	)
}

func shouldConfirmRepoSnapshotMutation(console glue.ConsoleOperations, skipPrompt bool) bool {
	return !skipPrompt && console.IsInteractive()
}

func confirmRepoSnapshotDelete(
	ctx context.Context,
	console glue.ConsoleOperations,
	storage storage.Storage,
	cfg *RepoSnapshotDeleteConfig,
) error {
	if !shouldConfirmRepoSnapshotMutation(console, cfg.SkipPrompt) {
		return nil
	}
	preview, err := collectRepoSnapshotDeletePreview(ctx, storage, &cfg.Config, cfg.BackupID)
	if err != nil {
		return errors.Trace(err)
	}
	console.Printf("About to delete snapshot backup %s (%s).\n", cfg.BackupID, formatRepoSnapshotBackupTime(cfg.BackupID))
	console.Println("This permanently removes snapshot metadata, data files, and pending markers for this backup.")
	info := console.CreateTable()
	if preview.HasBasic {
		info.Add("backup-size", formatRepoSnapshotBytes(preview.Basic.BackupSize))
		switch {
		case preview.Basic.IsRawKV:
			info.Add("backup-type", "raw-kv")
		case preview.Basic.IsTxnKV:
			info.Add("backup-type", "txn-kv")
		}
	}
	if preview.HasPending {
		info.Add("state", string(preview.Pending.State))
		info.Add("pending-markers", fmt.Sprintf("%d", len(preview.Pending.MarkerPaths)))
	}
	info.Print()
	if !console.PromptBool("Continue? ") {
		return errors.Trace(berrors.ErrOperationAborted)
	}
	return nil
}

func collectRepoSnapshotDeletePreview(
	ctx context.Context,
	storage storage.Storage,
	cfg *Config,
	backupID repo.BackupID,
) (repoSnapshotDeletePreview, error) {
	preview := repoSnapshotDeletePreview{}
	metadataStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID))
	hasBackupMeta, err := metadataStorage.FileExists(ctx, metautil.MetaFile)
	if err != nil {
		return preview, errors.Annotatef(err, "collect delete preview for snapshot %s: check metadata", backupID)
	}
	if hasBackupMeta {
		backupMeta, err := taskcommon.ReadBackupMetaFromStorage(ctx, metautil.MetaFile, metadataStorage, &cfg.CipherInfo)
		if err != nil {
			return preview, errors.Annotatef(err, "collect delete preview for snapshot %s: read metadata", backupID)
		}
		preview.HasBasic = true
		preview.Basic = convertRepoSnapshotBasicView(*backupMeta)
	}
	pendingBackups, err := repo.SnapshotOpsExtension(storage).ListPendingBackups(ctx)
	if err != nil {
		return preview, errors.Annotatef(err, "collect delete preview for snapshot %s: list pending backups", backupID)
	}
	for _, pending := range pendingBackups {
		if pending.BackupID == backupID {
			preview.HasPending = true
			preview.Pending = pending
			break
		}
	}
	return preview, nil
}

func formatRepoSnapshotBytes(size uint64) string {
	return fmt.Sprintf("%d (%s)", size, units.HumanSize(float64(size)))
}

func formatRepoSnapshotBackupTime(backupID repo.BackupID) string {
	return oracle.GetTimeFromTS(uint64(backupID)).Format("2006-01-02 15:04:05.999999999 -0700")
}

func waitRepoSnapshotProgressDone(progress glue.ProgressWaiter) {
	// Progress rendering is best-effort UI work. Once the underlying repo task has
	// succeeded, a late cancellation of the operation context should not rewrite a
	// successful mutation into a failed command result.
	_ = progress.Wait(context.Background())
}

func runRepoSnapshotSpinnerTask[T any](
	ctx context.Context,
	console glue.ConsoleOperations,
	title string,
	extraFields []glue.ExtraField,
	fn func(context.Context) (T, error),
) (T, error) {
	progress := console.StartProgressBar(title, glue.OnlyOneTask, append([]glue.ExtraField{glue.WithTimeCost()}, extraFields...)...)
	defer progress.Close()
	result, err := fn(ctx)
	if err != nil {
		return result, errors.Trace(err)
	}
	progress.Inc()
	waitRepoSnapshotProgressDone(progress)
	return result, nil
}

type repoSnapshotDynamicProgressTaskContext struct {
	context.Context
	AddTotal func(int64)
	Advance  func(int64)
}

func runRepoSnapshotDynamicProgressTask[T any](
	ctx context.Context,
	console glue.ConsoleOperations,
	title string,
	extraFields []glue.ExtraField,
	fn func(repoSnapshotDynamicProgressTaskContext) (T, error),
) (T, error) {
	progress := console.StartDynamicProgressBar(title, append([]glue.ExtraField{glue.WithTimeCost()}, extraFields...)...)
	defer progress.Close()
	taskProgress := repoSnapshotDynamicProgressTaskContext{
		Context:  ctx,
		AddTotal: progress.AddTotal,
	}
	taskProgress.Advance = func(delta int64) {
		if delta <= 0 {
			return
		}
		if delta == 1 {
			progress.Inc()
			return
		}
		progress.IncBy(delta)
	}
	result, err := fn(taskProgress)
	if err != nil {
		return result, errors.Trace(err)
	}
	progress.Complete()
	waitRepoSnapshotProgressDone(progress)
	return result, nil
}

func openSnapshotRepoStorage(ctx context.Context, cfg *Config) (storage.Storage, error) {
	_, storage, err := taskcommon.GetStorage(ctx, cfg.Storage, cfg.BackendOptions, cfg.NoCreds, cfg.SendCreds)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.LoadRepoMeta(ctx, storage); err != nil {
		storage.Close()
		return nil, errors.Trace(err)
	}
	return storage, nil
}

func withSnapshotRepoStorage[T any](
	ctx context.Context,
	cfg *Config,
	fn func(storage.Storage) (T, error),
) (T, error) {
	var zero T
	storage, err := openSnapshotRepoStorage(ctx, cfg)
	if err != nil {
		return zero, errors.Trace(err)
	}
	defer storage.Close()
	return fn(storage)
}

func normalizeRepoSnapshotMetaView(raw string) (repoSnapshotMetaView, error) {
	view := repoSnapshotMetaView(strings.ToLower(strings.TrimSpace(raw)))
	if view == "" {
		return repoSnapshotMetaViewBasic, nil
	}
	switch view {
	case repoSnapshotMetaViewBasic, repoSnapshotMetaViewTables, repoSnapshotMetaViewFiles:
		return view, nil
	default:
		return "", errors.Annotatef(
			berrors.ErrInvalidArgument,
			"unsupported snapshot metadata view %q; supported views are: basic, tables, files",
			raw,
		)
	}
}

func renderRepoSnapshotMetaView(
	ctx context.Context,
	reader *metautil.MetaReader,
	view repoSnapshotMetaView,
	out io.Writer,
) error {
	basic := reader.GetBasic()
	if err := validateRepoSnapshotMetaView(basic); err != nil {
		return errors.Trace(err)
	}
	switch view {
	case repoSnapshotMetaViewBasic:
		return writeRepoSnapshotJSONValue(out, convertRepoSnapshotBasicView(basic))
	case repoSnapshotMetaViewTables:
		if basic.IsRawKv {
			return errors.Annotatef(
				berrors.ErrInvalidArgument,
				"snapshot metadata view %q is unavailable for raw backups",
				view,
			)
		}
		return streamRepoSnapshotTables(ctx, reader, out)
	case repoSnapshotMetaViewFiles:
		return streamRepoSnapshotFiles(ctx, reader, out)
	default:
		return errors.Annotatef(berrors.ErrInvalidArgument, "unknown snapshot metadata view %q", view)
	}
}

func validateRepoSnapshotMetaView(meta backuppb.BackupMeta) error {
	if meta.EndVersion < meta.StartVersion {
		return errors.Annotatef(
			berrors.ErrInvalidMetaFile,
			"the start version(%d) is greater than the end version(%d), perhaps reading a backup meta from log backup",
			meta.StartVersion,
			meta.EndVersion,
		)
	}
	return nil
}

func writeRepoSnapshotJSONValue(out io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err := out.Write(payload); err != nil {
		return errors.Trace(err)
	}
	if _, err := io.WriteString(out, "\n"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func convertRepoSnapshotBasicView(meta backuppb.BackupMeta) repoSnapshotBasicView {
	return repoSnapshotBasicView{
		ClusterID:            meta.ClusterId,
		ClusterVersion:       meta.ClusterVersion,
		BRVersion:            meta.BrVersion,
		Version:              meta.Version,
		StartVersion:         meta.StartVersion,
		EndVersion:           meta.EndVersion,
		IsRawKV:              meta.IsRawKv,
		IsTxnKV:              meta.IsTxnKv,
		APIVersion:           int32(meta.ApiVersion),
		NewCollationsEnabled: meta.NewCollationsEnabled,
		BackupSize:           meta.BackupSize,
		BackupResult:         meta.BackupResult,
		Mode:                 int32(meta.Mode),
	}
}

func streamRepoSnapshotTables(ctx context.Context, reader *metautil.MetaReader, out io.Writer) error {
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outCh := make(chan *metautil.Table, 16)
	errCh := make(chan error, 1)
	go func() {
		errCh <- reader.ReadSchemasFiles(readCtx, outCh, metautil.SkipFiles, metautil.SkipStats)
		close(outCh)
	}()

	var resultErr error
	doneCh := ctx.Done()
	for {
		select {
		case <-doneCh:
			if resultErr == nil {
				resultErr = errors.Trace(ctx.Err())
				cancel()
			}
			doneCh = nil
		case table, ok := <-outCh:
			if !ok {
				if err := <-errCh; err != nil && resultErr == nil {
					resultErr = errors.Trace(err)
				}
				return resultErr
			}
			if resultErr != nil {
				continue
			}
			if err := writeRepoSnapshotJSONValue(out, convertRepoSnapshotTableView(table)); err != nil {
				resultErr = errors.Trace(err)
				cancel()
			}
		}
	}
}

func streamRepoSnapshotFiles(ctx context.Context, reader *metautil.MetaReader, out io.Writer) error {
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var resultErr error
	if err := reader.ReadDataFiles(readCtx, func(file *backuppb.File) {
		if resultErr != nil {
			return
		}
		if err := writeRepoSnapshotJSONValue(out, convertRepoSnapshotFileView(file)); err != nil {
			resultErr = errors.Trace(err)
			cancel()
		}
	}); err != nil && resultErr == nil {
		resultErr = errors.Trace(err)
	}
	return resultErr
}

func convertRepoSnapshotTableView(table *metautil.Table) repoSnapshotTableView {
	tableName := ""
	if table.Info != nil {
		tableName = table.Info.Name.String()
	}
	return repoSnapshotTableView{
		DBName:         table.DB.Name.String(),
		TableName:      tableName,
		KVCount:        table.TotalKvs,
		KVSize:         table.TotalBytes,
		TiFlashReplica: table.TiFlashReplicas,
	}
}

func convertRepoSnapshotFileView(file *backuppb.File) repoSnapshotFileView {
	return repoSnapshotFileView{
		Name:       file.GetName(),
		StartKey:   hex.EncodeToString(file.GetStartKey()),
		EndKey:     hex.EncodeToString(file.GetEndKey()),
		CF:         file.GetCf(),
		Size:       file.GetSize_(),
		TotalKVs:   file.GetTotalKvs(),
		TotalBytes: file.GetTotalBytes(),
		SHA256:     hex.EncodeToString(file.GetSha256()),
	}
}
