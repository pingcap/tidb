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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	taskcommon "github.com/pingcap/tidb/br/pkg/task/common"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tikv/client-go/v2/oracle"
)

type RepoSnapshotListConfig struct {
	Config
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

type RepoSnapshotPendingDiscardConfig struct {
	Config
	BackupID   repo.BackupID
	SkipPrompt bool
}

type RepoSnapshotPendingDiscardResult = repo.PendingDiscardResult

type RepoSnapshotOrphansConfig struct {
	Config
	SkipPrompt bool
}

type repoSnapshotConsole interface {
	glue.ConsoleGlue
	StartProgressBar(title string, total int, extraFields ...glue.ExtraField) glue.ProgressWaiter
	StartDynamicProgressBar(title string, extraFields ...glue.ExtraField) glue.DynamicProgressWaiter
	PromptBool(string) bool
	Println(...any)
	Printf(string, ...any)
	IsInteractive() bool
}

type repoSnapshotMetaView string

const (
	repoSnapshotMetaViewBasic  repoSnapshotMetaView = "basic"
	repoSnapshotMetaViewTables repoSnapshotMetaView = "tables"
	repoSnapshotMetaViewFiles  repoSnapshotMetaView = "files"

	repoSnapshotPromptSampleLimit = 5
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
	TiFlashReplica uint64 `json:"tiflash-replica"`
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

func RunRepoSnapshotList(
	ctx context.Context,
	consoleGlue glue.ConsoleGlue,
	cfg RepoSnapshotListConfig,
) ([]repo.BackupID, error) {
	console := normalizeRepoSnapshotConsole(consoleGlue)
	return runRepoSnapshotSpinnerTask(ctx, console, "Listing snapshot backups...", nil, func(taskCtx context.Context) ([]repo.BackupID, error) {
		return withSnapshotRepoStorage(taskCtx, &cfg.Config, func(storage storeapi.Storage) ([]repo.BackupID, error) {
			return repo.ListCompletedSnapshotIDs(taskCtx, storage)
		})
	})
}

func RunRepoSnapshotGet(
	ctx context.Context,
	consoleGlue glue.ConsoleGlue,
	cfg RepoSnapshotGetConfig,
) ([]byte, error) {
	var out bytes.Buffer
	if err := RunRepoSnapshotGetTo(ctx, consoleGlue, cfg, &out); err != nil {
		return nil, errors.Trace(err)
	}
	return out.Bytes(), nil
}

func RunRepoSnapshotGetTo(
	ctx context.Context,
	consoleGlue glue.ConsoleGlue,
	cfg RepoSnapshotGetConfig,
	out io.Writer,
) error {
	if out == nil {
		return errors.Annotatef(berrors.ErrInvalidArgument, "output writer is required")
	}
	console := normalizeRepoSnapshotConsole(consoleGlue)
	extraFields := []glue.ExtraField{glue.WithConstExtraField("backup-id", cfg.BackupID.String())}
	_, err := runRepoSnapshotSpinnerTask(ctx, console, "Loading snapshot metadata...", extraFields, func(taskCtx context.Context) (struct{}, error) {
		return withSnapshotRepoStorage(taskCtx, &cfg.Config, func(storage storeapi.Storage) (struct{}, error) {
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
	consoleGlue glue.ConsoleGlue,
	cfg RepoSnapshotDeleteConfig,
) (RepoSnapshotDeleteResult, error) {
	console := normalizeRepoSnapshotConsole(consoleGlue)
	var deleteResult RepoSnapshotDeleteResult
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) (RepoSnapshotDeleteResult, error) {
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

func RunRepoSnapshotPendingDiscard(
	ctx context.Context,
	consoleGlue glue.ConsoleGlue,
	cfg RepoSnapshotPendingDiscardConfig,
) (RepoSnapshotPendingDiscardResult, error) {
	console := normalizeRepoSnapshotConsole(consoleGlue)
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) (RepoSnapshotPendingDiscardResult, error) {
		snapshotOps := repo.SnapshotOpsExtension(storage)
		pendingBackups, err := snapshotOps.ListPendingBackups(ctx)
		if err != nil {
			return RepoSnapshotPendingDiscardResult{}, errors.Trace(err)
		}
		target, err := selectPendingBackupForDiscard(cfg.BackupID, pendingBackups)
		if err != nil {
			return RepoSnapshotPendingDiscardResult{}, errors.Trace(err)
		}
		if err := confirmRepoSnapshotPendingDiscard(console, &cfg, target); err != nil {
			return RepoSnapshotPendingDiscardResult{}, errors.Trace(err)
		}
		var discardResult RepoSnapshotPendingDiscardResult
		extraFields := append(
			[]glue.ExtraField{
				glue.WithConstExtraField("backup-id", target.BackupID.String()),
				glue.WithConstExtraField("state", target.State),
			},
			withPendingDiscardDeletedExtraFields(func() RepoSnapshotPendingDiscardResult { return discardResult })...,
		)
		return runRepoSnapshotDynamicProgressTask(
			ctx,
			console,
			"Discarding pending snapshot backup...",
			extraFields,
			func(progress repoSnapshotDynamicProgressTaskContext) (RepoSnapshotPendingDiscardResult, error) {
				result, err := snapshotOps.DiscardPendingSnapshot(
					progress.Context,
					*target,
					repo.WithMutationDiscoveredProgress(func(count int) { progress.AddTotal(int64(count)) }),
					repo.WithMutationDeletedProgress(func(count int) { progress.Advance(int64(count)) }),
				)
				discardResult = result
				return result, err
			},
		)
	})
}

func RunRepoSnapshotOrphansList(
	ctx context.Context,
	consoleGlue glue.ConsoleGlue,
	cfg RepoSnapshotOrphansConfig,
) ([]string, error) {
	console := normalizeRepoSnapshotConsole(consoleGlue)
	return runRepoSnapshotSpinnerTask(ctx, console, "Listing orphan snapshot objects...", nil, func(taskCtx context.Context) ([]string, error) {
		return withSnapshotRepoStorage(taskCtx, &cfg.Config, func(storage storeapi.Storage) ([]string, error) {
			return repo.SnapshotOpsExtension(storage).ListSnapshotOrphans(taskCtx)
		})
	})
}

func RunRepoSnapshotOrphansDelete(
	ctx context.Context,
	consoleGlue glue.ConsoleGlue,
	cfg RepoSnapshotOrphansConfig,
) (int, error) {
	console := normalizeRepoSnapshotConsole(consoleGlue)
	deletedCount := 0
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) (int, error) {
		snapshotOps := repo.SnapshotOpsExtension(storage)
		hasOrphans, err := confirmRepoSnapshotOrphansDelete(ctx, console, &cfg, snapshotOps)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !hasOrphans {
			return 0, nil
		}
		return runRepoSnapshotDynamicProgressTask(
			ctx,
			console,
			"Deleting orphan snapshot objects...",
			[]glue.ExtraField{withDeletedObjectsExtraField("orphan-objects#", func() int { return deletedCount })},
			func(progress repoSnapshotDynamicProgressTaskContext) (int, error) {
				deleted, err := snapshotOps.DeleteSnapshotOrphans(
					progress.Context,
					repo.WithMutationDiscoveredProgress(func(count int) { progress.AddTotal(int64(count)) }),
					repo.WithMutationDeletedProgress(func(count int) { progress.Advance(int64(count)) }),
				)
				deletedCount = deleted
				return deleted, err
			},
		)
	})
}

func normalizeRepoSnapshotConsole(consoleGlue glue.ConsoleGlue) repoSnapshotConsole {
	if consoleGlue == nil {
		return glue.ConsoleOperations{ConsoleGlue: glue.NoOPConsoleGlue{}}
	}
	if console, ok := consoleGlue.(repoSnapshotConsole); ok {
		return console
	}
	return glue.ConsoleOperations{ConsoleGlue: consoleGlue}
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

func withPendingDiscardDeletedExtraFields(result func() RepoSnapshotPendingDiscardResult) []glue.ExtraField {
	return withDeletedCountExtraFields(
		func() int { return result().MetadataDeleted },
		func() int { return result().DataDeleted },
		func() int { return result().PendingDeleted },
	)
}

func shouldConfirmRepoSnapshotMutation(console repoSnapshotConsole, skipPrompt bool) bool {
	return !skipPrompt && console != nil && console.IsInteractive()
}

func confirmRepoSnapshotDelete(
	ctx context.Context,
	console repoSnapshotConsole,
	storage storeapi.Storage,
	cfg *RepoSnapshotDeleteConfig,
) error {
	if !shouldConfirmRepoSnapshotMutation(console, cfg.SkipPrompt) {
		return nil
	}
	preview := collectRepoSnapshotDeletePreview(ctx, storage, &cfg.Config, cfg.BackupID)
	console.Printf("About to delete snapshot backup %s (%s).\n", cfg.BackupID, formatRepoSnapshotBackupTime(cfg.BackupID))
	console.Println("This permanently removes snapshot metadata, data files, and pending markers for this backup.")
	if preview.HasBasic {
		printRepoSnapshotConfirmField(console, "backup-size", formatRepoSnapshotBytes(preview.Basic.BackupSize))
		switch {
		case preview.Basic.IsRawKV:
			printRepoSnapshotConfirmField(console, "backup-type", "raw-kv")
		case preview.Basic.IsTxnKV:
			printRepoSnapshotConfirmField(console, "backup-type", "txn-kv")
		}
	}
	if preview.HasPending {
		printRepoSnapshotConfirmField(
			console,
			"pending-markers",
			fmt.Sprintf("%d (%s)", len(preview.Pending.MarkerPaths), preview.Pending.State),
		)
	}
	if !console.PromptBool("Continue? ") {
		return errors.Trace(berrors.ErrOperationAborted)
	}
	return nil
}

func collectRepoSnapshotDeletePreview(
	ctx context.Context,
	storage storeapi.Storage,
	cfg *Config,
	backupID repo.BackupID,
) repoSnapshotDeletePreview {
	preview := repoSnapshotDeletePreview{}
	metadataStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID))
	if backupMeta, err := taskcommon.ReadBackupMetaFromStorage(ctx, metautil.MetaFile, metadataStorage, &cfg.CipherInfo); err == nil {
		preview.HasBasic = true
		preview.Basic = convertRepoSnapshotBasicView(*backupMeta)
	}
	if pendingBackups, err := repo.SnapshotOpsExtension(storage).ListPendingBackups(ctx); err == nil {
		for _, pending := range pendingBackups {
			if pending.BackupID == backupID {
				preview.HasPending = true
				preview.Pending = pending
				break
			}
		}
	}
	return preview
}

func confirmRepoSnapshotPendingDiscard(
	console repoSnapshotConsole,
	cfg *RepoSnapshotPendingDiscardConfig,
	target *repo.PendingBackup,
) error {
	if !shouldConfirmRepoSnapshotMutation(console, cfg.SkipPrompt) {
		return nil
	}
	console.Printf("About to discard pending snapshot backup %s (%s).\n", target.BackupID, formatRepoSnapshotBackupTime(target.BackupID))
	printRepoSnapshotConfirmField(console, "state", string(target.State))
	printRepoSnapshotConfirmField(console, "pending-markers", fmt.Sprintf("%d", len(target.MarkerPaths)))
	switch target.State {
	case repo.PendingBackupStateStale:
		console.Println("This removes pending markers and leftover checkpoint files; completed snapshot metadata and data files, if present, are kept.")
	case repo.PendingBackupStateUnfinished:
		console.Println("This permanently removes checkpoint/metadata files, data files, and pending markers for the unfinished backup.")
	default:
		console.Printf("Unknown pending state %q.\n", target.State)
	}
	if !console.PromptBool("Continue? ") {
		return errors.Trace(berrors.ErrOperationAborted)
	}
	return nil
}

func confirmRepoSnapshotOrphansDelete(
	ctx context.Context,
	console repoSnapshotConsole,
	cfg *RepoSnapshotOrphansConfig,
	snapshotOps repo.SnapshotOps,
) (bool, error) {
	if !shouldConfirmRepoSnapshotMutation(console, cfg.SkipPrompt) {
		return true, nil
	}
	samplePaths, hasMore, err := collectRepoSnapshotOrphanSamples(ctx, snapshotOps, repoSnapshotPromptSampleLimit)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(samplePaths) == 0 {
		return false, nil
	}
	if hasMore {
		console.Println("About to delete orphan snapshot objects.")
		console.Printf("The exact count is not precomputed before confirmation; showing the first %d discovered object(s).\n", len(samplePaths))
	} else {
		console.Printf("About to delete %d orphan snapshot object(s).\n", len(samplePaths))
	}
	console.Println("These objects are not referenced by any completed snapshot backup and will be permanently removed.")
	console.Println("Sample orphan objects:")
	for _, orphanPath := range samplePaths {
		console.Printf("  - %s\n", orphanPath)
	}
	if hasMore {
		console.Println("  ... more orphan objects may exist")
	}
	if !console.PromptBool("Continue? ") {
		return false, errors.Trace(berrors.ErrOperationAborted)
	}
	return true, nil
}

func collectRepoSnapshotOrphanSamples(
	ctx context.Context,
	snapshotOps repo.SnapshotOps,
	limit int,
) ([]string, bool, error) {
	samplePaths := make([]string, 0, limit)
	for err, orphanPath := range snapshotOps.WalkSnapshotOrphans(ctx) {
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if len(samplePaths) < limit {
			samplePaths = append(samplePaths, orphanPath)
			continue
		}
		return samplePaths, true, nil
	}
	return samplePaths, false, nil
}

func printRepoSnapshotConfirmField(console repoSnapshotConsole, key string, value string) {
	console.Printf("  %s: %s\n", key, value)
}

func formatRepoSnapshotBytes(size uint64) string {
	return fmt.Sprintf("%d (%s)", size, units.HumanSize(float64(size)))
}

func formatRepoSnapshotBackupTime(backupID repo.BackupID) string {
	return utils.FormatDate(oracle.GetTimeFromTS(uint64(backupID)))
}

func waitRepoSnapshotProgressDone(progress glue.ProgressWaiter) {
	// Progress rendering is best-effort UI work. Once the underlying repo task has
	// succeeded, a late cancellation of the operation context should not rewrite a
	// successful mutation into a failed command result.
	_ = progress.Wait(context.Background())
}

func runRepoSnapshotSpinnerTask[T any](
	ctx context.Context,
	console repoSnapshotConsole,
	title string,
	extraFields []glue.ExtraField,
	fn func(context.Context) (T, error),
) (T, error) {
	if console == nil {
		return fn(ctx)
	}
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
	console repoSnapshotConsole,
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

func openSnapshotRepoStorage(ctx context.Context, cfg *Config) (storeapi.Storage, error) {
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
	fn func(storeapi.Storage) (T, error),
) (T, error) {
	var zero T
	storage, err := openSnapshotRepoStorage(ctx, cfg)
	if err != nil {
		return zero, errors.Trace(err)
	}
	defer storage.Close()
	return fn(storage)
}

func selectPendingBackupForDiscard(
	backupID repo.BackupID,
	backups []repo.PendingBackup,
) (*repo.PendingBackup, error) {
	if len(backups) == 0 {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "no pending repo-v1 snapshot backups were found")
	}
	if backupID.IsZero() {
		if len(backups) != 1 {
			return nil, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"found multiple pending repo-v1 snapshot backups: %s; backup id is required",
				formatRepoV1BackupIDs(extractPendingBackupIDs(backups)),
			)
		}
		return &backups[0], nil
	}
	for i := range backups {
		if backups[i].BackupID == backupID {
			return &backups[i], nil
		}
	}
	return nil, errors.Annotatef(berrors.ErrInvalidArgument, "pending repo-v1 snapshot backup %s was not found", backupID)
}

func extractPendingBackupIDs(backups []repo.PendingBackup) []repo.BackupID {
	ids := make([]repo.BackupID, 0, len(backups))
	for _, backup := range backups {
		ids = append(ids, backup.BackupID)
	}
	return ids
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
		TiFlashReplica: uint64(table.TiFlashReplicas),
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
