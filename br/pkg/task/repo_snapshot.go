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
	"iter"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/repo/snapshotpaths"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
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
	BackupID repo.BackupID
}

type RepoSnapshotDeleteResult = repo.SnapshotDeleteResult

type RepoSnapshotPendingDiscardConfig struct {
	Config
	BackupID repo.BackupID
}

type RepoSnapshotPendingDiscardResult = repo.PendingDiscardResult

type RepoSnapshotOrphansConfig struct {
	Config
}

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

func RunRepoSnapshotList(ctx context.Context, cfg RepoSnapshotListConfig) ([]repo.BackupID, error) {
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) ([]repo.BackupID, error) {
		return repo.ListCompletedSnapshotIDs(ctx, storage)
	})
}

func RunRepoSnapshotGet(ctx context.Context, cfg RepoSnapshotGetConfig) ([]byte, error) {
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) ([]byte, error) {
		view, err := normalizeRepoSnapshotMetaView(cfg.View)
		if err != nil {
			return nil, errors.Trace(err)
		}
		metadataStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(cfg.BackupID))
		backupMeta, err := ReadBackupMetaFromStorage(ctx, metautil.MetaFile, metadataStorage, &cfg.Config)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reader := metautil.NewMetaReader(backupMeta, metadataStorage, &cfg.CipherInfo)
		return renderRepoSnapshotMetaView(ctx, reader, view)
	})
}

func RunRepoSnapshotDelete(ctx context.Context, cfg RepoSnapshotDeleteConfig) (*RepoSnapshotDeleteResult, error) {
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) (*RepoSnapshotDeleteResult, error) {
		return repo.DeleteSnapshot(ctx, storage, cfg.BackupID)
	})
}

func RunRepoSnapshotPendingDiscard(
	ctx context.Context,
	cfg RepoSnapshotPendingDiscardConfig,
) (*RepoSnapshotPendingDiscardResult, error) {
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) (*RepoSnapshotPendingDiscardResult, error) {
		pendingBackups, err := repo.ListPendingBackups(ctx, storage)
		if err != nil {
			return nil, errors.Trace(err)
		}
		target, err := selectPendingBackupForDiscard(cfg.BackupID, pendingBackups)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return repo.DiscardPendingSnapshot(ctx, storage, *target)
	})
}

func RunRepoSnapshotOrphansList(
	ctx context.Context,
	cfg RepoSnapshotOrphansConfig,
) ([]string, error) {
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) ([]string, error) {
		return repo.ListSnapshotOrphans(ctx, storage)
	})
}

func WalkRepoSnapshotOrphans(
	ctx context.Context,
	cfg RepoSnapshotOrphansConfig,
) iter.Seq2[string, error] {
	storage, err := openSnapshotRepoStorage(ctx, &cfg.Config)
	if err != nil {
		return func(yield func(string, error) bool) {
			yield("", errors.Trace(err))
		}
	}
	return func(yield func(string, error) bool) {
		defer storage.Close()
		for orphanPath, err := range repo.WalkSnapshotOrphans(ctx, storage) {
			if !yield(orphanPath, err) {
				return
			}
		}
	}
}

func RunRepoSnapshotOrphansDelete(
	ctx context.Context,
	cfg RepoSnapshotOrphansConfig,
) (int, error) {
	return withSnapshotRepoStorage(ctx, &cfg.Config, func(storage storeapi.Storage) (int, error) {
		return repo.DeleteSnapshotOrphans(ctx, storage)
	})
}

func openSnapshotRepoStorage(ctx context.Context, cfg *Config) (storeapi.Storage, error) {
	_, storage, err := GetStorage(ctx, cfg.Storage, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := repo.LoadRepoMeta(ctx, storage, snapshotpaths.RepoMetaPath); err != nil {
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
) ([]byte, error) {
	basic := reader.GetBasic()
	if err := validateRepoSnapshotMetaView(basic); err != nil {
		return nil, errors.Trace(err)
	}
	switch view {
	case repoSnapshotMetaViewBasic:
		return marshalRepoSnapshotView(convertRepoSnapshotBasicView(basic))
	case repoSnapshotMetaViewTables:
		if basic.IsRawKv {
			return nil, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"snapshot metadata view %q is unavailable for raw backups",
				view,
			)
		}
		tables, err := collectRepoSnapshotTables(ctx, reader)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return marshalRepoSnapshotView(tables)
	case repoSnapshotMetaViewFiles:
		files, err := collectRepoSnapshotFiles(ctx, reader)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return marshalRepoSnapshotView(files)
	default:
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "unknown snapshot metadata view %q", view)
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

func marshalRepoSnapshotView(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return append(payload, '\n'), nil
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

func collectRepoSnapshotTables(ctx context.Context, reader *metautil.MetaReader) ([]repoSnapshotTableView, error) {
	out := make(chan *metautil.Table, 16)
	errc := make(chan error, 1)
	go func() {
		errc <- reader.ReadSchemasFiles(ctx, out, metautil.SkipFiles, metautil.SkipStats)
		close(out)
	}()

	var tables []repoSnapshotTableView
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case table, ok := <-out:
			if !ok {
				if err := <-errc; err != nil {
					return nil, errors.Trace(err)
				}
				slices.SortFunc(tables, func(a, b repoSnapshotTableView) int {
					switch {
					case a.DBName < b.DBName:
						return -1
					case a.DBName > b.DBName:
						return 1
					case a.TableName < b.TableName:
						return -1
					case a.TableName > b.TableName:
						return 1
					case a.KVCount < b.KVCount:
						return -1
					case a.KVCount > b.KVCount:
						return 1
					case a.KVSize < b.KVSize:
						return -1
					case a.KVSize > b.KVSize:
						return 1
					case a.TiFlashReplica < b.TiFlashReplica:
						return -1
					case a.TiFlashReplica > b.TiFlashReplica:
						return 1
					default:
						return 0
					}
				})
				return tables, nil
			}
			tableName := ""
			if table.Info != nil {
				tableName = table.Info.Name.String()
			}
			tables = append(tables, repoSnapshotTableView{
				DBName:         table.DB.Name.String(),
				TableName:      tableName,
				KVCount:        table.TotalKvs,
				KVSize:         table.TotalBytes,
				TiFlashReplica: uint64(table.TiFlashReplicas),
			})
		}
	}
}

func collectRepoSnapshotFiles(ctx context.Context, reader *metautil.MetaReader) ([]repoSnapshotFileView, error) {
	out := make(chan *backuppb.File, 16)
	errc := make(chan error, 1)
	go func() {
		errc <- reader.ReadDataFiles(ctx, out)
		close(out)
	}()

	var files []repoSnapshotFileView
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case file, ok := <-out:
			if !ok {
				if err := <-errc; err != nil {
					return nil, errors.Trace(err)
				}
				slices.SortFunc(files, func(a, b repoSnapshotFileView) int {
					switch {
					case a.Name < b.Name:
						return -1
					case a.Name > b.Name:
						return 1
					case a.CF < b.CF:
						return -1
					case a.CF > b.CF:
						return 1
					case a.StartKey < b.StartKey:
						return -1
					case a.StartKey > b.StartKey:
						return 1
					case a.EndKey < b.EndKey:
						return -1
					case a.EndKey > b.EndKey:
						return 1
					default:
						return 0
					}
				})
				return files, nil
			}
			files = append(files, convertRepoSnapshotFileView(file))
		}
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
