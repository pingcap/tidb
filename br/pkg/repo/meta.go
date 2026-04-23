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

package repo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const RepoVersion = 1

const repoInitLockPath = RootLockPath + ".init"

var errRepoRootContainsArtifacts = errors.New("repo root contains snapshot artifacts")

type RepoMeta struct {
	RepoVersion int    `json:"repo_version"`
	RepoID      string `json:"repo_id"`
	CreatedAt   string `json:"created_at"`
	CreatedBy   string `json:"created_by"`
}

func LoadRepoMeta(ctx context.Context, storage storeapi.Storage) (*RepoMeta, error) {
	data, err := storage.ReadFile(ctx, RepoMetaPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta := &RepoMeta{}
	if err := json.Unmarshal(data, meta); err != nil {
		return nil, errors.Annotate(err, "decode repo metadata")
	}
	if meta.RepoVersion != RepoVersion {
		return nil, errors.Errorf("unsupported repo version %d", meta.RepoVersion)
	}
	return meta, nil
}

func EnsureRepo(
	ctx context.Context,
	storage storeapi.Storage,
	createdBy string,
) (*RepoMeta, error) {
	lock, err := objstore.LockWithRetry(
		ctx,
		objstore.TryLockRemote,
		storage,
		repoInitLockPath,
		"initialize BR snapshot repository metadata",
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer lock.UnlockOnCleanUp(ctx)

	exists, err := storage.FileExists(ctx, RepoMetaPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if exists {
		meta, err := LoadRepoMeta(ctx, storage)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err := ensureRepoGuard(ctx, storage); err != nil {
			return nil, errors.Trace(err)
		}
		return meta, nil
	}

	if err := ensureRepoRootIsCleanForInit(ctx, storage); err != nil {
		return nil, errors.Trace(err)
	}

	meta := &RepoMeta{
		RepoVersion: RepoVersion,
		RepoID:      uuid.NewString(),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		CreatedBy:   createdBy,
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := storage.WriteFile(ctx, RepoMetaPath, payload); err != nil {
		return nil, errors.Annotate(err, "write repo metadata")
	}
	if err := ensureRepoGuard(ctx, storage); err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

func ensureRepoRootIsCleanForInit(ctx context.Context, storage storeapi.Storage) error {
	for _, path := range []string{
		metautil.MetaFile,
		metautil.LockFile,
		checkpoint.CheckpointMetaPathForBackup,
	} {
		exists, err := storage.FileExists(ctx, path)
		if err != nil {
			return errors.Trace(err)
		}
		if exists {
			return errors.Errorf("storage %s already contains legacy snapshot artifact %q", storage.URI(), path)
		}
	}
	for _, prefix := range []string{
		snapshotMetadataRootDir,
		pendingRootDir,
		snapshotDataRootDir,
	} {
		found := false
		err := storage.WalkDir(ctx, &storeapi.WalkOption{SubDir: prefix}, func(string, int64) error {
			found = true
			return errRepoRootContainsArtifacts
		})
		if err != nil && !errors.ErrorEqual(err, errRepoRootContainsArtifacts) {
			return errors.Trace(err)
		}
		if found {
			return errors.Errorf("storage %s already contains repo-v1 snapshot artifact under %q", storage.URI(), prefix)
		}
	}
	return nil
}

func ensureRepoGuard(ctx context.Context, storage storeapi.Storage) error {
	exists, err := storage.FileExists(ctx, RootLockPath)
	if err != nil {
		return errors.Trace(err)
	}
	if exists {
		return nil
	}
	return storage.WriteFile(ctx, RootLockPath, []byte("DO NOT DELETE\nThis path is managed as a BR snapshot repository."))
}
