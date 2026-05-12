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

// revive:disable-next-line:file-header
package repo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
)

const RepoVersion = 1

const (
	repoInitLockPath = RootLockPath + ".init"
	repoGuardFile    = "DO NOT DELETE\n" +
		"This path is managed as a BR snapshot repository."
)

var errRepoRootContainsArtifacts = errors.New(
	"repo root contains snapshot artifacts",
)

type Meta struct {
	RepoVersion int    `json:"repo_version"`
	RepoID      string `json:"repo_id"`
	CreatedAt   string `json:"created_at"`
	CreatedBy   string `json:"created_by"`
}

// revive:disable-next-line:cyclomatic
func LoadRepoMeta(
	ctx context.Context,
	st storage.Storage,
) (*Meta, error) {
	data, err := st.ReadFile(ctx, RepoMetaPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta := &Meta{}
	if err := json.Unmarshal(data, meta); err != nil {
		return nil, errors.Annotate(err, "decode repo metadata")
	}
	if meta.RepoVersion != RepoVersion {
		return nil, errors.Errorf(
			"unsupported repo version %d",
			meta.RepoVersion,
		)
	}
	return meta, nil
}

// revive:disable-next-line:cognitive-complexity
func EnsureRepo(
	ctx context.Context,
	st storage.Storage,
	createdBy string,
) (*Meta, error) {
	if err := storage.TryLockRemote(ctx, st, repoInitLockPath, "initialize BR snapshot repository metadata"); err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = storage.UnlockRemote(ctx, st, repoInitLockPath)
	}()

	exists, err := st.FileExists(ctx, RepoMetaPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if exists {
		meta, err := LoadRepoMeta(ctx, st)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err := ensureRepoGuard(ctx, st); err != nil {
			return nil, errors.Trace(err)
		}
		return meta, nil
	}

	if err := ensureRepoRootIsCleanForInit(ctx, st); err != nil {
		return nil, errors.Trace(err)
	}

	meta := &Meta{
		RepoVersion: RepoVersion,
		RepoID:      uuid.NewString(),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		CreatedBy:   createdBy,
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := st.WriteFile(ctx, RepoMetaPath, payload); err != nil {
		return nil, errors.Annotate(err, "write repo metadata")
	}
	if err := ensureRepoGuard(ctx, st); err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

// revive:disable-next-line:cognitive-complexity
func ensureRepoRootIsCleanForInit(ctx context.Context, st storage.Storage) error {
	for _, path := range []string{
		metautil.MetaFile,
		metautil.LockFile,
		checkpoint.CheckpointMetaPathForBackup,
	} {
		exists, err := st.FileExists(ctx, path)
		if err != nil {
			return errors.Trace(err)
		}
		if exists {
			return errors.Errorf(
				"storage %s already contains legacy snapshot artifact %q",
				st.URI(),
				path,
			)
		}
	}
	for _, prefix := range []string{
		snapshotMetadataRootDir,
		pendingRootDir,
		snapshotDataRootDir,
	} {
		found := false
		err := st.WalkDir(
			ctx,
			&storage.WalkOption{SubDir: prefix},
			func(string, int64) error {
				found = true
				return errRepoRootContainsArtifacts
			},
		)
		if err != nil && !errors.ErrorEqual(err, errRepoRootContainsArtifacts) {
			return errors.Trace(err)
		}
		if found {
			return errors.Errorf(
				"storage %s already contains repo snapshot artifact "+
					"under %q",
				st.URI(),
				prefix,
			)
		}
	}
	return nil
}

func ensureRepoGuard(ctx context.Context, st storage.Storage) error {
	exists, err := st.FileExists(ctx, RootLockPath)
	if err != nil {
		return errors.Trace(err)
	}
	if exists {
		return nil
	}
	return st.WriteFile(ctx, RootLockPath, []byte(repoGuardFile))
}
