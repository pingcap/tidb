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

package repo_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestLayoutAndBackupID(t *testing.T) {
	layout, err := repo.ParseLayout("")
	require.NoError(t, err)
	require.Equal(t, repo.LayoutLegacy, layout)

	layout, err = repo.ParseLayout("repo-v1")
	require.NoError(t, err)
	require.Equal(t, repo.LayoutRepoV1, layout)

	_, err = repo.ParseLayout("unknown")
	require.Error(t, err)

	id, err := repo.NewBackupID(0xf00d)
	require.NoError(t, err)
	require.Equal(t, "61453", id.String())
	require.Equal(t, "000000000000F00D", id.StorageName())

	parsed, err := repo.ParseBackupID("61453")
	require.NoError(t, err)
	require.Equal(t, id, parsed)

	parsed, err = repo.ParseBackupIDStorageName("000000000000F00D")
	require.NoError(t, err)
	require.Equal(t, id, parsed)

	_, err = repo.ParseBackupID("bad")
	require.Error(t, err)
}

func TestPrefixedStorage(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewMemStorage()
	view := repo.NewPrefixedStorage(base, "root/meta")

	require.NoError(t, view.WriteFile(ctx, "backupmeta", []byte("meta")))
	require.NoError(t, view.WriteFile(ctx, "nested/file.txt", []byte("nested")))

	exists, err := base.FileExists(ctx, "root/meta/backupmeta")
	require.NoError(t, err)
	require.True(t, exists)

	got, err := view.ReadFile(ctx, "backupmeta")
	require.NoError(t, err)
	require.Equal(t, []byte("meta"), got)

	var walked []string
	err = view.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, _ int64) error {
		walked = append(walked, path)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"backupmeta", "nested/file.txt"}, walked)
}

func TestEnsureRepo(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()

	meta, err := repo.EnsureRepo(ctx, storage, "br test")
	require.NoError(t, err)
	require.Equal(t, repo.RepoVersion, meta.RepoVersion)

	reloaded, err := repo.LoadRepoMeta(ctx, storage)
	require.NoError(t, err)
	require.Equal(t, meta.RepoID, reloaded.RepoID)

	exists, err := storage.FileExists(ctx, repo.RootLockPath)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestEnsureRepoRejectsLegacyArtifacts(t *testing.T) {
	ctx := context.Background()
	for _, artifact := range []string{
		metautil.MetaFile,
		metautil.LockFile,
		checkpoint.CheckpointMetaPathForBackup,
	} {
		storage := objstore.NewMemStorage()
		require.NoError(t, storage.WriteFile(ctx, artifact, []byte("x")))

		_, err := repo.EnsureRepo(ctx, storage, "br test")
		require.Error(t, err)
		require.Contains(t, err.Error(), artifact)
	}
}

func TestEnsureRepoRejectsExistingRepoArtifactsWithoutMeta(t *testing.T) {
	ctx := context.Background()
	for _, artifact := range []string{
		repo.SnapshotMetadataFile(repo.BackupID(1)),
		repo.PendingFile([]byte("hash"), repo.BackupID(2)),
		repo.SnapshotStoreDataPrefix(3, repo.BackupID(4)) + "/sst",
	} {
		storage := objstore.NewMemStorage()
		require.NoError(t, storage.WriteFile(ctx, artifact, []byte("x")))

		_, err := repo.EnsureRepo(ctx, storage, "br test")
		require.Error(t, err)
		require.Contains(t, err.Error(), "repo-v1 snapshot artifact")
	}
}
