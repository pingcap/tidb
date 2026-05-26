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

package operator

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

type syncedStorage struct {
	storeapi.Storage
	synced bool
}

func (s syncedStorage) FileSynced(context.Context, string) (bool, error) {
	return s.synced, nil
}

func TestNewCRRCheckpointServiceRejectsNonLogBackupUpstream(t *testing.T) {
	ctx := context.Background()
	cfg := CRRCheckpointConfig{
		UpstreamStorage: "local://" + filepath.ToSlash(t.TempDir()),
	}
	cfg.CRRConfig.TaskName = "test-task"

	svc, cleanup, err := NewCRRCheckpointService(ctx, nil, cfg)
	require.Nil(t, svc)
	require.Nil(t, cleanup)
	require.ErrorContains(t, err, "is not a log backup directory")
	require.ErrorContains(t, err, metautil.LockFile)
}

func TestCheckCRRUpstreamStorage(t *testing.T) {
	ctx := context.Background()
	upstream, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(upstream.Close)

	err = checkCRRUpstreamStorage(ctx, upstream)
	require.ErrorContains(t, err, "is not a log backup directory")
	require.ErrorContains(t, err, metautil.LockFile)

	require.NoError(t, upstream.WriteFile(ctx, metautil.LockFile, nil))
	require.NoError(t, checkCRRUpstreamStorage(ctx, upstream))
}

func TestBuildObjectSyncChecker(t *testing.T) {
	ctx := context.Background()
	upstream, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(upstream.Close)
	downstream, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(downstream.Close)
	require.NoError(t, downstream.WriteFile(ctx, "synced.log", []byte("synced")))

	checker, err := buildObjectSyncChecker(upstream, downstream, true)
	require.NoError(t, err)
	synced, err := checker.FileSynced(ctx, "synced.log")
	require.NoError(t, err)
	require.True(t, synced)

	checker, err = buildObjectSyncChecker(syncedStorage{Storage: upstream, synced: true}, downstream, false)
	require.NoError(t, err)
	synced, err = checker.FileSynced(ctx, "missing-downstream.log")
	require.NoError(t, err)
	require.True(t, synced)

	checker, err = buildObjectSyncChecker(upstream, downstream, false)
	require.ErrorContains(t, err, "upstream storage cannot check object sync")

	_, err = buildObjectSyncChecker(upstream, nil, false)
	require.ErrorContains(t, err, "upstream storage cannot check object sync")
}
