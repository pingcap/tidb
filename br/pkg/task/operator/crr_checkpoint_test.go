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
	"time"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

type syncedStorage struct {
	storeapi.Storage
	synced bool
}

func (s syncedStorage) FileSynced(context.Context, string) (bool, error) {
	return s.synced, nil
}

func TestNewEtcdClientConfig(t *testing.T) {
	ctx := context.Background()
	cfg := task.Config{
		PD:                   []string{"pd-service:2379"},
		GRPCKeepaliveTime:    10 * time.Second,
		GRPCKeepaliveTimeout: 3 * time.Second,
	}

	backoffCfg := etcdGRPCBackoffConfig()
	require.Equal(t, 3*time.Second, backoffCfg.MaxDelay)
	keepaliveParams := etcdKeepaliveParams(cfg)
	require.Equal(t, 10*time.Second, keepaliveParams.Time)
	require.Equal(t, 3*time.Second, keepaliveParams.Timeout)
	require.True(t, keepaliveParams.PermitWithoutStream)

	etcdCfg, err := newEtcdClientConfig(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, cfg.PD, etcdCfg.Endpoints)
	require.Equal(t, 30*time.Second, etcdCfg.AutoSyncInterval)
	require.Equal(t, 5*time.Second, etcdCfg.DialTimeout)
	require.Equal(t, ctx, etcdCfg.Context)
	require.Len(t, etcdCfg.DialOptions, 4)
}

func TestNewCRRCheckpointServiceRejectsNonLogBackupUpstream(t *testing.T) {
	ctx := context.Background()
	flags := pflag.NewFlagSet("crr-checkpoint", pflag.ContinueOnError)
	task.DefineCommonFlags(flags)
	DefineFlagsForCRRCheckpointConfig(flags)
	require.NoError(t, flags.Set(flagTaskName, "test-task"))
	require.NoError(t, flags.Set(flagUpstreamStorage, "local://"+filepath.ToSlash(t.TempDir())))

	parseCfg := CRRCheckpointConfig{}
	err := parseCfg.ParseFromFlags(flags)
	require.ErrorContains(t, err, "missing required flag --downstream-storage")

	cfg := CRRCheckpointConfig{
		UpstreamStorage:   "local://" + filepath.ToSlash(t.TempDir()),
		DownstreamStorage: "local://" + filepath.ToSlash(t.TempDir()),
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

	stateStore := buildResumeStateStore(upstream)
	storageStore, ok := stateStore.(*storageResumeStateStore)
	require.True(t, ok)
	require.Equal(t, "crr-checkpoint/resume-state.json", storageStore.path)

	err = checkCRRExternalStorage(ctx, upstream, "upstream")
	require.ErrorContains(t, err, "is not a log backup directory")
	require.ErrorContains(t, err, "upstream storage")
	require.ErrorContains(t, err, metautil.LockFile)

	require.NoError(t, upstream.WriteFile(ctx, metautil.LockFile, nil))
	require.NoError(t, checkCRRExternalStorage(ctx, upstream, "upstream"))

	downstream, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(downstream.Close)

	err = checkCRRExternalStorage(ctx, downstream, "downstream")
	require.ErrorContains(t, err, "is not a log backup directory")
	require.ErrorContains(t, err, "downstream storage")
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
