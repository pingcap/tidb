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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/metaservice"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type syncedStorage struct {
	storeapi.Storage
	synced bool
}

func (s syncedStorage) FileSynced(context.Context, string) (bool, error) {
	return s.synced, nil
}

type metaServiceGroupPDClient struct {
	pd.Client
	members      []*pdpb.Member
	keyspaceMeta *keyspacepb.KeyspaceMeta
}

func (c *metaServiceGroupPDClient) GetAllMembers(context.Context) (*pdpb.GetMembersResponse, error) {
	return &pdpb.GetMembersResponse{Members: c.members}, nil
}

func (c *metaServiceGroupPDClient) LoadKeyspace(context.Context, string) (*keyspacepb.KeyspaceMeta, error) {
	return c.keyspaceMeta, nil
}

func (*metaServiceGroupPDClient) Close() {}

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

func TestDialEtcdWithCfgUsesMetaServiceGroup(t *testing.T) {
	integration.BeforeTestExternal(t)
	metaCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer metaCluster.Terminate(t)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Id:   46,
		Name: "ks5",
		Config: map[string]string{
			"gc_management_type":      "keyspace_level",
			metaservice.GroupIDKey:    "group5",
			metaservice.GroupAddrsKey: strings.Join(metaCluster.Client(0).Endpoints(), ","),
		},
	}
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, keyspaceMeta)
	require.NoError(t, err)

	mockPD := &metaServiceGroupPDClient{
		members: []*pdpb.Member{{
			ClientUrls: []string{"http://127.0.0.1:2379"},
		}},
		keyspaceMeta: keyspaceMeta,
	}
	orig := newPDClientWithAPIContext
	newPDClientWithAPIContext = func(context.Context, pd.APIContext, caller.Component, []string, pd.SecurityOption, ...opt.ClientOption) (pd.Client, error) {
		return mockPD, nil
	}
	t.Cleanup(func() {
		newPDClientWithAPIContext = orig
	})

	etcdCli, err := dialEtcdWithCfg(context.Background(), task.Config{
		PD:           []string{"127.0.0.1:2379"},
		KeyspaceName: keyspaceMeta.Name,
	})
	require.NoError(t, err)
	defer etcdCli.Close()

	_, err = etcdCli.Put(context.Background(), "crr-key", "1")
	require.NoError(t, err)

	prefix := keyspace.MakeKeyspaceEtcdNamespace(codec)
	resp, err := metaCluster.Client(0).Get(context.Background(), prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Contains(t, string(resp.Kvs[0].Key), "crr-key")
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
