// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestTopology(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	currentID := "test"

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	client := cluster.RandClient()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/serverinfo/mockServerInfo", "return(true)"))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/domain/serverinfo/mockServerInfo")
		require.NoError(t, err)
	}()

	info := NewSyncer(currentID, func() uint64 { return 1 }, client, nil)

	err := info.NewTopologySessionAndStoreServerInfo(ctx)
	require.NoError(t, err)

	topology, err := info.getTopologyFromEtcd(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1282967700), topology.StartTimestamp)

	v, ok := topology.Labels["foo"]
	require.True(t, ok)
	require.Equal(t, "bar", v)
	selfInfo := info.GetLocalServerInfo()
	require.Equal(t, selfInfo.ToTopologyInfo(), *topology)

	nonTTLKey := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, selfInfo.IP, selfInfo.Port)
	ttlKey := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, selfInfo.IP, selfInfo.Port)

	err = etcd.DeleteKeyFromEtcd(nonTTLKey, client, util2.NewSessionDefaultRetryCnt, time.Second)
	require.NoError(t, err)

	// Refresh and re-test if the key exists
	err = info.RestartTopology(ctx)
	require.NoError(t, err)

	topology, err = info.getTopologyFromEtcd(ctx)
	require.NoError(t, err)

	s, err := os.Executable()
	require.NoError(t, err)

	dir := path.Dir(s)
	require.Equal(t, dir, topology.DeployPath)
	require.Equal(t, int64(1282967700), topology.StartTimestamp)
	require.Equal(t, info.GetLocalServerInfo().ToTopologyInfo(), *topology)

	// check ttl key
	ttlExists, err := info.ttlKeyExists(ctx)
	require.NoError(t, err)
	require.True(t, ttlExists)

	err = etcd.DeleteKeyFromEtcd(ttlKey, client, util2.NewSessionDefaultRetryCnt, time.Second)
	require.NoError(t, err)

	err = info.updateTopologyAliveness(ctx)
	require.NoError(t, err)

	ttlExists, err = info.ttlKeyExists(ctx)
	require.NoError(t, err)
	require.True(t, ttlExists)
}

func (s *Syncer) getTopologyFromEtcd(ctx context.Context) (*TopologyInfo, error) {
	info := s.GetLocalServerInfo()
	key := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, info.IP, info.Port)
	resp, err := s.etcdCli.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("not-exists")
	}
	if len(resp.Kvs) != 1 {
		return nil, errors.New("resp.Kvs error")
	}
	var ret TopologyInfo
	err = json.Unmarshal(resp.Kvs[0].Value, &ret)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func (s *Syncer) ttlKeyExists(ctx context.Context) (bool, error) {
	info := s.GetLocalServerInfo()
	key := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, info.IP, info.Port)
	resp, err := s.etcdCli.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) >= 2 {
		return false, errors.New("too many arguments in resp.Kvs")
	}
	return len(resp.Kvs) == 1, nil
}

func TestCleanupStaleServerAndOwnerInfo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	client := cluster.RandClient()

	// Configure global config so that new Syncers get IP=1.1.1.1, Port=4000.
	bak := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(bak)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.AdvertiseAddress = "1.1.1.1"
		conf.Port = 4000
	})

	// --- Setup: write stale ServerInfo with same IP+Port but different UUID ---
	staleID := "stale-uuid-old"
	staleInfo := &ServerInfo{
		StaticInfo: StaticInfo{
			ID:             staleID,
			IP:             "1.1.1.1",
			Port:           4000,
			ServerIDGetter: func() uint64 { return 0 },
		},
	}
	staleInfoBuf, err := staleInfo.Marshal()
	require.NoError(t, err)
	staleInfoPath := serverInfoKeyPath(staleID)
	_, err = client.Put(ctx, staleInfoPath, string(staleInfoBuf))
	require.NoError(t, err)

	// --- Setup: write stale DDL owner election key with the stale UUID as value ---
	staleOwnerKey := util.DDLOwnerKey + "/12345"
	_, err = client.Put(ctx, staleOwnerKey, staleID)
	require.NoError(t, err)

	// --- Setup: write another node's ServerInfo with different IP (should NOT be deleted) ---
	otherID := "other-uuid"
	otherInfo := &ServerInfo{
		StaticInfo: StaticInfo{
			ID:             otherID,
			IP:             "2.2.2.2",
			Port:           4000,
			ServerIDGetter: func() uint64 { return 0 },
		},
	}
	otherInfoBuf, err := otherInfo.Marshal()
	require.NoError(t, err)
	otherInfoPath := serverInfoKeyPath(otherID)
	_, err = client.Put(ctx, otherInfoPath, string(otherInfoBuf))
	require.NoError(t, err)

	// --- Act: create a new Syncer with same IP+Port and call NewSessionAndStoreServerInfo ---
	newID := "new-uuid"
	syncer := NewSyncer(newID, func() uint64 { return 1 }, client, nil)
	// Verify the new Syncer has the same IP+Port as the stale entry.
	newInfo := syncer.GetLocalServerInfo()
	require.Equal(t, "1.1.1.1", newInfo.IP)
	require.Equal(t, uint(4000), newInfo.Port)
	err = syncer.NewSessionAndStoreServerInfo(ctx)
	require.NoError(t, err)

	// --- Assert: stale ServerInfo should be deleted ---
	resp, err := client.Get(ctx, staleInfoPath)
	require.NoError(t, err)
	require.Empty(t, resp.Kvs, "stale server info should have been deleted")

	// --- Assert: stale DDL owner key should be deleted ---
	resp, err = client.Get(ctx, staleOwnerKey)
	require.NoError(t, err)
	require.Empty(t, resp.Kvs, "stale DDL owner key should have been deleted")

	// --- Assert: other node's ServerInfo should still exist ---
	resp, err = client.Get(ctx, otherInfoPath)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1, "other node's server info should not be deleted")

	// --- Assert: new ServerInfo should be registered ---
	newInfoPath := serverInfoKeyPath(newID)
	resp, err = client.Get(ctx, newInfoPath)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1, "new server info should be registered")
}

func TestAssumedServerInfoSyncer(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only for nextgen kernel")
	}
	bak := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(bak)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = keyspace.System
	})

	// current ks
	syncer := NewSyncer("1", func() uint64 { return 1 }, nil, nil)
	info := syncer.GetLocalServerInfo()
	require.False(t, info.IsAssumed())
	require.Empty(t, info.AssumedKeyspace)
	require.EqualValues(t, keyspace.System, info.Keyspace)

	// cross ks
	syncer = NewCrossKSSyncer("1", func() uint64 { return 1 }, nil, nil, "ks1")
	info = syncer.GetLocalServerInfo()
	require.True(t, info.IsAssumed())
	require.Equal(t, "ks1", info.AssumedKeyspace)
	require.EqualValues(t, keyspace.System, info.Keyspace)
}
