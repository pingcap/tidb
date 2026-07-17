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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
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
	clientv3 "go.etcd.io/etcd/client/v3"
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

func TestBuildStatusEndpointClaim(t *testing.T) {
	tests := []struct {
		name             string
		host             string
		statusPort       uint
		reportStatus     bool
		assumedKeyspace  string
		expectedEndpoint string
	}{
		{
			name:             "IPv4",
			host:             " 127.0.0.1 ",
			statusPort:       10080,
			reportStatus:     true,
			expectedEndpoint: "127.0.0.1:10080",
		},
		{
			name:             "expanded IPv6",
			host:             "2001:0db8:0000:0000:0000:0000:0000:0001",
			statusPort:       10080,
			reportStatus:     true,
			expectedEndpoint: "[2001:db8::1]:10080",
		},
		{
			name:             "compressed IPv6",
			host:             "2001:db8::1",
			statusPort:       10080,
			reportStatus:     true,
			expectedEndpoint: "[2001:db8::1]:10080",
		},
		{
			name:             "DNS case and root dot",
			host:             "DB.Example.COM.",
			statusPort:       10080,
			reportStatus:     true,
			expectedEndpoint: "db.example.com:10080",
		},
		{
			name:             "different port",
			host:             "db.example.com",
			statusPort:       10081,
			reportStatus:     true,
			expectedEndpoint: "db.example.com:10081",
		},
		{
			name:             "special hostname text",
			host:             "db/name",
			statusPort:       10080,
			reportStatus:     true,
			expectedEndpoint: "db/name:10080",
		},
		{
			name:         "status reporting disabled",
			host:         "127.0.0.1",
			statusPort:   10080,
			reportStatus: false,
		},
		{
			name:            "assumed keyspace",
			host:            "127.0.0.1",
			statusPort:      10080,
			reportStatus:    true,
			assumedKeyspace: "ks1",
		},
		{
			name:         "empty host",
			statusPort:   10080,
			reportStatus: true,
		},
		{
			name:         "zero status port",
			host:         "127.0.0.1",
			reportStatus: true,
		},
	}

	keys := make(map[string]string)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := &ServerInfo{StaticInfo: StaticInfo{
				ID:              "server",
				IP:              test.host,
				StatusPort:      test.statusPort,
				AssumedKeyspace: test.assumedKeyspace,
			}}
			endpoint, key := buildStatusEndpointClaim(info, test.reportStatus)
			if test.expectedEndpoint == "" {
				require.Empty(t, endpoint)
				require.Empty(t, key)
				return
			}

			require.Equal(t, test.expectedEndpoint, endpoint)
			require.Equal(t, statusEndpointClaimTestKey(test.expectedEndpoint), key)
			segment := strings.TrimPrefix(key, "/tidb/server/status_addr/")
			require.NotEmpty(t, segment)
			require.NotContains(t, segment, "/")
			require.Equal(t, 4, strings.Count(key, "/"))
			keys[test.name] = key
		})
	}

	require.Equal(t, keys["expanded IPv6"], keys["compressed IPv6"])
	require.NotEqual(t, keys["DNS case and root dot"], keys["different port"])

	firstAlias := &ServerInfo{StaticInfo: StaticInfo{IP: "db-a.example.com", StatusPort: 10080}}
	secondAlias := &ServerInfo{StaticInfo: StaticInfo{IP: "db-b.example.com", StatusPort: 10080}}
	_, firstAliasKey := buildStatusEndpointClaim(firstAlias, true)
	_, secondAliasKey := buildStatusEndpointClaim(secondAlias, true)
	require.NotEqual(t, firstAliasKey, secondAliasKey)
}

func TestStatusEndpointClaim(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	client := cluster.RandClient()

	bak := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(bak)
	})
	t.Run("different ID conflict is warning-only", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.1", 4000, 10080, true)
		first := NewSyncer("server-r", func() uint64 { return 1 }, client, nil)
		var firstResult statusEndpointClaimResult
		firstCalls := 0
		first.statusEndpointClaimReport = func(result statusEndpointClaimResult) {
			firstCalls++
			firstResult = result
		}
		require.NoError(t, first.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(first)

		setStatusEndpointTestConfig("127.0.0.1", 4001, 10080, true)
		second := NewSyncer("server-o", func() uint64 { return 2 }, client, nil)
		var secondResult statusEndpointClaimResult
		secondCalls := 0
		second.statusEndpointClaimReport = func(result statusEndpointClaimResult) {
			secondCalls++
			secondResult = result
		}
		require.NoError(t, second.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(second)

		claimKey := statusEndpointClaimTestKey("127.0.0.1:10080")
		value, lease := requireStatusEndpointKV(ctx, t, client, claimKey)
		require.Equal(t, "server-r", value)
		require.Equal(t, first.session.Lease(), lease)
		requireServerInfoKey(ctx, t, client, first.serverInfoPath)
		requireServerInfoKey(ctx, t, client, second.serverInfoPath)

		require.Equal(t, 1, firstCalls)
		require.Equal(t, statusEndpointClaimAcquired, firstResult.state)
		require.Equal(t, 1, secondCalls)
		require.Equal(t, statusEndpointClaimConflict, secondResult.state)
		require.Equal(t, "127.0.0.1:10080", secondResult.endpoint)
		require.Equal(t, claimKey, secondResult.claimKey)
		require.Equal(t, "server-o", secondResult.localID)
		require.Equal(t, "server-r", secondResult.existingID)
		require.Equal(t, first.session.Lease(), secondResult.existingLease)
	})

	t.Run("different endpoints acquire independent claims", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.2", 4010, 10081, true)
		first := NewSyncer("different-endpoint-a", func() uint64 { return 10 }, client, nil)
		require.NoError(t, first.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(first)

		setStatusEndpointTestConfig("127.0.0.2", 4011, 10082, true)
		second := NewSyncer("different-endpoint-b", func() uint64 { return 11 }, client, nil)
		require.NoError(t, second.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(second)

		value, lease := requireStatusEndpointKV(ctx, t, client, statusEndpointClaimTestKey("127.0.0.2:10081"))
		require.Equal(t, "different-endpoint-a", value)
		require.Equal(t, first.session.Lease(), lease)
		value, lease = requireStatusEndpointKV(ctx, t, client, statusEndpointClaimTestKey("127.0.0.2:10082"))
		require.Equal(t, "different-endpoint-b", value)
		require.Equal(t, second.session.Lease(), lease)
	})

	t.Run("concurrent registrations choose one claim holder", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.3", 4020, 10083, true)
		first := NewSyncer("concurrent-a", func() uint64 { return 20 }, client, nil)
		setStatusEndpointTestConfig("127.0.0.3", 4021, 10083, true)
		second := NewSyncer("concurrent-b", func() uint64 { return 21 }, client, nil)

		var firstResult, secondResult statusEndpointClaimResult
		firstCalls, secondCalls := 0, 0
		first.statusEndpointClaimReport = func(result statusEndpointClaimResult) {
			firstCalls++
			firstResult = result
		}
		second.statusEndpointClaimReport = func(result statusEndpointClaimResult) {
			secondCalls++
			secondResult = result
		}

		start := make(chan struct{})
		errCh := make(chan error, 2)
		go func() {
			<-start
			errCh <- first.NewSessionAndStoreServerInfo(ctx)
		}()
		go func() {
			<-start
			errCh <- second.NewSessionAndStoreServerInfo(ctx)
		}()
		close(start)
		require.NoError(t, <-errCh)
		require.NoError(t, <-errCh)
		defer orphanSyncerSession(first)
		defer orphanSyncerSession(second)

		require.Equal(t, 1, firstCalls)
		require.Equal(t, 1, secondCalls)
		states := []statusEndpointClaimState{firstResult.state, secondResult.state}
		require.ElementsMatch(t,
			[]statusEndpointClaimState{statusEndpointClaimAcquired, statusEndpointClaimConflict},
			states,
		)

		claimKey := statusEndpointClaimTestKey("127.0.0.3:10083")
		value, lease := requireStatusEndpointKV(ctx, t, client, claimKey)
		switch value {
		case "concurrent-a":
			require.Equal(t, first.session.Lease(), lease)
			require.Equal(t, "concurrent-b", secondResult.localID)
			require.Equal(t, "concurrent-a", secondResult.existingID)
		case "concurrent-b":
			require.Equal(t, second.session.Lease(), lease)
			require.Equal(t, "concurrent-a", firstResult.localID)
			require.Equal(t, "concurrent-b", firstResult.existingID)
		default:
			require.Failf(t, "unexpected claim holder", "value: %s", value)
		}
		requireServerInfoKey(ctx, t, client, first.serverInfoPath)
		requireServerInfoKey(ctx, t, client, second.serverInfoPath)
	})

	t.Run("same ID restart reattaches the claim to the new lease", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.4", 4030, 10084, true)
		syncer := NewSyncer("restart-same-id", func() uint64 { return 30 }, client, nil)
		results := make([]statusEndpointClaimResult, 0, 2)
		syncer.statusEndpointClaimReport = func(result statusEndpointClaimResult) {
			results = append(results, result)
		}
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
		oldSession := syncer.session
		oldLease := oldSession.Lease()
		defer oldSession.Orphan()

		require.NoError(t, syncer.Restart(ctx))
		newSession := syncer.session
		defer newSession.Orphan()
		require.NotEqual(t, oldLease, newSession.Lease())

		value, lease := requireStatusEndpointKV(ctx, t, client, syncer.statusEndpointClaimKey)
		require.Equal(t, "restart-same-id", value)
		require.Equal(t, newSession.Lease(), lease)
		require.Len(t, results, 2)
		require.Equal(t, statusEndpointClaimAcquired, results[0].state)
		require.Equal(t, statusEndpointClaimAcquired, results[1].state)

		serverInfoValue, serverInfoLease := requireStatusEndpointKV(ctx, t, client, syncer.serverInfoPath)
		require.NotEmpty(t, serverInfoValue)
		require.Equal(t, newSession.Lease(), serverInfoLease)
	})

	t.Run("reattach race reports the current claim without blind overwrite", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.5", 4040, 10085, true)
		first := NewSyncer("reattach-race", func() uint64 { return 40 }, client, nil)
		require.NoError(t, first.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(first)

		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, faultClient.Close()) }()
		faultKV := &statusEndpointFaultKV{
			KV:           faultClient.KV,
			beforeCommit: make(map[int]func()),
		}
		faultClient.KV = faultKV

		setStatusEndpointTestConfig("127.0.0.5", 4041, 10085, true)
		second := NewSyncer("reattach-race", func() uint64 { return 41 }, faultClient, nil)
		newGeneration, err := client.Grant(ctx, util.SessionTTL)
		require.NoError(t, err)
		defer func() {
			_, revokeErr := client.Revoke(ctx, newGeneration.ID)
			require.NoError(t, revokeErr)
		}()
		faultKV.beforeCommit[2] = func() {
			_, putErr := client.Put(ctx, second.statusEndpointClaimKey, "reattach-race",
				clientv3.WithLease(newGeneration.ID))
			require.NoError(t, putErr)
		}
		var result statusEndpointClaimResult
		calls := 0
		second.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
			calls++
			result = got
		}
		require.NoError(t, second.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(second)

		require.Equal(t, 1, calls)
		require.Equal(t, statusEndpointClaimCheckFailed, result.state)
		require.Equal(t, "reattach-race", result.localID)
		require.Equal(t, "reattach-race", result.existingID)
		require.Equal(t, newGeneration.ID, result.existingLease)
		require.ErrorContains(t, result.err, "claim changed while reattaching")
		value, lease := requireStatusEndpointKV(ctx, t, client, second.statusEndpointClaimKey)
		require.Equal(t, "reattach-race", value)
		require.Equal(t, newGeneration.ID, lease)
		require.Equal(t, 3, faultKV.transactionCount())
		requireServerInfoKey(ctx, t, client, second.serverInfoPath)
	})

	t.Run("graceful removal releases only the owner claim", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.6", 4050, 10086, true)
		holder := NewSyncer("graceful-holder", func() uint64 { return 50 }, client, nil)
		require.NoError(t, holder.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(holder)
		claimKey := holder.statusEndpointClaimKey

		holder.RemoveServerInfo()
		requireEtcdKeyAbsent(ctx, t, client, claimKey)
		requireEtcdKeyAbsent(ctx, t, client, holder.serverInfoPath)

		setStatusEndpointTestConfig("127.0.0.6", 4051, 10086, true)
		replacement := NewSyncer("graceful-replacement", func() uint64 { return 51 }, client, nil)
		var result statusEndpointClaimResult
		replacement.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
			result = got
		}
		require.NoError(t, replacement.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(replacement)
		require.Equal(t, statusEndpointClaimAcquired, result.state)
		value, lease := requireStatusEndpointKV(ctx, t, client, claimKey)
		require.Equal(t, "graceful-replacement", value)
		require.Equal(t, replacement.session.Lease(), lease)
	})

	t.Run("loser removal leaves the winner claim", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.7", 4060, 10087, true)
		holder := NewSyncer("loser-cleanup-holder", func() uint64 { return 60 }, client, nil)
		require.NoError(t, holder.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(holder)

		setStatusEndpointTestConfig("127.0.0.7", 4061, 10087, true)
		loser := NewSyncer("loser-cleanup-loser", func() uint64 { return 61 }, client, nil)
		require.NoError(t, loser.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(loser)

		loser.RemoveServerInfo()
		value, lease := requireStatusEndpointKV(ctx, t, client, holder.statusEndpointClaimKey)
		require.Equal(t, "loser-cleanup-holder", value)
		require.Equal(t, holder.session.Lease(), lease)
		requireEtcdKeyAbsent(ctx, t, client, loser.serverInfoPath)
	})

	t.Run("old same-ID generation cannot remove the new claim", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.8", 4070, 10088, true)
		syncer := NewSyncer("same-id-cleanup", func() uint64 { return 70 }, client, nil)
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
		oldSession := syncer.session
		oldLease := oldSession.Lease()
		defer oldSession.Orphan()

		require.NoError(t, syncer.Restart(ctx))
		newSession := syncer.session
		defer newSession.Orphan()
		require.NotEqual(t, oldLease, newSession.Lease())

		require.NoError(t, syncer.removeStatusEndpointClaim(ctx, oldLease))
		value, lease := requireStatusEndpointKV(ctx, t, client, syncer.statusEndpointClaimKey)
		require.Equal(t, "same-id-cleanup", value)
		require.Equal(t, newSession.Lease(), lease)
	})

	t.Run("lease revoke removes the claim and server info together", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.9", 4080, 10089, true)
		syncer := NewSyncer("lease-revoke", func() uint64 { return 80 }, client, nil)
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(syncer)

		lease := syncer.session.Lease()
		ttl, err := client.TimeToLive(ctx, lease)
		require.NoError(t, err)
		require.Equal(t, int64(util.SessionTTL), ttl.GrantedTTL)
		_, err = client.Revoke(ctx, lease)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			claimResp, claimErr := client.Get(ctx, syncer.statusEndpointClaimKey)
			infoResp, infoErr := client.Get(ctx, syncer.serverInfoPath)
			return claimErr == nil && infoErr == nil && len(claimResp.Kvs) == 0 && len(infoResp.Kvs) == 0
		}, 5*time.Second, 20*time.Millisecond)
	})

	t.Run("namespaced primary clients claim independently", func(t *testing.T) {
		firstClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, firstClient.Close()) }()
		secondClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, secondClient.Close()) }()
		etcd.SetEtcdCliByNamespace(firstClient, "keyspace-a/")
		etcd.SetEtcdCliByNamespace(secondClient, "keyspace-b/")

		setStatusEndpointTestConfig("127.0.0.10", 4090, 10090, true)
		first := NewSyncer("namespaced-a", func() uint64 { return 90 }, firstClient, nil)
		var firstResult statusEndpointClaimResult
		first.statusEndpointClaimReport = func(result statusEndpointClaimResult) {
			firstResult = result
		}
		require.NoError(t, first.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(first)

		setStatusEndpointTestConfig("127.0.0.10", 4091, 10090, true)
		second := NewSyncer("namespaced-b", func() uint64 { return 91 }, secondClient, nil)
		var secondResult statusEndpointClaimResult
		second.statusEndpointClaimReport = func(result statusEndpointClaimResult) {
			secondResult = result
		}
		require.NoError(t, second.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(second)

		require.Equal(t, statusEndpointClaimAcquired, firstResult.state)
		require.Equal(t, statusEndpointClaimAcquired, secondResult.state)
		value, lease := requireStatusEndpointKV(ctx, t, firstClient, first.statusEndpointClaimKey)
		require.Equal(t, "namespaced-a", value)
		require.Equal(t, first.session.Lease(), lease)
		value, lease = requireStatusEndpointKV(ctx, t, secondClient, second.statusEndpointClaimKey)
		require.Equal(t, "namespaced-b", value)
		require.Equal(t, second.session.Lease(), lease)
	})

	t.Run("CrossKS registration stores server info without a claim", func(t *testing.T) {
		crossClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, crossClient.Close()) }()
		etcd.SetEtcdCliByNamespace(crossClient, "cross-keyspace/")

		setStatusEndpointTestConfig("127.0.0.11", 4100, 10091, true)
		syncer := NewCrossKSSyncer("cross-virtual", func() uint64 { return 100 }, crossClient, nil, "target-ks")
		var result statusEndpointClaimResult
		calls := 0
		syncer.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
			calls++
			result = got
		}
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(syncer)

		require.True(t, syncer.info.Load().IsAssumed())
		require.Empty(t, syncer.statusEndpoint)
		require.Empty(t, syncer.statusEndpointClaimKey)
		require.Equal(t, 1, calls)
		require.Equal(t, statusEndpointClaimSkipped, result.state)
		requireServerInfoKey(ctx, t, crossClient, syncer.serverInfoPath)
		resp, err := crossClient.Get(ctx, serverStatusAddressPath, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})

	t.Run("invalid status services skip the claim but keep registration", func(t *testing.T) {
		tests := []struct {
			name         string
			host         string
			statusPort   uint
			reportStatus bool
			sqlPort      uint
		}{
			{name: "report status disabled", host: "127.0.0.12", statusPort: 10092, reportStatus: false, sqlPort: 4110},
			{name: "empty advertised host", statusPort: 10093, reportStatus: true, sqlPort: 4111},
			{name: "zero status port", host: "127.0.0.13", reportStatus: true, sqlPort: 4112},
		}
		for i, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				setStatusEndpointTestConfig(test.host, test.sqlPort, test.statusPort, test.reportStatus)
				id := fmt.Sprintf("skip-claim-%d", i)
				syncer := NewSyncer(id, func() uint64 { return uint64(110 + i) }, client, nil)
				var result statusEndpointClaimResult
				calls := 0
				syncer.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
					calls++
					result = got
				}
				require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
				defer orphanSyncerSession(syncer)

				require.Empty(t, syncer.statusEndpointClaimKey)
				require.Equal(t, 1, calls)
				require.Equal(t, statusEndpointClaimSkipped, result.state)
				requireServerInfoKey(ctx, t, client, syncer.serverInfoPath)
			})
		}
	})

	t.Run("nil etcd client keeps the existing no-op behavior", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.14", 4120, 10094, true)
		syncer := NewSyncer("nil-etcd", func() uint64 { return 120 }, nil, nil)
		calls := 0
		syncer.statusEndpointClaimReport = func(statusEndpointClaimResult) {
			calls++
		}
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
		require.Equal(t, 0, calls)
		require.Nil(t, syncer.session)
	})

	t.Run("claim check error does not block server info registration", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, faultClient.Close()) }()
		faultClient.KV = &statusEndpointFaultKV{
			KV:        faultClient.KV,
			txnFaults: []statusEndpointTxnFault{statusEndpointTxnFailBeforeCommit},
		}

		setStatusEndpointTestConfig("127.0.0.15", 4130, 10095, true)
		syncer := NewSyncer("claim-check-error", func() uint64 { return 130 }, faultClient, nil)
		var result statusEndpointClaimResult
		calls := 0
		syncer.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
			calls++
			result = got
		}
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(syncer)

		require.Equal(t, 1, calls)
		require.Equal(t, statusEndpointClaimCheckFailed, result.state)
		require.ErrorIs(t, result.err, errStatusEndpointTxnFault)
		requireEtcdKeyAbsent(ctx, t, client, syncer.statusEndpointClaimKey)
		requireServerInfoKey(ctx, t, client, syncer.serverInfoPath)
	})

	t.Run("unknown claim outcome is cleaned after server info failure", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, faultClient.Close()) }()
		storeErr := errors.New("injected server info put failure")
		faultKV := &statusEndpointFaultKV{
			KV:        faultClient.KV,
			txnFaults: []statusEndpointTxnFault{statusEndpointTxnFailAfterCommit},
			putErr:    storeErr,
		}
		faultClient.KV = faultKV

		setStatusEndpointTestConfig("127.0.0.16", 4140, 10096, true)
		syncer := NewSyncer("unknown-outcome", func() uint64 { return 140 }, faultClient, nil)
		faultKV.failPutKey = syncer.serverInfoPath
		var result statusEndpointClaimResult
		syncer.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
			result = got
		}

		err = syncer.NewSessionAndStoreServerInfo(ctx)
		require.ErrorIs(t, err, storeErr)
		require.Equal(t, statusEndpointClaimCheckFailed, result.state)
		require.ErrorIs(t, result.err, errStatusEndpointTxnFault)
		requireSessionDone(t, syncer)
		require.Eventually(t, func() bool {
			resp, getErr := client.Get(ctx, syncer.statusEndpointClaimKey)
			return getErr == nil && len(resp.Kvs) == 0
		}, 5*time.Second, 20*time.Millisecond)
		requireEtcdKeyAbsent(ctx, t, client, syncer.serverInfoPath)

		setStatusEndpointTestConfig("127.0.0.16", 4141, 10096, true)
		replacement := NewSyncer("unknown-outcome-replacement", func() uint64 { return 141 }, client, nil)
		var replacementResult statusEndpointClaimResult
		replacement.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
			replacementResult = got
		}
		require.NoError(t, replacement.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(replacement)
		require.Equal(t, statusEndpointClaimAcquired, replacementResult.state)
		value, lease := requireStatusEndpointKV(ctx, t, client, replacement.statusEndpointClaimKey)
		require.Equal(t, "unknown-outcome-replacement", value)
		require.Equal(t, replacement.session.Lease(), lease)
	})

	t.Run("failed conflict registration cannot remove the winner claim", func(t *testing.T) {
		setStatusEndpointTestConfig("127.0.0.17", 4150, 10097, true)
		holder := NewSyncer("failed-conflict-holder", func() uint64 { return 150 }, client, nil)
		require.NoError(t, holder.NewSessionAndStoreServerInfo(ctx))
		defer orphanSyncerSession(holder)

		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, faultClient.Close()) }()
		storeErr := errors.New("injected conflict server info put failure")
		faultKV := &statusEndpointFaultKV{KV: faultClient.KV, putErr: storeErr}
		faultClient.KV = faultKV

		setStatusEndpointTestConfig("127.0.0.17", 4151, 10097, true)
		loser := NewSyncer("failed-conflict-loser", func() uint64 { return 151 }, faultClient, nil)
		faultKV.failPutKey = loser.serverInfoPath
		var result statusEndpointClaimResult
		loser.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
			result = got
		}

		err = loser.NewSessionAndStoreServerInfo(ctx)
		require.ErrorIs(t, err, storeErr)
		require.Equal(t, statusEndpointClaimConflict, result.state)
		requireSessionDone(t, loser)
		value, lease := requireStatusEndpointKV(ctx, t, client, holder.statusEndpointClaimKey)
		require.Equal(t, "failed-conflict-holder", value)
		require.Equal(t, holder.session.Lease(), lease)
		requireEtcdKeyAbsent(ctx, t, client, loser.serverInfoPath)
	})

	t.Run("failed registration cleanup remains bounded when etcd operations fail", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, faultClient.Close()) }()
		storeErr := errors.New("injected bounded cleanup store failure")
		cleanupErr := errors.New("injected cleanup failure")
		faultKV := &statusEndpointFaultKV{
			KV: faultClient.KV,
			txnFaults: []statusEndpointTxnFault{
				statusEndpointTxnNoFault,
				statusEndpointTxnWaitForContext,
			},
			putErr: storeErr,
		}
		faultClient.KV = faultKV
		faultLease := &statusEndpointFaultLease{
			Lease:     faultClient.Lease,
			revokeErr: cleanupErr,
		}
		faultClient.Lease = faultLease

		setStatusEndpointTestConfig("127.0.0.18", 4160, 10098, true)
		syncer := NewSyncer("bounded-cleanup", func() uint64 { return 160 }, faultClient, nil)
		faultKV.failPutKey = syncer.serverInfoPath
		var result statusEndpointClaimResult
		syncer.statusEndpointClaimReport = func(got statusEndpointClaimResult) {
			result = got
		}

		start := time.Now()
		err = syncer.NewSessionAndStoreServerInfo(ctx)
		elapsed := time.Since(start)
		require.ErrorIs(t, err, storeErr)
		require.GreaterOrEqual(t, elapsed, 900*time.Millisecond)
		require.Less(t, elapsed, 2*time.Second)
		require.Equal(t, 2, faultKV.transactionCount())
		require.Equal(t, statusEndpointClaimAcquired, result.state)
		requireSessionDone(t, syncer)
		revokeCalls := faultLease.revokeCallsSnapshot()
		require.Len(t, revokeCalls, 1)
		require.True(t, revokeCalls[0].hasDeadline)
		require.GreaterOrEqual(t, revokeCalls[0].deadline.Sub(start), 900*time.Millisecond)
		require.Less(t, revokeCalls[0].deadline.Sub(start), 2*time.Second)

		value, lease := requireStatusEndpointKV(ctx, t, client, syncer.statusEndpointClaimKey)
		require.Equal(t, "bounded-cleanup", value)
		require.Equal(t, syncer.session.Lease(), lease)
		requireEtcdKeyAbsent(ctx, t, client, syncer.serverInfoPath)

		_, err = client.Revoke(ctx, lease)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			resp, getErr := client.Get(ctx, syncer.statusEndpointClaimKey)
			return getErr == nil && len(resp.Kvs) == 0
		}, 5*time.Second, 20*time.Millisecond)
	})

	t.Run("parent cancellation produces no misleading claim warning", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, faultClient.Close()) }()
		started := make(chan struct{})
		faultClient.KV = &statusEndpointFaultKV{
			KV:        faultClient.KV,
			txnFaults: []statusEndpointTxnFault{statusEndpointTxnWaitForContext},
			started:   started,
		}

		setStatusEndpointTestConfig("127.0.0.19", 4170, 10099, true)
		syncer := NewSyncer("parent-cancel", func() uint64 { return 170 }, faultClient, nil)
		calls := 0
		syncer.statusEndpointClaimReport = func(statusEndpointClaimResult) {
			calls++
		}
		cancelCtx, cancelClaim := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- syncer.NewSessionAndStoreServerInfo(cancelCtx)
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "claim transaction did not start")
		}
		cancelClaim()
		err = <-errCh

		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 0, calls)
		requireSessionDone(t, syncer)
		requireEtcdKeyAbsent(ctx, t, client, syncer.statusEndpointClaimKey)
		requireEtcdKeyAbsent(ctx, t, client, syncer.serverInfoPath)
	})

	t.Run("server info sync loop backs off after restart failures and exits", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		defer func() { require.NoError(t, faultClient.Close()) }()
		faultLease := &statusEndpointFaultLease{
			Lease:       faultClient.Lease,
			grantEvents: make(chan time.Time, 8),
		}
		faultClient.Lease = faultLease

		setStatusEndpointTestConfig("127.0.0.20", 4180, 10100, false)
		syncer := NewSyncer("restart-backoff", func() uint64 { return 180 }, faultClient, nil)
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
		syncer.session.Orphan()
		requireSessionDone(t, syncer)

		faultLease.enableGrantFailure(errors.New("injected session restart failure"))
		exitCh := make(chan struct{})
		loopDone := make(chan struct{})
		go func() {
			syncer.ServerInfoSyncLoop(nil, exitCh)
			close(loopDone)
		}()

		grantTimes := make([]time.Time, 0, 4)
		collectTimeout := time.NewTimer(5 * time.Second)
	collectGrantEvents:
		for len(grantTimes) < 4 {
			select {
			case grantTime := <-faultLease.grantEvents:
				grantTimes = append(grantTimes, grantTime)
			case <-collectTimeout.C:
				break collectGrantEvents
			}
		}
		if !collectTimeout.Stop() {
			select {
			case <-collectTimeout.C:
			default:
			}
		}
		close(exitCh)

		loopExited := false
		select {
		case <-loopDone:
			loopExited = true
		case <-time.After(5 * time.Second):
		}
		require.True(t, loopExited)
		require.Len(t, grantTimes, 4)
		restartGap := grantTimes[3].Sub(grantTimes[2])
		require.GreaterOrEqual(t, restartGap, 900*time.Millisecond)
		require.Less(t, restartGap, 2500*time.Millisecond)
		require.Equal(t, 6, faultLease.grantFailureCount())
	})
}

func statusEndpointClaimTestKey(endpoint string) string {
	return "/tidb/server/status_addr/" + base64.RawURLEncoding.EncodeToString([]byte(endpoint))
}

func setStatusEndpointTestConfig(host string, sqlPort, statusPort uint, reportStatus bool) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.AdvertiseAddress = host
		conf.Port = sqlPort
		conf.Status.ReportStatus = reportStatus
		conf.Status.StatusPort = statusPort
	})
}

func requireStatusEndpointKV(
	ctx context.Context,
	t *testing.T,
	client *clientv3.Client,
	key string,
) (string, clientv3.LeaseID) {
	t.Helper()
	resp, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	return string(resp.Kvs[0].Value), clientv3.LeaseID(resp.Kvs[0].Lease)
}

func requireEtcdKeyAbsent(ctx context.Context, t *testing.T, client *clientv3.Client, key string) {
	t.Helper()
	resp, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Empty(t, resp.Kvs)
}

func requireServerInfoKey(ctx context.Context, t *testing.T, client *clientv3.Client, key string) {
	t.Helper()
	resp, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
}

func orphanSyncerSession(syncer *Syncer) {
	if syncer.session != nil {
		syncer.session.Orphan()
	}
}

func requireSessionDone(t *testing.T, syncer *Syncer) {
	t.Helper()
	select {
	case <-syncer.session.Done():
	default:
		require.Fail(t, "server info session is still running")
	}
}

type statusEndpointTxnFault int

const (
	statusEndpointTxnNoFault statusEndpointTxnFault = iota
	statusEndpointTxnFailBeforeCommit
	statusEndpointTxnFailAfterCommit
	statusEndpointTxnWaitForContext
)

var errStatusEndpointTxnFault = errors.New("injected status endpoint transaction failure")

type statusEndpointFaultKV struct {
	clientv3.KV

	mu           sync.Mutex
	txnFaults    []statusEndpointTxnFault
	txnCount     int
	beforeCommit map[int]func()
	failPutKey   string
	putErr       error
	started      chan struct{}
	startOnce    sync.Once
}

func (kv *statusEndpointFaultKV) Put(
	ctx context.Context,
	key, value string,
	opts ...clientv3.OpOption,
) (*clientv3.PutResponse, error) {
	if key == kv.failPutKey && kv.putErr != nil {
		return nil, kv.putErr
	}
	return kv.KV.Put(ctx, key, value, opts...)
}

func (kv *statusEndpointFaultKV) Txn(ctx context.Context) clientv3.Txn {
	kv.mu.Lock()
	kv.txnCount++
	txnNumber := kv.txnCount
	fault := statusEndpointTxnNoFault
	if len(kv.txnFaults) > 0 {
		fault = kv.txnFaults[0]
		kv.txnFaults = kv.txnFaults[1:]
	}
	beforeCommit := kv.beforeCommit[txnNumber]
	kv.mu.Unlock()
	return &statusEndpointFaultTxn{
		Txn:          kv.KV.Txn(ctx),
		ctx:          ctx,
		fault:        fault,
		beforeCommit: beforeCommit,
		signalStarted: func() {
			if kv.started != nil {
				kv.startOnce.Do(func() { close(kv.started) })
			}
		},
	}
}

func (kv *statusEndpointFaultKV) transactionCount() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.txnCount
}

type statusEndpointFaultTxn struct {
	clientv3.Txn

	ctx           context.Context
	fault         statusEndpointTxnFault
	beforeCommit  func()
	signalStarted func()
}

func (txn *statusEndpointFaultTxn) If(cmps ...clientv3.Cmp) clientv3.Txn {
	txn.Txn = txn.Txn.If(cmps...)
	return txn
}

func (txn *statusEndpointFaultTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Then(ops...)
	return txn
}

func (txn *statusEndpointFaultTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Else(ops...)
	return txn
}

func (txn *statusEndpointFaultTxn) Commit() (*clientv3.TxnResponse, error) {
	if txn.beforeCommit != nil {
		txn.beforeCommit()
	}
	switch txn.fault {
	case statusEndpointTxnFailBeforeCommit:
		return nil, errStatusEndpointTxnFault
	case statusEndpointTxnFailAfterCommit:
		resp, err := txn.Txn.Commit()
		if err != nil {
			return resp, err
		}
		return nil, errStatusEndpointTxnFault
	case statusEndpointTxnWaitForContext:
		txn.signalStarted()
		<-txn.ctx.Done()
		return nil, txn.ctx.Err()
	default:
		return txn.Txn.Commit()
	}
}

type statusEndpointFaultLease struct {
	clientv3.Lease

	mu            sync.Mutex
	revokeErr     error
	revokeCalls   []statusEndpointRevokeCall
	failGrant     bool
	grantErr      error
	grantEvents   chan time.Time
	grantFailures int
}

func (lease *statusEndpointFaultLease) Revoke(
	ctx context.Context,
	id clientv3.LeaseID,
) (*clientv3.LeaseRevokeResponse, error) {
	deadline, hasDeadline := ctx.Deadline()
	lease.mu.Lock()
	lease.revokeCalls = append(lease.revokeCalls, statusEndpointRevokeCall{
		deadline:    deadline,
		hasDeadline: hasDeadline,
	})
	revokeErr := lease.revokeErr
	lease.mu.Unlock()
	if revokeErr != nil {
		return nil, revokeErr
	}
	return lease.Lease.Revoke(ctx, id)
}

func (lease *statusEndpointFaultLease) Grant(
	ctx context.Context,
	ttl int64,
) (*clientv3.LeaseGrantResponse, error) {
	lease.mu.Lock()
	if !lease.failGrant {
		lease.mu.Unlock()
		return lease.Lease.Grant(ctx, ttl)
	}
	grantErr := lease.grantErr
	grantEvents := lease.grantEvents
	lease.grantFailures++
	lease.mu.Unlock()
	if grantEvents != nil {
		grantEvents <- time.Now()
	}
	return nil, grantErr
}

func (lease *statusEndpointFaultLease) enableGrantFailure(err error) {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	lease.failGrant = true
	lease.grantErr = err
}

func (lease *statusEndpointFaultLease) grantFailureCount() int {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.grantFailures
}

func (lease *statusEndpointFaultLease) revokeCallsSnapshot() []statusEndpointRevokeCall {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return append([]statusEndpointRevokeCall(nil), lease.revokeCalls...)
}

type statusEndpointRevokeCall struct {
	deadline    time.Time
	hasDeadline bool
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
