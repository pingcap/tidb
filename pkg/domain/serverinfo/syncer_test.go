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
	// Verify endpoint normalization, claim-key encoding, and cases that should not create a claim.
	tests := []struct {
		name             string
		host             string
		statusPort       uint
		reportStatus     bool
		assumedKeyspace  string
		expectedEndpoint string
	}{
		{name: "IPv4", host: " 127.0.0.1 ", statusPort: 10080, reportStatus: true, expectedEndpoint: "127.0.0.1:10080"},
		{name: "expanded IPv6", host: "2001:0db8:0000:0000:0000:0000:0000:0001", statusPort: 10080, reportStatus: true, expectedEndpoint: "[2001:db8::1]:10080"},
		{name: "compressed IPv6", host: "2001:db8::1", statusPort: 10080, reportStatus: true, expectedEndpoint: "[2001:db8::1]:10080"},
		{name: "DNS case and root dot", host: "DB.Example.COM.", statusPort: 10080, reportStatus: true, expectedEndpoint: "db.example.com:10080"},
		{name: "different DNS name", host: "db-b.example.com", statusPort: 10080, reportStatus: true, expectedEndpoint: "db-b.example.com:10080"},
		{name: "different port", host: "db.example.com", statusPort: 10081, reportStatus: true, expectedEndpoint: "db.example.com:10081"},
		{name: "special hostname text", host: "db/name", statusPort: 10080, reportStatus: true, expectedEndpoint: "db/name:10080"},
		{name: "status reporting disabled", host: "127.0.0.1", statusPort: 10080},
		{name: "assumed keyspace", host: "127.0.0.1", statusPort: 10080, reportStatus: true, assumedKeyspace: "ks1"},
		{name: "empty host", statusPort: 10080, reportStatus: true},
		{name: "zero status port uses production default", host: "127.0.0.1", reportStatus: true, expectedEndpoint: "127.0.0.1:10080"},
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
	require.NotEqual(t, keys["DNS case and root dot"], keys["different DNS name"])
	require.NotEqual(t, keys["DNS case and root dot"], keys["different port"])
}

func TestStatusEndpointClaim(t *testing.T) {
	// Verify claim ownership remains safe across conflicts, restarts, cleanup, namespaces, and failures.
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
		first, firstRecorder := startStatusEndpointTestSyncer(
			ctx, t, client, "server-r", "127.0.0.1", 4000, 10080, true,
		)
		second, secondRecorder := startStatusEndpointTestSyncer(
			ctx, t, client, "server-o", "127.0.0.1", 4001, 10080, true,
		)

		claimKey := statusEndpointClaimTestKey("127.0.0.1:10080")
		value, lease := requireStatusEndpointKV(ctx, t, client, claimKey)
		require.Equal(t, "server-r", value)
		require.Equal(t, first.session.Lease(), lease)
		requireServerInfoKey(ctx, t, client, first.serverInfoPath)
		requireServerInfoKey(ctx, t, client, second.serverInfoPath)

		firstRecorder.requireSingle(t, statusEndpointClaimAcquired)
		secondResult := secondRecorder.requireSingle(t, statusEndpointClaimConflict)
		require.Equal(t, "127.0.0.1:10080", secondResult.endpoint)
		require.Equal(t, claimKey, secondResult.claimKey)
		require.Equal(t, "server-o", secondResult.localID)
		require.Equal(t, "server-r", secondResult.existingID)
		require.Equal(t, first.session.Lease(), secondResult.existingLease)
	})

	t.Run("different endpoints acquire independent claims", func(t *testing.T) {
		first, firstRecorder := startStatusEndpointTestSyncer(
			ctx, t, client, "different-endpoint-a", "127.0.0.2", 4010, 10081, true,
		)
		second, secondRecorder := startStatusEndpointTestSyncer(
			ctx, t, client, "different-endpoint-b", "127.0.0.2", 4011, 10082, true,
		)
		firstRecorder.requireSingle(t, statusEndpointClaimAcquired)
		secondRecorder.requireSingle(t, statusEndpointClaimAcquired)

		value, lease := requireStatusEndpointKV(ctx, t, client, statusEndpointClaimTestKey("127.0.0.2:10081"))
		require.Equal(t, "different-endpoint-a", value)
		require.Equal(t, first.session.Lease(), lease)
		value, lease = requireStatusEndpointKV(ctx, t, client, statusEndpointClaimTestKey("127.0.0.2:10082"))
		require.Equal(t, "different-endpoint-b", value)
		require.Equal(t, second.session.Lease(), lease)
	})

	t.Run("concurrent registrations choose one claim holder", func(t *testing.T) {
		first, firstRecorder := newStatusEndpointTestSyncer(
			t, client, "concurrent-a", "127.0.0.3", 4020, 10083, true,
		)
		second, secondRecorder := newStatusEndpointTestSyncer(
			t, client, "concurrent-b", "127.0.0.3", 4021, 10083, true,
		)

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

		firstResult := firstRecorder.single(t)
		secondResult := secondRecorder.single(t)
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
		syncer, recorder := startStatusEndpointTestSyncer(
			ctx, t, client, "restart-same-id", "127.0.0.4", 4030, 10084, true,
		)
		oldSession := syncer.session
		oldLease := oldSession.Lease()
		defer oldSession.Orphan()

		require.NoError(t, syncer.Restart(ctx))
		newSession := syncer.session
		require.NotEqual(t, oldLease, newSession.Lease())

		value, lease := requireStatusEndpointKV(ctx, t, client, syncer.endpointClaim.key)
		require.Equal(t, "restart-same-id", value)
		require.Equal(t, newSession.Lease(), lease)
		require.Len(t, recorder.results, 2)
		require.Equal(t, statusEndpointClaimAcquired, recorder.results[0].state)
		require.Equal(t, statusEndpointClaimAcquired, recorder.results[1].state)

		serverInfoValue, serverInfoLease := requireStatusEndpointKV(ctx, t, client, syncer.serverInfoPath)
		require.NotEmpty(t, serverInfoValue)
		require.Equal(t, newSession.Lease(), serverInfoLease)
	})

	t.Run("reattach race reports the current claim without blind overwrite", func(t *testing.T) {
		startStatusEndpointTestSyncer(
			ctx, t, client, "reattach-race", "127.0.0.5", 4040, 10085, true,
		)

		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, faultClient.Close()) })
		faultKV := &statusEndpointFaultKV{
			KV:           faultClient.KV,
			beforeCommit: make(map[int]func()),
		}
		faultClient.KV = faultKV

		second, recorder := newStatusEndpointTestSyncer(
			t, faultClient, "reattach-race", "127.0.0.5", 4041, 10085, true,
		)
		newGeneration, err := client.Grant(ctx, util.SessionTTL)
		require.NoError(t, err)
		defer func() {
			_, revokeErr := client.Revoke(ctx, newGeneration.ID)
			require.NoError(t, revokeErr)
		}()
		faultKV.beforeCommit[2] = func() {
			_, putErr := client.Put(ctx, second.endpointClaim.key, "reattach-race",
				clientv3.WithLease(newGeneration.ID))
			require.NoError(t, putErr)
		}
		require.NoError(t, second.NewSessionAndStoreServerInfo(ctx))

		result := recorder.requireSingle(t, statusEndpointClaimCheckFailed)
		require.Equal(t, "reattach-race", result.localID)
		require.Equal(t, "reattach-race", result.existingID)
		require.Equal(t, newGeneration.ID, result.existingLease)
		require.ErrorContains(t, result.err, "claim changed while reattaching")
		value, lease := requireStatusEndpointKV(ctx, t, client, second.endpointClaim.key)
		require.Equal(t, "reattach-race", value)
		require.Equal(t, newGeneration.ID, lease)
		require.Equal(t, 3, faultKV.transactionCount())
		requireServerInfoKey(ctx, t, client, second.serverInfoPath)
	})

	t.Run("graceful removal releases only the owner claim", func(t *testing.T) {
		holder, _ := startStatusEndpointTestSyncer(
			ctx, t, client, "graceful-holder", "127.0.0.6", 4050, 10086, true,
		)
		claimKey := holder.endpointClaim.key

		holder.RemoveServerInfo()
		requireEtcdKeyAbsent(ctx, t, client, claimKey)
		requireEtcdKeyAbsent(ctx, t, client, holder.serverInfoPath)

		replacement, recorder := startStatusEndpointTestSyncer(
			ctx, t, client, "graceful-replacement", "127.0.0.6", 4051, 10086, true,
		)
		recorder.requireSingle(t, statusEndpointClaimAcquired)
		value, lease := requireStatusEndpointKV(ctx, t, client, claimKey)
		require.Equal(t, "graceful-replacement", value)
		require.Equal(t, replacement.session.Lease(), lease)
	})

	t.Run("loser removal leaves the winner claim", func(t *testing.T) {
		holder, _ := startStatusEndpointTestSyncer(
			ctx, t, client, "loser-cleanup-holder", "127.0.0.7", 4060, 10087, true,
		)
		loser, recorder := startStatusEndpointTestSyncer(
			ctx, t, client, "loser-cleanup-loser", "127.0.0.7", 4061, 10087, true,
		)
		recorder.requireSingle(t, statusEndpointClaimConflict)

		loser.RemoveServerInfo()
		value, lease := requireStatusEndpointKV(ctx, t, client, holder.endpointClaim.key)
		require.Equal(t, "loser-cleanup-holder", value)
		require.Equal(t, holder.session.Lease(), lease)
		requireEtcdKeyAbsent(ctx, t, client, loser.serverInfoPath)
	})

	t.Run("old same-ID generation cannot remove the new claim", func(t *testing.T) {
		syncer, _ := startStatusEndpointTestSyncer(
			ctx, t, client, "same-id-cleanup", "127.0.0.8", 4070, 10088, true,
		)
		oldSession := syncer.session
		oldLease := oldSession.Lease()
		defer oldSession.Orphan()

		require.NoError(t, syncer.Restart(ctx))
		newSession := syncer.session
		require.NotEqual(t, oldLease, newSession.Lease())

		require.NoError(t, syncer.endpointClaim.remove(ctx, oldLease))
		value, lease := requireStatusEndpointKV(ctx, t, client, syncer.endpointClaim.key)
		require.Equal(t, "same-id-cleanup", value)
		require.Equal(t, newSession.Lease(), lease)
	})

	t.Run("lease revoke removes the claim and server info together", func(t *testing.T) {
		syncer, _ := startStatusEndpointTestSyncer(
			ctx, t, client, "lease-revoke", "127.0.0.9", 4080, 10089, true,
		)

		lease := syncer.session.Lease()
		ttl, err := client.TimeToLive(ctx, lease)
		require.NoError(t, err)
		require.Equal(t, int64(util.SessionTTL), ttl.GrantedTTL)
		_, err = client.Revoke(ctx, lease)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			claimResp, claimErr := client.Get(ctx, syncer.endpointClaim.key)
			infoResp, infoErr := client.Get(ctx, syncer.serverInfoPath)
			return claimErr == nil && infoErr == nil && len(claimResp.Kvs) == 0 && len(infoResp.Kvs) == 0
		}, 5*time.Second, 20*time.Millisecond)
	})

	t.Run("namespaced primary clients claim independently", func(t *testing.T) {
		firstClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, firstClient.Close()) })
		secondClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, secondClient.Close()) })
		etcd.SetEtcdCliByNamespace(firstClient, "keyspace-a/")
		etcd.SetEtcdCliByNamespace(secondClient, "keyspace-b/")

		first, firstRecorder := startStatusEndpointTestSyncer(
			ctx, t, firstClient, "namespaced-a", "127.0.0.10", 4090, 10090, true,
		)
		second, secondRecorder := startStatusEndpointTestSyncer(
			ctx, t, secondClient, "namespaced-b", "127.0.0.10", 4091, 10090, true,
		)

		firstRecorder.requireSingle(t, statusEndpointClaimAcquired)
		secondRecorder.requireSingle(t, statusEndpointClaimAcquired)
		value, lease := requireStatusEndpointKV(ctx, t, firstClient, first.endpointClaim.key)
		require.Equal(t, "namespaced-a", value)
		require.Equal(t, first.session.Lease(), lease)
		value, lease = requireStatusEndpointKV(ctx, t, secondClient, second.endpointClaim.key)
		require.Equal(t, "namespaced-b", value)
		require.Equal(t, second.session.Lease(), lease)
	})

	t.Run("CrossKS registration stores server info without a claim", func(t *testing.T) {
		crossClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, crossClient.Close()) })
		etcd.SetEtcdCliByNamespace(crossClient, "cross-keyspace/")

		setStatusEndpointTestConfig("127.0.0.11", 4100, 10091, true)
		syncer := NewCrossKSSyncer("cross-virtual", func() uint64 { return 100 }, crossClient, nil, "target-ks")
		recorder := &statusEndpointClaimRecorder{}
		syncer.endpointClaim.report = recorder.record
		t.Cleanup(func() { orphanSyncerSession(syncer) })
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))

		require.True(t, syncer.info.Load().IsAssumed())
		require.Empty(t, syncer.endpointClaim.endpoint)
		require.Empty(t, syncer.endpointClaim.key)
		recorder.requireSingle(t, statusEndpointClaimSkipped)
		requireServerInfoKey(ctx, t, crossClient, syncer.serverInfoPath)
		resp, err := crossClient.Get(ctx, serverStatusAddressPath, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})

	t.Run("explicitly disabled claim keeps server registration", func(t *testing.T) {
		syncer, recorder := startStatusEndpointTestSyncer(
			ctx, t, client, "claim-disabled", "127.0.0.20", 4200, 10100, true,
			WithoutStatusEndpointClaim(),
		)

		require.Empty(t, syncer.endpointClaim.endpoint)
		require.Empty(t, syncer.endpointClaim.key)
		recorder.requireSingle(t, statusEndpointClaimSkipped)
		requireServerInfoKey(ctx, t, client, syncer.serverInfoPath)
		requireEtcdKeyAbsent(ctx, t, client, statusEndpointClaimTestKey("127.0.0.20:10100"))

		oldSession := syncer.session
		oldLease := oldSession.Lease()
		defer oldSession.Orphan()
		require.NoError(t, syncer.Restart(ctx))
		require.NotEqual(t, oldLease, syncer.session.Lease())
		require.Len(t, recorder.results, 2)
		require.Equal(t, statusEndpointClaimSkipped, recorder.results[1].state)
		_, serverInfoLease := requireStatusEndpointKV(ctx, t, client, syncer.serverInfoPath)
		require.Equal(t, syncer.session.Lease(), serverInfoLease)
		requireEtcdKeyAbsent(ctx, t, client, statusEndpointClaimTestKey("127.0.0.20:10100"))
	})

	t.Run("status services without a claim keep registration", func(t *testing.T) {
		tests := []struct {
			name         string
			host         string
			statusPort   uint
			reportStatus bool
			sqlPort      uint
		}{
			{name: "report status disabled", host: "127.0.0.12", statusPort: 10092, reportStatus: false, sqlPort: 4110},
			{name: "empty advertised host", statusPort: 10093, reportStatus: true, sqlPort: 4111},
		}
		for i, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				id := fmt.Sprintf("skip-claim-%d", i)
				syncer, recorder := startStatusEndpointTestSyncer(
					ctx, t, client, id, test.host, test.sqlPort, test.statusPort, test.reportStatus,
				)

				require.Empty(t, syncer.endpointClaim.key)
				recorder.requireSingle(t, statusEndpointClaimSkipped)
				requireServerInfoKey(ctx, t, client, syncer.serverInfoPath)
			})
		}
	})

	t.Run("zero status port claims the production default endpoint", func(t *testing.T) {
		syncer, recorder := startStatusEndpointTestSyncer(
			ctx, t, client, "zero-status-port", "127.0.0.13", 4112, 0, true,
		)

		claimKey := statusEndpointClaimTestKey("127.0.0.13:10080")
		result := recorder.requireSingle(t, statusEndpointClaimAcquired)
		require.Equal(t, "127.0.0.13:10080", result.endpoint)
		require.Equal(t, claimKey, result.claimKey)
		value, lease := requireStatusEndpointKV(ctx, t, client, claimKey)
		require.Equal(t, "zero-status-port", value)
		require.Equal(t, syncer.session.Lease(), lease)
		require.Equal(t, uint(0), syncer.GetLocalServerInfo().StatusPort)
		requireServerInfoKey(ctx, t, client, syncer.serverInfoPath)
	})

	t.Run("nil etcd client returns before the claim attempt", func(t *testing.T) {
		syncer, recorder := newStatusEndpointTestSyncer(
			t, nil, "nil-etcd-client", "127.0.0.14", 4120, 10094, true,
		)

		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
		require.Empty(t, recorder.results)
		require.Nil(t, syncer.session)
	})

	t.Run("claim check error does not block server info registration", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, faultClient.Close()) })
		faultClient.KV = &statusEndpointFaultKV{
			KV:        faultClient.KV,
			txnFaults: []statusEndpointTxnFault{statusEndpointTxnFailBeforeCommit},
		}

		syncer, recorder := newStatusEndpointTestSyncer(
			t, faultClient, "claim-check-error", "127.0.0.15", 4130, 10095, true,
		)
		require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))

		result := recorder.requireSingle(t, statusEndpointClaimCheckFailed)
		require.ErrorIs(t, result.err, errStatusEndpointTxnFault)
		requireEtcdKeyAbsent(ctx, t, client, syncer.endpointClaim.key)
		requireServerInfoKey(ctx, t, client, syncer.serverInfoPath)
	})

	t.Run("unknown claim outcome is cleaned after server info failure", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, faultClient.Close()) })
		storeErr := errors.New("injected server info put failure")
		faultKV := &statusEndpointFaultKV{
			KV:        faultClient.KV,
			txnFaults: []statusEndpointTxnFault{statusEndpointTxnFailAfterCommit},
			putErr:    storeErr,
		}
		faultClient.KV = faultKV

		syncer, recorder := newStatusEndpointTestSyncer(
			t, faultClient, "unknown-outcome", "127.0.0.16", 4140, 10096, true,
		)
		faultKV.failPutKey = syncer.serverInfoPath

		err = syncer.NewSessionAndStoreServerInfo(ctx)
		require.ErrorIs(t, err, storeErr)
		result := recorder.requireSingle(t, statusEndpointClaimCheckFailed)
		require.ErrorIs(t, result.err, errStatusEndpointTxnFault)
		requireSessionDone(t, syncer)
		require.Eventually(t, func() bool {
			resp, getErr := client.Get(ctx, syncer.endpointClaim.key)
			return getErr == nil && len(resp.Kvs) == 0
		}, 5*time.Second, 20*time.Millisecond)
		requireEtcdKeyAbsent(ctx, t, client, syncer.serverInfoPath)

		replacement, replacementRecorder := startStatusEndpointTestSyncer(
			ctx, t, client, "unknown-outcome-replacement", "127.0.0.16", 4141, 10096, true,
		)
		replacementRecorder.requireSingle(t, statusEndpointClaimAcquired)
		value, lease := requireStatusEndpointKV(ctx, t, client, replacement.endpointClaim.key)
		require.Equal(t, "unknown-outcome-replacement", value)
		require.Equal(t, replacement.session.Lease(), lease)
	})

	t.Run("failed conflict registration cannot remove the winner claim", func(t *testing.T) {
		holder, _ := startStatusEndpointTestSyncer(
			ctx, t, client, "failed-conflict-holder", "127.0.0.17", 4150, 10097, true,
		)

		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, faultClient.Close()) })
		storeErr := errors.New("injected conflict server info put failure")
		faultKV := &statusEndpointFaultKV{KV: faultClient.KV, putErr: storeErr}
		faultClient.KV = faultKV

		loser, recorder := newStatusEndpointTestSyncer(
			t, faultClient, "failed-conflict-loser", "127.0.0.17", 4151, 10097, true,
		)
		faultKV.failPutKey = loser.serverInfoPath

		err = loser.NewSessionAndStoreServerInfo(ctx)
		require.ErrorIs(t, err, storeErr)
		recorder.requireSingle(t, statusEndpointClaimConflict)
		requireSessionDone(t, loser)
		value, lease := requireStatusEndpointKV(ctx, t, client, holder.endpointClaim.key)
		require.Equal(t, "failed-conflict-holder", value)
		require.Equal(t, holder.session.Lease(), lease)
		requireEtcdKeyAbsent(ctx, t, client, loser.serverInfoPath)
	})

	t.Run("failed registration cleanup remains bounded when etcd operations fail", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, faultClient.Close()) })
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

		syncer, recorder := newStatusEndpointTestSyncer(
			t, faultClient, "bounded-cleanup", "127.0.0.18", 4160, 10098, true,
		)
		faultKV.failPutKey = syncer.serverInfoPath

		start := time.Now()
		err = syncer.NewSessionAndStoreServerInfo(ctx)
		elapsed := time.Since(start)
		require.ErrorIs(t, err, storeErr)
		require.GreaterOrEqual(t, elapsed, 900*time.Millisecond)
		require.Less(t, elapsed, 10*time.Second)
		require.Equal(t, 2, faultKV.transactionCount())
		recorder.requireSingle(t, statusEndpointClaimAcquired)
		requireSessionDone(t, syncer)
		revokeCalls := faultLease.revokeCallsSnapshot()
		require.Len(t, revokeCalls, 1)
		require.True(t, revokeCalls[0].hasDeadline)
		require.GreaterOrEqual(t, revokeCalls[0].deadline.Sub(start), 900*time.Millisecond)
		require.Less(t, revokeCalls[0].deadline.Sub(start), 10*time.Second)

		value, lease := requireStatusEndpointKV(ctx, t, client, syncer.endpointClaim.key)
		require.Equal(t, "bounded-cleanup", value)
		require.Equal(t, syncer.session.Lease(), lease)
		requireEtcdKeyAbsent(ctx, t, client, syncer.serverInfoPath)

		_, err = client.Revoke(ctx, lease)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			resp, getErr := client.Get(ctx, syncer.endpointClaim.key)
			return getErr == nil && len(resp.Kvs) == 0
		}, 5*time.Second, 20*time.Millisecond)
	})

	t.Run("parent cancellation produces no misleading claim warning", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, faultClient.Close()) })
		started := make(chan struct{})
		faultClient.KV = &statusEndpointFaultKV{
			KV:        faultClient.KV,
			txnFaults: []statusEndpointTxnFault{statusEndpointTxnWaitForContext},
			started:   started,
		}

		syncer, recorder := newStatusEndpointTestSyncer(
			t, faultClient, "parent-cancel", "127.0.0.19", 4170, 10099, true,
		)
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
		require.Empty(t, recorder.results)
		requireSessionDone(t, syncer)
		requireEtcdKeyAbsent(ctx, t, client, syncer.endpointClaim.key)
		requireEtcdKeyAbsent(ctx, t, client, syncer.serverInfoPath)
	})

	t.Run("server info sync loop observes shutdown before restart", func(t *testing.T) {
		exitCh := make(chan struct{})
		require.False(t, serverInfoSyncLoopExitRequested(exitCh))
		close(exitCh)
		require.True(t, serverInfoSyncLoopExitRequested(exitCh))
	})

	t.Run("server info sync loop backs off after restart failures and exits", func(t *testing.T) {
		faultClient, err := cluster.NewClientV3(0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, faultClient.Close()) })
		faultLease := &statusEndpointFaultLease{
			Lease:       faultClient.Lease,
			grantEvents: make(chan time.Time, 8),
		}
		faultClient.Lease = faultLease

		syncer, _ := startStatusEndpointTestSyncer(
			ctx, t, faultClient, "restart-backoff", "127.0.0.20", 4180, 10100, false,
		)
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
		collectTimeout := time.NewTimer(15 * time.Second)
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
		case <-time.After(15 * time.Second):
		}
		require.True(t, loopExited)
		require.Len(t, grantTimes, 4)
		restartGap := grantTimes[3].Sub(grantTimes[2])
		require.GreaterOrEqual(t, restartGap, 900*time.Millisecond)
		require.Less(t, restartGap, 10*time.Second)
		require.Equal(t, 6, faultLease.grantFailureCount())
	})
}

type statusEndpointClaimRecorder struct {
	results []statusEndpointClaimResult
}

func (r *statusEndpointClaimRecorder) record(result statusEndpointClaimResult) {
	r.results = append(r.results, result)
}

func (r *statusEndpointClaimRecorder) requireSingle(
	t *testing.T,
	state statusEndpointClaimState,
) statusEndpointClaimResult {
	t.Helper()
	result := r.single(t)
	require.Equal(t, state, result.state)
	return result
}

func (r *statusEndpointClaimRecorder) single(t *testing.T) statusEndpointClaimResult {
	t.Helper()
	require.Len(t, r.results, 1)
	return r.results[0]
}

func newStatusEndpointTestSyncer(
	t *testing.T,
	client *clientv3.Client,
	id, host string,
	sqlPort, statusPort uint,
	reportStatus bool,
	options ...SyncerOption,
) (*Syncer, *statusEndpointClaimRecorder) {
	t.Helper()
	setStatusEndpointTestConfig(host, sqlPort, statusPort, reportStatus)
	syncer := NewSyncer(id, func() uint64 { return uint64(sqlPort) }, client, nil, options...)
	recorder := &statusEndpointClaimRecorder{}
	syncer.endpointClaim.report = recorder.record
	t.Cleanup(func() { orphanSyncerSession(syncer) })
	return syncer, recorder
}

func startStatusEndpointTestSyncer(
	ctx context.Context,
	t *testing.T,
	client *clientv3.Client,
	id, host string,
	sqlPort, statusPort uint,
	reportStatus bool,
	options ...SyncerOption,
) (*Syncer, *statusEndpointClaimRecorder) {
	t.Helper()
	syncer, recorder := newStatusEndpointTestSyncer(
		t, client, id, host, sqlPort, statusPort, reportStatus, options...,
	)
	require.NoError(t, syncer.NewSessionAndStoreServerInfo(ctx))
	return syncer, recorder
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
