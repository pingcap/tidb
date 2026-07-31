// Copyright 2024 PingCAP, Inc.
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

package store

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
)

type mockEtcdBackend struct {
	kv.Storage
	kv.EtcdBackend
	pdAddrs   []string
	metaAddrs []string
}

func (mebd *mockEtcdBackend) EtcdAddrs() ([]string, error) {
	return mebd.metaAddrs, nil
}

func (mebd *mockEtcdBackend) GetPDAddrs() ([]string, error) {
	return mebd.pdAddrs, nil
}

func (*mockEtcdBackend) TLSConfig() *tls.Config { return nil }

func (*mockEtcdBackend) StartGCWorker() error { return nil }

func TestNewEtcdCliGetEtcdAddrs(t *testing.T) {
	etcdStore, addrs, err := GetEtcdAddrs(nil)
	require.NoError(t, err)
	require.Empty(t, addrs)
	require.Nil(t, etcdStore)

	etcdStore, addrs, err = GetEtcdAddrs(&mockEtcdBackend{
		pdAddrs:   []string{"localhost:2379"},
		metaAddrs: []string{"localhost:2389"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"localhost:2389"}, addrs)
	require.NotNil(t, etcdStore)

	cli, err := NewEtcdCli(nil)
	require.NoError(t, err)
	require.Nil(t, cli)
}

func newEmbeddedEtcdConfig(t *testing.T, name string) *embed.Config {
	t.Helper()
	allocURL := func() url.URL {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		require.NoError(t, listener.Close())
		parsed, err := url.Parse("http://" + address)
		require.NoError(t, err)
		return *parsed
	}

	cfg := embed.NewConfig()
	cfg.Name = name
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	cfg.StrictReconfigCheck = false
	cfg.ListenPeerUrls = []url.URL{allocURL()}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.ListenClientUrls = []url.URL{allocURL()}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.InitialCluster = fmt.Sprintf("%s=%s", name, cfg.AdvertisePeerUrls[0].String())
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

func startEmbeddedEtcd(t *testing.T, cfg *embed.Config) *embed.Etcd {
	t.Helper()
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	select {
	case <-server.Server.ReadyNotify():
	case err := <-server.Err():
		server.Close()
		require.FailNow(t, "embedded etcd server stopped before becoming ready", "%v", err)
	case <-time.After(10 * time.Second):
		server.Close()
		require.FailNow(t, "embedded etcd server did not become ready")
	}
	return server
}

func closeEmbeddedEtcdServers(servers ...*embed.Etcd) {
	var wg sync.WaitGroup
	wg.Add(len(servers))
	for _, server := range servers {
		go func() {
			defer wg.Done()
			server.Close()
		}()
	}
	wg.Wait()
}

func enableFastEtcdHealthCheck(t *testing.T) {
	t.Helper()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/fastEtcdHealthCheck", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/fastEtcdHealthCheck"))
	})
}

type testEtcdCluster struct {
	configs     []*embed.Config
	servers     []*embed.Etcd
	adminClient *clientv3.Client
}

func startTestEtcdCluster(t *testing.T, memberCount int) *testEtcdCluster {
	t.Helper()
	return startTestEtcdClusterWithFirstConfig(t, memberCount, newEmbeddedEtcdConfig(t, "etcd-1"))
}

func startTestEtcdClusterWithFirstConfig(t *testing.T, memberCount int, firstConfig *embed.Config) *testEtcdCluster {
	t.Helper()
	require.Positive(t, memberCount)
	firstServer := startEmbeddedEtcd(t, firstConfig)
	cluster := &testEtcdCluster{
		configs: []*embed.Config{firstConfig},
		servers: []*embed.Etcd{firstServer},
	}
	t.Cleanup(func() {
		activeServers := make([]*embed.Etcd, 0, len(cluster.servers))
		for _, server := range cluster.servers {
			if server != nil {
				activeServers = append(activeServers, server)
			}
		}
		closeEmbeddedEtcdServers(activeServers...)
	})

	var err error
	cluster.adminClient, err = clientv3.New(clientv3.Config{Endpoints: []string{firstConfig.AdvertiseClientUrls[0].String()}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cluster.adminClient.Close()) })
	for len(cluster.servers) < memberCount {
		cluster.addMember(t)
	}
	return cluster
}

func (cluster *testEtcdCluster) addMember(t *testing.T) {
	t.Helper()
	memberNumber := len(cluster.configs) + 1
	config := newEmbeddedEtcdConfig(t, fmt.Sprintf("etcd-%d", memberNumber))
	clusterParts := make([]string, 0, memberNumber)
	for _, existingConfig := range cluster.configs {
		clusterParts = append(clusterParts, fmt.Sprintf("%s=%s", existingConfig.Name, existingConfig.AdvertisePeerUrls[0].String()))
	}
	clusterParts = append(clusterParts, fmt.Sprintf("%s=%s", config.Name, config.AdvertisePeerUrls[0].String()))
	config.InitialCluster = strings.Join(clusterParts, ",")
	config.ClusterState = embed.ClusterStateFlagExisting

	addCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := cluster.adminClient.MemberAdd(addCtx, []string{config.AdvertisePeerUrls[0].String()})
	cancel()
	require.NoError(t, err)
	server := startEmbeddedEtcd(t, config)
	cluster.configs = append(cluster.configs, config)
	cluster.servers = append(cluster.servers, server)
	require.Eventually(t, func() bool {
		listCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		members, err := cluster.adminClient.MemberList(listCtx)
		return err == nil && len(members.Members) == memberNumber
	}, 10*time.Second, 100*time.Millisecond)
}

func (cluster *testEtcdCluster) endpoints() []string {
	endpoints := make([]string, 0, len(cluster.configs))
	for _, config := range cluster.configs {
		endpoints = append(endpoints, config.AdvertiseClientUrls[0].String())
	}
	return endpoints
}

func (cluster *testEtcdCluster) stop(t *testing.T, index int) {
	t.Helper()
	require.NotNil(t, cluster.servers[index])
	cluster.servers[index].Close()
	cluster.servers[index] = nil
}

func (cluster *testEtcdCluster) restart(t *testing.T, index int) {
	t.Helper()
	require.Nil(t, cluster.servers[index])
	cluster.servers[index] = startEmbeddedEtcd(t, cluster.configs[index])
}

func startTwoMemberEtcdCluster(t *testing.T) ([]string, uint64) {
	t.Helper()
	cluster := startTestEtcdCluster(t, 2)
	return cluster.endpoints(), uint64(cluster.servers[0].Server.ID())
}

func TestEtcdClientTracksMemberRemoval(t *testing.T) {
	enableFastEtcdHealthCheck(t)
	addrs, memberIDToRemove := startTwoMemberEtcdCluster(t)

	cli, err := NewEtcdCliWithAddrs(addrs, &mockEtcdBackend{},
		WithEtcdHealthChecker(EtcdClientPurpose("test-member-removal")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	require.Len(t, cli.Endpoints(), 2)

	adminClient, err := clientv3.New(clientv3.Config{Endpoints: []string{addrs[1]}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adminClient.Close()) })
	removeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = adminClient.MemberRemove(removeCtx, memberIDToRemove)
	cancel()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(cli.Endpoints()) == 1
	}, 15*time.Second, 100*time.Millisecond)
	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Get(readCtx, "health")
	require.NoError(t, err)
}

func TestEtcdClientTracksMemberAddition(t *testing.T) {
	enableFastEtcdHealthCheck(t)
	cluster := startTestEtcdCluster(t, 1)
	initialEndpoint := cluster.endpoints()[0]
	purpose := EtcdClientPurpose("test-member-addition")
	cli, err := NewEtcdCliWithAddrs([]string{initialEndpoint}, &mockEtcdBackend{},
		WithEtcdHealthChecker(purpose))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	cluster.addMember(t)
	expectedEndpoints := cluster.endpoints()
	require.Eventually(t, func() bool {
		return equivalentStringSlices(cli.Endpoints(), expectedEndpoints)
	}, 10*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.EtcdClientActiveEndpoints.WithLabelValues(string(purpose))) == 2
	}, 10*time.Second, 50*time.Millisecond)
	for _, endpoint := range expectedEndpoints {
		require.Equal(t, float64(1), testutil.ToFloat64(
			metrics.EtcdClientEndpointState.WithLabelValues(string(purpose), endpoint)))
	}
}

func TestEtcdClientRecoversStoppedMembers(t *testing.T) {
	enableFastEtcdHealthCheck(t)
	cluster := startTestEtcdCluster(t, 3)
	allEndpoints := cluster.endpoints()
	purpose := EtcdClientPurpose("test-member-restart")
	cli, err := NewEtcdCliWithAddrs(allEndpoints, &mockEtcdBackend{}, WithEtcdHealthChecker(purpose))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.EtcdClientActiveEndpoints.WithLabelValues(string(purpose))) == 3
	}, 10*time.Second, 50*time.Millisecond)

	for _, stoppedIndex := range []int{0, 1, 2, 2, 1, 0} {
		stoppedEndpoint := allEndpoints[stoppedIndex]
		cluster.stop(t, stoppedIndex)
		require.Eventually(t, func() bool {
			endpoints := cli.Endpoints()
			return len(endpoints) == 2 && !stringSliceContains(endpoints, stoppedEndpoint)
		}, 10*time.Second, 50*time.Millisecond)
		require.Equal(t, float64(0), testutil.ToFloat64(
			metrics.EtcdClientEndpointState.WithLabelValues(string(purpose), stoppedEndpoint)))
		readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err = cli.Get(readCtx, "health")
		cancel()
		require.NoError(t, err)

		cluster.restart(t, stoppedIndex)
		require.Eventually(t, func() bool {
			return equivalentStringSlices(cli.Endpoints(), allEndpoints)
		}, 10*time.Second, 50*time.Millisecond)
		require.Equal(t, float64(1), testutil.ToFloat64(
			metrics.EtcdClientEndpointState.WithLabelValues(string(purpose), stoppedEndpoint)))
	}
}

func TestEtcdClientHandlesHangingEndpoint(t *testing.T) {
	for _, enableChecker := range []bool{true, false} {
		name := "checker-disabled"
		if enableChecker {
			name = "checker-enabled"
		}
		t.Run(name, func(t *testing.T) {
			enableFastEtcdHealthCheck(t)
			firstConfig := newEmbeddedEtcdConfig(t, "etcd-1")
			proxy := startEtcdDiscardProxy(t, firstConfig.ListenClientUrls[0].String())
			proxyURL, err := url.Parse(proxy.endpoint())
			require.NoError(t, err)
			firstConfig.AdvertiseClientUrls = []url.URL{*proxyURL}
			cluster := startTestEtcdClusterWithFirstConfig(t, 1, firstConfig)
			cluster.addMember(t)
			var opts []EtcdClientOption
			if enableChecker {
				opts = append(opts, WithEtcdHealthChecker(EtcdClientPurpose("test-hanging-endpoint")))
			}
			cli, err := NewEtcdCliWithAddrs([]string{proxy.endpoint()}, &mockEtcdBackend{}, opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cli.Close()) })

			if enableChecker {
				require.Eventually(t, func() bool {
					return equivalentStringSlices(cli.Endpoints(), cluster.endpoints())
				}, 10*time.Second, 50*time.Millisecond)
			} else {
				require.Equal(t, []string{proxy.endpoint()}, cli.Endpoints())
			}

			proxy.discard.Store(true)
			if enableChecker {
				require.Eventually(t, func() bool {
					return !stringSliceContains(cli.Endpoints(), proxy.endpoint())
				}, 10*time.Second, 50*time.Millisecond)
				requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err = cli.Get(requestCtx, "health")
				cancel()
				require.NoError(t, err)
			} else {
				requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err = cli.Get(requestCtx, "health")
				cancel()
				require.Error(t, err)
			}
		})
	}
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type etcdDiscardProxy struct {
	listener    net.Listener
	server      string
	discard     atomic.Bool
	cancel      context.CancelFunc
	connections sync.Map
	loops       sync.WaitGroup
}

func startEtcdDiscardProxy(t *testing.T, server string) *etcdDiscardProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &etcdDiscardProxy{
		listener: listener,
		server:   strings.TrimPrefix(server, "http://"),
		cancel:   cancel,
	}
	proxy.loops.Add(1)
	go proxy.serve(ctx)
	t.Cleanup(proxy.close)
	return proxy
}

func (proxy *etcdDiscardProxy) endpoint() string {
	return "http://" + proxy.listener.Addr().String()
}

func (proxy *etcdDiscardProxy) serve(ctx context.Context) {
	defer proxy.loops.Done()
	for {
		clientConnection, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		serverConnection, err := net.DialTimeout("tcp", proxy.server, 3*time.Second)
		if err != nil {
			_ = clientConnection.Close()
			continue
		}
		proxy.connections.Store(clientConnection, struct{}{})
		proxy.connections.Store(serverConnection, struct{}{})
		proxy.loops.Add(1)
		go func() {
			defer proxy.loops.Done()
			defer proxy.connections.Delete(clientConnection)
			defer proxy.connections.Delete(serverConnection)
			proxy.pipe(ctx, clientConnection, serverConnection)
		}()
	}
}

func (proxy *etcdDiscardProxy) pipe(ctx context.Context, first, second net.Conn) {
	errors := make(chan error, 2)
	go func() { errors <- proxy.copy(ctx, first, second) }()
	go func() { errors <- proxy.copy(ctx, second, first) }()
	<-errors
	_ = first.Close()
	_ = second.Close()
	<-errors
}

func (proxy *etcdDiscardProxy) copy(ctx context.Context, destination, source net.Conn) error {
	buffer := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		readCount, readErr := source.Read(buffer)
		if readCount > 0 && !proxy.discard.Load() {
			writeCount, writeErr := destination.Write(buffer[:readCount])
			if writeErr != nil {
				return writeErr
			}
			if writeCount != readCount {
				return io.ErrShortWrite
			}
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (proxy *etcdDiscardProxy) close() {
	proxy.cancel()
	_ = proxy.listener.Close()
	proxy.connections.Range(func(connection, _ any) bool {
		_ = connection.(net.Conn).Close()
		return true
	})
	proxy.loops.Wait()
}

func TestEtcdHealthChecker(t *testing.T) {
	type healthCheckerState struct {
		probes          []etcdHealthProbe
		expectedEvicted map[string]int
		expectedPicked  []string
	}
	fastProbes := func(endpoints ...string) []etcdHealthProbe {
		probes := make([]etcdHealthProbe, 0, len(endpoints))
		for _, endpoint := range endpoints {
			probes = append(probes, etcdHealthProbe{endpoint: endpoint, took: time.Millisecond})
		}
		return probes
	}
	checkStates := func(t *testing.T, states []healthCheckerState) {
		t.Helper()
		checker := &etcdHealthChecker{purpose: EtcdClientPurpose("test-state-transitions")}
		var lastEndpoints []string
		for i, state := range states {
			probeCh := make(chan etcdHealthProbe, len(state.probes))
			for _, probe := range state.probes {
				probeCh <- probe
				checker.healthyClients.LoadOrStore(probe.endpoint, &healthyEtcdClient{})
			}
			close(probeCh)
			pickedEndpoints := checker.pickEndpoints(probeCh)
			checker.updateEvictedEndpoints(lastEndpoints, pickedEndpoints)
			pickedEndpoints = checker.filterEvictedEndpoints(pickedEndpoints)

			actualEvicted := make(map[string]int)
			checker.evictedEndpoints.Range(func(key, value any) bool {
				actualEvicted[key.(string)] = value.(int)
				return true
			})
			require.Equalf(t, state.expectedEvicted, actualEvicted, "state %d", i)
			require.Equalf(t, state.expectedPicked, pickedEndpoints, "state %d", i)
			lastEndpoints = pickedEndpoints
		}
	}

	t.Run("pick fastest latency bucket", func(t *testing.T) {
		testCases := []struct {
			name     string
			probes   []etcdHealthProbe
			expected []string
		}{
			{
				name: "sub-second endpoints",
				probes: []etcdHealthProbe{
					{endpoint: "a", took: 100 * time.Millisecond},
					{endpoint: "b", took: 900 * time.Millisecond},
					{endpoint: "c", took: 1100 * time.Millisecond},
				},
				expected: []string{"a", "b"},
			},
			{
				name: "exact bucket boundaries",
				probes: []etcdHealthProbe{
					{endpoint: "a", took: time.Second},
					{endpoint: "b", took: time.Second},
					{endpoint: "c", took: 2 * time.Second},
				},
				expected: []string{"a", "b"},
			},
			{
				name: "one endpoint per bucket",
				probes: []etcdHealthProbe{
					{endpoint: "a", took: time.Second},
					{endpoint: "b", took: 2 * time.Second},
					{endpoint: "c", took: 3 * time.Second},
				},
				expected: []string{"a"},
			},
		}
		checker := &etcdHealthChecker{}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				probeCh := make(chan etcdHealthProbe, len(testCase.probes))
				for _, probe := range testCase.probes {
					probeCh <- probe
				}
				close(probeCh)
				require.Equal(t, testCase.expected, checker.pickEndpoints(probeCh))
			})
		}
	})

	t.Run("PD endpoint state transitions", func(t *testing.T) {
		states := []healthCheckerState{
			{fastProbes("A", "B"), map[string]int{}, []string{"A", "B"}},
			{fastProbes("A", "B", "C"), map[string]int{}, []string{"A", "B", "C"}},
			{fastProbes("A", "B", "C"), map[string]int{}, []string{"A", "B", "C"}},
			{fastProbes("C"), map[string]int{"A": 0, "B": 0}, []string{"C"}},
			{fastProbes("A", "B", "C"), map[string]int{"A": 1, "B": 1}, []string{"C"}},
			{fastProbes("B", "C"), map[string]int{"A": 0, "B": 2}, []string{"C"}},
			{fastProbes("A", "B", "C"), map[string]int{"A": 1}, []string{"B", "C"}},
			{fastProbes("D"), map[string]int{"A": 0, "B": 0, "C": 0}, []string{"D"}},
			{fastProbes("B", "C"), map[string]int{"A": 0, "B": 1, "C": 1, "D": 0}, []string{"B", "C"}},
			{fastProbes("A", "B", "C"), map[string]int{"A": 1, "B": 2, "C": 2, "D": 0}, []string{"A", "B", "C"}},
			{fastProbes("A", "C", "E"), map[string]int{"A": 2, "B": 0, "D": 0}, []string{"C", "E"}},
		}
		checkStates(t, states)
	})

	t.Run("PD latency state transitions", func(t *testing.T) {
		states := []healthCheckerState{
			{fastProbes("A", "B"), map[string]int{}, []string{"A", "B"}},
			{
				[]etcdHealthProbe{{endpoint: "A", took: time.Millisecond}, {endpoint: "B", took: time.Millisecond}, {endpoint: "C", took: time.Second}},
				map[string]int{}, []string{"A", "B"},
			},
			{
				[]etcdHealthProbe{{endpoint: "A", took: time.Second}, {endpoint: "B", took: time.Second}, {endpoint: "C", took: 2 * time.Second}},
				map[string]int{}, []string{"A", "B"},
			},
			{
				[]etcdHealthProbe{{endpoint: "A", took: time.Second}, {endpoint: "B", took: 2 * time.Second}, {endpoint: "C", took: 3 * time.Second}},
				map[string]int{"B": 0}, []string{"A"},
			},
			{
				[]etcdHealthProbe{{endpoint: "A", took: time.Second}, {endpoint: "B", took: time.Second}, {endpoint: "C", took: time.Millisecond}},
				map[string]int{"A": 0, "B": 0}, []string{"C"},
			},
			{
				[]etcdHealthProbe{{endpoint: "A", took: time.Millisecond}, {endpoint: "B", took: time.Millisecond}, {endpoint: "C", took: time.Second}},
				map[string]int{"A": 1, "B": 1, "C": 0}, []string{"A", "B"},
			},
		}
		checkStates(t, states)
	})

	t.Run("require consecutive picks before rejoining", func(t *testing.T) {
		checker := &etcdHealthChecker{}
		checker.healthyClients.Store("a", &healthyEtcdClient{})
		checker.healthyClients.Store("b", &healthyEtcdClient{})

		checker.updateEvictedEndpoints([]string{"a", "b"}, []string{"a"})
		require.Equal(t, []string{"a"}, checker.filterEvictedEndpoints([]string{"a"}))

		for pickedCount := 1; pickedCount <= etcdEndpointPickedThreshold; pickedCount++ {
			checker.updateEvictedEndpoints([]string{"a"}, []string{"a", "b"})
			filtered := checker.filterEvictedEndpoints([]string{"a", "b"})
			if pickedCount < etcdEndpointPickedThreshold {
				require.Equal(t, []string{"a"}, filtered)
			} else {
				require.Equal(t, []string{"a", "b"}, filtered)
			}
		}
		_, stillEvicted := checker.evictedEndpoints.Load("b")
		require.False(t, stillEvicted)
	})

	t.Run("do not evict removed endpoint", func(t *testing.T) {
		checker := &etcdHealthChecker{}
		for _, endpoint := range []string{"a", "b", "c"} {
			checker.healthyClients.Store(endpoint, &healthyEtcdClient{})
		}
		lastEndpoints := []string{"a", "b", "c"}
		pickedEndpoints := []string{"a", "c"}
		checker.updateEvictedEndpoints(lastEndpoints, pickedEndpoints)
		_, evicted := checker.evictedEndpoints.Load("b")
		require.True(t, evicted)

		checker.healthyClients.Delete("b")
		checker.evictedEndpoints.Delete("b")
		checker.updateEvictedEndpoints(lastEndpoints, pickedEndpoints)
		_, evicted = checker.evictedEndpoints.Load("b")
		require.False(t, evicted)
	})

	t.Run("keep availability when every endpoint is evicted", func(t *testing.T) {
		checker := &etcdHealthChecker{}
		checker.evictedEndpoints.Store("a", 0)
		checker.evictedEndpoints.Store("b", 0)
		require.Equal(t, []string{"a", "b"}, checker.filterEvictedEndpoints([]string{"a", "b"}))
	})

	t.Run("close probe clients with guarded client", func(t *testing.T) {
		serverConfig := newEmbeddedEtcdConfig(t, "etcd-close")
		server := startEmbeddedEtcd(t, serverConfig)
		t.Cleanup(func() { closeEmbeddedEtcdServers(server) })
		clientConfig := clientv3.Config{Endpoints: []string{serverConfig.AdvertiseClientUrls[0].String()}}
		cli, err := clientv3.New(clientConfig)
		require.NoError(t, err)
		t.Cleanup(func() {
			if cli.Ctx().Err() == nil {
				require.NoError(t, cli.Close())
			}
		})
		checker := initEtcdHealthChecker(time.Hour, clientConfig, cli, EtcdClientPurpose("test"))

		var probeClient *clientv3.Client
		require.Eventually(t, func() bool {
			clients, _ := checker.snapshotClients()
			if len(clients) != 1 {
				return false
			}
			probeClient = clients[0].client.Client
			return true
		}, 10*time.Second, 100*time.Millisecond)

		require.NoError(t, cli.Close())
		select {
		case <-probeClient.Ctx().Done():
		case <-time.After(10 * time.Second):
			require.FailNow(t, "health probe client was not closed with the guarded client")
		}
	})
}

func TestEtcdHealthCheckerRejectsStalePatrol(t *testing.T) {
	serverConfig := newEmbeddedEtcdConfig(t, "etcd-stale-patrol")
	server := startEmbeddedEtcd(t, serverConfig)
	t.Cleanup(func() { closeEmbeddedEtcdServers(server) })
	serverEndpoint := serverConfig.AdvertiseClientUrls[0].String()

	mainClient, err := clientv3.New(clientv3.Config{Endpoints: []string{serverEndpoint}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mainClient.Close()) })
	mainClient.SetEndpoints("removed", "blocked")

	removedClient, err := clientv3.New(clientv3.Config{Endpoints: []string{serverEndpoint}})
	require.NoError(t, err)
	blocked := make(chan struct{})
	release := make(chan struct{})
	var signalOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	blockedClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{serverEndpoint},
		DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(
			func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				signalOnce.Do(func() { close(blocked) })
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			},
		)},
	})
	require.NoError(t, err)

	oldHealth := time.Now().Add(-time.Hour)
	removedHealthyClient := newHealthyEtcdClient(removedClient, oldHealth)
	checker := &etcdHealthChecker{client: mainClient}
	checker.healthyClients.Store("removed", removedHealthyClient)
	checker.healthyClients.Store("blocked", newHealthyEtcdClient(blockedClient, oldHealth))
	t.Cleanup(func() {
		checker.removeClient("removed")
		checker.removeClient("blocked")
	})

	resultCh := make(chan []string, 1)
	go func() {
		patrol := checker.patrol(context.Background())
		resultCh <- patrol.pickedEndpoints
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "second probe did not block")
	}
	require.Eventually(t, func() bool {
		return removedHealthyClient.lastHealth().After(oldHealth)
	}, 5*time.Second, time.Millisecond)

	checker.removeClient("removed")
	require.Nil(t, checker.loadClient("removed"))
	releaseOnce.Do(func() { close(release) })
	select {
	case pickedEndpoints := <-resultCh:
		require.NotContains(t, pickedEndpoints, "removed")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "patrol did not finish")
	}
}
