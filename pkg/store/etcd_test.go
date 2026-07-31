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
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
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

func startTwoMemberEtcdCluster(t *testing.T) ([]string, uint64) {
	t.Helper()
	servers := make([]*embed.Etcd, 0, 2)
	t.Cleanup(func() { closeEmbeddedEtcdServers(servers...) })
	firstConfig := newEmbeddedEtcdConfig(t, "etcd-1")
	firstServer := startEmbeddedEtcd(t, firstConfig)
	servers = append(servers, firstServer)
	bootstrapClient, err := clientv3.New(clientv3.Config{Endpoints: []string{firstConfig.AdvertiseClientUrls[0].String()}})
	require.NoError(t, err)
	defer func() { require.NoError(t, bootstrapClient.Close()) }()

	secondConfig := newEmbeddedEtcdConfig(t, "etcd-2")
	secondConfig.InitialCluster = fmt.Sprintf("%s=%s,%s=%s",
		firstConfig.Name, firstConfig.AdvertisePeerUrls[0].String(),
		secondConfig.Name, secondConfig.AdvertisePeerUrls[0].String())
	secondConfig.ClusterState = embed.ClusterStateFlagExisting

	addCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = bootstrapClient.MemberAdd(addCtx, []string{secondConfig.AdvertisePeerUrls[0].String()})
	cancel()
	require.NoError(t, err)
	secondServer := startEmbeddedEtcd(t, secondConfig)
	servers = append(servers, secondServer)
	require.Eventually(t, func() bool {
		listCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		members, err := bootstrapClient.MemberList(listCtx)
		return err == nil && len(members.Members) == 2
	}, 10*time.Second, 100*time.Millisecond)

	return []string{
		firstConfig.AdvertiseClientUrls[0].String(),
		secondConfig.AdvertiseClientUrls[0].String(),
	}, uint64(firstServer.Server.ID())
}

func TestEtcdClientTracksMemberRemoval(t *testing.T) {
	addrs, memberIDToRemove := startTwoMemberEtcdCluster(t)

	restoreConfig := config.RestoreFunc()
	t.Cleanup(restoreConfig)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.GrpcKeepAliveTime = 1
	})

	cli, err := NewEtcdCliWithAddrs(addrs, &mockEtcdBackend{})
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

func TestEtcdHealthChecker(t *testing.T) {
	t.Run("pick fastest latency bucket", func(t *testing.T) {
		checker := &etcdHealthChecker{}
		probeCh := make(chan etcdHealthProbe, 3)
		probeCh <- etcdHealthProbe{endpoint: "a", took: 100 * time.Millisecond}
		probeCh <- etcdHealthProbe{endpoint: "b", took: 900 * time.Millisecond}
		probeCh <- etcdHealthProbe{endpoint: "c", took: 1100 * time.Millisecond}
		close(probeCh)

		require.Equal(t, []string{"a", "b"}, checker.pickEndpoints(probeCh))
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
		checker := initEtcdHealthChecker(time.Hour, clientConfig, cli)

		var probeClient *clientv3.Client
		require.Eventually(t, func() bool {
			clients := checker.snapshotClients()
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
