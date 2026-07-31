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

package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultEtcdHealthCheckInterval = 10 * time.Second
	etcdHealthCheckTimeout         = 10 * time.Second
	etcdSlowRequestTime            = time.Second
	etcdServerOfflineTimeout       = 30 * time.Minute
	etcdServerDisconnectedTimeout  = time.Minute
	etcdEndpointPickedThreshold    = 3
)

// healthyEtcdClient maintains a connection to exactly one endpoint so a health
// probe cannot be served by a different member through the etcd load balancer.
type healthyEtcdClient struct {
	*clientv3.Client
	lastHealthUnixNano atomic.Int64
}

func newHealthyEtcdClient(client *clientv3.Client, lastHealth time.Time) *healthyEtcdClient {
	healthyClient := &healthyEtcdClient{Client: client}
	healthyClient.updateLastHealth(lastHealth)
	return healthyClient
}

func (client *healthyEtcdClient) updateLastHealth(lastHealth time.Time) {
	client.lastHealthUnixNano.Store(lastHealth.UnixNano())
}

func (client *healthyEtcdClient) lastHealth() time.Time {
	return time.Unix(0, client.lastHealthUnixNano.Load())
}

// etcdHealthChecker discovers cluster members and keeps the guarded client's
// endpoints limited to members that respond within the best latency bucket.
type etcdHealthChecker struct {
	tickerInterval time.Duration
	clientConfig   clientv3.Config
	loops          sync.WaitGroup

	// endpoint(string) -> *healthyEtcdClient
	healthyClients sync.Map
	// endpoint(string) -> consecutive picked count(int)
	evictedEndpoints sync.Map

	client *clientv3.Client
}

func initEtcdHealthChecker(tickerInterval time.Duration, clientConfig clientv3.Config, client *clientv3.Client) *etcdHealthChecker {
	checker := &etcdHealthChecker{
		tickerInterval: tickerInterval,
		clientConfig:   clientConfig,
		client:         client,
	}
	ctx := client.Ctx()
	checker.loops.Add(2)
	go util.WithRecovery(func() {
		defer checker.loops.Done()
		checker.syncer(ctx)
	}, nil)
	go util.WithRecovery(func() {
		defer checker.loops.Done()
		checker.inspector(ctx)
	}, nil)
	go util.WithRecovery(func() {
		<-ctx.Done()
		checker.loops.Wait()
		checker.close()
	}, nil)
	return checker
}

func (checker *etcdHealthChecker) syncer(ctx context.Context) {
	checker.update(ctx)
	ticker := time.NewTicker(checker.tickerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logutil.BgLogger().Info("etcd client is closed, exit the endpoint syncer")
			return
		case <-ticker.C:
			checker.update(ctx)
		}
	}
}

func (checker *etcdHealthChecker) inspector(ctx context.Context) {
	ticker := time.NewTicker(checker.tickerInterval)
	defer ticker.Stop()
	lastAvailable := time.Now()
	for {
		select {
		case <-ctx.Done():
			logutil.BgLogger().Info("etcd client is closed, exit the health inspector")
			return
		case <-ticker.C:
			lastEndpoints, pickedEndpoints, changed := checker.patrol(ctx)
			if len(pickedEndpoints) == 0 {
				// Resetting closes the existing sub-connections and avoids waiting for
				// gRPC's exponential reconnect backoff when every endpoint is down.
				if time.Since(lastAvailable) > etcdServerDisconnectedTimeout {
					logutil.BgLogger().Info("no available etcd endpoint, reset endpoints",
						zap.Strings("last-endpoints", lastEndpoints))
					resetEtcdClientEndpoints(checker.client, lastEndpoints...)
				}
				continue
			}
			if changed {
				checker.client.SetEndpoints(pickedEndpoints...)
				logutil.BgLogger().Info("update etcd endpoints",
					zap.Int("last-endpoint-count", len(lastEndpoints)),
					zap.Int("endpoint-count", len(pickedEndpoints)),
					zap.Strings("last-endpoints", lastEndpoints),
					zap.Strings("endpoints", checker.client.Endpoints()))
			}
			lastAvailable = time.Now()
		}
	}
}

func (checker *etcdHealthChecker) close() {
	checker.healthyClients.Range(func(key, _ any) bool {
		checker.removeClient(key.(string))
		return true
	})
}

func resetEtcdClientEndpoints(client *clientv3.Client, endpoints ...string) {
	client.SetEndpoints()
	client.SetEndpoints(endpoints...)
}

type etcdHealthProbe struct {
	endpoint string
	took     time.Duration
}

type etcdHealthProbeClient struct {
	endpoint string
	client   *healthyEtcdClient
}

func (checker *etcdHealthChecker) patrol(ctx context.Context) (lastEndpoints, pickedEndpoints []string, changed bool) {
	clients := checker.snapshotClients()
	probeCh := make(chan etcdHealthProbe, len(clients))
	var wg sync.WaitGroup
	for _, probeClient := range clients {
		wg.Add(1)
		go util.WithRecovery(func() {
			defer wg.Done()
			endpoint := probeClient.endpoint
			healthyClient := probeClient.client
			start := time.Now()
			if !isEtcdEndpointHealthy(ctx, healthyClient.Client) {
				logutil.BgLogger().Warn("etcd endpoint is unhealthy",
					zap.String("endpoint", endpoint),
					zap.Duration("took", time.Since(start)))
				return
			}
			took := time.Since(start)
			if checker.loadClient(endpoint) != healthyClient {
				return
			}
			healthyClient.updateLastHealth(start)
			probeCh <- etcdHealthProbe{endpoint: endpoint, took: took}
		}, nil)
	}
	wg.Wait()
	close(probeCh)

	lastEndpoints = checker.client.Endpoints()
	pickedEndpoints = checker.pickEndpoints(probeCh)
	if len(pickedEndpoints) > 0 {
		checker.updateEvictedEndpoints(lastEndpoints, pickedEndpoints)
		pickedEndpoints = checker.filterEvictedEndpoints(pickedEndpoints)
	}
	return lastEndpoints, pickedEndpoints, !equivalentStringSlices(lastEndpoints, pickedEndpoints)
}

func isEtcdEndpointHealthy(ctx context.Context, client *clientv3.Client) bool {
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), etcdHealthCheckTimeout)
	defer cancel()
	_, err := client.Get(ctx, "health")
	// Permission denied proves that the request reached consensus, which is enough
	// for an endpoint health probe.
	return err == nil || err == rpctypes.ErrPermissionDenied
}

// pickEndpoints chooses the healthy endpoints in the lowest one-second latency
// bucket. This prevents a slow member from delaying requests while faster members
// are available.
func (*etcdHealthChecker) pickEndpoints(probeCh <-chan etcdHealthProbe) []string {
	probeCount := len(probeCh)
	if probeCount == 0 {
		return nil
	}
	probes := make([]etcdHealthProbe, 0, probeCount)
	for probe := range probeCh {
		probes = append(probes, probe)
	}

	pickedEndpoints := make([]string, 0, probeCount)
	for i := range int(etcdHealthCheckTimeout / etcdSlowRequestTime) {
		minLatency := etcdSlowRequestTime * time.Duration(i)
		maxLatency := etcdSlowRequestTime * time.Duration(i+1)
		for _, probe := range probes {
			if minLatency <= probe.took && probe.took < maxLatency {
				pickedEndpoints = append(pickedEndpoints, probe.endpoint)
			}
		}
		if len(pickedEndpoints) > 0 {
			break
		}
	}
	return pickedEndpoints
}

func (checker *etcdHealthChecker) updateEvictedEndpoints(lastEndpoints, pickedEndpoints []string) {
	pickedSet := make(map[string]struct{}, len(pickedEndpoints))
	for _, endpoint := range pickedEndpoints {
		pickedSet[endpoint] = struct{}{}
	}

	checker.evictedEndpoints.Range(func(key, value any) bool {
		endpoint := key.(string)
		pickedCount := value.(int)
		if _, picked := pickedSet[endpoint]; pickedCount > 0 && !picked {
			checker.evictedEndpoints.Store(endpoint, 0)
			logutil.BgLogger().Info("reset evicted etcd endpoint picked count",
				zap.String("endpoint", endpoint),
				zap.Int("previous-count", pickedCount))
		}
		return true
	})

	for _, endpoint := range lastEndpoints {
		if _, picked := pickedSet[endpoint]; picked {
			continue
		}
		if checker.loadClient(endpoint) == nil {
			continue
		}
		checker.evictedEndpoints.Store(endpoint, 0)
		logutil.BgLogger().Info("evict etcd endpoint", zap.String("endpoint", endpoint))
	}

	for _, endpoint := range pickedEndpoints {
		if value, ok := checker.evictedEndpoints.Load(endpoint); ok {
			pickedCount := value.(int) + 1
			checker.evictedEndpoints.Store(endpoint, pickedCount)
			logutil.BgLogger().Info("evicted etcd endpoint picked again",
				zap.String("endpoint", endpoint),
				zap.Int("picked-count", pickedCount),
				zap.Int("picked-count-threshold", etcdEndpointPickedThreshold))
		}
	}
}

func (checker *etcdHealthChecker) filterEvictedEndpoints(endpoints []string) []string {
	pickedEndpoints := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if value, ok := checker.evictedEndpoints.Load(endpoint); ok {
			pickedCount := value.(int)
			if pickedCount < etcdEndpointPickedThreshold {
				continue
			}
			checker.evictedEndpoints.Delete(endpoint)
			logutil.BgLogger().Info("add evicted etcd endpoint back",
				zap.String("endpoint", endpoint),
				zap.Int("picked-count", pickedCount))
		}
		pickedEndpoints = append(pickedEndpoints, endpoint)
	}
	if len(pickedEndpoints) == 0 {
		logutil.BgLogger().Warn("all etcd endpoints are evicted, use the picked endpoints",
			zap.Strings("endpoints", endpoints))
		return endpoints
	}
	return pickedEndpoints
}

func (checker *etcdHealthChecker) update(ctx context.Context) {
	endpoints := checker.syncURLs(ctx)
	if len(endpoints) == 0 {
		logutil.BgLogger().Warn("no available etcd endpoint returned by the cluster")
		return
	}

	endpointSet := make(map[string]struct{}, len(endpoints))
	for _, endpoint := range endpoints {
		endpointSet[endpoint] = struct{}{}
	}
	for endpoint := range endpointSet {
		client := checker.loadClient(endpoint)
		if client == nil {
			checker.initClient(endpoint)
			continue
		}
		sinceLastHealth := time.Since(client.lastHealth())
		if sinceLastHealth > etcdServerOfflineTimeout {
			logutil.BgLogger().Info("etcd server might be offline, remove health probe client",
				zap.String("endpoint", endpoint),
				zap.Duration("since-last-health", sinceLastHealth))
			checker.removeClient(endpoint)
			continue
		}
		if sinceLastHealth > etcdServerDisconnectedTimeout {
			logutil.BgLogger().Info("etcd server might be disconnected, reconnect health probe client",
				zap.String("endpoint", endpoint),
				zap.Duration("since-last-health", sinceLastHealth))
			resetEtcdClientEndpoints(client.Client, endpoint)
		}
	}

	checker.healthyClients.Range(func(key, _ any) bool {
		endpoint := key.(string)
		if _, exists := endpointSet[endpoint]; !exists {
			logutil.BgLogger().Info("remove stale etcd health probe client", zap.String("endpoint", endpoint))
			checker.removeClient(endpoint)
		}
		return true
	})
}

func (checker *etcdHealthChecker) snapshotClients() []etcdHealthProbeClient {
	clients := make([]etcdHealthProbeClient, 0)
	checker.healthyClients.Range(func(key, value any) bool {
		clients = append(clients, etcdHealthProbeClient{
			endpoint: key.(string),
			client:   value.(*healthyEtcdClient),
		})
		return true
	})
	return clients
}

func (checker *etcdHealthChecker) loadClient(endpoint string) *healthyEtcdClient {
	client, ok := checker.healthyClients.Load(endpoint)
	if !ok {
		return nil
	}
	return client.(*healthyEtcdClient)
}

func (checker *etcdHealthChecker) initClient(endpoint string) {
	clientConfig := checker.clientConfig
	clientConfig.Endpoints = []string{endpoint}
	clientConfig.AutoSyncInterval = 0
	client, err := clientv3.New(clientConfig)
	if err != nil {
		logutil.BgLogger().Error("failed to create etcd health probe client",
			zap.String("endpoint", endpoint),
			zap.Error(err))
		return
	}
	checker.healthyClients.Store(endpoint, newHealthyEtcdClient(client, time.Now()))
}

func (checker *etcdHealthChecker) removeClient(endpoint string) {
	client, ok := checker.healthyClients.LoadAndDelete(endpoint)
	if ok {
		if err := client.(*healthyEtcdClient).Close(); err != nil {
			logutil.BgLogger().Warn("failed to close etcd health probe client",
				zap.String("endpoint", endpoint),
				zap.Error(err))
		}
	}
	checker.evictedEndpoints.Delete(endpoint)
}

func (checker *etcdHealthChecker) syncURLs(ctx context.Context) []string {
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), etcdHealthCheckTimeout)
	defer cancel()
	response, err := clientv3.RetryClusterClient(checker.client).MemberList(ctx, &etcdserverpb.MemberListRequest{Linearizable: false})
	if err != nil {
		logutil.BgLogger().Warn("failed to list etcd members", zap.Error(err))
		return nil
	}

	endpoints := make([]string, 0, len(response.Members))
	for _, member := range response.Members {
		if len(member.Name) == 0 || member.IsLearner {
			continue
		}
		endpoints = append(endpoints, member.ClientURLs...)
	}
	return endpoints
}

func equivalentStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[string]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	return true
}
