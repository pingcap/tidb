// Copyright 2018 PingCAP, Inc.
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

package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"
)

// Node organizes the ectd query result as a Trie tree
type Node struct {
	Childs map[string]*Node
	Value  []byte
}

// OpType is operation's type in etcd
type OpType string

var (
	// CreateOp is create operation type
	CreateOp OpType = "create"

	// UpdateOp is update operation type
	UpdateOp OpType = "update"

	// DeleteOp is delete operation type
	DeleteOp OpType = "delete"
)

// Operation represents an operation in etcd, include create, update and delete.
type Operation struct {
	Tp         OpType
	Key        string
	Value      string
	Opts       []clientv3.OpOption
	TTL        int64
	WithPrefix bool
}

// String implements Stringer interface.
func (o *Operation) String() string {
	return fmt.Sprintf("{Tp: %s, Key: %s, Value: %s, TTL: %d, WithPrefix: %v, Opts: %v}", o.Tp, o.Key, o.Value, o.TTL, o.WithPrefix, o.Opts)
}

// Client is a wrapped etcd client that support some simple method
type Client struct {
	client   *clientv3.Client
	rootPath string
}

// NewClient returns a wrapped etcd client
func NewClient(cli *clientv3.Client, root string) *Client {
	return &Client{
		client:   cli,
		rootPath: root,
	}
}

// NewClientFromCfg returns a wrapped etcd client
func NewClientFromCfg(endpoints []string, _ time.Duration, root string, security *tls.Config, source string) (*Client, error) {
	cli, err := CreateEtcdClient(security, endpoints, source)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Client{
		client:   cli,
		rootPath: root,
	}, nil
}

// Close shutdowns the connection to etcd
func (e *Client) Close() error {
	if err := e.client.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetClient returns client
func (e *Client) GetClient() *clientv3.Client {
	return e.client
}

// Create guarantees to set a key = value with some options(like ttl)
func (e *Client) Create(ctx context.Context, key string, val string, opts []clientv3.OpOption) (int64, error) {
	key = keyWithPrefix(e.rootPath, key)
	txnResp, err := e.client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), "=", 0),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return 0, errors.AlreadyExistsf("key %s in etcd", key)
	}

	if txnResp.Header != nil {
		return txnResp.Header.Revision, nil
	}

	// impossible to happen
	return 0, errors.New("revision is unknown")
}

// Get returns a key/value matchs the given key
func (e *Client) Get(ctx context.Context, key string) (value []byte, revision int64, err error) {
	key = keyWithPrefix(e.rootPath, key)
	resp, err := e.client.KV.Get(ctx, key)
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, -1, errors.NotFoundf("key %s in etcd", key)
	}

	return resp.Kvs[0].Value, resp.Header.Revision, nil
}

// Update updates a key/value.
// set ttl 0 to disable the Lease ttl feature
func (e *Client) Update(ctx context.Context, key string, val string, ttl int64) error {
	key = keyWithPrefix(e.rootPath, key)

	var opts []clientv3.OpOption
	if ttl > 0 {
		lcr, err := e.client.Lease.Grant(ctx, ttl)
		if err != nil {
			return errors.Trace(err)
		}

		opts = []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	}

	txnResp, err := e.client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), ">", 0),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()
	if err != nil {
		return errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return errors.NotFoundf("key %s in etcd", key)
	}

	return nil
}

// UpdateOrCreate updates a key/value, if the key does not exist then create, or update
func (e *Client) UpdateOrCreate(ctx context.Context, key string, val string, ttl int64) error {
	key = keyWithPrefix(e.rootPath, key)

	var opts []clientv3.OpOption
	if ttl > 0 {
		lcr, err := e.client.Lease.Grant(ctx, ttl)
		if err != nil {
			return errors.Trace(err)
		}

		opts = []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	}

	_, err := e.client.KV.Do(ctx, clientv3.OpPut(key, val, opts...))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// List returns the trie struct that constructed by the key/value with same prefix
func (e *Client) List(ctx context.Context, key string) (node *Node, revision int64, err error) {
	key = keyWithPrefix(e.rootPath, key)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	resp, err := e.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	root := new(Node)
	length := len(key)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if len(key) <= length {
			continue
		}

		keyTail := key[length:]
		tailNode := parseToDirTree(root, keyTail)
		tailNode.Value = kv.Value
	}

	return root, resp.Header.Revision, nil
}

// Delete deletes the key/values with matching prefix or key
func (e *Client) Delete(ctx context.Context, key string, withPrefix bool) error {
	key = keyWithPrefix(e.rootPath, key)
	var opts []clientv3.OpOption
	if withPrefix {
		opts = []clientv3.OpOption{clientv3.WithPrefix()}
	}

	_, err := e.client.KV.Delete(ctx, key, opts...)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Watch watchs the events of key with prefix.
func (e *Client) Watch(ctx context.Context, prefix string, revision int64) clientv3.WatchChan {
	if revision > 0 {
		return e.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
	}
	return e.client.Watch(ctx, prefix, clientv3.WithPrefix())
}

// DoTxn does some operation in one transaction.
// Note: should only have one opereration for one key, otherwise will get duplicate key error.
func (e *Client) DoTxn(ctx context.Context, operations []*Operation) (int64, error) {
	cmps := make([]clientv3.Cmp, 0, len(operations))
	ops := make([]clientv3.Op, 0, len(operations))

	for _, operation := range operations {
		operation.Key = keyWithPrefix(e.rootPath, operation.Key)

		if operation.TTL > 0 {
			if operation.Tp == DeleteOp {
				return 0, errors.Errorf("unexpected TTL in delete operation")
			}

			lcr, err := e.client.Lease.Grant(ctx, operation.TTL)
			if err != nil {
				return 0, errors.Trace(err)
			}
			operation.Opts = append(operation.Opts, clientv3.WithLease(lcr.ID))
		}

		if operation.WithPrefix {
			operation.Opts = append(operation.Opts, clientv3.WithPrefix())
		}

		switch operation.Tp {
		case CreateOp:
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(operation.Key), "=", 0))
			ops = append(ops, clientv3.OpPut(operation.Key, operation.Value, operation.Opts...))
		case UpdateOp:
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(operation.Key), ">", 0))
			ops = append(ops, clientv3.OpPut(operation.Key, operation.Value, operation.Opts...))
		case DeleteOp:
			ops = append(ops, clientv3.OpDelete(operation.Key, operation.Opts...))
		default:
			return 0, errors.Errorf("unknown operation type %s", operation.Tp)
		}
	}

	txnResp, err := e.client.KV.Txn(ctx).If(
		cmps...,
	).Then(
		ops...,
	).Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return 0, errors.Errorf("do transaction failed, operations: %+v", operations)
	}

	return txnResp.Header.Revision, nil
}

func parseToDirTree(root *Node, p string) *Node {
	pathDirs := strings.Split(p, "/")
	current := root
	var next *Node
	var ok bool

	for _, dir := range pathDirs {
		if current.Childs == nil {
			current.Childs = make(map[string]*Node)
		}

		next, ok = current.Childs[dir]
		if !ok {
			current.Childs[dir] = new(Node)
			next = current.Childs[dir]
		}

		current = next
	}

	return current
}

func keyWithPrefix(prefix, key string) string {
	if strings.HasPrefix(key, prefix) {
		return key
	}

	return path.Join(prefix, key)
}

// SetEtcdCliByNamespace is used to add an etcd namespace prefix before etcd path.
func SetEtcdCliByNamespace(cli *clientv3.Client, namespacePrefix string) {
	cli.KV = namespace.NewKV(cli.KV, namespacePrefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, namespacePrefix)
	cli.Lease = namespace.NewLease(cli.Lease, namespacePrefix)
}

const (
	// defaultEtcdClientTimeout is the default timeout for etcd client.
	defaultEtcdClientTimeout = 3 * time.Second

	// defaultDialKeepAliveTime is the time after which client pings the server to see if transport is alive.
	defaultDialKeepAliveTime = 10 * time.Second

	// defaultDialKeepAliveTimeout is the time that the client waits for a response for the
	// keep-alive probe. If the response is not received in this time, the connection is closed.
	defaultDialKeepAliveTimeout = 3 * time.Second

	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultSlowRequestTime 1s for the threshold for normal request, for those
	// longer then 1s, they are considered as slow requests.
	DefaultSlowRequestTime = time.Second

	healthyPath = "health"

	// MaxEtcdTxnOps is the max value of operations in an etcd txn. The default limit of etcd txn op is 128.
	// We use 120 here to leave some space for other operations.
	// See: https://github.com/etcd-io/etcd/blob/d3e43d4de6f6d9575b489dd7850a85e37e0f6b6c/server/embed/config.go#L61
	MaxEtcdTxnOps = 120
)

const (
	// etcdServerOfflineTimeout is the timeout for an unhealthy etcd endpoint to be offline from healthy checker.
	etcdServerOfflineTimeout = 30 * time.Minute
	// etcdServerDisconnectedTimeout is the timeout for an unhealthy etcd endpoint to be disconnected from healthy checker.
	etcdServerDisconnectedTimeout = 1 * time.Minute
)

func newClient(tlsConfig *tls.Config, endpoints ...string) (*clientv3.Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("empty etcd endpoints")
	}
	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientv3.New(clientv3.Config{
		AutoSyncInterval:     0,
		Endpoints:            endpoints,
		DialTimeout:          defaultEtcdClientTimeout,
		TLS:                  tlsConfig,
		LogConfig:            &lgc,
		DialKeepAliveTime:    defaultDialKeepAliveTime,
		DialKeepAliveTimeout: defaultDialKeepAliveTimeout,
	})
	return client, err
}

// CreateEtcdClient creates etcd v3 client with detecting endpoints.
func CreateEtcdClient(tlsConfig *tls.Config, endpoints []string, source string) (*clientv3.Client, error) {
	client, err := newClient(tlsConfig, endpoints...)
	if err != nil {
		return nil, err
	}

	tickerInterval := defaultDialKeepAliveTime
	initHealthChecker(tickerInterval, tlsConfig, client, source)

	return client, err
}

const pickedCountThreshold = 3

// healthyClient will wrap an etcd client and record its last health time.
// The etcd client inside will only maintain one connection to the etcd server
// to make sure each healthyClient could be used to check the health of a certain
// etcd endpoint without involving the load balancer of etcd client.
type healthyClient struct {
	*clientv3.Client
	lastHealth time.Time
}

// healthChecker is used to check the health of etcd endpoints. Inside the checker,
// we will maintain a map from each available etcd endpoint to its healthyClient.
type healthChecker struct {
	client         *clientv3.Client
	tlsConfig      *tls.Config
	healthyClients sync.Map
	evictedEps     sync.Map
	source         string
	tickerInterval time.Duration
}

// initHealthChecker initializes the health checker for etcd client.
func initHealthChecker(
	tickerInterval time.Duration,
	tlsConfig *tls.Config,
	client *clientv3.Client,
	source string,
) {
	healthChecker := &healthChecker{
		source:         source,
		tickerInterval: tickerInterval,
		tlsConfig:      tlsConfig,
		client:         client,
	}
	// A health checker has the same lifetime with the given etcd client.
	ctx := client.Ctx()
	// Sync etcd endpoints and check the last health time of each endpoint periodically.
	go healthChecker.syncer(ctx)
	// Inspect the health of each endpoint by reading the health key periodically.
	go healthChecker.inspector(ctx)
}

func (checker *healthChecker) syncer(ctx context.Context) {
	checker.update()
	ticker := time.NewTicker(checker.tickerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("etcd client is closed, exit the endpoint syncer goroutine",
				zap.String("source", checker.source))
			return
		case <-ticker.C:
			checker.update()
		}
	}
}

func (checker *healthChecker) inspector(ctx context.Context) {
	ticker := time.NewTicker(checker.tickerInterval)
	defer ticker.Stop()
	lastAvailable := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Info("etcd client is closed, exit the health inspector goroutine",
				zap.String("source", checker.source))
			checker.close()
			return
		case <-ticker.C:
			lastEps, pickedEps, changed := checker.patrol(ctx)
			if len(pickedEps) == 0 {
				// when no endpoint could be used, try to reset endpoints to update connect rather
				// than delete them to avoid there is no any endpoint in client.
				// Note: reset endpoints will trigger sub-connection closed, and then trigger reconnection.
				// Otherwise, the sub-connection will be retrying in gRPC layer and use exponential backoff,
				// and it cannot recover as soon as possible.
				if time.Since(lastAvailable) > etcdServerDisconnectedTimeout {
					log.Info("no available endpoint, try to reset endpoints",
						zap.Strings("last-endpoints", lastEps),
						zap.String("source", checker.source))
					resetClientEndpoints(checker.client, lastEps...)
				}
				continue
			}
			if changed {
				oldNum, newNum := len(lastEps), len(pickedEps)
				checker.client.SetEndpoints(pickedEps...)
				log.Info("update endpoints",
					zap.String("num-change", fmt.Sprintf("%d->%d", oldNum, newNum)),
					zap.Strings("last-endpoints", lastEps),
					zap.Strings("endpoints", checker.client.Endpoints()),
					zap.String("source", checker.source))
			}
			lastAvailable = time.Now()
		}
	}
}

func (checker *healthChecker) close() {
	checker.healthyClients.Range(func(_, value any) bool {
		healthyCli := value.(*healthyClient)
		err := healthyCli.Client.Close()
		if err != nil {
			log.Error("failed to close etcd client",
				zap.String("source", checker.source),
				zap.Error(err))
		}
		return true
	})
}

// Reset the etcd client endpoints to trigger reconnect.
func resetClientEndpoints(client *clientv3.Client, endpoints ...string) {
	client.SetEndpoints()
	client.SetEndpoints(endpoints...)
}

type healthProbe struct {
	ep   string
	took time.Duration
}

// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L105-L145
func (checker *healthChecker) patrol(ctx context.Context) ([]string, []string, bool) {
	var (
		count   = checker.clientCount()
		probeCh = make(chan healthProbe, count)
		wg      sync.WaitGroup
	)
	checker.healthyClients.Range(func(key, value any) bool {
		wg.Add(1)
		go func(key, value any) {
			defer wg.Done()
			var (
				ep         = key.(string)
				healthyCli = value.(*healthyClient)
				client     = healthyCli.Client
				start      = time.Now()
			)
			// Check the health of the endpoint.
			healthy := IsHealthy(ctx, client)
			took := time.Since(start)
			if !healthy {
				log.Warn("etcd endpoint is unhealthy",
					zap.String("endpoint", ep),
					zap.Duration("took", took),
					zap.String("source", checker.source))
				return
			}
			// If the endpoint is healthy, update its last health time.
			checker.storeClient(ep, client, start)
			// Send the healthy probe result to the channel.
			probeCh <- healthProbe{ep, took}
		}(key, value)
		return true
	})
	wg.Wait()
	close(probeCh)
	var (
		lastEps   = checker.client.Endpoints()
		pickedEps = checker.pickEps(probeCh)
	)
	if len(pickedEps) > 0 {
		checker.updateEvictedEps(lastEps, pickedEps)
		pickedEps = checker.filterEps(pickedEps)
	}
	return lastEps, pickedEps, !AreStringSlicesEquivalent(lastEps, pickedEps)
}

// Divide the acceptable latency range into several parts, and pick the endpoints which
// are in the first acceptable latency range. Currently, we only take the latency of the
// last health check into consideration, and maybe in the future we could introduce more
// factors to help improving the selection strategy.
func (checker *healthChecker) pickEps(probeCh <-chan healthProbe) []string {
	var (
		count     = len(probeCh)
		pickedEps = make([]string, 0, count)
	)
	if count == 0 {
		return pickedEps
	}
	// Consume the `probeCh` to build a reusable slice.
	probes := make([]healthProbe, 0, count)
	for probe := range probeCh {
		probes = append(probes, probe)
	}
	// Take the default value as an example, if we have 3 endpoints with latency like:
	//   - A: 175ms
	//   - B: 50ms
	//   - C: 2.5s
	// the distribution will be like:
	//   - [0, 1s) -> {A, B}
	//   - [1s, 2s)
	//   - [2s, 3s) -> {C}
	//   - ...
	//  - [9s, 10s)
	// Then the picked endpoints will be {A, B} and if C is in the last used endpoints, it will be evicted later.
	factor := int(DefaultRequestTimeout / DefaultSlowRequestTime)
	for i := 0; i < factor; i++ {
		minLatency, maxLatency := DefaultSlowRequestTime*time.Duration(i), DefaultSlowRequestTime*time.Duration(i+1)
		for _, probe := range probes {
			if minLatency <= probe.took && probe.took < maxLatency {
				log.Debug("pick healthy etcd endpoint within acceptable latency range",
					zap.Duration("min-latency", minLatency),
					zap.Duration("max-latency", maxLatency),
					zap.Duration("took", probe.took),
					zap.String("endpoint", probe.ep),
					zap.String("source", checker.source))
				pickedEps = append(pickedEps, probe.ep)
			}
		}
		if len(pickedEps) > 0 {
			break
		}
	}
	return pickedEps
}

func (checker *healthChecker) updateEvictedEps(lastEps, pickedEps []string) {
	// Create a set of picked endpoints for faster lookup
	pickedSet := make(map[string]bool, len(pickedEps))
	for _, ep := range pickedEps {
		pickedSet[ep] = true
	}
	// Reset the count to 0 if it's in `evictedEps` but not in `pickedEps`.
	checker.evictedEps.Range(func(key, value any) bool {
		ep := key.(string)
		count := value.(int)
		if count > 0 && !pickedSet[ep] {
			checker.evictedEps.Store(ep, 0)
			log.Info("reset evicted etcd endpoint picked count",
				zap.String("endpoint", ep),
				zap.Int("previous-count", count),
				zap.String("source", checker.source))
		}
		return true
	})
	// Find all endpoints which are in `lastEps` and `healthyClients` but not in `pickedEps`,
	// and add them to the `evictedEps`.
	for _, ep := range lastEps {
		if pickedSet[ep] {
			continue
		}
		if hc := checker.loadClient(ep); hc == nil {
			continue
		}
		checker.evictedEps.Store(ep, 0)
		log.Info("evicted etcd endpoint found",
			zap.String("endpoint", ep),
			zap.String("source", checker.source))
	}
	// Find all endpoints which are in both `pickedEps` and `evictedEps` to
	// increase their picked count.
	for _, ep := range pickedEps {
		if count, ok := checker.evictedEps.Load(ep); ok {
			// Increase the count the endpoint being picked continuously.
			checker.evictedEps.Store(ep, count.(int)+1)
			log.Info("evicted etcd endpoint picked again",
				zap.Int("picked-count-threshold", pickedCountThreshold),
				zap.Int("picked-count", count.(int)+1),
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
		}
	}
}

// Filter out the endpoints that are in evictedEps and have not been continuously picked
// for `pickedCountThreshold` times still, this is to ensure the evicted endpoints truly
// become available before adding them back to the client.
func (checker *healthChecker) filterEps(eps []string) []string {
	pickedEps := make([]string, 0, len(eps))
	for _, ep := range eps {
		if count, ok := checker.evictedEps.Load(ep); ok {
			if count.(int) < pickedCountThreshold {
				continue
			}
			checker.evictedEps.Delete(ep)
			log.Info("add evicted etcd endpoint back",
				zap.Int("picked-count-threshold", pickedCountThreshold),
				zap.Int("picked-count", count.(int)),
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
		}
		pickedEps = append(pickedEps, ep)
	}
	// If the pickedEps is empty, it means all endpoints are evicted,
	// to gain better availability, just use the original picked endpoints.
	if len(pickedEps) == 0 {
		log.Warn("all etcd endpoints are evicted, use the picked endpoints directly",
			zap.Strings("endpoints", eps),
			zap.String("source", checker.source))
		return eps
	}
	return pickedEps
}

func (checker *healthChecker) update() {
	eps := checker.syncURLs()
	if len(eps) == 0 {
		log.Warn("no available etcd endpoint returned by etcd cluster",
			zap.String("source", checker.source))
		return
	}
	epMap := make(map[string]struct{}, len(eps))
	for _, ep := range eps {
		epMap[ep] = struct{}{}
	}
	// Check if client exists:
	//   - If not, create one.
	//   - If exists, check if it's offline or disconnected for a long time.
	for ep := range epMap {
		client := checker.loadClient(ep)
		if client == nil {
			checker.initClient(ep)
			continue
		}
		since := time.Since(client.lastHealth)
		// Check if it's offline for a long time and try to remove it.
		if since > etcdServerOfflineTimeout {
			log.Info("etcd server might be offline, try to remove it",
				zap.Duration("since-last-health", since),
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
			checker.removeClient(ep)
			continue
		}
		// Check if it's disconnected for a long time and try to reconnect.
		if since > etcdServerDisconnectedTimeout {
			log.Info("etcd server might be disconnected, try to reconnect",
				zap.Duration("since-last-health", since),
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
			resetClientEndpoints(client.Client, ep)
		}
	}
	// Clean up the stale clients which are not in the etcd cluster anymore.
	checker.healthyClients.Range(func(key, _ any) bool {
		ep := key.(string)
		if _, ok := epMap[ep]; !ok {
			log.Info("remove stale etcd client",
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
			checker.removeClient(ep)
		}
		return true
	})
}

func (checker *healthChecker) clientCount() int {
	count := 0
	checker.healthyClients.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (checker *healthChecker) loadClient(ep string) *healthyClient {
	if client, ok := checker.healthyClients.Load(ep); ok {
		return client.(*healthyClient)
	}
	return nil
}

func (checker *healthChecker) initClient(ep string) {
	client, err := newClient(checker.tlsConfig, ep)
	if err != nil {
		log.Error("failed to create etcd healthy client",
			zap.String("endpoint", ep),
			zap.String("source", checker.source),
			zap.Error(err))
		return
	}
	checker.storeClient(ep, client, time.Now())
}

func (checker *healthChecker) storeClient(ep string, client *clientv3.Client, lastHealth time.Time) {
	checker.healthyClients.Store(ep, &healthyClient{
		Client:     client,
		lastHealth: lastHealth,
	})
}

func (checker *healthChecker) removeClient(ep string) {
	if client, ok := checker.healthyClients.LoadAndDelete(ep); ok {
		healthyCli := client.(*healthyClient)
		if err := healthyCli.Close(); err != nil {
			log.Error("failed to close etcd healthy client",
				zap.String("endpoint", ep),
				zap.String("source", checker.source),
				zap.Error(err))
		}
	}
	checker.evictedEps.Delete(ep)
}

// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/clientv3/client.go#L170-L183
func (checker *healthChecker) syncURLs() (eps []string) {
	resp, err := ListEtcdMembers(clientv3.WithRequireLeader(checker.client.Ctx()), checker.client)
	if err != nil {
		log.Error("failed to list members",
			zap.String("source", checker.source),
			zap.Error(err))
		return nil
	}
	for _, m := range resp.Members {
		if len(m.Name) == 0 || m.IsLearner {
			continue
		}
		eps = append(eps, m.ClientURLs...)
	}
	return eps
}

// IsHealthy checks if the etcd is healthy.
func IsHealthy(ctx context.Context, client *clientv3.Client) bool {
	timeout := DefaultRequestTimeout
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), timeout)
	defer cancel()
	_, err := client.Get(ctx, healthyPath)
	return err == nil
}

// ListEtcdMembers returns a list of internal etcd members.
func ListEtcdMembers(ctx context.Context, client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	newCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	return client.MemberList(newCtx)
}

// AreStringSlicesEquivalent checks if two string slices are equivalent.
// If the slices are of the same length and contain the same elements (but possibly in different order), the function returns true.
func AreStringSlicesEquivalent(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
