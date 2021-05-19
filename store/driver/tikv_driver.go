// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/copr"
	derr "github.com/pingcap/tidb/store/driver/error"
	txn_driver "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/util"
	"github.com/pingcap/tidb/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type storeCache struct {
	sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

func init() {
	mc.cache = make(map[string]*tikvStore)
	rand.Seed(time.Now().UnixNano())
}

// Option is a function that changes some config of Driver
type Option func(*TiKVDriver)

// WithSecurity changes the config.Security used by tikv driver.
func WithSecurity(s config.Security) Option {
	return func(c *TiKVDriver) {
		c.security = s
	}
}

// WithTiKVClientConfig changes the config.TiKVClient used by tikv driver.
func WithTiKVClientConfig(client config.TiKVClient) Option {
	return func(c *TiKVDriver) {
		c.tikvConfig = client
	}
}

// WithTxnLocalLatches changes the config.TxnLocalLatches used by tikv driver.
func WithTxnLocalLatches(t config.TxnLocalLatches) Option {
	return func(c *TiKVDriver) {
		c.txnLocalLatches = t
	}
}

// WithPDClientConfig changes the config.PDClient used by tikv driver.
func WithPDClientConfig(client config.PDClient) Option {
	return func(c *TiKVDriver) {
		c.pdConfig = client
	}
}

// TiKVDriver implements engine TiKV.
type TiKVDriver struct {
	pdConfig        config.PDClient
	security        config.Security
	tikvConfig      config.TiKVClient
	txnLocalLatches config.TxnLocalLatches
}

// Open opens or creates an TiKV storage with given path using global config.
// Path example: tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func (d TiKVDriver) Open(path string) (kv.Storage, error) {
	return d.OpenWithOptions(path)
}

func (d *TiKVDriver) setDefaultAndOptions(options ...Option) {
	tidbCfg := config.GetGlobalConfig()
	d.pdConfig = tidbCfg.PDClient
	d.security = tidbCfg.Security
	d.tikvConfig = tidbCfg.TiKVClient
	d.txnLocalLatches = tidbCfg.TxnLocalLatches
	for _, f := range options {
		f(d)
	}
}

// OpenWithOptions is used by other program that use tidb as a library, to avoid modifying GlobalConfig
// unspecified options will be set to global config
func (d TiKVDriver) OpenWithOptions(path string, options ...Option) (kv.Storage, error) {
	mc.Lock()
	defer mc.Unlock()
	d.setDefaultAndOptions(options...)
	etcdAddrs, disableGC, err := config.ParsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   d.security.ClusterSSLCA,
		CertPath: d.security.ClusterSSLCert,
		KeyPath:  d.security.ClusterSSLKey,
	},
		pd.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(d.tikvConfig.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(d.tikvConfig.GrpcKeepAliveTimeout) * time.Second,
			}),
		),
		pd.WithCustomTimeoutOption(time.Duration(d.pdConfig.PDServerTimeout)*time.Second),
		pd.WithForwardingOption(config.GetGlobalConfig().EnableForwarding))
	pdCli = util.InterceptedPDClient{Client: pdCli}

	if err != nil {
		return nil, errors.Trace(err)
	}

	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	tlsConfig, err := d.security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := tikv.NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdClient := tikv.CodecPDClient{Client: pdCli}
	s, err := tikv.NewKVStore(uuid, &pdClient, spkv, tikv.NewRPCClient(d.security))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if d.txnLocalLatches.Enabled {
		s.EnableTxnLocalLatches(d.txnLocalLatches.Capacity)
	}
	coprCacheConfig := &config.GetGlobalConfig().TiKVClient.CoprCache
	coprStore, err := copr.NewStore(s, coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	store := &tikvStore{
		KVStore:   s,
		etcdAddrs: etcdAddrs,
		tlsConfig: tlsConfig,
		memCache:  kv.NewCacheDB(),
		pdClient:  &pdClient,
		enableGC:  !disableGC,
		coprStore: coprStore,
	}

	mc.cache[uuid] = store
	return store, nil
}

type tikvStore struct {
	*tikv.KVStore
	etcdAddrs []string
	tlsConfig *tls.Config
	memCache  kv.MemManager // this is used to query from memory
	pdClient  pd.Client
	enableGC  bool
	gcWorker  *gcworker.GCWorker
	coprStore *copr.Store
}

// Name gets the name of the storage engine
func (s *tikvStore) Name() string {
	return "TiKV"
}

// Describe returns of brief introduction of the storage
func (s *tikvStore) Describe() string {
	return "TiKV is a distributed transactional key-value database"
}

var (
	ldflagGetEtcdAddrsFromConfig = "0" // 1:Yes, otherwise:No
)

const getAllMembersBackoff = 5000

// EtcdAddrs returns etcd server addresses.
func (s *tikvStore) EtcdAddrs() ([]string, error) {
	if s.etcdAddrs == nil {
		return nil, nil
	}

	if ldflagGetEtcdAddrsFromConfig == "1" {
		// For automated test purpose.
		// To manipulate connection to etcd by mandatorily setting path to a proxy.
		cfg := config.GetGlobalConfig()
		return strings.Split(cfg.Path, ","), nil
	}

	ctx := context.Background()
	bo := tikv.NewBackoffer(ctx, getAllMembersBackoff)
	etcdAddrs := make([]string, 0)
	pdClient := s.GetPDClient()
	if pdClient == nil {
		return nil, errors.New("Etcd client not found")
	}
	for {
		members, err := pdClient.GetAllMembers(ctx)
		if err != nil {
			err := bo.Backoff(tikv.BoRegionMiss, err)
			if err != nil {
				return nil, err
			}
			continue
		}
		for _, member := range members {
			if len(member.ClientUrls) > 0 {
				u, err := url.Parse(member.ClientUrls[0])
				if err != nil {
					logutil.BgLogger().Error("fail to parse client url from pd members", zap.String("client_url", member.ClientUrls[0]), zap.Error(err))
					return nil, err
				}
				etcdAddrs = append(etcdAddrs, u.Host)
			}
		}
		return etcdAddrs, nil
	}
}

// TLSConfig returns the tls config to connect to etcd.
func (s *tikvStore) TLSConfig() *tls.Config {
	return s.tlsConfig
}

// StartGCWorker starts GC worker, it's called in BootstrapSession, don't call this function more than once.
func (s *tikvStore) StartGCWorker() error {
	if !s.enableGC {
		return nil
	}

	gcWorker, err := gcworker.NewGCWorker(s, s.pdClient)
	if err != nil {
		return derr.ToTiDBErr(err)
	}
	gcWorker.Start()
	s.gcWorker = gcWorker
	return nil
}

func (s *tikvStore) GetClient() kv.Client {
	return s.coprStore.GetClient()
}

func (s *tikvStore) GetMPPClient() kv.MPPClient {
	return s.coprStore.GetMPPClient()
}

// Close and unregister the store.
func (s *tikvStore) Close() error {
	mc.Lock()
	defer mc.Unlock()
	delete(mc.cache, s.UUID())
	if s.gcWorker != nil {
		s.gcWorker.Close()
	}
	s.coprStore.Close()
	err := s.KVStore.Close()
	return derr.ToTiDBErr(err)
}

// GetMemCache return memory manager of the storage
func (s *tikvStore) GetMemCache() kv.MemManager {
	return s.memCache
}

// Begin a global transaction.
func (s *tikvStore) Begin() (kv.Transaction, error) {
	txn, err := s.KVStore.Begin()
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	return txn_driver.NewTiKVTxn(txn), err
}

// BeginWithOption begins a transaction with given option
func (s *tikvStore) BeginWithOption(option tikv.StartTSOption) (kv.Transaction, error) {
	txn, err := s.KVStore.BeginWithOption(option)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	return txn_driver.NewTiKVTxn(txn), err
}

// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
func (s *tikvStore) GetSnapshot(ver kv.Version) kv.Snapshot {
	return txn_driver.NewSnapshot(s.KVStore.GetSnapshot(ver.Ver))
}

// CurrentVersion returns current max committed version with the given txnScope (local or global).
func (s *tikvStore) CurrentVersion(txnScope string) (kv.Version, error) {
	ver, err := s.KVStore.CurrentTimestamp(txnScope)
	return kv.NewVersion(ver), derr.ToTiDBErr(err)
}

// ShowStatus returns the specified status of the storage
func (s *tikvStore) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, kv.ErrNotImplemented
}
