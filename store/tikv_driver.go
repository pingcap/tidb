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

package store

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
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/util/execdetails"
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

// TiKVDriver implements engine TiKV.
type TiKVDriver struct {
}

// Open opens or creates an TiKV storage with given path.
// Path example: tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func (d TiKVDriver) Open(path string) (kv.Storage, error) {
	mc.Lock()
	defer mc.Unlock()
	security := config.GetGlobalConfig().Security
	pdConfig := config.GetGlobalConfig().PDClient
	tikvConfig := config.GetGlobalConfig().TiKVClient
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	etcdAddrs, disableGC, err := config.ParsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	}, pd.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(tikvConfig.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(tikvConfig.GrpcKeepAliveTimeout) * time.Second,
		}),
	), pd.WithCustomTimeoutOption(time.Duration(pdConfig.PDServerTimeout)*time.Second))
	pdCli = execdetails.InterceptedPDClient{Client: pdCli}

	if err != nil {
		return nil, errors.Trace(err)
	}

	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := tikv.NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	coprCacheConfig := &config.GetGlobalConfig().TiKVClient.CoprCache
	s, err := tikv.NewKVStore(uuid, &tikv.CodecPDClient{Client: pdCli}, spkv, tikv.NewRPCClient(security), !disableGC, coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if txnLocalLatches.Enabled {
		s.EnableTxnLocalLatches(txnLocalLatches.Capacity)
	}

	store := &tikvStore{
		KVStore:   s,
		etcdAddrs: etcdAddrs,
		tlsConfig: tlsConfig,
	}

	mc.cache[uuid] = store
	return store, nil
}

type tikvStore struct {
	*tikv.KVStore
	etcdAddrs []string
	tlsConfig *tls.Config
}

var (
	ldflagGetEtcdAddrsFromConfig = "0" // 1:Yes, otherwise:No
)

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
	bo := tikv.NewBackoffer(ctx, tikv.GetAllMembersBackoff)
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

// Close and unregister the store.
func (s *tikvStore) Close() error {
	mc.Lock()
	defer mc.Unlock()
	delete(mc.cache, s.UUID())
	return s.KVStore.Close()
}

// TLSConfig returns the tls config to connect to etcd.
func (s *tikvStore) TLSConfig() *tls.Config {
	return s.tlsConfig
}
