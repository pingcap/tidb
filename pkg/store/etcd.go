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
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// NewEtcdCli creates a new clientv3.Client from store if the store support it.
// the returned client might be nil.
// TODO currently uni-store/mock-tikv/tikv all implements EtcdBackend while they don't support actually.
// refactor this part.
func NewEtcdCli(store kv.Storage) (*clientv3.Client, error) {
	etcdStore, addrs, err := GetEtcdAddrs(store)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, nil
	}
	cli, err := NewEtcdCliWithAddrs(addrs, etcdStore)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

// GetEtcdAddrs gets the etcd addrs from store if the store support it.
func GetEtcdAddrs(store kv.Storage) (kv.EtcdBackend, []string, error) {
	etcdStore, ok := store.(kv.EtcdBackend)
	if !ok {
		return nil, nil, nil
	}
	addrs, err := etcdStore.EtcdAddrs()
	if err != nil {
		return nil, nil, err
	}
	return etcdStore, addrs, nil
}

// NewEtcdCliWithAddrs creates a new clientv3.Client with specified addrs and etcd backend.
func NewEtcdCliWithAddrs(addrs []string, ebd kv.EtcdBackend) (*clientv3.Client, error) {
	cfg := config.GetGlobalConfig()
	etcdLogCfg := zap.NewProductionConfig()
	etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	backoffCfg := backoff.DefaultConfig
	backoffCfg.MaxDelay = 3 * time.Second
	cli, err := clientv3.New(clientv3.Config{
		LogConfig:        &etcdLogCfg,
		Endpoints:        addrs,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoffCfg,
			}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
			}),
		},
		TLS: ebd.TLSConfig(),
	})
	return cli, err
}
