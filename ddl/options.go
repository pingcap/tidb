// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://wwm.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"go.etcd.io/etcd/clientv3"
)

// Option represents an option to initialize the DDL module
type Option func(*Options)

// Options represents all the options of the DDL module needs
type Options struct {
	EtcdCli   *clientv3.Client
	Store     kv.Storage
	InfoCache *infoschema.InfoCache
	Hook      Callback
	Lease     time.Duration
}

// WithEtcdClient specifies the `clientv3.Client` of DDL used to request the etcd service
func WithEtcdClient(client *clientv3.Client) Option {
	return func(options *Options) {
		options.EtcdCli = client
	}
}

// WithStore specifies the `kv.Storage` of DDL used to request the KV service
func WithStore(store kv.Storage) Option {
	return func(options *Options) {
		options.Store = store
	}
}

// WithInfoCache specifies the `infoschema.InfoCache`
func WithInfoCache(ic *infoschema.InfoCache) Option {
	return func(options *Options) {
		options.InfoCache = ic
	}
}

// WithHook specifies the `Callback` of DDL used to notify the outer module when events are triggered
func WithHook(callback Callback) Option {
	return func(options *Options) {
		options.Hook = callback
	}
}

// WithLease specifies the schema lease duration
func WithLease(lease time.Duration) Option {
	return func(options *Options) {
		options.Lease = lease
	}
}
