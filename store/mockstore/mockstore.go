// Copyright 2020 PingCAP, Inc.
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

package mockstore

import (
	"net/url"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

// MockTiKVDriver is in memory mock TiKV driver.
type MockTiKVDriver struct{}

// Open creates a MockTiKV storage.
func (d MockTiKVDriver) Open(path string) (kv.Storage, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !strings.EqualFold(u.Scheme, "mocktikv") {
		return nil, errors.Errorf("Uri scheme expected(mocktikv) but found (%s)", u.Scheme)
	}

	opts := []MockTiKVStoreOption{WithPath(u.Path), WithStoreType(MockTiKV)}
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	if txnLocalLatches.Enabled {
		opts = append(opts, WithTxnLocalLatches(txnLocalLatches.Capacity))
	}

	return NewMockStore(opts...)
}

// EmbedUnistoreDriver is in embedded unistore driver.
type EmbedUnistoreDriver struct{}

// Open creates a EmbedUnistore storage.
func (d EmbedUnistoreDriver) Open(path string) (kv.Storage, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !strings.EqualFold(u.Scheme, "unistore") {
		return nil, errors.Errorf("Uri scheme expected(unistore) but found (%s)", u.Scheme)
	}

	opts := []MockTiKVStoreOption{WithPath(u.Path), WithStoreType(EmbedUnistore)}
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	if txnLocalLatches.Enabled {
		opts = append(opts, WithTxnLocalLatches(txnLocalLatches.Capacity))
	}

	return NewMockStore(opts...)
}

// StoreType is the type of backend mock storage.
type StoreType uint8

const (
	// MockTiKV is the mock storage based on goleveldb.
	MockTiKV StoreType = iota
	// EmbedUnistore is the mock storage based on unistore.
	EmbedUnistore

	defaultStoreType = EmbedUnistore
)

type mockOptions struct {
	clusterInspector func(testutils.Cluster)
	clientHijacker   func(tikv.Client) tikv.Client
	pdClientHijacker func(pd.Client) pd.Client
	path             string
	txnLocalLatches  uint
	storeType        StoreType
}

// MockTiKVStoreOption is used to control some behavior of mock tikv.
type MockTiKVStoreOption func(*mockOptions)

// WithMultipleOptions merges multiple options into one option.
func WithMultipleOptions(opts ...MockTiKVStoreOption) MockTiKVStoreOption {
	return func(args *mockOptions) {
		for _, opt := range opts {
			opt(args)
		}
	}
}

// WithClientHijacker hijacks KV client's behavior, makes it easy to simulate the network
// problem between TiDB and TiKV.
func WithClientHijacker(hijacker func(tikv.Client) tikv.Client) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.clientHijacker = hijacker
	}
}

// WithPDClientHijacker hijacks PD client's behavior, makes it easy to simulate the network
// problem between TiDB and PD.
func WithPDClientHijacker(hijacker func(pd.Client) pd.Client) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.pdClientHijacker = hijacker
	}
}

// WithClusterInspector lets user to inspect the mock cluster handler.
func WithClusterInspector(inspector func(testutils.Cluster)) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.clusterInspector = inspector
	}
}

// WithStoreType lets user choose the backend storage's type.
func WithStoreType(tp StoreType) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.storeType = tp
	}
}

// WithPath specifies the mocktikv path.
func WithPath(path string) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.path = path
	}
}

// WithTxnLocalLatches enable txnLocalLatches, when capacity > 0.
func WithTxnLocalLatches(capacity uint) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.txnLocalLatches = capacity
	}
}

// NewMockStore creates a mocked tikv store, the path is the file path to store the data.
// If path is an empty string, a memory storage will be created.
func NewMockStore(options ...MockTiKVStoreOption) (kv.Storage, error) {
	opt := mockOptions{
		clusterInspector: func(c testutils.Cluster) {
			BootstrapWithSingleStore(c)
		},
		storeType: defaultStoreType,
	}
	for _, f := range options {
		f(&opt)
	}

	switch opt.storeType {
	case MockTiKV:
		return newMockTikvStore(&opt)
	case EmbedUnistore:
		return newUnistore(&opt)
	default:
		panic("unsupported mockstore")
	}
}

// BootstrapWithSingleStore initializes a Cluster with 1 Region and 1 Store.
func BootstrapWithSingleStore(cluster testutils.Cluster) (storeID, peerID, regionID uint64) {
	switch x := cluster.(type) {
	case *testutils.MockCluster:
		return testutils.BootstrapWithSingleStore(x)
	case *unistore.Cluster:
		return unistore.BootstrapWithSingleStore(x)
	default:
		panic("unsupported cluster type")
	}
}

// BootstrapWithMultiStores initializes a Cluster with 1 Region and n Stores.
func BootstrapWithMultiStores(cluster testutils.Cluster, n int) (storeIDs, peerIDs []uint64, regionID uint64, leaderPeer uint64) {
	switch x := cluster.(type) {
	case *testutils.MockCluster:
		return testutils.BootstrapWithMultiStores(x, n)
	case *unistore.Cluster:
		return unistore.BootstrapWithMultiStores(x, n)
	default:
		panic("unsupported cluster type")
	}
}

// BootstrapWithMultiRegions initializes a Cluster with multiple Regions and 1
// Store. The number of Regions will be len(splitKeys) + 1.
func BootstrapWithMultiRegions(cluster testutils.Cluster, splitKeys ...[]byte) (storeID uint64, regionIDs, peerIDs []uint64) {
	switch x := cluster.(type) {
	case *testutils.MockCluster:
		return testutils.BootstrapWithMultiRegions(x, splitKeys...)
	case *unistore.Cluster:
		return unistore.BootstrapWithMultiRegions(x, splitKeys...)
	default:
		panic("unsupported cluster type")
	}
}
