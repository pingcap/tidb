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

package tikv_test

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	txndriver "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/util/codec"
	pd "github.com/tikv/pd/client"
)

var (
	withTiKVGlobalLock sync.RWMutex
	WithTiKV           = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
	pdAddrs            = flag.String("pd-addrs", "127.0.0.1:2379", "pd addrs")
)

// NewTestStore creates a KVStore for testing purpose.
func NewTestStore(c *C) *tikv.KVStore {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *WithTiKV {
		addrs := strings.Split(*pdAddrs, ",")
		pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
		c.Assert(err, IsNil)
		var securityConfig config.Security
		tlsConfig, err := securityConfig.ToTLSConfig()
		c.Assert(err, IsNil)
		spKV, err := tikv.NewEtcdSafePointKV(addrs, tlsConfig)
		c.Assert(err, IsNil)
		store, err := tikv.NewKVStore("test-store", &tikv.CodecPDClient{Client: pdClient}, spKV, tikv.NewRPCClient(securityConfig))
		c.Assert(err, IsNil)
		err = clearStorage(store)
		c.Assert(err, IsNil)
		return store
	}
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	return store
}

func clearStorage(store *tikv.KVStore) error {
	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	iter, err := txn.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	for iter.Valid() {
		txn.Delete(iter.Key())
		if err := iter.Next(); err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Commit(context.Background())
}

// OneByOneSuite is a suite, When with-tikv flag is true, there is only one storage, so the test suite have to run one by one.
type OneByOneSuite struct{}

func (s *OneByOneSuite) SetUpSuite(c *C) {
	if *WithTiKV {
		withTiKVGlobalLock.Lock()
	} else {
		withTiKVGlobalLock.RLock()
	}
}

func (s *OneByOneSuite) TearDownSuite(c *C) {
	if *WithTiKV {
		withTiKVGlobalLock.Unlock()
	} else {
		withTiKVGlobalLock.RUnlock()
	}
}

func encodeKey(prefix, s string) []byte {
	return codec.EncodeBytes(nil, []byte(fmt.Sprintf("%s_%s", prefix, s)))
}

func valueBytes(n int) []byte {
	return []byte(fmt.Sprintf("value%d", n))
}

// s08d is for returning format string "%s%08d" to keep string sorted.
// e.g.: "0002" < "0011", otherwise "2" > "11"
func s08d(prefix string, n int) string {
	return fmt.Sprintf("%s%08d", prefix, n)
}

func toTiDBTxn(txn *tikv.TxnProbe) kv.Transaction {
	return txndriver.NewTiKVTxn(txn.KVTxn)
}

func toTiDBKeys(keys [][]byte) []kv.Key {
	kvKeys := *(*[]kv.Key)(unsafe.Pointer(&keys))
	return kvKeys
}
