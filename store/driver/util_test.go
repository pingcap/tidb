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
	"flag"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/tikv/client-go/v2/tikv"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	tikv.EnableFailpoints()
	TestingT(t)
}

var (
	pdAddrs  = flag.String("pd-addrs", "127.0.0.1:2379", "pd addrs")
	withTiKV = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
)

// NewTestStore creates a kv.Storage for testing purpose.
func NewTestStore(c *C) kv.Storage {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *withTiKV {
		var d TiKVDriver
		store, err := d.Open(fmt.Sprintf("tikv://%s", *pdAddrs))
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
	coprStore, err := copr.NewStore(store, nil)
	c.Assert(err, IsNil)
	return &tikvStore{KVStore: store, coprStore: coprStore}
}

func clearStorage(store kv.Storage) error {
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
