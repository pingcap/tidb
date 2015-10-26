// Copyright 2015 PingCAP, Inc.
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

package hbasekv

import (
	"strings"

	"sync"

	"github.com/c4pt0r/go-hbase"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/tidb/kv"
)

const (
	// ColFamily is the hbase col family name.
	ColFamily = "f"
	// Qualifier is the hbase col name.
	Qualifier = "q"
	// FmlAndQual is a shortcut.
	FmlAndQual = ColFamily + ":" + Qualifier
)

var (
	_ kv.Storage = (*hbaseStore)(nil)
)

var (
	// ErrHasNotZk is used when has not zookeeper info.
	ErrHasNotZk = errors.New("Error: has not zookeeper info")
)

var mc storeCache

func init() {
	mc.cache = make(map[string]*hbaseStore)
}

type hbaseStore struct {
	mu        sync.Mutex
	zkInfo    string
	storeName string
	cli       hbase.HBaseClient
}

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*hbaseStore
}

func (s *hbaseStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t := themis.NewTxn(s.cli)
	txn := newHbaseTxn(t, s.storeName)
	txn.UnionStore = kv.NewUnionStore(&hbaseSnapshot{t, s.storeName})
	return txn, nil
}

func newHbaseTxn(t *themis.Txn, storeName string) *hbaseTxn {
	return &hbaseTxn{
		Txn:        t,
		valid:      true,
		storeName:  storeName,
		tID:        t.GetStartTS(),
		UnionStore: kv.NewUnionStore(&hbaseSnapshot{t, storeName}),
	}
}

func (s *hbaseStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.cache, s.zkInfo)
	return s.cli.Close()
}

func (s *hbaseStore) UUID() string {
	return "hbase." + s.storeName + "." + s.zkInfo
}

func (s *hbaseStore) GetSnapshot() (kv.MvccSnapshot, error) {
	return nil, nil
}

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates a storage database with given path.
func (d Driver) Open(zkInfo string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if len(zkInfo) == 0 {
		log.Error("db uri is error, has not zk info")
		return nil, ErrHasNotZk
	}

	if store, ok := mc.cache[zkInfo]; ok {
		// TODO: check the cache store has the same engine with this Driver.
		log.Info("cache store", zkInfo)
		return store, nil
	}

	zks := strings.Split(zkInfo, ",")
	c, err := hbase.NewClient(zks, "/hbase")
	if err != nil {
		return nil, errors.Trace(err)
	}

	storeName := "tidb"
	if !c.TableExists(storeName) {
		log.Warn("auto create table:" + storeName)
		// create new hbase table for store
		t := hbase.NewTableDesciptor(hbase.NewTableNameWithDefaultNS(storeName))
		cf := hbase.NewColumnFamilyDescriptor(ColFamily)
		cf.AddStrAddr("THEMIS_ENABLE", "true")
		t.AddColumnDesc(cf)
		//TODO: specify split?
		err := c.CreateTable(t, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	s := &hbaseStore{
		zkInfo:    zkInfo,
		storeName: storeName,
		cli:       c,
	}
	mc.cache[zkInfo] = s
	return s, nil
}
