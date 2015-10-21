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

	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/tidb/kv"
	"github.com/juju/errors"
	"sync"
)

const (
	// ColFamily is the hbase col family name.
	ColFamily = "f"
	// Qualifier is the hbase col name.
	Qualifier = "q"
	FmlAndQual = ColFamily+":"+Qualifier
)

var _ kv.Storage = (*hbaseStore)(nil)

type hbaseStore struct {
	mu    	  sync.Mutex
	zkInfo    string
	storeName string
	cli       hbase.HBaseClient
}

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*hbaseStore
}

var mc storeCache
func init() {
	mc.cache = make(map[string]*hbaseStore)
}

func (s *hbaseStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cli == nil {
		return nil, errors.New("error zk connection")
	}

	t := themis.NewTxn(s.cli)
	txn := newHbaseTxn(t, s.storeName)
	var err error
	txn.UnionStore, err = kv.NewUnionStore(&hbaseSnapshot{t, s.storeName})
	if err != nil {
		return nil, errors.Trace(err)
	}
	txn.UnionStore.TID = txn.tID
	return txn, nil
}

func newHbaseTxn(t *themis.Txn, storeName string) *hbaseTxn {
	return &hbaseTxn{
		Txn:       t,
		valid:     true,
		storeName: storeName,
		tID:       t.GetStartTS(),
	}
}

func (s *hbaseStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.cache, s.zkInfo)
	return s.Close()
}

func (s *hbaseStore) UUID() string {
	return "hbase." + s.storeName
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
		return nil, errors.New("must has zk info")
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
		zkInfo:   zkInfo,
		storeName: storeName,
		cli:       c,
	}
	mc.cache[zkInfo] = s
	return s, nil
}
