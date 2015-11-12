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

	"github.com/juju/errors"
	"github.com/pingcap/go-hbase"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/tidb/kv"
)

const (
	// hbaseColFamily is the hbase column family name.
	hbaseColFamily = "f"
	// hbaseQualifier is the hbase column name.
	hbaseQualifier = "q"
	// hbaseFmlAndQual is a shortcut.
	hbaseFmlAndQual = hbaseColFamily + ":" + hbaseQualifier
)

var (
	hbaseColFamilyBytes = []byte(hbaseColFamily)
	hbaseQualifierBytes = []byte(hbaseQualifier)
)

var (
	_ kv.Storage = (*hbaseStore)(nil)
)

var (
	// ErrZkInvalid is returned when zookeeper info is invalid.
	ErrZkInvalid = errors.New("zk info invalid")
)

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*hbaseStore
}

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

func (s *hbaseStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t := themis.NewTxn(s.cli)
	txn := newHbaseTxn(t, s.storeName)
	txn.UnionStore = kv.NewUnionStore(newHbaseSnapshot(t, s.storeName))
	return txn, nil
}

func (s *hbaseStore) GetSnapshot(ver kv.Version) (kv.MvccSnapshot, error) {
	t := themis.NewTxn(s.cli)
	return newHbaseSnapshot(t, s.storeName), nil
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

func (s *hbaseStore) CurrentVersion() (kv.Version, error) {
	t := themis.NewTxn(s.cli)
	defer t.Release()

	return kv.Version{Ver: t.GetStartTS()}, nil
}

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates a storage database with given path.
func (d Driver) Open(zkInfo string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if len(zkInfo) == 0 {
		return nil, errors.Trace(ErrZkInvalid)
	}
	pos := strings.LastIndex(zkInfo, "/")
	if pos == -1 {
		return nil, errors.Trace(ErrZkInvalid)
	}
	tableName := zkInfo[pos+1:]
	zks := strings.Split(zkInfo[:pos], ",")

	if store, ok := mc.cache[zkInfo]; ok {
		// TODO: check the cache store has the same engine with this Driver.
		return store, nil
	}

	c, err := hbase.NewClient(zks, "/hbase")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !c.TableExists(tableName) {
		// Create new hbase table for store.
		t := hbase.NewTableDesciptor(hbase.NewTableNameWithDefaultNS(tableName))
		cf := hbase.NewColumnFamilyDescriptor(hbaseColFamily)
		cf.AddStrAddr("THEMIS_ENABLE", "true")
		t.AddColumnDesc(cf)
		//TODO: specify split?
		if err := c.CreateTable(t, nil); err != nil {
			return nil, errors.Trace(err)
		}
	}
	s := &hbaseStore{
		zkInfo:    zkInfo,
		storeName: tableName,
		cli:       c,
	}
	mc.cache[zkInfo] = s
	return s, nil
}
