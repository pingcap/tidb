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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/go-themis/oracle"
	"github.com/pingcap/go-themis/oracle/oracles"
	"github.com/pingcap/tidb/kv"
)

const (
	// hbaseColFamily is the hbase column family name.
	hbaseColFamily = "f"
	// hbaseQualifier is the hbase column name.
	hbaseQualifier = "q"
	// hbaseFmlAndQual is a shortcut.
	hbaseFmlAndQual = hbaseColFamily + ":" + hbaseQualifier
	// fix length conn pool
	hbaseConnPoolSize = 10
)

var (
	hbaseColFamilyBytes = []byte(hbaseColFamily)
	hbaseQualifierBytes = []byte(hbaseQualifier)
)

var (
	_ kv.Storage = (*hbaseStore)(nil)
)

var (
	// ErrInvalidDSN is returned when store dsn is invalid.
	ErrInvalidDSN = errors.New("invalid dsn")
)

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*hbaseStore
}

var mc storeCache

func init() {
	mc.cache = make(map[string]*hbaseStore)
	rand.Seed(time.Now().UnixNano())
}

type hbaseStore struct {
	mu        sync.Mutex
	dsn       string
	storeName string
	oracle    oracle.Oracle
	conns     []hbase.HBaseClient
}

func (s *hbaseStore) getHBaseClient() hbase.HBaseClient {
	// return hbase connection randomly
	return s.conns[rand.Intn(hbaseConnPoolSize)]
}

func (s *hbaseStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hbaseCli := s.getHBaseClient()
	t, err := themis.NewTxn(hbaseCli, s.oracle)
	if err != nil {
		return nil, errors.Trace(err)
	}
	txn := newHbaseTxn(t, s.storeName)
	return txn, nil
}

func (s *hbaseStore) GetSnapshot(ver kv.Version) (kv.MvccSnapshot, error) {
	hbaseCli := s.getHBaseClient()
	t, err := themis.NewTxn(hbaseCli, s.oracle)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newHbaseSnapshot(t, s.storeName), nil
}

func (s *hbaseStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.cache, s.dsn)

	var err error
	for _, conn := range s.conns {
		err = conn.Close()
		if err != nil {
			log.Error(err)
		}
	}
	// return last error
	return err
}

func (s *hbaseStore) UUID() string {
	return fmt.Sprintf("hbase.%s.%s", s.storeName, s.dsn)
}

func (s *hbaseStore) CurrentVersion() (kv.Version, error) {
	hbaseCli := s.getHBaseClient()
	t, err := themis.NewTxn(hbaseCli, s.oracle)
	if err != nil {
		return kv.Version{Ver: 0}, errors.Trace(err)
	}
	defer t.Release()

	return kv.Version{Ver: t.GetStartTS()}, nil
}

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates an HBase storage with given dsn, format should be 'zk1,zk2,zk3|tsoaddr:port/tblName'.
// If tsoAddr is not provided, it will use a local oracle instead.
func (d Driver) Open(dsn string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if store, ok := mc.cache[dsn]; ok {
		// TODO: check the cache store has the same engine with this Driver.
		return store, nil
	}

	zks, oracleAddr, tableName, err := parseDSN(dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// create buffered HBase connections, HBaseClient is goroutine-safe, so
	// it's OK to redistribute to transactions.
	conns := make([]hbase.HBaseClient, 0, hbaseConnPoolSize)
	for i := 0; i < hbaseConnPoolSize; i++ {
		c, err := hbase.NewClient(zks, "/hbase")
		if err != nil {
			return nil, errors.Trace(err)
		}
		conns = append(conns, c)
	}

	c := conns[0]
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

	var ora oracle.Oracle
	if len(oracleAddr) == 0 {
		ora = oracles.NewLocalOracle()
	} else {
		ora = oracles.NewRemoteOracle(oracleAddr)
	}

	s := &hbaseStore{
		dsn:       dsn,
		storeName: tableName,
		oracle:    ora,
		conns:     conns,
	}
	mc.cache[dsn] = s
	return s, nil
}

func parseDSN(dsn string) (zks []string, oracleAddr, tableName string, err error) {
	pos := strings.LastIndex(dsn, "/")
	if pos == -1 {
		err = errors.Trace(ErrInvalidDSN)
		return
	}
	tableName = dsn[pos+1:]
	addrs := dsn[:pos]

	pos = strings.LastIndex(addrs, "|")
	if pos != -1 {
		oracleAddr = addrs[pos+1:]
		addrs = addrs[:pos]
	}
	zks = strings.Split(addrs, ",")
	return
}
