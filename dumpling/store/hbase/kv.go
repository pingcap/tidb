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
	"net/url"
	"path/filepath"
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
	mu         sync.Mutex
	uuid       string
	storeName  string
	oracleAddr string
	oracle     oracle.Oracle
	conns      []hbase.HBaseClient
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

func (s *hbaseStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
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

	delete(mc.cache, s.uuid)

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
	return s.uuid
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

// Open opens or creates an HBase storage with given path.
//
// The format of path should be 'hbase://zk1,zk2,zk3/table[?tso=host:port]'.
// If tso is not provided, it will use a local oracle instead. (for test only)
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	zks, oracleAddr, tableName, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	uuid := fmt.Sprintf("hbase-%v-%v", zks, tableName)
	if store, ok := mc.cache[uuid]; ok {
		if oracleAddr != store.oracleAddr {
			err = errors.Errorf("hbase: store(%s) is opened with a different tso, old: %v, new: %v", uuid, store.oracleAddr, oracleAddr)
			log.Warn(errors.ErrorStack(err))
			return nil, err
		}
		return store, nil
	}

	// create buffered HBase connections, HBaseClient is goroutine-safe, so
	// it's OK to redistribute to transactions.
	conns := make([]hbase.HBaseClient, 0, hbaseConnPoolSize)
	for i := 0; i < hbaseConnPoolSize; i++ {
		var c hbase.HBaseClient
		c, err = hbase.NewClient(zks, "/hbase")
		if err != nil {
			return nil, errors.Trace(err)
		}
		conns = append(conns, c)
	}

	c := conns[0]
	var b bool
	b, err = c.TableExists(tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !b {
		// Create new hbase table for store.
		t := hbase.NewTableDesciptor(tableName)
		cf := hbase.NewColumnFamilyDescriptor(hbaseColFamily)
		cf.AddAttr("THEMIS_ENABLE", "true")
		t.AddColumnDesc(cf)
		//TODO: specify split?
		if err := c.CreateTable(t, nil); err != nil {
			return nil, errors.Trace(err)
		}
	}

	var ora oracle.Oracle
	if len(oracleAddr) == 0 {
		log.Warnf("hbase: store(%s) is using local oracle(for test only)", uuid)
		ora = oracles.NewLocalOracle()
	} else {
		ora = oracles.NewRemoteOracle(oracleAddr)
	}

	s := &hbaseStore{
		uuid:       uuid,
		storeName:  tableName,
		oracleAddr: oracleAddr,
		oracle:     ora,
		conns:      conns,
	}
	mc.cache[uuid] = s
	return s, nil
}

func parsePath(path string) (zks []string, oracleAddr, tableName string, err error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, "", "", errors.Trace(err)
	}
	if strings.ToLower(u.Scheme) != "hbase" {
		return nil, "", "", errors.Trace(ErrInvalidDSN)
	}
	p, tableName := filepath.Split(u.Path)
	if p != "/" {
		return nil, "", "", errors.Trace(ErrInvalidDSN)
	}
	zks = strings.Split(u.Host, ",")
	oracleAddr = u.Query().Get("tso")
	return zks, oracleAddr, tableName, nil
}
