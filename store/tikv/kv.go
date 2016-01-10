// Copyright 2016 PingCAP, Inc.
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

package tikv

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
	"github.com/pingcap/go-themis/oracle"
	"github.com/pingcap/go-themis/oracle/oracles"
	"github.com/pingcap/tidb/kv"
)

const (
	// fix length conn pool
	tikvConnPoolSize = 10
)

var (
//_ kv.Storage = (*hbaseStore)(nil)
)

var (
	// ErrInvalidDSN is returned when store dsn is invalid.
	ErrInvalidDSN = errors.New("invalid dsn")
)

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates an TiKV storage with given path.
//
// The format of path should be 'tikv://zk1,zk2,zk3/table[?tso=local|zk]'.
// If tso is not provided, it will use a local oracle instead. (for test only)
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	zks, tso, tableName, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tso != tsoTypeLocal && tso != tsoTypeZK {
		return nil, errors.Trace(ErrInvalidDSN)
	}

	uuid := fmt.Sprintf("tikv-%v-%v", zks, tableName)
	if tso == tsoTypeLocal {
		log.Warnf("tikv: store(%s) is using local oracle(for test only)", uuid)
	}
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	// create buffered TiKV connections
	conns := make([]Client, 0, tikvConnPoolSize)
	for i := 0; i < tikvConnPoolSize; i++ {
		var c Client
		c, err = NewClient(strings.Split(zks, ","), "/tikv")
		if err != nil {
			return nil, errors.Trace(err)
		}
		conns = append(conns, c)
	}

	//c := conns[0]
	//var b bool
	//b, err = c.TableExists(tableName)
	//if err != nil {
	//return nil, errors.Trace(err)
	//}
	//if !b {
	//// Create new TiKV table for store.
	//}

	var ora oracle.Oracle
	switch tso {
	case tsoTypeLocal:
		ora = oracles.NewLocalOracle()
	case tsoTypeZK:
		ora = oracles.NewRemoteOracle(zks, tsoZKPath)
	}

	s := &tikvStore{
		uuid:      uuid,
		storeName: tableName,
		oracle:    ora,
		conns:     conns,
	}
	mc.cache[uuid] = s
	return s, nil
}

type tikvStore struct {
	mu        sync.Mutex
	uuid      string
	storeName string
	oracle    oracle.Oracle
	conns     []Client
}

func (s *tikvStore) getTiKVClient() Client {
	// return TiKV connection randomly
	return s.conns[rand.Intn(tikvConnPoolSize)]
}

func (s *tikvStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO: impl this
	return nil, nil

	//t, err := themis.NewTxn(hbaseCli, s.oracle)
	//if err != nil {
	//return nil, errors.Trace(err)
	//}
	//txn := newHbaseTxn(t, s.storeName)
	//return txn, nil
}

func (s *tikvStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	// TODO: impl this
	return nil, nil

	//hbaseCli := s.getHBaseClient()
	//t, err := themis.NewTxn(hbaseCli, s.oracle)
	//if err != nil {
	//return nil, errors.Trace(err)
	//}
	//return newHbaseSnapshot(t, s.storeName), nil
}

func (s *tikvStore) Close() error {
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

func (s *tikvStore) UUID() string {
	return s.uuid
}

func (s *tikvStore) CurrentVersion() (kv.Version, error) {
	// TODO: impl this
	return kv.Version{Ver: 0}, nil
	//tikvCli := s.getTiKVClient()
	//txn, err := NewTxn(tikvCli, s.oracle)
	//if err != nil {
	//return kv.Version{Ver: 0}, errors.Trace(err)
	//}
	//defer t.Release()

	//return kv.Version{Ver: t.GetStartTS()}, nil
}

const (
	tsoTypeLocal = "local"
	tsoTypeZK    = "zk"

	tsoZKPath = "/zk/tso"
)

func parsePath(path string) (zks, tso, tableName string, err error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", "", "", errors.Trace(err)
	}
	if strings.ToLower(u.Scheme) != "hbase" {
		return "", "", "", errors.Trace(ErrInvalidDSN)
	}
	p, tableName := filepath.Split(u.Path)
	if p != "/" {
		return "", "", "", errors.Trace(ErrInvalidDSN)
	}
	zks = u.Host
	tso = u.Query().Get("tso")
	if tso == "" {
		tso = tsoTypeLocal
	}
	return zks, tso, tableName, nil
}

func init() {
	mc.cache = make(map[string]*tikvStore)
	rand.Seed(time.Now().UnixNano())
}
