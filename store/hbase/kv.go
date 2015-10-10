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
	"errors"
	"strings"

	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/tidb/kv"
)

const (
	// ColFamily is the hbase col family name.
	ColFamily    = "f"
	// Qualifier is the hbase col name.
	Qualifier    = "q"
)

var _ kv.Storage = (*hbaseStore)(nil)

type hbaseStore struct {
	zkPaths   []string
	storeName string
	cli       hbase.HBaseClient
}

func (s *hbaseStore) Begin() (kv.Transaction, error) {
	if s.cli == nil {
		return nil, errors.New("error zk connection")
	}

	t := themis.NewTxn(s.cli)
	txn := newHbaseTxn(t, s.storeName)
	var err error
	txn.UnionStore, err = kv.NewUnionStore(&hbaseSnapshot{t, s.storeName})
	if err != nil {
		return nil, err
	}
	return txn, nil
}

func newHbaseTxn(t *themis.Txn, storeName string) *hbaseTxn {
	return &hbaseTxn{
		Txn:       t,
		valid:     true,
		storeName: storeName,
	}
}

func (s *hbaseStore) Close() error {
	return nil
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
func (d Driver) Open(dbPath string) (kv.Storage, error) {
	zks, storeName := getHBaseZkAndStoreNameFromURI(dbPath)
	if zks == nil {
		log.Fatal("db uri is error, has not zk info, dbPath:" + dbPath)
		return nil, nil
	}
	c, err := hbase.NewClient(zks, "/hbase")
	if err != nil {
		log.Fatal(err)
	}

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
			log.Fatal(err)
		}
	}

	return &hbaseStore{
		zkPaths:   zks,
		storeName: storeName,
		cli:       c,
	}, nil
}

// get zk and storename from uri like: hbase://zk1,zk2,zk3/tidb
func getHBaseZkAndStoreNameFromURI(uri string) ([]string, string) {
	uri = uri[len("hbase://"):]
	parts := strings.SplitN(uri, "/", 2)
	if len(parts) == 0 {
		log.Fatal("db uri error, url:" + uri)
		return nil, ""
	}

	storeName := "tidb"
	if len(parts) == 2 {
		storeName = parts[1]
	}

	return strings.Split(parts[0], ","), storeName
}
