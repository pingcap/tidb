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
	ColFamily = "f"
	// Qualifier is the hbase col name.
	Qualifier = "q"
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
func (d Driver) Open(zkInfo string) (kv.Storage, error) {
	zks := strings.Split(zkInfo, ",")
	if zks == nil {
		log.Fatal("db uri is error, has not zk info, zkInfo:" + zkInfo)
		return nil, nil
	}
	c, err := hbase.NewClient(zks, "/hbase")
	if err != nil {
		log.Fatal(err)
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
			log.Fatal(err)
		}
	}

	return &hbaseStore{
		zkPaths:   zks,
		storeName: storeName,
		cli:       c,
	}, nil
}
