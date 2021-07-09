//// Copyright 2021 PingCAP, Inc.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// See the License for the specific language governing permissions and
//// limitations under the License.
//
//package main
//
//import (
//	"context"
//	"fmt"
//	"log"
//	"strings"
//
//	"github.com/pingcap/tidb/meta"
//
//	driver2 "github.com/pingcap/tidb/store/driver"
//
//	"github.com/pingcap/tidb/store/tikv/oracle"
//
//	"github.com/pingcap/tidb/kv"
//)
//
//func Hex(str interface{}) string {
//	var hex []byte
//	if h, ok := str.([]byte); ok {
//		hex = h
//	} else if s, ok := str.(string); ok {
//		hex = []byte(s)
//	} else if k, ok := str.(kv.Key); ok {
//		hex = k
//	} else {
//		panic("not supported")
//	}
//
//	builder := strings.Builder{}
//	for _, c := range hex {
//		if c >= ' ' && c <= '~' {
//			builder.WriteByte(c)
//		} else {
//			builder.WriteString(fmt.Sprintf("\\%03d", c))
//		}
//	}
//
//	return builder.String()
//}
//
//type TestStore struct {
//	kv.Storage
//	ctx context.Context
//}
//
//func CreateStore() *TestStore {
//	driver := driver2.TiKVDriver{}
//	store, err := driver.Open("tikv://127.0.0.1:2379")
//
//	//driver := mockstore.MockTiKVDriver{}
//	//store, err := driver.Open("mocktikv:///tmp/mocktikvtest")
//	if err != nil {
//		log.Panic(err)
//	}
//	return &TestStore{Storage: store, ctx: context.Background()}
//}
//
//func (s *TestStore) NewVersion() kv.Version {
//	ts, err := s.Storage.GetOracle().GetTimestamp(s.ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
//	if err != nil {
//		panic(err)
//	}
//	return kv.NewVersion(ts)
//}
//
//func (s *TestStore) Get(key string) string {
//	snapshot := s.Storage.GetSnapshot(s.NewVersion())
//	value, err := snapshot.Get(s.ctx, []byte(key))
//	if err != nil {
//		panic(err)
//	}
//	return string(value)
//}
//
//func (s *TestStore) Begin() *TestTxn {
//	txn, err := s.Storage.Begin()
//	if err != nil {
//		log.Panic(err)
//	}
//
//	return &TestTxn{Transaction: txn, ctx: s.ctx}
//}
//
//type TestTxn struct {
//	kv.Transaction
//	ctx context.Context
//}
//
//func (t *TestTxn) LockKey(key string) {
//	if err := t.Transaction.LockKeys(t.ctx, new(kv.LockCtx), kv.Key(key)); err != nil {
//		panic(err)
//	}
//}
//
//func (t *TestTxn) Set(key string, value string) {
//	err := t.Transaction.Set(kv.Key(key), []byte(value))
//	if err != nil {
//		panic(err)
//	}
//}
//
//func (t *TestTxn) Get(key string) string {
//	value, err := t.Transaction.Get(t.ctx, []byte(key))
//	if err != nil {
//		panic(err)
//	}
//
//	return string(value)
//}
//
//func (t *TestTxn) Iter(key, upper string) kv.Iterator {
//	snap := t.GetSnapshot()
//	iter, err := snap.Iter([]byte(key), []byte(upper))
//	if err != nil {
//		panic(err)
//	}
//
//	return iter
//}
//
//func (t *TestTxn) Commit() {
//	if err := t.Transaction.Commit(t.ctx); err != nil {
//		log.Panic(err)
//	}
//}
//
//func main() {
//	store := CreateStore()
//	txn := store.Begin()
//
//	m := meta.NewMeta(txn.Transaction)
//	dbs, err := m.ListDatabases()
//	if err != nil {
//		panic(err)
//	}
//
//	for _, db := range dbs {
//		fmt.Printf("%s\n", db.Name)
//		if db.Name.L == "test" {
//			tbs, err := m.ListTables(db.ID)
//			if err != nil {
//				panic(err)
//			}
//
//			for _, tb := range tbs {
//				fmt.Printf("  %s", tb.Name)
//			}
//		}
//	}
//
//	//iter := txn.Iter("", "zzzzzzz")
//	//for {
//	//	if !iter.Valid() {
//	//		break
//	//	}
//	//	key := iter.Key()
//	//	fmt.Printf("%s\n", Hex(key))
//	//	err := iter.Next()
//	//	if err != nil {
//	//		panic(err)
//	//	}
//	//}
//
//	//txn.LockKey("key0")
//	//txn.Set("key1", "value1")
//	//txn.Set("key2", "value2222")
//	//txn.Commit()
//	//
//	//fmt.Printf("final key1: %s, key2: %s", store.Get("key1"), store.Get("key2"))
//}
