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
//
//	"github.com/pingcap/tidb/kv"
//	"github.com/pingcap/tidb/session"
//	"github.com/pingcap/tidb/store/mockstore"
//)
//
//func CreateSession() session.Session {
//	driver := mockstore.MockTiKVDriver{}
//	//storage, err := driver.Open("mocktikv:///tmp/tidbmockstore")
//	storage, err := driver.Open("mocktikv:///Users/wangchao/Desktop/tidb/store")
//	if err != nil {
//		panic(err)
//	}
//
//	sess, err := session.CreateSession(storage)
//	if err != nil {
//		panic(err)
//	}
//
//	err = sess.NewTxn(context.Background())
//	if err != nil {
//		panic(err)
//	}
//
//	return sess
//}
//
//func TxnSet(txn kv.Transaction, key string, value string) {
//	err := txn.Set([]byte(key), []byte(value))
//	if err != nil {
//		panic(err)
//	}
//}
//
//func TxnGet(txn kv.Transaction, key string) string {
//	value, err := txn.Get(context.Background(), []byte(key))
//	if err != nil {
//		panic(err)
//	}
//
//	return string(value)
//}
//
//func SessionCommit(s session.Session) {
//	txn, err := s.Txn(true)
//	if err != nil {
//		panic(err)
//	}
//
//	s.StmtCommit()
//	err = txn.Commit(context.Background())
//	if err != nil {
//		panic(err)
//	}
//}
//
//func main() {
//	createSql := `
//		CREATE TABLE test.taa (
//		  id int(11) unsigned NOT NULL AUTO_INCREMENT,
//		  PRIMARY KEY (id)
//		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
//	`
//
//	s := CreateSession()
//	//txn, err := s.Txn(true)
//	//if err != nil {
//	//	panic(err)
//	//}
//
//	stmts, err := s.Parse(context.Background(), createSql)
//	if err != nil {
//		panic(err)
//	}
//
//	stmt := stmts[0]
//	//bytes, err := json.Marshal(stmt)
//	//if err != nil {
//	//	panic(err)
//	//}
//	//
//	//fmt.Printf("%s\n", string(bytes))
//
//	rs, err := s.ExecuteStmt(context.Background(), stmt)
//	if err != nil {
//		panic(err)
//	}
//
//	chunk := rs.NewChunk()
//	for {
//		err = rs.Next(context.Background(), chunk)
//		if err != nil {
//			panic(err)
//		}
//
//		rowCount := chunk.NumRows()
//		if rowCount == 0 {
//			continue
//		}
//
//		for i := 0; i < rowCount; i++ {
//			row := chunk.GetRow(i)
//			id := row.GetInt64(0)
//			value := int64(-1)
//			if !row.IsNull(1) {
//				value = row.GetInt64(1)
//			}
//
//			fmt.Printf("Row, %d, %d\n", id, value)
//		}
//	}
//
//	//TxnSet(txn, "foo", "fv1")
//	//TxnSet(txn, "bar", "bv1")
//	//
//	//v := TxnGet(txn, "foo")
//	//fmt.Printf("Value is %s", v)
//
//	SessionCommit(s)
//}
