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

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

var (
	store    kv.Storage
	logLevel = flag.String("L", "error", "Log level")
)

// init memory store
func init() {
	path := fmt.Sprintf("memory://%d", time.Now().UnixNano())
	d := localstore.Driver{
		goleveldb.MemoryDriver{},
	}
	var err error
	store, err = d.Open(path)
	if err != nil {
		panic(err)
	}
}

func dump() {
	startTs := time.Now()
	tx, _ := store.Begin()
	it, err := tx.Seek([]byte{0}, nil)
	if err != nil {
		log.Error(err)
	}
	cnt := 0
	for it.Valid() {
		log.Info(it.Key(), it.Value())
		it, _ = it.Next(nil)
		cnt++
	}
	tx.Commit()
	elapse := time.Since(startTs)
	fmt.Println(cnt, elapse)
}

func renew() {
	tx, _ := store.Begin()
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("record-%d", i)
		val := fmt.Sprintf("%d", time.Now().Unix())
		tx.Set([]byte(key), []byte(val))
	}
	tx.Commit()
}

func main() {
	flag.Parse()
	log.SetLevelByString(*logLevel)

	for i := 0; i < 100; i++ {
		fmt.Printf("\n====== Round %d =====\n", i)
		if i%2 == 0 {
			renew()
			dump()
			store.DumpRaw()
		}
		fmt.Println("====================")
		time.Sleep(2 * time.Second)
	}
}
