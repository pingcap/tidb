// Copyright 2017 PingCAP, Inc.
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
	"sync"
	"time"

	gofail "github.com/coreos/gofail/runtime"
	. "github.com/pingcap/check"
	goctx "golang.org/x/net/context"
)

func (s *testStoreSuite) TestFailBusyServerKV(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	gofail.Enable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcServerBusy", `return(true)`)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		gofail.Disable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcServerBusy")
	}()

	go func() {
		defer wg.Done()
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		val, err := txn.Get([]byte("key"))
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, []byte("value"))
	}()

	wg.Wait()
}
