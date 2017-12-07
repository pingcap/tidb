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

package tikv_test

import (
	"sync"
	"time"

	gofail "github.com/coreos/gofail/runtime"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/store/tikv"
	goctx "golang.org/x/net/context"
)

var _ = Suite(new(testSQLSuite))

type testSQLSuite struct {
	store tikv.Storage
}

func (s *testSQLSuite) SetUpSuite(c *C) {
	s.store, _ = tikv.NewTestTiKVStorage(false, "")
}

func (s *testSQLSuite) TestFailBusyServerCop(c *C) {
	_, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	session, err := tidb.CreateSession4Test(s.store)
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
		rs, err := session.Execute(goctx.Background(), `SELECT variable_value FROM mysql.tidb WHERE variable_name="bootstrapped"`)
		c.Assert(err, IsNil)
		row, err := rs[0].Next(goctx.Background())
		c.Assert(err, IsNil)
		c.Assert(row, NotNil)
		c.Assert(row.GetString(0), Equals, "True")
	}()

	wg.Wait()
}
