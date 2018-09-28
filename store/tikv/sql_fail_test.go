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
	"fmt"
	"sync"
	"time"

	gofail "github.com/etcd-io/gofail/runtime"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	. "github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"golang.org/x/net/context"
)

var _ = Suite(new(testSQLSuite))

type testSQLSuite struct {
	OneByOneSuite
	store Storage
	dom   *domain.Domain
}

func (s *testSQLSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	var err error
	s.store = NewTestStore(c).(Storage)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testSQLSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSQLSuite) TestFailBusyServerCop(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcServerBusy", `return(true)`)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcServerBusy")
	}()

	go func() {
		defer wg.Done()
		rs, err := se.Execute(context.Background(), `SELECT variable_value FROM mysql.tidb WHERE variable_name="bootstrapped"`)
		if len(rs) > 0 {
			defer terror.Call(rs[0].Close)
		}
		c.Assert(err, IsNil)
		chk := rs[0].NewChunk()
		err = rs[0].Next(context.Background(), chk)
		c.Assert(err, IsNil)
		c.Assert(chk.NumRows() == 0, IsFalse)
		c.Assert(chk.GetRow(0).GetString(0), Equals, "True")
	}()

	wg.Wait()
}

func (s *testSQLSuite) TestCoprocessorStreamRecvTimeout(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int)")
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}

	// rowsPerChunk in MockTiKV is 64, so the result will be 4 chunks.
	enable := true
	ctx := context.WithValue(context.Background(), mock.HookKeyForTest("mockTiKVStreamRecvHook"), func(ctx context.Context) {
		if !enable {
			return
		}

		select {
		case <-ctx.Done():
		case <-time.After(time.Minute):
		}
		enable = false
	})

	res, err := tk.Se.Execute(ctx, "select * from t")
	c.Assert(err, IsNil)

	chk := res[0].NewChunk()
	for {
		err := res[0].Next(ctx, chk)
		c.Assert(err, IsNil)
		if chk.NumRows() == 0 {
			break
		}
		chk.Reset()
	}
}
