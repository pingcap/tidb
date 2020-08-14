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
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	. "github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testSQLSuite{})
var _ = SerialSuites(&testSQLSerialSuite{})

type testSQLSuite struct {
	testSQLSuiteBase
}

type testSQLSerialSuite struct {
	testSQLSuiteBase
}

type testSQLSuiteBase struct {
	OneByOneSuite
	store Storage
	dom   *domain.Domain
}

func (s *testSQLSuiteBase) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	var err error
	s.store = NewTestStore(c).(Storage)
	// actual this is better done in `OneByOneSuite.SetUpSuite`, but this would cause circle dependency
	if *WithTiKV {
		session.ResetStoreForWithTiKVTest(s.store)
	}

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testSQLSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSQLSerialSuite) TestFailBusyServerCop(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcServerBusy", `return(true)`), IsNil)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcServerBusy"), IsNil)
	}()

	go func() {
		defer wg.Done()
		rs, err := se.Execute(context.Background(), `SELECT variable_value FROM mysql.tidb WHERE variable_name="bootstrapped"`)
		if len(rs) > 0 {
			defer terror.Call(rs[0].Close)
		}
		c.Assert(err, IsNil)
		req := rs[0].NewChunk()
		err = rs[0].Next(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(req.NumRows() == 0, IsFalse)
		c.Assert(req.GetRow(0).GetString(0), Equals, "True")
	}()

	wg.Wait()
}

func TestMain(m *testing.M) {
	ReadTimeoutMedium = 2 * time.Second
	os.Exit(m.Run())
}

func (s *testSQLSuite) TestCoprocessorStreamRecvTimeout(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table cop_stream_timeout (id int)")
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into cop_stream_timeout values (%d)", i))
	}
	tk.Se.GetSessionVars().EnableStreaming = true

	{
		enable := true
		visited := make(chan int, 1)
		timeouted := false
		timeout := ReadTimeoutMedium + 100*time.Second
		ctx := context.WithValue(context.Background(), mock.HookKeyForTest("mockTiKVStreamRecvHook"), func(ctx context.Context) {
			if !enable {
				return
			}
			visited <- 1

			select {
			case <-ctx.Done():
			case <-time.After(timeout):
				timeouted = true
			}
			enable = false
		})

		res, err := tk.Se.Execute(ctx, "select * from cop_stream_timeout")
		c.Assert(err, IsNil)

		req := res[0].NewChunk()
		for i := 0; ; i++ {
			err := res[0].Next(ctx, req)
			c.Assert(err, IsNil)
			if req.NumRows() == 0 {
				break
			}
			req.Reset()
		}
		select {
		case <-visited:
			// run with mocktikv
			c.Assert(timeouted, IsFalse)
		default:
			// run with real tikv
		}
	}

	{
		enable := true
		visited := make(chan int, 1)
		timeouted := false
		timeout := 1 * time.Millisecond
		ctx := context.WithValue(context.Background(), mock.HookKeyForTest("mockTiKVStreamRecvHook"), func(ctx context.Context) {
			if !enable {
				return
			}
			visited <- 1

			select {
			case <-ctx.Done():
			case <-time.After(timeout):
				timeouted = true
			}
			enable = false
		})

		res, err := tk.Se.Execute(ctx, "select * from cop_stream_timeout")
		c.Assert(err, IsNil)

		req := res[0].NewChunk()
		for i := 0; ; i++ {
			err := res[0].Next(ctx, req)
			c.Assert(err, IsNil)
			if req.NumRows() == 0 {
				break
			}
			req.Reset()
		}
		select {
		case <-visited:
			// run with mocktikv
			c.Assert(timeouted, IsTrue)
		default:
			// run with real tikv
		}
	}
}
