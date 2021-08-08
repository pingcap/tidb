// Copyright 2018 PingCAP, Inc.
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
// +build !windows

package owner

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

// Ignore this test on the windows platform, because calling unix socket with address in
// host:port format fails on windows.
func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	err := logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		t.Fatal(err)
	}
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (s *testSuite) SetUpSuite(c *C) {
}

func (s *testSuite) TearDownSuite(c *C) {
}

var (
	dialTimeout = 5 * time.Second
	retryCnt    = math.MaxInt32
)

func (s *testSuite) TestFailNewSession(c *C) {
	os.Remove("new_session:0")
	ln, err := net.Listen("unix", "new_session:0")
	c.Assert(err, IsNil)
	addr := ln.Addr()
	endpoints := []string{fmt.Sprintf("%s://%s", addr.Network(), addr.String())}
	c.Assert(err, IsNil)
	srv := grpc.NewServer(grpc.ConnectionTimeout(time.Minute))
	var stop sync.WaitGroup
	stop.Add(1)
	go func() {
		if err = srv.Serve(ln); err != nil {
			c.Errorf("can't serve gRPC requests %v", err)
		}
		stop.Done()
	}()

	leakFunc := testleak.AfterTest(c)
	defer func() {
		srv.Stop()
		stop.Wait()
		leakFunc()
	}()

	func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: dialTimeout,
		})
		c.Assert(err, IsNil)
		defer func() {
			if cli != nil {
				cli.Close()
			}
			c.Assert(failpoint.Disable("github.com/pingcap/tidb/owner/closeClient"), IsNil)
		}()
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/owner/closeClient", `return(true)`), IsNil)

		// TODO: It takes more than 2s here in etcd client, the CI takes 5s to run this test.
		// The config is hard coded, not way to control it outside.
		// Call stack:
		// https://github.com/etcd-io/etcd/blob/ae9734e/clientv3/concurrency/session.go#L38
		// https://github.com/etcd-io/etcd/blob/ae9734ed278b7a1a7dfc82e800471ebbf9fce56f/clientv3/client.go#L253
		// https://github.com/etcd-io/etcd/blob/ae9734ed278b7a1a7dfc82e800471ebbf9fce56f/clientv3/retry_interceptor.go#L63
		_, err = NewSession(context.Background(), "fail_new_serssion", cli, retryCnt, ManagerSessionTTL)
		isContextDone := terror.ErrorEqual(grpc.ErrClientConnClosing, err) || terror.ErrorEqual(context.Canceled, err)
		c.Assert(isContextDone, IsTrue, Commentf("err %v", err))
	}()

	func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: dialTimeout,
		})
		c.Assert(err, IsNil)
		defer func() {
			if cli != nil {
				cli.Close()
			}
			c.Assert(failpoint.Disable("github.com/pingcap/tidb/owner/closeGrpc"), IsNil)
		}()
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/owner/closeGrpc", `return(true)`), IsNil)

		// TODO: It takes more than 2s here in etcd client, the CI takes 5s to run this test.
		// The config is hard coded, not way to control it outside.
		_, err = NewSession(context.Background(), "fail_new_serssion", cli, retryCnt, ManagerSessionTTL)
		isContextDone := terror.ErrorEqual(grpc.ErrClientConnClosing, err) || terror.ErrorEqual(context.Canceled, err)
		c.Assert(isContextDone, IsTrue, Commentf("err %v", err))
	}()
}
