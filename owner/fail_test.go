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

package owner

import (
	"math"
	"net"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	gofail "github.com/etcd-io/gofail/runtime"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testleak"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level:  logLevel,
		Format: "highlight",
	})
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	ln net.Listener
}

func (s *testSuite) SetUpSuite(c *C) {
	ln, err := net.Listen("unix", "new_session:12379")
	c.Assert(err, IsNil)
	s.ln = ln
}

func (s *testSuite) TearDownSuite(c *C) {
	err := s.ln.Close()
	c.Assert(err, IsNil)
}

var (
	endpoints   = []string{"unix://new_session:12379"}
	dialTimeout = 5 * time.Second
	retryCnt    = int(math.MaxInt32)
)

func (s *testSuite) TestFailNewSession(c *C) {
	defer testleak.AfterTest(c)()

	func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: dialTimeout,
		})
		defer func() {
			if cli != nil {
				cli.Close()
			}
		}()
		gofail.Enable("github.com/pingcap/tidb/owner/closeClient", `return(true)`)
		_, err = NewSession(context.Background(), "fail_new_serssion", cli, retryCnt, ManagerSessionTTL)
		isContextDone := terror.ErrorEqual(grpc.ErrClientConnClosing, err) || terror.ErrorEqual(context.Canceled, err)
		c.Assert(isContextDone, IsTrue, Commentf("err %v", err))
	}()

	func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: dialTimeout,
		})
		defer func() {
			if cli != nil {
				cli.Close()
			}
		}()
		gofail.Enable("github.com/pingcap/tidb/owner/closeGrpc", `return(true)`)
		_, err = NewSession(context.Background(), "fail_new_serssion", cli, retryCnt, ManagerSessionTTL)
		isContextDone := terror.ErrorEqual(grpc.ErrClientConnClosing, err) || terror.ErrorEqual(context.Canceled, err)
		c.Assert(isContextDone, IsTrue, Commentf("err %v", err))
	}()

}
