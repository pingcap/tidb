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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package owner

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	dialTimeout = 3 * time.Second
	retryCnt    = math.MaxInt32
)

func TestFailNewSession(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	_ = os.Remove("new_session:0")
	ln, err := net.Listen("unix", "new_session:0")
	require.NoError(t, err)

	addr := ln.Addr()
	endpoints := []string{fmt.Sprintf("%s://%s", addr.Network(), addr.String())}
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.ConnectionTimeout(time.Minute))

	var stop util.WaitGroupWrapper
	stop.Run(func() {
		err = srv.Serve(ln)
		assert.NoError(t, err)
	})

	defer func() {
		srv.Stop()
		stop.Wait()
	}()

	func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: dialTimeout,
		})
		require.NoError(t, err)
		defer func() {
			if cli != nil {
				_ = cli.Close()
			}
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/closeClient"))
		}()
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/closeClient", `return(true)`))

		// TODO: It takes more than 2s here in etcd client, the CI takes 5s to run this test.
		// The config is hard coded, not way to control it outside.
		// Call stack:
		// https://github.com/etcd-io/etcd/blob/ae9734e/clientv3/concurrency/session.go#L38
		// https://github.com/etcd-io/etcd/blob/ae9734ed278b7a1a7dfc82e800471ebbf9fce56f/clientv3/client.go#L253
		// https://github.com/etcd-io/etcd/blob/ae9734ed278b7a1a7dfc82e800471ebbf9fce56f/clientv3/retry_interceptor.go#L63
		_, err = util.NewSession(context.Background(), "fail_new_session", cli, retryCnt, ManagerSessionTTL)
		isContextDone := terror.ErrorEqual(grpc.ErrClientConnClosing, err) || terror.ErrorEqual(context.Canceled, err)
		require.Truef(t, isContextDone, "err %v", err)
	}()

	func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: dialTimeout,
		})
		require.NoError(t, err)
		defer func() {
			if cli != nil {
				_ = cli.Close()
			}
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/closeGrpc"))
		}()
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/closeGrpc", `return(true)`))

		// TODO: It takes more than 2s here in etcd client, the CI takes 5s to run this test.
		// The config is hard coded, not way to control it outside.
		_, err = util.NewSession(context.Background(), "fail_new_session", cli, retryCnt, ManagerSessionTTL)
		isContextDone := terror.ErrorEqual(grpc.ErrClientConnClosing, err) || terror.ErrorEqual(context.Canceled, err)
		require.Truef(t, isContextDone, "err %v", err)
	}()
}
