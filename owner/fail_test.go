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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

// Ignore this test on the windows platform, because calling unix socket with address in
// host:port format fails on windows.
func TestT(t *testing.T) {
	logLevel := os.Getenv("log_level")
	err := logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		t.Fatal(err)
	}
}

var (
	dialTimeout = 5 * time.Second
	retryCnt    = math.MaxInt32
)

func TestFailNewSession(t *testing.T) {
	t.Parallel()
	os.Remove("new_session:0")
	ln, err := net.Listen("unix", "new_session:0")
	require.NoError(t, err)
	addr := ln.Addr()
	endpoints := []string{fmt.Sprintf("%s://%s", addr.Network(), addr.String())}
	require.Nil(t, err)
	srv := grpc.NewServer(grpc.ConnectionTimeout(time.Minute))
	var stop sync.WaitGroup
	stop.Add(1)
	go func() {
		if err = srv.Serve(ln); err != nil {
			require.Errorf(t, err, "can't serve gRPC requests %v")
		}
		stop.Done()
	}()

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
				cli.Close()
			}
			require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/owner/closeClient"))
		}()
		require.NoError(t, err, failpoint.Enable("github.com/pingcap/tidb/owner/closeClient", `return(true)`))
		_, err = NewSession(context.Background(), "fail_new_serssion", cli, retryCnt, ManagerSessionTTL)
		isContextDone := terror.ErrorEqual(grpc.ErrClientConnClosing, err) || terror.ErrorEqual(context.Canceled, err)
		require.True(t, isContextDone, "err %v", err)
	}()

	func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: dialTimeout,
		})
		require.NoError(t, err)
		defer func() {
			if cli != nil {
				cli.Close()
			}
			require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/owner/closeGrpc"))
		}()
		require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/owner/closeGrpc", `return(false)`))
		_, err = NewSession(context.Background(), "fail_new_serssion", cli, retryCnt, ManagerSessionTTL)
		isContextDone := terror.ErrorEqual(grpc.ErrClientConnClosing, err) || terror.ErrorEqual(context.Canceled, err)
		require.True(t, isContextDone, "err %v", err)
	}()

}
