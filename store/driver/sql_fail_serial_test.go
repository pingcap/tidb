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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestFailBusyServerCop(t *testing.T) {
	store, _, clean := createTestStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	require.NoError(t, failpoint.Enable("tikvclient/rpcServerBusy", `return(true)`))
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, failpoint.Disable("tikvclient/rpcServerBusy"))
	}()

	go func() {
		defer wg.Done()
		rs, err := se.Execute(context.Background(), `SELECT variable_value FROM mysql.tidb WHERE variable_name="bootstrapped"`)
		if len(rs) > 0 {
			defer func() {
				require.NoError(t, rs[0].Close())
			}()
		}
		require.NoError(t, err)
		req := rs[0].NewChunk(nil)
		err = rs[0].Next(context.Background(), req)
		require.NoError(t, err)
		require.NotEqual(t, 0, req.NumRows())
		require.Equal(t, "True", req.GetRow(0).GetString(0))
	}()

	wg.Wait()
}

func TestCoprocessorStreamRecvTimeout(t *testing.T) {
	store, _, clean := createTestStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cop_stream_timeout (id int)")
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into cop_stream_timeout values (%d)", i))
	}
	tk.Session().GetSessionVars().EnableStreaming = true

	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{"timeout", tikv.ReadTimeoutMedium + 100*time.Second},
		{"no timeout", time.Millisecond},
	}

	for _, test := range tests {
		timeout := test.timeout
		t.Run(test.name, func(t *testing.T) {
			enable := true
			visited := make(chan int, 1)
			isTimeout := false
			ctx := context.WithValue(context.Background(), mock.HookKeyForTest("mockTiKVStreamRecvHook"), func(ctx context.Context) {
				if !enable {
					return
				}
				visited <- 1

				select {
				case <-ctx.Done():
				case <-time.After(timeout):
					isTimeout = true
				}
				enable = false
			})

			res, err := tk.Session().Execute(ctx, "select * from cop_stream_timeout")
			require.NoError(t, err)

			req := res[0].NewChunk(nil)
			for i := 0; ; i++ {
				err := res[0].Next(ctx, req)
				require.NoError(t, err)
				if req.NumRows() == 0 {
					break
				}
				req.Reset()
			}
			select {
			case <-visited:
				// run with mock tikv
				require.False(t, isTimeout)
			default:
				// run with real tikv
			}
		})
	}
}
