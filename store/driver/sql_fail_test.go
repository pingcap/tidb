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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestFailBusyServerCop(t *testing.T) {
	store, _, clean := createTestStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var wg util.WaitGroupWrapper

	require.NoError(t, failpoint.Enable("tikvclient/rpcServerBusy", `return(true)`))
	wg.Run(func() {
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, failpoint.Disable("tikvclient/rpcServerBusy"))
	})

	wg.Run(func() {
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
	})
	wg.Wait()
}
