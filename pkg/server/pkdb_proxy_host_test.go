// Copyright 2026 PingCAP, Inc.
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

package server

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestProxyHostProcessInfoRetainsExpensiveLogTimes(t *testing.T) {
	// This scenario models the expensive-query checker on a PROXY connection:
	// publish process info, mutate both throttle timestamps through one process-list
	// read, then verify a later read observes the same session-owned state.
	const (
		connectionID = uint64(1)
		proxyHost    = "10.0.0.1:12345"
	)
	store := testkit.CreateMockStore(t)
	ctx, err := NewTiDBDriver(store).OpenCtx(connectionID, 0, mysql.DefaultCollationID, "", nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ctx.Close())
	})

	conn := &clientConn{
		connectionID: connectionID,
		proxyHost:    proxyHost,
	}
	conn.SetCtx(ctx)
	ctx.SetProcessInfo("select sleep(100)", time.Now(), mysql.ComQuery, 0)
	server := &Server{clients: map[uint64]*clientConn{connectionID: conn}}

	first := server.ShowProcessList()[connectionID]
	require.NotNil(t, first)
	require.Equal(t, proxyHost, first.ProxyHost)
	queryLogTime := time.Unix(10, 0)
	txnLogTime := time.Unix(20, 0)
	first.ExpensiveLogTime = queryLogTime
	first.ExpensiveTxnLogTime = txnLogTime

	second := server.ShowProcessList()[connectionID]
	require.Same(t, first, second)
	require.Equal(t, queryLogTime, second.ExpensiveLogTime)
	require.Equal(t, txnLogTime, second.ExpensiveTxnLogTime)
}
