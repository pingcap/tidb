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

package executor_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	clientutil "github.com/tikv/client-go/v2/util"
)

const ignoredLockHintTxnID = uint64(42)

type ignoredLockHintSQLClient struct {
	tikv.Client
	targetCmd      tikvrpc.CmdType
	tableID        atomic.Int64
	armed          atomic.Bool
	readRequests   atomic.Int32
	statusRequests atomic.Int32
	missingHint    atomic.Bool
}

func (c *ignoredLockHintSQLClient) SendRequest(
	ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration,
) (*tikvrpc.Response, error) {
	if !c.armed.Load() {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}

	if req.Type == tikvrpc.CmdCheckTxnStatus && req.CheckTxnStatus().LockTs == ignoredLockHintTxnID {
		c.statusRequests.Add(1)
		return &tikvrpc.Response{Resp: &kvrpcpb.CheckTxnStatusResponse{}}, nil
	}
	if req.Type != c.targetCmd || requestTableID(req) != c.tableID.Load() {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}

	requestCount := c.readRequests.Add(1)
	if requestCount >= 2 && !containsTxnID(req.ResolvedLocks, ignoredLockHintTxnID) {
		c.missingHint.Store(true)
	}
	// The first response discovers the lock. The next two responses simulate
	// TiKV ignoring the resolved-lock hint carried by the retries.
	if requestCount <= 3 {
		return ignoredLockHintResponse(req), nil
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func requestTableID(req *tikvrpc.Request) int64 {
	var key []byte
	switch req.Type {
	case tikvrpc.CmdGet:
		key = req.Get().Key
	case tikvrpc.CmdBatchGet:
		if len(req.BatchGet().Keys) > 0 {
			key = req.BatchGet().Keys[0]
		}
	case tikvrpc.CmdCop:
		if len(req.Cop().Ranges) > 0 {
			key = req.Cop().Ranges[0].Start
		}
	}
	return tablecodec.DecodeTableID(key)
}

func containsTxnID(txnIDs []uint64, target uint64) bool {
	for _, txnID := range txnIDs {
		if txnID == target {
			return true
		}
	}
	return false
}

func ignoredLockHintResponse(req *tikvrpc.Request) *tikvrpc.Response {
	lock := &kvrpcpb.LockInfo{
		Key:         []byte("ignored-lock-hint-key"),
		PrimaryLock: []byte("ignored-lock-hint-key"),
		LockVersion: ignoredLockHintTxnID,
		LockTtl:     1,
		TxnSize:     1,
		LockType:    kvrpcpb.Op_Put,
	}
	keyErr := &kvrpcpb.KeyError{Locked: lock}
	switch req.Type {
	case tikvrpc.CmdGet:
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Error: keyErr}}
	case tikvrpc.CmdBatchGet:
		return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{Error: keyErr}}
	case tikvrpc.CmdCop:
		return &tikvrpc.Response{Resp: &coprocessor.Response{Locked: lock}}
	default:
		panic("unexpected request type")
	}
}

func TestSQLReadBacksOffWhenTiKVIgnoresLockHint(t *testing.T) {
	tests := []struct {
		name     string
		cmd      tikvrpc.CmdType
		plan     string
		sql      string
		expected [][]any
	}{
		{
			name:     "point-get",
			cmd:      tikvrpc.CmdGet,
			plan:     "Point_Get",
			sql:      "select v from t where id = 1",
			expected: testkit.Rows("10"),
		},
		{
			name:     "batch-point-get",
			cmd:      tikvrpc.CmdBatchGet,
			plan:     "Batch_Point_Get",
			sql:      "select id, v from t where id in (1, 2) order by id",
			expected: testkit.Rows("1 10", "2 20"),
		},
		{
			name:     "cop-task",
			cmd:      tikvrpc.CmdCop,
			plan:     "TableReader",
			sql:      "select sum(v) from t",
			expected: testkit.Rows("30"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &ignoredLockHintSQLClient{targetCmd: test.cmd}
			store := testkit.CreateMockStore(t,
				mockstore.WithStoreType(mockstore.EmbedUnistore),
				mockstore.WithClientHijacker(func(base tikv.Client) tikv.Client {
					client.Client = base
					return client
				}),
			)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t (id int primary key, v int)")
			tk.MustExec("insert into t values (1, 10), (2, 20)")
			tk.MustExec("set @@tidb_backoff_lock_fast = 1")
			tk.MustHavePlan(test.sql, test.plan)

			table := external.GetTableByName(t, tk, "test", "t")
			client.tableID.Store(table.Meta().ID)
			client.armed.Store(true)
			detail := &clientutil.ExecDetails{}
			ctx := context.WithValue(context.Background(), clientutil.ExecDetailsKey, detail)
			tk.MustQueryWithContext(ctx, test.sql).Check(test.expected)

			require.Equal(t, int32(4), client.readRequests.Load())
			require.Equal(t, int32(1), client.statusRequests.Load())
			require.False(t, client.missingHint.Load())
			require.Equal(t, int64(2), atomic.LoadInt64(&detail.BackoffCount))
			if test.cmd == tikvrpc.CmdCop {
				copDetails := tk.Session().GetSessionVars().StmtCtx.CopTasksDetails()
				require.NotNil(t, copDetails)
				require.Equal(t, map[string]int{"txnLockFast": 2}, copDetails.TotBackoffTimes)
			}
		})
	}
}
