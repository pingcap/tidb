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
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/prometheus/client_golang/prometheus"
	promtestutils "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestActiveActiveDMLMetrics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().(sessionctx.Context)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int) active_active='on' softdelete retention 7 day")
	tk.MustExec("create table t2(id int primary key, v int) active_active='on' softdelete retention 7 day")

	type counterWithVal struct {
		counter prometheus.Counter
		val     float64
	}

	counter := func(c prometheus.Counter) *counterWithVal {
		return &counterWithVal{
			counter: c,
			val:     promtestutils.ToFloat64(c),
		}
	}

	checkCounterInc := func(inc float64, val *counterWithVal) {
		require.Equal(t, inc, promtestutils.ToFloat64(val.counter)-val.val)
	}

	for _, c := range []struct {
		name                  string
		sql                   string
		loadData              string
		disableRewrite        bool
		unsafeOriginTsRowsInc float64
		unsafeOriginTsStmtInc float64
		hardDeleteInc         float64
	}{
		{
			name: "normal insert",
			sql:  "insert into t (id, v) values (5, 500)",
		},
		{
			name:                  "insert _tidb_origin_ts",
			sql:                   "insert into t (id, v, _tidb_origin_ts) values (5, 50, 500), (6, 60, 600), (7, 70, NULL)",
			unsafeOriginTsRowsInc: 2,
			unsafeOriginTsStmtInc: 1,
		},
		{
			name: "normal update",
			sql:  "update t set v = v + 1 where id in (1, 2)",
		},
		{
			name:                  "update _tidb_origin_ts",
			sql:                   "update t set v = v + 1, _tidb_origin_ts = 111 where id in (1, 3, 4)",
			unsafeOriginTsRowsInc: 3,
			unsafeOriginTsStmtInc: 1,
		},
		{
			name:                  "update join",
			sql:                   "update t join t2 on t.id = t2.id set t._tidb_origin_ts=100, t2._tidb_origin_ts=200 where t.id in (1, 2)",
			unsafeOriginTsRowsInc: 4,
			unsafeOriginTsStmtInc: 1,
		},
		{
			name: "insert on duplicate key update (safe)",
			sql:  "insert into t (id, v) values (1, 101), (5, 500) on duplicate key update v = values(v)",
		},
		{
			name:                  "insert on duplicate key update (unsafe)",
			sql:                   "insert into t (id, v, _tidb_origin_ts) values (1, 11, 111), (2, 22, 222), (3, 33, 333), (4, 44, 444) on duplicate key update v = values(v), _tidb_origin_ts = values(_tidb_origin_ts)",
			unsafeOriginTsRowsInc: 4,
			unsafeOriginTsStmtInc: 1,
		},
		{
			name: "replace (safe)",
			sql:  "replace into t (id, v) values (1, 1000)",
		},
		{
			name:                  "replace (unsafe)",
			sql:                   "replace into t (id, v, _tidb_origin_ts) values (1, 10, 111), (2, 20, 222), (5, 50, 555), (6, 60, 666), (7, 70, 777)",
			unsafeOriginTsRowsInc: 5,
			unsafeOriginTsStmtInc: 1,
		},
		{
			name:     "load data (safe)",
			sql:      "LOAD DATA LOCAL INFILE '/tmp/nonexistence.csv' INTO TABLE t fields terminated by ' ' lines terminated by '\\n' (id, v)",
			loadData: "8 80\n9 90\n",
		},
		{
			name:                  "load data (unsafe)",
			sql:                   "LOAD DATA LOCAL INFILE '/tmp/nonexistence.csv' INTO TABLE t fields terminated by ' ' lines terminated by '\\n' (_tidb_origin_ts, id, v)",
			loadData:              "111 8 80\n222 9 90\n333 10 100\n444 11 110\n555 12 120\n666 13 130\n",
			unsafeOriginTsRowsInc: 6,
			unsafeOriginTsStmtInc: 1,
		},
		{
			name: "soft delete",
			sql:  "delete from t where id in (1, 2, 3)",
		},
		{
			name:           "delete hard delete metrics",
			sql:            "delete from t where id in (1, 2, 3)",
			disableRewrite: true,
			hardDeleteInc:  1,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			// prepare, clean table t and insert some prepared rows
			tk.MustExec("set @@tidb_translate_softdelete_sql = 'OFF'")
			tk.MustExec("delete from t")
			tk.MustExec("delete from t2")
			tk.MustExec("insert into t (id, v, _tidb_origin_ts) values (1, 10, NULL),(2, 20, 200),(3, 30, NULL),(4, 40, 400)")
			tk.MustExec("insert into t2 (id, v, _tidb_origin_ts) values (1, 11, NULL),(2, 21, 201),(3, 31, NULL),(4, 41, 401)")
			if !c.disableRewrite {
				tk.MustExec("set @@tidb_translate_softdelete_sql = 'ON'")
			}

			unsafeOriginTsRows := counter(metrics.ActiveActiveWriteUnsafeOriginTsRowCounter)
			unsafeOriginTsStmt := counter(metrics.ActiveActiveWriteUnsafeOriginTsStmtCounter)
			hardDelete := counter(metrics.ActiveActiveHardDeleteStmtCounter)

			if c.loadData != "" {
				readerBuilder := executor.LoadDataReaderBuilder{
					Build: func(_ string) (io.ReadCloser, error) {
						return mydump.NewStringReader(c.loadData), nil
					},
					Wg: &sync.WaitGroup{},
				}
				ctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
				t.Cleanup(func() {
					ctx.SetValue(executor.LoadDataReaderBuilderKey, nil)
				})
			}

			tk.MustExec(c.sql)
			checkCounterInc(c.unsafeOriginTsRowsInc, unsafeOriginTsRows)
			checkCounterInc(c.unsafeOriginTsStmtInc, unsafeOriginTsStmt)
			checkCounterInc(c.hardDeleteInc, hardDelete)
		})
	}
}

type blockGetClient struct {
	t *testing.T
	tikv.Client
	batchGetCalled atomic.Bool
}

func (c *blockGetClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	switch req.Type {
	case tikvrpc.CmdBatchGet:
		c.batchGetCalled.Store(true)
	case tikvrpc.CmdGet:
		require.Nil(c.t, ctx.Value("blockGet"), "unexpected CmdGet request issued")
		if ctx.Value("blockGet") != nil {
			require.FailNow(c.t, "unexpected CmdGet request issued")
		}
	default:
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

// TestActiveActiveInsertPrefetchCache tests that in active-active table, the insert will prefetch
// the existing rows by BatchGet to reduce performance overhead.
func TestActiveActiveInsertPrefetchCache(t *testing.T) {
	var hijacked *blockGetClient
	store := testkit.CreateMockStore(t,
		mockstore.WithClientHijacker(func(c tikv.Client) tikv.Client {
			hijacked = &blockGetClient{t: t, Client: c}
			return hijacked
		}),
	)
	require.NotNil(t, hijacked)

	tk := testkit.NewTestKit(t, store)
	getLastCommitTS := func() uint64 {
		type txnInfo struct {
			CommitTS uint64 `json:"commit_ts"`
		}
		var lastTxn txnInfo
		require.NoError(t, json.Unmarshal([]byte(tk.Session().GetSessionVars().LastTxnInfo), &lastTxn))
		require.NotZero(t, lastTxn.CommitTS)
		return lastTxn.CommitTS
	}

	blockGetCtx := context.WithValue(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers),
		"blockGet", "enable",
	)

	for _, clustered := range []string{"CLUSTERED", "NONCLUSTERED"} {
		t.Run(clustered, func(t *testing.T) {
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec(fmt.Sprintf(
				"create table t (id int primary key %s, v bigint unsigned) "+
					"softdelete retention 1 day active_active='on'",
				clustered,
			))
			tk.MustExec("set @@tidb_translate_softdelete_sql = 'ON'")
			// normal insert
			hijacked.batchGetCalled.Store(false)
			tk.MustExecWithContext(blockGetCtx, "insert into t values (1, 10)")
			require.True(t, hijacked.batchGetCalled.Load())
			tk.MustQuery("select * from t").Check(testkit.Rows("1 10"))

			// insert on duplicate key update
			hijacked.batchGetCalled.Store(false)
			tk.MustExecWithContext(blockGetCtx, "insert into t values (1, 100) on duplicate key update v = v+values(v)")
			require.True(t, hijacked.batchGetCalled.Load())
			commitTS1 := getLastCommitTS()
			tk.MustQuery("select * from t").Check(testkit.Rows("1 110"))

			// insert on duplicate key update with _tidb_commit_ts
			hijacked.batchGetCalled.Store(false)
			tk.MustExecWithContext(blockGetCtx, "insert into t values (1, 10) on duplicate key update v = _tidb_commit_ts")
			require.True(t, hijacked.batchGetCalled.Load())
			commitTS2 := getLastCommitTS()
			tk.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("1 %d", commitTS1)))

			// insert on duplicate key update when @@tidb_translate_softdelete_sql = 'OFF'
			tk.MustExec("set @@tidb_translate_softdelete_sql = 'OFF'")
			tk.MustExecWithContext(blockGetCtx, "insert into t values (1, 10) on duplicate key update v = _tidb_commit_ts*2")
			require.True(t, hijacked.batchGetCalled.Load())
			tk.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("1 %d", commitTS2*2)))
		})
	}
}
