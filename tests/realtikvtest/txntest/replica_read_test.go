// Copyright 2025 PingCAP, Inc.
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

package txntest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

func TestReplicaReadEffectScope(t *testing.T) {
	defer config.RestoreFunc()()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	testCases := []struct {
		name        string
		sql         string
		replicaRead bool
	}{
		{
			"select can use replica read",
			"SELECT * FROM t WHERE %s",
			true,
		},
		{
			"select for update can't use replica read",
			"SELECT * FROM t WHERE %s FOR UPDATE",
			false,
		},
		{
			"select for share can't use replica read",
			"SELECT * FROM t WHERE %s FOR SHARE",
			false,
		},
		{
			"update can't use replica read",
			"UPDATE t SET v = v + 1 WHERE %s",
			false,
		},
		{
			"delete can't use replica read",
			"DELETE FROM t WHERE %s",
			false,
		},
		{
			"insert can't use replica read",
			"INSERT INTO t SELECT id + 100, v + 100 FROM t WHERE %s",
			false,
		},
		{
			"insert on duplicate key update can't use replica read",
			"INSERT INTO t SELECT id + 100, v + 100 FROM t WHERE %s ON DUPLICATE KEY UPDATE v = VALUES(v)",
			false,
		},
		{
			"replace can't use replica read",
			"REPLACE INTO t SELECT id, v + 100 FROM t WHERE %s",
			false,
		},
	}

	conds := []string{
		"id = 1",       // point get
		"id IN (1, 2)", // batch get
		"id > 1",       // coprocessor
	}

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("set session tidb_replica_read = 'follower'")
	tk.MustExec("set session tidb_enable_noop_functions='on'")
	for _, testCase := range testCases {
		for _, cond := range conds {
			t.Run(fmt.Sprintf("%s(%s)", testCase.name, cond), func(t *testing.T) {
				interceptorCtx := interceptor.WithRPCInterceptor(context.Background(), interceptor.NewRPCInterceptor(testCase.name, func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
					return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
						tikvrpc.AttachContext(req, req.Context)
						switch req.Req.(type) {
						case *kvrpcpb.GetRequest, *kvrpcpb.BatchGetRequest, *coprocessor.Request:
							replicaRead := req.Context.GetReplicaRead()
							require.Equal(t, replicaRead, testCase.replicaRead)
						default:
						}
						return next(target, req)
					}
				}))

				tk.MustExec("delete from t")
				tk.MustExec("insert into t values (1, 10), (2, 20), (3, 30)")

				sql := fmt.Sprintf(testCase.sql, cond)
				tk.MustExec("begin pessimistic")
				if strings.HasPrefix(sql, "SELECT") {
					tk.MustQueryWithContext(interceptorCtx, sql)
				} else {
					tk.MustExecWithContext(interceptorCtx, sql)
				}
				tk.MustExec("rollback")
				if strings.HasPrefix(sql, "SELECT") {
					tk.MustQueryWithContext(interceptorCtx, sql)
				} else {
					tk.MustExecWithContext(interceptorCtx, sql)
				}
			})
		}
	}
}
