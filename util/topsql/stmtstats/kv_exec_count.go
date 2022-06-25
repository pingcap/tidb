// Copyright 2021 PingCAP, Inc.
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

package stmtstats

import (
	"sync"

	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

// CreateKvExecCounter creates an associated KvExecCounter from StatementStats.
// The created KvExecCounter can only be used during a single statement execution
// and cannot be reused.
func (s *StatementStats) CreateKvExecCounter(sqlDigest, planDigest []byte) *KvExecCounter {
	return &KvExecCounter{
		stats:  s,
		digest: SQLPlanDigest{SQLDigest: BinaryDigest(sqlDigest), PlanDigest: BinaryDigest(planDigest)},
		marked: map[string]struct{}{},
	}
}

// KvExecCounter is used to count the number of SQL executions of the kv layer.
// It internally calls addKvExecCount of StatementStats at the right time, to
// ensure the semantic of "SQL execution count of TiKV".
type KvExecCounter struct {
	stats  *StatementStats
	digest SQLPlanDigest
	mu     sync.Mutex
	marked map[string]struct{} // HashSet<Target>
}

// RPCInterceptor returns an interceptor.RPCInterceptor for client-go.
// The returned interceptor is generally expected to be bind to transaction or
// snapshot. In this way, the logic preset by KvExecCounter will be executed before
// each RPC request is initiated, in order to count the number of SQL executions of
// the TiKV dimension.
func (c *KvExecCounter) RPCInterceptor() interceptor.RPCInterceptor {
	return func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			if topsqlstate.TopSQLEnabled() {
				c.mark(target)
			}
			return next(target, req)
		}
	}
}

// mark this target during the current execution of statement.
// If this target is marked for the first time, then increase the number of execution.
// mark is thread-safe.
func (c *KvExecCounter) mark(target string) {
	firstMark := false
	c.mu.Lock()
	if _, ok := c.marked[target]; !ok {
		c.marked[target] = struct{}{}
		firstMark = true
	}
	c.mu.Unlock()
	if firstMark {
		c.stats.addKvExecCount([]byte(c.digest.SQLDigest), []byte(c.digest.PlanDigest), target, 1)
	}
}
