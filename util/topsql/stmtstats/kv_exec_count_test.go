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
	"testing"

	"github.com/pingcap/tidb/util/topsql/state"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestKvExecCounter(t *testing.T) {
	state.EnableTopSQL()
	stats := CreateStatementStats()
	counter := stats.CreateKvExecCounter([]byte("SQL-1"), []byte(""))
	interceptor := counter.RPCInterceptor()
	for n := 0; n < 10; n++ {
		_, _ = interceptor(func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			return nil, nil
		})("TIKV-1", nil)
	}
	for n := 0; n < 10; n++ {
		_, _ = interceptor(func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			return nil, nil
		})("TIKV-2", nil)
	}
	assert.Len(t, counter.marked, 2)
	assert.Contains(t, counter.marked, "TIKV-1")
	assert.Contains(t, counter.marked, "TIKV-2")
	assert.NotNil(t, stats.data[SQLPlanDigest{SQLDigest: "SQL-1"}])
	assert.Equal(t, uint64(1), stats.data[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["TIKV-1"])
}
