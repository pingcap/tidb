// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestLogFormat(t *testing.T) {
	mem := memory.NewTracker(-1, -1)
	mem.Consume(1<<30 + 1<<29 + 1<<28 + 1<<27)
	info := &util.ProcessInfo{
		ID:            233,
		User:          "PingCAP",
		Host:          "127.0.0.1",
		DB:            "Database",
		Info:          "select * from table where a > 1",
		CurTxnStartTS: 23333,
		StatsInfo: func(interface{}) map[string]uint64 {
			return nil
		},
		StmtCtx: &stmtctx.StatementContext{
			MemTracker: mem,
		},
		RedactSQL: false,
	}
	costTime := time.Second * 233
	logFields := genLogFields(costTime, info)

	assert.Len(t, logFields, 7)
	assert.Equal(t, "cost_time", logFields[0].Key)
	assert.Equal(t, "233s", logFields[0].String)
	assert.Equal(t, "conn_id", logFields[1].Key)
	assert.Equal(t, int64(233), logFields[1].Integer)
	assert.Equal(t, "user", logFields[2].Key)
	assert.Equal(t, "PingCAP", logFields[2].String)
	assert.Equal(t, "database", logFields[3].Key)
	assert.Equal(t, "Database", logFields[3].String)
	assert.Equal(t, "txn_start_ts", logFields[4].Key)
	assert.Equal(t, int64(23333), logFields[4].Integer)
	assert.Equal(t, "mem_max", logFields[5].Key)
	assert.Equal(t, "2013265920 Bytes (1.88 GB)", logFields[5].String)
	assert.Equal(t, "sql", logFields[6].Key)
	assert.Equal(t, "select * from table where a > 1", logFields[6].String)

	info.RedactSQL = true
	logFields = genLogFields(costTime, info)
	assert.Equal(t, "select * from table where `a` > ?", logFields[6].String)
}
