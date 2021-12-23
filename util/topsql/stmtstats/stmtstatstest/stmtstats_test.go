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

package stmtstatstest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/testutils"
)

func TestExecCount(t *testing.T) {
	// Prepare stmt stats.
	stmtstats.SetupAggregator()
	defer stmtstats.CloseAggregator()

	// Register stmt stats collector.
	var mu sync.Mutex
	total := stmtstats.StatementStatsMap{}
	stmtstats.RegisterCollector(newMockCollector(func(rs []stmtstats.StatementStatsRecord) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range rs {
			total.Merge(r.Data)
		}
	}))

	// Create mock store.
	store, err := mockstore.NewMockStore(mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
	}))
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.Close())
	}()

	// Prepare mock store.
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	d, err := session.BootstrapSession(store)
	assert.NoError(t, err)
	defer d.Close()
	d.SetStatsUpdating(true)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionLog
	})

	// Create table for testing.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")

	// Enable TopSQL
	variable.TopSQLVariable.Enable.Store(true)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = "mock-agent"
	})

	// Execute CRUD.
	const ExecCountPerSQL = 3
	var insertSQLDigest, updateSQLDigest, selectSQLDigest, deleteSQLDigest, prepareExecDigest *parser.Digest
	for n := 0; n < ExecCountPerSQL; n++ {
		sql := fmt.Sprintf("insert into t values (%d, sleep(0.1));", n)
		if n == 0 {
			_, insertSQLDigest = parser.NormalizeDigest(sql)
		}
		tk.MustExec(sql)
	}
	for n := 0; n < ExecCountPerSQL; n++ {
		sql := fmt.Sprintf("update t set a = %d where a = %d and sleep(0.1);", n, n)
		if n == 0 {
			_, updateSQLDigest = parser.NormalizeDigest(sql)
		}
		tk.MustExec(sql)
	}
	for n := 0; n < ExecCountPerSQL; n++ {
		sql := fmt.Sprintf("select a from t where a = %d and sleep(0.1);", n)
		if n == 0 {
			_, selectSQLDigest = parser.NormalizeDigest(sql)
		}
		tk.MustQuery(sql)
	}
	for n := 0; n < ExecCountPerSQL; n++ {
		sql := fmt.Sprintf("delete from t where a = %d and sleep(0.1);", n)
		if n == 0 {
			_, deleteSQLDigest = parser.NormalizeDigest(sql)
		}
		tk.MustExec(sql)
	}

	_, prepareExecDigest = parser.NormalizeDigest("delete from t where sleep(0.1) and a = 1")
	prepareSQL := "prepare stmt from 'delete from t where sleep(?) and a = ?';"
	tk.MustExec(prepareSQL)
	tk.MustExec("set @a=0.1;")
	tk.MustExec("set @b=1;")
	for n := 0; n < ExecCountPerSQL; n++ {
		execSQL := "execute stmt using @a, @b;"
		tk.MustExec(execSQL)
	}

	// Wait for collect.
	time.Sleep(2 * time.Second)

	// Assertion.
	func() {
		mu.Lock()
		defer mu.Unlock()

		assert.NotEmpty(t, total)
		sqlDigests := map[stmtstats.BinaryDigest]struct{}{
			stmtstats.BinaryDigest(insertSQLDigest.Bytes()):   {},
			stmtstats.BinaryDigest(updateSQLDigest.Bytes()):   {},
			stmtstats.BinaryDigest(selectSQLDigest.Bytes()):   {},
			stmtstats.BinaryDigest(deleteSQLDigest.Bytes()):   {},
			stmtstats.BinaryDigest(prepareExecDigest.Bytes()): {},
		}
		found := 0
		for digest, item := range total {
			if _, ok := sqlDigests[digest.SQLDigest]; ok {
				found++
				assert.Equal(t, uint64(ExecCountPerSQL), item.ExecCount)
				assert.True(t, item.SumExecNanoDuration > uint64(time.Millisecond*100*ExecCountPerSQL))
				assert.True(t, item.SumExecNanoDuration < uint64(time.Millisecond*150*ExecCountPerSQL))
				var kvSum uint64
				for _, kvCount := range item.KvStatsItem.KvExecCount {
					kvSum += kvCount
				}
				assert.Equal(t, uint64(ExecCountPerSQL), kvSum)
			}
		}
		assert.Equal(t, 5, found) // insert, update, select, delete
	}()

	// Drop table.
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

type mockCollector struct {
	f func(records []stmtstats.StatementStatsRecord)
}

func newMockCollector(f func(records []stmtstats.StatementStatsRecord)) stmtstats.Collector {
	return &mockCollector{f: f}
}

func (c *mockCollector) CollectStmtStatsRecords(records []stmtstats.StatementStatsRecord) {
	c.f(records)
}
