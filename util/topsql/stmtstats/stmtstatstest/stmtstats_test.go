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
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
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
	stmtstats.RegisterCollector(newMockCollector(func(data stmtstats.StatementStatsMap) {
		mu.Lock()
		defer mu.Unlock()
		total.Merge(data)
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
	tk.MustExec("create table t(a int);")

	// Enable TopSQL
	topsqlstate.GlobalState.Enable.Store(true)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = "mock-agent"
	})

	// Execute CRUD.
	const ExecCountPerSQL = 100
	_, insertSQLDigest := parser.NormalizeDigest("insert into t values (0);")
	for n := 0; n < ExecCountPerSQL; n++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d);", n))
	}
	_, updateSQLDigest := parser.NormalizeDigest("update t set a = 0 where a = 0;")
	for n := 0; n < ExecCountPerSQL; n++ {
		tk.MustExec(fmt.Sprintf("update t set a = %d where a = %d;", n, n))
	}
	_, selectSQLDigest := parser.NormalizeDigest("select a from t where a = 0;")
	for n := 0; n < ExecCountPerSQL; n++ {
		tk.MustQuery(fmt.Sprintf("select a from t where a = %d;", n))
	}
	_, deleteSQLDigest := parser.NormalizeDigest("delete from t where a = 0;")
	for n := 1; n <= ExecCountPerSQL; n++ {
		tk.MustExec(fmt.Sprintf("delete from t where a = %d;", n))
	}

	// Wait for collect.
	time.Sleep(2 * time.Second)

	// Assertion.
	func() {
		mu.Lock()
		defer mu.Unlock()

		assert.NotEmpty(t, total)
		sqlDigests := map[stmtstats.BinaryDigest]struct{}{
			stmtstats.BinaryDigest(insertSQLDigest.Bytes()): {},
			stmtstats.BinaryDigest(updateSQLDigest.Bytes()): {},
			stmtstats.BinaryDigest(selectSQLDigest.Bytes()): {},
			stmtstats.BinaryDigest(deleteSQLDigest.Bytes()): {},
		}
		found := 0
		for digest, item := range total {
			if _, ok := sqlDigests[digest.SQLDigest]; ok {
				found++
				assert.Equal(t, uint64(ExecCountPerSQL), item.ExecCount)
				var kvSum uint64
				for _, kvCount := range item.KvStatsItem.KvExecCount {
					kvSum += kvCount
				}
				assert.Equal(t, uint64(ExecCountPerSQL), kvSum)
			}
		}
		assert.Equal(t, 4, found) // insert, update, select, delete
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
	f func(data stmtstats.StatementStatsMap)
}

func newMockCollector(f func(data stmtstats.StatementStatsMap)) stmtstats.Collector {
	return &mockCollector{f: f}
}

func (c *mockCollector) CollectStmtStatsMap(data stmtstats.StatementStatsMap) {
	c.f(data)
}
