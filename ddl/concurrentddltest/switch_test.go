// Copyright 2022 PingCAP, Inc.
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

package concurrentddltest

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestConcurrentDDLSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)

	type table struct {
		columnIdx int
		indexIdx  int
	}

	var tables []*table
	tblCount := 20
	for i := 0; i < tblCount; i++ {
		tables = append(tables, &table{1, 0})
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=0")
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt=1")
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size=32")

	for i := range tables {
		tk.MustExec(fmt.Sprintf("create table t%d (col0 int)", i))
		for j := 0; j < 1000; j++ {
			tk.MustExec(fmt.Sprintf("insert into t%d values (%d)", i, j))
		}
	}

	ddls := make([]string, 0, tblCount)
	ddlCount := 100
	for i := 0; i < ddlCount; i++ {
		tblIdx := rand.Intn(tblCount)
		if rand.Intn(2) == 0 {
			ddls = append(ddls, fmt.Sprintf("alter table t%d add index idx%d (col0)", tblIdx, tables[tblIdx].indexIdx))
			tables[tblIdx].indexIdx++
		} else {
			ddls = append(ddls, fmt.Sprintf("alter table t%d add column col%d int", tblIdx, tables[tblIdx].columnIdx))
			tables[tblIdx].columnIdx++
		}
	}

	c := atomic.NewInt32(0)
	ch := make(chan struct{})
	go func() {
		var wg util.WaitGroupWrapper
		for i := range ddls {
			wg.Add(1)
			go func(idx int) {
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec(ddls[idx])
				c.Add(1)
				wg.Done()
			}(i)
		}
		wg.Wait()
		ch <- struct{}{}
	}()

	ticker := time.NewTicker(time.Second)
	count := 0
	done := false
	for !done {
		select {
		case <-ch:
			done = true
		case <-ticker.C:
			var b bool
			var err error
			err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, false, func(ctx context.Context, txn kv.Transaction) error {
				b, err = meta.NewMeta(txn).IsConcurrentDDL()
				return err
			})
			require.NoError(t, err)
			rs, err := testkit.NewTestKit(t, store).Exec(fmt.Sprintf("set @@global.tidb_enable_concurrent_ddl=%t", !b))
			if rs != nil {
				require.NoError(t, rs.Close())
			}
			if err == nil {
				count++
				if b {
					tk := testkit.NewTestKit(t, store)
					tk.MustQuery("select count(*) from mysql.tidb_ddl_job").Check(testkit.Rows("0"))
					tk.MustQuery("select count(*) from mysql.tidb_ddl_reorg").Check(testkit.Rows("0"))
				}
			}
		}
	}

	require.Equal(t, int32(ddlCount), c.Load())
	require.Greater(t, count, 0)

	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for i, tbl := range tables {
		tk.MustQuery(fmt.Sprintf("select count(*) from information_schema.columns where TABLE_SCHEMA = 'test' and TABLE_NAME = 't%d'", i)).Check(testkit.Rows(fmt.Sprintf("%d", tbl.columnIdx)))
		tk.MustExec(fmt.Sprintf("admin check table t%d", i))
		for j := 0; j < tbl.indexIdx; j++ {
			tk.MustExec(fmt.Sprintf("admin check index t%d idx%d", i, j))
		}
	}
}
