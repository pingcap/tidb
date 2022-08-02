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

package ddl_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
)

func TestDDLSchedulingMultiTimes(t *testing.T) {
	if !variable.EnableConcurrentDDL.Load() {
		t.Skipf("test requires concurrent ddl")
	}
	for i := 0; i < 3; i++ {
		testDDLScheduling(t)
	}
}

// testDDLScheduling tests the DDL scheduling. See Concurrent DDL RFC for the rules of DDL scheduling.
// This test checks the chosen job records to see if there are wrong scheduling, if job A and job B cannot run concurrently,
// then the all the record of job A must before or after job B, no cross record between these 2 jobs should be in between.
func testDDLScheduling(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE e (id INT NOT NULL) PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (50), PARTITION p2 VALUES LESS THAN (100));")
	tk.MustExec("CREATE TABLE e2 (id INT NOT NULL);")
	tk.MustExec("CREATE TABLE e3 (id INT NOT NULL);")

	d := dom.DDL()

	ddlJobs := []string{
		"alter table e2 add index idx(id)",
		"alter table e2 add index idx1(id)",
		"alter table e2 add index idx2(id)",
		"create table e5 (id int)",
		"ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e2;",
		"alter table e add index idx(id)",
		"alter table e add partition (partition p3 values less than (150))",
		"create table e4 (id int)",
		"alter table e3 add index idx1(id)",
		"ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e3;",
	}

	hook := &ddl.TestDDLCallback{}
	var wg util.WaitGroupWrapper
	wg.Add(1)
	var once sync.Once
	hook.OnGetJobBeforeExported = func(jobType string) {
		once.Do(func() {
			for i, job := range ddlJobs {
				wg.Run(func() {
					tk := testkit.NewTestKit(t, store)
					tk.MustExec("use test")
					tk.MustExec("set @@tidb_enable_exchange_partition=1")
					recordSet, _ := tk.Exec(job)
					if recordSet != nil {
						require.NoError(t, recordSet.Close())
					}
				})
				for {
					time.Sleep(time.Millisecond * 100)
					jobs, err := ddl.GetAllDDLJobs(testkit.NewTestKit(t, store).Session(), nil)
					require.NoError(t, err)
					if len(jobs) == i+1 {
						break
					}
				}
			}
			wg.Done()
		})
	}

	record := make([]int64, 0, 16)
	hook.OnGetJobAfterExported = func(jobType string, job *model.Job) {
		// record the job schedule order
		record = append(record, job.ID)
	}

	err := failpoint.Enable("github.com/pingcap/tidb/ddl/mockRunJobTime", `return(true)`)
	require.NoError(t, err)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/ddl/mockRunJobTime")
		require.NoError(t, err)
	}()

	d.SetHook(hook)
	wg.Wait()

	// sort all the job id.
	ids := make(map[int64]struct{}, 16)
	for _, id := range record {
		ids[id] = struct{}{}
	}

	sortedIDs := make([]int64, 0, 16)
	for id := range ids {
		sortedIDs = append(sortedIDs, id)
	}
	slices.Sort(sortedIDs)

	// map the job id to the DDL sequence.
	// sortedIDs may looks like [30, 32, 34, 36, ...], it is the same order with the job in `ddlJobs`, 30 is the first job in `ddlJobs`, 32 is second...
	// record may looks like [30, 30, 32, 32, 34, 32, 36, 34, ...]
	// and the we map the record to the DDL sequence, [0, 0, 1, 1, 2, 1, 3, 2, ...]
	for i := range record {
		idx, b := slices.BinarySearch(sortedIDs, record[i])
		require.True(t, b)
		record[i] = int64(idx)
	}

	check(t, record, 0, 1, 2)
	check(t, record, 0, 4)
	check(t, record, 1, 4)
	check(t, record, 2, 4)
	check(t, record, 4, 5)
	check(t, record, 4, 6)
	check(t, record, 4, 9)
	check(t, record, 5, 6)
	check(t, record, 5, 9)
	check(t, record, 6, 9)
	check(t, record, 8, 9)
}

// check will check if there are any cross between ids.
// e.g. if ids is [1, 2] this function checks all `1` is before or after than `2` in record.
func check(t *testing.T, record []int64, ids ...int64) {
	// have return true if there are any `i` is before `j`, false if there are any `j` is before `i`.
	have := func(i, j int64) bool {
		for _, id := range record {
			if id == i {
				return true
			}
			if id == j {
				return false
			}
		}
		require.FailNow(t, "should not reach here")
		return false
	}

	// all checks if all `i` is before `j`.
	all := func(i, j int64) {
		meet := false
		for _, id := range record {
			if id == j {
				meet = true
			}
			require.False(t, meet && id == i)
		}
	}

	for i := 0; i < len(ids)-1; i++ {
		for j := i + 1; j < len(ids); j++ {
			if have(ids[i], ids[j]) {
				all(ids[i], ids[j])
			} else {
				all(ids[j], ids[i])
			}
		}
	}
}

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
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt=1")
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size=32")

	for i := range tables {
		tk.MustExec(fmt.Sprintf("create table t%d (col0 int) partition by range columns (col0) ("+
			"partition p1 values less than (100), "+
			"partition p2 values less than (300), "+
			"partition p3 values less than (500), "+
			"partition p4 values less than (700), "+
			"partition p5 values less than (1000), "+
			"partition p6 values less than maxvalue);",
			i))
		for j := 0; j < 1000; j++ {
			tk.MustExec(fmt.Sprintf("insert into t%d values (%d)", i, j))
		}
	}

	ddls := make([]string, 0, tblCount)
	ddlCount := 500
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

	ticker := time.NewTicker(time.Second * 2)
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
