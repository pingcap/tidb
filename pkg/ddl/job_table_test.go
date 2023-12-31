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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/syncer"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

// TestDDLScheduling tests the DDL scheduling. See Concurrent DDL RFC for the rules of DDL scheduling.
// This test checks the chosen job records to see if there are wrong scheduling, if job A and job B cannot run concurrently,
// then all the records of job A must before or after job B, no cross record between these 2 jobs.
func TestDDLScheduling(t *testing.T) {
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

	hook := &callback.TestDDLCallback{}
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
					jobs, err := ddl.GetAllDDLJobs(testkit.NewTestKit(t, store).Session())
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

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockRunJobTime", `return(true)`)
	require.NoError(t, err)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockRunJobTime")
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
		require.FailNow(t, "should not reach here", record)
		return false
	}

	// all checks if all `i` is before `j`.
	all := func(i, j int64) {
		meet := false
		for _, id := range record {
			if id == j {
				meet = true
			}
			require.False(t, meet && id == i, record)
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

func TestUpgradingRelatedJobState(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE e2 (id INT NOT NULL);")

	d := dom.DDL()

	testCases := []struct {
		sql      string
		jobState model.JobState
		err      error
	}{
		{"alter table e2 add index idx(id)", model.JobStateDone, nil},
		{"alter table e2 add index idx1(id)", model.JobStateCancelling, errors.New("[ddl:8214]Cancelled DDL job")},
		{"alter table e2 add index idx2(id)", model.JobStateRollingback, errors.New("[ddl:8214]Cancelled DDL job")},
		{"alter table e2 add index idx3(id)", model.JobStateRollbackDone, errors.New("[ddl:8214]Cancelled DDL job")},
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpgradingState", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpgradingState"))
	}()

	num := 0
	hook := &callback.TestDDLCallback{}
	hook.OnGetJobBeforeExported = func(jobType string) {
		for i := 0; i < 100; i++ {
			time.Sleep(time.Millisecond * 100)
			jobs, err := ddl.GetAllDDLJobs(testkit.NewTestKit(t, store).Session())
			require.NoError(t, err)
			if len(jobs) < 1 || jobs[0].Query != testCases[num].sql {
				continue
			}

			if testCases[num].err != nil && jobs[0].SchemaState == model.StateWriteOnly {
				tk.MustExec("use test")
				tk.MustExec(fmt.Sprintf("admin cancel ddl jobs %d", jobs[0].ID))
			}
			if jobs[0].State == testCases[num].jobState {
				dom.DDL().StateSyncer().UpdateGlobalState(context.Background(), &syncer.StateInfo{State: syncer.StateUpgrading})
			}
			break
		}
	}
	hook.OnGetJobAfterExported = func(jobType string, getJob *model.Job) {
		if getJob.Query == testCases[num].sql && getJob.State == testCases[num].jobState {
			dom.DDL().StateSyncer().UpdateGlobalState(context.Background(), &syncer.StateInfo{State: syncer.StateNormalRunning})
		}
	}
	d.SetHook(hook)

	for i, tc := range testCases {
		num = i
		if tc.err == nil {
			tk.MustExec(tc.sql)
		} else {
			_, err := tk.Exec(tc.sql)
			require.Equal(t, tc.err.Error(), err.Error())
		}
	}
}
