// Copyright 2024 PingCAP, Inc.
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
	"cmp"
	"context"
	"flag"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestGetDDLJobs(t *testing.T) {
	store := testkit.CreateMockStore(t)

	sess := testkit.NewTestKit(t, store).Session()
	_, err := sess.Execute(context.Background(), "begin")
	require.NoError(t, err)

	txn, err := sess.Txn(true)
	require.NoError(t, err)

	cnt := 10
	jobs := make([]*model.Job, cnt)
	var currJobs2 []*model.Job
	for i := 0; i < cnt; i++ {
		jobs[i] = &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		err := addDDLJobs(sess, txn, jobs[i])
		require.NoError(t, err)

		currJobs, err := ddl.GetAllDDLJobs(sess)
		require.NoError(t, err)
		require.Len(t, currJobs, i+1)

		currJobs2 = currJobs2[:0]
		err = ddl.IterAllDDLJobs(sess, txn, func(jobs []*model.Job) (b bool, e error) {
			for _, job := range jobs {
				if !job.NotStarted() {
					return true, nil
				}
				currJobs2 = append(currJobs2, job)
			}
			return false, nil
		})
		require.NoError(t, err)
		require.Len(t, currJobs2, i+1)
	}

	currJobs, err := ddl.GetAllDDLJobs(sess)
	require.NoError(t, err)

	for i, job := range jobs {
		require.Equal(t, currJobs[i].ID, job.ID)
		require.Equal(t, int64(1), job.SchemaID)
		require.Equal(t, model.ActionCreateTable, job.Type)
	}
	require.Equal(t, currJobs2, currJobs)

	_, err = sess.Execute(context.Background(), "rollback")
	require.NoError(t, err)
}

func TestGetDDLJobsIsSort(t *testing.T) {
	store := testkit.CreateMockStore(t)

	sess := testkit.NewTestKit(t, store).Session()
	_, err := sess.Execute(context.Background(), "begin")
	require.NoError(t, err)

	txn, err := sess.Txn(true)
	require.NoError(t, err)

	// insert 5 drop table jobs to DefaultJobListKey queue
	enQueueDDLJobs(t, sess, txn, model.ActionDropTable, 10, 15)

	// insert 5 create table jobs to DefaultJobListKey queue
	enQueueDDLJobs(t, sess, txn, model.ActionCreateTable, 0, 5)

	// insert add index jobs to AddIndexJobListKey queue
	enQueueDDLJobs(t, sess, txn, model.ActionAddIndex, 5, 10)

	currJobs, err := ddl.GetAllDDLJobs(sess)
	require.NoError(t, err)
	require.Len(t, currJobs, 15)

	isSort := slices.IsSortedFunc(currJobs, func(i, j *model.Job) int {
		return cmp.Compare(i.ID, j.ID)
	})
	require.True(t, isSort)

	_, err = sess.Execute(context.Background(), "rollback")
	require.NoError(t, err)
}

func TestIsJobRollbackable(t *testing.T) {
	cases := []struct {
		tp     model.ActionType
		state  model.SchemaState
		result bool
	}{
		{model.ActionDropIndex, model.StateNone, true},
		{model.ActionDropIndex, model.StateDeleteOnly, false},
		{model.ActionDropSchema, model.StateDeleteOnly, false},
		{model.ActionDropColumn, model.StateDeleteOnly, false},
	}
	job := &model.Job{}
	for _, ca := range cases {
		job.Type = ca.tp
		job.SchemaState = ca.state
		re := job.IsRollbackable()
		require.Equal(t, ca.result, re)
	}
}

func enQueueDDLJobs(t *testing.T, sess sessiontypes.Session, txn kv.Transaction, jobType model.ActionType, start, end int) {
	for i := start; i < end; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     jobType,
		}
		err := addDDLJobs(sess, txn, job)
		require.NoError(t, err)
	}
}

func TestCreateViewConcurrently(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int);")
	tk.MustExec("create view v as select * from t;")
	var (
		counterErr error
		counter    int
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onDDLCreateView", func(job *model.Job) {
		counter++
		if counter > 1 {
			counterErr = fmt.Errorf("create view job should not run concurrently")
			return
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterDeliveryJob", func(job *model.Job) {
		if job.Type == model.ActionCreateView {
			counter--
		}
	})
	var eg errgroup.Group
	for i := 0; i < 5; i++ {
		eg.Go(func() error {
			newTk := testkit.NewTestKit(t, store)
			_, err := newTk.Exec("use test")
			if err != nil {
				return err
			}
			_, err = newTk.Exec("create or replace view v as select * from t;")
			return err
		})
	}
	err := eg.Wait()
	require.NoError(t, err)
	require.NoError(t, counterErr)
}

func TestCreateDropCreateTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("create table t (a int);")

	wg := sync.WaitGroup{}
	var createErr error
	var fpErr error
	var createTable bool

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", func(job *model.Job) {
		if job.Type == model.ActionDropTable && job.SchemaState == model.StateWriteOnly && !createTable {
			fpErr = failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/schemaver/mockOwnerCheckAllVersionSlow", fmt.Sprintf("return(%d)", job.ID))
			wg.Add(1)
			go func() {
				_, createErr = tk1.Exec("create table t (b int);")
				wg.Done()
			}()
			createTable = true
		}
	})
	tk.MustExec("drop table t;")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated")

	wg.Wait()
	require.NoError(t, createErr)
	require.NoError(t, fpErr)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/schemaver/mockOwnerCheckAllVersionSlow"))

	rs := tk.MustQuery("admin show ddl jobs 3;").Rows()
	create1JobID := rs[0][0].(string)
	dropJobID := rs[1][0].(string)
	create0JobID := rs[2][0].(string)
	jobRecordSet, err := tk.Exec("select job_meta from mysql.tidb_ddl_history where job_id in (?, ?, ?);",
		create1JobID, dropJobID, create0JobID)
	require.NoError(t, err)

	var finishTSs []uint64
	req := jobRecordSet.NewChunk(nil)
	err = jobRecordSet.Next(context.Background(), req)
	require.Greater(t, req.NumRows(), 0)
	require.NoError(t, err)
	iter := chunk.NewIterator4Chunk(req.CopyConstruct())
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		jobMeta := row.GetBytes(0)
		job := model.Job{}
		err = job.Decode(jobMeta)
		require.NoError(t, err)
		finishTSs = append(finishTSs, job.BinlogInfo.FinishedTS)
	}
	create1TS, dropTS, create0TS := finishTSs[0], finishTSs[1], finishTSs[2]
	require.Less(t, create0TS, dropTS, "first create should finish before drop")
	require.Less(t, dropTS, create1TS, "second create should finish after drop")
}

func getGlobalID(ctx context.Context, t *testing.T, store kv.Storage) int64 {
	res := int64(0)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		id, err := m.GetGlobalID()
		require.NoError(t, err)
		res = id
		return nil
	}))
	return res
}

func TestGenIDAndInsertJobsWithRetry(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	// disable DDL to avoid it interfere the test
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	dom.DDL().OwnerManager().CampaignCancel()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	// avoid outer retry
	bak := kv.MaxRetryCnt
	kv.MaxRetryCnt = 1
	t.Cleanup(func() {
		kv.MaxRetryCnt = bak
	})

	jobs := []*ddl.JobWrapper{{
		Job: &model.Job{
			Type:       model.ActionCreateTable,
			SchemaName: "test",
			TableName:  "t1",
			Args:       []any{&model.TableInfo{}},
		},
	}}
	initialGID := getGlobalID(ctx, t, store)
	threads, iterations := 10, 500
	tks := make([]*testkit.TestKit, threads)
	for i := 0; i < threads; i++ {
		tks[i] = testkit.NewTestKit(t, store)
	}
	var wg util.WaitGroupWrapper
	for i := 0; i < threads; i++ {
		idx := i
		wg.Run(func() {
			kit := tks[idx]
			ddlSe := sess.NewSession(kit.Session())
			for j := 0; j < iterations; j++ {
				require.NoError(t, ddl.GenGIDAndInsertJobsWithRetry(ctx, ddlSe, jobs))
			}
		})
	}
	wg.Wait()

	jobCount := threads * iterations
	gotJobs, err := ddl.GetAllDDLJobs(tk.Session())
	require.NoError(t, err)
	require.Len(t, gotJobs, jobCount)
	currGID := getGlobalID(ctx, t, store)
	require.Greater(t, currGID-initialGID, int64(jobCount))
	uniqueJobIDs := make(map[int64]struct{}, jobCount)
	for _, j := range gotJobs {
		require.Greater(t, j.ID, initialGID)
		uniqueJobIDs[j.ID] = struct{}{}
	}
	require.Len(t, uniqueJobIDs, jobCount)
}

type idAllocationCase struct {
	jobW            *ddl.JobWrapper
	requiredIDCount int
}

func TestCombinedIDAllocation(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	// disable DDL to avoid it interfere the test
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	dom.DDL().OwnerManager().CampaignCancel()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	// avoid outer retry
	bak := kv.MaxRetryCnt
	kv.MaxRetryCnt = 1
	t.Cleanup(func() {
		kv.MaxRetryCnt = bak
	})

	genTblInfo := func(partitionCnt int) *model.TableInfo {
		info := &model.TableInfo{Partition: &model.PartitionInfo{}}
		for i := 0; i < partitionCnt; i++ {
			info.Partition.Enable = true
			info.Partition.Definitions = append(info.Partition.Definitions, model.PartitionDefinition{})
		}
		return info
	}

	genCreateTblJob := func(tp model.ActionType, partitionCnt int) *model.Job {
		return &model.Job{
			Type: tp,
			Args: []any{genTblInfo(partitionCnt)},
		}
	}

	genCreateTblsJob := func(partitionCounts ...int) *model.Job {
		infos := make([]*model.TableInfo, 0, len(partitionCounts))
		for _, c := range partitionCounts {
			infos = append(infos, genTblInfo(c))
		}
		return &model.Job{
			Type: model.ActionCreateTables,
			Args: []any{infos},
		}
	}

	genCreateDBJob := func() *model.Job {
		info := &model.DBInfo{}
		return &model.Job{
			Type: model.ActionCreateSchema,
			Args: []any{info},
		}
	}

	cases := []idAllocationCase{
		{
			jobW:            ddl.NewJobWrapper(genCreateTblsJob(1, 2, 0), false),
			requiredIDCount: 1 + 3 + 1 + 2,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateTblsJob(3, 4), true),
			requiredIDCount: 1,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateTblJob(model.ActionCreateTable, 3), false),
			requiredIDCount: 1 + 1 + 3,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateTblJob(model.ActionCreateTable, 0), false),
			requiredIDCount: 1 + 1,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateTblJob(model.ActionCreateTable, 8), true),
			requiredIDCount: 1,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateTblJob(model.ActionCreateSequence, 0), false),
			requiredIDCount: 2,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateTblJob(model.ActionCreateSequence, 0), true),
			requiredIDCount: 1,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateTblJob(model.ActionCreateView, 0), false),
			requiredIDCount: 2,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateTblJob(model.ActionCreateView, 0), true),
			requiredIDCount: 1,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateDBJob(), false),
			requiredIDCount: 2,
		},
		{
			jobW:            ddl.NewJobWrapper(genCreateDBJob(), true),
			requiredIDCount: 1,
		},
	}

	t.Run("process one by one", func(t *testing.T) {
		tk.MustExec("delete from mysql.tidb_ddl_job")
		for _, c := range cases {
			currentGlobalID := getGlobalID(ctx, t, store)
			require.NoError(t, ddl.GenGIDAndInsertJobsWithRetry(ctx, sess.NewSession(tk.Session()), []*ddl.JobWrapper{c.jobW}))
			require.Equal(t, currentGlobalID+int64(c.requiredIDCount), getGlobalID(ctx, t, store))
		}
		gotJobs, err := ddl.GetAllDDLJobs(tk.Session())
		require.NoError(t, err)
		require.Len(t, gotJobs, len(cases))
	})

	t.Run("process together", func(t *testing.T) {
		tk.MustExec("delete from mysql.tidb_ddl_job")

		totalRequiredCnt := 0
		jobWs := make([]*ddl.JobWrapper, 0, len(cases))
		for _, c := range cases {
			totalRequiredCnt += c.requiredIDCount
			jobWs = append(jobWs, c.jobW)
		}
		currentGlobalID := getGlobalID(ctx, t, store)
		require.NoError(t, ddl.GenGIDAndInsertJobsWithRetry(ctx, sess.NewSession(tk.Session()), jobWs))
		require.Equal(t, currentGlobalID+int64(totalRequiredCnt), getGlobalID(ctx, t, store))

		gotJobs, err := ddl.GetAllDDLJobs(tk.Session())
		require.NoError(t, err)
		require.Len(t, gotJobs, len(cases))
	})

	t.Run("process IDAllocated = false", func(t *testing.T) {
		tk.MustExec("delete from mysql.tidb_ddl_job")

		initialGlobalID := getGlobalID(ctx, t, store)
		allocIDCaseCount, allocatedIDCount := 0, 0
		for _, c := range cases {
			if !c.jobW.IDAllocated {
				allocIDCaseCount++
				allocatedIDCount += c.requiredIDCount
				require.NoError(t, ddl.GenGIDAndInsertJobsWithRetry(ctx, sess.NewSession(tk.Session()), []*ddl.JobWrapper{c.jobW}))
			}
		}
		require.EqualValues(t, 6, allocIDCaseCount)
		uniqueIDs := make(map[int64]struct{}, len(cases))
		checkTableInfo := func(info *model.TableInfo) {
			uniqueIDs[info.ID] = struct{}{}
			require.Greater(t, info.ID, initialGlobalID)
			if pInfo := info.GetPartitionInfo(); pInfo != nil {
				for _, def := range pInfo.Definitions {
					uniqueIDs[def.ID] = struct{}{}
					require.Greater(t, def.ID, initialGlobalID)
				}
			}
		}
		gotJobs, err := ddl.GetAllDDLJobs(tk.Session())
		require.NoError(t, err)
		require.Len(t, gotJobs, allocIDCaseCount)
		for _, j := range gotJobs {
			uniqueIDs[j.ID] = struct{}{}
			require.Greater(t, j.ID, initialGlobalID)
			switch j.Type {
			case model.ActionCreateTable, model.ActionCreateView, model.ActionCreateSequence:
				require.Greater(t, j.TableID, initialGlobalID)
				info := &model.TableInfo{}
				require.NoError(t, j.DecodeArgs(info))
				require.Equal(t, j.TableID, info.ID)
				checkTableInfo(info)
			case model.ActionCreateTables:
				var infos []*model.TableInfo
				require.NoError(t, j.DecodeArgs(&infos))
				for _, info := range infos {
					checkTableInfo(info)
				}
			case model.ActionCreateSchema:
				require.Greater(t, j.SchemaID, initialGlobalID)
				info := &model.DBInfo{}
				require.NoError(t, j.DecodeArgs(info))
				uniqueIDs[info.ID] = struct{}{}
				require.Equal(t, j.SchemaID, info.ID)
			}
		}
		require.Len(t, uniqueIDs, allocatedIDCount)
	})
}

var (
	threadVar             = flag.Int("threads", 100, "number of threads")
	iterationPerThreadVar = flag.Int("iterations", 30000, "number of iterations per thread")
	payloadSizeVar        = flag.Int("payload-size", 1024, "size of payload in bytes")
)

func TestGenIDAndInsertJobsWithRetryQPS(t *testing.T) {
	t.Skip("it's for offline test only, skip it in CI")
	thread, iterationPerThread, payloadSize := *threadVar, *iterationPerThreadVar, *payloadSizeVar
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	// disable DDL to avoid it interfere the test
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	dom.DDL().OwnerManager().CampaignCancel()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	payload := strings.Repeat("a", payloadSize)
	jobs := []*ddl.JobWrapper{{
		Job: &model.Job{
			Type:       model.ActionCreateTable,
			SchemaName: "test",
			TableName:  "t1",
			Args:       []any{&model.TableInfo{Comment: payload}},
		},
	}}
	counters := make([]atomic.Int64, thread+1)
	var wg util.WaitGroupWrapper
	for i := 0; i < thread; i++ {
		index := i
		wg.Run(func() {
			kit := testkit.NewTestKit(t, store)
			ddlSe := sess.NewSession(kit.Session())
			for i := 0; i < iterationPerThread; i++ {
				require.NoError(t, ddl.GenGIDAndInsertJobsWithRetry(ctx, ddlSe, jobs))

				counters[0].Add(1)
				counters[index+1].Add(1)
			}
		})
	}
	go func() {
		getCounts := func() []int64 {
			res := make([]int64, len(counters))
			for i := range counters {
				res[i] = counters[i].Load()
			}
			return res
		}
		lastCnt := getCounts()
		for {
			time.Sleep(5 * time.Second)
			currCnt := getCounts()
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("QPS - total:%.0f", float64(currCnt[0]-lastCnt[0])/5))
			for i := 1; i < min(len(counters), 10); i++ {
				sb.WriteString(fmt.Sprintf(", thread-%d: %.0f", i, float64(currCnt[i]-lastCnt[i])/5))
			}
			if len(counters) > 10 {
				sb.WriteString("...")
			}
			lastCnt = currCnt
			fmt.Println(sb.String())
		}
	}()
	wg.Wait()
}
