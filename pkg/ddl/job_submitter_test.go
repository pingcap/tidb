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
	"context"
	"flag"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

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
	submitter := ddl.NewJobSubmitterForTest()
	for i := 0; i < threads; i++ {
		idx := i
		wg.Run(func() {
			kit := tks[idx]
			ddlSe := sess.NewSession(kit.Session())
			for j := 0; j < iterations; j++ {
				require.NoError(t, submitter.GenGIDAndInsertJobsWithRetry(ctx, ddlSe, jobs))
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

	submitter := ddl.NewJobSubmitterForTest()
	t.Run("process one by one", func(t *testing.T) {
		tk.MustExec("delete from mysql.tidb_ddl_job")
		for _, c := range cases {
			currentGlobalID := getGlobalID(ctx, t, store)
			require.NoError(t, submitter.GenGIDAndInsertJobsWithRetry(ctx, sess.NewSession(tk.Session()), []*ddl.JobWrapper{c.jobW}))
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
		require.NoError(t, submitter.GenGIDAndInsertJobsWithRetry(ctx, sess.NewSession(tk.Session()), jobWs))
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
				require.NoError(t, submitter.GenGIDAndInsertJobsWithRetry(ctx, sess.NewSession(tk.Session()), []*ddl.JobWrapper{c.jobW}))
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
	submitter := ddl.NewJobSubmitterForTest()
	for i := 0; i < thread; i++ {
		index := i
		wg.Run(func() {
			kit := testkit.NewTestKit(t, store)
			ddlSe := sess.NewSession(kit.Session())
			for i := 0; i < iterationPerThread; i++ {
				require.NoError(t, submitter.GenGIDAndInsertJobsWithRetry(ctx, ddlSe, jobs))

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

func TestGenGIDAndInsertJobsWithRetryOnErr(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	// disable DDL to avoid it interfere the test
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	dom.DDL().OwnerManager().CampaignCancel()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	ddlSe := sess.NewSession(tk.Session())
	jobs := []*ddl.JobWrapper{{
		Job: &model.Job{
			Type:       model.ActionCreateTable,
			SchemaName: "test",
			TableName:  "t1",
			Args:       []any{&model.TableInfo{}},
		},
	}}
	submitter := ddl.NewJobSubmitterForTest()
	// retry for 3 times
	currGID := getGlobalID(ctx, t, store)
	var counter int64
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockGenGIDRetryableError", `3*return(true)`)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onGenGIDRetry", func() {
		m := submitter.DDLJobDoneChMap()
		require.Equal(t, 1, len(m.Keys()))
		_, ok := m.Load(currGID + counter*100 + 2)
		require.True(t, ok)
		counter++
		require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			_, err := m.GenGlobalIDs(100)
			require.NoError(t, err)
			return nil
		}))
	})
	require.Zero(t, len(submitter.DDLJobDoneChMap().Keys()))
	require.NoError(t, submitter.GenGIDAndInsertJobsWithRetry(ctx, ddlSe, jobs))
	require.EqualValues(t, 3, counter)
	newGID := getGlobalID(ctx, t, store)
	require.Equal(t, currGID+300+2, newGID)
	m := submitter.DDLJobDoneChMap()
	require.Equal(t, 1, len(m.Keys()))
	_, ok := m.Load(newGID)
	require.True(t, ok)
	require.Equal(t, newGID-1, jobs[0].TableID)
}
