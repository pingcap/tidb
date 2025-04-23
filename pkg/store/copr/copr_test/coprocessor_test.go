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

package copr_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

func TestBuildCopIteratorWithRowCountHint(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithMultiRegions(c, []byte("g"), []byte("n"), []byte("t"))
		}),
	)
	require.NoError(t, err)
	defer require.NoError(t, store.Close())
	copClient := store.GetClient().(*copr.CopClient)
	ctx := context.Background()
	killed := uint32(0)
	vars := kv.NewVariables(&killed)
	opt := &kv.ClientSendOption{}

	ranges := copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req := &kv.Request{
		Tp:          kv.ReqTypeDAG,
		KeyRanges:   kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, copr.CopSmallTaskRow}),
		Concurrency: 15,
	}
	it, errRes := copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc := it.GetConcurrency()
	rateLimit := it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 1)
	require.Equal(t, rateLimit.GetCapacity(), 2)

	ranges = copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req = &kv.Request{
		Tp:          kv.ReqTypeDAG,
		KeyRanges:   kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency: 15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 2)
	require.Equal(t, rateLimit.GetCapacity(), 3)

	// cross-region long range
	ranges = copr.BuildKeyRanges("a", "z")
	req = &kv.Request{
		Tp:          kv.ReqTypeDAG,
		KeyRanges:   kv.NewNonParitionedKeyRangesWithHint(ranges, []int{10}),
		Concurrency: 15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 2)
	require.Equal(t, rateLimit.GetCapacity(), 3)

	ranges = copr.BuildKeyRanges("a", "z")
	req = &kv.Request{
		Tp:          kv.ReqTypeDAG,
		KeyRanges:   kv.NewNonParitionedKeyRangesWithHint(ranges, []int{copr.CopSmallTaskRow + 1}),
		Concurrency: 15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 4)
	require.Equal(t, smallConc, 0)
	require.Equal(t, rateLimit.GetCapacity(), 4)
}

func TestBuildCopIteratorWithBatchStoreCopr(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithMultiRegions(c, []byte("g"), []byte("n"), []byte("t"))
		}),
	)
	require.NoError(t, err)
	defer require.NoError(t, store.Close())
	copClient := store.GetClient().(*copr.CopClient)
	ctx := context.Background()
	killed := uint32(0)
	vars := kv.NewVariables(&killed)
	opt := &kv.ClientSendOption{}

	ranges := copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req := &kv.Request{
		Tp:             kv.ReqTypeDAG,
		KeyRanges:      kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency:    15,
		StoreBatchSize: 1,
	}
	it, errRes := copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	tasks := it.GetTasks()
	require.Equal(t, len(tasks), 2)
	require.Equal(t, len(tasks[0].ToPBBatchTasks()), 1)
	require.Equal(t, tasks[0].RowCountHint, 5)
	require.Equal(t, len(tasks[1].ToPBBatchTasks()), 1)
	require.Equal(t, tasks[1].RowCountHint, 9)

	ranges = copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req = &kv.Request{
		Tp:             kv.ReqTypeDAG,
		KeyRanges:      kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency:    15,
		StoreBatchSize: 3,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	tasks = it.GetTasks()
	require.Equal(t, len(tasks), 1)
	require.Equal(t, len(tasks[0].ToPBBatchTasks()), 3)
	require.Equal(t, tasks[0].RowCountHint, 14)

	// paging will disable store batch.
	ranges = copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req = &kv.Request{
		Tp:             kv.ReqTypeDAG,
		KeyRanges:      kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency:    15,
		StoreBatchSize: 3,
		Paging: struct {
			Enable        bool
			MinPagingSize uint64
			MaxPagingSize uint64
		}{
			Enable:        true,
			MinPagingSize: 1,
			MaxPagingSize: 1024,
		},
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	tasks = it.GetTasks()
	require.Equal(t, len(tasks), 4)

	// only small tasks will be batched.
	ranges = copr.BuildKeyRanges("a", "b", "h", "i", "o", "p")
	req = &kv.Request{
		Tp:             kv.ReqTypeDAG,
		KeyRanges:      kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 33, 32}),
		Concurrency:    15,
		StoreBatchSize: 3,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	tasks = it.GetTasks()
	require.Equal(t, len(tasks), 2)
	require.Equal(t, len(tasks[0].ToPBBatchTasks()), 1)
	require.Equal(t, len(tasks[1].ToPBBatchTasks()), 0)
}

type mockResourceGroupProvider struct {
	rmclient.ResourceGroupProvider
	cfg rmclient.Config
}

func (p *mockResourceGroupProvider) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	if !bytes.Equal(pd.ControllerConfigPathPrefixBytes, key) {
		return nil, errors.New("unsupported configPath")
	}
	payload, _ := json.Marshal(&p.cfg)
	return &meta_storagepb.GetResponse{
		Count: 1,
		Kvs: []*meta_storagepb.KeyValue{
			{
				Key:   key,
				Value: payload,
			},
		},
	}, nil
}

func (p *mockResourceGroupProvider) GetResourceGroup(ctx context.Context, name string, opts ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	group1 := "rg1"
	if name == group1 {
		return &rmpb.ResourceGroup{
			Name: group1,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   2000,
						BurstLimit: 2000,
					},
				},
			},
			RunawaySettings: &rmpb.RunawaySettings{
				Rule: &rmpb.RunawayRule{
					ExecElapsedTimeMs: 1000,
				},
				Action: rmpb.RunawayAction_DryRun,
			},
		}, nil
	}
	return nil, errors.New("not found")
}

func TestBuildCopIteratorWithRunawayChecker(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithMultiRegions(c, []byte("g"), []byte("n"), []byte("t"))
		}),
	)
	require.NoError(t, err)
	defer require.NoError(t, store.Close())
	copClient := store.GetClient().(*copr.CopClient)
	ctx := context.Background()
	killed := uint32(0)
	vars := kv.NewVariables(&killed)
	opt := &kv.ClientSendOption{}
	mockPrivider := &mockResourceGroupProvider{
		cfg: *rmclient.DefaultConfig(),
	}

	ranges := copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	resourceCtl, err := rmclient.NewResourceGroupController(context.Background(), 1, mockPrivider, nil)
	require.NoError(t, err)
	manager := runaway.NewRunawayManager(resourceCtl, "mock://test", nil, nil, nil, nil)
	defer manager.Stop()

	sql := "select * from t"
	group1 := "rg1"
	checker := manager.DeriveChecker(group1, sql, "test", "test", time.Now())
	manager.AddWatch(&runaway.QuarantineRecord{
		ID:                1,
		ResourceGroupName: group1,
		Watch:             rmpb.RunawayWatchType_Exact,
		WatchText:         sql,
		Action:            rmpb.RunawayAction_CoolDown,
	})
	req := &kv.Request{
		Tp:                kv.ReqTypeDAG,
		KeyRanges:         kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency:       15,
		RunawayChecker:    checker,
		ResourceGroupName: group1,
	}
	checker.BeforeExecutor()
	it, errRes := copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	concurrency, smallTaskConcurrency := it.GetConcurrency()
	require.Equal(t, concurrency, 1)
	require.Equal(t, smallTaskConcurrency, 0)
}

func TestQueryWithConcurrentSmallCop(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, b int, c int, index idx_b(b)) partition by hash(id) partitions 10;")
	for i := range 10 {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%v, %v, %v)", i, i, i))
	}
	tk.MustExec("create table t2 (id bigint unsigned key, b int, index idx_b (b));")
	tk.MustExec("insert into t2 values (1,1), (18446744073709551615,2)")
	tk.MustExec("set @@tidb_distsql_scan_concurrency=15")
	tk.MustExec("set @@tidb_executor_concurrency=15")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCSlowCop", `return(100)`))
	// Test for https://github.com/pingcap/tidb/pull/57522#discussion_r1875515863
	start := time.Now()
	tk.MustQuery("select sum(c) from t1 use index (idx_b) where b < 10;")
	require.Less(t, time.Since(start), time.Millisecond*250)
	// Test for index reader with partition table
	start = time.Now()
	tk.MustQuery("select id, b from t1 use index (idx_b) where b < 10;")
	require.Less(t, time.Since(start), time.Millisecond*150)
	// Test for table reader with partition table.
	start = time.Now()
	tk.MustQuery("select * from t1 where c < 10;")
	require.Less(t, time.Since(start), time.Millisecond*150)
	// 	// Test for table reader with 2 parts ranges.
	start = time.Now()
	tk.MustQuery("select * from t2 where id >= 1 and id <= 18446744073709551615 order by id;")
	require.Less(t, time.Since(start), time.Millisecond*150)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCSlowCop"))
}

func TestDMLWithLiteCopWorker(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id bigint auto_increment key, b int);")
	tk.MustExec("insert into t1 (b) values (1),(2),(3),(4),(5),(6),(7),(8);")
	for range 8 {
		tk.MustExec("insert into t1 (b) select b from t1;")
	}
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("2048"))
	tk.MustQuery("split table t1 by (1025);").Check(testkit.Rows("1 1"))
	tk.MustExec("set @@tidb_enable_paging = off")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCSlowCop", `return(200)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/distsql/mockConsumeSelectRespSlow", `return(100)`))
	start := time.Now()
	tk.MustExec("update t1 set b=b+1 where id >= 0;")
	require.Less(t, time.Since(start), time.Millisecond*800) // 3 * 200ms + 1 * 100ms
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCSlowCop"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/distsql/mockConsumeSelectRespSlow"))

	// Test select after split table.
	tk.MustExec("truncate table t1;")
	tk.MustExec("insert into t1 (b) values (1),(2),(3),(4),(5),(6),(7),(8);")
	tk.MustQuery("split table t1 by (3), (6), (9);").Check(testkit.Rows("3 1"))
	tk.MustQuery("select b from t1 order by id").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8"))
}
