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
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

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

	jobs := []*model.Job{
		{
			Type:       model.ActionCreateTable,
			SchemaName: "test",
			TableName:  "t1",
		},
	}
	var wg util.WaitGroupWrapper
	for i := 0; i < 10; i++ {
		wg.Run(func() {
			kit := testkit.NewTestKit(t, store)
			ddlSe := sess.NewSession(kit.Session())
			for i := 0; i < 1000; i++ {
				require.NoError(t, ddl.GenIDAndInsertJobsWithRetry(ctx, ddlSe, jobs))
			}
		})
	}
	wg.Wait()

	jobs, err := ddl.GetAllDDLJobs(tk.Session())
	require.NoError(t, err)
	require.Len(t, jobs, 10000)
	var maxID, currUsedGID int64
	for _, j := range jobs {
		maxID = max(maxID, j.ID)
	}
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		currUsedGID, err = m.GetGlobalID()
		require.NoError(t, err)
		return nil
	}))
	require.Greater(t, currUsedGID, int64(10000))
	require.Equal(t, currUsedGID, maxID)
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

	payload := []byte(strings.Repeat("a", payloadSize))
	jobs := []*model.Job{
		{
			Type:       model.ActionCreateTable,
			SchemaName: "test",
			TableName:  "t1",
			Args:       []any{payload},
		},
	}
	counters := make([]atomic.Int64, thread+1)
	var wg util.WaitGroupWrapper
	for i := 0; i < thread; i++ {
		index := i
		wg.Run(func() {
			kit := testkit.NewTestKit(t, store)
			ddlSe := sess.NewSession(kit.Session())
			for i := 0; i < iterationPerThread; i++ {
				require.NoError(t, ddl.GenIDAndInsertJobsWithRetry(ctx, ddlSe, jobs))

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
