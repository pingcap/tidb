// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttlworker_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testflag"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/stretchr/testify/require"
)

func TestCancelWhileScan(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create table test.t (id int, created_at datetime) TTL= created_at + interval 1 hour")
	testTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	for i := 0; i < 10000; i++ {
		tk.MustExec(fmt.Sprintf("insert into test.t values (%d, NOW() - INTERVAL 24 HOUR)", i))
	}
	testPhysicalTableCache, err := cache.NewPhysicalTable(ast.NewCIStr("test"), testTable.Meta(), ast.NewCIStr(""))
	require.NoError(t, err)

	delCh := make(chan *ttlworker.TTLDeleteTask)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range delCh {
			// do nothing
		}
	}()

	testStart := time.Now()
	testDuration := time.Second
	if testflag.Long() {
		testDuration = time.Minute
	}
	for time.Since(testStart) < testDuration {
		ctx, cancel := context.WithCancel(context.Background())
		ttlTask := ttlworker.NewTTLScanTask(ctx, testPhysicalTableCache, &cache.TTLTask{
			JobID:            "test",
			TableID:          1,
			ScanID:           1,
			ScanRangeStart:   nil,
			ScanRangeEnd:     nil,
			ExpireTime:       time.Now().Add(-12 * time.Hour),
			OwnerID:          "test",
			OwnerAddr:        "test",
			OwnerHBTime:      time.Now(),
			Status:           cache.TaskStatusRunning,
			StatusUpdateTime: time.Now(),
			State:            &cache.TTLTaskState{},
			CreatedTime:      time.Now(),
		})

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			ttlTask.DoScan(ctx, delCh, dom.SysSessionPool())
		}()

		// randomly sleep for a while and cancel the scan
		time.Sleep(time.Duration(rand.Int() % int(time.Millisecond*10)))
		startCancel := time.Now()
		cancel()

		wg.Wait()
		// make sure the scan is canceled in time
		require.Less(t, time.Since(startCancel), time.Second)
	}

	close(delCh)
	wg.Wait()
}
