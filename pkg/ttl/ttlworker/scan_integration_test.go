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
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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
	for i := range 10000 {
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

			ttlTask.DoScan(ctx, delCh, dom.AdvancedSysSessionPool())
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

func TestCancelWhileScanAtStatementBoundary(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	origBatchSize := vardef.TTLScanBatchSize.Load()
	vardef.TTLScanBatchSize.Store(30)
	t.Cleanup(func() {
		vardef.TTLScanBatchSize.Store(origBatchSize)
	})

	tk.MustExec("create table test.t (id int primary key, created_at datetime) TTL= created_at + interval 1 hour")
	tk.MustExec("split table test.t between (0) and (30000) regions 30")
	for i := range 30 {
		tk.MustExec(fmt.Sprintf("insert into test.t values (%d, NOW() - INTERVAL 24 HOUR)", i*1000))
	}
	testTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	testPhysicalTableCache, err := cache.NewPhysicalTable(ast.NewCIStr("test"), testTable.Meta(), ast.NewCIStr(""))
	require.NoError(t, err)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/store/copr/sleepCoprRequest", "return(2000)")

	taskCtx, cancelTask := context.WithCancel(context.Background())
	defer cancelTask()
	ttlTask := ttlworker.NewTTLScanTask(taskCtx, testPhysicalTableCache, &cache.TTLTask{
		JobID:            "test",
		TableID:          testTable.Meta().ID,
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

	triggerCancel := make(chan struct{})
	var cancelOnce sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/beforeResetSQLKillerForTTLScan", func(stmt ast.StmtNode) {
		if _, ok := stmt.(*ast.SelectStmt); !ok {
			return
		}

		cancelOnce.Do(func() {
			cancelTask()
			close(triggerCancel)
			time.Sleep(100 * time.Millisecond)
		})
	})

	delCh := make(chan *ttlworker.TTLDeleteTask)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		for range delCh {
		}
	}()

	doScanDone := make(chan struct{})
	go func() {
		defer close(doScanDone)
		ttlTask.DoScan(context.Background(), delCh, dom.AdvancedSysSessionPool())
	}()

	select {
	case <-triggerCancel:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "TTL scan SELECT was not reached")
	}

	select {
	case <-doScanDone:
	case <-time.After(time.Second):
		require.FailNow(t, "TTL scan was not canceled within 1s after statement-boundary cancel")
	}

	close(delCh)
	<-doneCh
}
