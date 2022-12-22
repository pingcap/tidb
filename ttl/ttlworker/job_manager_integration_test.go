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

package ttlworker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	dbsession "github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/ttl/ttlworker"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func TestParallelLockNewJob(t *testing.T) {
	store := testkit.CreateMockStore(t)

	sessionFactory := func() session.Session {
		dbSession, err := dbsession.CreateSession4Test(store)
		require.NoError(t, err)
		se := session.NewSession(dbSession, dbSession, nil)

		_, err = se.ExecuteSQL(context.Background(), "ROLLBACK")
		require.NoError(t, err)
		_, err = se.ExecuteSQL(context.Background(), "set tidb_retry_limit=0")
		require.NoError(t, err)

		return se
	}

	storedTTLJobRunInterval := variable.TTLJobRunInterval.Load()
	variable.TTLJobRunInterval.Store(0)
	defer func() {
		variable.TTLJobRunInterval.Store(storedTTLJobRunInterval)
	}()

	testTable := &cache.PhysicalTable{ID: 2, TableInfo: &model.TableInfo{ID: 1, TTLInfo: &model.TTLInfo{IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitDay)}}}
	// simply lock a new job
	m := ttlworker.NewJobManager("test-id", nil, store)
	se := sessionFactory()
	job, err := m.LockNewJob(context.Background(), se, testTable, time.Now())
	require.NoError(t, err)
	job.Finish(se, time.Now())

	// lock one table in parallel, only one of them should lock successfully
	testTimes := 100
	concurrency := 5
	for i := 0; i < testTimes; i++ {
		successCounter := atomic.NewUint64(0)
		successJob := &ttlworker.TTLJob{}

		wg := sync.WaitGroup{}
		for j := 0; j < concurrency; j++ {
			jobManagerID := fmt.Sprintf("test-ttl-manager-%d", j)
			wg.Add(1)
			go func() {
				m := ttlworker.NewJobManager(jobManagerID, nil, store)

				se := sessionFactory()
				job, err := m.LockNewJob(context.Background(), se, testTable, time.Now())
				if err == nil {
					successCounter.Add(1)
					successJob = job
				} else {
					logutil.BgLogger().Error("lock new job with error", zap.Error(err))
				}
				wg.Done()
			}()
		}
		wg.Wait()

		require.Equal(t, uint64(1), successCounter.Load())
		successJob.Finish(se, time.Now())
	}
}
