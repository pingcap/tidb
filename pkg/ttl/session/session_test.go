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

package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSessionTTLJobRU(t *testing.T) {
	original := config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()
	t.Cleanup(func() { config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(original) })
	config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(true)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table ttl_ru(id int primary key, v int)")
	tk.MustExec("insert into ttl_ru values (1, 10), (2, 20)")
	vars := tk.Session().GetSessionVars()
	vars.InRestrictedSQL = true
	se := session.NewSession(tk.Session(), func() {})
	jobSe := session.WithJob(se, "job-1")
	ctx := context.Background()

	exec := func(se session.Session, sql string, counted bool) {
		t.Helper()
		before := testutil.ToFloat64(metrics.RUV3Total)
		ttlBefore := testutil.ToFloat64(metrics.RUV3TTLTotal)
		_, err := se.ExecuteSQL(ctx, sql)
		require.NoError(t, err)
		after := testutil.ToFloat64(metrics.RUV3Total)
		ttlAfter := testutil.ToFloat64(metrics.RUV3TTLTotal)
		if counted {
			require.Greater(t, after, before, sql)
			require.InDelta(t, after-before, ttlAfter-ttlBefore, 1e-9, sql)
		} else {
			require.Equal(t, before, after, sql)
			require.Equal(t, ttlBefore, ttlAfter, sql)
		}
		require.Empty(t, vars.TTLJobID)
		require.True(t, vars.InRestrictedSQL)
	}
	exec(se, "select * from ttl_ru", false)
	exec(session.WithJob(se, ""), "select * from ttl_ru", false)
	exec(jobSe, "select * from ttl_ru", true)
	exec(jobSe, "delete from ttl_ru where id=1", true)

	var statements, jobIDs []string
	var committedKeys, committedBytes float64
	var publications int
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/observeStatementRUCalibrationUnitsForTest", func(
		connectionID uint64, _ string, _, _, _, _, _, _ float64,
		_ float64, _ float64, keys, bytes float64,
	) {
		if connectionID == vars.ConnectionID {
			publications++
			committedKeys += keys
			committedBytes += bytes
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/observeStatementRUOwnerInstallForTest", func(stmt *executor.ExecStmt) {
		if stmt.Ctx == tk.Session() {
			statements = append(statements, stmt.GetTextToLog(false))
			jobIDs = append(jobIDs, vars.TTLJobID)
		}
	})
	require.NoError(t, jobSe.RunInTxn(ctx, func() error {
		exec(jobSe, "delete from ttl_ru where id=2", true)
		// A global query in the same transaction must not inherit job attribution.
		exec(se, "select count(*) from ttl_ru", false)
		return nil
	}, session.TxnModeOptimistic))
	require.Equal(t, []string{"job-1", "job-1", "", "job-1"}, jobIDs, statements)
	require.Equal(t, 2, publications, "DELETE and COMMIT each publish once")
	require.Positive(t, committedKeys)
	require.Positive(t, committedBytes)
	require.Empty(t, vars.TTLJobID)

	// A rewrapped session uses the new job, and cancellation/error cleanup does
	// not retain either job on the pooled session.
	jobIDs = nil
	jobSe = session.WithJob(jobSe, "job-2")
	require.ErrorContains(t, jobSe.RunInTxn(ctx, func() error {
		return errors.New("abort job transaction")
	}, session.TxnModeOptimistic), "abort job transaction")
	require.Equal(t, []string{"job-2", "job-2"}, jobIDs)
	require.Empty(t, vars.TTLJobID)

	_, err := jobSe.ExecuteSQL(ctx, "select * from missing_ttl_ru_table")
	require.Error(t, err)
	require.Empty(t, vars.TTLJobID)
	exec(se, "select * from ttl_ru", false)
}

func TestSessionRunInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, v int)")
	se := session.NewSession(tk.Session(), func() {})
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	require.NoError(t, se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (1, 10)")
		return nil
	}, session.TxnModeOptimistic))
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10"))

	err := se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (2, 20)")
		return errors.New("mockErr")
	}, session.TxnModeOptimistic)
	require.EqualError(t, err, "mockErr")
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10"))

	require.NoError(t, se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (3, 30)")
		return nil
	}, session.TxnModeOptimistic))
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10", "3 30"))
}

func TestSessionKill(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	se := session.NewSession(tk.Session(), func() {})
	sleepStmt := "select sleep(123)"
	wg := util.WaitGroupWrapper{}
	wg.Run(func() {
		start := time.Now()
		for time.Since(start) < 10*time.Second {
			time.Sleep(10 * time.Millisecond)
			processes := do.InfoSyncer().GetSessionManager().ShowProcessList()
			for _, proc := range processes {
				if proc.Info == sleepStmt {
					se.KillStmt()
					return
				}
			}
		}
		require.FailNow(t, "wait sleep stmt timeout")
	})
	// the killed sleep stmt will return "1"
	tk.MustQuery(sleepStmt).Check(testkit.Rows("1"))
	wg.Wait()
}
