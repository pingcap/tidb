// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type staticSessionManager struct {
	sessmgr.Manager
	processes map[uint64]*sessmgr.ProcessInfo
	calls     atomic.Int32
}

func (sm *staticSessionManager) ShowProcessList() map[uint64]*sessmgr.ProcessInfo {
	sm.calls.Add(1)
	return sm.processes
}

func TestExpensiveTxnSkipsSnapshotOnlySession(t *testing.T) {
	core, observed := observer.New(zapcore.WarnLevel)
	restoreLogger := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{
		Core:  core,
		Level: zap.NewAtomicLevelAt(zapcore.WarnLevel),
	})
	defer restoreLogger()

	stmtCtx := stmtctx.NewStmtCtx()
	var refCount stmtctx.ReferenceCount
	newProcessInfo := func(id, startTS uint64, createTime time.Time) *sessmgr.ProcessInfo {
		return &sessmgr.ProcessInfo{
			ID:                id,
			CurTxnStartTS:     startTS,
			CurTxnCreateTime:  createTime,
			StmtCtx:           stmtCtx,
			RefCountOfStmtCtx: &refCount,
			StatsInfo:         func(any) map[string]uint64 { return nil },
		}
	}
	sm := &staticSessionManager{processes: map[uint64]*sessmgr.ProcessInfo{
		1: newProcessInfo(1, 123, time.Time{}),
		2: newProcessInfo(2, 456, time.Now().Add(-time.Hour)),
	}}
	exitCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		NewExpensiveQueryHandle(exitCh).SetSessionManager(sm).Run()
		close(done)
	}()
	defer func() {
		close(exitCh)
		<-done
	}()

	// By the second poll, the first process list has been fully checked.
	require.Eventually(t, func() bool { return sm.calls.Load() >= 2 }, 5*time.Second, 10*time.Millisecond)
	entries := observed.FilterMessage("expensive_txn").All()
	require.Len(t, entries, 1)
	require.EqualValues(t, 2, entries[0].ContextMap()["conn"])
}

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}
