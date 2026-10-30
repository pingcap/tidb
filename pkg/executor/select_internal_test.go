// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type lockCtxTxnManager struct {
	sessiontxn.TxnManager
	readTS            uint64
	forUpdateTS       uint64
	readSnapshot      kv.Snapshot
	forUpdateSnapshot kv.Snapshot
}

func (m lockCtxTxnManager) GetStmtReadTS() (uint64, error)      { return m.readTS, nil }
func (m lockCtxTxnManager) GetStmtForUpdateTS() (uint64, error) { return m.forUpdateTS, nil }

func (m lockCtxTxnManager) GetSnapshotWithStmtReadTS() (kv.Snapshot, error) {
	return m.readSnapshot, nil
}

func (m lockCtxTxnManager) GetSnapshotWithStmtForUpdateTS() (kv.Snapshot, error) {
	return m.forUpdateSnapshot, nil
}

type mviewSnapshotProbe struct{ kv.Snapshot }

func (*mviewSnapshotProbe) SetOption(int, any) {}

func TestNewLockCtxPropagatesSharedLockUpgrade(t *testing.T) {
	originalGetTxnManager := sessiontxn.GetTxnManager
	sessiontxn.GetTxnManager = func(sessionctx.Context) sessiontxn.TxnManager {
		return lockCtxTxnManager{forUpdateTS: 9527}
	}
	t.Cleanup(func() {
		sessiontxn.GetTxnManager = originalGetTxnManager
	})

	start := time.Unix(1, 0)
	sctx := &maxExecutionTimeTestContext{
		Context: mock.NewContext(),
		processInfo: &sessmgr.ProcessInfo{
			Time:             start,
			MaxExecutionTime: 1500,
		},
	}
	sctx.GetSessionVars().EnableSharedLockUpgrade = true
	sctx.GetSessionVars().MaxExecutionTime = 0

	lockCtx, err := newLockCtx(sctx, 123, 1, true)
	require.NoError(t, err)
	require.True(t, lockCtx.InShareMode)
	require.True(t, lockCtx.AllowSharedLockUpgrade)
	require.Equal(t, uint64(9527), lockCtx.ForUpdateTS)
	// DML's effective budget applies even when the SELECT timeout is disabled.
	require.Equal(t, start.Add(1500*time.Millisecond), lockCtx.MaxExecutionDeadline)
}

func TestMViewDeltaMergeSnapshotSelection(t *testing.T) {
	readSnapshot := &mviewSnapshotProbe{}
	forUpdateSnapshot := &mviewSnapshotProbe{}
	originalGetTxnManager := sessiontxn.GetTxnManager
	sessiontxn.GetTxnManager = func(sessionctx.Context) sessiontxn.TxnManager {
		return lockCtxTxnManager{
			readTS: 101, forUpdateTS: 202,
			readSnapshot: readSnapshot, forUpdateSnapshot: forUpdateSnapshot,
		}
	}
	t.Cleanup(func() { sessiontxn.GetTxnManager = originalGetTxnManager })

	for _, tc := range []struct {
		name     string
		builder  executorBuilder
		ts       uint64
		snapshot kv.Snapshot
	}{
		{name: "read", ts: 101, snapshot: readSnapshot},
		{name: "update", builder: executorBuilder{inUpdateStmt: true}, ts: 202, snapshot: forUpdateSnapshot},
		{name: "fast_refresh", builder: executorBuilder{inMViewDeltaMergeStmt: true}, ts: 202, snapshot: forUpdateSnapshot},
	} {
		b := tc.builder
		b.sctx = mock.NewContext()
		t.Run(tc.name+"/timestamp", func(t *testing.T) {
			ts, err := b.getSnapshotTS()
			require.NoError(t, err)
			require.Equal(t, tc.ts, ts)
		})
		t.Run(tc.name+"/snapshot", func(t *testing.T) {
			snapshot, err := b.getSnapshot()
			require.NoError(t, err)
			require.Same(t, tc.snapshot, snapshot)
		})
	}

	t.Run("explicit_snapshot", func(t *testing.T) {
		b := executorBuilder{
			sctx:                  mock.NewContext(),
			inMViewDeltaMergeStmt: true,
			forDataReaderBuilder:  true,
			dataReaderTS:          303,
		}
		ts, err := b.getSnapshotTS()
		require.NoError(t, err)
		require.Equal(t, uint64(303), ts)
	})
}

func TestMViewCompleteDeltaApplySnapshotSelection(t *testing.T) {
	readSnapshot := &mviewSnapshotProbe{}
	forUpdateSnapshot := &mviewSnapshotProbe{}
	originalGetTxnManager := sessiontxn.GetTxnManager
	sessiontxn.GetTxnManager = func(sessionctx.Context) sessiontxn.TxnManager {
		return lockCtxTxnManager{
			readTS: 101, forUpdateTS: 202,
			readSnapshot: readSnapshot, forUpdateSnapshot: forUpdateSnapshot,
		}
	}
	t.Cleanup(func() { sessiontxn.GetTxnManager = originalGetTxnManager })

	for _, tc := range []struct {
		name     string
		builder  executorBuilder
		ts       uint64
		snapshot kv.Snapshot
	}{
		{name: "read", ts: 101, snapshot: readSnapshot},
		{name: "update", builder: executorBuilder{inUpdateStmt: true}, ts: 202, snapshot: forUpdateSnapshot},
		{name: "complete_delta_apply", builder: executorBuilder{inMViewCompleteDeltaApplyStmt: true}, ts: 202, snapshot: forUpdateSnapshot},
	} {
		b := tc.builder
		b.sctx = mock.NewContext()
		t.Run(tc.name+"/timestamp", func(t *testing.T) {
			ts, err := b.getSnapshotTS()
			require.NoError(t, err)
			require.Equal(t, tc.ts, ts)
		})
		t.Run(tc.name+"/snapshot", func(t *testing.T) {
			snapshot, err := b.getSnapshot()
			require.NoError(t, err)
			require.Same(t, tc.snapshot, snapshot)
		})
	}

	t.Run("explicit_snapshot", func(t *testing.T) {
		b := executorBuilder{
			sctx:                          mock.NewContext(),
			inMViewCompleteDeltaApplyStmt: true,
			forDataReaderBuilder:          true,
			dataReaderTS:                  303,
		}
		ts, err := b.getSnapshotTS()
		require.NoError(t, err)
		require.Equal(t, uint64(303), ts)
	})
}

func TestMViewDataReaderBuilderSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name     string
		snapshot *plannercore.DataReaderSnapshot
		err      string
	}{
		{"nil_snapshot", nil, "snapshot is nil"},
		{"zero_ts", &plannercore.DataReaderSnapshot{}, "snapshot ts is zero"},
		{"nil_infoschema", &plannercore.DataReaderSnapshot{TS: 303}, "snapshot infoschema is nil"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := executorBuilder{}
			reader, err := b.newDataReaderBuilderWithSnapshot(nil, tc.snapshot)
			require.Nil(t, reader)
			require.EqualError(t, err, tc.err)
		})
	}

	originalGetTxnManager := sessiontxn.GetTxnManager
	sessiontxn.GetTxnManager = func(sessionctx.Context) sessiontxn.TxnManager {
		return lockCtxTxnManager{readTS: 101, forUpdateTS: 202}
	}
	t.Cleanup(func() { sessiontxn.GetTxnManager = originalGetTxnManager })

	for _, tc := range []struct {
		name    string
		builder executorBuilder
		ts      uint64
	}{
		{name: "read", ts: 101},
		{name: "fast_refresh", builder: executorBuilder{inMViewDeltaMergeStmt: true}, ts: 202},
		{name: "complete_delta_apply", builder: executorBuilder{inMViewCompleteDeltaApplyStmt: true}, ts: 202},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.builder
			sctx := mock.NewContext()
			b.sctx = sctx
			b.is = infoschema.MockInfoSchema(nil)
			plan := physicalop.PhysicalTableDual{RowCount: 1}.Init(sctx, &property.StatsInfo{RowCount: 1}, 0)
			snapshot := &plannercore.DataReaderSnapshot{TS: 303, InfoSchema: infoschema.MockInfoSchema(nil)}
			for _, explicit := range []bool{false, true} {
				name := "statement_snapshot"
				if explicit {
					name = "explicit_snapshot"
				}
				t.Run(name, func(t *testing.T) {
					var reader *dataReaderBuilder
					var err error
					if explicit {
						reader, err = b.newDataReaderBuilderWithSnapshot(plan, snapshot)
					} else {
						reader, err = b.newDataReaderBuilder(plan)
					}
					require.NoError(t, err)
					require.Same(t, plan, reader.plan)
					require.True(t, reader.forDataReaderBuilder)
					require.Equal(t, b.inMViewDeltaMergeStmt, reader.inMViewDeltaMergeStmt)
					require.Equal(t, b.inMViewCompleteDeltaApplyStmt, reader.inMViewCompleteDeltaApplyStmt)
					require.NotNil(t, reader.stmtCtxLock)
					require.NotNil(t, reader.once)
					ts, err := reader.getSnapshotTS()
					require.NoError(t, err)
					if explicit {
						require.Equal(t, snapshot.TS, ts)
						require.Same(t, snapshot.InfoSchema, reader.is)
						require.True(t, reader.isStaleness)
					} else {
						require.Equal(t, tc.ts, ts)
						require.Same(t, b.is, reader.is)
						require.False(t, reader.isStaleness)
					}
				})
			}
			require.False(t, b.forDataReaderBuilder)
			require.False(t, b.isStaleness)
			require.Zero(t, b.dataReaderTS)
			require.Nil(t, b.stmtCtxLock)
		})
	}
}
