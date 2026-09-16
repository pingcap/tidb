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
	"context"
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
<<<<<<< HEAD
=======
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlmock "github.com/pingcap/tidb/pkg/util/topsql/collector/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
>>>>>>> b82bed1eca2 (executor, session: add tidb_dml_max_execution_time for transactional DML (#70568))
	"github.com/stretchr/testify/require"
)

<<<<<<< HEAD
=======
type stmtStatsTestContext struct {
	*mock.Context
	stmtStats *stmtstats.StatementStats
}

type maxExecutionTimeTestContext struct {
	*mock.Context
	processInfo *sessmgr.ProcessInfo
}

func (c *maxExecutionTimeTestContext) ShowProcess() *sessmgr.ProcessInfo {
	return c.processInfo
}

>>>>>>> b82bed1eca2 (executor, session: add tidb_dml_max_execution_time for transactional DML (#70568))
type sharedLockMemBufferForTest struct {
	kv.MemBuffer
	getLocal func(key []byte) ([]byte, error)
	rLocks   int
	rUnlocks int
}

func (m *sharedLockMemBufferForTest) GetLocal(_ context.Context, key []byte) ([]byte, error) {
	return m.getLocal(key)
}

func (m *sharedLockMemBufferForTest) RLock() {
	m.rLocks++
}

func (m *sharedLockMemBufferForTest) RUnlock() {
	m.rUnlocks++
}

type sharedLockTxnForTest struct {
	kv.Transaction
	memBuffer kv.MemBuffer
}

func (t *sharedLockTxnForTest) GetMemBuffer() kv.MemBuffer {
	return t.memBuffer
}

<<<<<<< HEAD
=======
func (c *stmtStatsTestContext) GetStmtStats() *stmtstats.StatementStats {
	return c.stmtStats
}

func resetTopProfilingStateForTest(t *testing.T) {
	t.Helper()
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	t.Cleanup(func() {
		topsqlstate.DisableTopSQL()
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})
}

func newExecStmtWithStmtStatsForTest(goCtx context.Context, t *testing.T) (*ExecStmt, *stmtstats.StatementStats) {
	t.Helper()

	stats := stmtstats.CreateStatementStats()
	t.Cleanup(stats.SetFinished)

	sctx := mock.NewContext()
	sctx.GetSessionVars().User = &auth.UserIdentity{Username: "u1", Hostname: "%"}
	sc := sctx.GetSessionVars().StmtCtx
	sc.OriginalSQL = "select * from t where a = 1"
	_, sqlDigest := sc.SQLDigest()
	require.NotNil(t, sqlDigest)
	const normalizedPlan = "TableReader(table:t)->Selection(eq(test.t.a, ?))"
	planDigest := parser.NewDigest([]byte("topru-plan-digest"))
	sc.SetPlanDigest(normalizedPlan, planDigest)

	return &ExecStmt{
		Ctx: &stmtStatsTestContext{
			Context:   sctx,
			stmtStats: stats,
		},
		GoCtx: goCtx,
	}, stats
}

func newFinishedRecordSetForTest() *recordSet {
	ft := types.NewFieldType(mysql.TypeLonglong)
	return &recordSet{
		schema: expression.NewSchema(&expression.Column{RetType: ft}),
		stmt:   &ExecStmt{Ctx: mock.NewContext()},
	}
}

func TestRecordSetNewChunkAfterFinish(t *testing.T) {
	rs := newFinishedRecordSetForTest()

	req := rs.NewChunk(nil)
	require.NotNil(t, req)
	require.Equal(t, 1, req.NumCols())

	req = rs.NewChunk(chunk.NewAllocator())
	require.NotNil(t, req)
	require.Equal(t, 1, req.NumCols())
}

func TestRecordSetNextAfterFinish(t *testing.T) {
	rs := newFinishedRecordSetForTest()

	err := rs.Next(context.Background(), chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1))
	require.Error(t, err)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(err), err)
}

func TestCheckMaxExecutionTimeExceededPreservesPendingKillReason(t *testing.T) {
	sctx := &maxExecutionTimeTestContext{
		Context: mock.NewContext(),
		processInfo: &sessmgr.ProcessInfo{
			Time:             time.Now().Add(-time.Hour),
			MaxExecutionTime: 1,
		},
	}
	sctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)

	err := checkMaxExecutionTimeExceeded(sctx)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(err), err)
}

func ruKeyForStmt(t *testing.T, stmt *ExecStmt) stmtstats.RUKey {
	t.Helper()

	sqlDigest, planDigest := stmt.getSQLPlanDigest()
	require.NotNil(t, sqlDigest)
	require.NotNil(t, planDigest)
	return stmtstats.RUKey{
		User:       stmt.Ctx.GetSessionVars().User.String(),
		SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
		PlanDigest: stmtstats.BinaryDigest(planDigest),
	}
}

>>>>>>> b82bed1eca2 (executor, session: add tidb_dml_max_execution_time for transactional DML (#70568))
func TestMoveWrittenSharedLockKeysToExclusive(t *testing.T) {
	injectedErr := errors.New("injected get local error")

	tests := []struct {
		name              string
		exclusiveKeys     []kv.Key
		sharedKeys        []kv.Key
		writtenKeys       map[string]struct{}
		getLocalErrors    map[string]error
		wantExclusiveKeys []kv.Key
		wantSharedKeys    []kv.Key
		wantErr           error
	}{
		{
			name:              "no shared keys",
			exclusiveKeys:     []kv.Key{kv.Key("exclusive")},
			wantExclusiveKeys: []kv.Key{kv.Key("exclusive")},
		},
		{
			name:          "deduplicate exclusive and promote written keys",
			exclusiveKeys: []kv.Key{kv.Key("exclusive")},
			sharedKeys: []kv.Key{
				kv.Key("exclusive"),
				kv.Key("written"),
				kv.Key("shared"),
			},
			writtenKeys: map[string]struct{}{
				"written": {},
			},
			wantExclusiveKeys: []kv.Key{kv.Key("exclusive"), kv.Key("written")},
			wantSharedKeys:    []kv.Key{kv.Key("shared")},
		},
		{
			name: "propagate get local error",
			sharedKeys: []kv.Key{
				kv.Key("bad"),
			},
			getLocalErrors: map[string]error{
				"bad": injectedErr,
			},
			wantErr: injectedErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memBuffer := &sharedLockMemBufferForTest{
				getLocal: func(key []byte) ([]byte, error) {
					if err, ok := tt.getLocalErrors[string(key)]; ok {
						return nil, err
					}
					if _, ok := tt.writtenKeys[string(key)]; ok {
						return []byte("value"), nil
					}
					return nil, kv.ErrNotExist
				},
			}
			txn := &sharedLockTxnForTest{memBuffer: memBuffer}

			exclusiveKeys, sharedKeys, err := moveWrittenSharedLockKeysToExclusive(
				context.Background(),
				txn,
				tt.exclusiveKeys,
				tt.sharedKeys,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, exclusiveKeys)
				require.Nil(t, sharedKeys)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantExclusiveKeys, exclusiveKeys)
				require.Equal(t, tt.wantSharedKeys, sharedKeys)
			}
			if len(tt.sharedKeys) > 0 {
				require.Equal(t, 1, memBuffer.rLocks)
				require.Equal(t, 1, memBuffer.rUnlocks)
			} else {
				require.Zero(t, memBuffer.rLocks)
				require.Zero(t, memBuffer.rUnlocks)
			}
		})
	}
}
