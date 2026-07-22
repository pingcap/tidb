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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlmock "github.com/pingcap/tidb/pkg/util/topsql/collector/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	metastorage "github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/opt"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type stmtStatsTestContext struct {
	*mock.Context
	stmtStats *stmtstats.StatementStats
}

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

func TestReadBillingDemoGeneralLogUnits(t *testing.T) {
	core, recorded := observer.New(zap.InfoLevel)
	oldLogger := logutil.GeneralLogger
	logutil.GeneralLogger = zap.New(core)
	oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
	t.Cleanup(func() {
		logutil.GeneralLogger = oldLogger
		vardef.ProcessGeneralLog.Store(oldGeneralLog)
	})

	sctx := mock.NewContext()
	sessVars := sctx.GetSessionVars()
	sessVars.ConnectionID = 42
	sessVars.StmtCtx.OriginalSQL = "select 12345, 'secret_literal'"
	expectedNormalizedSQL, expectedDigest := parser.NormalizeDigest(sessVars.StmtCtx.OriginalSQL)
	stats := stmtsummary.ReadBillingDemoStatementStats{
		ModelVersion:  "v4",
		WeightVersion: "test-v4-calibrated",
		BaseUnits: []stmtsummary.ReadBillingDemoBaseUnitSample{
			{
				Site: "tikv", OpClass: "join_hash", OperatorKind: "hashjoin", Unit: "input_rows", Value: 2,
				DMLKind: "insert", InputSource: "child", InputSide: "left", RowWidthSource: "scan", RowWidth: 4,
			},
			{
				Site: "tikv", OpClass: "join_hash", OperatorKind: "hashjoin", Unit: "input_rows", Value: 3,
				DMLKind: "update", InputSource: "runtime", InputSide: "right", RowWidthSource: "schema", RowWidth: 8,
			},
			{Site: "tidb", OpClass: "agg_hash", OperatorKind: "hashagg", Unit: "output_rows", Value: 1},
			{Site: "tidb", OpClass: "agg_hash", OperatorKind: "hashagg", Unit: "input_bytes", Value: 8},
			{Site: "tidb", OpClass: "kv_mutation", OperatorKind: "memdb_mutation", DMLKind: "insert", Unit: "cpu_work", InputSource: "stmt_memdb_mutation_calls", InputSide: "all", Value: 3.5},
			{Site: "tidb", OpClass: "kv_mutation", OperatorKind: "memdb_mutation", DMLKind: "insert", Unit: "encoded_mutation_count", InputSource: "stmt_memdb_mutation_calls", InputSide: "all", Value: 2},
			{Site: "tidb", OpClass: "kv_mutation", OperatorKind: "memdb_mutation", DMLKind: "insert", Unit: "encoded_mutation_bytes", InputSource: "stmt_memdb_mutation_calls", InputSide: "all", Value: 15},
			{Site: "tidb", OpClass: "kv_mutation", OperatorKind: "memdb_mutation", DMLKind: "insert", Unit: "set_count", InputSource: "stmt_memdb_mutation_calls", InputSide: "all", Value: 2},
			{Site: "tidb", OpClass: "kv_mutation", OperatorKind: "memdb_mutation", DMLKind: "insert", Unit: "delete_count", InputSource: "stmt_memdb_mutation_calls", InputSide: "all", Value: 0},
			{Site: "tidb", OpClass: "kv_mutation", OperatorKind: "memdb_mutation", DMLKind: "insert", Unit: "key_bytes", InputSource: "stmt_memdb_mutation_calls", InputSide: "all", Value: 5},
			{Site: "tidb", OpClass: "kv_mutation", OperatorKind: "memdb_mutation", DMLKind: "insert", Unit: "value_bytes", InputSource: "stmt_memdb_mutation_calls", InputSide: "all", Value: 10},
			{Site: "tidb", OpClass: "reader_transport", OperatorKind: "mixed_reader", Unit: "read_request_count", InputSource: "ruv2_metrics", InputSide: "all", Value: 4},
			{Site: "tikv", OpClass: "kv_write", OperatorKind: "txn_write", DMLKind: "update", Unit: "write_request_count", InputSource: "ruv2_metrics", InputSide: "all", Value: 2},
		},
	}

	// Neither individual gate is sufficient.
	sessVars.EnableReadBillingDemo = true
	LogReadBillingDemoGeneralLog(sctx, stats)
	require.Zero(t, recorded.Len())
	vardef.ProcessGeneralLog.Store(true)
	sessVars.EnableReadBillingDemo = false
	LogReadBillingDemoGeneralLog(sctx, stats)
	require.Zero(t, recorded.Len())

	// Internal SQL is excluded whether marked at session or statement scope.
	sessVars.EnableReadBillingDemo = true
	sessVars.InRestrictedSQL = true
	LogReadBillingDemoGeneralLog(sctx, stats)
	require.Zero(t, recorded.Len())
	sessVars.InRestrictedSQL = false
	sessVars.StmtCtx.InRestrictedSQL = true
	LogReadBillingDemoGeneralLog(sctx, stats)
	require.Zero(t, recorded.Len())
	sessVars.StmtCtx.InRestrictedSQL = false

	LogReadBillingDemoGeneralLog(sctx, stats)
	entries := recorded.TakeAll()
	require.Len(t, entries, 1)
	require.Equal(t, readBillingDemoGeneralLogMessage, entries[0].Message)
	fields := entries[0].ContextMap()
	require.Len(t, fields, 6)
	require.Equal(t, uint64(42), fields["conn"])
	require.Equal(t, "v4", fields["model_version"])
	require.Equal(t, "test-v4-calibrated", fields["weight_version"])
	require.Equal(t, expectedNormalizedSQL, fields["normalized_sql"])
	require.Equal(t, expectedDigest.String(), fields["sql_digest"])
	require.NotContains(t, fields["normalized_sql"], "12345")
	require.NotContains(t, fields["normalized_sql"], "secret_literal")
	require.NotContains(t, fields, "sql")
	rawUnits, ok := fields["units"].([]any)
	require.True(t, ok)
	require.Len(t, rawUnits, 13)

	units := make([]map[string]any, 0, len(rawUnits))
	for _, rawUnit := range rawUnits {
		unit, ok := rawUnit.(map[string]any)
		require.True(t, ok)
		require.Len(t, unit, 8)
		for _, field := range []string{"site", "op_class", "operator_kind", "dml_kind", "unit", "input_source", "input_side", "value"} {
			require.Contains(t, unit, field)
		}
		for _, internalField := range []string{"row_width_source", "row_width", "operator_id"} {
			require.NotContains(t, unit, internalField)
		}
		units = append(units, unit)
	}
	require.Equal(t, map[string]any{
		"site": "tidb", "op_class": "agg_hash", "operator_kind": "hashagg", "dml_kind": "", "unit": "input_bytes", "input_source": "", "input_side": "", "value": float64(8),
	}, units[0])
	require.Equal(t, map[string]any{
		"site": "tidb", "op_class": "agg_hash", "operator_kind": "hashagg", "dml_kind": "", "unit": "output_rows", "input_source": "", "input_side": "", "value": float64(1),
	}, units[1])
	expectedMutationValues := map[string]float64{
		"cpu_work": 3.5, "delete_count": 0, "encoded_mutation_bytes": 15, "encoded_mutation_count": 2,
		"key_bytes": 5, "set_count": 2, "value_bytes": 10,
	}
	for i, unitName := range []string{"cpu_work", "delete_count", "encoded_mutation_bytes", "encoded_mutation_count", "key_bytes", "set_count", "value_bytes"} {
		require.Equal(t, map[string]any{
			"site": "tidb", "op_class": "kv_mutation", "operator_kind": "memdb_mutation", "dml_kind": "insert",
			"unit": unitName, "input_source": "stmt_memdb_mutation_calls", "input_side": "all", "value": expectedMutationValues[unitName],
		}, units[i+2])
	}
	require.Equal(t, map[string]any{
		"site": "tidb", "op_class": "reader_transport", "operator_kind": "mixed_reader", "dml_kind": "", "unit": "read_request_count", "input_source": "ruv2_metrics", "input_side": "all", "value": float64(4),
	}, units[9])
	require.Equal(t, map[string]any{
		"site": "tikv", "op_class": "join_hash", "operator_kind": "hashjoin", "dml_kind": "insert", "unit": "input_rows", "input_source": "child", "input_side": "left", "value": float64(2),
	}, units[10])
	require.Equal(t, map[string]any{
		"site": "tikv", "op_class": "join_hash", "operator_kind": "hashjoin", "dml_kind": "update", "unit": "input_rows", "input_source": "runtime", "input_side": "right", "value": float64(3),
	}, units[11])
	require.Equal(t, map[string]any{
		"site": "tikv", "op_class": "kv_write", "operator_kind": "txn_write", "dml_kind": "update", "unit": "write_request_count", "input_source": "ruv2_metrics", "input_side": "all", "value": float64(2),
	}, units[12])

	// A valid completed snapshot with no unit samples stays structured and does
	// not invent one statement status from the snapshot's multi-status model.
	LogReadBillingDemoGeneralLog(sctx, stmtsummary.ReadBillingDemoStatementStats{
		ModelVersion:  "v3",
		WeightVersion: "v2",
	})
	entries = recorded.TakeAll()
	require.Len(t, entries, 1)
	fields = entries[0].ContextMap()
	require.Len(t, fields, 6)
	require.NotContains(t, fields, "status")
	require.NotContains(t, fields, "reason")
	rawUnits, ok = fields["units"].([]any)
	require.True(t, ok)
	require.Empty(t, rawUnits)

	// Explicitly initialized empty identity is safe and does not require a
	// digest object to be present.
	emptyIdentityCtx := mock.NewContext()
	emptyIdentityCtx.GetSessionVars().EnableReadBillingDemo = true
	emptyIdentityCtx.GetSessionVars().StmtCtx.InitSQLDigest("", nil)
	LogReadBillingDemoGeneralLog(emptyIdentityCtx, stmtsummary.ReadBillingDemoStatementStats{
		ModelVersion:  "v3",
		WeightVersion: "v2",
	})
	entries = recorded.TakeAll()
	require.Len(t, entries, 1)
	fields = entries[0].ContextMap()
	require.Equal(t, "", fields["normalized_sql"])
	require.Equal(t, "", fields["sql_digest"])
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

// TestObserveStmtBeginOnTopProfiling verifies SQL and plan registration on profiling begin.
func TestObserveStmtBeginOnTopProfiling(t *testing.T) {
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.EnableTopRU()
	t.Cleanup(func() {
		topsqlstate.DisableTopSQL()
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})

	topCollector := topsqlmock.NewTopSQLCollector()
	topsql.SetupTopProfilingForTest(topCollector)

	sctx := mock.NewContext()
	sc := sctx.GetSessionVars().StmtCtx
	sc.OriginalSQL = "select * from t where a = 1"
	normalizedSQL, sqlDigest := sc.SQLDigest()
	require.NotNil(t, sqlDigest)
	const normalizedPlan = "TableReader(table:t)->Selection(eq(test.t.a, ?))"
	planDigest := parser.NewDigest([]byte("topru-plan-digest"))
	sc.SetPlanDigest(normalizedPlan, planDigest)

	stmt := &ExecStmt{
		Ctx:   sctx,
		GoCtx: context.Background(),
	}
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	require.Equal(t, normalizedSQL, topCollector.GetSQL(sqlDigest.Bytes()))
	require.Equal(t, normalizedPlan, topCollector.GetPlan(planDigest.Bytes()))
}

func TestObserveStmtBeginOnTopProfilingRUV2Wiring(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	t.Run("domain ru version v2 drives top ru sampling", func(t *testing.T) {
		stmt, stats := newExecStmtWithStmtStatsForTest(context.Background(), t)
		testCtx := stmt.Ctx.(*stmtStatsTestContext)
		testCtx.BindDomainAndSchValidator(newMockDomainWithRUVersion(t, rmclient.RUVersionV2), nil)

		vars := stmt.Ctx.GetSessionVars()
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddPlanCnt(3)
		vars.RUV2Metrics = metrics
		expectedRU := metrics.TotalRU(vars.RUV2Weights(), 0, 0)

		_ = stmt.observeStmtBeginForTopProfiling(context.Background())

		key := ruKeyForStmt(t, stmt)
		m := stats.MergeRUInto()
		require.Len(t, m, 1)
		require.Equal(t, uint64(1), m[key].ExecCount)
		require.InDelta(t, expectedRU, m[key].TotalRU, 1e-9)
	})

	t.Run("nil domain falls back to default ru version", func(t *testing.T) {
		stmt, stats := newExecStmtWithStmtStatsForTest(context.Background(), t)

		vars := stmt.Ctx.GetSessionVars()
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddPlanCnt(3)
		vars.RUV2Metrics = metrics

		_ = stmt.observeStmtBeginForTopProfiling(context.Background())

		key := ruKeyForStmt(t, stmt)
		m := stats.MergeRUInto()
		require.Len(t, m, 1)
		require.Equal(t, uint64(1), m[key].ExecCount)
		require.InDelta(t, 0.0, m[key].TotalRU, 1e-9)
	})
}

// TestObserveStmtFinishedOnTopProfiling verifies stale RU exec context is cleared
// before the first tick after re-enable.
// Flow: begin-on -> disable -> finish -> re-enable -> tick-before-new-begin
func TestObserveStmtFinishedOnTopProfiling(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	ru := util.NewRUDetailsWith(0, 0, 0)
	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, ru), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	topsqlstate.EnableTopRU()
	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)

	// Use a non-zero sentinel RU bump (5 is arbitrary) to prove stale execCtx
	// has been cleared; otherwise the next MergeRUInto would leak a positive delta.
	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	require.Len(t, stats.MergeRUInto(), 0)
}

// TestObserveStmtFinishedOnTopProfilingDoes verifies stale baseline is not reused
// across TopRU toggle windows.
// Flow: begin-on -> disable -> finish -> begin-off(same key) -> re-enable -> finish-on
func TestObserveStmtFinishedOnTopProfilingDoes(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	ruA := util.NewRUDetailsWith(0, 0, 0)
	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, ruA), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	ruA.Merge(util.NewRUDetailsWith(10, 0, 0))
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	// TopRU is still disabled here; begin-off must not create/reuse an RU execCtx.
	ruB := util.NewRUDetailsWith(20, 0, 0)
	stmt.GoCtx = context.WithValue(context.Background(), util.RUDetailsCtxKey, ruB)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	topsqlstate.EnableTopRU()
	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	// Expect only the begin-based execution count and no RU delta from stale baseline.
	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)
}

// TestObserveStmtFinishedOnTopProfilingKeeps verifies TopSQL-only finish stats are
// preserved when TopRU is disabled.
// Flow: begin(topSQL-on/topRU-off) -> finish -> verify duration/network-out stats
func TestObserveStmtFinishedOnTopProfilingKeeps(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopSQL()

	stmt, stats := newExecStmtWithStmtStatsForTest(context.Background(), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	vars := stmt.Ctx.GetSessionVars()
	vars.OutPacketBytes.Store(123)
	vars.StartTime = time.Now().Add(-time.Second)

	stmt.observeStmtFinishedForTopProfiling()

	data := stats.Take()
	require.Len(t, data, 1)
	for _, item := range data {
		require.Equal(t, uint64(1), item.ExecCount)
		require.Equal(t, uint64(1), item.DurationCount)
		require.Greater(t, item.SumDurationNs, uint64(0))
		require.Equal(t, uint64(123), item.NetworkOutBytes)
	}
}

// TestObserveStmtFinishedOnTopProfilingIgnores verifies unexpected RUDetails
// context types do not panic and keep TopRU sampling stable.
// Flow: begin(topRU-on, bad RUDetails type) -> finish -> no panic -> zero RU delta
func TestObserveStmtFinishedOnTopProfilingIgnores(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, "bad-type"), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	require.NotPanics(t, func() {
		stmt.observeStmtFinishedForTopProfiling()
	})

	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)
}

type mockResourceGroupProvider struct {
	config *rmclient.Config
}

func newMockDomainWithRUVersion(t *testing.T, version rmclient.RUVersion) *domain.Domain {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := rmclient.DefaultConfig()
	cfg.RUVersionPolicy = &rmclient.RUVersionPolicy{Default: version}
	provider := &mockResourceGroupProvider{config: cfg}
	controller, err := rmclient.NewResourceGroupController(ctx, 1, provider, nil, 1)
	require.NoError(t, err)

	do := domain.NewMockDomain()
	do.SetResourceGroupsController(controller)
	return do
}

func (m *mockResourceGroupProvider) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	value, err := json.Marshal(m.config)
	if err != nil {
		return nil, err
	}
	return &meta_storagepb.GetResponse{
		Kvs: []*meta_storagepb.KeyValue{{Value: value}},
	}, nil
}

func (*mockResourceGroupProvider) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	ch := make(chan []*meta_storagepb.Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (*mockResourceGroupProvider) Put(context.Context, []byte, []byte, ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return &meta_storagepb.PutResponse{}, nil
}

func (*mockResourceGroupProvider) GetResourceGroup(context.Context, string, ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (*mockResourceGroupProvider) ListResourceGroups(context.Context, ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (*mockResourceGroupProvider) AddResourceGroup(context.Context, *rmpb.ResourceGroup) (string, error) {
	return "", nil
}

func (*mockResourceGroupProvider) ModifyResourceGroup(context.Context, *rmpb.ResourceGroup) (string, error) {
	return "", nil
}

func (*mockResourceGroupProvider) DeleteResourceGroup(context.Context, string) (string, error) {
	return "", nil
}

func (*mockResourceGroupProvider) AcquireTokenBuckets(context.Context, *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	return nil, nil
}

func (*mockResourceGroupProvider) LoadResourceGroups(context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	return nil, 0, nil
}

var (
	_ metastorage.Client             = (*mockResourceGroupProvider)(nil)
	_ rmclient.ResourceGroupProvider = (*mockResourceGroupProvider)(nil)
)
