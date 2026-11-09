package core

import (
	"bytes"
	"context"
	"math"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestShowConfigUsesExecutorPrivilegeCheck(t *testing.T) {
	p := parser.New()
	sctx := MockContext()
	builder, _ := NewPlanBuilder().Init(sctx, nil, hint.NewQBHintHandler(nil))

	stmt, err := p.ParseOneStmt("show config", "", "")
	require.NoError(t, err)

	_, err = builder.Build(context.Background(), resolve.NewNodeW(stmt))
	require.NoError(t, err)
	require.Empty(t, builder.visitInfo)
}

func TestGetLogReplOptsMap(t *testing.T) {
	ret, err := getLogReplOptsMap([]*ast.LogReplicationOpt{})
	require.NoError(t, err)
	require.Len(t, ret, 0)

	ret, err = getLogReplOptsMap([]*ast.LogReplicationOpt{
		{
			Tp:    ast.LogReplicationOptSourceHost,
			Value: "host",
		},
		{
			Tp:        ast.LogReplicationOptSourcePort,
			UintValue: 1234,
		},
	})
	require.NoError(t, err)
	require.Len(t, ret, 2)

	ret, err = getLogReplOptsMap([]*ast.LogReplicationOpt{
		{
			Tp:    ast.LogReplicationOptSourceHost,
			Value: "host",
		},
		{
			Tp:    ast.LogReplicationOptSourceHost,
			Value: "host-2",
		},
	})
	require.ErrorContains(t, err, "duplicate option type: SOURCE_HOST")
}

func TestIllegalAlterLogRepl(t *testing.T) {
	b := &PlanBuilder{}
	ctx := context.Background()

	sourceID := uint64(1234)

	cases := []struct {
		name   string
		a      *ast.AlterLogReplication
		errMsg string
	}{
		{
			name: "change-source-with-options",
			a: &ast.AlterLogReplication{
				Name:               model.NewCIStr("repl"),
				NewSourceClusterID: &sourceID,
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumProtection)},
				},
			},
			errMsg: "ALTER LOG REPLICATION CHANGE SOURCE cannot be combined with option list",
		},
		{
			name: "rename-with-options",
			a: &ast.AlterLogReplication{
				Name:    model.NewCIStr("repl"),
				NewName: model.NewCIStr("repl_renamed"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumProtection)},
				},
			},
			errMsg: "ALTER LOG REPLICATION RENAME cannot be combined with other alter options",
		},
		{
			name: "rename-with-change-source",
			a: &ast.AlterLogReplication{
				Name:               model.NewCIStr("repl"),
				NewSourceClusterID: &sourceID,
				NewName:            model.NewCIStr("repl_renamed"),
			},
			errMsg: "ALTER LOG REPLICATION RENAME cannot be combined with other alter options",
		},
		{
			name: "unsupported-option",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptSourceHost, Value: "host"},
				},
			},
			errMsg: "SOURCE_HOST is not supported in ALTER LOG REPLICATION",
		},
		{
			name: "degrade-timeout-without-max-availability",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptDegradeTimeout, Value: "10s"},
				},
			},
			errMsg: "DEGRADE_TIMEOUT is only allowed when PROTECTION_MODE is MAXIMUM_AVAILABILITY",
		},
		{
			name: "max-availability-without-degrade-timeout",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumAvailability)},
				},
			},
			errMsg: "DEGRADE_TIMEOUT is required when PROTECTION_MODE is MAXIMUM_AVAILABILITY",
		},
		{
			name: "invalid-protection-mode",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: 3},
				},
			},
			errMsg: "invalid PROTECTION_MODE value: 3, expected one of 0, 1, 2",
		},
		{
			name: "invalid-degrade-timeout-value",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumAvailability)},
					{Tp: ast.LogReplicationOptDegradeTimeout, Value: "bad-duration"},
				},
			},
			errMsg: "invalid DEGRADE_TIMEOUT value: bad-duration",
		},
		{
			name: "non-positive-degrade-timeout",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumAvailability)},
					{Tp: ast.LogReplicationOptDegradeTimeout, Value: "0s"},
				},
			},
			errMsg: "DEGRADE_TIMEOUT must be greater than 0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := b.buildAdminAlterLogReplication(ctx, tc.a)
			require.Nil(t, p)
			require.ErrorContains(t, err, tc.errMsg)
		})
	}
}

func BenchmarkRowLocatorRangeConstruction(b *testing.B) {
	b.Run("NoLocatorQueryBlock", func(b *testing.B) {
		stmt, err := parser.New().ParseOneStmt("select * from t", "", "")
		if err != nil {
			b.Fatal(err)
		}
		selectStmt := stmt.(*ast.SelectStmt)
		nodeW := resolve.NewNodeW(stmt)
		nodeW.GetResolveContext().FinishTiDBRowIDQueryBlockScan()
		builder := &PlanBuilder{resolveCtx: nodeW.GetResolveContext()}

		b.ReportAllocs()
		for range b.N {
			builder.pushRowLocatorScope(selectStmt, selectStmt.From.TableRefs)
			if builder.currentRowLocatorScope() != nil {
				b.Fatal("query block without TIDB_ROW_ID allocated a locator scope")
			}
			builder.popRowLocatorScope()
		}
	})

	ctx := mock.NewContext()
	for _, partitionCount := range []int{1, 16, 256, 1024} {
		b.Run(strconv.Itoa(partitionCount)+"Partitions", func(b *testing.B) {
			definitions := make([]metamodel.PartitionDefinition, partitionCount)
			for i := range definitions {
				definitions[i] = metamodel.PartitionDefinition{
					ID:   int64(1000 + i),
					Name: model.NewCIStr("p" + strconv.Itoa(i)),
				}
			}
			tableInfo := &metamodel.TableInfo{
				ID:         999,
				PKIsHandle: true,
				Partition: &metamodel.PartitionInfo{
					Enable:      true,
					Definitions: definitions,
				},
			}
			bound, err := tablecodec.EncodeTiDBRowID(definitions[partitionCount/2].ID, kv.IntHandle(0))
			if err != nil {
				b.Fatal(err)
			}
			bounds := []expression.Expression{expression.NewStrConst(bound)}
			ops := []expression.TiDBRowIDRangeOp{expression.TiDBRowIDRangeGT}

			b.ReportAllocs()
			var physicalIDs []int64
			var keyRanges []kv.KeyRange
			for range b.N {
				physicalIDs, keyRanges, err = resolveTiDBRowIDRangeBounds(
					ctx.GetEvalCtx(), tableInfo, nil, ops, bounds,
				)
				if err != nil {
					b.Fatal(err)
				}
			}
			runtime.KeepAlive(physicalIDs)
			runtime.KeepAlive(keyRanges)
		})
	}
}

func TestTiDBRowIDPhysicalTableRangeBoundaries(t *testing.T) {
	const physicalID int64 = 2048
	intTable := &metamodel.TableInfo{ID: physicalID, PKIsHandle: true}
	intStart, intEnd, err := tiDBRowIDPhysicalTableRange(intTable, physicalID)
	require.NoError(t, err)
	require.Equal(t, tablecodec.EncodeRowKeyWithHandle(physicalID, kv.IntHandle(math.MinInt64)), intStart)
	require.Equal(t, tablecodec.EncodeRowKeyWithHandle(physicalID, kv.IntHandle(math.MaxInt64)).Next(), intEnd)

	commonTable := &metamodel.TableInfo{ID: physicalID, IsCommonHandle: true}
	commonStart, commonEnd, err := tiDBRowIDPhysicalTableRange(commonTable, physicalID)
	require.NoError(t, err)
	encodedMin, err := codec.EncodeKey(time.UTC, nil, types.MinNotNullDatum())
	require.NoError(t, err)
	encodedMax, err := codec.EncodeKey(time.UTC, nil, types.MaxValueDatum())
	require.NoError(t, err)
	require.Equal(t, tablecodec.EncodeRowKey(physicalID, encodedMin), commonStart)
	require.Equal(t, tablecodec.EncodeRowKey(physicalID, kv.Key(encodedMax).PrefixNext()), commonEnd)
	recordPrefix := tablecodec.GenTableRecordPrefix(physicalID)
	require.LessOrEqual(t, bytes.Compare(recordPrefix, commonStart), 0)
	require.LessOrEqual(t, bytes.Compare(commonEnd, recordPrefix.PrefixNext()), 0)

	for _, datums := range [][]types.Datum{
		{types.NewStringDatum("")},
		{types.NewBytesDatum([]byte{0x00, 0xff, 0x00})},
		{types.NewBytesDatum([]byte{0xff, 0xff, 0xff, 0xff})},
		{types.NewStringDatum("prefix"), types.NewIntDatum(-1)},
	} {
		encodedHandle, err := codec.EncodeKey(time.UTC, nil, datums...)
		require.NoError(t, err)
		handle, err := kv.NewCommonHandle(encodedHandle)
		require.NoError(t, err)
		rowKey := tablecodec.EncodeRowKeyWithHandle(physicalID, handle)
		require.LessOrEqual(t, bytes.Compare(commonStart, rowKey), 0)
		require.Less(t, bytes.Compare(rowKey, commonEnd), 0)

		token, err := tablecodec.EncodeTiDBRowID(physicalID, handle)
		require.NoError(t, err)
		physicalIDs, ranges, err := resolveTiDBRowIDRangeBounds(
			mock.NewContext().GetEvalCtx(),
			commonTable,
			nil,
			[]expression.TiDBRowIDRangeOp{expression.TiDBRowIDRangeLE},
			[]expression.Expression{expression.NewStrConst(token)},
		)
		require.NoError(t, err)
		require.Equal(t, []int64{physicalID}, physicalIDs)
		require.Equal(t, []kv.KeyRange{{StartKey: commonStart, EndKey: rowKey.Next()}}, ranges)
	}
}

func TestBuildAdminCreateLogReplicationDetached(t *testing.T) {
	b := &PlanBuilder{}
	ctx := context.Background()

	p, err := b.buildAdminCreateLogReplication(ctx, &ast.CreateLogReplication{
		Name: model.NewCIStr("repl"),
		Opts: []*ast.LogReplicationOpt{
			{Tp: ast.LogReplicationOptSourceHost, Value: "127.0.0.1"},
			{Tp: ast.LogReplicationOptDetached},
		},
	})
	require.NoError(t, err)

	createPlan, ok := p.(*CreateLogReplication)
	require.True(t, ok)
	require.True(t, createPlan.Detached)
	require.Equal(t, 1, createPlan.Schema().Len())
	require.Equal(t, "WORKFLOW_ID", createPlan.OutputNames()[0].ColName.O)
}

func TestBuildAdminAlterLogReplicationRename(t *testing.T) {
	b := &PlanBuilder{}
	ctx := context.Background()

	p, err := b.buildAdminAlterLogReplication(ctx, &ast.AlterLogReplication{
		Name:    model.NewCIStr("test"),
		NewName: model.NewCIStr("test_renamed"),
	})
	require.NoError(t, err)

	renamePlan, ok := p.(*AlterLogReplication)
	require.True(t, ok)
	require.Equal(t, model.NewCIStr("test"), renamePlan.Name)
	require.Equal(t, model.NewCIStr("test_renamed"), renamePlan.NewName)
}

func TestBuildAdminCancelLogReplicationWorkflow(t *testing.T) {
	b := &PlanBuilder{}
	ctx := context.Background()

	p, err := b.buildAdminCancelLogReplicationWorkflow(ctx, &ast.CancelLogReplicationWorkflow{
		WorkflowID: 123,
	})
	require.NoError(t, err)

	cancelPlan, ok := p.(*CancelLogReplicationWorkflow)
	require.True(t, ok)
	require.Equal(t, uint64(123), cancelPlan.WorkflowID)
}

func TestIllegalCreateLogReplication(t *testing.T) {
	b := &PlanBuilder{}
	ctx := context.Background()

	p, err := b.buildAdminCreateLogReplication(ctx, &ast.CreateLogReplication{
		Name: model.NewCIStr("repl"),
		Opts: []*ast.LogReplicationOpt{
			{Tp: ast.LogReplicationOptSourceHost, Value: "127.0.0.1"},
			{Tp: ast.LogReplicationOptProtectionMode, UintValue: 3},
		},
	})
	require.Nil(t, p)
	require.ErrorContains(t, err, "invalid PROTECTION_MODE value: 3, expected one of 0, 1, 2")
}

func TestParseDegradeTimeoutOption(t *testing.T) {
	cases := []struct {
		name           string
		mode           ast.ProtectionMode
		optsMap        map[ast.LogReplicationOptType]*ast.LogReplicationOpt
		wantTimeoutSec uint64
		errMsg         string
	}{
		{
			name:    "no-timeout-no-max-availability",
			mode:    ast.ProtectionModeMaximumProtection,
			optsMap: nil,
		},
		{
			name:    "no-timeout-but-max-availability",
			mode:    ast.ProtectionModeMaximumAvailability,
			optsMap: nil,
			errMsg:  "DEGRADE_TIMEOUT is required when PROTECTION_MODE is MAXIMUM_AVAILABILITY",
		},
		{
			name: "timeout-without-max-availability",
			mode: ast.ProtectionModeMaximumProtection,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "10s"},
			},
			errMsg: "DEGRADE_TIMEOUT is only allowed when PROTECTION_MODE is MAXIMUM_AVAILABILITY",
		},
		{
			name: "invalid-duration-missing-unit",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "10"},
			},
			errMsg: "invalid DEGRADE_TIMEOUT value: 10",
		},
		{
			name: "invalid-duration-bad-string",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "bad-duration"},
			},
			errMsg: "invalid DEGRADE_TIMEOUT value: bad-duration",
		},
		{
			name: "non-positive-duration-zero",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "0s"},
			},
			errMsg: "DEGRADE_TIMEOUT must be greater than 0",
		},
		{
			name: "non-positive-duration-negative",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "-1s"},
			},
			errMsg: "DEGRADE_TIMEOUT must be greater than 0",
		},
		{
			name: "positive-duration-less-than-one-second",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "500ms"},
			},
			errMsg: "the minimal value of DEGRADE_TIMEOUT is 1 second, got 500ms",
		},
		{
			name: "valid-duration-mixed-units",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "1m30s"},
			},
			wantTimeoutSec: 90,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var timeoutSec uint64
			err := parseDegradeTimeoutOption(&timeoutSec, tc.mode, tc.optsMap)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.errMsg)
			}
			if tc.wantTimeoutSec > 0 {
				require.Equal(t, tc.wantTimeoutSec, timeoutSec)
			}
		})
	}
}

// TestLogReplicationLearnerCountPlanning parses CREATE/ALTER statements and
// builds plans, checking range validation, omission and explicit mode zero.
func TestLogReplicationLearnerCountPlanning(t *testing.T) {
	cases := []struct {
		sql           string
		count         uint32
		modeSet       bool
		errorContains string
	}{
		{sql: "ADMIN CREATE LOG REPLICATION lr SOURCE_HOST='source'"},
		{sql: "ADMIN CREATE LOG REPLICATION lr SOURCE_HOST='source' PROTECTION_MODE=MAXIMUM_AVAILABILITY DEGRADE_TIMEOUT='30s'"},
		{sql: "ADMIN CREATE LOG REPLICATION lr SOURCE_HOST='source' PROTECTION_MODE=MAXIMUM_PROTECTION"},
		{sql: "ADMIN CREATE LOG REPLICATION lr SOURCE_HOST='source' LEARNER_COUNT=1", count: 1},
		{sql: "ADMIN CREATE LOG REPLICATION lr SOURCE_HOST='source' LEARNER_COUNT=3", count: 3},
		{sql: "ADMIN CREATE LOG REPLICATION lr SOURCE_HOST='source' LEARNER_COUNT=4294967295", count: ^uint32(0)},
		{sql: "ADMIN ALTER LOG REPLICATION lr PROTECTION_MODE=2", modeSet: true},
		{sql: "ADMIN ALTER LOG REPLICATION lr PROTECTION_MODE=0", modeSet: true},
		{sql: "ADMIN ALTER LOG REPLICATION lr PROTECTION_MODE=1 DEGRADE_TIMEOUT='30s'", modeSet: true},
		{sql: "ADMIN ALTER LOG REPLICATION lr LEARNER_COUNT=2", count: 2},
		{sql: "ADMIN ALTER LOG REPLICATION lr PROTECTION_MODE=0 LEARNER_COUNT=3", count: 3, modeSet: true},
		{sql: "ADMIN ALTER LOG REPLICATION lr PROTECTION_MODE=1 DEGRADE_TIMEOUT='30s' LEARNER_COUNT=3", count: 3, modeSet: true},
		{sql: "ADMIN CREATE LOG REPLICATION lr SOURCE_HOST='source' LEARNER_COUNT=0", errorContains: "LEARNER_COUNT must be between"},
		{sql: "ADMIN ALTER LOG REPLICATION lr LEARNER_COUNT=0", errorContains: "LEARNER_COUNT must be between"},
		{sql: "ADMIN ALTER LOG REPLICATION lr LEARNER_COUNT=4294967296", errorContains: "LEARNER_COUNT must be between"},
		{sql: "ADMIN ALTER LOG REPLICATION lr LEARNER_COUNT=1 LEARNER_COUNT=2", errorContains: "duplicate option"},
		{sql: "ADMIN CREATE LOG REPLICATION lr SOURCE_HOST='source' LEARNER_COUNT=1 LEARNER_COUNT=2", errorContains: "duplicate option"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err)
			admin := stmt.(*ast.AdminStmt)
			builder := &PlanBuilder{}
			var count uint32
			if admin.Tp == ast.AdminCreateLogReplication {
				plan, planErr := builder.buildAdminCreateLogReplication(context.Background(), admin.CreateLogReplication)
				err = planErr
				if err == nil {
					count = plan.(*CreateLogReplication).LearnerCount
				}
			} else {
				plan, planErr := builder.buildAdminAlterLogReplication(context.Background(), admin.AlterLogReplication)
				err = planErr
				if err == nil {
					alter := plan.(*AlterLogReplication)
					count = alter.LearnerCount
					require.Equal(t, tc.modeSet, alter.ProtectionModeSet)
				}
			}
			if tc.errorContains != "" {
				require.ErrorContains(t, err, tc.errorContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.count, count)
		})
	}
}
