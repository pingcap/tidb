// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executil "github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/stretchr/testify/require"
)

// Note: it's a tricky way to export the `inspectionSummaryRules` and `inspectionRules` for unit test but invisible for normal code
var (
	InspectionSummaryRules = inspectionSummaryRules
	InspectionRules        = inspectionRules
)

func TestBuildKvRangesForIndexJoinWithoutCwc(t *testing.T) {
	indexRanges := make([]*ranger.Range, 0, 6)
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 2))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 3, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 2, 1, 1))

	joinKeyRows := make([]*join.IndexJoinLookUpContent, 0, 5)
	joinKeyRows = append(joinKeyRows, &join.IndexJoinLookUpContent{Keys: generateDatumSlice(1, 1)})
	joinKeyRows = append(joinKeyRows, &join.IndexJoinLookUpContent{Keys: generateDatumSlice(1, 2)})
	joinKeyRows = append(joinKeyRows, &join.IndexJoinLookUpContent{Keys: generateDatumSlice(2, 1)})
	joinKeyRows = append(joinKeyRows, &join.IndexJoinLookUpContent{Keys: generateDatumSlice(2, 2)})
	joinKeyRows = append(joinKeyRows, &join.IndexJoinLookUpContent{Keys: generateDatumSlice(2, 3)})

	keyOff2IdxOff := []int{1, 3}
	ctx := mock.NewContext()
	kvRanges, err := buildKvRangesForIndexJoin(ctx.GetDistSQLCtx(), ctx.GetRangerCtx(), 0, 0, joinKeyRows, indexRanges, keyOff2IdxOff, nil, nil, nil)
	require.NoError(t, err)
	// Check the kvRanges is in order.
	for i, kvRange := range kvRanges {
		require.True(t, kvRange.StartKey.Cmp(kvRange.EndKey) < 0)
		if i > 0 {
			require.True(t, kvRange.StartKey.Cmp(kvRanges[i-1].EndKey) >= 0)
		}
	}
}

func TestShouldUseImportIntoForMVRefreshOutOfPlace(t *testing.T) {
	require.True(t, shouldUseImportIntoForMVRefreshOutOfPlace("TiKV"))
	require.False(t, shouldUseImportIntoForMVRefreshOutOfPlace("tikv"))
	require.False(t, shouldUseImportIntoForMVRefreshOutOfPlace(kv.TiDB.Name()))
	require.False(t, shouldUseImportIntoForMVRefreshOutOfPlace("mock-storage"))
}

func TestHistTimeUsesLocation(t *testing.T) {
	input := time.Date(2026, 6, 14, 12, 0, 0, 123456789, time.UTC)
	loc := time.FixedZone("UTC+8", 8*60*60)

	require.Equal(t, "2026-06-14 20:00:00.123456 +0800", histTime(input, loc).Format("2006-01-02 15:04:05.000000 -0700"))
	require.Equal(t, "2026-06-14 12:00:00.123456 +0000", histTime(input, time.UTC).Format("2006-01-02 15:04:05.000000 -0700"))
}

func newTestMViewCompleteDeltaTargetTable(tp *types.FieldType) *tables.TableCommon {
	return tables.MockTableFromMeta(&model.TableInfo{
		ID:    1,
		Name:  pmodel.NewCIStr("mv"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      pmodel.NewCIStr("a"),
				Offset:    0,
				FieldType: *tp,
				State:     model.StatePublic,
			},
		},
	}).(*tables.TableCommon)
}

func TestValidateMViewCompleteDeltaWritableInputColTypesAllowsNullableInput(t *testing.T) {
	targetTp := types.NewFieldType(mysql.TypeLonglong)
	targetTp.AddFlag(mysql.NotNullFlag)
	targetTbl := newTestMViewCompleteDeltaTargetTable(targetTp)

	inputTp := targetTp.Clone()
	inputTp.DelFlag(mysql.NotNullFlag)
	require.NoError(t, validateMViewCompleteDeltaWritableInputColTypes(targetTbl, []*types.FieldType{inputTp}, []int{0}))

	unsignedInputTp := inputTp.Clone()
	unsignedInputTp.AddFlag(mysql.UnsignedFlag)
	err := validateMViewCompleteDeltaWritableInputColTypes(targetTbl, []*types.FieldType{unsignedInputTp}, []int{0})
	require.ErrorContains(t, err, "type mismatch")
}

func TestBuildMViewCompleteDeltaWritableInputColIDsColumnOffsets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		offsets [2]int
		err     string
	}{
		{name: "dense", offsets: [2]int{0, 1}},
		{name: "reordered", offsets: [2]int{1, 0}, err: "column `a` offset 1 != writable ordinal 0"},
		{name: "gapped", offsets: [2]int{0, 2}, err: "column `b` offset 2 != writable ordinal 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tp := types.NewFieldType(mysql.TypeLonglong)
			target := tables.MockTableFromMeta(&model.TableInfo{
				ID:    1,
				Name:  pmodel.NewCIStr("mv"),
				State: model.StatePublic,
				Columns: []*model.ColumnInfo{
					{ID: 1, Name: pmodel.NewCIStr("a"), Offset: tc.offsets[0], FieldType: *tp, State: model.StatePublic},
					{ID: 2, Name: pmodel.NewCIStr("b"), Offset: tc.offsets[1], FieldType: *tp, State: model.StatePublic},
				},
			})
			require.Len(t, target.Cols(), 2)
			require.Len(t, target.WritableCols(), 2)
			mapping, err := buildMViewCompleteDeltaWritableInputColIDs(target, []int{4, 2})
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				require.Nil(t, mapping)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []int{4, 2}, mapping)
		})
	}
}

type mviewCompleteDeltaApplyOpenProbeChild struct {
	exec.BaseExecutor
	opened bool
}

func (e *mviewCompleteDeltaApplyOpenProbeChild) Open(context.Context) error {
	e.opened = true
	return nil
}

func TestMViewCompleteDeltaApplyOpenValidatesMappingsBeforeOpeningChild(t *testing.T) {
	sctx := mock.NewContext()
	targetTp := types.NewFieldType(mysql.TypeLonglong)
	targetTbl := newTestMViewCompleteDeltaTargetTable(targetTp)
	childSchema := expression.NewSchema(&expression.Column{
		Index:   0,
		RetType: targetTp,
	})
	child := &mviewCompleteDeltaApplyOpenProbeChild{
		BaseExecutor: exec.NewBaseExecutor(sctx, childSchema, 0),
	}
	applyExec := &MViewCompleteDeltaApplyExec{
		BaseExecutor:                  exec.NewBaseExecutor(sctx, nil, 0, child),
		TargetTable:                   targetTbl,
		TargetHandleCols:              plannerutil.NewIntHandleCols(&expression.Column{Index: 0, RetType: targetTp}),
		CurrentWritableInputColIDs:    []int{1},
		RecomputedWritableInputColIDs: []int{0},
	}

	err := applyExec.Open(context.Background())
	require.ErrorContains(t, err, "writable input col id 1")
	require.False(t, child.opened)
}

func TestBuildMVRefreshOutOfPlaceBuildSQLImportOptions(t *testing.T) {
	tblInfo := &model.TableInfo{
		Name: pmodel.NewCIStr("mv"),
		MaterializedView: &model.MaterializedViewInfo{
			SQLContent: "select a, count(1) from t group by a",
		},
	}

	sql, err := buildMVRefreshOutOfPlaceBuildSQL("test", "__mv_shadow_1", tblInfo, "TiKV", 12, "64gib")
	require.NoError(t, err)
	require.Contains(t, sql, "IMPORT INTO `test`.`__mv_shadow_1` FROM (")
	require.Contains(t, sql, "WITH "+strings.Join(ddl.BuildMViewImportIntoOptions(12, "64gib"), ", "))

	sql, err = buildMVRefreshOutOfPlaceBuildSQL("test", "__mv_shadow_1", tblInfo, kv.TiDB.Name(), 12, "64gib")
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO `test`.`__mv_shadow_1` select a, count(1) from t group by a", sql)
}

func TestMarkMViewCompleteDeltaTouchedRowsByColumnStringUsesBinaryCompare(t *testing.T) {
	testCases := []struct {
		name      string
		collation string
		oldVal    string
		newVal    string
		touched   bool
	}{
		{
			name:      "case-insensitive collation still compares bytes",
			collation: "utf8mb4_general_ci",
			oldVal:    "a",
			newVal:    "A",
			touched:   true,
		},
		{
			name:      "binary collation keeps byte-sensitive change",
			collation: "utf8mb4_bin",
			oldVal:    "a",
			newVal:    "A",
			touched:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ft := types.NewFieldTypeWithCollation(mysql.TypeVarString, tc.collation, types.UnspecifiedLength)
			input := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 1)
			input.AppendString(0, tc.oldVal)
			input.AppendString(1, tc.newVal)

			updateTouchedBitmap := make([]uint8, 1)
			err := executil.MarkTouchedRowsByColumn(
				[]int{0},
				updateTouchedBitmap,
				1,
				0,
				input.Column(0),
				input.Column(1),
				ft,
				true,
				"test",
			)
			require.NoError(t, err)
			require.Equal(t, tc.touched, updateTouchedBitmap[0] != 0)
		})
	}
}

func TestMarkMViewCompleteDeltaTouchedRowsByColumnEnumSetUseBinaryNameCompare(t *testing.T) {
	enumFT := types.NewFieldType(mysql.TypeEnum)
	enumFT.SetCharset("utf8mb4")
	enumFT.SetCollate("utf8mb4_general_ci")
	enumChk := chunk.NewChunkWithCapacity([]*types.FieldType{enumFT, enumFT}, 1)
	enumChk.AppendEnum(0, types.Enum{Name: "x", Value: 1})
	enumChk.AppendEnum(1, types.Enum{Name: "x", Value: 2})

	enumBitmap := make([]uint8, 1)
	err := executil.MarkTouchedRowsByColumn(
		[]int{0},
		enumBitmap,
		1,
		0,
		enumChk.Column(0),
		enumChk.Column(1),
		enumFT,
		true,
		"test",
	)
	require.NoError(t, err)
	require.Equal(t, []uint8{0}, enumBitmap)

	setFT := types.NewFieldType(mysql.TypeSet)
	setFT.SetCharset("utf8mb4")
	setFT.SetCollate("utf8mb4_general_ci")
	setChk := chunk.NewChunkWithCapacity([]*types.FieldType{setFT, setFT}, 1)
	setChk.AppendSet(0, types.Set{Name: "a,b", Value: 3})
	setChk.AppendSet(1, types.Set{Name: "a,b", Value: 7})

	setBitmap := make([]uint8, 1)
	err = executil.MarkTouchedRowsByColumn(
		[]int{0},
		setBitmap,
		1,
		0,
		setChk.Column(0),
		setChk.Column(1),
		setFT,
		true,
		"test",
	)
	require.NoError(t, err)
	require.Equal(t, []uint8{0}, setBitmap)
}

func TestMViewCompleteDeltaApplyRuntimeStatsString(t *testing.T) {
	stats := &mviewCompleteDeltaApplyRuntimeStats{
		writerTime: 12 * time.Millisecond,
		writerDetail: mviewCompleteDeltaApplyWriterStats{
			chunks:     2,
			rowOps:     7,
			insertRows: 1,
			updateRows: 3,
			deleteRows: 2,
		},
	}
	s := stats.String()
	require.Contains(t, s, "mview_complete_delta_apply")
	require.Contains(t, s, "writer:{time:12ms")
	require.Contains(t, s, "chunks:2")
	require.Contains(t, s, "row_ops:7")
	require.Contains(t, s, "rows:{insert:1, update:3, delete:2}")
}

func TestMViewCompleteDeltaApplyRuntimeStatsMergeAndClone(t *testing.T) {
	left := &mviewCompleteDeltaApplyRuntimeStats{
		writerTime: 5 * time.Millisecond,
		writerDetail: mviewCompleteDeltaApplyWriterStats{
			chunks:     1,
			rowOps:     4,
			insertRows: 1,
			updateRows: 2,
			deleteRows: 1,
		},
	}
	right := &mviewCompleteDeltaApplyRuntimeStats{
		writerTime: 7 * time.Millisecond,
		writerDetail: mviewCompleteDeltaApplyWriterStats{
			chunks:     2,
			rowOps:     5,
			updateRows: 1,
			deleteRows: 1,
		},
	}

	left.Merge(right)
	require.Equal(t, 12*time.Millisecond, left.writerTime)
	require.Equal(t, int64(3), left.writerDetail.chunks)
	require.Equal(t, int64(9), left.writerDetail.rowOps)
	require.Equal(t, int64(1), left.writerDetail.insertRows)
	require.Equal(t, int64(3), left.writerDetail.updateRows)
	require.Equal(t, int64(2), left.writerDetail.deleteRows)

	cloned, ok := left.Clone().(*mviewCompleteDeltaApplyRuntimeStats)
	require.True(t, ok)
	require.Equal(t, left.writerTime, cloned.writerTime)
	require.Equal(t, left.writerDetail, cloned.writerDetail)
}

func TestBuildKvRangesForIndexJoinWithoutCwcAndWithMemoryTracker(t *testing.T) {
	indexRanges := make([]*ranger.Range, 0, 6)
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 2))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 3, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 2, 1, 1))

	bytesConsumed1 := int64(0)
	{
		joinKeyRows := make([]*join.IndexJoinLookUpContent, 0, 10)
		for i := int64(0); i < 10; i++ {
			joinKeyRows = append(joinKeyRows, &join.IndexJoinLookUpContent{Keys: generateDatumSlice(1, i)})
		}

		keyOff2IdxOff := []int{1, 3}
		ctx := mock.NewContext()
		memTracker := memory.NewTracker(memory.LabelForIndexWorker, -1)
		kvRanges, err := buildKvRangesForIndexJoin(ctx.GetDistSQLCtx(), ctx.GetRangerCtx(), 0, 0, joinKeyRows, indexRanges, keyOff2IdxOff, nil, memTracker, nil)
		require.NoError(t, err)
		// Check the kvRanges is in order.
		for i, kvRange := range kvRanges {
			require.True(t, kvRange.StartKey.Cmp(kvRange.EndKey) < 0)
			if i > 0 {
				require.True(t, kvRange.StartKey.Cmp(kvRanges[i-1].EndKey) >= 0)
			}
		}
		bytesConsumed1 = memTracker.BytesConsumed()
	}

	bytesConsumed2 := int64(0)
	{
		joinKeyRows := make([]*join.IndexJoinLookUpContent, 0, 20)
		for i := int64(0); i < 20; i++ {
			joinKeyRows = append(joinKeyRows, &join.IndexJoinLookUpContent{Keys: generateDatumSlice(1, i)})
		}

		keyOff2IdxOff := []int{1, 3}
		ctx := mock.NewContext()
		memTracker := memory.NewTracker(memory.LabelForIndexWorker, -1)
		kvRanges, err := buildKvRangesForIndexJoin(ctx.GetDistSQLCtx(), ctx.GetRangerCtx(), 0, 0, joinKeyRows, indexRanges, keyOff2IdxOff, nil, memTracker, nil)
		require.NoError(t, err)
		// Check the kvRanges is in order.
		for i, kvRange := range kvRanges {
			require.True(t, kvRange.StartKey.Cmp(kvRange.EndKey) < 0)
			if i > 0 {
				require.True(t, kvRange.StartKey.Cmp(kvRanges[i-1].EndKey) >= 0)
			}
		}
		bytesConsumed2 = memTracker.BytesConsumed()
	}

	require.Equal(t, 2*bytesConsumed1, bytesConsumed2)
	require.Equal(t, int64(25560), bytesConsumed1)
}

func generateIndexRange(vals ...int64) *ranger.Range {
	lowDatums := generateDatumSlice(vals...)
	highDatums := make([]types.Datum, len(vals))
	copy(highDatums, lowDatums)
	return &ranger.Range{LowVal: lowDatums, HighVal: highDatums, Collators: collate.GetBinaryCollatorSlice(len(lowDatums))}
}

func generateDatumSlice(vals ...int64) []types.Datum {
	datums := make([]types.Datum, len(vals))
	for i, val := range vals {
		datums[i].SetInt64(val)
	}
	return datums
}

func TestSlowQueryRuntimeStats(t *testing.T) {
	stats := &slowQueryRuntimeStats{
		totalFileNum: 2,
		readFileNum:  2,
		readFile:     time.Second,
		initialize:   time.Millisecond,
		readFileSize: 1024 * 1024 * 1024,
		parseLog:     int64(time.Millisecond * 100),
		concurrent:   15,
	}
	require.Equal(t, "initialize: 1ms, read_file: 1s, parse_log: {time:100ms, concurrency:15}, total_file: 2, read_file: 2, read_size: 1024 MB", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	stats.Merge(stats.Clone())
	require.Equal(t, "initialize: 2ms, read_file: 2s, parse_log: {time:200ms, concurrency:15}, total_file: 4, read_file: 4, read_size: 2 GB", stats.String())
}

func TestFilterTemporaryTableKeys(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	const tableID int64 = 3
	vars.TxnCtx = &variable.TransactionContext{
		TxnCtxNoNeedToRestore: variable.TxnCtxNoNeedToRestore{
			TemporaryTables: map[int64]tableutil.TempTable{tableID: nil},
		},
	}

	res := filterTemporaryTableKeys(vars, []kv.Key{tablecodec.EncodeTablePrefix(tableID), tablecodec.EncodeTablePrefix(42)})
	require.Len(t, res, 1)
}

func TestErrLevelsForResetStmtContext(t *testing.T) {
	ctx := mock.NewContext()
	ctx.BindDomain(&domain.Domain{})

	cases := []struct {
		name    string
		sqlMode mysql.SQLMode
		stmt    []ast.StmtNode
		levels  errctx.LevelMap
	}{
		{
			name:    "strict,write",
			sqlMode: mysql.ModeStrictAllTables | mysql.ModeErrorForDivisionByZero,
			stmt:    []ast.StmtNode{&ast.InsertStmt{}, &ast.UpdateStmt{}, &ast.DeleteStmt{}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelError
				l[errctx.ErrGroupDupKey] = errctx.LevelError
				l[errctx.ErrGroupBadNull] = errctx.LevelError
				l[errctx.ErrGroupNoDefault] = errctx.LevelError
				l[errctx.ErrGroupDividedByZero] = errctx.LevelError
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelError
				return
			}(),
		},
		{
			name:    "non-strict,write",
			sqlMode: mysql.ModeErrorForDivisionByZero,
			stmt:    []ast.StmtNode{&ast.InsertStmt{}, &ast.UpdateStmt{}, &ast.DeleteStmt{}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelWarn
				l[errctx.ErrGroupDupKey] = errctx.LevelError
				l[errctx.ErrGroupBadNull] = errctx.LevelWarn
				l[errctx.ErrGroupNoDefault] = errctx.LevelWarn
				l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelError
				return
			}(),
		},
		{
			name:    "strict,insert ignore",
			sqlMode: mysql.ModeStrictAllTables | mysql.ModeErrorForDivisionByZero,
			stmt:    []ast.StmtNode{&ast.InsertStmt{IgnoreErr: true}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelWarn
				l[errctx.ErrGroupDupKey] = errctx.LevelWarn
				l[errctx.ErrGroupBadNull] = errctx.LevelWarn
				l[errctx.ErrGroupNoDefault] = errctx.LevelWarn
				l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelWarn
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
				return
			}(),
		},
		{
			name:    "strict,update ignore",
			sqlMode: mysql.ModeStrictAllTables | mysql.ModeErrorForDivisionByZero,
			stmt:    []ast.StmtNode{&ast.UpdateStmt{IgnoreErr: true}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelWarn
				l[errctx.ErrGroupDupKey] = errctx.LevelWarn
				l[errctx.ErrGroupBadNull] = errctx.LevelWarn
				l[errctx.ErrGroupNoDefault] = errctx.LevelWarn
				l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
				return
			}(),
		},
		{
			name:    "strict,delete ignore",
			sqlMode: mysql.ModeStrictAllTables | mysql.ModeErrorForDivisionByZero,
			stmt:    []ast.StmtNode{&ast.DeleteStmt{IgnoreErr: true}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelWarn
				l[errctx.ErrGroupDupKey] = errctx.LevelWarn
				l[errctx.ErrGroupBadNull] = errctx.LevelWarn
				l[errctx.ErrGroupNoDefault] = errctx.LevelWarn
				l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelError
				return
			}(),
		},
		{
			name:    "strict without error_for_division_by_zero,write",
			sqlMode: mysql.ModeStrictAllTables,
			stmt:    []ast.StmtNode{&ast.InsertStmt{}, &ast.UpdateStmt{}, &ast.DeleteStmt{}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelError
				l[errctx.ErrGroupDupKey] = errctx.LevelError
				l[errctx.ErrGroupBadNull] = errctx.LevelError
				l[errctx.ErrGroupNoDefault] = errctx.LevelError
				l[errctx.ErrGroupDividedByZero] = errctx.LevelIgnore
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelError
				return
			}(),
		},
		{
			name:    "strict,select/union",
			sqlMode: mysql.ModeStrictAllTables | mysql.ModeErrorForDivisionByZero,
			stmt:    []ast.StmtNode{&ast.SelectStmt{}, &ast.SetOprStmt{}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelWarn
				l[errctx.ErrGroupDupKey] = errctx.LevelError
				l[errctx.ErrGroupBadNull] = errctx.LevelError
				l[errctx.ErrGroupNoDefault] = errctx.LevelError
				l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelError
				return
			}(),
		},
		{
			name:    "non-strict,select/union",
			sqlMode: mysql.ModeStrictAllTables | mysql.ModeErrorForDivisionByZero,
			stmt:    []ast.StmtNode{&ast.SelectStmt{}, &ast.SetOprStmt{}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelWarn
				l[errctx.ErrGroupDupKey] = errctx.LevelError
				l[errctx.ErrGroupBadNull] = errctx.LevelError
				l[errctx.ErrGroupNoDefault] = errctx.LevelError
				l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelError
				return
			}(),
		},
		{
			name:    "strict,load_data",
			sqlMode: mysql.ModeStrictAllTables | mysql.ModeErrorForDivisionByZero,
			stmt:    []ast.StmtNode{&ast.LoadDataStmt{}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelError
				l[errctx.ErrGroupDupKey] = errctx.LevelError
				l[errctx.ErrGroupBadNull] = errctx.LevelError
				l[errctx.ErrGroupNoDefault] = errctx.LevelError
				l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
				return
			}(),
		},
		{
			name:    "non-strict,load_data",
			sqlMode: mysql.SQLMode(0),
			stmt:    []ast.StmtNode{&ast.LoadDataStmt{}},
			levels: func() (l errctx.LevelMap) {
				l[errctx.ErrGroupTruncate] = errctx.LevelError
				l[errctx.ErrGroupDupKey] = errctx.LevelError
				l[errctx.ErrGroupBadNull] = errctx.LevelError
				l[errctx.ErrGroupNoDefault] = errctx.LevelError
				l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				l[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelError
				l[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
				return
			}(),
		},
	}

	for i, c := range cases {
		require.NotEmpty(t, c.stmt, c.name)
		for _, stmt := range c.stmt {
			msg := fmt.Sprintf("%d: %s, stmt: %T", i, c.name, stmt)
			ctx.GetSessionVars().SQLMode = c.sqlMode
			require.NoError(t, ResetContextOfStmt(ctx, stmt), msg)
			ec := ctx.GetSessionVars().StmtCtx.ErrCtx()
			require.Equal(t, c.levels, ec.LevelMap(), msg)
		}
	}
}
