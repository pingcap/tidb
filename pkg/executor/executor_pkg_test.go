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
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/hashicorp/go-version"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
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
	require.Equal(t, int64(20760), bytesConsumed1)
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

// Test whether the actual buckets in Golang Map is same with the estimated number.
// The test relies on the implement of Golang Map. ref https://github.com/golang/go/blob/go1.13/src/runtime/map.go#L114
func TestAggPartialResultMapperB(t *testing.T) {
	// skip err, since we guarantee the success of execution
	go113, _ := version.NewVersion(`1.13`)
	// go version format is `gox.y.z foobar`, we only need x.y.z part
	// The following is pretty hacky, but it only in test which is ok to do so.
	actualVer, err := version.NewVersion(runtime.Version()[2:6])
	if err != nil {
		t.Fatalf("Cannot get actual go version with error %v\n", err)
	}
	if actualVer.LessThan(go113) {
		t.Fatalf("Unsupported version and should never use any version less than go1.13\n")
	}
	type testCase struct {
		rowNum          int
		expectedB       int
		expectedGrowing bool
	}
	var cases []testCase
	// https://github.com/golang/go/issues/63438
	// in 1.21, the load factor of map is 6 rather than 6.5 and the go team refused to backport to 1.21.
	if strings.Contains(runtime.Version(), `go1.21`) {
		cases = []testCase{
			{
				rowNum:          0,
				expectedB:       0,
				expectedGrowing: false,
			},
			{
				rowNum:          95,
				expectedB:       4,
				expectedGrowing: false,
			},
			{
				rowNum:          10000, // 6 * (1 << 11) is 12288
				expectedB:       11,
				expectedGrowing: false,
			},
			{
				rowNum:          1000000, // 6 * (1 << 18) is 1572864
				expectedB:       18,
				expectedGrowing: false,
			},
			{
				rowNum:          786432, // 6 * (1 << 17)
				expectedB:       17,
				expectedGrowing: false,
			},
			{
				rowNum:          786433, // 6 * (1 << 17) + 1
				expectedB:       18,
				expectedGrowing: true,
			},
			{
				rowNum:          393216, // 6 * (1 << 16)
				expectedB:       16,
				expectedGrowing: false,
			},
			{
				rowNum:          393217, // 6 * (1 << 16) + 1
				expectedB:       17,
				expectedGrowing: true,
			},
		}
	} else {
		cases = []testCase{
			{
				rowNum:          0,
				expectedB:       0,
				expectedGrowing: false,
			},
			{
				rowNum:          100,
				expectedB:       4,
				expectedGrowing: false,
			},
			{
				rowNum:          10000,
				expectedB:       11,
				expectedGrowing: false,
			},
			{
				rowNum:          1000000,
				expectedB:       18,
				expectedGrowing: false,
			},
			{
				rowNum:          851968, // 6.5 * (1 << 17)
				expectedB:       17,
				expectedGrowing: false,
			},
			{
				rowNum:          851969, // 6.5 * (1 << 17) + 1
				expectedB:       18,
				expectedGrowing: true,
			},
			{
				rowNum:          425984, // 6.5 * (1 << 16)
				expectedB:       16,
				expectedGrowing: false,
			},
			{
				rowNum:          425985, // 6.5 * (1 << 16) + 1
				expectedB:       17,
				expectedGrowing: true,
			},
		}
	}

	for _, tc := range cases {
		aggMap := make(aggfuncs.AggPartialResultMapper)
		tempSlice := make([]aggfuncs.PartialResult, 10)
		for num := 0; num < tc.rowNum; num++ {
			aggMap[strconv.Itoa(num)] = tempSlice
		}

		require.Equal(t, tc.expectedB, getB(aggMap))
		require.Equal(t, tc.expectedGrowing, getGrowing(aggMap))
	}
}

// A header for a Go map.
// nolint:structcheck
type hmap struct {
	// Note: the format of the hmap is also encoded in cmd/compile/internal/gc/reflect.go.
	// Make sure this stays in sync with the compiler's definition.
	count     int    // nolint:unused // # live cells == size of map.  Must be first (used by len() builtin)
	flags     uint8  // nolint:unused
	B         uint8  // nolint:unused // log_2 of # of buckets (can hold up to loadFactor * 2^B items)
	noverflow uint16 // nolint:unused // approximate number of overflow buckets; see incrnoverflow for details
	hash0     uint32 // nolint:unused // hash seed

	buckets    unsafe.Pointer // nolint:unused // array of 2^B Buckets. may be nil if count==0.
	oldbuckets unsafe.Pointer // nolint:unused // previous bucket array of half the size, non-nil only when growing
	nevacuate  uintptr        // nolint:unused // progress counter for evacuation (buckets less than this have been evacuated)
}

func getB(m aggfuncs.AggPartialResultMapper) int {
	point := (**hmap)(unsafe.Pointer(&m))
	value := *point
	return int(value.B)
}

func getGrowing(m aggfuncs.AggPartialResultMapper) bool {
	point := (**hmap)(unsafe.Pointer(&m))
	value := *point
	return value.oldbuckets != nil
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
	domain.BindDomain(ctx, &domain.Domain{})

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
